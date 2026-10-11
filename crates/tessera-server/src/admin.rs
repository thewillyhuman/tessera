// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The HTTP administration API (CT-2): tenants are created, altered,
//! suspended, dropped and restored while the system runs.
//!
//! | Method and path                         | Effect                          |
//! |----------------------------------------|---------------------------------|
//! | `GET /healthz`                         | Liveness                        |
//! | `GET /api/v1/tenants`                  | List tenants                    |
//! | `POST /api/v1/tenants`                 | Create: `{name, settings?}`     |
//! | `GET /api/v1/tenants/{name}`           | One tenant                      |
//! | `PATCH /api/v1/tenants/{name}`         | Merge settings                  |
//! | `POST /api/v1/tenants/{name}/rename`   | `{new_name}`                    |
//! | `POST /api/v1/tenants/{name}/suspend`  | Suspend                         |
//! | `POST /api/v1/tenants/{name}/resume`   | Resume                          |
//! | `DELETE /api/v1/tenants/{name}`        | Drop (restorable, CT-8)         |
//! | `POST /api/v1/tenants/{name}/restore`  | Restore a dropped tenant        |
//!
//! Errors follow the shape described in the crate's error module.
//! | `GET /api/v1/tenants/{name}/keys`      | List API keys, without secrets |
//! | `POST /api/v1/tenants/{name}/keys`     | Issue: `{name, roles?, expires_in_seconds?}` |
//! | `DELETE /api/v1/tenants/{name}/keys/{id}` | Revoke                       |
//!
//! Everything but liveness needs a cluster administrator (CT-5): a key
//! of the `_system` tenant with the admin role, or anonymous access
//! where it is enabled.

use std::time::Duration;

use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, Path, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tessera_catalog::{
    AccessRole, ApiKey, CatalogError, Table, Tenant, TenantName, TenantSettings,
};
use tessera_types::Timestamp;

use crate::{ApiError, AppState};

/// The administration router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/tenants", get(list_tenants).post(create_tenant))
        .route(
            "/api/v1/tenants/{name}",
            get(get_tenant).patch(alter_tenant).delete(drop_tenant),
        )
        .route("/api/v1/tenants/{name}/rename", post(rename_tenant))
        .route("/api/v1/tenants/{name}/suspend", post(suspend_tenant))
        .route("/api/v1/tenants/{name}/resume", post(resume_tenant))
        .route("/api/v1/tenants/{name}/restore", post(restore_tenant))
        .route(
            "/api/v1/tenants/{name}/keys",
            get(list_keys).post(issue_key),
        )
        .route("/api/v1/tenants/{name}/keys/{id}", delete(revoke_key))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_cluster_admin,
        ))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
}

/// Lets a request through when it comes from a cluster administrator.
async fn require_cluster_admin(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let principal = crate::auth::principal(&state, request.headers()).await?;
    if !principal.cluster_admin {
        return Err(ApiError::forbidden(
            "cluster administration needs a _system admin key",
        ));
    }
    Ok(next.run(request).await)
}

/// The body of `POST /api/v1/tenants/{name}/keys`.
#[derive(Deserialize)]
struct IssueKey {
    name: String,
    #[serde(default = "reader")]
    roles: Vec<String>,
    #[serde(default)]
    expires_in_seconds: Option<u64>,
}

fn reader() -> Vec<String> {
    vec![AccessRole::READER.to_owned()]
}

/// A key as listed: the record without its secret hash.
#[derive(Serialize)]
struct KeyView {
    id: String,
    name: String,
    roles: Vec<String>,
    created_at: Timestamp,
    expires_at: Option<Timestamp>,
}

impl From<ApiKey> for KeyView {
    fn from(key: ApiKey) -> Self {
        KeyView {
            id: key.id,
            name: key.name,
            roles: key.roles,
            created_at: key.created_at,
            expires_at: key.expires_at,
        }
    }
}

/// A key as issued: the view plus the text shown once.
#[derive(Serialize)]
struct IssuedView {
    key: String,
    #[serde(flatten)]
    view: KeyView,
}

fn tenant_of(state: &AppState, name: &str) -> Result<Tenant, ApiError> {
    let name = self::name(name)?;
    state
        .catalog
        .cached()
        .state()
        .tenant_by_name(&name)
        .cloned()
        .ok_or_else(|| ApiError::not_found(format!("unknown tenant {name}")))
}

async fn issue_key(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Body(request): Body<IssueKey>,
) -> Result<(StatusCode, Json<IssuedView>), ApiError> {
    let tenant = tenant_of(&state, &name)?;
    let now = state.env.clock.now();
    let expires_at = request
        .expires_in_seconds
        .and_then(|s| now.checked_add(Duration::from_secs(s)));
    let issued = tessera_catalog::auth::issue(
        &tenant.name,
        &request.name,
        request.roles,
        expires_at,
        now,
        &mut state.env.random.clone(),
    );
    let document =
        serde_json::to_value(&issued.record).map_err(|e| ApiError::bad_request(e.to_string()))?;
    state
        .controls
        .open(&tenant.id)
        .put(Table::ApiKeys, &issued.record.id, document)
        .await
        .map_err(|e| ApiError::unavailable(e.to_string()))?;
    tracing::info!(tenant = %tenant.name, key = %issued.record.id, name = %issued.record.name, "api key issued");
    Ok((
        StatusCode::CREATED,
        Json(IssuedView {
            key: issued.text,
            view: issued.record.into(),
        }),
    ))
}

async fn list_keys(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Vec<KeyView>>, ApiError> {
    let tenant = tenant_of(&state, &name)?;
    let rows = state
        .controls
        .open(&tenant.id)
        .rows(Table::ApiKeys)
        .await
        .map_err(|e| ApiError::unavailable(e.to_string()))?;
    Ok(Json(
        rows.into_iter()
            .filter_map(|row| serde_json::from_value::<ApiKey>(row.value).ok())
            .map(KeyView::from)
            .collect(),
    ))
}

async fn revoke_key(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let tenant = tenant_of(&state, &name)?;
    let control = state.controls.open(&tenant.id);
    if control
        .get(Table::ApiKeys, &id)
        .await
        .map_err(|e| ApiError::unavailable(e.to_string()))?
        .is_none()
    {
        return Err(ApiError::not_found(format!("unknown key {id}")));
    }
    control
        .delete(Table::ApiKeys, &id)
        .await
        .map_err(|e| ApiError::unavailable(e.to_string()))?;
    state.auth.forget(&id);
    tracing::info!(tenant = %tenant.name, key = %id, "api key revoked");
    Ok(StatusCode::NO_CONTENT)
}

/// A request body, with malformed JSON reported in the API's error shape.
struct Body<T>(T);

impl<S: Send + Sync, T: serde::de::DeserializeOwned> FromRequest<S> for Body<T> {
    type Rejection = ApiError;

    async fn from_request(request: axum::extract::Request, state: &S) -> Result<Self, ApiError> {
        match Json::<T>::from_request(request, state).await {
            Ok(Json(value)) => Ok(Body(value)),
            Err(rejection) => Err(bad_json(&rejection)),
        }
    }
}

fn bad_json(rejection: &JsonRejection) -> ApiError {
    ApiError::bad_request(rejection.body_text())
}

/// The body of `POST /api/v1/tenants`.
#[derive(Deserialize)]
struct CreateTenant {
    name: TenantName,
    #[serde(default)]
    settings: TenantSettings,
}

/// The body of `POST /api/v1/tenants/{name}/rename`.
#[derive(Deserialize)]
struct RenameTenant {
    new_name: TenantName,
}

fn name(text: &str) -> Result<TenantName, ApiError> {
    TenantName::new(text).map_err(|e| ApiError::bad_request(e.to_string()))
}

async fn list_tenants(State(state): State<AppState>) -> Result<Json<Vec<Tenant>>, ApiError> {
    let snapshot = state.catalog.snapshot().await?;
    Ok(Json(snapshot.state().tenants().cloned().collect()))
}

async fn create_tenant(
    State(state): State<AppState>,
    Body(request): Body<CreateTenant>,
) -> Result<(StatusCode, Json<Tenant>), ApiError> {
    let tenant = state
        .catalog
        .create_tenant(request.name, request.settings)
        .await?;
    tracing::info!(tenant = %tenant.name, id = %tenant.id, "tenant created");
    Ok((StatusCode::CREATED, Json(tenant)))
}

async fn get_tenant(
    State(state): State<AppState>,
    Path(text): Path<String>,
) -> Result<Json<Tenant>, ApiError> {
    let name = name(&text)?;
    let snapshot = state.catalog.snapshot().await?;
    let tenant = snapshot
        .state()
        .tenant_by_name(&name)
        .cloned()
        .ok_or(CatalogError::UnknownTenant(name))?;
    Ok(Json(tenant))
}

async fn alter_tenant(
    State(state): State<AppState>,
    Path(text): Path<String>,
    Body(patch): Body<TenantSettings>,
) -> Result<Json<Tenant>, ApiError> {
    Ok(Json(
        state.catalog.alter_tenant(&name(&text)?, patch).await?,
    ))
}

async fn rename_tenant(
    State(state): State<AppState>,
    Path(text): Path<String>,
    Body(request): Body<RenameTenant>,
) -> Result<Json<Tenant>, ApiError> {
    Ok(Json(
        state
            .catalog
            .rename_tenant(&name(&text)?, request.new_name)
            .await?,
    ))
}

async fn suspend_tenant(
    State(state): State<AppState>,
    Path(text): Path<String>,
) -> Result<Json<Tenant>, ApiError> {
    Ok(Json(state.catalog.suspend_tenant(&name(&text)?).await?))
}

async fn resume_tenant(
    State(state): State<AppState>,
    Path(text): Path<String>,
) -> Result<Json<Tenant>, ApiError> {
    Ok(Json(state.catalog.resume_tenant(&name(&text)?).await?))
}

async fn drop_tenant(
    State(state): State<AppState>,
    Path(text): Path<String>,
) -> Result<Json<Tenant>, ApiError> {
    let tenant = state.catalog.drop_tenant(&name(&text)?).await?;
    tracing::info!(tenant = %tenant.name, id = %tenant.id, "tenant dropped");
    Ok(Json(tenant))
}

async fn restore_tenant(
    State(state): State<AppState>,
    Path(text): Path<String>,
) -> Result<Json<Tenant>, ApiError> {
    Ok(Json(state.catalog.restore_tenant(&name(&text)?).await?))
}

#[cfg(test)]
mod tests {
    use axum::body::Body as HttpBody;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use tessera_catalog::TenantStatus;
    use tower::ServiceExt;

    use super::*;

    async fn app() -> Router {
        router(crate::testing::state().await)
    }

    async fn call(
        app: &Router,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> (StatusCode, serde_json::Value) {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.map_or_else(HttpBody::empty, |b| HttpBody::from(b.to_owned())))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::String(
                String::from_utf8_lossy(&bytes).into(),
            ))
        };
        (status, value)
    }

    #[tokio::test]
    async fn health_answers_ok() {
        let (status, body) = call(&app().await, "GET", "/healthz", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ok");
    }

    #[tokio::test]
    async fn lists_the_builtin_tenants() {
        let (status, body) = call(&app().await, "GET", "/api/v1/tenants", None).await;
        assert_eq!(status, StatusCode::OK);
        let names: Vec<&str> = body
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["_system", "default"]);
    }

    #[tokio::test]
    async fn creates_and_fetches_a_tenant() {
        let app = app().await;
        let body = r#"{"name":"acme","settings":{"query_concurrency":8}}"#;
        let (status, created) = call(&app, "POST", "/api/v1/tenants", Some(body)).await;
        assert_eq!(status, StatusCode::CREATED, "{created}");
        assert_eq!(created["name"], "acme");
        assert_eq!(created["settings"]["query_concurrency"], 8);
        let (status, fetched) = call(&app, "GET", "/api/v1/tenants/acme", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(fetched, created);
    }

    #[tokio::test]
    async fn a_duplicate_name_is_a_conflict() {
        let app = app().await;
        call(&app, "POST", "/api/v1/tenants", Some(r#"{"name":"acme"}"#)).await;
        let (status, body) =
            call(&app, "POST", "/api/v1/tenants", Some(r#"{"name":"acme"}"#)).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(body["error"].as_str().unwrap().contains("already exists"));
    }

    #[tokio::test]
    async fn malformed_requests_are_bad_requests() {
        let app = app().await;
        let (status, body) = call(
            &app,
            "POST",
            "/api/v1/tenants",
            Some(r#"{"name":"Not Valid"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        let (status, _) = call(&app, "POST", "/api/v1/tenants", Some("{not json")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = call(&app, "GET", "/api/v1/tenants/Not-Valid", None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn an_unknown_tenant_is_not_found() {
        let (status, _) = call(&app().await, "GET", "/api/v1/tenants/nobody", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn alters_renames_suspends_drops_and_restores() {
        let app = app().await;
        call(&app, "POST", "/api/v1/tenants", Some(r#"{"name":"acme"}"#)).await;
        let (status, altered) = call(
            &app,
            "PATCH",
            "/api/v1/tenants/acme",
            Some(r#"{"retention_seconds":60}"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{altered}");
        assert_eq!(altered["settings"]["retention_seconds"], 60);
        let (status, renamed) = call(
            &app,
            "POST",
            "/api/v1/tenants/acme/rename",
            Some(r#"{"new_name":"acme_corp"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{renamed}");
        assert_eq!(renamed["name"], "acme_corp");
        let (status, suspended) =
            call(&app, "POST", "/api/v1/tenants/acme_corp/suspend", None).await;
        assert_eq!(status, StatusCode::OK, "{suspended}");
        assert_eq!(
            suspended["status"],
            serde_json::to_value(TenantStatus::Suspended).unwrap()
        );
        let (status, _) = call(&app, "POST", "/api/v1/tenants/acme_corp/resume", None).await;
        assert_eq!(status, StatusCode::OK);
        let (status, dropped) = call(&app, "DELETE", "/api/v1/tenants/acme_corp", None).await;
        assert_eq!(status, StatusCode::OK, "{dropped}");
        assert_eq!(dropped["status"]["state"], "dropped");
        let (status, restored) =
            call(&app, "POST", "/api/v1/tenants/acme_corp/restore", None).await;
        assert_eq!(status, StatusCode::OK, "{restored}");
        assert_eq!(restored["status"]["state"], "active");
    }

    #[tokio::test]
    async fn builtin_tenants_cannot_be_dropped() {
        let (status, body) = call(&app().await, "DELETE", "/api/v1/tenants/default", None).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
    }
}
