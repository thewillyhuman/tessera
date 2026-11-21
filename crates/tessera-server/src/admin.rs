// SPDX-License-Identifier: Apache-2.0

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
//! | `GET /api/v1/settings`                 | Cluster-wide settings (CT-7)    |
//! | `PUT /api/v1/settings/{name}`          | Set one: `{value}`              |
//! | `DELETE /api/v1/settings/{name}`       | Remove one                      |
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
use axum::routing::{delete, get, post, put};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use tessera_catalog::{
    AccessRole, ApiKey, CatalogError, Principal, Table, Tenant, TenantName, TenantSettings,
};
use tessera_types::Timestamp;

use crate::{ApiError, AppState, audit};

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
        .route("/api/v1/settings", get(list_settings))
        .route(
            "/api/v1/settings/{name}",
            put(put_setting).delete(delete_setting),
        )
        .route("/api/v1/usage/reconcile", get(crate::reconcile::reconcile))
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
    let (parts, body) = request.into_parts();
    let principal = crate::auth::principal(&state, &parts).await?;
    if !principal.cluster_admin {
        return Err(ApiError::forbidden(
            "cluster administration needs a _system admin key",
        ));
    }
    let mut request = Request::from_parts(parts, body);
    request.extensions_mut().insert(principal);
    Ok(next.run(request).await)
}

/// The body of `POST /api/v1/tenants/{name}/keys`. A field it does not
/// know is refused: a misspelt `roles` would otherwise issue the key
/// with the default role and say nothing.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
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

async fn tenant_of(state: &AppState, name: &str) -> Result<Tenant, ApiError> {
    let name = self::name(name)?;
    state
        .catalog
        .tenant_named(&name)
        .await
        .ok_or_else(|| ApiError::not_found(format!("unknown tenant {name}")))
}

async fn issue_key(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
    Body(request): Body<IssueKey>,
) -> Result<(StatusCode, Json<IssuedView>), ApiError> {
    let tenant = tenant_of(&state, &name).await?;
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
    audit::record(
        &state,
        &principal,
        &tenant.id,
        "key.issue",
        &format!("{} ({})", issued.record.name, issued.record.id),
    );
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
    let tenant = tenant_of(&state, &name).await?;
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
    Extension(principal): Extension<Principal>,
    Path((name, id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let tenant = tenant_of(&state, &name).await?;
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
    audit::record(&state, &principal, &tenant.id, "key.revoke", &id);
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

/// Tenant settings as a request names them. Every name must be one a
/// tenant has: a setting misspelt is refused, where the settings
/// themselves, which a newer node may have written with names this
/// one does not know, let it pass.
#[derive(Default)]
struct Settings(TenantSettings);

impl<'de> Deserialize<'de> for Settings {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        let given = serde_json::Map::deserialize(deserializer)?;
        let known = serde_json::to_value(TenantSettings::default()).map_err(D::Error::custom)?;
        if let Some(unknown) = given.keys().find(|name| known.get(*name).is_none()) {
            return Err(D::Error::custom(format!("unknown setting `{unknown}`")));
        }
        serde_json::from_value(given.into())
            .map(Settings)
            .map_err(D::Error::custom)
    }
}

/// The body of `POST /api/v1/tenants`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateTenant {
    name: TenantName,
    #[serde(default)]
    settings: Settings,
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
    Extension(principal): Extension<Principal>,
    Body(request): Body<CreateTenant>,
) -> Result<(StatusCode, Json<Tenant>), ApiError> {
    let tenant = state
        .catalog
        .create_tenant(request.name, request.settings.0)
        .await?;
    audit::record(
        &state,
        &principal,
        &tenant.id,
        "tenant.create",
        tenant.name.as_str(),
    );
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
    Extension(principal): Extension<Principal>,
    Path(text): Path<String>,
    Body(Settings(patch)): Body<Settings>,
) -> Result<Json<Tenant>, ApiError> {
    let tenant = state.catalog.alter_tenant(&name(&text)?, patch).await?;
    audit::record(
        &state,
        &principal,
        &tenant.id,
        "tenant.alter",
        tenant.name.as_str(),
    );
    Ok(Json(tenant))
}

/// The value of a cluster-wide setting.
#[derive(Deserialize)]
struct SettingValue {
    value: String,
}

async fn list_settings(State(state): State<AppState>) -> Json<serde_json::Value> {
    let cached = state.catalog.cached();
    Json(serde_json::Value::Object(
        cached
            .state()
            .settings()
            .map(|(name, value)| (name.to_owned(), serde_json::Value::String(value.to_owned())))
            .collect(),
    ))
}

async fn put_setting(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
    Body(request): Body<SettingValue>,
) -> Result<StatusCode, ApiError> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_')
    {
        return Err(ApiError::bad_request(
            "a setting name is letters, digits, dots and underscores",
        ));
    }
    state
        .catalog
        .set_setting(&name, Some(request.value.clone()))
        .await?;
    audit::record(
        &state,
        &principal,
        &principal.tenant,
        "setting.put",
        &format!("{name} = {}", request.value),
    );
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_setting(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    state.catalog.set_setting(&name, None).await?;
    audit::record(
        &state,
        &principal,
        &principal.tenant,
        "setting.delete",
        &name,
    );
    Ok(StatusCode::NO_CONTENT)
}

async fn rename_tenant(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(text): Path<String>,
    Body(request): Body<RenameTenant>,
) -> Result<Json<Tenant>, ApiError> {
    let tenant = state
        .catalog
        .rename_tenant(&name(&text)?, request.new_name)
        .await?;
    audit::record(
        &state,
        &principal,
        &tenant.id,
        "tenant.rename",
        &format!("{text} -> {}", tenant.name),
    );
    Ok(Json(tenant))
}

async fn suspend_tenant(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(text): Path<String>,
) -> Result<Json<Tenant>, ApiError> {
    let tenant = state.catalog.suspend_tenant(&name(&text)?).await?;
    audit::record(
        &state,
        &principal,
        &tenant.id,
        "tenant.suspend",
        tenant.name.as_str(),
    );
    Ok(Json(tenant))
}

async fn resume_tenant(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(text): Path<String>,
) -> Result<Json<Tenant>, ApiError> {
    let tenant = state.catalog.resume_tenant(&name(&text)?).await?;
    audit::record(
        &state,
        &principal,
        &tenant.id,
        "tenant.resume",
        tenant.name.as_str(),
    );
    Ok(Json(tenant))
}

async fn drop_tenant(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(text): Path<String>,
) -> Result<Json<Tenant>, ApiError> {
    let tenant = state.catalog.drop_tenant(&name(&text)?).await?;
    audit::record(
        &state,
        &principal,
        &tenant.id,
        "tenant.drop",
        tenant.name.as_str(),
    );
    Ok(Json(tenant))
}

async fn restore_tenant(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(text): Path<String>,
) -> Result<Json<Tenant>, ApiError> {
    let tenant = state.catalog.restore_tenant(&name(&text)?).await?;
    audit::record(
        &state,
        &principal,
        &tenant.id,
        "tenant.restore",
        tenant.name.as_str(),
    );
    Ok(Json(tenant))
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
    async fn cluster_settings_are_managed_over_the_api() {
        let app = app().await;
        let (status, _) = call(
            &app,
            "PUT",
            "/api/v1/settings/price.ingest_byte",
            Some(r#"{"value":"0.001"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, _) = call(
            &app,
            "PUT",
            "/api/v1/settings/bad%20name",
            Some(r#"{"value":"x"}"#),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, listed) = call(&app, "GET", "/api/v1/settings", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed["price.ingest_byte"], "0.001");
        let (status, _) = call(&app, "DELETE", "/api/v1/settings/price.ingest_byte", None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (_, listed) = call(&app, "GET", "/api/v1/settings", None).await;
        assert!(listed.get("price.ingest_byte").is_none());
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

    /// A setting misspelt in a request would otherwise be dropped in
    /// silence, and the tenant left as it was with a 200.
    #[tokio::test]
    async fn a_setting_no_tenant_has_is_refused() {
        let app = app().await;
        call(&app, "POST", "/api/v1/tenants", Some(r#"{"name":"acme"}"#)).await;
        let (status, error) = call(
            &app,
            "PATCH",
            "/api/v1/tenants/acme",
            Some(r#"{"retention_secs":60}"#),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
        assert!(error.to_string().contains("retention_secs"), "{error}");
        let wrapped = r#"{"settings":{"retention_seconds":60}}"#;
        let (status, error) = call(&app, "PATCH", "/api/v1/tenants/acme", Some(wrapped)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");

        let misspelt = r#"{"name":"other","settings":{"query_concurency":2}}"#;
        let (status, error) = call(&app, "POST", "/api/v1/tenants", Some(misspelt)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
        let misplaced = r#"{"name":"other","retention_seconds":60}"#;
        let (status, error) = call(&app, "POST", "/api/v1/tenants", Some(misplaced)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    }

    #[tokio::test]
    async fn every_setting_a_tenant_has_is_taken() {
        let app = app().await;
        call(&app, "POST", "/api/v1/tenants", Some(r#"{"name":"acme"}"#)).await;
        let unset = serde_json::to_string(&TenantSettings::default()).unwrap();
        let (status, altered) = call(&app, "PATCH", "/api/v1/tenants/acme", Some(&unset)).await;
        assert_eq!(status, StatusCode::OK, "{altered}");
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
