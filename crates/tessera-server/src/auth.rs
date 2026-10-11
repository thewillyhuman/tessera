// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Who a request is (CT-5, SE-1). A request presents an API key as
//! `Authorization: Bearer tsk_<tenant>_<id>_<secret>`; the key's tenant
//! names the control store that holds it, the secret is checked against
//! the stored hash, and the key's roles become the principal's. A
//! request without credentials is anonymous: a cluster administrator
//! where anonymous access is enabled, which `tessera run` does, and
//! refused with 401 elsewhere. Principals are kept a few seconds per
//! key, so a stream of requests costs one lookup; a revoked key is
//! forgotten at once on the node that revoked it and within the keep
//! time elsewhere.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::http::HeaderMap;
use tessera_catalog::auth::{self as keys, ApiKey};
use tessera_catalog::{AccessRole, Principal, Table, TenantStatus};

use crate::{ApiError, AppState};

/// How long a resolved principal is kept.
pub const TTL: Duration = Duration::from_secs(5);

/// A node's authentication settings and its kept principals.
pub struct Auth {
    allow_anonymous: bool,
    /// The secret the nodes of a cluster present to each other on the
    /// `/internal` routes (SE-3); none means those routes are open, as
    /// on a single node or a private network.
    cluster_secret: Option<String>,
    kept: Mutex<HashMap<String, Kept>>,
}

struct Kept {
    at: Instant,
    id: String,
    principal: Principal,
}

impl Auth {
    /// Settings allowing anonymous access or not.
    #[must_use]
    pub fn new(allow_anonymous: bool) -> Self {
        Auth {
            allow_anonymous,
            cluster_secret: None,
            kept: Mutex::new(HashMap::new()),
        }
    }

    /// These settings with the `/internal` routes guarded by `secret`.
    #[must_use]
    pub fn with_cluster_secret(mut self, secret: Option<String>) -> Self {
        self.cluster_secret = secret.filter(|s| !s.is_empty());
        self
    }

    /// Whether requests without credentials are served.
    #[must_use]
    pub fn allows_anonymous(&self) -> bool {
        self.allow_anonymous
    }

    /// Checks that a request to an `/internal` route carries the
    /// cluster secret, where one is configured.
    ///
    /// # Errors
    ///
    /// 401 when the secret is missing or wrong.
    pub fn check_cluster(&self, headers: &HeaderMap) -> Result<(), ApiError> {
        let Some(expected) = &self.cluster_secret else {
            return Ok(());
        };
        match bearer(headers) {
            Some(presented) if presented == expected => Ok(()),
            _ => Err(ApiError::unauthorized(
                "the cluster secret is required on internal routes",
            )),
        }
    }

    /// Forgets the principals kept longer than they are valid (CT-6).
    pub fn forget_expired(&self) {
        self.kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|_, kept| kept.at.elapsed() < TTL);
    }

    /// Forgets the principals of key `id`, after it was revoked.
    pub fn forget(&self, id: &str) {
        self.kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|_, kept| kept.id != id);
    }

    fn lookup(&self, hashed: &str) -> Option<Principal> {
        let kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
        kept.get(hashed)
            .filter(|k| k.at.elapsed() < TTL)
            .map(|k| k.principal.clone())
    }

    fn keep(&self, hashed: String, id: String, principal: &Principal) {
        self.kept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                hashed,
                Kept {
                    at: Instant::now(),
                    id,
                    principal: principal.clone(),
                },
            );
    }
}

/// The principal of a request with `headers`.
///
/// # Errors
///
/// 401 without usable credentials where anonymous access is off, or
/// with a key that is unknown, expired or wrong; 403 for a key of a
/// tenant that is not active.
pub async fn principal(state: &AppState, headers: &HeaderMap) -> Result<Principal, ApiError> {
    match bearer(headers) {
        Some(text) => with_key(state, text).await,
        None => anonymous(state),
    }
}

/// The principal of a request without credentials.
///
/// # Errors
///
/// 401 where anonymous access is off.
pub fn anonymous(state: &AppState) -> Result<Principal, ApiError> {
    if state.auth.allows_anonymous() {
        Ok(Principal::anonymous())
    } else {
        Err(ApiError::unauthorized(
            "an API key is required: Authorization: Bearer tsk_...",
        ))
    }
}

/// The principal of the key `text`.
///
/// # Errors
///
/// As [`principal`].
pub async fn with_key(state: &AppState, text: &str) -> Result<Principal, ApiError> {
    let hashed = keys::hash(text);
    if let Some(principal) = state.auth.lookup(&hashed) {
        return Ok(principal);
    }
    let unknown = || ApiError::unauthorized("unknown or expired API key");
    let parsed = keys::parse(text).ok_or_else(|| ApiError::unauthorized("not an API key"))?;
    let tenant = state
        .catalog
        .cached()
        .state()
        .tenant_by_name(&parsed.tenant)
        .cloned()
        .ok_or_else(unknown)?;
    if tenant.status != TenantStatus::Active {
        return Err(ApiError::forbidden(format!(
            "tenant {} is not active",
            tenant.name
        )));
    }
    let control = state.controls.open(&tenant.id);
    let document = control
        .get(Table::ApiKeys, &parsed.id)
        .await
        .map_err(|e| ApiError::unavailable(e.to_string()))?
        .ok_or_else(unknown)?;
    let key: ApiKey = serde_json::from_value(document).map_err(|_| unknown())?;
    if !key.verifies(&parsed.secret, state.env.clock.now()) {
        return Err(unknown());
    }
    let roles: Vec<AccessRole> = control
        .rows(Table::Roles)
        .await
        .map_err(|e| ApiError::unavailable(e.to_string()))?
        .into_iter()
        .filter_map(|row| serde_json::from_value(row.value).ok())
        .collect();
    let principal = Principal::of(tenant.id, &key, &roles);
    state.auth.keep(hashed, key.id, &principal);
    Ok(principal)
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))?;
    Some(token.trim())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;

    async fn call(
        state: &AppState,
        method: &str,
        uri: &str,
        key: Option<&str>,
        headers: &[(&str, &str)],
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder().method(method).uri(uri);
        if let Some(key) = key {
            request = request.header(header::AUTHORIZATION, format!("Bearer {key}"));
        }
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let request = match body {
            Some(body) => request
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
            None => request.body(Body::empty()).unwrap(),
        };
        let response = crate::router(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value)
    }

    fn query(dataset: &str) -> Value {
        json!({ "dataset": dataset, "query": "SELECT count(*) FROM logs" })
    }

    fn logs() -> Value {
        json!({ "resourceLogs": [{ "scopeLogs": [{ "logRecords": [{
            "timeUnixNano": "1700000000000000000", "body": { "stringValue": "hi" } }] }] }] })
    }

    /// A state with anonymous access off, after `prepare` ran with it on.
    async fn locked(prepare: impl AsyncFnOnce(&AppState)) -> AppState {
        let mut state = crate::testing::state().await;
        prepare(&state).await;
        state.auth = Arc::new(Auth::new(false));
        state
    }

    async fn issue(state: &AppState, tenant: &str, name: &str, roles: &[&str]) -> String {
        let (status, created) = call(
            state,
            "POST",
            &format!("/api/v1/tenants/{tenant}/keys"),
            None,
            &[],
            Some(json!({ "name": name, "roles": roles })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{created}");
        created["key"].as_str().unwrap().to_owned()
    }

    /// CT-9: a cluster administrator names another tenant's table as
    /// `tenant.dataset.table`; a tenant's own credentials cannot.
    #[tokio::test]
    async fn cross_tenant_names_need_cluster_grants() {
        let state = crate::testing::state().await;
        let (status, body) = call(
            &state,
            "POST",
            "/api/v1/tenants",
            None,
            &[],
            Some(json!({ "name": "acme" })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let (status, body) = call(
            &state,
            "POST",
            "/v1/logs",
            None,
            &[("x-scope-orgid", "acme"), ("x-tessera-dataset", "prod")],
            Some(logs()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let across =
            json!({ "dataset": "default", "query": "SELECT count(*) AS n FROM acme.prod.logs" });
        let (status, body) = call(
            &state,
            "POST",
            "/api/v1/query",
            None,
            &[],
            Some(across.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["rows"][0][0], 1.0);
        let reader = issue(&state, "default", "r", &["reader"]).await;
        let (status, body) = call(
            &state,
            "POST",
            "/api/v1/query",
            Some(&reader),
            &[],
            Some(across),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("tenant acme is not known or not readable"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn without_credentials_requests_are_refused_once_anonymous_access_is_off() {
        let state = locked(async |_| {}).await;
        let (status, body) = call(
            &state,
            "POST",
            "/api/v1/query",
            None,
            &[],
            Some(query("prod")),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
        let (status, _) = call(&state, "GET", "/api/v1/tenants", None, &[], None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = call(&state, "POST", "/v1/logs", None, &[], Some(logs())).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let (status, _) = call(&state, "GET", "/healthz", None, &[], None).await;
        assert_eq!(status, StatusCode::OK, "liveness stays open");
        let (status, _) = call(
            &state,
            "POST",
            "/api/v1/query",
            Some("tsk_default_x_y"),
            &[],
            Some(query("prod")),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "an unknown key");
    }

    #[tokio::test]
    async fn a_tenant_key_acts_within_its_roles_and_tenant() {
        let mut reader = String::new();
        let mut writer = String::new();
        let state = locked(async |state| {
            let (status, body) = call(
                state,
                "POST",
                "/api/v1/tenants",
                None,
                &[],
                Some(json!({ "name": "acme" })),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED, "{body}");
            reader = issue(state, "acme", "grafana", &["reader"]).await;
            writer = issue(state, "acme", "collector", &["writer"]).await;
        })
        .await;
        let dataset = [("x-tessera-dataset", "prod")];
        let (status, body) = call(
            &state,
            "POST",
            "/v1/logs",
            Some(&writer),
            &dataset,
            Some(logs()),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call(
            &state,
            "POST",
            "/v1/logs",
            Some(&reader),
            &dataset,
            Some(logs()),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "a reader does not write: {body}"
        );
        let (status, body) = call(
            &state,
            "POST",
            "/api/v1/query",
            Some(&reader),
            &[],
            Some(query("prod")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["rows"][0][0], 1.0, "its own tenant's rows: {body}");
        let (status, body) = call(
            &state,
            "POST",
            "/api/v1/query",
            Some(&writer),
            &[],
            Some(query("prod")),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "a writer does not read: {body}"
        );
        let other = [("x-scope-orgid", "default")];
        let (status, body) = call(
            &state,
            "POST",
            "/api/v1/query",
            Some(&reader),
            &other,
            Some(query("prod")),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "another tenant: {body}");
        let (status, _) = call(&state, "GET", "/api/v1/tenants", Some(&reader), &[], None).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "not a cluster administrator");
        let (status, body) = call(
            &state,
            "POST",
            "/api/v1/query",
            Some(&reader),
            &[],
            Some(json!({ "dataset": "prod", "query": "CREATE ALERT a AS SELECT 1" })),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "control statements need an admin: {body}"
        );
    }

    #[tokio::test]
    async fn keys_are_listed_without_secrets_and_stop_working_when_deleted() {
        let mut key = String::new();
        // Only a _system admin administers the cluster; a tenant's admin
        // administers that tenant.
        let state = locked(async |state| {
            key = issue(state, "_system", "ci", &["admin"]).await;
        })
        .await;
        let (status, listed) = call(
            &state,
            "GET",
            "/api/v1/tenants/_system/keys",
            Some(&key),
            &[],
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{listed}");
        assert_eq!(listed[0]["name"], "ci");
        assert!(listed[0].get("secret_hash").is_none(), "{listed}");
        let id = listed[0]["id"].as_str().unwrap().to_owned();
        let (status, body) = call(
            &state,
            "POST",
            "/api/v1/query",
            Some(&key),
            &[],
            Some(query("prod")),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, _) = call(
            &state,
            "DELETE",
            &format!("/api/v1/tenants/_system/keys/{id}"),
            Some(&key),
            &[],
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, _) = call(
            &state,
            "POST",
            "/api/v1/query",
            Some(&key),
            &[],
            Some(query("prod")),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "revoked at once");
    }
}
