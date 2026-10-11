// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The audit dataset (SE-6): administrative actions and control
//! statements are recorded as events of `_system.audit`, one per action
//! that went through, with who did it, the tenant it concerned and what
//! it was. Events are written through the node's own write path without
//! holding the request up; a cluster administrator reads them with
//! `X-Scope-OrgID: _system` and dataset `audit`.

use std::collections::BTreeMap;

use tessera_catalog::Principal;
use tessera_types::event::Scalar;
use tessera_types::{Attributes, DatasetName, EventBatch, EventRow, Random, TenantId};

use crate::AppState;

/// The dataset of the `_system` tenant that holds the audit.
pub const DATASET: &str = "audit";

/// Records that `principal` did `action` on `subject` in `tenant`.
pub fn record(
    state: &AppState,
    principal: &Principal,
    tenant: &TenantId,
    action: &str,
    subject: &str,
) {
    let now = state.env.clock.now();
    let mut attributes: Attributes = BTreeMap::new();
    attributes.insert(
        "attr.principal".into(),
        Scalar::Text(principal.name.clone()),
    );
    attributes.insert(
        "attr.principal_tenant".into(),
        Scalar::Text(principal.tenant.to_string()),
    );
    attributes.insert("attr.tenant".into(), Scalar::Text(tenant.to_string()));
    attributes.insert("attr.action".into(), Scalar::Text(action.to_owned()));
    attributes.insert("attr.subject".into(), Scalar::Text(subject.to_owned()));
    let row = EventRow {
        timestamp: now,
        observed_timestamp: now,
        event_id: state.env.random.clone().next_u128().to_be_bytes(),
        trace_id: None,
        span_id: None,
        name: action.to_owned(),
        attributes,
    };
    let Ok(dataset) = DatasetName::new(DATASET) else {
        return;
    };
    let Ok(batch) = EventBatch::from_events(TenantId::SYSTEM, dataset, &[row]) else {
        return;
    };
    tracing::info!(principal = %principal.name, %tenant, action, subject, "audit");
    let state = state.clone();
    tokio::spawn(async move {
        if let Err(error) = state.write(batch).await {
            tracing::warn!(error = %error.message(), "audit event not written");
        }
    });
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
        headers: &[(&str, &str)],
        body: Value,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let request = request.body(Body::from(body.to_string())).unwrap();
        let response = crate::router(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn administration_and_control_statements_leave_audit_events() {
        let state = crate::testing::state().await;
        let (status, body) = call(
            &state,
            "POST",
            "/api/v1/tenants",
            &[],
            json!({ "name": "acme" }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let (status, body) = call(
            &state,
            "POST",
            "/api/v1/query",
            &[],
            json!({ "dataset": "prod", "query": "CREATE ALERT noisy AS SELECT count(*) AS n FROM logs HAVING count(*) > 1" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (status, audit) = call(
            &state,
            "POST",
            "/api/v1/query",
            &[("x-scope-orgid", "_system")],
            json!({ "dataset": "audit", "query": "SELECT name, attr.principal AS who, attr.tenant AS tenant, attr.subject AS subject FROM events ORDER BY timestamp" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{audit}");
        let rows = audit["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2, "{audit}");
        assert_eq!(rows[0][0], "tenant.create");
        assert_eq!(rows[0][1], "anonymous");
        assert_eq!(rows[0][3], "acme");
        assert_eq!(rows[1][0], "control");
        assert_eq!(rows[1][2], "default");
        assert_eq!(rows[1][3], "CREATE ALERT noisy");
    }
}
