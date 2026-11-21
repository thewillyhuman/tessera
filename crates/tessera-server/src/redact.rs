// SPDX-License-Identifier: Apache-2.0

//! Redaction at ingest (SE-4): a tenant's rules are applied to every
//! batch on the way in, before budgets, metering and the writer log, so
//! that a redacted value is never written anywhere.

use tessera_kernels::redact;
use tessera_types::EventBatch;

use crate::{ApiError, AppState};

/// `batch` with its tenant's redaction rules applied.
///
/// # Errors
///
/// If a column cannot be rebuilt.
pub fn apply(state: &AppState, batch: EventBatch) -> Result<EventBatch, ApiError> {
    let rules = state
        .catalog
        .cached()
        .state()
        .tenant(batch.tenant())
        .map(|t| t.settings.redactions.clone())
        .unwrap_or_default();
    if rules.is_empty() {
        return Ok(batch);
    }
    let rows =
        redact::apply(batch.rows(), &rules).map_err(|e| ApiError::bad_request(e.to_string()))?;
    EventBatch::new(
        batch.tenant().clone(),
        batch.dataset().clone(),
        batch.kind(),
        rows,
    )
    .map_err(|e| ApiError::bad_request(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tessera_catalog::{TenantName, TenantSettings};
    use tessera_types::event::Scalar;
    use tessera_types::{DatasetName, LogRow, Redaction, RedactionAction, TenantId, Timestamp};
    use tower::ServiceExt;

    use super::*;

    async fn sql(state: &AppState, query: &str) -> (StatusCode, Value) {
        let body = json!({ "dataset": "prod", "query": query });
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = crate::router(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn a_tenants_rules_apply_before_anything_is_written() {
        let state = crate::testing::state().await;
        state
            .catalog
            .alter_tenant(
                &TenantName::new("default").unwrap(),
                TenantSettings {
                    redactions: vec![
                        Redaction {
                            path: "attr.user.*".into(),
                            action: RedactionAction::Hash,
                            pattern: None,
                        },
                        Redaction {
                            path: "body".into(),
                            action: RedactionAction::Mask,
                            pattern: None,
                        },
                    ],
                    ..TenantSettings::default()
                },
            )
            .await
            .unwrap();
        let row = LogRow {
            timestamp: Timestamp::from_nanos(1_700_000_000_000_000_000),
            event_id: [1; 16],
            body: Some("secret".into()),
            attributes: BTreeMap::from([
                ("attr.user.email".to_owned(), Scalar::Text("a@b.c".into())),
                ("attr.service".to_owned(), Scalar::Text("checkout".into())),
            ]),
            ..LogRow::default()
        };
        let batch =
            EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &[row])
                .unwrap();
        state.write(batch).await.unwrap();
        let (status, answer) = sql(
            &state,
            "SELECT body, attr.user.email AS email, attr.service AS service FROM logs",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{answer}");
        let row = &answer["rows"][0];
        assert_eq!(row[0], "***", "{answer}");
        assert_eq!(row[1].as_str().unwrap().len(), 64, "{answer}");
        assert_eq!(row[2], "checkout", "{answer}");
    }
}
