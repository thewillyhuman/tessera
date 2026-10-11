// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Structured log bodies at ingest (DM-10), as the tenant's settings
//! ask: JSON objects by default, logfmt lines on request.

use tessera_kernels::{BodyParsing, parse_bodies};
use tessera_types::{EventBatch, Kind};

use crate::{ApiError, AppState};

/// `batch` with the fields of its structured bodies stored as `body.`
/// attributes, when it holds logs and the tenant parses them.
///
/// # Errors
///
/// If the rows cannot be rebuilt.
pub fn apply(state: &AppState, batch: EventBatch) -> Result<EventBatch, ApiError> {
    if batch.kind() != Kind::Log {
        return Ok(batch);
    }
    let parsing = {
        let cached = state.catalog.cached();
        let settings = cached.state().tenant(batch.tenant()).map(|t| &t.settings);
        BodyParsing {
            json: settings.and_then(|s| s.parse_json_bodies).unwrap_or(true),
            logfmt: settings
                .and_then(|s| s.parse_logfmt_bodies)
                .unwrap_or(false),
        }
    };
    if !parsing.json && !parsing.logfmt {
        return Ok(batch);
    }
    let rows =
        parse_bodies(batch.rows(), parsing).map_err(|e| ApiError::bad_request(e.to_string()))?;
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
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tessera_catalog::{TenantName, TenantSettings};
    use tower::ServiceExt;

    use crate::AppState;
    use crate::otlp::testing::export;

    async fn write(state: &AppState, bodies: &[&str]) {
        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(header::CONTENT_TYPE, "application/json")
            .header(crate::otlp::DATASET_HEADER, "prod")
            .body(Body::from(serde_json::to_vec(&export(bodies)).unwrap()))
            .unwrap();
        let response = crate::router(state.clone()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    async fn sql(state: &AppState, query: &str) -> Value {
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
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(status, StatusCode::OK, "{value}");
        value
    }

    #[tokio::test]
    async fn json_bodies_are_queried_as_body_paths_unless_the_tenant_says_not() {
        let state = crate::testing::state().await;
        write(
            &state,
            &["{\"user\":\"ann\",\"http\":{\"status\":500}}", "plain"],
        )
        .await;
        let value = sql(
            &state,
            "SELECT body.user, body.http.status, body FROM logs ORDER BY timestamp",
        )
        .await;
        assert_eq!(value["rows"][0][0], "ann");
        assert_eq!(value["rows"][0][1], "500");
        assert_eq!(
            value["rows"][0][2], "{\"user\":\"ann\",\"http\":{\"status\":500}}",
            "the text stays"
        );
        assert_eq!(value["rows"][1][0], Value::Null);
        let value = sql(
            &state,
            "SELECT count(*) FROM logs WHERE body.http.status = 500",
        )
        .await;
        assert_eq!(value["rows"][0][0], 1.0);
        state
            .catalog
            .alter_tenant(
                &TenantName::new("default").unwrap(),
                TenantSettings {
                    parse_json_bodies: Some(false),
                    ..TenantSettings::default()
                },
            )
            .await
            .unwrap();
        write(&state, &["{\"user\":\"bob\"}"]).await;
        let value = sql(&state, "SELECT count(*) FROM logs WHERE body.user = 'bob'").await;
        assert_eq!(value["rows"][0][0], 0.0, "parsing is off");
    }
}
