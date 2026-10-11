// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Queries over HTTP: `POST /api/v1/query` with a JSON body
//! `{"query": "...", "language": "sql", "dataset": "prod"}`. The
//! language defaults to SQL and the dataset to `default`. The same
//! route with a form body is Prometheus' instant query, which the
//! `prometheus` module answers.
//!
//! A query answers `{"columns": [{"name", "type"}], "rows": [[...]]}`;
//! an `EXPLAIN` answers `{"explain": "..."}`. Timestamps are RFC 3339
//! strings, binary columns hexadecimal, and the `_overflow` column is
//! returned as the JSON object it holds.

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response as HttpResponse};
use axum::routing::post;
use serde::{Deserialize, Serialize};
use tessera_exec::{Response, explain_text, query};
use tessera_ir::BindContext;
use tessera_sql::SqlFrontend;
use tessera_types::{DatasetName, TenantId};

use crate::encode::{ColumnJson, rows_json};
use crate::{ApiError, AppState};

/// The query router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/query", post(run_query))
        .with_state(state)
}

/// The request body.
#[derive(Deserialize)]
struct QueryRequest {
    query: String,
    #[serde(default = "default_language")]
    language: String,
    #[serde(default)]
    dataset: Option<String>,
}

fn default_language() -> String {
    "sql".into()
}

/// The response body.
#[derive(Serialize)]
#[serde(untagged)]
enum QueryResponse {
    Rows {
        columns: Vec<ColumnJson>,
        rows: Vec<Vec<serde_json::Value>>,
    },
    Explain {
        explain: String,
    },
}

/// Dispatches on the content type: JSON is the native query, anything
/// else Prometheus' form-encoded instant query.
async fn run_query(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> HttpResponse {
    let json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().starts_with("application/json"));
    if !json {
        return crate::prometheus::instant_query_raw(&state, &headers, &body).await;
    }
    let request: QueryRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(error) => {
            return ApiError::bad_request(format!("invalid request: {error}")).into_response();
        }
    };
    match run_native(&state, request).await {
        Ok(response) => response.into_response(),
        Err(error) => error.into_response(),
    }
}

async fn run_native(
    state: &AppState,
    request: QueryRequest,
) -> Result<Json<QueryResponse>, ApiError> {
    if request.language != "sql" {
        return Err(ApiError::bad_request(format!(
            "unsupported language {}: only sql is available",
            request.language
        )));
    }
    let dataset = match request.dataset {
        Some(name) => DatasetName::new(name).map_err(|e| ApiError::bad_request(e.to_string()))?,
        None => DatasetName::new("default").unwrap_or_else(|_| unreachable!("a valid name")),
    };
    let ctx = BindContext {
        tenant: TenantId::DEFAULT,
        dataset,
        now: state.env.clock.now(),
    };
    let _turn = state.admit_query(&ctx.tenant).await?;
    let started = std::time::Instant::now();
    let response = query::run(&state.engine, &SqlFrontend, &request.query, &ctx)
        .await
        .map_err(|e| ApiError::query(e, &request.query))?;
    Ok(Json(match response {
        Response::Rows(output) => {
            crate::workload::record_uses(
                state,
                output.uses.clone(),
                "sql",
                "ad hoc",
                started.elapsed(),
            );
            let (columns, rows) = rows_json(&output.schema, &output.batches)?;
            QueryResponse::Rows { columns, rows }
        }
        Response::Explain { plan, analysis } => QueryResponse::Explain {
            explain: explain_text(&plan, analysis.as_ref()),
        },
        Response::Control(statement) => {
            let message = crate::control::execute(state, &ctx.tenant, statement).await?;
            QueryResponse::Rows {
                columns: vec![ColumnJson {
                    name: "result".into(),
                    r#type: "text".into(),
                }],
                rows: vec![vec![serde_json::Value::String(message)]],
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use crate::otlp::testing::export;

    async fn post(app: &axum::Router, body: serde_json::Value) -> (StatusCode, serde_json::Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn queries_ingested_logs_with_sql() {
        let state = crate::testing::state().await;
        let app = crate::router(state);
        let body = serde_json::to_vec(&export(&["first", "second", "third"])).unwrap();
        let write = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(header::CONTENT_TYPE, "application/json")
            .header(crate::otlp::DATASET_HEADER, "prod")
            .body(Body::from(body))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(write).await.unwrap().status(),
            StatusCode::OK
        );

        let (status, value) = post(
            &app,
            serde_json::json!({
                "query": "SELECT body, attr.n AS n, timestamp FROM logs WHERE attr.n >= 1 ORDER BY timestamp DESC",
                "dataset": "prod"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["columns"][0]["name"], "body");
        assert_eq!(value["columns"][1]["type"], "text");
        assert_eq!(value["columns"][2]["type"], "timestamp");
        let rows = value["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], "third");
        assert_eq!(rows[0][1], "2");
        assert_eq!(rows[0][2], "2023-11-14T22:13:20.000000002Z");

        let (status, value) = post(
            &app,
            serde_json::json!({
                "query": "SELECT count(*) AS n, max(severity_text) FROM prod.logs"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["rows"][0][0], 3.0);
        assert_eq!(value["rows"][0][1], "INFO");

        let (status, value) = post(
            &app,
            serde_json::json!({ "query": "EXPLAIN ANALYZE SELECT * FROM prod.logs LIMIT 1" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let explain = value["explain"].as_str().unwrap();
        assert!(
            explain.starts_with("Limit: offset=0 count=1\n"),
            "{explain}"
        );
        assert!(
            explain.contains("rows: 0 from files, 3 from the tail"),
            "{explain}"
        );
    }

    #[tokio::test]
    async fn errors_are_bad_requests_with_positions() {
        let app = crate::router(crate::testing::state().await);
        let (status, value) = post(
            &app,
            serde_json::json!({ "query": "SELECT nope FROM logs" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(value["error"], "unknown column nope at line 1, column 8");
        let (status, _) = post(
            &app,
            serde_json::json!({ "query": "SELECT 1", "language": "promql" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, value) = post(&app, serde_json::json!({ "query": "SELECT 1 AS one" })).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["rows"][0][0], 1);
    }
}
