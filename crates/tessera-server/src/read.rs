// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! A plain read of a dataset's logs through the consistent view, so that
//! ingested data can be seen before the query engine exists (M2):
//! `GET /api/v1/datasets/{dataset}/logs?limit=N`, newest first.
//!
//! The tenant is `default` until authentication exists.

use arrow::array::{
    Array, FixedSizeBinaryArray, Int32Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::routing::get;
use serde::{Deserialize, Serialize};
use tessera_manifest::view;
use tessera_types::event::{
    BODY, EVENT_ID, OBSERVED_TIMESTAMP, OVERFLOW, SEVERITY_NUMBER, SEVERITY_TEXT, SPAN_ID,
    TIMESTAMP, TRACE_ID,
};
use tessera_types::{DatasetName, Kind, TenantId, Timestamp, Window};

use crate::{ApiError, AppState};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 10_000;

/// The read router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/datasets/{dataset}/logs", get(read_logs))
        .with_state(state)
}

/// Query parameters of the read.
#[derive(Deserialize)]
struct ReadQuery {
    limit: Option<usize>,
}

/// One log row as JSON.
#[derive(Serialize, Debug, PartialEq)]
struct LogJson {
    timestamp: String,
    observed_timestamp: String,
    event_id: String,
    trace_id: Option<String>,
    span_id: Option<String>,
    severity_number: Option<i32>,
    severity_text: Option<String>,
    body: Option<String>,
    attributes: serde_json::Value,
}

/// The response of the read.
#[derive(Serialize)]
struct ReadResponse {
    rows: Vec<LogJson>,
    files: usize,
    segments: usize,
}

async fn read_logs(
    State(state): State<AppState>,
    Path(dataset): Path<String>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<ReadResponse>, ApiError> {
    let dataset = DatasetName::new(dataset).map_err(|e| ApiError::bad_request(e.to_string()))?;
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let view = view::read(
        &state.pools,
        &state.format,
        &state.env,
        &TenantId::DEFAULT,
        &dataset,
        Kind::Log,
        Window::DEFAULT_LENGTH,
    )
    .await?;
    let mut rows: Vec<(i64, LogJson)> = view.rows.iter().flat_map(rows_of).collect();
    rows.sort_by_key(|row| std::cmp::Reverse(row.0));
    rows.truncate(limit);
    Ok(Json(ReadResponse {
        rows: rows.into_iter().map(|(_, row)| row).collect(),
        files: view.sources.files,
        segments: view.sources.segments,
    }))
}

fn rows_of(batch: &RecordBatch) -> Vec<(i64, LogJson)> {
    let timestamps = column::<TimestampNanosecondArray>(batch, TIMESTAMP);
    let observed = column::<TimestampNanosecondArray>(batch, OBSERVED_TIMESTAMP);
    let event_ids = column::<FixedSizeBinaryArray>(batch, EVENT_ID);
    let trace_ids = column::<FixedSizeBinaryArray>(batch, TRACE_ID);
    let span_ids = column::<FixedSizeBinaryArray>(batch, SPAN_ID);
    let severities = column::<Int32Array>(batch, SEVERITY_NUMBER);
    let severity_texts = column::<StringArray>(batch, SEVERITY_TEXT);
    let bodies = column::<StringArray>(batch, BODY);
    let overflow = column::<StringArray>(batch, OVERFLOW);
    (0..batch.num_rows())
        .map(|i| {
            let nanos = timestamps.map_or(0, |c| c.value(i));
            let row = LogJson {
                timestamp: Timestamp::from_nanos(nanos).to_string(),
                observed_timestamp: Timestamp::from_nanos(observed.map_or(0, |c| c.value(i)))
                    .to_string(),
                event_id: event_ids.map_or_else(String::new, |c| hex(c.value(i))),
                trace_id: trace_ids.filter(|c| !c.is_null(i)).map(|c| hex(c.value(i))),
                span_id: span_ids.filter(|c| !c.is_null(i)).map(|c| hex(c.value(i))),
                severity_number: severities.filter(|c| !c.is_null(i)).map(|c| c.value(i)),
                severity_text: severity_texts
                    .filter(|c| !c.is_null(i))
                    .map(|c| c.value(i).to_owned()),
                body: bodies
                    .filter(|c| !c.is_null(i))
                    .map(|c| c.value(i).to_owned()),
                attributes: overflow
                    .filter(|c| !c.is_null(i))
                    .and_then(|c| serde_json::from_str(c.value(i)).ok())
                    .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new())),
            };
            (nanos, row)
        })
        .collect()
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Option<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use tower::ServiceExt;

    fn export(bodies: &[&str]) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: bodies
                        .iter()
                        .enumerate()
                        .map(|(i, body)| LogRecord {
                            time_unix_nano: 1_700_000_000_000_000_000 + i as u64,
                            severity_text: "INFO".into(),
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue((*body).into())),
                            }),
                            attributes: vec![KeyValue {
                                key: "n".into(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::IntValue(
                                        i64::try_from(i).unwrap(),
                                    )),
                                }),
                                ..Default::default()
                            }],
                            trace_id: vec![1; 16],
                            ..Default::default()
                        })
                        .collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[tokio::test]
    async fn reads_back_what_was_ingested_newest_first() {
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

        let read = Request::builder()
            .uri("/api/v1/datasets/prod/logs?limit=2")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(read).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let rows = value["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["body"], "third");
        assert_eq!(rows[1]["body"], "second");
        assert_eq!(rows[0]["attributes"]["attr.n"], 2);
        assert_eq!(rows[0]["trace_id"], "01010101010101010101010101010101");
        assert_eq!(rows[0]["severity_text"], "INFO");
        assert_eq!(rows[0]["timestamp"], "2023-11-14T22:13:20.000000002Z");
        assert_eq!(value["segments"], 1);
        assert_eq!(value["files"], 0);

        let other = Request::builder()
            .uri("/api/v1/datasets/staging/logs")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(other).await.unwrap();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value["rows"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_invalid_dataset_name_is_a_bad_request() {
        let app = crate::router(crate::testing::state().await);
        let read = Request::builder()
            .uri("/api/v1/datasets/Not-Valid/logs")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(read).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
    }
}
