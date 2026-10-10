// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! OTLP over HTTP (IN-1): `POST /v1/logs` with a protobuf or JSON body.
//!
//! The dataset comes from the `X-Tessera-Dataset` header, `default` when
//! absent; the tenant is `default` until authentication exists (CT-5
//! allows unauthenticated access to `default` in single-node mode). A
//! request is answered only once its rows are durable (IN-10); a request
//! the node cannot store now gets 503 with `Retry-After` (IN-11).

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse;
use prost::Message;
use tessera_ingest::otlp;
use tessera_types::{DatasetName, TenantId};

use crate::{ApiError, AppState};

/// The header naming the dataset a request writes to.
pub const DATASET_HEADER: &str = "x-tessera-dataset";
const PROTOBUF: &str = "application/x-protobuf";
const JSON: &str = "application/json";

/// The OTLP router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/logs", post(export_logs))
        .with_state(state)
}

async fn export_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or(v).trim().to_ascii_lowercase())
        .unwrap_or_default();
    let request = match content_type.as_str() {
        PROTOBUF => otlp::parse_protobuf(&body),
        JSON => otlp::parse_json(&body),
        other => {
            return Err(ApiError::unsupported_media_type(format!(
                "unsupported content type {other:?}"
            )));
        }
    }
    .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let dataset = dataset(&headers)?;
    let now = state.env.clock.now();
    let batch = otlp::to_batch(
        &request,
        TenantId::DEFAULT,
        dataset,
        now,
        &mut state.env.random.clone(),
    )
    .map_err(|e| ApiError::bad_request(e.to_string()))?;
    if let Some(batch) = batch {
        state.ingest.write(batch).await?;
    }
    Ok(if content_type == PROTOBUF {
        let body = ExportLogsServiceResponse::default().encode_to_vec();
        (
            [(header::CONTENT_TYPE, HeaderValue::from_static(PROTOBUF))],
            body,
        )
            .into_response()
    } else {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, HeaderValue::from_static(JSON))],
            "{}",
        )
            .into_response()
    })
}

fn dataset(headers: &HeaderMap) -> Result<DatasetName, ApiError> {
    match headers.get(DATASET_HEADER) {
        None => Ok(
            DatasetName::new("default").unwrap_or_else(|_| unreachable!("a literal valid name"))
        ),
        Some(value) => {
            let text = value
                .to_str()
                .map_err(|_| ApiError::bad_request("dataset header is not text"))?;
            DatasetName::new(text).map_err(|e| ApiError::bad_request(e.to_string()))
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

    /// An export of `bodies` as INFO logs one nanosecond apart from
    /// 2023-11-14T22:13:20Z, each with `attr.n` set to its index and a
    /// trace id of ones.
    pub fn export(bodies: &[&str]) -> ExportLogsServiceRequest {
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
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use tower::ServiceExt;

    use super::*;

    fn request(body: &str) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("checkout".into())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        severity_number: 9,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(body.into())),
                        }),
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    async fn post(
        app: &Router,
        content_type: &str,
        dataset: Option<&str>,
        body: Vec<u8>,
    ) -> (StatusCode, Vec<u8>) {
        let mut builder = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(header::CONTENT_TYPE, content_type);
        if let Some(dataset) = dataset {
            builder = builder.header(DATASET_HEADER, dataset);
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        (status, bytes)
    }

    #[tokio::test]
    async fn accepts_json_and_protobuf_requests() {
        let app = router(crate::testing::state().await);
        let json = serde_json::to_vec(&request("hello")).unwrap();
        let (status, body) =
            post(&app, "application/json; charset=utf-8", Some("prod"), json).await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        assert_eq!(body, b"{}");
        let (status, body) = post(&app, PROTOBUF, None, request("hello").encode_to_vec()).await;
        assert_eq!(status, StatusCode::OK);
        assert!(ExportLogsServiceResponse::decode(body.as_slice()).is_ok());
    }

    #[tokio::test]
    async fn an_empty_request_is_fine() {
        let app = router(crate::testing::state().await);
        let (status, _) = post(
            &app,
            PROTOBUF,
            None,
            ExportLogsServiceRequest::default().encode_to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn malformed_requests_are_rejected() {
        let app = router(crate::testing::state().await);
        let (status, _) = post(&app, "text/plain", None, b"x".to_vec()).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let (status, _) = post(&app, "application/json", None, b"{".to_vec()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = post(&app, PROTOBUF, None, vec![0xff, 0xff]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = post(&app, "application/json", Some("Not Valid"), b"{}".to_vec()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
