// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! OTLP over HTTP (IN-1): `POST /v1/logs`, `POST /v1/traces` and
//! `POST /v1/metrics` with a protobuf or JSON body.
//!
//! The dataset comes from the `X-Tessera-Dataset` header, `default` when
//! absent; the tenant is `default` until authentication exists (CT-5
//! allows unauthenticated access to `default` in single-node mode). A
//! request is answered only once its rows are durable (IN-10); a request
//! the node cannot store now gets 503 with `Retry-After` (IN-11).

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse;
use prost::Message;
use tessera_ingest::otlp;

use crate::params::{Need, WriteParams, scope};
use crate::{ApiError, AppState};

/// The header naming the dataset a request writes to.
pub const DATASET_HEADER: &str = "x-tessera-dataset";
const PROTOBUF: &str = "application/x-protobuf";
const JSON: &str = "application/json";

/// The OTLP router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/logs", post(export_logs))
        .route("/v1/traces", post(export_traces))
        .route("/v1/metrics", post(export_metrics))
        .with_state(state)
}

/// The encoding of a request body, from its content type.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Encoding {
    Protobuf,
    Json,
}

fn encoding(headers: &HeaderMap) -> Result<Encoding, ApiError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or(v).trim().to_ascii_lowercase())
        .unwrap_or_default();
    match content_type.as_str() {
        PROTOBUF => Ok(Encoding::Protobuf),
        JSON => Ok(Encoding::Json),
        other => Err(ApiError::unsupported_media_type(format!(
            "unsupported content type {other:?}"
        ))),
    }
}

/// The export response in the request's encoding: an empty message.
fn export_response(encoding: Encoding, protobuf: Vec<u8>) -> Response {
    match encoding {
        Encoding::Protobuf => (
            [(header::CONTENT_TYPE, HeaderValue::from_static(PROTOBUF))],
            protobuf,
        )
            .into_response(),
        Encoding::Json => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, HeaderValue::from_static(JSON))],
            "{}",
        )
            .into_response(),
    }
}

async fn export_logs(
    State(state): State<AppState>,
    Query(params): Query<WriteParams>,
    parts: Parts,
    body: Bytes,
) -> Result<Response, ApiError> {
    let encoding = encoding(&parts.headers)?;
    let request = match encoding {
        Encoding::Protobuf => otlp::parse_protobuf(&body),
        Encoding::Json => otlp::parse_json(&body),
    }
    .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let scope = scope(&state, &parts, Need::Write).await?;
    let now = state.env.clock.now();
    let batch = otlp::to_batch(
        &request,
        scope.tenant,
        scope.dataset,
        now,
        &mut state.env.random.clone(),
    )
    .map_err(|e| ApiError::bad_request(e.to_string()))?;
    if let Some(batch) = batch {
        state.write_with(batch, params.ack()?).await?;
    }
    Ok(export_response(
        encoding,
        ExportLogsServiceResponse::default().encode_to_vec(),
    ))
}

/// Spans and their events are written as two batches, both durable
/// before the request is answered (IN-10).
async fn export_traces(
    State(state): State<AppState>,
    Query(params): Query<WriteParams>,
    parts: Parts,
    body: Bytes,
) -> Result<Response, ApiError> {
    let encoding = encoding(&parts.headers)?;
    let request = match encoding {
        Encoding::Protobuf => otlp::traces::parse_protobuf(&body),
        Encoding::Json => otlp::traces::parse_json(&body),
    }
    .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let scope = scope(&state, &parts, Need::Write).await?;
    let now = state.env.clock.now();
    let batches = otlp::traces::to_batches(&request, scope.tenant, scope.dataset, now)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    for batch in batches {
        state.write_with(batch, params.ack()?).await?;
    }
    Ok(export_response(
        encoding,
        ExportTraceServiceResponse::default().encode_to_vec(),
    ))
}

async fn export_metrics(
    State(state): State<AppState>,
    Query(params): Query<WriteParams>,
    parts: Parts,
    body: Bytes,
) -> Result<Response, ApiError> {
    let encoding = encoding(&parts.headers)?;
    let request = match encoding {
        Encoding::Protobuf => otlp::metrics::parse_protobuf(&body),
        Encoding::Json => otlp::metrics::parse_json(&body),
    }
    .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let scope = scope(&state, &parts, Need::Write).await?;
    let now = state.env.clock.now();
    let batch = otlp::metrics::to_batch(&request, scope.tenant, scope.dataset, now)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    if let Some(batch) = batch {
        state.write_with(batch, params.ack()?).await?;
    }
    Ok(export_response(
        encoding,
        ExportMetricsServiceResponse::default().encode_to_vec(),
    ))
}

#[cfg(test)]
pub(crate) mod testing {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::span::Event;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

    /// A span of `service`, in trace `trace` with id `id` and parent
    /// `parent`, starting `start` nanoseconds after
    /// 2023-11-14T22:13:20Z and lasting `duration` nanoseconds; status
    /// error when `error`.
    pub struct SpanSpec {
        /// The resource's service name.
        pub service: &'static str,
        /// The trace id, repeated over 16 bytes.
        pub trace: u8,
        /// The span id, repeated over 8 bytes.
        pub id: u8,
        /// The parent span id, repeated over 8 bytes.
        pub parent: Option<u8>,
        /// The span name.
        pub name: &'static str,
        /// Start offset in nanoseconds.
        pub start: u64,
        /// Duration in nanoseconds.
        pub duration: u64,
        /// Whether the status is error.
        pub error: bool,
    }

    /// An export of the gauge `name` of service `service`, one point per
    /// `(label, time offset in seconds, value)` under `attr.job`, from
    /// 2023-11-14T22:13:20Z.
    pub fn export_gauges(
        service: &str,
        name: &str,
        points: &[(&str, u64, f64)],
    ) -> opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
        use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
            number_data_point,
        };

        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(service.into())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![Metric {
                        name: name.into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: points
                                .iter()
                                .map(|(job, offset, value)| NumberDataPoint {
                                    attributes: vec![KeyValue {
                                        key: "job".into(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue(
                                                (*job).into(),
                                            )),
                                        }),
                                        ..Default::default()
                                    }],
                                    time_unix_nano: 1_700_000_000_000_000_000
                                        + offset * 1_000_000_000,
                                    value: Some(number_data_point::Value::AsDouble(*value)),
                                    ..Default::default()
                                })
                                .collect(),
                        })),
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    /// The export of `spans`, one resource per span, each span carrying
    /// `attr.n` set to its index and one event named `log`.
    pub fn export_spans(spans: &[SpanSpec]) -> ExportTraceServiceRequest {
        let text = |value: &str| AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        };
        ExportTraceServiceRequest {
            resource_spans: spans
                .iter()
                .enumerate()
                .map(|(i, spec)| ResourceSpans {
                    resource: Some(Resource {
                        attributes: vec![KeyValue {
                            key: "service.name".into(),
                            value: Some(text(spec.service)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    scope_spans: vec![ScopeSpans {
                        scope: None,
                        spans: vec![Span {
                            trace_id: vec![spec.trace; 16],
                            span_id: vec![spec.id; 8],
                            parent_span_id: spec.parent.map(|p| vec![p; 8]).unwrap_or_default(),
                            name: spec.name.into(),
                            kind: 2,
                            start_time_unix_nano: 1_700_000_000_000_000_000 + spec.start,
                            end_time_unix_nano: 1_700_000_000_000_000_000
                                + spec.start
                                + spec.duration,
                            attributes: vec![KeyValue {
                                key: "n".into(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::IntValue(
                                        i64::try_from(i).unwrap(),
                                    )),
                                }),
                                ..Default::default()
                            }],
                            events: vec![Event {
                                time_unix_nano: 1_700_000_000_000_000_000 + spec.start + 1,
                                name: "log".into(),
                                attributes: Vec::new(),
                                dropped_attributes_count: 0,
                            }],
                            status: spec.error.then(|| Status {
                                code: 2,
                                message: "failed".into(),
                            }),
                            ..Default::default()
                        }],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                })
                .collect(),
        }
    }

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
        post_to(app, "/v1/logs", content_type, dataset, body).await
    }

    async fn post_to(
        app: &Router,
        path: &str,
        content_type: &str,
        dataset: Option<&str>,
        body: Vec<u8>,
    ) -> (StatusCode, Vec<u8>) {
        let mut builder = Request::builder()
            .method("POST")
            .uri(path)
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
    async fn accepts_traces_in_both_encodings() {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse;

        let app = router(crate::testing::state().await);
        let spans = crate::otlp::testing::export_spans(&[crate::otlp::testing::SpanSpec {
            service: "api",
            trace: 1,
            id: 1,
            parent: None,
            name: "GET /",
            start: 0,
            duration: 1_000,
            error: false,
        }]);
        let json = serde_json::to_vec(&spans).unwrap();
        let (status, body) = post_to(&app, "/v1/traces", JSON, Some("prod"), json).await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        assert_eq!(body, b"{}");
        let (status, body) =
            post_to(&app, "/v1/traces", PROTOBUF, None, spans.encode_to_vec()).await;
        assert_eq!(status, StatusCode::OK);
        assert!(ExportTraceServiceResponse::decode(body.as_slice()).is_ok());
        let (status, _) = post_to(&app, "/v1/traces", JSON, None, b"{".to_vec()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (status, _) = post_to(&app, "/v1/traces", "text/plain", None, b"x".to_vec()).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn accepts_metrics_in_both_encodings() {
        let app = router(crate::testing::state().await);
        let metrics = crate::otlp::testing::export_gauges("api", "up", &[("a", 0, 1.0)]);
        let json = serde_json::to_vec(&metrics).unwrap();
        let (status, body) = post_to(&app, "/v1/metrics", JSON, Some("prod"), json).await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        assert_eq!(body, b"{}");
        let (status, body) =
            post_to(&app, "/v1/metrics", PROTOBUF, None, metrics.encode_to_vec()).await;
        assert_eq!(status, StatusCode::OK);
        assert!(ExportMetricsServiceResponse::decode(body.as_slice()).is_ok());
        let (status, _) = post_to(&app, "/v1/metrics", JSON, None, b"{".to_vec()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
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
