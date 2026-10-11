// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The Prometheus HTTP API (section 16): remote write, readiness and
//! build information. Queries, series and labels arrive with the
//! `PromQL` front end. Grafana's Prometheus data source works against
//! it unmodified (AP-1).
//!
//! The tenant and dataset come from the headers the `params` module
//! reads (CT-5).

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::json;
use tessera_ingest::remote_write;

use crate::params::scope;
use crate::{ApiError, AppState};

/// The Prometheus router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route(
            "/-/ready",
            get(|| async { "Prometheus Server is Ready.\n" }),
        )
        .route(
            "/-/healthy",
            get(|| async { "Prometheus Server is Healthy.\n" }),
        )
        .route(
            "/api/v1/status/buildinfo",
            get(|| async {
                Json(json!({
                    "status": "success",
                    "data": {
                        "version": env!("CARGO_PKG_VERSION"),
                        "revision": "",
                        "branch": "",
                        "buildUser": "",
                        "buildDate": "",
                        "goVersion": "none",
                    }
                }))
            }),
        )
        .route("/api/v1/write", post(write))
        .with_state(state)
}

/// Remote write: a snappy-compressed protobuf body, answered with 204
/// once its samples are durable (IN-10).
async fn write(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let scope = scope(&state, &headers)?;
    let request = remote_write::decode(&body).map_err(|e| ApiError::bad_request(e.to_string()))?;
    let now = state.env.clock.now();
    let batch = remote_write::to_batch(&request, scope.tenant, scope.dataset, now)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    if let Some(batch) = batch {
        state.ingest.write(batch).await?;
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[cfg(test)]
pub(crate) mod testing {
    use prost::Message;
    use tessera_ingest::remote_write::{Label, MetricMetadata, Sample, TimeSeries, WriteRequest};

    /// A series to write: name, labels, samples of (milliseconds, value).
    pub type Series<'a> = (&'a str, &'a [(&'a str, &'a str)], &'a [(i64, f64)]);

    /// A compressed remote write request of `series`; the families in
    /// `counters` are typed as counters.
    pub fn write_request(series: &[Series<'_>], counters: &[&str]) -> Vec<u8> {
        let request = WriteRequest {
            timeseries: series
                .iter()
                .map(|(name, labels, samples)| {
                    let mut all = vec![Label {
                        name: "__name__".into(),
                        value: (*name).into(),
                    }];
                    all.extend(labels.iter().map(|(n, v)| Label {
                        name: (*n).into(),
                        value: (*v).into(),
                    }));
                    TimeSeries {
                        labels: all,
                        samples: samples
                            .iter()
                            .map(|(t, v)| Sample {
                                value: *v,
                                timestamp: *t,
                            })
                            .collect(),
                    }
                })
                .collect(),
            metadata: counters
                .iter()
                .map(|family| MetricMetadata {
                    r#type: 1,
                    metric_family_name: (*family).into(),
                    help: String::new(),
                    unit: String::new(),
                })
                .collect(),
        };
        snap::raw::Encoder::new()
            .compress_vec(&request.encode_to_vec())
            .expect("a request compresses")
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, header};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn remote_write_stores_samples_and_rejects_garbage() {
        let app = crate::router(crate::testing::state().await);
        let body = testing::write_request(
            &[
                ("up", &[("job", "api")], &[(1_700_000_000_000, 1.0)]),
                (
                    "http_requests_total",
                    &[("job", "api")],
                    &[(1_700_000_000_000, 5.0)],
                ),
            ],
            &["http_requests"],
        );
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/write")
            .header(header::CONTENT_TYPE, "application/x-protobuf")
            .header(header::CONTENT_ENCODING, "snappy")
            .header("x-tessera-dataset", "prod")
            .body(Body::from(body))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/write")
            .body(Body::from("garbage"))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&bytes).contains("snappy"));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/-/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/status/buildinfo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["status"], "success");
    }
}
