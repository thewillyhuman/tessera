// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Differential testing of `PromQL` and the Prometheus API against
//! Prometheus (QL-4, section 18.5): the same samples are pushed to a
//! Prometheus instance and to an in-process Tessera node over remote
//! write, the same queries are sent to both over the Prometheus API,
//! and the answers must agree once reduced to what the API promises.
//!
//! Prometheus is reached through `TESSERA_PROMETHEUS_URL` (for example
//! `http://127.0.0.1:9090`, started with the remote write receiver
//! enabled); without it the suite reports that it was skipped. Every
//! deviation found this way is documented in the `PromQL` crate.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use prost::Message;
use serde_json::Value;
use tessera_ingest::remote_write::{Label, MetricMetadata, Sample, TimeSeries, WriteRequest};
use tessera_server::AppState;

/// One Prometheus-compatible endpoint.
#[derive(Clone, Debug)]
pub struct Endpoint {
    /// The base URL, without a trailing slash.
    pub base: String,
    /// What to call it in reports.
    pub name: &'static str,
}

/// A client that returns error statuses as responses.
fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .into()
}

impl Endpoint {
    /// Pushes `request` over remote write.
    ///
    /// # Panics
    ///
    /// If the endpoint rejects the push.
    pub fn push(&self, request: &WriteRequest) {
        let body = snap::raw::Encoder::new()
            .compress_vec(&request.encode_to_vec())
            .expect("a request compresses");
        let response = agent()
            .post(format!("{}/api/v1/write", self.base))
            .header("content-type", "application/x-protobuf")
            .header("content-encoding", "snappy")
            .header("x-prometheus-remote-write-version", "0.1.0")
            .send(body.as_slice())
            .unwrap_or_else(|e| panic!("{}: push failed: {e}", self.name));
        assert!(
            response.status().is_success(),
            "{}: push returned {}",
            self.name,
            response.status()
        );
    }

    /// `GET path?query`, returning the `data` of a successful answer or
    /// the endpoint's complaint.
    ///
    /// # Errors
    ///
    /// If the request fails or the endpoint answers an error.
    pub fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value, String> {
        let mut request = agent().get(format!("{}{path}", self.base));
        for (key, value) in query {
            request = request.query(*key, value);
        }
        let mut response = request
            .call()
            .map_err(|e| format!("{}: {path} failed: {e}", self.name))?;
        let text = response
            .body_mut()
            .read_to_string()
            .map_err(|e| format!("{}: unreadable response: {e}", self.name))?;
        let body: Value = serde_json::from_str(&text)
            .map_err(|e| format!("{}: {path} answered non-JSON ({e}): {text}", self.name))?;
        if body["status"] != "success" {
            return Err(format!(
                "{}: {} ({})",
                self.name,
                body["error"].as_str().unwrap_or("error"),
                body["errorType"].as_str().unwrap_or("?")
            ));
        }
        Ok(body["data"].clone())
    }

    /// A range query.
    ///
    /// # Errors
    ///
    /// If the request fails or the endpoint answers an error.
    pub fn query_range(
        &self,
        query: &str,
        start: i64,
        end: i64,
        step: &str,
    ) -> Result<Value, String> {
        self.get(
            "/api/v1/query_range",
            &[
                ("query", query.to_owned()),
                ("start", start.to_string()),
                ("end", end.to_string()),
                ("step", step.to_owned()),
            ],
        )
    }

    /// An instant query.
    ///
    /// # Errors
    ///
    /// If the request fails or the endpoint answers an error.
    pub fn query(&self, query: &str, time: i64) -> Result<Value, String> {
        self.get(
            "/api/v1/query",
            &[("query", query.to_owned()), ("time", time.to_string())],
        )
    }
}

/// A label set.
pub type Labels = BTreeMap<String, String>;
/// Samples as (seconds in thousandths, value in millionths, or the text
/// of a value that is not a finite number).
pub type Samples = Vec<(i64, String)>;

/// A result reduced to what both engines must agree on.
#[derive(Clone, Debug, PartialEq)]
pub enum Normalized {
    /// Series with their samples.
    Matrix(Vec<(Labels, Samples)>),
    /// Series with one value each.
    Vector(Vec<(Labels, String)>),
    /// A number.
    Scalar(String),
    /// A string.
    Text(String),
}

/// A value as text that compares exactly for finite numbers within a
/// relative tolerance of a millionth, and by name otherwise.
#[allow(clippy::cast_possible_truncation)]
fn value_text(value: &Value) -> String {
    let text = value.as_str().unwrap_or_default();
    match text.parse::<f64>() {
        Ok(v) if v.is_finite() => {
            // Round to seven significant digits, so that both engines'
            // floating-point paths agree.
            if v == 0.0 {
                "0".to_owned()
            } else {
                let magnitude = v.abs().log10().floor() as i32;
                let scale = 10f64.powi(6 - magnitude);
                format!("{}", (v * scale).round() / scale)
            }
        }
        Ok(v) if v.is_nan() => "NaN".to_owned(),
        Ok(v) if v > 0.0 => "+Inf".to_owned(),
        Ok(_) => "-Inf".to_owned(),
        Err(_) => text.to_owned(),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn millis(value: &Value) -> i64 {
    (value.as_f64().unwrap_or_default() * 1000.0).round() as i64
}

fn labels(value: &Value) -> Labels {
    value
        .as_object()
        .map(|o| {
            o.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_owned()))
                .collect()
        })
        .unwrap_or_default()
}

/// Reduces a `data` object to what both engines must agree on.
///
/// # Panics
///
/// If the data has an unknown result type.
#[must_use]
pub fn normalize(data: &Value) -> Normalized {
    match data["resultType"].as_str() {
        Some("matrix") => {
            let mut series: Vec<_> = data["result"]
                .as_array()
                .unwrap_or(&Vec::new())
                .iter()
                .map(|s| {
                    let samples: Samples = s["values"]
                        .as_array()
                        .unwrap_or(&Vec::new())
                        .iter()
                        .map(|p| (millis(&p[0]), value_text(&p[1])))
                        .collect();
                    (labels(&s["metric"]), samples)
                })
                .filter(|(_, samples)| !samples.is_empty())
                .collect();
            series.sort();
            Normalized::Matrix(series)
        }
        Some("vector") => {
            let mut series: Vec<_> = data["result"]
                .as_array()
                .unwrap_or(&Vec::new())
                .iter()
                .map(|s| (labels(&s["metric"]), value_text(&s["value"][1])))
                .collect();
            series.sort();
            Normalized::Vector(series)
        }
        Some("scalar") => Normalized::Scalar(value_text(&data["result"][1])),
        Some("string") => {
            Normalized::Text(data["result"][1].as_str().unwrap_or_default().to_owned())
        }
        other => panic!("unexpected result type {other:?}: {data}"),
    }
}

/// Serves an in-memory Tessera node on a loopback port.
///
/// # Panics
///
/// If no port can be bound.
pub async fn serve_tessera() -> Endpoint {
    let state = AppState::in_memory().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port");
    let address: SocketAddr = listener.local_addr().expect("a bound address");
    let app = tessera_server::router(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Endpoint {
        base: format!("http://{address}"),
        name: "tessera",
    }
}

/// The Prometheus endpoint named by `TESSERA_PROMETHEUS_URL`, if any.
#[must_use]
pub fn prometheus_from_env() -> Option<Endpoint> {
    std::env::var("TESSERA_PROMETHEUS_URL")
        .ok()
        .map(|base| Endpoint {
            base: base.trim_end_matches('/').to_owned(),
            name: "prometheus",
        })
}

/// The samples both engines receive, from `start` seconds, 15 seconds
/// apart over `count` points: a counter with resets, a gauge, a
/// classic histogram, and a series that goes stale; every series
/// carries `run`, which scopes the queries to this run.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn fixture(start: i64, count: i64, run: &str) -> WriteRequest {
    let label = |name: &str, value: &str| Label {
        name: name.into(),
        value: value.into(),
    };
    let series = |name: &str, extra: &[(&str, &str)], values: Vec<(i64, f64)>| TimeSeries {
        labels: std::iter::once(label("__name__", name))
            .chain(std::iter::once(label("run", run)))
            .chain(extra.iter().map(|(k, v)| label(k, v)))
            .collect(),
        samples: values
            .into_iter()
            .map(|(i, v)| Sample {
                value: v,
                timestamp: (start + i * 15) * 1000,
            })
            .collect(),
        histograms: Vec::new(),
    };
    let points = |f: &dyn Fn(i64) -> f64| (0..count).map(|i| (i, f(i))).collect::<Vec<_>>();
    let stale = f64::from_bits(tessera_ingest::remote_write::STALE_NAN_BITS);
    WriteRequest {
        timeseries: vec![
            series(
                "http_requests_total",
                &[("job", "api"), ("instance", "a")],
                points(&|i| (i * 10) as f64),
            ),
            series(
                "http_requests_total",
                &[("job", "api"), ("instance", "b")],
                points(&|i| {
                    if i < count / 2 {
                        (i * 7) as f64
                    } else {
                        ((i - count / 2) * 7) as f64
                    }
                }),
            ),
            series(
                "http_requests_total",
                &[("job", "web"), ("instance", "c")],
                points(&|i| (i * 3) as f64),
            ),
            series(
                "temperature",
                &[("job", "api"), ("instance", "a")],
                points(&|i| 20.0 + (i % 4) as f64 * 1.5),
            ),
            series(
                "temperature",
                &[("job", "web"), ("instance", "c")],
                points(&|i| 18.0 - (i % 3) as f64),
            ),
            series(
                "latency_bucket",
                &[("job", "api"), ("le", "0.1")],
                points(&|i| (i * 2) as f64),
            ),
            series(
                "latency_bucket",
                &[("job", "api"), ("le", "0.5")],
                points(&|i| (i * 5) as f64),
            ),
            series(
                "latency_bucket",
                &[("job", "api"), ("le", "+Inf")],
                points(&|i| (i * 6) as f64),
            ),
            series(
                "up",
                &[("job", "api"), ("instance", "a")],
                points(&|i| if i == count - 2 { stale } else { 1.0 }),
            ),
        ],
        metadata: vec![MetricMetadata {
            r#type: 1,
            metric_family_name: "http_requests".into(),
            help: String::new(),
            unit: String::new(),
        }],
    }
}

/// A readable difference between two normalized answers, if any.
#[must_use]
pub fn difference(case: &str, theirs: &Normalized, ours: &Normalized) -> Option<String> {
    if theirs == ours {
        return None;
    }
    Some(format!(
        "{case}\n  prometheus: {theirs:?}\n  tessera:    {ours:?}"
    ))
}
