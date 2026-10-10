// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Differential testing of `LogQL` against Loki (QL-4, section 18.5): the
//! same lines are pushed to a Loki instance and to an in-process Tessera
//! node over the same push API, the same queries are sent to both over
//! the same query API, and the answers must agree.
//!
//! Loki is reached through `TESSERA_LOKI_URL` (for example
//! `http://127.0.0.1:3100`); without it the suite reports that it was
//! skipped. Every deviation found this way is documented in the `LogQL`
//! crate.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use serde_json::{Value, json};
use tessera_server::AppState;

/// One Loki-compatible endpoint.
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
    /// Pushes `streams` (the body of `/loki/api/v1/push`).
    ///
    /// # Panics
    ///
    /// If the endpoint rejects the push.
    pub fn push(&self, streams: &Value) {
        let response = agent()
            .post(format!("{}/loki/api/v1/push", self.base))
            .header("content-type", "application/json")
            .send(streams.to_string().as_bytes())
            .unwrap_or_else(|e| panic!("{}: push failed: {e}", self.name));
        assert!(
            response.status().is_success(),
            "{}: push returned {}",
            self.name,
            response.status()
        );
    }

    /// Runs a range query and returns the `data` of the response.
    ///
    /// # Panics
    ///
    /// If the request fails or the endpoint answers an error.
    #[must_use]
    pub fn query_range(
        &self,
        query: &str,
        start: i64,
        end: i64,
        step: &str,
        direction: &str,
    ) -> Value {
        let url = format!("{}/loki/api/v1/query_range", self.base);
        let mut response = agent()
            .get(&url)
            .query("query", query)
            .query("start", start.to_string())
            .query("end", end.to_string())
            .query("step", step)
            .query("direction", direction)
            .query("limit", "1000")
            .call()
            .unwrap_or_else(|e| panic!("{}: query failed: {e}", self.name));
        let status = response.status();
        let body: Value = response
            .body_mut()
            .read_json()
            .unwrap_or_else(|e| panic!("{}: unreadable response: {e}", self.name));
        assert!(
            status.is_success(),
            "{}: {query} returned {status}: {body}",
            self.name
        );
        body["data"].clone()
    }
}

/// A label set.
pub type Labels = BTreeMap<String, String>;
/// Stream entries as (timestamp, line), ordered as returned.
pub type Entries = Vec<(String, String)>;
/// Samples as (milliseconds, value in thousandths), so that they compare
/// exactly.
pub type Samples = Vec<(i64, i64)>;

/// A normalized result: streams or series, each by label set.
#[derive(Clone, Debug, PartialEq)]
pub enum Normalized {
    /// Streams.
    Streams(Vec<(Labels, Entries)>),
    /// Series.
    Matrix(Vec<(Labels, Samples)>),
}

/// Reduces a `data` object to what both engines must agree on.
///
/// # Panics
///
/// If the data is neither streams nor a matrix.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn normalize(data: &Value) -> Normalized {
    let labels = |v: &Value| -> BTreeMap<String, String> {
        v.as_object()
            .map(|o| {
                o.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_owned()))
                    .collect()
            })
            .unwrap_or_default()
    };
    match data["resultType"].as_str() {
        Some("streams") => {
            let mut streams: Vec<_> = data["result"]
                .as_array()
                .unwrap_or(&Vec::new())
                .iter()
                .map(|s| {
                    let entries = s["values"]
                        .as_array()
                        .unwrap_or(&Vec::new())
                        .iter()
                        .map(|e| {
                            (
                                e[0].as_str().unwrap_or_default().to_owned(),
                                e[1].as_str().unwrap_or_default().to_owned(),
                            )
                        })
                        .collect();
                    (labels(&s["stream"]), entries)
                })
                .collect();
            streams.sort_by(|a, b| a.0.cmp(&b.0));
            Normalized::Streams(streams)
        }
        Some("matrix") => {
            let mut series: Vec<_> = data["result"]
                .as_array()
                .unwrap_or(&Vec::new())
                .iter()
                .map(|s| {
                    let samples = s["values"]
                        .as_array()
                        .unwrap_or(&Vec::new())
                        .iter()
                        .map(|p| {
                            let t = (p[0].as_f64().unwrap_or_default() * 1000.0).round() as i64;
                            let v = p[1]
                                .as_str()
                                .and_then(|v| v.parse::<f64>().ok())
                                .unwrap_or_default();
                            (t, (v * 1000.0).round() as i64)
                        })
                        .collect();
                    (labels(&s["metric"]), samples)
                })
                .collect();
            series.sort_by(|a, b| a.0.cmp(&b.0));
            Normalized::Matrix(series)
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

/// The Loki endpoint named by `TESSERA_LOKI_URL`, if any.
#[must_use]
pub fn loki_from_env() -> Option<Endpoint> {
    std::env::var("TESSERA_LOKI_URL").ok().map(|base| Endpoint {
        base: base.trim_end_matches('/').to_owned(),
        name: "loki",
    })
}

/// The lines both engines receive: three apps, two levels, JSON and
/// logfmt bodies, over `count` entries one second apart from `start`.
#[must_use]
pub fn fixture(start_seconds: i64, count: i64) -> Value {
    let apps = ["checkout", "web", "search"];
    let levels = ["info", "error"];
    let mut streams: BTreeMap<(usize, usize), Vec<Value>> = BTreeMap::new();
    for i in 0..count {
        let app = usize::try_from(i % 3).unwrap_or(0);
        let level = usize::try_from(i / 3 % 2).unwrap_or(0);
        let ts = (start_seconds + i) * 1_000_000_000;
        let line = if i % 2 == 0 {
            format!(
                r#"{{"status": {}, "path": "/{}", "latency": "{}ms"}}"#,
                200 + (i % 4) * 100,
                apps[app],
                50 + i * 10
            )
        } else {
            format!(
                "status={} path=/{} latency={}ms msg=\"request done\"",
                200 + (i % 4) * 100,
                apps[app],
                50 + i * 10
            )
        };
        streams
            .entry((app, level))
            .or_default()
            .push(json!([ts.to_string(), line]));
    }
    json!({
        "streams": streams
            .into_iter()
            .map(|((app, level), values)| json!({
                "stream": {"app": apps[app], "level": levels[level]},
                "values": values,
            }))
            .collect::<Vec<_>>()
    })
}
