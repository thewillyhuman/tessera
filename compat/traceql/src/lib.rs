// SPDX-License-Identifier: Apache-2.0

//! Differential testing of `TraceQL` and the Tempo API against Tempo
//! (QL-4, section 18.5): the same spans are pushed to a Tempo instance
//! and to an in-process Tessera node over OTLP, the same requests are
//! sent to both over the Tempo API, and the answers must agree once
//! reduced to what the API promises.
//!
//! Tempo is reached through `TESSERA_TEMPO_URL` (its API, for example
//! `http://127.0.0.1:3200`) and `TESSERA_TEMPO_OTLP_URL` (its OTLP
//! receiver, `http://127.0.0.1:4318` by default); without the first
//! the suite reports that it was skipped. `tempo.yaml` next to this
//! crate configures a Tempo that answers quickly. Every deviation found
//! this way is documented in the `TraceQL` crate.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;

use base64::Engine;
use serde_json::{Value, json};
use tessera_server::AppState;

/// One Tempo-compatible endpoint.
#[derive(Clone, Debug)]
pub struct Endpoint {
    /// The base URL of the Tempo API, without a trailing slash.
    pub api: String,
    /// The base URL of the OTLP receiver, without a trailing slash.
    pub otlp: String,
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
    /// Pushes an OTLP export of traces as JSON.
    ///
    /// # Panics
    ///
    /// If the endpoint rejects the push.
    pub fn push(&self, traces: &Value) {
        let response = agent()
            .post(format!("{}/v1/traces", self.otlp))
            .header("content-type", "application/json")
            .send(traces.to_string().as_bytes())
            .unwrap_or_else(|e| panic!("{}: push failed: {e}", self.name));
        assert!(
            response.status().is_success(),
            "{}: push returned {}",
            self.name,
            response.status()
        );
    }

    /// `GET path?query`, as JSON, or the endpoint's complaint.
    ///
    /// # Errors
    ///
    /// If the request fails or the endpoint answers an error.
    pub fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value, String> {
        let mut request = agent().get(format!("{}{path}", self.api));
        for (key, value) in query {
            request = request.query(*key, value);
        }
        let mut response = request
            .call()
            .map_err(|e| format!("{}: {path} failed: {e}", self.name))?;
        let status = response.status();
        let text = response
            .body_mut()
            .read_to_string()
            .map_err(|e| format!("{}: unreadable response: {e}", self.name))?;
        if !status.is_success() {
            return Err(format!("{}: {path} returned {status}: {text}", self.name));
        }
        serde_json::from_str(&text)
            .map_err(|e| format!("{}: {path} answered non-JSON ({e}): {text}", self.name))
    }

    /// A search, every span of every spanset included.
    ///
    /// # Errors
    ///
    /// If the request fails or the endpoint answers an error.
    pub fn search(&self, q: &str, start: i64, end: i64) -> Result<Value, String> {
        self.get(
            "/api/search",
            &[
                ("q", q.to_owned()),
                ("start", start.to_string()),
                ("end", end.to_string()),
                ("limit", "100".to_owned()),
                ("spss", "100".to_owned()),
            ],
        )
    }

    /// A metrics range query.
    ///
    /// # Errors
    ///
    /// If the request fails or the endpoint answers an error.
    pub fn metrics(&self, q: &str, start: i64, end: i64, step: &str) -> Result<Value, String> {
        self.get(
            "/api/metrics/query_range",
            &[
                ("q", q.to_owned()),
                ("start", start.to_string()),
                ("end", end.to_string()),
                ("step", step.to_owned()),
            ],
        )
    }
}

/// A trace id or span id as lowercase hex without leading zeros, from
/// the hex Tessera writes or the base64 Tempo writes.
fn id(value: &Value) -> String {
    let text = value.as_str().unwrap_or_default();
    let bytes = if text.len().is_multiple_of(2) && text.chars().all(|c| c.is_ascii_hexdigit()) {
        (0..text.len() / 2)
            .filter_map(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16).ok())
            .collect::<Vec<u8>>()
    } else {
        base64::engine::general_purpose::STANDARD
            .decode(text)
            .unwrap_or_default()
    };
    let hex = bytes.iter().fold(String::new(), |mut out, b| {
        use std::fmt::Write;
        let _ = write!(out, "{b:02x}");
        out
    });
    hex.trim_start_matches('0').to_owned()
}

/// A label set.
pub type Labels = BTreeMap<String, String>;
/// A spanset of a search result: matched count, span ids, `by` labels.
pub type NormalizedSpanset = (i64, Vec<String>, Labels);
/// A trace of a search result: id, root service, root name, spansets.
pub type NormalizedTrace = (String, String, String, Vec<NormalizedSpanset>);
/// Samples as (milliseconds, value in millionths).
pub type Samples = Vec<(i64, i64)>;

/// A search result reduced to its traces, each sorted.
#[must_use]
pub fn normalize_search(data: &Value) -> Vec<NormalizedTrace> {
    let mut traces: Vec<_> = data["traces"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|t| {
            let mut spansets: Vec<_> = t["spanSets"]
                .as_array()
                .unwrap_or(&Vec::new())
                .iter()
                .map(|s| {
                    let mut spans: Vec<String> = s["spans"]
                        .as_array()
                        .unwrap_or(&Vec::new())
                        .iter()
                        .map(|span| id(&span["spanID"]))
                        .collect();
                    spans.sort();
                    // Only `by` labels are promised; aggregate filters add
                    // their statistic in Tempo, which Tessera does not.
                    let labels: Labels = s["attributes"]
                        .as_array()
                        .unwrap_or(&Vec::new())
                        .iter()
                        .filter(|a| a["key"].as_str().unwrap_or_default().starts_with("by("))
                        .map(|a| {
                            (
                                a["key"].as_str().unwrap_or_default().to_owned(),
                                a["value"]["stringValue"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .to_owned(),
                            )
                        })
                        .collect();
                    (s["matched"].as_i64().unwrap_or_default(), spans, labels)
                })
                .collect();
            spansets.sort();
            (
                id(&t["traceID"]),
                t["rootServiceName"].as_str().unwrap_or_default().to_owned(),
                t["rootTraceName"].as_str().unwrap_or_default().to_owned(),
                spansets,
            )
        })
        .collect();
    traces.sort();
    traces
}

fn enum_code(value: &Value, prefix: &str) -> i64 {
    match value {
        Value::Number(n) => n.as_i64().unwrap_or_default(),
        Value::String(s) => match s.strip_prefix(prefix) {
            Some("UNSPECIFIED" | "UNSET") => 0,
            Some("INTERNAL" | "OK") => 1,
            Some("SERVER" | "ERROR") => 2,
            Some("CLIENT") => 3,
            Some("PRODUCER") => 4,
            Some("CONSUMER") => 5,
            _ => -1,
        },
        _ => 0,
    }
}

fn attributes(value: &Value) -> BTreeMap<String, String> {
    value
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|a| {
            let v = &a["value"];
            let text = v["stringValue"]
                .as_str()
                .map(str::to_owned)
                .or_else(|| v["intValue"].as_str().map(str::to_owned))
                .or_else(|| v["intValue"].as_i64().map(|i| i.to_string()))
                .or_else(|| v["boolValue"].as_bool().map(|b| b.to_string()))
                .or_else(|| v["doubleValue"].as_f64().map(|f| f.to_string()))
                .unwrap_or_else(|| v.to_string());
            (a["key"].as_str().unwrap_or_default().to_owned(), text)
        })
        .collect()
}

/// A span of a trace by id, reduced.
pub type NormalizedSpan = (
    String,
    String,
    String,
    i64,
    i64,
    String,
    String,
    BTreeMap<String, String>,
    Vec<(String, String, BTreeMap<String, String>)>,
);

/// A trace by id reduced to its resource spans: resource attributes,
/// scope name, and spans as (id, parent, name, kind, status, start, end,
/// attributes, events), sorted.
#[must_use]
pub fn normalize_trace(
    batches: &Value,
) -> Vec<(BTreeMap<String, String>, String, Vec<NormalizedSpan>)> {
    let mut out: Vec<_> = batches
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .flat_map(|batch| {
            let resource = attributes(&batch["resource"]["attributes"]);
            batch["scopeSpans"]
                .as_array()
                .unwrap_or(&Vec::new())
                .iter()
                .map(|scope| {
                    let mut spans: Vec<NormalizedSpan> = scope["spans"]
                        .as_array()
                        .unwrap_or(&Vec::new())
                        .iter()
                        .map(|s| {
                            let mut events: Vec<_> = s["events"]
                                .as_array()
                                .unwrap_or(&Vec::new())
                                .iter()
                                .map(|e| {
                                    (
                                        e["name"].as_str().unwrap_or_default().to_owned(),
                                        e["timeUnixNano"].as_str().unwrap_or_default().to_owned(),
                                        attributes(&e["attributes"]),
                                    )
                                })
                                .collect();
                            events.sort();
                            (
                                id(&s["spanId"]),
                                id(&s["parentSpanId"]),
                                s["name"].as_str().unwrap_or_default().to_owned(),
                                enum_code(&s["kind"], "SPAN_KIND_"),
                                enum_code(&s["status"]["code"], "STATUS_CODE_"),
                                s["startTimeUnixNano"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .to_owned(),
                                s["endTimeUnixNano"].as_str().unwrap_or_default().to_owned(),
                                attributes(&s["attributes"]),
                                events,
                            )
                        })
                        .collect();
                    spans.sort();
                    (
                        resource.clone(),
                        scope["scope"]["name"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned(),
                        spans,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    out.sort();
    out
}

/// The tag names of the `resource` and `span` scopes. The `event` and
/// `link` scopes and the intrinsics are left out: Tessera does not list
/// span-event attributes as tags, and lists only the intrinsics it
/// evaluates.
#[must_use]
pub fn normalize_tags(data: &Value) -> BTreeMap<String, BTreeSet<String>> {
    data["scopes"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter(|s| matches!(s["name"].as_str(), Some("resource" | "span")))
        .map(|s| {
            (
                s["name"].as_str().unwrap_or_default().to_owned(),
                s["tags"]
                    .as_array()
                    .unwrap_or(&Vec::new())
                    .iter()
                    .filter_map(|t| t.as_str().map(str::to_owned))
                    .collect(),
            )
        })
        .collect()
}

/// Tag values as (type, value) pairs.
#[must_use]
pub fn normalize_tag_values(data: &Value) -> BTreeSet<(String, String)> {
    data["tagValues"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|v| {
            (
                v["type"].as_str().unwrap_or_default().to_owned(),
                v["value"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect()
}

/// Metric series as label sets with their non-zero samples, values in
/// millionths so that they compare exactly.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn normalize_metrics(data: &Value) -> Vec<(Labels, Samples)> {
    let mut series: Vec<_> = data["series"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|s| {
            let labels: Labels = s["labels"]
                .as_array()
                .unwrap_or(&Vec::new())
                .iter()
                .map(|l| {
                    let v = &l["value"];
                    let text = v["stringValue"]
                        .as_str()
                        .map(str::to_owned)
                        .or_else(|| v["intValue"].as_str().map(str::to_owned))
                        .or_else(|| v["doubleValue"].as_f64().map(|f| f.to_string()))
                        .unwrap_or_else(|| v.to_string());
                    (l["key"].as_str().unwrap_or_default().to_owned(), text)
                })
                .collect();
            let mut samples: Samples = s["samples"]
                .as_array()
                .unwrap_or(&Vec::new())
                .iter()
                .filter_map(|p| {
                    let value = p["value"].as_f64()?;
                    if value == 0.0 {
                        return None;
                    }
                    let t = p["timestampMs"]
                        .as_str()
                        .and_then(|t| t.parse::<i64>().ok())
                        .or_else(|| p["timestampMs"].as_i64())?;
                    Some((t, (value * 1e6).round() as i64))
                })
                .collect();
            samples.sort_unstable();
            (labels, samples)
        })
        .filter(|(_, samples)| !samples.is_empty())
        .collect();
    series.sort();
    series
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
    let base = format!("http://{address}");
    Endpoint {
        api: base.clone(),
        otlp: base,
        name: "tessera",
    }
}

/// The Tempo endpoint named by `TESSERA_TEMPO_URL` (and
/// `TESSERA_TEMPO_OTLP_URL`), if any.
#[must_use]
pub fn tempo_from_env() -> Option<Endpoint> {
    let api = std::env::var("TESSERA_TEMPO_URL").ok()?;
    let otlp = std::env::var("TESSERA_TEMPO_OTLP_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:4318".to_owned());
    Some(Endpoint {
        api: api.trim_end_matches('/').to_owned(),
        otlp: otlp.trim_end_matches('/').to_owned(),
        name: "tempo",
    })
}

/// A span of the fixture.
struct FixtureSpan {
    service: &'static str,
    trace: u8,
    id: u8,
    parent: Option<u8>,
    name: &'static str,
    start_ms: i64,
    duration_ms: i64,
    error: bool,
    attributes: Vec<(&'static str, Value)>,
}

/// The ids of the fixture's traces, as hex.
#[must_use]
pub fn fixture_trace_ids(run: u8) -> Vec<String> {
    (1..=3).map(|t| format!("{run:02x}{t:030x}")).collect()
}

/// A fixture span as a row: service, trace, id, parent, name, start in
/// milliseconds after the fixture's start, duration in milliseconds and
/// whether the status is error.
type SpanRow = (
    &'static str,
    u8,
    u8,
    Option<u8>,
    &'static str,
    i64,
    i64,
    bool,
);

/// The spans of the fixture.
const SPANS: [SpanRow; 9] = [
    ("api", 1, 1, None, "GET /", 0, 100, false),
    ("db", 1, 2, Some(1), "db query", 10, 40, true),
    ("db", 1, 3, Some(2), "cache", 20, 5, false),
    ("auth", 1, 4, Some(1), "auth", 60, 10, false),
    ("api", 2, 5, None, "GET /", 15_000, 50, false),
    ("db", 2, 6, Some(5), "db query", 15_010, 20, false),
    ("web", 3, 7, None, "POST /cart", 31_000, 80, false),
    ("db", 3, 8, Some(7), "db query", 31_005, 30, true),
    ("db", 3, 9, Some(7), "db query", 31_040, 10, false),
];

/// The attributes of the fixture span `id`.
fn attributes_of(id: u8) -> Vec<(&'static str, Value)> {
    let text = |s: &str| json!({ "stringValue": s });
    let int = |i: i64| json!({ "intValue": i.to_string() });
    match id {
        1 => vec![("http.method", text("GET")), ("http.status_code", int(500))],
        4 => vec![("attempt", int(2)), ("ok", json!({ "boolValue": true }))],
        5 => vec![("http.method", text("GET")), ("http.status_code", int(200))],
        7 => vec![
            ("http.method", text("POST")),
            ("http.status_code", int(201)),
        ],
        2 | 6 | 8 | 9 => vec![("db.system", text("pg"))],
        _ => Vec::new(),
    }
}

/// The spans of the fixture, in the order they are pushed.
fn fixture_spans() -> Vec<FixtureSpan> {
    SPANS
        .iter()
        .map(
            |&(service, trace, id, parent, name, start_ms, duration_ms, error)| FixtureSpan {
                service,
                trace,
                id,
                parent,
                name,
                start_ms,
                duration_ms,
                error,
                attributes: attributes_of(id),
            },
        )
        .collect()
}

/// The traces both engines receive, starting `start_seconds` apart by
/// trace: an `api` request with a failing `db` query, a cache read
/// under it and an `auth` call; a healthy `api` request with a `db`
/// query; a `web` request with two sibling `db` queries, one failing.
/// Every resource carries `run`, which scopes the queries to this run,
/// and the trace ids start with `run` so that they never collide.
#[must_use]
pub fn fixture(start_seconds: i64, run: u8) -> Value {
    let spans = fixture_spans();
    let mut by_service: BTreeMap<&str, Vec<Value>> = BTreeMap::new();
    for span in spans {
        let start = start_seconds * 1_000_000_000 + span.start_ms * 1_000_000;
        let end = start + span.duration_ms * 1_000_000;
        let mut value = json!({
            "traceId": format!("{run:02x}{:030x}", span.trace),
            "spanId": format!("{run:02x}{:014x}", span.id),
            "name": span.name,
            "kind": 2,
            "startTimeUnixNano": start.to_string(),
            "endTimeUnixNano": end.to_string(),
            "attributes": span.attributes.iter().map(|(k, v)| json!({ "key": k, "value": v })).collect::<Vec<_>>(),
            "events": [{ "timeUnixNano": (start + 1_000_000).to_string(), "name": "log", "attributes": [{ "key": "severity", "value": { "stringValue": "info" } }] }],
        });
        if let Some(parent) = span.parent {
            value["parentSpanId"] = json!(format!("{run:02x}{:014x}", parent));
        }
        if span.error {
            value["status"] = json!({ "code": 2, "message": "failed" });
        }
        by_service.entry(span.service).or_default().push(value);
    }
    json!({
        "resourceSpans": by_service.into_iter().map(|(service, spans)| json!({
            "resource": { "attributes": [
                { "key": "service.name", "value": { "stringValue": service } },
                { "key": "run", "value": { "stringValue": format!("r{run}") } },
            ] },
            "scopeSpans": [{ "scope": { "name": "compat" }, "spans": spans }],
        })).collect::<Vec<_>>()
    })
}

/// A readable difference between two normalized answers, if any.
#[must_use]
pub fn difference<T: PartialEq + std::fmt::Debug>(
    case: &str,
    theirs: &T,
    ours: &T,
) -> Option<String> {
    if theirs == ours {
        return None;
    }
    Some(format!(
        "{case}\n  tempo:   {theirs:?}\n  tessera: {ours:?}"
    ))
}
