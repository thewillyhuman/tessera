// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The HTTP side of the benchmark: posting OTLP, running the queries
//! the targets name, and a webhook that times an eager alert.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prost::Message;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::{lock, now_nanos};

/// Posts `body` as OTLP protobuf to `url`, for dataset `prod`.
pub fn post_protobuf(url: &str, body: &[u8]) {
    let mut response = ureq::post(url)
        .config()
        .http_status_as_error(false)
        .build()
        .header("content-type", "application/x-protobuf")
        .header("x-tessera-dataset", "prod")
        .send(body)
        .expect("the node answers");
    let status = response.status().as_u16();
    assert_eq!(
        status,
        200,
        "ingest refused: {}",
        response.body_mut().read_to_string().unwrap_or_default()
    );
}

/// Runs `query` over the native API against dataset `prod`.
pub fn sql(base: &str, query: &str) -> Value {
    let mut response = ureq::post(format!("{base}/api/v1/query"))
        .config()
        .http_status_as_error(false)
        .build()
        .send_json(json!({ "dataset": "prod", "query": query }))
        .expect("the node answers");
    let status = response.status().as_u16();
    let text = response.body_mut().read_to_string().unwrap_or_default();
    assert_eq!(status, 200, "query failed: {text}");
    serde_json::from_str(&text).unwrap_or(Value::Null)
}

fn get(url: &str, params: &[(&str, String)]) -> String {
    let mut request = ureq::get(url)
        .config()
        .http_status_as_error(false)
        .build()
        .header("x-tessera-dataset", "prod");
    for (name, value) in params {
        request = request.query(*name, value);
    }
    let mut response = request.call().expect("the node answers");
    let status = response.status().as_u16();
    let text = response.body_mut().read_to_string().unwrap_or_default();
    assert_eq!(status, 200, "{url} failed: {text}");
    text
}

/// Waits until the node answers liveness.
pub fn wait_ready(base: &str) {
    let started = Instant::now();
    loop {
        if ureq::get(format!("{base}/healthz")).call().is_ok() {
            return;
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "node never became ready"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A named query, run once per call.
pub type Query = (&'static str, Box<dyn Fn()>);

/// The queries the targets name: over the logs and metrics, then the
/// traces.
pub fn queries(base: &str, trace_ids: &[String]) -> Vec<Query> {
    let now = now_nanos();
    let hour_ago = now - 3_600_000_000_000;
    let mut all = log_and_metric_queries(base, hour_ago, now);
    all.extend(trace_queries(base, trace_ids, hour_ago, now));
    all
}

fn seconds(nanos: u64) -> String {
    format!("{}", nanos / 1_000_000_000)
}

fn log_and_metric_queries(base: &str, hour_ago: u64, now: u64) -> Vec<Query> {
    let b = base.to_owned();
    let panel = move || {
        let answer = sql(
            &b,
            "SELECT resource.service.name AS service, count(*) AS n FROM logs \
             WHERE timestamp > now() - INTERVAL '1 hour' \
             GROUP BY resource.service.name, tumble(timestamp, INTERVAL '1 minute')",
        );
        assert!(
            answer["rows"].as_array().is_some_and(|r| !r.is_empty()),
            "{answer}"
        );
    };
    let b = base.to_owned();
    let errors = move || {
        let answer = sql(
            &b,
            "SELECT count(*) FROM logs WHERE severity_number >= 17 AND timestamp > now() - INTERVAL '1 hour'",
        );
        assert!(
            answer["rows"][0][0].as_f64().unwrap_or(0.0) > 0.0,
            "{answer}"
        );
    };
    let b = base.to_owned();
    let search = move || {
        let text = get(
            &format!("{b}/loki/api/v1/query_range"),
            &[
                ("query", "{host=\"h7\"} |= \"timed out\"".into()),
                ("start", hour_ago.to_string()),
                ("end", now.to_string()),
                ("limit", "100".into()),
            ],
        );
        let answer: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        assert!(
            answer["data"]["result"]
                .as_array()
                .is_some_and(|r| !r.is_empty()),
            "no log lines matched: {text}"
        );
    };
    let b = base.to_owned();
    let promql = move || {
        let text = get(
            &format!("{b}/api/v1/query_range"),
            &[
                (
                    "query",
                    "sum by (host) (avg_over_time(cpu_usage[5m]))".into(),
                ),
                ("start", seconds(hour_ago)),
                ("end", seconds(now)),
                ("step", "60".into()),
            ],
        );
        let answer: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        assert!(
            answer["data"]["result"]
                .as_array()
                .is_some_and(|r| !r.is_empty()),
            "no series matched: {text}"
        );
    };
    vec![
        ("Dashboard panel, SQL, 1 h by minute", Box::new(panel)),
        ("Error count, SQL, 1 h", Box::new(errors)),
        ("Line-filter search, LogQL, 1 h", Box::new(search)),
        ("Dashboard panel, PromQL, 1 h", Box::new(promql)),
    ]
}

fn trace_queries(base: &str, trace_ids: &[String], hour_ago: u64, now: u64) -> Vec<Query> {
    let b = base.to_owned();
    let ids: Vec<String> = trace_ids
        .iter()
        .step_by((trace_ids.len() / 40).max(1))
        .cloned()
        .collect();
    let next = Arc::new(Mutex::new(0usize));
    let trace = move || {
        let mut index = lock(&next);
        let id = &ids[*index % ids.len().max(1)];
        *index += 1;
        let text = get(&format!("{b}/api/traces/{id}"), &[]);
        assert!(text.contains("spans") || text.contains("batches"), "{text}");
    };
    let b = base.to_owned();
    let traceql = move || {
        let text = get(
            &format!("{b}/api/search"),
            &[
                ("q", "{ span.http.status_code = 500 }".into()),
                ("start", seconds(hour_ago)),
                ("end", seconds(now)),
                ("limit", "20".into()),
            ],
        );
        let answer: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        assert!(
            answer["traces"].as_array().is_some_and(|r| !r.is_empty()),
            "no traces matched: {text}"
        );
    };
    vec![
        ("Trace by id, Tempo", Box::new(trace)),
        ("Attribute search, TraceQL, 1 h", Box::new(traceql)),
    ]
}

/// The target a query stands in for, and how this run differs from it.
pub fn target_of(name: &str) -> (&'static str, &'static str) {
    match name {
        n if n.starts_with("Dashboard") => ("p95 <= 200 ms warm", ""),
        n if n.starts_with("Error count") => ("p95 <= 200 ms warm", ""),
        n if n.starts_with("Line-filter") => {
            ("p95 <= 5 s cold over 1 TB", " (this run: MBs, not a TB)")
        }
        n if n.starts_with("Trace by id") => (
            "p99 <= 500 ms cold over 30 days",
            " (this run: 1 h of spans)",
        ),
        n if n.starts_with("Attribute search") => {
            ("p95 <= 3 s cold over 24 h", " (this run: 1 h of spans)")
        }
        _ => ("", ""),
    }
}

/// What the alert measurement found.
pub struct AlertMeasured {
    /// Write acknowledged to webhook received, per probe.
    pub latencies: Vec<Duration>,
    /// What `EXPLAIN ALERT` said of the rule.
    pub mode: String,
}

/// Times an eager rule from a write's acknowledgment to the webhook's
/// receipt, `probes` times, each probe its own alert instance.
pub async fn alert_latency(base: &str, probes: usize) -> AlertMeasured {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a webhook port");
    let port = listener.local_addr().expect("an address").port();
    let received: Arc<Mutex<Vec<(Instant, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&received);
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let sink = Arc::clone(&sink);
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0u8; 4096];
                let body_start = loop {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buffer.extend_from_slice(&chunk[..n]);
                    if let Some(at) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                        break at + 4;
                    }
                };
                let head = String::from_utf8_lossy(&buffer[..body_start]).to_lowercase();
                let length: usize = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse().ok())
                    .unwrap_or(0);
                while buffer.len() < body_start + length {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        break;
                    }
                    buffer.extend_from_slice(&chunk[..n]);
                }
                let body = String::from_utf8_lossy(&buffer[body_start..]).into_owned();
                lock(&sink).push((Instant::now(), body));
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                    .await;
            });
        }
    });
    sql(
        base,
        &format!(
            "INSERT INTO system.notification_routes (name, kind, url) \
             VALUES ('bench', 'webhook', 'http://127.0.0.1:{port}/hook')"
        ),
    );
    sql(
        base,
        "CREATE ALERT bench_probe SEVERITY warning NOTIFY bench AS \
         SELECT attr.probe AS probe, count(*) AS n FROM logs WHERE attr.alert = 'yes' \
         GROUP BY attr.probe, tumble(timestamp, INTERVAL '1 minute') HAVING count(*) > 0",
    );
    let mode = sql(base, "EXPLAIN ALERT bench_probe")["rows"][0][0]
        .as_str()
        .unwrap_or("?")
        .to_owned();
    let url = format!("{base}/v1/logs");
    let mut latencies = Vec::with_capacity(probes);
    for probe in 0..probes {
        let name = format!("probe-{probe}");
        let body =
            crate::load::marker_request(vec![("alert", "yes"), ("probe", &name)], now_nanos())
                .encode_to_vec();
        post_protobuf(&url, &body);
        let acked = Instant::now();
        let deadline = acked + Duration::from_secs(30);
        let got = loop {
            if let Some((at, _)) = lock(&received)
                .iter()
                .find(|(_, body)| body.contains(&name))
            {
                break Some(*at);
            }
            if Instant::now() > deadline {
                break None;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        match got {
            Some(at) => latencies.push(at.saturating_duration_since(acked)),
            None => eprintln!("alert probe {name}: no notification within 30 s"),
        }
    }
    AlertMeasured { latencies, mode }
}
