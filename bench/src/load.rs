// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Load generation: representative application logs, gauge metrics and
//! spans, built as OTLP protobuf and posted to the node's OTLP HTTP
//! endpoints. Logs are generated before the clock starts, so the
//! measured time is the node's, and freshness probes are single rows
//! posted along the way and polled for.

// Gauge values and byte counts are small; the casts lose nothing that
// matters.
#![allow(clippy::cast_precision_loss)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use tessera_types::{Random, SeededRandom};

use crate::{lock, now_nanos, web};

const SERVICES: [&str; 3] = ["checkout", "search", "payments"];
const METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];
const PATHS: [&str; 5] = [
    "/api/orders",
    "/api/cart",
    "/api/search",
    "/api/users",
    "/api/payments",
];
/// Rows are spread over the last fifty-five minutes.
const SPREAD_NANOS: u64 = 55 * 60 * 1_000_000_000;

fn text(key: &str, value: impl Into<String>) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
        ..Default::default()
    }
}

fn int(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
        ..Default::default()
    }
}

fn resource(service: &str) -> Resource {
    Resource {
        attributes: vec![
            text("service.name", service),
            text("deployment.environment", "prod"),
        ],
        ..Default::default()
    }
}

/// One log record of the representative mix: request lines mostly, a
/// few timeouts and errors, with the attributes an application sends.
fn log_record(random: &mut SeededRandom, now: u64) -> LogRecord {
    let host = format!("h{}", random.next_u64() % 20 + 1);
    let method = METHODS[(random.next_u64() % 4) as usize];
    let path = PATHS[(random.next_u64() % 5) as usize];
    let id = random.next_u64() % 100_000;
    let ms = random.next_u64() % 900 + 1;
    let roll = random.next_u64() % 100;
    let (body, severity, status) = if roll < 5 {
        (
            format!(
                "connection to 10.0.{}.{} timed out after {ms} ms",
                random.next_u64() % 256,
                random.next_u64() % 256
            ),
            (17, "ERROR"),
            504,
        )
    } else if roll < 10 {
        (
            format!("{method} {path}/{id} failed: upstream returned 500 after {ms}ms"),
            (17, "ERROR"),
            500,
        )
    } else if roll < 30 {
        (format!("cache miss for key order:{id}"), (5, "DEBUG"), 200)
    } else {
        (
            format!("{method} {path}/{id} 200 in {ms}ms"),
            (9, "INFO"),
            200,
        )
    };
    let request_id = format!("{:016x}", random.next_u64());
    LogRecord {
        time_unix_nano: now - random.next_u64() % SPREAD_NANOS,
        severity_number: severity.0,
        severity_text: severity.1.into(),
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(body)),
        }),
        attributes: vec![
            text("host", host),
            text("http.method", method),
            text("http.route", path),
            int("http.status_code", status),
            int("duration_ms", i64::try_from(ms).unwrap_or(0)),
            text("request.id", request_id),
            int(
                "user.id",
                i64::try_from(random.next_u64() % 50_000).unwrap_or(0),
            ),
        ],
        ..Default::default()
    }
}

/// `rows` log records of one service, as a request.
fn log_request(seed: u64, rows: usize, now: u64) -> ExportLogsServiceRequest {
    let mut random = SeededRandom::new(seed);
    let service = SERVICES[(seed % 3) as usize];
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource(service)),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: (0..rows).map(|_| log_record(&mut random, now)).collect(),
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// A single-row request carrying `attributes`, for probes.
pub fn marker_request(attributes: Vec<(&str, &str)>, now: u64) -> ExportLogsServiceRequest {
    let mut record = log_record(&mut SeededRandom::new(now), now);
    record.time_unix_nano = now;
    record
        .attributes
        .extend(attributes.into_iter().map(|(k, v)| text(k, v)));
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource("probe")),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![record],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// What ingesting the logs measured.
pub struct Ingested {
    /// The wall time of the whole ingest.
    pub wall: Duration,
    /// OTLP protobuf bytes sent.
    pub otlp_bytes: u64,
    /// The same requests as OTLP JSON.
    pub json_bytes: u64,
    /// The same records as flat JSON objects, one per line, as an
    /// application would write them: the raw size compression is
    /// measured against.
    pub flat_bytes: u64,
    /// The latency of each request, request sent to acknowledged.
    pub ack_latencies: Vec<Duration>,
    /// Acknowledged to visible, per probe.
    pub freshness: Vec<Duration>,
}

/// Ingests `rows` logs in requests of `batch` rows from `clients`
/// threads, probing freshness at a quarter, half, three quarters and
/// the end.
pub async fn ingest_logs(base: &str, rows: usize, batch: usize, clients: usize) -> Ingested {
    let now = now_nanos();
    let batches = rows.div_ceil(batch);
    let prepared: Vec<(Vec<u8>, u64, u64)> = (0..batches)
        .map(|i| {
            let request = log_request(i as u64, batch.min(rows - i * batch), now);
            let json = serde_json::to_vec(&request).map_or(0, |j| j.len() as u64);
            let flat = flat_json_bytes(&request);
            (request.encode_to_vec(), json, flat)
        })
        .collect();
    let otlp_bytes: u64 = prepared.iter().map(|(b, _, _)| b.len() as u64).sum();
    let json_bytes: u64 = prepared.iter().map(|(_, j, _)| *j).sum();
    let flat_bytes: u64 = prepared.iter().map(|(_, _, f)| *f).sum();
    let prepared = Arc::new(prepared);
    let done = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(batches)));
    let started = Instant::now();
    let mut workers = Vec::new();
    for client in 0..clients.max(1) {
        let prepared = Arc::clone(&prepared);
        let done = Arc::clone(&done);
        let latencies = Arc::clone(&latencies);
        let url = format!("{base}/v1/logs");
        workers.push(std::thread::spawn(move || {
            let mut mine = Vec::new();
            for i in (client..prepared.len()).step_by(clients.max(1)) {
                let at = Instant::now();
                web::post_protobuf(&url, &prepared[i].0);
                mine.push(at.elapsed());
                done.fetch_add(1, Ordering::Relaxed);
            }
            lock(&latencies).extend(mine);
        }));
    }
    let mut freshness = Vec::new();
    for (probe, share) in [(0, 1), (1, 2), (2, 3), (3, 4)] {
        while done.load(Ordering::Relaxed) * 4 < batches * share {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let name = format!("fresh-{probe}");
        let request = marker_request(vec![("probe", &name)], now_nanos());
        let url = format!("{base}/v1/logs");
        let body = request.encode_to_vec();
        web::post_protobuf(&url, &body);
        let acked = Instant::now();
        let query = format!("SELECT count(*) FROM logs WHERE attr.probe = '{name}'");
        loop {
            let answer = web::sql(base, &query);
            if answer["rows"][0][0].as_f64().unwrap_or(0.0) >= 1.0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        freshness.push(acked.elapsed());
    }
    for worker in workers {
        let _ = tokio::task::spawn_blocking(move || worker.join()).await;
    }
    let wall = started.elapsed();
    let ack_latencies = lock(&latencies).clone();
    Ingested {
        wall,
        otlp_bytes,
        json_bytes,
        flat_bytes,
        ack_latencies,
        freshness,
    }
}

/// The records of `request` as an application would log them: one flat
/// JSON object per line with the timestamp, level, message and
/// attributes, newline-terminated.
fn flat_json_bytes(request: &ExportLogsServiceRequest) -> u64 {
    let mut total = 0u64;
    for resource_logs in &request.resource_logs {
        let service = resource_logs
            .resource
            .as_ref()
            .and_then(|r| r.attributes.first())
            .and_then(|a| a.value.as_ref())
            .and_then(|v| match &v.value {
                Some(any_value::Value::StringValue(s)) => Some(s.clone()),
                _ => None,
            })
            .unwrap_or_default();
        for scope in &resource_logs.scope_logs {
            for record in &scope.log_records {
                let mut object = serde_json::Map::new();
                object.insert(
                    "ts".into(),
                    serde_json::json!(record.time_unix_nano / 1_000_000),
                );
                object.insert("level".into(), serde_json::json!(record.severity_text));
                object.insert("service".into(), serde_json::json!(service));
                let body = record.body.as_ref().and_then(|b| match &b.value {
                    Some(any_value::Value::StringValue(s)) => Some(s.clone()),
                    _ => None,
                });
                object.insert("msg".into(), serde_json::json!(body.unwrap_or_default()));
                for attribute in &record.attributes {
                    let value = match attribute.value.as_ref().and_then(|v| v.value.as_ref()) {
                        Some(any_value::Value::StringValue(s)) => serde_json::json!(s),
                        Some(any_value::Value::IntValue(i)) => serde_json::json!(i),
                        _ => serde_json::Value::Null,
                    };
                    object.insert(attribute.key.clone(), value);
                }
                total += serde_json::Value::Object(object).to_string().len() as u64 + 1;
            }
        }
    }
    total
}

/// What ingesting the metrics measured.
pub struct MetricsIngested {
    /// Samples sent.
    pub samples: u64,
    /// The wall time.
    pub wall: Duration,
}

/// `series` gauge series of `samples` points each, ten seconds apart,
/// ending now, in requests of ten series.
pub fn ingest_metrics(base: &str, series: usize, samples: usize) -> MetricsIngested {
    let now = now_nanos();
    let started = Instant::now();
    let url = format!("{base}/v1/metrics");
    let mut random = SeededRandom::new(7);
    let mut sent = 0u64;
    for chunk in (0..series).collect::<Vec<_>>().chunks(10) {
        let metrics: Vec<Metric> = ["cpu_usage", "memory_bytes"]
            .iter()
            .map(|name| Metric {
                name: (*name).into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: chunk
                        .iter()
                        .flat_map(|s| (0..samples).map(move |i| (s, i)))
                        .map(|(s, i)| NumberDataPoint {
                            attributes: vec![
                                text("host", format!("h{}", s % 20 + 1)),
                                text("job", format!("job{}", s % 7)),
                            ],
                            time_unix_nano: now - (samples - i) as u64 * 10_000_000_000,
                            value: Some(number_data_point::Value::AsDouble(
                                (random.next_u64() % 10_000) as f64 / 100.0,
                            )),
                            ..Default::default()
                        })
                        .collect(),
                })),
                ..Default::default()
            })
            .collect();
        sent += (chunk.len() * samples * 2) as u64;
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(resource("node-exporter")),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        web::post_protobuf(&url, &request.encode_to_vec());
    }
    MetricsIngested {
        samples: sent,
        wall: started.elapsed(),
    }
}

/// What ingesting the traces measured.
pub struct TracesIngested {
    /// The trace ids, as hex.
    pub ids: Vec<String>,
    /// The wall time.
    pub wall: Duration,
}

/// `traces` traces of five spans each, a root and four children, in
/// requests of a hundred traces.
pub fn ingest_traces(base: &str, traces: usize) -> TracesIngested {
    let now = now_nanos();
    let started = Instant::now();
    let url = format!("{base}/v1/traces");
    let mut random = SeededRandom::new(11);
    let mut ids = Vec::with_capacity(traces);
    for chunk in (0..traces).collect::<Vec<_>>().chunks(100) {
        let mut spans = Vec::new();
        for t in chunk {
            let trace_id = random.next_u128().to_be_bytes().to_vec();
            ids.push(
                trace_id
                    .iter()
                    .fold(String::with_capacity(32), |mut hex, b| {
                        use std::fmt::Write;
                        let _ = write!(hex, "{b:02x}");
                        hex
                    }),
            );
            let start = now - random.next_u64() % SPREAD_NANOS;
            let root_id = random.next_u64().to_be_bytes().to_vec();
            let status = u64::from(t % 20 == 0);
            spans.push(Span {
                trace_id: trace_id.clone(),
                span_id: root_id.clone(),
                name: format!("{} {}", METHODS[t % 4], PATHS[t % 5]),
                kind: 2,
                start_time_unix_nano: start,
                end_time_unix_nano: start + 50_000_000,
                attributes: vec![
                    int("http.status_code", if status == 1 { 500 } else { 200 }),
                    text("host", format!("h{}", t % 20 + 1)),
                ],
                status: Some(Status {
                    code: if status == 1 { 2 } else { 1 },
                    message: String::new(),
                }),
                ..Default::default()
            });
            for child in 0..4u64 {
                spans.push(Span {
                    trace_id: trace_id.clone(),
                    span_id: random.next_u64().to_be_bytes().to_vec(),
                    parent_span_id: root_id.clone(),
                    name: format!("db.query.{child}"),
                    kind: 3,
                    start_time_unix_nano: start + child * 5_000_000,
                    end_time_unix_nano: start + child * 5_000_000 + 4_000_000,
                    attributes: vec![text("db.system", "postgresql")],
                    status: Some(Status {
                        code: 1,
                        message: String::new(),
                    }),
                    ..Default::default()
                });
            }
        }
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(resource("checkout")),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        web::post_protobuf(&url, &request.encode_to_vec());
    }
    TracesIngested {
        ids,
        wall: started.elapsed(),
    }
}
