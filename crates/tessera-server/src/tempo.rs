// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The Tempo HTTP API (section 16): trace by id, search with `TraceQL`,
//! tag names and values, and `TraceQL` metrics, under `/api`. Grafana's
//! Tempo data source works against it unmodified (AP-1).
//!
//! The tenant and dataset come from the headers the `params` module
//! reads (CT-5). Times are Unix seconds, or nanoseconds when large, as
//! Tempo takes them; a request without a range looks at the last hour.
//! A trace by id is served as OTLP JSON, or as protobuf when the
//! request accepts it, which Grafana's backend uses; the protobuf of
//! `TracesData` is wire-compatible with Tempo's `Trace` message.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use arrow::array::{
    Array, FixedSizeBinaryArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use axum::extract::{Path, Query, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, TracesData};
use prost::Message;
use serde::Deserialize;
use serde_json::{Value, json};
use tessera_exec::QueryError;
use tessera_ir::trace::{
    MATCHED, ROOT_NAME, ROOT_SERVICE, SPANSET, SPANSET_LABELS, TRACE_DURATION_NS, TRACE_START,
};
use tessera_ir::{BinaryOp, BindContext, Expr, Literal, Plan, Scan, SortKey, VALUE};
use tessera_kernels::attributes_column;
use tessera_opt::optimize;
use tessera_traceql::{Lowered, RangeParams, ResultKind, TraceqlFrontend};
use tessera_types::event::{
    DURATION_NS, LINKS, NAME, PARENT_SPAN_ID, SPAN_ID, SPAN_KIND, STATUS_CODE, STATUS_MESSAGE,
    TIMESTAMP, TRACE_ID,
};
use tessera_types::{Kind, TimeRange, Timestamp};

use crate::params::{Need, Scope, parse_step, parse_time, scope};
use crate::{ApiError, AppState};

/// The Tempo router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/echo", get(|| async { "echo" }))
        .route(
            "/api/status/buildinfo",
            get(|| async {
                Json(json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "revision": "",
                    "branch": "",
                    "buildUser": "",
                    "buildDate": "",
                    "goVersion": "none",
                }))
            }),
        )
        .route("/api/search", get(search))
        .route("/api/traces/{id}", get(trace_by_id))
        .route("/api/v2/traces/{id}", get(trace_by_id_v2))
        .route("/api/search/tags", get(tags_v1))
        .route("/api/search/tag/{tag}/values", get(tag_values_v1))
        .route("/api/v2/search/tags", get(tags_v2))
        .route("/api/v2/search/tag/{tag}/values", get(tag_values_v2))
        .route("/api/metrics/query_range", get(metrics_query_range))
        .route("/api/metrics/query", get(metrics_query_instant))
        .with_state(state)
}

/// Traces a search returns when the request does not say.
const DEFAULT_LIMIT: usize = 20;
/// Spans per spanset a search returns when the request does not say.
const DEFAULT_SPANS_PER_SPANSET: usize = 3;
/// How many spans the tag endpoints look at, newest first.
const DISCOVERY_ROWS: usize = 50_000;
/// What Tempo reports as the root service of a trace whose root span
/// is missing.
const ROOT_NOT_RECEIVED: &str = "<root span not yet received>";
/// The intrinsics the `TraceQL` front end evaluates, for tag discovery.
const INTRINSICS: [&str; 16] = [
    "duration",
    "kind",
    "name",
    "rootName",
    "rootServiceName",
    "status",
    "statusMessage",
    "traceDuration",
    "span:duration",
    "span:kind",
    "span:name",
    "span:status",
    "span:statusMessage",
    "trace:duration",
    "trace:rootName",
    "trace:rootService",
];

/// The query parameters of the endpoints.
#[derive(Deserialize, Default)]
struct Params {
    q: Option<String>,
    start: Option<String>,
    end: Option<String>,
    step: Option<String>,
    limit: Option<usize>,
    spss: Option<usize>,
    scope: Option<String>,
}

/// The time range of a request: `start` and `end`, or the last hour.
fn time_range(params: &Params, now: Timestamp) -> Result<(Timestamp, Timestamp), ApiError> {
    let end = match &params.end {
        Some(end) => parse_time(end, now)?,
        None => now,
    };
    let start = match &params.start {
        Some(start) => parse_time(start, now)?,
        None => Timestamp::from_nanos(end.as_nanos().saturating_sub(3_600_000_000_000)),
    };
    Ok((start, end))
}

fn lower(
    text: &str,
    scope: &Scope,
    params: RangeParams,
    now: Timestamp,
) -> Result<Lowered, ApiError> {
    let ctx = BindContext::new(scope.tenant.clone(), scope.dataset.clone(), now);
    TraceqlFrontend { params }
        .lower_query(text, &ctx)
        .map_err(|e| ApiError::query(QueryError::Syntax(e), text))
}

async fn run(state: &AppState, scope: &Scope, plan: Plan) -> Result<Vec<RecordBatch>, ApiError> {
    let plan = optimize(
        crate::params::restricted(state, scope, plan),
        state.engine.registry(),
    )
    .map_err(|e| ApiError::query(QueryError::Plan(e), ""))?;
    let started = std::time::Instant::now();
    let output = state
        .engine
        .run(&plan)
        .await
        .map_err(|e| ApiError::query(QueryError::Engine(e), ""))?;
    crate::workload::record(
        state,
        &plan,
        &output.analysis,
        "traceql",
        "api",
        started.elapsed(),
    );
    Ok(output.batches)
}

fn text<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

fn cell(column: Option<&StringArray>, i: usize) -> Option<&str> {
    column.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

fn fixed<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a FixedSizeBinaryArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
}

fn ints<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Int64Array> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
}

fn times<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a TimestampNanosecondArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, b| {
        use std::fmt::Write;
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// The attributes of a row, from its `_overflow` JSON.
fn attributes(overflow: Option<&str>) -> BTreeMap<String, Value> {
    match overflow.and_then(|o| serde_json::from_str::<Value>(o).ok()) {
        Some(Value::Object(object)) => object.into_iter().collect(),
        _ => BTreeMap::new(),
    }
}

/// A JSON scalar as an OTLP `AnyValue` in JSON: integers as strings,
/// as the protobuf JSON mapping writes 64-bit numbers.
fn any_value_json(value: &Value) -> Value {
    match value {
        Value::Bool(b) => json!({ "boolValue": b }),
        Value::Number(n) if n.is_i64() => json!({ "intValue": n.to_string() }),
        Value::Number(n) => json!({ "doubleValue": n }),
        Value::String(s) => json!({ "stringValue": s }),
        other => json!({ "stringValue": other.to_string() }),
    }
}

/// A JSON scalar as an OTLP `AnyValue`.
fn any_value(value: &Value) -> AnyValue {
    let value = match value {
        Value::Bool(b) => Some(any_value::Value::BoolValue(*b)),
        Value::Number(n) if n.is_i64() => n.as_i64().map(any_value::Value::IntValue),
        Value::Number(n) => n.as_f64().map(any_value::Value::DoubleValue),
        Value::String(s) => Some(any_value::Value::StringValue(s.clone())),
        other => Some(any_value::Value::StringValue(other.to_string())),
    };
    AnyValue { value }
}

/// The Tempo type name of a tag value.
fn value_type(value: &Value) -> &'static str {
    match value {
        Value::Bool(_) => "bool",
        Value::Number(n) if n.is_i64() => "int",
        Value::Number(_) => "float",
        _ => "string",
    }
}

/// The attribute path a `TraceQL` name reads: `span.x` is `attr.x`,
/// `resource.x` is `resource.x`, `.x` is both in that order.
fn paths_of(name: &str) -> Vec<String> {
    if let Some(rest) = name.strip_prefix("span.") {
        vec![format!("attr.{rest}")]
    } else if let Some(rest) = name.strip_prefix("resource.") {
        vec![format!("resource.{rest}")]
    } else if let Some(rest) = name.strip_prefix("instrumentation.") {
        vec![format!("scope.{rest}")]
    } else if let Some(rest) = name.strip_prefix('.') {
        vec![format!("attr.{rest}"), format!("resource.{rest}")]
    } else {
        Vec::new()
    }
}

// ---- search ---------------------------------------------------------

async fn search(
    State(state): State<AppState>,
    parts: Parts,
    Query(params): Query<Params>,
) -> Result<Response, ApiError> {
    let scope = scope(&state, &parts, Need::Read).await?;
    let _turn = state.admit_query(&scope.tenant).await?;
    let now = state.env.clock.now();
    let (start, end) = time_range(&params, now)?;
    let query = params.q.clone().filter(|q| !q.trim().is_empty());
    let query = query.unwrap_or_else(|| "{ }".to_owned());
    let lowered = lower(
        &query,
        &scope,
        RangeParams {
            start,
            end,
            step: Duration::ZERO,
        },
        now,
    )?;
    if lowered.kind != ResultKind::Spansets {
        return Err(ApiError::bad_request(
            "a search needs a spanset query, not a metrics query",
        ));
    }
    let batches = run(&state, &scope, lowered.plan).await?;
    let traces = traces(
        &batches,
        &lowered.shown,
        params.limit.unwrap_or(DEFAULT_LIMIT).max(1),
        params.spss.unwrap_or(DEFAULT_SPANS_PER_SPANSET).max(1),
    );
    Ok(Json(json!({ "traces": traces, "metrics": {} })).into_response())
}

/// One matched span of a search result.
struct MatchedSpan {
    id: Vec<u8>,
    name: Option<String>,
    start: i64,
    duration: i64,
    attributes: Vec<Value>,
}

/// One trace of a search result, with its spansets in order.
struct MatchedTrace {
    id: Vec<u8>,
    start: i64,
    duration: i64,
    root_name: Option<String>,
    root_service: Option<String>,
    spansets: BTreeMap<i64, (Option<String>, i64, Vec<MatchedSpan>)>,
}

/// The shown values of a span, as OTLP key-values: intrinsics from the
/// columns, attributes from `overflow`, the merged attribute view of
/// the span's batch.
fn shown_attributes(
    batch: &RecordBatch,
    overflow: Option<&StringArray>,
    i: usize,
    shown: &[String],
) -> Vec<Value> {
    let attributes = attributes(cell(overflow, i));
    let mut out = Vec::new();
    for name in shown {
        let value = match name.as_str() {
            "name" | "span:name" => cell(text(batch, NAME), i).map(|v| json!(v)),
            "status" | "span:status" => cell(text(batch, STATUS_CODE), i).map(|v| json!(v)),
            "statusMessage" | "span:statusMessage" => {
                cell(text(batch, STATUS_MESSAGE), i).map(|v| json!(v))
            }
            "kind" | "span:kind" => cell(text(batch, SPAN_KIND), i).map(|v| json!(v)),
            "duration" | "span:duration" => ints(batch, DURATION_NS)
                .filter(|d| !d.is_null(i))
                .map(|d| json!(d.value(i))),
            "rootName" | "trace:rootName" => cell(text(batch, ROOT_NAME), i).map(|v| json!(v)),
            "rootServiceName" | "trace:rootService" => {
                cell(text(batch, ROOT_SERVICE), i).map(|v| json!(v))
            }
            "traceDuration" | "trace:duration" => ints(batch, TRACE_DURATION_NS)
                .filter(|d| !d.is_null(i))
                .map(|d| json!(d.value(i))),
            other => paths_of(other)
                .iter()
                .find_map(|path| attributes.get(path).cloned()),
        };
        if let Some(value) = value {
            // Tempo writes an attribute under its own name, scope aside.
            let key = name
                .strip_prefix("span.")
                .or_else(|| name.strip_prefix("resource."))
                .or_else(|| name.strip_prefix("instrumentation."))
                .or_else(|| name.strip_prefix('.'))
                .unwrap_or(name);
            out.push(json!({ "key": key, "value": any_value_json(&value) }));
        }
    }
    out
}

/// The traces of trace-group rows, newest first, at most `limit` of
/// them with at most `spss` spans per spanset, as the search response
/// lists them.
fn traces(batches: &[RecordBatch], shown: &[String], limit: usize, spss: usize) -> Vec<Value> {
    let mut by_trace: BTreeMap<Vec<u8>, MatchedTrace> = BTreeMap::new();
    for batch in batches {
        let (Some(trace_ids), Some(span_ids), Some(starts), Some(trace_starts)) = (
            fixed(batch, TRACE_ID),
            fixed(batch, SPAN_ID),
            times(batch, TIMESTAMP),
            times(batch, TRACE_START),
        ) else {
            continue;
        };
        let durations = ints(batch, DURATION_NS);
        let trace_durations = ints(batch, TRACE_DURATION_NS);
        let spansets = ints(batch, SPANSET);
        let matched = ints(batch, MATCHED);
        let labels = text(batch, SPANSET_LABELS);
        let merged = attributes_column(batch);
        for i in 0..batch.num_rows() {
            let trace = by_trace
                .entry(trace_ids.value(i).to_vec())
                .or_insert_with(|| MatchedTrace {
                    id: trace_ids.value(i).to_vec(),
                    start: trace_starts.value(i),
                    duration: trace_durations.map_or(0, |d| d.value(i)),
                    root_name: cell(text(batch, ROOT_NAME), i).map(str::to_owned),
                    root_service: cell(text(batch, ROOT_SERVICE), i).map(str::to_owned),
                    spansets: BTreeMap::new(),
                });
            let index = spansets.map_or(0, |s| s.value(i));
            let spanset = trace.spansets.entry(index).or_insert_with(|| {
                (
                    cell(labels, i).map(str::to_owned),
                    matched.map_or(0, |m| m.value(i)),
                    Vec::new(),
                )
            });
            spanset.2.push(MatchedSpan {
                id: span_ids.value(i).to_vec(),
                name: cell(text(batch, NAME), i).map(str::to_owned),
                start: starts.value(i),
                duration: durations
                    .filter(|d| !d.is_null(i))
                    .map_or(0, |d| d.value(i)),
                attributes: shown_attributes(batch, merged.as_ref(), i, shown),
            });
        }
    }
    let mut traces: Vec<MatchedTrace> = by_trace.into_values().collect();
    traces.sort_by(|a, b| b.start.cmp(&a.start).then_with(|| a.id.cmp(&b.id)));
    traces
        .into_iter()
        .take(limit)
        .map(|trace| {
            let spansets: Vec<Value> = trace
                .spansets
                .into_values()
                .map(|(labels, matched, mut spans)| {
                    spans.sort_by_key(|s| s.start);
                    let attributes: Vec<Value> = labels
                        .and_then(|l| serde_json::from_str::<BTreeMap<String, String>>(&l).ok())
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(key, value)| json!({ "key": key, "value": { "stringValue": value } }))
                        .collect();
                    json!({
                        "spans": spans.iter().take(spss).map(|span| json!({
                            "spanID": hex(&span.id),
                            "name": span.name,
                            "startTimeUnixNano": span.start.to_string(),
                            "durationNanos": span.duration.to_string(),
                            "attributes": span.attributes,
                        })).collect::<Vec<_>>(),
                        "matched": matched,
                        "attributes": attributes,
                    })
                })
                .collect();
            json!({
                "traceID": hex(&trace.id),
                "rootServiceName": trace.root_service.unwrap_or_else(|| ROOT_NOT_RECEIVED.to_owned()),
                "rootTraceName": trace.root_name.unwrap_or_default(),
                "startTimeUnixNano": trace.start.to_string(),
                "durationMs": trace.duration / 1_000_000,
                "spanSet": spansets.first().cloned().unwrap_or(Value::Null),
                "spanSets": spansets,
            })
        })
        .collect()
}

// ---- trace by id ----------------------------------------------------

/// A trace id in hex, 32 digits or fewer padded on the left as Tempo
/// pads them.
fn parse_trace_id(text: &str) -> Result<Vec<u8>, ApiError> {
    let digits = text.trim();
    if digits.is_empty() || digits.len() > 32 || !digits.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(ApiError::bad_request(format!("invalid trace id {text:?}")));
    }
    let padded = format!("{digits:0>32}");
    (0..16)
        .map(|i| u8::from_str_radix(&padded[2 * i..2 * i + 2], 16))
        .collect::<Result<Vec<u8>, _>>()
        .map_err(|_| ApiError::bad_request(format!("invalid trace id {text:?}")))
}

/// Every span and span event of trace `id` in `range`, as OTLP
/// resource spans; `None` when the trace has no span.
async fn load_trace(
    state: &AppState,
    scope: &Scope,
    id: &[u8],
    range: TimeRange,
) -> Result<Option<TracesData>, ApiError> {
    let by_id =
        Expr::column(TRACE_ID).binary(BinaryOp::Eq, Expr::Literal(Literal::Bytes(id.to_vec())));
    let mut spans = Scan::all(scope.tenant.clone(), scope.dataset.clone(), Kind::Span);
    spans.time_range = range;
    spans.predicates = vec![by_id.clone()];
    let span_rows = run(
        state,
        scope,
        Plan::Scan(spans).sort(vec![SortKey::ascending(Expr::column(TIMESTAMP))]),
    )
    .await?;
    if span_rows.iter().all(|b| b.num_rows() == 0) {
        return Ok(None);
    }
    let mut events = Scan::all(scope.tenant.clone(), scope.dataset.clone(), Kind::Event);
    events.time_range = range;
    events.predicates = vec![by_id];
    let event_rows = run(state, scope, Plan::Scan(events)).await?;
    let mut events_by_span: BTreeMap<Vec<u8>, Vec<Event>> = BTreeMap::new();
    for batch in &event_rows {
        let (Some(ids), Some(starts)) = (fixed(batch, SPAN_ID), times(batch, TIMESTAMP)) else {
            continue;
        };
        let merged = attributes_column(batch);
        for i in 0..batch.num_rows() {
            if ids.is_null(i) {
                continue;
            }
            let attributes = attributes(cell(merged.as_ref(), i));
            events_by_span
                .entry(ids.value(i).to_vec())
                .or_default()
                .push(Event {
                    time_unix_nano: u64::try_from(starts.value(i)).unwrap_or(0),
                    name: cell(text(batch, NAME), i).unwrap_or_default().to_owned(),
                    attributes: key_values(&attributes, "attr."),
                    dropped_attributes_count: 0,
                });
        }
    }
    Ok(Some(traces_data(&span_rows, id, &mut events_by_span)))
}

/// The attributes under `prefix`, as OTLP key-values without it.
fn key_values(attributes: &BTreeMap<String, Value>, prefix: &str) -> Vec<KeyValue> {
    attributes
        .iter()
        .filter_map(|(path, value)| {
            path.strip_prefix(prefix).map(|key| KeyValue {
                key: key.to_owned(),
                value: Some(any_value(value)),
                ..Default::default()
            })
        })
        .collect()
}

/// Resource spans under construction, keyed by the JSON of the
/// resource attributes, then of the scope's.
type Grouped = BTreeMap<
    String,
    (
        Resource,
        BTreeMap<String, (InstrumentationScope, Vec<Span>)>,
    ),
>;

/// The JSON of the attributes under `prefix`, as a grouping key.
fn group_key(attributes: &BTreeMap<String, Value>, prefix: &str) -> String {
    let under: BTreeMap<&String, &Value> = attributes
        .iter()
        .filter(|(path, _)| path.starts_with(prefix))
        .collect();
    serde_json::to_string(&under).unwrap_or_default()
}

/// The instrumentation scope of a row's attributes.
fn scope_of(attributes: &BTreeMap<String, Value>) -> InstrumentationScope {
    let text = |key: &str| {
        attributes
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    InstrumentationScope {
        name: text("scope.name"),
        version: text("scope.version"),
        attributes: attributes
            .iter()
            .filter(|(path, _)| {
                path.starts_with("scope.") && *path != "scope.name" && *path != "scope.version"
            })
            .map(|(path, value)| KeyValue {
                key: path["scope.".len()..].to_owned(),
                value: Some(any_value(value)),
                ..Default::default()
            })
            .collect(),
        dropped_attributes_count: 0,
    }
}

/// Row `i` of `batch` as an OTLP span of `trace_id`, with its events.
fn span_of(
    batch: &RecordBatch,
    i: usize,
    trace_id: &[u8],
    attributes: &BTreeMap<String, Value>,
    events: Vec<Event>,
) -> Span {
    let start = times(batch, TIMESTAMP).map_or(0, |t| u64::try_from(t.value(i)).unwrap_or(0));
    let duration = ints(batch, DURATION_NS)
        .filter(|d| !d.is_null(i))
        .map_or(0, |d| u64::try_from(d.value(i)).unwrap_or(0));
    let status_code = cell(text(batch, STATUS_CODE), i)
        .and_then(tessera_types::StatusCode::parse)
        .unwrap_or_default();
    Span {
        trace_id: trace_id.to_vec(),
        span_id: fixed(batch, SPAN_ID).map_or_else(Vec::new, |s| s.value(i).to_vec()),
        parent_span_id: fixed(batch, PARENT_SPAN_ID)
            .filter(|p| !p.is_null(i))
            .map(|p| p.value(i).to_vec())
            .unwrap_or_default(),
        name: cell(text(batch, NAME), i).unwrap_or_default().to_owned(),
        kind: cell(text(batch, SPAN_KIND), i)
            .and_then(tessera_types::SpanKind::parse)
            .map_or(0, |k| i32::try_from(k as usize).unwrap_or(0)),
        start_time_unix_nano: start,
        end_time_unix_nano: start.saturating_add(duration),
        attributes: key_values(attributes, "attr."),
        events,
        links: links(cell(text(batch, LINKS), i)),
        status: Some(Status {
            message: cell(text(batch, STATUS_MESSAGE), i)
                .unwrap_or_default()
                .to_owned(),
            code: i32::try_from(status_code as usize).unwrap_or(0),
        }),
        ..Default::default()
    }
}

/// Span rows grouped by resource and scope into OTLP resource spans.
fn traces_data(
    batches: &[RecordBatch],
    trace_id: &[u8],
    events_by_span: &mut BTreeMap<Vec<u8>, Vec<Event>>,
) -> TracesData {
    let mut grouped: Grouped = BTreeMap::new();
    for batch in batches {
        let Some(ids) = fixed(batch, SPAN_ID) else {
            continue;
        };
        let merged = attributes_column(batch);
        for i in 0..batch.num_rows() {
            let attributes = attributes(cell(merged.as_ref(), i));
            let (_, scopes) = grouped
                .entry(group_key(&attributes, "resource."))
                .or_insert_with(|| {
                    (
                        Resource {
                            attributes: key_values(&attributes, "resource."),
                            ..Default::default()
                        },
                        BTreeMap::new(),
                    )
                });
            let (_, spans) = scopes
                .entry(group_key(&attributes, "scope."))
                .or_insert_with(|| (scope_of(&attributes), Vec::new()));
            let events = events_by_span.remove(ids.value(i)).unwrap_or_default();
            spans.push(span_of(batch, i, trace_id, &attributes, events));
        }
    }
    TracesData {
        resource_spans: grouped
            .into_values()
            .map(|(resource, scopes)| ResourceSpans {
                resource: Some(resource),
                scope_spans: scopes
                    .into_values()
                    .map(|(scope, spans)| ScopeSpans {
                        scope: Some(scope),
                        spans,
                        schema_url: String::new(),
                    })
                    .collect(),
                schema_url: String::new(),
            })
            .collect(),
    }
}

/// The links of a span from the JSON of its `links` column (DM-9).
fn links(column: Option<&str>) -> Vec<Link> {
    let Some(Value::Array(list)) = column.and_then(|l| serde_json::from_str::<Value>(l).ok())
    else {
        return Vec::new();
    };
    list.iter()
        .map(|link| {
            let from_hex = |key: &str| -> Vec<u8> {
                let text = link.get(key).and_then(Value::as_str).unwrap_or_default();
                (0..text.len() / 2)
                    .filter_map(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16).ok())
                    .collect()
            };
            let attributes: BTreeMap<String, Value> = match link.get("attributes") {
                Some(Value::Object(object)) => object.clone().into_iter().collect(),
                _ => BTreeMap::new(),
            };
            Link {
                trace_id: from_hex("trace_id"),
                span_id: from_hex("span_id"),
                attributes: key_values(&attributes, ""),
                ..Default::default()
            }
        })
        .collect()
}

fn wants_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|accept| accept.contains("protobuf"))
}

/// The trace by id of `id` in the request's range.
async fn trace_response(
    state: &AppState,
    parts: &Parts,
    id: &str,
    params: &Params,
) -> Result<Option<TracesData>, ApiError> {
    let scope = scope(state, parts, Need::Read).await?;
    let _turn = state.admit_query(&scope.tenant).await?;
    let id = parse_trace_id(id)?;
    let now = state.env.clock.now();
    let range = if params.start.is_some() || params.end.is_some() {
        let (start, end) = time_range(params, now)?;
        TimeRange::new(
            start,
            Timestamp::from_nanos(end.as_nanos().saturating_add(1)),
        )
    } else {
        TimeRange::ALL
    };
    load_trace(state, &scope, &id, range).await
}

async fn trace_by_id(
    State(state): State<AppState>,
    parts: Parts,
    Path(id): Path<String>,
    Query(params): Query<Params>,
) -> Result<Response, ApiError> {
    let Some(trace) = trace_response(&state, &parts, &id, &params).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    Ok(if wants_protobuf(&parts.headers) {
        protobuf(trace.encode_to_vec())
    } else {
        Json(
            json!({ "batches": serde_json::to_value(&trace).unwrap_or_default()["resourceSpans"] }),
        )
        .into_response()
    })
}

async fn trace_by_id_v2(
    State(state): State<AppState>,
    parts: Parts,
    Path(id): Path<String>,
    Query(params): Query<Params>,
) -> Result<Response, ApiError> {
    let Some(trace) = trace_response(&state, &parts, &id, &params).await? else {
        return Ok(StatusCode::NOT_FOUND.into_response());
    };
    Ok(if wants_protobuf(&parts.headers) {
        // Tempo's TraceByIDResponse: field 1 the trace, field 2 the status
        // (0, complete).
        let inner = trace.encode_to_vec();
        let mut out = Vec::with_capacity(inner.len() + 12);
        prost::encoding::encode_key(1, prost::encoding::WireType::LengthDelimited, &mut out);
        prost::encoding::encode_varint(inner.len() as u64, &mut out);
        out.extend_from_slice(&inner);
        protobuf(out)
    } else {
        Json(json!({
            "trace": serde_json::to_value(&trace).unwrap_or_default(),
            "status": "COMPLETE",
            "message": "",
        }))
        .into_response()
    })
}

fn protobuf(bytes: Vec<u8>) -> Response {
    (
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/protobuf"),
        )],
        bytes,
    )
        .into_response()
}

// ---- tags -----------------------------------------------------------

/// The span rows the tag endpoints look at: the spans a `q` query
/// matches, else the newest spans of the range.
async fn discovery_rows(
    state: &AppState,
    parts: &Parts,
    params: &Params,
) -> Result<Vec<RecordBatch>, ApiError> {
    let scope = scope(state, parts, Need::Read).await?;
    let _turn = state.admit_query(&scope.tenant).await?;
    let now = state.env.clock.now();
    let (start, end) = time_range(params, now)?;
    let Some(query) = params.q.as_deref().filter(|q| !q.trim().is_empty()) else {
        let mut scan = Scan::all(scope.tenant.clone(), scope.dataset.clone(), Kind::Span);
        scan.time_range = TimeRange::new(
            start,
            Timestamp::from_nanos(end.as_nanos().saturating_add(1)),
        );
        return run(
            state,
            &scope,
            Plan::Scan(scan)
                .sort(vec![SortKey::descending(Expr::column(TIMESTAMP))])
                .limit(0, Some(DISCOVERY_ROWS)),
        )
        .await;
    };
    let lowered = lower(
        query,
        &scope,
        RangeParams {
            start,
            end,
            step: Duration::ZERO,
        },
        now,
    )?;
    if lowered.kind != ResultKind::Spansets {
        return Err(ApiError::bad_request(
            "tag discovery takes a spanset query, not a metrics query",
        ));
    }
    run(state, &scope, lowered.plan).await
}

/// The attribute names of `batches` by scope.
fn tag_names(batches: &[RecordBatch]) -> BTreeMap<&'static str, BTreeSet<String>> {
    let mut names: BTreeMap<&'static str, BTreeSet<String>> = BTreeMap::new();
    for batch in batches {
        let merged = attributes_column(batch);
        let overflow = merged.as_ref();
        for i in 0..batch.num_rows() {
            for path in attributes(cell(overflow, i)).keys() {
                let (scope, name) = if let Some(name) = path.strip_prefix("attr.") {
                    ("span", name)
                } else if let Some(name) = path.strip_prefix("resource.") {
                    ("resource", name)
                } else if let Some(name) = path.strip_prefix("scope.") {
                    ("instrumentation", name)
                } else {
                    continue;
                };
                names.entry(scope).or_default().insert(name.to_owned());
            }
        }
    }
    names
}

/// The distinct values of the tag `name` in `batches`: an intrinsic by
/// name, or an attribute under the `TraceQL` scopes.
fn tag_values(batches: &[RecordBatch], name: &str) -> BTreeSet<(String, String)> {
    let mut values = BTreeSet::new();
    // Tempo lists the whole enumeration of a status or kind, whatever
    // the spans hold.
    match name {
        "status" | "span:status" => {
            return tessera_types::StatusCode::ALL
                .iter()
                .map(|s| ("keyword".to_owned(), s.as_str().to_owned()))
                .collect();
        }
        "kind" | "span:kind" => {
            return tessera_types::SpanKind::ALL
                .iter()
                .map(|k| ("keyword".to_owned(), k.as_str().to_owned()))
                .collect();
        }
        _ => {}
    }
    let paths = paths_of(name);
    for batch in batches {
        let merged = attributes_column(batch);
        let overflow = merged.as_ref();
        let parents = fixed(batch, PARENT_SPAN_ID);
        for i in 0..batch.num_rows() {
            let is_root = parents.is_none_or(|p| p.is_null(i));
            let column = match name {
                "name" | "span:name" => {
                    cell(text(batch, NAME), i).map(|v| ("string", v.to_owned()))
                }
                "statusMessage" | "span:statusMessage" => {
                    cell(text(batch, STATUS_MESSAGE), i).map(|v| ("string", v.to_owned()))
                }
                "rootName" | "trace:rootName" if is_root => {
                    cell(text(batch, NAME), i).map(|v| ("string", v.to_owned()))
                }
                "rootServiceName" | "trace:rootService" if is_root => attributes(cell(overflow, i))
                    .get("resource.service.name")
                    .and_then(Value::as_str)
                    .map(|v| ("string", v.to_owned())),
                _ => None,
            };
            if let Some((kind, value)) = column {
                values.insert((kind.to_owned(), value));
                continue;
            }
            if paths.is_empty() {
                continue;
            }
            let attributes = attributes(cell(overflow, i));
            for path in &paths {
                if let Some(value) = attributes.get(path) {
                    let text = match value {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    values.insert((value_type(value).to_owned(), text));
                }
            }
        }
    }
    values
}

async fn tags_v1(
    State(state): State<AppState>,
    parts: Parts,
    Query(params): Query<Params>,
) -> Result<Response, ApiError> {
    let batches = discovery_rows(&state, &parts, &params).await?;
    let names: BTreeSet<String> = tag_names(&batches).into_values().flatten().collect();
    Ok(Json(json!({ "tagNames": names })).into_response())
}

async fn tag_values_v1(
    State(state): State<AppState>,
    parts: Parts,
    Path(tag): Path<String>,
    Query(params): Query<Params>,
) -> Result<Response, ApiError> {
    let batches = discovery_rows(&state, &parts, &params).await?;
    // An unscoped name reads both the span's and the resource's attributes.
    let name = if tag.contains(':') || INTRINSICS.contains(&tag.as_str()) {
        tag
    } else {
        format!(".{tag}")
    };
    let values: BTreeSet<String> = tag_values(&batches, &name)
        .into_iter()
        .map(|(_, value)| value)
        .collect();
    Ok(Json(json!({ "tagValues": values })).into_response())
}

async fn tags_v2(
    State(state): State<AppState>,
    parts: Parts,
    Query(params): Query<Params>,
) -> Result<Response, ApiError> {
    let batches = discovery_rows(&state, &parts, &params).await?;
    let mut names = tag_names(&batches);
    names.insert(
        "intrinsic",
        INTRINSICS.iter().map(|s| (*s).to_owned()).collect(),
    );
    let wanted = params
        .scope
        .as_deref()
        .filter(|s| !s.is_empty() && *s != "none");
    let scopes: Vec<Value> = ["resource", "span", "instrumentation", "intrinsic"]
        .iter()
        .filter(|scope| wanted.is_none_or(|w| w == **scope))
        .map(|scope| {
            json!({
                "name": scope,
                "tags": names.get(scope).cloned().unwrap_or_default(),
            })
        })
        .collect();
    Ok(Json(json!({ "scopes": scopes })).into_response())
}

async fn tag_values_v2(
    State(state): State<AppState>,
    parts: Parts,
    Path(tag): Path<String>,
    Query(params): Query<Params>,
) -> Result<Response, ApiError> {
    let batches = discovery_rows(&state, &parts, &params).await?;
    let values: Vec<Value> = tag_values(&batches, &tag)
        .into_iter()
        .map(|(kind, value)| json!({ "type": kind, "value": value }))
        .collect();
    Ok(Json(json!({ "tagValues": values })).into_response())
}

// ---- metrics --------------------------------------------------------

/// One series of a metrics result: its labels and its samples by
/// bucket start.
#[derive(Default)]
struct Series {
    samples: BTreeMap<i64, f64>,
}

/// The series of metrics rows, keyed by their label values in `labels`
/// order.
fn series(batches: &[RecordBatch], labels: &[String]) -> BTreeMap<Vec<Option<String>>, Series> {
    let mut out: BTreeMap<Vec<Option<String>>, Series> = BTreeMap::new();
    for batch in batches {
        let (Some(timestamps), Some(values)) = (
            times(batch, TIMESTAMP),
            batch
                .column_by_name(VALUE)
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>()),
        ) else {
            continue;
        };
        let columns: Vec<Option<&StringArray>> = labels.iter().map(|l| text(batch, l)).collect();
        for i in 0..batch.num_rows() {
            if values.is_null(i) {
                continue;
            }
            let key: Vec<Option<String>> = columns
                .iter()
                .map(|c| cell(*c, i).map(str::to_owned))
                .collect();
            out.entry(key)
                .or_default()
                .samples
                .insert(timestamps.value(i), values.value(i));
        }
    }
    out
}

/// The labels of a series as Tempo writes them: a missing value is
/// `nil`, and a series without labels carries the function's name.
fn label_values(labels: &[String], key: &[Option<String>], function: &str) -> Vec<Value> {
    if labels.is_empty() {
        return vec![json!({ "key": "__name__", "value": { "stringValue": function } })];
    }
    labels
        .iter()
        .zip(key)
        .map(|(label, value)| {
            let value = value.as_deref().unwrap_or("nil");
            json!({ "key": label, "value": { "stringValue": value } })
        })
        .collect()
}

async fn metrics_common(
    state: &AppState,
    parts: &Parts,
    params: &Params,
    instant: bool,
) -> Result<Response, ApiError> {
    let scope = scope(state, parts, Need::Read).await?;
    let _turn = state.admit_query(&scope.tenant).await?;
    let now = state.env.clock.now();
    let (start, end) = time_range(params, now)?;
    let query = params
        .q
        .clone()
        .ok_or_else(|| ApiError::bad_request("q is required"))?;
    let step = if instant {
        Duration::ZERO
    } else {
        match &params.step {
            Some(step) => parse_step(step)?,
            None => default_step(start, end),
        }
    };
    let lowered = lower(&query, &scope, RangeParams { start, end, step }, now)?;
    if lowered.kind != ResultKind::Metrics {
        return Err(ApiError::bad_request(
            "a metrics query needs a metrics function such as rate() or count_over_time()",
        ));
    }
    let labels = lowered.labels.clone();
    let function = lowered.function.clone();
    let batches = run(state, &scope, lowered.plan).await?;
    let series = series(&batches, &labels);
    let out: Vec<Value> = if instant {
        series
            .iter()
            .map(|(key, series)| {
                json!({
                    "labels": label_values(&labels, key, &function),
                    "value": series.samples.values().last().copied().unwrap_or(0.0),
                })
            })
            .collect()
    } else {
        let step_nanos = i64::try_from(step.as_nanos()).unwrap_or(i64::MAX).max(1);
        let first = start.as_nanos().div_euclid(step_nanos) * step_nanos;
        let last = end.as_nanos().div_euclid(step_nanos) * step_nanos;
        series
            .iter()
            .map(|(key, series)| {
                let samples: Vec<Value> = (first..=last)
                    .step_by(usize::try_from(step_nanos).unwrap_or(usize::MAX))
                    .map(|bucket| {
                        json!({
                            "timestampMs": (bucket / 1_000_000).to_string(),
                            "value": series.samples.get(&bucket).copied().unwrap_or(0.0),
                        })
                    })
                    .collect();
                json!({ "labels": label_values(&labels, key, &function), "samples": samples })
            })
            .collect()
    };
    Ok(Json(json!({ "series": out, "metrics": {} })).into_response())
}

/// Tempo's default step: the range over 100 points, at least a second.
fn default_step(start: Timestamp, end: Timestamp) -> Duration {
    let span = end.as_nanos().saturating_sub(start.as_nanos()).max(0);
    Duration::from_nanos(u64::try_from(span / 100).unwrap_or(0)).max(Duration::from_secs(1))
}

async fn metrics_query_range(
    State(state): State<AppState>,
    parts: Parts,
    Query(params): Query<Params>,
) -> Result<Response, ApiError> {
    metrics_common(&state, &parts, &params, false).await
}

async fn metrics_query_instant(
    State(state): State<AppState>,
    parts: Parts,
    Query(params): Query<Params>,
) -> Result<Response, ApiError> {
    metrics_common(&state, &parts, &params, true).await
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::otlp::testing::{SpanSpec, export_spans};

    async fn call(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value)
    }

    fn get(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    /// Trace 1: `GET /` of `api` (100µs) with children `db query` of
    /// `db` (40µs, error) and `auth` of `auth`; trace 2: `GET /` of
    /// `api` (50µs) with `db query` of `db`, a second later.
    async fn pushed() -> Router {
        let app = crate::router(crate::testing::state().await);
        let spans = export_spans(&[
            SpanSpec {
                service: "api",
                trace: 1,
                id: 1,
                parent: None,
                name: "GET /",
                start: 0,
                duration: 100_000,
                error: false,
            },
            SpanSpec {
                service: "db",
                trace: 1,
                id: 2,
                parent: Some(1),
                name: "db query",
                start: 10_000,
                duration: 40_000,
                error: true,
            },
            SpanSpec {
                service: "auth",
                trace: 1,
                id: 3,
                parent: Some(1),
                name: "auth",
                start: 60_000,
                duration: 10_000,
                error: false,
            },
            SpanSpec {
                service: "api",
                trace: 2,
                id: 4,
                parent: None,
                name: "GET /",
                start: 1_000_000_000,
                duration: 50_000,
                error: false,
            },
            SpanSpec {
                service: "db",
                trace: 2,
                id: 5,
                parent: Some(4),
                name: "db query",
                start: 1_000_010_000,
                duration: 20_000,
                error: false,
            },
        ]);
        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&spans).unwrap()))
            .unwrap();
        let (status, _) = call(&app, request).await;
        assert_eq!(status, StatusCode::OK);
        app
    }

    const RANGE: &str = "start=1699999000&end=1700001000";

    /// QL-15: the `event.` scope matches spans by their events and the
    /// `link.` scope by their links.
    #[tokio::test]
    async fn event_and_link_scopes_search_spans() {
        let app = pushed().await;
        let search = |query: &str| {
            let encoded: String = form_urlencoded::byte_serialize(query.as_bytes()).collect();
            get(&format!("/api/search?q={encoded}&{RANGE}"))
        };
        let (status, value) = call(&app, search(r#"{ event:name = "exception" }"#)).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let traces = value["traces"].as_array().unwrap();
        assert_eq!(traces.len(), 1, "{value}");
        assert_eq!(traces[0]["spanSets"][0]["matched"], 1);
        assert_eq!(
            traces[0]["spanSets"][0]["spans"][0]["spanID"],
            "02".repeat(8)
        );
        let (status, value) = call(
            &app,
            search(r#"{ event.exception.message = "failed" && status = error }"#),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["traces"].as_array().unwrap().len(), 1, "{value}");
        let (status, value) = call(&app, search(r#"{ link.rel = "parent" }"#)).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let traces = value["traces"].as_array().unwrap();
        assert_eq!(traces.len(), 2, "children in both traces: {value}");
        let matched: u64 = traces
            .iter()
            .map(|t| t["spanSets"][0]["matched"].as_u64().unwrap())
            .sum();
        assert_eq!(matched, 3);
        let (status, value) = call(&app, search(r#"{ link:spanID = "0101010101010101" }"#)).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let traces = value["traces"].as_array().unwrap();
        assert_eq!(traces.len(), 1, "{value}");
        assert_eq!(traces[0]["spanSets"][0]["matched"], 2);
    }

    #[tokio::test]
    async fn search_returns_traces_newest_first_with_their_spansets() {
        let app = pushed().await;
        let (status, value) = call(
            &app,
            get(&format!(
                "/api/search?q=%7B%20status%20%3D%20error%20%7D&{RANGE}"
            )),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let traces = value["traces"].as_array().unwrap();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0]["traceID"], "01".repeat(16));
        assert_eq!(traces[0]["rootServiceName"], "api");
        assert_eq!(traces[0]["rootTraceName"], "GET /");
        assert_eq!(traces[0]["startTimeUnixNano"], "1700000000000000000");
        assert_eq!(traces[0]["durationMs"], 0);
        let spanset = &traces[0]["spanSets"][0];
        assert_eq!(spanset["matched"], 1);
        assert_eq!(spanset["spans"][0]["spanID"], "02".repeat(8));
        assert_eq!(spanset["spans"][0]["name"], "db query");
        assert_eq!(spanset["spans"][0]["durationNanos"], "40000");
        assert_eq!(
            spanset["spans"][0]["attributes"],
            json!([{ "key": "status", "value": { "stringValue": "error" } }])
        );

        let (_, value) = call(
            &app,
            get(&format!(
                "/api/search?q=%7B%20%7D%20%7C%20by(resource.service.name)&{RANGE}&limit=1&spss=1"
            )),
        )
        .await;
        let traces = value["traces"].as_array().unwrap();
        assert_eq!(traces.len(), 1, "limited to one trace");
        assert_eq!(traces[0]["traceID"], "02".repeat(16), "the newest");
        let spansets = traces[0]["spanSets"].as_array().unwrap();
        assert_eq!(spansets.len(), 2, "api and db");
        assert_eq!(
            spansets[0]["attributes"],
            json!([{ "key": "by(resource.service.name)", "value": { "stringValue": "api" } }])
        );

        let (_, value) = call(&app, get(&format!("/api/search?{RANGE}"))).await;
        assert_eq!(
            value["traces"].as_array().unwrap().len(),
            2,
            "no query lists every trace"
        );
        let (status, value) = call(
            &app,
            get(&format!("/api/search?q=%7B%20.a%20%3D%20%7D&{RANGE}")),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            value["error"]
                .as_str()
                .unwrap()
                .contains("expected a value"),
            "{value}"
        );
    }

    #[tokio::test]
    async fn traces_are_served_by_id_as_json_and_protobuf() {
        let app = pushed().await;
        let (status, value) = call(&app, get(&format!("/api/traces/{}", "01".repeat(16)))).await;
        assert_eq!(status, StatusCode::OK);
        let batches = value["batches"].as_array().unwrap();
        assert_eq!(batches.len(), 3, "one resource per service");
        let spans: Vec<&Value> = batches
            .iter()
            .flat_map(|b| b["scopeSpans"][0]["spans"].as_array().unwrap())
            .collect();
        assert_eq!(spans.len(), 3);
        let db = spans.iter().find(|s| s["name"] == "db query").unwrap();
        assert_eq!(db["traceId"], "01".repeat(16));
        assert_eq!(db["parentSpanId"], "01".repeat(8));
        assert_eq!(db["kind"], 2);
        assert_eq!(db["status"]["code"], 2);
        assert_eq!(db["status"]["message"], "failed");
        assert_eq!(db["startTimeUnixNano"], "1700000000000010000");
        assert_eq!(db["endTimeUnixNano"], "1700000000000050000");
        assert_eq!(db["events"][0]["name"], "exception");
        assert_eq!(db["attributes"][0]["key"], "n");

        let (status, value) = call(&app, get(&format!("/api/v2/traces/{}", "1".repeat(32)))).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{value}");
        let (status, value) = call(&app, get("/api/traces/xyz")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        let (status, value) = call(&app, get(&format!("/api/v2/traces/{}", "02".repeat(16)))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(value["status"], "COMPLETE");
        assert_eq!(value["trace"]["resourceSpans"].as_array().unwrap().len(), 2);

        let request = Request::builder()
            .uri(format!("/api/traces/{}", "01".repeat(16)))
            .header(header::ACCEPT, "application/protobuf")
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let decoded = TracesData::decode(bytes.as_ref()).unwrap();
        assert_eq!(decoded.resource_spans.len(), 3);
    }

    #[tokio::test]
    async fn tag_endpoints_list_names_and_values() {
        let app = pushed().await;
        let (_, value) = call(&app, get(&format!("/api/v2/search/tags?{RANGE}"))).await;
        let scopes = value["scopes"].as_array().unwrap();
        let find = |name: &str| {
            scopes
                .iter()
                .find(|s| s["name"] == name)
                .map_or(Value::Null, |s| s["tags"].clone())
        };
        assert_eq!(find("resource"), json!(["service.name"]));
        assert_eq!(find("span"), json!(["n"]));
        assert!(
            find("intrinsic")
                .as_array()
                .unwrap()
                .iter()
                .any(|t| t == "name")
        );
        let (_, value) = call(
            &app,
            get(&format!("/api/v2/search/tags?scope=span&{RANGE}")),
        )
        .await;
        assert_eq!(value["scopes"].as_array().unwrap().len(), 1);

        let (_, value) = call(
            &app,
            get(&format!(
                "/api/v2/search/tag/resource.service.name/values?{RANGE}"
            )),
        )
        .await;
        assert_eq!(
            value["tagValues"],
            json!([
                { "type": "string", "value": "api" },
                { "type": "string", "value": "auth" },
                { "type": "string", "value": "db" }
            ])
        );
        let (_, value) = call(
            &app,
            get(&format!(
                "/api/v2/search/tag/span.n/values?q=%7B%20status%20%3D%20error%20%7D&{RANGE}"
            )),
        )
        .await;
        assert_eq!(value["tagValues"], json!([{ "type": "int", "value": "1" }]));
        let (_, value) = call(
            &app,
            get(&format!("/api/v2/search/tag/status/values?{RANGE}")),
        )
        .await;
        assert_eq!(
            value["tagValues"],
            json!([
                { "type": "keyword", "value": "error" },
                { "type": "keyword", "value": "ok" },
                { "type": "keyword", "value": "unset" }
            ]),
            "the whole enumeration, as Tempo lists it"
        );
        let (_, value) = call(
            &app,
            get(&format!("/api/v2/search/tag/rootName/values?{RANGE}")),
        )
        .await;
        assert_eq!(
            value["tagValues"],
            json!([{ "type": "string", "value": "GET /" }])
        );

        let (_, value) = call(&app, get(&format!("/api/search/tags?{RANGE}"))).await;
        assert_eq!(value["tagNames"], json!(["n", "service.name"]));
        let (_, value) = call(
            &app,
            get(&format!("/api/search/tag/service.name/values?{RANGE}")),
        )
        .await;
        assert_eq!(value["tagValues"], json!(["api", "auth", "db"]));
    }

    /// QL-12: a quantile series carries its quantile as `p`, a
    /// histogram series its power-of-two bucket as `__bucket`, in
    /// seconds for durations.
    #[tokio::test]
    async fn quantile_and_histogram_metrics_label_their_series() {
        let app = pushed().await;
        let (status, value) = call(
            &app,
            get("/api/metrics/query?q=%7B%20%7D%20%7C%20quantile_over_time(duration,%200.5)&start=1700000000&end=1700000002"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let series = value["series"].as_array().unwrap();
        assert_eq!(series.len(), 1, "{value}");
        assert_eq!(
            series[0]["labels"][0],
            json!({ "key": "p", "value": { "stringValue": "0.5" } })
        );
        let median = series[0]["value"].as_f64().unwrap();
        assert!(
            (median - 4e-5).abs() < 1e-12,
            "the median of 10, 20, 40, 50 and 100 microseconds: {median}"
        );

        let (status, value) = call(
            &app,
            get("/api/metrics/query?q=%7B%20%7D%20%7C%20histogram_over_time(duration)&start=1700000000&end=1700000002"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let buckets: Vec<(f64, f64)> = value["series"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| {
                assert_eq!(s["labels"][0]["key"], "__bucket", "{s}");
                let bucket: f64 = s["labels"][0]["value"]["stringValue"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                (bucket, s["value"].as_f64().unwrap())
            })
            .collect();
        assert_eq!(buckets.len(), 4, "{buckets:?}");
        let shared = buckets
            .iter()
            .find(|(_, count)| (*count - 2.0).abs() < f64::EPSILON)
            .expect("40 and 50 microseconds share a bucket");
        assert!(
            (shared.0 - 65.536e-6).abs() < 1e-12,
            "the bucket above both, in seconds: {buckets:?}"
        );
    }

    #[tokio::test]
    async fn metrics_queries_return_series_per_step() {
        let app = pushed().await;
        let (status, value) = call(
            &app,
            get("/api/metrics/query_range?q=%7B%20%7D%20%7C%20count_over_time()%20by%20(resource.service.name)&start=1700000000&end=1700000002&step=1s"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let series = value["series"].as_array().unwrap();
        assert_eq!(series.len(), 3);
        let api = series
            .iter()
            .find(|s| s["labels"][0]["value"]["stringValue"] == "api")
            .unwrap();
        assert_eq!(api["labels"][0]["key"], "resource.service.name");
        assert_eq!(
            api["samples"],
            json!([
                { "timestampMs": "1700000000000", "value": 1.0 },
                { "timestampMs": "1700000001000", "value": 1.0 },
                { "timestampMs": "1700000002000", "value": 0.0 }
            ])
        );
        let (_, value) = call(
            &app,
            get("/api/metrics/query_range?q=%7B%20status%20%3D%20error%20%7D%20%7C%20rate()&start=1700000000&end=1700000001&step=1s"),
        )
        .await;
        assert_eq!(value["series"][0]["samples"][0]["value"], 1.0);
        assert_eq!(value["series"][0]["samples"][1]["value"], 0.0);
        let (status, value) = call(
            &app,
            get("/api/metrics/query?q=%7B%20%7D%20%7C%20max_over_time(duration)&start=1700000000&end=1700000002"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["series"][0]["value"], 0.0001, "seconds: {value}");
        assert_eq!(value["series"][0]["labels"][0]["key"], "__name__");
        let (status, _) = call(
            &app,
            get("/api/metrics/query_range?q=%7B%20%7D&start=1700000000&end=1700000002"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn echo_and_buildinfo_answer() {
        let app = crate::router(crate::testing::state().await);
        let response = app.clone().oneshot(get("/api/echo")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let (_, value) = call(&app, get("/api/status/buildinfo")).await;
        assert!(value["version"].is_string());
    }
}
