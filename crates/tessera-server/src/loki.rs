// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The Loki HTTP API (section 16): push, instant and range queries,
//! labels, label values, series, index statistics and volume, detected
//! fields and live tail, under `/loki/api/v1`. Grafana's Loki data
//! source works against it unmodified (AP-1).
//!
//! The tenant and dataset come from the headers the `params` module
//! reads (CT-5). A pushed stream's labels become record
//! attributes (`attr.<label>`), and every attribute of a stored row is a
//! label under the mapping of section 9.8, so a selector matches what
//! was pushed through any protocol (QL-8).
//!
//! Push accepts the JSON body; the protobuf body arrives with its own
//! commit.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use arrow::array::{Array, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, RawForm, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tessera_exec::QueryError;
use tessera_ir::{BindContext, Expr, Plan, Scan, SortKey};
use tessera_kernels::row_labels;
use tessera_logql::{LogqlFrontend, Lowered, RangeParams, ResultKind};
use tessera_opt::optimize;
use tessera_types::event::{BODY, EXTRACTED, OVERFLOW, SEVERITY_TEXT, TIMESTAMP, VALUE_LABELS};
use tessera_types::{EventBatch, Id128, Kind, LogRow, TimeRange, Timestamp};

use crate::params::{Need, Scope, parse_step, parse_time, scope};
use crate::{ApiError, AppState};

/// The Loki router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/ready", get(|| async { "ready" }))
        .route(
            "/loki/api/v1/status/buildinfo",
            get(|| async {
                Json(json!({"version": env!("CARGO_PKG_VERSION"), "goVersion": "none"}))
            }),
        )
        .route("/loki/api/v1/push", post(push))
        .route("/loki/api/v1/query", get(instant_query).post(instant_query))
        .route(
            "/loki/api/v1/query_range",
            get(range_query).post(range_query),
        )
        .route("/loki/api/v1/labels", get(labels))
        .route("/loki/api/v1/label", get(labels))
        .route("/loki/api/v1/label/{name}/values", get(label_values))
        .route("/loki/api/v1/series", get(series).post(series))
        .route("/loki/api/v1/index/stats", get(index_stats))
        .route("/loki/api/v1/index/volume", get(index_volume))
        .route("/loki/api/v1/detected_fields", get(detected_fields))
        .route("/loki/api/v1/detected_labels", get(detected_labels))
        .route("/loki/api/v1/tail", get(tail))
        .with_state(state)
}

/// How many rows the label-discovery endpoints look at, newest first.
const DISCOVERY_ROWS: usize = 50_000;
/// The default limit of a log query.
const DEFAULT_LIMIT: usize = 100;
/// How often tail polls for new rows.
const TAIL_INTERVAL: Duration = Duration::from_secs(1);

/// Parameters of the query endpoints; both GET and POST carry them as
/// form fields.
#[derive(Deserialize, Default)]
struct QueryParams {
    query: Option<String>,
    start: Option<String>,
    end: Option<String>,
    time: Option<String>,
    since: Option<String>,
    step: Option<String>,
    limit: Option<usize>,
    direction: Option<String>,
    #[serde(rename = "match[]")]
    matches: Option<String>,
    #[serde(rename = "targetLabels")]
    target_labels: Option<String>,
}

/// The time range of a request: `start` and `end`, `since`, or the last
/// hour, in that order of preference.
fn time_range(params: &QueryParams, now: Timestamp) -> Result<(Timestamp, Timestamp), ApiError> {
    let end = match &params.end {
        Some(end) => parse_time(end, now)?,
        None => now,
    };
    let start = match (&params.start, &params.since) {
        (Some(start), _) => parse_time(start, now)?,
        (None, Some(since)) => {
            let since = parse_step(since)?;
            Timestamp::from_nanos(
                end.as_nanos()
                    .saturating_sub(i64::try_from(since.as_nanos()).unwrap_or(i64::MAX)),
            )
        }
        (None, None) => Timestamp::from_nanos(end.as_nanos().saturating_sub(3_600_000_000_000)),
    };
    Ok((start, end))
}

/// Runs a lowered query, with the log direction and limit applied.
async fn run(
    state: &AppState,
    lowered: Lowered,
    limit: usize,
    backward: bool,
) -> Result<(Lowered, Vec<RecordBatch>), ApiError> {
    let plan = if lowered.kind == ResultKind::Streams {
        let key = if backward {
            SortKey::descending(Expr::column(TIMESTAMP))
        } else {
            SortKey::ascending(Expr::column(TIMESTAMP))
        };
        lowered.plan.clone().sort(vec![key]).limit(0, Some(limit))
    } else {
        lowered.plan.clone()
    };
    let plan = optimize(plan, state.engine.registry())
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
        "logql",
        "api",
        started.elapsed(),
    );
    Ok((lowered, output.batches))
}

fn lower(
    text: &str,
    scope: &Scope,
    params: RangeParams,
    now: Timestamp,
) -> Result<Lowered, ApiError> {
    let ctx = BindContext {
        tenant: scope.tenant.clone(),
        dataset: scope.dataset.clone(),
        now,
    };
    LogqlFrontend { params }
        .lower_query(text, &ctx)
        .map_err(|e| ApiError::query(QueryError::Syntax(e), text))
}

fn text<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

fn cell(column: Option<&StringArray>, i: usize) -> Option<&str> {
    column.filter(|c| !c.is_null(i)).map(|c| c.value(i))
}

/// The label set of every row of `batches`, with the row's timestamp and
/// line (for streams) or value (for metrics).
struct Row {
    labels: BTreeMap<String, String>,
    timestamp: i64,
    line: Option<String>,
    value: Option<f64>,
}

fn rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut out = Vec::new();
    for batch in batches {
        let Some(timestamps) = batch
            .column_by_name(TIMESTAMP)
            .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
        else {
            continue;
        };
        let (body, overflow, extracted, severity) = (
            text(batch, BODY),
            text(batch, OVERFLOW),
            text(batch, EXTRACTED),
            text(batch, SEVERITY_TEXT),
        );
        let values = batch
            .column_by_name(VALUE_LABELS)
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        for i in 0..batch.num_rows() {
            out.push(Row {
                labels: row_labels(cell(overflow, i), cell(extracted, i), cell(severity, i)),
                timestamp: timestamps.value(i),
                line: cell(body, i).map(str::to_owned),
                value: values.filter(|v| !v.is_null(i)).map(|v| v.value(i)),
            });
        }
    }
    out
}

/// Rows grouped by label set into Loki streams, entries in `backward`
/// or forward order.
fn streams(rows: Vec<Row>, backward: bool) -> Value {
    let mut by_labels: BTreeMap<BTreeMap<String, String>, Vec<(i64, String)>> = BTreeMap::new();
    for row in rows {
        by_labels
            .entry(row.labels)
            .or_default()
            .push((row.timestamp, row.line.unwrap_or_default()));
    }
    let result: Vec<Value> = by_labels
        .into_iter()
        .map(|(labels, mut entries)| {
            entries.sort_by_key(|(t, _)| if backward { -*t } else { *t });
            json!({
                "stream": labels,
                "values": entries.iter().map(|(t, line)| json!([t.to_string(), line])).collect::<Vec<_>>(),
            })
        })
        .collect();
    json!({ "resultType": "streams", "result": result, "stats": {} })
}

/// Metric rows grouped by label set into a matrix or a vector.
#[allow(clippy::cast_precision_loss)]
fn metric(rows: Vec<Row>, kind: ResultKind) -> Value {
    let mut by_labels: BTreeMap<BTreeMap<String, String>, Vec<(f64, f64)>> = BTreeMap::new();
    for row in rows {
        if let Some(value) = row.value {
            by_labels
                .entry(row.labels)
                .or_default()
                .push((row.timestamp as f64 / 1e9, value));
        }
    }
    let result: Vec<Value> = by_labels
        .into_iter()
        .map(|(labels, mut samples)| {
            samples.sort_by(|a, b| a.0.total_cmp(&b.0));
            if kind == ResultKind::Vector {
                let (t, v) = samples.last().copied().unwrap_or((0.0, f64::NAN));
                json!({ "metric": labels, "value": [t, v.to_string()] })
            } else {
                json!({
                    "metric": labels,
                    "values": samples.iter().map(|(t, v)| json!([t, v.to_string()])).collect::<Vec<_>>(),
                })
            }
        })
        .collect();
    let result_type = match kind {
        ResultKind::Vector => "vector",
        _ => "matrix",
    };
    json!({ "resultType": result_type, "result": result, "stats": {} })
}

fn success(data: Value) -> Response {
    let mut body = serde_json::Map::new();
    body.insert("status".into(), Value::String("success".into()));
    body.insert("data".into(), data);
    Json(Value::Object(body)).into_response()
}

async fn query_common(
    state: &AppState,
    headers: &HeaderMap,
    params: QueryParams,
    instant: bool,
) -> Result<Response, ApiError> {
    let scope = scope(state, headers, Need::Read).await?;
    let _turn = state.admit_query(&scope.tenant).await?;
    let now = state.env.clock.now();
    let query = params
        .query
        .clone()
        .ok_or_else(|| ApiError::bad_request("query is required"))?;
    let (start, end, step) = if instant {
        let time = match &params.time {
            Some(time) => parse_time(time, now)?,
            None => now,
        };
        (time, time, Duration::ZERO)
    } else {
        let (start, end) = time_range(&params, now)?;
        let step = match &params.step {
            Some(step) => parse_step(step)?,
            None => default_step(start, end),
        };
        (start, end, step)
    };
    let backward = params.direction.as_deref() != Some("forward");
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).max(1);
    let range = RangeParams { start, end, step };
    let lowered = if instant && !query.trim_start().starts_with('{') {
        lower(&query, &scope, range, now)?
    } else if instant {
        // An instant log query lists the newest lines up to `time`.
        lower(
            &query,
            &scope,
            RangeParams {
                start: Timestamp::MIN,
                end,
                step,
            },
            now,
        )?
    } else {
        lower(&query, &scope, range, now)?
    };
    let (lowered, batches) = run(state, lowered, limit, backward).await?;
    let data = match lowered.kind {
        ResultKind::Streams => streams(rows(&batches), backward),
        ResultKind::Scalar => {
            let value = rows(&batches)
                .first()
                .and_then(|r| r.value)
                .unwrap_or(f64::NAN);
            #[allow(clippy::cast_precision_loss)]
            let t = end.as_nanos() as f64 / 1e9;
            json!({ "resultType": "scalar", "result": [t, value.to_string()], "stats": {} })
        }
        kind => metric(rows(&batches), kind),
    };
    Ok(success(data))
}

/// Loki's default step: the range over 250 points, at least a second.
fn default_step(start: Timestamp, end: Timestamp) -> Duration {
    let span = end.as_nanos().saturating_sub(start.as_nanos()).max(0);
    Duration::from_nanos(u64::try_from(span / 250).unwrap_or(0)).max(Duration::from_secs(1))
}

async fn instant_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawForm(raw): RawForm,
) -> Result<Response, ApiError> {
    query_common(&state, &headers, params(&raw)?, true).await
}

async fn range_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawForm(raw): RawForm,
) -> Result<Response, ApiError> {
    query_common(&state, &headers, params(&raw)?, false).await
}

/// The parameters of a request: the query string of a GET or the form
/// body of a POST, which `RawForm` reads either way.
fn params(raw: &[u8]) -> Result<QueryParams, ApiError> {
    serde_urlencoded::from_bytes(raw)
        .map_err(|e| ApiError::bad_request(format!("invalid parameters: {e}")))
}

/// The rows the discovery endpoints look at: the newest rows of the
/// selector (or of everything) in the time range.
async fn discovery_rows(
    state: &AppState,
    headers: &HeaderMap,
    params: &QueryParams,
) -> Result<Vec<Row>, ApiError> {
    let scope = scope(state, headers, Need::Read).await?;
    let _turn = state.admit_query(&scope.tenant).await?;
    let now = state.env.clock.now();
    let (start, end) = time_range(params, now)?;
    let selector = params
        .matches
        .clone()
        .or_else(|| params.query.clone())
        .filter(|q| !q.trim().is_empty());
    let lowered = if let Some(selector) = selector {
        lower(
            &selector,
            &scope,
            RangeParams {
                start,
                end,
                step: Duration::ZERO,
            },
            now,
        )?
    } else {
        let mut scan = Scan::all(scope.tenant.clone(), scope.dataset.clone(), Kind::Log);
        scan.time_range = TimeRange::new(
            start,
            Timestamp::from_nanos(end.as_nanos().saturating_add(1)),
        );
        Lowered {
            plan: Plan::Scan(scan),
            kind: ResultKind::Streams,
        }
    };
    if lowered.kind != ResultKind::Streams {
        return Err(ApiError::bad_request(
            "a selector is needed here, not a metric query",
        ));
    }
    let (_, batches) = run(state, lowered, DISCOVERY_ROWS, true).await?;
    Ok(rows(&batches))
}

async fn labels(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Response, ApiError> {
    let names: BTreeSet<String> = discovery_rows(&state, &headers, &params)
        .await?
        .into_iter()
        .flat_map(|r| r.labels.into_keys())
        .collect();
    Ok(success(json!(names)))
}

async fn label_values(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(params): Query<QueryParams>,
) -> Result<Response, ApiError> {
    let values: BTreeSet<String> = discovery_rows(&state, &headers, &params)
        .await?
        .into_iter()
        .filter_map(|mut r| r.labels.remove(&name))
        .collect();
    Ok(success(json!(values)))
}

async fn series(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawForm(raw): RawForm,
) -> Result<Response, ApiError> {
    let params = params(&raw)?;
    let sets: BTreeSet<BTreeMap<String, String>> = discovery_rows(&state, &headers, &params)
        .await?
        .into_iter()
        .map(|r| r.labels)
        .collect();
    Ok(success(json!(sets)))
}

async fn index_stats(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Response, ApiError> {
    let rows = discovery_rows(&state, &headers, &params).await?;
    let streams: BTreeSet<&BTreeMap<String, String>> = rows.iter().map(|r| &r.labels).collect();
    let bytes: usize = rows
        .iter()
        .map(|r| r.line.as_ref().map_or(0, String::len))
        .sum();
    Ok(Json(json!({
        "streams": streams.len(),
        "chunks": streams.len(),
        "entries": rows.len(),
        "bytes": bytes,
    }))
    .into_response())
}

#[allow(clippy::cast_precision_loss)]
async fn index_volume(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Response, ApiError> {
    let now = state.env.clock.now();
    let (_, end) = time_range(&params, now)?;
    let targets: Option<Vec<String>> = params
        .target_labels
        .as_ref()
        .map(|t| t.split(',').map(|s| s.trim().to_owned()).collect());
    let rows = discovery_rows(&state, &headers, &params).await?;
    let mut volume: BTreeMap<BTreeMap<String, String>, usize> = BTreeMap::new();
    for row in rows {
        let key: BTreeMap<String, String> = match &targets {
            Some(targets) => row
                .labels
                .into_iter()
                .filter(|(k, _)| targets.contains(k))
                .collect(),
            None => row.labels,
        };
        *volume.entry(key).or_default() += row.line.as_ref().map_or(0, String::len);
    }
    let t = end.as_nanos() as f64 / 1e9;
    let result: Vec<Value> = volume
        .into_iter()
        .map(|(labels, bytes)| json!({ "metric": labels, "value": [t, bytes.to_string()] }))
        .collect();
    Ok(success(
        json!({ "resultType": "vector", "result": result, "stats": {} }),
    ))
}

async fn detected_fields(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Response, ApiError> {
    let rows = discovery_rows(&state, &headers, &params).await?;
    let mut values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for row in &rows {
        for (name, value) in &row.labels {
            values
                .entry(name.clone())
                .or_default()
                .insert(value.clone());
        }
    }
    let fields: Vec<Value> = values
        .into_iter()
        .map(|(label, values)| {
            let numeric = values.iter().all(|v| v.parse::<f64>().is_ok());
            json!({
                "label": label,
                "type": if numeric { "float" } else { "string" },
                "cardinality": values.len(),
                "parsers": Vec::<String>::new(),
            })
        })
        .collect();
    Ok(Json(json!({ "fields": fields, "limit": rows.len() })).into_response())
}

async fn detected_labels(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Response, ApiError> {
    let rows = discovery_rows(&state, &headers, &params).await?;
    let mut values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for row in &rows {
        for (name, value) in &row.labels {
            values
                .entry(name.clone())
                .or_default()
                .insert(value.clone());
        }
    }
    let labels: Vec<Value> = values
        .into_iter()
        .map(|(label, values)| json!({ "label": label, "cardinality": values.len() }))
        .collect();
    Ok(Json(json!({ "detectedLabels": labels })).into_response())
}

/// The push body: `{"streams": [{"stream": {labels}, "values": [[ts, line, meta?]]}]}`.
#[derive(Deserialize)]
struct PushBody {
    streams: Vec<PushStream>,
}

#[derive(Deserialize)]
struct PushStream {
    #[serde(default)]
    stream: BTreeMap<String, String>,
    #[serde(default)]
    values: Vec<Vec<Value>>,
}

async fn push(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, ApiError> {
    let scope = scope(&state, &headers, Need::Write).await?;
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !content_type.starts_with("application/json") {
        return Err(ApiError::unsupported_media_type(format!(
            "push accepts application/json, got {content_type:?}"
        )));
    }
    let body: PushBody = serde_json::from_slice(&body)
        .map_err(|e| ApiError::bad_request(format!("malformed push body: {e}")))?;
    let now = state.env.clock.now();
    let mut random = state.env.random.clone();
    let mut rows = Vec::new();
    for stream in body.streams {
        for entry in stream.values {
            let (Some(ts), Some(line)) = (
                entry.first().and_then(Value::as_str),
                entry.get(1).and_then(Value::as_str),
            ) else {
                return Err(ApiError::bad_request(
                    "each entry is [timestamp, line, metadata?]",
                ));
            };
            let timestamp = Timestamp::from_nanos(
                ts.parse::<i64>()
                    .map_err(|_| ApiError::bad_request(format!("invalid timestamp {ts:?}")))?,
            );
            let mut attributes = BTreeMap::new();
            for (name, value) in &stream.stream {
                attributes.insert(
                    format!("attr.{name}"),
                    tessera_types::Scalar::Text(value.clone()),
                );
            }
            if let Some(Value::Object(metadata)) = entry.get(2) {
                for (name, value) in metadata {
                    let text = value
                        .as_str()
                        .map_or_else(|| value.to_string(), str::to_owned);
                    attributes.insert(format!("attr.{name}"), tessera_types::Scalar::Text(text));
                }
            }
            let severity_text = stream
                .stream
                .get("level")
                .or_else(|| stream.stream.get("detected_level"))
                .cloned();
            rows.push(LogRow {
                timestamp,
                observed_timestamp: now,
                event_id: Id128::random(&mut random).as_u128().to_le_bytes(),
                severity_text,
                body: Some(line.to_owned()),
                attributes,
                ..LogRow::default()
            });
        }
    }
    if !rows.is_empty() {
        let batch = EventBatch::from_logs(scope.tenant, scope.dataset, &rows)
            .map_err(|e| ApiError::bad_request(e.to_string()))?;
        state.write(batch).await?;
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Live tail (QL-11): a WebSocket that sends every new stream entry of
/// the selector as it becomes visible.
async fn tail(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let scope = scope(&state, &headers, Need::Read).await?;
    let _turn = state.admit_query(&scope.tenant).await?;
    let query = params
        .query
        .clone()
        .ok_or_else(|| ApiError::bad_request("query is required"))?;
    let now = state.env.clock.now();
    let start = match &params.start {
        Some(start) => parse_time(start, now)?,
        None => now,
    };
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).max(1);
    Ok(upgrade.on_upgrade(move |socket| tail_loop(socket, state, scope, query, start, limit)))
}

async fn tail_loop(
    mut socket: WebSocket,
    state: AppState,
    scope: Scope,
    query: String,
    start: Timestamp,
    limit: usize,
) {
    let mut after = start;
    loop {
        let now = state.env.clock.now();
        let params = RangeParams {
            start: Timestamp::from_nanos(after.as_nanos().saturating_add(1)),
            end: now,
            step: Duration::ZERO,
        };
        let batches = match lower(&query, &scope, params, now) {
            Ok(lowered) => run(&state, lowered, limit, false).await.map(|(_, b)| b),
            Err(error) => Err(error),
        };
        match batches {
            Ok(batches) => {
                let rows = rows(&batches);
                if let Some(newest) = rows.iter().map(|r| r.timestamp).max() {
                    after = Timestamp::from_nanos(newest);
                    let payload = streams(rows, false);
                    let message = json!({ "streams": payload["result"] }).to_string();
                    if socket.send(Message::Text(message.into())).await.is_err() {
                        return;
                    }
                }
            }
            Err(error) => {
                let _ = socket
                    .send(Message::Text(
                        json!({ "error": error.message() }).to_string().into(),
                    ))
                    .await;
                return;
            }
        }
        tokio::select! {
            () = tokio::time::sleep(TAIL_INTERVAL) => {}
            message = socket.recv() => {
                if matches!(message, None | Some(Err(_) | Ok(Message::Close(_)))) {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, header};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    async fn call(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value)
    }

    async fn pushed() -> Router {
        let app = crate::router(crate::testing::state().await);
        let body = json!({
            "streams": [
                {"stream": {"app": "checkout", "level": "error"}, "values": [
                    ["1700000000000000000", "payment timeout", {"trace_id": "abc"}],
                    ["1700000002000000000", "{\"status\": 500, \"path\": \"/pay\"}", {"trace_id": "abc"}]
                ]},
                {"stream": {"app": "web", "level": "info"}, "values": [["1700000001000000000", "GET / ok"]]}
            ]
        });
        let request = Request::builder()
            .method("POST")
            .uri("/loki/api/v1/push")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let (status, _) = call(&app, request).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        app
    }

    fn get(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn pushed_streams_come_back_from_range_queries() {
        let app = pushed().await;
        let (status, value) = call(
            &app,
            get("/loki/api/v1/query_range?query=%7Bapp%3D%22checkout%22%7D%20%7C%3D%20%22timeout%22&start=1699999999000000000&end=1700000010000000000&limit=10"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["status"], "success");
        assert_eq!(value["data"]["resultType"], "streams");
        let result = value["data"]["result"].as_array().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["stream"]["app"], "checkout");
        assert_eq!(result[0]["stream"]["level"], "error");
        assert_eq!(result[0]["stream"]["trace_id"], "abc");
        assert_eq!(result[0]["values"][0][0], "1700000000000000000");
        assert_eq!(result[0]["values"][0][1], "payment timeout");

        // Newest first by default, forward on request.
        let (_, value) = call(&app, get("/loki/api/v1/query_range?query=%7Bapp%3D~%22.%2B%22%7D&start=1&end=1700000010000000000")).await;
        let result = value["data"]["result"].as_array().unwrap();
        let checkout = result
            .iter()
            .find(|s| s["stream"]["app"] == "checkout")
            .unwrap();
        assert_eq!(checkout["values"][0][0], "1700000002000000000");
        let (_, value) = call(&app, get("/loki/api/v1/query_range?query=%7Bapp%3D~%22.%2B%22%7D&start=1&end=1700000010000000000&direction=forward")).await;
        let result = value["data"]["result"].as_array().unwrap();
        let checkout = result
            .iter()
            .find(|s| s["stream"]["app"] == "checkout")
            .unwrap();
        assert_eq!(checkout["values"][0][0], "1700000000000000000");
    }

    #[tokio::test]
    async fn parser_stages_and_metric_queries() {
        let app = pushed().await;
        let (status, value) = call(
            &app,
            get("/loki/api/v1/query_range?query=%7Bapp%3D%22checkout%22%7D%20%7C%20json%20%7C%20status%20%3E%3D%20500&start=1&end=1700000010000000000"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let result = value["data"]["result"].as_array().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["stream"]["status"], "500");
        assert_eq!(result[0]["stream"]["path"], "/pay");

        let (status, value) = call(
            &app,
            get("/loki/api/v1/query_range?query=sum%20by%20(app)%20(count_over_time(%7Bapp%3D~%22.%2B%22%7D%5B20s%5D))&start=1700000000000000000&end=1700000010000000000&step=10s"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["data"]["resultType"], "matrix");
        let result = value["data"]["result"].as_array().unwrap();
        // Windows are (t - range, t]: at t0 only the first checkout line,
        // at t1 both; the web line only at t1.
        let checkout = result
            .iter()
            .find(|s| s["metric"]["app"] == "checkout")
            .unwrap();
        assert_eq!(
            checkout["values"],
            json!([[1_700_000_000.0, "1"], [1_700_000_010.0, "2"]])
        );
        let web = result.iter().find(|s| s["metric"]["app"] == "web").unwrap();
        assert_eq!(web["values"], json!([[1_700_000_010.0, "1"]]));

        let (status, value) = call(
            &app,
            get("/loki/api/v1/query?query=count_over_time(%7Bapp%3D%22web%22%7D%5B1h%5D)&time=1700000010000000000"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["data"]["resultType"], "vector");
        assert_eq!(value["data"]["result"][0]["value"][1], "1");
    }

    #[tokio::test]
    async fn discovery_endpoints_describe_the_labels() {
        let app = pushed().await;
        let (_, value) = call(
            &app,
            get("/loki/api/v1/labels?start=1&end=1700000010000000000"),
        )
        .await;
        assert_eq!(value["data"], json!(["app", "level", "trace_id"]));
        let (_, value) = call(
            &app,
            get("/loki/api/v1/label/app/values?start=1&end=1700000010000000000"),
        )
        .await;
        assert_eq!(value["data"], json!(["checkout", "web"]));
        let (_, value) = call(&app, get("/loki/api/v1/series?match%5B%5D=%7Bapp%3D%22web%22%7D&start=1&end=1700000010000000000")).await;
        assert_eq!(value["data"], json!([{"app": "web", "level": "info"}]));
        let (_, value) = call(&app, get("/loki/api/v1/index/stats?query=%7Bapp%3D~%22.%2B%22%7D&start=1&end=1700000010000000000")).await;
        assert_eq!(value["entries"], 3);
        assert_eq!(value["streams"], 2);
        let (_, value) = call(&app, get("/loki/api/v1/index/volume?query=%7Bapp%3D~%22.%2B%22%7D&start=1&end=1700000010000000000&targetLabels=app")).await;
        let result = value["data"]["result"].as_array().unwrap();
        assert_eq!(result.len(), 2);
        let (_, value) = call(&app, get("/loki/api/v1/detected_fields?query=%7Bapp%3D~%22.%2B%22%7D&start=1&end=1700000010000000000")).await;
        let fields = value["fields"].as_array().unwrap();
        assert!(
            fields
                .iter()
                .any(|f| f["label"] == "trace_id" && f["cardinality"] == 1)
        );
        let (status, _) = call(&app, get("/ready")).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn errors_and_tenants() {
        let app = pushed().await;
        let (status, value) = call(
            &app,
            get("/loki/api/v1/query_range?query=%7Bapp%3D&start=1&end=2"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{value}");
        let (status, _) = call(&app, get("/loki/api/v1/query_range?start=1&end=2")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let request = Request::builder()
            .uri("/loki/api/v1/labels?start=1&end=2")
            .header("x-scope-orgid", "nobody")
            .body(Body::empty())
            .unwrap();
        let (status, value) = call(&app, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(value["error"].as_str().unwrap().contains("unknown tenant"));
        let bad_push = Request::builder()
            .method("POST")
            .uri("/loki/api/v1/push")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{\"streams\": [{\"values\": [[\"x\"]]}]}"))
            .unwrap();
        let (status, _) = call(&app, bad_push).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
