// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The Prometheus HTTP API (section 16): remote write, instant and
//! range queries, series, labels, label values, metadata, readiness
//! and build information, under `/api/v1`. Grafana's Prometheus data
//! source works against it unmodified (AP-1).
//!
//! The tenant and dataset come from the headers the `params` module
//! reads (CT-5). Times are Unix seconds or RFC 3339, steps durations
//! or seconds, as Prometheus takes them. Errors take Prometheus' shape
//! (`status`, `errorType`, `error`). Rules and alerts are served by
//! the rules module; exemplars come from the trace ids the points
//! carry (section 16).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use arrow::array::{
    Array, FixedSizeBinaryArray, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, RawForm, State};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::{Value, json};
use tessera_exec::QueryError;
use tessera_ingest::remote_write;
use tessera_ir::{BindContext, Expr, NamedExpr, Plan, Span, VALUE};
use tessera_kernels::promql::format_value;
use tessera_kernels::{attributes_column, row_labels};
use tessera_opt::optimize;
use tessera_promql::ast::{Expr as PromAst, Matcher};
use tessera_promql::{Lowered, PromqlFrontend, RangeParams, ResultKind, parse, selector_plan};
use tessera_types::event::{EXTRACTED, SPAN_ID, TIMESTAMP, TRACE_ID};
use tessera_types::{TimeRange, Timestamp};

use crate::params::{Need, Scope, parse_step, parse_time, scope};
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
        .route("/api/v1/status/buildinfo", get(buildinfo))
        .route("/api/v1/write", post(write))
        // POST /api/v1/query belongs to the query module, which hands a
        // form body here.
        .route("/api/v1/query", get(instant_query))
        .route("/api/v1/query_range", get(range_query).post(range_query))
        .route("/api/v1/series", get(series).post(series))
        .route("/api/v1/labels", get(labels).post(labels))
        .route("/api/v1/label/{name}/values", get(label_values))
        .route("/api/v1/metadata", get(empty_object))
        .route(
            "/api/v1/query_exemplars",
            get(query_exemplars).post(query_exemplars),
        )
        .with_state(state)
}

/// How many rows the discovery endpoints look at per selector.
const DISCOVERY_ROWS: usize = 50_000;

async fn buildinfo() -> Response {
    success(&json!({
        "version": env!("CARGO_PKG_VERSION"),
        "revision": "",
        "branch": "",
        "buildUser": "",
        "buildDate": "",
        "goVersion": "none",
    }))
}

async fn empty_object() -> Response {
    success(&json!({}))
}

/// Exemplars (section 16): the points of the query's selectors that
/// carry a trace id within the range, as Prometheus lists them, one
/// entry per series. A point's exemplar is the trace and span ids its
/// producer attached (section 4.2).
async fn query_exemplars(
    State(state): State<AppState>,
    parts: Parts,
    RawForm(raw): RawForm,
) -> Response {
    answer(async {
        let params = Params::parse(&raw);
        let text = params
            .get("query")
            .filter(|q| !q.is_empty())
            .ok_or_else(|| ApiError::bad_request("invalid parameter \"query\": empty"))?
            .to_owned();
        let scope = scope(&state, &parts, Need::Read).await?;
        let _turn = state
            .admit_query(&scope.tenant, scope.principal.team())
            .await?;
        let now = state.env.clock.now();
        let end = params.time("end", now, now)?;
        let start = params.time(
            "start",
            Timestamp::from_nanos(end.as_nanos().saturating_sub(3_600_000_000_000)),
            now,
        )?;
        let range = TimeRange::new(
            start,
            Timestamp::from_nanos(end.as_nanos().saturating_add(1)),
        );
        let ctx = BindContext::new(scope.tenant.clone(), scope.dataset.clone(), now);
        let expr = parse(&text).map_err(|e| {
            ApiError::bad_request(format!(
                "invalid parameter \"query\": {}",
                e.describe(&text)
            ))
        })?;
        let mut selectors = Vec::new();
        selectors_of(&expr, &mut selectors);
        let mut by_series: BTreeMap<BTreeMap<String, String>, Vec<Value>> = BTreeMap::new();
        for (name, matchers, span) in selectors {
            let plan = selector_plan(&ctx, name, matchers, range, span)
                .map_err(|e| ApiError::bad_request(e.describe(&text)))?;
            let plan = with_exemplar_ids(plan).limit(0, Some(DISCOVERY_ROWS));
            let batches = run(&state, &scope, plan).await?;
            exemplars_of(&batches, &mut by_series);
        }
        Ok(json!(
            by_series
                .into_iter()
                .map(
                    |(labels, exemplars)| json!({ "seriesLabels": labels, "exemplars": exemplars })
                )
                .collect::<Vec<_>>()
        ))
    })
    .await
}

/// Every selector of `expr`: its name, matchers and position.
fn selectors_of<'a>(expr: &'a PromAst, into: &mut Vec<(Option<&'a str>, &'a [Matcher], Span)>) {
    match expr {
        PromAst::Selector {
            name,
            matchers,
            span,
            ..
        } => into.push((name.as_deref(), matchers, *span)),
        PromAst::Subquery { expr, .. } | PromAst::Unary { expr, .. } | PromAst::Paren(expr) => {
            selectors_of(expr, into);
        }
        PromAst::Binary { left, right, .. } => {
            selectors_of(left, into);
            selectors_of(right, into);
        }
        PromAst::Aggregate { expr, param, .. } => {
            selectors_of(expr, into);
            if let Some(param) = param {
                selectors_of(param, into);
            }
        }
        PromAst::Call { args, .. } => {
            for arg in args {
                selectors_of(arg, into);
            }
        }
        PromAst::Number(_) | PromAst::String(_) => {}
    }
}

/// A selector's plan keeping the rows with a trace id and projecting
/// the trace and span ids beside the series columns.
fn with_exemplar_ids(plan: Plan) -> Plan {
    match plan {
        Plan::Project { input, mut exprs } => {
            exprs.push(NamedExpr::new(Expr::column(TRACE_ID), TRACE_ID));
            exprs.push(NamedExpr::new(Expr::column(SPAN_ID), SPAN_ID));
            let traced = Expr::IsNull {
                expr: Box::new(Expr::column(TRACE_ID)),
                negated: true,
            };
            Plan::Project {
                input: Box::new(input.filter(traced)),
                exprs,
            }
        }
        other => other,
    }
}

/// Adds every row of `batches` with a value and a trace id to its
/// series' exemplars.
fn exemplars_of(
    batches: &[RecordBatch],
    by_series: &mut BTreeMap<BTreeMap<String, String>, Vec<Value>>,
) {
    for batch in batches {
        let text = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        };
        let ids = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
        };
        let (Some(timestamps), Some(values), Some(traces)) = (
            batch
                .column_by_name(TIMESTAMP)
                .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>()),
            batch
                .column_by_name(VALUE)
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>()),
            ids(TRACE_ID),
        ) else {
            continue;
        };
        let spans = ids(SPAN_ID);
        let merged = attributes_column(batch);
        let names = text(tessera_kernels::promql::NAME_LABEL);
        for i in 0..batch.num_rows() {
            if values.is_null(i) || traces.is_null(i) {
                continue;
            }
            let attributes = merged
                .as_ref()
                .filter(|o| !o.is_null(i))
                .map(|o| o.value(i));
            let mut labels = row_labels(attributes, None, None);
            if let Some(name) = names.filter(|n| !n.is_null(i)) {
                labels.insert(
                    tessera_kernels::promql::NAME_LABEL.to_owned(),
                    name.value(i).to_owned(),
                );
            }
            let mut exemplar_labels =
                BTreeMap::from([("trace_id", crate::tempo::hex(traces.value(i)))]);
            if let Some(span) = spans.filter(|s| !s.is_null(i)) {
                exemplar_labels.insert("span_id", crate::tempo::hex(span.value(i)));
            }
            #[allow(clippy::cast_precision_loss)]
            let seconds = timestamps.value(i) as f64 / 1e9;
            by_series.entry(labels).or_default().push(json!({
                "labels": exemplar_labels,
                "value": format_value(values.value(i)),
                "timestamp": seconds,
            }));
        }
    }
}

fn success(data: &Value) -> Response {
    Json(json!({ "status": "success", "data": data })).into_response()
}

/// An error as Prometheus writes it, with its status.
fn failure(error: &ApiError) -> Response {
    let status = error.status();
    let error_type = match status {
        StatusCode::BAD_REQUEST => "bad_data",
        StatusCode::SERVICE_UNAVAILABLE => "unavailable",
        StatusCode::UNPROCESSABLE_ENTITY => "execution",
        _ => "internal",
    };
    (
        status,
        Json(json!({ "status": "error", "errorType": error_type, "error": error.message() })),
    )
        .into_response()
}

/// Runs `f` and writes its error the Prometheus way.
async fn answer(f: impl Future<Output = Result<Value, ApiError>>) -> Response {
    match f.await {
        Ok(data) => success(&data),
        Err(error) => failure(&error),
    }
}

/// Remote write: a snappy-compressed protobuf body, answered with 204
/// once its samples are durable (IN-10).
async fn write(
    State(state): State<AppState>,
    parts: Parts,
    body: Bytes,
) -> Result<Response, ApiError> {
    let scope = scope(&state, &parts, Need::Write).await?;
    let request = remote_write::decode(&body).map_err(|e| ApiError::bad_request(e.to_string()))?;
    let now = state.env.clock.now();
    let batch = remote_write::to_batch(&request, scope.tenant, scope.dataset, now)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    if let Some(batch) = batch {
        state.write(batch).await?;
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// The parameters of a request: the query string of a GET or the form
/// body of a POST, which `RawForm` reads either way; `match[]` repeats.
struct Params {
    values: BTreeMap<String, String>,
    matches: Vec<String>,
}

impl Params {
    fn parse(raw: &[u8]) -> Params {
        let mut values = BTreeMap::new();
        let mut matches = Vec::new();
        for (key, value) in form_urlencoded::parse(raw) {
            if key == "match[]" || key == "match" {
                matches.push(value.into_owned());
            } else {
                values.insert(key.into_owned(), value.into_owned());
            }
        }
        Params { values, matches }
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    fn required(&self, key: &str) -> Result<&str, ApiError> {
        self.get(key)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| ApiError::bad_request(format!("invalid parameter \"{key}\": missing")))
    }

    fn time(&self, key: &str, default: Timestamp, now: Timestamp) -> Result<Timestamp, ApiError> {
        match self.get(key).filter(|v| !v.is_empty()) {
            Some(text) => parse_time(text, now).map_err(|e| {
                ApiError::bad_request(format!("invalid parameter \"{key}\": {}", e.message()))
            }),
            None => Ok(default),
        }
    }
}

fn lower(
    text: &str,
    scope: &Scope,
    params: RangeParams,
    now: Timestamp,
) -> Result<Lowered, ApiError> {
    let ctx = BindContext::new(scope.tenant.clone(), scope.dataset.clone(), now);
    PromqlFrontend { params }
        .lower_query(text, &ctx)
        .map_err(|e| {
            ApiError::bad_request(format!("invalid parameter \"query\": {}", e.describe(text)))
        })
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
        "promql",
        "api",
        started.elapsed(),
        scope.principal.team(),
    );
    Ok(output.batches)
}

/// A row of a result: step, labels (none for a scalar) and value.
struct Row {
    time: i64,
    labels: Option<BTreeMap<String, String>>,
    value: f64,
}

fn rows(batches: &[RecordBatch]) -> Vec<Row> {
    let mut out = Vec::new();
    for batch in batches {
        let (Some(times), Some(values)) = (
            batch
                .column_by_name(TIMESTAMP)
                .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>()),
            batch
                .column_by_name(VALUE)
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>()),
        ) else {
            continue;
        };
        let labels = batch
            .column_by_name(EXTRACTED)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        for i in 0..batch.num_rows() {
            let labels = labels
                .filter(|l| !l.is_null(i))
                .and_then(|l| serde_json::from_str(l.value(i)).ok());
            out.push(Row {
                time: times.value(i),
                labels,
                value: values.value(i),
            });
        }
    }
    out
}

/// A time as Prometheus writes it: seconds, with milliseconds when
/// they are not zero.
#[allow(clippy::cast_precision_loss)]
fn seconds(nanos: i64) -> Value {
    let millis = nanos.div_euclid(1_000_000);
    if millis % 1000 == 0 {
        json!(millis / 1000)
    } else {
        json!(millis as f64 / 1000.0)
    }
}

/// `[time, "value"]`.
fn sample(time: i64, value: f64) -> Value {
    json!([seconds(time), format_value(value)])
}

/// The `data` of a query result from its rows.
fn result_data(lowered: &Lowered, rows: Vec<Row>, instant: bool, time: Timestamp) -> Value {
    match lowered.kind {
        ResultKind::String => json!({
            "resultType": "string",
            "result": [seconds(time.as_nanos()), lowered.string.clone().unwrap_or_default()],
        }),
        ResultKind::Scalar if instant => {
            let value = rows.first().map_or(f64::NAN, |r| r.value);
            json!({ "resultType": "scalar", "result": sample(time.as_nanos(), value) })
        }
        ResultKind::Vector if instant => json!({
            "resultType": "vector",
            "result": rows
                .iter()
                .map(|r| json!({ "metric": r.labels.clone().unwrap_or_default(), "value": sample(r.time, r.value) }))
                .collect::<Vec<_>>(),
        }),
        _ => {
            let mut by_labels: BTreeMap<BTreeMap<String, String>, Vec<Value>> = BTreeMap::new();
            for row in rows {
                by_labels
                    .entry(row.labels.unwrap_or_default())
                    .or_default()
                    .push(sample(row.time, row.value));
            }
            json!({
                "resultType": "matrix",
                "result": by_labels
                    .into_iter()
                    .map(|(labels, values)| json!({ "metric": labels, "values": values }))
                    .collect::<Vec<_>>(),
            })
        }
    }
}

async fn query_common(
    state: &AppState,
    parts: &Parts,
    raw: &[u8],
    instant: bool,
) -> Result<Value, ApiError> {
    let scope = scope(state, parts, Need::Read).await?;
    let _turn = state
        .admit_query(&scope.tenant, scope.principal.team())
        .await?;
    let params = Params::parse(raw);
    let now = state.env.clock.now();
    let query = params.required("query")?;
    let (start, end, step) = if instant {
        let time = params.time("time", now, now)?;
        (time, time, Duration::ZERO)
    } else {
        let start = params.time("start", now, now)?;
        let end = params.time("end", now, now)?;
        let step = parse_step(params.required("step")?).map_err(|e| {
            ApiError::bad_request(format!("invalid parameter \"step\": {}", e.message()))
        })?;
        if step.is_zero() {
            return Err(ApiError::bad_request(
                "invalid parameter \"step\": zero or negative query resolution step widths are not accepted. Try a positive integer",
            ));
        }
        if end < start {
            return Err(ApiError::bad_request(
                "invalid parameter \"end\": end timestamp must not be before start time",
            ));
        }
        (start, end, step)
    };
    let lowered = lower(query, &scope, RangeParams { start, end, step }, now)?;
    if !instant && matches!(lowered.kind, ResultKind::Matrix | ResultKind::String) {
        return Err(ApiError::bad_request(format!(
            "invalid parameter \"query\": invalid expression type \"{}\" for range query, must be Scalar or instant Vector",
            if lowered.kind == ResultKind::Matrix {
                "range vector"
            } else {
                "string"
            }
        )));
    }
    let batches = run(state, &scope, lowered.plan.clone()).await?;
    Ok(result_data(&lowered, rows(&batches), instant, end))
}

async fn instant_query(
    State(state): State<AppState>,
    parts: Parts,
    RawForm(raw): RawForm,
) -> Response {
    instant_query_raw(&state, &parts, &raw).await
}

/// An instant query from its raw parameters, for the shared POST route.
pub(crate) async fn instant_query_raw(state: &AppState, parts: &Parts, raw: &[u8]) -> Response {
    answer(query_common(state, parts, raw, true)).await
}

async fn range_query(
    State(state): State<AppState>,
    parts: Parts,
    RawForm(raw): RawForm,
) -> Response {
    answer(query_common(&state, &parts, &raw, false)).await
}

/// The label sets the selectors of `match[]` admit in the range: the
/// newest rows of each selector, labelled.
async fn discovery_rows(
    state: &AppState,
    parts: &Parts,
    params: &Params,
) -> Result<Vec<BTreeMap<String, String>>, ApiError> {
    let scope = scope(state, parts, Need::Read).await?;
    let _turn = state
        .admit_query(&scope.tenant, scope.principal.team())
        .await?;
    let now = state.env.clock.now();
    let end = params.time("end", now, now)?;
    let start = params.time(
        "start",
        Timestamp::from_nanos(end.as_nanos().saturating_sub(3_600_000_000_000)),
        now,
    )?;
    let range = TimeRange::new(
        start,
        Timestamp::from_nanos(end.as_nanos().saturating_add(1)),
    );
    let ctx = BindContext::new(scope.tenant.clone(), scope.dataset.clone(), now);
    let selectors: Vec<String> = if params.matches.is_empty() {
        vec!["{__name__=~\".+\"}".to_owned()]
    } else {
        params.matches.clone()
    };
    let mut sets: BTreeSet<BTreeMap<String, String>> = BTreeSet::new();
    for text in selectors {
        let expr = parse(&text).map_err(|e| {
            ApiError::bad_request(format!(
                "invalid parameter \"match[]\": {}",
                e.describe(&text)
            ))
        })?;
        let tessera_promql::ast::Expr::Selector {
            name,
            matchers,
            span,
            ..
        } = &expr
        else {
            return Err(ApiError::bad_request(
                "invalid parameter \"match[]\": expected a selector",
            ));
        };
        let plan = selector_plan(&ctx, name.as_deref(), matchers, range, *span)
            .map_err(|e| ApiError::bad_request(e.describe(&text)))?
            .limit(0, Some(DISCOVERY_ROWS));
        let batches = run(state, &scope, plan).await?;
        let series = tessera_exec::promql::series_of(&batches)?;
        sets.extend(series.into_iter().map(|s| s.labels));
    }
    Ok(sets.into_iter().collect())
}

async fn series(State(state): State<AppState>, parts: Parts, RawForm(raw): RawForm) -> Response {
    answer(async {
        let params = Params::parse(&raw);
        if params.matches.is_empty() {
            return Err(ApiError::bad_request("no match[] parameter provided"));
        }
        let sets = discovery_rows(&state, &parts, &params).await?;
        Ok(json!(sets))
    })
    .await
}

async fn labels(State(state): State<AppState>, parts: Parts, RawForm(raw): RawForm) -> Response {
    answer(async {
        let params = Params::parse(&raw);
        let names: BTreeSet<String> = discovery_rows(&state, &parts, &params)
            .await?
            .into_iter()
            .flat_map(BTreeMap::into_keys)
            .collect();
        Ok(json!(names))
    })
    .await
}

async fn label_values(
    State(state): State<AppState>,
    parts: Parts,
    Path(name): Path<String>,
    RawForm(raw): RawForm,
) -> Response {
    answer(async {
        let params = Params::parse(&raw);
        let values: BTreeSet<String> = discovery_rows(&state, &parts, &params)
            .await?
            .into_iter()
            .filter_map(|mut set| set.remove(&name))
            .collect();
        Ok(json!(values))
    })
    .await
}

impl From<tessera_exec::ExecError> for ApiError {
    fn from(error: tessera_exec::ExecError) -> Self {
        ApiError::bad_request(error.to_string())
    }
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

    /// A counter for jobs api and web over a minute from
    /// 2023-11-14T22:13:20Z, and `up` for api.
    async fn pushed() -> Router {
        let app = crate::router(crate::testing::state().await);
        let base = 1_700_000_000_000;
        let api: Vec<(i64, f64)> = (0..5i32)
            .map(|i| (base + i64::from(i) * 15_000, 10.0 * f64::from(i)))
            .collect();
        let web: Vec<(i64, f64)> = [0.0, 5.0, 2.0, 4.0, 6.0]
            .iter()
            .zip(0..5i32)
            .map(|(v, i)| (base + i64::from(i) * 15_000, *v))
            .collect();
        let body = testing::write_request(
            &[
                (
                    "http_requests_total",
                    &[("job", "api"), ("instance", "a")],
                    &api,
                ),
                (
                    "http_requests_total",
                    &[("job", "web"), ("instance", "b")],
                    &web,
                ),
                (
                    "up",
                    &[("job", "api"), ("instance", "a")],
                    &[(base, 1.0), (base + 60_000, 1.0)],
                ),
            ],
            &["http_requests"],
        );
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/write")
            .header(header::CONTENT_TYPE, "application/x-protobuf")
            .header(header::CONTENT_ENCODING, "snappy")
            .body(Body::from(body))
            .unwrap();
        let (status, _) = call(&app, request).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        app
    }

    /// QL-5: a histogram stored from OTLP answers `_bucket` as the
    /// classic series, so `histogram_quantile` reads it unchanged.
    #[tokio::test]
    async fn stored_histograms_answer_as_classic_series() {
        let app = crate::router(crate::testing::state().await);
        let metrics = crate::otlp::testing::export_histogram(
            "api",
            "latency",
            "a",
            0,
            &[10.0, 20.0],
            &[1, 2, 0],
        );
        let request = Request::builder()
            .method("POST")
            .uri("/v1/metrics")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&metrics).unwrap()))
            .unwrap();
        let (status, _) = call(&app, request).await;
        assert_eq!(status, StatusCode::OK);

        let (status, value) = call(
            &app,
            get("/api/v1/query?query=latency_bucket&time=1700000030"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let result = value["data"]["result"].as_array().unwrap();
        let mut buckets: Vec<(String, String)> = result
            .iter()
            .map(|s| {
                (
                    s["metric"]["le"].as_str().unwrap().to_owned(),
                    s["value"][1].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        buckets.sort();
        assert_eq!(
            buckets,
            vec![
                ("+Inf".to_owned(), "3".to_owned()),
                ("10".to_owned(), "1".to_owned()),
                ("20".to_owned(), "3".to_owned())
            ],
            "{value}"
        );
        assert_eq!(result[0]["metric"]["__name__"], "latency_bucket");
        assert_eq!(result[0]["metric"]["job"], "a");

        let (status, value) = call(
            &app,
            get("/api/v1/query?query=histogram_quantile(0.5,%20latency_bucket)&time=1700000030"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(
            value["data"]["result"][0]["value"][1], "12.5",
            "the median interpolates within (10, 20]: {value}"
        );
        let (_, value) = call(&app, get("/api/v1/query?query=latency_sum&time=1700000030")).await;
        assert_eq!(value["data"]["result"][0]["value"][1], "30", "{value}");
        let (_, value) = call(&app, get("/api/v1/query?query=latency&time=1700000030")).await;
        assert_eq!(
            value["data"]["result"].as_array().unwrap().len(),
            0,
            "the plain name has no sample type yet: {value}"
        );
    }

    /// Exemplars (section 16): a point ingested with an exemplar
    /// answers a panel's query with the trace it names.
    #[tokio::test]
    async fn exemplars_come_from_the_points_trace_ids() {
        use prost::Message;
        let app = crate::router(crate::testing::state().await);
        let metrics = crate::otlp::testing::export_gauge_with_exemplar(
            "api", "up", "a", 1.0, [0xab; 16], [0xcd; 8],
        );
        let request = Request::builder()
            .method("POST")
            .uri("/v1/metrics")
            .header(header::CONTENT_TYPE, "application/x-protobuf")
            .body(Body::from(metrics.encode_to_vec()))
            .unwrap();
        let (status, _) = call(&app, request).await;
        assert_eq!(status, StatusCode::OK);

        let (status, value) = call(
            &app,
            get("/api/v1/query_exemplars?query=sum(rate(up%5B1m%5D))&start=1700000000&end=1700000060"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["status"], "success");
        let data = value["data"].as_array().unwrap();
        assert_eq!(data.len(), 1, "{value}");
        assert_eq!(data[0]["seriesLabels"]["__name__"], "up");
        assert_eq!(data[0]["seriesLabels"]["job"], "a");
        let exemplar = &data[0]["exemplars"][0];
        assert_eq!(exemplar["labels"]["trace_id"], "ab".repeat(16));
        assert_eq!(exemplar["labels"]["span_id"], "cd".repeat(8));
        assert_eq!(exemplar["value"], "1");
        assert_eq!(exemplar["timestamp"], 1_700_000_000.0);

        let (_, value) = call(
            &app,
            get("/api/v1/query_exemplars?query=1%2B1&start=1700000000&end=1700000060"),
        )
        .await;
        assert_eq!(value["data"], json!([]), "no selector, no exemplars");
        let (status, _) = call(&app, get("/api/v1/query_exemplars?query=")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn instant_and_range_queries_answer_like_prometheus() {
        let app = pushed().await;
        let (status, value) = call(&app, get("/api/v1/query?query=up&time=1700000060")).await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["status"], "success");
        assert_eq!(value["data"]["resultType"], "vector");
        let result = value["data"]["result"].as_array().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["metric"]["__name__"], "up");
        assert_eq!(result[0]["metric"]["job"], "api");
        assert_eq!(result[0]["value"], json!([1_700_000_060, "1"]));

        let (status, value) = call(
            &app,
            get("/api/v1/query_range?query=sum%20by%20(job)%20(rate(http_requests_total%5B1m%5D))&start=1700000030&end=1700000060&step=30"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        assert_eq!(value["data"]["resultType"], "matrix");
        let result = value["data"]["result"].as_array().unwrap();
        assert_eq!(result.len(), 2);
        let api = result.iter().find(|s| s["metric"]["job"] == "api").unwrap();
        assert_eq!(api["values"].as_array().unwrap().len(), 2);
        assert_eq!(api["values"][1][0], 1_700_000_060);
        assert_eq!(api["values"][1][1], "0.6666666666666666");

        let (_, value) = call(&app, get("/api/v1/query?query=1%2B1")).await;
        assert_eq!(value["data"]["resultType"], "scalar");
        assert_eq!(value["data"]["result"][1], "2");
        let (_, value) = call(&app, get("/api/v1/query?query=%22hi%22&time=5")).await;
        assert_eq!(
            value["data"],
            json!({ "resultType": "string", "result": [5, "hi"] })
        );
        let (_, value) = call(&app, get("/api/v1/query?query=up%5B2m%5D&time=1700000060")).await;
        assert_eq!(value["data"]["resultType"], "matrix");
        assert_eq!(
            value["data"]["result"][0]["values"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn errors_take_prometheus_shape() {
        let app = pushed().await;
        let (status, value) = call(&app, get("/api/v1/query?query=nope(")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(value["status"], "error");
        assert_eq!(value["errorType"], "bad_data");
        assert!(
            value["error"]
                .as_str()
                .unwrap()
                .starts_with("invalid parameter \"query\""),
            "{value}"
        );
        let (status, value) = call(
            &app,
            get("/api/v1/query_range?query=up%5B5m%5D&start=1&end=2&step=1"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            value["error"].as_str().unwrap().contains("range vector"),
            "{value}"
        );
        let (status, _) = call(&app, get("/api/v1/query_range?query=up&start=1&end=2")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "step is required");
        let (status, _) = call(
            &app,
            get("/api/v1/query_range?query=up&start=1&end=2&step=0"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn series_labels_and_values_describe_the_samples() {
        let app = pushed().await;
        let (status, value) = call(
            &app,
            get("/api/v1/series?match[]=http_requests_total&start=1700000000&end=1700000100"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{value}");
        let sets = value["data"].as_array().unwrap();
        assert_eq!(sets.len(), 2);
        assert_eq!(sets[0]["__name__"], "http_requests_total");
        let (_, value) = call(&app, get("/api/v1/labels?start=1700000000&end=1700000100")).await;
        assert_eq!(value["data"], json!(["__name__", "instance", "job"]));
        let (_, value) = call(
            &app,
            get("/api/v1/label/__name__/values?start=1700000000&end=1700000100"),
        )
        .await;
        assert_eq!(value["data"], json!(["http_requests_total", "up"]));
        let (_, value) = call(
            &app,
            get("/api/v1/label/job/values?match[]=up&start=1700000000&end=1700000100"),
        )
        .await;
        assert_eq!(value["data"], json!(["api"]));
        let (status, _) = call(&app, get("/api/v1/series")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let (_, value) = call(&app, get("/api/v1/metadata")).await;
        assert_eq!(value["data"], json!({}));
        let (_, value) = call(&app, get("/api/v1/rules")).await;
        assert_eq!(value["data"]["groups"], json!([]));
    }

    #[tokio::test]
    async fn remote_write_rejects_garbage_and_readiness_answers() {
        let app = crate::router(crate::testing::state().await);
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/write")
            .body(Body::from("garbage"))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&bytes).contains("snappy"));
        let response = app.clone().oneshot(get("/-/ready")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let (_, value) = call(&app, get("/api/v1/status/buildinfo")).await;
        assert_eq!(value["status"], "success");
    }
}
