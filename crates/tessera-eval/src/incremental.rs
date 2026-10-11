// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The incremental subset (CQ-7): an SQL rule that filters, groups by
//! keys and a tumbling window, and aggregates with `count`, `sum`,
//! `min`, `max` or `avg` is maintained from writer-log segments as they
//! are written (CQ-1), never waiting for data files, so its latency is
//! bounded by the write-ahead log's. A source task reads each writer's
//! segments past the position the state remembers (CQ-5), only the
//! blocks of the rule's tenant, dataset and kind (CQ-2), and folds the
//! rows into per-window, per-key partial aggregates (CQ-4). Windows
//! are evaluated eagerly when the condition is monotone in arriving
//! data (CQ-9), else once the watermark passes their end (CQ-10); rows
//! older than the allowed lateness are counted and ignored (CQ-11).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tessera_commit::Action;
use tessera_ir::{AggregateExpr, BinaryOp, BindContext, Expr, Frontend, Literal, Plan, Statement};
use tessera_kernels::{evaluate, evaluate_predicate};
use tessera_manifest::registry;
use tessera_object::ObjectStore;
use tessera_opt::optimize;
use tessera_sql::SqlFrontend;
use tessera_types::event::TIMESTAMP;
use tessera_types::{DatasetName, Kind, Seq, TenantId, Timestamp};
use tessera_wal::{EntryBody, reader};

use crate::lifecycle::{Observation, fingerprint};
use crate::rule::{AlertRule, Language};
use crate::state::EvalState;

/// How long a writer may be quiet before its watermark follows the
/// clock (section 11.5).
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(2);
/// How late a row may be and still count (section 11.5).
pub const LATENESS: Duration = Duration::from_secs(10);

/// How the condition fires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Firing {
    /// As soon as the condition holds: more data cannot make it false.
    Eager,
    /// Once the watermark passes the end of the window.
    Watermark,
}

/// The incremental shape of a rule.
#[derive(Clone, Debug, PartialEq)]
pub struct Shape {
    /// The dataset.
    pub dataset: DatasetName,
    /// The kind of event.
    pub kind: Kind,
    /// The row filter, over the kind's schema.
    pub predicate: Option<Expr>,
    /// The grouping keys other than the window, with their names.
    pub keys: Vec<(String, Expr)>,
    /// The window length.
    pub window: Duration,
    /// The aggregates, in output order.
    pub aggregates: Vec<AggregateExpr>,
    /// The type of each key, in the order of `keys`.
    pub key_types: Vec<DataType>,
    /// The type of each aggregate, in the order of `aggregates`.
    pub aggregate_types: Vec<DataType>,
    /// The condition over the keys and the aggregates.
    pub having: Option<Expr>,
    /// How the condition fires.
    pub firing: Firing,
}

impl Shape {
    /// Whether the shape reads `dataset`.
    #[must_use]
    pub fn target_is(&self, dataset: &str) -> bool {
        self.dataset.as_str() == dataset
    }
}

/// The incremental shape of `rule`, or why it has none.
///
/// # Errors
///
/// The reason the rule is evaluated periodically instead (CQ-8).
pub fn shape_of(rule: &AlertRule, tenant: &TenantId) -> Result<Shape, String> {
    if rule.language != Language::Sql {
        return Err(format!(
            "{} rules are evaluated through the engine; the incremental subset is SQL",
            rule.language
        ));
    }
    shape_of_sql(&rule.expr, &rule.dataset, tenant)
}

/// The shape of the SQL query `expr` over `dataset`, or why it has none.
///
/// # Errors
///
/// Why the query is not a grouped aggregation over one tumbling window
/// of mergeable aggregates.
pub fn shape_of_sql(expr: &str, dataset: &str, tenant: &TenantId) -> Result<Shape, String> {
    let dataset = DatasetName::new(dataset).map_err(|e| e.to_string())?;
    let ctx = BindContext::new(tenant.clone(), dataset, Timestamp::from_nanos(0));
    let plan = match SqlFrontend
        .lower(expr, &ctx)
        .map_err(|d| d.describe(expr))?
    {
        Statement::Query(plan) => plan,
        Statement::Explain { .. } | Statement::Control(_) => {
            return Err("the expression is not a query".into());
        }
    };
    let plan = optimize(plan, &tessera_ir::Registry::builtin()).map_err(|e| e.to_string())?;
    shape_of_plan(&plan)
}

fn shape_of_plan(plan: &Plan) -> Result<Shape, String> {
    let mut node = plan;
    let mut having = None;
    loop {
        match node {
            Plan::Project { input, .. } | Plan::Limit { input, .. } => node = input,
            Plan::Filter { input, predicate } if matches!(**input, Plan::Aggregate { .. }) => {
                having = Some(predicate.clone());
                node = input;
            }
            Plan::Aggregate { .. } => break,
            other => {
                return Err(format!(
                    "the query is not a grouped aggregation: {}",
                    other.to_string().lines().next().unwrap_or_default()
                ));
            }
        }
    }
    let Plan::Aggregate {
        input,
        group_by,
        aggregates,
    } = node
    else {
        unreachable!("the loop stops at an aggregate");
    };
    let mut predicates = Vec::new();
    let mut source = &**input;
    let scan = loop {
        match source {
            Plan::Filter { input, predicate } => {
                predicates.push(predicate.clone());
                source = input;
            }
            Plan::Scan(scan) => break scan,
            _ => return Err("the aggregation reads something other than a table".into()),
        }
    };
    predicates.extend(scan.predicates.iter().cloned());
    let predicate = predicates
        .into_iter()
        .reduce(|a, b| a.binary(BinaryOp::And, b));
    let (keys, window) = keys_and_window(group_by)?;
    check_aggregates(aggregates)?;
    let schema = node
        .schema(&tessera_ir::Registry::builtin())
        .map_err(|e| e.to_string())?;
    let key_types: Vec<DataType> = group_by
        .iter()
        .enumerate()
        .filter(|(_, key)| !matches!(&key.expr, Expr::Call { name, .. } if name == "tumble" || name == "time_bucket"))
        .map(|(i, _)| schema.field(i).data_type().clone())
        .collect();
    let aggregate_types: Vec<DataType> = (0..aggregates.len())
        .map(|i| schema.field(group_by.len() + i).data_type().clone())
        .collect();
    let firing = if having.as_ref().is_none_or(|h| monotone(h, aggregates)) {
        Firing::Eager
    } else {
        Firing::Watermark
    };
    Ok(Shape {
        dataset: scan.dataset.clone(),
        kind: scan.kind,
        predicate,
        keys,
        window,
        aggregates: aggregates.clone(),
        key_types,
        aggregate_types,
        having,
        firing,
    })
}

/// The grouping keys and the one tumbling window of a `GROUP BY`.
fn keys_and_window(
    group_by: &[tessera_ir::NamedExpr],
) -> Result<(Vec<(String, Expr)>, Duration), String> {
    let mut keys = Vec::new();
    let mut window = None;
    for key in group_by {
        match &key.expr {
            Expr::Call { name, args } if name == "tumble" || name == "time_bucket" => {
                let (interval, time) = if name == "tumble" {
                    (args.get(1), args.first())
                } else {
                    (args.first(), args.get(1))
                };
                let length = match interval {
                    Some(Expr::Literal(Literal::Duration(d))) => *d,
                    _ => return Err("the window needs a constant interval".into()),
                };
                if !matches!(time, Some(Expr::Column(c)) if c == TIMESTAMP) {
                    return Err("the window must be over the timestamp column".into());
                }
                if window.replace(length).is_some() {
                    return Err("only one window is supported".into());
                }
            }
            expr => keys.push((key.name.clone(), expr.clone())),
        }
    }
    let Some(window) = window else {
        return Err("no tumbling window in GROUP BY".into());
    };
    if window.is_zero() {
        return Err("the window must be longer than zero".into());
    }
    Ok((keys, window))
}

/// Every aggregate must be mergeable (CQ-7) with at most one argument.
fn check_aggregates(aggregates: &[AggregateExpr]) -> Result<(), String> {
    for aggregate in aggregates {
        let supported = matches!(
            aggregate.function.as_str(),
            "count" | "count_raw" | "sum" | "sum_raw" | "avg" | "avg_raw" | "min" | "max"
        );
        if !supported {
            return Err(format!(
                "{} is not a mergeable aggregate",
                aggregate.function
            ));
        }
        if aggregate.args.len() > 1 {
            return Err(format!("{} takes one argument", aggregate.function));
        }
    }
    Ok(())
}

/// Whether a condition, once true for a window, stays true as rows
/// arrive: a conjunction of `count`, `sum` or `max` compared with `>`
/// or `>=` to a constant, or `min` with `<` or `<=`.
fn monotone(having: &Expr, aggregates: &[AggregateExpr]) -> bool {
    match having {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => monotone(left, aggregates) && monotone(right, aggregates),
        Expr::Binary { op, left, right } => {
            let (column, constant, grows) = match (&**left, &**right) {
                (Expr::Column(c), Expr::Literal(_)) => (c, true, true),
                (Expr::Literal(_), Expr::Column(c)) => (c, true, false),
                _ => return false,
            };
            let _ = constant;
            let Some(aggregate) = aggregates.iter().find(|a| &a.name == column) else {
                return false;
            };
            let upward = matches!(
                aggregate.function.as_str(),
                "count" | "count_raw" | "sum" | "sum_raw" | "max"
            );
            let downward = aggregate.function == "min";
            match (op, grows) {
                (BinaryOp::Gt | BinaryOp::GtEq, true) | (BinaryOp::Lt | BinaryOp::LtEq, false) => {
                    upward
                }
                (BinaryOp::Lt | BinaryOp::LtEq, true) | (BinaryOp::Gt | BinaryOp::GtEq, false) => {
                    downward
                }
                _ => false,
            }
        }
        _ => false,
    }
}

/// A mergeable partial aggregate.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Partial {
    /// Rows counted.
    pub count: u64,
    /// Sum of the argument.
    pub sum: f64,
    /// Smallest argument.
    pub min: f64,
    /// Largest argument.
    pub max: f64,
}

impl Partial {
    fn fold(&mut self, value: Option<f64>) {
        self.count += 1;
        if let Some(v) = value {
            if self.sum == 0.0 && self.min == 0.0 && self.max == 0.0 && self.count == 1 {
                self.min = v;
                self.max = v;
            } else {
                self.min = self.min.min(v);
                self.max = self.max.max(v);
            }
            self.sum += v;
        }
    }

    fn value(&self, function: &str) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let count = self.count as f64;
        match function {
            "count" | "count_raw" => count,
            "sum" | "sum_raw" => self.sum,
            "avg" | "avg_raw" => {
                if self.count == 0 {
                    f64::NAN
                } else {
                    self.sum / count
                }
            }
            "min" => self.min,
            "max" => self.max,
            _ => f64::NAN,
        }
    }
}

/// The state of one window of one group of keys.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WindowState {
    /// The rule.
    pub rule: String,
    /// The window's start.
    pub start: Timestamp,
    /// The keys.
    pub keys: BTreeMap<String, String>,
    /// One partial per aggregate, in the rule's order.
    pub partials: Vec<Partial>,
}

impl WindowState {
    /// The key of the window in the state: `rule/start/fingerprint`.
    #[must_use]
    pub fn key(&self) -> String {
        window_key(&self.rule, self.start, &fingerprint(&self.keys))
    }
}

fn window_key(rule: &str, start: Timestamp, fingerprint: &str) -> String {
    format!("{rule}/{:020}/{fingerprint}", start.as_nanos())
}

/// What a source task read.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Ingested {
    /// The positions the task started from, so a commit against a
    /// state that moved past them is refused.
    pub positions_before: BTreeMap<String, Seq>,
    /// The last entry read per writer.
    pub positions: BTreeMap<String, Seq>,
    /// The newest observed time per writer, from this and earlier reads.
    pub watermarks: BTreeMap<String, Timestamp>,
    /// The windows touched, updated.
    pub windows: BTreeMap<String, WindowState>,
    /// Rows older than the allowed lateness, ignored (CQ-11).
    pub late_rows: u64,
}

/// Reads every writer's new segments for `rule` and folds them into
/// the windows the state remembers.
///
/// # Errors
///
/// If the registry or a segment cannot be read, or an expression
/// cannot be evaluated.
pub async fn ingest(
    store: &Arc<dyn ObjectStore>,
    tenant: &TenantId,
    rule: &AlertRule,
    shape: &Shape,
    state: &EvalState,
    now: Timestamp,
) -> Result<Ingested, String> {
    let mut ingested = Ingested {
        positions_before: state.positions.clone(),
        positions: state.positions.clone(),
        watermarks: state.watermarks.clone(),
        windows: BTreeMap::new(),
        late_rows: 0,
    };
    let writers = registry::list(&**store, tenant, &shape.dataset)
        .await
        .map_err(|e| e.to_string())?;
    let horizon = Timestamp::from_nanos(
        watermark(
            state,
            &writers
                .iter()
                .map(|r| r.writer.to_string())
                .collect::<Vec<_>>(),
            now,
        )
        .as_nanos(),
    );
    for registration in writers {
        let writer = registration.writer;
        let name = writer.to_string();
        let from = state.positions.get(&name).copied().unwrap_or(Seq::ZERO);
        let mut entries = reader::entries(Arc::clone(store), writer, from);
        while let Some(entry) = entries.try_next().await.map_err(|e| e.to_string())? {
            if entry.seq() <= from {
                continue;
            }
            ingested.positions.insert(name.clone(), entry.seq());
            let EntryBody::Segment { blocks } = entry.body() else {
                continue;
            };
            if let Some(newest) = blocks.iter().map(|b| b.max_observed).max() {
                let seen = ingested.watermarks.entry(name.clone()).or_insert(newest);
                *seen = (*seen).max(newest);
            }
            for batch in entry
                .blocks_for(tenant, &shape.dataset, shape.kind)
                .map_err(|e| e.to_string())?
            {
                fold_batch(rule, shape, state, &mut ingested, batch.rows(), horizon)?;
            }
        }
    }
    Ok(ingested)
}

/// The dataset's watermark (section 11.5): the least of every writer's,
/// a writer's being the observed time of its last segment or the clock
/// less the idle timeout when it has been quiet, less the lateness.
#[must_use]
pub fn watermark(state: &EvalState, writers: &[String], now: Timestamp) -> Timestamp {
    let idle = Timestamp::from_nanos(
        now.as_nanos()
            .saturating_sub(i64::try_from(IDLE_TIMEOUT.as_nanos()).unwrap_or(i64::MAX)),
    );
    let least = writers
        .iter()
        .map(|w| match state.watermarks.get(w) {
            Some(seen) if *seen > idle => *seen,
            _ => idle,
        })
        .min()
        .unwrap_or(idle);
    Timestamp::from_nanos(
        least
            .as_nanos()
            .saturating_sub(i64::try_from(LATENESS.as_nanos()).unwrap_or(i64::MAX)),
    )
}

/// Folds one block's rows into the windows.
fn fold_batch(
    rule: &AlertRule,
    shape: &Shape,
    state: &EvalState,
    ingested: &mut Ingested,
    rows: &RecordBatch,
    horizon: Timestamp,
) -> Result<(), String> {
    let rows = match &shape.predicate {
        Some(predicate) => {
            let keep: BooleanArray =
                evaluate_predicate(predicate, rows).map_err(|e| e.to_string())?;
            filter_record_batch(rows, &keep).map_err(|e| e.to_string())?
        }
        None => rows.clone(),
    };
    if rows.num_rows() == 0 {
        return Ok(());
    }
    let times = rows
        .column_by_name(TIMESTAMP)
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
        .ok_or("rows without timestamps")?;
    let keys: Vec<ArrayRef> = shape
        .keys
        .iter()
        .map(|(_, expr)| evaluate(expr, &rows).map_err(|e| e.to_string()))
        .collect::<Result<_, _>>()?;
    let args: Vec<Option<ArrayRef>> = shape
        .aggregates
        .iter()
        .map(|a| {
            a.args
                .first()
                .map(|arg| evaluate(arg, &rows).map_err(|e| e.to_string()))
                .transpose()
        })
        .collect::<Result<_, _>>()?;
    let length = i64::try_from(shape.window.as_nanos()).unwrap_or(i64::MAX);
    for row in 0..rows.num_rows() {
        let at = times.value(row);
        if Timestamp::from_nanos(at) < horizon {
            ingested.late_rows += 1;
            continue;
        }
        let start = Timestamp::from_nanos(at.div_euclid(length) * length);
        let labels: BTreeMap<String, String> = shape
            .keys
            .iter()
            .zip(&keys)
            .filter_map(|((name, _), array)| cell(array, row).map(|v| (name.clone(), v)))
            .collect();
        let key = window_key(&rule.name, start, &fingerprint(&labels));
        let window = ingested.windows.entry(key.clone()).or_insert_with(|| {
            state
                .windows
                .get(&key)
                .cloned()
                .unwrap_or_else(|| WindowState {
                    rule: rule.name.clone(),
                    start,
                    keys: labels.clone(),
                    partials: vec![Partial::default(); shape.aggregates.len()],
                })
        });
        for (partial, arg) in window.partials.iter_mut().zip(&args) {
            partial.fold(arg.as_ref().and_then(|a| number(a, row)));
        }
    }
    Ok(())
}

/// A cell as text, for a grouping key.
fn cell(array: &ArrayRef, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    let any = array.as_any();
    if let Some(a) = any.downcast_ref::<StringArray>() {
        return Some(a.value(row).to_owned());
    }
    if let Some(a) = any.downcast_ref::<Int64Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = any.downcast_ref::<Int32Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = any.downcast_ref::<Float64Array>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = any.downcast_ref::<BooleanArray>() {
        return Some(a.value(row).to_string());
    }
    if let Some(a) = any.downcast_ref::<TimestampNanosecondArray>() {
        return Some(Timestamp::from_nanos(a.value(row)).to_string());
    }
    None
}

/// A cell as a number, for an aggregate's argument.
fn number(array: &ArrayRef, row: usize) -> Option<f64> {
    if array.is_null(row) {
        return None;
    }
    let any = array.as_any();
    if let Some(a) = any.downcast_ref::<Float64Array>() {
        return Some(a.value(row));
    }
    #[allow(clippy::cast_precision_loss)]
    if let Some(a) = any.downcast_ref::<Int64Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = any.downcast_ref::<Int32Array>() {
        return Some(f64::from(a.value(row)));
    }
    #[allow(clippy::cast_precision_loss)]
    if let Some(a) = any.downcast_ref::<TimestampNanosecondArray>() {
        return Some(a.value(row) as f64);
    }
    None
}

/// What the windows say now: the observations of the groups whose
/// condition holds, from each group's newest evaluable window.
///
/// # Errors
///
/// If the condition cannot be evaluated.
pub fn observe(
    rule: &AlertRule,
    shape: &Shape,
    windows: &BTreeMap<String, WindowState>,
    horizon: Timestamp,
) -> Result<Vec<Observation>, String> {
    let length = i64::try_from(shape.window.as_nanos()).unwrap_or(i64::MAX);
    let mut newest: BTreeMap<String, &WindowState> = BTreeMap::new();
    for window in windows.values().filter(|w| w.rule == rule.name) {
        let end = Timestamp::from_nanos(window.start.as_nanos().saturating_add(length));
        if shape.firing == Firing::Watermark && end > horizon {
            continue;
        }
        let fp = fingerprint(&window.keys);
        match newest.get(&fp) {
            Some(seen) if seen.start >= window.start => {}
            _ => {
                newest.insert(fp, window);
            }
        }
    }
    let mut observations = Vec::new();
    for window in newest.values() {
        let values: Vec<f64> = shape
            .aggregates
            .iter()
            .zip(&window.partials)
            .map(|(a, p)| p.value(&a.function))
            .collect();
        let holds = match &shape.having {
            None => true,
            Some(having) => {
                let batch = condition_batch(shape, window, &values)?;
                let keep = evaluate_predicate(having, &batch).map_err(|e| e.to_string())?;
                keep.len() == 1 && keep.value(0)
            }
        };
        if holds {
            observations.push(Observation {
                labels: window.keys.clone(),
                value: values.first().copied().unwrap_or(f64::NAN),
            });
        }
    }
    Ok(observations)
}

/// One row with the keys and the aggregate values, for the condition.
fn condition_batch(
    shape: &Shape,
    window: &WindowState,
    values: &[f64],
) -> Result<RecordBatch, String> {
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (name, _) in &shape.keys {
        fields.push(Field::new(name, DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(vec![
            window.keys.get(name).cloned(),
        ])));
    }
    fields.push(Field::new(
        "window",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    ));
    columns.push(Arc::new(TimestampNanosecondArray::from(vec![
        window.start.as_nanos(),
    ])));
    for (aggregate, value) in shape.aggregates.iter().zip(values) {
        fields.push(Field::new(&aggregate.name, DataType::Float64, true));
        columns.push(Arc::new(Float64Array::from(vec![*value])));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| e.to_string())
}

/// The actions that record what a source task read: positions,
/// watermarks and windows, with windows older than two lengths before
/// the horizon forgotten.
#[must_use]
pub fn actions(
    rule: &AlertRule,
    shape: &Shape,
    ingested: &Ingested,
    state: &EvalState,
    horizon: Timestamp,
) -> Vec<Action> {
    let mut actions = Vec::new();
    for (writer, seq) in &ingested.positions {
        if state.positions.get(writer) != Some(seq) {
            actions.push(EvalState::put_position(writer, *seq));
        }
    }
    for (writer, seen) in &ingested.watermarks {
        if state.watermarks.get(writer) != Some(seen) {
            actions.push(EvalState::put_watermark(writer, *seen));
        }
    }
    for window in ingested.windows.values() {
        actions.push(EvalState::put_window(window));
    }
    let length = i64::try_from(shape.window.as_nanos()).unwrap_or(i64::MAX);
    let forget_before = horizon.as_nanos().saturating_sub(length.saturating_mul(2));
    for (key, window) in &state.windows {
        if window.rule == rule.name
            && window.start.as_nanos() < forget_before
            && !ingested.windows.contains_key(key)
        {
            actions.push(EvalState::delete_window(key));
        }
    }
    actions
}
