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
//! In a group with several state partitions the segments are read by
//! the group's source tasks instead, and a partition folds the partial
//! windows they leave for it ([`crate::source`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use serde::{Deserialize, Serialize};
use tessera_commit::Action;
use tessera_exec::Engine;
use tessera_ir::{AggregateExpr, BinaryOp, BindContext, Expr, Frontend, Literal, Plan, Statement};
use tessera_kernels::{evaluate, evaluate_predicate, row_weight};
use tessera_manifest::registry;
use tessera_object::{ObjectError, ObjectStore, layout};
use tessera_opt::optimize;
use tessera_sql::SqlFrontend;
use tessera_types::event::{OBSERVED_TIMESTAMP, SAMPLE_RATE, TIMESTAMP};
use tessera_types::{
    DatasetName, EventBatch, Keys, Kind, Seq, TenantId, TimeRange, Timestamp, WriterId,
};
use tessera_wal::{Entry, EntryBody, WalError, reader};

use crate::lifecycle::{Observation, fingerprint};
use crate::rule::{AlertRule, Language};
use crate::source;
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
    /// How far apart hopping windows start (CQ-7); tumbling when none.
    pub hop: Option<Duration>,
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
    shape_of_sql(
        &rule.expr,
        &rule.dataset,
        tenant,
        Some(Duration::from_secs(rule.every_seconds.max(1))),
    )
}

/// The shape of the SQL query `expr` over `dataset`, or why it has none.
///
/// # Errors
///
/// Why the query is not a grouped aggregation over one tumbling window
/// of mergeable aggregates.
pub fn shape_of_sql(
    expr: &str,
    dataset: &str,
    tenant: &TenantId,
    every: Option<Duration>,
) -> Result<Shape, String> {
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
    shape_of_plan(&plan, every)
}

fn shape_of_plan(plan: &Plan, every: Option<Duration>) -> Result<Shape, String> {
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
    let (keys, window, hop) = keys_and_window(group_by, every)?;
    check_aggregates(aggregates)?;
    let schema = node
        .schema(&tessera_ir::Registry::builtin())
        .map_err(|e| e.to_string())?;
    let key_types: Vec<DataType> = group_by
        .iter()
        .enumerate()
        .filter(|(_, key)| !matches!(&key.expr, Expr::Call { name, .. } if is_window(name)))
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
        hop,
        aggregates: aggregates.clone(),
        key_types,
        aggregate_types,
        having,
        firing,
    })
}

/// Whether a function call in `GROUP BY` is the window.
fn is_window(name: &str) -> bool {
    matches!(name, "tumble" | "time_bucket" | "hop" | "slide")
}

/// The grouping keys of a `GROUP BY`, its window's length, and how far
/// apart the window's starts are when it hops.
type KeysAndWindow = (Vec<(String, Expr)>, Duration, Option<Duration>);

/// The grouping keys and the one window of a `GROUP BY`: its length,
/// and how far apart its starts are when it hops (CQ-7). A sliding
/// window advances by `every`, the rule's evaluation interval.
fn keys_and_window(
    group_by: &[tessera_ir::NamedExpr],
    every: Option<Duration>,
) -> Result<KeysAndWindow, String> {
    let mut keys = Vec::new();
    let mut window = None;
    let constant = |expr: Option<&Expr>| match expr {
        Some(Expr::Literal(Literal::Duration(d))) => Ok(*d),
        _ => Err("the window needs a constant interval".to_owned()),
    };
    for key in group_by {
        match &key.expr {
            Expr::Call { name, args } if is_window(name) => {
                let (interval, time, step) = match name.as_str() {
                    "tumble" | "slide" => (args.get(1), args.first(), None),
                    "hop" => (args.get(1), args.first(), Some(args.get(2))),
                    _ => (args.first(), args.get(1), None),
                };
                let length = constant(interval)?;
                let mut hop = step.map(constant).transpose()?;
                if name == "slide" {
                    hop = Some(every.ok_or_else(|| {
                        "a sliding window advances by a rule's evaluation interval; only a rule can slide".to_owned()
                    })?);
                }
                if !matches!(time, Some(Expr::Column(c)) if c == TIMESTAMP) {
                    return Err("the window must be over the timestamp column".into());
                }
                if window.replace((length, hop)).is_some() {
                    return Err("only one window is supported".into());
                }
            }
            expr => keys.push((key.name.clone(), expr.clone())),
        }
    }
    let Some((window, hop)) = window else {
        return Err("no tumbling or hopping window in GROUP BY".into());
    };
    if window.is_zero() {
        return Err("the window must be longer than zero".into());
    }
    if let Some(hop) = hop
        && (hop.is_zero() || hop > window || window.as_nanos() % hop.as_nanos() != 0)
    {
        return Err(
            "a window's step must be longer than zero, at most the window and divide it; \
                 a sliding window's step is the rule's EVERY"
                .into(),
        );
    }
    Ok((keys, window, hop))
}

/// The starts of the windows a row at `at` belongs to: one for a
/// tumbling window, `length / hop` for a hopping one, newest first.
fn window_starts(at: i64, length: i64, hop: Option<i64>) -> Vec<Timestamp> {
    let hop = hop.unwrap_or(length).max(1);
    let newest = at.div_euclid(hop) * hop;
    let count = (length / hop).max(1);
    (0..count)
        .map(|k| Timestamp::from_nanos(newest - k * hop))
        .collect()
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

/// A mergeable partial aggregate. Rows count and sum with the weight
/// `1 / sample_rate` (QE-13), as the batch aggregates do, and as they
/// are for the raw forms.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Partial {
    /// Rows counted, as they are.
    pub count: u64,
    /// The rows' weights added; none in a state written before weights
    /// were kept, which counted every row as one.
    #[serde(default)]
    pub weight: Option<f64>,
    /// Sum of the argument, each row weighted.
    pub sum: f64,
    /// Sum of the argument, rows as they are.
    #[serde(default)]
    pub sum_raw: f64,
    /// Smallest argument.
    pub min: f64,
    /// Largest argument.
    pub max: f64,
}

impl Partial {
    fn fold(&mut self, value: Option<f64>, weight: f64) {
        self.weight = Some(self.weighted_count() + weight);
        self.count += 1;
        if let Some(v) = value {
            if self.sum == 0.0 && self.min == 0.0 && self.max == 0.0 && self.count == 1 {
                self.min = v;
                self.max = v;
            } else {
                self.min = self.min.min(v);
                self.max = self.max.max(v);
            }
            self.sum += v * weight;
            self.sum_raw += v;
        }
    }

    /// Adds what `other` folded of the same aggregate from other rows.
    fn merge(&mut self, other: &Partial) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other.clone();
            return;
        }
        self.weight = Some(self.weighted_count() + other.weighted_count());
        self.count += other.count;
        self.sum += other.sum;
        self.sum_raw += other.sum_raw;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    /// The rows' weights added: the row count where none were kept.
    fn weighted_count(&self) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        self.weight.unwrap_or(self.count as f64)
    }

    fn value(&self, function: &str) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let count = self.count as f64;
        let weighted = self.weighted_count();
        let average = |sum: f64, count: f64| if count == 0.0 { f64::NAN } else { sum / count };
        match function {
            "count" => weighted,
            "count_raw" => count,
            "sum" => self.sum,
            "sum_raw" => self.sum_raw,
            "avg" => average(self.sum, weighted),
            "avg_raw" => average(self.sum_raw, count),
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
    /// When the last of the window's rows was received: the newest
    /// observed time among them, for the alert latency of section 17.1.
    #[serde(default)]
    pub received: Option<Timestamp>,
}

impl WindowState {
    /// The key of the window in the state: `rule/start/fingerprint`.
    #[must_use]
    pub fn key(&self) -> String {
        window_key(&self.rule, self.start, &fingerprint(&self.keys))
    }

    /// Adds what `other` folded of the same window from other rows.
    fn merge(&mut self, other: &WindowState) {
        for (mine, theirs) in self.partials.iter_mut().zip(&other.partials) {
            mine.merge(theirs);
        }
        self.received = self.received.max(other.received);
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
    /// The last entry read per writer; a writer gone with its log is
    /// left out, so that its position is forgotten.
    pub positions: BTreeMap<String, Seq>,
    /// The writers whose logs were read to their end.
    pub ended: BTreeSet<String>,
    /// The newest observed time per writer, from this and earlier reads.
    pub watermarks: BTreeMap<String, Timestamp>,
    /// The windows touched, updated.
    pub windows: BTreeMap<String, WindowState>,
    /// Rows older than the allowed lateness, ignored (CQ-11).
    pub late_rows: u64,
    /// When the windows were rebuilt from the files because a log had
    /// been trimmed past the position held (section 11.7), if they were.
    pub rebuilt: Option<Timestamp>,
    /// The rules whose expressions could not be evaluated over the
    /// rows read, with why. Their windows are left as they were.
    pub failed: BTreeMap<String, String>,
}

/// Where a source task reads from.
pub struct Source<'a> {
    /// The root store, where the writer logs are.
    pub store: &'a Arc<dyn ObjectStore>,
    /// The keys sealed blocks are unlocked with (SE-2).
    pub keys: &'a Arc<Keys>,
    /// The engine a rebuild reads the consistent view through.
    pub engine: &'a Engine,
    /// The tenant.
    pub tenant: &'a TenantId,
    /// The rule group.
    pub group: &'a str,
    /// The state partition that reads.
    pub part: u32,
    /// How many state partitions the group has. With more than one,
    /// what is read of a writer is the partials the group's source
    /// tasks left for `part` (CQ-4), not the writer's log.
    pub parts: u32,
}

/// One rule as a source task folds it: its shape, and the horizon its
/// late rows are judged against.
#[derive(Clone, Copy)]
pub(crate) struct Folding<'a> {
    pub(crate) rule: &'a AlertRule,
    pub(crate) shape: &'a Shape,
    pub(crate) horizon: Timestamp,
}

/// Reads every writer's new segments once for all of `rules`, the
/// incremental rules of one group, and folds them into the windows the
/// state remembers. The rules share the positions in the logs (CQ-5),
/// so they are read together: an entry is behind the position for all
/// of them or for none. A log trimmed past the position held (GC-2)
/// means segments were lost, and so does the log of a writer that left
/// the registry gone whole before it was read to its end: the windows
/// of every rule are then rebuilt from the consistent view instead
/// (section 11.7) and the positions move to where the view read, so
/// the live logs resume there. A log read to its end is forgotten
/// once its writer has left the registry. A rule whose expressions
/// cannot be evaluated is reported in `failed` and holds no other back.
///
/// # Errors
///
/// If the registry, a segment or the view cannot be read.
pub async fn ingest(
    source: &Source<'_>,
    rules: &[(&AlertRule, &Shape)],
    state: &EvalState,
    now: Timestamp,
) -> Result<Ingested, String> {
    let mut ingested = Ingested {
        positions_before: state.positions.clone(),
        positions: state.positions.clone(),
        ended: state.ended.clone(),
        watermarks: state.watermarks.clone(),
        windows: BTreeMap::new(),
        late_rows: 0,
        rebuilt: None,
        failed: BTreeMap::new(),
    };
    // A dataset's horizon follows the writers registered for it.
    let mut horizons: BTreeMap<&DatasetName, Timestamp> = BTreeMap::new();
    let mut registered: BTreeSet<WriterId> = BTreeSet::new();
    for (_, shape) in rules {
        if horizons.contains_key(&shape.dataset) {
            continue;
        }
        let writers = registry::list(&**source.store, source.tenant, &shape.dataset)
            .await
            .map_err(|e| e.to_string())?;
        let names: Vec<String> = writers.iter().map(|r| r.writer.to_string()).collect();
        horizons.insert(&shape.dataset, watermark(state, &names, now));
        registered.extend(writers.into_iter().map(|r| r.writer));
    }
    let foldings: Vec<Folding<'_>> = rules
        .iter()
        .map(|&(rule, shape)| Folding {
            rule,
            shape,
            horizon: horizons[&shape.dataset],
        })
        .collect();
    let reading = Reading {
        source,
        rules: &foldings,
        state,
    };
    if read_logs(&reading, &registered, &mut ingested).await? == Read::Gap {
        rebuild(&reading, now, &mut ingested).await?;
    }
    let Ingested {
        windows, failed, ..
    } = &mut ingested;
    windows.retain(|_, window| !failed.contains_key(&window.rule));
    Ok(ingested)
}

/// Reads what is new of every writer: the `registered` ones, then the
/// ones the state holds a position of that left the registry. A gap is
/// a re-split that asked for the windows' rebuild, a log trimmed past
/// the position held, or one gone whole before its end was read.
async fn read_logs(
    reading: &Reading<'_>,
    registered: &BTreeSet<WriterId>,
    ingested: &mut Ingested,
) -> Result<Read, String> {
    let Reading {
        source,
        rules,
        state,
    } = *reading;
    if rules.iter().any(|f| state.rebuild.contains(&f.rule.name)) {
        tracing::info!(
            tenant = %source.tenant, group = %source.group,
            "rebuilding the windows from the files after a re-split"
        );
        return Ok(Read::Gap);
    }
    for writer in registered {
        let entries = available(source, *writer).await?;
        if read_new(reading, ingested, *writer, entries).await? == Read::Gap {
            return Ok(Read::Gap);
        }
    }
    // A writer that left the registry wrote its log to the end: read
    // to it, the log is forgotten; gone before that, it is a gap.
    for name in state.positions.keys() {
        let Ok(writer) = name.parse::<WriterId>() else {
            continue;
        };
        if registered.contains(&writer) {
            continue;
        }
        if state.ended.contains(name) {
            ingested.positions.remove(name);
            ingested.watermarks.remove(name);
            ingested.ended.remove(name);
            continue;
        }
        let entries = available(source, writer).await?;
        if entries.is_empty() {
            tracing::warn!(
                tenant = %source.tenant, group = %source.group, %writer,
                "writer log gone whole before its end was read; rebuilding the windows from the files"
            );
            ingested.positions.remove(name);
            ingested.watermarks.remove(name);
            return Ok(Read::Gap);
        }
        if read_new(reading, ingested, writer, entries).await? == Read::Gap {
            return Ok(Read::Gap);
        }
    }
    Ok(Read::Caught)
}

/// The positions `writer` can be read at: the entries of its log, or
/// in a group with several partitions the entries whose partials are
/// left for this one. The source task removes a log's partials with
/// its entries, so either listing is trimmed and gone as the log is.
async fn available(source: &Source<'_>, writer: WriterId) -> Result<Vec<Seq>, String> {
    if source.parts > 1 {
        let prefix = layout::eval_partials(
            source.tenant,
            source.group,
            source.parts,
            source.part,
            writer,
        );
        source::list(&**source.store, &prefix).await
    } else {
        reader::list(&**source.store, writer)
            .await
            .map_err(|e| e.to_string())
    }
}

/// What a source task reads for: the rules, each with its horizon,
/// and the state it starts from.
#[derive(Clone, Copy)]
struct Reading<'a> {
    source: &'a Source<'a>,
    rules: &'a [Folding<'a>],
    state: &'a EvalState,
}

/// What reading the writer logs found.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Read {
    /// Everything new, or nothing new.
    Caught,
    /// Entries past a position held are gone (GC-2), or the windows
    /// wait for a rebuild.
    Gap,
}

/// Reads the `entries` of `writer` past the position held, folding
/// them into the windows and remembering the log's end.
async fn read_new(
    reading: &Reading<'_>,
    ingested: &mut Ingested,
    writer: WriterId,
    entries: Vec<Seq>,
) -> Result<Read, String> {
    let Reading { source, state, .. } = *reading;
    let name = writer.to_string();
    let from = state.positions.get(&name).copied().unwrap_or(Seq::ZERO);
    let new: Vec<Seq> = entries.into_iter().filter(|seq| *seq > from).collect();
    if from != Seq::ZERO && new.first().is_some_and(|first| *first != from.next()) {
        tracing::warn!(
            tenant = %source.tenant, group = %source.group, %writer, held = %from, first = %new[0],
            "writer log trimmed past the position held; rebuilding the windows from the files"
        );
        return Ok(Read::Gap);
    }
    for seq in new {
        // An entry collected between the listing and the read is
        // found missing next time, as a gap.
        let found = if source.parts > 1 {
            read_partials(reading, ingested, writer, seq).await?
        } else {
            read_entry(reading, ingested, writer, seq).await?
        };
        if !found {
            break;
        }
    }
    Ok(Read::Caught)
}

/// Folds entry `seq` of `writer`'s log into the windows. False when
/// the entry is gone.
async fn read_entry(
    reading: &Reading<'_>,
    ingested: &mut Ingested,
    writer: WriterId,
    seq: Seq,
) -> Result<bool, String> {
    let Reading {
        source,
        rules,
        state,
    } = *reading;
    let mut entry = match reader::read(&**source.store, writer, seq).await {
        Ok(entry) => entry,
        Err(WalError::Store(ObjectError::NotFound { .. })) => return Ok(false),
        Err(error) => return Err(error.to_string()),
    };
    entry.unlock(source.keys).map_err(|e| e.to_string())?;
    let name = writer.to_string();
    ingested.positions.insert(name.clone(), seq);
    let blocks = match entry.body() {
        EntryBody::Segment { blocks } => blocks,
        EntryBody::End => {
            ingested.ended.insert(name);
            return Ok(true);
        }
        EntryBody::Flush { .. } | EntryBody::Seal { .. } => return Ok(true),
    };
    if let Some(newest) = blocks.iter().map(|b| b.max_observed).max() {
        let seen = ingested.watermarks.entry(name).or_insert(newest);
        *seen = (*seen).max(newest);
    }
    fold_entry(&entry, source.tenant, rules, state, ingested)?;
    Ok(true)
}

/// Folds the blocks `entry` holds for each of `rules` into the
/// windows, the blocks of a dataset and kind decoded once for the
/// rules that read them (CQ-2).
///
/// # Errors
///
/// If a block is corrupt.
pub(crate) fn fold_entry(
    entry: &Entry,
    tenant: &TenantId,
    rules: &[Folding<'_>],
    state: &EvalState,
    ingested: &mut Ingested,
) -> Result<(), String> {
    let mut decoded: Vec<(&DatasetName, Kind, Vec<EventBatch>)> = Vec::new();
    for folding in rules {
        let (dataset, kind) = (&folding.shape.dataset, folding.shape.kind);
        let found = decoded
            .iter()
            .position(|(d, k, _)| *d == dataset && *k == kind);
        let at = if let Some(at) = found {
            at
        } else {
            let blocks = entry
                .blocks_for(tenant, dataset, kind)
                .map_err(|e| e.to_string())?;
            decoded.push((dataset, kind, blocks));
            decoded.len() - 1
        };
        let rows = decoded[at].2.iter().map(EventBatch::rows);
        fold_rule(folding, folding.horizon, state, ingested, rows);
    }
    Ok(())
}

/// Folds `rows` into the windows of one rule, rows before `horizon`
/// being late. A rule whose expressions cannot be evaluated is marked
/// failed, and nothing more is folded for it.
fn fold_rule<'r>(
    folding: &Folding<'_>,
    horizon: Timestamp,
    state: &EvalState,
    ingested: &mut Ingested,
    rows: impl IntoIterator<Item = &'r RecordBatch>,
) {
    let Folding { rule, shape, .. } = *folding;
    if ingested.failed.contains_key(&rule.name) {
        return;
    }
    for batch in rows {
        if let Err(error) = fold_batch(rule, shape, state, ingested, batch, horizon) {
            ingested.failed.insert(rule.name.clone(), error);
            return;
        }
    }
}

/// Folds the partials the source task left for this partition of
/// entry `seq` of `writer`'s log into the windows. False when they are
/// gone. The source keeps every row, so lateness is judged here and by
/// the window: one the horizon has closed is late whole (CQ-11), and
/// one still open keeps the rows a single partition would have found
/// late one by one.
async fn read_partials(
    reading: &Reading<'_>,
    ingested: &mut Ingested,
    writer: WriterId,
    seq: Seq,
) -> Result<bool, String> {
    let Reading {
        source,
        rules,
        state,
    } = *reading;
    let key = layout::eval_partial(
        source.tenant,
        source.group,
        source.parts,
        source.part,
        writer,
        seq,
    );
    let Some(partials) = source::read(&**source.store, &key).await? else {
        return Ok(false);
    };
    let name = writer.to_string();
    ingested.positions.insert(name.clone(), seq);
    if partials.ended {
        ingested.ended.insert(name.clone());
    }
    if let Some(newest) = partials.max_observed {
        let seen = ingested.watermarks.entry(name).or_insert(newest);
        *seen = (*seen).max(newest);
    }
    for window in &partials.windows {
        let Some(Folding { shape, horizon, .. }) =
            rules.iter().find(|f| f.rule.name == window.rule)
        else {
            continue;
        };
        let length = i64::try_from(shape.window.as_nanos()).unwrap_or(i64::MAX);
        if window.start.as_nanos().saturating_add(length) <= horizon.as_nanos() {
            ingested.late_rows += window.partials.first().map_or(0, |p| p.count);
            continue;
        }
        // The window as the state remembers it, or empty, the first
        // time this read touches it.
        ingested
            .windows
            .entry(window.key())
            .or_insert_with_key(|key| {
                state
                    .windows
                    .get(key)
                    .cloned()
                    .unwrap_or_else(|| WindowState {
                        partials: vec![Partial::default(); shape.aggregates.len()],
                        received: None,
                        ..window.clone()
                    })
            })
            .merge(window);
    }
    Ok(true)
}

/// Rebuilds the windows of every rule from the consistent view
/// (section 11.7): every window still open at a rule's horizon is
/// folded again from scratch out of the rows the files and the tail
/// hold for it, and the positions move to where the view read each log
/// to. The rules of one dataset and kind are folded from one view.
/// Rules of different ones are not: where their views read a log to
/// different entries the position is the earlier, so that no rule
/// misses a row, and the later view's rules may count again what lies
/// between until their windows close; the degraded mark covers it.
async fn rebuild(
    reading: &Reading<'_>,
    now: Timestamp,
    ingested: &mut Ingested,
) -> Result<(), String> {
    let Reading { source, rules, .. } = *reading;
    // The oldest open window of a rule starts after its floor; rows
    // before it belong to closed windows and are not late, just not
    // needed.
    let floor = |folding: &Folding<'_>| {
        let length = i64::try_from(folding.shape.window.as_nanos()).unwrap_or(i64::MAX);
        Timestamp::from_nanos(folding.horizon.as_nanos().saturating_sub(length))
    };
    let fresh = EvalState::default();
    let late_before = ingested.late_rows;
    ingested.windows.clear();
    let mut positions: BTreeMap<WriterId, Seq> = BTreeMap::new();
    let mut seen: Vec<(&DatasetName, Kind)> = Vec::new();
    for first in rules {
        let (dataset, kind) = (&first.shape.dataset, first.shape.kind);
        if seen.contains(&(dataset, kind)) {
            continue;
        }
        seen.push((dataset, kind));
        let together: Vec<&Folding<'_>> = rules
            .iter()
            .filter(|f| f.shape.dataset == *dataset && f.shape.kind == kind)
            .collect();
        let earliest = together
            .iter()
            .map(|f| floor(f))
            .min()
            .unwrap_or(floor(first));
        let view = source
            .engine
            .view(
                source.tenant,
                dataset,
                kind,
                TimeRange::new(earliest, TimeRange::ALL.end),
            )
            .await
            .map_err(|e| e.to_string())?;
        for folding in together {
            fold_rule(folding, floor(folding), &fresh, ingested, &view.rows);
        }
        for (writer, seq) in view.sources.positions {
            positions
                .entry(writer)
                .and_modify(|held| *held = (*held).min(seq))
                .or_insert(seq);
        }
    }
    ingested.late_rows = late_before;
    for (writer, seq) in positions {
        ingested.positions.insert(writer.to_string(), seq);
    }
    ingested.rebuilt = Some(now);
    Ok(())
}

/// The degraded mark `rule` carries after this read (section 11.7):
/// when its windows were last rebuilt, kept until the horizon has
/// closed every window the rebuild could have touched, a window length
/// after it.
#[must_use]
pub fn degraded_mark(
    rule: &AlertRule,
    shape: &Shape,
    ingested: &Ingested,
    state: &EvalState,
    horizon: Timestamp,
) -> Option<Timestamp> {
    if let Some(at) = ingested.rebuilt {
        return Some(at);
    }
    let at = *state.degraded.get(&rule.name)?;
    let length = i64::try_from(shape.window.as_nanos()).unwrap_or(i64::MAX);
    let until = at.as_nanos().saturating_add(length);
    (horizon.as_nanos() < until).then_some(at)
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
pub(crate) fn fold_batch(
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
    let sample_rates = rows
        .column_by_name(SAMPLE_RATE)
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
    let observed = rows
        .column_by_name(OBSERVED_TIMESTAMP)
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>());
    let length = i64::try_from(shape.window.as_nanos()).unwrap_or(i64::MAX);
    let hop = shape
        .hop
        .map(|h| i64::try_from(h.as_nanos()).unwrap_or(i64::MAX));
    for row in 0..rows.num_rows() {
        let at = times.value(row);
        let weight = row_weight(sample_rates, row);
        if Timestamp::from_nanos(at) < horizon {
            ingested.late_rows += 1;
            continue;
        }
        let labels: BTreeMap<String, String> = shape
            .keys
            .iter()
            .zip(&keys)
            .filter_map(|((name, _), array)| cell(array, row).map(|v| (name.clone(), v)))
            .collect();
        let group = fingerprint(&labels);
        for start in window_starts(at, length, hop) {
            let key = window_key(&rule.name, start, &group);
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
                        received: None,
                    })
            });
            for (partial, arg) in window.partials.iter_mut().zip(&args) {
                partial.fold(arg.as_ref().and_then(|a| number(a, row)), weight);
            }
            if let Some(observed) = observed
                && !observed.is_null(row)
            {
                let at = Timestamp::from_nanos(observed.value(row));
                window.received = Some(window.received.map_or(at, |seen| seen.max(at)));
            }
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
    // Absence (CQ-7): a group whose newest window ended before the last
    // window the watermark has closed had no rows in that window, so it
    // is observed with an empty one, and a condition such as
    // `count(*) < 1` can hold. Only conditions that wait for the
    // watermark can be made true by an empty window.
    let mut candidates: Vec<WindowState> = newest.values().map(|w| (*w).clone()).collect();
    if shape.firing == Firing::Watermark {
        let hop = shape
            .hop
            .map_or(length, |h| i64::try_from(h.as_nanos()).unwrap_or(i64::MAX))
            .max(1);
        let last_closed = horizon.as_nanos().saturating_sub(length).div_euclid(hop) * hop;
        for window in newest.values() {
            if window.start.as_nanos() < last_closed {
                candidates.push(WindowState {
                    rule: rule.name.clone(),
                    start: Timestamp::from_nanos(last_closed),
                    keys: window.keys.clone(),
                    partials: vec![Partial::default(); shape.aggregates.len()],
                    received: None,
                });
            }
        }
    }
    let mut observations = Vec::new();
    for window in &candidates {
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
                received: window.received,
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

/// The actions that record where a read left the logs, once for all
/// the rules of a group: positions, watermarks and the logs read to
/// their end, and after a rebuild every rebuild a re-split asked for
/// done.
#[must_use]
pub fn position_actions(ingested: &Ingested, state: &EvalState) -> Vec<Action> {
    let mut actions = Vec::new();
    if ingested.rebuilt.is_some() {
        actions.extend(
            state
                .rebuild
                .iter()
                .map(|rule| EvalState::delete_rebuild(rule)),
        );
    }
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
    for writer in state.positions.keys() {
        if !ingested.positions.contains_key(writer) {
            actions.push(EvalState::delete_position(writer));
        }
    }
    for writer in state.watermarks.keys() {
        if !ingested.watermarks.contains_key(writer) {
            actions.push(EvalState::delete_watermark(writer));
        }
    }
    for writer in ingested.ended.difference(&state.ended) {
        actions.push(EvalState::put_ended(writer));
    }
    for writer in state.ended.difference(&ingested.ended) {
        actions.push(EvalState::delete_ended(writer));
    }
    actions
}

/// The actions that record what a read did to the windows of `rule`:
/// the windows touched, with windows older than two lengths before the
/// horizon forgotten, every old window after a rebuild, and the rule's
/// degraded mark set or cleared (section 11.7).
#[must_use]
pub fn window_actions(
    rule: &AlertRule,
    shape: &Shape,
    ingested: &Ingested,
    state: &EvalState,
    horizon: Timestamp,
) -> Vec<Action> {
    let mut actions = Vec::new();
    match (
        degraded_mark(rule, shape, ingested, state, horizon),
        state.degraded.get(&rule.name),
    ) {
        (Some(at), Some(was)) if *was == at => {}
        (Some(at), _) => actions.push(EvalState::put_degraded(&rule.name, at)),
        (None, Some(_)) => actions.push(EvalState::delete_degraded(&rule.name)),
        (None, None) => {}
    }
    for window in ingested.windows.values().filter(|w| w.rule == rule.name) {
        actions.push(EvalState::put_window(window));
    }
    let length = i64::try_from(shape.window.as_nanos()).unwrap_or(i64::MAX);
    let forget_before = horizon.as_nanos().saturating_sub(length.saturating_mul(2));
    for (key, window) in &state.windows {
        if window.rule == rule.name
            && (window.start.as_nanos() < forget_before || ingested.rebuilt.is_some())
            && !ingested.windows.contains_key(key)
        {
            actions.push(EvalState::delete_window(key));
        }
    }
    actions
}

#[cfg(test)]
mod tests {
    use tessera_types::{DatasetName, EventBatch, LogRow};

    use super::*;
    use crate::rule::Language;

    /// QE-13 in the incremental subset: a row counts and sums with the
    /// weight `1 / sample_rate` as a batch aggregate does, so that a
    /// continuous query equals the batch one (QE-20), while the raw
    /// forms take the rows as they are.
    #[test]
    fn rows_fold_weighted_by_their_sample_rate() {
        let sql = "SELECT count(*) AS n, count_raw(*) AS raw, sum(severity_number) AS s, \
                   sum_raw(severity_number) AS s_raw, avg(severity_number) AS a FROM logs \
                   GROUP BY tumble(timestamp, INTERVAL '1 minute') HAVING count(*) >= 1";
        let shape = shape_of_sql(sql, "prod", &TenantId::DEFAULT, None).unwrap();
        let at = Timestamp::from_nanos(1_700_000_000_000_000_000);
        let rows: Vec<LogRow> = [(1u8, 2), (2, 4), (3, 6)]
            .into_iter()
            .map(|(i, severity)| LogRow {
                timestamp: at,
                observed_timestamp: at,
                event_id: [i; 16],
                severity_number: Some(severity),
                ..LogRow::default()
            })
            .collect();
        let batch =
            EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &rows)
                .unwrap();
        let rows = batch.rows();
        let index = rows.schema().index_of(SAMPLE_RATE).unwrap();
        let mut columns = rows.columns().to_vec();
        columns[index] = Arc::new(Float64Array::from(vec![0.5, 0.25, 1.0]));
        let rows = RecordBatch::try_new(rows.schema(), columns).unwrap();
        let rule = AlertRule::new("weighted", Language::Sql, sql);
        let mut ingested = Ingested::default();
        fold_batch(
            &rule,
            &shape,
            &EvalState::default(),
            &mut ingested,
            &rows,
            Timestamp::from_nanos(0),
        )
        .unwrap();
        let window = ingested.windows.values().next().unwrap();
        let value = |i: usize| window.partials[i].value(&shape.aggregates[i].function);
        assert!((value(0) - 7.0).abs() < f64::EPSILON, "count: 2 + 4 + 1");
        assert!(
            (value(1) - 3.0).abs() < f64::EPSILON,
            "count_raw: three rows"
        );
        assert!(
            (value(2) - 26.0).abs() < f64::EPSILON,
            "sum: 2*2 + 4*4 + 6*1"
        );
        assert!((value(3) - 12.0).abs() < f64::EPSILON, "sum_raw: 2 + 4 + 6");
        assert!((value(4) - 26.0 / 7.0).abs() < 1e-9, "avg over the weights");
        let old = Partial {
            count: 3,
            weight: None,
            sum: 12.0,
            sum_raw: 0.0,
            min: 2.0,
            max: 6.0,
        };
        assert!(
            (old.value("count") - 3.0).abs() < f64::EPSILON,
            "an old state counts rows"
        );
        let mut grown = old;
        grown.fold(Some(1.0), 4.0);
        assert!((grown.value("count") - 7.0).abs() < f64::EPSILON);
    }

    /// CQ-4: the partials a source task folds from separate entries
    /// merge into what one fold of all their rows gives.
    #[test]
    fn merged_partials_equal_the_rows_folded_together() {
        let rows = [
            (Some(4.0), 2.0),
            (None, 1.0),
            (Some(-1.5), 4.0),
            (Some(9.0), 1.0),
        ];
        let fold = |rows: &[(Option<f64>, f64)]| {
            let mut partial = Partial::default();
            for (value, weight) in rows {
                partial.fold(*value, *weight);
            }
            partial
        };
        let whole = fold(&rows);
        for cut in 0..=rows.len() {
            let mut merged = fold(&rows[..cut]);
            merged.merge(&fold(&rows[cut..]));
            assert_eq!(merged, whole, "cut at {cut}");
        }
        let mut window = WindowState {
            rule: "r".into(),
            start: Timestamp::from_nanos(0),
            keys: BTreeMap::new(),
            partials: vec![fold(&rows[..2])],
            received: Some(Timestamp::from_nanos(7)),
        };
        window.merge(&WindowState {
            partials: vec![fold(&rows[2..])],
            received: Some(Timestamp::from_nanos(5)),
            ..window.clone()
        });
        assert_eq!(window.partials, [whole]);
        assert_eq!(window.received, Some(Timestamp::from_nanos(7)));
    }

    /// Section 17.1: a window remembers when the last of its rows was
    /// received, which its observations carry on to the transitions.
    #[test]
    fn a_window_remembers_when_its_last_row_was_received() {
        let sql = "SELECT count(*) AS n FROM logs \
                   GROUP BY tumble(timestamp, INTERVAL '1 minute') HAVING count(*) >= 1";
        let shape = shape_of_sql(sql, "prod", &TenantId::DEFAULT, None).unwrap();
        let at = Timestamp::from_nanos(1_700_000_000_000_000_000);
        let rows: Vec<LogRow> = [1u8, 2, 3]
            .into_iter()
            .map(|i| LogRow {
                timestamp: at,
                observed_timestamp: Timestamp::from_nanos(at.as_nanos() + i64::from(i) * 7),
                event_id: [i; 16],
                ..LogRow::default()
            })
            .collect();
        let batch =
            EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &rows)
                .unwrap();
        let rule = AlertRule::new("timed", Language::Sql, sql);
        let mut ingested = Ingested::default();
        fold_batch(
            &rule,
            &shape,
            &EvalState::default(),
            &mut ingested,
            batch.rows(),
            Timestamp::from_nanos(0),
        )
        .unwrap();
        let window = ingested.windows.values().next().unwrap();
        let newest = Timestamp::from_nanos(at.as_nanos() + 21);
        assert_eq!(window.received, Some(newest));
        let observed = observe(&rule, &shape, &ingested.windows, Timestamp::MAX).unwrap();
        assert_eq!(observed[0].received, Some(newest));
    }
}
