// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The operator tree (spec section 10.2).
//!
//! Operators arrive with the milestone that needs them; the tree holds
//! only what the engine can run. Every node answers its time range and
//! its ordering (QE-3) from its inputs, so no rewrite can forget them.
//! [`Plan`] displays as the text of `EXPLAIN` (QE-4).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};
use tessera_types::event::{EXTRACTED, SAMPLE_RATE, TIMESTAMP};
use tessera_types::{DatasetName, Kind, TenantId, TimeRange, Timestamp, event};

use crate::expr::{Expr, Literal};
use crate::function::Registry;
use crate::promql::PromExpr;
use crate::trace::{Spanset, trace_group_schema};
use crate::typing::{TypeError, expr_type};

/// Why a plan is not well formed.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum PlanError {
    /// A `system` table the planner does not know.
    #[error("unknown system table {0}")]
    UnknownSystemTable(String),
    /// An expression has no type over its input.
    #[error("{0}")]
    Type(#[from] TypeError),
    /// A projection names a column the scan does not have.
    #[error("scan projects unknown column {0}")]
    UnknownProjection(String),
    /// The inputs of a union differ in schema.
    #[error("union inputs have different schemas")]
    UnionMismatch,
    /// A union has no inputs.
    #[error("union has no inputs")]
    EmptyUnion,
}

/// An expression with the name it is output under.
#[derive(Clone, PartialEq, Debug)]
pub struct NamedExpr {
    /// The expression.
    pub expr: Expr,
    /// The output column name.
    pub name: String,
}

impl NamedExpr {
    /// `expr AS name`.
    #[must_use]
    pub fn new(expr: Expr, name: impl Into<String>) -> Self {
        NamedExpr {
            expr,
            name: name.into(),
        }
    }
}

/// An aggregate function application.
#[derive(Clone, PartialEq, Debug)]
pub struct AggregateExpr {
    /// The function, by registry name.
    pub function: String,
    /// Its arguments; empty for `count(*)`.
    pub args: Vec<Expr>,
    /// The output column name.
    pub name: String,
}

/// One key of an ordering.
#[derive(Clone, PartialEq, Debug)]
pub struct SortKey {
    /// The sorted expression.
    pub expr: Expr,
    /// Largest first when true.
    pub descending: bool,
}

impl SortKey {
    /// Ascending on `expr`.
    #[must_use]
    pub fn ascending(expr: Expr) -> Self {
        SortKey {
            expr,
            descending: false,
        }
    }

    /// Descending on `expr`.
    #[must_use]
    pub fn descending(expr: Expr) -> Self {
        SortKey {
            expr,
            descending: true,
        }
    }
}

impl fmt::Display for SortKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let direction = if self.descending { "DESC" } else { "ASC" };
        write!(f, "{} {direction}", self.expr)
    }
}

/// A read of one dataset and kind with everything pushed down into it.
/// It serializes, so that a leaf task can be sent to another node
/// (QE-14).
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct Scan {
    /// The tenant.
    pub tenant: TenantId,
    /// The dataset.
    pub dataset: DatasetName,
    /// The kind of event.
    pub kind: Kind,
    /// Rows outside this range are not read.
    pub time_range: TimeRange,
    /// Predicates every output row satisfies, as a conjunction.
    pub predicates: Vec<Expr>,
    /// The columns to read, or every column.
    pub projection: Option<Vec<String>>,
    /// Stop after this many rows, read newest first (the time-ordered
    /// top-K scan of section 10.6).
    pub newest_first_limit: Option<usize>,
}

impl Scan {
    /// A scan of every row of `kind` in `dataset`.
    #[must_use]
    pub fn all(tenant: TenantId, dataset: DatasetName, kind: Kind) -> Self {
        Scan {
            tenant,
            dataset,
            kind,
            time_range: TimeRange::ALL,
            predicates: Vec::new(),
            projection: None,
            newest_first_limit: None,
        }
    }

    /// The schema of the rows the scan produces.
    ///
    /// # Errors
    ///
    /// `UnknownProjection` if the projection names a column the kind does
    /// not have.
    pub fn schema(&self) -> Result<SchemaRef, PlanError> {
        let full = event::schema(self.kind);
        match &self.projection {
            None => Ok(full),
            Some(names) => {
                let fields = names
                    .iter()
                    .map(|name| {
                        full.field_with_name(name)
                            .cloned()
                            .map_err(|_| PlanError::UnknownProjection(name.clone()))
                    })
                    .collect::<Result<Vec<Field>, _>>()?;
                Ok(Arc::new(Schema::new(fields)))
            }
        }
    }
}

/// How a parser stage derives labels from text (`Extract`, section
/// 10.2; `LogQL` section 9.4).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ExtractStage {
    /// A JSON object: every property becomes a label, nested objects
    /// flattened with `_`.
    Json,
    /// `key=value` pairs.
    Logfmt,
    /// A regular expression with named groups.
    Regexp(String),
    /// A pattern with `<name>` captures between literal text.
    Pattern(String),
    /// A JSON object whose `_entry` property replaces the line and whose
    /// other properties become labels.
    Unpack,
}

impl fmt::Display for ExtractStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExtractStage::Json => write!(f, "json"),
            ExtractStage::Logfmt => write!(f, "logfmt"),
            ExtractStage::Regexp(pattern) => write!(f, "regexp {pattern:?}"),
            ExtractStage::Pattern(pattern) => write!(f, "pattern {pattern:?}"),
            ExtractStage::Unpack => write!(f, "unpack"),
        }
    }
}

/// A function over the samples of one series in a sliding window
/// (`RangeVector`, section 10.2), with `LogQL` and `PromQL` semantics.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum RangeFunction {
    /// Samples in the window.
    CountOverTime,
    /// Samples per second.
    Rate,
    /// Bytes of the lines in the window.
    BytesOverTime,
    /// Bytes per second.
    BytesRate,
    /// Sum of the unwrapped values.
    SumOverTime,
    /// Mean of the unwrapped values.
    AvgOverTime,
    /// Smallest unwrapped value.
    MinOverTime,
    /// Largest unwrapped value.
    MaxOverTime,
    /// The oldest unwrapped value.
    FirstOverTime,
    /// The newest unwrapped value.
    LastOverTime,
    /// Population standard deviation of the unwrapped values.
    StddevOverTime,
    /// Population variance of the unwrapped values.
    StdvarOverTime,
    /// The given quantile of the unwrapped values.
    QuantileOverTime(f64),
    /// 1 where the window holds no sample.
    AbsentOverTime,
}

impl fmt::Display for RangeFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RangeFunction::CountOverTime => write!(f, "count_over_time"),
            RangeFunction::Rate => write!(f, "rate"),
            RangeFunction::BytesOverTime => write!(f, "bytes_over_time"),
            RangeFunction::BytesRate => write!(f, "bytes_rate"),
            RangeFunction::SumOverTime => write!(f, "sum_over_time"),
            RangeFunction::AvgOverTime => write!(f, "avg_over_time"),
            RangeFunction::MinOverTime => write!(f, "min_over_time"),
            RangeFunction::MaxOverTime => write!(f, "max_over_time"),
            RangeFunction::FirstOverTime => write!(f, "first_over_time"),
            RangeFunction::LastOverTime => write!(f, "last_over_time"),
            RangeFunction::StddevOverTime => write!(f, "stddev_over_time"),
            RangeFunction::StdvarOverTime => write!(f, "stdvar_over_time"),
            RangeFunction::QuantileOverTime(q) => write!(f, "quantile_over_time({q})"),
            RangeFunction::AbsentOverTime => write!(f, "absent_over_time"),
        }
    }
}

/// Which labels identify a series in a range vector: `by` keeps the
/// named labels, `without` drops them. Samples of rows whose remaining
/// labels agree merge into one series, as `LogQL` groups range functions.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SeriesGrouping {
    /// `by` when true, `without` otherwise.
    pub by: bool,
    /// The labels.
    pub labels: Vec<String>,
}

impl fmt::Display for SeriesGrouping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let word = if self.by { "by" } else { "without" };
        write!(f, "{word} ({})", self.labels.join(", "))
    }
}

/// The output column of a range vector: the step time.
pub const VALUE: &str = "value";

/// The schema every `RangeVector` produces: the step `timestamp`, the
/// series labels in `_extracted`, and `value`.
#[must_use]
pub fn range_vector_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            TIMESTAMP,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(EXTRACTED, DataType::Utf8, true),
        Field::new(VALUE, DataType::Float64, true),
    ]))
}

/// A node of the operator tree.
#[derive(Clone, PartialEq, Debug)]
pub enum Plan {
    /// Read a dataset.
    Scan(Scan),
    /// Keep the rows where `predicate` holds.
    Filter {
        /// The input.
        input: Box<Plan>,
        /// The condition.
        predicate: Expr,
    },
    /// Compute output columns from each input row.
    Project {
        /// The input.
        input: Box<Plan>,
        /// The output columns.
        exprs: Vec<NamedExpr>,
    },
    /// Group rows and aggregate each group. With no keys, one group.
    Aggregate {
        /// The input.
        input: Box<Plan>,
        /// The grouping keys, output first.
        group_by: Vec<NamedExpr>,
        /// The aggregates, output after the keys.
        aggregates: Vec<AggregateExpr>,
    },
    /// Order the rows.
    Sort {
        /// The input.
        input: Box<Plan>,
        /// The ordering.
        keys: Vec<SortKey>,
    },
    /// Skip `offset` rows and keep at most `count`.
    Limit {
        /// The input.
        input: Box<Plan>,
        /// Rows to skip.
        offset: usize,
        /// Rows to keep, or every remaining row.
        count: Option<usize>,
    },
    /// The first `k` rows under an ordering, found without a full sort.
    TopK {
        /// The input.
        input: Box<Plan>,
        /// The ordering.
        keys: Vec<SortKey>,
        /// How many rows to keep.
        k: usize,
    },
    /// The rows of every input, which share one schema.
    Union {
        /// The inputs.
        inputs: Vec<Plan>,
    },
    /// Derive labels from text: parse `source` by `stage` and add the
    /// results to the `_extracted` column, later stages overriding
    /// earlier labels of the same name.
    Extract {
        /// The input.
        input: Box<Plan>,
        /// The text to parse.
        source: Expr,
        /// How to parse it.
        stage: ExtractStage,
    },
    /// Rewrite each row's line and labels: `LogQL` `line_format`,
    /// `label_format`, `drop` and `keep`. Templates use `{{.label}}` and
    /// `{{__line__}}`.
    Format {
        /// The input.
        input: Box<Plan>,
        /// A template for the new line, if the line changes.
        line: Option<String>,
        /// Labels to set, each from a template.
        labels: Vec<(String, String)>,
        /// Labels to remove.
        drop: Vec<String>,
        /// The only labels to keep, when set.
        keep: Option<Vec<String>>,
    },
    /// Per-series sliding windows (section 10.6): the input rows are
    /// grouped by their full label set, and `function` is evaluated at
    /// every step from `start` to `end` over `(t - range, t]`.
    RangeVector {
        /// The input rows.
        input: Box<Plan>,
        /// The function.
        function: RangeFunction,
        /// The unwrapped value for the functions that need one; `None`
        /// counts rows or measures lines.
        value: Option<Expr>,
        /// The window length.
        range: Duration,
        /// The distance between steps.
        step: Duration,
        /// The first step.
        start: Timestamp,
        /// The last step, inclusive.
        end: Timestamp,
        /// Which labels identify a series; every label when `None`.
        grouping: Option<SeriesGrouping>,
    },
    /// Constant rows, for queries without a table.
    Values {
        /// The schema of the rows.
        schema: SchemaRef,
        /// The rows, one literal per column.
        rows: Vec<Vec<Literal>>,
    },
    /// The rows of a `system` table of a tenant (CS-3): the control
    /// store's or the rule state logs', provided at run time.
    SystemTable {
        /// The tenant.
        tenant: TenantId,
        /// The table, a name of [`crate::system::TABLES`].
        table: String,
    },
    /// Evaluate a `PromQL` expression over the samples of its inputs at
    /// every step from `start` to `end` with Prometheus' semantics
    /// (QL-5); one row per series and step, like `RangeVector`.
    PromQL {
        /// The inputs, one per selector, each producing samples with
        /// their labels.
        inputs: Vec<Plan>,
        /// The expression, referring to inputs by index.
        expr: PromExpr,
        /// The first step.
        start: Timestamp,
        /// The last step, inclusive.
        end: Timestamp,
        /// The distance between steps; one step when zero.
        step: Duration,
    },
    /// Group spans by trace and evaluate a spanset expression on each
    /// trace (QL-13). The engine reads the trace ids of `candidates`,
    /// then every span of those traces from `spans`.
    TraceGroup {
        /// The spans that make their trace a candidate; every trace is
        /// one when `None`.
        candidates: Option<Box<Plan>>,
        /// Every span of the candidate traces.
        spans: Box<Plan>,
        /// The expression evaluated on each trace.
        spanset: Spanset,
    },
}

impl Plan {
    /// Wraps `self` in a filter.
    #[must_use]
    pub fn filter(self, predicate: Expr) -> Self {
        Plan::Filter {
            input: Box::new(self),
            predicate,
        }
    }

    /// Wraps `self` in a projection.
    #[must_use]
    pub fn project(self, exprs: Vec<NamedExpr>) -> Self {
        Plan::Project {
            input: Box::new(self),
            exprs,
        }
    }

    /// Wraps `self` in a sort.
    #[must_use]
    pub fn sort(self, keys: Vec<SortKey>) -> Self {
        Plan::Sort {
            input: Box::new(self),
            keys,
        }
    }

    /// Wraps `self` in a limit.
    #[must_use]
    pub fn limit(self, offset: usize, count: Option<usize>) -> Self {
        Plan::Limit {
            input: Box::new(self),
            offset,
            count,
        }
    }

    /// The inputs of the node, in order.
    #[must_use]
    pub fn inputs(&self) -> Vec<&Plan> {
        match self {
            Plan::Scan(_) | Plan::Values { .. } | Plan::SystemTable { .. } => Vec::new(),
            Plan::Filter { input, .. }
            | Plan::Project { input, .. }
            | Plan::Aggregate { input, .. }
            | Plan::Sort { input, .. }
            | Plan::Limit { input, .. }
            | Plan::TopK { input, .. }
            | Plan::Extract { input, .. }
            | Plan::Format { input, .. }
            | Plan::RangeVector { input, .. } => vec![input],
            Plan::Union { inputs } | Plan::PromQL { inputs, .. } => inputs.iter().collect(),
            Plan::TraceGroup {
                candidates, spans, ..
            } => candidates
                .iter()
                .map(AsRef::as_ref)
                .chain(std::iter::once(spans.as_ref()))
                .collect(),
        }
    }

    /// The event time the node's rows can lie in (QE-3): the union of its
    /// inputs' ranges, or the scan's own.
    #[must_use]
    pub fn time_range(&self) -> TimeRange {
        match self {
            Plan::Scan(scan) => scan.time_range,
            Plan::Values { .. } | Plan::SystemTable { .. } => TimeRange::ALL,
            Plan::Union { inputs } | Plan::PromQL { inputs, .. } => inputs
                .iter()
                .map(Plan::time_range)
                .reduce(|a, b| TimeRange::new(a.start.min(b.start), a.end.max(b.end)))
                .unwrap_or(TimeRange::ALL),
            Plan::TraceGroup { spans, .. } => spans.time_range(),
            other => other.inputs()[0].time_range(),
        }
    }

    /// The ordering the node's rows are known to have (QE-3). Empty when
    /// the order is unspecified.
    #[must_use]
    pub fn ordering(&self) -> Vec<SortKey> {
        match self {
            Plan::Scan(scan) => match scan.newest_first_limit {
                Some(_) => vec![SortKey::descending(Expr::column(TIMESTAMP))],
                None => Vec::new(),
            },
            Plan::Sort { keys, .. } | Plan::TopK { keys, .. } => keys.clone(),
            Plan::Filter { input, .. }
            | Plan::Limit { input, .. }
            | Plan::Extract { input, .. }
            | Plan::Format { input, .. } => input.ordering(),
            Plan::Project { input, exprs } => {
                // The ordering survives a projection that passes its keys through
                // unchanged.
                let keys = input.ordering();
                let passed = keys.iter().all(|k| {
                    exprs
                        .iter()
                        .any(|e| e.expr == k.expr && e.name == k.expr.to_string())
                });
                if passed { keys } else { Vec::new() }
            }
            Plan::Aggregate { .. }
            | Plan::Union { .. }
            | Plan::Values { .. }
            | Plan::SystemTable { .. }
            | Plan::RangeVector { .. }
            | Plan::PromQL { .. }
            | Plan::TraceGroup { .. } => Vec::new(),
        }
    }

    /// Whether the node's rows still carry `sample_rate`, so that an
    /// aggregate above it is weighted (QE-13).
    ///
    /// # Errors
    ///
    /// If the plan is not well formed.
    pub fn weighted(&self, registry: &Registry) -> Result<bool, PlanError> {
        Ok(self.schema(registry)?.field_with_name(SAMPLE_RATE).is_ok())
    }

    /// The schema of the node's rows.
    ///
    /// # Errors
    ///
    /// If an expression has no type, a projection or union is malformed.
    pub fn schema(&self, registry: &Registry) -> Result<SchemaRef, PlanError> {
        match self {
            Plan::Scan(scan) => scan.schema(),
            Plan::Values { schema, .. } => Ok(Arc::clone(schema)),
            Plan::SystemTable { table, .. } => crate::system::system_table_schema(table)
                .ok_or_else(|| PlanError::UnknownSystemTable(table.clone())),
            Plan::Filter { input, .. }
            | Plan::Sort { input, .. }
            | Plan::Limit { input, .. }
            | Plan::TopK { input, .. } => input.schema(registry),
            Plan::Extract { input, .. } => Ok(with_extracted(&input.schema(registry)?)),
            Plan::RangeVector { .. } | Plan::PromQL { .. } => Ok(range_vector_schema()),
            Plan::TraceGroup { .. } => Ok(trace_group_schema()),
            Plan::Format { input, labels, .. } => {
                let input = input.schema(registry)?;
                Ok(if labels.is_empty() {
                    input
                } else {
                    with_extracted(&input)
                })
            }
            Plan::Project { input, exprs } => {
                let input = input.schema(registry)?;
                let fields = exprs
                    .iter()
                    .map(|e| {
                        expr_type(&e.expr, &input, registry)
                            .map(|t| Field::new(&e.name, t, true))
                            .map_err(PlanError::Type)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Arc::new(Schema::new(fields)))
            }
            Plan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let input = input.schema(registry)?;
                let mut fields = Vec::with_capacity(group_by.len() + aggregates.len());
                for key in group_by {
                    let t = expr_type(&key.expr, &input, registry)?;
                    fields.push(Field::new(&key.name, t, true));
                }
                for aggregate in aggregates {
                    fields.push(Field::new(
                        &aggregate.name,
                        aggregate_type(aggregate, &input, registry)?,
                        true,
                    ));
                }
                Ok(Arc::new(Schema::new(fields)))
            }
            Plan::Union { inputs } => {
                let first = inputs.first().ok_or(PlanError::EmptyUnion)?;
                let schema = first.schema(registry)?;
                for input in &inputs[1..] {
                    if input.schema(registry)? != schema {
                        return Err(PlanError::UnionMismatch);
                    }
                }
                Ok(schema)
            }
        }
    }

    fn fmt_tree(&self, f: &mut fmt::Formatter<'_>, depth: usize) -> fmt::Result {
        let pad = "  ".repeat(depth);
        match self {
            Plan::Scan(scan) => fmt_scan(f, &pad, scan),
            Plan::Union { inputs } => {
                writeln!(f, "{pad}Union")?;
                inputs.iter().try_for_each(|i| i.fmt_tree(f, depth + 1))
            }
            Plan::Values { rows, .. } => writeln!(f, "{pad}Values: {} rows", rows.len()),
            Plan::SystemTable { tenant, table } => {
                writeln!(f, "{pad}SystemTable: {tenant} system.{table}")
            }
            Plan::TraceGroup {
                candidates,
                spans,
                spanset,
            } => fmt_trace_group(f, depth, candidates.as_deref(), spans, spanset),
            Plan::PromQL {
                inputs,
                expr,
                start,
                end,
                step,
            } => fmt_promql(
                f,
                depth,
                inputs,
                &format!("{expr} from {start} to {end} step {}s", step.as_secs_f64()),
            ),
            single => {
                single.fmt_line(f, &pad)?;
                single.inputs()[0].fmt_tree(f, depth + 1)
            }
        }
    }

    /// The line of a node with one input, which `fmt_tree` follows with
    /// the input.
    fn fmt_line(&self, f: &mut fmt::Formatter<'_>, pad: &str) -> fmt::Result {
        match self {
            Plan::Filter { predicate, .. } => writeln!(f, "{pad}Filter: {predicate}"),
            Plan::Project { exprs, .. } => fmt_project(f, pad, exprs),
            Plan::Aggregate {
                group_by,
                aggregates,
                ..
            } => fmt_aggregate(f, pad, group_by, aggregates),
            Plan::Sort { keys, .. } => {
                let shown: Vec<String> = keys.iter().map(ToString::to_string).collect();
                writeln!(f, "{pad}Sort: {}", shown.join(", "))
            }
            Plan::Limit { offset, count, .. } => fmt_limit(f, pad, *offset, *count),
            Plan::TopK { keys, k, .. } => {
                let shown: Vec<String> = keys.iter().map(ToString::to_string).collect();
                writeln!(f, "{pad}TopK: k={k} order=[{}]", shown.join(", "))
            }
            Plan::Extract { source, stage, .. } => {
                writeln!(f, "{pad}Extract: {stage} from {source}")
            }
            Plan::Format {
                line,
                labels,
                drop,
                keep,
                ..
            } => fmt_format(f, pad, line.as_deref(), labels, drop, keep.as_deref()),
            Plan::RangeVector {
                function,
                value,
                range,
                step,
                start,
                end,
                grouping,
                ..
            } => {
                fmt_range_vector(
                    f,
                    pad,
                    *function,
                    value.as_ref(),
                    *range,
                    *step,
                    *start,
                    *end,
                )?;
                match grouping {
                    Some(grouping) => writeln!(f, "{pad}  series {grouping}"),
                    None => Ok(()),
                }
            }
            Plan::Scan(_)
            | Plan::Union { .. }
            | Plan::Values { .. }
            | Plan::SystemTable { .. }
            | Plan::TraceGroup { .. }
            | Plan::PromQL { .. } => Ok(()),
        }
    }
}

/// The `PromQL` lines of `EXPLAIN`: the expression, then each input
/// under its index.
fn fmt_promql(
    f: &mut fmt::Formatter<'_>,
    depth: usize,
    inputs: &[Plan],
    header: &str,
) -> fmt::Result {
    let pad = "  ".repeat(depth);
    writeln!(f, "{pad}PromQL: {header}")?;
    for (i, input) in inputs.iter().enumerate() {
        writeln!(f, "{pad}  input{i}:")?;
        input.fmt_tree(f, depth + 2)?;
    }
    Ok(())
}

/// The `TraceGroup` lines of `EXPLAIN`: the expression, then each scan
/// under its role.
fn fmt_trace_group(
    f: &mut fmt::Formatter<'_>,
    depth: usize,
    candidates: Option<&Plan>,
    spans: &Plan,
    spanset: &Spanset,
) -> fmt::Result {
    let pad = "  ".repeat(depth);
    writeln!(f, "{pad}TraceGroup: {spanset}")?;
    if let Some(candidates) = candidates {
        writeln!(f, "{pad}  candidates:")?;
        candidates.fmt_tree(f, depth + 2)?;
    }
    writeln!(f, "{pad}  spans:")?;
    spans.fmt_tree(f, depth + 2)
}

/// The `Limit` line of `EXPLAIN`.
fn fmt_limit(
    f: &mut fmt::Formatter<'_>,
    pad: &str,
    offset: usize,
    count: Option<usize>,
) -> fmt::Result {
    match count {
        Some(count) => writeln!(f, "{pad}Limit: offset={offset} count={count}"),
        None => writeln!(f, "{pad}Limit: offset={offset}"),
    }
}

/// The `Project` line of `EXPLAIN`.
fn fmt_project(f: &mut fmt::Formatter<'_>, pad: &str, exprs: &[NamedExpr]) -> fmt::Result {
    let shown: Vec<String> = exprs
        .iter()
        .map(|e| format!("{} AS {}", e.expr, e.name))
        .collect();
    writeln!(f, "{pad}Project: {}", shown.join(", "))
}

/// The `Scan` line of `EXPLAIN`.
fn fmt_scan(f: &mut fmt::Formatter<'_>, pad: &str, scan: &Scan) -> fmt::Result {
    write!(
        f,
        "{pad}Scan: {}/{} {} {}",
        scan.dataset, scan.kind, scan.time_range, scan.tenant
    )?;
    if !scan.predicates.is_empty() {
        let shown: Vec<String> = scan.predicates.iter().map(ToString::to_string).collect();
        write!(f, " predicates=[{}]", shown.join(", "))?;
    }
    if let Some(columns) = &scan.projection {
        write!(f, " columns=[{}]", columns.join(", "))?;
    }
    if let Some(limit) = scan.newest_first_limit {
        write!(f, " newest_first_limit={limit}")?;
    }
    writeln!(f)
}

/// The `Aggregate` line of `EXPLAIN`.
fn fmt_aggregate(
    f: &mut fmt::Formatter<'_>,
    pad: &str,
    group_by: &[NamedExpr],
    aggregates: &[AggregateExpr],
) -> fmt::Result {
    let keys: Vec<String> = group_by
        .iter()
        .map(|e| format!("{} AS {}", e.expr, e.name))
        .collect();
    let aggs: Vec<String> = aggregates
        .iter()
        .map(|a| {
            let args: Vec<String> = a.args.iter().map(ToString::to_string).collect();
            format!("{}({}) AS {}", a.function, args.join(", "), a.name)
        })
        .collect();
    writeln!(
        f,
        "{pad}Aggregate: group_by=[{}] aggregates=[{}]",
        keys.join(", "),
        aggs.join(", ")
    )
}

/// The `Format` line of `EXPLAIN`.
fn fmt_format(
    f: &mut fmt::Formatter<'_>,
    pad: &str,
    line: Option<&str>,
    labels: &[(String, String)],
    drop: &[String],
    keep: Option<&[String]>,
) -> fmt::Result {
    write!(f, "{pad}Format:")?;
    if let Some(line) = line {
        write!(f, " line={line:?}")?;
    }
    for (name, template) in labels {
        write!(f, " {name}={template:?}")?;
    }
    if !drop.is_empty() {
        write!(f, " drop=[{}]", drop.join(", "))?;
    }
    if let Some(keep) = keep {
        write!(f, " keep=[{}]", keep.join(", "))?;
    }
    writeln!(f)
}

/// The `RangeVector` line of `EXPLAIN`.
#[allow(clippy::too_many_arguments)]
fn fmt_range_vector(
    f: &mut fmt::Formatter<'_>,
    pad: &str,
    function: RangeFunction,
    value: Option<&Expr>,
    range: Duration,
    step: Duration,
    start: Timestamp,
    end: Timestamp,
) -> fmt::Result {
    write!(f, "{pad}RangeVector: {function}")?;
    if let Some(value) = value {
        write!(f, " of {value}")?;
    }
    writeln!(
        f,
        " range={}s step={}s steps=[{start}, {end}]",
        range.as_secs_f64(),
        step.as_secs_f64()
    )
}

/// `schema` with the `_extracted` column, added when absent.
fn with_extracted(schema: &SchemaRef) -> SchemaRef {
    if schema.field_with_name(EXTRACTED).is_ok() {
        return Arc::clone(schema);
    }
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(EXTRACTED, DataType::Utf8, true));
    Arc::new(Schema::new(fields))
}

/// The result type of an aggregate over `input`.
///
/// # Errors
///
/// If the function is unknown or its arguments do not fit.
pub fn aggregate_type(
    aggregate: &AggregateExpr,
    input: &Schema,
    registry: &Registry,
) -> Result<DataType, PlanError> {
    let function = registry
        .aggregate(&aggregate.function)
        .ok_or_else(|| TypeError::UnknownAggregate(aggregate.function.clone()))?;
    let types = aggregate
        .args
        .iter()
        .map(|a| expr_type(a, input, registry))
        .collect::<Result<Vec<_>, _>>()?;
    (function.return_type)(&types).ok_or_else(|| {
        let shown: Vec<String> = types.iter().map(ToString::to_string).collect();
        TypeError::Mismatch(format!(
            "{} does not accept ({})",
            aggregate.function,
            shown.join(", ")
        ))
        .into()
    })
}

impl fmt::Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_tree(f, 0)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::TimeUnit;
    use tessera_types::Timestamp;
    use tessera_types::event::{BODY, SEVERITY_NUMBER};

    use super::*;
    use crate::expr::BinaryOp;

    fn scan() -> Scan {
        Scan::all(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Kind::Log,
        )
    }

    fn registry() -> Registry {
        Registry::builtin()
    }

    #[test]
    fn a_scan_schema_is_the_kind_schema_projected() {
        let full = Plan::Scan(scan()).schema(&registry()).unwrap();
        assert_eq!(full, event::schema(Kind::Log));
        let mut narrow = scan();
        narrow.projection = Some(vec![BODY.into(), TIMESTAMP.into()]);
        let schema = narrow.schema().unwrap();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec![BODY, TIMESTAMP]);
        narrow.projection = Some(vec!["nope".into()]);
        assert_eq!(
            narrow.schema(),
            Err(PlanError::UnknownProjection("nope".into()))
        );
    }

    #[test]
    fn projections_and_aggregates_are_typed() {
        let plan = Plan::Scan(scan())
            .filter(Expr::column(SEVERITY_NUMBER).binary(BinaryOp::GtEq, Expr::literal(17)))
            .project(vec![
                NamedExpr::new(Expr::column(BODY), "body"),
                NamedExpr::new(
                    Expr::Call {
                        name: "length".into(),
                        args: vec![Expr::column(BODY)],
                    },
                    "len",
                ),
            ]);
        let schema = plan.schema(&registry()).unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);

        let grouped = Plan::Aggregate {
            input: Box::new(Plan::Scan(scan())),
            group_by: vec![NamedExpr::new(Expr::column("severity_text"), "severity")],
            aggregates: vec![
                AggregateExpr {
                    function: "count".into(),
                    args: vec![],
                    name: "n".into(),
                },
                AggregateExpr {
                    function: "max".into(),
                    args: vec![Expr::column(TIMESTAMP)],
                    name: "latest".into(),
                },
            ],
        };
        let schema = grouped.schema(&registry()).unwrap();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["severity", "n", "latest"]);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert!(Plan::Scan(scan()).weighted(&registry()).unwrap());
        assert!(!grouped.weighted(&registry()).unwrap());
    }

    #[test]
    fn unions_need_matching_inputs() {
        let a = Plan::Scan(scan());
        let b = Plan::Scan(scan()).project(vec![NamedExpr::new(Expr::column(BODY), "body")]);
        assert_eq!(
            Plan::Union {
                inputs: vec![a.clone(), b]
            }
            .schema(&registry()),
            Err(PlanError::UnionMismatch)
        );
        assert!(
            Plan::Union {
                inputs: vec![a.clone(), a]
            }
            .schema(&registry())
            .is_ok()
        );
        assert_eq!(
            Plan::Union { inputs: vec![] }.schema(&registry()),
            Err(PlanError::EmptyUnion)
        );
    }

    #[test]
    fn time_range_and_ordering_follow_the_inputs() {
        let mut bounded = scan();
        bounded.time_range = TimeRange::new(Timestamp::from_nanos(5), Timestamp::from_nanos(9));
        let plan = Plan::Scan(bounded.clone())
            .filter(Expr::literal(true))
            .sort(vec![SortKey::descending(Expr::column(TIMESTAMP))])
            .limit(0, Some(3));
        assert_eq!(plan.time_range(), bounded.time_range);
        assert_eq!(
            plan.ordering(),
            vec![SortKey::descending(Expr::column(TIMESTAMP))]
        );
        let projected = plan
            .clone()
            .project(vec![NamedExpr::new(Expr::column(TIMESTAMP), TIMESTAMP)]);
        assert_eq!(projected.ordering().len(), 1);
        let renamed = plan.project(vec![NamedExpr::new(Expr::column(TIMESTAMP), "t")]);
        assert!(renamed.ordering().is_empty());

        let mut other = scan();
        other.time_range = TimeRange::new(Timestamp::from_nanos(1), Timestamp::from_nanos(6));
        let union = Plan::Union {
            inputs: vec![Plan::Scan(bounded), Plan::Scan(other)],
        };
        assert_eq!(
            union.time_range(),
            TimeRange::new(Timestamp::from_nanos(1), Timestamp::from_nanos(9))
        );
        assert!(union.ordering().is_empty());
    }

    #[test]
    fn displays_as_an_explain_tree() {
        let mut s = scan();
        s.predicates =
            vec![Expr::column(SEVERITY_NUMBER).binary(BinaryOp::GtEq, Expr::literal(17))];
        s.projection = Some(vec![TIMESTAMP.into(), BODY.into()]);
        let plan = Plan::Scan(s)
            .sort(vec![SortKey::descending(Expr::column(TIMESTAMP))])
            .limit(0, Some(10));
        assert_eq!(
            plan.to_string(),
            "Limit: offset=0 count=10\n  Sort: timestamp DESC\n    Scan: prod/log [all time) default predicates=[(severity_number >= 17)] columns=[timestamp, body]\n"
        );
    }

    #[test]
    fn extract_and_format_add_the_extracted_column_once() {
        let plan = Plan::Extract {
            input: Box::new(Plan::Scan(scan())),
            source: Expr::column(BODY),
            stage: ExtractStage::Json,
        };
        let schema = plan.schema(&registry()).unwrap();
        assert_eq!(schema.fields().last().unwrap().name(), EXTRACTED);
        let again = Plan::Extract {
            input: Box::new(plan.clone()),
            source: Expr::column(BODY),
            stage: ExtractStage::Regexp("(?P<x>.)".into()),
        };
        assert_eq!(
            again.schema(&registry()).unwrap().fields().len(),
            schema.fields().len()
        );
        let formatted = Plan::Format {
            input: Box::new(Plan::Scan(scan())),
            line: Some("{{.x}}".into()),
            labels: vec![],
            drop: vec!["y".into()],
            keep: None,
        };
        assert_eq!(
            formatted.schema(&registry()).unwrap(),
            event::schema(Kind::Log)
        );
        assert!(
            again
                .to_string()
                .starts_with("Extract: regexp \"(?P<x>.)\" from body\n  Extract: json from body\n")
        );
        assert_eq!(
            formatted.to_string(),
            "Format: line=\"{{.x}}\" drop=[y]\n  Scan: prod/log [all time) default\n"
        );
    }

    #[test]
    fn scans_round_trip_through_json() {
        let mut scan = scan();
        scan.time_range = TimeRange::new(Timestamp::from_nanos(5), Timestamp::from_nanos(9));
        scan.predicates = vec![
            Expr::Path {
                path: "attr.http.method".into(),
                data_type: DataType::Utf8,
            }
            .binary(BinaryOp::Eq, Expr::literal("GET")),
            Expr::InList {
                expr: Box::new(Expr::column("trace_id")),
                list: vec![Literal::Bytes(vec![1, 2]), Literal::Null],
                negated: false,
            },
            Expr::Like {
                expr: Box::new(Expr::column("body")),
                pattern: "%x%".into(),
                negated: true,
                case_insensitive: true,
            },
            Expr::Call {
                name: "time_bucket".into(),
                args: vec![
                    Expr::literal(Duration::from_secs(60)),
                    Expr::column("timestamp"),
                ],
            },
        ];
        scan.projection = Some(vec!["timestamp".into(), "body".into()]);
        scan.newest_first_limit = Some(10);
        let json = serde_json::to_string(&scan).unwrap();
        let back: Scan = serde_json::from_str(&json).unwrap();
        assert_eq!(back, scan);
    }

    #[test]
    fn a_promql_node_lists_its_inputs_and_keeps_the_range_vector_schema() {
        let mut scan = scan();
        scan.kind = Kind::Metric;
        let plan = Plan::PromQL {
            inputs: vec![Plan::Scan(scan.clone()), Plan::Scan(scan)],
            expr: PromExpr::Selector {
                input: 1,
                range: None,
                offset: 0,
                at: None,
                equal: Vec::new(),
            },
            start: Timestamp::from_nanos(0),
            end: Timestamp::from_nanos(60_000_000_000),
            step: Duration::from_secs(15),
        };
        assert_eq!(plan.inputs().len(), 2);
        assert_eq!(
            plan.schema(&Registry::builtin()).unwrap(),
            range_vector_schema()
        );
        assert!(plan.ordering().is_empty());
        let text = plan.to_string();
        assert!(
            text.starts_with(
                "PromQL: input1 from 1970-01-01T00:00:00Z to 1970-01-01T00:01:00Z step 15s\n  input0:\n    Scan: prod/metric"
            ),
            "{text}"
        );
        assert!(
            text.contains("\n  input1:\n    Scan: prod/metric"),
            "{text}"
        );
    }

    #[test]
    fn range_vectors_have_a_fixed_schema_and_no_ordering() {
        let plan = Plan::RangeVector {
            input: Box::new(Plan::Scan(scan())),
            function: RangeFunction::Rate,
            value: None,
            range: Duration::from_secs(300),
            step: Duration::from_secs(60),
            start: Timestamp::UNIX_EPOCH,
            end: Timestamp::from_nanos(600_000_000_000),
            grouping: None,
        };
        let schema = plan.schema(&registry()).unwrap();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec![TIMESTAMP, EXTRACTED, VALUE]);
        assert!(plan.ordering().is_empty());
        assert!(plan.to_string().starts_with("RangeVector: rate range=300s step=60s steps=[1970-01-01T00:00:00Z, 1970-01-01T00:10:00Z]\n"));
    }
}
