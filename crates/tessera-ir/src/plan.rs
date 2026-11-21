// SPDX-License-Identifier: Apache-2.0

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
use tessera_types::event::{EXTRACTED, HISTOGRAM, SAMPLE_RATE, TIMESTAMP};
use tessera_types::{DatasetName, Kind, TenantId, TimeRange, Timestamp, event};

use crate::expr::{BinaryOp, Expr, Literal, qualify};
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
    /// The sides of a join have one name, so their columns would clash.
    #[error("both sides of the join are named {0}")]
    JoinNames(String),
    /// A join has no key.
    #[error("join has no condition")]
    EmptyJoin,
    /// A join key has different types on the two sides.
    #[error("join keys have different types: {0} and {1}")]
    JoinKeyTypes(String, String),
    /// The times of an `ASOF JOIN` have different types on the two sides.
    #[error("asof times have different types: {0} and {1}")]
    AsofTypes(String, String),
    /// The times of an `ASOF JOIN` cannot be ordered.
    #[error("asof times must be timestamps or numbers, not {0}")]
    AsofType(String),
    /// A window function is misused.
    #[error("window function {0}")]
    Window(String),
}

/// Which rows a join keeps (DM-2).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum JoinKind {
    /// The rows with a match on both sides.
    Inner,
    /// Every left row, with nulls in the right columns where no right
    /// row matches.
    Left,
}

impl fmt::Display for JoinKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinKind::Inner => write!(f, "inner"),
            JoinKind::Left => write!(f, "left"),
        }
    }
}

/// Which right row an `ASOF JOIN` takes among those with equal keys:
/// the one whose time is nearest the left row's, on the given side of
/// it (section 9.2).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum AsofMatch {
    /// The latest right time at or before the left one: `left >= right`.
    AtOrBefore,
    /// The latest right time before the left one: `left > right`.
    Before,
    /// The earliest right time at or after the left one: `left <= right`.
    AtOrAfter,
    /// The earliest right time after the left one: `left < right`.
    After,
}

impl AsofMatch {
    /// The match `left op right` asks for; none unless `op` is an
    /// inequality.
    #[must_use]
    pub fn from_op(op: BinaryOp) -> Option<Self> {
        match op {
            BinaryOp::GtEq => Some(AsofMatch::AtOrBefore),
            BinaryOp::Gt => Some(AsofMatch::Before),
            BinaryOp::LtEq => Some(AsofMatch::AtOrAfter),
            BinaryOp::Lt => Some(AsofMatch::After),
            _ => None,
        }
    }

    /// The inequality `left op right` every pair satisfies.
    #[must_use]
    pub fn op(self) -> BinaryOp {
        match self {
            AsofMatch::AtOrBefore => BinaryOp::GtEq,
            AsofMatch::Before => BinaryOp::Gt,
            AsofMatch::AtOrAfter => BinaryOp::LtEq,
            AsofMatch::After => BinaryOp::Lt,
        }
    }
}

/// The time match of an `ASOF JOIN`: each left row pairs with at most
/// one right row of equal keys, the one [`AsofMatch`] picks by time.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct Asof {
    /// The left row's time, over the left side.
    pub left: Expr,
    /// The right row's time, over the right side.
    pub right: Expr,
    /// Which right row is taken.
    pub matching: AsofMatch,
}

/// One side of a join: the rows and the name that qualifies their
/// columns in the output, so that both sides may have a `timestamp`.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct JoinSide {
    /// The rows.
    pub plan: Box<Plan>,
    /// The qualifier: `name.column` is this side's `column`.
    pub name: String,
}

impl JoinSide {
    /// `plan` qualified by `name`.
    #[must_use]
    pub fn new(plan: Plan, name: impl Into<String>) -> Self {
        JoinSide {
            plan: Box::new(plan),
            name: name.into(),
        }
    }
}

/// An expression with the name it is output under.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
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
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct AggregateExpr {
    /// The function, by registry name.
    pub function: String,
    /// Its arguments; empty for `count(*)`.
    pub args: Vec<Expr>,
    /// The output column name.
    pub name: String,
}

/// One key of an ordering.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
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

/// A window function (QE-20): a value for every row out of the rows of
/// its partition, in the window's order. With an order, a row's frame
/// is the rows up to its last peer, as SQL's default frame is; without
/// one it is the whole partition.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum WindowFunction {
    /// The row's number in its partition, from 1.
    RowNumber,
    /// The number of the first of the row's peers, from 1.
    Rank,
    /// The number of the row's peer group, from 1.
    DenseRank,
    /// The value `offset` rows before the row, or the default.
    Lag,
    /// The value `offset` rows after the row, or the default.
    Lead,
    /// The value of the partition's first row.
    FirstValue,
    /// The value of the last of the row's peers.
    LastValue,
    /// An aggregate of the registry over the row's frame.
    Aggregate(String),
}

impl WindowFunction {
    /// The function called `name`: one of the window functions, or an
    /// aggregate `registry` knows; none otherwise.
    #[must_use]
    pub fn named(name: &str, registry: &Registry) -> Option<Self> {
        Some(match name {
            "row_number" => WindowFunction::RowNumber,
            "rank" => WindowFunction::Rank,
            "dense_rank" => WindowFunction::DenseRank,
            "lag" => WindowFunction::Lag,
            "lead" => WindowFunction::Lead,
            "first_value" => WindowFunction::FirstValue,
            "last_value" => WindowFunction::LastValue,
            aggregate if registry.is_aggregate(aggregate) => {
                WindowFunction::Aggregate(aggregate.to_owned())
            }
            _ => return None,
        })
    }

    /// Whether the function is computed over a frame, and so may be
    /// given one: the numbering functions and the offsets are not.
    #[must_use]
    pub fn takes_frame(&self) -> bool {
        matches!(
            self,
            WindowFunction::FirstValue | WindowFunction::LastValue | WindowFunction::Aggregate(_)
        )
    }

    /// The function's name as written.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            WindowFunction::RowNumber => "row_number",
            WindowFunction::Rank => "rank",
            WindowFunction::DenseRank => "dense_rank",
            WindowFunction::Lag => "lag",
            WindowFunction::Lead => "lead",
            WindowFunction::FirstValue => "first_value",
            WindowFunction::LastValue => "last_value",
            WindowFunction::Aggregate(name) => name,
        }
    }
}

/// One end of a window frame: a row of the partition, counted from
/// the row the frame is of in the window's order.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum FrameBound {
    /// `UNBOUNDED PRECEDING`: the partition's first row.
    UnboundedPreceding,
    /// `n PRECEDING`: the row `n` rows before.
    Preceding(u64),
    /// `CURRENT ROW`.
    CurrentRow,
    /// `n FOLLOWING`: the row `n` rows after.
    Following(u64),
    /// `UNBOUNDED FOLLOWING`: the partition's last row.
    UnboundedFollowing,
}

impl FrameBound {
    /// Where the bound stands among the five kinds, earliest first.
    fn rank(self) -> u8 {
        match self {
            FrameBound::UnboundedPreceding => 0,
            FrameBound::Preceding(_) => 1,
            FrameBound::CurrentRow => 2,
            FrameBound::Following(_) => 3,
            FrameBound::UnboundedFollowing => 4,
        }
    }
}

impl fmt::Display for FrameBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameBound::UnboundedPreceding => f.write_str("UNBOUNDED PRECEDING"),
            FrameBound::Preceding(rows) => write!(f, "{rows} PRECEDING"),
            FrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            FrameBound::Following(rows) => write!(f, "{rows} FOLLOWING"),
            FrameBound::UnboundedFollowing => f.write_str("UNBOUNDED FOLLOWING"),
        }
    }
}

/// `ROWS BETWEEN start AND end`: the rows of its partition that a
/// row's aggregate, first value or last value is taken over, counted
/// by position in the window's order, whatever their peers (QE-20).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Frame {
    /// The first row of the frame.
    pub start: FrameBound,
    /// The last row of the frame.
    pub end: FrameBound,
}

impl Frame {
    /// The frame from `start` to `end`.
    ///
    /// # Errors
    ///
    /// `Window` when the frame starts after it ends by the kind of its
    /// bounds, as one from the current row to a row before it does.
    pub fn new(start: FrameBound, end: FrameBound) -> Result<Self, PlanError> {
        if start.rank() > end.rank() {
            return Err(PlanError::Window(format!(
                "a frame cannot start at {start} and end at {end}"
            )));
        }
        Ok(Frame { start, end })
    }

    /// The rows the frame of row `row` holds, of a partition of
    /// `length` rows; empty when the frame lies outside the partition.
    #[must_use]
    pub fn rows(&self, row: usize, length: usize) -> std::ops::Range<usize> {
        let rows = |n: u64| usize::try_from(n).unwrap_or(usize::MAX);
        let start = match self.start {
            FrameBound::UnboundedPreceding => 0,
            FrameBound::Preceding(n) => row.saturating_sub(rows(n)),
            FrameBound::CurrentRow => row,
            FrameBound::Following(n) => row.saturating_add(rows(n)),
            FrameBound::UnboundedFollowing => length,
        };
        let end = match self.end {
            FrameBound::UnboundedPreceding => 0,
            FrameBound::Preceding(n) => (row + 1).saturating_sub(rows(n)),
            FrameBound::CurrentRow => row + 1,
            FrameBound::Following(n) => (row + 1).saturating_add(rows(n)),
            FrameBound::UnboundedFollowing => length,
        };
        let end = end.min(length);
        start.min(end)..end
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ROWS BETWEEN {} AND {}", self.start, self.end)
    }
}

/// One window function of a `Window` node, with the column it outputs.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct WindowExpr {
    /// The function.
    pub function: WindowFunction,
    /// The arguments: the value for `lag`, `lead`, `first_value`,
    /// `last_value` and an aggregate, then for `lag` and `lead` the
    /// offset and the default, as literals; none for the numbering
    /// functions and `count(*)`.
    pub args: Vec<Expr>,
    /// The frame an aggregate, `first_value` or `last_value` is taken
    /// over; none for the default, the rows up to the row's last peer.
    #[serde(default)]
    pub frame: Option<Frame>,
    /// The output column name.
    pub name: String,
}

impl fmt::Display for WindowExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let args: Vec<String> = self.args.iter().map(ToString::to_string).collect();
        write!(f, "{}({})", self.function.name(), args.join(", "))?;
        if let Some(frame) = &self.frame {
            write!(f, " {frame}")?;
        }
        write!(f, " AS {}", self.name)
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
    /// Attribute paths the scan outputs as columns of their own (AD-4):
    /// read from a file's dedicated column where it has one, extracted
    /// from `_overflow` elsewhere.
    #[serde(default)]
    pub paths: Vec<PathColumn>,
    /// Whether `_overflow` must carry every attribute, with the ones a
    /// file keeps in dedicated columns merged back (AD-4): what label
    /// sets and `_overflow` itself need, and what a plan that reads
    /// attributes by path only does without. On until the optimizer
    /// proves otherwise.
    pub overflow_whole: bool,
    /// Predicates over the columns a completed trace stores (QL-14),
    /// as a conjunction, that every output row of a file storing them
    /// satisfies; a file without them, and the tail, are read whole,
    /// since what they hold must be numbered first.
    #[serde(default)]
    pub stored_predicates: Vec<Expr>,
}

/// An attribute path a scan materializes as a column named by the path.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct PathColumn {
    /// The path, with its scope prefix.
    pub path: String,
    /// The type the column is read as.
    pub data_type: DataType,
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
            paths: Vec::new(),
            overflow_whole: true,
            stored_predicates: Vec::new(),
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
        let mut fields: Vec<Field> = match &self.projection {
            None => full.fields().iter().map(|f| f.as_ref().clone()).collect(),
            Some(names) => names
                .iter()
                .map(|name| {
                    full.field_with_name(name)
                        .cloned()
                        .map_err(|_| PlanError::UnknownProjection(name.clone()))
                })
                .collect::<Result<Vec<Field>, _>>()?,
        };
        for path in &self.paths {
            if fields.iter().all(|f| f.name() != &path.path) {
                fields.push(Field::new(&path.path, path.data_type.clone(), true));
            }
        }
        Ok(Arc::new(Schema::new(fields)))
    }
}

/// How a parser stage derives labels from text (`Extract`, section
/// 10.2; `LogQL` section 9.4).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
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
#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
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
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
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

/// The schema a `PromQL` node produces: the range-vector schema and
/// `histogram`, a native histogram sample in the stored shape of a
/// histogram point (QL-5), set where `value` is null.
#[must_use]
pub fn promql_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            TIMESTAMP,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(EXTRACTED, DataType::Utf8, true),
        Field::new(VALUE, DataType::Float64, true),
        Field::new(HISTOGRAM, DataType::Utf8, true),
    ]))
}

/// A node of the operator tree.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
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
    /// The first half of an aggregation split across nodes (QE-14):
    /// group rows and give each group's running state, the keys first,
    /// then each aggregate's state columns ([`state_column`]).
    PartialAggregate {
        /// The input.
        input: Box<Plan>,
        /// The grouping keys, output first.
        group_by: Vec<NamedExpr>,
        /// The aggregates whose states are output after the keys.
        aggregates: Vec<AggregateExpr>,
    },
    /// The second half: fold the states of a `PartialAggregate`, the
    /// node's input, into each group's results, as `Aggregate` would
    /// have given them from the rows. The keys are the state rows'
    /// key columns.
    FinalAggregate {
        /// The states, a `PartialAggregate` or rows in its schema.
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
    /// The input's rows with one column added per window function,
    /// each computed over the rows of equal `partition_by` values in
    /// `order_by` order (QE-20). The rows come out partition by
    /// partition, in that order.
    Window {
        /// The input.
        input: Box<Plan>,
        /// The expressions whose equal values make a partition.
        partition_by: Vec<Expr>,
        /// The order of the rows within a partition; none leaves every
        /// row a peer of every other.
        order_by: Vec<SortKey>,
        /// The functions, each adding a column.
        functions: Vec<WindowExpr>,
    },
    /// The first `k` rows under an ordering, found without a full sort;
    /// within each distinct value of `partition_by` when there is one.
    TopK {
        /// The input.
        input: Box<Plan>,
        /// The ordering.
        keys: Vec<SortKey>,
        /// How many rows to keep.
        k: usize,
        /// The expressions whose distinct values each keep `k` rows;
        /// none ranks every row together.
        partition_by: Vec<Expr>,
    },
    /// The rows of every input, which share one schema.
    Union {
        /// The inputs.
        inputs: Vec<Plan>,
    },
    /// The pairs of rows of two inputs whose keys are equal (DM-2),
    /// each side's columns qualified by the side's name. A key is an
    /// expression over its own side; null keys match nothing.
    Join {
        /// The side whose rows a left join keeps.
        left: JoinSide,
        /// The other side.
        right: JoinSide,
        /// The keys, a left expression and the right one it must equal.
        on: Vec<(Expr, Expr)>,
        /// Which rows are kept.
        kind: JoinKind,
        /// The time match, under an `ASOF JOIN`: of the right rows with
        /// equal keys, each left row pairs with the one nearest in time.
        #[serde(default)]
        asof: Option<Asof>,
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
        #[serde(with = "schema_ref")]
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

    /// Wraps `self` in a window of `functions` over `partition_by` in
    /// `order_by` order.
    #[must_use]
    pub fn window(
        self,
        partition_by: Vec<Expr>,
        order_by: Vec<SortKey>,
        functions: Vec<WindowExpr>,
    ) -> Self {
        Plan::Window {
            input: Box::new(self),
            partition_by,
            order_by,
            functions,
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

    /// The first `k` rows of this plan under `keys`, per distinct value
    /// of `partition_by` when given.
    #[must_use]
    pub fn top_k(self, keys: Vec<SortKey>, k: usize, partition_by: Vec<Expr>) -> Self {
        Plan::TopK {
            input: Box::new(self),
            keys,
            k,
            partition_by,
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
            | Plan::PartialAggregate { input, .. }
            | Plan::FinalAggregate { input, .. }
            | Plan::Sort { input, .. }
            | Plan::Window { input, .. }
            | Plan::Limit { input, .. }
            | Plan::TopK { input, .. }
            | Plan::Extract { input, .. }
            | Plan::Format { input, .. }
            | Plan::RangeVector { input, .. } => vec![input],
            Plan::Union { inputs } | Plan::PromQL { inputs, .. } => inputs.iter().collect(),
            Plan::Join { left, right, .. } => vec![&left.plan, &right.plan],
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
            Plan::Window {
                partition_by,
                order_by,
                ..
            } => partition_by
                .iter()
                .cloned()
                .map(SortKey::ascending)
                .chain(order_by.iter().cloned())
                .collect(),
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
            | Plan::PartialAggregate { .. }
            | Plan::FinalAggregate { .. }
            | Plan::Union { .. }
            | Plan::Join { .. }
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
            Plan::Window {
                input, functions, ..
            } => {
                let input = input.schema(registry)?;
                let mut fields: Vec<Field> =
                    input.fields().iter().map(|f| f.as_ref().clone()).collect();
                for function in functions {
                    fields.push(Field::new(
                        &function.name,
                        window_type(function, &input, registry)?,
                        true,
                    ));
                }
                Ok(Arc::new(Schema::new(fields)))
            }
            Plan::Extract { input, .. } => Ok(with_extracted(&input.schema(registry)?)),
            Plan::RangeVector { .. } => Ok(range_vector_schema()),
            Plan::PromQL { .. } => Ok(promql_schema()),
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
            Plan::PartialAggregate {
                input,
                group_by,
                aggregates,
            } => partial_aggregate_schema(input, group_by, aggregates, registry),
            Plan::FinalAggregate {
                input,
                group_by,
                aggregates,
            } => final_aggregate_schema(input, group_by, aggregates, registry),
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
            Plan::Join {
                left,
                right,
                on,
                kind,
                asof,
            } => join_schema(left, right, on, *kind, asof.as_ref(), registry),
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
            Plan::Join {
                left,
                right,
                on,
                kind,
                asof,
            } => {
                let mut keys: Vec<String> = on
                    .iter()
                    .map(|(l, r)| {
                        format!(
                            "{} = {}",
                            qualified_key(&left.name, l),
                            qualified_key(&right.name, r)
                        )
                    })
                    .collect();
                if let Some(asof) = asof {
                    keys.push(format!(
                        "{} {} {}",
                        qualified_key(&left.name, &asof.left),
                        asof.matching.op().symbol(),
                        qualified_key(&right.name, &asof.right)
                    ));
                }
                let word = if asof.is_some() { " asof" } else { "" };
                writeln!(f, "{pad}Join {kind}{word}: {}", keys.join(" AND "))?;
                left.plan.fmt_tree(f, depth + 1)?;
                right.plan.fmt_tree(f, depth + 1)
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
            } => fmt_aggregate(f, pad, "Aggregate", group_by, aggregates),
            Plan::PartialAggregate {
                group_by,
                aggregates,
                ..
            } => fmt_aggregate(f, pad, "PartialAggregate", group_by, aggregates),
            Plan::FinalAggregate {
                group_by,
                aggregates,
                ..
            } => fmt_aggregate(f, pad, "FinalAggregate", group_by, aggregates),
            Plan::Sort { keys, .. } => {
                let shown: Vec<String> = keys.iter().map(ToString::to_string).collect();
                writeln!(f, "{pad}Sort: {}", shown.join(", "))
            }
            Plan::Window {
                partition_by,
                order_by,
                functions,
                ..
            } => fmt_window(f, pad, partition_by, order_by, functions),
            Plan::Limit { offset, count, .. } => fmt_limit(f, pad, *offset, *count),
            Plan::TopK {
                keys,
                k,
                partition_by,
                ..
            } => {
                let shown: Vec<String> = keys.iter().map(ToString::to_string).collect();
                if partition_by.is_empty() {
                    writeln!(f, "{pad}TopK: k={k} order=[{}]", shown.join(", "))
                } else {
                    let per: Vec<String> = partition_by.iter().map(ToString::to_string).collect();
                    writeln!(
                        f,
                        "{pad}TopK: k={k} order=[{}] per=[{}]",
                        shown.join(", "),
                        per.join(", ")
                    )
                }
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
            | Plan::Join { .. }
            | Plan::Values { .. }
            | Plan::SystemTable { .. }
            | Plan::TraceGroup { .. }
            | Plan::PromQL { .. } => Ok(()),
        }
    }
}

/// The schema of a join: the left columns then the right ones, each
/// qualified by its side's name; the right ones nullable under a left
/// join. The keys must type on their sides, pairwise alike, and the
/// times of an `ASOF JOIN` alike and ordered.
fn join_schema(
    left: &JoinSide,
    right: &JoinSide,
    on: &[(Expr, Expr)],
    kind: JoinKind,
    asof: Option<&Asof>,
    registry: &Registry,
) -> Result<SchemaRef, PlanError> {
    if left.name == right.name {
        return Err(PlanError::JoinNames(left.name.clone()));
    }
    if on.is_empty() {
        return Err(PlanError::EmptyJoin);
    }
    let left_schema = left.plan.schema(registry)?;
    let right_schema = right.plan.schema(registry)?;
    for (l, r) in on {
        let left_type = expr_type(l, &left_schema, registry)?;
        let right_type = expr_type(r, &right_schema, registry)?;
        if left_type != right_type {
            return Err(PlanError::JoinKeyTypes(
                left_type.to_string(),
                right_type.to_string(),
            ));
        }
    }
    if let Some(asof) = asof {
        let left_type = expr_type(&asof.left, &left_schema, registry)?;
        let right_type = expr_type(&asof.right, &right_schema, registry)?;
        if left_type != right_type {
            return Err(PlanError::AsofTypes(
                left_type.to_string(),
                right_type.to_string(),
            ));
        }
        if !(left_type.is_temporal() || left_type.is_numeric()) {
            return Err(PlanError::AsofType(left_type.to_string()));
        }
    }
    let qualified = |schema: &Schema, name: &str, nullable: bool| {
        schema
            .fields()
            .iter()
            .map(|f| {
                let field = f.as_ref().clone().with_name(qualify(name, f.name()));
                if nullable {
                    field.with_nullable(true)
                } else {
                    field
                }
            })
            .collect::<Vec<Field>>()
    };
    let mut fields = qualified(&left_schema, &left.name, false);
    fields.extend(qualified(
        &right_schema,
        &right.name,
        kind == JoinKind::Left,
    ));
    Ok(Arc::new(Schema::new(fields)))
}

/// A join key as `EXPLAIN` shows it: the side's name on a column or
/// path, around anything else.
fn qualified_key(name: &str, key: &Expr) -> String {
    match key {
        Expr::Column(column) | Expr::Path { path: column, .. } => qualify(name, column),
        other => format!("{name}.({other})"),
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
    if !scan.paths.is_empty() {
        let shown: Vec<&str> = scan.paths.iter().map(|p| p.path.as_str()).collect();
        write!(f, " paths=[{}]", shown.join(", "))?;
    }
    if !scan.stored_predicates.is_empty() {
        let shown: Vec<String> = scan
            .stored_predicates
            .iter()
            .map(ToString::to_string)
            .collect();
        write!(f, " stored_predicates=[{}]", shown.join(", "))?;
    }
    writeln!(f)
}

/// The `Aggregate` line of `EXPLAIN`.
fn fmt_aggregate(
    f: &mut fmt::Formatter<'_>,
    pad: &str,
    label: &str,
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
        "{pad}{label}: group_by=[{}] aggregates=[{}]",
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

/// The `EXPLAIN` line of a window: its partition, order and functions.
fn fmt_window(
    f: &mut fmt::Formatter<'_>,
    pad: &str,
    partition_by: &[Expr],
    order_by: &[SortKey],
    functions: &[WindowExpr],
) -> fmt::Result {
    let shown = |items: Vec<String>| items.join(", ");
    writeln!(
        f,
        "{pad}Window: partition_by=[{}] order_by=[{}] functions=[{}]",
        shown(partition_by.iter().map(ToString::to_string).collect()),
        shown(order_by.iter().map(ToString::to_string).collect()),
        shown(functions.iter().map(ToString::to_string).collect())
    )
}

/// The type of a window function's column: a number for the numbering
/// functions, the value's type for the ones that take a value, and the
/// aggregate's for an aggregate.
///
/// # Errors
///
/// `Window` if the arguments do not fit the function, `Type` if one
/// does not type.
pub fn window_type(
    window: &WindowExpr,
    input: &Schema,
    registry: &Registry,
) -> Result<DataType, PlanError> {
    let misuse = |message: &str| PlanError::Window(format!("{} {message}", window.function.name()));
    if window.frame.is_some() && !window.function.takes_frame() {
        return Err(misuse("takes no frame: it is not computed over one"));
    }
    match &window.function {
        WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
            if window.args.is_empty() {
                Ok(DataType::Int64)
            } else {
                Err(misuse("takes no argument"))
            }
        }
        WindowFunction::Lag | WindowFunction::Lead => {
            let (Some(value), true) = (window.args.first(), window.args.len() <= 3) else {
                return Err(misuse("takes a value, an offset and a default"));
            };
            if !window.args[1..]
                .iter()
                .all(|a| matches!(a, Expr::Literal(_)))
            {
                return Err(misuse("takes its offset and default as constants"));
            }
            Ok(expr_type(value, input, registry)?)
        }
        WindowFunction::FirstValue | WindowFunction::LastValue => match window.args.as_slice() {
            [value] => Ok(expr_type(value, input, registry)?),
            _ => Err(misuse("takes one value")),
        },
        WindowFunction::Aggregate(function) => aggregate_type(
            &AggregateExpr {
                function: function.clone(),
                args: window.args.clone(),
                name: window.name.clone(),
            },
            input,
            registry,
        ),
    }
}

/// The schema of a `PartialAggregate`: the keys, then each aggregate's
/// state columns.
fn partial_aggregate_schema(
    input: &Plan,
    group_by: &[NamedExpr],
    aggregates: &[AggregateExpr],
    registry: &Registry,
) -> Result<SchemaRef, PlanError> {
    let input = input.schema(registry)?;
    let mut fields = Vec::with_capacity(group_by.len() + aggregates.len());
    for key in group_by {
        let t = expr_type(&key.expr, &input, registry)?;
        fields.push(Field::new(&key.name, t, true));
    }
    for aggregate in aggregates {
        for (i, t) in aggregate_state_types(aggregate, &input, registry)?
            .into_iter()
            .enumerate()
        {
            fields.push(Field::new(state_column(&aggregate.name, i), t, true));
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// The schema of a `FinalAggregate`: the keys as the states carry
/// them, then each aggregate's result typed over the rows the partial
/// aggregate read.
fn final_aggregate_schema(
    input: &Plan,
    group_by: &[NamedExpr],
    aggregates: &[AggregateExpr],
    registry: &Registry,
) -> Result<SchemaRef, PlanError> {
    let Plan::PartialAggregate { input: rows, .. } = input else {
        return Err(
            TypeError::Mismatch("a final aggregate reads a partial aggregate".into()).into(),
        );
    };
    let states = input.schema(registry)?;
    let rows = rows.schema(registry)?;
    let mut fields = Vec::with_capacity(group_by.len() + aggregates.len());
    for key in group_by {
        let t = expr_type(&key.expr, &states, registry)?;
        fields.push(Field::new(&key.name, t, true));
    }
    for aggregate in aggregates {
        fields.push(Field::new(
            &aggregate.name,
            aggregate_type(aggregate, &rows, registry)?,
            true,
        ));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// The columns of an aggregate's running state, as a `PartialAggregate`
/// gives it and a `FinalAggregate` folds it (QE-14): counts and sums
/// as floats, an average as its sum and weight, a variance as its
/// weight, mean and second moment, a quantile as every value seen,
/// an extreme as the value so far. Each kernel accumulator gives and
/// takes exactly these.
///
/// # Errors
///
/// If the function does not type over `input`, or cannot be merged
/// across nodes.
pub fn aggregate_state_types(
    aggregate: &AggregateExpr,
    input: &Schema,
    registry: &Registry,
) -> Result<Vec<DataType>, PlanError> {
    let output = aggregate_type(aggregate, input, registry)?;
    let floats = |n: usize| vec![DataType::Float64; n];
    match aggregate.function.as_str() {
        "count" | "count_raw" | "sum" | "sum_raw" => Ok(floats(1)),
        "avg" | "avg_raw" => Ok(floats(2)),
        "stddev" | "stddev_raw" | "stdvar" | "stdvar_raw" => Ok(floats(3)),
        "quantile" => Ok(vec![DataType::List(Arc::new(Field::new(
            "item",
            DataType::Float64,
            true,
        )))]),
        "min" | "max" => Ok(vec![output]),
        other => Err(TypeError::Mismatch(format!("{other} cannot be merged across nodes")).into()),
    }
}

/// The name of the `i`-th state column of the aggregate output as
/// `name`.
#[must_use]
pub fn state_column(name: &str, i: usize) -> String {
    format!("{name}:state{i}")
}

/// `SchemaRef` through serde, for the plan of a stage sent to another
/// node (QE-14).
mod schema_ref {
    use std::sync::Arc;

    use arrow::datatypes::{Schema, SchemaRef};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(schema: &SchemaRef, serializer: S) -> Result<S::Ok, S::Error> {
        schema.as_ref().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SchemaRef, D::Error> {
        Schema::deserialize(deserializer).map(Arc::new)
    }
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
    use tessera_types::event::{BODY, SEVERITY_NUMBER, TRACE_ID};

    use super::*;

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
    fn a_join_qualifies_both_sides_and_types_its_keys() {
        let alerts = Plan::Scan(Scan::all(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Kind::Alert,
        ));
        let silences = Plan::SystemTable {
            tenant: TenantId::DEFAULT,
            table: "silences".into(),
        };
        let on = vec![(Expr::column(event::NAME), Expr::column("id"))];
        let join = Plan::Join {
            left: JoinSide::new(alerts.clone(), "a"),
            right: JoinSide::new(silences.clone(), "s"),
            on: on.clone(),
            kind: JoinKind::Left,
            asof: None,
        };
        let schema = join.schema(&registry()).unwrap();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"a.timestamp") && names.contains(&"s.id"));
        assert!(
            schema.field_with_name("s.id").unwrap().is_nullable(),
            "a left join may leave the right side null"
        );
        assert!(join.ordering().is_empty());
        assert_eq!(join.inputs().len(), 2);
        assert!(
            join.to_string().starts_with(
                "Join left: a.name = s.id
  Scan: default/prod logs"
            ) || join.to_string().starts_with(
                "Join left: a.name = s.id
  Scan:"
            ),
            "{join}"
        );
        let same_names = Plan::Join {
            left: JoinSide::new(alerts.clone(), "t"),
            right: JoinSide::new(silences.clone(), "t"),
            on: on.clone(),
            kind: JoinKind::Inner,
            asof: None,
        };
        assert_eq!(
            same_names.schema(&registry()),
            Err(PlanError::JoinNames("t".into()))
        );
        let no_keys = Plan::Join {
            left: JoinSide::new(alerts.clone(), "a"),
            right: JoinSide::new(silences.clone(), "s"),
            on: Vec::new(),
            kind: JoinKind::Inner,
            asof: None,
        };
        assert_eq!(no_keys.schema(&registry()), Err(PlanError::EmptyJoin));
        let mismatched = Plan::Join {
            left: JoinSide::new(alerts, "a"),
            right: JoinSide::new(silences, "s"),
            on: vec![(Expr::column(TIMESTAMP), Expr::column("id"))],
            kind: JoinKind::Inner,
            asof: None,
        };
        assert!(matches!(
            mismatched.schema(&registry()),
            Err(PlanError::JoinKeyTypes(..))
        ));
    }

    /// Section 9.2: an `ASOF JOIN` pairs each left row with one right
    /// row by time; its times must share one orderable type, and
    /// `EXPLAIN` shows the inequality the match answers.
    #[test]
    fn an_asof_join_types_its_times_and_shows_its_match() {
        let logs = Plan::Scan(scan());
        let spans = Plan::Scan(Scan::all(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Kind::Span,
        ));
        let asof = |left: Expr, right: Expr| Plan::Join {
            left: JoinSide::new(logs.clone(), "l"),
            right: JoinSide::new(spans.clone(), "s"),
            on: vec![(Expr::column(TRACE_ID), Expr::column(TRACE_ID))],
            kind: JoinKind::Left,
            asof: Some(Asof {
                left,
                right,
                matching: AsofMatch::AtOrBefore,
            }),
        };
        let join = asof(Expr::column(TIMESTAMP), Expr::column(TIMESTAMP));
        assert!(join.schema(&registry()).is_ok());
        assert!(
            join.to_string().starts_with(
                "Join left asof: l.trace_id = s.trace_id AND l.timestamp >= s.timestamp\n"
            ),
            "{join}"
        );
        assert!(matches!(
            asof(Expr::column(TIMESTAMP), Expr::column(event::NAME)).schema(&registry()),
            Err(PlanError::AsofTypes(..))
        ));
        assert!(matches!(
            asof(Expr::column(BODY), Expr::column(event::NAME)).schema(&registry()),
            Err(PlanError::AsofType(..))
        ));
        assert_eq!(AsofMatch::from_op(BinaryOp::Lt), Some(AsofMatch::After));
        assert_eq!(AsofMatch::from_op(BinaryOp::Eq), None);
        assert_eq!(AsofMatch::Before.op(), BinaryOp::Gt);
    }

    /// QE-20: a frame is the rows between its bounds, counted from
    /// the row and cut at the partition's ends.
    #[test]
    fn a_frame_is_the_rows_between_its_bounds() {
        use FrameBound::{
            CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding,
        };
        let rows = |start, end, row| Frame::new(start, end).unwrap().rows(row, 10);
        assert_eq!(rows(Preceding(2), CurrentRow, 5), 3..6);
        assert_eq!(rows(Preceding(2), CurrentRow, 1), 0..2, "cut at the start");
        assert_eq!(rows(Preceding(1), Following(1), 9), 8..10, "cut at the end");
        assert_eq!(rows(UnboundedPreceding, CurrentRow, 4), 0..5);
        assert_eq!(rows(CurrentRow, UnboundedFollowing, 4), 4..10);
        assert_eq!(rows(UnboundedPreceding, UnboundedFollowing, 4), 0..10);
        assert!(
            rows(Preceding(3), Preceding(1), 0).is_empty(),
            "before the partition"
        );
        assert_eq!(rows(Preceding(3), Preceding(1), 5), 2..5);
        assert!(
            rows(Following(2), Following(4), 9).is_empty(),
            "after the partition"
        );
        assert!(
            rows(Preceding(1), Preceding(3), 5).is_empty(),
            "ends before it starts"
        );

        let backwards = Frame::new(CurrentRow, Preceding(1)).unwrap_err();
        assert!(backwards.to_string().contains("CURRENT ROW"), "{backwards}");
        assert!(Frame::new(UnboundedFollowing, UnboundedFollowing).is_ok());
        assert_eq!(
            Frame::new(Preceding(2), CurrentRow).unwrap().to_string(),
            "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"
        );
    }

    /// QE-20: only what is computed over a frame takes one.
    #[test]
    fn a_frame_is_refused_where_it_means_nothing() {
        let frame = Frame::new(FrameBound::Preceding(1), FrameBound::CurrentRow).unwrap();
        let framed = |function, args| WindowExpr {
            function,
            args,
            frame: Some(frame),
            name: "w".into(),
        };
        let input = Plan::Scan(scan()).schema(&registry()).unwrap();
        let numbered = framed(WindowFunction::RowNumber, Vec::new());
        let error = window_type(&numbered, &input, &registry()).unwrap_err();
        assert!(error.to_string().contains("takes no frame"), "{error}");
        let summed = framed(
            WindowFunction::Aggregate("sum".into()),
            vec![Expr::column(SEVERITY_NUMBER)],
        );
        assert!(window_type(&summed, &input, &registry()).is_ok());
        assert!(
            summed
                .to_string()
                .contains("ROWS BETWEEN 1 PRECEDING AND CURRENT ROW")
        );
    }

    /// QE-20: a window adds one typed column per function after the
    /// input's, shows its partition, order and functions, and refuses
    /// arguments that do not fit a function.
    #[test]
    fn a_window_adds_a_typed_column_per_function() {
        let window = |function, args: Vec<Expr>| WindowExpr {
            function,
            args,
            frame: None,
            name: "w".into(),
        };
        let plan = Plan::Scan(scan()).window(
            vec![Expr::column(SEVERITY_NUMBER)],
            vec![SortKey::ascending(Expr::column(TIMESTAMP))],
            vec![
                WindowExpr {
                    function: WindowFunction::RowNumber,
                    args: Vec::new(),
                    frame: None,
                    name: "n".into(),
                },
                WindowExpr {
                    function: WindowFunction::Lag,
                    args: vec![Expr::column(BODY), Expr::literal(1)],
                    frame: None,
                    name: "previous".into(),
                },
                WindowExpr {
                    function: WindowFunction::Aggregate("count".into()),
                    args: Vec::new(),
                    frame: None,
                    name: "so_far".into(),
                },
            ],
        );
        let schema = plan.schema(&registry()).unwrap();
        let input = Plan::Scan(scan()).schema(&registry()).unwrap();
        assert_eq!(schema.fields().len(), input.fields().len() + 3);
        assert_eq!(
            schema.field_with_name("n").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("previous").unwrap().data_type(),
            &DataType::Utf8
        );
        // Counts over logs are weighted by the sample rate (QE-13).
        assert_eq!(
            schema.field_with_name("so_far").unwrap().data_type(),
            &DataType::Float64
        );
        assert_eq!(plan.inputs().len(), 1);
        assert!(
            plan.to_string().starts_with(
                "Window: partition_by=[severity_number] order_by=[timestamp ASC] \
                 functions=[row_number() AS n, lag(body, 1) AS previous, count() AS so_far]\n"
            ),
            "{plan}"
        );
        let registry = registry();
        assert_eq!(
            WindowFunction::named("dense_rank", &registry),
            Some(WindowFunction::DenseRank)
        );
        assert_eq!(
            WindowFunction::named("sum", &registry),
            Some(WindowFunction::Aggregate("sum".into()))
        );
        assert_eq!(WindowFunction::named("nope", &registry), None);
        let misuse = |function, args| {
            Plan::Scan(scan())
                .window(Vec::new(), Vec::new(), vec![window(function, args)])
                .schema(&registry)
        };
        assert!(matches!(
            misuse(WindowFunction::Rank, vec![Expr::column(BODY)]),
            Err(PlanError::Window(_))
        ));
        assert!(matches!(
            misuse(WindowFunction::Lag, Vec::new()),
            Err(PlanError::Window(_))
        ));
        assert!(matches!(
            misuse(
                WindowFunction::Lead,
                vec![Expr::column(BODY), Expr::column(BODY)]
            ),
            Err(PlanError::Window(_))
        ));
        assert!(matches!(
            misuse(WindowFunction::LastValue, Vec::new()),
            Err(PlanError::Window(_))
        ));
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
    fn a_promql_node_lists_its_inputs_and_adds_the_histogram_column() {
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
        assert_eq!(plan.schema(&Registry::builtin()).unwrap(), promql_schema());
        assert_eq!(
            promql_schema().fields().len(),
            range_vector_schema().fields().len() + 1,
            "the range-vector schema and the histogram column"
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
