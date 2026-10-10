// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Trace grouping (QL-13): the spanset algebra of `TraceQL` (QL-12) as
//! the `TraceGroup` operator evaluates it on each trace.
//!
//! A spanset expression maps the spans of one trace to zero or more
//! spansets. A filter keeps the spans where a condition holds; the
//! logical operators combine the spans both sides matched; a structural
//! operator keeps the right side's spans that stand in a relation to
//! the left side's on the trace's tree; an aggregate filter keeps a
//! spanset by a statistic of its spans; `by` splits a spanset by a key.
//! A trace with no spanset left does not match.
//!
//! Conditions are plain expressions over the span columns plus the
//! trace-level columns the operator derives ([`ROOT_NAME`],
//! [`ROOT_SERVICE`], [`TRACE_START`], [`TRACE_DURATION_NS`]).

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use tessera_types::event::{
    DURATION_NS, LINKS, NAME, OVERFLOW, PARENT_SPAN_ID, SPAN_ID, SPAN_KIND, STATUS_CODE,
    STATUS_MESSAGE, TIMESTAMP, TRACE_ID,
};

use crate::expr::{BinaryOp, Expr, Literal};

/// The start of the earliest span of the trace.
pub const TRACE_START: &str = "trace_start";
/// The end of the latest span minus the start of the earliest, in
/// nanoseconds (`traceDuration`; stored by trace completion, QL-14).
pub const TRACE_DURATION_NS: &str = "trace_duration_ns";
/// The name of the root span (`rootName`).
pub const ROOT_NAME: &str = "root_name";
/// The `resource.service.name` of the root span (`rootServiceName`).
pub const ROOT_SERVICE: &str = "root_service";
/// The index of the spanset within its trace, from zero.
pub const SPANSET: &str = "spanset";
/// The labels of the spanset, as a JSON object of `by(key)` to value;
/// null for an unsplit spanset.
pub const SPANSET_LABELS: &str = "spanset_labels";
/// The number of spans in the spanset.
pub const MATCHED: &str = "matched";

/// The columns derived per trace, which conditions may read.
pub const TRACE_COLUMNS: [&str; 4] = [TRACE_START, TRACE_DURATION_NS, ROOT_NAME, ROOT_SERVICE];

/// The span columns a trace group carries through.
pub const SPAN_COLUMNS: [&str; 10] = [
    TRACE_ID,
    SPAN_ID,
    PARENT_SPAN_ID,
    TIMESTAMP,
    DURATION_NS,
    NAME,
    SPAN_KIND,
    STATUS_CODE,
    STATUS_MESSAGE,
    LINKS,
];

/// The schema of a trace group's rows: one row per matched span, with
/// its trace's and its spanset's columns first.
#[must_use]
pub fn trace_group_schema() -> SchemaRef {
    let ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
    Arc::new(Schema::new(vec![
        Field::new(TRACE_ID, DataType::FixedSizeBinary(16), false),
        Field::new(TRACE_START, ns.clone(), false),
        Field::new(TRACE_DURATION_NS, DataType::Int64, false),
        Field::new(ROOT_NAME, DataType::Utf8, true),
        Field::new(ROOT_SERVICE, DataType::Utf8, true),
        Field::new(SPANSET, DataType::Int64, false),
        Field::new(SPANSET_LABELS, DataType::Utf8, true),
        Field::new(MATCHED, DataType::Int64, false),
        Field::new(SPAN_ID, DataType::FixedSizeBinary(8), false),
        Field::new(PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
        Field::new(TIMESTAMP, ns, false),
        Field::new(DURATION_NS, DataType::Int64, true),
        Field::new(NAME, DataType::Utf8, true),
        Field::new(SPAN_KIND, DataType::Utf8, true),
        Field::new(STATUS_CODE, DataType::Utf8, true),
        Field::new(STATUS_MESSAGE, DataType::Utf8, true),
        Field::new(LINKS, DataType::Utf8, true),
        Field::new(OVERFLOW, DataType::Utf8, true),
    ]))
}

/// A relation between a span on the right side of a structural
/// operator and the spans on its left.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Relation {
    /// `>>`: the right span has a left span among its ancestors.
    Descendant,
    /// `>`: the right span's parent is a left span.
    Child,
    /// `~`: the right span shares its parent with a left span.
    Sibling,
    /// `<<`: the right span is an ancestor of a left span.
    Ancestor,
    /// `<`: the right span is the parent of a left span.
    Parent,
}

impl Relation {
    /// The operator's symbol.
    #[must_use]
    pub fn symbol(self) -> &'static str {
        match self {
            Relation::Descendant => ">>",
            Relation::Child => ">",
            Relation::Sibling => "~",
            Relation::Ancestor => "<<",
            Relation::Parent => "<",
        }
    }
}

/// A structural operator: a relation and how it selects spans.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Structural {
    /// The relation.
    pub relation: Relation,
    /// `!>`: keep the right spans that are not in the relation.
    pub negated: bool,
    /// `&>`: keep the left spans that took part as well.
    pub union: bool,
}

impl fmt::Display for Structural {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefix = match (self.negated, self.union) {
            (true, _) => "!",
            (false, true) => "&",
            (false, false) => "",
        };
        write!(f, "{prefix}{}", self.relation.symbol())
    }
}

/// A statistic over the spans of one spanset.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SpanAggregate {
    /// The number of spans.
    Count,
    /// The mean of a value.
    Avg,
    /// The smallest value.
    Min,
    /// The largest value.
    Max,
    /// The total of a value.
    Sum,
}

impl SpanAggregate {
    /// The function's name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            SpanAggregate::Count => "count",
            SpanAggregate::Avg => "avg",
            SpanAggregate::Min => "min",
            SpanAggregate::Max => "max",
            SpanAggregate::Sum => "sum",
        }
    }
}

/// A spanset expression.
#[derive(Clone, PartialEq, Debug)]
pub enum Spanset {
    /// `{ condition }`: the spans where the condition holds, every span
    /// for `None`. One spanset, or none when no span matches.
    Filter(Option<Expr>),
    /// `A && B`: one spanset with the spans of both sides, when both
    /// sides match.
    And(Box<Spanset>, Box<Spanset>),
    /// `A || B`: one spanset with the spans of both sides, when either
    /// side matches.
    Or(Box<Spanset>, Box<Spanset>),
    /// `A op B`: the right side's spans in the relation to the left
    /// side's spans, when both sides match.
    Structural {
        /// The left side.
        left: Box<Spanset>,
        /// The operator.
        op: Structural,
        /// The right side.
        right: Box<Spanset>,
    },
    /// `A | function(arg) op value`: each spanset of `A` whose statistic
    /// satisfies the comparison.
    AggregateFilter {
        /// The spansets.
        input: Box<Spanset>,
        /// The statistic.
        function: SpanAggregate,
        /// The value the statistic is taken over; `None` for a count.
        arg: Option<Expr>,
        /// The comparison.
        op: BinaryOp,
        /// The threshold.
        value: Literal,
    },
    /// `A | by(key)`: each spanset of `A` split by the value of `key`,
    /// labelled `by(key)`.
    By {
        /// The spansets.
        input: Box<Spanset>,
        /// The key.
        key: Expr,
    },
    /// `A | coalesce()`: every spanset of `A` merged into one.
    Coalesce(Box<Spanset>),
}

impl Spanset {
    /// The columns every expression of the spanset reads.
    #[must_use]
    pub fn references(&self) -> BTreeSet<String> {
        let mut columns = BTreeSet::new();
        self.visit(&mut |expr| columns.extend(expr.references()));
        columns
    }

    /// A predicate every span that any filter matches satisfies, for
    /// reading the candidate traces, or `None` when every span is a
    /// candidate: some filter matches every span, or reads a column only
    /// the trace provides.
    #[must_use]
    pub fn candidates(&self) -> Option<Expr> {
        let mut leaves = Vec::new();
        if !self.collect_leaves(&mut leaves) {
            return None;
        }
        leaves.into_iter().reduce(Expr::or)
    }

    /// Collects the filters into `leaves`; false if some filter matches
    /// every span.
    fn collect_leaves(&self, leaves: &mut Vec<Expr>) -> bool {
        match self {
            Spanset::Filter(None) => false,
            Spanset::Filter(Some(condition)) => {
                let references = condition.references();
                if TRACE_COLUMNS.iter().any(|c| references.contains(*c)) {
                    return false;
                }
                leaves.push(condition.clone());
                true
            }
            Spanset::And(left, right)
            | Spanset::Or(left, right)
            | Spanset::Structural { left, right, .. } => {
                left.collect_leaves(leaves) && right.collect_leaves(leaves)
            }
            Spanset::AggregateFilter { input, .. }
            | Spanset::By { input, .. }
            | Spanset::Coalesce(input) => input.collect_leaves(leaves),
        }
    }

    /// Calls `f` on every expression of the spanset.
    pub fn visit(&self, f: &mut dyn FnMut(&Expr)) {
        match self {
            Spanset::Filter(condition) => {
                if let Some(condition) = condition {
                    f(condition);
                }
            }
            Spanset::And(left, right)
            | Spanset::Or(left, right)
            | Spanset::Structural { left, right, .. } => {
                left.visit(f);
                right.visit(f);
            }
            Spanset::AggregateFilter { input, arg, .. } => {
                input.visit(f);
                if let Some(arg) = arg {
                    f(arg);
                }
            }
            Spanset::By { input, key } => {
                input.visit(f);
                f(key);
            }
            Spanset::Coalesce(input) => input.visit(f),
        }
    }

    /// The spanset with `f` applied to every expression.
    #[must_use]
    pub fn map(self, f: &mut dyn FnMut(Expr) -> Expr) -> Self {
        match self {
            Spanset::Filter(condition) => Spanset::Filter(condition.map(&mut *f)),
            Spanset::And(left, right) => {
                Spanset::And(Box::new(left.map(f)), Box::new(right.map(f)))
            }
            Spanset::Or(left, right) => Spanset::Or(Box::new(left.map(f)), Box::new(right.map(f))),
            Spanset::Structural { left, op, right } => Spanset::Structural {
                left: Box::new(left.map(f)),
                op,
                right: Box::new(right.map(f)),
            },
            Spanset::AggregateFilter {
                input,
                function,
                arg,
                op,
                value,
            } => Spanset::AggregateFilter {
                input: Box::new(input.map(f)),
                function,
                arg: arg.map(&mut *f),
                op,
                value,
            },
            Spanset::By { input, key } => Spanset::By {
                input: Box::new(input.map(f)),
                key: f(key),
            },
            Spanset::Coalesce(input) => Spanset::Coalesce(Box::new(input.map(f))),
        }
    }
}

impl fmt::Display for Spanset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Spanset::Filter(None) => write!(f, "{{ }}"),
            Spanset::Filter(Some(condition)) => write!(f, "{{ {condition} }}"),
            Spanset::And(left, right) => write!(f, "({left} && {right})"),
            Spanset::Or(left, right) => write!(f, "({left} || {right})"),
            Spanset::Structural { left, op, right } => write!(f, "({left} {op} {right})"),
            Spanset::AggregateFilter {
                input,
                function,
                arg,
                op,
                value,
            } => {
                let arg = arg.as_ref().map(ToString::to_string).unwrap_or_default();
                write!(
                    f,
                    "{input} | {}({arg}) {} {value}",
                    function.name(),
                    op.symbol()
                )
            }
            Spanset::By { input, key } => write!(f, "{input} | by({key})"),
            Spanset::Coalesce(input) => write!(f, "{input} | coalesce()"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(column: &str, value: &str) -> Spanset {
        Spanset::Filter(Some(
            Expr::column(column).binary(BinaryOp::Eq, Expr::literal(value)),
        ))
    }

    #[test]
    fn candidates_are_the_union_of_the_filters() {
        let spanset = Spanset::Structural {
            left: Box::new(filter(NAME, "a")),
            op: Structural {
                relation: Relation::Descendant,
                negated: false,
                union: false,
            },
            right: Box::new(filter(STATUS_CODE, "error")),
        };
        assert_eq!(
            spanset.candidates().unwrap().to_string(),
            "((name = 'a') OR (status_code = 'error'))"
        );
        assert_eq!(
            spanset.to_string(),
            "({ (name = 'a') } >> { (status_code = 'error') })"
        );
        assert_eq!(
            spanset.references(),
            BTreeSet::from([NAME.to_owned(), STATUS_CODE.to_owned()])
        );
    }

    #[test]
    fn a_filter_that_matches_everything_makes_every_trace_a_candidate() {
        let all = Spanset::And(Box::new(filter(NAME, "a")), Box::new(Spanset::Filter(None)));
        assert_eq!(all.candidates(), None);
        let trace_level = Spanset::Filter(Some(
            Expr::column(ROOT_NAME).binary(BinaryOp::Eq, Expr::literal("a")),
        ));
        assert_eq!(trace_level.candidates(), None);
        assert_eq!(
            Spanset::Coalesce(Box::new(filter(NAME, "a")))
                .candidates()
                .unwrap()
                .to_string(),
            "(name = 'a')"
        );
    }

    #[test]
    fn stages_display_and_map_their_expressions() {
        let spanset = Spanset::By {
            input: Box::new(Spanset::AggregateFilter {
                input: Box::new(filter(NAME, "a")),
                function: SpanAggregate::Avg,
                arg: Some(Expr::column(DURATION_NS)),
                op: BinaryOp::Gt,
                value: Literal::Int(5),
            }),
            key: Expr::column(SPAN_KIND),
        };
        assert_eq!(
            spanset.to_string(),
            "{ (name = 'a') } | avg(duration_ns) > 5 | by(span_kind)"
        );
        let mut visited = 0;
        let renamed = spanset.map(&mut |expr| {
            visited += 1;
            match expr {
                Expr::Column(name) => Expr::column(format!("x_{name}")),
                other => other,
            }
        });
        assert_eq!(visited, 3, "the filter, the aggregate's value and the key");
        assert_eq!(
            renamed.references(),
            BTreeSet::from([
                NAME.to_owned(),
                "x_duration_ns".to_owned(),
                "x_span_kind".to_owned()
            ])
        );
        let op = Structural {
            relation: Relation::Child,
            negated: true,
            union: false,
        };
        assert_eq!(op.to_string(), "!>");
        let union = Structural {
            relation: Relation::Sibling,
            negated: false,
            union: true,
        };
        assert_eq!(union.to_string(), "&~");
    }

    #[test]
    fn the_schema_leads_with_the_trace_and_spanset_columns() {
        let schema = trace_group_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            &names[..8],
            &[
                TRACE_ID,
                TRACE_START,
                TRACE_DURATION_NS,
                ROOT_NAME,
                ROOT_SERVICE,
                SPANSET,
                SPANSET_LABELS,
                MATCHED
            ]
        );
        for column in SPAN_COLUMNS {
            assert!(schema.column_with_name(column).is_some(), "{column}");
        }
        assert!(schema.column_with_name(OVERFLOW).is_some());
    }
}
