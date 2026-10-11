// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `PromQL` evaluation (section 9.3): the expression a `PromQL` node
//! evaluates over the samples of its inputs with Prometheus' semantics
//! (QL-5): lookback, staleness, counter resets, vector matching,
//! subqueries, `offset` and `@`.
//!
//! The front end lowers each selector to an input plan that reads the
//! selector's samples (the scan carries the matchers and the time
//! range), so the expression refers to inputs by index and holds only
//! what the evaluator needs.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

use crate::plan::SeriesGrouping;

/// Prometheus' lookback delta: how far back an instant selector looks
/// for a sample.
pub const LOOKBACK: Duration = Duration::from_secs(300);

/// A binary operator between vectors or scalars.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum PromOp {
    /// `+`.
    Add,
    /// `-`.
    Sub,
    /// `*`.
    Mul,
    /// `/`.
    Div,
    /// `%`.
    Mod,
    /// `^`.
    Pow,
    /// `atan2`.
    Atan2,
    /// `==`.
    Eq,
    /// `!=`.
    NotEq,
    /// `>`.
    Gt,
    /// `<`.
    Lt,
    /// `>=`.
    GtEq,
    /// `<=`.
    LtEq,
    /// `and`.
    And,
    /// `or`.
    Or,
    /// `unless`.
    Unless,
}

impl PromOp {
    /// The operator as written.
    #[must_use]
    pub fn symbol(self) -> &'static str {
        match self {
            PromOp::Add => "+",
            PromOp::Sub => "-",
            PromOp::Mul => "*",
            PromOp::Div => "/",
            PromOp::Mod => "%",
            PromOp::Pow => "^",
            PromOp::Atan2 => "atan2",
            PromOp::Eq => "==",
            PromOp::NotEq => "!=",
            PromOp::Gt => ">",
            PromOp::Lt => "<",
            PromOp::GtEq => ">=",
            PromOp::LtEq => "<=",
            PromOp::And => "and",
            PromOp::Or => "or",
            PromOp::Unless => "unless",
        }
    }

    /// Whether the operator compares, yielding a filter or a boolean.
    #[must_use]
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            PromOp::Eq | PromOp::NotEq | PromOp::Gt | PromOp::Lt | PromOp::GtEq | PromOp::LtEq
        )
    }

    /// Whether the operator combines sets of series.
    #[must_use]
    pub fn is_set(self) -> bool {
        matches!(self, PromOp::And | PromOp::Or | PromOp::Unless)
    }
}

/// How the series of two vectors pair up.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Matching {
    /// `on` when true, `ignoring` otherwise.
    pub on: bool,
    /// The labels of `on` or `ignoring`.
    pub labels: Vec<String>,
    /// A many-to-one match, when written.
    pub group: Option<Group>,
}

/// `group_left` or `group_right`: which side may have several series
/// per match, and the labels copied from the other side.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Group {
    /// `group_left` when true, `group_right` otherwise.
    pub left: bool,
    /// The labels taken from the one side.
    pub include: Vec<String>,
}

/// An aggregation operator.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum PromAggregate {
    /// `sum`.
    Sum,
    /// `min`.
    Min,
    /// `max`.
    Max,
    /// `avg`.
    Avg,
    /// `group`.
    Group,
    /// `stddev`.
    Stddev,
    /// `stdvar`.
    Stdvar,
    /// `count`.
    Count,
    /// `count_values`.
    CountValues,
    /// `bottomk`.
    Bottomk,
    /// `topk`.
    Topk,
    /// `quantile`.
    Quantile,
    /// `limitk`: the first k series of each group.
    Limitk,
    /// `limit_ratio`: a deterministic share of each group's series.
    LimitRatio,
}

impl PromAggregate {
    /// The operator's name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            PromAggregate::Sum => "sum",
            PromAggregate::Min => "min",
            PromAggregate::Max => "max",
            PromAggregate::Avg => "avg",
            PromAggregate::Group => "group",
            PromAggregate::Stddev => "stddev",
            PromAggregate::Stdvar => "stdvar",
            PromAggregate::Count => "count",
            PromAggregate::CountValues => "count_values",
            PromAggregate::Bottomk => "bottomk",
            PromAggregate::Topk => "topk",
            PromAggregate::Quantile => "quantile",
            PromAggregate::Limitk => "limitk",
            PromAggregate::LimitRatio => "limit_ratio",
        }
    }
}

/// A `PromQL` expression.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum PromExpr {
    /// A selector over input `input`: an instant vector, or a range
    /// vector when `range` is set; shifted back by `offset` (which may
    /// be negative) and pinned to `at` when set.
    Selector {
        /// The input holding the selector's samples.
        input: usize,
        /// The range of a range vector.
        range: Option<Duration>,
        /// The offset in nanoseconds.
        offset: i64,
        /// The evaluation time in nanoseconds, when pinned with `@`.
        at: Option<i64>,
        /// The selector's equality matchers, which `absent` reports as
        /// the labels of the missing series.
        equal: Vec<(String, String)>,
    },
    /// A number.
    Number(f64),
    /// A string.
    Text(String),
    /// `-expr`.
    Negate(Box<PromExpr>),
    /// `left op right`.
    Binary {
        /// The operator.
        op: PromOp,
        /// The left side.
        left: Box<PromExpr>,
        /// The right side.
        right: Box<PromExpr>,
        /// How vectors pair up; `None` for the default matching.
        matching: Option<Matching>,
        /// `bool`: a comparison yields 0 or 1 instead of filtering.
        bool_result: bool,
    },
    /// An aggregation over the series of `input`.
    Aggregate {
        /// The operator.
        op: PromAggregate,
        /// The vector.
        input: Box<PromExpr>,
        /// The parameter of `count_values`, `topk`, `bottomk` and
        /// `quantile`.
        parameter: Option<Box<PromExpr>>,
        /// `by` or `without`; every label when `None`.
        grouping: Option<SeriesGrouping>,
    },
    /// A function call.
    Call {
        /// The function.
        function: String,
        /// The arguments.
        args: Vec<PromExpr>,
    },
    /// `input[range:step]`: the inner expression evaluated at every
    /// step of the range, as a range vector.
    Subquery {
        /// The expression.
        input: Box<PromExpr>,
        /// The range.
        range: Duration,
        /// The step; the default resolution when `None`.
        step: Option<Duration>,
        /// The offset in nanoseconds.
        offset: i64,
        /// The evaluation time in nanoseconds, when pinned with `@`.
        at: Option<i64>,
    },
}

impl PromExpr {
    /// The inputs the expression reads.
    #[must_use]
    pub fn inputs(&self) -> Vec<usize> {
        let mut out = Vec::new();
        self.collect_inputs(&mut out);
        out.sort_unstable();
        out.dedup();
        out
    }

    fn collect_inputs(&self, into: &mut Vec<usize>) {
        match self {
            PromExpr::Selector { input, .. } => into.push(*input),
            PromExpr::Number(_) | PromExpr::Text(_) => {}
            PromExpr::Negate(inner) => inner.collect_inputs(into),
            PromExpr::Binary { left, right, .. } => {
                left.collect_inputs(into);
                right.collect_inputs(into);
            }
            PromExpr::Aggregate {
                input, parameter, ..
            } => {
                input.collect_inputs(into);
                if let Some(parameter) = parameter {
                    parameter.collect_inputs(into);
                }
            }
            PromExpr::Call { args, .. } => {
                for arg in args {
                    arg.collect_inputs(into);
                }
            }
            PromExpr::Subquery { input, .. } => input.collect_inputs(into),
        }
    }
}

fn fmt_modifiers(f: &mut fmt::Formatter<'_>, offset: i64, at: Option<i64>) -> fmt::Result {
    if offset != 0 {
        write!(f, " offset {offset}ns")?;
    }
    if let Some(at) = at {
        write!(f, " @ {at}ns")?;
    }
    Ok(())
}

impl fmt::Display for PromExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PromExpr::Selector {
                input,
                range,
                offset,
                at,
                ..
            } => {
                write!(f, "input{input}")?;
                if let Some(range) = range {
                    write!(f, "[{}ns]", range.as_nanos())?;
                }
                fmt_modifiers(f, *offset, *at)
            }
            PromExpr::Number(n) => write!(f, "{n}"),
            PromExpr::Text(s) => write!(f, "{s:?}"),
            PromExpr::Negate(inner) => write!(f, "-({inner})"),
            PromExpr::Binary {
                op,
                left,
                right,
                matching,
                bool_result,
            } => {
                write!(f, "({left} {}", op.symbol())?;
                if *bool_result {
                    write!(f, " bool")?;
                }
                if let Some(matching) = matching {
                    let word = if matching.on { "on" } else { "ignoring" };
                    write!(f, " {word}({})", matching.labels.join(", "))?;
                    if let Some(group) = &matching.group {
                        let side = if group.left {
                            "group_left"
                        } else {
                            "group_right"
                        };
                        write!(f, " {side}({})", group.include.join(", "))?;
                    }
                }
                write!(f, " {right})")
            }
            PromExpr::Aggregate {
                op,
                input,
                parameter,
                grouping,
            } => {
                write!(f, "{}", op.name())?;
                if let Some(grouping) = grouping {
                    write!(f, " {grouping}")?;
                }
                write!(f, "(")?;
                if let Some(parameter) = parameter {
                    write!(f, "{parameter}, ")?;
                }
                write!(f, "{input})")
            }
            PromExpr::Call { function, args } => {
                let shown: Vec<String> = args.iter().map(ToString::to_string).collect();
                write!(f, "{function}({})", shown.join(", "))
            }
            PromExpr::Subquery {
                input,
                range,
                step,
                offset,
                at,
            } => {
                write!(f, "({input})[{}ns:", range.as_nanos())?;
                if let Some(step) = step {
                    write!(f, "{}ns", step.as_nanos())?;
                }
                write!(f, "]")?;
                fmt_modifiers(f, *offset, *at)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expressions_list_their_inputs_and_display() {
        let rate = PromExpr::Call {
            function: "rate".into(),
            args: vec![PromExpr::Selector {
                input: 1,
                range: Some(Duration::from_secs(300)),
                offset: 60_000_000_000,
                at: None,
                equal: Vec::new(),
            }],
        };
        let expr = PromExpr::Binary {
            op: PromOp::Div,
            left: Box::new(PromExpr::Aggregate {
                op: PromAggregate::Sum,
                input: Box::new(rate),
                parameter: None,
                grouping: Some(SeriesGrouping {
                    by: true,
                    labels: vec!["job".into()],
                }),
            }),
            right: Box::new(PromExpr::Selector {
                input: 0,
                range: None,
                offset: 0,
                at: Some(5),
                equal: vec![("job".into(), "api".into())],
            }),
            matching: Some(Matching {
                on: true,
                labels: vec!["job".into()],
                group: Some(Group {
                    left: true,
                    include: vec![],
                }),
            }),
            bool_result: false,
        };
        assert_eq!(expr.inputs(), vec![0, 1]);
        assert_eq!(
            expr.to_string(),
            "(sum by (job)(rate(input1[300000000000ns] offset 60000000000ns)) / on(job) group_left() input0 @ 5ns)"
        );
        assert!(PromOp::Gt.is_comparison() && !PromOp::Gt.is_set());
        assert!(PromOp::Unless.is_set());
        assert_eq!(PromAggregate::CountValues.name(), "count_values");
        let subquery = PromExpr::Subquery {
            input: Box::new(PromExpr::Number(1.0)),
            range: Duration::from_secs(60),
            step: None,
            offset: 0,
            at: None,
        };
        assert_eq!(subquery.to_string(), "(1)[60000000000ns:]");
        assert!(subquery.inputs().is_empty());
    }
}
