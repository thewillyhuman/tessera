// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Lowering `PromQL` into the shared plan.
//!
//! Every selector becomes an input of one `PromQL` node: a scan of the
//! metric rows the matchers admit over the time the selector can be
//! asked about (its evaluation window moved back by its offset and
//! widened by its range or the lookback delta), projected to the
//! sample time, the value, the attributes and the Prometheus name of
//! the metric (QL-19). The expression itself is lowered one to one; the
//! evaluator applies Prometheus' semantics (QL-5).
//!
//! The metric name matches `prometheus_name(name, unit, metric_type,
//! is_monotonic)`, so a metric ingested over OTLP answers to the name
//! Prometheus would give it, and a label matches the attribute whose
//! label name it is (QL-19, QL-20).

use std::time::Duration;

use arrow::datatypes::DataType;
use tessera_ir::promql::{Group, Matching, PromAggregate, PromExpr, PromOp};
use tessera_ir::{
    BinaryOp as IrOp, BindContext, Diagnostic, Expr as IrExpr, LOOKBACK, NamedExpr, Plan, Scan,
    SeriesGrouping, Span, VALUE,
};
use tessera_types::event::{IS_MONOTONIC, METRIC_TYPE, NAME, OVERFLOW, TIMESTAMP, UNIT};
use tessera_types::{Kind, TimeRange, Timestamp};

use crate::ast::{At, BinaryOp, Expr, MatchOp, Matcher};

/// The time parameters of a query, from the API that received it.
#[derive(Clone, Copy, Debug)]
pub struct RangeParams {
    /// The first step, inclusive.
    pub start: Timestamp,
    /// The last step, inclusive.
    pub end: Timestamp,
    /// The distance between steps; zero for an instant query at `start`.
    pub step: Duration,
}

/// What a lowered expression produces.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ResultKind {
    /// A number.
    Scalar,
    /// A string.
    String,
    /// One value per series at each step.
    Vector,
    /// Samples over a range per series.
    Matrix,
}

/// A lowered expression.
#[derive(Clone, PartialEq, Debug)]
pub struct Lowered {
    /// The plan.
    pub plan: Plan,
    /// What the rows mean.
    pub kind: ResultKind,
    /// The value of a string expression, which produces no rows.
    pub string: Option<String>,
}

/// The arguments a function takes: the least and the most.
fn arity(function: &str) -> (usize, usize) {
    match function {
        "time" | "pi" => (0, 0),
        "round" => (1, 2),
        "clamp" | "label_replace" | "holt_winters" | "double_exponential_smoothing" => {
            if function == "label_replace" {
                (5, 5)
            } else {
                (3, 3)
            }
        }
        "clamp_min" | "clamp_max" | "predict_linear" | "quantile_over_time"
        | "histogram_quantile" => (2, 2),
        "label_join" => (3, usize::MAX),
        "day_of_month" | "day_of_week" | "day_of_year" | "days_in_month" | "hour" | "minute"
        | "month" | "year" => (0, 1),
        _ => (1, 1),
    }
}

/// Lowers `expr` under `ctx` and `params`.
///
/// # Errors
///
/// A construct the engine cannot run, with its position.
pub fn lower(expr: &Expr, ctx: &BindContext, params: RangeParams) -> Result<Lowered, Diagnostic> {
    let mut lowerer = Lowerer {
        ctx,
        params,
        inputs: Vec::new(),
    };
    let window = (params.start.as_nanos(), params.end.as_nanos());
    let prom = lowerer.lower(expr, window)?;
    let kind = kind_of(expr);
    let string = match expr {
        Expr::String(s) => Some(s.clone()),
        _ => None,
    };
    Ok(Lowered {
        plan: Plan::PromQL {
            inputs: lowerer.inputs,
            expr: prom,
            start: params.start,
            end: params.end,
            step: params.step,
        },
        kind,
        string,
    })
}

/// The kind of value `expr` yields.
#[must_use]
pub fn kind_of(expr: &Expr) -> ResultKind {
    match expr {
        Expr::Number(_) => ResultKind::Scalar,
        Expr::String(_) => ResultKind::String,
        Expr::Selector { range: None, .. } | Expr::Aggregate { .. } => ResultKind::Vector,
        Expr::Selector { .. } | Expr::Subquery { .. } => ResultKind::Matrix,
        Expr::Unary { expr, .. } | Expr::Paren(expr) => kind_of(expr),
        Expr::Binary { left, right, .. } => {
            if kind_of(left) == ResultKind::Scalar && kind_of(right) == ResultKind::Scalar {
                ResultKind::Scalar
            } else {
                ResultKind::Vector
            }
        }
        Expr::Call { function, .. } => match function.as_str() {
            "time" | "pi" | "scalar" => ResultKind::Scalar,
            _ => ResultKind::Vector,
        },
    }
}

struct Lowerer<'a> {
    ctx: &'a BindContext,
    params: RangeParams,
    inputs: Vec<Plan>,
}

fn nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

fn offset_nanos(offset: Option<(bool, Duration)>) -> i64 {
    match offset {
        Some((true, d)) => -nanos(d),
        Some((false, d)) => nanos(d),
        None => 0,
    }
}

impl Lowerer<'_> {
    fn at_nanos(&self, at: Option<At>) -> Option<i64> {
        at.map(|at| match at {
            #[allow(clippy::cast_possible_truncation)]
            At::Time(seconds) => (seconds * 1e9) as i64,
            At::Start => self.params.start.as_nanos(),
            At::End => self.params.end.as_nanos(),
        })
    }

    /// Lowers `expr`, which may be asked about any time in `window`.
    fn lower(&mut self, expr: &Expr, window: (i64, i64)) -> Result<PromExpr, Diagnostic> {
        Ok(match expr {
            Expr::Number(n) => PromExpr::Number(*n),
            Expr::String(s) => PromExpr::Text(s.clone()),
            Expr::Paren(inner) => self.lower(inner, window)?,
            Expr::Unary { negative, expr } => {
                let inner = self.lower(expr, window)?;
                if *negative {
                    PromExpr::Negate(Box::new(inner))
                } else {
                    inner
                }
            }
            Expr::Selector { .. } => self.lower_selector(expr, window)?,
            Expr::Subquery { .. } => self.lower_subquery(expr, window)?,
            Expr::Binary { .. } => self.lower_binary(expr, window)?,
            Expr::Aggregate { .. } => self.lower_aggregate(expr, window)?,
            Expr::Call { .. } => self.lower_call(expr, window)?,
        })
    }

    fn lower_selector(&mut self, expr: &Expr, window: (i64, i64)) -> Result<PromExpr, Diagnostic> {
        Ok(match expr {
            Expr::Selector {
                name,
                matchers,
                range,
                offset,
                at,
                span,
            } => {
                let offset = offset_nanos(*offset);
                let at = self.at_nanos(*at);
                let (from, to) = match at {
                    Some(at) => (at, at),
                    None => (
                        window.0.saturating_sub(offset),
                        window.1.saturating_sub(offset),
                    ),
                };
                let back = range.map_or(nanos(LOOKBACK), nanos);
                let scan = TimeRange::new(
                    Timestamp::from_nanos(from.saturating_sub(back)),
                    Timestamp::from_nanos(to.saturating_add(1)),
                );
                let input = self.inputs.len();
                self.inputs.push(selector_plan(
                    self.ctx,
                    name.as_deref(),
                    matchers,
                    scan,
                    *span,
                )?);
                PromExpr::Selector {
                    input,
                    range: *range,
                    offset,
                    at,
                    equal: matchers
                        .iter()
                        .filter(|m| m.op == MatchOp::Eq && m.name != "__name__")
                        .map(|m| (m.name.clone(), m.value.clone()))
                        .collect(),
                }
            }
            _ => return Err(Diagnostic::unlocated("not a selector")),
        })
    }

    fn lower_subquery(&mut self, expr: &Expr, window: (i64, i64)) -> Result<PromExpr, Diagnostic> {
        Ok(match expr {
            Expr::Subquery {
                expr,
                range,
                step,
                offset,
                at,
                ..
            } => {
                let offset = offset_nanos(*offset);
                let at = self.at_nanos(*at);
                let (from, to) = match at {
                    Some(at) => (at, at),
                    None => (
                        window.0.saturating_sub(offset),
                        window.1.saturating_sub(offset),
                    ),
                };
                let inner = self.lower(expr, (from.saturating_sub(nanos(*range)), to))?;
                PromExpr::Subquery {
                    input: Box::new(inner),
                    range: *range,
                    step: *step,
                    offset,
                    at,
                }
            }
            _ => return Err(Diagnostic::unlocated("not a subquery")),
        })
    }

    fn lower_binary(&mut self, expr: &Expr, window: (i64, i64)) -> Result<PromExpr, Diagnostic> {
        Ok(match expr {
            Expr::Binary {
                op,
                left,
                right,
                matching,
                bool_modifier,
                ..
            } => PromExpr::Binary {
                op: match op {
                    BinaryOp::Add => PromOp::Add,
                    BinaryOp::Sub => PromOp::Sub,
                    BinaryOp::Mul => PromOp::Mul,
                    BinaryOp::Div => PromOp::Div,
                    BinaryOp::Mod => PromOp::Mod,
                    BinaryOp::Pow => PromOp::Pow,
                    BinaryOp::Atan2 => PromOp::Atan2,
                    BinaryOp::Eq => PromOp::Eq,
                    BinaryOp::NotEq => PromOp::NotEq,
                    BinaryOp::Gt => PromOp::Gt,
                    BinaryOp::Lt => PromOp::Lt,
                    BinaryOp::GtEq => PromOp::GtEq,
                    BinaryOp::LtEq => PromOp::LtEq,
                    BinaryOp::And => PromOp::And,
                    BinaryOp::Or => PromOp::Or,
                    BinaryOp::Unless => PromOp::Unless,
                },
                left: Box::new(self.lower(left, window)?),
                right: Box::new(self.lower(right, window)?),
                matching: matching.as_ref().map(|m| Matching {
                    on: m.on,
                    labels: m.labels.clone(),
                    group: m.group.as_ref().map(|(left, include)| Group {
                        left: *left,
                        include: include.clone(),
                    }),
                }),
                bool_result: *bool_modifier,
            },
            _ => return Err(Diagnostic::unlocated("not a binary")),
        })
    }

    fn lower_aggregate(&mut self, expr: &Expr, window: (i64, i64)) -> Result<PromExpr, Diagnostic> {
        Ok(match expr {
            Expr::Aggregate {
                op,
                expr,
                param,
                grouping,
                span,
            } => PromExpr::Aggregate {
                op: match op.as_str() {
                    "sum" => PromAggregate::Sum,
                    "min" => PromAggregate::Min,
                    "max" => PromAggregate::Max,
                    "avg" => PromAggregate::Avg,
                    "group" => PromAggregate::Group,
                    "stddev" => PromAggregate::Stddev,
                    "stdvar" => PromAggregate::Stdvar,
                    "count" => PromAggregate::Count,
                    "count_values" => PromAggregate::CountValues,
                    "bottomk" => PromAggregate::Bottomk,
                    "topk" => PromAggregate::Topk,
                    "quantile" => PromAggregate::Quantile,
                    "limitk" => PromAggregate::Limitk,
                    "limit_ratio" => PromAggregate::LimitRatio,
                    other => {
                        return Err(Diagnostic::at(
                            *span,
                            format!("unknown aggregation {other:?}"),
                        ));
                    }
                },
                input: Box::new(self.lower(expr, window)?),
                parameter: match param {
                    Some(p) => Some(Box::new(self.lower(p, window)?)),
                    None => None,
                },
                grouping: grouping.as_ref().map(|g| SeriesGrouping {
                    by: g.by,
                    labels: g.labels.clone(),
                }),
            },
            _ => return Err(Diagnostic::unlocated("not a aggregate")),
        })
    }

    fn lower_call(&mut self, expr: &Expr, window: (i64, i64)) -> Result<PromExpr, Diagnostic> {
        Ok(match expr {
            Expr::Call {
                function,
                args,
                span,
            } => {
                let (least, most) = arity(function);
                if args.len() < least || args.len() > most {
                    return Err(Diagnostic::at(
                        *span,
                        format!(
                            "{function} takes {} argument{}, found {}",
                            if least == most {
                                least.to_string()
                            } else if most == usize::MAX {
                                format!("at least {least}")
                            } else {
                                format!("{least} to {most}")
                            },
                            if least == 1 && most == 1 { "" } else { "s" },
                            args.len()
                        ),
                    ));
                }
                PromExpr::Call {
                    function: function.clone(),
                    args: args
                        .iter()
                        .map(|a| self.lower(a, window))
                        .collect::<Result<_, _>>()?,
                }
            }
            _ => return Err(Diagnostic::unlocated("not a call")),
        })
    }
}

/// The input of a selector: the rows its matchers admit in `range`,
/// projected to what the evaluator reads.
///
/// # Errors
///
/// A matcher the engine cannot evaluate, with its position.
pub fn selector_plan(
    ctx: &BindContext,
    name: Option<&str>,
    matchers: &[Matcher],
    range: TimeRange,
    location: Span,
) -> Result<Plan, Diagnostic> {
    let mut scan = Scan::all(ctx.tenant.clone(), ctx.dataset.clone(), Kind::Metric);
    scan.time_range = range;
    if let Some(name) = name {
        scan.predicates
            .push(prometheus_name().binary(IrOp::Eq, IrExpr::literal(name)));
    }
    for matcher in matchers {
        if matcher.name == "__name__" {
            if name.is_some() && matcher.op == MatchOp::Eq {
                return Err(Diagnostic::at(
                    location,
                    "metric name must not be set twice",
                ));
            }
            scan.predicates
                .push(matcher_predicate(&prometheus_name(), matcher, false));
        } else {
            let label = IrExpr::Label {
                name: matcher.name.clone(),
                data_type: DataType::Utf8,
            };
            scan.predicates
                .push(matcher_predicate(&label, matcher, true));
        }
    }
    Ok(Plan::Scan(scan).project(vec![
        NamedExpr::new(IrExpr::column(TIMESTAMP), TIMESTAMP),
        NamedExpr::new(IrExpr::column(VALUE), VALUE),
        NamedExpr::new(IrExpr::column(OVERFLOW), OVERFLOW),
        NamedExpr::new(prometheus_name(), "__name__"),
    ]))
}

/// The Prometheus name of each row's metric.
fn prometheus_name() -> IrExpr {
    IrExpr::Call {
        name: "prometheus_name".into(),
        args: vec![
            IrExpr::column(NAME),
            IrExpr::column(UNIT),
            IrExpr::column(METRIC_TYPE),
            IrExpr::column(IS_MONOTONIC),
        ],
    }
}

/// The predicate of a matcher over `value`; a negative matcher also
/// admits rows without the label when `nullable`, as Prometheus matches
/// an absent label against `!=` and `!~` (and against `=""`).
fn matcher_predicate(value: &IrExpr, matcher: &Matcher, nullable: bool) -> IrExpr {
    let absent = IrExpr::IsNull {
        expr: Box::new(value.clone()),
        negated: false,
    };
    let regex = |value: IrExpr| IrExpr::Call {
        name: "regexp_match".into(),
        args: vec![value, IrExpr::literal(format!("^(?:{})$", matcher.value))],
    };
    let positive = match matcher.op {
        MatchOp::Eq => value
            .clone()
            .binary(IrOp::Eq, IrExpr::literal(matcher.value.as_str())),
        MatchOp::NotEq => value
            .clone()
            .binary(IrOp::NotEq, IrExpr::literal(matcher.value.as_str())),
        MatchOp::Regex => regex(value.clone()),
        MatchOp::NotRegex => IrExpr::Not(Box::new(regex(value.clone()))),
    };
    let matches_empty = match matcher.op {
        MatchOp::Eq => matcher.value.is_empty(),
        MatchOp::NotEq => !matcher.value.is_empty(),
        MatchOp::Regex => {
            regex::Regex::new(&format!("^(?:{})$", matcher.value)).is_ok_and(|r| r.is_match(""))
        }
        MatchOp::NotRegex => {
            regex::Regex::new(&format!("^(?:{})$", matcher.value)).is_ok_and(|r| !r.is_match(""))
        }
    };
    if nullable && matches_empty {
        absent.or(positive)
    } else {
        positive
    }
}

#[cfg(test)]
mod tests {
    use tessera_types::{DatasetName, TenantId};

    use super::*;
    use crate::parse;

    fn ctx() -> BindContext {
        BindContext::new(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Timestamp::from_nanos(1_000_000_000_000),
        )
    }

    const S: i64 = 1_000_000_000;

    fn params(step: u64) -> RangeParams {
        RangeParams {
            start: Timestamp::from_nanos(600 * S),
            end: Timestamp::from_nanos(900 * S),
            step: Duration::from_secs(step),
        }
    }

    fn query(text: &str) -> Lowered {
        lower(&parse(text).unwrap(), &ctx(), params(30)).unwrap()
    }

    #[test]
    fn selectors_become_projected_scans_over_their_window() {
        let lowered = query(
            r#"sum by (job) (rate(http_requests_total{job=~"api.*", env!="test"}[5m] offset 1m))"#,
        );
        assert_eq!(lowered.kind, ResultKind::Vector);
        let text = lowered.plan.to_string();
        assert!(
            text.starts_with("PromQL: sum by (job)(rate(input0[300000000000ns] offset 60000000000ns)) from 1970-01-01T00:10:00Z to 1970-01-01T00:15:00Z step 30s\n  input0:\n    Project: timestamp AS timestamp, value AS value, _overflow AS _overflow, prometheus_name(name, unit, metric_type, is_monotonic) AS __name__\n      Scan: prod/metric [1970-01-01T00:04:00Z, 1970-01-01T00:14:00.000000001Z)"),
            "{text}"
        );
        assert!(
            text.contains(
                "(prometheus_name(name, unit, metric_type, is_monotonic) = 'http_requests_total')"
            ),
            "{text}"
        );
        assert!(
            text.contains("regexp_match(label(job), '^(?:api.*)$')"),
            "{text}"
        );
        assert!(
            text.contains("(label(env) IS NULL OR (label(env) <> 'test'))"),
            "{text}"
        );
    }

    #[test]
    fn modifiers_and_subqueries_widen_the_window() {
        let lowered = query("max_over_time(rate(up[1m])[10m:1m] offset 2m)");
        let text = lowered.plan.to_string();
        // Start 600s, offset 120s, subquery 600s, range 60s: 600-120-600-60.
        assert!(
            text.contains(
                "Scan: prod/metric [1969-12-31T23:57:00Z, 1970-01-01T00:13:00.000000001Z)"
            ),
            "{text}"
        );
        let pinned = query("up @ 100");
        let text = pinned.plan.to_string();
        assert!(
            text.contains("[1969-12-31T23:56:40Z, 1970-01-01T00:01:40.000000001Z)"),
            "lookback before the pinned time: {text}"
        );
        assert!(text.contains("input0 @ 100000000000ns"), "{text}");
        let at_end = query("up @ end()");
        assert!(at_end.plan.to_string().contains("@ 900000000000ns"));
    }

    #[test]
    fn kinds_strings_and_arity() {
        assert_eq!(query("1 + 2").kind, ResultKind::Scalar);
        assert_eq!(query("up[5m]").kind, ResultKind::Matrix);
        assert_eq!(query("scalar(up)").kind, ResultKind::Scalar);
        assert_eq!(query("up > 1").kind, ResultKind::Vector);
        let text = query(r#""hello""#);
        assert_eq!(text.kind, ResultKind::String);
        assert_eq!(text.string.as_deref(), Some("hello"));
        let error = lower(&parse("rate(up[5m], 1)").unwrap(), &ctx(), params(30)).unwrap_err();
        assert!(
            error.message.contains("rate takes 1 argument, found 2"),
            "{}",
            error.message
        );
        let error = lower(&parse(r#"up{__name__="x"}"#).unwrap(), &ctx(), params(30)).unwrap_err();
        assert!(error.message.contains("twice"));
        let absent = query(r#"absent(up{job="a", instance=~"b"})"#);
        let Plan::PromQL { expr, .. } = absent.plan else {
            panic!("not a promql node");
        };
        let PromExpr::Call { args, .. } = expr else {
            panic!("not a call");
        };
        assert!(
            matches!(&args[0], PromExpr::Selector { equal, .. } if equal == &[("job".to_owned(), "a".to_owned())])
        );
    }
}
