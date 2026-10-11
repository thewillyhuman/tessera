// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Lowering `LogQL` into the shared plan.
//!
//! A log query becomes `Scan → Filter(selector) → stages...`: line
//! filters are text predicates that the indexes serve (QL-9), parser
//! stages are `Extract` nodes, label filters read labels (QL-8), and the
//! formatting stages are `Format` nodes. A metric query wraps the log
//! plan in a `RangeVector` whose scan starts one window early, with
//! grouping applied to the series labels before the window (`by` and
//! `without` on range functions) or before the aggregation.

use std::time::Duration;

use arrow::datatypes::DataType;
use tessera_ir::{
    AggregateExpr, BindContext, Diagnostic, Expr, ExtractStage, JoinKind, JoinSide, Literal,
    NamedExpr, Plan, RangeFunction, Scan, SeriesGrouping, SortKey, Span, VALUE,
};
use tessera_types::event::{BODY, EXTRACTED, TIMESTAMP};
use tessera_types::{Kind, TimeRange, Timestamp, event};

use crate::ast::{
    AggregateOp, BinaryOp, CompareOp, Conversion, FilterValue, Grouping, LabelFilter, LineOp,
    LogQuery, MatchOp, Matcher, MetricExpr, Query, Stage,
};

/// The time parameters of a query, from the API that received it.
#[derive(Clone, Copy, Debug)]
pub struct RangeParams {
    /// The first time of the query, inclusive.
    pub start: Timestamp,
    /// The last time of the query, inclusive.
    pub end: Timestamp,
    /// The distance between steps of a metric query; zero for an instant
    /// query, evaluated at `end`.
    pub step: Duration,
}

/// What a lowered query produces.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ResultKind {
    /// Log lines in streams.
    Streams,
    /// One series of values per step.
    Matrix,
    /// One value per series, at the query time.
    Vector,
    /// A single number.
    Scalar,
}

/// A lowered query.
#[derive(Clone, PartialEq, Debug)]
pub struct Lowered {
    /// The plan. Log queries carry no ordering or limit: the API adds
    /// its direction and limit.
    pub plan: Plan,
    /// What the rows mean.
    pub kind: ResultKind,
}

/// Lowers `query` under `ctx` and `params`.
///
/// # Errors
///
/// The first construct the engine cannot run yet, with its position.
pub fn lower(query: &Query, ctx: &BindContext, params: RangeParams) -> Result<Lowered, Diagnostic> {
    match query {
        Query::Log(log) => {
            let range = TimeRange::new(params.start, next(params.end));
            let (plan, _) = log_plan(log, ctx, range);
            Ok(Lowered {
                plan,
                kind: ResultKind::Streams,
            })
        }
        Query::Metric(metric) => {
            let plan = metric_plan(metric, ctx, params)?;
            let kind = match metric {
                _ if constant(metric).is_some() => ResultKind::Scalar,
                _ if params.step.is_zero() => ResultKind::Vector,
                _ => ResultKind::Matrix,
            };
            Ok(Lowered { plan, kind })
        }
    }
}

fn next(t: Timestamp) -> Timestamp {
    Timestamp::from_nanos(t.as_nanos().saturating_add(1))
}

/// What a log pipeline left for a metric query.
struct Pipeline {
    unwrap: Option<Expr>,
    unwrap_span: Option<Span>,
}

/// The plan of a log query over `range`, and what its pipeline left.
fn log_plan(log: &LogQuery, ctx: &BindContext, range: TimeRange) -> (Plan, Pipeline) {
    let mut scan = Scan::all(ctx.tenant.clone(), ctx.dataset.clone(), Kind::Log);
    scan.time_range = range;
    let mut plan = Plan::Scan(scan);
    if let Some(selector) = Expr::all(log.matchers.iter().map(matcher)) {
        plan = plan.filter(selector);
    }
    let mut pipeline = Pipeline {
        unwrap: None,
        unwrap_span: None,
    };
    let mut extracted = false;
    for stage in &log.stages {
        plan = match stage {
            Stage::Line { op, patterns, .. } => plan.filter(line_filter(*op, patterns)),
            Stage::Json(paths) => {
                extracted = true;
                let plan = extract(plan, ExtractStage::Json);
                if paths.is_empty() {
                    plan
                } else {
                    let labels = paths
                        .iter()
                        .map(|(label, path)| {
                            (
                                label.clone(),
                                format!("{{{{.{}}}}}", tessera_types::label_name(path)),
                            )
                        })
                        .collect();
                    format(plan, None, labels, Vec::new(), None)
                }
            }
            Stage::Logfmt => {
                extracted = true;
                extract(plan, ExtractStage::Logfmt)
            }
            Stage::Regexp(pattern) => {
                extracted = true;
                extract(plan, ExtractStage::Regexp(pattern.clone()))
            }
            Stage::Pattern(pattern) => {
                extracted = true;
                extract(plan, ExtractStage::Pattern(pattern.clone()))
            }
            Stage::Unpack => {
                extracted = true;
                extract(plan, ExtractStage::Unpack)
            }
            Stage::Filter(filter) => plan.filter(label_filter(filter)),
            Stage::LineFormat(template) => {
                format(plan, Some(template.clone()), Vec::new(), Vec::new(), None)
            }
            Stage::LabelFormat(labels) => {
                extracted = true;
                format(plan, None, labels.clone(), Vec::new(), None)
            }
            Stage::Drop(labels) => format(plan, None, Vec::new(), labels.clone(), None),
            Stage::Keep(labels) => format(plan, None, Vec::new(), Vec::new(), Some(labels.clone())),
            Stage::Decolorize => decolorize(plan, extracted),
            Stage::Unwrap {
                label,
                conversion,
                span,
            } => {
                let value = Expr::Label {
                    name: label.clone(),
                    data_type: match conversion {
                        Conversion::None => DataType::Float64,
                        Conversion::Duration | Conversion::Bytes => DataType::Utf8,
                    },
                };
                pipeline.unwrap = Some(match conversion {
                    Conversion::None => value,
                    Conversion::Duration => call("parse_duration", vec![value]),
                    Conversion::Bytes => call("parse_bytes", vec![value]),
                });
                pipeline.unwrap_span = Some(*span);
                plan
            }
        };
    }
    (plan, pipeline)
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Call {
        name: name.to_owned(),
        args,
    }
}

fn label(name: &str, data_type: DataType) -> Expr {
    Expr::Label {
        name: name.to_owned(),
        data_type,
    }
}

fn extract(plan: Plan, stage: ExtractStage) -> Plan {
    Plan::Extract {
        input: Box::new(plan),
        source: Expr::column(BODY),
        stage,
    }
}

fn format(
    plan: Plan,
    line: Option<String>,
    labels: Vec<(String, String)>,
    drop: Vec<String>,
    keep: Option<Vec<String>>,
) -> Plan {
    Plan::Format {
        input: Box::new(plan),
        line,
        labels,
        drop,
        keep,
    }
}

/// A projection of every column with the body stripped of colour codes.
fn decolorize(plan: Plan, extracted: bool) -> Plan {
    let mut exprs: Vec<NamedExpr> = event::schema(Kind::Log)
        .fields()
        .iter()
        .map(|f| {
            let name = f.name().clone();
            if name == BODY {
                NamedExpr::new(call("decolorize", vec![Expr::column(BODY)]), name)
            } else {
                NamedExpr::new(Expr::column(&name), name)
            }
        })
        .collect();
    if extracted {
        exprs.push(NamedExpr::new(Expr::column(EXTRACTED), EXTRACTED));
    }
    plan.project(exprs)
}

/// A selector matcher as a predicate on a label. `!=` and `!~` hold for
/// streams without the label, as in Loki.
fn matcher(m: &Matcher) -> Expr {
    let value = label(&m.name, DataType::Utf8);
    let absent = Expr::IsNull {
        expr: Box::new(value.clone()),
        negated: false,
    };
    match m.op {
        MatchOp::Eq => value.binary(tessera_ir::BinaryOp::Eq, Expr::literal(m.value.as_str())),
        MatchOp::NotEq => {
            absent.or(value.binary(tessera_ir::BinaryOp::NotEq, Expr::literal(m.value.as_str())))
        }
        MatchOp::Regex => regex_match(value, &m.value),
        MatchOp::NotRegex => absent.or(Expr::Not(Box::new(regex_match(value, &m.value)))),
    }
}

/// A whole-value regular expression match, as Prometheus and Loki anchor
/// selector patterns.
fn regex_match(value: Expr, pattern: &str) -> Expr {
    call(
        "regexp_match",
        vec![value, Expr::literal(format!("^(?:{pattern})$"))],
    )
}

fn line_filter(op: LineOp, patterns: &[String]) -> Expr {
    let one = |pattern: &String| match op {
        LineOp::Contains | LineOp::NotContains => call(
            "contains",
            vec![Expr::column(BODY), Expr::literal(pattern.as_str())],
        ),
        LineOp::Regex | LineOp::NotRegex => call(
            "regexp_match",
            vec![Expr::column(BODY), Expr::literal(pattern.as_str())],
        ),
    };
    let any = patterns
        .iter()
        .map(one)
        .reduce(Expr::or)
        .unwrap_or_else(|| Expr::literal(false));
    match op {
        LineOp::Contains | LineOp::Regex => any,
        LineOp::NotContains | LineOp::NotRegex => Expr::Not(Box::new(any)),
    }
}

fn label_filter(filter: &LabelFilter) -> Expr {
    match filter {
        LabelFilter::And(a, b) => label_filter(a).and(label_filter(b)),
        LabelFilter::Or(a, b) => label_filter(a).or(label_filter(b)),
        LabelFilter::Compare {
            name, op, value, ..
        } => {
            let (target, literal) = match value {
                FilterValue::Ip(pattern) => {
                    let matched = call(
                        "ip_matches",
                        vec![label(name, DataType::Utf8), Expr::literal(pattern.as_str())],
                    );
                    return if *op == CompareOp::NotEq {
                        Expr::Not(Box::new(matched))
                    } else {
                        matched
                    };
                }
                FilterValue::Text(text) => {
                    (label(name, DataType::Utf8), Literal::Text(text.clone()))
                }
                FilterValue::Number(n) => (label(name, DataType::Float64), Literal::Float(*n)),
                FilterValue::Duration(d) => (
                    call("parse_duration", vec![label(name, DataType::Utf8)]),
                    Literal::Float(d.as_secs_f64()),
                ),
                FilterValue::Bytes(b) => (
                    call("parse_bytes", vec![label(name, DataType::Utf8)]),
                    Literal::Float(*b),
                ),
            };
            match op {
                CompareOp::Regex => regex_match(target, &literal_text(&literal)),
                CompareOp::NotRegex => {
                    Expr::Not(Box::new(regex_match(target, &literal_text(&literal))))
                }
                _ => target.binary(
                    match op {
                        CompareOp::Eq => tessera_ir::BinaryOp::Eq,
                        CompareOp::NotEq => tessera_ir::BinaryOp::NotEq,
                        CompareOp::Gt => tessera_ir::BinaryOp::Gt,
                        CompareOp::Gte => tessera_ir::BinaryOp::GtEq,
                        CompareOp::Lt => tessera_ir::BinaryOp::Lt,
                        CompareOp::Lte | CompareOp::Regex | CompareOp::NotRegex => {
                            tessera_ir::BinaryOp::LtEq
                        }
                    },
                    Expr::Literal(literal),
                ),
            }
        }
    }
}

fn literal_text(literal: &Literal) -> String {
    match literal {
        Literal::Text(text) => text.clone(),
        other => other.to_string(),
    }
}

fn range_function(
    name: &str,
    parameter: Option<f64>,
    span: Span,
) -> Result<RangeFunction, Diagnostic> {
    Ok(match name {
        "count_over_time" => RangeFunction::CountOverTime,
        "rate" => RangeFunction::Rate,
        "bytes_over_time" => RangeFunction::BytesOverTime,
        "bytes_rate" => RangeFunction::BytesRate,
        "absent_over_time" => RangeFunction::AbsentOverTime,
        "sum_over_time" => RangeFunction::SumOverTime,
        "avg_over_time" => RangeFunction::AvgOverTime,
        "min_over_time" => RangeFunction::MinOverTime,
        "max_over_time" => RangeFunction::MaxOverTime,
        "first_over_time" => RangeFunction::FirstOverTime,
        "last_over_time" => RangeFunction::LastOverTime,
        "stddev_over_time" => RangeFunction::StddevOverTime,
        "stdvar_over_time" => RangeFunction::StdvarOverTime,
        "quantile_over_time" => RangeFunction::QuantileOverTime(
            parameter.ok_or_else(|| Diagnostic::at(span, "quantile_over_time needs a quantile"))?,
        ),
        other => return Err(Diagnostic::at(span, format!("unknown function {other}"))),
    })
}

/// `Format` that keeps or drops the grouping labels.
fn group_labels(plan: Plan, grouping: Option<&Grouping>) -> Plan {
    match grouping {
        Some(Grouping { by: true, labels }) => {
            format(plan, None, Vec::new(), Vec::new(), Some(labels.clone()))
        }
        Some(Grouping { by: false, labels }) => {
            format(plan, None, Vec::new(), labels.clone(), None)
        }
        None => format(plan, None, Vec::new(), Vec::new(), Some(Vec::new())),
    }
}

fn metric_plan(
    metric: &MetricExpr,
    ctx: &BindContext,
    params: RangeParams,
) -> Result<Plan, Diagnostic> {
    match metric {
        MetricExpr::Scalar(value) => Ok(Plan::Values {
            schema: std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new(VALUE, DataType::Float64, true),
            ])),
            rows: vec![vec![Literal::Float(*value)]],
        }),
        MetricExpr::Range {
            function,
            parameter,
            log,
            range,
            offset,
            grouping,
            span,
        } => {
            let range = RangeSpec {
                function,
                parameter: *parameter,
                range: *range,
                offset: *offset,
                grouping: grouping.as_ref(),
                span: *span,
            };
            range_plan(&range, log, ctx, params)
        }
        MetricExpr::Aggregate {
            op,
            grouping,
            inner,
            span,
        } => aggregate_plan(*op, grouping.as_ref(), inner, *span, ctx, params),
        MetricExpr::Binary {
            op,
            left,
            right,
            span,
        } => binary_plan(*op, left, right, *span, ctx, params),
    }
}

/// A range function as written.
struct RangeSpec<'a> {
    function: &'a str,
    parameter: Option<f64>,
    range: Duration,
    offset: Duration,
    grouping: Option<&'a Grouping>,
    span: Span,
}

/// A range function over a log query: the scan starts one window early
/// so that the first step has a full window. Under an offset the
/// windows are read that much earlier and the steps moved back to the
/// query's own grid.
fn range_plan(
    spec: &RangeSpec<'_>,
    log: &LogQuery,
    ctx: &BindContext,
    params: RangeParams,
) -> Result<Plan, Diagnostic> {
    if !spec.offset.is_zero() {
        let back = |t: Timestamp| {
            Timestamp::from_nanos(
                t.as_nanos()
                    .saturating_sub(i64::try_from(spec.offset.as_nanos()).unwrap_or(i64::MAX)),
            )
        };
        let earlier = RangeParams {
            start: back(params.start),
            end: back(params.end),
            step: params.step,
        };
        let unshifted = RangeSpec {
            offset: Duration::ZERO,
            ..*spec
        };
        return Ok(range_plan(&unshifted, log, ctx, earlier)?.project(vec![
            NamedExpr::new(
                Expr::column(TIMESTAMP)
                    .binary(tessera_ir::BinaryOp::Add, Expr::literal(spec.offset)),
                TIMESTAMP,
            ),
            NamedExpr::new(Expr::column(EXTRACTED), EXTRACTED),
            NamedExpr::new(Expr::column(VALUE), VALUE),
        ]));
    }
    let function = range_function(spec.function, spec.parameter, spec.span)?;
    let window_start = Timestamp::from_nanos(
        params
            .start
            .as_nanos()
            .saturating_sub(i64::try_from(spec.range.as_nanos()).unwrap_or(i64::MAX)),
    );
    let (plan, pipeline) = log_plan(
        log,
        ctx,
        TimeRange::new(next(window_start), next(params.end)),
    );
    let needs_value = !matches!(
        function,
        RangeFunction::CountOverTime
            | RangeFunction::Rate
            | RangeFunction::BytesOverTime
            | RangeFunction::BytesRate
            | RangeFunction::AbsentOverTime
    );
    if needs_value && pipeline.unwrap.is_none() {
        return Err(Diagnostic::at(
            spec.span,
            format!("{function} needs an unwrapped value"),
        ));
    }
    let instant = params.step.is_zero();
    Ok(Plan::RangeVector {
        input: Box::new(plan),
        function,
        value: if needs_value { pipeline.unwrap } else { None },
        range: spec.range,
        step: if instant {
            Duration::from_nanos(1)
        } else {
            params.step
        },
        start: if instant { params.end } else { params.start },
        end: params.end,
        grouping: spec.grouping.map(|g| SeriesGrouping {
            by: g.by,
            labels: g.labels.clone(),
        }),
    })
}

/// An aggregation over a vector: the grouping keeps or drops labels, then
/// rows group by step and label set.
fn aggregate_plan(
    op: AggregateOp,
    grouping: Option<&Grouping>,
    inner: &MetricExpr,
    span: Span,
    ctx: &BindContext,
    params: RangeParams,
) -> Result<Plan, Diagnostic> {
    let function = match op {
        AggregateOp::Sum => "sum_raw",
        AggregateOp::Avg => "avg_raw",
        AggregateOp::Min => "min",
        AggregateOp::Max => "max",
        AggregateOp::Count => "count_raw",
        AggregateOp::Stddev => "stddev_raw",
        AggregateOp::Stdvar => "stdvar_raw",
        AggregateOp::Topk(k) | AggregateOp::Bottomk(k) => {
            return ranked_plan(op, k, grouping, inner, span, ctx, params);
        }
    };
    let input = group_labels(metric_plan(inner, ctx, params)?, grouping);
    Ok(Plan::Aggregate {
        input: Box::new(input),
        group_by: vec![
            NamedExpr::new(Expr::column(TIMESTAMP), TIMESTAMP),
            NamedExpr::new(Expr::column(EXTRACTED), EXTRACTED),
        ],
        aggregates: vec![AggregateExpr {
            function: function.into(),
            args: vec![Expr::column(VALUE)],
            name: VALUE.into(),
        }],
    })
}

/// The value of an expression made of scalars and arithmetic alone.
fn constant(expr: &MetricExpr) -> Option<f64> {
    match expr {
        MetricExpr::Scalar(value) => Some(*value),
        MetricExpr::Binary {
            op, left, right, ..
        } => {
            let (l, r) = (constant(left)?, constant(right)?);
            match op {
                BinaryOp::Add => Some(l + r),
                BinaryOp::Sub => Some(l - r),
                BinaryOp::Mul => Some(l * r),
                BinaryOp::Div => Some(l / r),
                BinaryOp::Rem => Some(l % r),
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Gt
                | BinaryOp::Gte
                | BinaryOp::Lt
                | BinaryOp::Lte => None,
            }
        }
        MetricExpr::Range { .. } | MetricExpr::Aggregate { .. } => None,
    }
}

/// Two scalars folded, where the operator is arithmetic.
fn fold_scalars(op: BinaryOp, left: f64, right: f64, span: Span) -> Result<Plan, Diagnostic> {
    let value = match op {
        BinaryOp::Add => left + right,
        BinaryOp::Sub => left - right,
        BinaryOp::Mul => left * right,
        BinaryOp::Div => left / right,
        BinaryOp::Rem => left % right,
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Gt
        | BinaryOp::Gte
        | BinaryOp::Lt
        | BinaryOp::Lte => {
            return Err(Diagnostic::at(
                span,
                "a comparison between two scalars is not supported",
            ));
        }
    };
    Ok(Plan::Values {
        schema: std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(VALUE, DataType::Float64, true),
        ])),
        rows: vec![vec![Literal::Float(value)]],
    })
}

/// `topk` and `bottomk`: at each step the `k` largest or smallest
/// values, within each group of the `by` labels, the series' labels
/// kept as they are.
fn ranked_plan(
    op: AggregateOp,
    k: usize,
    grouping: Option<&Grouping>,
    inner: &MetricExpr,
    span: Span,
    ctx: &BindContext,
    params: RangeParams,
) -> Result<Plan, Diagnostic> {
    let mut partition_by = vec![Expr::column(TIMESTAMP)];
    match grouping {
        Some(g) if g.by => {
            partition_by.extend(g.labels.iter().map(|l| label(l, DataType::Utf8)));
        }
        Some(_) => {
            return Err(Diagnostic::at(
                span,
                "topk and bottomk group with `by`; `without` is not supported yet",
            ));
        }
        None => {}
    }
    Ok(Plan::TopK {
        input: Box::new(metric_plan(inner, ctx, params)?),
        keys: vec![SortKey {
            expr: Expr::column(VALUE),
            descending: matches!(op, AggregateOp::Topk(_)),
        }],
        k,
        partition_by,
    })
}

/// The engine's operator for `op`, and whether it compares.
fn operator(op: BinaryOp) -> (tessera_ir::BinaryOp, bool) {
    match op {
        BinaryOp::Add => (tessera_ir::BinaryOp::Add, false),
        BinaryOp::Sub => (tessera_ir::BinaryOp::Sub, false),
        BinaryOp::Mul => (tessera_ir::BinaryOp::Mul, false),
        BinaryOp::Div => (tessera_ir::BinaryOp::Div, false),
        BinaryOp::Rem => (tessera_ir::BinaryOp::Rem, false),
        BinaryOp::Eq => (tessera_ir::BinaryOp::Eq, true),
        BinaryOp::NotEq => (tessera_ir::BinaryOp::NotEq, true),
        BinaryOp::Gt => (tessera_ir::BinaryOp::Gt, true),
        BinaryOp::Gte => (tessera_ir::BinaryOp::GtEq, true),
        BinaryOp::Lt => (tessera_ir::BinaryOp::Lt, true),
        BinaryOp::Lte => (tessera_ir::BinaryOp::LtEq, true),
    }
}

/// Two vectors matched one to one on their step and their labels, as
/// `LogQL` matches vectors without `on` or `ignoring`: arithmetic
/// combines the two values, a comparison keeps the left rows that
/// pass. A series without a match on the other side drops out.
fn vector_binary_plan(
    op: BinaryOp,
    left: &MetricExpr,
    right: &MetricExpr,
    ctx: &BindContext,
    params: RangeParams,
) -> Result<Plan, Diagnostic> {
    let joined = Plan::Join {
        left: JoinSide::new(metric_plan(left, ctx, params)?, "l"),
        right: JoinSide::new(metric_plan(right, ctx, params)?, "r"),
        on: vec![
            (Expr::column(TIMESTAMP), Expr::column(TIMESTAMP)),
            (Expr::column(EXTRACTED), Expr::column(EXTRACTED)),
        ],
        kind: JoinKind::Inner,
    };
    let (l, r) = (Expr::column("l.value"), Expr::column("r.value"));
    let keep = |value: Expr| {
        vec![
            NamedExpr::new(Expr::column("l.timestamp"), TIMESTAMP),
            NamedExpr::new(Expr::column("l._extracted"), EXTRACTED),
            NamedExpr::new(value, VALUE),
        ]
    };
    let (ir_op, comparison) = operator(op);
    Ok(if comparison {
        joined.filter(l.clone().binary(ir_op, r)).project(keep(l))
    } else {
        joined.project(keep(l.binary(ir_op, r)))
    })
}

/// A vector against a scalar: arithmetic projects the value, a comparison
/// filters the rows; two scalars fold.
fn binary_plan(
    op: BinaryOp,
    left: &MetricExpr,
    right: &MetricExpr,
    span: Span,
    ctx: &BindContext,
    params: RangeParams,
) -> Result<Plan, Diagnostic> {
    let (vector, scalar, flipped) = match (constant(left), constant(right)) {
        (Some(l), Some(r)) => return fold_scalars(op, l, r, span),
        (None, Some(s)) => (left, s, false),
        (Some(s), None) => (right, s, true),
        (None, None) => return vector_binary_plan(op, left, right, ctx, params),
    };
    let input = metric_plan(vector, ctx, params)?;
    let (a, b) = if flipped {
        (Expr::literal(scalar), Expr::column(VALUE))
    } else {
        (Expr::column(VALUE), Expr::literal(scalar))
    };
    let arithmetic = |op| Plan::Project {
        input: Box::new(input.clone()),
        exprs: vec![
            NamedExpr::new(Expr::column(TIMESTAMP), TIMESTAMP),
            NamedExpr::new(Expr::column(EXTRACTED), EXTRACTED),
            NamedExpr::new(a.clone().binary(op, b.clone()), VALUE),
        ],
    };
    let comparison = |op| input.clone().filter(a.clone().binary(op, b.clone()));
    Ok(match op {
        BinaryOp::Add => arithmetic(tessera_ir::BinaryOp::Add),
        BinaryOp::Sub => arithmetic(tessera_ir::BinaryOp::Sub),
        BinaryOp::Mul => arithmetic(tessera_ir::BinaryOp::Mul),
        BinaryOp::Div => arithmetic(tessera_ir::BinaryOp::Div),
        BinaryOp::Rem => arithmetic(tessera_ir::BinaryOp::Rem),
        BinaryOp::Eq => comparison(tessera_ir::BinaryOp::Eq),
        BinaryOp::NotEq => comparison(tessera_ir::BinaryOp::NotEq),
        BinaryOp::Gt => comparison(tessera_ir::BinaryOp::Gt),
        BinaryOp::Gte => comparison(tessera_ir::BinaryOp::GtEq),
        BinaryOp::Lt => comparison(tessera_ir::BinaryOp::Lt),
        BinaryOp::Lte => comparison(tessera_ir::BinaryOp::LtEq),
    })
}

#[cfg(test)]
mod tests {
    use tessera_ir::Registry;
    use tessera_types::{DatasetName, TenantId};

    use super::*;
    use crate::parser::parse;

    fn ctx() -> BindContext {
        BindContext::new(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Timestamp::from_nanos(1_000_000_000_000),
        )
    }

    fn params(step: u64) -> RangeParams {
        RangeParams {
            start: Timestamp::from_nanos(600_000_000_000),
            end: Timestamp::from_nanos(1_000_000_000_000),
            step: Duration::from_secs(step),
        }
    }

    fn lowered(text: &str, step: u64) -> Lowered {
        let out = lower(&parse(text).unwrap(), &ctx(), params(step)).unwrap();
        out.plan.schema(&Registry::builtin()).unwrap();
        out
    }

    #[test]
    fn a_log_query_becomes_scan_filters_and_stages() {
        let out = lowered(
            r#"{app="checkout", level!="debug"} |= "timeout" or "reset" | json | status >= 500 and method="GET" | line_format "{{.method}}" | keep status"#,
            0,
        );
        assert_eq!(out.kind, ResultKind::Streams);
        assert_eq!(
            out.plan.to_string(),
            "Format: keep=[status]\n  \
             Format: line=\"{{.method}}\"\n    \
             Filter: ((label(status) >= 500.0) AND (label(method) = 'GET'))\n      \
             Extract: json from body\n        \
             Filter: (contains(body, 'timeout') OR contains(body, 'reset'))\n          \
             Filter: ((label(app) = 'checkout') AND (label(level) IS NULL OR (label(level) <> 'debug')))\n            \
             Scan: prod/log [1970-01-01T00:10:00Z, 1970-01-01T00:16:40.000000001Z) default\n"
        );
    }

    #[test]
    fn regex_matchers_are_anchored_and_negations_keep_absent_labels() {
        let out = lowered(
            r#"{pod=~"c.*", env!~"test"} !~ "noise" | latency > 250ms | size > 1KB | decolorize"#,
            0,
        );
        let text = out.plan.to_string();
        assert!(
            text.contains("regexp_match(label(pod), '^(?:c.*)$')"),
            "{text}"
        );
        assert!(
            text.contains("(label(env) IS NULL OR NOT regexp_match(label(env), '^(?:test)$'))"),
            "{text}"
        );
        assert!(text.contains("NOT regexp_match(body, 'noise')"), "{text}");
        assert!(
            text.contains("(parse_duration(label(latency)) > 0.25)"),
            "{text}"
        );
        assert!(
            text.contains("(parse_bytes(label(size)) > 1000.0)"),
            "{text}"
        );
        assert!(
            text.starts_with("Project: timestamp AS timestamp"),
            "{text}"
        );
        assert!(text.contains("decolorize(body) AS body"), "{text}");
    }

    #[test]
    fn metric_queries_wrap_a_range_vector_and_aggregate_by_labels() {
        let out = lowered(r#"sum by (app) (rate({app="x"} |= "e" [5m])) / 60"#, 60);
        assert_eq!(out.kind, ResultKind::Matrix);
        assert_eq!(
            out.plan.to_string(),
            "Project: timestamp AS timestamp, _extracted AS _extracted, (value / 60.0) AS value\n  \
             Aggregate: group_by=[timestamp AS timestamp, _extracted AS _extracted] aggregates=[sum_raw(value) AS value]\n    \
             Format: keep=[app]\n      \
             RangeVector: rate range=300s step=60s steps=[1970-01-01T00:10:00Z, 1970-01-01T00:16:40Z]\n        \
             Filter: contains(body, 'e')\n          \
             Filter: (label(app) = 'x')\n            \
             Scan: prod/log [1970-01-01T00:05:00.000000001Z, 1970-01-01T00:16:40.000000001Z) default\n"
        );
        let instant = lowered(r#"count_over_time({a="b"}[1m])"#, 0);
        assert_eq!(instant.kind, ResultKind::Vector);
        assert!(
            instant
                .plan
                .to_string()
                .contains("steps=[1970-01-01T00:16:40Z, 1970-01-01T00:16:40Z]")
        );
    }

    #[test]
    fn unwrapped_and_grouped_range_functions() {
        let out = lowered(
            r#"quantile_over_time(0.9, {a="b"} | json | unwrap duration(latency) [1m]) by (route)"#,
            30,
        );
        let text = out.plan.to_string();
        assert!(text.starts_with("RangeVector: quantile_over_time(0.9) of parse_duration(label(latency)) range=60s step=30s"), "{text}");
        assert!(
            text.contains("series by (route)\n  Extract: json from body"),
            "{text}"
        );
        let out = lowered(r#"sum(count_over_time({a="b"}[1m])) > 5"#, 30);
        assert!(
            out.plan
                .to_string()
                .starts_with("Filter: (value > 5.0)\n  Aggregate:"),
            "{}",
            out.plan
        );
        assert!(
            out.plan.to_string().contains("Format: keep=[]"),
            "no grouping drops every label"
        );
    }

    #[test]
    fn unsupported_constructs_are_reported_with_positions() {
        let e = lower(
            &parse(r#"sum_over_time({a="b"}[1m])"#).unwrap(),
            &ctx(),
            params(0),
        )
        .unwrap_err();
        assert_eq!(e.message, "sum_over_time needs an unwrapped value");
        assert_eq!(e.span, Some(Span::new(0, 26)));
        let ranked = lowered(r#"topk(2, rate({a="b"}[1m]))"#, 30);
        assert!(
            ranked.plan.to_string().starts_with(
                "TopK: k=2 order=[value DESC] per=[timestamp]
  RangeVector: rate"
            ),
            "{}",
            ranked.plan
        );
        let grouped = lowered(r#"bottomk by (app) (1, rate({a="b"}[1m]))"#, 30);
        assert!(
            grouped
                .plan
                .to_string()
                .starts_with("TopK: k=1 order=[value ASC] per=[timestamp, label(app)]"),
            "{}",
            grouped.plan
        );
        let e = lower(
            &parse(r#"topk without (app) (2, rate({a="b"}[1m]))"#).unwrap(),
            &ctx(),
            params(0),
        )
        .unwrap_err();
        assert!(e.message.contains("without"));
        let out = lowered(r#"stddev by (app) (rate({a="b"}[1m]))"#, 30);
        assert!(
            out.plan
                .to_string()
                .contains("aggregates=[stddev_raw(value) AS value]"),
            "{}",
            out.plan
        );
        let out = lowered("stdvar(rate({a=\"b\"}[1m]))", 30);
        assert!(out.plan.to_string().contains("stdvar_raw(value)"));
        let addressed = lowered(
            r#"{a="b"} | logfmt | addr = ip("10.0.0.0/8") | peer != ip("::1")"#,
            0,
        );
        let text = addressed.plan.to_string();
        assert!(
            text.contains("Filter: NOT ip_matches(label(peer), '::1')\n")
                && text.contains("Filter: ip_matches(label(addr), '10.0.0.0/8')\n"),
            "{text}"
        );
        let shifted = lowered(r#"rate({a="b"}[5m] offset 1h)"#, 60);
        let text = shifted.plan.to_string();
        assert!(
            text.starts_with(
                "Project: (timestamp + INTERVAL '3600000000000 ns') AS timestamp, _extracted AS _extracted, value AS value\n  RangeVector: rate"
            ),
            "{text}"
        );
        assert!(
            text.contains("steps=[1969-12-31T23:10:00Z, 1969-12-31T23:16:40Z]"),
            "the windows are read an hour earlier: {text}"
        );
        let folded = lowered("(2 + 3) * 4", 0);
        assert_eq!(folded.plan.to_string(), "Values: 1 rows\n");
        assert!(
            matches!(&folded.plan, Plan::Values { rows, .. } if rows[0][0] == Literal::Float(20.0))
        );
        let ratio = lowered(r#"sum(rate({a="b"}[1m])) / sum(rate({c="d"}[1m]))"#, 30);
        assert!(
            ratio.plan.to_string().starts_with(
                "Project: l.timestamp AS timestamp, l._extracted AS _extracted, (l.value / r.value) AS value
  \
                 Join inner: l.timestamp = r.timestamp AND l._extracted = r._extracted
"
            ),
            "{}",
            ratio.plan
        );
        let above = lowered(r#"rate({a="b"}[1m]) > rate({c="d"}[1m])"#, 30);
        assert!(
            above.plan.to_string().starts_with(
                "Project: l.timestamp AS timestamp, l._extracted AS _extracted, l.value AS value
  \
                 Filter: (l.value > r.value)
    \
                 Join inner:"
            ),
            "{}",
            above.plan
        );
    }
}
