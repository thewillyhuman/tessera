// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Lowering `TraceQL` into the shared plan.
//!
//! A search becomes a `TraceGroup` (QL-13): the spanset expression over
//! a scan of every span of the candidate traces, with the candidates
//! read by a scan of the spans any filter matches. Intrinsics are the
//! span columns and the trace-level columns the operator derives;
//! `span.` attributes are `attr.` paths, `resource.` attributes are
//! `resource.` paths, `instrumentation.` attributes are `scope.` paths,
//! and an unscoped attribute reads the span's value, then the
//! resource's (QL-21). A metrics query aggregates the matching spans
//! per step bucket and series label.
//!
//! The `event.` scope (QL-15) reads span events stored as `event` rows:
//! each event condition becomes a flag column on the spans, set by a
//! left join against the events that pass it, so the condition composes
//! with the rest of the filter and with structural operators. The
//! `link.` scope reads the `links` column through a kernel that tests
//! any link for equality.
//!
//! `quantile_over_time` is exact, one union branch per quantile with
//! the quantile as the `p` label; `histogram_over_time` counts spans
//! per power-of-two bucket of the value, the bucket as the `__bucket`
//! label (QL-12). The nested-set intrinsics read the numbers the trace
//! group assigns each span (QL-14).

use std::time::Duration;

use arrow::datatypes::DataType;
use tessera_ir::trace::{
    NEST_LEFT, NEST_PARENT, NEST_RIGHT, ROOT_NAME, ROOT_SERVICE, TRACE_DURATION_NS,
};
use tessera_ir::{
    AggregateExpr, BinaryOp, BindContext, Diagnostic, Expr, JoinKind, JoinSide, Literal, NamedExpr,
    Plan, Relation, Scan, SortKey, Span, SpanAggregate, Spanset, Structural, VALUE,
};
use tessera_types::event::{
    self, DURATION_NS, LINKS, NAME, SPAN_ID, SPAN_KIND, STATUS_CODE, STATUS_MESSAGE, TIMESTAMP,
    TRACE_ID,
};
use tessera_types::{Kind, TimeRange, Timestamp};

use crate::ast::{FieldExpr, Metrics, Pipeline, Query, SpansetExpr, Stage, Static};

/// The time parameters of a query, from the API that received it.
#[derive(Clone, Copy, Debug)]
pub struct RangeParams {
    /// The first time of the query, inclusive.
    pub start: Timestamp,
    /// The last time of the query, inclusive.
    pub end: Timestamp,
    /// The distance between steps of a metrics query; zero for one
    /// value over the whole range.
    pub step: Duration,
}

/// What lowering collects as it walks the query.
#[derive(Default)]
struct Context {
    /// The attributes the query reads or selects, as written.
    shown: Vec<String>,
    /// The conditions on span events, each the `_event_<i>` flag of the
    /// spans (QL-15).
    events: Vec<Expr>,
}

/// What a lowered query produces.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ResultKind {
    /// Matched spans grouped by trace and spanset.
    Spansets,
    /// One value per step and series.
    Metrics,
}

/// A lowered query.
#[derive(Clone, PartialEq, Debug)]
pub struct Lowered {
    /// The plan.
    pub plan: Plan,
    /// What the rows mean.
    pub kind: ResultKind,
    /// The attributes the query reads or selects, as written, for the
    /// API to show with each span.
    pub shown: Vec<String>,
    /// The series label columns of a metrics query, as written.
    pub labels: Vec<String>,
    /// The metrics function, as written; empty for a search.
    pub function: String,
}

/// Lowers `query` under `ctx` and `params`.
///
/// # Errors
///
/// The first construct the engine cannot run yet, with its position.
pub fn lower(query: &Query, ctx: &BindContext, params: RangeParams) -> Result<Lowered, Diagnostic> {
    let range = TimeRange::new(params.start, next(params.end));
    let mut cx = Context::default();
    let spanset = pipeline(&query.pipeline, &mut cx)?;
    match &query.metrics {
        None => Ok(Lowered {
            plan: trace_group(spanset, ctx, range, &cx.events),
            kind: ResultKind::Spansets,
            shown: cx.shown,
            labels: Vec::new(),
            function: String::new(),
        }),
        Some(metrics) => {
            // A plain filter scans the spans directly, unless it reads
            // what only the whole trace provides.
            let direct = query.pipeline.stages.is_empty() && !spanset.reads_trace_columns();
            let input = match (&query.pipeline.first, direct) {
                (SpansetExpr::Filter { .. }, true) => match spanset {
                    Spanset::Filter(condition) => {
                        let source = spans_source(ctx, range, &cx.events);
                        match (source, condition) {
                            (Plan::Scan(mut scan), condition) => {
                                scan.predicates.extend(condition);
                                Plan::Scan(scan)
                            }
                            (joined, Some(condition)) => joined.filter(condition),
                            (joined, None) => joined,
                        }
                    }
                    other => trace_group(other, ctx, range, &cx.events),
                },
                _ => trace_group(spanset, ctx, range, &cx.events),
            };
            let (plan, labels) = metrics_plan(metrics, input, params, &mut cx)?;
            Ok(Lowered {
                plan,
                kind: ResultKind::Metrics,
                shown: cx.shown,
                labels,
                function: metrics.function.clone(),
            })
        }
    }
}

/// The instant after `t`.
fn next(t: Timestamp) -> Timestamp {
    Timestamp::from_nanos(t.as_nanos().saturating_add(1))
}

/// A trace group over `spanset`: candidates from the filters' union when
/// it narrows anything, every span of the range otherwise.
fn trace_group(spanset: Spanset, ctx: &BindContext, range: TimeRange, events: &[Expr]) -> Plan {
    let spans = spans_source(ctx, range, events);
    let candidates = match (spanset.candidates(), spanset.candidates_stored(), &spans) {
        (Some(predicate), _, Plan::Scan(scan)) => {
            let mut scan = scan.clone();
            scan.predicates = vec![predicate];
            Some(Box::new(Plan::Scan(scan)))
        }
        (Some(predicate), _, joined) => Some(Box::new(joined.clone().filter(predicate))),
        // A trace-level condition narrows the candidates in the files
        // that store the traces' shape (QL-14); the rest read whole.
        (None, Some(stored), Plan::Scan(scan)) => {
            let mut scan = scan.clone();
            scan.stored_predicates = vec![stored];
            Some(Box::new(Plan::Scan(scan)))
        }
        (None, _, _) => None,
    };
    Plan::TraceGroup {
        candidates,
        spans: Box::new(spans),
        spanset,
    }
}

/// The spans a query reads: the scan, or, with event conditions, the
/// scan left-joined to the events passing each condition, grouped by
/// span, so that every span carries a flag per condition (QL-15).
fn spans_source(ctx: &BindContext, range: TimeRange, events: &[Expr]) -> Plan {
    let mut spans = Scan::all(ctx.tenant.clone(), ctx.dataset.clone(), Kind::Span);
    spans.time_range = range;
    let mut plan = Plan::Scan(spans);
    let columns: Vec<String> = event::schema(Kind::Span)
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    for (index, condition) in events.iter().enumerate() {
        let mut scan = Scan::all(ctx.tenant.clone(), ctx.dataset.clone(), Kind::Event);
        scan.time_range = range;
        scan.predicates = vec![condition.clone()];
        let matched = Plan::Aggregate {
            input: Box::new(Plan::Scan(scan)),
            group_by: vec![
                NamedExpr::new(Expr::column(TRACE_ID), TRACE_ID),
                NamedExpr::new(Expr::column(SPAN_ID), SPAN_ID),
            ],
            aggregates: vec![AggregateExpr {
                function: "count_raw".into(),
                args: Vec::new(),
                name: "n".into(),
            }],
        };
        let side = format!("e{index}");
        let carried: Vec<String> = columns
            .iter()
            .cloned()
            .chain((0..index).map(event_flag))
            .collect();
        let mut exprs: Vec<NamedExpr> = carried
            .iter()
            .map(|name| NamedExpr::new(Expr::column(format!("s.{name}")), name.as_str()))
            .collect();
        exprs.push(NamedExpr::new(
            Expr::IsNull {
                expr: Box::new(Expr::column(format!("{side}.n"))),
                negated: true,
            },
            event_flag(index),
        ));
        plan = Plan::Join {
            left: JoinSide::new(plan, "s"),
            right: JoinSide::new(matched, side),
            on: vec![
                (Expr::column(TRACE_ID), Expr::column(TRACE_ID)),
                (Expr::column(SPAN_ID), Expr::column(SPAN_ID)),
            ],
            kind: JoinKind::Left,
        }
        .project(exprs);
    }
    plan
}

/// The flag column of event condition `index`.
fn event_flag(index: usize) -> String {
    format!("_event_{index}")
}

/// The scope a field was written with.
fn scope_of(expr: &FieldExpr) -> Option<&str> {
    match expr {
        FieldExpr::Intrinsic { scope, .. } | FieldExpr::Attribute { scope, .. } => scope.as_deref(),
        _ => None,
    }
}

/// `expr` read against an event row: `event:name` is the event's name,
/// `event.foo` its attribute.
fn on_event(expr: &FieldExpr, span: Span) -> Result<FieldExpr, Diagnostic> {
    Ok(match expr {
        FieldExpr::Intrinsic { name, span, .. } if name == "name" => FieldExpr::Intrinsic {
            scope: None,
            name: name.clone(),
            span: *span,
        },
        FieldExpr::Attribute { name, span, .. } => FieldExpr::Attribute {
            scope: Some("span".into()),
            name: name.clone(),
            span: *span,
        },
        FieldExpr::Static(_) => expr.clone(),
        _ => {
            return Err(Diagnostic::at(
                span,
                "an event condition compares event:name or event.<attribute> with a value",
            ));
        }
    })
}

/// A comparison on the `event.` scope: the condition over event rows is
/// kept for the spans' source, and the span's flag for it stands in.
fn event_condition(
    op: &str,
    left: &FieldExpr,
    right: &FieldExpr,
    span: Span,
    cx: &mut Context,
) -> Result<Expr, Diagnostic> {
    let other_scoped = |side: &FieldExpr| scope_of(side).is_some_and(|s| s != "event");
    if other_scoped(left) || other_scoped(right) {
        return Err(Diagnostic::at(
            span,
            "an event field compares with a value, not with a span field",
        ));
    }
    let (l, r) = (on_event(left, span)?, on_event(right, span)?);
    let mut inner = Context::default();
    let condition = binary(op, &l, &r, span, &mut inner)?;
    cx.events.push(condition);
    Ok(Expr::column(event_flag(cx.events.len() - 1)))
}

/// A comparison on the `link.` scope: any link of the span with the
/// field equal to the value, or none for `!=`.
fn link_condition(
    op: &str,
    left: &FieldExpr,
    right: &FieldExpr,
    span: Span,
) -> Result<Expr, Diagnostic> {
    let ((field, FieldExpr::Static(value)) | (FieldExpr::Static(value), field)) = (left, right)
    else {
        return Err(Diagnostic::at(
            span,
            "a link field compares with a value, not with a span field",
        ));
    };
    let key = match field {
        FieldExpr::Intrinsic { name, .. } if name == "traceID" => "trace_id".to_owned(),
        FieldExpr::Intrinsic { name, .. } if name == "spanID" => "span_id".to_owned(),
        FieldExpr::Attribute { name, .. } => format!("attributes.{name}"),
        _ => {
            return Err(Diagnostic::at(
                span,
                "link fields are link:traceID, link:spanID and link.<attribute>",
            ));
        }
    };
    let value = match value {
        Static::String(s) | Static::Status(s) | Static::Kind(s) => s.clone(),
        Static::Int(i) => i.to_string(),
        Static::Float(f) => f.to_string(),
        Static::Bool(b) => b.to_string(),
        Static::Nil | Static::Duration(_) => {
            return Err(Diagnostic::at(
                span,
                "a link field compares with text, a number or a boolean",
            ));
        }
    };
    let matched = Expr::Call {
        name: "links_match".into(),
        args: vec![
            Expr::column(LINKS),
            Expr::literal(key),
            Expr::literal(value),
        ],
    };
    match op {
        "=" => Ok(matched),
        "!=" => Ok(Expr::Not(Box::new(matched))),
        other => Err(Diagnostic::at(
            span,
            format!("link fields compare with = or !=, not {other}"),
        )),
    }
}

fn pipeline(pipeline: &Pipeline, cx: &mut Context) -> Result<Spanset, Diagnostic> {
    let mut current = spanset(&pipeline.first, cx)?;
    for stage in &pipeline.stages {
        current = match stage {
            Stage::Spanset(expr) => Spanset::Pipe(Box::new(current), Box::new(spanset(expr, cx)?)),
            Stage::Aggregate {
                function,
                arg,
                op,
                value,
                span,
            } => Spanset::AggregateFilter {
                input: Box::new(current),
                function: match function.as_str() {
                    "count" => SpanAggregate::Count,
                    "avg" => SpanAggregate::Avg,
                    "min" => SpanAggregate::Min,
                    "max" => SpanAggregate::Max,
                    _ => SpanAggregate::Sum,
                },
                arg: match arg {
                    Some(arg) => Some(field(arg, Some(DataType::Float64), cx)?),
                    None => None,
                },
                op: comparison(op, *span)?,
                value: threshold(value, *span)?,
            },
            Stage::Select(fields) => {
                for field in fields {
                    note_shown(field, &mut cx.shown);
                }
                current
            }
            Stage::By(key) => Spanset::By {
                input: Box::new(current),
                key: field(key, Some(DataType::Utf8), cx)?,
                label: format!("by({key})"),
            },
            Stage::Coalesce => Spanset::Coalesce(Box::new(current)),
        };
    }
    Ok(current)
}

fn spanset(expr: &SpansetExpr, cx: &mut Context) -> Result<Spanset, Diagnostic> {
    Ok(match expr {
        SpansetExpr::Filter {
            condition: None | Some(FieldExpr::Static(Static::Bool(true))),
            ..
        } => Spanset::Filter(None),
        SpansetExpr::Filter {
            condition: Some(condition),
            ..
        } => Spanset::Filter(Some(field(condition, Some(DataType::Boolean), cx)?)),
        SpansetExpr::And(left, right) => {
            Spanset::And(Box::new(spanset(left, cx)?), Box::new(spanset(right, cx)?))
        }
        SpansetExpr::Or(left, right) => {
            Spanset::Or(Box::new(spanset(left, cx)?), Box::new(spanset(right, cx)?))
        }
        SpansetExpr::Structural { left, op, right } => Spanset::Structural {
            left: Box::new(spanset(left, cx)?),
            op: Structural {
                relation: match op.relation {
                    ">>" => Relation::Descendant,
                    ">" => Relation::Child,
                    "~" => Relation::Sibling,
                    "<<" => Relation::Ancestor,
                    _ => Relation::Parent,
                },
                negated: op.negated,
                union: op.union,
            },
            right: Box::new(spanset(right, cx)?),
        },
    })
}

fn comparison(op: &str, span: Span) -> Result<BinaryOp, Diagnostic> {
    Ok(match op {
        "=" => BinaryOp::Eq,
        "!=" => BinaryOp::NotEq,
        "<" => BinaryOp::Lt,
        "<=" => BinaryOp::LtEq,
        ">" => BinaryOp::Gt,
        ">=" => BinaryOp::GtEq,
        other => return Err(Diagnostic::at(span, format!("unknown comparison {other}"))),
    })
}

fn threshold(value: &Static, span: Span) -> Result<Literal, Diagnostic> {
    match literal(value) {
        Literal::Null | Literal::Bool(_) | Literal::Text(_) => Err(Diagnostic::at(
            span,
            "an aggregate filter compares with a number or a duration",
        )),
        number => Ok(number),
    }
}

/// A literal; durations become nanoseconds, statuses and kinds the
/// names the columns store.
fn literal(value: &Static) -> Literal {
    match value {
        Static::String(s) | Static::Status(s) | Static::Kind(s) => Literal::Text(s.clone()),
        Static::Int(i) => Literal::Int(*i),
        Static::Float(f) => Literal::Float(*f),
        Static::Bool(b) => Literal::Bool(*b),
        Static::Nil => Literal::Null,
        Static::Duration(d) => Literal::Int(i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)),
    }
}

/// The type a comparison with `value` reads an attribute as.
fn static_type(value: &Static) -> Option<DataType> {
    match value {
        Static::String(_) | Static::Status(_) | Static::Kind(_) => Some(DataType::Utf8),
        Static::Int(_) | Static::Duration(_) => Some(DataType::Int64),
        Static::Float(_) => Some(DataType::Float64),
        Static::Bool(_) => Some(DataType::Boolean),
        Static::Nil => None,
    }
}

/// The column of an intrinsic and its type.
fn intrinsic(scope: Option<&str>, name: &str, span: Span) -> Result<(Expr, DataType), Diagnostic> {
    let column = |c: &str, t: DataType| Ok((Expr::column(c), t));
    match (scope, name) {
        (None | Some("span"), "duration") => column(DURATION_NS, DataType::Int64),
        (None | Some("span"), "name") => column(NAME, DataType::Utf8),
        (None | Some("span"), "status") => column(STATUS_CODE, DataType::Utf8),
        (None | Some("span"), "statusMessage") => column(STATUS_MESSAGE, DataType::Utf8),
        (None | Some("span"), "kind") => column(SPAN_KIND, DataType::Utf8),
        (None | Some("trace"), "rootName") => column(ROOT_NAME, DataType::Utf8),
        (None, "rootServiceName") | (Some("trace"), "rootService") => {
            column(ROOT_SERVICE, DataType::Utf8)
        }
        (None, "traceDuration") | (Some("trace"), "duration") => {
            column(TRACE_DURATION_NS, DataType::Int64)
        }
        (None | Some("span"), "nestedSetLeft") => column(NEST_LEFT, DataType::Int64),
        (None | Some("span"), "nestedSetRight") => column(NEST_RIGHT, DataType::Int64),
        (None | Some("span"), "nestedSetParent") => column(NEST_PARENT, DataType::Int64),
        (Some("instrumentation"), "name") => {
            Ok((path("scope.name", DataType::Utf8), DataType::Utf8))
        }
        (Some("instrumentation"), "version") => {
            Ok((path("scope.version", DataType::Utf8), DataType::Utf8))
        }
        _ => Err(Diagnostic::at(
            span,
            format!(
                "the intrinsic {}{name} is not supported yet",
                scope.map(|s| format!("{s}:")).unwrap_or_default()
            ),
        )),
    }
}

fn path(path: &str, data_type: DataType) -> Expr {
    Expr::Path {
        path: path.to_owned(),
        data_type,
    }
}

/// The expression of an attribute, read as `data_type`.
fn attribute(
    scope: Option<&str>,
    name: &str,
    data_type: DataType,
    span: Span,
) -> Result<Expr, Diagnostic> {
    Ok(match scope {
        Some("span") => path(&format!("attr.{name}"), data_type),
        Some("resource") => path(&format!("resource.{name}"), data_type),
        Some("instrumentation") => path(&format!("scope.{name}"), data_type),
        None => Expr::Call {
            name: "coalesce".into(),
            args: vec![
                path(&format!("attr.{name}"), data_type.clone()),
                path(&format!("resource.{name}"), data_type),
            ],
        },
        Some(other) => {
            return Err(Diagnostic::at(
                span,
                format!("the {other}. scope is not supported yet"),
            ));
        }
    })
}

/// Records an attribute or intrinsic the API should show.
fn note_shown(expr: &FieldExpr, shown: &mut Vec<String>) {
    if matches!(
        expr,
        FieldExpr::Attribute { .. } | FieldExpr::Intrinsic { .. }
    ) {
        let text = expr.to_string();
        if !shown.contains(&text) {
            shown.push(text);
        }
    }
}

/// The type `expr` reads as when it decides the type of the other side
/// of a comparison.
fn type_of(expr: &FieldExpr) -> Option<DataType> {
    match expr {
        FieldExpr::Static(value) => static_type(value),
        FieldExpr::Intrinsic { scope, name, span } => intrinsic(scope.as_deref(), name, *span)
            .ok()
            .map(|(_, t)| t),
        _ => None,
    }
}

/// Lowers a condition or value; `hint` is the type an attribute is read
/// as when nothing else decides it.
fn field(expr: &FieldExpr, hint: Option<DataType>, cx: &mut Context) -> Result<Expr, Diagnostic> {
    match expr {
        FieldExpr::Static(value) => Ok(Expr::Literal(literal(value))),
        FieldExpr::Intrinsic { scope, name, span } => {
            note_shown(expr, &mut cx.shown);
            intrinsic(scope.as_deref(), name, *span).map(|(e, _)| e)
        }
        FieldExpr::Attribute { scope, name, span } => {
            note_shown(expr, &mut cx.shown);
            attribute(
                scope.as_deref(),
                name,
                hint.unwrap_or(DataType::Utf8),
                *span,
            )
        }
        FieldExpr::Binary {
            op,
            left,
            right,
            span,
        } => binary(op, left, right, *span, cx),
        FieldExpr::Not(inner) => Ok(Expr::Not(Box::new(field(
            inner,
            Some(DataType::Boolean),
            cx,
        )?))),
        FieldExpr::Negate(inner) => Ok(Expr::Negate(Box::new(field(
            inner,
            Some(DataType::Float64),
            cx,
        )?))),
    }
}

fn binary(
    op: &str,
    left: &FieldExpr,
    right: &FieldExpr,
    span: Span,
    cx: &mut Context,
) -> Result<Expr, Diagnostic> {
    if !matches!(op, "&&" | "||") {
        let scoped = |scope: &str| scope_of(left) == Some(scope) || scope_of(right) == Some(scope);
        if scoped("event") {
            return event_condition(op, left, right, span, cx);
        }
        if scoped("link") {
            return link_condition(op, left, right, span);
        }
    }
    match op {
        "&&" | "||" => {
            let l = field(left, Some(DataType::Boolean), cx)?;
            let r = field(right, Some(DataType::Boolean), cx)?;
            Ok(if op == "&&" { l.and(r) } else { l.or(r) })
        }
        "=~" | "!~" => {
            let FieldExpr::Static(Static::String(pattern)) = right else {
                return Err(Diagnostic::at(
                    span,
                    "a regular expression match needs a string on its right",
                ));
            };
            let value = field(left, Some(DataType::Utf8), cx)?;
            let matched = Expr::Call {
                name: "regexp_match".into(),
                args: vec![value, Expr::literal(format!("^(?:{pattern})$"))],
            };
            Ok(if op == "=~" {
                matched
            } else {
                Expr::Not(Box::new(matched))
            })
        }
        "=" | "!="
            if matches!(right, FieldExpr::Static(Static::Nil))
                || matches!(left, FieldExpr::Static(Static::Nil)) =>
        {
            let tested = if matches!(right, FieldExpr::Static(Static::Nil)) {
                left
            } else {
                right
            };
            Ok(Expr::IsNull {
                expr: Box::new(field(tested, Some(DataType::Utf8), cx)?),
                negated: op == "!=",
            })
        }
        "=" | "!=" | "<" | "<=" | ">" | ">=" => {
            // An attribute reads as the type of the other side.
            let hint = type_of(right).or_else(|| type_of(left));
            let l = field(left, hint.clone(), cx)?;
            let r = field(right, hint, cx)?;
            Ok(l.binary(comparison(op, span)?, r))
        }
        "+" | "-" | "*" | "/" | "%" => {
            let l = field(left, Some(DataType::Float64), cx)?;
            let r = field(right, Some(DataType::Float64), cx)?;
            let op = match op {
                "+" => BinaryOp::Add,
                "-" => BinaryOp::Sub,
                "*" => BinaryOp::Mul,
                "/" => BinaryOp::Div,
                _ => BinaryOp::Rem,
            };
            Ok(l.binary(op, r))
        }
        other => Err(Diagnostic::at(
            span,
            format!("the {other} operator is not supported yet"),
        )),
    }
}

/// Whether `expr` is a duration intrinsic, measured in nanoseconds.
fn is_duration(expr: &FieldExpr) -> bool {
    match expr {
        // `trace:duration` and `span:duration` share the name `duration`.
        FieldExpr::Intrinsic { name, .. } => name == "duration" || name == "traceDuration",
        _ => false,
    }
}

/// The series label of `quantile_over_time`: the quantile, as Tempo
/// labels it.
const QUANTILE_LABEL: &str = "p";
/// The series label of `histogram_over_time`: the bucket's upper bound.
const BUCKET_LABEL: &str = "__bucket";

/// The metrics plan over `input` rows of spans: one value per step
/// bucket and series, with the series labels as written, then `p` for
/// `quantile_over_time` and `__bucket` for `histogram_over_time`.
fn metrics_plan(
    metrics: &Metrics,
    input: Plan,
    params: RangeParams,
    cx: &mut Context,
) -> Result<(Plan, Vec<String>), Diagnostic> {
    // One bucket over the whole range for an instant query; the whole
    // range is then also the step `rate` divides by.
    let (bucket, step) = if params.step.is_zero() {
        let whole = Duration::from_nanos(
            u64::try_from(next(params.end).as_nanos() - params.start.as_nanos()).unwrap_or(1),
        );
        (Expr::literal(params.start), whole)
    } else {
        (
            Expr::Call {
                name: "time_bucket".into(),
                args: vec![Expr::literal(params.step), Expr::column(TIMESTAMP)],
            },
            params.step,
        )
    };
    let mut group_by = vec![NamedExpr::new(bucket, TIMESTAMP)];
    let mut labels = Vec::new();
    for key in &metrics.by {
        let name = key.to_string();
        group_by.push(NamedExpr::new(
            field(key, Some(DataType::Utf8), cx)?,
            name.clone(),
        ));
        labels.push(name);
    }
    let value = match &metrics.arg {
        Some(arg) => Some(field(arg, Some(DataType::Float64), cx)?),
        None => None,
    };
    let duration = metrics.arg.as_ref().is_some_and(is_duration);
    let function = metrics.function.as_str();
    let aggregated = match (function, value) {
        ("quantile_over_time", Some(v)) => {
            if metrics.quantiles.is_empty() {
                return Err(Diagnostic::at(
                    metrics.span,
                    "quantile_over_time needs at least one quantile",
                ));
            }
            labels.push(QUANTILE_LABEL.into());
            quantiles_plan(&input, &group_by, &v, &metrics.quantiles)
        }
        ("histogram_over_time", Some(v)) => {
            labels.push(BUCKET_LABEL.into());
            histogram_plan(input, group_by, v, duration)
        }
        (function, value) => {
            let (function, args) = simple_aggregate(function, value, metrics.span)?;
            Plan::Aggregate {
                input: Box::new(input),
                group_by,
                aggregates: vec![AggregateExpr {
                    function: function.into(),
                    args,
                    name: VALUE.into(),
                }],
            }
        }
    };
    let mut exprs = vec![NamedExpr::new(Expr::column(TIMESTAMP), TIMESTAMP)];
    for label in &labels {
        exprs.push(NamedExpr::new(Expr::column(label.as_str()), label.as_str()));
    }
    // Every value is a float, whatever the aggregate's own type.
    let value = Expr::Cast {
        expr: Box::new(Expr::column(VALUE)),
        to: DataType::Float64,
    };
    let value = if function == "rate" {
        value.binary(BinaryOp::Div, Expr::literal(step.as_secs_f64()))
    } else if duration && function != "histogram_over_time" {
        // A duration is reported in seconds, as Tempo reports it.
        value.binary(BinaryOp::Div, Expr::literal(1e9))
    } else {
        value
    };
    exprs.push(NamedExpr::new(value, VALUE));
    let plan = aggregated
        .project(exprs)
        .sort(vec![SortKey::ascending(Expr::column(TIMESTAMP))]);
    Ok((plan, labels))
}

/// The engine aggregate of a plain metrics function over `value`.
fn simple_aggregate(
    function: &str,
    value: Option<Expr>,
    span: Span,
) -> Result<(&'static str, Vec<Expr>), Diagnostic> {
    match (function, value) {
        ("rate" | "count_over_time", _) => Ok(("count", Vec::new())),
        ("min_over_time", Some(v)) => Ok(("min", vec![v])),
        ("max_over_time", Some(v)) => Ok(("max", vec![v])),
        ("avg_over_time", Some(v)) => Ok(("avg", vec![v])),
        ("sum_over_time", Some(v)) => Ok(("sum", vec![v])),
        (other, _) => Err(Diagnostic::at(
            span,
            format!("{other} is not supported yet"),
        )),
    }
}

/// The spans of `input` counted per power-of-two bucket of `value`,
/// the bucket a `__bucket` key after `group_by`; a duration is
/// bucketed in nanoseconds and labelled in seconds, as Tempo labels it.
fn histogram_plan(input: Plan, mut group_by: Vec<NamedExpr>, value: Expr, duration: bool) -> Plan {
    let bucket = Expr::Call {
        name: "log2_bucket".into(),
        args: vec![value],
    };
    let bucket = if duration {
        bucket.binary(BinaryOp::Div, Expr::literal(1e9))
    } else {
        bucket
    };
    group_by.push(NamedExpr::new(
        Expr::Cast {
            expr: Box::new(bucket),
            to: DataType::Utf8,
        },
        BUCKET_LABEL,
    ));
    Plan::Aggregate {
        input: Box::new(input),
        group_by,
        aggregates: vec![AggregateExpr {
            function: "count".into(),
            args: Vec::new(),
            name: VALUE.into(),
        }],
    }
}

/// One branch per quantile, each the aggregate over its own scan of
/// `input` with the quantile as the `p` label, in a union.
fn quantiles_plan(input: &Plan, group_by: &[NamedExpr], value: &Expr, quantiles: &[f64]) -> Plan {
    let mut branches: Vec<Plan> = quantiles
        .iter()
        .map(|q| {
            let aggregated = Plan::Aggregate {
                input: Box::new(input.clone()),
                group_by: group_by.to_vec(),
                aggregates: vec![AggregateExpr {
                    function: "quantile".into(),
                    args: vec![value.clone(), Expr::literal(*q)],
                    name: VALUE.into(),
                }],
            };
            let mut exprs: Vec<NamedExpr> = group_by
                .iter()
                .map(|key| NamedExpr::new(Expr::column(key.name.as_str()), key.name.as_str()))
                .collect();
            exprs.push(NamedExpr::new(Expr::literal(q.to_string()), QUANTILE_LABEL));
            exprs.push(NamedExpr::new(Expr::column(VALUE), VALUE));
            aggregated.project(exprs)
        })
        .collect();
    if branches.len() == 1 {
        branches.remove(0)
    } else {
        Plan::Union { inputs: branches }
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
            Timestamp::from_nanos(10_000),
        )
    }

    fn params(step: u64) -> RangeParams {
        RangeParams {
            start: Timestamp::from_nanos(1_000),
            end: Timestamp::from_nanos(9_999),
            step: Duration::from_nanos(step),
        }
    }

    fn search(text: &str) -> Lowered {
        lower(&parse(text).unwrap(), &ctx(), params(0)).unwrap()
    }

    #[test]
    fn a_search_becomes_a_trace_group_with_candidates() {
        let lowered = search(
            r#"{ resource.service.name = "api" && span.http.status_code >= 500 } >> { status = error && .retry = true }"#,
        );
        assert_eq!(lowered.kind, ResultKind::Spansets);
        let text = lowered.plan.to_string();
        assert!(
            text.starts_with(
                "TraceGroup: ({ ((resource.service.name = 'api') AND (attr.http.status_code >= 500)) } >> { ((status_code = 'error') AND (coalesce(attr.retry, resource.retry) = true)) })"
            ),
            "{text}"
        );
        assert!(text.contains("candidates:"), "{text}");
        assert!(
            text.contains("[1970-01-01T00:00:00.000001000Z, 1970-01-01T00:00:00.000010000Z)"),
            "{text}"
        );
        assert_eq!(
            lowered.shown,
            vec![
                "resource.service.name",
                "span.http.status_code",
                "status",
                ".retry"
            ]
        );
        let Plan::TraceGroup { spanset, .. } = lowered.plan else {
            panic!("not a trace group");
        };
        let Spanset::Structural { right, .. } = spanset else {
            panic!("not structural");
        };
        let Spanset::Filter(Some(condition)) = *right else {
            panic!("not a filter");
        };
        assert!(
            condition
                .to_string()
                .contains("coalesce(attr.retry, resource.retry)"),
            "an unscoped attribute reads the span, then the resource"
        );
        let Expr::Binary { right, .. } = condition else {
            panic!("not binary");
        };
        let Expr::Binary { left, .. } = *right else {
            panic!("not binary");
        };
        let Expr::Call { args, .. } = *left else {
            panic!("not a call");
        };
        assert!(
            matches!(
                &args[0],
                Expr::Path {
                    data_type: DataType::Boolean,
                    ..
                }
            ),
            "typed from the literal"
        );
    }

    /// QL-15: an event condition flags the spans through a left join on
    /// the events that pass it; a link condition reads the links column.
    #[test]
    fn event_and_link_scopes_lower() {
        let lowered =
            search("{ event:name = \"exception\" && event.level != \"debug\" && span.x = 1 }");
        let text = lowered.plan.to_string();
        assert!(
            text.starts_with(
                "TraceGroup: { ((_event_0 AND _event_1) AND (attr.x = 1)) }\n  \
                 candidates:\n    Filter: ((_event_0 AND _event_1) AND (attr.x = 1))\n      Project:"
            ),
            "{text}"
        );
        assert!(
            text.contains("Join left: s.trace_id = e1.trace_id AND s.span_id = e1.span_id"),
            "{text}"
        );
        assert!(
            text.contains("Aggregate: group_by=[trace_id AS trace_id, span_id AS span_id] aggregates=[count_raw() AS n]\n")
                && text.contains("Scan: prod/event"),
            "{text}"
        );
        assert!(
            text.contains("(name = 'exception')") && text.contains("(attr.level <> 'debug')"),
            "the conditions read the event rows: {text}"
        );
        assert_eq!(
            lowered.shown,
            vec!["span.x"],
            "event fields are not span columns"
        );
        let linked = search("{ link.rel = \"parent\" || link:spanID != \"0202\" }");
        let text = linked.plan.to_string();
        assert!(
            text.starts_with(
                "TraceGroup: { (links_match(links, 'attributes.rel', 'parent') OR NOT links_match(links, 'span_id', '0202')) }"
            ),
            "{text}"
        );
    }

    #[test]
    fn stages_nil_regex_and_durations_lower() {
        let lowered = search(
            "{ duration > 100ms && span.x != nil && name =~ `GET.*` } | count() > 2 | avg(duration) < 1s | by(resource.service.name) | select(span.y) | coalesce() | { .z = 1 }",
        );
        let text = lowered.plan.to_string();
        assert!(
            text.starts_with(
                "TraceGroup: { (((duration_ns > 100000000) AND attr.x IS NOT NULL) AND regexp_match(name, '^(?:GET.*)$')) } | count() > 2 | avg(duration_ns) < 1000000000 | by(resource.service.name) | coalesce() | { (coalesce(attr.z, resource.z) = 1) }"
            ),
            "{text}"
        );
        assert_eq!(
            lowered.shown,
            vec![
                "duration",
                "span.x",
                "name",
                "resource.service.name",
                "span.y",
                ".z"
            ]
        );
        assert!(matches!(
            search("{ } | { .a }").plan,
            Plan::TraceGroup {
                candidates: None,
                ..
            }
        ));
    }

    #[test]
    fn metrics_aggregate_per_step_and_label() {
        let lowered = lower(
            &parse("{ status = error } | rate() by (resource.service.name, span.http.method)")
                .unwrap(),
            &ctx(),
            params(1_000),
        )
        .unwrap();
        assert_eq!(lowered.kind, ResultKind::Metrics);
        assert_eq!(
            lowered.labels,
            vec!["resource.service.name", "span.http.method"]
        );
        let text = lowered.plan.to_string();
        assert!(text.starts_with("Sort: timestamp ASC\n  Project: timestamp AS timestamp, resource.service.name AS resource.service.name, span.http.method AS span.http.method, (CAST(value AS Float64) / 1e-6) AS value\n    Aggregate:"), "{text}");
        assert!(
            text.contains("time_bucket(INTERVAL '1000 ns', timestamp) AS timestamp"),
            "{text}"
        );
        assert!(text.contains("count() AS value"), "{text}");
        assert!(text.contains("Scan: prod/span"), "{text}");
        assert!(
            !text.contains("TraceGroup"),
            "a plain filter scans directly"
        );

        let structural = lower(
            &parse("{ .a } >> { .b } | max_over_time(duration)").unwrap(),
            &ctx(),
            params(0),
        )
        .unwrap();
        let text = structural.plan.to_string();
        assert!(text.contains("TraceGroup"), "{text}");
        assert!(text.contains("max(duration_ns) AS value"), "{text}");
        assert!(
            text.contains("(CAST(value AS Float64) / 1000000000.0) AS value"),
            "durations in seconds: {text}"
        );
        assert_eq!(structural.function, "max_over_time");
        assert!(
            text.contains("group_by=[TIMESTAMP '1970-01-01T00:00:00.000001000Z' AS timestamp]"),
            "one bucket over the range: {text}"
        );
    }

    /// QL-12: quantiles are one union branch each, labelled `p`, and
    /// histograms count per power-of-two bucket, labelled `__bucket`.
    #[test]
    fn quantiles_and_histograms_label_their_series() {
        let quantiles = lower(
            &parse("{ } | quantile_over_time(duration, 0.5, 0.99) by (resource.service.name)")
                .unwrap(),
            &ctx(),
            params(1_000),
        )
        .unwrap();
        assert_eq!(quantiles.labels, vec!["resource.service.name", "p"]);
        let text = quantiles.plan.to_string();
        assert!(text.contains("Union"), "{text}");
        assert!(
            text.contains("quantile(duration_ns, 0.5) AS value"),
            "{text}"
        );
        assert!(
            text.contains("quantile(duration_ns, 0.99) AS value"),
            "{text}"
        );
        assert!(text.contains("'0.99' AS p"), "{text}");
        assert!(
            text.contains("(CAST(value AS Float64) / 1000000000.0) AS value"),
            "durations in seconds: {text}"
        );

        let one = lower(
            &parse("{ } | quantile_over_time(duration, 0.5)").unwrap(),
            &ctx(),
            params(0),
        )
        .unwrap();
        assert!(
            !one.plan.to_string().contains("Union"),
            "one quantile, no union"
        );

        let histogram = lower(
            &parse("{ } | histogram_over_time(duration)").unwrap(),
            &ctx(),
            params(1_000),
        )
        .unwrap();
        assert_eq!(histogram.labels, vec!["__bucket"]);
        let text = histogram.plan.to_string();
        assert!(
            text.contains("CAST((log2_bucket(duration_ns) / 1000000000.0) AS Utf8) AS __bucket"),
            "{text}"
        );
        assert!(text.contains("count() AS value"), "{text}");
        assert!(
            !text.contains("(CAST(value AS Float64) / 1000000000.0)"),
            "a count is not a duration: {text}"
        );
    }

    /// The nested-set intrinsics are the trace group's columns, so a
    /// search on them reads every span and a metrics query over them
    /// goes through the group rather than a direct scan.
    #[test]
    fn nested_set_intrinsics_read_the_trace_groups_numbers() {
        let roots = search("{ nestedSetParent = -1 && nestedSetLeft >= 1 }");
        let text = roots.plan.to_string();
        assert!(
            text.contains("TraceGroup: { ((nest_parent = -1) AND (nest_left >= 1)) }"),
            "{text}"
        );
        assert!(
            !text.contains(" predicates=[(nest_parent"),
            "every span of a file without the shape is a candidate: {text}"
        );
        assert!(
            text.contains("stored_predicates=[((nest_parent = -1) AND (nest_left >= 1))]"),
            "a file storing the shape answers the condition (QL-14): {text}"
        );
        let metrics = lower(
            &parse("{ nestedSetParent < 0 } | rate()").unwrap(),
            &ctx(),
            params(1_000),
        )
        .unwrap();
        let text = metrics.plan.to_string();
        assert!(text.contains("TraceGroup"), "{text}");
        assert!(text.contains("nest_parent < 0"), "{text}");
    }

    #[test]
    fn unsupported_constructs_are_reported_with_positions() {
        let error = lower(
            &parse("{ event.name = span.name }").unwrap(),
            &ctx(),
            params(0),
        )
        .unwrap_err();
        assert!(error.message.contains("span field"), "{}", error.message);
        let error = lower(&parse("{ link.rel > 1 }").unwrap(), &ctx(), params(0)).unwrap_err();
        assert!(error.message.contains("= or !="), "{}", error.message);
        let error = lower(&parse("{ childCount > 0 }").unwrap(), &ctx(), params(0)).unwrap_err();
        assert!(error.message.contains("childCount"), "{}", error.message);
        let error = lower(
            &parse("{ } | quantile_over_time(duration)").unwrap(),
            &ctx(),
            params(0),
        )
        .unwrap_err();
        assert!(
            error.message.contains("quantile_over_time needs"),
            "{}",
            error.message
        );
        let error = lower(&parse("{ .a =~ 1 }").unwrap(), &ctx(), params(0)).unwrap_err();
        assert!(error.message.contains("string"), "{}", error.message);
        let error = lower(&parse("{ 2 ^ 3 = 8 }").unwrap(), &ctx(), params(0)).unwrap_err();
        assert!(error.message.contains("^ operator"), "{}", error.message);
    }
}
