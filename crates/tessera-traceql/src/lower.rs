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
//! Not supported yet: the `event.` and `link.` scopes (QL-15), the
//! nested-set intrinsics, `quantile_over_time` and
//! `histogram_over_time`.

use std::time::Duration;

use arrow::datatypes::DataType;
use tessera_ir::trace::{ROOT_NAME, ROOT_SERVICE, TRACE_DURATION_NS};
use tessera_ir::{
    AggregateExpr, BinaryOp, BindContext, Diagnostic, Expr, Literal, NamedExpr, Plan, Relation,
    Scan, SortKey, Span, SpanAggregate, Spanset, Structural, VALUE,
};
use tessera_types::event::{DURATION_NS, NAME, SPAN_KIND, STATUS_CODE, STATUS_MESSAGE, TIMESTAMP};
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
    let mut shown = Vec::new();
    let spanset = pipeline(&query.pipeline, &mut shown)?;
    match &query.metrics {
        None => Ok(Lowered {
            plan: trace_group(spanset, ctx, range),
            kind: ResultKind::Spansets,
            shown,
            labels: Vec::new(),
            function: String::new(),
        }),
        Some(metrics) => {
            let input = match (&query.pipeline.first, query.pipeline.stages.is_empty()) {
                (SpansetExpr::Filter { .. }, true) => match spanset {
                    Spanset::Filter(condition) => {
                        let mut scan =
                            Scan::all(ctx.tenant.clone(), ctx.dataset.clone(), Kind::Span);
                        scan.time_range = range;
                        scan.predicates.extend(condition);
                        Plan::Scan(scan)
                    }
                    other => trace_group(other, ctx, range),
                },
                _ => trace_group(spanset, ctx, range),
            };
            let (plan, labels) = metrics_plan(metrics, input, params)?;
            Ok(Lowered {
                plan,
                kind: ResultKind::Metrics,
                shown,
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
fn trace_group(spanset: Spanset, ctx: &BindContext, range: TimeRange) -> Plan {
    let mut spans = Scan::all(ctx.tenant.clone(), ctx.dataset.clone(), Kind::Span);
    spans.time_range = range;
    let candidates = spanset.candidates().map(|predicate| {
        let mut scan = spans.clone();
        scan.predicates = vec![predicate];
        Box::new(Plan::Scan(scan))
    });
    Plan::TraceGroup {
        candidates,
        spans: Box::new(Plan::Scan(spans)),
        spanset,
    }
}

fn pipeline(pipeline: &Pipeline, shown: &mut Vec<String>) -> Result<Spanset, Diagnostic> {
    let mut current = spanset(&pipeline.first, shown)?;
    for stage in &pipeline.stages {
        current = match stage {
            Stage::Spanset(expr) => {
                Spanset::Pipe(Box::new(current), Box::new(spanset(expr, shown)?))
            }
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
                    Some(arg) => Some(field(arg, Some(DataType::Float64), shown)?),
                    None => None,
                },
                op: comparison(op, *span)?,
                value: threshold(value, *span)?,
            },
            Stage::Select(fields) => {
                for field in fields {
                    note_shown(field, shown);
                }
                current
            }
            Stage::By(key) => Spanset::By {
                input: Box::new(current),
                key: field(key, Some(DataType::Utf8), shown)?,
                label: format!("by({key})"),
            },
            Stage::Coalesce => Spanset::Coalesce(Box::new(current)),
        };
    }
    Ok(current)
}

fn spanset(expr: &SpansetExpr, shown: &mut Vec<String>) -> Result<Spanset, Diagnostic> {
    Ok(match expr {
        SpansetExpr::Filter {
            condition: None | Some(FieldExpr::Static(Static::Bool(true))),
            ..
        } => Spanset::Filter(None),
        SpansetExpr::Filter {
            condition: Some(condition),
            ..
        } => Spanset::Filter(Some(field(condition, Some(DataType::Boolean), shown)?)),
        SpansetExpr::And(left, right) => Spanset::And(
            Box::new(spanset(left, shown)?),
            Box::new(spanset(right, shown)?),
        ),
        SpansetExpr::Or(left, right) => Spanset::Or(
            Box::new(spanset(left, shown)?),
            Box::new(spanset(right, shown)?),
        ),
        SpansetExpr::Structural { left, op, right } => Spanset::Structural {
            left: Box::new(spanset(left, shown)?),
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
            right: Box::new(spanset(right, shown)?),
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
fn field(
    expr: &FieldExpr,
    hint: Option<DataType>,
    shown: &mut Vec<String>,
) -> Result<Expr, Diagnostic> {
    match expr {
        FieldExpr::Static(value) => Ok(Expr::Literal(literal(value))),
        FieldExpr::Intrinsic { scope, name, span } => {
            note_shown(expr, shown);
            intrinsic(scope.as_deref(), name, *span).map(|(e, _)| e)
        }
        FieldExpr::Attribute { scope, name, span } => {
            note_shown(expr, shown);
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
        } => binary(op, left, right, *span, shown),
        FieldExpr::Not(inner) => Ok(Expr::Not(Box::new(field(
            inner,
            Some(DataType::Boolean),
            shown,
        )?))),
        FieldExpr::Negate(inner) => Ok(Expr::Negate(Box::new(field(
            inner,
            Some(DataType::Float64),
            shown,
        )?))),
    }
}

fn binary(
    op: &str,
    left: &FieldExpr,
    right: &FieldExpr,
    span: Span,
    shown: &mut Vec<String>,
) -> Result<Expr, Diagnostic> {
    match op {
        "&&" | "||" => {
            let l = field(left, Some(DataType::Boolean), shown)?;
            let r = field(right, Some(DataType::Boolean), shown)?;
            Ok(if op == "&&" { l.and(r) } else { l.or(r) })
        }
        "=~" | "!~" => {
            let FieldExpr::Static(Static::String(pattern)) = right else {
                return Err(Diagnostic::at(
                    span,
                    "a regular expression match needs a string on its right",
                ));
            };
            let value = field(left, Some(DataType::Utf8), shown)?;
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
                expr: Box::new(field(tested, Some(DataType::Utf8), shown)?),
                negated: op == "!=",
            })
        }
        "=" | "!=" | "<" | "<=" | ">" | ">=" => {
            // An attribute reads as the type of the other side.
            let hint = type_of(right).or_else(|| type_of(left));
            let l = field(left, hint.clone(), shown)?;
            let r = field(right, hint, shown)?;
            Ok(l.binary(comparison(op, span)?, r))
        }
        "+" | "-" | "*" | "/" | "%" => {
            let l = field(left, Some(DataType::Float64), shown)?;
            let r = field(right, Some(DataType::Float64), shown)?;
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

/// The metrics plan over `input` rows of spans: one value per step
/// bucket and series, with the series labels as written.
fn metrics_plan(
    metrics: &Metrics,
    input: Plan,
    params: RangeParams,
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
    let mut shown = Vec::new();
    let mut group_by = vec![NamedExpr::new(bucket, TIMESTAMP)];
    let mut labels = Vec::new();
    for key in &metrics.by {
        let name = key.to_string();
        group_by.push(NamedExpr::new(
            field(key, Some(DataType::Utf8), &mut shown)?,
            name.clone(),
        ));
        labels.push(name);
    }
    let value = match &metrics.arg {
        Some(arg) => Some(field(arg, Some(DataType::Float64), &mut shown)?),
        None => None,
    };
    let (function, args) = match (metrics.function.as_str(), value) {
        ("rate" | "count_over_time", _) => ("count", Vec::new()),
        ("min_over_time", Some(v)) => ("min", vec![v]),
        ("max_over_time", Some(v)) => ("max", vec![v]),
        ("avg_over_time", Some(v)) => ("avg", vec![v]),
        ("sum_over_time", Some(v)) => ("sum", vec![v]),
        (other, _) => {
            return Err(Diagnostic::at(
                metrics.span,
                format!("{other} is not supported yet"),
            ));
        }
    };
    let aggregated = Plan::Aggregate {
        input: Box::new(input),
        group_by,
        aggregates: vec![AggregateExpr {
            function: function.into(),
            args,
            name: VALUE.into(),
        }],
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
    let value = if metrics.function == "rate" {
        value.binary(BinaryOp::Div, Expr::literal(step.as_secs_f64()))
    } else if metrics.arg.as_ref().is_some_and(is_duration) {
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

#[cfg(test)]
mod tests {
    use tessera_types::{DatasetName, TenantId};

    use super::*;
    use crate::parse;

    fn ctx() -> BindContext {
        BindContext {
            tenant: TenantId::DEFAULT,
            dataset: DatasetName::new("prod").unwrap(),
            now: Timestamp::from_nanos(10_000),
        }
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

    #[test]
    fn unsupported_constructs_are_reported_with_positions() {
        let error =
            lower(&parse("{ event.name = \"x\" }").unwrap(), &ctx(), params(0)).unwrap_err();
        assert_eq!(error.span, Some(Span::new(2, 12)));
        assert!(error.message.contains("event. scope"), "{}", error.message);
        let error = lower(&parse("{ nestedSetLeft > 0 }").unwrap(), &ctx(), params(0)).unwrap_err();
        assert!(error.message.contains("nestedSetLeft"), "{}", error.message);
        let error = lower(
            &parse("{ } | quantile_over_time(duration, 0.9)").unwrap(),
            &ctx(),
            params(0),
        )
        .unwrap_err();
        assert!(
            error.message.contains("quantile_over_time"),
            "{}",
            error.message
        );
        let error = lower(&parse("{ .a =~ 1 }").unwrap(), &ctx(), params(0)).unwrap_err();
        assert!(error.message.contains("string"), "{}", error.message);
        let error = lower(&parse("{ 2 ^ 3 = 8 }").unwrap(), &ctx(), params(0)).unwrap_err();
        assert!(error.message.contains("^ operator"), "{}", error.message);
    }
}
