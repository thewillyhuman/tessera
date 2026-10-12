// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Lowering: from the syntax tree to the shared plan.
//!
//! A query becomes `Scan → Filter → [Aggregate] → [Sort] → [Project] →
//! [Sort] → [Limit]`. Names resolve against the kind's schema; dotted
//! names under `attr`, `resource` or `scope` are attribute paths. With
//! aggregates or `GROUP BY`, every select item must be a grouping
//! expression, an aggregate, or an expression over those, as in
//! `PostgreSQL`.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use tessera_ir::{
    AggregateExpr, AlertDefinition, Asof, AsofMatch, BinaryOp, BindContext, ControlStatement,
    Diagnostic, Expr, JoinKind, JoinSide, Literal, NamedExpr, Plan, PlanError, Registry,
    RollupDefinition, Scan, SortKey, Span, Statement, qualify, unqualify,
};
use tessera_types::{
    DatasetName, Kind, RetentionAction, RetentionClause, RetentionPolicy, TenantId, Timestamp,
    event,
};

use crate::ast;

/// Lowers a parsed statement under `ctx`.
///
/// # Errors
///
/// The first name, function or literal that cannot be resolved, with its
/// position.
pub fn lower(statement: &ast::Statement, ctx: &BindContext) -> Result<Statement, Diagnostic> {
    lower_with(statement, ctx, &Parameters::Bound(&[]))
}

/// A value bound to a `$n` parameter, as a client sends it.
#[derive(Clone, PartialEq, Debug)]
pub enum Parameter {
    /// SQL `NULL`.
    Null,
    /// A boolean.
    Bool(bool),
    /// An integer.
    Int(i64),
    /// A floating-point number.
    Float(f64),
    /// Text; against a timestamp column it reads as a timestamp, as a
    /// text literal does.
    Text(String),
    /// A timestamp.
    Timestamp(Timestamp),
}

impl Parameter {
    /// The literal the parameter stands for.
    fn literal(&self) -> Literal {
        match self {
            Parameter::Null => Literal::Null,
            Parameter::Bool(b) => Literal::from(*b),
            Parameter::Int(i) => Literal::from(*i),
            Parameter::Float(f) => Literal::from(*f),
            Parameter::Text(t) => Literal::from(t.as_str()),
            Parameter::Timestamp(t) => Literal::from(*t),
        }
    }
}

/// How `$n` parameters are read while lowering.
#[derive(Debug)]
pub enum Parameters<'a> {
    /// Each stands for the value at its position.
    Bound(&'a [Parameter]),
    /// None has a value yet: a client is asking what types the
    /// statement takes. Each lowers as a null and the type of what it
    /// is compared with or cast to is recorded at its position.
    Inferring(RefCell<Vec<Option<DataType>>>),
}

/// Lowers `statement` with its `$n` parameters read as `parameters` say.
///
/// # Errors
///
/// As [`lower`], and when a parameter has no value.
pub fn lower_with(
    statement: &ast::Statement,
    ctx: &BindContext,
    parameters: &Parameters,
) -> Result<Statement, Diagnostic> {
    let registry = Registry::builtin();
    match statement {
        ast::Statement::Select(query) => Ok(Statement::Query(lower_query(
            query, ctx, &registry, parameters,
        )?)),
        ast::Statement::Explain { analyze, query } => Ok(Statement::Explain {
            plan: lower_query(query, ctx, &registry, parameters)?,
            analyze: *analyze,
        }),
        ast::Statement::CreateAlert(alert) => lower_alert(alert, ctx, &registry, parameters),
        ast::Statement::DropAlert { name } => Ok(Statement::Control(ControlStatement::DropAlert {
            name: name.clone(),
        })),
        ast::Statement::CreateRollup(rollup) => lower_rollup(rollup, ctx, &registry, parameters),
        ast::Statement::DropRollup { name } => {
            Ok(Statement::Control(ControlStatement::DropRollup {
                name: name.clone(),
            }))
        }
        ast::Statement::ExplainAlert { name } => {
            Ok(Statement::Control(ControlStatement::ExplainAlert {
                name: name.clone(),
            }))
        }
        ast::Statement::CreateRetentionPolicy(policy) => lower_retention_policy(policy),
        ast::Statement::DropRetentionPolicy { name } => {
            Ok(Statement::Control(ControlStatement::DropRetentionPolicy {
                name: name.clone(),
            }))
        }
        ast::Statement::ExplainRetention { name } => {
            Ok(Statement::Control(ControlStatement::ExplainRetention {
                name: name.clone(),
            }))
        }
        ast::Statement::ExplainLayout { dataset } => {
            let dataset = match dataset {
                Some(name) => DatasetName::new(name.clone())
                    .map_err(|e| Diagnostic::unlocated(format!("invalid dataset: {e}")))?,
                None => ctx.dataset.clone(),
            };
            Ok(Statement::Control(ControlStatement::ExplainLayout {
                dataset,
            }))
        }
        ast::Statement::Insert {
            table,
            columns,
            rows,
        } => Ok(Statement::Control(ControlStatement::Insert {
            table: control_table(table)?,
            rows: rows
                .iter()
                .map(|row| {
                    columns
                        .iter()
                        .cloned()
                        .zip(row)
                        .map(|(c, e)| Ok((c, literal(e, ctx)?)))
                        .collect()
                })
                .collect::<Result<_, Diagnostic>>()?,
        })),
        ast::Statement::Update { table, set, filter } => {
            Ok(Statement::Control(ControlStatement::Update {
                table: control_table(table)?,
                set: literals(set, ctx)?,
                filter: literals(filter, ctx)?,
            }))
        }
        ast::Statement::Delete {
            table,
            filter,
            filter_text,
        } => lower_delete(
            table,
            filter.as_ref(),
            filter_text,
            ctx,
            &registry,
            parameters,
        ),
    }
}

/// `DELETE`: on a system table, the equalities of `WHERE`; on a
/// dataset's table, a tombstone over the kind's rows (SE-5), which
/// needs a `WHERE`.
fn lower_delete(
    table: &ast::TableRef,
    filter: Option<&ast::Expr>,
    filter_text: &str,
    ctx: &BindContext,
    registry: &Registry,
    parameters: &Parameters,
) -> Result<Statement, Diagnostic> {
    own_tenant(table)?;
    if table.dataset.as_deref() == Some("system") {
        let pairs = match filter {
            Some(filter) => equalities_of(filter)?,
            None => Vec::new(),
        };
        return Ok(Statement::Control(ControlStatement::Delete {
            table: table.table.clone(),
            filter: literals(&pairs, ctx)?,
        }));
    }
    let Some(filter) = filter else {
        return Err(Diagnostic::at(
            table.span,
            "DELETE on a dataset needs a WHERE; to remove everything, drop the dataset",
        ));
    };
    let (dataset, kind) = resolve_table(table, ctx)?;
    let lowerer = Lowerer {
        ctx,
        registry,
        parameters,
        scope: Scope::single(TableScope {
            name: table.table.clone(),
            schema: event::schema(kind),
            kinds: vec![kind],
        }),
        substitution: None,
    };
    let predicate = lowerer.expr(filter)?;
    Ok(Statement::Control(ControlStatement::DeleteRows {
        dataset,
        kind,
        predicate,
        text: filter_text.to_owned(),
    }))
}

/// `column = literal [AND ...]`, the only condition a system table takes.
fn equalities_of(filter: &ast::Expr) -> Result<Vec<(String, ast::Expr)>, Diagnostic> {
    match &filter.kind {
        ast::ExprKind::Binary {
            op: ast::BinaryOp::And,
            left,
            right,
        } => {
            let mut pairs = equalities_of(left)?;
            pairs.extend(equalities_of(right)?);
            Ok(pairs)
        }
        ast::ExprKind::Binary {
            op: ast::BinaryOp::Eq,
            left,
            right,
        } => match &left.kind {
            ast::ExprKind::Name(parts) => Ok(vec![(parts.join("."), (**right).clone())]),
            _ => Err(Diagnostic::at(
                left.span,
                "a system table condition is column = literal",
            )),
        },
        _ => Err(Diagnostic::at(
            filter.span,
            "a system table condition is column = literal [AND ...]",
        )),
    }
}

/// `CREATE ALERT`: the query must be one the engine can run before the
/// rule is stored.
fn lower_alert(
    alert: &ast::CreateAlert,
    ctx: &BindContext,
    registry: &Registry,
    parameters: &Parameters,
) -> Result<Statement, Diagnostic> {
    lower_query(&alert.query, ctx, registry, parameters)?;
    Ok(Statement::Control(ControlStatement::CreateAlert(
        AlertDefinition {
            name: alert.name.clone(),
            severity: alert.severity.clone(),
            hold: duration(alert.hold.as_ref())?,
            every: duration(alert.every.as_ref())?,
            notify: alert.notify.clone(),
            labels: alert.labels.clone(),
            annotations: alert.annotations.clone(),
            query: alert.query_text.clone(),
            dataset: ctx.dataset.clone(),
        },
    )))
}

/// `CREATE ROLLUP`: the query must be one the engine can run, and the
/// target a dataset name, before the rollup is stored.
fn lower_rollup(
    rollup: &ast::CreateRollup,
    ctx: &BindContext,
    registry: &Registry,
    parameters: &Parameters,
) -> Result<Statement, Diagnostic> {
    lower_query(&rollup.query, ctx, registry, parameters)?;
    let target = match &rollup.target {
        Some(name) => Some(
            DatasetName::new(name)
                .map_err(|e| Diagnostic::at(Span::new(0, 0), format!("INTO {name}: {e}")))?,
        ),
        None => None,
    };
    Ok(Statement::Control(ControlStatement::CreateRollup(
        RollupDefinition {
            name: rollup.name.clone(),
            every: duration(rollup.every.as_ref())?,
            target,
            backfill: duration(rollup.backfill.as_ref())?,
            query: rollup.query_text.clone(),
            dataset: ctx.dataset.clone(),
        },
    )))
}

/// The control table a statement writes: only `system.<table>` can be
/// written through SQL (CS-3).
fn control_table(table: &ast::TableRef) -> Result<String, Diagnostic> {
    own_tenant(table)?;
    if table.dataset.as_deref() != Some("system") {
        return Err(Diagnostic::at(
            table.span,
            "INSERT, UPDATE and DELETE apply to system tables only; data arrives through ingestion",
        ));
    }
    Ok(table.table.clone())
}

fn literals(
    pairs: &[(String, ast::Expr)],
    ctx: &BindContext,
) -> Result<Vec<(String, Literal)>, Diagnostic> {
    pairs
        .iter()
        .map(|(column, expr)| Ok((column.clone(), literal(expr, ctx)?)))
        .collect()
}

/// A language embedded in `FROM` (QL-3): `promql('expr', start, end,
/// step)`, `logql('query', start, end [, step])` and `traceql('query',
/// start, end)`, each lowered under the request's tenant and dataset
/// into the rows its own API would return. The times are literals,
/// `now()`, or either plus or minus an interval.
fn table_function(
    from: &ast::TableRef,
    args: &[ast::Expr],
    ctx: &BindContext,
) -> Result<Plan, Diagnostic> {
    let name = from.table.as_str();
    if !matches!(name, "promql" | "logql" | "traceql") {
        return Err(Diagnostic::at(
            from.span,
            format!("{name} is not a table function; promql, logql and traceql are"),
        ));
    }
    let [query, start, end, rest @ ..] = args else {
        return Err(Diagnostic::at(
            from.span,
            format!("{name} takes the query, its start and its end, then a step"),
        ));
    };
    let ast::ExprKind::String(text) = &query.kind else {
        return Err(Diagnostic::at(query.span, "the query is a string"));
    };
    let time = |expr: &ast::Expr| match literal(expr, ctx)? {
        Literal::Timestamp(at) => Ok(at),
        _ => Err(Diagnostic::at(expr.span, "expected a timestamp")),
    };
    let (start, end) = (time(start)?, time(end)?);
    let step = match rest {
        [] => None,
        [step] => match literal(step, ctx)? {
            Literal::Duration(step) => Some(step),
            _ => return Err(Diagnostic::at(step.span, "expected an interval")),
        },
        [_, extra, ..] => return Err(Diagnostic::at(extra.span, "too many arguments")),
    };
    let located =
        |d: Diagnostic| Diagnostic::at(query.span, format!("{name}: {}", d.describe(text)));
    match name {
        "promql" => {
            let Some(step) = step else {
                return Err(Diagnostic::at(from.span, "promql needs a step"));
            };
            let params = tessera_promql::RangeParams { start, end, step };
            let lowered = tessera_promql::PromqlFrontend { params }
                .lower_query(text, ctx)
                .map_err(located)?;
            if lowered.kind == tessera_promql::ResultKind::String {
                return Err(Diagnostic::at(
                    query.span,
                    "a string expression has no rows",
                ));
            }
            Ok(lowered.plan)
        }
        "logql" => {
            // Loki's default step: the range over 250 points, at least a second.
            let step = step.unwrap_or_else(|| {
                let span = end.as_nanos().saturating_sub(start.as_nanos()).max(0);
                Duration::from_nanos(u64::try_from(span / 250).unwrap_or(0))
                    .max(Duration::from_secs(1))
            });
            let params = tessera_logql::RangeParams { start, end, step };
            Ok(tessera_logql::LogqlFrontend { params }
                .lower_query(text, ctx)
                .map_err(located)?
                .plan)
        }
        _ => {
            let params = tessera_traceql::RangeParams {
                start,
                end,
                step: step.unwrap_or(Duration::ZERO),
            };
            Ok(tessera_traceql::TraceqlFrontend { params }
                .lower_query(text, ctx)
                .map_err(located)?
                .plan)
        }
    }
}

/// A literal as written in a control statement; `now()` and `now()`
/// plus or minus an interval are read at the statement's time.
fn literal(expr: &ast::Expr, ctx: &BindContext) -> Result<Literal, Diagnostic> {
    match &expr.kind {
        ast::ExprKind::Call { name, args, .. } if name == "now" && args.is_empty() => {
            Ok(Literal::Timestamp(ctx.now))
        }
        ast::ExprKind::Binary {
            op: op @ (ast::BinaryOp::Add | ast::BinaryOp::Sub),
            left,
            right,
        } => match (literal(left, ctx)?, literal(right, ctx)?) {
            (Literal::Timestamp(at), Literal::Duration(by)) => {
                let by = i64::try_from(by.as_nanos()).unwrap_or(i64::MAX);
                let nanos = if *op == ast::BinaryOp::Add {
                    at.as_nanos().saturating_add(by)
                } else {
                    at.as_nanos().saturating_sub(by)
                };
                Ok(Literal::Timestamp(Timestamp::from_nanos(nanos)))
            }
            _ => Err(Diagnostic::at(
                expr.span,
                "only a timestamp plus or minus an interval can be computed here",
            )),
        },
        ast::ExprKind::Number(text) => match number(text, expr.span)? {
            Expr::Literal(literal) => Ok(literal),
            _ => Err(Diagnostic::at(expr.span, "expected a literal")),
        },
        ast::ExprKind::Negate(inner) => match literal(inner, ctx)? {
            Literal::Int(i) => Ok(Literal::Int(-i)),
            Literal::Float(f) => Ok(Literal::Float(-f)),
            _ => Err(Diagnostic::at(expr.span, "expected a number")),
        },
        ast::ExprKind::String(text) => Ok(Literal::Text(text.clone())),
        ast::ExprKind::Bool(b) => Ok(Literal::Bool(*b)),
        ast::ExprKind::Null => Ok(Literal::Null),
        ast::ExprKind::Timestamp(text) => text
            .parse::<Timestamp>()
            .map(Literal::Timestamp)
            .map_err(|e| Diagnostic::at(expr.span, format!("invalid timestamp: {e}"))),
        ast::ExprKind::Interval(text) => interval(text)
            .map(Literal::Duration)
            .ok_or_else(|| Diagnostic::at(expr.span, format!("invalid interval {text}"))),
        _ => Err(Diagnostic::at(
            expr.span,
            "control statements take literals only",
        )),
    }
}

/// `CREATE RETENTION POLICY` as the record the control store keeps.
fn lower_retention_policy(policy: &ast::CreateRetentionPolicy) -> Result<Statement, Diagnostic> {
    let mut clauses = Vec::new();
    for clause in &policy.clauses {
        let actions = clause
            .actions
            .iter()
            .map(retention_action)
            .collect::<Result<Vec<_>, _>>()?;
        clauses.push(RetentionClause {
            condition: clause.condition.as_ref().map(|(_, text)| text.clone()),
            actions,
        });
    }
    Ok(Statement::Control(ControlStatement::CreateRetentionPolicy(
        RetentionPolicy {
            name: policy.name.clone(),
            dataset: policy.dataset.clone(),
            clauses,
            enabled: true,
        },
    )))
}

/// A retention action with its durations read.
fn retention_action(action: &ast::RetentionActionAst) -> Result<RetentionAction, Diagnostic> {
    let seconds = |(text, span): &(String, Span)| {
        interval(text)
            .map(|d| d.as_secs())
            .ok_or_else(|| Diagnostic::at(*span, format!("invalid duration {text}")))
    };
    Ok(match action {
        ast::RetentionActionAst::Sample {
            rate,
            after,
            except_errors,
        } => {
            let fraction: f64 = rate
                .0
                .parse()
                .ok()
                .filter(|r| *r > 0.0 && *r <= 1.0)
                .ok_or_else(|| Diagnostic::at(rate.1, "SAMPLE keeps a fraction between 0 and 1"))?;
            RetentionAction::Sample {
                rate: fraction,
                after_seconds: seconds(after)?,
                except_errors: *except_errors,
            }
        }
        ast::RetentionActionAst::Drop { after } => RetentionAction::Drop {
            after_seconds: seconds(after)?,
        },
        ast::RetentionActionAst::Downsample { interval, after } => RetentionAction::Downsample {
            interval_seconds: seconds(interval)?,
            after_seconds: seconds(after)?,
        },
    })
}

/// A duration option of `CREATE ALERT`, as written.
fn duration(option: Option<&(String, Span)>) -> Result<Option<Duration>, Diagnostic> {
    match option {
        None => Ok(None),
        Some((text, span)) => interval(text)
            .map(Some)
            .ok_or_else(|| Diagnostic::at(*span, format!("invalid duration {text}"))),
    }
}

/// One table an expression may refer to.
#[derive(Clone)]
struct TableScope {
    /// The alias, or the table's own name: what qualifies its columns.
    name: String,
    schema: SchemaRef,
    /// The kinds of event the table holds: one for a view, every kind
    /// for `events`, none for a control table, which has no attributes.
    kinds: Vec<Kind>,
}

impl TableScope {
    fn has_attributes(&self) -> bool {
        !self.kinds.is_empty()
    }
}

/// The keys of a join, and the time match of an `ASOF JOIN`.
type JoinCondition = (Vec<(Expr, Expr)>, Option<Asof>);

/// The tables an expression may refer to. With one table its columns
/// go by their own names; with several, every column is qualified by
/// its table's name, as a join outputs it (DM-2).
struct Scope {
    tables: Vec<TableScope>,
}

impl Scope {
    fn single(table: TableScope) -> Self {
        Scope {
            tables: vec![table],
        }
    }

    fn empty() -> Self {
        Scope { tables: Vec::new() }
    }

    fn qualified(&self) -> bool {
        self.tables.len() > 1
    }

    fn table(&self, name: &str) -> Option<&TableScope> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// The name `column` of `table` goes by in the plan.
    fn output_name(&self, table: &TableScope, column: &str) -> String {
        if self.qualified() {
            qualify(&table.name, column)
        } else {
            column.to_owned()
        }
    }

    /// The type of a column by its plan name.
    fn column_type(&self, name: &str) -> Option<DataType> {
        let (table, column) = if let [table] = self.tables.as_slice() {
            (table, name)
        } else {
            let (table, column) = name.split_once('.')?;
            (self.table(table)?, column)
        };
        table
            .schema
            .field_with_name(column)
            .ok()
            .map(|f| f.data_type().clone())
    }
}

/// How aggregate calls and grouping expressions are replaced above an
/// `Aggregate` node.
#[derive(Default)]
struct Substitution {
    /// Grouping expressions, by their syntax, to output names.
    groups: Vec<(ast::Expr, String)>,
    /// Aggregate calls, by their syntax, to output names.
    aggregates: Vec<(ast::Expr, String)>,
}

struct Lowerer<'a> {
    ctx: &'a BindContext,
    registry: &'a Registry,
    parameters: &'a Parameters<'a>,
    scope: Scope,
    substitution: Option<Substitution>,
}

fn lower_query(
    query: &ast::Query,
    ctx: &BindContext,
    registry: &Registry,
    parameters: &Parameters,
) -> Result<Plan, Diagnostic> {
    let Some(from) = &query.from else {
        return lower_constant_query(query, ctx, registry, parameters);
    };
    let (table, mut plan) = source(from, ctx, registry, parameters)?;
    let mut scope = Scope::single(table);
    if let [_, second, ..] = query.joins.as_slice() {
        return Err(Diagnostic::at(
            second.table.span,
            "one JOIN per query is supported",
        ));
    }
    if let Some(join) = query.joins.first() {
        let (right, right_plan) = source(&join.table, ctx, registry, parameters)?;
        if scope.table(&right.name).is_some() {
            return Err(Diagnostic::at(
                join.table.span,
                format!(
                    "{} is already in the query: alias one of the two",
                    right.name
                ),
            ));
        }
        let left_name = scope.tables[0].name.clone();
        let right_name = right.name.clone();
        scope.tables.push(right);
        let lowerer = Lowerer {
            ctx,
            registry,
            parameters,
            scope,
            substitution: None,
        };
        let (on, asof) = lowerer.join_keys(&join.on, &left_name, &right_name, join.asof)?;
        scope = lowerer.scope;
        plan = Plan::Join {
            left: JoinSide::new(plan, left_name),
            right: JoinSide::new(right_plan, right_name),
            on,
            kind: match join.kind {
                ast::JoinKind::Inner => JoinKind::Inner,
                ast::JoinKind::Left => JoinKind::Left,
            },
            asof,
        };
    }
    let mut lowerer = Lowerer {
        ctx,
        registry,
        parameters,
        scope,
        substitution: None,
    };
    if let Some(filter) = &query.filter {
        let (subqueries, rest) = split_subquery_conditions(filter);
        for (index, condition) in subqueries.into_iter().enumerate() {
            plan = lowerer.semi_join(plan, condition, index)?;
        }
        if let Some(rest) = rest {
            plan = plan.filter(lowerer.expr(&rest)?);
        }
    }
    let grouped = !query.group_by.is_empty()
        || query.having.is_some()
        || query.items.iter().any(
            |i| matches!(i, ast::SelectItem::Expr { expr, .. } if lowerer.has_aggregate(expr)),
        );
    if grouped {
        lowerer.aggregate_query(query, plan)
    } else {
        lowerer.plain_query(query, plan)
    }
}

/// The `IN (SELECT ...)` and `EXISTS (SELECT ...)` conditions among the top-level conjuncts of
/// `filter`, and the rest of it joined back with `AND`.
fn split_subquery_conditions(filter: &ast::Expr) -> (Vec<&ast::Expr>, Option<ast::Expr>) {
    let (subqueries, rest): (Vec<_>, Vec<_>) = conjuncts(filter).into_iter().partition(|c| {
        matches!(
            c.kind,
            ast::ExprKind::InSubquery { .. } | ast::ExprKind::Exists { .. }
        )
    });
    (subqueries, conjunction(rest))
}

/// The top-level conjuncts of `expr`.
fn conjuncts(expr: &ast::Expr) -> Vec<&ast::Expr> {
    match &expr.kind {
        ast::ExprKind::Binary {
            op: ast::BinaryOp::And,
            left,
            right,
        } => {
            let mut all = conjuncts(left);
            all.extend(conjuncts(right));
            all
        }
        _ => vec![expr],
    }
}

/// `exprs` joined by `AND`; none for none.
fn conjunction(exprs: Vec<&ast::Expr>) -> Option<ast::Expr> {
    exprs.into_iter().cloned().reduce(|left, right| ast::Expr {
        span: Span::new(left.span.start, right.span.end),
        kind: ast::ExprKind::Binary {
            op: ast::BinaryOp::And,
            left: Box::new(left),
            right: Box::new(right),
        },
    })
}

/// The rows of a table in `FROM` or `JOIN`, and what its columns are
/// called: the alias when one is written.
fn source(
    from: &ast::TableRef,
    ctx: &BindContext,
    registry: &Registry,
    parameters: &Parameters,
) -> Result<(TableScope, Plan), Diagnostic> {
    if let Some(query) = &from.subquery {
        let plan = lower_query(query, ctx, registry, parameters)?;
        let schema = plan
            .schema(registry)
            .map_err(|e| Diagnostic::at(from.span, e.to_string()))?;
        return Ok((
            TableScope {
                name: from.alias.clone().unwrap_or_else(|| from.table.clone()),
                schema,
                kinds: Vec::new(),
            },
            plan,
        ));
    }
    if let Some(args) = &from.args {
        let plan = table_function(from, args, ctx)?;
        let schema = plan
            .schema(registry)
            .map_err(|e| Diagnostic::at(from.span, e.to_string()))?;
        let mut table = TableScope {
            name: from.table.clone(),
            schema,
            kinds: Vec::new(),
        };
        if let Some(alias) = &from.alias {
            table.name.clone_from(alias);
        }
        return Ok((table, plan));
    }
    let derived = matches!(from.table.as_str(), "usage" | "log_patterns");
    if from.tenant.is_some() && from.dataset.as_deref() == Some("system") && derived {
        return Err(Diagnostic::at(
            from.span,
            format!("system.{} is read for the request's tenant", from.table),
        ));
    }
    let (mut table, plan) = if from.dataset.as_deref() == Some("system") && from.table == "usage" {
        usage_source(ctx)
    } else if from.dataset.as_deref() == Some("system") && from.table == "log_patterns" {
        log_patterns_source(ctx)
    } else if from.dataset.as_deref() == Some("system") {
        let schema = tessera_ir::system_table_schema(&from.table).ok_or_else(|| {
            Diagnostic::at(
                from.span,
                format!(
                    "unknown system table {}: tables are {}",
                    from.table,
                    tessera_ir::system::TABLES.join(", ")
                ),
            )
        })?;
        (
            TableScope {
                name: from.table.clone(),
                schema,
                kinds: Vec::new(),
            },
            Plan::SystemTable {
                tenant: tenant_of(from, ctx)?,
                table: from.table.clone(),
            },
        )
    } else if from.table == "events" {
        events_source(&tenant_of(from, ctx)?, &dataset_of(from, ctx)?)
    } else {
        let (dataset, kind) = resolve_table(from, ctx)?;
        (
            TableScope {
                name: from.table.clone(),
                schema: event::schema(kind),
                kinds: vec![kind],
            },
            Plan::Scan(Scan::all(tenant_of(from, ctx)?, dataset, kind)),
        )
    };
    if let Some(alias) = &from.alias {
        table.name.clone_from(alias);
    }
    Ok((table, plan))
}

/// `events`, the one table of a dataset (DM-1): every kind's rows on the
/// unified columns, each kind a branch of a union with the columns it
/// lacks null. Every branch keeps `_overflow` whole, so an attribute
/// read above the union finds every attribute; the views read one kind
/// through its dedicated columns and are the faster path.
fn events_source(tenant: &TenantId, dataset: &DatasetName) -> (TableScope, Plan) {
    let unified = event::unified_schema();
    let inputs = Kind::ALL
        .iter()
        .map(|kind| {
            let own = event::schema(*kind);
            let exprs = unified
                .fields()
                .iter()
                .map(|field| {
                    let expr = if own.field_with_name(field.name()).is_ok() {
                        Expr::column(field.name())
                    } else {
                        Expr::Cast {
                            expr: Box::new(Expr::Literal(Literal::Null)),
                            to: field.data_type().clone(),
                        }
                    };
                    NamedExpr::new(expr, field.name())
                })
                .collect();
            Plan::Scan(Scan::all(tenant.clone(), dataset.clone(), *kind)).project(exprs)
        })
        .collect();
    (
        TableScope {
            name: "events".into(),
            schema: unified,
            kinds: Kind::ALL.to_vec(),
        },
        Plan::Union { inputs },
    )
}

/// `system.usage` (CO-4): the `_system.usage` events with their
/// attributes as columns, read whatever the request's tenant.
fn usage_source(ctx: &BindContext) -> (TableScope, Plan) {
    let schema = tessera_ir::usage_schema();
    let dataset = DatasetName::new("usage").unwrap_or_else(|_| ctx.dataset.clone());
    let scan = Plan::Scan(Scan::all(TenantId::SYSTEM, dataset, Kind::Event));
    let mut exprs = vec![
        NamedExpr::new(Expr::column(event::TIMESTAMP), event::TIMESTAMP),
        NamedExpr::new(Expr::column(event::NAME), "phase"),
    ];
    for (name, path, numeric) in tessera_ir::system::USAGE_COLUMNS {
        exprs.push(NamedExpr::new(
            Expr::Path {
                path: path.to_owned(),
                data_type: if numeric {
                    DataType::Int64
                } else {
                    DataType::Utf8
                },
            },
            name,
        ));
    }
    exprs.push(NamedExpr::new(usage_cost(ctx), "cost"));
    (
        TableScope {
            name: "usage".into(),
            schema,
            kinds: Vec::new(),
        },
        scan.project(exprs),
    )
}

/// The cost of a usage row at the unit prices in `ctx` (CO-4): its
/// bytes at the price of its phase, its CPU time and its requests at
/// theirs. A phase picks its price through the comparison cast to a
/// number, one for the phase and zero for the others.
fn usage_cost(ctx: &BindContext) -> Expr {
    // A row records the numbers of its phase only: the others count as
    // zero rather than making the cost unknown.
    let number = |path: &str| Expr::Call {
        name: "coalesce".into(),
        args: vec![
            Expr::Path {
                path: path.to_owned(),
                data_type: DataType::Int64,
            },
            Expr::literal(0_i64),
        ],
    };
    let price = |setting: &str| Expr::literal(ctx.price(setting));
    let phase_is = |phase: &str| Expr::Cast {
        expr: Box::new(Expr::column(event::NAME).binary(BinaryOp::Eq, Expr::literal(phase))),
        to: DataType::Float64,
    };
    let per_byte = phase_is("ingest")
        .binary(BinaryOp::Mul, price("price.ingest_byte"))
        .binary(
            BinaryOp::Add,
            phase_is("stored").binary(BinaryOp::Mul, price("price.stored_byte")),
        )
        .binary(
            BinaryOp::Add,
            phase_is("query").binary(BinaryOp::Mul, price("price.scanned_byte")),
        );
    number("attr.bytes")
        .binary(BinaryOp::Mul, per_byte)
        .binary(
            BinaryOp::Add,
            number("attr.cpu_ms").binary(BinaryOp::Mul, price("price.cpu_ms")),
        )
        .binary(
            BinaryOp::Add,
            number("attr.requests").binary(BinaryOp::Mul, price("price.request")),
        )
}

/// `system.log_patterns` (section 13.2): what compaction stored per
/// pattern, summed over the usage events.
fn log_patterns_source(ctx: &BindContext) -> (TableScope, Plan) {
    let (_, usage) = usage_source(ctx);
    let column = |name: &str| Expr::column(name);
    let stored = usage.filter(
        column("phase")
            .binary(BinaryOp::Eq, Expr::literal("stored"))
            .binary(
                BinaryOp::And,
                Expr::IsNull {
                    expr: Box::new(column("pattern_id")),
                    negated: true,
                },
            ),
    );
    let plan = Plan::Aggregate {
        input: Box::new(stored),
        group_by: vec![
            NamedExpr::new(column("tenant"), "tenant"),
            NamedExpr::new(column("dataset"), "dataset"),
            NamedExpr::new(column("pattern_id"), "pattern_id"),
            NamedExpr::new(column("pattern"), "pattern"),
        ],
        aggregates: vec![
            AggregateExpr {
                function: "sum_raw".into(),
                args: vec![column("events")],
                name: "rows".into(),
            },
            AggregateExpr {
                function: "sum_raw".into(),
                args: vec![column("bytes")],
                name: "bytes".into(),
            },
        ],
    };
    (
        TableScope {
            name: "log_patterns".into(),
            schema: tessera_ir::log_patterns_schema(),
            kinds: Vec::new(),
        },
        plan,
    )
}

/// `SELECT 1`, `SELECT now()`: constant rows without a table.
fn lower_constant_query(
    query: &ast::Query,
    ctx: &BindContext,
    registry: &Registry,
    parameters: &Parameters,
) -> Result<Plan, Diagnostic> {
    let lowerer = Lowerer {
        ctx,
        registry,
        parameters,
        scope: Scope::empty(),
        substitution: None,
    };
    let mut fields = Vec::new();
    let mut row = Vec::new();
    for (i, item) in query.items.iter().enumerate() {
        let ast::SelectItem::Expr { expr, alias } = item else {
            return Err(Diagnostic::unlocated("SELECT * needs a FROM clause"));
        };
        let value = lowerer.expr(expr)?;
        let Expr::Literal(literal) = value else {
            return Err(Diagnostic::at(
                expr.span,
                "only constants can be selected without a table",
            ));
        };
        let name = alias.clone().unwrap_or_else(|| format!("column{}", i + 1));
        fields.push(Field::new(
            name,
            literal.data_type().unwrap_or(DataType::Null),
            true,
        ));
        row.push(literal);
    }
    let plan = Plan::Values {
        schema: std::sync::Arc::new(Schema::new(fields)),
        rows: vec![row],
    };
    lowerer.apply_limit(plan, query)
}

fn resolve_table(
    from: &ast::TableRef,
    ctx: &BindContext,
) -> Result<(DatasetName, Kind), Diagnostic> {
    let kind = match from.table.as_str() {
        "logs" => Kind::Log,
        "spans" => Kind::Span,
        "metrics" => Kind::Metric,
        "alerts" => Kind::Alert,
        "events" => {
            return Err(Diagnostic::at(
                from.span,
                "events holds every kind: use one of its views, logs, spans, metrics or alerts",
            ));
        }
        other => {
            return Err(Diagnostic::at(
                from.span,
                format!(
                    "unknown table {other}: tables are events and its views logs, spans, \
                     metrics and alerts"
                ),
            ));
        }
    };
    Ok((dataset_of(from, ctx)?, kind))
}

/// Control statements act on the request's tenant: a tenant qualifier
/// is refused rather than ignored.
fn own_tenant(table: &ast::TableRef) -> Result<(), Diagnostic> {
    match &table.tenant {
        None => Ok(()),
        Some(name) => Err(Diagnostic::at(
            table.span,
            format!(
                "{name}.{}: control statements act on the request's tenant",
                table.table
            ),
        )),
    }
}

/// The tenant a table reference names (CT-9), or the request's. A
/// tenant the query may not name reads as unknown, so that names leak
/// nothing.
fn tenant_of(from: &ast::TableRef, ctx: &BindContext) -> Result<TenantId, Diagnostic> {
    match &from.tenant {
        None => Ok(ctx.tenant.clone()),
        Some(name) => ctx.tenant_named(name).cloned().ok_or_else(|| {
            Diagnostic::at(
                from.span,
                format!("tenant {name} is not known or not readable with these credentials"),
            )
        }),
    }
}

/// The dataset a table reference names, or the request's.
fn dataset_of(from: &ast::TableRef, ctx: &BindContext) -> Result<DatasetName, Diagnostic> {
    match &from.dataset {
        Some(name) => DatasetName::new(name.clone())
            .map_err(|e| Diagnostic::at(from.span, format!("invalid dataset: {e}"))),
        None => Ok(ctx.dataset.clone()),
    }
}

/// The only table of `candidates`, none, or the ambiguity.
fn one_of<'a>(
    candidates: &[&'a TableScope],
    column: &str,
    span: Span,
) -> Result<Option<&'a TableScope>, Diagnostic> {
    match candidates {
        [one] => Ok(Some(*one)),
        [] => Ok(None),
        [first, second, ..] => Err(Diagnostic::at(
            span,
            format!(
                "{column} is ambiguous: qualify it with {} or {}",
                first.name, second.name
            ),
        )),
    }
}

impl Lowerer<'_> {
    /// `plan` under the query's `LIMIT` and `OFFSET`, each written or a
    /// `$n` bound to a whole number; while types are inferred a `$n`
    /// is noted as an integer and no limit applies.
    fn apply_limit(&self, plan: Plan, query: &ast::Query) -> Result<Plan, Diagnostic> {
        let limit = self.count(query.limit)?;
        let offset = self.count(query.offset)?;
        Ok(if limit.is_some() || offset.is_some() {
            plan.limit(offset.unwrap_or(0), limit)
        } else {
            plan
        })
    }

    fn count(&self, count: Option<ast::Count>) -> Result<Option<usize>, Diagnostic> {
        let (n, span) = match count {
            None => return Ok(None),
            Some(ast::Count::Written(n)) => return Ok(Some(n)),
            Some(ast::Count::Parameter { n, span }) => (n, span),
        };
        self.note_parameter_type(n, DataType::Int64);
        let not_a_count = |value: &dyn fmt::Display| {
            Diagnostic::at(
                span,
                format!("LIMIT and OFFSET take a whole number; parameter ${n} is {value}"),
            )
        };
        match self.parameter(n, span)? {
            Expr::Literal(Literal::Int(value)) => usize::try_from(value)
                .map(Some)
                .map_err(|_| not_a_count(&value)),
            Expr::Literal(Literal::Null) if matches!(self.parameters, Parameters::Inferring(_)) => {
                Ok(None)
            }
            other => Err(not_a_count(&other)),
        }
    }
}

fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

/// The type a literal has on its own; none for a null or a duration,
/// which no parameter carries.
fn literal_type(literal: &Literal) -> Option<DataType> {
    Some(match literal {
        Literal::Bool(_) => DataType::Boolean,
        Literal::Int(_) => DataType::Int64,
        Literal::Float(_) => DataType::Float64,
        Literal::Text(_) => DataType::Utf8,
        Literal::Timestamp(_) => timestamp_type(),
        Literal::Null | Literal::Duration(_) | Literal::Bytes(_) => return None,
    })
}

/// Lowers `statement` with its `$n` parameters unbound, each read as a
/// null, and tells the types they take, by position, from the columns
/// they are compared with or the types they are cast to; none where
/// nothing says. This is what a client asking about a statement before
/// binding it needs.
///
/// # Errors
///
/// As [`lower`], short of the unbound parameters.
pub fn describe(
    statement: &ast::Statement,
    ctx: &BindContext,
) -> Result<(Statement, Vec<Option<DataType>>), Diagnostic> {
    let parameters = Parameters::Inferring(RefCell::new(Vec::new()));
    let lowered = lower_with(statement, ctx, &parameters)?;
    let Parameters::Inferring(types) = parameters else {
        unreachable!("inferring parameters stay inferring")
    };
    Ok((lowered, types.into_inner()))
}

impl Lowerer<'_> {
    fn plain_query(&mut self, query: &ast::Query, plan: Plan) -> Result<Plan, Diagnostic> {
        let items = self.select_items(query)?;
        let all_columns = query
            .items
            .iter()
            .all(|i| matches!(i, ast::SelectItem::Wildcard));
        let mut plan = plan;
        if !query.order_by.is_empty() {
            // Sort over the input, resolving aliases to their expressions,
            // so that any column may order the result.
            let aliases: BTreeMap<&str, &Expr> =
                items.iter().map(|i| (i.name.as_str(), &i.expr)).collect();
            let mut keys = Vec::new();
            for item in &query.order_by {
                let expr = match &item.expr.kind {
                    ast::ExprKind::Name(parts)
                        if parts.len() == 1 && aliases.contains_key(parts[0].as_str()) =>
                    {
                        aliases[parts[0].as_str()].clone()
                    }
                    _ => self.expr(&item.expr)?,
                };
                keys.push(SortKey {
                    expr,
                    descending: item.descending,
                });
            }
            plan = plan.sort(keys);
        }
        if !all_columns {
            plan = plan.project(items);
        }
        self.apply_limit(plan, query)
    }

    fn aggregate_query(&mut self, query: &ast::Query, plan: Plan) -> Result<Plan, Diagnostic> {
        let mut substitution = Substitution::default();
        let mut group_by = Vec::new();
        for (i, key) in query.group_by.iter().enumerate() {
            let expr = self.expr(key)?;
            let name = query
                .items
                .iter()
                .find_map(|item| match item {
                    ast::SelectItem::Expr {
                        expr,
                        alias: Some(alias),
                    } if expr.same_as(key) => Some(alias.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| match &key.kind {
                    ast::ExprKind::Name(parts) => parts.last().cloned().unwrap_or_default(),
                    _ => format!("group{}", i + 1),
                });
            substitution.groups.push((key.clone(), name.clone()));
            group_by.push(NamedExpr::new(expr, name));
        }
        let mut aggregates = Vec::new();
        for item in &query.items {
            let ast::SelectItem::Expr { expr, alias } = item else {
                return Err(Diagnostic::unlocated(
                    "SELECT * cannot be combined with aggregates or GROUP BY",
                ));
            };
            self.collect_aggregates(expr, alias.as_deref(), &mut substitution, &mut aggregates)?;
        }
        for item in &query.order_by {
            self.collect_aggregates(&item.expr, None, &mut substitution, &mut aggregates)?;
        }
        if let Some(having) = &query.having {
            self.collect_aggregates(having, None, &mut substitution, &mut aggregates)?;
        }
        let mut plan = Plan::Aggregate {
            input: Box::new(plan),
            group_by,
            aggregates,
        };
        self.substitution = Some(substitution);
        if let Some(having) = &query.having {
            plan = Plan::Filter {
                input: Box::new(plan),
                predicate: self.expr(having)?,
            };
        }
        let items = self.select_items(query)?;
        if !query.order_by.is_empty() {
            let aliases: BTreeMap<&str, &Expr> =
                items.iter().map(|i| (i.name.as_str(), &i.expr)).collect();
            let mut keys = Vec::new();
            for item in &query.order_by {
                let expr = match &item.expr.kind {
                    ast::ExprKind::Name(parts)
                        if parts.len() == 1 && aliases.contains_key(parts[0].as_str()) =>
                    {
                        aliases[parts[0].as_str()].clone()
                    }
                    _ => self.expr(&item.expr)?,
                };
                keys.push(SortKey {
                    expr,
                    descending: item.descending,
                });
            }
            plan = plan.sort(keys);
        }
        plan = plan.project(items);
        self.apply_limit(plan, query)
    }

    /// Registers every aggregate call in `expr`, naming the call by
    /// `alias` when the whole item is the call.
    fn collect_aggregates(
        &self,
        expr: &ast::Expr,
        alias: Option<&str>,
        substitution: &mut Substitution,
        aggregates: &mut Vec<AggregateExpr>,
    ) -> Result<(), Diagnostic> {
        if let ast::ExprKind::Call {
            name,
            args,
            wildcard,
        } = &expr.kind
            && self.registry.is_aggregate(name)
        {
            if substitution.aggregates.iter().any(|(e, _)| e.same_as(expr)) {
                return Ok(());
            }
            if *wildcard && !name.starts_with("count") {
                return Err(Diagnostic::at(expr.span, format!("{name}(*) is not valid")));
            }
            let lowered = args
                .iter()
                .map(|a| {
                    if self.has_aggregate(a) {
                        Err(Diagnostic::at(a.span, "aggregates cannot be nested"))
                    } else {
                        self.expr(a)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            let base = alias.map_or_else(|| name.clone(), str::to_owned);
            let taken = |n: &str| {
                substitution.aggregates.iter().any(|(_, m)| m == n)
                    || substitution.groups.iter().any(|(_, m)| m == n)
            };
            let mut out = base.clone();
            let mut i = 1;
            while taken(&out) {
                i += 1;
                out = format!("{base}_{i}");
            }
            substitution.aggregates.push((expr.clone(), out.clone()));
            aggregates.push(AggregateExpr {
                function: name.clone(),
                args: lowered,
                name: out,
            });
            return Ok(());
        }
        for child in children(expr) {
            self.collect_aggregates(child, None, substitution, aggregates)?;
        }
        Ok(())
    }

    fn has_aggregate(&self, expr: &ast::Expr) -> bool {
        if let ast::ExprKind::Call { name, .. } = &expr.kind
            && self.registry.is_aggregate(name)
        {
            return true;
        }
        children(expr).into_iter().any(|c| self.has_aggregate(c))
    }

    fn select_items(&self, query: &ast::Query) -> Result<Vec<NamedExpr>, Diagnostic> {
        let mut items = Vec::new();
        for (i, item) in query.items.iter().enumerate() {
            match item {
                ast::SelectItem::Wildcard => {
                    for table in &self.scope.tables {
                        for field in table.schema.fields() {
                            let name = self.scope.output_name(table, field.name());
                            items.push(NamedExpr::new(Expr::column(&name), name));
                        }
                    }
                }
                ast::SelectItem::Expr { expr, alias } => {
                    let name = alias.clone().unwrap_or_else(|| self.default_name(expr, i));
                    items.push(NamedExpr::new(self.expr(expr)?, name));
                }
            }
        }
        Ok(items)
    }

    /// The output name of an item without an alias: a name without the
    /// table that qualifies it, else [`default_name`].
    fn default_name(&self, expr: &ast::Expr, index: usize) -> String {
        match &expr.kind {
            ast::ExprKind::Name(parts) => match parts.as_slice() {
                [table, rest @ ..] if !rest.is_empty() && self.scope.table(table).is_some() => {
                    rest.join(".")
                }
                _ => parts.join("."),
            },
            _ => default_name(expr, index),
        }
    }

    /// The keys of `ON`: a conjunction of equalities, each between an
    /// expression over `left` and one over `right`, returned in that
    /// order and over their own tables' names; under an `ASOF JOIN`,
    /// one inequality between the two tables' times besides, as the
    /// time match (section 9.2).
    fn join_keys(
        &self,
        on: &ast::Expr,
        left: &str,
        right: &str,
        asof: bool,
    ) -> Result<JoinCondition, Diagnostic> {
        let unsupported = || {
            let message = if asof {
                format!(
                    "an ASOF JOIN condition is equalities between the two tables and one \
                     inequality between their times, such as {left}.trace_id = \
                     {right}.trace_id AND {left}.timestamp >= {right}.timestamp"
                )
            } else {
                format!(
                    "join conditions must be equalities between the two tables, \
                     such as {left}.trace_id = {right}.trace_id; other conditions go in WHERE"
                )
            };
            Diagnostic::at(on.span, message)
        };
        let side_of = |expr: &Expr| {
            let names = expr.names();
            if names.is_empty() {
                None
            } else if names.iter().all(|n| unqualify(left, n).is_some()) {
                Some(true)
            } else if names.iter().all(|n| unqualify(right, n).is_some()) {
                Some(false)
            } else {
                None
            }
        };
        let mut keys = Vec::new();
        let mut time = None;
        for conjunct in self.expr(on)?.conjuncts() {
            let Expr::Binary {
                op,
                left: l,
                right: r,
            } = conjunct
            else {
                return Err(unsupported());
            };
            let (l, r, op) = match (side_of(&l), side_of(&r)) {
                (Some(true), Some(false)) => (l.unqualify(left), r.unqualify(right), op),
                (Some(false), Some(true)) => (r.unqualify(left), l.unqualify(right), op.flipped()),
                _ => return Err(unsupported()),
            };
            if op == BinaryOp::Eq {
                keys.push((l, r));
                continue;
            }
            match AsofMatch::from_op(op) {
                Some(matching) if asof && time.is_none() => {
                    time = Some(Asof {
                        left: l,
                        right: r,
                        matching,
                    });
                }
                _ => return Err(unsupported()),
            }
        }
        if asof && (time.is_none() || keys.is_empty()) {
            return Err(unsupported());
        }
        Ok((keys, time))
    }

    /// Lowers an expression; under an aggregate, grouping expressions and
    /// aggregate calls become the columns the `Aggregate` node outputs.
    fn expr(&self, expr: &ast::Expr) -> Result<Expr, Diagnostic> {
        if let Some(substitution) = &self.substitution {
            if let Some((_, name)) = substitution.groups.iter().find(|(e, _)| e.same_as(expr)) {
                return Ok(Expr::column(name));
            }
            if let Some((_, name)) = substitution
                .aggregates
                .iter()
                .find(|(e, _)| e.same_as(expr))
            {
                return Ok(Expr::column(name));
            }
        }
        match &expr.kind {
            ast::ExprKind::Name(parts) => self.name(parts, expr.span),
            ast::ExprKind::Number(text) => number(text, expr.span),
            ast::ExprKind::String(s) => Ok(Expr::literal(s.as_str())),
            ast::ExprKind::Bool(b) => Ok(Expr::literal(*b)),
            ast::ExprKind::Null => Ok(Expr::Literal(Literal::Null)),
            ast::ExprKind::Timestamp(text) => text
                .parse::<Timestamp>()
                .map(Expr::literal)
                .map_err(|e| Diagnostic::at(expr.span, e.to_string())),
            ast::ExprKind::Interval(text) => interval(text)
                .map(Expr::literal)
                .ok_or_else(|| Diagnostic::at(expr.span, format!("invalid interval {text:?}"))),
            ast::ExprKind::Parameter(n) => self.parameter(*n, expr.span),
            ast::ExprKind::Binary { op, left, right } => self.binary(*op, left, right),
            ast::ExprKind::Not(inner) => Ok(Expr::Not(Box::new(self.expr(inner)?))),
            ast::ExprKind::Negate(inner) => match self.expr(inner)? {
                Expr::Literal(Literal::Int(i)) => Ok(Expr::literal(-i)),
                Expr::Literal(Literal::Float(f)) => Ok(Expr::literal(-f)),
                other => Ok(Expr::Negate(Box::new(other))),
            },
            ast::ExprKind::IsNull { expr, negated } => Ok(Expr::IsNull {
                expr: Box::new(self.expr(expr)?),
                negated: *negated,
            }),
            ast::ExprKind::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => Ok(Expr::Like {
                expr: Box::new(self.expr(expr)?),
                pattern: pattern.clone(),
                negated: *negated,
                case_insensitive: *case_insensitive,
            }),
            ast::ExprKind::InList {
                expr: tested,
                list,
                negated,
            } => self.in_list(tested, list, *negated),
            ast::ExprKind::InSubquery { .. } => Err(Diagnostic::at(
                expr.span,
                "IN (SELECT ...) is a condition of WHERE on its own, joined to the others by AND",
            )),
            ast::ExprKind::Exists { .. } => Err(Diagnostic::at(
                expr.span,
                "EXISTS (SELECT ...) is a condition of WHERE on its own, joined to the others by AND",
            )),
            ast::ExprKind::Between {
                expr: tested,
                low,
                high,
                negated,
            } => {
                let at_least = self.binary(ast::BinaryOp::GtEq, tested, low)?;
                let at_most = self.binary(ast::BinaryOp::LtEq, tested, high)?;
                let both = at_least.and(at_most);
                Ok(if *negated {
                    Expr::Not(Box::new(both))
                } else {
                    both
                })
            }
            ast::ExprKind::Call {
                name,
                args,
                wildcard,
            } => self.call(name, args, *wildcard, expr.span),
            ast::ExprKind::Cast {
                expr: inner,
                type_name,
            } => {
                let to = data_type(type_name).ok_or_else(|| {
                    Diagnostic::at(expr.span, format!("unknown type {type_name}"))
                })?;
                self.infer_as(inner, to.clone());
                Ok(Expr::Cast {
                    expr: Box::new(self.expr(inner)?),
                    to,
                })
            }
        }
    }

    /// `tested [NOT] IN (SELECT key ...)` or `[NOT] EXISTS (SELECT ...)`
    /// over `plan`: a join on the distinct keys of the subquery, inner
    /// to keep the rows with a match, left plus `IS NULL` to keep the
    /// rows without one, then a projection back to `plan`'s own columns
    /// so that the rest of the query sees the same names. `index`
    /// numbers the hidden sides. A null key on the right never matches,
    /// as `NOT EXISTS` behaves.
    fn semi_join(
        &self,
        plan: Plan,
        condition: &ast::Expr,
        index: usize,
    ) -> Result<Plan, Diagnostic> {
        let located = |e: PlanError| Diagnostic::at(condition.span, e.to_string());
        let (right, keys, negated) = match &condition.kind {
            ast::ExprKind::InSubquery {
                expr: tested,
                query,
                negated,
            } => {
                let right = lower_query(query, self.ctx, self.registry, self.parameters)?;
                let right_schema = right.schema(self.registry).map_err(located)?;
                let [key] = right_schema.fields().iter().collect::<Vec<_>>()[..] else {
                    return Err(Diagnostic::at(
                        condition.span,
                        format!(
                            "the subquery of IN must select one column, this one {}",
                            right_schema.fields().len()
                        ),
                    ));
                };
                (
                    right,
                    vec![(self.expr(tested)?, key.name().clone())],
                    *negated,
                )
            }
            ast::ExprKind::Exists { query, negated } => {
                let (right, keys) = self.exists_keys(condition.span, query)?;
                (right, keys, *negated)
            }
            _ => {
                return Err(Diagnostic::at(
                    condition.span,
                    "expected IN (SELECT ...) or EXISTS (SELECT ...)",
                ));
            }
        };
        let distinct = Plan::Aggregate {
            input: Box::new(right),
            group_by: keys
                .iter()
                .map(|(_, key)| NamedExpr::new(Expr::column(key), key))
                .collect(),
            aggregates: Vec::new(),
        };
        let own = plan.schema(self.registry).map_err(located)?;
        let left_name = format!("__rows{index}");
        let right_name = format!("__in{index}");
        let first_key = keys.first().map(|(_, key)| key.clone()).unwrap_or_default();
        let mut joined = Plan::Join {
            left: JoinSide::new(plan, left_name.clone()),
            right: JoinSide::new(distinct, right_name.clone()),
            on: keys
                .into_iter()
                .map(|(tested, key)| (tested, Expr::column(&key)))
                .collect(),
            kind: if negated {
                JoinKind::Left
            } else {
                JoinKind::Inner
            },
            asof: None,
        };
        if negated {
            joined = joined.filter(Expr::IsNull {
                expr: Box::new(Expr::column(qualify(&right_name, &first_key))),
                negated: false,
            });
        }
        Ok(Plan::Project {
            input: Box::new(joined),
            exprs: own
                .fields()
                .iter()
                .map(|f| NamedExpr::new(Expr::column(qualify(&left_name, f.name())), f.name()))
                .collect(),
        })
    }

    /// The subquery of `EXISTS` lowered to its correlation keys: each
    /// condition of its `WHERE` equating one of its columns with one of
    /// the query's is a key pair, the query's side lowered and the
    /// subquery's selected under a hidden name; the other conditions
    /// filter it. Without a key both sides key on the constant 1, so
    /// that the join keeps the rows when the subquery has any row.
    fn exists_keys(
        &self,
        span: Span,
        query: &ast::Query,
    ) -> Result<(Plan, Vec<(Expr, String)>), Diagnostic> {
        let simple = query.joins.is_empty() && query.group_by.is_empty() && query.having.is_none();
        let (Some(from), true) = (&query.from, simple) else {
            return Err(Diagnostic::at(
                span,
                "EXISTS takes a subquery of one table and a WHERE",
            ));
        };
        let (table, _) = source(from, self.ctx, self.registry, self.parameters)?;
        let inner = Lowerer {
            ctx: self.ctx,
            registry: self.registry,
            parameters: self.parameters,
            scope: Scope::single(table),
            substitution: None,
        };
        let mut keys: Vec<(Expr, ast::Expr)> = Vec::new();
        let mut rest: Vec<&ast::Expr> = Vec::new();
        for conjunct in query.filter.iter().flat_map(conjuncts) {
            if let ast::ExprKind::Binary {
                op: ast::BinaryOp::Eq,
                left,
                right,
            } = &conjunct.kind
            {
                // A side the subquery cannot lower but the query can is
                // the query's; the subquery's own names come first.
                let outer = |e: &ast::Expr| inner.expr(e).is_err() && self.expr(e).is_ok();
                let own = |e: &ast::Expr| inner.expr(e).is_ok();
                if outer(left) && own(right) {
                    keys.push((self.expr(left)?, (**right).clone()));
                    continue;
                }
                if own(left) && outer(right) {
                    keys.push((self.expr(right)?, (**left).clone()));
                    continue;
                }
            }
            if inner.expr(conjunct).is_err() && self.mentions_own_column(conjunct) {
                return Err(Diagnostic::at(
                    conjunct.span,
                    "an EXISTS subquery refers to the query's columns only in equalities with \
                     its own, such as s.trace_id = l.trace_id",
                ));
            }
            rest.push(conjunct);
        }
        let name = |i: usize| format!("__key{i}");
        let (items, outer): (Vec<ast::SelectItem>, Vec<Expr>) = if keys.is_empty() {
            let one = ast::Expr {
                kind: ast::ExprKind::Number("1".into()),
                span,
            };
            (
                vec![ast::SelectItem::Expr {
                    expr: one,
                    alias: Some(name(0)),
                }],
                vec![Expr::literal(1)],
            )
        } else {
            keys.into_iter()
                .enumerate()
                .map(|(i, (outer, own))| {
                    (
                        ast::SelectItem::Expr {
                            expr: own,
                            alias: Some(name(i)),
                        },
                        outer,
                    )
                })
                .unzip()
        };
        let rewritten = ast::Query {
            items,
            from: Some(from.clone()),
            joins: Vec::new(),
            filter: conjunction(rest),
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let right = lower_query(&rewritten, self.ctx, self.registry, self.parameters)?;
        Ok((
            right,
            outer
                .into_iter()
                .enumerate()
                .map(|(i, expr)| (expr, name(i)))
                .collect(),
        ))
    }

    /// Whether `expr` names a column of the tables in scope.
    fn mentions_own_column(&self, expr: &ast::Expr) -> bool {
        (matches!(expr.kind, ast::ExprKind::Name(_)) && self.expr(expr).is_ok())
            || children(expr)
                .into_iter()
                .any(|child| self.mentions_own_column(child))
    }

    /// `tested [NOT] IN (list)`: the list is constants, read in the
    /// tested expression's type as literals are.
    fn in_list(
        &self,
        tested: &ast::Expr,
        list: &[ast::Expr],
        negated: bool,
    ) -> Result<Expr, Diagnostic> {
        let tested = self.expr(tested)?;
        for item in list {
            self.infer(item, &tested);
        }
        let list = list
            .iter()
            .map(|item| match self.expr(item)? {
                Expr::Literal(literal) => Ok(self.coerce_literal(&tested, literal)),
                _ => Err(Diagnostic::at(item.span, "IN needs constant values")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Expr::InList {
            expr: Box::new(tested),
            list,
            negated,
        })
    }

    /// `$n`: the value bound at `n`, or, while types are inferred, a
    /// null whose position is noted.
    fn parameter(&self, n: usize, span: Span) -> Result<Expr, Diagnostic> {
        let index = n.saturating_sub(1);
        match self.parameters {
            Parameters::Bound(values) => values
                .get(index)
                .map(|value| Expr::Literal(value.literal()))
                .ok_or_else(|| {
                    Diagnostic::at(
                        span,
                        format!(
                            "parameter ${n} is not bound: {} value(s) were given",
                            values.len()
                        ),
                    )
                }),
            Parameters::Inferring(types) => {
                let mut types = types.borrow_mut();
                if types.len() < n {
                    types.resize(n, None);
                }
                Ok(Expr::Literal(Literal::Null))
            }
        }
    }

    /// Notes the type `parameter`, if it is a `$n`, takes from `other`,
    /// the expression it meets: a column's or a literal's type.
    fn infer(&self, parameter: &ast::Expr, other: &Expr) {
        let inferred = match other {
            Expr::Literal(literal) => literal_type(literal),
            _ => self.target_type(other),
        };
        if let Some(inferred) = inferred {
            self.infer_as(parameter, inferred);
        }
    }

    /// Notes that `parameter`, if it is a `$n`, has `data_type`.
    fn infer_as(&self, parameter: &ast::Expr, data_type: DataType) {
        if let ast::ExprKind::Parameter(n) = &parameter.kind {
            self.note_parameter_type(*n, data_type);
        }
    }

    /// Notes that `$n` has `data_type` while types are inferred; the
    /// first note wins.
    fn note_parameter_type(&self, n: usize, data_type: DataType) {
        let Parameters::Inferring(types) = self.parameters else {
            return;
        };
        let mut types = types.borrow_mut();
        if types.len() < n {
            types.resize(n, None);
        }
        let slot = &mut types[n - 1];
        if slot.is_none() {
            *slot = Some(data_type);
        }
    }

    /// A name: a column of a table in scope, the pattern id of a log
    /// table or an attribute path of a telemetry table, found in the
    /// table a leading part names, or in the only table that has it.
    fn name(&self, parts: &[String], span: Span) -> Result<Expr, Diagnostic> {
        let scope = &self.scope;
        let (tables, rest): (Vec<&TableScope>, &[String]) = match parts {
            [first, rest @ ..] if !rest.is_empty() && scope.table(first).is_some() => {
                (scope.table(first).into_iter().collect(), rest)
            }
            _ => (scope.tables.iter().collect(), parts),
        };
        let column = rest.join(".");
        let owners: Vec<&TableScope> = tables
            .iter()
            .filter(|t| t.schema.field_with_name(&column).is_ok())
            .copied()
            .collect();
        if let Some(table) = one_of(&owners, &column, span)? {
            return Ok(Expr::column(scope.output_name(table, &column)));
        }
        // The pattern of a compacted log row (section 13.2): a column of
        // compacted files, read like a path so that files without it
        // read as null.
        if column == event::PATTERN_ID {
            let logs: Vec<&TableScope> = tables
                .iter()
                .filter(|t| t.kinds == [Kind::Log])
                .copied()
                .collect();
            if let Some(table) = one_of(&logs, &column, span)? {
                return Ok(Expr::Path {
                    path: scope.output_name(table, &column),
                    data_type: DataType::Int64,
                });
            }
        }
        let scoped = column.split_once('.').is_some_and(|(prefix, _)| {
            event::SCOPES
                .iter()
                .any(|scope| scope.trim_end_matches('.') == prefix)
        });
        if scoped {
            let telemetry: Vec<&TableScope> = tables
                .iter()
                .filter(|t| t.has_attributes())
                .copied()
                .collect();
            if let Some(table) = one_of(&telemetry, &column, span)? {
                return Ok(path(&scope.output_name(table, &column)));
            }
        }
        Err(Diagnostic::at(
            span,
            format!("unknown column {}", parts.join(".")),
        ))
    }

    fn binary(
        &self,
        op: ast::BinaryOp,
        left: &ast::Expr,
        right: &ast::Expr,
    ) -> Result<Expr, Diagnostic> {
        let op = match op {
            ast::BinaryOp::And => BinaryOp::And,
            ast::BinaryOp::Or => BinaryOp::Or,
            ast::BinaryOp::Eq => BinaryOp::Eq,
            ast::BinaryOp::NotEq => BinaryOp::NotEq,
            ast::BinaryOp::Lt => BinaryOp::Lt,
            ast::BinaryOp::LtEq => BinaryOp::LtEq,
            ast::BinaryOp::Gt => BinaryOp::Gt,
            ast::BinaryOp::GtEq => BinaryOp::GtEq,
            ast::BinaryOp::Add => BinaryOp::Add,
            ast::BinaryOp::Sub => BinaryOp::Sub,
            ast::BinaryOp::Mul => BinaryOp::Mul,
            ast::BinaryOp::Div => BinaryOp::Div,
            ast::BinaryOp::Rem => BinaryOp::Rem,
        };
        let (left_syntax, right_syntax) = (left, right);
        let mut left = self.expr(left)?;
        let mut right = self.expr(right)?;
        self.infer(left_syntax, &right);
        self.infer(right_syntax, &left);
        if op.is_comparison() {
            // A text literal against a timestamp column is a timestamp, as
            // PostgreSQL reads an untyped literal in the column's type.
            if let Expr::Literal(literal) = &right {
                right = Expr::Literal(self.coerce_literal(&left, literal.clone()));
            }
            if let Expr::Literal(literal) = &left {
                left = Expr::Literal(self.coerce_literal(&right, literal.clone()));
            }
        }
        Ok(left.binary(op, right))
    }

    /// `literal` read in the type of `target` where SQL would: text as a
    /// timestamp against a timestamp column.
    fn coerce_literal(&self, target: &Expr, literal: Literal) -> Literal {
        match (&literal, self.target_type(target)) {
            (Literal::Text(text), Some(t)) if t == timestamp_type() => text
                .parse::<Timestamp>()
                .map_or(literal, Literal::Timestamp),
            _ => literal,
        }
    }

    /// The type a literal compared with `target` is read in: a column's
    /// type, or a cast's.
    fn target_type(&self, target: &Expr) -> Option<DataType> {
        match target {
            Expr::Column(name) => self
                .scope
                .column_type(name)
                .or_else(|| self.substituted_type(name)),
            Expr::Cast { to, .. } => Some(to.clone()),
            _ => None,
        }
    }

    /// The type of an `Aggregate` output column above the node: a
    /// grouping column's own, or the argument's for `min` and `max`,
    /// which keep their argument's type.
    fn substituted_type(&self, output: &str) -> Option<DataType> {
        let substitution = self.substitution.as_ref()?;
        let source = |expr: &ast::Expr| match &expr.kind {
            ast::ExprKind::Name(parts) => self.scope.column_type(&parts.join(".")),
            _ => None,
        };
        if let Some((expr, _)) = substitution.groups.iter().find(|(_, n)| n == output) {
            return source(expr);
        }
        let (expr, _) = substitution.aggregates.iter().find(|(_, n)| n == output)?;
        match &expr.kind {
            ast::ExprKind::Call { name, args, .. }
                if matches!(name.as_str(), "min" | "max") && args.len() == 1 =>
            {
                source(&args[0])
            }
            _ => None,
        }
    }

    fn call(
        &self,
        name: &str,
        args: &[ast::Expr],
        wildcard: bool,
        span: Span,
    ) -> Result<Expr, Diagnostic> {
        if self.registry.is_aggregate(name) {
            return Err(Diagnostic::at(
                span,
                format!("aggregate function {name} is not allowed here"),
            ));
        }
        if wildcard {
            return Err(Diagnostic::at(span, format!("{name}(*) is not valid")));
        }
        if name == "now" {
            return Ok(Expr::literal(self.ctx.now));
        }
        if name == "search" {
            return self.search(args, span);
        }
        if self.registry.scalar(name).is_none() {
            return Err(Diagnostic::at(span, format!("unknown function {name}")));
        }
        let args = args
            .iter()
            .map(|a| self.expr(a))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Expr::Call {
            name: name.to_owned(),
            args,
        })
    }
}

impl Lowerer<'_> {
    /// `search('...')` (QL-3): the search syntax as a predicate over the
    /// table's kind.
    fn search(&self, args: &[ast::Expr], span: Span) -> Result<Expr, Diagnostic> {
        let [
            ast::Expr {
                kind: ast::ExprKind::String(text),
                ..
            },
        ] = args
        else {
            return Err(Diagnostic::at(
                span,
                "search() takes one string of search syntax",
            ));
        };
        // Search syntax is read against one kind of event: the single
        // view in scope.
        let kinds: Vec<Kind> = self
            .scope
            .tables
            .iter()
            .flat_map(|t| t.kinds.iter().copied())
            .collect();
        let [kind] = kinds.as_slice() else {
            return Err(Diagnostic::at(
                span,
                "search() needs a table of one kind: logs, spans, metrics or alerts",
            ));
        };
        let kind = *kind;
        tessera_search::parse(text, kind).map_err(|inner| {
            // The inner position is within the string: report the call.
            Diagnostic::at(span, format!("in search(): {}", inner.message))
        })
    }
}

fn path(path: &str) -> Expr {
    Expr::Path {
        path: path.to_owned(),
        data_type: DataType::Utf8,
    }
}

fn children(expr: &ast::Expr) -> Vec<&ast::Expr> {
    match &expr.kind {
        ast::ExprKind::Binary { left, right, .. } => vec![left, right],
        ast::ExprKind::Not(inner)
        | ast::ExprKind::Negate(inner)
        | ast::ExprKind::IsNull { expr: inner, .. }
        | ast::ExprKind::Like { expr: inner, .. }
        | ast::ExprKind::InSubquery { expr: inner, .. }
        | ast::ExprKind::Cast { expr: inner, .. } => vec![inner],
        ast::ExprKind::InList { expr, list, .. } => {
            let mut all = vec![expr.as_ref()];
            all.extend(list.iter());
            all
        }
        ast::ExprKind::Between {
            expr, low, high, ..
        } => vec![expr, low, high],
        ast::ExprKind::Call { args, .. } => args.iter().collect(),
        _ => Vec::new(),
    }
}

/// The output name of an unaliased select item: a column's own name, a
/// function's name, or `column{n}`.
fn default_name(expr: &ast::Expr, index: usize) -> String {
    match &expr.kind {
        ast::ExprKind::Name(parts) => parts.join("."),
        ast::ExprKind::Call { name, .. } => name.clone(),
        ast::ExprKind::Cast { expr, .. } => default_name(expr, index),
        _ => format!("column{}", index + 1),
    }
}

fn number(text: &str, span: Span) -> Result<Expr, Diagnostic> {
    if let Ok(i) = text.parse::<i64>() {
        return Ok(Expr::literal(i));
    }
    text.parse::<f64>()
        .map(Expr::literal)
        .map_err(|_| Diagnostic::at(span, format!("invalid number {text}")))
}

fn data_type(name: &str) -> Option<DataType> {
    Some(match name {
        "int" | "integer" | "bigint" | "int8" | "int4" | "smallint" => DataType::Int64,
        "float" | "double" | "real" | "float8" | "float4" | "numeric" | "decimal" => {
            DataType::Float64
        }
        "text" | "varchar" | "string" | "char" => DataType::Utf8,
        "bool" | "boolean" => DataType::Boolean,
        "timestamp" | "timestamptz" => timestamp_type(),
        _ => return None,
    })
}

/// Parses an interval: `5 minutes`, `1 hour 30 minutes`, `1h30m`, `90s`,
/// `1.5 hours`.
fn interval(text: &str) -> Option<Duration> {
    let mut total = 0.0f64;
    let mut rest = text.trim();
    if rest.is_empty() {
        return None;
    }
    while !rest.is_empty() {
        let digits = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        let value: f64 = rest[..digits].parse().ok()?;
        rest = rest[digits..].trim_start();
        let unit_end = rest
            .find(|c: char| !c.is_ascii_alphabetic())
            .unwrap_or(rest.len());
        let unit = rest[..unit_end].to_ascii_lowercase();
        rest = rest[unit_end..].trim_start();
        let seconds = match unit.as_str() {
            "ns" | "nanosecond" | "nanoseconds" => 1e-9,
            "us" | "microsecond" | "microseconds" => 1e-6,
            "ms" | "millisecond" | "milliseconds" => 1e-3,
            "s" | "sec" | "secs" | "second" | "seconds" => 1.0,
            "m" | "min" | "mins" | "minute" | "minutes" => 60.0,
            "h" | "hr" | "hour" | "hours" => 3600.0,
            "d" | "day" | "days" => 86_400.0,
            "w" | "week" | "weeks" => 604_800.0,
            _ => return None,
        };
        total += value * seconds;
    }
    (total >= 0.0 && total.is_finite()).then(|| Duration::from_secs_f64(total))
}

#[cfg(test)]
mod tests {
    use tessera_types::{SeededRandom, TenantId};

    use super::*;
    use crate::parser::parse;

    fn ctx() -> BindContext {
        BindContext::new(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            Timestamp::from_nanos(1_700_000_000_000_000_000),
        )
    }

    fn lowered(sql: &str) -> Plan {
        match lower(&parse(sql).unwrap(), &ctx()).unwrap() {
            Statement::Query(plan) => plan,
            other => panic!("not a query: {other:?}"),
        }
    }

    /// A parenthesised query in `FROM` is a derived table with the
    /// columns it selects, named by its alias.
    #[test]
    fn derived_tables_nest_queries() {
        let plan = lowered(
            "SELECT t.service, n FROM (SELECT attr.service AS service, count(*) AS n FROM logs \
             GROUP BY attr.service) t WHERE n > 1 ORDER BY n DESC LIMIT 5",
        );
        let text = plan.to_string();
        assert!(
            text.starts_with(
                "Limit: offset=0 count=5\n  Project: service AS service, n AS n\n    Sort: n DESC\n      Filter: (n > 1)\n        Project: service AS service, n AS n\n          Aggregate:"
            ),
            "{text}"
        );
        assert!(text.contains("Scan: prod/log"), "{text}");
        let joined = lowered(
            "SELECT l.body FROM logs l JOIN (SELECT trace_id FROM spans WHERE status_code = 'error') e \
             ON l.trace_id = e.trace_id",
        );
        assert!(joined.to_string().contains("Join inner:"), "{joined}");
        assert!(
            parse("INSERT INTO (SELECT 1) VALUES (1)").is_err(),
            "a derived table is not a target"
        );
    }

    /// `[NOT] EXISTS (SELECT ...)` correlates by the equalities between
    /// the subquery's columns and the query's, which key a semi or anti
    /// join on the subquery's distinct rows; without one the join keys
    /// on a constant, and any other use of the query's columns is
    /// refused.
    #[test]
    fn exists_lowers_to_a_semi_join_on_its_correlation() {
        let plan = lowered(
            "SELECT l.body FROM logs l WHERE EXISTS (SELECT 1 FROM spans s \
             WHERE s.trace_id = l.trace_id AND s.status_code = 'error')",
        );
        let text = plan.to_string();
        assert!(
            text.contains("Join inner: __rows0.trace_id = __in0.__key0"),
            "{text}"
        );
        assert!(text.contains("status_code"), "{text}");
        let anti = lowered(
            "SELECT body FROM logs WHERE NOT EXISTS (SELECT 1 FROM spans \
             WHERE logs.trace_id = spans.trace_id)",
        );
        assert!(anti.to_string().contains("Join left:"), "{anti}");
        let constant = lowered(
            "SELECT body FROM logs WHERE EXISTS (SELECT 1 FROM spans WHERE status_code = 'error')",
        );
        let text = constant.to_string();
        assert!(
            text.contains("= __in0.__key0") && text.contains("Project: 1 AS __key0"),
            "{text}"
        );
        let error = |sql: &str| lower(&parse(sql).unwrap(), &ctx()).unwrap_err().message;
        assert!(
            error(
                "SELECT body FROM logs l WHERE EXISTS (SELECT 1 FROM spans s \
                 WHERE s.start_timestamp > l.timestamp)"
            )
            .contains("equalities"),
        );
        assert!(
            error("SELECT body FROM logs WHERE body = 'x' OR EXISTS (SELECT 1 FROM spans)")
                .contains("on its own"),
        );
        assert!(
            error("SELECT body FROM logs WHERE EXISTS (SELECT 1 FROM spans GROUP BY name)")
                .contains("one table"),
        );
    }

    /// Section 9.2: the one inequality of an `ASOF JOIN` is its time
    /// match, read with the left table first whichever side it was
    /// written on; a join without `ASOF` takes no inequality, and an
    /// `ASOF JOIN` needs exactly one and an equality besides.
    #[test]
    fn an_asof_join_lowers_its_inequality_as_the_time_match() {
        let plan = lowered(
            "SELECT l.body FROM logs l ASOF JOIN spans s ON l.trace_id = s.trace_id \
             AND s.timestamp <= l.timestamp",
        );
        assert!(
            plan.to_string().contains(
                "Join inner asof: l.trace_id = s.trace_id AND l.timestamp >= s.timestamp"
            ),
            "{plan}"
        );
        let error = |sql: &str| lower(&parse(sql).unwrap(), &ctx()).unwrap_err().message;
        assert!(
            error(
                "SELECT l.body FROM logs l JOIN spans s ON l.trace_id = s.trace_id \
                 AND l.timestamp >= s.timestamp"
            )
            .contains("equalities"),
        );
        for sql in [
            "SELECT l.body FROM logs l ASOF JOIN spans s ON l.trace_id = s.trace_id",
            "SELECT l.body FROM logs l ASOF JOIN spans s ON l.timestamp >= s.timestamp",
            "SELECT l.body FROM logs l ASOF JOIN spans s ON l.trace_id = s.trace_id \
             AND l.timestamp >= s.timestamp AND l.timestamp < s.timestamp",
        ] {
            assert!(error(sql).contains("ASOF JOIN"), "{sql}");
        }
    }

    /// QL-3: a language in `FROM` is a relation named after it, under
    /// the request's dataset, with the columns its own API returns.
    #[test]
    fn table_functions_embed_the_other_languages() {
        let plan = lowered(
            "SELECT timestamp, value FROM promql('up', TIMESTAMP '2024-01-01T00:00:00Z', \
             TIMESTAMP '2024-01-01T01:00:00Z', INTERVAL '1m') WHERE value > 0",
        );
        let text = plan.to_string();
        assert!(text.contains("PromQL"), "{text}");
        assert!(text.contains("prod/metric"), "{text}");
        assert!(
            text.starts_with(
                "Project: timestamp AS timestamp, value AS value\n  Filter: (value > 0)"
            ),
            "{text}"
        );

        let plan = lowered(
            r#"SELECT l.body FROM logql('{app="checkout"} |= "timeout"', now() - INTERVAL '1h', now()) l LIMIT 5"#,
        );
        let text = plan.to_string();
        assert!(text.contains("Scan: prod/log"), "{text}");
        assert!(
            text.contains("[2023-11-14T21:13:20Z, 2023-11-14T22:13:20.000000001Z)"),
            "an hour up to now: {text}"
        );

        let plan = lowered(
            "SELECT count(*) FROM traceql('{ status = error }', TIMESTAMP '2024-01-01T00:00:00Z', \
             TIMESTAMP '2024-01-02T00:00:00Z')",
        );
        assert!(plan.to_string().contains("TraceGroup"), "{plan}");

        let error = |sql: &str| lower(&parse(sql).unwrap(), &ctx()).unwrap_err().message;
        assert!(
            error("SELECT * FROM promql('up', 1, 2, INTERVAL '1m')").contains("timestamp"),
            "times are literals"
        );
        assert!(
            error("SELECT * FROM promql('up', now(), now())").contains("step"),
            "PromQL needs a step"
        );
        assert!(
            error("SELECT * FROM promql('rate(up', now(), now(), INTERVAL '1m')")
                .contains("promql:"),
            "the inner diagnostic is reported"
        );
        assert!(
            error("SELECT * FROM sqlish('x', now(), now())").contains("table function"),
            "only the three languages"
        );
    }

    fn control(sql: &str) -> ControlStatement {
        match lower(&parse(sql).unwrap(), &ctx()).unwrap() {
            Statement::Control(statement) => statement,
            other => panic!("not a control statement: {other:?}"),
        }
    }

    /// RS-5: no statement, however mangled, makes parsing or lowering
    /// panic.
    #[test]
    fn mangled_statements_never_panic() {
        let seeds = [
            "SELECT a.body, k.by FROM alerts a JOIN system.acknowledgments k ON a.attr.x = k.fingerprint WHERE k.by = 'ann' ORDER BY a.timestamp DESC LIMIT 10 OFFSET 2",
            "SELECT time_bucket(INTERVAL '5m', timestamp) AS t, count(*), max(severity_number) FROM prod.logs WHERE attr.n >= 1 AND body LIKE '%x%' GROUP BY 1 HAVING count(*) > 2",
            "CREATE ALERT errors SEVERITY critical EVERY 30s FOR 5m LABELS (team = 'pay') AS SELECT count(*) AS n FROM logs HAVING count(*) > 10",
            "INSERT INTO system.silences (id, matchers, starts_at, ends_at) VALUES ('s1', '{}', TIMESTAMP '2024-01-01T00:00:00Z', TIMESTAMP '2024-01-02T00:00:00Z')",
            "DELETE FROM logs WHERE severity_number < 9 AND timestamp < now() - INTERVAL '7d'",
            "EXPLAIN ANALYZE SELECT * FROM events WHERE kind = 'span' AND search('timeout service:api')",
        ];
        let mut random = SeededRandom::new(5);
        for round in 0..2_000 {
            let input = tessera_types::mutated(seeds[round % seeds.len()].as_bytes(), &mut random);
            let text = String::from_utf8_lossy(&input);
            if let Ok(statement) = parse(&text) {
                let _ = lower(&statement, &ctx());
            }
        }
    }

    #[test]
    fn a_join_qualifies_columns_by_table_and_lowers_its_keys() {
        let plan = lowered(
            "SELECT a.name, k.by FROM alerts a JOIN system.acknowledgments k \
             ON k.fingerprint = a.attr.alert.fingerprint WHERE k.by = 'ann' AND a.timestamp > 0",
        );
        assert_eq!(
            plan.to_string(),
            "Project: a.name AS name, k.by AS by\n  \
             Filter: ((k.by = 'ann') AND (a.timestamp > 0))\n    \
             Join inner: a.attr.alert.fingerprint = k.fingerprint\n      \
             Scan: prod/alert [all time) default\n      \
             SystemTable: default system.acknowledgments\n"
        );
        plan.schema(&Registry::builtin()).unwrap();
        let star = lowered("SELECT * FROM logs l LEFT JOIN spans s ON l.trace_id = s.trace_id");
        let Plan::Join { kind, .. } = &star else {
            panic!("a wildcard over a join projects nothing: {star}")
        };
        assert_eq!(*kind, JoinKind::Left);
        let schema = star.schema(&Registry::builtin()).unwrap();
        assert!(
            schema.field_with_name("l.body").is_ok() && schema.field_with_name("s.name").is_ok()
        );
    }

    #[test]
    fn three_part_names_read_the_tenants_the_query_may_name() {
        let mut ctx = ctx();
        let acme = TenantId::new("0123456789abcdef0123456789abcdef").unwrap();
        ctx.tenants.insert("acme".into(), acme.clone());
        let lowered = |sql: &str| match lower(&parse(sql).unwrap(), &ctx) {
            Ok(Statement::Query(plan)) => Ok(plan),
            Ok(other) => panic!("not a query: {other:?}"),
            Err(e) => Err(e.message),
        };
        let plan = lowered("SELECT count(*) FROM acme.prod.logs").unwrap();
        let Plan::Project { input, .. } = &plan else {
            panic!("{plan}")
        };
        let Plan::Aggregate { input, .. } = input.as_ref() else {
            panic!("{plan}")
        };
        let Plan::Scan(scan) = input.as_ref() else {
            panic!("{plan}")
        };
        assert_eq!(scan.tenant, acme);
        let plan = lowered("SELECT id FROM acme.system.silences").unwrap();
        assert!(
            plan.to_string()
                .contains(&format!("SystemTable: {acme} system.silences")),
            "{plan}"
        );
        let message = lowered("SELECT count(*) FROM other.prod.logs").unwrap_err();
        assert!(
            message.contains("tenant other is not known or not readable"),
            "{message}"
        );
        let message = lower(
            &parse("DELETE FROM acme.prod.logs WHERE severity_number < 9").unwrap(),
            &ctx,
        )
        .unwrap_err()
        .message;
        assert!(message.contains("request's tenant"), "{message}");
        let message = lower(
            &parse("INSERT INTO acme.system.silences (id) VALUES ('s1')").unwrap(),
            &ctx,
        )
        .unwrap_err()
        .message;
        assert!(message.contains("request's tenant"), "{message}");
    }

    /// CO-4: `system.usage` carries a `cost` column at the configured
    /// unit prices.
    #[test]
    fn usage_is_costed_at_the_configured_prices() {
        let mut ctx = ctx();
        ctx.prices.insert("price.ingest_byte".into(), 2.0);
        let Statement::Query(plan) = lower(
            &parse("SELECT phase, bytes, cost FROM system.usage WHERE cost > 0").unwrap(),
            &ctx,
        )
        .unwrap() else {
            panic!("not a query")
        };
        let schema = plan.schema(&Registry::builtin()).unwrap();
        assert_eq!(
            schema.field_with_name("cost").unwrap().data_type(),
            &DataType::Float64
        );
        assert!(plan.to_string().contains("* 2"), "{plan}");
    }

    #[test]
    fn the_events_table_unions_every_kind_on_one_schema() {
        let plan = lowered("SELECT kind, name, body FROM events WHERE kind = 'span'");
        let Plan::Project { input, .. } = &plan else {
            panic!("project: {plan}")
        };
        let Plan::Filter { input, predicate } = input.as_ref() else {
            panic!("filter: {plan}")
        };
        assert_eq!(predicate.to_string(), "(kind = 'span')");
        let Plan::Union { inputs } = input.as_ref() else {
            panic!("union: {plan}")
        };
        let kinds: Vec<Kind> = inputs
            .iter()
            .map(|branch| match branch {
                Plan::Project { input, .. } => match input.as_ref() {
                    Plan::Scan(scan) => scan.kind,
                    other => panic!("scan: {other}"),
                },
                other => panic!("project: {other}"),
            })
            .collect();
        assert_eq!(kinds, Kind::ALL);
        assert_eq!(plan.schema(&Registry::builtin()).unwrap().fields().len(), 3);
        let star = lowered("SELECT * FROM events");
        let names: Vec<String> = star
            .schema(&Registry::builtin())
            .unwrap()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names.first().map(String::as_str), Some("timestamp"));
        assert!(
            ["body", "duration_ns", "value", "event_id"]
                .iter()
                .all(|c| names.contains(&(*c).to_owned()))
        );
        assert_eq!(names.last().map(String::as_str), Some("_overflow"));
        lowered("SELECT attr.host FROM events WHERE resource.service.name = 'api'");
        let message = lower(
            &parse("DELETE FROM events WHERE kind = 'event'").unwrap(),
            &ctx(),
        )
        .unwrap_err()
        .message;
        assert!(message.contains("use one of its views"), "{message}");
    }

    #[test]
    fn join_mistakes_are_diagnosed() {
        let contains = |sql: &str, text: &str| {
            let message = lower(&parse(sql).unwrap(), &ctx()).unwrap_err().to_string();
            assert!(message.contains(text), "{message}");
        };
        contains(
            "SELECT timestamp FROM logs l JOIN spans s ON l.trace_id = s.trace_id",
            "timestamp is ambiguous",
        );
        contains(
            "SELECT * FROM logs JOIN logs ON logs.trace_id = logs.trace_id",
            "already in the query",
        );
        contains(
            "SELECT * FROM logs l JOIN spans s ON l.timestamp < s.timestamp",
            "equalities between the two tables",
        );
        contains(
            "SELECT * FROM logs l JOIN spans s ON l.trace_id = s.trace_id JOIN events e ON e.trace_id = s.trace_id",
            "one JOIN per query",
        );
        contains(
            "SELECT * FROM logs l JOIN spans s ON l.trace_id = s.trace_id WHERE attr.host = 'h1'",
            "attr.host is ambiguous",
        );
    }

    #[test]
    fn system_tables_lower_to_their_own_node() {
        let plan =
            lowered("SELECT name, severity FROM system.alert_rules WHERE severity = 'critical'");
        assert_eq!(
            plan.to_string(),
            "Project: name AS name, severity AS severity\n  \
             Filter: (severity = 'critical')\n    \
             SystemTable: default system.alert_rules\n"
        );
        plan.schema(&Registry::builtin()).unwrap();
        assert!(
            error("SELECT 1 FROM system.nope")
                .message
                .contains("unknown system table")
        );
    }

    #[test]
    fn usage_and_log_patterns_read_the_system_tenant() {
        let usage = lowered("SELECT phase, sum(bytes) AS b FROM system.usage GROUP BY phase");
        let text = usage.to_string();
        assert!(
            text.contains("Scan: usage/event [all time) _system"),
            "{text}"
        );
        assert!(text.contains("attr.bytes"), "{text}");
        usage.schema(&Registry::builtin()).unwrap();
        let patterns = lowered(
            "SELECT pattern, rows FROM system.log_patterns WHERE dataset = 'prod' ORDER BY bytes DESC",
        );
        let text = patterns.to_string();
        assert!(text.contains("Aggregate: group_by=[tenant AS tenant, dataset AS dataset, pattern_id AS pattern_id, pattern AS pattern]"), "{text}");
        assert!(text.contains("(phase = 'stored')"), "{text}");
        patterns.schema(&Registry::builtin()).unwrap();
        let by_pattern = lowered("SELECT pattern_id, count(*) FROM logs GROUP BY pattern_id");
        assert!(
            by_pattern.to_string().contains("pattern_id"),
            "{by_pattern}"
        );
        by_pattern.schema(&Registry::builtin()).unwrap();
    }

    #[test]
    fn sampling_may_keep_errored_traces_whole() {
        let ControlStatement::CreateRetentionPolicy(policy) = control(
            "CREATE RETENTION POLICY keep_errors ON prod AS WHEN kind = 'span' THEN SAMPLE 0.1 AFTER 7d EXCEPT ERRORS",
        ) else {
            panic!("not a policy")
        };
        assert_eq!(
            policy.clauses[0].actions,
            [RetentionAction::Sample {
                rate: 0.1,
                after_seconds: 7 * 86_400,
                except_errors: true,
            }]
        );
        assert_eq!(
            policy.clauses[0].actions[0].to_string(),
            "SAMPLE 0.1 AFTER 604800s EXCEPT ERRORS"
        );
    }

    #[test]
    fn retention_policies_lower_with_their_clauses() {
        let ControlStatement::CreateRetentionPolicy(policy) = control(
            "CREATE RETENTION POLICY trim_noise ON prod AS \
             WHEN kind = 'log' AND severity_number < 13 AND value_score < 0.01 \
               THEN SAMPLE 0.05 AFTER INTERVAL '3 days', DROP AFTER INTERVAL '30 days' \
             WHEN kind = 'metric' THEN DOWNSAMPLE TO INTERVAL '5 minutes' AFTER 30d \
             ELSE DROP AFTER INTERVAL '90 days'",
        ) else {
            panic!("not a policy");
        };
        assert_eq!(policy.name, "trim_noise");
        assert_eq!(policy.dataset, "prod");
        assert!(policy.enabled);
        assert_eq!(policy.clauses.len(), 3);
        assert_eq!(
            policy.clauses[0].condition.as_deref(),
            Some("kind = 'log' AND severity_number < 13 AND value_score < 0.01")
        );
        assert_eq!(
            policy.clauses[0].actions,
            [
                RetentionAction::Sample {
                    rate: 0.05,
                    after_seconds: 3 * 86_400,
                    except_errors: false,
                },
                RetentionAction::Drop {
                    after_seconds: 30 * 86_400
                }
            ]
        );
        assert_eq!(
            policy.clauses[1].actions,
            [RetentionAction::Downsample {
                interval_seconds: 300,
                after_seconds: 30 * 86_400
            }]
        );
        assert!(policy.clauses[2].condition.is_none());
        assert_eq!(
            control("DROP RETENTION POLICY trim_noise"),
            ControlStatement::DropRetentionPolicy {
                name: "trim_noise".into()
            }
        );
        assert_eq!(
            control("EXPLAIN RETENTION trim_noise"),
            ControlStatement::ExplainRetention {
                name: "trim_noise".into()
            }
        );
        assert!(
            error("CREATE RETENTION POLICY p ON prod AS ELSE SAMPLE 2 AFTER 1d")
                .message
                .contains("between 0 and 1")
        );
    }

    #[test]
    fn control_statements_lower_to_literals_and_durations() {
        let ControlStatement::CreateAlert(alert) = control(
            "CREATE ALERT errors FOR 2m EVERY '30 seconds' NOTIFY ops \
             AS SELECT count(*) AS n FROM logs HAVING count(*) > 1",
        ) else {
            panic!("not a create alert");
        };
        assert_eq!(alert.hold, Some(Duration::from_secs(120)));
        assert_eq!(alert.every, Some(Duration::from_secs(30)));
        assert_eq!(alert.dataset.as_str(), "prod");
        assert_eq!(
            alert.query,
            "SELECT count(*) AS n FROM logs HAVING count(*) > 1"
        );
        let bad = error("CREATE ALERT errors AS SELECT nothing FROM logs");
        assert!(
            bad.message.contains("nothing"),
            "the query is checked: {bad:?}"
        );

        let ControlStatement::Insert { table, rows } = control(
            "INSERT INTO system.acknowledgments (fingerprint, until, by, weight) \
             VALUES ('abcd', TIMESTAMP '2024-01-01T00:00:00Z', 'me', -1.5)",
        ) else {
            panic!("not an insert");
        };
        assert_eq!(table, "acknowledgments");
        assert_eq!(
            rows[0][0],
            ("fingerprint".to_owned(), Literal::Text("abcd".into()))
        );
        assert!(matches!(rows[0][1].1, Literal::Timestamp(_)));
        assert_eq!(rows[0][3].1, Literal::Float(-1.5));
        let ControlStatement::Delete { filter, .. } =
            control("DELETE FROM system.silences WHERE id = 's1'")
        else {
            panic!("not a delete");
        };
        assert_eq!(filter, [("id".to_owned(), Literal::Text("s1".into()))]);
        let ControlStatement::Insert { rows, .. } = control(
            "INSERT INTO system.silences (id, starts_at, ends_at) \
             VALUES ('s', now(), now() + INTERVAL '2 hours')",
        ) else {
            panic!("not an insert");
        };
        assert_eq!(rows[0][1].1, Literal::Timestamp(ctx().now));
        assert_eq!(
            rows[0][2].1,
            Literal::Timestamp(Timestamp::from_nanos(
                ctx().now.as_nanos() + 7_200 * 1_000_000_000
            ))
        );
        assert!(
            error("INSERT INTO logs (body) VALUES ('x')")
                .message
                .contains("system tables only")
        );
        assert!(error("DELETE FROM logs").message.contains("needs a WHERE"));
        let Statement::Control(ControlStatement::DeleteRows {
            kind,
            predicate,
            text,
            ..
        }) = lower(&parse("DELETE FROM logs WHERE body = 'x'").unwrap(), &ctx()).unwrap()
        else {
            panic!("not a delete of rows");
        };
        assert_eq!(kind, Kind::Log);
        assert_eq!(text, "body = 'x'");
        assert_eq!(predicate.to_string(), "(body = 'x')");
        assert!(
            error("INSERT INTO system.silences (id) VALUES (lower('x'))")
                .message
                .contains("literals only")
        );
    }

    fn error(sql: &str) -> Diagnostic {
        lower(&parse(sql).unwrap(), &ctx()).unwrap_err()
    }

    #[test]
    fn a_plain_query_becomes_scan_filter_sort_project_limit() {
        let plan = lowered(
            "SELECT body, attr.http.method AS method FROM logs \
             WHERE severity_number >= 17 AND timestamp > '2024-01-01' \
             ORDER BY timestamp DESC LIMIT 10",
        );
        assert_eq!(
            plan.to_string(),
            "Limit: offset=0 count=10\n  \
             Project: body AS body, attr.http.method AS method\n    \
             Sort: timestamp DESC\n      \
             Filter: ((severity_number >= 17) AND (timestamp > TIMESTAMP '2024-01-01T00:00:00Z'))\n        \
             Scan: prod/log [all time) default\n"
        );
        plan.schema(&Registry::builtin()).unwrap();
    }

    #[test]
    fn select_star_keeps_the_scan_schema() {
        let plan = lowered("SELECT * FROM staging.spans");
        assert_eq!(plan.to_string(), "Scan: staging/span [all time) default\n");
    }

    #[test]
    fn aggregates_group_and_project() {
        let plan = lowered(
            "SELECT severity_text, count(*) AS n, max(timestamp), count(*) / 2 \
             FROM logs GROUP BY severity_text ORDER BY n DESC",
        );
        assert_eq!(
            plan.to_string(),
            "Project: severity_text AS severity_text, n AS n, max AS max, (n / 2) AS column4\n  \
             Sort: n DESC\n    \
             Aggregate: group_by=[severity_text AS severity_text] aggregates=[count() AS n, max(timestamp) AS max]\n      \
             Scan: prod/log [all time) default\n"
        );
        plan.schema(&Registry::builtin()).unwrap();
        let having = lowered(
            "SELECT severity_text, count(*) AS n FROM logs \
             GROUP BY severity_text HAVING count(*) > 1 AND min(timestamp) > '2024-01-01'",
        );
        assert_eq!(
            having.to_string(),
            "Project: severity_text AS severity_text, n AS n\n  \
             Filter: ((n > 1) AND (min > TIMESTAMP '2024-01-01T00:00:00Z'))\n    \
             Aggregate: group_by=[severity_text AS severity_text] aggregates=[count() AS n, min(timestamp) AS min]\n      \
             Scan: prod/log [all time) default\n"
        );
        having.schema(&Registry::builtin()).unwrap();
        let bucketed = lowered(
            "SELECT time_bucket(INTERVAL '5m', timestamp) AS bucket, avg(severity_number) \
             FROM logs GROUP BY time_bucket(INTERVAL '5m', timestamp)",
        );
        assert!(
            bucketed
                .to_string()
                .starts_with("Project: bucket AS bucket, avg AS avg")
        );
    }

    #[test]
    fn constants_and_now() {
        let plan = lowered("SELECT 1 AS one, 'x', now()");
        let schema = plan.schema(&Registry::builtin()).unwrap();
        assert_eq!(schema.field(0).name(), "one");
        assert_eq!(schema.field(1).name(), "column2");
        assert_eq!(schema.field(2).data_type(), &timestamp_type());
        let Plan::Values { rows, .. } = plan else {
            panic!()
        };
        assert_eq!(rows[0][2], Literal::Timestamp(ctx().now));
    }

    #[test]
    fn predicates_lower_with_coercions() {
        let plan = lowered(
            "SELECT * FROM logs WHERE body NOT ILIKE '%x%' AND severity_number IN (1, 2) \
             AND timestamp BETWEEN '2024-01-01' AND '2024-01-02' AND trace_id IS NULL \
             AND attr.n::int = 2 AND -severity_number < -1",
        );
        let Plan::Filter { predicate, .. } = plan else {
            panic!()
        };
        let text = predicate.to_string();
        assert!(text.contains("body NOT ILIKE '%x%'"));
        assert!(text.contains("severity_number IN (1, 2)"));
        assert!(text.contains("(timestamp >= TIMESTAMP '2024-01-01T00:00:00Z')"));
        assert!(text.contains("trace_id IS NULL"));
        assert!(text.contains("CAST(attr.n AS Int64) = 2"));
        assert!(text.contains("(-severity_number < -1)"));
    }

    #[test]
    fn search_lowers_to_a_predicate_over_the_table() {
        let plan =
            lowered("SELECT body FROM logs WHERE search('timeout -healthcheck service:checkout')");
        let Plan::Project { input, .. } = plan else {
            panic!()
        };
        let Plan::Filter { predicate, .. } = *input else {
            panic!()
        };
        assert_eq!(
            predicate.to_string(),
            "((has_token(body, 'timeout') AND NOT has_token(body, 'healthcheck')) AND (resource.service.name = 'checkout'))"
        );
        let e = error("SELECT search('x')");
        assert!(e.message.contains("needs a table"));
        let e = error("SELECT body FROM logs WHERE search(body)");
        assert!(e.message.contains("takes one string"));
        let e = error("SELECT body FROM logs WHERE search('severity:loud')");
        assert_eq!(e.message, "in search(): unknown severity loud");
    }

    #[test]
    fn errors_point_at_the_offending_name() {
        let e = error("SELECT nope FROM logs");
        assert_eq!(e.message, "unknown column nope");
        assert_eq!(e.span, Some(Span::new(7, 11)));
        let e = error("SELECT * FROM nowhere");
        assert!(e.message.starts_with("unknown table nowhere"));
        let e = error("SELECT median(x) FROM logs");
        assert_eq!(e.message, "unknown function median");
        let e = error("SELECT body FROM logs WHERE count(*) > 1");
        assert!(e.message.contains("not allowed here"));
        let e = error("SELECT max(*) FROM logs");
        assert_eq!(e.message, "max(*) is not valid");
        let e = error("SELECT TIMESTAMP 'yesterday' FROM logs");
        assert!(e.message.starts_with("invalid timestamp"));
        let e = error("SELECT INTERVAL '5 fortnights' FROM logs");
        assert!(e.message.starts_with("invalid interval"));
        let e = error("SELECT body FROM logs WHERE x IN (body)");
        assert!(e.message.contains("unknown column x"));
    }

    #[test]
    fn intervals_cover_sql_and_compact_forms() {
        assert_eq!(interval("5 minutes"), Some(Duration::from_secs(300)));
        assert_eq!(
            interval("1 hour 30 minutes"),
            Some(Duration::from_secs(5400))
        );
        assert_eq!(interval("1h30m"), Some(Duration::from_secs(5400)));
        assert_eq!(interval("250ms"), Some(Duration::from_millis(250)));
        assert_eq!(interval("1.5 days"), Some(Duration::from_secs(129_600)));
        assert_eq!(interval(""), None);
        assert_eq!(interval("5"), None);
        assert_eq!(interval("x minutes"), None);
    }

    #[test]
    fn parameters_bind_to_the_values_given() {
        let statement =
            parse("SELECT body FROM logs WHERE body = $1 AND timestamp > $2 AND $3 IS NULL")
                .unwrap();
        let values = [
            Parameter::Text("x".into()),
            Parameter::Text("2024-01-01".into()),
            Parameter::Null,
        ];
        let plan = match lower_with(&statement, &ctx(), &Parameters::Bound(&values)).unwrap() {
            Statement::Query(plan) => plan,
            other => panic!("not a query: {other:?}"),
        };
        let Plan::Project { input, .. } = plan else {
            panic!("{plan:?}")
        };
        let Plan::Filter { predicate, .. } = *input else {
            panic!("{input:?}")
        };
        let text = predicate.to_string();
        assert!(text.contains("body = 'x'"), "{text}");
        assert!(
            text.contains("timestamp > TIMESTAMP '2024-01-01T00:00:00Z'"),
            "text binds as a timestamp against a timestamp column: {text}"
        );
        assert!(text.contains("NULL IS NULL"), "{text}");
    }

    /// A `$n` in `LIMIT` or `OFFSET` binds to a whole number, is
    /// described as an integer, and anything else is refused.
    #[test]
    fn parameters_bind_in_limit_and_offset() {
        let statement = parse("SELECT body FROM logs LIMIT $1 OFFSET $2").unwrap();
        let values = [Parameter::Int(5), Parameter::Int(10)];
        let plan = match lower_with(&statement, &ctx(), &Parameters::Bound(&values)).unwrap() {
            Statement::Query(plan) => plan,
            other => panic!("not a query: {other:?}"),
        };
        assert!(
            plan.to_string().starts_with("Limit: offset=10 count=5\n"),
            "{plan}"
        );
        let (_, types) = describe(&statement, &ctx()).unwrap();
        assert_eq!(types, vec![Some(DataType::Int64), Some(DataType::Int64)]);
        let error = lower_with(
            &statement,
            &ctx(),
            &Parameters::Bound(&[Parameter::Text("ten".into()), Parameter::Int(0)]),
        )
        .unwrap_err();
        assert!(error.message.contains("whole number"), "{}", error.message);
        let error = lower_with(
            &statement,
            &ctx(),
            &Parameters::Bound(&[Parameter::Int(-1), Parameter::Int(0)]),
        )
        .unwrap_err();
        assert!(error.message.contains("whole number"), "{}", error.message);
    }

    #[test]
    fn an_unbound_parameter_is_an_error() {
        let statement = parse("SELECT body FROM logs WHERE body = $2").unwrap();
        let values = [Parameter::Text("x".into())];
        let error = lower_with(&statement, &ctx(), &Parameters::Bound(&values)).unwrap_err();
        assert_eq!(
            error.message,
            "parameter $2 is not bound: 1 value(s) were given"
        );
        assert!(error.span.is_some(), "the error points at the parameter");
        assert!(
            lower(&statement, &ctx()).is_err(),
            "without values none is bound"
        );
    }

    #[test]
    fn parameter_types_follow_what_they_are_compared_with() {
        let statement = parse(
            "SELECT body FROM logs WHERE timestamp > $1 AND body = $2 AND $3::int = 1 \
             AND severity_number IN ($4, 3) AND $5 = 1.5 AND $6 IS NULL",
        )
        .unwrap();
        let (lowered, types) = describe(&statement, &ctx()).unwrap();
        assert!(matches!(lowered, Statement::Query(_)));
        assert_eq!(
            types,
            vec![
                Some(timestamp_type()),
                Some(DataType::Utf8),
                Some(DataType::Int64),
                Some(DataType::Int32),
                Some(DataType::Float64),
                None,
            ]
        );
        let constant = parse("SELECT $1 AS t, $2 AS n").unwrap();
        let (_, types) = describe(&constant, &ctx()).unwrap();
        assert_eq!(types, vec![None, None], "nothing says what they are");
    }

    #[test]
    fn in_subquery_is_a_join_on_distinct_keys() {
        let plan = lowered(
            "SELECT body FROM logs WHERE trace_id IN (SELECT trace_id FROM spans WHERE \
             status_code = 'error') AND severity_number > 1",
        );
        let text = plan.to_string();
        assert!(
            text.starts_with(
                "Project: body AS body\n  Filter: (severity_number > 1)\n    Project: "
            ),
            "the rest of WHERE and the select see the table's own names: {text}"
        );
        assert!(
            text.contains("Join inner: __rows0.trace_id = __in0.trace_id"),
            "{text}"
        );
        assert!(
            text.contains("Aggregate: group_by=[trace_id AS trace_id] aggregates=[]"),
            "the keys are distinct so rows are not multiplied: {text}"
        );
        assert!(text.contains("Scan: prod/span"), "{text}");
        let anti =
            lowered("SELECT body FROM logs WHERE trace_id NOT IN (SELECT trace_id FROM spans)");
        let text = anti.to_string();
        assert!(
            text.contains("Join left: __rows0.trace_id = __in0.trace_id"),
            "{text}"
        );
        assert!(text.contains("Filter: __in0.trace_id IS NULL"), "{text}");
    }

    #[test]
    fn in_subquery_must_stand_alone_and_select_one_column() {
        let e = error("SELECT body FROM logs WHERE trace_id IN (SELECT trace_id, name FROM spans)");
        assert_eq!(
            e.message,
            "the subquery of IN must select one column, this one 2"
        );
        let e = error(
            "SELECT body FROM logs WHERE body = 'x' OR trace_id IN (SELECT trace_id FROM spans)",
        );
        assert!(
            e.message
                .starts_with("IN (SELECT ...) is a condition of WHERE on its own"),
            "{e:?}"
        );
    }
}
