// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Lowering: from the syntax tree to the shared plan.
//!
//! A query becomes `Scan → Filter → [Aggregate] → [Sort] → [Project] →
//! [Sort] → [Limit]`. Names resolve against the kind's schema; dotted
//! names under `attr`, `resource` or `scope` are attribute paths. With
//! aggregates or `GROUP BY`, every select item must be a grouping
//! expression, an aggregate, or an expression over those, as in
//! `PostgreSQL`.

use std::collections::BTreeMap;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use tessera_ir::{
    AggregateExpr, AlertDefinition, BinaryOp, BindContext, ControlStatement, Diagnostic, Expr,
    Literal, NamedExpr, Plan, Registry, Scan, SortKey, Span, Statement,
};
use tessera_types::{DatasetName, Kind, TenantId, Timestamp, event};

use crate::ast;

/// Lowers a parsed statement under `ctx`.
///
/// # Errors
///
/// The first name, function or literal that cannot be resolved, with its
/// position.
pub fn lower(statement: &ast::Statement, ctx: &BindContext) -> Result<Statement, Diagnostic> {
    let registry = Registry::builtin();
    match statement {
        ast::Statement::Select(query) => Ok(Statement::Query(lower_query(query, ctx, &registry)?)),
        ast::Statement::Explain { analyze, query } => Ok(Statement::Explain {
            plan: lower_query(query, ctx, &registry)?,
            analyze: *analyze,
        }),
        ast::Statement::CreateAlert(alert) => {
            // The query must be one the engine can run before the rule
            // is stored.
            lower_query(&alert.query, ctx, &registry)?;
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
        ast::Statement::DropAlert { name } => Ok(Statement::Control(ControlStatement::DropAlert {
            name: name.clone(),
        })),
        ast::Statement::ExplainAlert { name } => {
            Ok(Statement::Control(ControlStatement::ExplainAlert {
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
        ast::Statement::Delete { table, filter } => {
            Ok(Statement::Control(ControlStatement::Delete {
                table: control_table(table)?,
                filter: literals(filter, ctx)?,
            }))
        }
    }
}

/// The control table a statement writes: only `system.<table>` can be
/// written through SQL (CS-3).
fn control_table(table: &ast::TableRef) -> Result<String, Diagnostic> {
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

/// A duration option of `CREATE ALERT`, as written.
fn duration(option: Option<&(String, Span)>) -> Result<Option<Duration>, Diagnostic> {
    match option {
        None => Ok(None),
        Some((text, span)) => interval(text)
            .map(Some)
            .ok_or_else(|| Diagnostic::at(*span, format!("invalid duration {text}"))),
    }
}

/// The columns an expression may refer to.
struct Scope {
    schema: SchemaRef,
    table: Option<String>,
    kind: Option<Kind>,
}

impl Scope {
    fn column_type(&self, name: &str) -> Option<DataType> {
        self.schema
            .field_with_name(name)
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
    scope: Scope,
    substitution: Option<Substitution>,
}

fn lower_query(
    query: &ast::Query,
    ctx: &BindContext,
    registry: &Registry,
) -> Result<Plan, Diagnostic> {
    let Some(from) = &query.from else {
        return lower_constant_query(query, ctx, registry);
    };
    let (scope, mut plan) = if from.dataset.as_deref() == Some("system") && from.table == "usage" {
        usage_source(ctx)
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
            Scope {
                schema,
                table: Some(from.table.clone()),
                kind: None,
            },
            Plan::SystemTable {
                tenant: ctx.tenant.clone(),
                table: from.table.clone(),
            },
        )
    } else {
        let (dataset, kind) = resolve_table(from, ctx)?;
        (
            Scope {
                schema: event::schema(kind),
                table: Some(from.table.clone()),
                kind: Some(kind),
            },
            Plan::Scan(Scan::all(ctx.tenant.clone(), dataset, kind)),
        )
    };
    let mut lowerer = Lowerer {
        ctx,
        registry,
        scope,
        substitution: None,
    };
    if let Some(filter) = &query.filter {
        plan = plan.filter(lowerer.expr(filter)?);
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

/// `system.usage` (CO-4): the `_system.usage` events with their
/// attributes as columns, read whatever the request's tenant.
fn usage_source(ctx: &BindContext) -> (Scope, Plan) {
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
    (
        Scope {
            schema,
            table: Some("usage".into()),
            kind: None,
        },
        scan.project(exprs),
    )
}

/// `SELECT 1`, `SELECT now()`: constant rows without a table.
fn lower_constant_query(
    query: &ast::Query,
    ctx: &BindContext,
    registry: &Registry,
) -> Result<Plan, Diagnostic> {
    let lowerer = Lowerer {
        ctx,
        registry,
        scope: Scope {
            schema: std::sync::Arc::new(Schema::empty()),
            table: None,
            kind: None,
        },
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
    Ok(apply_limit(plan, query))
}

fn resolve_table(
    from: &ast::TableRef,
    ctx: &BindContext,
) -> Result<(DatasetName, Kind), Diagnostic> {
    let kind = match from.table.as_str() {
        "logs" => Kind::Log,
        "spans" => Kind::Span,
        "metrics" => Kind::Metric,
        "events" => Kind::Event,
        "alerts" => Kind::Alert,
        other => {
            return Err(Diagnostic::at(
                from.span,
                format!(
                    "unknown table {other}: tables are logs, spans, metrics, events and alerts"
                ),
            ));
        }
    };
    let dataset = match &from.dataset {
        Some(name) => DatasetName::new(name.clone())
            .map_err(|e| Diagnostic::at(from.span, format!("invalid dataset: {e}")))?,
        None => ctx.dataset.clone(),
    };
    Ok((dataset, kind))
}

fn apply_limit(plan: Plan, query: &ast::Query) -> Plan {
    if query.limit.is_some() || query.offset.is_some() {
        plan.limit(query.offset.unwrap_or(0), query.limit)
    } else {
        plan
    }
}

fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
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
        Ok(apply_limit(plan, query))
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
        Ok(apply_limit(plan, query))
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
        {
            if self.registry.is_aggregate(name) {
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
        }
        for child in children(expr) {
            self.collect_aggregates(child, None, substitution, aggregates)?;
        }
        Ok(())
    }

    fn has_aggregate(&self, expr: &ast::Expr) -> bool {
        if let ast::ExprKind::Call { name, .. } = &expr.kind {
            if self.registry.is_aggregate(name) {
                return true;
            }
        }
        children(expr).into_iter().any(|c| self.has_aggregate(c))
    }

    fn select_items(&self, query: &ast::Query) -> Result<Vec<NamedExpr>, Diagnostic> {
        let mut items = Vec::new();
        for (i, item) in query.items.iter().enumerate() {
            match item {
                ast::SelectItem::Wildcard => {
                    for field in self.scope.schema.fields() {
                        items.push(NamedExpr::new(Expr::column(field.name()), field.name()));
                    }
                }
                ast::SelectItem::Expr { expr, alias } => {
                    let name = alias.clone().unwrap_or_else(|| default_name(expr, i));
                    items.push(NamedExpr::new(self.expr(expr)?, name));
                }
            }
        }
        Ok(items)
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
            } => {
                let tested = self.expr(tested)?;
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
                    negated: *negated,
                })
            }
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
            } => Ok(Expr::Cast {
                expr: Box::new(self.expr(inner)?),
                to: data_type(type_name).ok_or_else(|| {
                    Diagnostic::at(expr.span, format!("unknown type {type_name}"))
                })?,
            }),
        }
    }

    fn name(&self, parts: &[String], span: Span) -> Result<Expr, Diagnostic> {
        let scopes = ["attr", "resource", "scope"];
        let parts: Vec<&str> = match parts {
            [table, rest @ ..] if Some(table) == self.scope.table.as_ref() && !rest.is_empty() => {
                rest.iter().map(String::as_str).collect()
            }
            _ => parts.iter().map(String::as_str).collect(),
        };
        if let [single] = parts.as_slice() {
            if self.scope.column_type(single).is_some() {
                return Ok(Expr::column(*single));
            }
            if let Some((prefix, _)) = single.split_once('.') {
                if scopes.contains(&prefix) {
                    return Ok(path(single));
                }
            }
        }
        if parts.len() > 1 && scopes.contains(&parts[0]) {
            return Ok(path(&parts.join(".")));
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
        let mut left = self.expr(left)?;
        let mut right = self.expr(right)?;
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
        let target_type = match target {
            Expr::Column(name) => self
                .scope
                .column_type(name)
                .or_else(|| self.substituted_type(name)),
            Expr::Cast { to, .. } => Some(to.clone()),
            _ => None,
        };
        match (&literal, target_type) {
            (Literal::Text(text), Some(t)) if t == timestamp_type() => text
                .parse::<Timestamp>()
                .map_or(literal, Literal::Timestamp),
            _ => literal,
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
        let kind = self
            .scope
            .kind
            .ok_or_else(|| Diagnostic::at(span, "search() needs a table"))?;
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
    use tessera_types::TenantId;

    use super::*;
    use crate::parser::parse;

    fn ctx() -> BindContext {
        BindContext {
            tenant: TenantId::DEFAULT,
            dataset: DatasetName::new("prod").unwrap(),
            now: Timestamp::from_nanos(1_700_000_000_000_000_000),
        }
    }

    fn lowered(sql: &str) -> Plan {
        match lower(&parse(sql).unwrap(), &ctx()).unwrap() {
            Statement::Query(plan) => plan,
            other => panic!("not a query: {other:?}"),
        }
    }

    fn control(sql: &str) -> ControlStatement {
        match lower(&parse(sql).unwrap(), &ctx()).unwrap() {
            Statement::Control(statement) => statement,
            other => panic!("not a control statement: {other:?}"),
        }
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
            error("DELETE FROM logs WHERE body = 'x'")
                .message
                .contains("system tables only")
        );
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
}
