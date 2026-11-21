// SPDX-License-Identifier: Apache-2.0

//! A recursive-descent parser for the supported SQL.
//!
//! Precedence, lowest first: `OR`, `AND`, `NOT`, comparison and
//! predicates (`=`, `<>`, `<`, `<=`, `>`, `>=`, `LIKE`, `ILIKE`, `IN`,
//! `BETWEEN`, `IS NULL`), `+ -`, `* / %`, unary `-`, `::` casts.

use tessera_ir::{Diagnostic, Span};

use crate::ast::{
    BinaryOp, Count, CreateAlert, CreateRetentionPolicy, CreateRollup, Expr, ExprKind, Join,
    JoinKind, OrderItem, Query, RetentionActionAst, RetentionClauseAst, SelectItem, Statement,
    TableRef,
};
use crate::lexer::{Spanned, Token, tokenize};

/// Parses one statement.
///
/// # Errors
///
/// The first syntax error, with its position.
pub fn parse(text: &str) -> Result<Statement, Diagnostic> {
    let tokens = tokenize(text)?;
    let mut parser = Parser {
        text,
        tokens,
        at: 0,
        end: text.len(),
    };
    let statement = parser.statement()?;
    let _ = parser.eat_symbol(";");
    if let Some(extra) = parser.peek() {
        return Err(Diagnostic::at(
            extra.span,
            format!("unexpected {} after the statement", describe(&extra.token)),
        ));
    }
    Ok(statement)
}

struct Parser<'a> {
    text: &'a str,
    tokens: Vec<Spanned>,
    at: usize,
    end: usize,
}

fn describe(token: &Token) -> String {
    match token {
        Token::Ident { name, .. } => format!("{name:?}"),
        Token::Number(n) => format!("number {n}"),
        Token::String(s) => format!("string {s:?}"),
        Token::Symbol(s) => format!("{s:?}"),
        Token::Parameter(n) => format!("${n}"),
    }
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Spanned> {
        self.tokens.get(self.at)
    }

    fn span_here(&self) -> Span {
        self.peek()
            .map_or(Span::new(self.end, self.end), |t| t.span)
    }

    fn advance(&mut self) -> Option<Spanned> {
        let token = self.tokens.get(self.at).cloned();
        if token.is_some() {
            self.at += 1;
        }
        token
    }

    fn is_keyword(&self, keyword: &str) -> bool {
        matches!(
            self.peek(),
            Some(Spanned {
                token: Token::Ident { name, quoted: false },
                ..
            }) if name == keyword
        )
    }

    fn is_symbol(&self, symbol: &str) -> bool {
        matches!(self.peek(), Some(Spanned { token: Token::Symbol(s), .. }) if *s == symbol)
    }

    fn eat_keyword(&mut self, keyword: &str) -> bool {
        if self.is_keyword(keyword) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn eat_symbol(&mut self, symbol: &str) -> bool {
        if self.is_symbol(symbol) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<(), Diagnostic> {
        if self.eat_keyword(keyword) {
            Ok(())
        } else {
            Err(self.unexpected(&format!("expected {}", keyword.to_ascii_uppercase())))
        }
    }

    fn expect_symbol(&mut self, symbol: &str) -> Result<Span, Diagnostic> {
        let span = self.span_here();
        if self.eat_symbol(symbol) {
            Ok(span)
        } else {
            Err(self.unexpected(&format!("expected {symbol:?}")))
        }
    }

    fn unexpected(&self, expected: &str) -> Diagnostic {
        match self.peek() {
            Some(token) => Diagnostic::at(
                token.span,
                format!("{expected}, found {}", describe(&token.token)),
            ),
            None => Diagnostic::at(
                Span::new(self.end, self.end),
                format!("{expected}, found end of input"),
            ),
        }
    }

    fn identifier(&mut self) -> Result<(String, Span), Diagnostic> {
        match self.peek() {
            Some(Spanned {
                token: Token::Ident { name, .. },
                span,
            }) => {
                let out = (name.clone(), *span);
                self.at += 1;
                Ok(out)
            }
            _ => Err(self.unexpected("expected a name")),
        }
    }

    fn statement(&mut self) -> Result<Statement, Diagnostic> {
        if self.eat_keyword("explain") {
            if self.eat_keyword("alert") {
                let (name, _) = self.identifier()?;
                return Ok(Statement::ExplainAlert { name });
            }
            if self.eat_keyword("retention") {
                let (name, _) = self.identifier()?;
                return Ok(Statement::ExplainRetention { name });
            }
            if self.eat_keyword("layout") {
                let dataset = match self.peek() {
                    Some(Spanned {
                        token: Token::Ident { .. },
                        ..
                    }) => Some(self.identifier()?.0),
                    _ => None,
                };
                return Ok(Statement::ExplainLayout { dataset });
            }
            let analyze = self.eat_keyword("analyze");
            let query = self.query()?;
            return Ok(Statement::Explain { analyze, query });
        }
        if self.eat_keyword("create") {
            if self.eat_keyword("retention") {
                self.expect_keyword("policy")?;
                return self
                    .create_retention_policy()
                    .map(Statement::CreateRetentionPolicy);
            }
            if self.eat_keyword("rollup") {
                return self
                    .create_rollup()
                    .map(|rollup| Statement::CreateRollup(Box::new(rollup)));
            }
            self.expect_keyword("alert")?;
            return self
                .create_alert()
                .map(|alert| Statement::CreateAlert(Box::new(alert)));
        }
        if self.eat_keyword("drop") {
            if self.eat_keyword("retention") {
                self.expect_keyword("policy")?;
                let (name, _) = self.identifier()?;
                return Ok(Statement::DropRetentionPolicy { name });
            }
            if self.eat_keyword("rollup") {
                let (name, _) = self.identifier()?;
                return Ok(Statement::DropRollup { name });
            }
            self.expect_keyword("alert")?;
            let (name, _) = self.identifier()?;
            return Ok(Statement::DropAlert { name });
        }
        if self.eat_keyword("insert") {
            return self.insert();
        }
        if self.eat_keyword("update") {
            return self.update();
        }
        if self.eat_keyword("delete") {
            return self.delete();
        }
        Ok(Statement::Select(self.query()?))
    }

    /// After `CREATE ALERT`: the name, the options in any order, then
    /// `AS` and the query.
    fn create_alert(&mut self) -> Result<CreateAlert, Diagnostic> {
        let (name, _) = self.identifier()?;
        let (mut severity, mut hold, mut every) = (None, None, None);
        let (mut notify, mut labels, mut annotations) = (Vec::new(), Vec::new(), Vec::new());
        loop {
            if self.eat_keyword("severity") {
                severity = Some(self.name_or_string()?);
            } else if self.eat_keyword("for") {
                hold = Some(self.duration_text()?);
            } else if self.eat_keyword("every") {
                every = Some(self.duration_text()?);
            } else if self.eat_keyword("notify") {
                notify.push(self.name_or_string()?);
                while self.eat_symbol(",") {
                    notify.push(self.name_or_string()?);
                }
            } else if self.eat_keyword("labels") {
                labels = self.pairs()?;
            } else if self.eat_keyword("annotations") {
                annotations = self.pairs()?;
            } else {
                break;
            }
        }
        self.expect_keyword("as")?;
        let start = self.span_here().start;
        let query = self.query()?;
        let end = self
            .tokens
            .get(self.at.saturating_sub(1))
            .map_or(start, |t| t.span.end);
        Ok(CreateAlert {
            name,
            severity,
            hold,
            every,
            notify,
            labels,
            annotations,
            query,
            query_text: self.text[start..end].to_owned(),
        })
    }

    /// After `CREATE ROLLUP`: the name, `EVERY` and `INTO` in any order,
    /// then `AS` and the query.
    fn create_rollup(&mut self) -> Result<CreateRollup, Diagnostic> {
        let (name, _) = self.identifier()?;
        let (mut every, mut target, mut backfill) = (None, None, None);
        loop {
            if self.eat_keyword("every") {
                every = Some(self.duration_text()?);
            } else if self.eat_keyword("into") {
                target = Some(self.identifier()?.0);
            } else if self.eat_keyword("backfill") {
                backfill = Some(self.duration_text()?);
            } else {
                break;
            }
        }
        self.expect_keyword("as")?;
        let start = self.span_here().start;
        let query = self.query()?;
        let end = self
            .tokens
            .get(self.at.saturating_sub(1))
            .map_or(start, |t| t.span.end);
        Ok(CreateRollup {
            name,
            every,
            target,
            backfill,
            query,
            query_text: self.text[start..end].to_owned(),
        })
    }

    /// After `CREATE RETENTION POLICY`: `name ON dataset AS` then the
    /// clauses.
    fn create_retention_policy(&mut self) -> Result<CreateRetentionPolicy, Diagnostic> {
        let (name, _) = self.identifier()?;
        self.expect_keyword("on")?;
        let (dataset, _) = self.identifier()?;
        self.expect_keyword("as")?;
        let mut clauses = Vec::new();
        loop {
            if self.eat_keyword("when") {
                let start = self.span_here().start;
                let condition = self.expr()?;
                let end = self
                    .tokens
                    .get(self.at.saturating_sub(1))
                    .map_or(start, |t| t.span.end);
                let text = self.text[start..end].to_owned();
                self.expect_keyword("then")?;
                clauses.push(RetentionClauseAst {
                    condition: Some((condition, text)),
                    actions: self.retention_actions()?,
                });
            } else if self.eat_keyword("else") {
                clauses.push(RetentionClauseAst {
                    condition: None,
                    actions: self.retention_actions()?,
                });
                break;
            } else {
                break;
            }
        }
        if clauses.is_empty() {
            return Err(self.unexpected("expected WHEN or ELSE"));
        }
        Ok(CreateRetentionPolicy {
            name,
            dataset,
            clauses,
        })
    }

    /// `action [, action]*` of a retention clause.
    fn retention_actions(&mut self) -> Result<Vec<RetentionActionAst>, Diagnostic> {
        let mut actions = vec![self.retention_action()?];
        while self.eat_symbol(",") {
            actions.push(self.retention_action()?);
        }
        Ok(actions)
    }

    fn retention_action(&mut self) -> Result<RetentionActionAst, Diagnostic> {
        if self.eat_keyword("sample") {
            let rate = match self.advance() {
                Some(Spanned {
                    token: Token::Number(n),
                    span,
                }) => (n, span),
                _ => return Err(self.unexpected("expected the fraction to keep")),
            };
            self.expect_keyword("after")?;
            let after = self.interval_text()?;
            let except_errors = if self.eat_keyword("except") {
                self.expect_keyword("errors")?;
                true
            } else {
                false
            };
            return Ok(RetentionActionAst::Sample {
                rate,
                after,
                except_errors,
            });
        }
        if self.eat_keyword("drop") {
            self.expect_keyword("after")?;
            let after = self.interval_text()?;
            return Ok(RetentionActionAst::Drop { after });
        }
        if self.eat_keyword("downsample") {
            self.expect_keyword("to")?;
            let interval = self.interval_text()?;
            self.expect_keyword("after")?;
            let after = self.interval_text()?;
            return Ok(RetentionActionAst::Downsample { interval, after });
        }
        Err(self.unexpected("expected SAMPLE, DROP or DOWNSAMPLE"))
    }

    /// A duration written as `INTERVAL '...'` or bare, as in `CREATE
    /// ALERT`.
    fn interval_text(&mut self) -> Result<(String, Span), Diagnostic> {
        if self.eat_keyword("interval") {
            return self.string();
        }
        self.duration_text()
    }

    /// A bare name or a string.
    fn name_or_string(&mut self) -> Result<String, Diagnostic> {
        match self.peek() {
            Some(Spanned {
                token: Token::String(_),
                ..
            }) => Ok(self.string()?.0),
            _ => Ok(self.identifier()?.0),
        }
    }

    /// A duration as written: a string such as `'2m'`, or a number with
    /// an optional unit such as `2m` or `90 seconds`.
    fn duration_text(&mut self) -> Result<(String, Span), Diagnostic> {
        match self.peek().cloned() {
            Some(Spanned {
                token: Token::String(_),
                ..
            }) => self.string(),
            Some(Spanned {
                token: Token::Number(number),
                span,
            }) => {
                self.at += 1;
                let mut text = number;
                let mut end = span.end;
                if let Some(Spanned {
                    token:
                        Token::Ident {
                            name,
                            quoted: false,
                        },
                    span: unit,
                }) = self.peek().cloned()
                    && !RESERVED.contains(&name.as_str())
                    && !ALERT_KEYWORDS.contains(&name.as_str())
                {
                    self.at += 1;
                    text.push(' ');
                    text.push_str(&name);
                    end = unit.end;
                }
                Ok((text, Span::new(span.start, end)))
            }
            _ => Err(self.unexpected("expected a duration")),
        }
    }

    /// `(name = 'value', ...)`.
    fn pairs(&mut self) -> Result<Vec<(String, String)>, Diagnostic> {
        self.expect_symbol("(")?;
        let mut pairs = Vec::new();
        loop {
            let (name, _) = self.identifier()?;
            self.expect_symbol("=")?;
            let (value, _) = self.string()?;
            pairs.push((name, value));
            if !self.eat_symbol(",") {
                break;
            }
        }
        self.expect_symbol(")")?;
        Ok(pairs)
    }

    /// After `INSERT`: `INTO table (columns) VALUES (...), ...`.
    fn insert(&mut self) -> Result<Statement, Diagnostic> {
        self.expect_keyword("into")?;
        let table = self.table_ref(false)?;
        self.expect_symbol("(")?;
        let mut columns = vec![self.identifier()?.0];
        while self.eat_symbol(",") {
            columns.push(self.identifier()?.0);
        }
        self.expect_symbol(")")?;
        self.expect_keyword("values")?;
        let mut rows = Vec::new();
        loop {
            let open = self.expect_symbol("(")?;
            let mut row = vec![self.expr()?];
            while self.eat_symbol(",") {
                row.push(self.expr()?);
            }
            self.expect_symbol(")")?;
            if row.len() != columns.len() {
                return Err(Diagnostic::at(
                    open,
                    format!("{} values for {} columns", row.len(), columns.len()),
                ));
            }
            rows.push(row);
            if !self.eat_symbol(",") {
                break;
            }
        }
        Ok(Statement::Insert {
            table,
            columns,
            rows,
        })
    }

    /// After `UPDATE`: `table SET column = literal, ... [WHERE ...]`.
    fn update(&mut self) -> Result<Statement, Diagnostic> {
        let table = self.table_ref(false)?;
        self.expect_keyword("set")?;
        let mut set = vec![self.assignment()?];
        while self.eat_symbol(",") {
            set.push(self.assignment()?);
        }
        let filter = self.equalities()?;
        Ok(Statement::Update { table, set, filter })
    }

    /// After `DELETE`: `FROM table [WHERE ...]`.
    fn delete(&mut self) -> Result<Statement, Diagnostic> {
        self.expect_keyword("from")?;
        let table = self.table_ref(false)?;
        let (filter, filter_text) = if self.eat_keyword("where") {
            let start = self.span_here().start;
            let filter = self.expr()?;
            let end = self
                .tokens
                .get(self.at.saturating_sub(1))
                .map_or(start, |t| t.span.end);
            (Some(filter), self.text[start..end].to_owned())
        } else {
            (None, String::new())
        };
        Ok(Statement::Delete {
            table,
            filter,
            filter_text,
        })
    }

    /// `column = value`, where the value is a literal, so `AND` ends it.
    fn assignment(&mut self) -> Result<(String, Expr), Diagnostic> {
        let (column, _) = self.identifier()?;
        self.expect_symbol("=")?;
        Ok((column, self.additive()?))
    }

    /// An optional `WHERE column = literal [AND ...]`.
    fn equalities(&mut self) -> Result<Vec<(String, Expr)>, Diagnostic> {
        let mut filter = Vec::new();
        if self.eat_keyword("where") {
            filter.push(self.assignment()?);
            while self.eat_keyword("and") {
                filter.push(self.assignment()?);
            }
        }
        Ok(filter)
    }

    fn query(&mut self) -> Result<Query, Diagnostic> {
        self.expect_keyword("select")?;
        let mut items = vec![self.select_item()?];
        while self.eat_symbol(",") {
            items.push(self.select_item()?);
        }
        let from = if self.eat_keyword("from") {
            Some(self.table_ref(true)?)
        } else {
            None
        };
        let mut joins = Vec::new();
        while let Some((kind, asof)) = self.join_kind()? {
            let table = self.table_ref(true)?;
            self.expect_keyword("on")?;
            let on = self.expr()?;
            joins.push(Join {
                kind,
                asof,
                table,
                on,
            });
        }
        let filter = if self.eat_keyword("where") {
            Some(self.expr()?)
        } else {
            None
        };
        let mut group_by = Vec::new();
        if self.eat_keyword("group") {
            self.expect_keyword("by")?;
            group_by.push(self.expr()?);
            while self.eat_symbol(",") {
                group_by.push(self.expr()?);
            }
        }
        let having = if self.eat_keyword("having") {
            Some(self.expr()?)
        } else {
            None
        };
        let order_by = self.order_by()?;
        let mut limit = None;
        let mut offset = None;
        loop {
            if self.eat_keyword("limit") {
                limit = Some(self.count()?);
            } else if self.eat_keyword("offset") {
                offset = Some(self.count()?);
            } else {
                break;
            }
        }
        Ok(Query {
            items,
            from,
            joins,
            filter,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    /// `ORDER BY item [ASC|DESC], ...`, if it starts here.
    fn order_by(&mut self) -> Result<Vec<OrderItem>, Diagnostic> {
        let mut order_by = Vec::new();
        if self.eat_keyword("order") {
            self.expect_keyword("by")?;
            loop {
                let expr = self.expr()?;
                let descending = if self.eat_keyword("desc") {
                    true
                } else {
                    let _ = self.eat_keyword("asc");
                    false
                };
                order_by.push(OrderItem { expr, descending });
                if !self.eat_symbol(",") {
                    break;
                }
            }
        }
        Ok(order_by)
    }

    /// A row count of `LIMIT` or `OFFSET`: a whole number or `$n`.
    fn count(&mut self) -> Result<Count, Diagnostic> {
        match self.advance() {
            Some(Spanned {
                token: Token::Number(n),
                span,
            }) => n
                .parse()
                .map(Count::Written)
                .map_err(|_| Diagnostic::at(span, format!("expected a whole number, found {n}"))),
            Some(Spanned {
                token: Token::Parameter(n),
                span,
            }) => Ok(Count::Parameter { n, span }),
            Some(other) => Err(Diagnostic::at(
                other.span,
                format!("expected a number, found {}", describe(&other.token)),
            )),
            None => Err(self.unexpected("expected a number")),
        }
    }

    fn select_item(&mut self) -> Result<SelectItem, Diagnostic> {
        if self.eat_symbol("*") {
            return Ok(SelectItem::Wildcard);
        }
        let expr = self.expr()?;
        let alias = self.alias(&CLAUSE_KEYWORDS)?;
        Ok(SelectItem::Expr { expr, alias })
    }

    /// `AS name`, or a bare name that is not one of `keywords`, which
    /// end the item an alias would follow.
    fn alias(&mut self, keywords: &[&str]) -> Result<Option<String>, Diagnostic> {
        if self.eat_keyword("as") {
            return Ok(Some(self.identifier()?.0));
        }
        match self.peek() {
            Some(Spanned {
                token: Token::Ident { name, quoted },
                ..
            }) if *quoted || !keywords.contains(&name.as_str()) => Ok(Some(self.identifier()?.0)),
            _ => Ok(None),
        }
    }

    /// `JOIN`, `INNER JOIN`, `LEFT JOIN` or `LEFT OUTER JOIN`, each
    /// after `ASOF` or not, if one starts here: the kind and whether it
    /// is an `ASOF JOIN`.
    fn join_kind(&mut self) -> Result<Option<(JoinKind, bool)>, Diagnostic> {
        let asof = self.eat_keyword("asof");
        let kind = if self.eat_keyword("join") {
            Some(JoinKind::Inner)
        } else if self.eat_keyword("inner") {
            self.expect_keyword("join")?;
            Some(JoinKind::Inner)
        } else if self.eat_keyword("left") {
            let _ = self.eat_keyword("outer");
            self.expect_keyword("join")?;
            Some(JoinKind::Left)
        } else {
            None
        };
        if asof && kind.is_none() {
            self.expect_keyword("join")?;
        }
        Ok(kind.map(|kind| (kind, asof)))
    }

    /// `[[tenant.]dataset.]table [[AS] alias]`, or where `functions`
    /// may appear, a table function `name(args) [[AS] alias]` (QL-3) or
    /// a derived table `(SELECT ...) [[AS] alias]`.
    fn table_ref(&mut self, functions: bool) -> Result<TableRef, Diagnostic> {
        if functions && self.is_symbol("(") {
            let start = self.expect_symbol("(")?;
            let query = self.query()?;
            let end = self.expect_symbol(")")?;
            let alias = self.alias(&table_keywords())?;
            return Ok(TableRef {
                tenant: None,
                dataset: None,
                table: "subquery".into(),
                alias,
                args: None,
                subquery: Some(Box::new(query)),
                span: Span::new(start.start, end.end),
            });
        }
        let (first, start) = self.identifier()?;
        let mut parts = vec![first];
        let mut end = start;
        while parts.len() < 3 && self.eat_symbol(".") {
            let (part, span) = self.identifier()?;
            parts.push(part);
            end = span;
        }
        let mut args = None;
        if functions && parts.len() == 1 && self.eat_symbol("(") {
            let mut list = Vec::new();
            if !self.eat_symbol(")") {
                loop {
                    list.push(self.expr()?);
                    if !self.eat_symbol(",") {
                        end = self.expect_symbol(")")?;
                        break;
                    }
                }
            }
            args = Some(list);
        }
        let span = Span::new(start.start, end.end);
        let table = parts.pop().unwrap_or_default();
        let dataset = parts.pop();
        let tenant = parts.pop();
        let alias = self.alias(&table_keywords())?;
        Ok(TableRef {
            tenant,
            dataset,
            table,
            alias,
            args,
            subquery: None,
            span,
        })
    }

    fn expr(&mut self) -> Result<Expr, Diagnostic> {
        self.or_expr()
    }
}

/// The words that end a table reference rather than alias it.
fn table_keywords() -> Vec<&'static str> {
    CLAUSE_KEYWORDS
        .iter()
        .chain(ALERT_KEYWORDS.iter())
        .chain(TABLE_KEYWORDS.iter())
        .copied()
        .collect()
}

impl Parser<'_> {
    fn or_expr(&mut self) -> Result<Expr, Diagnostic> {
        let mut left = self.and_expr()?;
        while self.eat_keyword("or") {
            let right = self.and_expr()?;
            left = binary(BinaryOp::Or, left, right);
        }
        Ok(left)
    }

    fn and_expr(&mut self) -> Result<Expr, Diagnostic> {
        let mut left = self.not_expr()?;
        while self.eat_keyword("and") {
            let right = self.not_expr()?;
            left = binary(BinaryOp::And, left, right);
        }
        Ok(left)
    }

    fn not_expr(&mut self) -> Result<Expr, Diagnostic> {
        let start = self.span_here();
        if self.eat_keyword("not") {
            let inner = self.not_expr()?;
            let span = Span::new(start.start, inner.span.end);
            let kind = match inner.kind {
                ExprKind::Exists { query, negated } => ExprKind::Exists {
                    query,
                    negated: !negated,
                },
                other => ExprKind::Not(Box::new(Expr {
                    kind: other,
                    span: inner.span,
                })),
            };
            return Ok(Expr { kind, span });
        }
        self.comparison()
    }

    fn comparison(&mut self) -> Result<Expr, Diagnostic> {
        let left = self.additive()?;
        let negated = self.eat_keyword("not");
        if self.eat_keyword("like") || self.is_keyword("ilike") {
            let case_insensitive = self.eat_keyword("ilike");
            let (pattern, end) = self.string()?;
            let span = Span::new(left.span.start, end.end);
            return Ok(Expr {
                kind: ExprKind::Like {
                    expr: Box::new(left),
                    pattern,
                    negated,
                    case_insensitive,
                },
                span,
            });
        }
        if self.eat_keyword("in") {
            self.expect_symbol("(")?;
            if self.is_keyword("select") {
                let query = self.query()?;
                let end = self.expect_symbol(")")?;
                let span = Span::new(left.span.start, end.end);
                return Ok(Expr {
                    kind: ExprKind::InSubquery {
                        expr: Box::new(left),
                        query: Box::new(query),
                        negated,
                    },
                    span,
                });
            }
            let mut list = vec![self.expr()?];
            while self.eat_symbol(",") {
                list.push(self.expr()?);
            }
            let end = self.expect_symbol(")")?;
            let span = Span::new(left.span.start, end.end);
            return Ok(Expr {
                kind: ExprKind::InList {
                    expr: Box::new(left),
                    list,
                    negated,
                },
                span,
            });
        }
        if self.eat_keyword("between") {
            let low = self.additive()?;
            self.expect_keyword("and")?;
            let high = self.additive()?;
            let span = Span::new(left.span.start, high.span.end);
            return Ok(Expr {
                kind: ExprKind::Between {
                    expr: Box::new(left),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated,
                },
                span,
            });
        }
        if negated {
            return Err(self.unexpected("expected LIKE, ILIKE, IN or BETWEEN after NOT"));
        }
        if self.eat_keyword("is") {
            let negated = self.eat_keyword("not");
            let end = self.span_here();
            self.expect_keyword("null")?;
            let span = Span::new(left.span.start, end.end);
            return Ok(Expr {
                kind: ExprKind::IsNull {
                    expr: Box::new(left),
                    negated,
                },
                span,
            });
        }
        let op = match self.peek() {
            Some(Spanned {
                token: Token::Symbol(s),
                ..
            }) => match *s {
                "=" => BinaryOp::Eq,
                "<>" | "!=" => BinaryOp::NotEq,
                "<" => BinaryOp::Lt,
                "<=" => BinaryOp::LtEq,
                ">" => BinaryOp::Gt,
                ">=" => BinaryOp::GtEq,
                _ => return Ok(left),
            },
            _ => return Ok(left),
        };
        self.at += 1;
        let right = self.additive()?;
        Ok(binary(op, left, right))
    }

    fn additive(&mut self) -> Result<Expr, Diagnostic> {
        let mut left = self.multiplicative()?;
        loop {
            let op = if self.eat_symbol("+") {
                BinaryOp::Add
            } else if self.eat_symbol("-") {
                BinaryOp::Sub
            } else {
                return Ok(left);
            };
            let right = self.multiplicative()?;
            left = binary(op, left, right);
        }
    }

    fn multiplicative(&mut self) -> Result<Expr, Diagnostic> {
        let mut left = self.unary()?;
        loop {
            let op = if self.eat_symbol("*") {
                BinaryOp::Mul
            } else if self.eat_symbol("/") {
                BinaryOp::Div
            } else if self.eat_symbol("%") {
                BinaryOp::Rem
            } else {
                return Ok(left);
            };
            let right = self.unary()?;
            left = binary(op, left, right);
        }
    }

    fn unary(&mut self) -> Result<Expr, Diagnostic> {
        let start = self.span_here();
        if self.eat_symbol("-") {
            let inner = self.unary()?;
            let span = Span::new(start.start, inner.span.end);
            return Ok(Expr {
                kind: ExprKind::Negate(Box::new(inner)),
                span,
            });
        }
        let _ = self.eat_symbol("+");
        self.postfix()
    }

    fn postfix(&mut self) -> Result<Expr, Diagnostic> {
        let mut expr = self.primary()?;
        while self.eat_symbol("::") {
            let (type_name, end) = self.identifier()?;
            let span = Span::new(expr.span.start, end.end);
            expr = Expr {
                kind: ExprKind::Cast {
                    expr: Box::new(expr),
                    type_name,
                },
                span,
            };
        }
        Ok(expr)
    }

    fn string(&mut self) -> Result<(String, Span), Diagnostic> {
        match self.advance() {
            Some(Spanned {
                token: Token::String(s),
                span,
            }) => Ok((s, span)),
            Some(other) => Err(Diagnostic::at(
                other.span,
                format!("expected a string, found {}", describe(&other.token)),
            )),
            None => Err(self.unexpected("expected a string")),
        }
    }

    /// After `(` at `start`: a scalar subquery, or an expression in
    /// parentheses.
    fn parenthesised(&mut self, start: Span) -> Result<Expr, Diagnostic> {
        if self.is_keyword("select") {
            let query = self.query()?;
            let end = self.expect_symbol(")")?;
            return Ok(Expr {
                kind: ExprKind::Subquery(Box::new(query)),
                span: Span::new(start.start, end.end),
            });
        }
        let inner = self.expr()?;
        let end = self.expect_symbol(")")?;
        Ok(Expr {
            kind: inner.kind,
            span: Span::new(start.start, end.end),
        })
    }

    /// `OVER ([PARTITION BY exprs] [ORDER BY items])` after the call
    /// of `function` on `args`, which it makes a window function.
    fn over(
        &mut self,
        function: String,
        args: Vec<Expr>,
        wildcard: bool,
        start: Span,
    ) -> Result<Expr, Diagnostic> {
        self.expect_keyword("over")?;
        self.expect_symbol("(")?;
        let mut partition_by = Vec::new();
        if self.eat_keyword("partition") {
            self.expect_keyword("by")?;
            partition_by.push(self.expr()?);
            while self.eat_symbol(",") {
                partition_by.push(self.expr()?);
            }
        }
        let order_by = self.order_by()?;
        let end = self.expect_symbol(")")?;
        Ok(Expr {
            kind: ExprKind::Window {
                function,
                args,
                wildcard,
                partition_by,
                order_by,
            },
            span: Span::new(start.start, end.end),
        })
    }

    /// `EXISTS (SELECT ...)`, after the keyword at `start`.
    fn exists(&mut self, start: Span) -> Result<Expr, Diagnostic> {
        self.expect_symbol("(")?;
        let query = self.query()?;
        let end = self.expect_symbol(")")?;
        Ok(Expr {
            kind: ExprKind::Exists {
                query: Box::new(query),
                negated: false,
            },
            span: Span::new(start.start, end.end),
        })
    }

    fn primary(&mut self) -> Result<Expr, Diagnostic> {
        let Some(next) = self.advance() else {
            return Err(self.unexpected("expected an expression"));
        };
        let span = next.span;
        match next.token {
            Token::Number(n) => Ok(Expr {
                kind: ExprKind::Number(n),
                span,
            }),
            Token::String(s) => Ok(Expr {
                kind: ExprKind::String(s),
                span,
            }),
            Token::Parameter(n) => Ok(Expr {
                kind: ExprKind::Parameter(n),
                span,
            }),
            Token::Symbol("(") => self.parenthesised(span),
            Token::Symbol(s) => Err(Diagnostic::at(
                span,
                format!("expected an expression, found {s:?}"),
            )),
            Token::Ident {
                name,
                quoted: false,
            } if RESERVED.contains(&name.as_str()) => Err(Diagnostic::at(
                span,
                format!("expected an expression, found {name:?}"),
            )),
            Token::Ident {
                name,
                quoted: false,
            } => match name.as_str() {
                "true" => Ok(Expr {
                    kind: ExprKind::Bool(true),
                    span,
                }),
                "false" => Ok(Expr {
                    kind: ExprKind::Bool(false),
                    span,
                }),
                "null" => Ok(Expr {
                    kind: ExprKind::Null,
                    span,
                }),
                "timestamp"
                    if matches!(
                        self.peek(),
                        Some(Spanned {
                            token: Token::String(_),
                            ..
                        })
                    ) =>
                {
                    let (text, end) = self.string()?;
                    Ok(Expr {
                        kind: ExprKind::Timestamp(text),
                        span: Span::new(span.start, end.end),
                    })
                }
                "interval" => {
                    let (text, end) = self.string()?;
                    Ok(Expr {
                        kind: ExprKind::Interval(text),
                        span: Span::new(span.start, end.end),
                    })
                }
                "exists" => self.exists(span),
                "cast" => {
                    self.expect_symbol("(")?;
                    let inner = self.expr()?;
                    self.expect_keyword("as")?;
                    let (type_name, _) = self.identifier()?;
                    let end = self.expect_symbol(")")?;
                    Ok(Expr {
                        kind: ExprKind::Cast {
                            expr: Box::new(inner),
                            type_name,
                        },
                        span: Span::new(span.start, end.end),
                    })
                }
                _ => self.name_or_call(name, span),
            },
            Token::Ident { name, quoted: true } => self.name_or_call(name, span),
        }
    }

    fn name_or_call(&mut self, first: String, start: Span) -> Result<Expr, Diagnostic> {
        if self.eat_symbol("(") {
            let wildcard = self.eat_symbol("*");
            let mut args = Vec::new();
            if !wildcard && !self.is_symbol(")") {
                args.push(self.expr()?);
                while self.eat_symbol(",") {
                    args.push(self.expr()?);
                }
            }
            let end = self.expect_symbol(")")?;
            if self.is_keyword("over") {
                return self.over(first, args, wildcard, start);
            }
            return Ok(Expr {
                kind: ExprKind::Call {
                    name: first,
                    args,
                    wildcard,
                },
                span: Span::new(start.start, end.end),
            });
        }
        let mut parts = vec![first];
        let mut end = start;
        while self.eat_symbol(".") {
            let (part, span) = self.identifier()?;
            parts.push(part);
            end = span;
        }
        Ok(Expr {
            kind: ExprKind::Name(parts),
            span: Span::new(start.start, end.end),
        })
    }
}

/// Keywords that cannot start an expression.
const RESERVED: [&str; 22] = [
    "select", "from", "where", "group", "by", "order", "limit", "offset", "and", "or", "not", "as",
    "in", "is", "like", "ilike", "between", "asc", "desc", "having", "explain", "analyze",
];

/// Keywords of `CREATE ALERT` and `CREATE ROLLUP` options and retention
/// clauses, which end a duration's unit.
const ALERT_KEYWORDS: [&str; 13] = [
    "backfill",
    "except",
    "severity",
    "for",
    "every",
    "into",
    "notify",
    "labels",
    "annotations",
    "when",
    "else",
    "then",
    "after",
];

/// Keywords that end a select item, so a bare name before them is not an
/// alias.
const CLAUSE_KEYWORDS: [&str; 17] = [
    "from", "where", "group", "order", "limit", "offset", "and", "or", "as", "having", "join",
    "inner", "left", "outer", "on", "asof", "over",
];

/// Keywords that may follow a table in the statements that name one,
/// so a bare name before them is not an alias.
const TABLE_KEYWORDS: [&str; 2] = ["set", "values"];

fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    let span = Span::new(left.span.start, right.span.end);
    Expr {
        kind: ExprKind::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        },
        span,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(text: &str) -> Query {
        match parse(text).unwrap() {
            Statement::Select(q) => q,
            other => panic!("not a select: {other:?}"),
        }
    }

    #[test]
    fn create_alert_takes_its_options_in_any_order_and_keeps_the_query_text() {
        let Statement::CreateAlert(alert) = parse(
            "CREATE ALERT checkout_errors FOR 2m SEVERITY critical EVERY '30 seconds' \
             NOTIFY 'oncall-payments', ops LABELS (team = 'payments') \
             ANNOTATIONS (summary = 'too many errors') \
             AS SELECT service, count(*) AS errors FROM logs GROUP BY service HAVING count(*) > 100;",
        )
        .unwrap() else {
            panic!("not a create alert");
        };
        assert_eq!(alert.name, "checkout_errors");
        assert_eq!(alert.severity.as_deref(), Some("critical"));
        assert_eq!(alert.hold.as_ref().map(|h| h.0.as_str()), Some("2m"));
        assert_eq!(
            alert.every.as_ref().map(|e| e.0.as_str()),
            Some("30 seconds")
        );
        assert_eq!(alert.notify, ["oncall-payments", "ops"]);
        assert_eq!(alert.labels, [("team".to_owned(), "payments".to_owned())]);
        assert_eq!(alert.annotations[0].1, "too many errors");
        assert_eq!(
            alert.query_text,
            "SELECT service, count(*) AS errors FROM logs GROUP BY service HAVING count(*) > 100"
        );
        assert!(alert.query.having.is_some());
        assert!(parse("CREATE ALERT x AS SELECT 1").is_ok());
        assert!(parse("CREATE ALERT x FOR soon AS SELECT 1").is_err());
        assert!(parse("CREATE ALERT x").is_err(), "AS is required");
    }

    #[test]
    fn create_rollup_takes_every_and_into_and_keeps_the_query_text() {
        let Statement::CreateRollup(rollup) = parse(
            "CREATE ROLLUP errors_by_service EVERY 5m INTO errors_1m BACKFILL 7d AS \
             SELECT attr.service AS service, count(*) AS n FROM logs \
             GROUP BY attr.service, tumble(timestamp, INTERVAL '1 minute')",
        )
        .unwrap() else {
            panic!("not a create rollup");
        };
        assert_eq!(rollup.name, "errors_by_service");
        assert_eq!(rollup.every.as_ref().map(|e| e.0.as_str()), Some("5m"));
        assert_eq!(rollup.target.as_deref(), Some("errors_1m"));
        assert_eq!(rollup.backfill.as_ref().map(|b| b.0.as_str()), Some("7d"));
        assert!(rollup.query_text.starts_with("SELECT attr.service"));
        assert!(rollup.query_text.ends_with("INTERVAL '1 minute')"));
        assert!(parse("CREATE ROLLUP x AS SELECT 1").is_ok());
        assert!(parse("CREATE ROLLUP x").is_err(), "AS is required");
        assert_eq!(
            parse("DROP ROLLUP x").unwrap(),
            Statement::DropRollup { name: "x".into() }
        );
    }

    #[test]
    fn drop_explain_insert_update_and_delete_parse() {
        assert_eq!(
            parse("DROP ALERT x").unwrap(),
            Statement::DropAlert { name: "x".into() }
        );
        assert_eq!(
            parse("EXPLAIN ALERT x").unwrap(),
            Statement::ExplainAlert { name: "x".into() }
        );
        assert_eq!(
            parse("EXPLAIN LAYOUT prod").unwrap(),
            Statement::ExplainLayout {
                dataset: Some("prod".into())
            }
        );
        assert_eq!(
            parse("EXPLAIN LAYOUT").unwrap(),
            Statement::ExplainLayout { dataset: None }
        );
        let Statement::Insert {
            table,
            columns,
            rows,
        } = parse("INSERT INTO system.silences (id, comment) VALUES ('s1', 'x'), ('s2', 'y')")
            .unwrap()
        else {
            panic!("not an insert");
        };
        assert_eq!(
            (table.dataset.as_deref(), table.table.as_str()),
            (Some("system"), "silences")
        );
        assert_eq!(columns, ["id", "comment"]);
        assert_eq!(rows.len(), 2);
        assert!(
            parse("INSERT INTO system.silences (id, comment) VALUES ('s1')").is_err(),
            "arity"
        );
        let Statement::Update { set, filter, .. } =
            parse("UPDATE system.alert_rules SET severity = 'warning', every_seconds = 30 WHERE name = 'x' AND dataset = 'prod'")
                .unwrap()
        else {
            panic!("not an update");
        };
        assert_eq!(set.len(), 2);
        assert_eq!(filter.len(), 2);
        let Statement::Delete { filter, .. } = parse("DELETE FROM system.silences").unwrap() else {
            panic!("not a delete");
        };
        assert!(filter.is_none());
        let Statement::Delete {
            filter,
            filter_text,
            ..
        } = parse("DELETE FROM logs WHERE attr.user = 'u1' AND severity_number < 9").unwrap()
        else {
            panic!("not a delete");
        };
        assert!(filter.is_some());
        assert_eq!(filter_text, "attr.user = 'u1' AND severity_number < 9");
    }

    #[test]
    fn parses_three_part_names() {
        let q = query("SELECT * FROM acme.prod.logs l WHERE true");
        let from = q.from.unwrap();
        assert_eq!(from.tenant.as_deref(), Some("acme"));
        assert_eq!(from.dataset.as_deref(), Some("prod"));
        assert_eq!(from.table, "logs");
        assert_eq!(from.alias.as_deref(), Some("l"));
        let from = query("SELECT * FROM prod.logs").from.unwrap();
        assert_eq!((from.tenant, from.dataset.as_deref()), (None, Some("prod")));
    }

    /// Section 9.2: `ASOF` before any join kind marks the join, and is
    /// not an alias of the table before it.
    #[test]
    fn parses_asof_joins() {
        let q = query(
            "SELECT * FROM logs ASOF JOIN spans s ON logs.trace_id = s.trace_id \
             AND logs.timestamp >= s.timestamp",
        );
        assert_eq!(q.from.unwrap().alias, None);
        assert!(q.joins[0].asof);
        assert_eq!(q.joins[0].kind, JoinKind::Inner);
        let left = query(
            "SELECT * FROM logs l ASOF LEFT OUTER JOIN spans s ON l.trace_id = s.trace_id \
             AND l.timestamp >= s.timestamp",
        );
        assert!(left.joins[0].asof);
        assert_eq!(left.joins[0].kind, JoinKind::Left);
        let error = parse("SELECT * FROM logs ASOF spans ON true").unwrap_err();
        assert!(error.message.contains("JOIN"), "{}", error.message);
    }

    #[test]
    fn parses_joins_with_aliases() {
        let q = query(
            "SELECT a.name, k.by FROM alerts AS a LEFT OUTER JOIN system.acknowledgments k \
             ON a.attr.alert.fingerprint = k.fingerprint AND a.name = k.comment WHERE k.by = 'ann'",
        );
        let from = q.from.unwrap();
        assert_eq!(from.table, "alerts");
        assert_eq!(from.alias.as_deref(), Some("a"));
        assert_eq!(q.joins.len(), 1);
        assert_eq!(q.joins[0].kind, JoinKind::Left);
        assert_eq!(q.joins[0].table.dataset.as_deref(), Some("system"));
        assert_eq!(q.joins[0].table.alias.as_deref(), Some("k"));
        assert!(matches!(
            &q.joins[0].on.kind,
            ExprKind::Binary {
                op: BinaryOp::And,
                ..
            }
        ));
        assert!(q.filter.is_some());
        let inner = query("SELECT * FROM logs l INNER JOIN spans ON l.trace_id = spans.trace_id");
        assert_eq!(inner.joins[0].kind, JoinKind::Inner);
        assert!(!inner.joins[0].asof);
        assert_eq!(inner.joins[0].table.alias, None);
        let plain = query("SELECT * FROM logs WHERE true");
        assert_eq!(
            plain.from.unwrap().alias,
            None,
            "a clause keyword is not an alias"
        );
    }

    #[test]
    fn parses_every_clause() {
        let q = query(
            "SELECT body AS text, count(*) n FROM prod.logs WHERE severity_number >= 17 \
             GROUP BY body ORDER BY n DESC, body LIMIT 10 OFFSET 5;",
        );
        assert_eq!(q.items.len(), 2);
        assert!(matches!(&q.items[0], SelectItem::Expr { alias: Some(a), .. } if a == "text"));
        assert!(matches!(&q.items[1], SelectItem::Expr { alias: Some(a), .. } if a == "n"));
        let from = q.from.unwrap();
        assert_eq!(from.dataset.as_deref(), Some("prod"));
        assert_eq!(from.table, "logs");
        assert!(q.filter.is_some());
        assert_eq!(q.group_by.len(), 1);
        assert_eq!(q.order_by.len(), 2);
        assert!(q.order_by[0].descending);
        assert!(!q.order_by[1].descending);
        assert_eq!(q.limit, Some(Count::Written(10)));
        assert_eq!(q.offset, Some(Count::Written(5)));
        let bound = query("SELECT body FROM logs LIMIT $1 OFFSET $2");
        assert!(matches!(bound.limit, Some(Count::Parameter { n: 1, .. })));
        assert!(matches!(bound.offset, Some(Count::Parameter { n: 2, .. })));
        assert!(parse("SELECT body FROM logs LIMIT 'ten'").is_err());
    }

    #[test]
    fn respects_precedence() {
        let q = query("SELECT * FROM logs WHERE a = 1 OR b = 2 AND NOT c < 3 + 4 * 5");
        let ExprKind::Binary {
            op: BinaryOp::Or,
            right,
            ..
        } = q.filter.unwrap().kind
        else {
            panic!("expected OR at the top");
        };
        let ExprKind::Binary {
            op: BinaryOp::And,
            right,
            ..
        } = right.kind
        else {
            panic!("expected AND under OR");
        };
        let ExprKind::Not(inner) = right.kind else {
            panic!("expected NOT");
        };
        let ExprKind::Binary {
            op: BinaryOp::Lt,
            right,
            ..
        } = inner.kind
        else {
            panic!("expected <");
        };
        let ExprKind::Binary {
            op: BinaryOp::Add,
            right,
            ..
        } = right.kind
        else {
            panic!("expected + under <");
        };
        assert!(matches!(
            right.kind,
            ExprKind::Binary {
                op: BinaryOp::Mul,
                ..
            }
        ));
    }

    #[test]
    fn parses_predicates_literals_and_casts() {
        let q = query(
            "SELECT attr.http.method, \"attr.x\"::int, CAST(n AS float), TIMESTAMP '2024-01-01', \
             INTERVAL '5 minutes', -1, now() FROM logs \
             WHERE body NOT LIKE '%x%' AND b ILIKE 'y' AND n IN (1, 2) AND t IS NOT NULL \
             AND n BETWEEN 1 AND 2 AND z NOT IN ('a')",
        );
        let kinds: Vec<&ExprKind> = q
            .items
            .iter()
            .map(|i| match i {
                SelectItem::Expr { expr, .. } => &expr.kind,
                SelectItem::Wildcard => panic!(),
            })
            .collect();
        assert_eq!(
            kinds[0],
            &ExprKind::Name(vec!["attr".into(), "http".into(), "method".into()])
        );
        assert!(matches!(kinds[1], ExprKind::Cast { type_name, .. } if type_name == "int"));
        assert!(matches!(kinds[2], ExprKind::Cast { type_name, .. } if type_name == "float"));
        assert_eq!(kinds[3], &ExprKind::Timestamp("2024-01-01".into()));
        assert_eq!(kinds[4], &ExprKind::Interval("5 minutes".into()));
        assert!(matches!(kinds[5], ExprKind::Negate(_)));
        assert!(
            matches!(kinds[6], ExprKind::Call { name, args, wildcard: false } if name == "now" && args.is_empty())
        );
        let mut conjuncts = Vec::new();
        let mut cursor = q.filter.unwrap();
        loop {
            match cursor.kind {
                ExprKind::Binary {
                    op: BinaryOp::And,
                    left,
                    right,
                } => {
                    conjuncts.push(right.kind);
                    cursor = *left;
                }
                other => {
                    conjuncts.push(other);
                    break;
                }
            }
        }
        conjuncts.reverse();
        assert!(matches!(
            &conjuncts[0],
            ExprKind::Like {
                negated: true,
                case_insensitive: false,
                ..
            }
        ));
        assert!(matches!(
            &conjuncts[1],
            ExprKind::Like {
                negated: false,
                case_insensitive: true,
                ..
            }
        ));
        assert!(
            matches!(&conjuncts[2], ExprKind::InList { negated: false, list, .. } if list.len() == 2)
        );
        assert!(matches!(
            &conjuncts[3],
            ExprKind::IsNull { negated: true, .. }
        ));
        assert!(matches!(
            &conjuncts[4],
            ExprKind::Between { negated: false, .. }
        ));
        assert!(matches!(
            &conjuncts[5],
            ExprKind::InList { negated: true, .. }
        ));
    }

    #[test]
    fn explain_wraps_a_query() {
        assert!(matches!(
            parse("EXPLAIN ANALYZE SELECT 1").unwrap(),
            Statement::Explain { analyze: true, .. }
        ));
        assert!(matches!(
            parse("explain select 1").unwrap(),
            Statement::Explain { analyze: false, .. }
        ));
    }

    #[test]
    fn errors_name_what_was_expected_and_where() {
        let error = parse("SELECT FROM logs").unwrap_err();
        assert_eq!(error.message, "expected an expression, found \"from\"");
        assert_eq!(error.span, Some(Span::new(7, 11)));
        let error = parse("SELECT * FROM logs WHERE").unwrap_err();
        assert_eq!(error.message, "expected an expression, found end of input");
        let error = parse("SELECT * FROM logs LIMIT x").unwrap_err();
        assert!(error.message.starts_with("expected a number"));
        let error = parse("SELECT * FROM logs l extra").unwrap_err();
        assert_eq!(error.message, "unexpected \"extra\" after the statement");
        let error = parse("SELECT (1 FROM logs").unwrap_err();
        assert!(error.message.starts_with("expected \")\""));
    }

    #[test]
    fn parameters_are_expressions() {
        let q = query("SELECT body FROM logs WHERE body = $1 AND n IN ($2, 3)");
        let filter = q.filter.unwrap();
        let ExprKind::Binary { left, right, .. } = &filter.kind else {
            panic!("{filter:?}")
        };
        assert!(matches!(
            &left.kind,
            ExprKind::Binary { right, .. } if right.kind == ExprKind::Parameter(1)
        ));
        assert!(matches!(
            &right.kind,
            ExprKind::InList { list, .. } if list[0].kind == ExprKind::Parameter(2)
        ));
        assert!(parse("SELECT $ FROM logs").is_err());
    }

    /// QE-20: `OVER` after a call makes it a window function, with its
    /// partition and order, either empty; `OVER` is not an alias.
    #[test]
    fn parses_window_functions() {
        let q = query(
            "SELECT lag(body, 1) OVER (PARTITION BY severity_text, attr.service \
             ORDER BY timestamp DESC) AS prev, count(*) OVER () FROM logs",
        );
        let SelectItem::Expr { expr, alias } = &q.items[0] else {
            panic!("not an item");
        };
        assert_eq!(alias.as_deref(), Some("prev"));
        let ExprKind::Window {
            function,
            args,
            wildcard,
            partition_by,
            order_by,
        } = &expr.kind
        else {
            panic!("not a window: {expr:?}");
        };
        assert_eq!(function, "lag");
        assert_eq!(args.len(), 2);
        assert!(!wildcard);
        assert_eq!(partition_by.len(), 2);
        assert_eq!(order_by.len(), 1);
        assert!(order_by[0].descending);
        let SelectItem::Expr { expr, alias: None } = &q.items[1] else {
            panic!("not an unaliased item");
        };
        assert!(matches!(
            &expr.kind,
            ExprKind::Window {
                wildcard: true,
                partition_by,
                order_by,
                ..
            } if partition_by.is_empty() && order_by.is_empty()
        ));
        assert!(parse("SELECT count(*) OVER FROM logs").is_err());
    }

    /// `(SELECT ...)` in an expression is a scalar subquery.
    #[test]
    fn a_parenthesised_select_is_a_scalar_subquery() {
        let q = query(
            "SELECT body FROM logs WHERE severity_number > (SELECT avg(severity_number) FROM logs)",
        );
        let ExprKind::Binary { right, .. } = &q.filter.unwrap().kind else {
            panic!("not a comparison");
        };
        let ExprKind::Subquery(inner) = &right.kind else {
            panic!("not a subquery: {right:?}");
        };
        assert_eq!(inner.from.as_ref().unwrap().table, "logs");
        let q = query("SELECT (SELECT count(*) FROM spans) AS spans, (1 + 2) AS three FROM logs");
        assert!(
            matches!(&q.items[0], SelectItem::Expr { expr, .. } if matches!(expr.kind, ExprKind::Subquery(_)))
        );
        assert!(
            matches!(&q.items[1], SelectItem::Expr { expr, .. } if matches!(expr.kind, ExprKind::Binary { .. }))
        );
    }

    /// `EXISTS (SELECT ...)` is an expression, negated by `NOT` in front
    /// of it rather than wrapped.
    #[test]
    fn exists_takes_a_subquery() {
        let q = query(
            "SELECT body FROM logs l WHERE EXISTS (SELECT 1 FROM spans s WHERE s.trace_id = l.trace_id)",
        );
        let ExprKind::Exists {
            query: subquery,
            negated,
        } = &q.filter.unwrap().kind
        else {
            panic!("not an EXISTS");
        };
        assert!(!negated);
        assert_eq!(subquery.from.as_ref().unwrap().table, "spans");
        let q = query("SELECT body FROM logs WHERE NOT EXISTS (SELECT 1 FROM spans) AND true");
        let ExprKind::Binary { left, .. } = &q.filter.unwrap().kind else {
            panic!("not a conjunction");
        };
        assert!(matches!(left.kind, ExprKind::Exists { negated: true, .. }));
        assert!(parse("SELECT body FROM logs WHERE EXISTS spans").is_err());
    }

    #[test]
    fn in_takes_a_subquery() {
        let q = query(
            "SELECT body FROM logs WHERE trace_id NOT IN (SELECT trace_id FROM spans WHERE \
             status_code = 'error') AND n IN (1)",
        );
        let filter = q.filter.unwrap();
        let ExprKind::Binary { left, .. } = &filter.kind else {
            panic!("{filter:?}")
        };
        let ExprKind::InSubquery {
            query,
            negated: true,
            ..
        } = &left.kind
        else {
            panic!("{left:?}")
        };
        assert_eq!(query.from.as_ref().unwrap().table, "spans");
        assert!(query.filter.is_some());
        assert!(parse("SELECT body FROM logs WHERE n IN (SELECT)").is_err());
    }
}
