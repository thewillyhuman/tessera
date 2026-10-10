// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! A recursive-descent parser for `TraceQL` (section 9.5).
//!
//! A query is a spanset expression, pipeline stages after `|`, and
//! optionally a metrics function last. Structural operators bind
//! tighter than `&&` and `||`, both left-associative, as in Tempo.
//! Inside braces, a condition has the usual precedence: `||`, `&&`,
//! comparisons, additive, multiplicative, `^`, then unary `-` and `!`.

use tessera_ir::{Diagnostic, Span};

use crate::ast::{FieldExpr, Metrics, Pipeline, Query, SpansetExpr, Stage, Static, StructuralOp};
use crate::lexer::{Spanned, Token, tokenize};

/// The aggregate functions of an aggregate filter.
pub const AGGREGATES: [&str; 5] = ["count", "avg", "min", "max", "sum"];

/// The metrics functions and whether they take a value.
pub const METRIC_FUNCTIONS: [(&str, bool); 8] = [
    ("rate", false),
    ("count_over_time", false),
    ("min_over_time", true),
    ("max_over_time", true),
    ("avg_over_time", true),
    ("sum_over_time", true),
    ("quantile_over_time", true),
    ("histogram_over_time", true),
];

/// The intrinsics that may be written without a scope.
pub const INTRINSICS: [&str; 12] = [
    "duration",
    "name",
    "status",
    "statusMessage",
    "kind",
    "rootName",
    "rootServiceName",
    "traceDuration",
    "nestedSetLeft",
    "nestedSetRight",
    "nestedSetParent",
    "childCount",
];

const STATUSES: [&str; 3] = ["ok", "error", "unset"];
const KINDS: [&str; 6] = [
    "server",
    "client",
    "producer",
    "consumer",
    "internal",
    "unspecified",
];

/// Parses one query.
///
/// # Errors
///
/// The first syntax error, with its position.
pub fn parse(text: &str) -> Result<Query, Diagnostic> {
    let tokens = tokenize(text)?;
    let mut parser = Parser {
        tokens,
        at: 0,
        end: text.len(),
    };
    let query = parser.query()?;
    if let Some(extra) = parser.peek() {
        return Err(Diagnostic::at(
            extra.span,
            format!("unexpected {} after the query", describe(&extra.token)),
        ));
    }
    Ok(query)
}

struct Parser {
    tokens: Vec<Spanned>,
    at: usize,
    end: usize,
}

fn describe(token: &Token) -> String {
    match token {
        Token::Ident(name) => format!("{name:?}"),
        Token::ScopedIntrinsic(scope, name) => format!("{scope}:{name}"),
        Token::Attribute(Some(scope), name) => format!("attribute {scope}.{name}"),
        Token::Attribute(None, name) => format!("attribute .{name}"),
        Token::Int(n) => format!("number {n}"),
        Token::Float(n) => format!("number {n}"),
        Token::String(s) => format!("string {s:?}"),
        Token::Duration(d) => format!("duration {}s", d.as_secs_f64()),
        Token::Symbol(s) => format!("{s:?}"),
    }
}

impl Parser {
    fn peek(&self) -> Option<&Spanned> {
        self.tokens.get(self.at)
    }

    fn span_here(&self) -> Span {
        self.peek()
            .map_or(Span::new(self.end, self.end), |t| t.span)
    }

    fn previous_end(&self) -> usize {
        self.tokens
            .get(self.at.wrapping_sub(1))
            .map_or(0, |t| t.span.end)
    }

    fn advance(&mut self) -> Option<Spanned> {
        let token = self.tokens.get(self.at).cloned();
        if token.is_some() {
            self.at += 1;
        }
        token
    }

    fn is_symbol(&self, symbol: &str) -> bool {
        matches!(self.peek(), Some(Spanned { token: Token::Symbol(s), .. }) if *s == symbol)
    }

    fn is_ident(&self, name: &str) -> bool {
        matches!(self.peek(), Some(Spanned { token: Token::Ident(n), .. }) if n == name)
    }

    fn peek_symbol(&self) -> Option<&'static str> {
        match self.peek() {
            Some(Spanned {
                token: Token::Symbol(s),
                ..
            }) => Some(s),
            _ => None,
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

    fn unexpected(&self, expected: &str) -> Diagnostic {
        match self.peek() {
            Some(token) => Diagnostic::at(
                token.span,
                format!("expected {expected}, found {}", describe(&token.token)),
            ),
            None => Diagnostic::at(
                Span::new(self.end, self.end),
                format!("expected {expected}, found the end of the query"),
            ),
        }
    }

    fn expect_symbol(&mut self, symbol: &str) -> Result<Span, Diagnostic> {
        if self.is_symbol(symbol) {
            let span = self.span_here();
            self.at += 1;
            Ok(span)
        } else {
            Err(self.unexpected(&format!("{symbol:?}")))
        }
    }

    fn query(&mut self) -> Result<Query, Diagnostic> {
        let first = self.spanset_expr(0)?;
        let mut stages = Vec::new();
        let mut metrics = None;
        while self.eat_symbol("|") {
            let is_metric = matches!(self.peek(), Some(Spanned { token: Token::Ident(n), .. })
                if METRIC_FUNCTIONS.iter().any(|(f, _)| f == n));
            if is_metric {
                metrics = Some(self.metrics()?);
                break;
            }
            stages.push(self.stage()?);
        }
        Ok(Query {
            pipeline: Pipeline { first, stages },
            metrics,
        })
    }

    /// `&&` and `||` at precedence 1, structural operators at 2.
    fn spanset_op(&self) -> Option<(&'static str, u8)> {
        let symbol = self.peek_symbol()?;
        match symbol {
            "&&" | "||" => Some((symbol, 1)),
            ">>" | ">" | "~" | "<<" | "<" | "!>>" | "!>" | "!~" | "!<<" | "!<" | "&>>" | "&>"
            | "&~" | "&<<" | "&<" => Some((symbol, 2)),
            _ => None,
        }
    }

    fn spanset_expr(&mut self, min_precedence: u8) -> Result<SpansetExpr, Diagnostic> {
        let mut left = self.spanset_atom()?;
        while let Some((symbol, precedence)) = self.spanset_op() {
            if precedence < min_precedence {
                break;
            }
            self.at += 1;
            let right = self.spanset_expr(precedence + 1)?;
            left = match symbol {
                "&&" => SpansetExpr::And(Box::new(left), Box::new(right)),
                "||" => SpansetExpr::Or(Box::new(left), Box::new(right)),
                structural => SpansetExpr::Structural {
                    left: Box::new(left),
                    op: structural_op(structural),
                    right: Box::new(right),
                },
            };
        }
        Ok(left)
    }

    fn spanset_atom(&mut self) -> Result<SpansetExpr, Diagnostic> {
        if self.eat_symbol("(") {
            let inner = self.spanset_expr(0)?;
            self.expect_symbol(")")?;
            return Ok(inner);
        }
        let open = self.expect_symbol("{")?;
        let condition = if self.is_symbol("}") {
            None
        } else {
            Some(self.field_expr(0)?)
        };
        let close = self.expect_symbol("}")?;
        Ok(SpansetExpr::Filter {
            condition,
            span: Span::new(open.start, close.end),
        })
    }

    fn stage(&mut self) -> Result<Stage, Diagnostic> {
        if self.is_symbol("{") || self.is_symbol("(") {
            return Ok(Stage::Spanset(self.spanset_expr(0)?));
        }
        let start = self.span_here();
        let Some(Spanned {
            token: Token::Ident(name),
            ..
        }) = self.advance()
        else {
            return Err(Diagnostic::at(
                start,
                "expected a pipeline stage: a spanset, count, avg, min, max, sum, select, by, coalesce or a metrics function",
            ));
        };
        self.expect_symbol("(")?;
        match name.as_str() {
            "select" => {
                let mut fields = vec![self.field_expr(0)?];
                while self.eat_symbol(",") {
                    fields.push(self.field_expr(0)?);
                }
                self.expect_symbol(")")?;
                Ok(Stage::Select(fields))
            }
            "by" => {
                let key = self.field_expr(0)?;
                self.expect_symbol(")")?;
                Ok(Stage::By(key))
            }
            "coalesce" => {
                self.expect_symbol(")")?;
                Ok(Stage::Coalesce)
            }
            function if AGGREGATES.contains(&function) => {
                let arg = if self.is_symbol(")") {
                    None
                } else {
                    Some(self.field_expr(0)?)
                };
                self.expect_symbol(")")?;
                let Some(op @ ("=" | "!=" | "<" | "<=" | ">" | ">=")) = self.peek_symbol() else {
                    return Err(self.unexpected("a comparison"));
                };
                self.at += 1;
                let value = match self.advance() {
                    Some(Spanned {
                        token: Token::Int(n),
                        ..
                    }) => Static::Int(n),
                    Some(Spanned {
                        token: Token::Float(n),
                        ..
                    }) => Static::Float(n),
                    Some(Spanned {
                        token: Token::Duration(d),
                        ..
                    }) => Static::Duration(d),
                    _ => {
                        self.at = self.at.saturating_sub(1);
                        return Err(self.unexpected("a number or duration"));
                    }
                };
                Ok(Stage::Aggregate {
                    function: function.to_owned(),
                    arg,
                    op,
                    value,
                    span: Span::new(start.start, self.previous_end()),
                })
            }
            other => Err(Diagnostic::at(start, format!("unknown stage {other:?}"))),
        }
    }

    fn metrics(&mut self) -> Result<Metrics, Diagnostic> {
        let start = self.span_here();
        let Some(Spanned {
            token: Token::Ident(function),
            ..
        }) = self.advance()
        else {
            return Err(self.unexpected("a metrics function"));
        };
        let takes_value = METRIC_FUNCTIONS
            .iter()
            .any(|(f, takes)| *f == function && *takes);
        self.expect_symbol("(")?;
        let mut arg = None;
        let mut quantiles = Vec::new();
        if takes_value {
            arg = Some(self.field_expr(0)?);
            while self.eat_symbol(",") {
                match self.advance() {
                    Some(Spanned {
                        token: Token::Float(q),
                        ..
                    }) => quantiles.push(q),
                    Some(Spanned {
                        token: Token::Int(q),
                        ..
                    }) => quantiles.push(f64::from(i32::try_from(q).unwrap_or(i32::MAX))),
                    _ => {
                        self.at = self.at.saturating_sub(1);
                        return Err(self.unexpected("a quantile"));
                    }
                }
            }
        }
        self.expect_symbol(")")?;
        let mut by = Vec::new();
        if self.is_ident("by") {
            self.at += 1;
            self.expect_symbol("(")?;
            by.push(self.field_expr(0)?);
            while self.eat_symbol(",") {
                by.push(self.field_expr(0)?);
            }
            self.expect_symbol(")")?;
        }
        Ok(Metrics {
            function,
            arg,
            quantiles,
            by,
            span: Span::new(start.start, self.previous_end()),
        })
    }

    /// The binary operator next and its precedence.
    fn field_op(&self) -> Option<(&'static str, u8)> {
        let symbol = self.peek_symbol()?;
        let precedence = match symbol {
            "||" => 1,
            "&&" => 2,
            "=" | "!=" | "<" | "<=" | ">" | ">=" | "=~" | "!~" => 3,
            "+" | "-" => 4,
            "*" | "/" | "%" => 5,
            "^" => 6,
            _ => return None,
        };
        Some((symbol, precedence))
    }

    fn field_expr(&mut self, min_precedence: u8) -> Result<FieldExpr, Diagnostic> {
        let mut left = self.field_unary()?;
        while let Some((op, precedence)) = self.field_op() {
            if precedence < min_precedence {
                break;
            }
            let span = self.span_here();
            self.at += 1;
            // `^` is right-associative; everything else left.
            let next = if op == "^" {
                precedence
            } else {
                precedence + 1
            };
            let right = self.field_expr(next)?;
            left = FieldExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
                span,
            };
        }
        Ok(left)
    }

    fn field_unary(&mut self) -> Result<FieldExpr, Diagnostic> {
        if self.eat_symbol("-") {
            return Ok(FieldExpr::Negate(Box::new(self.field_unary()?)));
        }
        if self.eat_symbol("!") {
            return Ok(FieldExpr::Not(Box::new(self.field_unary()?)));
        }
        self.field_atom()
    }

    fn field_atom(&mut self) -> Result<FieldExpr, Diagnostic> {
        if self.eat_symbol("(") {
            let inner = self.field_expr(0)?;
            self.expect_symbol(")")?;
            return Ok(inner);
        }
        let span = self.span_here();
        let Some(token) = self.advance() else {
            return Err(self.unexpected("a value"));
        };
        Ok(match token.token {
            Token::String(s) => FieldExpr::Static(Static::String(s)),
            Token::Int(n) => FieldExpr::Static(Static::Int(n)),
            Token::Float(n) => FieldExpr::Static(Static::Float(n)),
            Token::Duration(d) => FieldExpr::Static(Static::Duration(d)),
            Token::Attribute(scope, name) => FieldExpr::Attribute { scope, name, span },
            Token::ScopedIntrinsic(scope, name) => FieldExpr::Intrinsic {
                scope: Some(scope),
                name,
                span,
            },
            Token::Ident(name) => match name.as_str() {
                "true" => FieldExpr::Static(Static::Bool(true)),
                "false" => FieldExpr::Static(Static::Bool(false)),
                "nil" => FieldExpr::Static(Static::Nil),
                s if STATUSES.contains(&s) => FieldExpr::Static(Static::Status(name)),
                k if KINDS.contains(&k) => FieldExpr::Static(Static::Kind(name)),
                i if INTRINSICS.contains(&i) => FieldExpr::Intrinsic {
                    scope: None,
                    name,
                    span,
                },
                other => {
                    return Err(Diagnostic::at(
                        span,
                        format!(
                            "unknown intrinsic {other:?}; attributes are written .{other}, span.{other} or resource.{other}"
                        ),
                    ));
                }
            },
            Token::Symbol(_) => {
                self.at -= 1;
                return Err(self.unexpected("a value"));
            }
        })
    }
}

/// The structural operator written as `symbol`.
fn structural_op(symbol: &str) -> StructuralOp {
    let (negated, union, relation) = match symbol.as_bytes()[0] {
        b'!' => (true, false, &symbol[1..]),
        b'&' => (false, true, &symbol[1..]),
        _ => (false, false, symbol),
    };
    let relation = match relation {
        ">>" => ">>",
        ">" => ">",
        "~" => "~",
        "<<" => "<<",
        _ => "<",
    };
    StructuralOp {
        relation,
        negated,
        union,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(text: &str) -> SpansetExpr {
        match parse(text) {
            Ok(Query {
                pipeline: Pipeline { first, stages },
                metrics: None,
            }) if stages.is_empty() => first,
            other => panic!("not a bare filter: {other:?}"),
        }
    }

    fn condition(text: &str) -> String {
        match filter(text) {
            SpansetExpr::Filter {
                condition: Some(c), ..
            } => c.to_string(),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parses_conditions_with_precedence() {
        assert_eq!(
            condition(
                r#"{ span.http.status_code >= 500 && resource.service.name = "api" || .x = nil }"#
            ),
            r#"(((span.http.status_code >= 500) && (resource.service.name = "api")) || (.x = nil))"#
        );
        assert_eq!(
            condition("{ duration > 1s + 500ms * 2 && !(status = error) }"),
            "((duration > (1000000000ns + (500000000ns * 2))) && !(status = error))"
        );
        assert_eq!(condition("{ -span.x = -1 }"), "(-span.x = -1)");
        assert_eq!(condition("{ 2 ^ 3 ^ 2 = 512 }"), "((2 ^ (3 ^ 2)) = 512)");
        assert_eq!(
            condition("{ kind = server && trace:rootName =~ `a.*` }"),
            "((kind = server) && (trace:rootName =~ \"a.*\"))"
        );
        assert!(matches!(
            filter("{ }"),
            SpansetExpr::Filter {
                condition: None,
                ..
            }
        ));
    }

    #[test]
    fn structural_operators_bind_tighter_than_logical_ones() {
        let query = filter("{ .a } >> { .b } && { .c } !> { .d } || { .e }");
        let SpansetExpr::Or(left, right) = query else {
            panic!("{query:?}");
        };
        assert!(matches!(*right, SpansetExpr::Filter { .. }));
        let SpansetExpr::And(desc, child) = *left else {
            panic!("{left:?}");
        };
        assert!(matches!(
            *desc,
            SpansetExpr::Structural {
                op: StructuralOp {
                    relation: ">>",
                    negated: false,
                    union: false
                },
                ..
            }
        ));
        assert!(matches!(
            *child,
            SpansetExpr::Structural {
                op: StructuralOp {
                    relation: ">",
                    negated: true,
                    union: false
                },
                ..
            }
        ));
        let grouped = filter("({ .a } || { .b }) &~ { .c }");
        assert!(matches!(
            grouped,
            SpansetExpr::Structural {
                op: StructuralOp {
                    relation: "~",
                    negated: false,
                    union: true
                },
                ..
            }
        ));
    }

    #[test]
    fn parses_stages_and_metrics() {
        let query = parse(
            "{ .a } | count() > 2 | avg(duration) >= 1s | select(span.x, .y) | by(resource.service.name) | coalesce() | { .z }",
        )
        .unwrap();
        let stages = query.pipeline.stages;
        assert_eq!(stages.len(), 6);
        assert!(matches!(
            &stages[0],
            Stage::Aggregate {
                function,
                arg: None,
                op: ">",
                value: Static::Int(2),
                ..
            } if function == "count"
        ));
        assert!(matches!(
            &stages[1],
            Stage::Aggregate {
                arg: Some(FieldExpr::Intrinsic { .. }),
                op: ">=",
                value: Static::Duration(_),
                ..
            }
        ));
        assert!(matches!(&stages[2], Stage::Select(fields) if fields.len() == 2));
        assert!(matches!(&stages[3], Stage::By(key) if key.to_string() == "resource.service.name"));
        assert_eq!(stages[4], Stage::Coalesce);
        assert!(matches!(&stages[5], Stage::Spanset(_)));

        let metrics = parse("{ status = error } | quantile_over_time(duration, 0.5, .99) by (resource.service.name, span.http.method)")
            .unwrap()
            .metrics
            .unwrap();
        assert_eq!(metrics.function, "quantile_over_time");
        assert_eq!(metrics.quantiles, vec![0.5, 0.99]);
        assert_eq!(metrics.by.len(), 2);
        let rate = parse("{ } | rate()").unwrap().metrics.unwrap();
        assert_eq!(rate.function, "rate");
        assert!(rate.arg.is_none() && rate.by.is_empty());
    }

    #[test]
    fn errors_are_positioned() {
        let error = parse("{ .a = }").unwrap_err();
        assert_eq!(error.span, Some(Span::new(7, 8)));
        assert!(
            error.message.starts_with("expected a value"),
            "{}",
            error.message
        );
        let error = parse("{ foo = 1 }").unwrap_err();
        assert!(
            error.message.contains("unknown intrinsic"),
            "{}",
            error.message
        );
        let error = parse("{ .a } | nope()").unwrap_err();
        assert_eq!(error.message, "unknown stage \"nope\"");
        let error = parse("{ .a } | count() 3").unwrap_err();
        assert!(error.message.starts_with("expected a comparison"));
        let error = parse("{ .a } | rate() | { .b }").unwrap_err();
        assert!(
            error.message.contains("after the query"),
            "{}",
            error.message
        );
        let error = parse("{ .a }}").unwrap_err();
        assert_eq!(error.span, Some(Span::new(6, 7)));
        assert_eq!(parse("{ .a ").unwrap_err().span, Some(Span::new(5, 5)));
    }
}
