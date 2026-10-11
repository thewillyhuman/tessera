// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! A recursive-descent parser for `PromQL` (section 9.3), with
//! Prometheus' precedence: `^` binds tightest and to the right, then
//! `*`, `/`, `%` and `atan2`, then `+` and `-`, then the comparisons,
//! then `and` and `unless`, then `or`; a unary minus binds tighter than
//! everything but `^`. Keywords are case-insensitive. Functions and
//! aggregation operators are checked against Prometheus' lists so that
//! an unknown name is an error at its position.

use std::time::Duration;

use tessera_ir::{Diagnostic, Span};

use crate::ast::{At, BinaryOp, Expr, Grouping, MatchOp, Matcher, VectorMatching};
use crate::lexer::{Spanned, Token, tokenize};

/// The aggregation operators.
pub const AGGREGATIONS: [&str; 14] = [
    "sum",
    "min",
    "max",
    "avg",
    "group",
    "stddev",
    "stdvar",
    "count",
    "count_values",
    "bottomk",
    "topk",
    "quantile",
    "limitk",
    "limit_ratio",
];

/// The aggregation operators that take a parameter first.
pub const PARAMETERIZED: [&str; 6] = [
    "count_values",
    "bottomk",
    "topk",
    "quantile",
    "limitk",
    "limit_ratio",
];

/// The functions.
pub const FUNCTIONS: [&str; 72] = [
    "abs",
    "absent",
    "absent_over_time",
    "acos",
    "acosh",
    "asin",
    "asinh",
    "atan",
    "atanh",
    "avg_over_time",
    "ceil",
    "changes",
    "clamp",
    "clamp_max",
    "clamp_min",
    "cos",
    "cosh",
    "count_over_time",
    "day_of_month",
    "day_of_week",
    "day_of_year",
    "days_in_month",
    "deg",
    "delta",
    "deriv",
    "double_exponential_smoothing",
    "exp",
    "floor",
    "histogram_quantile",
    "holt_winters",
    "hour",
    "idelta",
    "increase",
    "irate",
    "label_join",
    "label_replace",
    "last_over_time",
    "ln",
    "log10",
    "log2",
    "mad_over_time",
    "max_over_time",
    "min_over_time",
    "minute",
    "month",
    "pi",
    "predict_linear",
    "present_over_time",
    "quantile_over_time",
    "rad",
    "rate",
    "resets",
    "round",
    "scalar",
    "sgn",
    "sin",
    "sinh",
    "sort",
    "sort_desc",
    "sqrt",
    "stddev_over_time",
    "stdvar_over_time",
    "sum_over_time",
    "tan",
    "tanh",
    "time",
    "timestamp",
    "vector",
    "year",
    "asin_placeholder_unused",
    "acos_placeholder_unused",
    "atan_placeholder_unused",
];

/// Parses one expression.
///
/// # Errors
///
/// The first syntax error, with its position.
pub fn parse(text: &str) -> Result<Expr, Diagnostic> {
    let tokens = tokenize(text)?;
    let mut parser = Parser {
        tokens,
        at: 0,
        end: text.len(),
    };
    let expr = parser.expr(0)?;
    if let Some(extra) = parser.peek() {
        return Err(Diagnostic::at(
            extra.span,
            format!("unexpected {} after the expression", describe(&extra.token)),
        ));
    }
    Ok(expr)
}

struct Parser {
    tokens: Vec<Spanned>,
    at: usize,
    end: usize,
}

fn describe(token: &Token) -> String {
    match token {
        Token::Ident(name) => format!("{name:?}"),
        Token::Number(n) => format!("number {n}"),
        Token::Duration(d) => format!("duration {}s", d.as_secs_f64()),
        Token::String(s) => format!("string {s:?}"),
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

    /// Whether the next token is the keyword `word`, case-insensitively.
    fn is_keyword(&self, word: &str) -> bool {
        matches!(self.peek(), Some(Spanned { token: Token::Ident(n), .. }) if n.eq_ignore_ascii_case(word))
    }

    fn eat_symbol(&mut self, symbol: &str) -> bool {
        if self.is_symbol(symbol) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn eat_keyword(&mut self, word: &str) -> bool {
        if self.is_keyword(word) {
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
                format!("expected {expected}, found the end of the expression"),
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

    fn ident(&mut self) -> Result<(String, Span), Diagnostic> {
        match self.peek() {
            Some(Spanned {
                token: Token::Ident(name),
                span,
            }) => {
                let out = (name.clone(), *span);
                self.at += 1;
                Ok(out)
            }
            _ => Err(self.unexpected("a name")),
        }
    }

    fn string(&mut self) -> Result<String, Diagnostic> {
        match self.peek() {
            Some(Spanned {
                token: Token::String(s),
                ..
            }) => {
                let out = s.clone();
                self.at += 1;
                Ok(out)
            }
            _ => Err(self.unexpected("a string")),
        }
    }

    /// A duration, or a number of seconds.
    fn duration(&mut self) -> Result<Duration, Diagnostic> {
        match self.advance() {
            Some(Spanned {
                token: Token::Duration(d),
                ..
            }) => Ok(d),
            Some(Spanned {
                token: Token::Number(n),
                span,
            }) if n.is_finite() && n >= 0.0 => {
                Duration::try_from_secs_f64(n).map_err(|_| Diagnostic::at(span, "invalid duration"))
            }
            _ => {
                self.at = self.at.saturating_sub(1);
                Err(self.unexpected("a duration"))
            }
        }
    }

    /// The binary operator next and its precedence.
    fn binary_op(&self) -> Option<(BinaryOp, u8)> {
        let token = &self.peek()?.token;
        Some(match token {
            Token::Symbol("^") => (BinaryOp::Pow, 6),
            Token::Symbol("*") => (BinaryOp::Mul, 5),
            Token::Symbol("/") => (BinaryOp::Div, 5),
            Token::Symbol("%") => (BinaryOp::Mod, 5),
            Token::Symbol("+") => (BinaryOp::Add, 4),
            Token::Symbol("-") => (BinaryOp::Sub, 4),
            Token::Symbol("==") => (BinaryOp::Eq, 3),
            Token::Symbol("!=") => (BinaryOp::NotEq, 3),
            Token::Symbol(">") => (BinaryOp::Gt, 3),
            Token::Symbol("<") => (BinaryOp::Lt, 3),
            Token::Symbol(">=") => (BinaryOp::GtEq, 3),
            Token::Symbol("<=") => (BinaryOp::LtEq, 3),
            Token::Ident(word) => match word.to_ascii_lowercase().as_str() {
                "atan2" => (BinaryOp::Atan2, 5),
                "and" => (BinaryOp::And, 2),
                "unless" => (BinaryOp::Unless, 2),
                "or" => (BinaryOp::Or, 1),
                _ => return None,
            },
            _ => return None,
        })
    }

    fn expr(&mut self, min_precedence: u8) -> Result<Expr, Diagnostic> {
        let mut left = self.unary()?;
        while let Some((op, precedence)) = self.binary_op() {
            if precedence < min_precedence {
                break;
            }
            let span = self.span_here();
            self.at += 1;
            let bool_modifier = self.eat_keyword("bool");
            let matching = self.vector_matching()?;
            // `^` is right-associative; everything else left.
            let next = if op == BinaryOp::Pow {
                precedence
            } else {
                precedence + 1
            };
            let right = self.expr(next)?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
                matching,
                bool_modifier,
                span,
            };
        }
        Ok(left)
    }

    fn vector_matching(&mut self) -> Result<Option<VectorMatching>, Diagnostic> {
        let on = if self.eat_keyword("on") {
            true
        } else if self.eat_keyword("ignoring") {
            false
        } else {
            return Ok(None);
        };
        let labels = self.label_list()?;
        let group = if self.eat_keyword("group_left") {
            Some((true, self.optional_label_list()?))
        } else if self.eat_keyword("group_right") {
            Some((false, self.optional_label_list()?))
        } else {
            None
        };
        Ok(Some(VectorMatching { on, labels, group }))
    }

    /// `(a, b)`, possibly empty.
    fn label_list(&mut self) -> Result<Vec<String>, Diagnostic> {
        self.expect_symbol("(")?;
        let mut labels = Vec::new();
        if !self.is_symbol(")") {
            labels.push(self.ident()?.0);
            while self.eat_symbol(",") {
                if self.is_symbol(")") {
                    break;
                }
                labels.push(self.ident()?.0);
            }
        }
        self.expect_symbol(")")?;
        Ok(labels)
    }

    fn optional_label_list(&mut self) -> Result<Vec<String>, Diagnostic> {
        if self.is_symbol("(") {
            self.label_list()
        } else {
            Ok(Vec::new())
        }
    }

    fn unary(&mut self) -> Result<Expr, Diagnostic> {
        if self.eat_symbol("-") {
            // A sign binds tighter than everything but `^`.
            let operand = self.expr(6)?;
            return Ok(Expr::Unary {
                negative: true,
                expr: Box::new(operand),
            });
        }
        if self.eat_symbol("+") {
            let operand = self.expr(6)?;
            return Ok(Expr::Unary {
                negative: false,
                expr: Box::new(operand),
            });
        }
        let primary = self.primary()?;
        self.modifiers(primary)
    }

    /// `[range]`, `[range:step]`, `offset` and `@` after an expression.
    fn modifiers(&mut self, mut expr: Expr) -> Result<Expr, Diagnostic> {
        loop {
            if self.is_symbol("[") {
                let open = self.span_here();
                self.at += 1;
                let range = self.duration()?;
                if self.eat_symbol(":") {
                    let step = if self.is_symbol("]") {
                        None
                    } else {
                        Some(self.duration()?)
                    };
                    let close = self.expect_symbol("]")?;
                    expr = Expr::Subquery {
                        expr: Box::new(expr),
                        range,
                        step,
                        offset: None,
                        at: None,
                        span: Span::new(open.start, close.end),
                    };
                } else {
                    self.expect_symbol("]")?;
                    match expr {
                        Expr::Selector {
                            range: ref mut slot @ None,
                            ..
                        } => *slot = Some(range),
                        _ => {
                            return Err(Diagnostic::at(
                                open,
                                "a range applies to a selector; write [range:] for a subquery",
                            ));
                        }
                    }
                }
            } else if self.is_keyword("offset") {
                let span = self.span_here();
                self.at += 1;
                let negative = self.eat_symbol("-");
                let duration = self.duration()?;
                set_offset(&mut expr, (negative, duration), span)?;
            } else if self.is_symbol("@") {
                let span = self.span_here();
                self.at += 1;
                let at = if self.eat_keyword("start") {
                    self.expect_symbol("(")?;
                    self.expect_symbol(")")?;
                    At::Start
                } else if self.eat_keyword("end") {
                    self.expect_symbol("(")?;
                    self.expect_symbol(")")?;
                    At::End
                } else {
                    let negative = self.eat_symbol("-");
                    let Some(Spanned {
                        token: Token::Number(n),
                        ..
                    }) = self.advance()
                    else {
                        self.at = self.at.saturating_sub(1);
                        return Err(self.unexpected("a time, start() or end()"));
                    };
                    At::Time(if negative { -n } else { n })
                };
                set_at(&mut expr, at, span)?;
            } else {
                return Ok(expr);
            }
        }
    }

    fn primary(&mut self) -> Result<Expr, Diagnostic> {
        let span = self.span_here();
        match self.peek().map(|t| t.token.clone()) {
            Some(Token::Number(n)) => {
                self.at += 1;
                Ok(Expr::Number(n))
            }
            Some(Token::Duration(d)) => {
                // A bare duration is a number of seconds, as Prometheus 3
                // reads it.
                self.at += 1;
                Ok(Expr::Number(d.as_secs_f64()))
            }
            Some(Token::String(s)) => {
                self.at += 1;
                Ok(Expr::String(s))
            }
            Some(Token::Symbol("(")) => {
                self.at += 1;
                let inner = self.expr(0)?;
                self.expect_symbol(")")?;
                Ok(Expr::Paren(Box::new(inner)))
            }
            Some(Token::Symbol("{")) => self.selector(None, span),
            Some(Token::Ident(name)) => {
                self.at += 1;
                let lower = name.to_ascii_lowercase();
                if AGGREGATIONS.contains(&lower.as_str())
                    && (self.is_symbol("(") || self.is_keyword("by") || self.is_keyword("without"))
                {
                    return self.aggregation(&lower, span);
                }
                if self.is_symbol("(") {
                    if !FUNCTIONS.contains(&name.as_str()) {
                        return Err(Diagnostic::at(span, format!("unknown function {name:?}")));
                    }
                    return self.call(name, span);
                }
                self.selector(Some(name), span)
            }
            Some(other) => Err(Diagnostic::at(
                span,
                format!("expected an expression, found {}", describe(&other)),
            )),
            None => Err(self.unexpected("an expression")),
        }
    }

    fn selector(&mut self, name: Option<String>, start: Span) -> Result<Expr, Diagnostic> {
        let mut matchers = Vec::new();
        if self.eat_symbol("{") {
            while !self.is_symbol("}") {
                matchers.push(self.matcher()?);
                if !self.eat_symbol(",") {
                    break;
                }
            }
            self.expect_symbol("}")?;
        }
        if name.is_none() && matchers.is_empty() {
            return Err(Diagnostic::at(
                Span::new(start.start, self.previous_end()),
                "a selector needs a metric name or at least one matcher",
            ));
        }
        Ok(Expr::Selector {
            name,
            matchers,
            range: None,
            offset: None,
            at: None,
            span: Span::new(start.start, self.previous_end()),
        })
    }

    fn matcher(&mut self) -> Result<Matcher, Diagnostic> {
        let (name, start) = self.ident()?;
        let op = match self.advance() {
            Some(Spanned {
                token: Token::Symbol("="),
                ..
            }) => MatchOp::Eq,
            Some(Spanned {
                token: Token::Symbol("!="),
                ..
            }) => MatchOp::NotEq,
            Some(Spanned {
                token: Token::Symbol("=~"),
                ..
            }) => MatchOp::Regex,
            Some(Spanned {
                token: Token::Symbol("!~"),
                ..
            }) => MatchOp::NotRegex,
            _ => {
                self.at = self.at.saturating_sub(1);
                return Err(self.unexpected("=, !=, =~ or !~"));
            }
        };
        let value = self.string()?;
        Ok(Matcher {
            name,
            op,
            value,
            span: Span::new(start.start, self.previous_end()),
        })
    }

    fn aggregation(&mut self, op: &str, start: Span) -> Result<Expr, Diagnostic> {
        let mut grouping = self.grouping()?;
        self.expect_symbol("(")?;
        let mut param = None;
        if PARAMETERIZED.contains(&op) {
            param = Some(Box::new(self.expr(0)?));
            self.expect_symbol(",")?;
        }
        let expr = self.expr(0)?;
        self.expect_symbol(")")?;
        if grouping.is_none() {
            grouping = self.grouping()?;
        }
        Ok(Expr::Aggregate {
            op: op.to_owned(),
            expr: Box::new(expr),
            param,
            grouping,
            span: Span::new(start.start, self.previous_end()),
        })
    }

    fn grouping(&mut self) -> Result<Option<Grouping>, Diagnostic> {
        let by = if self.eat_keyword("by") {
            true
        } else if self.eat_keyword("without") {
            false
        } else {
            return Ok(None);
        };
        Ok(Some(Grouping {
            by,
            labels: self.label_list()?,
        }))
    }

    fn call(&mut self, function: String, start: Span) -> Result<Expr, Diagnostic> {
        self.expect_symbol("(")?;
        let mut args = Vec::new();
        if !self.is_symbol(")") {
            args.push(self.expr(0)?);
            while self.eat_symbol(",") {
                args.push(self.expr(0)?);
            }
        }
        self.expect_symbol(")")?;
        Ok(Expr::Call {
            function,
            args,
            span: Span::new(start.start, self.previous_end()),
        })
    }
}

fn set_offset(expr: &mut Expr, offset: (bool, Duration), span: Span) -> Result<(), Diagnostic> {
    match expr {
        Expr::Selector {
            offset: slot @ None,
            ..
        }
        | Expr::Subquery {
            offset: slot @ None,
            ..
        } => {
            *slot = Some(offset);
            Ok(())
        }
        Expr::Selector { .. } | Expr::Subquery { .. } => {
            Err(Diagnostic::at(span, "offset may not be set twice"))
        }
        Expr::Paren(inner) => set_offset(inner, offset, span),
        _ => Err(Diagnostic::at(
            span,
            "offset applies to a selector or a subquery",
        )),
    }
}

fn set_at(expr: &mut Expr, at: At, span: Span) -> Result<(), Diagnostic> {
    match expr {
        Expr::Selector {
            at: slot @ None, ..
        }
        | Expr::Subquery {
            at: slot @ None, ..
        } => {
            *slot = Some(at);
            Ok(())
        }
        Expr::Selector { .. } | Expr::Subquery { .. } => {
            Err(Diagnostic::at(span, "@ may not be set twice"))
        }
        Expr::Paren(inner) => set_at(inner, at, span),
        _ => Err(Diagnostic::at(
            span,
            "@ applies to a selector or a subquery",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RS-5: no query, however mangled, makes the parser panic.
    #[test]
    fn mangled_queries_never_panic() {
        let seeds = [
            r#"sum by (job) (rate(http_requests_total{code!="200"}[5m])) / ignoring(code) group_left sum(rate(x[5m]))"#,
            "histogram_quantile(0.9, sum by (le) (rate(req_duration_bucket[10m]))) > bool 0.5",
            r#"label_replace(up{job="a"} offset 1h, "x", "$1", "job", "(.*)") and on() absent(y)"#,
        ];
        let mut random = tessera_types::SeededRandom::new(5);
        for round in 0..2_000 {
            let input = tessera_types::mutated(seeds[round % seeds.len()].as_bytes(), &mut random);
            let _ = parse(&String::from_utf8_lossy(&input));
        }
    }

    #[test]
    fn parses_selectors_with_modifiers() {
        let expr =
            parse(r#"http_requests_total{job="api", code=~"5.."}[5m] offset 1h @ 1700000000"#)
                .unwrap();
        let Expr::Selector {
            name,
            matchers,
            range,
            offset,
            at,
            ..
        } = expr
        else {
            panic!("not a selector: {expr:?}");
        };
        assert_eq!(name.as_deref(), Some("http_requests_total"));
        assert_eq!(matchers.len(), 2);
        assert_eq!(matchers[1].op, MatchOp::Regex);
        assert_eq!(range, Some(Duration::from_secs(300)));
        assert_eq!(offset, Some((false, Duration::from_secs(3600))));
        assert_eq!(at, Some(At::Time(1_700_000_000.0)));
        assert!(matches!(
            parse(r#"{__name__=~"up|down"} offset -5m @ end()"#).unwrap(),
            Expr::Selector {
                name: None,
                offset: Some((true, _)),
                at: Some(At::End),
                ..
            }
        ));
        assert!(parse("{}").is_err());
        assert!(parse("up offset 5m offset 1m").is_err());
    }

    #[test]
    fn precedence_and_matching() {
        let expr = parse("-a ^ 2 + b * c > bool 1 and d or e").unwrap();
        // ((((-(a^2)) + (b*c)) > bool 1) and d) or e
        let Expr::Binary {
            op: BinaryOp::Or,
            left,
            ..
        } = expr
        else {
            panic!("{expr:?}");
        };
        let Expr::Binary {
            op: BinaryOp::And,
            left,
            ..
        } = *left
        else {
            panic!("{left:?}");
        };
        let Expr::Binary {
            op: BinaryOp::Gt,
            left,
            bool_modifier: true,
            ..
        } = *left
        else {
            panic!("{left:?}");
        };
        let Expr::Binary {
            op: BinaryOp::Add,
            left,
            ..
        } = *left
        else {
            panic!("{left:?}");
        };
        let Expr::Unary {
            negative: true,
            expr,
        } = *left
        else {
            panic!("{left:?}");
        };
        assert!(matches!(
            *expr,
            Expr::Binary {
                op: BinaryOp::Pow,
                ..
            }
        ));
        let right = parse("2 ^ 3 ^ 2").unwrap();
        let Expr::Binary { right, .. } = right else {
            panic!("{right:?}");
        };
        assert!(
            matches!(
                *right,
                Expr::Binary {
                    op: BinaryOp::Pow,
                    ..
                }
            ),
            "right-associative"
        );
        let matched = parse("a / on(job, instance) group_left(env) b").unwrap();
        let Expr::Binary {
            matching: Some(matching),
            ..
        } = matched
        else {
            panic!("{matched:?}");
        };
        assert!(matching.on);
        assert_eq!(matching.labels, vec!["job", "instance"]);
        assert_eq!(matching.group, Some((true, vec!["env".to_owned()])));
    }

    #[test]
    fn aggregations_functions_and_subqueries() {
        let expr = parse("sum by (job) (rate(http_requests_total[5m]))").unwrap();
        let Expr::Aggregate {
            op, grouping, expr, ..
        } = expr
        else {
            panic!("{expr:?}");
        };
        assert_eq!(op, "sum");
        assert_eq!(grouping.unwrap().labels, vec!["job"]);
        assert!(matches!(*expr, Expr::Call { ref function, .. } if function == "rate"));
        let expr = parse("topk(3, up) without (instance)").unwrap();
        assert!(matches!(
            expr,
            Expr::Aggregate {
                param: Some(_),
                grouping: Some(Grouping { by: false, .. }),
                ..
            }
        ));
        let expr = parse("max_over_time(rate(up[1m])[30m:1m] offset 5m)").unwrap();
        let Expr::Call { args, .. } = expr else {
            panic!("{expr:?}");
        };
        assert!(matches!(
            &args[0],
            Expr::Subquery {
                step: Some(_),
                offset: Some((false, _)),
                ..
            }
        ));
        assert!(matches!(parse("time()").unwrap(), Expr::Call { ref args, .. } if args.is_empty()));
        assert!(matches!(parse("SUM(up)").unwrap(), Expr::Aggregate { .. }));
        assert!(
            matches!(parse("up[300]").unwrap(), Expr::Selector { range: Some(r), .. } if r == Duration::from_secs(300))
        );
    }

    #[test]
    fn errors_are_positioned() {
        let error = parse("nope(up)").unwrap_err();
        assert_eq!(error.span, Some(Span::new(0, 4)));
        assert!(error.message.contains("unknown function"));
        let error = parse("up{job=}").unwrap_err();
        assert_eq!(error.span, Some(Span::new(7, 8)));
        let error = parse("rate(up[5m])[5m]").unwrap_err();
        assert!(error.message.contains("subquery"), "{}", error.message);
        let error = parse("up +").unwrap_err();
        assert_eq!(error.span, Some(Span::new(4, 4)));
        let error = parse("up up").unwrap_err();
        assert!(error.message.contains("after the expression"));
    }
}
