// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! A recursive-descent parser for `LogQL` (section 9.4).
//!
//! A query that starts with `{` is a log query: a selector followed by
//! pipeline stages. Anything else is a metric expression: range
//! functions over log queries, aggregations with `by` and `without`,
//! and binary operations with the usual precedence.

use std::time::Duration;

use tessera_ir::{Diagnostic, Span};

use crate::ast::{
    AggregateOp, BinaryOp, CompareOp, Conversion, FilterValue, Grouping, LabelFilter, LineOp,
    LogQuery, MatchOp, Matcher, MetricExpr, Query, Stage, VectorMatching,
};
use crate::lexer::{Spanned, Token, tokenize};

/// Range functions and whether they read an unwrapped value.
pub const RANGE_FUNCTIONS: [(&str, bool); 14] = [
    ("count_over_time", false),
    ("rate", false),
    ("bytes_over_time", false),
    ("bytes_rate", false),
    ("absent_over_time", false),
    ("sum_over_time", true),
    ("avg_over_time", true),
    ("min_over_time", true),
    ("max_over_time", true),
    ("first_over_time", true),
    ("last_over_time", true),
    ("stddev_over_time", true),
    ("stdvar_over_time", true),
    ("quantile_over_time", true),
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
    let query = if parser.is_symbol("{") {
        Query::Log(parser.log_query(false)?)
    } else {
        Query::Metric(parser.metric_expr(0)?)
    };
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
        Token::Number(n) => format!("number {n}"),
        Token::String(s) => format!("string {s:?}"),
        Token::Duration(d) => format!("duration {}s", d.as_secs_f64()),
        Token::Bytes(b) => format!("size {b} bytes"),
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

    fn eat_symbol(&mut self, symbol: &str) -> bool {
        if self.is_symbol(symbol) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn eat_ident(&mut self, name: &str) -> bool {
        if self.is_ident(name) {
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
                format!("{expected}, found {}", describe(&token.token)),
            ),
            None => Diagnostic::at(
                Span::new(self.end, self.end),
                format!("{expected}, found end of query"),
            ),
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
            _ => Err(self.unexpected("expected a name")),
        }
    }

    fn string(&mut self) -> Result<(String, Span), Diagnostic> {
        match self.peek() {
            Some(Spanned {
                token: Token::String(value),
                span,
            }) => {
                let out = (value.clone(), *span);
                self.at += 1;
                Ok(out)
            }
            _ => Err(self.unexpected("expected a string")),
        }
    }

    fn number(&mut self) -> Result<(f64, Span), Diagnostic> {
        match self.peek() {
            Some(Spanned {
                token: Token::Number(text),
                span,
            }) => {
                let value = text
                    .parse::<f64>()
                    .map_err(|_| Diagnostic::at(*span, format!("invalid number {text}")))?;
                let span = *span;
                self.at += 1;
                Ok((value, span))
            }
            _ => Err(self.unexpected("expected a number")),
        }
    }

    /// `{ matchers } stages...`; inside a range, stages stop at `[`.
    fn log_query(&mut self, in_range: bool) -> Result<LogQuery, Diagnostic> {
        let start = self.expect_symbol("{")?;
        let mut matchers = Vec::new();
        while !self.is_symbol("}") {
            matchers.push(self.matcher()?);
            if !self.eat_symbol(",") {
                break;
            }
        }
        self.expect_symbol("}")?;
        let mut stages = Vec::new();
        loop {
            if in_range && self.is_symbol("[") {
                break;
            }
            let Some(next) = self.peek() else {
                break;
            };
            match &next.token {
                Token::Symbol("|=" | "!=" | "|~" | "!~") => {
                    stages.push(self.line_filter()?);
                }
                Token::Symbol("|") => {
                    self.at += 1;
                    stages.push(self.pipe_stage()?);
                }
                _ => break,
            }
        }
        Ok(LogQuery {
            matchers,
            stages,
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
            Some(other) => {
                return Err(Diagnostic::at(
                    other.span,
                    format!(
                        "expected a matcher operator, found {}",
                        describe(&other.token)
                    ),
                ));
            }
            None => return Err(self.unexpected("expected a matcher operator")),
        };
        let (value, end) = self.string()?;
        Ok(Matcher {
            name,
            op,
            value,
            span: Span::new(start.start, end.end),
        })
    }

    fn line_filter(&mut self) -> Result<Stage, Diagnostic> {
        let Some(Spanned {
            token: Token::Symbol(symbol),
            span,
        }) = self.advance()
        else {
            return Err(self.unexpected("expected a line filter"));
        };
        let op = match symbol {
            "|=" => LineOp::Contains,
            "!=" => LineOp::NotContains,
            "|~" => LineOp::Regex,
            _ => LineOp::NotRegex,
        };
        let mut patterns = vec![self.string()?.0];
        while self.eat_ident("or") {
            patterns.push(self.string()?.0);
        }
        Ok(Stage::Line {
            op,
            patterns,
            span: Span::new(span.start, self.previous_end()),
        })
    }

    /// The stage after a `|`.
    fn pipe_stage(&mut self) -> Result<Stage, Diagnostic> {
        let Some(Spanned {
            token: Token::Ident(word),
            span,
        }) = self.peek().cloned()
        else {
            return Err(self.unexpected("expected a pipeline stage"));
        };
        let simple = match word.as_str() {
            "logfmt" => Some(Stage::Logfmt),
            "unpack" => Some(Stage::Unpack),
            "decolorize" => Some(Stage::Decolorize),
            _ => None,
        };
        if let Some(stage) = simple {
            self.at += 1;
            return Ok(stage);
        }
        match word.as_str() {
            "json" => {
                self.at += 1;
                self.json_stage()
            }
            "regexp" => {
                self.at += 1;
                Ok(Stage::Regexp(self.string()?.0))
            }
            "pattern" => {
                self.at += 1;
                Ok(Stage::Pattern(self.string()?.0))
            }
            "line_format" => {
                self.at += 1;
                Ok(Stage::LineFormat(self.string()?.0))
            }
            "label_format" => {
                self.at += 1;
                self.label_format_stage()
            }
            "drop" | "keep" => {
                self.at += 1;
                let mut labels = vec![self.ident()?.0];
                while self.eat_symbol(",") {
                    labels.push(self.ident()?.0);
                }
                Ok(if word == "drop" {
                    Stage::Drop(labels)
                } else {
                    Stage::Keep(labels)
                })
            }
            "unwrap" => {
                self.at += 1;
                self.unwrap_stage(span)
            }
            _ => Ok(Stage::Filter(self.label_filter()?)),
        }
    }

    /// `json` with optional `label="path"` parameters.
    fn json_stage(&mut self) -> Result<Stage, Diagnostic> {
        let mut paths = Vec::new();
        while matches!(
            self.peek(),
            Some(Spanned {
                token: Token::Ident(_),
                ..
            })
        ) && !self.is_stage_keyword_next()
        {
            let (label, _) = self.ident()?;
            self.expect_symbol("=")?;
            let (path, _) = self.string()?;
            paths.push((label, path));
            if !self.eat_symbol(",") {
                break;
            }
        }
        Ok(Stage::Json(paths))
    }

    /// `label_format a="template", b=other`.
    fn label_format_stage(&mut self) -> Result<Stage, Diagnostic> {
        let mut labels = Vec::new();
        loop {
            let (name, _) = self.ident()?;
            self.expect_symbol("=")?;
            let template = match self.advance() {
                Some(Spanned {
                    token: Token::String(s),
                    ..
                }) => s,
                Some(Spanned {
                    token: Token::Ident(other),
                    ..
                }) => format!("{{{{.{other}}}}}"),
                _ => return Err(self.unexpected("expected a template or a label")),
            };
            labels.push((name, template));
            if !self.eat_symbol(",") {
                break;
            }
        }
        Ok(Stage::LabelFormat(labels))
    }

    /// `unwrap x`, `unwrap duration(x)`, `unwrap bytes(x)`.
    fn unwrap_stage(&mut self, span: Span) -> Result<Stage, Diagnostic> {
        let (first, _) = self.ident()?;
        let (label, conversion) = match first.as_str() {
            "duration" | "duration_seconds" | "bytes" if self.is_symbol("(") => {
                self.expect_symbol("(")?;
                let (label, _) = self.ident()?;
                self.expect_symbol(")")?;
                let conversion = if first == "bytes" {
                    Conversion::Bytes
                } else {
                    Conversion::Duration
                };
                (label, conversion)
            }
            _ => (first, Conversion::None),
        };
        Ok(Stage::Unwrap {
            label,
            conversion,
            span: Span::new(span.start, self.previous_end()),
        })
    }

    /// Whether the next identifier starts a new stage rather than a `json`
    /// parameter: `json` parameters are always followed by `=`.
    fn is_stage_keyword_next(&self) -> bool {
        !matches!(
            self.tokens.get(self.at + 1),
            Some(Spanned {
                token: Token::Symbol("="),
                ..
            })
        )
    }

    fn label_filter(&mut self) -> Result<LabelFilter, Diagnostic> {
        let mut left = self.label_filter_and()?;
        while self.eat_ident("or") {
            let right = self.label_filter_and()?;
            left = LabelFilter::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn label_filter_and(&mut self) -> Result<LabelFilter, Diagnostic> {
        let mut left = self.label_filter_atom()?;
        while self.eat_ident("and") || self.eat_symbol(",") {
            let right = self.label_filter_atom()?;
            left = LabelFilter::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn label_filter_atom(&mut self) -> Result<LabelFilter, Diagnostic> {
        if self.eat_symbol("(") {
            let inner = self.label_filter()?;
            self.expect_symbol(")")?;
            return Ok(inner);
        }
        let (name, start) = self.ident()?;
        let op = match self.advance() {
            Some(Spanned {
                token: Token::Symbol(s),
                ..
            }) => match s {
                "=" | "==" => CompareOp::Eq,
                "!=" => CompareOp::NotEq,
                "=~" => CompareOp::Regex,
                "!~" => CompareOp::NotRegex,
                ">" => CompareOp::Gt,
                ">=" => CompareOp::Gte,
                "<" => CompareOp::Lt,
                "<=" => CompareOp::Lte,
                other => {
                    return Err(Diagnostic::at(
                        start,
                        format!("expected a comparison after {name}, found {other:?}"),
                    ));
                }
            },
            _ => return Err(self.unexpected("expected a comparison")),
        };
        let value = match self.advance() {
            Some(Spanned {
                token: Token::String(s),
                ..
            }) => FilterValue::Text(s),
            Some(Spanned {
                token: Token::Number(n),
                span,
            }) => FilterValue::Number(
                n.parse()
                    .map_err(|_| Diagnostic::at(span, format!("invalid number {n}")))?,
            ),
            Some(Spanned {
                token: Token::Duration(d),
                ..
            }) => FilterValue::Duration(d),
            Some(Spanned {
                token: Token::Bytes(b),
                ..
            }) => FilterValue::Bytes(b),
            Some(Spanned {
                token: Token::Ident(word),
                span,
            }) if word == "ip" => {
                if !matches!(op, CompareOp::Eq | CompareOp::NotEq) {
                    return Err(Diagnostic::at(span, "ip() is compared with = or !="));
                }
                self.expect_symbol("(")?;
                let (pattern, _) = self.string()?;
                self.expect_symbol(")")?;
                FilterValue::Ip(pattern)
            }
            _ => return Err(self.unexpected("expected a value")),
        };
        Ok(LabelFilter::Compare {
            name,
            op,
            value,
            span: Span::new(start.start, self.previous_end()),
        })
    }

    fn metric_expr(&mut self, min_precedence: u8) -> Result<MetricExpr, Diagnostic> {
        let mut left = self.metric_atom()?;
        while let Some((op, precedence)) = self.binary_op() {
            if precedence < min_precedence {
                break;
            }
            let start = self.span_here();
            self.at += 1;
            let bool_modifier = self.eat_ident("bool");
            let matching = self.vector_matching()?;
            let right = self.metric_expr(precedence + 1)?;
            left = MetricExpr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
                matching,
                bool_modifier,
                span: Span::new(start.start, self.previous_end()),
            };
        }
        Ok(left)
    }

    fn binary_op(&self) -> Option<(BinaryOp, u8)> {
        let Some(Spanned {
            token: Token::Symbol(s),
            ..
        }) = self.peek()
        else {
            return None;
        };
        Some(match *s {
            "*" => (BinaryOp::Mul, 3),
            "/" => (BinaryOp::Div, 3),
            "%" => (BinaryOp::Rem, 3),
            "+" => (BinaryOp::Add, 2),
            "-" => (BinaryOp::Sub, 2),
            "==" => (BinaryOp::Eq, 1),
            "!=" => (BinaryOp::NotEq, 1),
            ">" => (BinaryOp::Gt, 1),
            ">=" => (BinaryOp::Gte, 1),
            "<" => (BinaryOp::Lt, 1),
            "<=" => (BinaryOp::Lte, 1),
            _ => return None,
        })
    }

    fn metric_atom(&mut self) -> Result<MetricExpr, Diagnostic> {
        if self.eat_symbol("(") {
            let inner = self.metric_expr(0)?;
            self.expect_symbol(")")?;
            return Ok(inner);
        }
        if let Some(Spanned {
            token: Token::Number(_),
            ..
        }) = self.peek()
        {
            return Ok(MetricExpr::Scalar(self.number()?.0));
        }
        let (name, start) = self.ident()?;
        if let Some((_, _)) = RANGE_FUNCTIONS.iter().find(|(f, _)| *f == name) {
            return self.range_expr(name, start);
        }
        if name == "vector" {
            self.expect_symbol("(")?;
            let (value, _) = self.number()?;
            self.expect_symbol(")")?;
            return Ok(MetricExpr::Vector(value));
        }
        let op = match name.as_str() {
            "sum" => AggregateOp::Sum,
            "avg" => AggregateOp::Avg,
            "min" => AggregateOp::Min,
            "max" => AggregateOp::Max,
            "count" => AggregateOp::Count,
            "stddev" => AggregateOp::Stddev,
            "stdvar" => AggregateOp::Stdvar,
            "topk" | "bottomk" => {
                let grouping = self.grouping()?;
                self.expect_symbol("(")?;
                let (k, kspan) = self.number()?;
                if k < 1.0 || k.fract() != 0.0 {
                    return Err(Diagnostic::at(
                        kspan,
                        "k must be a whole number of at least 1",
                    ));
                }
                self.expect_symbol(",")?;
                let inner = self.metric_expr(0)?;
                self.expect_symbol(")")?;
                let grouping = grouping.or(self.grouping()?);
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let k = k as usize;
                return Ok(MetricExpr::Aggregate {
                    op: if name == "topk" {
                        AggregateOp::Topk(k)
                    } else {
                        AggregateOp::Bottomk(k)
                    },
                    grouping,
                    inner: Box::new(inner),
                    span: Span::new(start.start, self.previous_end()),
                });
            }
            _ => {
                return Err(Diagnostic::at(start, format!("unknown function {name}")));
            }
        };
        let grouping = self.grouping()?;
        self.expect_symbol("(")?;
        let inner = self.metric_expr(0)?;
        self.expect_symbol(")")?;
        let grouping = grouping.or(self.grouping()?);
        Ok(MetricExpr::Aggregate {
            op,
            grouping,
            inner: Box::new(inner),
            span: Span::new(start.start, self.previous_end()),
        })
    }

    /// `function( [parameter,] log_query [range] ) [grouping]`.
    fn range_expr(&mut self, function: String, start: Span) -> Result<MetricExpr, Diagnostic> {
        self.expect_symbol("(")?;
        let parameter = if function == "quantile_over_time" {
            let (q, _) = self.number()?;
            self.expect_symbol(",")?;
            Some(q)
        } else {
            None
        };
        let log = self.log_query(true)?;
        self.expect_symbol("[")?;
        let Some(Spanned {
            token: Token::Duration(range),
            ..
        }) = self.advance()
        else {
            return Err(self.unexpected("expected a range such as [5m]"));
        };
        self.expect_symbol("]")?;
        let offset = if self.eat_ident("offset") {
            match self.advance() {
                Some(Spanned {
                    token: Token::Duration(d),
                    ..
                }) => d,
                _ => return Err(self.unexpected("expected a duration after offset")),
            }
        } else {
            Duration::ZERO
        };
        self.expect_symbol(")")?;
        let grouping = self.grouping()?;
        Ok(MetricExpr::Range {
            function,
            parameter,
            log,
            range,
            offset,
            grouping,
            span: Span::new(start.start, self.previous_end()),
        })
    }

    fn grouping(&mut self) -> Result<Option<Grouping>, Diagnostic> {
        let by = if self.eat_ident("by") {
            true
        } else if self.eat_ident("without") {
            false
        } else {
            return Ok(None);
        };
        let labels = self.label_list()?;
        Ok(Some(Grouping { by, labels }))
    }

    /// `on (...)` or `ignoring (...)` after a binary operator, then
    /// `group_left` or `group_right` with an optional label list.
    fn vector_matching(&mut self) -> Result<Option<VectorMatching>, Diagnostic> {
        let on = if self.eat_ident("on") {
            true
        } else if self.eat_ident("ignoring") {
            false
        } else {
            return Ok(None);
        };
        let labels = self.label_list()?;
        let group = if self.eat_ident("group_left") {
            Some((true, self.optional_label_list()?))
        } else if self.eat_ident("group_right") {
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
        while !self.is_symbol(")") {
            labels.push(self.ident()?.0);
            if !self.eat_symbol(",") {
                break;
            }
        }
        self.expect_symbol(")")?;
        Ok(labels)
    }

    /// A label list when a parenthesis follows, else none.
    fn optional_label_list(&mut self) -> Result<Vec<String>, Diagnostic> {
        if self.is_symbol("(") {
            self.label_list()
        } else {
            Ok(Vec::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(text: &str) -> LogQuery {
        match parse(text).unwrap() {
            Query::Log(q) => q,
            Query::Metric(_) => panic!("metric"),
        }
    }

    fn metric(text: &str) -> MetricExpr {
        match parse(text).unwrap() {
            Query::Metric(m) => m,
            Query::Log(_) => panic!("log"),
        }
    }

    /// RS-5: no query, however mangled, makes the parser panic.
    #[test]
    fn mangled_queries_never_panic() {
        let seeds = [
            r#"{app="checkout"} |= "timeout" | json | status >= 500 | line_format "{{.msg}}""#,
            r#"sum by (app) (rate({app=~"a.*"} | logfmt | unwrap duration [5m]))"#,
            r#"topk(3, count_over_time({job="x"} != "noise" | pattern "<ip> - <_>" [1h] offset 5m))"#,
        ];
        let mut random = tessera_types::SeededRandom::new(5);
        for round in 0..2_000 {
            let input = tessera_types::mutated(seeds[round % seeds.len()].as_bytes(), &mut random);
            let _ = parse(&String::from_utf8_lossy(&input));
        }
    }

    #[test]
    fn parses_selectors_and_every_stage() {
        let q = log(
            r#"{app="checkout", level!="debug", pod=~"c.*", env!~"test"} |= "timeout" or "reset" != "health" |~ "err.*" !~ "x" | json | logfmt | pattern "<ip> <_>" | regexp "(?P<m>.)" | unpack | status >= 500, method="GET" or (size > 2KB and latency > 250ms) | line_format "{{.m}}" | label_format a="{{.b}}", c=d | drop a, b | keep c | decolorize | unwrap duration(latency)"#,
        );
        assert_eq!(q.matchers.len(), 4);
        assert_eq!(q.matchers[2].op, MatchOp::Regex);
        assert_eq!(q.stages.len(), 16);
        assert_eq!(
            q.stages[0],
            Stage::Line {
                op: LineOp::Contains,
                patterns: vec!["timeout".into(), "reset".into()],
                span: Span::new(58, 81)
            }
        );
        assert!(matches!(
            &q.stages[1],
            Stage::Line {
                op: LineOp::NotContains,
                ..
            }
        ));
        assert!(matches!(
            &q.stages[2],
            Stage::Line {
                op: LineOp::Regex,
                ..
            }
        ));
        assert!(matches!(
            &q.stages[3],
            Stage::Line {
                op: LineOp::NotRegex,
                ..
            }
        ));
        assert_eq!(q.stages[4], Stage::Json(vec![]));
        assert_eq!(q.stages[5], Stage::Logfmt);
        assert_eq!(q.stages[6], Stage::Pattern("<ip> <_>".into()));
        assert_eq!(q.stages[7], Stage::Regexp("(?P<m>.)".into()));
        assert_eq!(q.stages[8], Stage::Unpack);
        let Stage::Filter(filter) = &q.stages[9] else {
            panic!()
        };
        let LabelFilter::Or(left, right) = filter else {
            panic!("{filter:?}")
        };
        assert!(matches!(**left, LabelFilter::And(..)));
        assert!(matches!(**right, LabelFilter::And(..)));
        assert_eq!(q.stages[10], Stage::LineFormat("{{.m}}".into()));
        assert_eq!(
            q.stages[11],
            Stage::LabelFormat(vec![
                ("a".into(), "{{.b}}".into()),
                ("c".into(), "{{.d}}".into())
            ])
        );
        assert_eq!(q.stages[12], Stage::Drop(vec!["a".into(), "b".into()]));
        assert_eq!(q.stages[13], Stage::Keep(vec!["c".into()]));
        assert_eq!(q.stages[14], Stage::Decolorize);
        assert!(
            matches!(&q.stages[15], Stage::Unwrap { label, conversion: Conversion::Duration, .. } if label == "latency")
        );
    }

    /// QL-7: `ip()` is a label filter value for `=` and `!=`.
    #[test]
    fn ip_filters_take_a_pattern() {
        let q = log(r#"{a="b"} | logfmt | addr = ip("10.0.0.0/8") | peer != ip("::1")"#);
        assert!(matches!(
            &q.stages[1],
            Stage::Filter(LabelFilter::Compare { name, op: CompareOp::Eq, value: FilterValue::Ip(p), .. })
                if name == "addr" && p == "10.0.0.0/8"
        ));
        assert!(matches!(
            &q.stages[2],
            Stage::Filter(LabelFilter::Compare {
                op: CompareOp::NotEq,
                value: FilterValue::Ip(_),
                ..
            })
        ));
        let e = parse(r#"{a="b"} | addr > ip("10.0.0.1")"#).unwrap_err();
        assert!(e.message.contains("= or !="));
    }

    #[test]
    fn json_parameters_and_byte_filters() {
        let q = log(
            r#"{a="b"} | json code="response.code", m="method" | size > 1MiB | line_format "x""#,
        );
        assert_eq!(
            q.stages[0],
            Stage::Json(vec![
                ("code".into(), "response.code".into()),
                ("m".into(), "method".into())
            ])
        );
        let Stage::Filter(LabelFilter::Compare { value, .. }) = &q.stages[1] else {
            panic!()
        };
        assert_eq!(*value, FilterValue::Bytes(1024.0 * 1024.0));
    }

    #[test]
    fn parses_vector_matching_modifiers() {
        let m = metric(
            r#"sum by (app) (rate({a="b"}[1m])) / on(app) group_left(env) sum by (app, env) (rate({c="d"}[1m]))"#,
        );
        let MetricExpr::Binary {
            op: BinaryOp::Div,
            matching,
            ..
        } = m
        else {
            panic!("{m:?}")
        };
        assert_eq!(
            matching,
            Some(VectorMatching {
                on: true,
                labels: vec!["app".into()],
                group: Some((true, vec!["env".into()])),
            })
        );
        let m = metric(r#"rate({a="b"}[1m]) > bool ignoring(pod) rate({c="d"}[1m])"#);
        let MetricExpr::Binary {
            matching,
            bool_modifier,
            ..
        } = m
        else {
            panic!("{m:?}")
        };
        assert!(bool_modifier, "`bool` comes before the matching");
        assert_eq!(
            matching,
            Some(VectorMatching {
                on: false,
                labels: vec!["pod".into()],
                group: None,
            })
        );
        let m = metric(r#"rate({a="b"}[1m]) + rate({c="d"}[1m])"#);
        let MetricExpr::Binary {
            matching,
            bool_modifier,
            ..
        } = m
        else {
            panic!("{m:?}")
        };
        assert_eq!(matching, None);
        assert!(!bool_modifier);
    }

    #[test]
    fn parses_metric_expressions_with_precedence() {
        let m = metric(r#"sum by (app) (rate({app="x"} |= "e" [5m])) / 60 + 1"#);
        let MetricExpr::Binary {
            op: BinaryOp::Add,
            left,
            ..
        } = m
        else {
            panic!("{m:?}")
        };
        let MetricExpr::Binary {
            op: BinaryOp::Div,
            left,
            ..
        } = *left
        else {
            panic!()
        };
        let MetricExpr::Aggregate {
            op: AggregateOp::Sum,
            grouping,
            inner,
            ..
        } = *left
        else {
            panic!()
        };
        assert_eq!(
            grouping,
            Some(Grouping {
                by: true,
                labels: vec!["app".into()]
            })
        );
        let MetricExpr::Range {
            function,
            log,
            range,
            ..
        } = *inner
        else {
            panic!()
        };
        assert_eq!(function, "rate");
        assert_eq!(log.stages.len(), 1);
        assert_eq!(range, Duration::from_secs(300));

        let m = metric(r#"quantile_over_time(0.99, {a="b"} | unwrap latency [1m]) by (c)"#);
        let MetricExpr::Range {
            parameter,
            grouping,
            ..
        } = m
        else {
            panic!()
        };
        assert_eq!(parameter, Some(0.99));
        assert_eq!(
            grouping,
            Some(Grouping {
                by: true,
                labels: vec!["c".into()]
            })
        );

        let m = metric(r#"topk(3, count_over_time({a="b"}[1m])) without (x)"#);
        let MetricExpr::Aggregate {
            op: AggregateOp::Topk(3),
            grouping,
            ..
        } = m
        else {
            panic!("{m:?}")
        };
        assert_eq!(
            grouping,
            Some(Grouping {
                by: false,
                labels: vec!["x".into()]
            })
        );
    }

    #[test]
    fn errors_are_positioned() {
        let e = parse(r#"{app="x"} | json | status >"#).unwrap_err();
        assert!(e.message.starts_with("expected a value"));
        let e = parse(r#"rate({a="b"})"#).unwrap_err();
        assert_eq!(e.message, "expected \"[\", found \")\"");
        let e = parse(r#"frobnicate({a="b"}[5m])"#).unwrap_err();
        assert_eq!(e.message, "unknown function frobnicate");
        assert_eq!(e.span, Some(Span::new(0, 10)));
        assert!(matches!(
            metric("vector(1.5)"),
            MetricExpr::Vector(v) if (v - 1.5).abs() < f64::EPSILON
        ));
        assert!(matches!(
            metric("vector(1) + vector(1)"),
            MetricExpr::Binary {
                op: BinaryOp::Add,
                ..
            }
        ));
        let e = parse("vector(x)").unwrap_err();
        assert!(e.message.starts_with("expected a number"), "{}", e.message);
        let e = parse(r#"{a="b"} extra"#).unwrap_err();
        assert!(e.message.starts_with("unexpected \"extra\""));
        let e = parse(r#"{a="b"}[5m]"#).unwrap_err();
        assert!(e.message.starts_with("unexpected \"[\""));
        let MetricExpr::Range { offset, .. } = metric(r#"rate({a="b"}[5m] offset 1h)"#) else {
            panic!()
        };
        assert_eq!(offset, Duration::from_secs(3600));
        let e = parse(r#"rate({a="b"}[5m] offset)"#).unwrap_err();
        assert!(e.message.contains("duration after offset"));
    }
}
