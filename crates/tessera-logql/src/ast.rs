// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The syntax tree of a `LogQL` query, with spans for diagnostics.

use std::time::Duration;

use tessera_ir::Span;

/// A stream selector matcher operator.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MatchOp {
    /// `=`.
    Eq,
    /// `!=`.
    NotEq,
    /// `=~`.
    Regex,
    /// `!~`.
    NotRegex,
}

/// `name op "value"`.
#[derive(Clone, PartialEq, Debug)]
pub struct Matcher {
    /// The label.
    pub name: String,
    /// The operator.
    pub op: MatchOp,
    /// The value or pattern.
    pub value: String,
    /// Where it was written.
    pub span: Span,
}

/// A line filter operator.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LineOp {
    /// `|=`.
    Contains,
    /// `!=`.
    NotContains,
    /// `|~`.
    Regex,
    /// `!~`.
    NotRegex,
}

/// A comparison in a label filter.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompareOp {
    /// `=` or `==`.
    Eq,
    /// `!=`.
    NotEq,
    /// `=~`.
    Regex,
    /// `!~`.
    NotRegex,
    /// `>`.
    Gt,
    /// `>=`.
    Gte,
    /// `<`.
    Lt,
    /// `<=`.
    Lte,
}

/// The right side of a label filter.
#[derive(Clone, PartialEq, Debug)]
pub enum FilterValue {
    /// A string.
    Text(String),
    /// A number.
    Number(f64),
    /// A duration, compared in seconds.
    Duration(Duration),
    /// A byte size, compared in bytes.
    Bytes(f64),
}

/// A label filter expression.
#[derive(Clone, PartialEq, Debug)]
pub enum LabelFilter {
    /// `name op value`.
    Compare {
        /// The label.
        name: String,
        /// The operator.
        op: CompareOp,
        /// The value.
        value: FilterValue,
        /// Where it was written.
        span: Span,
    },
    /// Both hold.
    And(Box<LabelFilter>, Box<LabelFilter>),
    /// Either holds.
    Or(Box<LabelFilter>, Box<LabelFilter>),
}

/// How an unwrapped label is read.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Conversion {
    /// As a number.
    None,
    /// `duration(x)` or `duration_seconds(x)`: as seconds.
    Duration,
    /// `bytes(x)`: as bytes.
    Bytes,
}

/// One stage of a log pipeline.
#[derive(Clone, PartialEq, Debug)]
pub enum Stage {
    /// A line filter with its alternatives (`|= "a" or "b"`).
    Line {
        /// The operator.
        op: LineOp,
        /// The patterns, any of which matches.
        patterns: Vec<String>,
        /// Where it was written.
        span: Span,
    },
    /// `| json`, with optional `label="path"` extractions.
    Json(Vec<(String, String)>),
    /// `| logfmt`.
    Logfmt,
    /// `| regexp "..."`.
    Regexp(String),
    /// `| pattern "..."`.
    Pattern(String),
    /// `| unpack`.
    Unpack,
    /// A label filter.
    Filter(LabelFilter),
    /// `| line_format "..."`.
    LineFormat(String),
    /// `| label_format a="...", b=c`.
    LabelFormat(Vec<(String, String)>),
    /// `| drop a, b`.
    Drop(Vec<String>),
    /// `| keep a, b`.
    Keep(Vec<String>),
    /// `| decolorize`.
    Decolorize,
    /// `| unwrap x`, with an optional conversion.
    Unwrap {
        /// The label.
        label: String,
        /// How it is read.
        conversion: Conversion,
        /// Where it was written.
        span: Span,
    },
}

/// A log query: a selector and its pipeline.
#[derive(Clone, PartialEq, Debug)]
pub struct LogQuery {
    /// The stream selector.
    pub matchers: Vec<Matcher>,
    /// The stages, in order.
    pub stages: Vec<Stage>,
    /// Where it was written.
    pub span: Span,
}

/// `by (...)` or `without (...)`.
#[derive(Clone, PartialEq, Debug)]
pub struct Grouping {
    /// `by` when true, `without` otherwise.
    pub by: bool,
    /// The labels.
    pub labels: Vec<String>,
}

/// An aggregation operator over a metric vector.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum AggregateOp {
    /// `sum`.
    Sum,
    /// `avg`.
    Avg,
    /// `min`.
    Min,
    /// `max`.
    Max,
    /// `count`.
    Count,
    /// `stddev`.
    Stddev,
    /// `stdvar`.
    Stdvar,
    /// `topk(k, ...)`.
    Topk(usize),
    /// `bottomk(k, ...)`.
    Bottomk(usize),
}

/// A binary operator between vectors or scalars.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BinaryOp {
    /// `+`.
    Add,
    /// `-`.
    Sub,
    /// `*`.
    Mul,
    /// `/`.
    Div,
    /// `%`.
    Rem,
    /// `==`.
    Eq,
    /// `!=`.
    NotEq,
    /// `>`.
    Gt,
    /// `>=`.
    Gte,
    /// `<`.
    Lt,
    /// `<=`.
    Lte,
}

/// A metric query.
#[derive(Clone, PartialEq, Debug)]
pub enum MetricExpr {
    /// A range function over a log query.
    Range {
        /// The function name, as written.
        function: String,
        /// A parameter, for `quantile_over_time`.
        parameter: Option<f64>,
        /// The log query.
        log: LogQuery,
        /// The window.
        range: Duration,
        /// Grouping applied to the series before the window.
        grouping: Option<Grouping>,
        /// Where it was written.
        span: Span,
    },
    /// An aggregation over a vector.
    Aggregate {
        /// The operator.
        op: AggregateOp,
        /// The grouping.
        grouping: Option<Grouping>,
        /// The aggregated expression.
        inner: Box<MetricExpr>,
        /// Where it was written.
        span: Span,
    },
    /// A binary operation.
    Binary {
        /// The operator.
        op: BinaryOp,
        /// The left side.
        left: Box<MetricExpr>,
        /// The right side.
        right: Box<MetricExpr>,
        /// Where it was written.
        span: Span,
    },
    /// A number.
    Scalar(f64),
}

/// A parsed query.
#[derive(Clone, PartialEq, Debug)]
pub enum Query {
    /// Streams.
    Log(LogQuery),
    /// A metric.
    Metric(MetricExpr),
}
