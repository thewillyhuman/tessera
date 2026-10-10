// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The syntax tree of a `TraceQL` query (section 9.5).

use std::fmt;
use std::time::Duration;

use tessera_ir::Span;

/// A query: a spanset pipeline, optionally turned into metrics.
#[derive(Clone, PartialEq, Debug)]
pub struct Query {
    /// The pipeline.
    pub pipeline: Pipeline,
    /// The metrics stage, when the query is a metrics query.
    pub metrics: Option<Metrics>,
}

/// A spanset expression followed by pipeline stages.
#[derive(Clone, PartialEq, Debug)]
pub struct Pipeline {
    /// The first expression.
    pub first: SpansetExpr,
    /// The stages after it, in order.
    pub stages: Vec<Stage>,
}

/// An expression over the spans of a trace.
#[derive(Clone, PartialEq, Debug)]
pub enum SpansetExpr {
    /// `{ condition }`, or `{ }` for every span.
    Filter {
        /// The condition.
        condition: Option<FieldExpr>,
        /// The braces.
        span: Span,
    },
    /// `A && B`.
    And(Box<SpansetExpr>, Box<SpansetExpr>),
    /// `A || B`.
    Or(Box<SpansetExpr>, Box<SpansetExpr>),
    /// `A op B` for a structural operator.
    Structural {
        /// The left side.
        left: Box<SpansetExpr>,
        /// The operator.
        op: StructuralOp,
        /// The right side.
        right: Box<SpansetExpr>,
    },
}

/// A structural operator, as written.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct StructuralOp {
    /// The relation: `>>`, `>`, `~`, `<<` or `<`.
    pub relation: &'static str,
    /// `!` prefix.
    pub negated: bool,
    /// `&` prefix.
    pub union: bool,
}

/// A pipeline stage after `|`.
#[derive(Clone, PartialEq, Debug)]
pub enum Stage {
    /// A spanset expression applied to the spans so far.
    Spanset(SpansetExpr),
    /// `count() > 3`, `avg(duration) > 1s`.
    Aggregate {
        /// The function.
        function: String,
        /// The value aggregated; none for `count`.
        arg: Option<FieldExpr>,
        /// The comparison.
        op: &'static str,
        /// The threshold.
        value: Static,
        /// The stage's text.
        span: Span,
    },
    /// `select(a, b)`.
    Select(Vec<FieldExpr>),
    /// `by(a)`.
    By(FieldExpr),
    /// `coalesce()`.
    Coalesce,
}

/// A metrics stage: `rate()`, `count_over_time()`, `quantile_over_time(
/// duration, 0.9)` and the like, with an optional `by (...)`.
#[derive(Clone, PartialEq, Debug)]
pub struct Metrics {
    /// The function.
    pub function: String,
    /// The value aggregated, for the functions that take one.
    pub arg: Option<FieldExpr>,
    /// The quantiles of `quantile_over_time`.
    pub quantiles: Vec<f64>,
    /// The series labels.
    pub by: Vec<FieldExpr>,
    /// The stage's text.
    pub span: Span,
}

/// A literal.
#[derive(Clone, PartialEq, Debug)]
pub enum Static {
    /// A string.
    String(String),
    /// An integer.
    Int(i64),
    /// A float.
    Float(f64),
    /// `true` or `false`.
    Bool(bool),
    /// `nil`.
    Nil,
    /// A duration.
    Duration(Duration),
    /// A status: `ok`, `error` or `unset`.
    Status(String),
    /// A span kind: `server`, `client`, `producer`, `consumer`,
    /// `internal` or `unspecified`.
    Kind(String),
}

impl fmt::Display for Static {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Static::String(s) => write!(f, "{s:?}"),
            Static::Int(i) => write!(f, "{i}"),
            Static::Float(x) => write!(f, "{x}"),
            Static::Bool(b) => write!(f, "{b}"),
            Static::Nil => write!(f, "nil"),
            Static::Duration(d) => write!(f, "{}ns", d.as_nanos()),
            Static::Status(s) | Static::Kind(s) => write!(f, "{s}"),
        }
    }
}

/// A value read from a span.
#[derive(Clone, PartialEq, Debug)]
pub enum FieldExpr {
    /// A literal.
    Static(Static),
    /// An intrinsic: `duration`, `name`, `span:kind`, `trace:rootName`,
    /// ... as written, with its scope when written with one.
    Intrinsic {
        /// The scope (`span`, `trace`, ...), when written.
        scope: Option<String>,
        /// The intrinsic's name.
        name: String,
        /// Its text.
        span: Span,
    },
    /// An attribute: `.foo`, `span.foo`, `resource.foo`.
    Attribute {
        /// The scope, when written.
        scope: Option<String>,
        /// The attribute name.
        name: String,
        /// Its text.
        span: Span,
    },
    /// `left op right` for a comparison, logical or arithmetic operator.
    Binary {
        /// The operator, as written.
        op: &'static str,
        /// The left operand.
        left: Box<FieldExpr>,
        /// The right operand.
        right: Box<FieldExpr>,
        /// The operator's text.
        span: Span,
    },
    /// `!expr`.
    Not(Box<FieldExpr>),
    /// `-expr`.
    Negate(Box<FieldExpr>),
}

impl fmt::Display for FieldExpr {
    /// The expression as `TraceQL` writes it, for labels.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldExpr::Static(value) => write!(f, "{value}"),
            FieldExpr::Intrinsic {
                scope: Some(scope),
                name,
                ..
            } => write!(f, "{scope}:{name}"),
            FieldExpr::Intrinsic { name, .. } => write!(f, "{name}"),
            FieldExpr::Attribute {
                scope: Some(scope),
                name,
                ..
            } => write!(f, "{scope}.{name}"),
            FieldExpr::Attribute { name, .. } => write!(f, ".{name}"),
            FieldExpr::Binary {
                op, left, right, ..
            } => write!(f, "({left} {op} {right})"),
            FieldExpr::Not(inner) => write!(f, "!{inner}"),
            FieldExpr::Negate(inner) => write!(f, "-{inner}"),
        }
    }
}
