// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The syntax tree of a `PromQL` expression (section 9.3).

use std::time::Duration;

use tessera_ir::Span;

/// A label matcher operator.
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

/// A label matcher.
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

/// The `@` modifier.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum At {
    /// A Unix time in seconds.
    Time(f64),
    /// `start()`.
    Start,
    /// `end()`.
    End,
}

/// A binary operator.
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
    Mod,
    /// `^`.
    Pow,
    /// `atan2`.
    Atan2,
    /// `==`.
    Eq,
    /// `!=`.
    NotEq,
    /// `>`.
    Gt,
    /// `<`.
    Lt,
    /// `>=`.
    GtEq,
    /// `<=`.
    LtEq,
    /// `and`.
    And,
    /// `or`.
    Or,
    /// `unless`.
    Unless,
}

/// `on`/`ignoring` with an optional `group_left`/`group_right`.
#[derive(Clone, PartialEq, Debug)]
pub struct VectorMatching {
    /// `on` when true, `ignoring` otherwise.
    pub on: bool,
    /// The labels.
    pub labels: Vec<String>,
    /// `group_left` (true) or `group_right` (false), with the included
    /// labels, when written.
    pub group: Option<(bool, Vec<String>)>,
}

/// `by` or `without`.
#[derive(Clone, PartialEq, Debug)]
pub struct Grouping {
    /// `by` when true, `without` otherwise.
    pub by: bool,
    /// The labels.
    pub labels: Vec<String>,
}

/// An expression.
#[derive(Clone, PartialEq, Debug)]
pub enum Expr {
    /// A number.
    Number(f64),
    /// A string.
    String(String),
    /// A vector selector, with its range when a range vector.
    Selector {
        /// The metric name, when written outside the braces.
        name: Option<String>,
        /// The matchers.
        matchers: Vec<Matcher>,
        /// The range of a range vector.
        range: Option<Duration>,
        /// The offset, negative when written `offset -5m`.
        offset: Option<(bool, Duration)>,
        /// The `@` modifier.
        at: Option<At>,
        /// Where it was written.
        span: Span,
    },
    /// `expr[range:step]`.
    Subquery {
        /// The expression.
        expr: Box<Expr>,
        /// The range.
        range: Duration,
        /// The step, when written.
        step: Option<Duration>,
        /// The offset, negative when written `offset -5m`.
        offset: Option<(bool, Duration)>,
        /// The `@` modifier.
        at: Option<At>,
        /// Where it was written.
        span: Span,
    },
    /// `-expr` or `+expr`.
    Unary {
        /// Whether the sign is minus.
        negative: bool,
        /// The operand.
        expr: Box<Expr>,
    },
    /// `left op right`.
    Binary {
        /// The operator.
        op: BinaryOp,
        /// The left operand.
        left: Box<Expr>,
        /// The right operand.
        right: Box<Expr>,
        /// How vectors match.
        matching: Option<VectorMatching>,
        /// The `bool` modifier.
        bool_modifier: bool,
        /// The operator's text.
        span: Span,
    },
    /// An aggregation.
    Aggregate {
        /// The operator, as written.
        op: String,
        /// The vector.
        expr: Box<Expr>,
        /// The parameter of `topk`, `bottomk`, `quantile` and
        /// `count_values`.
        param: Option<Box<Expr>>,
        /// `by` or `without`.
        grouping: Option<Grouping>,
        /// The operator's text.
        span: Span,
    },
    /// A function call.
    Call {
        /// The function.
        function: String,
        /// The arguments.
        args: Vec<Expr>,
        /// The call's text.
        span: Span,
    },
    /// `(expr)`.
    Paren(Box<Expr>),
}

impl Expr {
    /// Where the expression was written, for the forms that keep it.
    #[must_use]
    pub fn span(&self) -> Option<Span> {
        match self {
            Expr::Selector { span, .. }
            | Expr::Subquery { span, .. }
            | Expr::Binary { span, .. }
            | Expr::Aggregate { span, .. }
            | Expr::Call { span, .. } => Some(*span),
            Expr::Unary { expr, .. } | Expr::Paren(expr) => expr.span(),
            Expr::Number(_) | Expr::String(_) => None,
        }
    }
}
