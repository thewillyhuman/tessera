// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Scalar expressions.
//!
//! An expression reads dedicated columns by name and attribute paths
//! (`attr.http.method`) through variant access (section 10.6): a path is
//! read from a dedicated column where a file has one and from the
//! `_overflow` column elsewhere, behind this one node. The binder gives a
//! path the type the query needs it in.

use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use arrow::datatypes::{DataType, TimeUnit};
use tessera_types::Timestamp;
use tessera_types::event::{EXTRACTED, OVERFLOW, PATTERN_ID, SCOPES, SEVERITY_TEXT};

/// `column` as a side of a join outputs it: `name.column` (DM-2).
#[must_use]
pub fn qualify(name: &str, column: &str) -> String {
    format!("{name}.{column}")
}

/// The column behind `qualified` if it is `name.column`, else `None`.
#[must_use]
pub fn unqualify<'a>(name: &str, qualified: &'a str) -> Option<&'a str> {
    qualified
        .strip_prefix(name)
        .and_then(|rest| rest.strip_prefix('.'))
}

/// The join side and attribute path of a path written `name.attr.x`:
/// the part before the scope prefix, or before the pattern id, names
/// the side. A path with no side, or whose first part is itself a
/// scope, is not qualified.
#[must_use]
pub fn split_qualifier(path: &str) -> Option<(&str, &str)> {
    let (name, rest) = path.split_once('.')?;
    let scoped = rest == PATTERN_ID || SCOPES.iter().any(|scope| rest.starts_with(scope));
    let name_is_scope = SCOPES
        .iter()
        .any(|scope| scope.trim_end_matches('.') == name);
    (scoped && !name_is_scope).then_some((name, rest))
}

/// A constant.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum Literal {
    /// SQL `NULL`; typed by context.
    Null,
    /// A boolean.
    Bool(bool),
    /// A 64-bit integer.
    Int(i64),
    /// A 64-bit float.
    Float(f64),
    /// Text.
    Text(String),
    /// A point in time.
    Timestamp(Timestamp),
    /// A length of time.
    Duration(Duration),
    /// Raw bytes, such as a trace id.
    Bytes(Vec<u8>),
}

impl Literal {
    /// The literal's type, or `None` for `NULL`.
    #[must_use]
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Literal::Null => None,
            Literal::Bool(_) => Some(DataType::Boolean),
            Literal::Int(_) => Some(DataType::Int64),
            Literal::Float(_) => Some(DataType::Float64),
            Literal::Text(_) => Some(DataType::Utf8),
            Literal::Timestamp(_) => Some(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            Literal::Duration(_) => Some(DataType::Duration(TimeUnit::Nanosecond)),
            Literal::Bytes(_) => Some(DataType::Binary),
        }
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Null => write!(f, "NULL"),
            Literal::Bool(b) => write!(f, "{b}"),
            Literal::Int(i) => write!(f, "{i}"),
            Literal::Float(x) => write!(f, "{x:?}"),
            Literal::Text(s) => write!(f, "'{}'", s.replace('\'', "''")),
            Literal::Timestamp(t) => write!(f, "TIMESTAMP '{t}'"),
            Literal::Duration(d) => write!(f, "INTERVAL '{} ns'", d.as_nanos()),
            Literal::Bytes(b) => {
                write!(f, "X'")?;
                for byte in b {
                    write!(f, "{byte:02x}")?;
                }
                write!(f, "'")
            }
        }
    }
}

/// A binary operator.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum BinaryOp {
    /// Logical and.
    And,
    /// Logical or.
    Or,
    /// `=`.
    Eq,
    /// `<>`.
    NotEq,
    /// `<`.
    Lt,
    /// `<=`.
    LtEq,
    /// `>`.
    Gt,
    /// `>=`.
    GtEq,
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
}

impl BinaryOp {
    /// Whether the operator compares its operands and yields a boolean.
    #[must_use]
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
        )
    }

    /// Whether the operator combines booleans.
    #[must_use]
    pub fn is_logical(self) -> bool {
        matches!(self, BinaryOp::And | BinaryOp::Or)
    }

    /// The operator with its operands swapped: `a < b` is `b > a`.
    #[must_use]
    pub fn flipped(self) -> Self {
        match self {
            BinaryOp::Lt => BinaryOp::Gt,
            BinaryOp::LtEq => BinaryOp::GtEq,
            BinaryOp::Gt => BinaryOp::Lt,
            BinaryOp::GtEq => BinaryOp::LtEq,
            other => other,
        }
    }

    /// The operator as written in SQL.
    #[must_use]
    pub fn symbol(self) -> &'static str {
        match self {
            BinaryOp::And => "AND",
            BinaryOp::Or => "OR",
            BinaryOp::Eq => "=",
            BinaryOp::NotEq => "<>",
            BinaryOp::Lt => "<",
            BinaryOp::LtEq => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::GtEq => ">=",
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Rem => "%",
        }
    }
}

/// A scalar expression.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub enum Expr {
    /// A dedicated column of the input, by name.
    Column(String),
    /// An attribute path read through variant access, as the given type.
    Path {
        /// The dotted path, with its scope prefix (`attr.`, `resource.`,
        /// `scope.`).
        path: String,
        /// The type the value is read as.
        data_type: DataType,
    },
    /// A Prometheus or Loki label (QL-19): a label a parser stage derived
    /// (`_extracted`), else the attribute whose path maps to `name`, the
    /// more specific scope first (QL-20), else a well-known column under
    /// its label name (`level` for the severity text).
    Label {
        /// The label name.
        name: String,
        /// The type the value is read as.
        data_type: DataType,
    },
    /// A constant.
    Literal(Literal),
    /// A binary operation.
    Binary {
        /// The operator.
        op: BinaryOp,
        /// The left operand.
        left: Box<Expr>,
        /// The right operand.
        right: Box<Expr>,
    },
    /// Logical negation.
    Not(Box<Expr>),
    /// Arithmetic negation.
    Negate(Box<Expr>),
    /// `IS NULL` or `IS NOT NULL`.
    IsNull {
        /// The tested expression.
        expr: Box<Expr>,
        /// `IS NOT NULL` when true.
        negated: bool,
    },
    /// SQL `LIKE` with `%` and `_` wildcards.
    Like {
        /// The tested text.
        expr: Box<Expr>,
        /// The pattern.
        pattern: String,
        /// `NOT LIKE` when true.
        negated: bool,
        /// `ILIKE` when true.
        case_insensitive: bool,
    },
    /// `IN (...)` over constants.
    InList {
        /// The tested expression.
        expr: Box<Expr>,
        /// The candidates.
        list: Vec<Literal>,
        /// `NOT IN` when true.
        negated: bool,
    },
    /// A scalar function call, resolved by name in the registry.
    Call {
        /// The function.
        name: String,
        /// Its arguments.
        args: Vec<Expr>,
    },
    /// A type conversion.
    Cast {
        /// The converted expression.
        expr: Box<Expr>,
        /// The target type.
        to: DataType,
    },
}

impl Expr {
    /// A column reference.
    #[must_use]
    pub fn column(name: impl Into<String>) -> Self {
        Expr::Column(name.into())
    }

    /// A constant.
    #[must_use]
    pub fn literal(value: impl Into<Literal>) -> Self {
        Expr::Literal(value.into())
    }

    /// `self <op> right`.
    #[must_use]
    pub fn binary(self, op: BinaryOp, right: Expr) -> Self {
        Expr::Binary {
            op,
            left: Box::new(self),
            right: Box::new(right),
        }
    }

    /// `self AND right`.
    #[must_use]
    pub fn and(self, right: Expr) -> Self {
        self.binary(BinaryOp::And, right)
    }

    /// `self OR right`.
    #[must_use]
    pub fn or(self, right: Expr) -> Self {
        self.binary(BinaryOp::Or, right)
    }

    /// The conjunction of `exprs`, or `None` if there are none.
    #[must_use]
    pub fn all(exprs: impl IntoIterator<Item = Expr>) -> Option<Self> {
        exprs.into_iter().reduce(Expr::and)
    }

    /// Splits a conjunction into its terms: `a AND (b AND c)` gives
    /// `[a, b, c]`. Anything else is a single term.
    #[must_use]
    pub fn conjuncts(self) -> Vec<Expr> {
        match self {
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => {
                let mut terms = left.conjuncts();
                terms.extend(right.conjuncts());
                terms
            }
            other => vec![other],
        }
    }

    /// The input columns the expression reads. A path reads the overflow
    /// column.
    #[must_use]
    pub fn references(&self) -> BTreeSet<String> {
        let mut columns = BTreeSet::new();
        self.collect_references(&mut columns);
        columns
    }

    /// The names the expression reads as written: columns by name and
    /// attributes by path, with no mapping to the columns that serve
    /// them. A label names nothing: it may come from any attribute.
    #[must_use]
    pub fn names(&self) -> BTreeSet<String> {
        let mut names = BTreeSet::new();
        self.collect_names(&mut names);
        names
    }

    /// The expression with every column name and attribute path
    /// renamed by `f`: how a join side's names gain and lose their
    /// qualifier.
    #[must_use]
    pub fn rename(self, f: &impl Fn(String) -> String) -> Expr {
        match self {
            Expr::Column(name) => Expr::Column(f(name)),
            Expr::Path { path, data_type } => Expr::Path {
                path: f(path),
                data_type,
            },
            Expr::Label { .. } | Expr::Literal(_) => self,
            Expr::Binary { op, left, right } => Expr::Binary {
                op,
                left: Box::new(left.rename(f)),
                right: Box::new(right.rename(f)),
            },
            Expr::Not(expr) => Expr::Not(Box::new(expr.rename(f))),
            Expr::Negate(expr) => Expr::Negate(Box::new(expr.rename(f))),
            Expr::IsNull { expr, negated } => Expr::IsNull {
                expr: Box::new(expr.rename(f)),
                negated,
            },
            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => Expr::Like {
                expr: Box::new(expr.rename(f)),
                pattern,
                negated,
                case_insensitive,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(expr.rename(f)),
                list,
                negated,
            },
            Expr::Call { name, args } => Expr::Call {
                name,
                args: args.into_iter().map(|a| a.rename(f)).collect(),
            },
            Expr::Cast { expr, to } => Expr::Cast {
                expr: Box::new(expr.rename(f)),
                to,
            },
        }
    }

    /// The expression with the qualifier `side` removed from every
    /// name: what a join side evaluates over its own rows.
    #[must_use]
    pub fn unqualify(self, side: &str) -> Expr {
        self.rename(&|name| unqualify(side, &name).map_or(name.clone(), str::to_owned))
    }

    /// Whether the expression reads `_overflow` as a whole: the column
    /// itself, on any join side, or a label, which may come from any
    /// attribute. A path reads one attribute, which a dedicated column
    /// serves as well.
    #[must_use]
    pub fn needs_overflow_whole(&self) -> bool {
        match self {
            Expr::Column(name) => name
                .strip_suffix(OVERFLOW)
                .is_some_and(|side| side.is_empty() || side.ends_with('.')),
            Expr::Label { .. } => true,
            Expr::Path { .. } | Expr::Literal(_) => false,
            Expr::Binary { left, right, .. } => {
                left.needs_overflow_whole() || right.needs_overflow_whole()
            }
            Expr::Not(expr)
            | Expr::Negate(expr)
            | Expr::IsNull { expr, .. }
            | Expr::Like { expr, .. }
            | Expr::InList { expr, .. }
            | Expr::Cast { expr, .. } => expr.needs_overflow_whole(),
            Expr::Call { args, .. } => args.iter().any(Expr::needs_overflow_whole),
        }
    }

    fn collect_names(&self, into: &mut BTreeSet<String>) {
        match self {
            Expr::Column(name) | Expr::Path { path: name, .. } => {
                into.insert(name.clone());
            }
            Expr::Label { .. } | Expr::Literal(_) => {}
            Expr::Binary { left, right, .. } => {
                left.collect_names(into);
                right.collect_names(into);
            }
            Expr::Not(expr)
            | Expr::Negate(expr)
            | Expr::IsNull { expr, .. }
            | Expr::Like { expr, .. }
            | Expr::InList { expr, .. }
            | Expr::Cast { expr, .. } => expr.collect_names(into),
            Expr::Call { args, .. } => {
                for arg in args {
                    arg.collect_names(into);
                }
            }
        }
    }

    fn collect_references(&self, into: &mut BTreeSet<String>) {
        match self {
            Expr::Column(name) => {
                into.insert(name.clone());
            }
            Expr::Path { path, .. } => {
                // A qualified path reads its own side's overflow.
                into.insert(match split_qualifier(path) {
                    Some((side, _)) => qualify(side, OVERFLOW),
                    None => OVERFLOW.to_owned(),
                });
            }
            Expr::Label { .. } => {
                into.insert(EXTRACTED.to_owned());
                into.insert(OVERFLOW.to_owned());
                into.insert(SEVERITY_TEXT.to_owned());
            }
            Expr::Literal(_) => {}
            Expr::Binary { left, right, .. } => {
                left.collect_references(into);
                right.collect_references(into);
            }
            Expr::Not(expr)
            | Expr::Negate(expr)
            | Expr::IsNull { expr, .. }
            | Expr::Like { expr, .. }
            | Expr::InList { expr, .. }
            | Expr::Cast { expr, .. } => expr.collect_references(into),
            Expr::Call { args, .. } => {
                for arg in args {
                    arg.collect_references(into);
                }
            }
        }
    }

    /// Whether the expression reads any input at all.
    #[must_use]
    pub fn is_constant(&self) -> bool {
        self.references().is_empty()
    }
}

impl From<bool> for Literal {
    fn from(value: bool) -> Self {
        Literal::Bool(value)
    }
}

impl From<i64> for Literal {
    fn from(value: i64) -> Self {
        Literal::Int(value)
    }
}

impl From<f64> for Literal {
    fn from(value: f64) -> Self {
        Literal::Float(value)
    }
}

impl From<&str> for Literal {
    fn from(value: &str) -> Self {
        Literal::Text(value.to_owned())
    }
}

impl From<String> for Literal {
    fn from(value: String) -> Self {
        Literal::Text(value)
    }
}

impl From<Timestamp> for Literal {
    fn from(value: Timestamp) -> Self {
        Literal::Timestamp(value)
    }
}

impl From<Duration> for Literal {
    fn from(value: Duration) -> Self {
        Literal::Duration(value)
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column(name) => write!(f, "{name}"),
            Expr::Path { path, .. } => write!(f, "{path}"),
            Expr::Label { name, .. } => write!(f, "label({name})"),
            Expr::Literal(value) => write!(f, "{value}"),
            Expr::Binary { op, left, right } => {
                write!(f, "({left} {} {right})", op.symbol())
            }
            Expr::Not(expr) => write!(f, "NOT {expr}"),
            Expr::Negate(expr) => write!(f, "-{expr}"),
            Expr::IsNull {
                expr,
                negated: false,
            } => write!(f, "{expr} IS NULL"),
            Expr::IsNull {
                expr,
                negated: true,
            } => write!(f, "{expr} IS NOT NULL"),
            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => {
                let not = if *negated { "NOT " } else { "" };
                let op = if *case_insensitive { "ILIKE" } else { "LIKE" };
                write!(f, "{expr} {not}{op} '{pattern}'")
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let not = if *negated { "NOT " } else { "" };
                let items: Vec<String> = list.iter().map(ToString::to_string).collect();
                write!(f, "{expr} {not}IN ({})", items.join(", "))
            }
            Expr::Call { name, args } => {
                let args: Vec<String> = args.iter().map(ToString::to_string).collect();
                write!(f, "{name}({})", args.join(", "))
            }
            Expr::Cast { expr, to } => write!(f, "CAST({expr} AS {to})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified_names_read_their_own_sides_overflow() {
        let path = Expr::Path {
            path: "a.attr.n".into(),
            data_type: DataType::Int64,
        };
        let expr = path.clone().binary(BinaryOp::Eq, Expr::column("s.id"));
        assert_eq!(
            expr.references(),
            BTreeSet::from(["a._overflow".to_owned(), "s.id".to_owned()])
        );
        assert_eq!(
            expr.names(),
            BTreeSet::from(["a.attr.n".to_owned(), "s.id".to_owned()])
        );
        assert_eq!(split_qualifier("a.attr.n"), Some(("a", "attr.n")));
        assert_eq!(
            split_qualifier("attr.attr.n"),
            None,
            "a scope is not a side"
        );
        assert_eq!(split_qualifier("attr.n"), None);
        assert_eq!(unqualify("a", "a.attr.n"), Some("attr.n"));
        assert_eq!(unqualify("a", "ab.x"), None);
        let stripped = expr.unqualify("a");
        assert_eq!(
            stripped.names(),
            BTreeSet::from(["attr.n".to_owned(), "s.id".to_owned()])
        );
        assert!(Expr::column("a._overflow").needs_overflow_whole());
        assert!(!Expr::column("a.body").needs_overflow_whole());
        assert!(!path.needs_overflow_whole());
    }

    #[test]
    fn conjuncts_flatten_nested_ands() {
        let a = Expr::column("a");
        let b = Expr::column("b");
        let c = Expr::column("c");
        let expr = a.clone().and(b.clone().and(c.clone()));
        assert_eq!(expr.conjuncts(), vec![a.clone(), b.clone(), c.clone()]);
        let or = a.clone().or(b);
        assert_eq!(or.clone().conjuncts(), vec![or]);
    }

    #[test]
    fn references_include_the_overflow_column_for_paths() {
        let expr = Expr::column("severity_number")
            .binary(BinaryOp::GtEq, Expr::literal(17))
            .and(
                Expr::Path {
                    path: "attr.http.method".into(),
                    data_type: DataType::Utf8,
                }
                .binary(BinaryOp::Eq, Expr::literal("GET")),
            );
        let refs: Vec<String> = expr.references().into_iter().collect();
        assert_eq!(refs, vec!["_overflow", "severity_number"]);
        assert!(Expr::literal(1).is_constant());
    }

    #[test]
    fn displays_in_sql_form() {
        let expr = Expr::column("body")
            .binary(BinaryOp::NotEq, Expr::literal("it's"))
            .and(Expr::IsNull {
                expr: Box::new(Expr::column("trace_id")),
                negated: true,
            });
        assert_eq!(
            expr.to_string(),
            "((body <> 'it''s') AND trace_id IS NOT NULL)"
        );
        let call = Expr::Call {
            name: "time_bucket".into(),
            args: vec![
                Expr::literal(Duration::from_secs(60)),
                Expr::column("timestamp"),
            ],
        };
        assert_eq!(
            call.to_string(),
            "time_bucket(INTERVAL '60000000000 ns', timestamp)"
        );
    }

    #[test]
    fn literal_types_follow_arrow() {
        assert_eq!(Literal::Null.data_type(), None);
        assert_eq!(Literal::from(1.5).data_type(), Some(DataType::Float64));
        assert_eq!(
            Literal::from(Timestamp::UNIX_EPOCH).data_type(),
            Some(DataType::Timestamp(TimeUnit::Nanosecond, None))
        );
    }

    #[test]
    fn flipping_a_comparison_swaps_its_direction() {
        assert_eq!(BinaryOp::Lt.flipped(), BinaryOp::Gt);
        assert_eq!(BinaryOp::GtEq.flipped(), BinaryOp::LtEq);
        assert_eq!(BinaryOp::Eq.flipped(), BinaryOp::Eq);
        assert!(BinaryOp::Eq.is_comparison());
        assert!(BinaryOp::And.is_logical());
    }

    #[test]
    fn labels_read_the_overflow_and_the_severity_text() {
        let label = Expr::Label {
            name: "level".into(),
            data_type: DataType::Utf8,
        };
        let refs: Vec<String> = label.references().into_iter().collect();
        assert_eq!(refs, vec!["_extracted", "_overflow", "severity_text"]);
        assert_eq!(label.to_string(), "label(level)");
        assert_eq!(Literal::Bytes(vec![0xab, 1]).to_string(), "X'ab01'");
        assert_eq!(Literal::Bytes(vec![]).data_type(), Some(DataType::Binary));
    }
}
