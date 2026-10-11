// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The syntax tree of a statement, with the span of every node so that
//! lowering can point at what it rejects.

use tessera_ir::Span;

/// A parsed statement.
#[derive(Clone, PartialEq, Debug)]
pub enum Statement {
    /// `SELECT ...`.
    Select(Query),
    /// `EXPLAIN [ANALYZE] SELECT ...`.
    Explain {
        /// Whether to run the query as well.
        analyze: bool,
        /// The explained query.
        query: Query,
    },
    /// `CREATE ALERT ...`.
    CreateAlert(Box<CreateAlert>),
    /// `DROP ALERT name`.
    DropAlert {
        /// The rule.
        name: String,
    },
    /// `EXPLAIN ALERT name`.
    ExplainAlert {
        /// The rule.
        name: String,
    },
    /// `EXPLAIN LAYOUT [dataset]`.
    ExplainLayout {
        /// The dataset, if named.
        dataset: Option<String>,
    },
    /// `CREATE RETENTION POLICY name ON dataset AS clauses`.
    CreateRetentionPolicy(CreateRetentionPolicy),
    /// `DROP RETENTION POLICY name`.
    DropRetentionPolicy {
        /// The policy.
        name: String,
    },
    /// `EXPLAIN RETENTION name`.
    ExplainRetention {
        /// The policy.
        name: String,
    },
    /// `INSERT INTO table (columns) VALUES (...), ...`.
    Insert {
        /// The table.
        table: TableRef,
        /// The columns, in the order of the values.
        columns: Vec<String>,
        /// The rows of literals.
        rows: Vec<Vec<Expr>>,
    },
    /// `UPDATE table SET column = literal, ... [WHERE ...]`.
    Update {
        /// The table.
        table: TableRef,
        /// The assignments.
        set: Vec<(String, Expr)>,
        /// The equalities of `WHERE`, joined by `AND`.
        filter: Vec<(String, Expr)>,
    },
    /// `DELETE FROM table [WHERE ...]`.
    Delete {
        /// The table.
        table: TableRef,
        /// The equalities of `WHERE`, joined by `AND`.
        filter: Vec<(String, Expr)>,
    },
}

/// `CREATE ALERT name [SEVERITY s] [FOR d] [EVERY d] [NOTIFY 'r', ...]
/// [LABELS (k = 'v', ...)] [ANNOTATIONS (k = 'v', ...)] AS query`.
#[derive(Clone, PartialEq, Debug)]
pub struct CreateAlert {
    /// The rule's name.
    pub name: String,
    /// `SEVERITY`.
    pub severity: Option<String>,
    /// `FOR`, as written.
    pub hold: Option<(String, Span)>,
    /// `EVERY`, as written.
    pub every: Option<(String, Span)>,
    /// `NOTIFY` routes.
    pub notify: Vec<String>,
    /// `LABELS`.
    pub labels: Vec<(String, String)>,
    /// `ANNOTATIONS`.
    pub annotations: Vec<(String, String)>,
    /// The query.
    pub query: Query,
    /// The query's text, as written.
    pub query_text: String,
}

/// A `SELECT` query.
#[derive(Clone, PartialEq, Debug)]
pub struct Query {
    /// The select list.
    pub items: Vec<SelectItem>,
    /// `FROM`, if any.
    pub from: Option<TableRef>,
    /// `WHERE`, if any.
    pub filter: Option<Expr>,
    /// `GROUP BY` expressions.
    pub group_by: Vec<Expr>,
    /// `HAVING`, if any: a condition on the groups.
    pub having: Option<Expr>,
    /// `ORDER BY` items.
    pub order_by: Vec<OrderItem>,
    /// `LIMIT`, if any.
    pub limit: Option<usize>,
    /// `OFFSET`, if any.
    pub offset: Option<usize>,
}

/// One item of the select list.
#[derive(Clone, PartialEq, Debug)]
pub enum SelectItem {
    /// `*`.
    Wildcard,
    /// An expression, with an alias if written.
    Expr {
        /// The expression.
        expr: Expr,
        /// `AS name`.
        alias: Option<String>,
    },
}

/// A table in `FROM`: a kind of event in a dataset.
#[derive(Clone, PartialEq, Debug)]
pub struct TableRef {
    /// The dataset, if qualified.
    pub dataset: Option<String>,
    /// The table name: `logs`, `spans`, ...
    pub table: String,
    /// Where it was written.
    pub span: Span,
}

/// One `ORDER BY` item.
#[derive(Clone, PartialEq, Debug)]
pub struct OrderItem {
    /// The sorted expression.
    pub expr: Expr,
    /// `DESC` when true.
    pub descending: bool,
}

/// An expression with its span.
#[derive(Clone, PartialEq, Debug)]
pub struct Expr {
    /// The expression.
    pub kind: ExprKind,
    /// Where it was written.
    pub span: Span,
}

impl Expr {
    /// Whether two expressions are the same text in different places:
    /// equal in everything but their spans. `GROUP BY` matches select
    /// items this way.
    #[must_use]
    pub fn same_as(&self, other: &Expr) -> bool {
        self.without_spans() == other.without_spans()
    }

    fn without_spans(&self) -> Expr {
        let kind = match &self.kind {
            ExprKind::Binary { op, left, right } => ExprKind::Binary {
                op: *op,
                left: Box::new(left.without_spans()),
                right: Box::new(right.without_spans()),
            },
            ExprKind::Not(e) => ExprKind::Not(Box::new(e.without_spans())),
            ExprKind::Negate(e) => ExprKind::Negate(Box::new(e.without_spans())),
            ExprKind::IsNull { expr, negated } => ExprKind::IsNull {
                expr: Box::new(expr.without_spans()),
                negated: *negated,
            },
            ExprKind::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => ExprKind::Like {
                expr: Box::new(expr.without_spans()),
                pattern: pattern.clone(),
                negated: *negated,
                case_insensitive: *case_insensitive,
            },
            ExprKind::InList {
                expr,
                list,
                negated,
            } => ExprKind::InList {
                expr: Box::new(expr.without_spans()),
                list: list.iter().map(Expr::without_spans).collect(),
                negated: *negated,
            },
            ExprKind::Between {
                expr,
                low,
                high,
                negated,
            } => ExprKind::Between {
                expr: Box::new(expr.without_spans()),
                low: Box::new(low.without_spans()),
                high: Box::new(high.without_spans()),
                negated: *negated,
            },
            ExprKind::Call {
                name,
                args,
                wildcard,
            } => ExprKind::Call {
                name: name.clone(),
                args: args.iter().map(Expr::without_spans).collect(),
                wildcard: *wildcard,
            },
            ExprKind::Cast { expr, type_name } => ExprKind::Cast {
                expr: Box::new(expr.without_spans()),
                type_name: type_name.clone(),
            },
            leaf => leaf.clone(),
        };
        Expr {
            kind,
            span: Span::default(),
        }
    }
}

/// A binary operator as written.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BinaryOp {
    /// `AND`.
    And,
    /// `OR`.
    Or,
    /// `=`.
    Eq,
    /// `<>` or `!=`.
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

/// The kinds of expression.
#[derive(Clone, PartialEq, Debug)]
pub enum ExprKind {
    /// A possibly dotted name: `body`, `attr.http.method`, `t.body`.
    Name(Vec<String>),
    /// A number, as written.
    Number(String),
    /// A string literal.
    String(String),
    /// `TRUE` or `FALSE`.
    Bool(bool),
    /// `NULL`.
    Null,
    /// `TIMESTAMP '...'`.
    Timestamp(String),
    /// `INTERVAL '...'`.
    Interval(String),
    /// A binary operation.
    Binary {
        /// The operator.
        op: BinaryOp,
        /// The left operand.
        left: Box<Expr>,
        /// The right operand.
        right: Box<Expr>,
    },
    /// `NOT expr`.
    Not(Box<Expr>),
    /// `-expr`.
    Negate(Box<Expr>),
    /// `expr IS [NOT] NULL`.
    IsNull {
        /// The tested expression.
        expr: Box<Expr>,
        /// `IS NOT NULL` when true.
        negated: bool,
    },
    /// `expr [NOT] LIKE | ILIKE 'pattern'`.
    Like {
        /// The tested expression.
        expr: Box<Expr>,
        /// The pattern.
        pattern: String,
        /// `NOT` when true.
        negated: bool,
        /// `ILIKE` when true.
        case_insensitive: bool,
    },
    /// `expr [NOT] IN (...)`.
    InList {
        /// The tested expression.
        expr: Box<Expr>,
        /// The candidates.
        list: Vec<Expr>,
        /// `NOT` when true.
        negated: bool,
    },
    /// `expr [NOT] BETWEEN low AND high`.
    Between {
        /// The tested expression.
        expr: Box<Expr>,
        /// The lower bound, inclusive.
        low: Box<Expr>,
        /// The upper bound, inclusive.
        high: Box<Expr>,
        /// `NOT` when true.
        negated: bool,
    },
    /// A function call; `count(*)` has `wildcard` set and no arguments.
    Call {
        /// The function name, lowercased.
        name: String,
        /// The arguments.
        args: Vec<Expr>,
        /// `f(*)`.
        wildcard: bool,
    },
    /// `CAST(expr AS type)` or `expr::type`.
    Cast {
        /// The converted expression.
        expr: Box<Expr>,
        /// The type name, lowercased.
        type_name: String,
    },
}

/// `CREATE RETENTION POLICY name ON dataset AS WHEN cond THEN actions
/// [WHEN ...] [ELSE actions]`.
#[derive(Clone, PartialEq, Debug)]
pub struct CreateRetentionPolicy {
    /// The policy's name.
    pub name: String,
    /// The dataset.
    pub dataset: String,
    /// The clauses in order.
    pub clauses: Vec<RetentionClauseAst>,
}

/// One `WHEN ... THEN ...` or `ELSE ...` of a retention policy.
#[derive(Clone, PartialEq, Debug)]
pub struct RetentionClauseAst {
    /// The condition and its text, none for `ELSE`.
    pub condition: Option<(Expr, String)>,
    /// The actions.
    pub actions: Vec<RetentionActionAst>,
}

/// One action of a retention clause, durations as written.
#[derive(Clone, PartialEq, Debug)]
pub enum RetentionActionAst {
    /// `SAMPLE rate AFTER duration`.
    Sample {
        /// The fraction kept, as written.
        rate: (String, Span),
        /// The age.
        after: (String, Span),
    },
    /// `DROP AFTER duration`.
    Drop {
        /// The age.
        after: (String, Span),
    },
    /// `DOWNSAMPLE TO duration AFTER duration`.
    Downsample {
        /// The interval.
        interval: (String, Span),
        /// The age.
        after: (String, Span),
    },
}
