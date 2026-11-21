// SPDX-License-Identifier: Apache-2.0

//! The contract between a query language and the engine.
//!
//! A front end turns text into a [`Statement`] over the shared plan. It
//! owns its lexer and parser (QL-2) and reports errors with positions in
//! the terms of its language.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use tessera_types::{DatasetName, Kind, RetentionPolicy, TenantId, Timestamp};

use crate::expr::{Expr, Literal};
use crate::plan::Plan;

/// A byte range in the query text.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Span {
    /// The first byte.
    pub start: usize,
    /// One past the last byte.
    pub end: usize,
}

impl Span {
    /// The range `[start, end)`.
    #[must_use]
    pub const fn new(start: usize, end: usize) -> Self {
        Span { start, end }
    }

    /// The one-based line and column of the span's start in `text`.
    #[must_use]
    pub fn line_column(self, text: &str) -> (usize, usize) {
        let start = self.start.min(text.len());
        let before = &text[..start];
        let line = before.matches('\n').count() + 1;
        let column = before.rfind('\n').map_or(start, |nl| start - nl - 1) + 1;
        (line, column)
    }
}

/// An error in a query, located in its text.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub struct Diagnostic {
    /// What is wrong, in the terms of the language.
    pub message: String,
    /// Where, if the error has a location.
    pub span: Option<Span>,
}

impl Diagnostic {
    /// An error at `span`.
    #[must_use]
    pub fn at(span: Span, message: impl Into<String>) -> Self {
        Diagnostic {
            message: message.into(),
            span: Some(span),
        }
    }

    /// An error without a location.
    #[must_use]
    pub fn unlocated(message: impl Into<String>) -> Self {
        Diagnostic {
            message: message.into(),
            span: None,
        }
    }

    /// The message with its line and column in `text`.
    #[must_use]
    pub fn describe(&self, text: &str) -> String {
        match self.span {
            Some(span) => {
                let (line, column) = span.line_column(text);
                format!("{} at line {line}, column {column}", self.message)
            }
            None => self.message.clone(),
        }
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.span {
            Some(span) => write!(f, "{} at byte {}", self.message, span.start),
            None => write!(f, "{}", self.message),
        }
    }
}

/// What a query runs under: who asks, where unqualified names resolve
/// and what time it is.
#[derive(Clone, Debug)]
pub struct BindContext {
    /// The tenant the query reads.
    pub tenant: TenantId,
    /// The dataset an unqualified table name refers to.
    pub dataset: DatasetName,
    /// The query's notion of now, fixed once per query.
    pub now: Timestamp,
    /// The tenants the query may name as `tenant.dataset.table` (CT-9),
    /// by name: the ones its principal has grants in. Empty for a query
    /// confined to its own tenant.
    pub tenants: BTreeMap<String, TenantId>,
    /// The unit prices usage is costed with (CO-4), by the settings that
    /// name them; a price nobody set is zero.
    pub prices: BTreeMap<String, f64>,
}

/// The settings that price usage (CO-4): per byte received, stored
/// and scanned, per millisecond of query CPU and per object-store
/// request.
pub const PRICE_SETTINGS: [&str; 5] = [
    "price.ingest_byte",
    "price.stored_byte",
    "price.scanned_byte",
    "price.cpu_ms",
    "price.request",
];

impl BindContext {
    /// A context confined to `tenant`.
    #[must_use]
    pub fn new(tenant: TenantId, dataset: DatasetName, now: Timestamp) -> Self {
        BindContext {
            tenant,
            dataset,
            now,
            tenants: BTreeMap::new(),
            prices: BTreeMap::new(),
        }
    }

    /// The tenant a query names, if it may.
    #[must_use]
    pub fn tenant_named(&self, name: &str) -> Option<&TenantId> {
        self.tenants.get(name)
    }

    /// The unit price a setting names, zero when nobody set it.
    #[must_use]
    pub fn price(&self, setting: &str) -> f64 {
        self.prices.get(setting).copied().unwrap_or(0.0)
    }
}

/// A lowered statement.
#[derive(Clone, PartialEq, Debug)]
pub enum Statement {
    /// A query to run.
    Query(Plan),
    /// `EXPLAIN` of a query (QE-4): show the plan, and run it when
    /// `analyze` is set.
    Explain {
        /// The explained plan.
        plan: Plan,
        /// Whether to run the plan and report what it did.
        analyze: bool,
    },
    /// A statement that changes or describes the tenant's control store
    /// rather than reading data (section 9.5, CS-3).
    Control(ControlStatement),
}

/// A control-plane statement. The front end checks its shape; the node
/// that holds the control store carries it out.
#[derive(Clone, PartialEq, Debug)]
pub enum ControlStatement {
    /// `CREATE ALERT ... AS <query>` (section 11.2).
    CreateAlert(AlertDefinition),
    /// `DROP ALERT name`.
    DropAlert {
        /// The rule.
        name: String,
    },
    /// `EXPLAIN ALERT name`: which evaluation mode applies and why
    /// (CQ-8).
    ExplainAlert {
        /// The rule.
        name: String,
    },
    /// `CREATE ROLLUP ... AS <query>` (section 8.7).
    CreateRollup(RollupDefinition),
    /// `DROP ROLLUP name`.
    DropRollup {
        /// The rollup.
        name: String,
    },
    /// `EXPLAIN LAYOUT [dataset]`: the dataset's layout, the alternatives
    /// the advisor considered and the queries that drove it (AD-11).
    ExplainLayout {
        /// The dataset.
        dataset: DatasetName,
    },
    /// `CREATE RETENTION POLICY ... ON dataset AS WHEN ... THEN ...`
    /// (section 13.3).
    CreateRetentionPolicy(RetentionPolicy),
    /// `DROP RETENTION POLICY name`.
    DropRetentionPolicy {
        /// The policy.
        name: String,
    },
    /// `EXPLAIN RETENTION name`: what the policy would remove and save
    /// (CO-9).
    ExplainRetention {
        /// The policy.
        name: String,
    },
    /// `INSERT INTO system.<table> (columns) VALUES (...), ...`.
    Insert {
        /// The control table.
        table: String,
        /// The rows, each a column name to a literal.
        rows: Vec<Vec<(String, Literal)>>,
    },
    /// `UPDATE system.<table> SET ... WHERE column = literal AND ...`.
    Update {
        /// The control table.
        table: String,
        /// The columns to set.
        set: Vec<(String, Literal)>,
        /// The equalities every changed row satisfies; empty for all.
        filter: Vec<(String, Literal)>,
    },
    /// `DELETE FROM <table> WHERE predicate` on a dataset's rows (SE-5):
    /// a tombstone that hides the matching rows at once and has them
    /// removed by compaction.
    DeleteRows {
        /// The dataset.
        dataset: DatasetName,
        /// The kind of event the table holds.
        kind: Kind,
        /// The predicate, lowered against the kind's schema.
        predicate: Expr,
        /// The predicate as written.
        text: String,
    },
    /// `DELETE FROM system.<table> WHERE column = literal AND ...`.
    Delete {
        /// The control table.
        table: String,
        /// The equalities every removed row satisfies; empty for all.
        filter: Vec<(String, Literal)>,
    },
}

/// What `CREATE ALERT` said.
#[derive(Clone, PartialEq, Debug)]
pub struct AlertDefinition {
    /// The rule's name.
    pub name: String,
    /// `SEVERITY`, if given.
    pub severity: Option<String>,
    /// `FOR`, if given.
    pub hold: Option<Duration>,
    /// `EVERY`, if given.
    pub every: Option<Duration>,
    /// `NOTIFY` routes.
    pub notify: Vec<String>,
    /// `LABELS`.
    pub labels: Vec<(String, String)>,
    /// `ANNOTATIONS`.
    pub annotations: Vec<(String, String)>,
    /// The query, as written, in the statement's language.
    pub query: String,
    /// The dataset an unqualified table in the query means.
    pub dataset: DatasetName,
}

/// What `CREATE ROLLUP` said.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RollupDefinition {
    /// The rollup's name.
    pub name: String,
    /// `EVERY`, if given.
    pub every: Option<Duration>,
    /// `INTO`, the derived dataset, if given.
    pub target: Option<DatasetName>,
    /// `BACKFILL`, if given: how far back the first rounds reach.
    pub backfill: Option<Duration>,
    /// The query, as written.
    pub query: String,
    /// The dataset an unqualified table in the query means.
    pub dataset: DatasetName,
}

/// A front end: text in one language to the shared plan (section 18.2).
pub trait Frontend: Send + Sync {
    /// Lowers `text`.
    ///
    /// # Errors
    ///
    /// The first error found, with its position.
    fn lower(&self, text: &str, ctx: &BindContext) -> Result<Statement, Diagnostic>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spans_locate_lines_and_columns() {
        let text = "SELECT *\nFROM logs\nWHERE x";
        assert_eq!(Span::new(0, 6).line_column(text), (1, 1));
        assert_eq!(Span::new(9, 13).line_column(text), (2, 1));
        assert_eq!(Span::new(25, 26).line_column(text), (3, 7));
        assert_eq!(Span::new(999, 999).line_column(text), (3, 8));
    }

    #[test]
    fn diagnostics_describe_their_position() {
        let text = "SELECT *\nFROM nowhere";
        let error = Diagnostic::at(Span::new(14, 21), "unknown table nowhere");
        assert_eq!(
            error.describe(text),
            "unknown table nowhere at line 2, column 6"
        );
        assert_eq!(Diagnostic::unlocated("no").describe(text), "no");
    }
}
