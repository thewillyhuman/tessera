// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The contract between a query language and the engine.
//!
//! A front end turns text into a [`Statement`] over the shared plan. It
//! owns its lexer and parser (QL-2) and reports errors with positions in
//! the terms of its language.

use std::fmt;

use tessera_types::{DatasetName, TenantId, Timestamp};

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
