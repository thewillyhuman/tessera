// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `LogQL` front end (spec section 9.4): a hand-written lexer and
//! parser with positioned errors (QL-2) and lowering into the shared
//! plan (QL-1).
//!
//! A stream selector matches any attribute of a log event under the
//! label-name mapping of section 9.8 (QL-8); line filters become the
//! text predicates the term and trigram indexes serve (QL-9); parser
//! stages become `Extract` nodes and formatting stages `Format` nodes;
//! metric queries wrap the log plan in a `RangeVector`.
//!
//! Two vectors combine through a join on the step and the label set,
//! narrowed by `on` or `ignoring` and widened again by `group_left` or
//! `group_right`; several matches on one side multiply where Loki
//! fails the query.
//!
//! Deviations from Loki found by the differential suite (QL-4):
//!
//! - A double-quoted string keeps an unknown escape such as `\d` as the
//!   escaped character; Loki rejects it. Regular expressions are best
//!   written as raw strings in backticks, which both accept.
//! - A line a parser stage cannot read keeps the `__error__` label with
//!   Loki's error names (`JSONParserErr`, `LogfmtParserErr`), but the
//!   text of `__error_details__` is this implementation's own.
//! - Loki may add `detected_level` and `service_name` labels at ingest
//!   when configured to; Tessera adds no labels of its own.

pub mod ast;
pub mod lexer;
pub mod lower;
pub mod parser;

use tessera_ir::{BindContext, Diagnostic, Frontend, Statement};

pub use lower::{Lowered, RangeParams, ResultKind, lower};
pub use parser::parse;

/// The `LogQL` front end for one query's time parameters.
#[derive(Clone, Copy, Debug)]
pub struct LogqlFrontend {
    /// The query's time range and step.
    pub params: RangeParams,
}

impl LogqlFrontend {
    /// Lowers `text` and says what its rows mean.
    ///
    /// # Errors
    ///
    /// A syntax error or an unsupported construct, with its position.
    pub fn lower_query(&self, text: &str, ctx: &BindContext) -> Result<Lowered, Diagnostic> {
        lower(&parse(text)?, ctx, self.params)
    }
}

impl Frontend for LogqlFrontend {
    fn lower(&self, text: &str, ctx: &BindContext) -> Result<Statement, Diagnostic> {
        Ok(Statement::Query(self.lower_query(text, ctx)?.plan))
    }
}
