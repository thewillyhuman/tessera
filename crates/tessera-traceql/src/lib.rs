// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `TraceQL` front end (spec section 9.5): a hand-written lexer and
//! parser with positioned errors (QL-2) and lowering into the shared
//! plan (QL-1).
//!
//! A search lowers to a `TraceGroup` over the spans of the candidate
//! traces (QL-13); a metrics query aggregates the matching spans per
//! step and series label. The deviations from Tempo found by the
//! differential suite (QL-4) are listed here as they are found.

pub mod ast;
pub mod lexer;
pub mod lower;
pub mod parser;

use tessera_ir::{BindContext, Diagnostic, Frontend, Statement};

pub use lower::{Lowered, RangeParams, ResultKind, lower};
pub use parser::parse;

/// The `TraceQL` front end for one query's time parameters.
#[derive(Clone, Copy, Debug)]
pub struct TraceqlFrontend {
    /// The query's time range and step.
    pub params: RangeParams,
}

impl TraceqlFrontend {
    /// Lowers `text` and says what its rows mean.
    ///
    /// # Errors
    ///
    /// A syntax error or an unsupported construct, with its position.
    pub fn lower_query(&self, text: &str, ctx: &BindContext) -> Result<Lowered, Diagnostic> {
        lower(&parse(text)?, ctx, self.params)
    }
}

impl Frontend for TraceqlFrontend {
    fn lower(&self, text: &str, ctx: &BindContext) -> Result<Statement, Diagnostic> {
        Ok(Statement::Query(self.lower_query(text, ctx)?.plan))
    }
}
