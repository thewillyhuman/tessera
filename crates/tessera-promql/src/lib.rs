// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `PromQL` front end (spec section 9.3): a hand-written lexer and
//! parser with positioned errors (QL-2) and lowering into the shared
//! plan (QL-1).
//!
//! Every selector lowers to a projected scan of the metric rows its
//! matchers admit, and the expression to a `PromQL` node the engine
//! evaluates with Prometheus' semantics (QL-5).
//!
//! A histogram stored from OTLP answers `name_bucket`, `name_count`
//! and `name_sum` as the classic series a scrape would give, so
//! `histogram_quantile` and the rate family read it unchanged; a
//! summary answers its `quantile` series under its own name. Not
//! supported yet: Prometheus' native histogram sample type and the
//! `histogram_*` functions over it, and `info`.
//!
//! The differential suite (QL-4) found no deviation from Prometheus
//! 3.15 on its cases, which cover selectors, the rate family,
//! `_over_time` functions, aggregations, vector matching, set
//! operators, `histogram_quantile`, label functions, subqueries,
//! `offset`, `@`, staleness and the result types. The compliance
//! suite (QL-6) has not been run.

pub mod ast;
pub mod lexer;
pub mod lower;
pub mod parser;

use tessera_ir::{BindContext, Diagnostic, Frontend, Statement};

pub use lower::{Lowered, RangeParams, ResultKind, kind_of, lower, selector_plan};
pub use parser::parse;

/// The `PromQL` front end for one query's time parameters.
#[derive(Clone, Copy, Debug)]
pub struct PromqlFrontend {
    /// The query's time range and step.
    pub params: RangeParams,
}

impl PromqlFrontend {
    /// Lowers `text` and says what its rows mean.
    ///
    /// # Errors
    ///
    /// A syntax error or an unsupported construct, with its position.
    pub fn lower_query(&self, text: &str, ctx: &BindContext) -> Result<Lowered, Diagnostic> {
        lower(&parse(text)?, ctx, self.params)
    }
}

impl Frontend for PromqlFrontend {
    fn lower(&self, text: &str, ctx: &BindContext) -> Result<Statement, Diagnostic> {
        Ok(Statement::Query(self.lower_query(text, ctx)?.plan))
    }
}
