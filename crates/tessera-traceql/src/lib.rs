// SPDX-License-Identifier: Apache-2.0

//! The `TraceQL` front end (spec section 9.5): a hand-written lexer and
//! parser with positioned errors (QL-2) and lowering into the shared
//! plan (QL-1).
//!
//! A search lowers to a `TraceGroup` over the spans of the candidate
//! traces (QL-13); a metrics query aggregates the matching spans per
//! step and series label.
//!
//! Not supported yet: the `traceql(...)` table function in SQL (QL-3).
//!
//! Deviations from Tempo 2.7 found by the differential suite (QL-4):
//!
//! - An attribute compared with a literal of another type: Tessera
//!   reads the attribute as the literal's type, so `span.attempt = "2"`
//!   matches an integer attribute whose text is `2` and
//!   `span.http.status_code = 500.0` matches an integer `500`, since
//!   integers and floats compare numerically (DM-5); Tempo matches
//!   neither. A number never matches text: `span.attempt = 2` leaves a
//!   text attribute `"2"` out, as in Tempo.
//! - `span.foo = nil` and `span.foo != nil` are accepted as null tests;
//!   Tempo rejects them.
//! - An aggregate filter such as `count() > 2` adds its statistic to
//!   the spanset's attributes in Tempo's search response; Tessera adds
//!   only the `by()` labels.
//! - `quantile_over_time` is exact and `histogram_over_time` counts
//!   spans per power-of-two bucket of the value; Tempo estimates its
//!   quantiles from such buckets, so the two differ on the same spans.
//! - The nested-set numbers behind `nestedSetLeft`, `nestedSetRight`
//!   and `nestedSetParent` are assigned on every search, children in
//!   start order; Tempo assigns them when it writes a block, so a
//!   trace still in its ingester has none there.
//! - Tag discovery lists no `event` scope, since span events are rows
//!   of their own (DM-8), and lists only the intrinsics Tessera
//!   evaluates.

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
