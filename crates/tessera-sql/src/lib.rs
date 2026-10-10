// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The SQL front end (spec section 9.2): a hand-written lexer and parser
//! with positioned errors (QL-2), and lowering into the shared plan
//! (QL-1).
//!
//! The dialect follows `PostgreSQL` where it applies. A table is a kind of
//! event in a dataset: `logs`, `spans`, `metrics`, `events` or `alerts`,
//! optionally qualified as `dataset.logs`; an unqualified name reads the
//! dataset of the [`BindContext`]. Attribute paths are written
//! `attr.http.request.method`, or quoted `attr."http.request.method"`.
//! A bare attribute reads as text; compared with a number, cast
//! (`attr.n::int`) or used by a numeric function it reads as that type.
//!
//! Statements of the control plane (`CREATE ALERT`, `CREATE TENANT` and
//! the others of section 9.2) arrive with their milestones.

pub mod ast;
pub mod lexer;
pub mod lower;
pub mod parser;

use tessera_ir::{BindContext, Diagnostic, Frontend, Statement};

/// The SQL front end.
#[derive(Clone, Copy, Debug, Default)]
pub struct SqlFrontend;

impl Frontend for SqlFrontend {
    fn lower(&self, text: &str, ctx: &BindContext) -> Result<Statement, Diagnostic> {
        let statement = parser::parse(text)?;
        lower::lower(&statement, ctx)
    }
}
