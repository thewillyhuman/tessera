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
//! `search('...')` embeds the search syntax of section 9.6 as a
//! predicate, and `promql('...', start, end, step)`, `logql('...',
//! start, end)` and `traceql('...', start, end)` embed the other
//! languages as tables in `FROM` (QL-3), where a parenthesised query
//! is a derived table too, and `x IN (SELECT ...)` in `WHERE` keeps the
//! rows whose value the subquery returns. `$1`, `$2`, ... are parameters a client
//! binds values to, as the `PostgreSQL` protocol does; [`BoundSql`]
//! lowers a statement with its values and [`describe`] tells a client
//! what types they take.
//!
//! Statements of the control plane (`CREATE ALERT`, `CREATE TENANT` and
//! the others of section 9.2) arrive with their milestones.

pub mod ast;
pub mod lexer;
pub mod lower;
pub mod parser;

use arrow::datatypes::DataType;
pub use lower::Parameter;
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

/// The SQL front end with the values of the `$n` parameters of the text
/// it lowers, in order; a text that names a parameter past them fails.
#[derive(Clone, Debug, Default)]
pub struct BoundSql {
    /// The values, `$1` first.
    pub parameters: Vec<Parameter>,
}

impl Frontend for BoundSql {
    fn lower(&self, text: &str, ctx: &BindContext) -> Result<Statement, Diagnostic> {
        let statement = parser::parse(text)?;
        lower::lower_with(&statement, ctx, &lower::Parameters::Bound(&self.parameters))
    }
}

/// A statement lowered with its `$n` parameters unbound, for a client
/// asking about it before binding.
#[derive(Debug)]
pub struct Described {
    /// The statement, each parameter read as a null.
    pub statement: Statement,
    /// The type each parameter takes, by position, where the statement
    /// says: a column it is compared with, a type it is cast to. None
    /// where nothing says.
    pub parameters: Vec<Option<DataType>>,
}

/// Lowers `text` with its parameters unbound; see [`Described`].
///
/// # Errors
///
/// A syntax or binding error in `text`, short of the unbound parameters.
pub fn describe(text: &str, ctx: &BindContext) -> Result<Described, Diagnostic> {
    let statement = parser::parse(text)?;
    let (statement, parameters) = lower::describe(&statement, ctx)?;
    Ok(Described {
        statement,
        parameters,
    })
}
