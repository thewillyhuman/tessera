// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The plan representation (spec section 10.2): one relational algebra
//! with time-series extensions that every query language lowers into
//! (QL-1). Nothing after lowering knows which language a query came from.
//!
//! - [`expr`]: scalar expressions over the columns and attribute paths of
//!   an event.
//! - [`plan`]: the operator tree. Every node answers its time range and
//!   ordering (QE-3) so that rewrites can preserve them.
//! - [`function`]: the registry of scalar and aggregate functions. The
//!   registry names and types each function; its single implementation
//!   lives in the kernels (QE-20).
//! - [`frontend`]: the contract between a language and the engine.
//!
//! Arrow's data types are the engine's types (QE-2); this crate adds no
//! type system of its own.

pub mod expr;
pub mod frontend;
pub mod function;
pub mod plan;
pub mod typing;

pub use expr::{BinaryOp, Expr, Literal};
pub use frontend::{BindContext, Diagnostic, Frontend, Span, Statement};
pub use function::{AggregateFunction, Registry, ScalarFunction};
pub use plan::{
    AggregateExpr, ExtractStage, NamedExpr, Plan, PlanError, RangeFunction, Scan, SortKey, VALUE,
    range_vector_schema,
};
pub use typing::{TypeError, expr_type};
