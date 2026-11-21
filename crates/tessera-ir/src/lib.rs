// SPDX-License-Identifier: Apache-2.0

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
pub mod promql;
pub mod rollup;
pub mod system;
pub mod trace;
pub mod typing;

pub use expr::{BinaryOp, Expr, Literal, qualify, split_qualifier, unqualify};
pub use frontend::{
    AlertDefinition, BindContext, ControlStatement, Diagnostic, Frontend, PRICE_SETTINGS,
    RollupDefinition, Span, Statement,
};
pub use function::{AggregateFunction, Registry, ScalarFunction};
pub use plan::{
    AggregateExpr, Asof, AsofMatch, ExtractStage, JoinKind, JoinSide, NamedExpr, PathColumn, Plan,
    PlanError, RangeFunction, Scan, SeriesGrouping, SortKey, VALUE, WindowExpr, WindowFunction,
    aggregate_state_types, promql_schema, range_vector_schema, state_column, window_type,
};
pub use promql::{Group, LOOKBACK, Matching, PromAggregate, PromExpr, PromOp};
pub use system::{log_patterns_schema, system_table_schema, usage_schema};
pub use trace::{Relation, SpanAggregate, Spanset, Structural, trace_group_schema};
pub use typing::{TypeError, expr_type};
