// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The optimizer (spec section 10.3) and manifest-level pruning (section
//! 10.4, stages 1 to 4).
//!
//! [`optimize`] binds a plan a front end lowered (types every attribute
//! path from its context and checks every expression), then applies the
//! rewrites that are always beneficial (QE-5): predicate and projection
//! pushdown into `Scan`, time-range inference, and limit pushdown with
//! early termination (QE-6). Cost-based rewrites arrive with the
//! statistics they need.

pub mod bind;
pub mod index;
pub mod paths;
pub mod prune;
pub mod rewrite;
pub mod rollup;

use tessera_ir::{Plan, PlanError, Registry};

pub use bind::bind;
pub use index::{Decision, decide};
pub use prune::{PruneReport, bounds, prune_files};
pub use rewrite::rewrite;
pub use rollup::substitute;

/// Why a plan could not be optimized.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum OptError {
    /// The plan is not well formed.
    #[error("{0}")]
    Plan(#[from] PlanError),
}

/// Binds and rewrites `plan`.
///
/// # Errors
///
/// If the plan refers to unknown columns or functions, or its expressions
/// do not type.
pub fn optimize(plan: Plan, registry: &Registry) -> Result<Plan, OptError> {
    let bound = bind(plan, registry)?;
    let rewritten = paths::promote_paths(rewrite(bound));
    // Rewrites preserve typing; checking once more makes a broken rule a
    // planning error instead of a runtime one.
    rewritten.schema(registry)?;
    Ok(rewritten)
}
