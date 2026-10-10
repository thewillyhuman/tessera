// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Vectorized kernels over Arrow batches (spec section 10.5).
//!
//! Arrow's array containers are the vector format (QE-2), and where
//! Arrow's own compute kernels do the job on those containers
//! (comparison, arithmetic, three-valued logic, `LIKE`, take, filter,
//! sort) this crate calls them rather than keeping a second copy. What
//! the workload needs beyond that is here: expression evaluation with
//! SQL coercions, variant access into the overflow column, grouping,
//! weight-aware aggregation (QE-13) and time bucketing.
//!
//! Each function of the registry has exactly one implementation in this
//! crate (QE-20). Kernels are CPU work and run on the dedicated pool
//! (RS-4); nothing here is async or touches storage.

pub mod aggregate;
pub mod eval;
pub mod extract;
pub mod group;
pub mod template;
pub mod time;
pub mod variant;
pub mod window;

use arrow::error::ArrowError;

pub use aggregate::{Accumulator, accumulator};
pub use eval::{evaluate, evaluate_predicate};
pub use extract::{Extracted, extract};
pub use group::Grouper;
pub use variant::row_labels;

/// Why a kernel could not run.
#[derive(Debug, thiserror::Error)]
pub enum KernelError {
    /// Arrow rejected the operation.
    #[error("arrow: {0}")]
    Arrow(#[from] ArrowError),
    /// The expression reads a column the batch does not have.
    #[error("batch has no column {0}")]
    MissingColumn(String),
    /// The operation is not implemented for these operands.
    #[error("{0}")]
    Unsupported(String),
}
