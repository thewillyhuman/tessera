// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Query execution (spec section 10.5).
//!
//! Execution is vectorized and push-based (QE-9): a plan is split into
//! pipelines of [`Operator`]s, each pushing batches into the next.
//! Filters produce selection vectors rather than copies (QE-10); rows are
//! materialized only where an operator needs them whole. Every blocking
//! operator accounts its memory against a [`MemoryPool`] (QE-11, RS-3)
//! and fails by itself when its grant runs out, never taking the node
//! down (QE-18).

pub mod batch;
pub mod memory;
pub mod operator;
pub mod pipeline;

pub use batch::Batch;
pub use memory::{MemoryPool, OutOfMemory};
pub use operator::{CollectSink, Flow, Operator, Sink};
pub use pipeline::Pipeline;

/// Why execution failed.
#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    /// A kernel failed.
    #[error("{0}")]
    Kernel(#[from] tessera_kernels::KernelError),
    /// Arrow rejected an operation.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A blocking operator exhausted its memory grant.
    #[error("{0}")]
    Memory(#[from] OutOfMemory),
    /// The plan asks for something the engine cannot run.
    #[error("{0}")]
    Unsupported(String),
}
