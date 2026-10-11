// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Query execution (spec section 10.5).
//!
//! Execution is vectorized and push-based (QE-9): a plan is split into
//! pipelines of [`Operator`]s, each pushing batches into the next.
//! Filters produce selection vectors rather than copies (QE-10); rows are
//! materialized only where an operator needs them whole. Every blocking
//! operator accounts its memory against a [`MemoryPool`] (QE-11, RS-3)
//! and fails by itself when its grant runs out, never taking the node
//! down (QE-18). CPU work runs on the [`CpuPool`], never on the async
//! runtime (RS-4).

pub mod batch;
pub mod cache;
pub mod engine;
pub mod memory;
pub mod operator;
pub mod pipeline;
pub mod promql;
pub mod query;
pub mod remote;
pub mod scan;
pub mod scheduler;
pub mod system;
pub mod workload;

pub use batch::Batch;
pub use cache::{ResultCache, ResultKey};
pub use engine::{Analysis, Engine, EngineError, QueryLimits, QueryOutput};
pub use memory::{MemoryPool, OutOfMemory};
pub use operator::{CollectSink, Flow, Operator, Sink};
pub use pipeline::Pipeline;
pub use query::{QueryError, Response, explain_text};
pub use remote::{LeafTask, Peer, Peers, RemoteError, RemoteScan};
pub use scan::ScanAnalysis;
pub use scheduler::{CpuPool, Task, TaskError};
pub use system::SystemTables;
pub use workload::{PredicateUse, ScanUse};

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
