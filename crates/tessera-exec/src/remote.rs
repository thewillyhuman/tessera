// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Leaf tasks on other nodes (section 10.7): a scan's files are assigned
//! to the live `query` nodes by rendezvous hashing on file id (QE-15),
//! each node reads its share and the coordinator gathers the rows. The
//! assignment is a hint: a task a peer cannot run is read here (QE-17).
//! A stage's partial half runs on each node over its share, and its
//! output either comes back to the coordinator or stays on the node as
//! an [`Exchange`] for the workers of the final half (QE-14).
//!
//! The engine knows peers only through these traits; the node provides
//! the membership view and the transport.

use arrow::array::RecordBatch;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tessera_ir::{Expr, Plan, Scan};
use tessera_types::{FileId, FileMeta, NodeId};

/// The files of one scan that one node reads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeafTask {
    /// The scan, with its predicates, projection and time range.
    pub scan: Scan,
    /// The files to read.
    pub files: Vec<FileMeta>,
}

/// A stage of a plan that one node runs over its share of a scan's
/// files (QE-14): a plan whose one scan reads `files` and nothing else,
/// a partial aggregate, top-k or limit over them, whose output the
/// coordinator merges with the other shares', or which the node keeps
/// for the workers of the final stage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StageTask {
    /// The plan fragment to run.
    pub plan: Plan,
    /// The files its scan reads.
    pub files: Vec<FileMeta>,
    /// Where to keep the output rather than return it, when the final
    /// stage runs on the workers.
    #[serde(default)]
    pub exchange: Option<Exchange>,
}

/// Where a partial stage keeps its output for the workers of the
/// final stage (QE-14): under `id`, split into `partitions` parts by a
/// hash of the `keys` columns, each part fetched once by the worker
/// that runs the final stage over it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Exchange {
    /// The exchange, unique to one stage of one query.
    pub id: String,
    /// How many parts the output splits into, one per worker.
    pub partitions: u32,
    /// The expressions over the output whose values choose the part.
    pub keys: Vec<Expr>,
}

/// A final stage over one partition of its inputs' exchanges (QE-14):
/// `plan` folds, for each of its inputs, the rows of `partition` that
/// the worker itself kept, those the coordinator hands over with the
/// task, and those fetched from the input's other producers.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FinalTask {
    /// The final plan: a final aggregate over its partial plan, or a
    /// join over its two sides.
    pub plan: Plan,
    /// The partition this worker folds.
    pub partition: u32,
    /// One per input of the plan, in its order.
    pub inputs: Vec<FinalInput>,
}

/// One input of a final stage: the exchange its rows were kept under
/// and the other nodes that kept some.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FinalInput {
    /// The exchange.
    pub exchange: Exchange,
    /// The other nodes that kept a partial output under it.
    pub producers: Vec<Peer>,
}

/// Another node that runs leaf tasks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    /// The node.
    pub node: NodeId,
    /// The base URL of its HTTP API.
    pub http: String,
}

/// Which node reads which file.
pub trait Peers: Send + Sync {
    /// The peer that should read `file`, or `None` for this node.
    fn owner_of(&self, file: FileId) -> Option<Peer>;
}

/// Why a peer could not run a task; the task is run here instead.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct RemoteError(pub String);

/// The transport to a peer.
pub trait RemoteScan: Send + Sync {
    /// Runs `task` on `peer` and returns its rows.
    fn leaf<'a>(
        &'a self,
        peer: &'a Peer,
        task: &'a LeafTask,
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, RemoteError>>;

    /// Runs the stage `task` on `peer` and returns its output (QE-14),
    /// or nothing when the task names an exchange to keep it under. A
    /// transport or a peer that cannot answers with an error, and the
    /// stage runs on the coordinator instead.
    fn stage<'a>(
        &'a self,
        peer: &'a Peer,
        task: &'a StageTask,
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, RemoteError>> {
        let _ = (peer, task);
        Box::pin(async {
            Err(RemoteError(
                "stages are not supported by this transport".into(),
            ))
        })
    }

    /// Whether peers keep a stage's output, hand it out and run a
    /// final stage over an exchange (QE-14); a transport that cannot
    /// has every final stage run on the coordinator.
    fn supports_exchange(&self) -> bool {
        false
    }

    /// The rows `peer` kept under exchange `id` for `partition`.
    fn fetch<'a>(
        &'a self,
        peer: &'a Peer,
        id: &'a str,
        partition: u32,
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, RemoteError>> {
        let _ = (peer, id, partition);
        Box::pin(async {
            Err(RemoteError(
                "exchanges are not supported by this transport".into(),
            ))
        })
    }

    /// Runs the final stage `task` on `peer` over `rows`, the
    /// coordinator's share of the partition for each input, and what
    /// the inputs' producers kept.
    fn finalize<'a>(
        &'a self,
        peer: &'a Peer,
        task: &'a FinalTask,
        rows: &'a [Vec<RecordBatch>],
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, RemoteError>> {
        let _ = (peer, task, rows);
        Box::pin(async {
            Err(RemoteError(
                "exchanges are not supported by this transport".into(),
            ))
        })
    }
}
