// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Leaf tasks on other nodes (section 10.7): a scan's files are assigned
//! to the live `query` nodes by rendezvous hashing on file id (QE-15),
//! each node reads its share and the coordinator gathers the rows. The
//! assignment is a hint: a task a peer cannot run is read here (QE-17).
//!
//! The engine knows peers only through these traits; the node provides
//! the membership view and the transport.

use arrow::array::RecordBatch;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tessera_ir::{Plan, Scan};
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
/// coordinator merges with the other shares'.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StageTask {
    /// The plan fragment to run.
    pub plan: Plan,
    /// The files its scan reads.
    pub files: Vec<FileMeta>,
}

/// Another node that runs leaf tasks.
#[derive(Clone, Debug, PartialEq, Eq)]
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

    /// Runs the stage `task` on `peer` and returns its output (QE-14).
    /// A transport or a peer that cannot answers with an error, and
    /// the stage runs on the coordinator instead.
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
}
