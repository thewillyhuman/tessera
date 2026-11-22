// SPDX-License-Identifier: Apache-2.0

//! Which node reads which file (QE-15): rendezvous hashing of the file
//! id over the live `query` nodes, so that a file tends to be scanned by
//! the node that has it cached. The assignment is a hint: the engine
//! reads a file here when its peer cannot.

use std::sync::Arc;

use tessera_cluster::{Membership, Role};
use tessera_exec::{Peer, Peers};
use tessera_types::FileId;

/// Peers as the membership view sees them.
pub struct MembershipPeers {
    membership: Arc<Membership>,
}

impl MembershipPeers {
    /// Peers from `membership`.
    #[must_use]
    pub fn new(membership: Arc<Membership>) -> Self {
        MembershipPeers { membership }
    }
}

impl Peers for MembershipPeers {
    fn owner_of(&self, file: FileId) -> Option<Peer> {
        let key = file.to_string();
        let preferred = self.membership.preferred(Role::Query, key.as_bytes(), 1);
        let owner = preferred.first()?;
        if owner.node == self.membership.node() {
            return None;
        }
        Some(Peer {
            node: owner.node,
            http: owner.http.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tessera_cluster::{NodeRecord, registry};
    use tessera_types::{Env, NodeId, SeededRandom, Timestamp};

    use super::*;

    fn record(env: &Env, node: NodeId, role: Role) -> NodeRecord {
        NodeRecord {
            format: registry::FORMAT,
            reads_through: registry::READS_THROUGH,
            woken: Vec::new(),
            node,
            roles: BTreeSet::from([role]),
            http: format!("http://{node}"),
            started_at: env.clock.now(),
            heartbeat_at: env.clock.now(),
        }
    }

    #[test]
    fn files_are_shared_with_live_query_peers_only() {
        let env = Env::simulated(7, Timestamp::from_nanos(1_000));
        let mut random = SeededRandom::new(1);
        let own = record(&env, env.node, Role::Query);
        let membership = Arc::new(Membership::new(env.clone(), own));
        let peers = MembershipPeers::new(Arc::clone(&membership));
        let files: Vec<FileId> = (0..64).map(|_| FileId::random(&mut random)).collect();
        assert!(
            files.iter().all(|f| peers.owner_of(*f).is_none()),
            "alone, every file is read here"
        );

        let query = NodeId::random(&mut random);
        let ingest = NodeId::random(&mut random);
        membership.replace(vec![
            membership.records().remove(0),
            record(&env, query, Role::Query),
            record(&env, ingest, Role::Ingest),
        ]);
        let remote = files.iter().filter_map(|f| peers.owner_of(*f)).count();
        assert!(
            remote > 0 && remote < files.len(),
            "{remote} of 64 go to the peer"
        );
        assert!(
            files
                .iter()
                .filter_map(|f| peers.owner_of(*f))
                .all(|p| p.node == query && p.http == format!("http://{query}")),
            "never to a node without the query role"
        );
    }
}
