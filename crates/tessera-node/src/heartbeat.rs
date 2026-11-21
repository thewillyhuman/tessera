// SPDX-License-Identifier: Apache-2.0

//! The node's presence in the cluster (section 14.1): on every heartbeat
//! the node announces itself in the registry, re-reads the registry
//! into its membership view and gossips the view with a few peers: the
//! seeds it was given, static or looked up in DNS, and two live nodes
//! chosen anew each round. All of it is a hint (P2): a missed heartbeat
//! makes the node invisible for a while and a stale view slows the
//! cluster down, neither makes it incorrect.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use tessera_cluster::{Membership, Registry};
use tessera_server::HttpRemote;

/// Live peers gossiped with each round beyond the seeds.
const GOSSIP_FANOUT: usize = 2;

/// How the node gossips.
pub struct Gossip {
    /// The client of the peers' internal routes.
    pub remote: HttpRemote,
    /// Base URLs gossiped with on every round.
    pub seeds: Vec<String>,
    /// A `host:port` whose addresses are gossiped with on every round.
    pub dns: Option<String>,
    /// The scheme the DNS seeds are reached under.
    pub scheme: &'static str,
}

impl Gossip {
    /// The base URLs to gossip with on `round`: the seeds, the DNS seed's
    /// addresses and a few live peers, `own` left out.
    async fn peers(&self, membership: &Membership, round: u64, own: &str) -> BTreeSet<String> {
        let mut peers: BTreeSet<String> = self.seeds.iter().cloned().collect();
        if let Some(name) = &self.dns {
            peers.extend(resolve(name, self.scheme).await);
        }
        peers.extend(
            membership
                .gossip_peers(round, GOSSIP_FANOUT)
                .into_iter()
                .map(|r| r.http),
        );
        peers.remove(own);
        peers
    }
}

/// The base URLs of the addresses `name` (a `host:port`) resolves to
/// under `scheme`; none when it does not resolve, which is logged.
pub async fn resolve(name: &str, scheme: &str) -> Vec<String> {
    match tokio::net::lookup_host(name).await {
        Ok(addresses) => addresses
            .map(|address| format!("{scheme}://{address}"))
            .collect(),
        Err(error) => {
            tracing::debug!(seed = %name, error = %error, "dns seed did not resolve");
            Vec::new()
        }
    }
}

/// Announces, refreshes and gossips every `interval`, forever. Failures
/// are logged and retried on the next heartbeat.
pub async fn run(
    registry: Arc<Registry>,
    membership: Arc<Membership>,
    interval: Duration,
    gossip: Gossip,
) {
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut round: u64 = 0;
    loop {
        ticks.tick().await;
        round = round.wrapping_add(1);
        let own = match registry.announce().await {
            Ok(record) => record.http,
            Err(error) => {
                tracing::warn!(error = %error, "heartbeat failed");
                String::new()
            }
        };
        match registry.list().await {
            Ok(records) => membership.replace(records),
            Err(error) => tracing::warn!(error = %error, "membership refresh failed"),
        }
        for peer in gossip.peers(&membership, round, &own).await {
            match gossip.remote.gossip(&peer, &membership.records()).await {
                Ok(theirs) => membership.merge(theirs),
                Err(error) => tracing::debug!(%peer, error = %error, "gossip failed"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_dns_seed_resolves_to_base_urls() {
        let urls = resolve("localhost:8080", "http").await;
        assert!(!urls.is_empty());
        assert!(
            urls.iter()
                .all(|u| u.starts_with("http://") && u.ends_with(":8080")),
            "{urls:?}"
        );
        assert!(resolve("no.such.host.invalid:1", "http").await.is_empty());
    }
}
