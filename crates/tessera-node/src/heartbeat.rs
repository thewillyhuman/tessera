// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The node's presence in the cluster (section 14.1): on every heartbeat
//! the node announces itself in the registry and re-reads the registry
//! into its membership view. Both are hints (P2): a missed heartbeat
//! makes the node invisible for a while and a stale view slows the
//! cluster down, neither makes it incorrect.

use std::sync::Arc;
use std::time::Duration;

use tessera_cluster::{Membership, Registry};

/// Announces and refreshes every `interval`, forever. Failures are
/// logged and retried on the next heartbeat.
pub async fn run(registry: Arc<Registry>, membership: Arc<Membership>, interval: Duration) {
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        if let Err(error) = registry.announce().await {
            tracing::warn!(error = %error, "heartbeat failed");
        }
        match registry.list().await {
            Ok(records) => membership.replace(records),
            Err(error) => tracing::warn!(error = %error, "membership refresh failed"),
        }
    }
}
