// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Sharing budget counts across the cluster (section 13.4): on every
//! tick the node publishes what it counted today and absorbs what the
//! other nodes published, so that a tenant served by several ingesters
//! has one budget, within a tick. Failures are logged and the next tick
//! tries again.

use std::time::Duration;

use tessera_server::{AppState, share_budgets};

/// Shares the node's budget counts every `interval`, forever.
pub async fn run(state: AppState, interval: Duration) {
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        if let Err(error) = share_budgets(&state).await {
            tracing::debug!(error = %error, "budget counts not shared");
        }
    }
}
