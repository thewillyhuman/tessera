// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `compact` role's background work in a single node: intake for every
//! registered writer (section 6.7) and recovery of writers that are dead
//! (ST-9, ST-10).
//!
//! Without cluster membership (M6), a dead writer is recognised by its
//! registration: a writer registered by another node process whose log
//! has not ended and has been quiet for a while. Writers of this process
//! are never recovered; the ingester ends or rotates its own logs.

use std::sync::Arc;
use std::time::Duration;

use tessera_catalog::Catalog;
use tessera_format::FileFormat;
use tessera_manifest::{intake, registry};
use tessera_object::ObjectStore;
use tessera_types::{Env, PoolId, TenantId, Window, WriterId};
use tessera_wal::{EntryBody, FlushTarget, reader, recover};

/// How often intake and recovery run.
const INTERVAL: Duration = Duration::from_secs(5);
/// How long a foreign writer may be quiet before it is recovered.
const RECOVER_AFTER: Duration = Duration::from_secs(300);

/// What the loop needs.
pub struct Background {
    /// The root pool's store.
    pub store: Arc<dyn ObjectStore>,
    /// The node's environment.
    pub env: Env,
    /// The catalog, for the list of tenants.
    pub catalog: Arc<Catalog>,
    /// The file format recovery writes with.
    pub format: Arc<dyn FileFormat>,
    /// The pool recovery places files in.
    pub pool: PoolId,
}

/// Runs intake and recovery forever. Failures are logged and retried on
/// the next round.
pub async fn run(background: Background) {
    let mut ticks = tokio::time::interval(INTERVAL);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        let tenants: Vec<TenantId> = background
            .catalog
            .cached()
            .state()
            .tenants()
            .map(|t| t.id.clone())
            .collect();
        for tenant in tenants {
            if let Err(error) = round(&background, &tenant).await {
                tracing::warn!(tenant = %tenant, error = %error, "background round failed");
            }
        }
    }
}

async fn round(
    background: &Background,
    tenant: &TenantId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for dataset in registry::datasets(&*background.store, tenant).await? {
        for registration in registry::list(&*background.store, tenant, &dataset).await? {
            let writer = registration.writer;
            let outcome = intake::run(
                Arc::clone(&background.store),
                &background.env,
                tenant,
                &dataset,
                writer,
            )
            .await?;
            if outcome.adoptions > 0 || outcome.removed {
                tracing::info!(%tenant, %dataset, %writer, adoptions = outcome.adoptions, removed = outcome.removed, "intake");
            }
            if !outcome.ended
                && registration.node != background.env.node
                && is_quiet(background, writer).await?
            {
                let target = FlushTarget {
                    store: Arc::clone(&background.store),
                    pool: background.pool.clone(),
                    format: Arc::clone(&background.format),
                };
                let recovered =
                    recover(&target, &background.env, writer, Window::DEFAULT_LENGTH).await?;
                tracing::info!(%tenant, %dataset, %writer, ?recovered, "recovery");
            }
        }
    }
    Ok(())
}

/// Whether a writer's log has been quiet for longer than `RECOVER_AFTER`.
async fn is_quiet(
    background: &Background,
    writer: WriterId,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let Some(last) = reader::last(&*background.store, writer).await? else {
        return Ok(true);
    };
    let entry = reader::read(&*background.store, writer, last).await?;
    if matches!(entry.body(), EntryBody::End) {
        return Ok(false);
    }
    let now = background.env.clock.now();
    Ok(now
        .duration_since(entry.header().written_at)
        .is_some_and(|quiet| quiet >= RECOVER_AFTER))
}
