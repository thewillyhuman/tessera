// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `compact` role's background work in a single node: intake for every
//! registered writer (section 6.7), recovery of writers that are dead
//! (ST-9, ST-10), compaction of every partition that is due (section 6.9)
//! and garbage collection of replaced files and adopted segments (section
//! 6.10).
//!
//! Without cluster membership (M6), a dead writer is recognised by its
//! registration: a writer registered by another node process whose log
//! has not ended and has been quiet for a while. Writers of this process
//! are never recovered; the ingester ends or rotates its own logs.

use std::sync::Arc;
use std::time::Duration;

use tessera_catalog::Catalog;
use tessera_compact::gc::{DEFAULT_FILE_RETENTION, DEFAULT_SEGMENT_GRACE};
use tessera_compact::{Compactor, Outcome, Policy, collect_files, collect_log, plan};
use tessera_format::FileFormat;
use tessera_manifest::{Manifest, intake, list_partitions, registry};
use tessera_object::{ObjectStore, Pools};
use tessera_types::{DatasetName, Env, Kind, PoolId, TenantId, Window, WriterId};
use tessera_wal::{EntryBody, FlushTarget, reader, recover};

/// How often the background work runs.
const INTERVAL: Duration = Duration::from_secs(5);
/// How long a foreign writer may be quiet before it is recovered.
const RECOVER_AFTER: Duration = Duration::from_secs(300);

type Failure = Box<dyn std::error::Error + Send + Sync>;

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
    /// Every pool, for compaction and collection.
    pub pools: Arc<Pools>,
}

/// Runs the background work forever. Failures are logged and retried on
/// the next round.
pub async fn run(background: Background) {
    let compactor = Compactor::new(
        Arc::clone(&background.pools),
        Arc::clone(&background.format),
        background.env.clone(),
    );
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
            if let Err(error) = round(&background, &compactor, &tenant).await {
                tracing::warn!(tenant = %tenant, error = %error, "background round failed");
            }
        }
        if let Err(error) = collect_logs(&background).await {
            tracing::warn!(error = %error, "segment collection failed");
        }
    }
}

async fn round(
    background: &Background,
    compactor: &Compactor,
    tenant: &TenantId,
) -> Result<(), Failure> {
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
        for kind in Kind::ALL {
            compact_and_collect(background, compactor, tenant, &dataset, kind).await?;
        }
    }
    Ok(())
}

/// Compacts every partition of a dataset and kind that is due and
/// collects its replaced files.
async fn compact_and_collect(
    background: &Background,
    compactor: &Compactor,
    tenant: &TenantId,
    dataset: &DatasetName,
    kind: Kind,
) -> Result<(), Failure> {
    let now = background.env.clock.now();
    for partition in list_partitions(&*background.store, tenant, dataset, kind).await? {
        let manifest = Manifest::open(
            Arc::clone(&background.store),
            partition.clone(),
            background.env.clone(),
        );
        let snapshot = manifest.snapshot().await?;
        if let Some(job) = plan(&partition, snapshot.state(), now, &Policy::default()) {
            let inputs = job.inputs.len();
            match compactor.run(&manifest, &job).await? {
                Outcome::Replaced {
                    output,
                    rows,
                    duplicates,
                } => {
                    tracing::info!(%tenant, %dataset, %kind, window = %partition.window, inputs, %output, rows, duplicates, "compacted");
                }
                Outcome::Superseded => {
                    tracing::info!(%tenant, %dataset, %kind, window = %partition.window, "compaction superseded");
                }
            }
        }
        let collected =
            collect_files(&manifest, &background.pools, now, DEFAULT_FILE_RETENTION).await?;
        if collected > 0 {
            tracing::info!(%tenant, %dataset, %kind, window = %partition.window, collected, "files collected");
        }
    }
    Ok(())
}

/// Collects adopted segments of every writer log in the store.
async fn collect_logs(background: &Background) -> Result<(), Failure> {
    let now = background.env.clock.now();
    for writer in reader::writers(&*background.store).await? {
        let collected = collect_log(
            &background.store,
            &background.env,
            writer,
            now,
            DEFAULT_SEGMENT_GRACE,
            Window::DEFAULT_LENGTH,
        )
        .await?;
        if collected.deleted > 0 {
            tracing::info!(%writer, deleted = collected.deleted, gone = collected.log_gone, "segments collected");
        }
    }
    Ok(())
}

/// Whether a writer's log has been quiet for longer than `RECOVER_AFTER`.
async fn is_quiet(background: &Background, writer: WriterId) -> Result<bool, Failure> {
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
