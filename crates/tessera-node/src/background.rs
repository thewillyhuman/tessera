// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `compact` role's background work: intake for every registered
//! writer (section 6.7), recovery of writers that are dead (ST-9, ST-10),
//! compaction of every partition that is due (section 6.9) and garbage
//! collection of replaced files and adopted segments (section 6.10).
//!
//! Each item is done by the node that owns its slot (AR-4): writers and
//! their logs under the intake and recovery kinds, partitions under the
//! partition kind. A writer is dead when the node that registered it is
//! no longer live in the membership view and its log has been quiet for
//! a few heartbeats, or, whatever the view says, when the log has been
//! quiet for a long time. Writers of this process are never recovered;
//! the ingester ends or rotates its own logs.

use std::sync::Arc;
use std::time::Duration;

use tessera_catalog::Catalog;
use tessera_cluster::Membership;
use tessera_compact::gc::{DEFAULT_FILE_RETENTION, DEFAULT_SEGMENT_GRACE};
use tessera_compact::{Compactor, Outcome, Policy, collect_files, collect_log, input_bytes, plan};
use tessera_format::FileFormat;
use tessera_manifest::{DatasetLogs, Manifest, Registration, intake, list_partitions, registry};
use tessera_object::{ObjectStore, Pools};
use tessera_types::{DatasetName, Env, Kind, Level, PoolId, TenantId, Window, WorkKind, WriterId};
use tessera_wal::{EntryBody, FlushTarget, reader, recover};

use crate::leases::Owned;

/// How often the background work runs by default.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);
/// How long the writer of a node that left the membership view may be
/// quiet before it is recovered: enough for a registration to reach the
/// view before its node does.
const DEAD_QUIET: Duration = Duration::from_secs(15);
/// How long any foreign writer may be quiet before it is recovered.
const RECOVER_AFTER: Duration = Duration::from_secs(300);
/// Bytes of files of an older layout epoch rewritten per round (AD-10).
const RECLUSTER_BUDGET: u64 = 256 << 20;

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
    /// The slots this node owns.
    pub owned: Owned,
    /// The cluster, for which writers' nodes are gone.
    pub membership: Arc<Membership>,
    /// How often a round runs.
    pub interval: Duration,
    /// The dataset logs, for each dataset's layout.
    pub datasets: Arc<DatasetLogs>,
}

/// Runs the background work forever. Failures are logged and retried on
/// the next round.
pub async fn run(background: Background) {
    let compactor = Compactor::new(
        Arc::clone(&background.pools),
        Arc::clone(&background.format),
        background.env.clone(),
    );
    let mut ticks = tokio::time::interval(background.interval);
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
            let key = writer.to_string();
            let mut ended = false;
            if background.owned.owns(WorkKind::Intake, key.as_bytes()) {
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
                ended = outcome.ended;
            }
            if !ended
                && background.owned.owns(WorkKind::Recovery, key.as_bytes())
                && is_dead(background, &registration).await?
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
        let mut budget = RECLUSTER_BUDGET;
        for kind in Kind::ALL {
            compact_and_collect(background, compactor, tenant, &dataset, kind, &mut budget).await?;
        }
    }
    Ok(())
}

/// Compacts every partition of a dataset and kind that is due, rewrites
/// files of an older layout epoch within `budget`, recent partitions
/// first (AD-10), and collects replaced files.
async fn compact_and_collect(
    background: &Background,
    compactor: &Compactor,
    tenant: &TenantId,
    dataset: &DatasetName,
    kind: Kind,
    budget: &mut u64,
) -> Result<(), Failure> {
    let now = background.env.clock.now();
    let record = background
        .datasets
        .open(tenant, dataset)
        .layout(kind)
        .await?;
    let mut partitions = list_partitions(&*background.store, tenant, dataset, kind).await?;
    partitions.sort_by_key(|p| std::cmp::Reverse(p.window.start()));
    for partition in partitions {
        if !background
            .owned
            .owns(WorkKind::Partition, partition.to_string().as_bytes())
        {
            continue;
        }
        let manifest = Manifest::open(
            Arc::clone(&background.store),
            partition.clone(),
            background.env.clone(),
        );
        let snapshot = manifest.snapshot().await?;
        let policy = Policy {
            layout_epoch: record.epoch,
            ..Policy::default()
        };
        let job = plan(&partition, snapshot.state(), now, &policy).filter(|job| {
            let reclustering = job.inputs.iter().all(|f| f.level != Level::L1);
            if !reclustering {
                return true;
            }
            let bytes = input_bytes(job);
            if bytes > *budget {
                return false;
            }
            *budget -= bytes;
            true
        });
        if let Some(job) = job {
            let inputs = job.inputs.len();
            match compactor.run(&manifest, &job, &record).await? {
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
        if !background
            .owned
            .owns(WorkKind::Intake, writer.to_string().as_bytes())
        {
            continue;
        }
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

/// Whether a foreign writer counts as dead: its node is gone from the
/// view and its log quiet for a few heartbeats, or the log quiet for a
/// long time whatever the view says. A writer of this node or one whose
/// log has ended is never dead.
async fn is_dead(background: &Background, registration: &Registration) -> Result<bool, Failure> {
    if registration.node == background.env.node {
        return Ok(false);
    }
    let Some(quiet) = quiet_for(background, registration.writer).await? else {
        return Ok(false);
    };
    let gone = !background.membership.is_live(registration.node);
    Ok(quiet >= RECOVER_AFTER || (gone && quiet >= DEAD_QUIET))
}

/// How long a writer's log has been quiet; `None` once it has ended. A
/// log without entries has been quiet forever.
async fn quiet_for(background: &Background, writer: WriterId) -> Result<Option<Duration>, Failure> {
    let Some(last) = reader::last(&*background.store, writer).await? else {
        return Ok(Some(Duration::MAX));
    };
    let entry = reader::read(&*background.store, writer, last).await?;
    if matches!(entry.body(), EntryBody::End) {
        return Ok(None);
    }
    let now = background.env.clock.now();
    Ok(Some(
        now.duration_since(entry.header().written_at)
            .unwrap_or(Duration::ZERO),
    ))
}
