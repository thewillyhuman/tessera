// SPDX-License-Identifier: Apache-2.0

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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use tessera_catalog::{Catalog, ControlStores, Table, TenantSettingsSource};
use tessera_cluster::{Load, Membership};
use tessera_compact::gc::{DEFAULT_FILE_RETENTION, DEFAULT_SEGMENT_GRACE};
use tessera_compact::{
    Checkpoints, Compactor, Outcome, Policy, collect_files, collect_log, input_bytes, plan,
};
use tessera_eval::AlertRule;
use tessera_eval::rule::decode_rows;
use tessera_format::FileFormat;
use tessera_iceberg::{Exporter, Outcome as Export};
use tessera_manifest::{
    DatasetLog, DatasetLogs, DatasetState, Manifest, ManifestState, Registration, intake,
    list_partitions, registry,
};
use tessera_object::{ObjectStore, Pools};
use tessera_types::{
    DatasetName, Env, Held, Kind, Level, Meter, PartitionId, PoolId, RetentionPolicy, Seq,
    TenantId, Timestamp, Windows, WorkKind, WriterId,
};
use tessera_wal::{EntryBody, FlushTarget, reader, recover};

use crate::leases::Owned;
use crate::retention;

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
/// How often what the partitions hold is recorded in `system.usage`
/// (section 13.1): the grain a storage bill is reconciled at.
const HELD_EVERY: Duration = Duration::from_secs(3_600);

/// How often the Iceberg metadata of the tables this node owns the
/// export of is brought up to date (OP-9): a table whose files did not
/// change costs its hint and metadata reads, nothing more.
pub const EXPORT_EVERY: Duration = Duration::from_secs(600);

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
    /// Where compaction reports what it stores (CO-2).
    pub meter: Arc<dyn Meter>,
    /// The control stores, for retention policies (section 13.3).
    pub controls: Arc<ControlStores>,
    /// Where the backlogs are reported (OP-5).
    pub load: Arc<Load>,
    /// The Iceberg exporter, when the node has a location to publish
    /// under (OP-9).
    pub export: Option<Exporter>,
    /// How often the exporter runs.
    pub export_interval: Duration,
}

/// Where intake stopped in each writer's log, per dataset: a round
/// reads on from there (IT-2 lets one start over, so nothing is lost
/// with this memory but the reading it saves).
type Positions = BTreeMap<(TenantId, DatasetName, WriterId), Seq>;

/// What a round left undone or had to repair.
#[derive(Clone, Copy, Debug, Default)]
struct Backlog {
    /// Compaction jobs deferred by the re-clustering budget.
    deferred: u64,
    /// Writers whose logs had to be recovered.
    recoveries: u64,
}

/// Runs the background work forever. Failures are logged and retried on
/// the next round.
pub async fn run(background: Background) {
    let tenants: Arc<dyn TenantSettingsSource> = background.catalog.clone();
    let compactor = Compactor::new(
        Arc::clone(&background.pools),
        Arc::clone(&background.format),
        background.env.clone(),
    )
    .with_meter(Arc::clone(&background.meter))
    .with_tenants(tenants);
    let mut ticks = tokio::time::interval(background.interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_held: Option<Timestamp> = None;
    let mut last_export: Option<Timestamp> = None;
    let mut positions = Positions::new();
    loop {
        ticks.tick().await;
        let tenants: Vec<TenantId> = background
            .catalog
            .cached()
            .state()
            .tenants()
            .map(|t| t.id.clone())
            .collect();
        let mut backlog = Backlog::default();
        let mut held = Vec::new();
        positions.retain(|(tenant, _, _), _| tenants.contains(tenant));
        for tenant in &tenants {
            let mut cursors = Cursors {
                positions: &mut positions,
                met: BTreeSet::new(),
            };
            match round(&background, &compactor, tenant, &mut held, &mut cursors).await {
                Ok(left) => {
                    backlog.deferred += left.deferred;
                    backlog.recoveries += left.recoveries;
                    // A writer a whole round did not meet has left its
                    // registry or this node's care: its position goes
                    // with it. A round that failed met only some.
                    let met = cursors.met;
                    positions.retain(|at, _| at.0 != *tenant || met.contains(at));
                }
                Err(error) => {
                    tracing::warn!(tenant = %tenant, error = %error, "background round failed");
                }
            }
        }
        background.load.set_compaction_backlog(backlog.deferred);
        background.load.set_intake_backlog(backlog.recoveries);
        // What the owned partitions hold, once an hour, the first round
        // included: each partition has one owner, so the cluster
        // records each once.
        let now = background.env.clock.now();
        let due = last_held.is_none_or(|at: Timestamp| {
            now.duration_since(at)
                .is_some_and(|since| since >= HELD_EVERY)
        });
        if due && !held.is_empty() {
            background.meter.held(held);
            last_held = Some(now);
        }
        if let Err(error) = collect_logs(&background).await {
            tracing::warn!(error = %error, "segment collection failed");
        }
        if let Some(exporter) = &background.export {
            let due = last_export.is_none_or(|at: Timestamp| {
                now.duration_since(at)
                    .is_some_and(|since| since >= background.export_interval)
            });
            // A node with no table yet looks again every round, so that a
            // dataset's first files are published the round they are
            // adopted; the interval counts from a round that found one.
            if due && export_round(&background, exporter, &tenants).await > 0 {
                last_export = Some(now);
            }
        }
    }
}

/// Publishes the Iceberg metadata of every table this node owns the
/// export of (OP-9): one table per dataset and kind, its export the
/// partition slot of the table's key. Returns how many tables there
/// are, published now or before.
async fn export_round(background: &Background, exporter: &Exporter, tenants: &[TenantId]) -> usize {
    let mut tables = 0;
    for tenant in tenants {
        let datasets = match registry::datasets(&*background.store, tenant).await {
            Ok(datasets) => datasets,
            Err(error) => {
                tracing::warn!(%tenant, error = %error, "listing datasets for export failed");
                continue;
            }
        };
        for dataset in datasets {
            for kind in Kind::ALL {
                let item = Exporter::item(tenant, &dataset, kind);
                if !background.owned.owns(WorkKind::Partition, item.as_bytes()) {
                    continue;
                }
                match exporter.export(tenant, &dataset, kind).await {
                    Ok(Export::Published { version, files }) => {
                        tables += 1;
                        tracing::info!(%tenant, %dataset, %kind, version, files, "iceberg metadata published");
                    }
                    Ok(Export::Unchanged | Export::Lost { .. }) => tables += 1,
                    Ok(Export::Empty) => {}
                    Err(error) => {
                        tracing::warn!(%tenant, %dataset, %kind, error = %error, "iceberg export failed");
                    }
                }
            }
        }
    }
    tables
}

/// What intake carries through a tenant's round.
struct Cursors<'a> {
    /// Where each log was left, read and moved on by the round.
    positions: &'a mut Positions,
    /// The logs the round ran intake for.
    met: BTreeSet<(TenantId, DatasetName, WriterId)>,
}

async fn round(
    background: &Background,
    compactor: &Compactor,
    tenant: &TenantId,
    held: &mut Vec<Held>,
    cursors: &mut Cursors<'_>,
) -> Result<Backlog, Failure> {
    let mut backlog = Backlog::default();
    for dataset in registry::datasets(&*background.store, tenant).await? {
        for registration in registry::list(&*background.store, tenant, &dataset).await? {
            let writer = registration.writer;
            let key = writer.to_string();
            let mut ended = false;
            if background.owned.owns(WorkKind::Intake, key.as_bytes()) {
                let at = (tenant.clone(), dataset.clone(), writer);
                let after = cursors.positions.get(&at).copied().unwrap_or(Seq::ZERO);
                cursors.met.insert(at.clone());
                // A run that fails leaves the position where it was, to
                // be repeated from there.
                let outcome = intake::run_after(
                    Arc::clone(&background.store),
                    &background.env,
                    tenant,
                    &dataset,
                    writer,
                    after,
                )
                .await?;
                cursors.positions.insert(at, outcome.through);
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
                let windows = windows_of(background, std::slice::from_ref(tenant)).await?;
                let recovered = recover(&target, &background.env, writer, windows).await?;
                backlog.recoveries += 1;
                tracing::info!(%tenant, %dataset, %writer, ?recovered, "recovery");
            }
        }
        let mut budget = RECLUSTER_BUDGET;
        let policies = policies_of(background, tenant).await;
        // Read once for the five kinds: each asks for its layout, its
        // tombstones and its value score.
        let settings = background.datasets.open(tenant, &dataset).state().await?;
        for kind in Kind::ALL {
            backlog.deferred += compact_and_collect(
                background,
                compactor,
                tenant,
                &dataset,
                &settings,
                kind,
                &mut budget,
                &policies,
                held,
            )
            .await?;
        }
    }
    Ok(backlog)
}

/// Compacts every partition of a dataset and kind that is due, rewrites
/// files of an older layout epoch within `budget`, recent partitions
/// first (AD-10), and collects replaced files. Returns how many jobs
/// the budget deferred.
#[allow(clippy::too_many_arguments)]
async fn compact_and_collect(
    background: &Background,
    compactor: &Compactor,
    tenant: &TenantId,
    dataset: &DatasetName,
    settings: &DatasetState,
    kind: Kind,
    budget: &mut u64,
    policies: &[RetentionPolicy],
    held: &mut Vec<Held>,
) -> Result<u64, Failure> {
    let mut deferred = 0;
    let now = background.env.clock.now();
    let record = settings.layout(kind)?;
    let value_score = settings.value_score(kind);
    let tombstones = settings.tombstones(kind)?;
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
        held.push(held_of(&partition, snapshot.state()));
        let age = now
            .duration_since(partition.window.end())
            .unwrap_or(Duration::ZERO);
        let decision = retention::decide(policies, tenant, dataset, kind, age, value_score);
        let mut retain = None;
        let mut retention_after = None;
        match decision {
            retention::Decision::Keep => {}
            retention::Decision::Drop => {
                if !snapshot.state().is_dropped() {
                    manifest.drop_partition().await?;
                    tracing::info!(%tenant, %dataset, %kind, window = %partition.window, "partition dropped by retention");
                }
                collect_files(&manifest, &background.pools, now, DEFAULT_FILE_RETENTION).await?;
                continue;
            }
            retention::Decision::Retire { retain: r, after } => {
                retain = Some(r);
                retention_after = Some(after);
            }
        }
        let policy = Policy {
            layout_epoch: record.epoch,
            retention_after,
            ..Policy::default()
        };
        let job = plan(&partition, snapshot.state(), now, &policy).filter(|job| {
            let reclustering = job.inputs.iter().all(|f| f.level != Level::L1);
            if !reclustering {
                return true;
            }
            let bytes = input_bytes(job);
            if bytes > *budget {
                deferred += 1;
                return false;
            }
            *budget -= bytes;
            true
        });
        if let Some(job) = job {
            let inputs = job.inputs.len();
            match compactor
                .run(&manifest, &job, &record, retain.as_ref(), &tombstones)
                .await?
            {
                Outcome::Replaced {
                    output,
                    rows,
                    duplicates,
                } => {
                    tracing::info!(%tenant, %dataset, %kind, window = %partition.window, inputs, %output, rows, duplicates, "compacted");
                    // A checkpoint after every rewrite (CL-3, MF-5): a cold
                    // reader folds the few commits since it, and a range
                    // read fetches only the row groups it needs.
                    if let Err(error) = manifest.checkpoint().await {
                        tracing::warn!(%tenant, %dataset, %kind, window = %partition.window, error = %error, "manifest checkpoint failed");
                    }
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
    if !tombstones.is_empty() {
        let key = format!("{tenant}/{dataset}/{kind}/tombstones");
        if background.owned.owns(WorkKind::Partition, key.as_bytes()) {
            let log = background.datasets.open(tenant, dataset);
            let removed = collect_tombstones(background, &log, tenant, dataset, kind, now).await?;
            if removed > 0 {
                tracing::info!(%tenant, %dataset, %kind, removed, "tombstones applied everywhere");
            }
        }
    }
    Ok(deferred)
}

/// How long a tombstone stays after its delete, so that every file
/// flushed before it has been compacted with it applied (SE-5).
const TOMBSTONE_RETENTION: Duration = Duration::from_secs(30 * 24 * 3600);

/// Removes the tombstones of a dataset and kind that every file beyond
/// L1 was written under, once the retention of SE-5 has passed; the
/// files are read across every partition, so one node does it.
async fn collect_tombstones(
    background: &Background,
    log: &DatasetLog,
    tenant: &TenantId,
    dataset: &DatasetName,
    kind: Kind,
    now: Timestamp,
) -> Result<usize, Failure> {
    let mut oldest_epoch = u64::MAX;
    for partition in list_partitions(&*background.store, tenant, dataset, kind).await? {
        let manifest = Manifest::open(
            Arc::clone(&background.store),
            partition,
            background.env.clone(),
        );
        let snapshot = manifest.snapshot().await?;
        for file in snapshot.state().files().filter(|f| f.level != Level::L1) {
            oldest_epoch = oldest_epoch.min(file.layout_epoch);
        }
    }
    Ok(log
        .remove_tombstones(kind, |t| {
            t.epoch <= oldest_epoch
                && now
                    .duration_since(t.created_at)
                    .is_some_and(|age| age >= TOMBSTONE_RETENTION)
        })
        .await?)
}

/// The enabled retention policies of `tenant`; a store or a document
/// that fails is logged and treated as no policy.
async fn policies_of(background: &Background, tenant: &TenantId) -> Vec<RetentionPolicy> {
    let rows = match background
        .controls
        .open(tenant)
        .rows(Table::RetentionPolicies)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(%tenant, error = %error, "retention policies not read");
            return Vec::new();
        }
    };
    let (policies, bad) = tessera_eval::rule::decode_rows::<RetentionPolicy>(&rows);
    for problem in bad {
        tracing::warn!(%tenant, problem, "retention policy does not decode");
    }
    policies.into_iter().filter(|p| p.enabled).collect()
}

/// Where every tenant's rule groups are in the writer logs (GC-2 c),
/// from their state logs.
async fn checkpoints(background: &Background) -> Result<Checkpoints, Failure> {
    let mut checkpoints = Checkpoints::none();
    let tenants: Vec<TenantId> = background
        .catalog
        .cached()
        .state()
        .tenants()
        .map(|t| t.id.clone())
        .collect();
    for tenant in tenants {
        let rows = background
            .controls
            .open(&tenant)
            .rows(Table::AlertRules)
            .await?;
        let (rules, _) = decode_rows::<AlertRule>(&rows);
        let groups: BTreeSet<String> = rules.into_iter().map(|r| r.group).collect();
        let settings = background.catalog.settings_of(&tenant);
        for group in groups {
            // Every partition of the group follows the logs (CQ-6),
            // its own or through the partials their sources leave
            // (CQ-4), which go with the entries: the slowest holds the
            // segments.
            for part in 0..settings.rule_partitions_of(&group) {
                let log = tessera_eval::state::open(
                    Arc::clone(&background.store),
                    &tenant,
                    &group,
                    part,
                    background.env.clone(),
                );
                let snapshot = log.snapshot().await?;
                for (writer, seq) in &snapshot.state().positions {
                    if let Ok(writer) = writer.parse::<WriterId>() {
                        checkpoints.hold(writer, *seq);
                    }
                }
            }
        }
    }
    Ok(checkpoints)
}

/// What `partition` holds: the bytes and rows of its adopted files.
fn held_of(partition: &PartitionId, state: &ManifestState) -> Held {
    Held {
        tenant: partition.tenant.clone(),
        dataset: partition.dataset.clone(),
        kind: partition.kind,
        bytes: state.files().map(|f| f.size_bytes).sum(),
        rows: state.files().map(|f| f.row_count).sum(),
    }
}

/// The partition windows of every dataset of `tenants` (MF-2), from
/// their dataset logs: what places the rows of a writer log in
/// partitions.
async fn windows_of(background: &Background, tenants: &[TenantId]) -> Result<Windows, Failure> {
    let mut windows = Windows::default();
    for tenant in tenants {
        for dataset in registry::datasets(&*background.store, tenant).await? {
            let schedule = background.datasets.open(tenant, &dataset).window().await?;
            windows.insert(tenant.clone(), dataset, schedule);
        }
    }
    Ok(windows)
}

/// Collects adopted segments of every writer log in the store that
/// every rule group has read past (GC-2).
async fn collect_logs(background: &Background) -> Result<(), Failure> {
    let now = background.env.clock.now();
    let checkpoints = checkpoints(background).await?;
    let tenants: Vec<TenantId> = background
        .catalog
        .cached()
        .state()
        .tenants()
        .map(|t| t.id.clone())
        .collect();
    let windows = windows_of(background, &tenants).await?;
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
            &windows,
            &checkpoints,
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
