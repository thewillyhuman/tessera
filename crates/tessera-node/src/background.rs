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
//! a few heartbeats, or, whatever the view says, when rows have waited
//! in its log for a long time without a flush. Writers of this process
//! are never recovered; the ingester ends or rotates its own logs.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tessera_catalog::{Catalog, ControlStores, Table, TenantSettingsSource};
use tessera_cluster::{Load, Membership, Work};
use tessera_compact::gc::{DEFAULT_FILE_RETENTION, DEFAULT_SEGMENT_GRACE};
use tessera_compact::{
    Checkpoints, Compactor, Outcome, Policy, Retain, Spared, collect_files, collect_log,
    input_bytes, plan,
};
use tessera_eval::AlertRule;
use tessera_eval::rule::decode_rows;
use tessera_format::FileFormat;
use tessera_iceberg::{Exporter, Outcome as Export};
use tessera_manifest::{
    DatasetLog, DatasetLogs, DatasetState, Manifest, ManifestState, Manifests, Registration,
    intake, list_datasets, list_kinds, list_partitions, registry,
};
use tessera_object::{ObjectStore, Pools};
use tessera_types::{
    Clock, DatasetName, Env, FileMeta, Held, Kind, Level, Meter, PartitionId, PoolId,
    RetentionPolicy, Seq, TenantId, Timestamp, Window, Windows, WorkKind, WriterId,
};
use tessera_wal::{EntryBody, FlushTarget, reader, recover};

use crate::leases::{COMPACT_KINDS, Owned};
use crate::retention;

/// How often the background work runs by default.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);
/// How long the writer of a node that left the membership view may be
/// quiet before it is recovered: enough for a registration to reach the
/// view before its node does.
const DEAD_QUIET: Duration = Duration::from_secs(15);
/// How long rows may wait in a quiet log of a node that is in the view
/// before its writer is recovered: more than twice what a live
/// ingester lets them wait for a flush.
const RECOVER_AFTER: Duration = Duration::from_secs(300);
/// Bytes of files of an older layout epoch rewritten per round (AD-10).
const RECLUSTER_BUDGET: u64 = 256 << 20;
/// How often what the partitions hold is recorded in `system.usage`
/// (section 13.1): the grain a storage bill is reconciled at. It is
/// also how often every tenant is gone over, at rest or not
/// ([`Visit`]): recording what is held takes every partition.
const HELD_EVERY: Duration = Duration::from_secs(3_600);

/// How often the writers' logs are looked over for entries to collect
/// (GC-2). An entry stays an hour after it is adopted, so a look every
/// round finds what the one before found; a minute's delay on an hour
/// is not felt.
const COLLECT_EVERY: Duration = Duration::from_secs(60);

/// How many looks at the logs pass between two reads of the rules of
/// a tenant that had none. A rule group holds the entries it has not
/// read (GC-2), so each look reads where every group is, and a tenant
/// without rules has no group: reading its control store to find that
/// out again is a request per tenant and look. An entry is kept an
/// hour whoever holds it, so a tenant's first group is known here
/// well before an entry written after it began can go; one it had
/// still to read of those written before is gone as it would be past
/// the cap, and the group rebuilds its windows from the files
/// (section 11.7).
const RULES_EVERY: u64 = 30;

/// The tenants the last read of their rules found none for, and how
/// many looks at the logs were made.
#[derive(Debug, Default)]
struct Ruleless {
    tenants: BTreeSet<TenantId>,
    looks: u64,
}

impl Ruleless {
    /// Whether this look leaves `tenant`'s rules unread.
    fn skips(&self, tenant: &TenantId) -> bool {
        self.tenants.contains(tenant) && !self.looks.is_multiple_of(RULES_EVERY)
    }
}

/// How many rounds pass from one that goes over the whole of every
/// tenant that is not at rest to the next. Such a round asks the
/// store for every dataset of the tenant, their writers, their kinds
/// and their partitions, and reads the head of every manifest: what
/// it asks grows with what those tenants hold, whether or not
/// anything changed. The rounds between follow the logs of the
/// writers the last one found, a read each that finds nothing when
/// nothing was flushed, and go over a dataset only when one of them
/// brought it something or the round before merged files it had
/// flushed. A new dataset, a new writer and what another
/// node adopted wait for the next whole round, half a minute at the
/// default interval.
const WHOLE_EVERY: u64 = 6;

/// The tenants that are not at rest, each with what its last whole
/// round found registered: its datasets and their writers.
///
/// A tenant is at rest once a whole round found no writer registered
/// with any of its datasets and nothing to do in them: nothing is
/// being written to it, and what its partitions hold is as compact as
/// it gets until time passes. Such a tenant is left alone (CT-6: an
/// idle tenant costs storage only), and the rounds ask nothing of the
/// store for it. It is gone over again when a node says it began to
/// write to it ([`tessera_cluster::Woken`]), when the slots this node
/// owns change, and once an hour with every other tenant, which is
/// when its retention, the compaction of a window that closed, a
/// layout the advisor adopted and a delete are acted on.
type Known = BTreeMap<TenantId, Vec<Found>>;

/// What the last round over a dataset left for the rounds after it.
struct Found {
    dataset: DatasetName,
    /// The writers registered with it, whose logs are followed.
    writers: Vec<Registration>,
    /// Whether the round merged flushed files of the dataset. A round
    /// runs one job a partition, and a partition that had one due may
    /// have another: the next round goes over the dataset again,
    /// without waiting for a whole one, until a round merges nothing.
    merged: bool,
}

/// Which tenants a round goes over, and how.
#[derive(Clone, Copy, Debug)]
struct Visit {
    /// Every tenant is gone over whole, at rest or not.
    every: bool,
    /// The tenants that are not at rest are gone over whole; their
    /// writers' logs are followed otherwise.
    whole: bool,
}

/// Whether the round numbered `round` goes over every tenant: the
/// first does, and after it a whole round does when what the
/// partitions hold is due to be recorded or the node's slots moved.
/// Slots move one after the other while a node joins or leaves, and
/// a round over every tenant for each move would be many of them at
/// the moment the nodes have the least to spare: the moves since the
/// last such round wait for the next whole one.
fn goes_over_every_tenant(round: u64, held_due: bool, slots_moved: bool) -> bool {
    round.is_multiple_of(WHOLE_EVERY) && (round == 0 || held_due || slots_moved)
}

/// How a round goes over one tenant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pass {
    /// Not at all: the tenant is at rest.
    None,
    /// By following the logs of the writers its last whole round found.
    Follow,
    /// Whole: every dataset, writer and partition.
    Whole,
}

impl Visit {
    /// How a tenant is gone over: `open` when it is not at rest,
    /// `woken` when a node says it began to write to it.
    fn pass(self, open: bool, woken: bool) -> Pass {
        if self.every || (open && self.whole) || (!open && woken) {
            Pass::Whole
        } else if open {
            Pass::Follow
        } else {
            Pass::None
        }
    }
}

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
    /// The partitions' manifests, kept open from one round to the next
    /// so that a round reads what a partition committed since the last
    /// and not its history again.
    pub manifests: Arc<Manifests>,
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

impl Background {
    /// Does `work` and counts the time it took as `kind`'s.
    async fn timed<T>(&self, kind: Work, work: impl Future<Output = T>) -> T {
        timed(&*self.env.clock, &self.load, kind, work).await
    }
}

/// Does `work` and adds the time it took by `clock` to what `load`
/// reports of `kind` (`tessera_background_seconds_total`).
async fn timed<T>(clock: &dyn Clock, load: &Load, kind: Work, work: impl Future<Output = T>) -> T {
    let started = clock.now();
    let done = work.await;
    load.worked(kind, since(clock, started));
    done
}

/// How long ago `started` was by `clock`; nothing if the clock went
/// back.
fn since(clock: &dyn Clock, started: Timestamp) -> Duration {
    clock
        .now()
        .duration_since(started)
        .unwrap_or(Duration::ZERO)
}

/// The time `load` counted so far for the kinds of work that are
/// timed one by one, which is every kind but the rest of a round.
fn timed_so_far(load: &Load) -> Duration {
    load.snapshot()
        .background
        .iter()
        .filter(|(kind, _)| *kind != Work::Other.name())
        .map(|(_, took)| *took)
        .sum()
}

/// Where intake stopped in each writer's log, per dataset: a round
/// reads on from there (IT-2 lets one start over, so nothing is lost
/// with this memory but the reading it saves).
type Positions = BTreeMap<(TenantId, DatasetName, WriterId), Seq>;

/// What a round left undone, had to repair or did.
#[derive(Clone, Copy, Debug, Default)]
struct Backlog {
    /// Compaction jobs deferred by the re-clustering budget.
    deferred: u64,
    /// Writers whose logs had to be recovered.
    recoveries: u64,
    /// What it changed: flushes adopted, partitions compacted or
    /// dropped, files and tombstones collected. A round that changed
    /// something may have left the next one something to do.
    changes: u64,
    /// Jobs that merged flushed files, each of which may have left
    /// its partition another that is due.
    merges: u64,
}

impl Backlog {
    /// This backlog and `other` as one.
    fn and(self, other: Backlog) -> Backlog {
        Backlog {
            deferred: self.deferred + other.deferred,
            recoveries: self.recoveries + other.recoveries,
            changes: self.changes + other.changes,
            merges: self.merges + other.merges,
        }
    }

    /// Whether the round found nothing to do and left nothing undone.
    fn is_none(self) -> bool {
        self.deferred == 0 && self.recoveries == 0 && self.changes == 0
    }
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
    let mut last_collection: Option<Timestamp> = None;
    let mut ruleless = Ruleless::default();
    let mut positions = Positions::new();
    let mut known = Known::new();
    let mut slots = None;
    let mut rounds: u64 = 0;
    loop {
        ticks.tick().await;
        let now = background.env.clock.now();
        let timed_before = timed_so_far(&background.load);
        // Every tenant is gone over when what the partitions hold is
        // to be recorded, and when the slots this node owns changed
        // since it last went over them all: a tenant at rest may have
        // work in a slot that was another node's, and at its start a
        // node owns none.
        let owned = COMPACT_KINDS.map(|kind| background.owned.slots(kind));
        let owns_partitions = !background.owned.slots(WorkKind::Partition).is_empty();
        let held_due = owns_partitions && is_due(last_held, now, HELD_EVERY);
        let whole_round = rounds.is_multiple_of(WHOLE_EVERY);
        let every = goes_over_every_tenant(rounds, held_due, slots.as_ref() != Some(&owned));
        if every {
            slots = Some(owned);
        }
        let visit = Visit {
            every,
            whole: whole_round,
        };
        rounds += 1;
        let tenants: Vec<TenantId> = background
            .catalog
            .cached()
            .state()
            .tenants()
            .map(|t| t.id.clone())
            .collect();
        let mut held = Vec::new();
        positions.retain(|(tenant, _, _), _| tenants.contains(tenant));
        known.retain(|tenant, _| tenants.contains(tenant));
        let backlog = tenants_round(
            &background,
            &compactor,
            &tenants,
            visit,
            (&mut positions, &mut known),
            &mut held,
        )
        .await;
        // What a round between two whole ones left undone says nothing
        // of the datasets it did not go over. A tenant at rest has
        // nothing undone.
        if whole_round {
            background.load.set_compaction_backlog(backlog.deferred);
            background.load.set_intake_backlog(backlog.recoveries);
        }
        // What the owned partitions hold, once an hour, from the first
        // round in which the node owns any: each partition has one
        // owner, so the cluster records each once. Only a round over
        // every tenant has gone over every partition this node owns.
        if every && held_due {
            if !held.is_empty() {
                background.meter.held(held);
            }
            last_held = Some(now);
        }
        if is_due(last_collection, now, COLLECT_EVERY) {
            // A look that failed is taken again next round.
            let collected = background
                .timed(Work::Collection, collect_logs(&background, &mut ruleless))
                .await;
            match collected {
                Ok(()) => last_collection = Some(now),
                Err(error) => tracing::warn!(error = %error, "segment collection failed"),
            }
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
        // What the round took besides the work timed within it: the
        // looking for what there is to do.
        let timed_in_round = timed_so_far(&background.load).saturating_sub(timed_before);
        let round = since(&*background.env.clock, now);
        background
            .load
            .worked(Work::Other, round.saturating_sub(timed_in_round));
    }
}

/// One round over `tenants`: a whole one, or one that follows what
/// the last whole one found. A tenant whose round fails is logged and
/// has its next.
async fn tenants_round(
    background: &Background,
    compactor: &Compactor,
    tenants: &[TenantId],
    visit: Visit,
    (positions, known): (&mut Positions, &mut Known),
    held: &mut Vec<Held>,
) -> Backlog {
    let mut backlog = Backlog::default();
    let mut quiet = BTreeSet::new();
    let woken = background.membership.woken();
    for tenant in tenants {
        let pass = visit.pass(known.contains_key(tenant), woken.contains(tenant));
        if pass == Pass::None {
            continue;
        }
        let mut cursors = Cursors {
            positions: &mut *positions,
            met: BTreeSet::new(),
            whole: pass == Pass::Whole,
            quiet: &mut quiet,
        };
        let found = known.entry(tenant.clone()).or_default();
        let done = if pass == Pass::Whole {
            round(background, compactor, tenant, held, &mut cursors, found).await
        } else {
            follow(background, compactor, tenant, held, &mut cursors, found).await
        };
        match done {
            Ok(left) => {
                backlog = backlog.and(left);
                // A writer a whole round did not meet has left its
                // registry or this node's care: its position goes
                // with it. A round that failed in part met only some.
                let Cursors { met, whole, .. } = cursors;
                if whole {
                    positions.retain(|at, _| at.0 != *tenant || met.contains(at));
                    // With no writer registered and nothing found to
                    // do, the tenant is at rest.
                    if left.is_none() && found.iter().all(|found| found.writers.is_empty()) {
                        known.remove(tenant);
                    }
                }
            }
            Err(error) => {
                tracing::warn!(tenant = %tenant, error = %error, "background round failed");
            }
        }
    }
    backlog
}

/// Whether what was last done at `last`, if ever, is to be done again
/// at `now`: when it never was, or was `every` ago or longer.
fn is_due(last: Option<Timestamp>, now: Timestamp, every: Duration) -> bool {
    last.is_none_or(|at| now.duration_since(at).is_some_and(|since| since >= every))
}

/// Publishes the Iceberg metadata of every table this node owns the
/// export of (OP-9): one table per dataset and kind, its export the
/// partition slot of the table's key. Returns how many tables there
/// are, published now or before.
async fn export_round(background: &Background, exporter: &Exporter, tenants: &[TenantId]) -> usize {
    let mut tables = 0;
    for tenant in tenants {
        let datasets = match list_datasets(&*background.store, tenant).await {
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
    /// Whether every part of the round went through: one that failed
    /// may have met only some of its logs.
    whole: bool,
    /// The logs a round that follows found nothing new in, by writer
    /// and position. A writer is registered in every dataset it wrote
    /// to, and its log is one: asked once whether it goes on, the
    /// answer holds for all of them.
    quiet: &'a mut BTreeSet<(WriterId, Seq)>,
}

/// A whole round of a tenant: every dataset it has, gone over; what
/// it found registered is left in `found` for the rounds that follow.
async fn round(
    background: &Background,
    compactor: &Compactor,
    tenant: &TenantId,
    held: &mut Vec<Held>,
    cursors: &mut Cursors<'_>,
    found: &mut Vec<Found>,
) -> Result<Backlog, Failure> {
    let mut backlog = Backlog::default();
    found.clear();
    // Every dataset the tenant has anything of, with a writer or not:
    // what a dataset holds is compacted, retired and collected after
    // its writers ended, as all do when their nodes are replaced.
    for dataset in list_datasets(&*background.store, tenant).await? {
        // A dataset's failure is its own: the tenant's other datasets
        // still get their round, and this one its next.
        match dataset_round(background, compactor, tenant, &dataset, held, cursors).await {
            Ok((left, writers)) => {
                backlog = backlog.and(left);
                found.push(Found {
                    dataset,
                    writers,
                    merged: left.merges > 0,
                });
            }
            Err(error) => {
                cursors.whole = false;
                tracing::warn!(%tenant, %dataset, error = %error, "background round failed");
            }
        }
    }
    Ok(backlog)
}

/// A round between two whole ones: the logs of the writers the last
/// whole round found are followed, and a dataset one of them brought
/// something to is gone over at once, so that what was flushed reaches
/// its manifests and is compacted as soon as it would in a whole
/// round. So is a dataset the round before merged flushed files of,
/// which may have more of them due. Nothing is listed where nothing
/// was flushed or merged.
async fn follow(
    background: &Background,
    compactor: &Compactor,
    tenant: &TenantId,
    held: &mut Vec<Held>,
    cursors: &mut Cursors<'_>,
    found: &mut [Found],
) -> Result<Backlog, Failure> {
    let mut backlog = Backlog::default();
    for Found {
        dataset,
        writers,
        merged,
    } in found
    {
        let mut changed = false;
        for registration in writers.iter() {
            let writer = registration.writer;
            if !background
                .owned
                .owns(WorkKind::Intake, writer.to_string().as_bytes())
            {
                continue;
            }
            let at = (tenant.clone(), dataset.clone(), writer);
            let after = cursors.positions.get(&at).copied().unwrap_or(Seq::ZERO);
            if cursors.quiet.contains(&(writer, after)) {
                continue;
            }
            let following = intake::run_following(
                Arc::clone(&background.store),
                &background.env,
                tenant,
                dataset,
                writer,
                after,
            );
            let followed = background.timed(Work::Intake, following).await;
            match followed {
                Ok(outcome) => {
                    if outcome.through == after {
                        cursors.quiet.insert((writer, after));
                    }
                    cursors.positions.insert(at, outcome.through);
                    if outcome.adoptions > 0 || outcome.removed {
                        tracing::info!(%tenant, %dataset, %writer, adoptions = outcome.adoptions, removed = outcome.removed, "intake");
                        changed = true;
                    }
                }
                Err(error) => {
                    tracing::warn!(%tenant, %dataset, %writer, error = %error, "background round failed");
                }
            }
        }
        if !changed && !*merged {
            continue;
        }
        match dataset_round(background, compactor, tenant, dataset, held, cursors).await {
            Ok((left, listed)) => {
                backlog = backlog.and(left);
                *writers = listed;
                *merged = left.merges > 0;
            }
            Err(error) => {
                // What failed is tried again by the next whole round,
                // not by every round until then.
                *merged = false;
                tracing::warn!(%tenant, %dataset, error = %error, "background round failed");
            }
        }
    }
    Ok(backlog)
}

/// One dataset's part of a tenant's round: intake and recovery for its
/// writers, then compaction and collection of its partitions, kind by
/// kind. A kind that fails is logged and left for the next round; the
/// kinds after it are still gone through. Returns the writers it found
/// registered besides.
async fn dataset_round(
    background: &Background,
    compactor: &Compactor,
    tenant: &TenantId,
    dataset: &DatasetName,
    held: &mut Vec<Held>,
    cursors: &mut Cursors<'_>,
) -> Result<(Backlog, Vec<Registration>), Failure> {
    let mut backlog = Backlog::default();
    let registrations = registry::list(&*background.store, tenant, dataset).await?;
    for registration in &registrations {
        let writer = registration.writer;
        let key = writer.to_string();
        let mut ended = false;
        if background.owned.owns(WorkKind::Intake, key.as_bytes()) {
            let at = (tenant.clone(), dataset.clone(), writer);
            let after = cursors.positions.get(&at).copied().unwrap_or(Seq::ZERO);
            cursors.met.insert(at.clone());
            // A run that fails leaves the position where it was, to
            // be repeated from there.
            let run = intake::run_after(
                Arc::clone(&background.store),
                &background.env,
                tenant,
                dataset,
                writer,
                after,
            );
            let outcome = background.timed(Work::Intake, run).await?;
            cursors.positions.insert(at, outcome.through);
            if outcome.adoptions > 0 || outcome.removed {
                tracing::info!(%tenant, %dataset, %writer, adoptions = outcome.adoptions, removed = outcome.removed, "intake");
                backlog.changes += 1;
            }
            ended = outcome.ended;
        }
        if !ended
            && background.owned.owns(WorkKind::Recovery, key.as_bytes())
            && is_dead(background, registration).await?
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
    let settings = background.datasets.open(tenant, dataset).state().await?;
    let held_kinds = list_kinds(&*background.store, tenant, dataset).await?;
    for kind in Kind::ALL {
        // A kind the dataset holds nothing of has no partition to
        // look for; only its tombstones, if it has any, still have
        // to be seen through.
        if !held_kinds.contains(&kind) && settings.tombstones(kind)?.is_empty() {
            continue;
        }
        let compacted = compact_and_collect(
            background,
            compactor,
            tenant,
            dataset,
            &settings,
            kind,
            &mut budget,
            &policies,
            held,
        )
        .await;
        match compacted {
            Ok(left) => backlog = backlog.and(left),
            Err(error) => {
                cursors.whole = false;
                tracing::warn!(%tenant, %dataset, %kind, error = %error, "background round failed");
            }
        }
    }
    Ok((backlog, registrations))
}

/// Compacts every partition of a dataset and kind that is due, rewrites
/// files of an older layout epoch within `budget`, recent partitions
/// first (AD-10), and collects replaced files. Returns how many jobs
/// the budget deferred and how much it changed.
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
) -> Result<Backlog, Failure> {
    let mut deferred = 0;
    let mut changes = 0;
    let mut merges = 0;
    let now = background.env.clock.now();
    let record = settings.layout(kind)?;
    let value_score = settings.value_score(kind);
    let tombstones = settings.tombstones(kind)?;
    let mut partitions = list_partitions(&*background.store, tenant, dataset, kind).await?;
    partitions.sort_by_key(|p| std::cmp::Reverse(p.window.start()));
    let every = partitions.clone();
    for partition in partitions {
        if !background
            .owned
            .owns(WorkKind::Partition, partition.to_string().as_bytes())
        {
            continue;
        }
        let manifest = background.manifests.open(&partition);
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
                    changes += 1;
                }
                let dropped =
                    collect_files(&manifest, &background.pools, now, DEFAULT_FILE_RETENTION);
                background.timed(Work::Collection, dropped).await?;
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
            changes += 1;
            merges += u64::from(job.inputs.iter().any(|f| f.level == Level::L1));
            if let Some(retain) = retain.as_mut().filter(|r| spares(r, kind, job.level)) {
                retain.spared = Arc::new(spared_beside(background, &partition, &every).await?);
            }
            let run = compactor.run(&manifest, &job, &record, retain.as_ref(), &tombstones);
            let outcome = background.timed(Work::Compaction, run).await?;
            compacted(&manifest, &partition, outcome).await;
        }
        let replaced = collect_files(&manifest, &background.pools, now, DEFAULT_FILE_RETENTION);
        let collected = background.timed(Work::Collection, replaced).await?;
        if collected > 0 {
            tracing::info!(%tenant, %dataset, %kind, window = %partition.window, collected, "files collected");
            changes += 1;
        }
    }
    if !tombstones.is_empty() {
        let key = format!("{tenant}/{dataset}/{kind}/tombstones");
        if background.owned.owns(WorkKind::Partition, key.as_bytes()) {
            let log = background.datasets.open(tenant, dataset);
            let removed = collect_tombstones(background, &log, tenant, dataset, kind, now).await?;
            if removed > 0 {
                tracing::info!(%tenant, %dataset, %kind, removed, "tombstones applied everywhere");
                changes += 1;
            }
        }
    }
    Ok(Backlog {
        deferred,
        changes,
        merges,
        ..Backlog::default()
    })
}

/// Whether a job that writes at `level` samples the spans of a
/// partition of `kind` sparing the traces with errors: the job for
/// which the traces to spare are worth finding.
fn spares(retain: &Retain, kind: Kind, level: Level) -> bool {
    retain.except_errors && retain.sample.is_some() && kind == Kind::Span && level == Level::L3
}

/// Whether what is in `other` can belong with what is in `window`:
/// the two overlap or lie within a window's length of each other. A
/// trace that crosses the boundary of `window` has its other spans
/// there, and so has an alert about it.
fn is_beside(window: Window, other: Window) -> bool {
    let reach = i64::try_from(window.length().as_nanos()).unwrap_or(i64::MAX);
    let (start, end) = (window.start().as_nanos(), window.end().as_nanos());
    other.start().as_nanos() < end.saturating_add(reach)
        && other.end().as_nanos() > start.saturating_sub(reach)
}

/// The traces a policy that spares errors is to keep whole in the span
/// partition `partition` though the rows being sampled may not say so
/// (CO-7): those with an error span in the partition, which a job
/// may sample a part of at a time, or in the partitions beside it, of
/// `spans`, and those an alert event there names. A trace longer than
/// a window that fails further away is not found.
async fn spared_beside(
    background: &Background,
    partition: &PartitionId,
    spans: &[PartitionId],
) -> Result<Spared, Failure> {
    let (tenant, dataset) = (&partition.tenant, &partition.dataset);
    let alerts = list_partitions(&*background.store, tenant, dataset, Kind::Alert).await?;
    let mut files: [Vec<FileMeta>; 2] = Default::default();
    let beside = |others: &[PartitionId]| -> Vec<PartitionId> {
        others
            .iter()
            .filter(|other| is_beside(partition.window, other.window))
            .cloned()
            .collect()
    };
    for (kind, others) in [beside(spans), beside(&alerts)].into_iter().enumerate() {
        for other in others {
            let snapshot = background.manifests.open(&other).snapshot().await?;
            files[kind].extend(snapshot.state().files().cloned());
        }
    }
    let spared = tessera_compact::spared::traces(
        &background.pools,
        &background.format,
        &files[0],
        &files[1],
    )
    .await?;
    if !spared.is_empty() {
        tracing::info!(%tenant, %dataset, window = %partition.window, traces = spared.len(), "traces spared from sampling");
    }
    Ok(spared)
}

/// What follows a compaction of `partition`: it is logged, and a
/// rewrite is followed by a checkpoint of the manifest (CL-3, MF-5),
/// so that a cold reader folds the few commits since it and a range
/// read fetches only the row groups it needs.
async fn compacted(manifest: &Manifest, partition: &PartitionId, outcome: Outcome) {
    let PartitionId {
        tenant,
        dataset,
        kind,
        window,
    } = partition;
    match outcome {
        Outcome::Replaced {
            inputs,
            output,
            rows,
            duplicates,
        } => {
            tracing::info!(%tenant, %dataset, %kind, %window, inputs, %output, rows, duplicates, "compacted");
            if let Err(error) = manifest.checkpoint().await {
                tracing::warn!(%tenant, %dataset, %kind, %window, error = %error, "manifest checkpoint failed");
            }
        }
        Outcome::Superseded => {
            tracing::info!(%tenant, %dataset, %kind, %window, "compaction superseded");
        }
    }
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
        let manifest = background.manifests.open(&partition);
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
async fn checkpoints(
    background: &Background,
    ruleless: &mut Ruleless,
) -> Result<Checkpoints, Failure> {
    let mut checkpoints = Checkpoints::none();
    let tenants: Vec<TenantId> = background
        .catalog
        .cached()
        .state()
        .tenants()
        .map(|t| t.id.clone())
        .collect();
    ruleless.tenants.retain(|tenant| tenants.contains(tenant));
    for tenant in tenants {
        if ruleless.skips(&tenant) {
            continue;
        }
        let rows = background
            .controls
            .open(&tenant)
            .rows(Table::AlertRules)
            .await?;
        let (rules, _) = decode_rows::<AlertRule>(&rows);
        let groups: BTreeSet<String> = rules.into_iter().map(|r| r.group).collect();
        if groups.is_empty() {
            ruleless.tenants.insert(tenant);
            continue;
        }
        ruleless.tenants.remove(&tenant);
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
    ruleless.looks += 1;
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
async fn collect_logs(background: &Background, ruleless: &mut Ruleless) -> Result<(), Failure> {
    let now = background.env.clock.now();
    let checkpoints = checkpoints(background, ruleless).await?;
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
            &background.datasets,
            &checkpoints,
        )
        .await?;
        if collected.deleted > 0 {
            tracing::info!(%writer, deleted = collected.deleted, gone = collected.log_gone, "segments collected");
        }
    }
    Ok(())
}

/// Whether a foreign writer counts as dead ([`dead`]). A writer of
/// this node or one whose log has ended is never dead.
async fn is_dead(background: &Background, registration: &Registration) -> Result<bool, Failure> {
    if registration.node == background.env.node {
        return Ok(false);
    }
    let Some(log) = quiet_for(background, registration.writer).await? else {
        return Ok(false);
    };
    let gone = !background.membership.is_live(registration.node);
    Ok(dead(log, gone))
}

/// A writer's log that has not ended, as recovery judges it.
#[derive(Clone, Copy, Debug)]
struct Quiet {
    /// How long since its last entry; forever for a log without one.
    since: Duration,
    /// Whether its last entry is a segment: rows no flush has taken.
    holds_rows: bool,
}

/// Whether the writer of a log is to be recovered. One whose node is
/// gone from the view is, once its log has been quiet for a few
/// heartbeats. One whose node is in the view is only when rows have
/// been waiting in its log for longer than a live ingester leaves them
/// unflushed: a log that is quiet because nothing is written to it,
/// its last entry a flush, has nothing to recover, and recovering it
/// fenced a writer that was merely idle, every five minutes.
fn dead(log: Quiet, gone: bool) -> bool {
    if gone {
        log.since >= DEAD_QUIET
    } else {
        log.holds_rows && log.since >= RECOVER_AFTER
    }
}

/// How long a writer's log has been quiet and whether rows wait in it;
/// `None` once it has ended. A log without entries has been quiet
/// forever.
async fn quiet_for(background: &Background, writer: WriterId) -> Result<Option<Quiet>, Failure> {
    let Some(last) = reader::last(&*background.store, writer).await? else {
        return Ok(Some(Quiet {
            since: Duration::MAX,
            holds_rows: false,
        }));
    };
    // The header says what the entry is and when it was written; a
    // segment's rows are not asked for.
    let keys = &background.env.keys;
    let entry = reader::header(&*background.store, writer, last, keys).await?;
    if matches!(entry.body, EntryBody::End) {
        return Ok(None);
    }
    let now = background.env.clock.now();
    Ok(Some(Quiet {
        since: now
            .duration_since(entry.written_at)
            .unwrap_or(Duration::ZERO),
        holds_rows: matches!(entry.body, EntryBody::Segment { .. }),
    }))
}

#[cfg(test)]
mod tests {
    use tessera_types::ManualClock;

    use super::*;

    /// ST-9, ST-10: a writer is recovered when its node is gone, or
    /// when rows wait in its log that a live ingester would have
    /// flushed long ago. On a cluster, every idle writer was fenced
    /// five minutes after its last write, its node alive and its log
    /// flushed, and started a new log on the next.
    #[test]
    fn a_live_nodes_idle_writer_is_not_recovered() {
        let log = |seconds: u64, holds_rows: bool| Quiet {
            since: Duration::from_secs(seconds),
            holds_rows,
        };
        assert!(!dead(log(3_600, false), false), "idle, flushed, alive");
        assert!(dead(log(300, true), false), "rows left waiting");
        assert!(!dead(log(299, true), false), "not for long enough");
        assert!(dead(log(15, false), true), "its node is gone");
        assert!(
            !dead(log(14, false), true),
            "a registration ahead of the view"
        );
        let never_written = Quiet {
            since: Duration::MAX,
            holds_rows: false,
        };
        assert!(!dead(never_written, false));
        assert!(dead(never_written, true));
    }

    /// CT-6: a tenant at rest is asked nothing of until a node says it
    /// began to write to it, or every tenant is gone over.
    #[test]
    fn a_tenant_at_rest_is_gone_over_when_it_wakes_or_all_are() {
        let between = Visit {
            every: false,
            whole: false,
        };
        let whole = Visit {
            every: false,
            whole: true,
        };
        let every = Visit {
            every: true,
            whole: true,
        };
        let (open, at_rest, woken, quiet) = (true, false, true, false);
        // At rest: nothing, round after round.
        assert_eq!(between.pass(at_rest, quiet), Pass::None);
        assert_eq!(whole.pass(at_rest, quiet), Pass::None);
        // Written to again: gone over at once, whatever the round.
        assert_eq!(between.pass(at_rest, woken), Pass::Whole);
        assert_eq!(whole.pass(at_rest, woken), Pass::Whole);
        // Not at rest: as before, and a node that still names it
        // costs it nothing more.
        assert_eq!(between.pass(open, quiet), Pass::Follow);
        assert_eq!(between.pass(open, woken), Pass::Follow);
        assert_eq!(whole.pass(open, quiet), Pass::Whole);
        // Every tenant, once an hour.
        assert_eq!(every.pass(at_rest, quiet), Pass::Whole);
        assert_eq!(every.pass(open, quiet), Pass::Whole);
    }

    #[test]
    fn every_tenant_is_gone_over_at_a_whole_round_that_has_a_reason() {
        let (due, moved, no) = (true, true, false);
        assert!(goes_over_every_tenant(0, no, no), "the first round");
        // Nothing to record and no slot moved: the tenants at rest
        // are left alone, round after round.
        assert!((1..100).all(|round| !goes_over_every_tenant(round, no, no)));
        // A slot that moved, or what is held being due, waits for the
        // next whole round.
        for round in 1..=12 {
            let whole = round % WHOLE_EVERY == 0;
            assert_eq!(goes_over_every_tenant(round, no, moved), whole, "{round}");
            assert_eq!(goes_over_every_tenant(round, due, no), whole, "{round}");
        }
    }

    #[test]
    fn a_round_that_did_or_left_something_keeps_its_tenant_from_rest() {
        assert!(Backlog::default().is_none());
        let one = [
            Backlog {
                deferred: 1,
                ..Backlog::default()
            },
            Backlog {
                recoveries: 1,
                ..Backlog::default()
            },
            Backlog {
                changes: 1,
                ..Backlog::default()
            },
        ];
        for backlog in one {
            assert!(!backlog.is_none(), "{backlog:?}");
            assert!(!Backlog::default().and(backlog).is_none());
        }
        let all = one.into_iter().fold(Backlog::default(), Backlog::and);
        assert_eq!((all.deferred, all.recoveries, all.changes), (1, 1, 1));
    }

    #[test]
    fn the_rules_of_a_tenant_that_had_none_are_read_one_look_in_thirty() {
        let (acme, globex) = (
            TenantId::new("acme").unwrap(),
            TenantId::new("globex").unwrap(),
        );
        let mut ruleless = Ruleless::default();
        ruleless.tenants.insert(acme.clone());
        let read: Vec<u64> = (0..61)
            .filter(|look| {
                ruleless.looks = *look;
                !ruleless.skips(&acme)
            })
            .collect();
        assert_eq!(read, [0, 30, 60]);
        // One that has rules, or was never read, is read every look.
        ruleless.looks = 7;
        assert!(!ruleless.skips(&globex));
    }

    /// CO-7: the traces to spare are looked for in the windows a trace
    /// of this one can reach into, and for a job that samples spans
    /// sparing errors only.
    #[test]
    fn traces_to_spare_are_looked_for_in_the_windows_beside() {
        let hour = |n: i64| {
            Window::containing(
                Timestamp::from_nanos(n * 3_600_000_000_000),
                Duration::from_secs(3_600),
            )
            .unwrap()
        };
        let noon = hour(12);
        assert!(is_beside(noon, hour(11)) && is_beside(noon, hour(13)));
        assert!(is_beside(noon, noon), "an alert of the same hour");
        assert!(!is_beside(noon, hour(10)) && !is_beside(noon, hour(14)));
        // A day of alerts holds the hour.
        let day = Window::containing(noon.start(), Duration::from_secs(86_400)).unwrap();
        assert!(is_beside(noon, day));

        let sampling = Retain {
            sample: Some(0.1),
            except_errors: true,
            ..Retain::default()
        };
        assert!(spares(&sampling, Kind::Span, Level::L3));
        assert!(!spares(&sampling, Kind::Log, Level::L3));
        assert!(!spares(&sampling, Kind::Span, Level::L2), "not sampled yet");
        let all = Retain {
            except_errors: false,
            ..sampling.clone()
        };
        assert!(!spares(&all, Kind::Span, Level::L3));
        let dropping = Retain {
            sample: None,
            ..sampling
        };
        assert!(!spares(&dropping, Kind::Span, Level::L3));
    }

    #[tokio::test]
    async fn the_time_a_piece_of_work_takes_is_counted_as_its_kind() {
        let clock = ManualClock::new(Timestamp::from_nanos(0));
        let load = Load::new();
        let intake = async {
            clock.advance(Duration::from_secs(3));
            7
        };
        assert_eq!(timed(&clock, &load, Work::Intake, intake).await, 7);
        timed(&clock, &load, Work::Compaction, async {
            clock.advance(Duration::from_secs(20));
        })
        .await;
        assert_eq!(
            load.snapshot().background[..2],
            [
                ("intake", Duration::from_secs(3)),
                ("compaction", Duration::from_secs(20)),
            ]
        );
        // The rest of a round is what it took besides these.
        assert_eq!(timed_so_far(&load), Duration::from_secs(23));
        load.worked(Work::Other, Duration::from_secs(2));
        assert_eq!(timed_so_far(&load), Duration::from_secs(23));
    }

    #[test]
    fn periodic_work_is_due_at_first_and_then_every_interval() {
        let at = |seconds: i64| Timestamp::from_nanos(seconds * 1_000_000_000);
        assert!(is_due(None, at(0), COLLECT_EVERY));
        assert!(!is_due(Some(at(0)), at(59), COLLECT_EVERY));
        assert!(is_due(Some(at(0)), at(60), COLLECT_EVERY));
        // A clock set back does not bring the work forward.
        assert!(!is_due(Some(at(60)), at(0), COLLECT_EVERY));
    }
}
