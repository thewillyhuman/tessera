// SPDX-License-Identifier: Apache-2.0

//! Garbage collection (section 6.10).
//!
//! Files: a tombstone older than the snapshot-retention window means no
//! snapshot a reader could still hold references the file (GC-1); its
//! data file and sidecar are deleted, then the tombstone is dropped.
//!
//! Segments: a segment is deleted once every partition its rows touched
//! has adopted past it, a reader grace period has elapsed since it was
//! written, and every rule group has read past it or it is older than
//! the hard cap (GC-2). Entries are collected in order from the oldest;
//! the first entry that must stay stops the pass, so a log never has a
//! hole a reader could fall into. A log whose every entry can go,
//! including its end marker, is removed whole.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use tessera_manifest::{DatasetLogs, Manifest, list_partitions};
use tessera_object::{ObjectStore, Pools, layout};
use tessera_types::{
    DatasetName, Env, FileId, Kind, Seq, TenantId, TimeRange, Timestamp, WindowSchedule, WriterId,
};
use tessera_wal::{BlockMeta, EntryBody, reader};

use crate::CompactError;

/// How long a removed file's object is kept for readers of older
/// snapshots (GC-1).
pub const DEFAULT_FILE_RETENTION: Duration = Duration::from_secs(24 * 3_600);
/// How many entries of a log a pass asks about or deletes at once.
const ENTRIES_AT_ONCE: usize = 16;
/// How long an adopted segment is kept for readers of older views (GC-2).
pub const DEFAULT_SEGMENT_GRACE: Duration = Duration::from_secs(3_600);

/// Where the continuous queries are in the writer logs (GC-2 c): the
/// oldest position any rule group holds per writer. An entry past a
/// held position stays until the group reads it, or until it is older
/// than `cap`, the hard cap after which a group that stayed behind
/// rebuilds its windows from the files instead (section 11.7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoints {
    /// The oldest position held per writer.
    pub positions: BTreeMap<WriterId, Seq>,
    /// How old an entry may be before a held position stops keeping it.
    pub cap: Duration,
}

impl Checkpoints {
    /// The hard cap of GC-2.
    pub const DEFAULT_CAP: Duration = Duration::from_secs(24 * 3_600);

    /// No continuous query: nothing is held.
    #[must_use]
    pub fn none() -> Self {
        Checkpoints {
            positions: BTreeMap::new(),
            cap: Self::DEFAULT_CAP,
        }
    }

    /// Records that a group holds `seq` of `writer`; the oldest wins.
    pub fn hold(&mut self, writer: WriterId, seq: Seq) {
        self.positions
            .entry(writer)
            .and_modify(|held| *held = (*held).min(seq))
            .or_insert(seq);
    }

    /// Whether entry `seq` of `writer`, `age` old, is kept for a group
    /// that has not read it yet.
    #[must_use]
    pub fn keeps(&self, writer: WriterId, seq: Seq, age: Duration) -> bool {
        age < self.cap && self.positions.get(&writer).is_some_and(|held| *held < seq)
    }
}

impl Default for Checkpoints {
    fn default() -> Self {
        Self::none()
    }
}

/// Deletes the objects of tombstones older than `retention` and forgets
/// them. Returns how many files were collected.
///
/// # Errors
///
/// If the manifest cannot be read or committed, or a pool is unknown. A
/// failed deletion is left for the next pass.
pub async fn collect_files(
    manifest: &Manifest,
    pools: &Pools,
    now: Timestamp,
    retention: Duration,
) -> Result<usize, CompactError> {
    let snapshot = manifest.snapshot().await?;
    let mut collected: Vec<FileId> = Vec::new();
    for tombstone in snapshot.state().tombstones() {
        let expired = now
            .duration_since(tombstone.at)
            .is_some_and(|age| age >= retention);
        if !expired {
            continue;
        }
        let store = pools.get(&tombstone.meta.pool)?.store();
        let (partition, id) = (&tombstone.meta.partition, tombstone.meta.file_id);
        store.delete(&layout::data_file(partition, id)).await?;
        store.delete(&layout::sidecar(partition, id)).await?;
        collected.push(id);
    }
    if !collected.is_empty() {
        manifest.forget(&collected).await?;
    }
    Ok(collected.len())
}

/// What a pass over one writer log did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogCollection {
    /// Entries deleted.
    pub deleted: usize,
    /// Whether the whole log is gone, end marker included.
    pub log_gone: bool,
}

/// Collects the entries of `writer`'s log that every touched partition
/// has adopted, that are older than `grace`, and that `checkpoints`
/// do not keep for a rule group still to read them. What places a
/// segment's rows in partitions, the windows of their dataset (MF-2),
/// is asked of `datasets` for the datasets the pass meets, and for no
/// other.
///
/// # Errors
///
/// If the log or a manifest cannot be read, or a deletion fails.
pub async fn collect_log(
    store: &Arc<dyn ObjectStore>,
    env: &Env,
    writer: WriterId,
    now: Timestamp,
    grace: Duration,
    datasets: &DatasetLogs,
    checkpoints: &Checkpoints,
) -> Result<LogCollection, CompactError> {
    let mut positions = Positions::new(Arc::clone(store), env.clone(), datasets);
    let seqs = reader::list(&**store, writer).await?;
    let mut collection = LogCollection::default();
    let mut deletable: Vec<Seq> = Vec::new();
    let mut ended = false;
    // What an entry is and when it was written is in its header: the
    // rows of a segment are deleted without being read. The oldest
    // entry is asked about alone, since most passes find it too young
    // and need no other; the ones after it several at a time.
    let mut at_once = 1;
    let mut left = seqs.as_slice();
    'log: while !left.is_empty() {
        let (asked, rest) = left.split_at(at_once.min(left.len()));
        let headers = asked
            .iter()
            .map(|seq| reader::header(&**store, writer, *seq, &env.keys));
        for (seq, entry) in asked.iter().zip(try_join_all(headers).await?) {
            let age = now.duration_since(entry.written_at);
            let aged = age.is_some_and(|age| age >= grace);
            if !aged || checkpoints.keeps(writer, *seq, age.unwrap_or(Duration::ZERO)) {
                break 'log;
            }
            match &entry.body {
                EntryBody::Segment { blocks } => {
                    if !positions.adopted(blocks, writer, *seq).await? {
                        break 'log;
                    }
                }
                EntryBody::Flush { .. }
                | EntryBody::SealedFlush { .. }
                | EntryBody::Seal { .. } => {}
                EntryBody::End => ended = true,
            }
            deletable.push(*seq);
        }
        left = rest;
        at_once = ENTRIES_AT_ONCE;
    }
    // The newest entry stays unless the whole log goes: a log that still
    // has entries must still look ended, and a live writer's log must
    // still show where it is.
    let whole_log = ended && deletable.len() == seqs.len();
    if !whole_log {
        deletable.retain(|seq| seqs.last() != Some(seq));
    }
    for entries in deletable.chunks(ENTRIES_AT_ONCE) {
        let keys: Vec<_> = entries
            .iter()
            .map(|seq| layout::writer_log_entry(writer, *seq))
            .collect();
        try_join_all(keys.iter().map(|key| store.delete(key))).await?;
        collection.deleted += entries.len();
    }
    collection.log_gone = whole_log;
    Ok(collection)
}

/// Writer positions per partition and windows per dataset, read
/// lazily and cached for one pass.
struct Positions<'a> {
    store: Arc<dyn ObjectStore>,
    env: Env,
    datasets: &'a DatasetLogs,
    schedules: BTreeMap<(TenantId, DatasetName), WindowSchedule>,
    partitions: BTreeMap<(TenantId, DatasetName, Kind), Vec<tessera_types::PartitionId>>,
    by_partition: BTreeMap<tessera_types::PartitionId, BTreeMap<WriterId, Seq>>,
}

impl<'a> Positions<'a> {
    fn new(store: Arc<dyn ObjectStore>, env: Env, datasets: &'a DatasetLogs) -> Self {
        Positions {
            store,
            env,
            datasets,
            schedules: BTreeMap::new(),
            partitions: BTreeMap::new(),
            by_partition: BTreeMap::new(),
        }
    }

    /// Whether every partition the blocks' rows fall in has adopted
    /// `writer` past `seq`.
    async fn adopted(
        &mut self,
        blocks: &[BlockMeta],
        writer: WriterId,
        seq: Seq,
    ) -> Result<bool, CompactError> {
        for block in blocks {
            let key = (block.tenant.clone(), block.dataset.clone(), block.kind);
            if !self.partitions.contains_key(&key) {
                let listed =
                    list_partitions(&*self.store, &block.tenant, &block.dataset, block.kind)
                        .await?;
                self.partitions.insert(key.clone(), listed);
            }
            let range = TimeRange::new(
                block.min_timestamp,
                Timestamp::from_nanos(block.max_timestamp.as_nanos().saturating_add(1)),
            );
            // Rows in windows with no manifest yet are not adopted anywhere.
            let touched: Vec<_> = self.partitions[&key]
                .iter()
                .filter(|p| {
                    range.overlaps_closed(
                        p.window.start(),
                        Timestamp::from_nanos(p.window.end().as_nanos().saturating_sub(1)),
                    )
                })
                .cloned()
                .collect();
            let dataset = (block.tenant.clone(), block.dataset.clone());
            if !self.schedules.contains_key(&dataset) {
                let log = self.datasets.open(&block.tenant, &block.dataset);
                self.schedules.insert(dataset.clone(), log.window().await?);
            }
            if !covers_every_window(&self.schedules[&dataset], &touched, range) {
                return Ok(false);
            }
            for partition in touched {
                if !self.by_partition.contains_key(&partition) {
                    let manifest = Manifest::open(
                        Arc::clone(&self.store),
                        partition.clone(),
                        self.env.clone(),
                    );
                    let snapshot = manifest.snapshot().await?;
                    self.by_partition
                        .insert(partition.clone(), snapshot.state().positions().collect());
                }
                let position = self.by_partition[&partition]
                    .get(&writer)
                    .copied()
                    .unwrap_or(Seq::ZERO);
                if position < seq {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
}

/// Whether `partitions` include a manifest for every window of `range`
/// the block's rows could lie in under `schedule`. Blocks older than
/// the event-time bounds field read as the open range and fail this
/// test, which keeps them until their log is ended and fully adopted by
/// other means.
fn covers_every_window(
    schedule: &WindowSchedule,
    partitions: &[tessera_types::PartitionId],
    range: TimeRange,
) -> bool {
    if range.start == Timestamp::MIN || range.end == Timestamp::MAX {
        return false;
    }
    let mut at = range.start;
    loop {
        let Some(window) = schedule.containing(at) else {
            return false;
        };
        if !partitions.iter().any(|p| p.window == window) {
            return false;
        }
        if window.end() >= range.end {
            return true;
        }
        at = window.end();
    }
}
