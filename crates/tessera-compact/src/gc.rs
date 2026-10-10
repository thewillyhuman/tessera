// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Garbage collection (section 6.10).
//!
//! Files: a tombstone older than the snapshot-retention window means no
//! snapshot a reader could still hold references the file (GC-1); its
//! data file and sidecar are deleted, then the tombstone is dropped.
//!
//! Segments: a segment is deleted once every partition its rows touched
//! has adopted past it and a reader grace period has elapsed since it
//! was written (GC-2). Entries are collected in order from the oldest;
//! the first segment that must stay stops the pass, so a log never has a
//! hole a reader could fall into. A log whose every entry can go,
//! including its end marker, is removed whole.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tessera_manifest::{Manifest, list_partitions};
use tessera_object::{ObjectStore, Pools, layout};
use tessera_types::{
    DatasetName, Env, FileId, Kind, Seq, TenantId, TimeRange, Timestamp, WriterId,
};
use tessera_wal::{BlockMeta, EntryBody, reader};

use crate::CompactError;

/// How long a removed file's object is kept for readers of older
/// snapshots (GC-1).
pub const DEFAULT_FILE_RETENTION: Duration = Duration::from_secs(24 * 3_600);
/// How long an adopted segment is kept for readers of older views (GC-2).
pub const DEFAULT_SEGMENT_GRACE: Duration = Duration::from_secs(3_600);

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
/// has adopted and that are older than `grace`.
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
    window: Duration,
) -> Result<LogCollection, CompactError> {
    let mut positions = Positions::new(Arc::clone(store), env.clone(), window);
    let seqs = reader::list(&**store, writer).await?;
    let mut collection = LogCollection::default();
    let mut deletable: Vec<Seq> = Vec::new();
    let mut ended = false;
    for seq in &seqs {
        let entry = reader::read(&**store, writer, *seq).await?;
        let aged = now
            .duration_since(entry.header().written_at)
            .is_some_and(|age| age >= grace);
        match entry.body() {
            EntryBody::Segment { blocks } => {
                if !aged || !positions.adopted(blocks, writer, *seq).await? {
                    break;
                }
                deletable.push(*seq);
            }
            EntryBody::Flush { .. } | EntryBody::Seal { .. } => {
                if !aged {
                    break;
                }
                deletable.push(*seq);
            }
            EntryBody::End => {
                if !aged {
                    break;
                }
                ended = true;
                deletable.push(*seq);
            }
        }
    }
    // Keep the end marker until it is the last object: a log that still
    // has entries must still look ended.
    let whole_log = ended && deletable.len() == seqs.len();
    if !whole_log {
        deletable.retain(|seq| seqs.last() != Some(seq) || !ended);
    }
    for seq in &deletable {
        store
            .delete(&layout::writer_log_entry(writer, *seq))
            .await?;
        collection.deleted += 1;
    }
    collection.log_gone = whole_log;
    Ok(collection)
}

/// Writer positions per partition, read lazily and cached for one pass.
struct Positions {
    store: Arc<dyn ObjectStore>,
    env: Env,
    window: Duration,
    partitions: BTreeMap<(TenantId, DatasetName, Kind), Vec<tessera_types::PartitionId>>,
    by_partition: BTreeMap<tessera_types::PartitionId, BTreeMap<WriterId, Seq>>,
}

impl Positions {
    fn new(store: Arc<dyn ObjectStore>, env: Env, window: Duration) -> Self {
        Positions {
            store,
            env,
            window,
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
            if !self.covers_every_window(&touched, range) {
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

    /// Whether `partitions` include a manifest for every window of
    /// `range` the block's rows could lie in. Blocks older than the
    /// event-time bounds field read as the open range and fail this
    /// test, which keeps them until their log is ended and fully adopted
    /// by other means.
    fn covers_every_window(
        &self,
        partitions: &[tessera_types::PartitionId],
        range: TimeRange,
    ) -> bool {
        if range.start == Timestamp::MIN || range.end == Timestamp::MAX {
            return false;
        }
        let mut at = range.start;
        loop {
            let Some(window) = tessera_types::Window::containing(at, self.window) else {
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
}
