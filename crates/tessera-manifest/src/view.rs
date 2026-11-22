// SPDX-License-Identifier: Apache-2.0

//! The consistent view (spec section 6.8): the rows of a dataset and kind
//! at one moment, with no gaps and no duplicates (VW-1).
//!
//! ```text
//! rows(P, V) =  rows in the files listed in manifest(P) at V
//!             ⊎ for every writer w in registry(D):
//!                  rows of P in w's segments with seq > position(P, V, w)
//! ```
//!
//! The registry is read before any manifest (VW-2). Tail rows are read
//! from the segments in the object store, the source that is always
//! available (VW-3); the ingester's memory and the L1 files named in flush
//! records are optimizations for later.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch, TimestampNanosecondArray};
use arrow::compute::filter_record_batch;
use futures::{StreamExt, TryStreamExt};
use tessera_format::FileFormat;
use tessera_object::{Pools, layout};
use tessera_types::event::TIMESTAMP;
use tessera_types::{
    DatasetName, Env, FileMeta, Kind, PartitionId, Seq, TenantId, TimeRange, Timestamp, Version,
    Window, WindowSchedule, WriterId,
};
use tessera_wal::{BlockMeta, EntryBody, reader};

use crate::{ManifestError, Manifests, Partial, Registration, list_windows, registry};

/// How many of a dataset's manifests a query reads at once.
const MANIFESTS_AT_ONCE: usize = 16;

/// What a view read, for `EXPLAIN` and for tests.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ViewSources {
    /// Manifest versions the view was taken at.
    pub manifests: BTreeMap<PartitionId, Version>,
    /// Data files read.
    pub files: usize,
    /// Segments read for tail rows.
    pub segments: usize,
    /// The last entry read of each writer's log: where a consumer that
    /// folded this view resumes from the live logs (section 11.7).
    pub positions: BTreeMap<WriterId, Seq>,
    /// Row groups of the manifest checkpoints consulted (MF-5).
    pub manifest_row_groups: usize,
    /// Row groups of them fetched: the ones whose files could overlap
    /// the range.
    pub manifest_row_groups_read: usize,
}

/// The rows of a view and where they came from.
#[derive(Debug)]
pub struct View {
    /// Rows from data files followed by tail rows, as batches.
    pub rows: Vec<RecordBatch>,
    /// What was read.
    pub sources: ViewSources,
}

impl View {
    /// The total number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.iter().map(RecordBatch::num_rows).sum()
    }

    /// Whether there are no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The sources of a view: the files of the partitions considered and the
/// tail rows not yet adopted by any of them, taken at one moment (VW-2).
#[derive(Debug)]
pub struct Sources {
    /// Every partition of the dataset and kind, with its window.
    pub partitions: Vec<PartitionId>,
    /// The files of the partitions within `time_range`.
    pub files: Vec<FileMeta>,
    /// Tail rows from segments beyond every partition's position.
    pub tail: Vec<RecordBatch>,
    /// What was read.
    pub sources: ViewSources,
}

/// How many writers' logs are read at once for the tail.
const TAILS_AT_ONCE: usize = 16;

/// What one writer's log gave of the tail.
#[derive(Default)]
struct Tail {
    /// The last entry read.
    last: Option<Seq>,
    /// Segments read.
    segments: usize,
    /// The rows beyond the partitions' positions.
    rows: Vec<RecordBatch>,
}

/// What the tails of a dataset's writers are read with.
struct Reading<'a> {
    manifests: &'a Manifests,
    /// Which blocks of a segment are the dataset's. A segment holds
    /// the rows of every dataset its writer took: these are read, and
    /// the rest left in the store.
    of_the_dataset: Arc<dyn Fn(&BlockMeta) -> bool + Send + Sync>,
    /// Where each writer is adopted to in each partition.
    positions: &'a BTreeMap<Window, BTreeMap<WriterId, Seq>>,
    window: &'a WindowSchedule,
}

impl Reading<'_> {
    /// The rows of the dataset in the log of the writer of
    /// `registration` after what it says is adopted, those beyond the
    /// positions of their partitions.
    async fn tail_of(
        &self,
        registration: &Registration,
    ) -> Result<(WriterId, Tail), ManifestError> {
        let writer = registration.writer;
        let mut read = Tail::default();
        let mut entries = reader::blocks_after(
            Arc::clone(self.manifests.store()),
            writer,
            registration.adopted,
            Arc::clone(&self.manifests.env().keys),
            Arc::clone(&self.of_the_dataset),
        );
        while let Some(entry) = entries.try_next().await? {
            let seq = entry.header.seq;
            read.last = Some(seq);
            if !matches!(entry.header.body, EntryBody::Segment { .. }) {
                continue;
            }
            read.segments += 1;
            for batch in entry.rows {
                let position = |w: Window| {
                    self.positions
                        .get(&w)
                        .and_then(|p| p.get(&writer))
                        .copied()
                        .unwrap_or(Seq::ZERO)
                };
                if let Some(rows) = beyond_position(batch.rows(), seq, self.window, position)? {
                    read.rows.push(rows);
                }
            }
        }
        Ok((writer, read))
    }
}

/// Lists the files and reads the tail rows of one dataset and kind for
/// the partitions whose window overlaps `time_range` (pruning stage 1).
/// `window` is the dataset's window schedule (MF-2), used to place
/// tail rows in partitions.
///
/// # Errors
///
/// If the registry, a manifest or a segment cannot be read.
pub async fn sources(
    manifests: &Manifests,
    tenant: &TenantId,
    dataset: &DatasetName,
    kind: Kind,
    window: impl Into<WindowSchedule>,
    time_range: TimeRange,
) -> Result<Sources, ManifestError> {
    let window = window.into();
    let root = manifests.store();
    let writers = registry::list(&**root, tenant, dataset).await?;
    let mut sources = ViewSources::default();
    // The windows that hold anything; which of them are partitions
    // their manifests tell below.
    let windows = list_windows(&**root, tenant, dataset, kind).await?;
    let mut files = Vec::new();
    let mut positions: BTreeMap<Window, BTreeMap<WriterId, Seq>> = BTreeMap::new();
    // Every partition's manifest is needed, for its positions if not
    // for its files, and each read is a request or a few to the store:
    // they are made together, not one after another.
    let mut wanted = Vec::with_capacity(windows.len());
    for partition in &windows {
        let w = partition.window;
        let in_range = time_range.overlaps_closed(
            w.start(),
            Timestamp::from_nanos(w.end().as_nanos().saturating_sub(1)),
        );
        // A partition outside the range contributes its positions only:
        // an empty range reads the checkpoint's header rows and no file
        // row group (MF-5).
        let range = if in_range {
            time_range
        } else {
            TimeRange::new(Timestamp::MAX, Timestamp::MAX)
        };
        wanted.push((in_range, range, manifests.open(partition)));
    }
    let reads = wanted
        .into_iter()
        .map(|(in_range, range, manifest)| async move {
            Ok::<_, ManifestError>((in_range, manifest.read_in(range).await?))
        });
    let read: Vec<(bool, Partial)> = futures::stream::iter(reads)
        .buffered(MANIFESTS_AT_ONCE)
        .try_collect()
        .await?;
    let mut partitions = Vec::with_capacity(windows.len());
    for (partition, (in_range, partial)) in windows.into_iter().zip(read) {
        // Nothing was ever committed to it: a window whose files no
        // manifest has adopted yet, not a partition.
        if partial.version == Version::ZERO {
            continue;
        }
        sources.manifests.insert(partition.clone(), partial.version);
        if let Some(read) = partial.read {
            sources.manifest_row_groups += read.row_groups;
            sources.manifest_row_groups_read += read.row_groups_read;
        }
        positions.insert(partition.window, partial.positions);
        if in_range {
            files.extend(partial.files.into_values());
        }
        partitions.push(partition);
    }
    sources.files = files.len();

    // What the writer flushed through the position its registration
    // gives is in the manifests just read, whichever partitions it went
    // to: a tail starts after it, not at the log's first entry, which
    // is an hour of segments back (GC-2). The registration was read
    // before the manifests, so they hold at least what it says. The
    // logs do not depend on each other and are read together.
    let reading = Reading {
        manifests,
        of_the_dataset: {
            let (tenant, dataset) = (tenant.clone(), dataset.clone());
            Arc::new(move |block: &BlockMeta| {
                block.tenant == tenant && block.dataset == dataset && block.kind == kind
            })
        },
        positions: &positions,
        window: &window,
    };
    let reading = &reading;
    let tails = writers
        .into_iter()
        .map(|registration| async move { reading.tail_of(&registration).await });
    let read: Vec<(WriterId, Tail)> = futures::stream::iter(tails)
        .buffered(TAILS_AT_ONCE)
        .try_collect()
        .await?;
    let mut tail = Vec::new();
    for (writer, read) in read {
        if let Some(last) = read.last {
            sources.positions.insert(writer, last);
        }
        sources.segments += read.segments;
        tail.extend(read.rows);
    }
    Ok(Sources {
        partitions,
        files,
        tail,
        sources,
    })
}

/// Reads the rows of one dataset and kind. `window` is the dataset's
/// window schedule (MF-2), used to place tail rows in partitions.
///
/// # Errors
///
/// If the registry, a manifest, a file or a segment cannot be read.
pub async fn read(
    pools: &Pools,
    format: &Arc<dyn FileFormat>,
    env: &Env,
    tenant: &TenantId,
    dataset: &DatasetName,
    kind: Kind,
    window: impl Into<WindowSchedule>,
) -> Result<View, ManifestError> {
    let manifests = Manifests::new(Arc::clone(pools.root().store()), env.clone(), 1);
    let found = sources(&manifests, tenant, dataset, kind, window, TimeRange::ALL).await?;
    read_sources(pools, format, found).await
}

/// The rows of `found`: its files read with `format`, then its tail.
///
/// # Errors
///
/// If a file cannot be read.
pub async fn read_sources(
    pools: &Pools,
    format: &Arc<dyn FileFormat>,
    found: Sources,
) -> Result<View, ManifestError> {
    let mut rows = Vec::new();
    for file in &found.files {
        rows.extend(read_file(pools, format, file).await?);
    }
    rows.extend(found.tail);
    Ok(View {
        rows,
        sources: found.sources,
    })
}

async fn read_file(
    pools: &Pools,
    format: &Arc<dyn FileFormat>,
    file: &FileMeta,
) -> Result<Vec<RecordBatch>, ManifestError> {
    let store = pools.get(&file.pool).map_err(|e| ManifestError::Corrupt {
        key: layout::data_file(&file.partition, file.file_id),
        source: Box::new(e),
    })?;
    let bytes = store
        .store()
        .get(&layout::data_file(&file.partition, file.file_id))
        .await?;
    let format = Arc::clone(format);
    let key = layout::data_file(&file.partition, file.file_id);
    tokio::task::spawn_blocking(move || format.read(bytes))
        .await
        .map_err(|e| ManifestError::Corrupt {
            key: key.clone(),
            source: Box::new(e),
        })?
        .map_err(|e| ManifestError::Corrupt {
            key,
            source: Box::new(e),
        })
}

/// The rows of a segment block whose partition has not adopted the
/// segment yet, or `None` if there are none.
fn beyond_position(
    rows: &RecordBatch,
    seq: Seq,
    window: &WindowSchedule,
    position: impl Fn(Window) -> Seq,
) -> Result<Option<RecordBatch>, ManifestError> {
    let timestamps = rows
        .column_by_name(TIMESTAMP)
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
        .ok_or_else(|| ManifestError::Corrupt {
            key: layout::catalog(),
            source: "segment block has no timestamp column".into(),
        })?;
    let keep: BooleanArray = timestamps
        .values()
        .iter()
        .map(|v| {
            let adopted = window
                .containing(Timestamp::from_nanos(*v))
                .is_some_and(|w| position(w) >= seq);
            Some(!adopted)
        })
        .collect();
    if keep.true_count() == 0 {
        return Ok(None);
    }
    if keep.true_count() == rows.num_rows() {
        return Ok(Some(rows.clone()));
    }
    filter_record_batch(rows, &keep)
        .map(Some)
        .map_err(|e| ManifestError::Corrupt {
            key: layout::catalog(),
            source: Box::new(e),
        })
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, FixedSizeBinaryArray};
    use std::time::Duration;
    use tessera_format::ParquetFormat;
    use tessera_object::MemoryStore;
    use tessera_types::event::EVENT_ID;
    use tessera_types::{EventBatch, LogRow, PoolId};
    use tessera_wal::{FlushTarget, WriterLog, write_l1_files};

    use super::*;
    use crate::busy::Busy;
    use crate::intake;

    const WINDOW: Duration = Duration::from_secs(86_400);

    fn env() -> Env {
        Env::simulated(1, Timestamp::UNIX_EPOCH)
    }

    fn dataset() -> DatasetName {
        DatasetName::new("prod").unwrap()
    }

    fn pools(store: &MemoryStore) -> Pools {
        Pools::new(PoolId::new("root").unwrap(), Arc::new(store.clone()))
    }

    fn format() -> Arc<dyn FileFormat> {
        Arc::new(ParquetFormat::default())
    }

    /// Rows with distinct ids `from..from+count` on `day`.
    fn batch(day: i64, from: u8, count: u8) -> EventBatch {
        let rows: Vec<LogRow> = (from..from + count)
            .map(|i| LogRow {
                timestamp: Timestamp::from_nanos(day * 86_400_000_000_000 + i64::from(i)),
                event_id: [i; 16],
                body: Some("x".into()),
                ..LogRow::default()
            })
            .collect();
        EventBatch::from_logs(TenantId::DEFAULT, dataset(), &rows).unwrap()
    }

    fn ids(view: &View) -> Vec<u8> {
        let mut ids: Vec<u8> = view
            .rows
            .iter()
            .flat_map(|r| {
                let column = r.column_by_name(EVENT_ID).unwrap();
                let column = column
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                (0..column.len())
                    .map(|i| column.value(i)[0])
                    .collect::<Vec<_>>()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    async fn read_view(store: &MemoryStore) -> View {
        read(
            &pools(store),
            &format(),
            &env(),
            &TenantId::DEFAULT,
            &dataset(),
            Kind::Log,
            WINDOW,
        )
        .await
        .unwrap()
    }

    /// Flushes `batches` as segments `(after, through]` and appends the
    /// flush record.
    async fn flush(
        store: &MemoryStore,
        env: &Env,
        log: &mut WriterLog,
        after: u64,
        through: u64,
        batches: Vec<EventBatch>,
    ) {
        let target = FlushTarget {
            store: Arc::new(store.clone()),
            pool: PoolId::new("root").unwrap(),
            format: format(),
        };
        let (after, through) = (Seq::new(after), Seq::new(through));
        let files = write_l1_files(
            &target,
            env,
            log.writer_id(),
            after,
            through,
            WINDOW,
            batches,
        )
        .await
        .unwrap();
        log.append(EntryBody::Flush {
            tenant: TenantId::DEFAULT,
            dataset: dataset(),
            after,
            through,
            files,
        })
        .await
        .unwrap();
    }

    async fn adopt(store: &MemoryStore, log: &WriterLog) {
        intake::run(
            Arc::new(store.clone()),
            &env(),
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn rows_appear_exactly_once_before_during_and_after_adoption() {
        let store = MemoryStore::new();
        let env = env();
        let mut log = WriterLog::start(Arc::new(store.clone()), env.clone());
        registry::register(
            &store,
            &env,
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        log.append_segment(&[batch(0, 0, 3), batch(1, 10, 2)])
            .await
            .unwrap(); // seq 1: two windows
        log.append_segment(&[batch(0, 20, 2)]).await.unwrap(); // seq 2
        let expected: Vec<u8> = vec![0, 1, 2, 10, 11, 20, 21];

        let tail_only = read_view(&store).await;
        assert_eq!(ids(&tail_only), expected, "everything comes from the tail");
        assert_eq!(
            (tail_only.sources.files, tail_only.sources.segments),
            (0, 2)
        );

        flush(
            &store,
            &env,
            &mut log,
            0,
            1,
            vec![batch(0, 0, 3), batch(1, 10, 2)],
        )
        .await;
        adopt(&store, &log).await;
        let mixed = read_view(&store).await;
        assert_eq!(
            ids(&mixed),
            expected,
            "files plus the unadopted segment, no duplicates"
        );
        assert_eq!((mixed.sources.files, mixed.sources.manifests.len()), (2, 2));
        assert_eq!(
            mixed.sources.segments, 1,
            "the segment that was adopted is not read again"
        );

        flush(&store, &env, &mut log, 1, 2, vec![batch(0, 20, 2)]).await;
        log.end().await.unwrap();
        adopt(&store, &log).await;
        let files_only = read_view(&store).await;
        assert_eq!(ids(&files_only), expected);
        assert_eq!(
            (files_only.sources.files, files_only.sources.segments),
            (3, 0),
            "no registered writer, no segment read"
        );
    }

    /// Section 11.7: a consumer that folded a view resumes the live
    /// logs after the last entry the view read, flush records included.
    #[tokio::test]
    async fn the_view_reports_the_last_entry_read_of_every_log() {
        let store = MemoryStore::new();
        let env = env();
        let mut log = WriterLog::start(Arc::new(store.clone()), env.clone());
        registry::register(
            &store,
            &env,
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        log.append_segment(&[batch(0, 0, 3)]).await.unwrap(); // seq 1
        flush(&store, &env, &mut log, 0, 1, vec![batch(0, 0, 3)]).await; // seq 2
        adopt(&store, &log).await;
        log.append_segment(&[batch(0, 10, 2)]).await.unwrap(); // seq 3

        let view = read_view(&store).await;
        assert_eq!(ids(&view), vec![0, 1, 2, 10, 11]);
        assert_eq!(
            view.sources.positions,
            BTreeMap::from([(log.writer_id(), Seq::new(3))])
        );
    }

    #[tokio::test]
    async fn a_partition_without_a_manifest_is_served_from_the_tail() {
        let store = MemoryStore::new();
        let mut log = WriterLog::start(Arc::new(store.clone()), env());
        registry::register(
            &store,
            &env(),
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        log.append_segment(&[batch(5, 1, 2)]).await.unwrap();
        let view = read_view(&store).await;
        assert_eq!(ids(&view), [1, 2]);
        assert!(view.sources.manifests.is_empty());
    }

    #[tokio::test]
    async fn other_kinds_and_datasets_are_not_read() {
        let store = MemoryStore::new();
        let mut log = WriterLog::start(Arc::new(store.clone()), env());
        registry::register(
            &store,
            &env(),
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        log.append_segment(&[batch(0, 1, 1)]).await.unwrap();
        let spans = read(
            &pools(&store),
            &format(),
            &env(),
            &TenantId::DEFAULT,
            &dataset(),
            Kind::Span,
            WINDOW,
        )
        .await
        .unwrap();
        assert!(spans.is_empty());
        let other = read(
            &pools(&store),
            &format(),
            &env(),
            &TenantId::DEFAULT,
            &DatasetName::new("staging").unwrap(),
            Kind::Log,
            WINDOW,
        )
        .await
        .unwrap();
        assert!(other.is_empty());
        assert_eq!(
            other.sources.segments, 0,
            "no writer is registered for that dataset"
        );
    }

    /// A writer's segment holds the rows of every dataset it took. A
    /// read of one dataset asks for the segment's header and for the
    /// block of that dataset, not for the segment.
    #[tokio::test]
    async fn the_tail_is_read_for_the_blocks_of_its_dataset() {
        let inner = MemoryStore::new();
        let mut log = WriterLog::start(Arc::new(inner.clone()), env());
        registry::register(
            &inner,
            &env(),
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        // Lines of another dataset that no compression makes short.
        let mut random = tessera_types::SeededRandom::new(3);
        let others: Vec<LogRow> = (0..4_000)
            .map(|_| LogRow {
                body: Some(format!(
                    "{:032x}{:032x}",
                    tessera_types::Random::next_u128(&mut random),
                    tessera_types::Random::next_u128(&mut random)
                )),
                ..LogRow::default()
            })
            .collect();
        let staging = DatasetName::new("staging").unwrap();
        let other = EventBatch::from_logs(TenantId::DEFAULT, staging, &others).unwrap();
        log.append_segment(&[batch(0, 1, 2), other]).await.unwrap();

        let metered = Arc::new(tessera_object::MeteredStore::new(Arc::new(inner)));
        let manifests = Manifests::new(metered.clone(), env(), 8);
        let found = sources(
            &manifests,
            &TenantId::DEFAULT,
            &dataset(),
            Kind::Log,
            WINDOW,
            TimeRange::ALL,
        )
        .await
        .unwrap();
        assert_eq!(
            found.tail.iter().map(RecordBatch::num_rows).sum::<usize>(),
            2
        );
        let made = metered.requests().by_operation();
        let of = |operation: &str| {
            made.iter()
                .find(|(name, _)| *name == operation)
                .map(|(_, made)| *made)
        };
        assert_eq!(of("get_tail"), Some(2), "the header, and the log's end");
        assert_eq!(of("get_range"), Some(1), "the dataset's block");
    }

    #[tokio::test]
    async fn an_unregistered_writer_is_invisible() {
        let store = MemoryStore::new();
        let mut log = WriterLog::start(Arc::new(store.clone()), env());
        log.append_segment(&[batch(0, 1, 1)]).await.unwrap();
        assert!(read_view(&store).await.is_empty());
    }

    /// A query needs the manifest of every partition of its dataset,
    /// for the positions if not for the files. Over a bucket each is a
    /// request or a few: one after another they are the query's
    /// latency.
    #[tokio::test]
    async fn the_manifests_of_a_dataset_are_read_at_once() {
        let store = MemoryStore::new();
        let env = env();
        let mut log = WriterLog::start(Arc::new(store.clone()), env.clone());
        let writer = log.writer_id();
        registry::register(&store, &env, &TenantId::DEFAULT, &dataset(), writer)
            .await
            .unwrap();
        let days = vec![
            batch(0, 0, 1),
            batch(1, 10, 1),
            batch(2, 20, 1),
            batch(3, 30, 1),
        ];
        log.append_segment(&days).await.unwrap();
        flush(&store, &env, &mut log, 0, 1, days).await;
        adopt(&store, &log).await;

        let busy = Arc::new(Busy::over(store));
        let manifests = Manifests::new(busy.clone(), env, 8);
        let found = sources(
            &manifests,
            &TenantId::DEFAULT,
            &dataset(),
            Kind::Log,
            WINDOW,
            TimeRange::ALL,
        )
        .await
        .unwrap();
        assert_eq!(found.partitions.len(), 4);
        let most = busy.most();
        assert!(most > 1, "at most {most} read at a time");
    }

    /// A query finds its partitions on every run. Which windows hold
    /// anything is one listing; whether each has a manifest is told by
    /// the manifest, which is read anyway, and not by a listing of
    /// every window besides.
    #[tokio::test]
    async fn partitions_are_told_from_their_manifests_not_listed_one_by_one() {
        let store = MemoryStore::new();
        let env = env();
        let mut log = WriterLog::start(Arc::new(store.clone()), env.clone());
        registry::register(
            &store,
            &env,
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        let days = vec![batch(0, 0, 1), batch(1, 10, 1), batch(2, 20, 1)];
        log.append_segment(&days).await.unwrap();
        flush(&store, &env, &mut log, 0, 1, days).await;
        adopt(&store, &log).await;
        // A window with a file and no manifest: a flush not adopted yet.
        let unadopted = PartitionId {
            tenant: TenantId::DEFAULT,
            dataset: dataset(),
            kind: Kind::Log,
            window: Window::containing(Timestamp::from_nanos(9 * 86_400_000_000_000), WINDOW)
                .unwrap(),
        };
        let stray = layout::data_file(&unadopted, tessera_types::FileId::from_u128(9));
        tessera_object::ObjectStore::put(&store, &stray, "rows".into())
            .await
            .unwrap();

        let metered = Arc::new(tessera_object::MeteredStore::new(Arc::new(store)));
        let manifests = Manifests::new(metered.clone(), env, 8);
        let find = async || {
            sources(
                &manifests,
                &TenantId::DEFAULT,
                &dataset(),
                Kind::Log,
                WINDOW,
                TimeRange::ALL,
            )
            .await
            .unwrap()
        };
        assert_eq!(find().await.partitions.len(), 3, "the stray window is none");
        let before = metered.requests().by_operation();
        let again = find().await;
        assert_eq!((again.partitions.len(), again.files.len()), (3, 3));
        let listed: u64 = metered
            .requests()
            .by_operation()
            .into_iter()
            .zip(before)
            .filter(|((operation, _), _)| *operation == "list_names")
            .map(|((_, now), (_, then))| now - then)
            .sum();
        assert_eq!(listed, 1, "the windows of the kind, once");
    }

    /// Rows not yet in files are read from the logs of every writer of
    /// the dataset. One log after another is a round trip per entry
    /// per writer; the logs do not depend on each other.
    #[tokio::test]
    async fn the_tails_of_several_writers_are_read_together() {
        let store = MemoryStore::new();
        let env = env();
        for node in 1..=3u8 {
            let mut log = WriterLog::start(
                Arc::new(store.clone()),
                Env::simulated(u64::from(node), Timestamp::UNIX_EPOCH),
            );
            registry::register(
                &store,
                &env,
                &TenantId::DEFAULT,
                &dataset(),
                log.writer_id(),
            )
            .await
            .unwrap();
            log.append_segment(&[batch(0, node * 10, 2)]).await.unwrap();
        }
        let busy = Arc::new(Busy::watching(store, "_wal/"));
        let manifests = Manifests::new(busy.clone(), env, 8);
        let found = sources(
            &manifests,
            &TenantId::DEFAULT,
            &dataset(),
            Kind::Log,
            WINDOW,
            TimeRange::ALL,
        )
        .await
        .unwrap();
        assert_eq!(found.sources.segments, 3);
        assert_eq!(
            found.tail.iter().map(RecordBatch::num_rows).sum::<usize>(),
            6
        );
        assert!(busy.most() > 1, "at most {} read at a time", busy.most());
    }

    #[tokio::test]
    async fn sources_list_only_the_partitions_in_range() {
        let store = MemoryStore::new();
        let env = env();
        let mut log = WriterLog::start(Arc::new(store.clone()), env.clone());
        registry::register(
            &store,
            &env,
            &TenantId::DEFAULT,
            &dataset(),
            log.writer_id(),
        )
        .await
        .unwrap();
        log.append_segment(&[batch(0, 0, 3), batch(1, 10, 2)])
            .await
            .unwrap();
        flush(
            &store,
            &env,
            &mut log,
            0,
            1,
            vec![batch(0, 0, 3), batch(1, 10, 2)],
        )
        .await;
        adopt(&store, &log).await;
        log.append_segment(&[batch(1, 30, 1)]).await.unwrap();

        let manifests = Manifests::new(Arc::new(store.clone()), env.clone(), 8);
        let day = i64::try_from(WINDOW.as_nanos()).unwrap();
        let second_day = TimeRange::new(Timestamp::from_nanos(day), Timestamp::from_nanos(2 * day));
        let found = sources(
            &manifests,
            &TenantId::DEFAULT,
            &dataset(),
            Kind::Log,
            WINDOW,
            second_day,
        )
        .await
        .unwrap();
        assert_eq!(found.partitions.len(), 2);
        assert_eq!(found.files.len(), 1, "only the second day's file");
        assert_eq!(
            found.files[0].min_timestamp,
            Timestamp::from_nanos(day + 10)
        );
        assert_eq!(found.tail.len(), 1, "the unadopted segment is the tail");
        assert_eq!(found.sources.manifests.len(), 2);
    }
}
