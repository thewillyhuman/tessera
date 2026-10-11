// SPDX-License-Identifier: LicenseRef-Tessera-Pending

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
use std::time::Duration;

use arrow::array::{BooleanArray, RecordBatch, TimestampNanosecondArray};
use arrow::compute::filter_record_batch;
use futures::TryStreamExt;
use tessera_format::FileFormat;
use tessera_object::{Pools, layout};
use tessera_types::event::TIMESTAMP;
use tessera_types::{
    DatasetName, Env, FileMeta, Kind, PartitionId, Seq, TenantId, TimeRange, Timestamp, Version,
    Window, WriterId,
};
use tessera_wal::{EntryBody, reader};

use crate::{ManifestError, Manifests, list_partitions, registry};

/// What a view read, for `EXPLAIN` and for tests.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ViewSources {
    /// Manifest versions the view was taken at.
    pub manifests: BTreeMap<PartitionId, Version>,
    /// Data files read.
    pub files: usize,
    /// Segments read for tail rows.
    pub segments: usize,
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

/// Lists the files and reads the tail rows of one dataset and kind for
/// the partitions whose window overlaps `time_range` (pruning stage 1).
/// `window` is the dataset's partition window length, used to place
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
    window: Duration,
    time_range: TimeRange,
) -> Result<Sources, ManifestError> {
    let root = manifests.store();
    let writers = registry::list(&**root, tenant, dataset).await?;
    let mut sources = ViewSources::default();
    let partitions = list_partitions(&**root, tenant, dataset, kind).await?;
    let mut files = Vec::new();
    let mut positions: BTreeMap<Window, BTreeMap<WriterId, Seq>> = BTreeMap::new();
    for partition in &partitions {
        let w = partition.window;
        let in_range = time_range.overlaps_closed(
            w.start(),
            Timestamp::from_nanos(w.end().as_nanos().saturating_sub(1)),
        );
        let snapshot = manifests.open(partition).snapshot().await?;
        sources
            .manifests
            .insert(partition.clone(), snapshot.version());
        positions.insert(w, snapshot.state().positions().collect());
        if in_range {
            files.extend(snapshot.state().files().cloned());
        }
    }
    sources.files = files.len();

    let mut tail = Vec::new();
    for registration in writers {
        let writer = registration.writer;
        let mut entries = reader::entries(Arc::clone(root), writer, Seq::ZERO);
        while let Some(entry) = entries.try_next().await? {
            if !matches!(entry.body(), EntryBody::Segment { .. }) {
                continue;
            }
            sources.segments += 1;
            for batch in entry.blocks_for(tenant, dataset, kind)? {
                let position = |w: Window| {
                    positions
                        .get(&w)
                        .and_then(|p| p.get(&writer))
                        .copied()
                        .unwrap_or(Seq::ZERO)
                };
                if let Some(rows) = beyond_position(batch.rows(), entry.seq(), window, position)? {
                    tail.push(rows);
                }
            }
        }
    }
    Ok(Sources {
        partitions,
        files,
        tail,
        sources,
    })
}

/// Reads the rows of one dataset and kind. `window` is the dataset's
/// partition window length, used to place tail rows in partitions.
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
    window: Duration,
) -> Result<View, ManifestError> {
    let manifests = Manifests::new(Arc::clone(pools.root().store()), env.clone(), 1);
    let found = sources(&manifests, tenant, dataset, kind, window, TimeRange::ALL).await?;
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
    window: Duration,
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
            let adopted = Window::containing(Timestamp::from_nanos(*v), window)
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
    use tessera_format::ParquetFormat;
    use tessera_object::MemoryStore;
    use tessera_types::event::EVENT_ID;
    use tessera_types::{EventBatch, LogRow, PoolId};
    use tessera_wal::{FlushTarget, WriterLog, write_l1_files};

    use super::*;
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

    #[tokio::test]
    async fn an_unregistered_writer_is_invisible() {
        let store = MemoryStore::new();
        let mut log = WriterLog::start(Arc::new(store.clone()), env());
        log.append_segment(&[batch(0, 1, 1)]).await.unwrap();
        assert!(read_view(&store).await.is_empty());
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
