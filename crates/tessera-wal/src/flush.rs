// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The L1 flush: rows of a range of segments become Parquet files, one
//! per partition (section 6.9). Ingest flushes its own buffers this way,
//! and recovery flushes what a dead writer left behind (ST-10).
//!
//! Files are written with random ids and create-if-absent, so a flush
//! that fails halfway leaves garbage but never a half-written file; the
//! flush record that names the files is appended by the caller only after
//! every file is stored.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{BooleanArray, RecordBatch, TimestampNanosecondArray};
use arrow::compute::filter_record_batch;
use tessera_format::FileFormat;
use tessera_object::{ObjectStore, layout};
use tessera_types::event::TIMESTAMP;
use tessera_types::{
    Env, EventBatch, FileId, FileMeta, FileSource, Level, PartitionId, PoolId, Seq, Timestamp,
    Window, WriterId,
};

use crate::WalError;

/// Where L1 files go and how they are encoded.
#[derive(Clone)]
pub struct FlushTarget {
    /// The store of the pool.
    pub store: Arc<dyn ObjectStore>,
    /// The pool the files are placed in.
    pub pool: PoolId,
    /// The file format.
    pub format: Arc<dyn FileFormat>,
}

impl std::fmt::Debug for FlushTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlushTarget")
            .field("pool", &self.pool)
            .finish_non_exhaustive()
    }
}

/// Writes the rows of `batches`, which are the rows of segments
/// `(after, through]` of `writer`, as one L1 file per partition, using
/// `window` as the partition window length.
///
/// # Errors
///
/// `Unwindowed` for an event time before the epoch; `Format` if a file
/// cannot be encoded; `Store` if a file cannot be stored.
pub async fn write_l1_files(
    target: &FlushTarget,
    env: &Env,
    writer: WriterId,
    after: Seq,
    through: Seq,
    window: Duration,
    batches: Vec<EventBatch>,
) -> Result<Vec<FileMeta>, WalError> {
    let mut files = Vec::new();
    for (partition, rows) in split_by_partition(batches, window)? {
        let format = Arc::clone(&target.format);
        let written = tokio::task::spawn_blocking(move || format.write(&rows))
            .await
            .map_err(|e| WalError::Flush(format!("file encoding task failed: {e}")))??;
        let file_id = FileId::random(&mut env.random.clone());
        target
            .store
            .put_if_absent(
                &layout::data_file(&partition, file_id),
                written.bytes.clone(),
            )
            .await?;
        files.push(FileMeta {
            file_id,
            partition,
            pool: target.pool.clone(),
            level: Level::L1,
            source: FileSource::Writer {
                writer,
                after,
                through,
            },
            min_timestamp: written.min_timestamp,
            max_timestamp: written.max_timestamp,
            row_count: written.row_count,
            size_bytes: written.bytes.len() as u64,
            layout_epoch: 0,
            sort_key: written.sort_key,
            columns: written.columns,
        });
    }
    Ok(files)
}

/// Groups rows by the partition they belong to.
fn split_by_partition(
    batches: Vec<EventBatch>,
    window: Duration,
) -> Result<BTreeMap<PartitionId, Vec<RecordBatch>>, WalError> {
    let mut groups: BTreeMap<PartitionId, Vec<RecordBatch>> = BTreeMap::new();
    for batch in batches {
        let timestamps = batch
            .rows()
            .column_by_name(TIMESTAMP)
            .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
            .ok_or_else(|| WalError::Flush("batch has no timestamp column".into()))?;
        let mut windows: Vec<Window> = Vec::new();
        for value in timestamps.values() {
            let at = Timestamp::from_nanos(*value);
            let found =
                Window::containing(at, window).ok_or(WalError::Unwindowed { timestamp: at })?;
            if !windows.contains(&found) {
                windows.push(found);
            }
        }
        for found in windows {
            let rows = if windows_of(timestamps, window).all(|w| w == Some(found)) {
                batch.rows().clone()
            } else {
                let mask: BooleanArray = windows_of(timestamps, window)
                    .map(|w| Some(w == Some(found)))
                    .collect();
                filter_record_batch(batch.rows(), &mask)?
            };
            let partition = PartitionId {
                tenant: batch.tenant().clone(),
                dataset: batch.dataset().clone(),
                kind: batch.kind(),
                window: found,
            };
            groups.entry(partition).or_default().push(rows);
        }
    }
    Ok(groups)
}

fn windows_of(
    timestamps: &TimestampNanosecondArray,
    window: Duration,
) -> impl Iterator<Item = Option<Window>> {
    timestamps
        .values()
        .iter()
        .map(move |v| Window::containing(Timestamp::from_nanos(*v), window))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringArray};
    use tessera_format::ParquetFormat;
    use tessera_object::MemoryStore;
    use tessera_types::event::BODY;
    use tessera_types::{DatasetName, LogRow, TenantId};

    use super::*;

    const FIVE_MINUTES: Duration = Duration::from_secs(300);

    fn target(store: &MemoryStore) -> FlushTarget {
        FlushTarget {
            store: Arc::new(store.clone()),
            pool: PoolId::new("root").unwrap(),
            format: Arc::new(ParquetFormat),
        }
    }

    fn log(seconds: i64, body: &str) -> LogRow {
        LogRow {
            timestamp: Timestamp::from_nanos(seconds * 1_000_000_000),
            observed_timestamp: Timestamp::from_nanos(seconds * 1_000_000_000),
            body: Some(body.to_owned()),
            ..LogRow::default()
        }
    }

    fn batch(dataset: &str, rows: &[LogRow]) -> EventBatch {
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new(dataset).unwrap(), rows).unwrap()
    }

    #[tokio::test]
    async fn writes_one_file_per_partition_and_tags_the_segment_range() {
        let store = MemoryStore::new();
        let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
        let batches = vec![
            batch("prod", &[log(10, "a"), log(400, "c"), log(20, "b")]), // two windows
            batch("prod", &[log(30, "d")]),
            batch("staging", &[log(40, "e")]),
        ];
        let writer = WriterId::from_u128(5);
        let files = write_l1_files(
            &target(&store),
            &env,
            writer,
            Seq::new(2),
            Seq::new(4),
            FIVE_MINUTES,
            batches,
        )
        .await
        .unwrap();
        assert_eq!(files.len(), 3);
        let prod_first = files
            .iter()
            .find(|f| {
                f.partition.dataset.as_str() == "prod"
                    && f.partition.window.start() == Timestamp::UNIX_EPOCH
            })
            .unwrap();
        assert_eq!(prod_first.row_count, 3);
        assert_eq!(prod_first.level, Level::L1);
        assert_eq!(
            prod_first.source,
            FileSource::Writer {
                writer,
                after: Seq::new(2),
                through: Seq::new(4)
            }
        );
        assert_eq!(
            prod_first.min_timestamp,
            Timestamp::from_nanos(10_000_000_000)
        );
        let prod_second = files
            .iter()
            .find(|f| {
                f.partition.dataset.as_str() == "prod"
                    && f.partition.window.start() != Timestamp::UNIX_EPOCH
            })
            .unwrap();
        assert_eq!(prod_second.row_count, 1);
        let bytes = store
            .get(&layout::data_file(
                &prod_first.partition,
                prod_first.file_id,
            ))
            .await
            .unwrap();
        assert_eq!(bytes.len() as u64, prod_first.size_bytes);
        let rows = ParquetFormat.read(bytes).unwrap();
        let bodies: Vec<String> = rows
            .iter()
            .flat_map(|r| {
                r.column_by_name(BODY)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .flatten()
                    .map(String::from)
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(bodies, ["a", "b", "d"], "sorted by time within the file");
    }

    #[tokio::test]
    async fn rows_before_the_epoch_are_rejected() {
        let store = MemoryStore::new();
        let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
        let batches = vec![batch("prod", &[log(-1, "x")])];
        let error = write_l1_files(
            &target(&store),
            &env,
            WriterId::from_u128(1),
            Seq::ZERO,
            Seq::new(1),
            FIVE_MINUTES,
            batches,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, WalError::Unwindowed { .. }), "{error}");
        assert!(
            store
                .list(&tessera_object::Key::new("default").unwrap())
                .try_next_is_none()
                .await
        );
    }

    #[tokio::test]
    async fn nothing_to_flush_writes_nothing() {
        let store = MemoryStore::new();
        let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
        let files = write_l1_files(
            &target(&store),
            &env,
            WriterId::from_u128(1),
            Seq::ZERO,
            Seq::ZERO,
            FIVE_MINUTES,
            Vec::new(),
        )
        .await
        .unwrap();
        assert!(files.is_empty());
    }

    trait ListingExt {
        async fn try_next_is_none(self) -> bool;
    }

    impl ListingExt
        for futures::stream::BoxStream<
            '_,
            Result<tessera_object::ObjectMeta, tessera_object::ObjectError>,
        >
    {
        async fn try_next_is_none(mut self) -> bool {
            use futures::TryStreamExt;
            self.try_next().await.unwrap().is_none()
        }
    }
}
