// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The Parquet [`FileReader`]: footer by range request, pruning, range
//! planning with coalescing, and decoding over the fetched ranges.

use std::ops::Range;
use std::sync::Arc;

use arrow::error::ArrowError;
use bytes::Bytes;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
    RowFilter,
};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use tessera_object::{Key, ObjectStore};

use crate::FormatError;
use crate::footer::FooterCache;
use crate::prune::may_match;
use crate::scan::{Fetched, FileReader, ScanRequest, ScanStats};
use crate::sparse::SparseFile;

/// Bytes read from the end of a file in the first request, enough for
/// the footer of any file written by this crate.
const FOOTER_GUESS: u64 = 64 * 1024;
/// Two ranges closer than this are fetched as one.
const COALESCE_GAP: u64 = 64 * 1024;
/// Rows per decoded batch.
const BATCH_ROWS: usize = 8_192;
/// The Parquet footer: metadata length and magic.
const FOOTER_SIZE: u64 = 8;

/// A Parquet file in an object store.
#[derive(Debug)]
pub struct ParquetReader {
    store: Arc<dyn ObjectStore>,
    key: Key,
    size: u64,
    footers: Option<Arc<FooterCache>>,
}

impl ParquetReader {
    /// A reader of the `size`-byte object at `key`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, key: Key, size: u64) -> Self {
        ParquetReader {
            store,
            key,
            size,
            footers: None,
        }
    }

    /// The reader taking and keeping the footer in `footers`.
    #[must_use]
    pub fn with_footers(mut self, footers: Arc<FooterCache>) -> Self {
        self.footers = Some(footers);
        self
    }

    async fn get(&self, range: Range<u64>, stats: &mut ScanStats) -> Result<Bytes, FormatError> {
        let bytes = self
            .store
            .get_range(&self.key, range)
            .await
            .map_err(|e| FormatError::Unreadable(e.to_string()))?;
        stats.requests += 1;
        stats.bytes_read += bytes.len() as u64;
        Ok(bytes)
    }

    /// The footer, from the cache when it holds it.
    async fn metadata(&self, stats: &mut ScanStats) -> Result<Arc<ParquetMetaData>, FormatError> {
        if let Some(found) = self
            .footers
            .as_ref()
            .and_then(|c| c.get(&self.key, self.size))
        {
            stats.footer_cached = true;
            return Ok(found);
        }
        let footer = Arc::new(self.read_metadata(stats).await?);
        if let Some(footers) = &self.footers {
            footers.insert(&self.key, self.size, Arc::clone(&footer));
        }
        Ok(footer)
    }

    async fn read_metadata(&self, stats: &mut ScanStats) -> Result<ParquetMetaData, FormatError> {
        if self.size < FOOTER_SIZE {
            return Err(FormatError::Unreadable("file shorter than a footer".into()));
        }
        let tail_start = self.size.saturating_sub(FOOTER_GUESS);
        let tail = self.get(tail_start..self.size, stats).await?;
        let footer = &tail[tail.len() - 8..];
        if &footer[4..] != b"PAR1" {
            return Err(FormatError::Unreadable("missing parquet magic".into()));
        }
        let length = u64::from(u32::from_le_bytes([
            footer[0], footer[1], footer[2], footer[3],
        ]));
        let metadata_start = self
            .size
            .checked_sub(FOOTER_SIZE + length)
            .ok_or_else(|| FormatError::Unreadable("footer longer than the file".into()))?;
        let metadata = if metadata_start >= tail_start {
            let skip = usize::try_from(metadata_start - tail_start)
                .map_err(|e| FormatError::Unreadable(e.to_string()))?;
            tail.slice(skip..tail.len() - 8)
        } else {
            self.get(metadata_start..self.size - FOOTER_SIZE, stats)
                .await?
        };
        ParquetMetaDataReader::decode_metadata(&metadata)
            .map_err(|e| FormatError::Unreadable(e.to_string()))
    }
}

/// Merges sorted ranges that touch or lie within [`COALESCE_GAP`].
fn coalesce(mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
    ranges.sort_by_key(|r| r.start);
    let mut merged: Vec<Range<u64>> = Vec::new();
    for range in ranges {
        match merged.last_mut() {
            Some(last) if range.start <= last.end + COALESCE_GAP => {
                last.end = last.end.max(range.end);
            }
            _ => merged.push(range),
        }
    }
    merged
}

#[async_trait::async_trait]
impl FileReader for ParquetReader {
    async fn fetch(&self, request: ScanRequest) -> Result<Fetched, FormatError> {
        let mut stats = ScanStats::default();
        let metadata = self.metadata(&mut stats).await?;
        let arrow_metadata =
            ArrowReaderMetadata::try_new(Arc::clone(&metadata), ArrowReaderOptions::default())
                .map_err(|e| FormatError::Unreadable(e.to_string()))?;
        let schema = arrow_metadata.schema();
        let index_of = |name: &str| schema.index_of(name).ok();

        let projection: Vec<usize> = match &request.projection {
            None => (0..schema.fields().len()).collect(),
            Some(names) => names
                .iter()
                .map(|n| {
                    index_of(n)
                        .ok_or_else(|| FormatError::Unreadable(format!("file has no column {n}")))
                })
                .collect::<Result<_, _>>()?,
        };
        let filter_columns: Vec<usize> = request
            .filter
            .as_ref()
            .map(|f| f.columns.iter().filter_map(|n| index_of(n)).collect())
            .unwrap_or_default();

        stats.row_groups = metadata.num_row_groups();
        let kept: Vec<usize> = (0..metadata.num_row_groups())
            .filter(|i| {
                request
                    .row_groups
                    .as_ref()
                    .is_none_or(|rgs| rgs.contains(i))
            })
            .filter(|i| may_match(metadata.row_group(*i), &request.bounds, index_of))
            .collect();
        stats.row_groups_read = kept.len();
        stats.rows_scanned = kept
            .iter()
            .map(|i| u64::try_from(metadata.row_group(*i).num_rows()).unwrap_or(0))
            .sum();

        let mut needed: Vec<usize> = projection.iter().chain(&filter_columns).copied().collect();
        needed.sort_unstable();
        needed.dedup();
        let ranges: Vec<Range<u64>> = kept
            .iter()
            .flat_map(|rg| {
                let row_group = metadata.row_group(*rg);
                needed.iter().map(move |c| {
                    let (start, length) = row_group.column(*c).byte_range();
                    start..start + length
                })
            })
            .collect();
        let mut chunks = Vec::new();
        for range in coalesce(ranges) {
            let bytes = self.get(range.clone(), &mut stats).await?;
            chunks.push((range.start, bytes));
        }
        let file = SparseFile::new(self.size, chunks);

        let filter = request.filter;
        let decode = move || {
            let mut builder =
                ParquetRecordBatchReaderBuilder::new_with_metadata(file, arrow_metadata)
                    .with_batch_size(BATCH_ROWS)
                    .with_row_groups(kept);
            let parquet_schema = builder.parquet_schema().clone();
            builder = builder.with_projection(ProjectionMask::roots(&parquet_schema, projection));
            if let Some(filter) = filter {
                let mask = ProjectionMask::roots(&parquet_schema, filter_columns);
                let evaluate = filter.evaluate;
                let predicate = ArrowPredicateFn::new(mask, move |batch| {
                    evaluate(&batch).map_err(ArrowError::ComputeError)
                });
                builder = builder.with_row_filter(RowFilter::new(vec![Box::new(predicate)]));
            }
            builder
                .build()
                .map_err(|e| FormatError::Unreadable(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| FormatError::Unreadable(e.to_string()))
        };
        Ok(Fetched::new(stats, Box::new(decode)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tessera_object::MemoryStore;
    use tessera_types::event::Scalar;

    use super::*;
    use crate::scan::{Bound, RowPredicate};

    /// A file of 6 rows in row groups of 2: timestamps 0..6, bodies
    /// "row{i}", and a `pad` column wider than the coalescing gap so that
    /// projection changes the bytes read.
    async fn file() -> (ParquetReader, Arc<dyn ObjectStore>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("body", DataType::Utf8, true),
            Field::new("pad", DataType::Utf8, true),
        ]));
        let rows = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(TimestampNanosecondArray::from((0..6).collect::<Vec<i64>>())),
                Arc::new(StringArray::from_iter_values(
                    (0..6).map(|i| format!("row{i}")),
                )),
                Arc::new(StringArray::from_iter_values(
                    (0..6).map(|i| format!("{i}").repeat(50_000)),
                )),
            ],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .set_dictionary_enabled(false)
            .build();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties)).unwrap();
        writer.write(&rows).unwrap();
        writer.close().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let key = Key::new("data/file.parquet").unwrap();
        store.put(&key, Bytes::from(bytes.clone())).await.unwrap();
        (
            ParquetReader::new(Arc::clone(&store), key, bytes.len() as u64),
            store,
        )
    }

    fn timestamps(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("timestamp")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[tokio::test]
    async fn reads_every_row_with_a_full_request() {
        let (reader, _) = file().await;
        let fetched = reader.fetch(ScanRequest::default()).await.unwrap();
        let stats = fetched.stats();
        assert_eq!(stats.row_groups, 3);
        assert_eq!(stats.row_groups_read, 3);
        let batches = fetched.decode().unwrap();
        assert_eq!(timestamps(&batches), vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(batches[0].num_columns(), 3);
    }

    #[tokio::test]
    async fn prunes_row_groups_by_bounds() {
        let (reader, _) = file().await;
        let request = ScanRequest {
            projection: Some(vec!["timestamp".into()]),
            bounds: vec![Bound {
                column: "timestamp".into(),
                min: Some(Scalar::Int(2)),
                max: Some(Scalar::Int(3)),
            }],
            filter: None,
            row_groups: None,
        };
        let fetched = reader.fetch(request).await.unwrap();
        assert_eq!(fetched.stats().row_groups_read, 1);
        assert_eq!(timestamps(&fetched.decode().unwrap()), vec![2, 3]);

        let by_text = ScanRequest {
            projection: Some(vec!["timestamp".into()]),
            bounds: vec![Bound {
                column: "body".into(),
                min: Some(Scalar::Text("row4".into())),
                max: None,
            }],
            filter: None,
            row_groups: None,
        };
        let fetched = reader.fetch(by_text).await.unwrap();
        assert_eq!(fetched.stats().row_groups_read, 1);
        assert_eq!(timestamps(&fetched.decode().unwrap()), vec![4, 5]);
    }

    #[tokio::test]
    async fn a_projection_fetches_fewer_bytes() {
        let (reader, _) = file().await;
        let everything = reader.fetch(ScanRequest::default()).await.unwrap().stats();
        let narrow = ScanRequest {
            projection: Some(vec!["body".into()]),
            ..ScanRequest::default()
        };
        let narrow = reader.fetch(narrow).await.unwrap().stats();
        assert!(
            narrow.bytes_read * 4 < everything.bytes_read,
            "{narrow:?} vs {everything:?}"
        );
        assert!(narrow.requests >= 1);
    }

    #[tokio::test]
    async fn a_filter_materializes_matching_rows_only() {
        let (reader, _) = file().await;
        let request = ScanRequest {
            projection: Some(vec!["timestamp".into(), "pad".into()]),
            bounds: vec![],
            filter: Some(RowPredicate {
                columns: vec!["body".into()],
                evaluate: Arc::new(|batch| {
                    let bodies = batch
                        .column_by_name("body")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    Ok(bodies
                        .iter()
                        .map(|b| Some(b == Some("row1") || b == Some("row5")))
                        .collect::<BooleanArray>())
                }),
            }),
            row_groups: None,
        };
        let fetched = reader.fetch(request).await.unwrap();
        let batches = fetched.decode().unwrap();
        assert_eq!(timestamps(&batches), vec![1, 5]);
        assert!(batches.iter().all(|b| b.column_by_name("body").is_none()));
    }

    #[tokio::test]
    async fn rejects_files_without_a_footer() {
        let store: Arc<dyn ObjectStore> = Arc::new(MemoryStore::new());
        let key = Key::new("data/junk").unwrap();
        store
            .put(&key, Bytes::from_static(b"not a parquet file at all"))
            .await
            .unwrap();
        let reader = ParquetReader::new(store, key, 25);
        assert!(matches!(
            reader.fetch(ScanRequest::default()).await,
            Err(FormatError::Unreadable(_))
        ));
    }

    #[test]
    fn coalescing_merges_close_ranges_only() {
        let merged = coalesce(vec![200..300, 0..100, 100..150, 1_000_000..1_000_010]);
        assert_eq!(merged, vec![0..300, 1_000_000..1_000_010]);
    }

    #[test]
    fn unknown_columns_in_the_projection_are_errors() {
        let _ = Int64Array::from(vec![1]);
        let request = ScanRequest {
            projection: Some(vec!["nope".into()]),
            ..ScanRequest::default()
        };
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (reader, _) = runtime.block_on(file());
        assert!(runtime.block_on(reader.fetch(request)).is_err());
    }

    #[tokio::test]
    async fn an_index_decision_restricts_the_row_groups() {
        let (reader, _) = file().await;
        let request = ScanRequest {
            projection: Some(vec!["timestamp".into()]),
            row_groups: Some(vec![0, 2]),
            ..ScanRequest::default()
        };
        let fetched = reader.fetch(request).await.unwrap();
        assert_eq!(fetched.stats().row_groups_read, 2);
        assert_eq!(timestamps(&fetched.decode().unwrap()), vec![0, 1, 4, 5]);
    }
}
