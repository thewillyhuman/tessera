// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The Parquet implementation of [`FileFormat`].
//!
//! Rows are sorted by the layout's key, written in its row groups (one
//! batch per row group, so that the sidecar's postings line up with the
//! file's), and indexed into a sidecar when the layout asks for one.
//! Pages are compressed with zstd.

use arrow::array::{Array, RecordBatch, TimestampNanosecondArray};
use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use tessera_types::Timestamp;
use tessera_types::event::TIMESTAMP;

use std::sync::Arc;

use tessera_object::{Key, ObjectStore};

use crate::footer::FooterCache;
use crate::layout::Layout;
use crate::reader::ParquetReader;
use crate::scan::FileReader;
use crate::sidecar::Sidecar;
use crate::stats::column_stats;
use crate::{FileFormat, FormatError, Written};
use tessera_types::{IndexKind, IndexRef};

/// Parquet with zstd page compression.
#[derive(Clone, Debug, Default)]
pub struct ParquetFormat {
    footers: Option<Arc<FooterCache>>,
}

impl ParquetFormat {
    /// The format keeping the footers of the files it opens, at most
    /// `capacity` of them (section 10.8).
    #[must_use]
    pub fn with_footer_cache(capacity: usize) -> Self {
        ParquetFormat {
            footers: Some(Arc::new(FooterCache::new(capacity))),
        }
    }

    /// The footer cache, if this format keeps one.
    #[must_use]
    pub fn footers(&self) -> Option<&Arc<FooterCache>> {
        self.footers.as_ref()
    }

    /// zstd pages, and the event time delta-encoded rather than
    /// dictionary-encoded: event times rarely repeat, but within a sort
    /// run they grow by little, which the delta encoding stores in a
    /// few bits where a dictionary stores every distinct value whole.
    /// The observed time stays dictionary-encoded: a batch's rows share
    /// it, so it repeats, and it is not in time order within a file.
    fn properties() -> WriterProperties {
        let path = ColumnPath::from(TIMESTAMP);
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_column_dictionary_enabled(path.clone(), false)
            .set_column_encoding(path, Encoding::DELTA_BINARY_PACKED)
            .build()
    }
}

impl FileFormat for ParquetFormat {
    fn write(&self, batches: &[RecordBatch], layout: &Layout) -> Result<Written, FormatError> {
        let first = batches.first().ok_or(FormatError::Empty)?;
        let schema = first.schema();
        if batches.iter().any(|b| b.schema() != schema) {
            return Err(FormatError::MixedSchemas);
        }
        let rows = concat_batches(&schema, batches)?;
        if rows.num_rows() == 0 {
            return Err(FormatError::Empty);
        }
        timestamp_column(&rows)?;
        let sort_key = layout.full_sort_key();
        let sorted = sort_by(&rows, &sort_key)?;
        let timestamps = timestamp_column(&sorted)?;
        let min_timestamp =
            Timestamp::from_nanos(arrow::compute::min(timestamps).ok_or(FormatError::NoTimestamp)?);
        let max_timestamp =
            Timestamp::from_nanos(arrow::compute::max(timestamps).ok_or(FormatError::NoTimestamp)?);

        let rows_per_group = layout.row_group_rows.max(1);
        let groups: Vec<RecordBatch> = (0..sorted.num_rows())
            .step_by(rows_per_group)
            .map(|offset| sorted.slice(offset, rows_per_group.min(sorted.num_rows() - offset)))
            .collect();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema.clone(), Some(Self::properties()))
            .map_err(|e| FormatError::Unreadable(e.to_string()))?;
        for group in &groups {
            writer
                .write(group)
                .map_err(|e| FormatError::Unreadable(e.to_string()))?;
            writer
                .flush()
                .map_err(|e| FormatError::Unreadable(e.to_string()))?;
        }
        writer
            .close()
            .map_err(|e| FormatError::Unreadable(e.to_string()))?;

        let (sidecar, indexes) = build_sidecar(layout, &groups)?;
        let columns = schema
            .fields()
            .iter()
            .zip(sorted.columns())
            .map(|(field, array)| column_stats(field.name(), array))
            .collect();
        Ok(Written {
            bytes: Bytes::from(bytes),
            row_count: sorted.num_rows() as u64,
            min_timestamp,
            max_timestamp,
            sort_key,
            columns,
            row_groups: groups.len(),
            sidecar,
            indexes,
        })
    }

    fn open(&self, store: Arc<dyn ObjectStore>, key: Key, size: u64) -> Box<dyn FileReader> {
        let reader = ParquetReader::new(store, key, size);
        Box::new(match &self.footers {
            Some(footers) => reader.with_footers(Arc::clone(footers)),
            None => reader,
        })
    }

    fn read(&self, bytes: Bytes) -> Result<Vec<RecordBatch>, FormatError> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| FormatError::Unreadable(e.to_string()))?
            .build()
            .map_err(|e| FormatError::Unreadable(e.to_string()))?;
        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| FormatError::Unreadable(e.to_string()))
    }
}

fn timestamp_column(rows: &RecordBatch) -> Result<&TimestampNanosecondArray, FormatError> {
    rows.column_by_name(TIMESTAMP)
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
        .filter(|c| c.null_count() == 0)
        .ok_or(FormatError::NoTimestamp)
}

fn sort_by(rows: &RecordBatch, sort_key: &[String]) -> Result<RecordBatch, FormatError> {
    let columns = sort_key
        .iter()
        .map(|name| {
            rows.column_by_name(name)
                .map(|values| SortColumn {
                    values: Arc::clone(values),
                    options: Some(SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                })
                .ok_or_else(|| FormatError::UnknownColumn(name.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let indices = lexsort_to_indices(&columns, None)?;
    let columns = rows
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(rows.schema(), columns)?)
}

/// The sidecar of `layout`'s indexes over `groups`, and what it holds.
/// Index columns the rows lack are skipped: a file describes itself.
fn build_sidecar(
    layout: &Layout,
    groups: &[RecordBatch],
) -> Result<(Option<Bytes>, Vec<IndexRef>), FormatError> {
    let present = |name: &String| {
        groups
            .first()
            .is_some_and(|g| g.column_by_name(name).is_some())
    };
    let spec = crate::sidecar::IndexSpec {
        term: layout
            .indexes
            .term
            .iter()
            .filter(|c| present(c))
            .cloned()
            .collect(),
        trigram: layout
            .indexes
            .trigram
            .iter()
            .filter(|c| present(c))
            .cloned()
            .collect(),
        bloom: layout
            .indexes
            .bloom
            .iter()
            .filter(|c| present(c))
            .cloned()
            .collect(),
    };
    let mut indexes: Vec<IndexRef> = Vec::new();
    for (kind, columns) in [
        (IndexKind::Term, &spec.term),
        (IndexKind::Trigram, &spec.trigram),
        (IndexKind::Bloom, &spec.bloom),
    ] {
        indexes.extend(columns.iter().map(|c| IndexRef {
            kind,
            column: c.clone(),
        }));
    }
    if indexes.is_empty() {
        return Ok((None, indexes));
    }
    let sidecar = Sidecar::build(&spec, groups.len(), |name, rg| {
        groups.get(rg).and_then(|g| g.column_by_name(name).cloned())
    })?;
    Ok((Some(sidecar.encode()), indexes))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::array::StringArray;
    use tessera_types::event::{BODY, Scalar};
    use tessera_types::{DatasetName, EventBatch, LogRow, TenantId};

    use super::*;
    use tessera_types::event::OBSERVED_TIMESTAMP;

    fn log(ts: i64, body: &str) -> LogRow {
        LogRow {
            timestamp: Timestamp::from_nanos(ts),
            observed_timestamp: Timestamp::from_nanos(ts),
            event_id: event_id(ts),
            body: Some(body.to_owned()),
            attributes: BTreeMap::from([("attr.k".to_owned(), Scalar::Text("v".repeat(50)))]),
            ..LogRow::default()
        }
    }

    fn event_id(ts: i64) -> [u8; 16] {
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&ts.to_le_bytes());
        id
    }

    fn batch(rows: &[LogRow]) -> RecordBatch {
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), rows)
            .unwrap()
            .rows()
            .clone()
    }

    #[test]
    fn the_event_time_is_delta_encoded() {
        use parquet::file::reader::{FileReader, SerializedFileReader};

        let rows: Vec<LogRow> = (0..200).map(|i| log(1_000 + i * 7, "x")).collect();
        let written = ParquetFormat::default()
            .write(&[batch(&rows)], &Layout::l1())
            .unwrap();
        let reader = SerializedFileReader::new(written.bytes.clone()).unwrap();
        let group = &reader.metadata().row_groups()[0];
        let encodings = |name: &str| {
            group
                .columns()
                .iter()
                .find(|c| c.column_path().string() == name)
                .map(|c| c.encodings().collect::<Vec<_>>())
                .unwrap_or_default()
        };
        assert!(
            encodings(TIMESTAMP).contains(&Encoding::DELTA_BINARY_PACKED),
            "{:?}",
            encodings(TIMESTAMP)
        );
        assert!(
            !encodings(OBSERVED_TIMESTAMP).contains(&Encoding::DELTA_BINARY_PACKED),
            "the observed time repeats and stays dictionary-encoded: {:?}",
            encodings(OBSERVED_TIMESTAMP)
        );
        assert!(
            !encodings(TIMESTAMP).contains(&Encoding::RLE_DICTIONARY),
            "{:?}",
            encodings(TIMESTAMP)
        );
    }

    #[test]
    fn a_file_round_trips_sorted_by_time() {
        let written = ParquetFormat::default()
            .write(
                &[batch(&[log(30, "c"), log(10, "a")]), batch(&[log(20, "b")])],
                &Layout::l1(),
            )
            .unwrap();
        assert_eq!(written.row_count, 3);
        assert_eq!(
            (written.min_timestamp, written.max_timestamp),
            (Timestamp::from_nanos(10), Timestamp::from_nanos(30))
        );
        assert_eq!(written.sort_key, ["timestamp"]);
        let read = ParquetFormat::default().read(written.bytes).unwrap();
        let rows = concat_batches(&read[0].schema(), &read).unwrap();
        assert_eq!(rows.num_rows(), 3);
        let bodies = rows
            .column_by_name(BODY)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(bodies.iter().flatten().collect::<Vec<_>>(), ["a", "b", "c"]);
        assert_eq!(
            rows.schema(),
            batch(&[log(1, "x")]).schema(),
            "the schema survives the round trip"
        );
    }

    #[test]
    fn statistics_cover_every_column() {
        let written = ParquetFormat::default()
            .write(&[batch(&[log(5, "zzz"), log(1, "aaa")])], &Layout::l1())
            .unwrap();
        let body = written.columns.iter().find(|c| c.name == BODY).unwrap();
        assert_eq!(body.min, Some(Scalar::Text("aaa".into())));
        assert_eq!(body.max, Some(Scalar::Text("zzz".into())));
        let timestamp = written
            .columns
            .iter()
            .find(|c| c.name == TIMESTAMP)
            .unwrap();
        assert_eq!(
            (timestamp.min.clone(), timestamp.max.clone()),
            (Some(Scalar::Int(1)), Some(Scalar::Int(5)))
        );
        assert_eq!(written.columns.len(), batch(&[log(1, "x")]).num_columns());
    }

    #[test]
    fn repetitive_data_compresses() {
        let rows: Vec<LogRow> = (0..2_000)
            .map(|i| log(i, "the same body every time"))
            .collect();
        let raw: usize = batch(&rows).get_array_memory_size();
        let written = ParquetFormat::default()
            .write(&[batch(&rows)], &Layout::l1())
            .unwrap();
        assert!(
            written.bytes.len() * 4 < raw,
            "{} bytes for {raw} in memory",
            written.bytes.len()
        );
    }

    #[test]
    fn empty_and_mixed_inputs_are_rejected() {
        assert!(matches!(
            ParquetFormat::default().write(&[], &Layout::l1()),
            Err(FormatError::Empty)
        ));
        let log = batch(&[log(1, "x")]);
        let other = RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Utf8, true),
            ])),
            vec![std::sync::Arc::new(StringArray::from(vec!["a"]))],
        )
        .unwrap();
        assert!(matches!(
            ParquetFormat::default().write(&[log, other.clone()], &Layout::l1()),
            Err(FormatError::MixedSchemas)
        ));
        assert!(matches!(
            ParquetFormat::default().write(&[other], &Layout::l1()),
            Err(FormatError::NoTimestamp)
        ));
    }

    #[test]
    fn garbage_is_unreadable_not_a_panic() {
        assert!(matches!(
            ParquetFormat::default().read(Bytes::from_static(b"not parquet")),
            Err(FormatError::Unreadable(_))
        ));
        assert!(matches!(
            ParquetFormat::default().read(Bytes::new()),
            Err(FormatError::Unreadable(_))
        ));
    }

    #[test]
    fn a_layout_sorts_splits_and_indexes() {
        let mut rows: Vec<LogRow> = (0..10)
            .map(|i| {
                let mut row = log(100 - i, if i % 2 == 0 { "even checkout" } else { "odd" });
                row.severity_number = Some(if i < 5 { 17 } else { 9 });
                row.trace_id = Some([u8::try_from(i).unwrap(); 16]);
                row
            })
            .collect();
        rows.reverse();
        let layout = Layout {
            sort_key: vec!["severity_number".into()],
            row_group_rows: 4,
            indexes: crate::sidecar::IndexSpec {
                term: vec!["body".into()],
                trigram: vec!["body".into()],
                bloom: vec!["trace_id".into(), "absent".into()],
            },
        };
        let written = ParquetFormat::default()
            .write(&[batch(&rows)], &layout)
            .unwrap();
        assert_eq!(written.sort_key, vec!["severity_number", "timestamp"]);
        assert_eq!(written.row_groups, 3);
        assert_eq!(written.min_timestamp, Timestamp::from_nanos(91));
        assert_eq!(written.max_timestamp, Timestamp::from_nanos(100));
        assert_eq!(written.indexes.len(), 3, "the absent column is skipped");
        let read = ParquetFormat::default()
            .read(written.bytes.clone())
            .unwrap();
        let all = concat_batches(&read[0].schema(), &read).unwrap();
        let severities = all
            .column_by_name("severity_number")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert!(severities.values().windows(2).all(|w| w[0] <= w[1]));
        let sidecar = Sidecar::decode(written.sidecar.unwrap()).unwrap();
        assert_eq!(sidecar.row_groups(), 3);
        let with_checkout = sidecar.row_groups_with_tokens("body", "checkout").unwrap();
        assert!(!with_checkout.to_vec().is_empty());
        assert_eq!(sidecar.may_contain("trace_id", &[3u8; 16]), Some(true));
        assert_eq!(sidecar.may_contain("trace_id", &[42u8; 16]), Some(false));
    }

    #[test]
    fn unknown_sort_keys_are_rejected() {
        let layout = Layout {
            sort_key: vec!["nope".into()],
            ..Layout::l1()
        };
        assert!(matches!(
            ParquetFormat::default().write(&[batch(&[log(1, "a")])], &layout),
            Err(FormatError::UnknownColumn(c)) if c == "nope"
        ));
    }
}
