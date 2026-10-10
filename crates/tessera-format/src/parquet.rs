// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The Parquet implementation of [`FileFormat`].
//!
//! Rows are sorted by `timestamp` before writing: the default sort keys of
//! section 8.5 put attribute columns first, but those have no dedicated
//! column until the advisor promotes them (M8), so event time is the key
//! every file can be sorted by now. Pages are compressed with zstd.

use arrow::array::{Array, RecordBatch, TimestampNanosecondArray};
use arrow::compute::{concat_batches, sort_to_indices, take};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tessera_types::Timestamp;
use tessera_types::event::TIMESTAMP;

use std::sync::Arc;

use tessera_object::{Key, ObjectStore};

use crate::reader::ParquetReader;
use crate::scan::FileReader;
use crate::stats::column_stats;
use crate::{FileFormat, FormatError, Written};

/// Parquet with zstd page compression.
#[derive(Clone, Copy, Debug, Default)]
pub struct ParquetFormat;

impl ParquetFormat {
    fn properties() -> WriterProperties {
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build()
    }
}

impl FileFormat for ParquetFormat {
    fn write(&self, batches: &[RecordBatch]) -> Result<Written, FormatError> {
        let first = batches.first().ok_or(FormatError::Empty)?;
        let schema = first.schema();
        if batches.iter().any(|b| b.schema() != schema) {
            return Err(FormatError::MixedSchemas);
        }
        let rows = concat_batches(&schema, batches)?;
        if rows.num_rows() == 0 {
            return Err(FormatError::Empty);
        }
        let sorted = sort_by_timestamp(&rows)?;
        let timestamps = timestamp_column(&sorted)?;
        let min_timestamp = Timestamp::from_nanos(timestamps.value(0));
        let max_timestamp = Timestamp::from_nanos(timestamps.value(timestamps.len() - 1));

        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, schema.clone(), Some(Self::properties()))
            .map_err(|e| FormatError::Unreadable(e.to_string()))?;
        writer
            .write(&sorted)
            .map_err(|e| FormatError::Unreadable(e.to_string()))?;
        writer
            .close()
            .map_err(|e| FormatError::Unreadable(e.to_string()))?;

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
            sort_key: vec![TIMESTAMP.to_owned()],
            columns,
        })
    }

    fn open(&self, store: Arc<dyn ObjectStore>, key: Key, size: u64) -> Box<dyn FileReader> {
        Box::new(ParquetReader::new(store, key, size))
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

fn sort_by_timestamp(rows: &RecordBatch) -> Result<RecordBatch, FormatError> {
    let timestamps = timestamp_column(rows)?;
    let indices = sort_to_indices(timestamps, None, None)?;
    let columns = rows
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(rows.schema(), columns)?)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::array::StringArray;
    use tessera_types::event::{BODY, Scalar};
    use tessera_types::{DatasetName, EventBatch, LogRow, TenantId};

    use super::*;

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
    fn a_file_round_trips_sorted_by_time() {
        let written = ParquetFormat
            .write(&[batch(&[log(30, "c"), log(10, "a")]), batch(&[log(20, "b")])])
            .unwrap();
        assert_eq!(written.row_count, 3);
        assert_eq!(
            (written.min_timestamp, written.max_timestamp),
            (Timestamp::from_nanos(10), Timestamp::from_nanos(30))
        );
        assert_eq!(written.sort_key, ["timestamp"]);
        let read = ParquetFormat.read(written.bytes).unwrap();
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
        let written = ParquetFormat
            .write(&[batch(&[log(5, "zzz"), log(1, "aaa")])])
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
        let written = ParquetFormat.write(&[batch(&rows)]).unwrap();
        assert!(
            written.bytes.len() * 4 < raw,
            "{} bytes for {raw} in memory",
            written.bytes.len()
        );
    }

    #[test]
    fn empty_and_mixed_inputs_are_rejected() {
        assert!(matches!(ParquetFormat.write(&[]), Err(FormatError::Empty)));
        let log = batch(&[log(1, "x")]);
        let other = RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Utf8, true),
            ])),
            vec![std::sync::Arc::new(StringArray::from(vec!["a"]))],
        )
        .unwrap();
        assert!(matches!(
            ParquetFormat.write(&[log, other.clone()]),
            Err(FormatError::MixedSchemas)
        ));
        assert!(matches!(
            ParquetFormat.write(&[other]),
            Err(FormatError::NoTimestamp)
        ));
    }

    #[test]
    fn garbage_is_unreadable_not_a_panic() {
        assert!(matches!(
            ParquetFormat.read(Bytes::from_static(b"not parquet")),
            Err(FormatError::Unreadable(_))
        ));
        assert!(matches!(
            ParquetFormat.read(Bytes::new()),
            Err(FormatError::Unreadable(_))
        ));
    }
}
