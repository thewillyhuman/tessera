// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! A manifest's checkpoint as a Parquet file (MF-5): one row per file,
//! sorted by the files' first timestamps, in row groups whose
//! statistics let a reader fetch only the groups whose files can
//! overlap its range; the writer positions, the tombstones and the
//! dropped mark are rows too, timestamped so that no range leaves
//! them out. A reader that needs the whole state decodes every row;
//! one that needs the files of a range reads the footer and the groups
//! that matter, and nothing else (QE-7).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use tessera_commit::DecodeError;
use tessera_format::{FileFormat, IndexSpec, Layout, ParquetFormat};
use tessera_types::event::TIMESTAMP;
use tessera_types::{FileMeta, Seq, Timestamp, Version, WriterId};

use crate::manifest::{ManifestState, Tombstone};

/// Files per row group of a checkpoint: a range read fetches this many
/// files' rows at least, and a partition of a few thousand files is a
/// few groups.
pub const ROW_GROUP_ROWS: usize = 1_024;

/// The column saying what a row is.
const KIND: &str = "kind";
/// The column holding a row's record as JSON.
const META: &str = "meta";
/// The column holding a file's last timestamp.
const MAX_TIMESTAMP: &str = "max_timestamp";

const FILE: &str = "file";
const POSITION: &str = "position";
const TOMBSTONE: &str = "tombstone";
const DROPPED: &str = "dropped";
const VERSION: &str = "version";

/// One row of a checkpoint as read back.
#[derive(Debug)]
pub(crate) enum Row {
    File(FileMeta),
    Position(WriterId, Seq),
    Tombstone(Tombstone),
    Dropped,
    Version(Version),
}

/// A writer position as a row.
#[derive(serde::Serialize, serde::Deserialize)]
struct PositionRow {
    writer: WriterId,
    seq: Seq,
}

/// `state` at `version` as a Parquet checkpoint.
///
/// # Errors
///
/// If the rows cannot be built or written.
pub fn encode(state: &ManifestState, version: Version) -> Result<Vec<u8>, DecodeError> {
    encode_with(state, version, ROW_GROUP_ROWS)
}

/// [`encode`] with `row_group_rows` files per row group.
pub(crate) fn encode_with(
    state: &ManifestState,
    version: Version,
    row_group_rows: usize,
) -> Result<Vec<u8>, DecodeError> {
    let mut rows: Vec<(i64, i64, &str, String)> = Vec::new();
    let header = |kind: &'static str, meta: String| (i64::MIN, i64::MAX, kind, meta);
    rows.push(header(VERSION, version.to_string()));
    if state.is_dropped() {
        rows.push(header(DROPPED, String::new()));
    }
    for (writer, seq) in state.positions() {
        let meta = serde_json::to_string(&PositionRow { writer, seq }).map_err(encoding)?;
        rows.push(header(POSITION, meta));
    }
    for tombstone in state.tombstones() {
        rows.push(header(
            TOMBSTONE,
            serde_json::to_string(tombstone).map_err(encoding)?,
        ));
    }
    for file in state.files() {
        rows.push((
            file.min_timestamp.as_nanos(),
            file.max_timestamp.as_nanos(),
            FILE,
            serde_json::to_string(file).map_err(encoding)?,
        ));
    }
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampNanosecondArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.3.as_str()).collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(encoding)?;
    let layout = Layout {
        sort_key: vec![TIMESTAMP.to_owned()],
        row_group_rows: row_group_rows.max(1),
        indexes: IndexSpec::default(),
    };
    let written = ParquetFormat::default()
        .write(std::slice::from_ref(&batch), &layout, None)
        .map_err(encoding)?;
    Ok(written.bytes.to_vec())
}

/// The state in the checkpoint `bytes`, filed under `version`.
///
/// # Errors
///
/// If the file does not decode or claims another version.
pub fn decode(version: Version, bytes: &[u8]) -> Result<ManifestState, DecodeError> {
    let batches = ParquetFormat::default()
        .read(Bytes::copy_from_slice(bytes))
        .map_err(encoding)?;
    let mut state = ManifestState::default();
    let mut claimed = None;
    for batch in &batches {
        for row in rows(batch)? {
            match row {
                Row::File(file) => state.add_file(file),
                Row::Position(writer, seq) => state.set_position(writer, seq),
                Row::Tombstone(tombstone) => state.add_tombstone(tombstone),
                Row::Dropped => state.mark_dropped(),
                Row::Version(v) => claimed = Some(v),
            }
        }
    }
    match claimed {
        Some(found) if found != version => Err(DecodeError::WrongVersion {
            claimed: found,
            expected: version,
        }),
        Some(_) => Ok(state),
        None => Err(DecodeError::Encoding("no version row".into())),
    }
}

/// The schema of a checkpoint.
pub(crate) fn schema() -> Arc<Schema> {
    let ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
    Arc::new(Schema::new(vec![
        Field::new(TIMESTAMP, ns.clone(), false),
        Field::new(MAX_TIMESTAMP, ns, false),
        Field::new(KIND, DataType::Utf8, false),
        Field::new(META, DataType::Utf8, false),
    ]))
}

/// The rows of one batch of a checkpoint.
///
/// # Errors
///
/// If a row's record does not decode.
pub(crate) fn rows(batch: &RecordBatch) -> Result<Vec<Row>, DecodeError> {
    let text = |name: &str| -> Result<&StringArray, DecodeError> {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| DecodeError::Encoding(format!("no {name} column")))
    };
    let (kinds, metas) = (text(KIND)?, text(META)?);
    (0..batch.num_rows())
        .map(|i| {
            let meta = metas.value(i);
            Ok(match kinds.value(i) {
                FILE => Row::File(serde_json::from_str(meta).map_err(encoding)?),
                POSITION => {
                    let PositionRow { writer, seq } =
                        serde_json::from_str(meta).map_err(encoding)?;
                    Row::Position(writer, seq)
                }
                TOMBSTONE => Row::Tombstone(serde_json::from_str(meta).map_err(encoding)?),
                DROPPED => Row::Dropped,
                VERSION => Row::Version(
                    meta.parse()
                        .map_err(|_| DecodeError::Encoding(format!("version {meta:?}")))?,
                ),
                other => return Err(DecodeError::Encoding(format!("row kind {other:?}"))),
            })
        })
        .collect()
}

/// The first timestamp of file rows: the column a range read bounds.
pub(crate) const fn first_timestamp_column() -> &'static str {
    TIMESTAMP
}

/// The last timestamp of file rows: the other column a range read
/// bounds.
pub(crate) const fn last_timestamp_column() -> &'static str {
    MAX_TIMESTAMP
}

/// Whether a file row at `first`..=`last` can overlap `range`; header
/// rows span everything and always do.
pub(crate) fn overlaps(first: Timestamp, last: Timestamp, range: tessera_types::TimeRange) -> bool {
    range.overlaps_closed(first, last)
}

fn encoding(error: impl std::fmt::Display) -> DecodeError {
    DecodeError::Encoding(error.to_string())
}

#[cfg(test)]
mod tests {
    use tessera_types::event::TIMESTAMP as TS;
    use tessera_types::{
        DatasetName, FileId, FileSource, Kind, Level, PartitionId, PoolId, TenantId, Window,
    };

    use super::*;

    fn partition() -> PartitionId {
        PartitionId {
            tenant: TenantId::DEFAULT,
            dataset: DatasetName::new("prod").unwrap(),
            kind: Kind::Log,
            window: Window::containing(Timestamp::UNIX_EPOCH, Window::DEFAULT_LENGTH).unwrap(),
        }
    }

    fn file(id: u128, at: i64) -> FileMeta {
        FileMeta {
            file_id: FileId::from_u128(id),
            partition: partition(),
            pool: PoolId::new("root").unwrap(),
            level: Level::L2,
            source: FileSource::Writer {
                writer: WriterId::from_u128(1),
                after: Seq::ZERO,
                through: Seq::new(1),
            },
            min_timestamp: Timestamp::from_nanos(at),
            max_timestamp: Timestamp::from_nanos(at + 10),
            row_count: 1,
            size_bytes: 1,
            layout_epoch: 0,
            sort_key: vec![TS.into()],
            columns: Vec::new(),
            indexes: Vec::new(),
            ingested_at: None,
        }
    }

    #[test]
    fn a_checkpoint_round_trips_every_part_of_the_state() {
        let mut state = ManifestState::default();
        state.add_file(file(1, 100));
        state.add_file(file(2, 200));
        state.set_position(WriterId::from_u128(1), Seq::new(7));
        state.add_tombstone(Tombstone {
            meta: file(3, 300),
            at: Timestamp::from_nanos(5),
        });
        let bytes = encode(&state, Version::new(9)).unwrap();
        assert_eq!(decode(Version::new(9), &bytes).unwrap(), state);
        assert!(matches!(
            decode(Version::new(8), &bytes),
            Err(DecodeError::WrongVersion { .. })
        ));
        assert!(decode(Version::new(9), b"not parquet").is_err());
        let mut dropped = ManifestState::default();
        dropped.mark_dropped();
        let bytes = encode(&dropped, Version::ZERO).unwrap();
        assert!(decode(Version::ZERO, &bytes).unwrap().is_dropped());
    }
}
