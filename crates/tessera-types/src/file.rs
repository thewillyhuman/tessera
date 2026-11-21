// SPDX-License-Identifier: Apache-2.0

//! What a manifest records about a data file (ST-12). Files describe
//! themselves (P3): every layout decision is recorded per file, so files
//! of different layouts coexist in one partition.

use serde::{Deserialize, Serialize};

use crate::event::Scalar;
use crate::{FileId, PartitionId, PoolId, Seq, Timestamp, WriterId};

/// The level of a data file (section 6.9).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    /// Written by ingest flush or recovery: default sort, statistics only.
    L1,
    /// Written by compaction: current layout, full indexes.
    L2,
    /// As L2, after retention sampling, final clustering and trace
    /// completion.
    L3,
}

/// Where a file's rows came from.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(tag = "from", rename_all = "lowercase")]
pub enum FileSource {
    /// An L1 file: the rows of this partition in the writer's segments
    /// `(after, through]`.
    Writer {
        /// The writer.
        writer: WriterId,
        /// The last segment before the covered range.
        after: Seq,
        /// The last covered segment.
        through: Seq,
    },
    /// A compacted file: the files it replaced.
    Compaction {
        /// The input files.
        inputs: Vec<FileId>,
    },
}

/// A secondary index kept in a file's sidecar (ST-14, section 8.4).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexKind {
    /// A Bloom filter over the column's values, for point lookups.
    Bloom,
    /// A dictionary of tokens with row-group postings, for token search.
    Term,
    /// Trigrams with row-group postings, for substring and regex search.
    Trigram,
}

/// One index of a file: its kind and the column it covers.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct IndexRef {
    /// The kind of index.
    pub kind: IndexKind,
    /// The indexed column.
    pub column: String,
}

/// Per-column statistics of a dedicated column.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct ColumnStats {
    /// The column.
    pub name: String,
    /// The smallest non-null value, if any value is non-null.
    pub min: Option<Scalar>,
    /// The largest non-null value, if any value is non-null.
    pub max: Option<Scalar>,
    /// How many values are null.
    pub null_count: u64,
    /// Distinct non-null values, exact up to a cap past which the cap
    /// stands for "many" (AD-6); none when the type cannot be compared.
    #[serde(default)]
    pub distinct: Option<u64>,
    /// The octiles (AD-6): the non-null values at the eighths of their
    /// order, 1/8 to 7/8, off which the share of values at or below a
    /// bound is read; empty when the type has no order or the values
    /// are fewer than eight.
    #[serde(default)]
    pub quantiles: Vec<Scalar>,
}

/// A data file's manifest entry.
///
/// New fields get `serde(default)` so that entries written by older
/// releases stay readable (OP-8).
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct FileMeta {
    /// The file's identity; its key follows from the partition and the id.
    pub file_id: FileId,
    /// The partition the file belongs to.
    pub partition: PartitionId,
    /// The pool holding the file.
    pub pool: PoolId,
    /// The file's level.
    pub level: Level,
    /// Where its rows came from.
    pub source: FileSource,
    /// The earliest event time in the file.
    pub min_timestamp: Timestamp,
    /// The latest event time in the file.
    pub max_timestamp: Timestamp,
    /// How many rows it holds.
    pub row_count: u64,
    /// Its size in bytes.
    pub size_bytes: u64,
    /// The layout epoch it was written under (AD-9); zero is the default
    /// layout.
    pub layout_epoch: u64,
    /// The columns it is sorted by, in order.
    pub sort_key: Vec<String>,
    /// Statistics of its dedicated columns.
    #[serde(default)]
    pub columns: Vec<ColumnStats>,
    /// The indexes in its sidecar; empty when it has none.
    #[serde(default)]
    pub indexes: Vec<IndexRef>,
    /// When the file's rows were ingested (CP-2): the flush that wrote
    /// an L1 file, the latest input of a compacted one; none for a
    /// file written before this was recorded.
    #[serde(default)]
    pub ingested_at: Option<Timestamp>,
}

impl FileMeta {
    /// Whether the file has an index of `kind` on `column`.
    #[must_use]
    pub fn has_index(&self, kind: IndexKind, column: &str) -> bool {
        self.indexes
            .iter()
            .any(|i| i.kind == kind && i.column == column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DatasetName, Kind, TenantId, Window};

    fn meta() -> FileMeta {
        FileMeta {
            file_id: FileId::from_u128(7),
            partition: PartitionId {
                tenant: TenantId::DEFAULT,
                dataset: DatasetName::new("prod").unwrap(),
                kind: Kind::Log,
                window: Window::containing(Timestamp::UNIX_EPOCH, Window::DEFAULT_LENGTH).unwrap(),
            },
            pool: PoolId::new("root").unwrap(),
            level: Level::L1,
            source: FileSource::Writer {
                writer: WriterId::from_u128(1),
                after: Seq::new(2),
                through: Seq::new(5),
            },
            min_timestamp: Timestamp::from_nanos(10),
            max_timestamp: Timestamp::from_nanos(20),
            row_count: 3,
            size_bytes: 1_024,
            layout_epoch: 0,
            sort_key: vec!["timestamp".into()],
            columns: vec![ColumnStats {
                name: "body".into(),
                min: None,
                max: None,
                null_count: 3,
                distinct: None,
                quantiles: Vec::new(),
            }],
            indexes: Vec::new(),
            ingested_at: None,
        }
    }

    #[test]
    fn file_meta_round_trips_through_json() {
        let meta = meta();
        let json = serde_json::to_string(&meta).unwrap();
        assert_eq!(serde_json::from_str::<FileMeta>(&json).unwrap(), meta);
        assert!(json.contains(r#""level":"l1""#));
        assert!(json.contains(r#""from":"writer""#));
    }

    #[test]
    fn entries_without_newer_fields_still_decode() {
        let mut value: serde_json::Value = serde_json::to_value(meta()).unwrap();
        value.as_object_mut().unwrap().remove("columns");
        let decoded: FileMeta = serde_json::from_value(value).unwrap();
        assert!(decoded.columns.is_empty());
    }

    #[test]
    fn index_references_are_optional_and_queryable() {
        let mut file = meta();
        assert!(!file.has_index(IndexKind::Term, "body"));
        file.indexes.push(IndexRef {
            kind: IndexKind::Term,
            column: "body".into(),
        });
        assert!(file.has_index(IndexKind::Term, "body"));
        assert!(!file.has_index(IndexKind::Trigram, "body"));
        let json = serde_json::to_string(&file).unwrap();
        let without: FileMeta = serde_json::from_str(
            &json.replace(r#","indexes":[{"kind":"term","column":"body"}]"#, ""),
        )
        .unwrap();
        assert!(
            without.indexes.is_empty(),
            "older entries have no indexes field"
        );
    }
}
