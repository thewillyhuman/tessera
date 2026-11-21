// SPDX-License-Identifier: Apache-2.0

//! The table metadata file, `v{N}.metadata.json`, and the version hint
//! a reader finds the current one by, as a Hadoop-style catalog lays
//! them out under the table's `metadata/` prefix.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tessera_object::{Key, ObjectError, ObjectStore};

use crate::export::ExportError;
use crate::schema::TableSchema;

/// The table metadata, Iceberg v3.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Always 3.
    #[serde(rename = "format-version")]
    pub format_version: u8,
    /// The table's identity, kept across exports.
    #[serde(rename = "table-uuid")]
    pub table_uuid: String,
    /// The table's location URI.
    pub location: String,
    /// The highest sequence number given.
    #[serde(rename = "last-sequence-number")]
    pub last_sequence_number: i64,
    /// When the metadata was written, milliseconds since the epoch.
    #[serde(rename = "last-updated-ms")]
    pub last_updated_ms: i64,
    /// The highest field id given.
    #[serde(rename = "last-column-id")]
    pub last_column_id: i32,
    /// The current schema's id.
    #[serde(rename = "current-schema-id")]
    pub current_schema_id: i32,
    /// The schemas; only the current one is kept.
    pub schemas: Vec<TableSchema>,
    /// The partition spec in use.
    #[serde(rename = "default-spec-id")]
    pub default_spec_id: i32,
    /// The partition specs: one, unpartitioned.
    #[serde(rename = "partition-specs")]
    pub partition_specs: Vec<PartitionSpec>,
    /// The highest partition field id given.
    #[serde(rename = "last-partition-id")]
    pub last_partition_id: i32,
    /// The sort order in use.
    #[serde(rename = "default-sort-order-id")]
    pub default_sort_order_id: i32,
    /// The sort orders: one, unsorted.
    #[serde(rename = "sort-orders")]
    pub sort_orders: Vec<SortOrder>,
    /// Table properties.
    pub properties: BTreeMap<String, String>,
    /// The current snapshot's id.
    #[serde(rename = "current-snapshot-id")]
    pub current_snapshot_id: i64,
    /// The snapshots; only the current one is kept.
    pub snapshots: Vec<Snapshot>,
    /// The snapshot history; the current one.
    #[serde(rename = "snapshot-log")]
    pub snapshot_log: Vec<SnapshotLog>,
    /// The previous metadata file, if any.
    #[serde(rename = "metadata-log")]
    pub metadata_log: Vec<MetadataLog>,
    /// Branches: `main` at the current snapshot.
    pub refs: BTreeMap<String, Reference>,
    /// The next row id (row lineage).
    #[serde(rename = "next-row-id")]
    pub next_row_id: i64,
}

impl TableMetadata {
    /// The current schema.
    #[must_use]
    pub fn current_schema(&self) -> Option<&TableSchema> {
        self.schemas
            .iter()
            .find(|s| s.schema_id == self.current_schema_id)
    }
}

/// A partition spec.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PartitionSpec {
    /// The spec id.
    #[serde(rename = "spec-id")]
    pub spec_id: i32,
    /// Its fields; none for an unpartitioned table.
    pub fields: Vec<serde_json::Value>,
}

/// A sort order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SortOrder {
    /// The order id.
    #[serde(rename = "order-id")]
    pub order_id: i32,
    /// Its fields; none when unsorted.
    pub fields: Vec<serde_json::Value>,
}

/// A snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    /// The snapshot id.
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// Its sequence number.
    #[serde(rename = "sequence-number")]
    pub sequence_number: i64,
    /// When it was made, milliseconds since the epoch.
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    /// Its manifest list's location.
    #[serde(rename = "manifest-list")]
    pub manifest_list: String,
    /// What it did, `operation` at least.
    pub summary: BTreeMap<String, String>,
    /// The schema it was written with.
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    /// The first row id of its rows.
    #[serde(rename = "first-row-id")]
    pub first_row_id: i64,
    /// How many rows it added.
    #[serde(rename = "added-rows")]
    pub added_rows: i64,
}

/// A snapshot history entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SnapshotLog {
    /// When the snapshot became current.
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    /// The snapshot.
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
}

/// A metadata history entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadataLog {
    /// When the metadata file was written.
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    /// Its location.
    #[serde(rename = "metadata-file")]
    pub metadata_file: String,
}

/// A branch or tag.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Reference {
    /// The snapshot it points to.
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// `branch` or `tag`.
    #[serde(rename = "type")]
    pub kind: String,
}

/// The name of the version hint.
pub const HINT: &str = "version-hint.text";

/// The name of the metadata file of `version`.
#[must_use]
pub fn metadata_file(version: u64) -> String {
    format!("v{version}.metadata.json")
}

/// The name of the manifest list of `version`.
#[must_use]
pub fn manifest_list_file(version: u64) -> String {
    format!("snap-{version}.avro")
}

/// The name of the manifest of `version`.
#[must_use]
pub fn manifest_file(version: u64) -> String {
    format!("manifest-{version}.avro")
}

/// The key of `name` under a table's `metadata/` prefix.
///
/// # Panics
///
/// If `name` is not a valid key component, which none of this module's
/// names is.
#[must_use]
pub fn key(table: &Key, name: &str) -> Key {
    table
        .join("metadata")
        .and_then(|k| k.join(name))
        .unwrap_or_else(|e| unreachable!("metadata names are valid key components: {e}"))
}

/// The current version and metadata of the table at `table`, by its
/// hint; none when it was never exported.
///
/// # Errors
///
/// If the store fails or the metadata does not parse.
pub async fn current(
    store: &dyn ObjectStore,
    table: &Key,
) -> Result<Option<(u64, TableMetadata)>, ExportError> {
    let hint = key(table, HINT);
    let version = match store.get(&hint).await {
        Ok(bytes) => String::from_utf8_lossy(&bytes)
            .trim()
            .parse::<u64>()
            .map_err(|_| ExportError::Corrupt {
                key: hint.clone(),
                reason: "the version hint is not a number".into(),
            })?,
        Err(ObjectError::NotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let key = key(table, &metadata_file(version));
    let bytes = store.get(&key).await?;
    let metadata = serde_json::from_slice(&bytes).map_err(|e| ExportError::Corrupt {
        key,
        reason: e.to_string(),
    })?;
    Ok(Some((version, metadata)))
}
