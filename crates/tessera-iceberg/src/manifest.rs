// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! A snapshot's manifest file, one entry per data file with its row
//! count, size, value and null counts and bounds, and the manifest
//! list naming it: the Iceberg v3 Avro schemas, field ids included,
//! and the rows.

use tessera_types::FileMeta;
use tessera_types::event::{Scalar, TIMESTAMP};

use crate::avro::{self, Value};
use crate::schema::{TableSchema, Type};

/// The Avro schema of a manifest entry: the Iceberg v3 `manifest_entry`
/// with its `data_file`, unpartitioned.
pub const ENTRY_SCHEMA: &str = r#"{"type":"record","name":"manifest_entry","fields":[
{"name":"status","type":"int","field-id":0},
{"name":"snapshot_id","type":["null","long"],"default":null,"field-id":1},
{"name":"sequence_number","type":["null","long"],"default":null,"field-id":3},
{"name":"file_sequence_number","type":["null","long"],"default":null,"field-id":4},
{"name":"data_file","type":{"type":"record","name":"r2","fields":[
{"name":"content","type":"int","field-id":134},
{"name":"file_path","type":"string","field-id":100},
{"name":"file_format","type":"string","field-id":101},
{"name":"partition","type":{"type":"record","name":"r102","fields":[]},"field-id":102},
{"name":"record_count","type":"long","field-id":103},
{"name":"file_size_in_bytes","type":"long","field-id":104},
{"name":"column_sizes","type":["null",{"type":"array","items":{"type":"record","name":"k117_v118","fields":[{"name":"key","type":"int","field-id":117},{"name":"value","type":"long","field-id":118}]},"logicalType":"map"}],"default":null,"field-id":108},
{"name":"value_counts","type":["null",{"type":"array","items":{"type":"record","name":"k119_v120","fields":[{"name":"key","type":"int","field-id":119},{"name":"value","type":"long","field-id":120}]},"logicalType":"map"}],"default":null,"field-id":109},
{"name":"null_value_counts","type":["null",{"type":"array","items":{"type":"record","name":"k121_v122","fields":[{"name":"key","type":"int","field-id":121},{"name":"value","type":"long","field-id":122}]},"logicalType":"map"}],"default":null,"field-id":110},
{"name":"nan_value_counts","type":["null",{"type":"array","items":{"type":"record","name":"k138_v139","fields":[{"name":"key","type":"int","field-id":138},{"name":"value","type":"long","field-id":139}]},"logicalType":"map"}],"default":null,"field-id":137},
{"name":"lower_bounds","type":["null",{"type":"array","items":{"type":"record","name":"k126_v127","fields":[{"name":"key","type":"int","field-id":126},{"name":"value","type":"bytes","field-id":127}]},"logicalType":"map"}],"default":null,"field-id":125},
{"name":"upper_bounds","type":["null",{"type":"array","items":{"type":"record","name":"k129_v130","fields":[{"name":"key","type":"int","field-id":129},{"name":"value","type":"bytes","field-id":130}]},"logicalType":"map"}],"default":null,"field-id":128},
{"name":"key_metadata","type":["null","bytes"],"default":null,"field-id":131},
{"name":"split_offsets","type":["null",{"type":"array","items":"long","element-id":133}],"default":null,"field-id":132},
{"name":"equality_ids","type":["null",{"type":"array","items":"int","element-id":136}],"default":null,"field-id":135},
{"name":"sort_order_id","type":["null","int"],"default":null,"field-id":140},
{"name":"first_row_id","type":["null","long"],"default":null,"field-id":142},
{"name":"referenced_data_file","type":["null","string"],"default":null,"field-id":143},
{"name":"content_offset","type":["null","long"],"default":null,"field-id":144},
{"name":"content_size_in_bytes","type":["null","long"],"default":null,"field-id":145}
]},"field-id":2}
]}"#;

/// The Avro schema of a manifest list entry: the Iceberg v3
/// `manifest_file`.
pub const LIST_SCHEMA: &str = r#"{"type":"record","name":"manifest_file","fields":[
{"name":"manifest_path","type":"string","field-id":500},
{"name":"manifest_length","type":"long","field-id":501},
{"name":"partition_spec_id","type":"int","field-id":502},
{"name":"content","type":"int","field-id":517},
{"name":"sequence_number","type":"long","field-id":515},
{"name":"min_sequence_number","type":"long","field-id":516},
{"name":"added_snapshot_id","type":"long","field-id":503},
{"name":"added_files_count","type":"int","field-id":504},
{"name":"existing_files_count","type":"int","field-id":505},
{"name":"deleted_files_count","type":"int","field-id":506},
{"name":"added_rows_count","type":"long","field-id":512},
{"name":"existing_rows_count","type":"long","field-id":513},
{"name":"deleted_rows_count","type":"long","field-id":514},
{"name":"partitions","type":["null",{"type":"array","items":{"type":"record","name":"r508","fields":[{"name":"contains_null","type":"boolean","field-id":509},{"name":"contains_nan","type":["null","boolean"],"default":null,"field-id":518},{"name":"lower_bound","type":["null","bytes"],"default":null,"field-id":510},{"name":"upper_bound","type":["null","bytes"],"default":null,"field-id":511}]},"element-id":508}],"default":null,"field-id":507},
{"name":"key_metadata","type":["null","bytes"],"default":null,"field-id":519},
{"name":"first_row_id","type":["null","long"],"default":null,"field-id":520}
]}"#;

/// The Iceberg format version written.
pub const FORMAT_VERSION: &str = "3";

/// The partition spec of every exported table: none. A reader prunes
/// files by their timestamp bounds instead.
pub const PARTITION_SPEC: &str = "[]";

/// A snapshot's identity, as the manifests record it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// The snapshot id.
    pub id: i64,
    /// Its sequence number.
    pub sequence: i64,
    /// The first row id its files' rows take (row lineage).
    pub first_row_id: i64,
}

/// A data file of a snapshot: its location as a reader opens it, and
/// what the manifest knows about it.
#[derive(Clone, Debug)]
pub struct DataFile<'a> {
    /// The file's location URI.
    pub path: String,
    /// The file.
    pub meta: &'a FileMeta,
}

/// The manifest file of `snapshot`: every file as added by it, with
/// the table `schema` in the header.
#[must_use]
pub fn manifest(schema: &TableSchema, snapshot: Snapshot, files: &[DataFile<'_>]) -> Vec<u8> {
    let schema_json =
        serde_json::to_string(schema).unwrap_or_else(|e| unreachable!("a schema is JSON: {e}"));
    let schema_id = schema.schema_id.to_string();
    let entries: Vec<Value> = files.iter().map(|f| entry(schema, snapshot, f)).collect();
    avro::file(
        ENTRY_SCHEMA,
        &[
            ("schema", schema_json.as_bytes()),
            ("schema-id", schema_id.as_bytes()),
            ("partition-spec", PARTITION_SPEC.as_bytes()),
            ("partition-spec-id", b"0"),
            ("format-version", FORMAT_VERSION.as_bytes()),
            ("content", b"data"),
        ],
        &entries,
    )
}

/// The manifest list of `snapshot`: its one manifest at `path`, of
/// `length` bytes, holding `files` files and `rows` rows.
#[must_use]
pub fn manifest_list(
    path: &str,
    length: usize,
    snapshot: Snapshot,
    files: usize,
    rows: u64,
) -> Vec<u8> {
    let snapshot_id = snapshot.id.to_string();
    let sequence = snapshot.sequence.to_string();
    let entry = Value::Record(vec![
        Value::String(path.to_owned()),
        Value::Long(long(length)),
        Value::Int(0),
        Value::Int(0),
        Value::Long(snapshot.sequence),
        Value::Long(snapshot.sequence),
        Value::Long(snapshot.id),
        Value::Int(int(files)),
        Value::Int(0),
        Value::Int(0),
        Value::Long(long(rows)),
        Value::Long(0),
        Value::Long(0),
        Value::optional(Some(Value::Array(Vec::new()))),
        Value::optional(None),
        Value::optional(Some(Value::Long(snapshot.first_row_id))),
    ]);
    avro::file(
        LIST_SCHEMA,
        &[
            ("format-version", FORMAT_VERSION.as_bytes()),
            ("snapshot-id", snapshot_id.as_bytes()),
            ("sequence-number", sequence.as_bytes()),
            ("parent-snapshot-id", b"null"),
        ],
        &[entry],
    )
}

const STATUS_ADDED: i32 = 1;
const CONTENT_DATA: i32 = 0;

fn entry(schema: &TableSchema, snapshot: Snapshot, file: &DataFile<'_>) -> Value {
    let meta = file.meta;
    let rows = long(meta.row_count);
    let mut value_counts = Vec::new();
    let mut null_counts = Vec::new();
    let mut lower = Vec::new();
    let mut upper = Vec::new();
    if let Some(field) = schema.field(TIMESTAMP) {
        value_counts.push(pair(field.id, Value::Long(rows)));
        null_counts.push(pair(field.id, Value::Long(0)));
        lower.push(pair(
            field.id,
            Value::Bytes(meta.min_timestamp.as_nanos().to_le_bytes().to_vec()),
        ));
        upper.push(pair(
            field.id,
            Value::Bytes(meta.max_timestamp.as_nanos().to_le_bytes().to_vec()),
        ));
    }
    for stats in &meta.columns {
        let Some(field) = schema.field(&stats.name) else {
            continue;
        };
        let Some(ty) = Type::parse(&field.type_name) else {
            continue;
        };
        value_counts.push(pair(field.id, Value::Long(rows)));
        null_counts.push(pair(field.id, Value::Long(long(stats.null_count))));
        if let Some(bytes) = stats.min.as_ref().and_then(|s| bound(ty, s)) {
            lower.push(pair(field.id, Value::Bytes(bytes)));
        }
        if let Some(bytes) = stats.max.as_ref().and_then(|s| bound(ty, s)) {
            upper.push(pair(field.id, Value::Bytes(bytes)));
        }
    }
    let data_file = Value::Record(vec![
        Value::Int(CONTENT_DATA),
        Value::String(file.path.clone()),
        Value::String("PARQUET".into()),
        Value::Record(Vec::new()),
        Value::Long(rows),
        Value::Long(long(meta.size_bytes)),
        Value::optional(None),
        Value::optional(Some(Value::Array(value_counts))),
        Value::optional(Some(Value::Array(null_counts))),
        Value::optional(None),
        Value::optional(Some(Value::Array(lower))),
        Value::optional(Some(Value::Array(upper))),
        Value::optional(None),
        Value::optional(None),
        Value::optional(None),
        Value::optional(Some(Value::Int(0))),
        Value::optional(None),
        Value::optional(None),
        Value::optional(None),
        Value::optional(None),
    ]);
    Value::Record(vec![
        Value::Int(STATUS_ADDED),
        Value::optional(Some(Value::Long(snapshot.id))),
        Value::optional(Some(Value::Long(snapshot.sequence))),
        Value::optional(Some(Value::Long(snapshot.sequence))),
        data_file,
    ])
}

fn bound(ty: Type, scalar: &Scalar) -> Option<Vec<u8>> {
    ty.bound(scalar)
}

fn pair(id: i32, value: Value) -> Value {
    Value::Record(vec![Value::Int(id), value])
}

fn long(value: impl TryInto<i64>) -> i64 {
    value.try_into().unwrap_or(i64::MAX)
}

fn int(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use apache_avro::Reader;
    use apache_avro::types::Value as Read;
    use tessera_types::{
        ColumnStats, DatasetName, FileId, FileSource, Kind, Level, PartitionId, PoolId, Seq,
        TenantId, Timestamp, Window, WriterId,
    };

    use super::*;

    fn file(id: u128) -> FileMeta {
        FileMeta {
            file_id: FileId::from_u128(id),
            partition: PartitionId {
                tenant: TenantId::DEFAULT,
                dataset: DatasetName::new("prod").unwrap(),
                kind: Kind::Log,
                window: Window::containing(Timestamp::UNIX_EPOCH, Window::DEFAULT_LENGTH).unwrap(),
            },
            pool: PoolId::new("root").unwrap(),
            level: Level::L2,
            source: FileSource::Writer {
                writer: WriterId::from_u128(1),
                after: Seq::ZERO,
                through: Seq::new(1),
            },
            min_timestamp: Timestamp::from_nanos(100),
            max_timestamp: Timestamp::from_nanos(200),
            row_count: 3,
            size_bytes: 4_096,
            layout_epoch: 0,
            sort_key: vec![TIMESTAMP.into()],
            columns: vec![ColumnStats {
                name: "attr.code#i64".into(),
                min: Some(Scalar::Int(7)),
                max: Some(Scalar::Int(9)),
                null_count: 1,
                distinct: Some(2),
                quantiles: Vec::new(),
            }],
            indexes: Vec::new(),
        }
    }

    fn field<'a>(record: &'a [(String, Read)], name: &str) -> &'a Read {
        &record.iter().find(|(n, _)| n == name).unwrap().1
    }

    #[test]
    fn a_manifest_lists_each_file_with_its_counts_and_bounds() {
        let schema = TableSchema::of(
            Kind::Log,
            &BTreeSet::from(["attr.code#i64".to_owned()]),
            &BTreeMap::new(),
            None,
        );
        let meta = file(1);
        let snapshot = Snapshot {
            id: 5,
            sequence: 5,
            first_row_id: 0,
        };
        let bytes = manifest(
            &schema,
            snapshot,
            &[DataFile {
                path: "file:///d/a.parquet".into(),
                meta: &meta,
            }],
        );
        let reader = Reader::new(&bytes[..]).unwrap();
        assert_eq!(
            reader.user_metadata().get("format-version"),
            Some(&b"3".to_vec())
        );
        assert_eq!(
            reader.user_metadata().get("content"),
            Some(&b"data".to_vec())
        );
        let header: serde_json::Value =
            serde_json::from_slice(reader.user_metadata().get("schema").unwrap()).unwrap();
        assert_eq!(header["fields"][0]["name"], "timestamp");
        let rows: Vec<Read> = reader.map(Result::unwrap).collect();
        let Read::Record(entry) = &rows[0] else {
            panic!("{rows:?}");
        };
        assert_eq!(field(entry, "status"), &Read::Int(1));
        assert_eq!(
            field(entry, "snapshot_id"),
            &Read::Union(1, Box::new(Read::Long(5)))
        );
        let Read::Record(data) = field(entry, "data_file") else {
            panic!("{entry:?}");
        };
        assert_eq!(
            field(data, "file_path"),
            &Read::String("file:///d/a.parquet".into())
        );
        assert_eq!(field(data, "file_format"), &Read::String("PARQUET".into()));
        assert_eq!(field(data, "record_count"), &Read::Long(3));
        assert_eq!(field(data, "file_size_in_bytes"), &Read::Long(4_096));
        let code = schema.field("attr.code#i64").unwrap().id;
        let bounds = |name: &str| match field(data, name) {
            Read::Union(1, inner) => match &**inner {
                Read::Array(items) => items
                    .iter()
                    .map(|item| match item {
                        Read::Record(kv) => (field(kv, "key").clone(), field(kv, "value").clone()),
                        other => panic!("{other:?}"),
                    })
                    .collect::<Vec<_>>(),
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        };
        assert_eq!(
            bounds("lower_bounds"),
            vec![
                (Read::Int(1), Read::Bytes(100i64.to_le_bytes().to_vec())),
                (Read::Int(code), Read::Bytes(7i64.to_le_bytes().to_vec())),
            ]
        );
        assert_eq!(
            bounds("upper_bounds"),
            vec![
                (Read::Int(1), Read::Bytes(200i64.to_le_bytes().to_vec())),
                (Read::Int(code), Read::Bytes(9i64.to_le_bytes().to_vec())),
            ]
        );
        assert_eq!(
            bounds("null_value_counts"),
            vec![
                (Read::Int(1), Read::Long(0)),
                (Read::Int(code), Read::Long(1))
            ]
        );
    }

    #[test]
    fn a_manifest_list_names_the_manifest_and_its_totals() {
        let snapshot = Snapshot {
            id: 2,
            sequence: 2,
            first_row_id: 10,
        };
        let bytes = manifest_list("file:///t/metadata/manifest-2.avro", 1_234, snapshot, 4, 40);
        let reader = Reader::new(&bytes[..]).unwrap();
        assert_eq!(
            reader.user_metadata().get("snapshot-id"),
            Some(&b"2".to_vec())
        );
        let rows: Vec<Read> = reader.map(Result::unwrap).collect();
        let Read::Record(entry) = &rows[0] else {
            panic!("{rows:?}");
        };
        assert_eq!(
            field(entry, "manifest_path"),
            &Read::String("file:///t/metadata/manifest-2.avro".into())
        );
        assert_eq!(field(entry, "manifest_length"), &Read::Long(1_234));
        assert_eq!(field(entry, "added_snapshot_id"), &Read::Long(2));
        assert_eq!(field(entry, "added_files_count"), &Read::Int(4));
        assert_eq!(field(entry, "added_rows_count"), &Read::Long(40));
        assert_eq!(
            field(entry, "first_row_id"),
            &Read::Union(1, Box::new(Read::Long(10)))
        );
    }
}
