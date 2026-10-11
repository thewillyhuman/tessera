// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The schema of an exported table (OP-9): every column of the kind's
//! base schema and every dedicated column its files hold, as Iceberg
//! typed columns. A field keeps its id across exports, so that a
//! reader that cached one export's schema reads the next's files
//! right, and the files, which carry no field ids, are mapped by name.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::{DataType, TimeUnit};
use serde::{Deserialize, Serialize};
use tessera_types::Kind;
use tessera_types::event::{self, CLASS_BOOL, CLASS_FLOAT, CLASS_INT, Scalar, sub_class};

/// An Iceberg primitive type an exported column has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Type {
    /// `boolean`.
    Boolean,
    /// A 32-bit `int`.
    Int,
    /// A 64-bit `long`.
    Long,
    /// A 64-bit `double`.
    Double,
    /// A UTF-8 `string`.
    String,
    /// A `timestamp_ns`: nanoseconds since the epoch, no zone, as the
    /// files store event time.
    TimestampNs,
    /// `fixed[n]`: `n` bytes.
    Fixed(usize),
}

impl Type {
    /// The type's name in table metadata.
    #[must_use]
    pub fn name(self) -> String {
        match self {
            Type::Boolean => "boolean".into(),
            Type::Int => "int".into(),
            Type::Long => "long".into(),
            Type::Double => "double".into(),
            Type::String => "string".into(),
            Type::TimestampNs => "timestamp_ns".into(),
            Type::Fixed(n) => format!("fixed[{n}]"),
        }
    }

    /// The type named `name`, if the export writes it.
    #[must_use]
    pub fn parse(name: &str) -> Option<Type> {
        match name {
            "boolean" => Some(Type::Boolean),
            "int" => Some(Type::Int),
            "long" => Some(Type::Long),
            "double" => Some(Type::Double),
            "string" => Some(Type::String),
            "timestamp_ns" => Some(Type::TimestampNs),
            _ => name
                .strip_prefix("fixed[")
                .and_then(|n| n.strip_suffix(']'))
                .and_then(|n| n.parse().ok())
                .map(Type::Fixed),
        }
    }

    /// `scalar` in Iceberg's single-value binary serialization as a
    /// value of this type, which is how a file's bounds are written;
    /// none when the scalar is not of the type.
    #[must_use]
    pub fn bound(self, scalar: &Scalar) -> Option<Vec<u8>> {
        match (self, scalar) {
            (Type::Long | Type::TimestampNs, Scalar::Int(i)) => Some(i.to_le_bytes().to_vec()),
            (Type::Int, Scalar::Int(i)) => i32::try_from(*i).ok().map(|i| i.to_le_bytes().to_vec()),
            (Type::Double, Scalar::Float(f)) => Some(f.to_le_bytes().to_vec()),
            (Type::String, Scalar::Text(t)) => Some(t.as_bytes().to_vec()),
            (Type::Boolean, Scalar::Bool(b)) => Some(vec![u8::from(*b)]),
            _ => None,
        }
    }

    fn of_arrow(data_type: &DataType) -> Type {
        match data_type {
            DataType::Boolean => Type::Boolean,
            DataType::Int32 => Type::Int,
            DataType::Int64 => Type::Long,
            DataType::Float64 => Type::Double,
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Type::TimestampNs,
            DataType::FixedSizeBinary(n) => Type::Fixed(usize::try_from(*n).unwrap_or(0)),
            _ => Type::String,
        }
    }

    /// The type of a dedicated column: by its class suffix (DM-5), by
    /// the one well-known column compaction adds, else by a value seen
    /// in it, text when none was.
    fn of_column(name: &str, sample: Option<&Scalar>) -> Type {
        if let Some((_, class)) = sub_class(name) {
            return match class {
                CLASS_INT => Type::Long,
                CLASS_FLOAT => Type::Double,
                CLASS_BOOL => Type::Boolean,
                _ => Type::String,
            };
        }
        if name == event::PATTERN_ID {
            return Type::Long;
        }
        match sample {
            Some(Scalar::Int(_)) => Type::Long,
            Some(Scalar::Float(_)) => Type::Double,
            Some(Scalar::Bool(_)) => Type::Boolean,
            Some(Scalar::Text(_)) | None => Type::String,
        }
    }
}

/// A field of the table schema, as table metadata writes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    /// The field id, never reused.
    pub id: i32,
    /// The column name, as the files spell it.
    pub name: String,
    /// Whether every row has a value.
    pub required: bool,
    /// The type's name.
    #[serde(rename = "type")]
    pub type_name: String,
}

/// The table schema, as table metadata writes it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    /// Always `struct`.
    #[serde(rename = "type")]
    pub kind: String,
    /// The schema id, moved on when the fields change.
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    /// The fields, in id order.
    pub fields: Vec<Field>,
}

impl TableSchema {
    /// The schema of a table of `kind` whose files hold `columns`, with
    /// `samples` a value seen in a column where one was, built on
    /// `previous`: its fields keep their ids and their place, a column
    /// no file holds any more included (it reads as null), new columns
    /// take the next ids in name order, and the schema id moves on only
    /// when a field was added.
    #[must_use]
    pub fn of(
        kind: Kind,
        columns: &BTreeSet<String>,
        samples: &BTreeMap<String, Scalar>,
        previous: Option<&TableSchema>,
    ) -> TableSchema {
        let mut fields: Vec<Field> = previous.map(|p| p.fields.clone()).unwrap_or_default();
        let mut next_id = fields.iter().map(|f| f.id).max().unwrap_or(0) + 1;
        let base = event::schema(kind);
        let base_names: Vec<&str> = base.fields().iter().map(|f| f.name().as_str()).collect();
        let dedicated = columns
            .iter()
            .map(String::as_str)
            .filter(|c| !base_names.contains(c));
        let names: Vec<&str> = base_names.iter().copied().chain(dedicated).collect();
        for name in names {
            if fields.iter().any(|f| f.name == name) {
                continue;
            }
            let (type_name, required) = match base.field_with_name(name) {
                Ok(field) => (Type::of_arrow(field.data_type()), !field.is_nullable()),
                Err(_) => (Type::of_column(name, samples.get(name)), false),
            };
            fields.push(Field {
                id: next_id,
                name: name.to_owned(),
                required,
                type_name: type_name.name(),
            });
            next_id += 1;
        }
        let schema_id = match previous {
            Some(p) if p.fields == fields => p.schema_id,
            Some(p) => p.schema_id + 1,
            None => 0,
        };
        TableSchema {
            kind: "struct".into(),
            schema_id,
            fields,
        }
    }

    /// The highest field id given.
    #[must_use]
    pub fn last_column_id(&self) -> i32 {
        self.fields.iter().map(|f| f.id).max().unwrap_or(0)
    }

    /// The field named `name`.
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// The name mapping a reader of files without field ids uses, the
    /// `schema.name-mapping.default` property: each field by its name.
    #[must_use]
    pub fn name_mapping(&self) -> String {
        let mapping: Vec<serde_json::Value> = self
            .fields
            .iter()
            .map(|f| serde_json::json!({ "field-id": f.id, "names": [f.name] }))
            .collect();
        serde_json::to_string(&mapping).unwrap_or_else(|e| unreachable!("a mapping is JSON: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| (*n).to_owned()).collect()
    }

    #[test]
    fn base_columns_come_first_with_their_types_and_dedicated_ones_by_class() {
        let samples = BTreeMap::from([("attr.plain".to_owned(), Scalar::Float(1.5))]);
        let schema = TableSchema::of(
            Kind::Log,
            &columns(&["attr.code#i64", "attr.ok#bool", "attr.plain", "pattern_id"]),
            &samples,
            None,
        );
        assert_eq!(schema.schema_id, 0);
        let first = &schema.fields[0];
        assert_eq!(
            (
                first.id,
                first.name.as_str(),
                first.required,
                first.type_name.as_str()
            ),
            (1, "timestamp", true, "timestamp_ns")
        );
        let by_name = |name: &str| schema.field(name).map(|f| f.type_name.clone()).unwrap();
        assert_eq!(by_name("event_id"), "fixed[16]");
        assert_eq!(by_name("severity_number"), "int");
        assert_eq!(by_name("sample_rate"), "double");
        assert_eq!(by_name("_overflow"), "string");
        assert_eq!(by_name("attr.code#i64"), "long");
        assert_eq!(by_name("attr.ok#bool"), "boolean");
        assert_eq!(by_name("attr.plain"), "double");
        assert_eq!(by_name("pattern_id"), "long");
        assert!(!schema.field("body").unwrap().required);
        let ids: Vec<i32> = schema.fields.iter().map(|f| f.id).collect();
        assert_eq!(
            ids,
            (1..=i32::try_from(ids.len()).unwrap()).collect::<Vec<_>>()
        );
        assert!(
            schema
                .name_mapping()
                .contains(r#"{"field-id":1,"names":["timestamp"]}"#)
        );
    }

    #[test]
    fn a_later_schema_keeps_ids_and_moves_on_only_when_a_field_is_added() {
        let first = TableSchema::of(Kind::Log, &columns(&["attr.b#str"]), &BTreeMap::new(), None);
        let same = TableSchema::of(
            Kind::Log,
            &columns(&["attr.b#str"]),
            &BTreeMap::new(),
            Some(&first),
        );
        assert_eq!(same, first);
        let fewer = TableSchema::of(Kind::Log, &columns(&[]), &BTreeMap::new(), Some(&first));
        assert_eq!(
            fewer, first,
            "a column no file holds stays, readable as null"
        );
        let more = TableSchema::of(
            Kind::Log,
            &columns(&["attr.a#str", "attr.b#str"]),
            &BTreeMap::new(),
            Some(&first),
        );
        assert_eq!(more.schema_id, 1);
        assert_eq!(
            more.field("attr.b#str").unwrap().id,
            first.field("attr.b#str").unwrap().id
        );
        assert_eq!(
            more.field("attr.a#str").unwrap().id,
            first.last_column_id() + 1
        );
        assert_eq!(more.last_column_id(), first.last_column_id() + 1);
    }

    #[test]
    fn bounds_follow_the_single_value_serialization() {
        assert_eq!(
            Type::Long.bound(&Scalar::Int(1)),
            Some(1i64.to_le_bytes().to_vec())
        );
        assert_eq!(
            Type::TimestampNs.bound(&Scalar::Int(-1)),
            Some((-1i64).to_le_bytes().to_vec())
        );
        assert_eq!(
            Type::Int.bound(&Scalar::Int(7)),
            Some(7i32.to_le_bytes().to_vec())
        );
        assert_eq!(Type::Int.bound(&Scalar::Int(i64::MAX)), None);
        assert_eq!(
            Type::Double.bound(&Scalar::Float(0.5)),
            Some(0.5f64.to_le_bytes().to_vec())
        );
        assert_eq!(
            Type::String.bound(&Scalar::Text("ab".into())),
            Some(b"ab".to_vec())
        );
        assert_eq!(Type::Boolean.bound(&Scalar::Bool(true)), Some(vec![1]));
        assert_eq!(Type::String.bound(&Scalar::Int(1)), None);
        for name in [
            "boolean",
            "int",
            "long",
            "double",
            "string",
            "timestamp_ns",
            "fixed[16]",
        ] {
            assert_eq!(Type::parse(name).map(Type::name).as_deref(), Some(name));
        }
        assert_eq!(Type::parse("uuid"), None);
    }
}
