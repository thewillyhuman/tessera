// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Column promotion (section 8.2): when a file is written, an attribute
//! path gets a dedicated column when it is well known, when at least a
//! hundredth of the rows carry it, or when the layout asks for it (the
//! advisor's hot set and the sort key). At most [`MAX_DEDICATED`] paths
//! are promoted; the rest stay in `_overflow`, which keeps every
//! attribute either way so that a reader without the column loses
//! nothing (AD-3, AD-4). A path seen with values of several types gets
//! a column per type class, `attr.code#i64` and `attr.code#str`, each
//! null where the value is of another class (DM-5), so that a number
//! and its text stay apart.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::Value;
use tessera_kernels::variant;
use tessera_types::event::{
    CLASS_BOOL, CLASS_FLOAT, CLASS_INT, CLASS_TEXT, OVERFLOW, SCOPES, logical_path, sub_column,
};

use crate::CompactError;

/// Dedicated columns per file at most (AD-3).
pub const MAX_DEDICATED: usize = 1024;
/// The share of rows a path must appear in to earn a column.
pub const DENSITY: f64 = 0.01;
/// Paths promoted whenever present, being the well-known columns of
/// section 8.5's default sort keys.
pub const WELL_KNOWN: [&str; 1] = ["resource.service.name"];

/// What was seen of one path across the rows.
#[derive(Default)]
struct Seen {
    rows: usize,
    ints: usize,
    floats: usize,
    bools: usize,
    texts: usize,
}

impl Seen {
    /// The type classes the values fall in, each with its column type:
    /// numbers as `Int64`, or `Float64` when any is a float; text as
    /// `Utf8`; booleans as `Boolean`. One class makes a plain column,
    /// several make a sub-column each (DM-5).
    fn classes(&self) -> Vec<(&'static str, DataType)> {
        let mut classes = Vec::new();
        if self.ints + self.floats > 0 {
            classes.push(if self.floats == 0 {
                (CLASS_INT, DataType::Int64)
            } else {
                (CLASS_FLOAT, DataType::Float64)
            });
        }
        if self.texts > 0 {
            classes.push((CLASS_TEXT, DataType::Utf8));
        }
        if self.bools > 0 {
            classes.push((CLASS_BOOL, DataType::Boolean));
        }
        classes
    }
}

/// `rows` with a column per promoted path appended, and the paths.
/// `wanted` are promoted whatever their density, null where absent;
/// the others qualify by [`DENSITY`] or by being well known.
///
/// # Errors
///
/// If a column cannot be built.
#[allow(clippy::cast_precision_loss)]
pub fn promote(
    rows: &RecordBatch,
    wanted: &[String],
    cap: usize,
) -> Result<(RecordBatch, Vec<String>), CompactError> {
    let Some(overflow) = rows
        .column_by_name(OVERFLOW)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
    else {
        return Ok((rows.clone(), Vec::new()));
    };
    let mut seen: BTreeMap<String, Seen> = BTreeMap::new();
    for i in 0..overflow.len() {
        if overflow.is_null(i) {
            continue;
        }
        let Ok(Value::Object(object)) = serde_json::from_str::<Value>(overflow.value(i)) else {
            continue;
        };
        for (path, value) in object {
            let entry = seen.entry(path).or_default();
            entry.rows += 1;
            match value {
                Value::Number(n) if n.is_i64() || n.is_u64() => entry.ints += 1,
                Value::Number(_) => entry.floats += 1,
                Value::Bool(_) => entry.bools += 1,
                Value::Null => {}
                _ => entry.texts += 1,
            }
        }
    }
    let total = rows.num_rows() as f64;
    let existing = |path: &str| {
        rows.schema()
            .fields()
            .iter()
            .any(|f| logical_path(f.name()) == path)
    };
    let mut chosen: Vec<String> = wanted.iter().filter(|p| !existing(p)).cloned().collect();
    let mut dense: Vec<(&String, &Seen)> = seen
        .iter()
        .filter(|(path, s)| {
            !chosen.contains(path)
                && !existing(path)
                && (WELL_KNOWN.contains(&path.as_str())
                    || (total > 0.0 && (s.rows as f64) / total >= DENSITY))
        })
        .collect();
    dense.sort_by(|a, b| b.1.rows.cmp(&a.1.rows).then_with(|| a.0.cmp(b.0)));
    chosen.extend(dense.into_iter().map(|(path, _)| path.clone()));
    chosen.dedup();
    chosen.truncate(cap);
    // Attributes with a dedicated column, new or from an earlier epoch,
    // leave `_overflow`: stored once, read back merged (AD-4).
    let stripped: Vec<String> = chosen
        .iter()
        .cloned()
        .chain(
            rows.schema()
                .fields()
                .iter()
                .map(|f| logical_path(f.name()).to_owned())
                .filter(|name| SCOPES.iter().any(|scope| name.starts_with(scope))),
        )
        .filter(|path| seen.contains_key(path))
        .collect();
    if chosen.is_empty() && stripped.is_empty() {
        return Ok((rows.clone(), Vec::new()));
    }
    let mut fields: Vec<Field> = rows
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns: Vec<ArrayRef> = rows.columns().to_vec();
    for path in &chosen {
        let classes = seen.get(path).map(Seen::classes).unwrap_or_default();
        match classes.as_slice() {
            [] | [_] => {
                let data_type = classes
                    .first()
                    .map_or(DataType::Utf8, |(_, data_type)| data_type.clone());
                fields.push(Field::new(path, data_type.clone(), true));
                columns.push(variant::extract(overflow, path, &data_type)?);
            }
            several => {
                for (class, data_type) in several {
                    fields.push(Field::new(sub_column(path, class), data_type.clone(), true));
                    columns.push(variant::extract_class(overflow, path, class)?);
                }
            }
        }
    }
    if let Ok(index) = rows.schema().index_of(OVERFLOW) {
        columns[index] = without(overflow, &stripped);
    }
    let promoted = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    Ok((promoted, chosen))
}

/// `overflow` without the attributes at `paths`; a row left with none
/// becomes null.
fn without(overflow: &StringArray, paths: &[String]) -> ArrayRef {
    let stripped: StringArray = (0..overflow.len())
        .map(|i| {
            if overflow.is_null(i) {
                return None;
            }
            let Ok(Value::Object(mut object)) = serde_json::from_str::<Value>(overflow.value(i))
            else {
                return Some(overflow.value(i).to_owned());
            };
            object.retain(|path, _| !paths.contains(path));
            if object.is_empty() {
                None
            } else {
                Some(Value::Object(object).to_string())
            }
        })
        .collect();
    Arc::new(stripped)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::array::Int64Array;
    use tessera_types::event::Scalar;
    use tessera_types::{DatasetName, EventBatch, LogRow, TenantId, Timestamp};

    use super::*;

    fn rows() -> RecordBatch {
        let logs: Vec<LogRow> = (0..200u8)
            .map(|i| {
                let mut attributes = BTreeMap::from([
                    ("attr.host".to_owned(), Scalar::Text(format!("h{}", i % 3))),
                    ("attr.n".to_owned(), Scalar::Int(i64::from(i))),
                ]);
                if i == 7 {
                    attributes.insert("attr.rare".into(), Scalar::Bool(true));
                }
                if i % 50 == 0 {
                    attributes.insert("resource.service.name".into(), Scalar::Text("api".into()));
                }
                LogRow {
                    timestamp: Timestamp::from_nanos(i64::from(i)),
                    event_id: [i; 16],
                    body: Some("x".into()),
                    attributes,
                    ..LogRow::default()
                }
            })
            .collect();
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &logs)
            .unwrap()
            .rows()
            .clone()
    }

    #[test]
    fn promoted_attributes_leave_the_overflow_and_read_back_merged() {
        let (promoted, paths) = promote(&rows(), &[], MAX_DEDICATED).unwrap();
        assert!(paths.contains(&"attr.host".to_owned()), "{paths:?}");
        let overflow = promoted
            .column_by_name(OVERFLOW)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        // Row 0 had host, n and the service, all promoted: nothing is left.
        assert!(overflow.is_null(0), "stored once, in their columns");
        // Row 7 also had the rare attribute, which stays.
        let row7: serde_json::Value = serde_json::from_str(overflow.value(7)).unwrap();
        assert_eq!(row7, serde_json::json!({ "attr.rare": true }));
        let merged = tessera_kernels::attributes_column(&promoted).unwrap();
        let back: serde_json::Value = serde_json::from_str(merged.value(0)).unwrap();
        assert_eq!(back["attr.host"], "h0");
        assert_eq!(back["attr.n"], 0);
        // Rows whose every attribute was promoted carry no overflow at all.
        let again = promote(&promoted, &[], MAX_DEDICATED).unwrap().0;
        assert_eq!(
            again.column_by_name(OVERFLOW).unwrap().null_count(),
            overflow.null_count(),
            "a second pass changes nothing"
        );
    }

    #[test]
    fn dense_wanted_and_well_known_paths_become_typed_columns() {
        let (promoted, paths) = promote(&rows(), &["attr.want".to_owned()], MAX_DEDICATED).unwrap();
        assert_eq!(
            paths,
            ["attr.want", "attr.host", "attr.n", "resource.service.name"],
            "wanted first, then by density, well known whatever its density"
        );
        let schema = promoted.schema();
        assert_eq!(
            schema.field_with_name("attr.n").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("attr.host").unwrap().data_type(),
            &DataType::Utf8
        );
        assert!(
            schema.field_with_name("attr.rare").is_err(),
            "one row in two hundred"
        );
        let want = promoted.column_by_name("attr.want").unwrap();
        assert_eq!(want.null_count(), 200, "wanted but absent: all null");
        let n = promoted
            .column_by_name("attr.n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(n.value(5), 5);
        assert!(
            promoted.column_by_name(OVERFLOW).is_some(),
            "the variant column stays"
        );

        let (capped, paths) = promote(&rows(), &[], 1).unwrap();
        assert_eq!(paths, ["attr.host"], "the densest within the cap");
        assert_eq!(capped.num_columns(), rows().num_columns() + 1);
    }

    /// DM-5: `attr.code` is a number on even rows and text on odd ones.
    #[test]
    fn a_path_seen_with_several_types_gets_a_column_per_class() {
        let logs: Vec<LogRow> = (0..100u8)
            .map(|i| LogRow {
                timestamp: Timestamp::from_nanos(i64::from(i)),
                event_id: [i; 16],
                attributes: BTreeMap::from([(
                    "attr.code".to_owned(),
                    if i % 2 == 0 {
                        Scalar::Int(500)
                    } else {
                        Scalar::Text("x".into())
                    },
                )]),
                ..LogRow::default()
            })
            .collect();
        let rows =
            EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &logs)
                .unwrap()
                .rows()
                .clone();
        let (promoted, paths) = promote(&rows, &[], MAX_DEDICATED).unwrap();
        assert_eq!(paths, ["attr.code"], "the logical path");
        let schema = promoted.schema();
        assert!(
            schema.field_with_name("attr.code").is_err(),
            "no plain column"
        );
        assert_eq!(
            schema.field_with_name("attr.code#i64").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("attr.code#str").unwrap().data_type(),
            &DataType::Utf8
        );
        let ints = promoted
            .column_by_name("attr.code#i64")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ints.value(0), 500);
        assert!(ints.is_null(1), "text rows are null in the number column");
        assert_eq!(
            promoted.column_by_name(OVERFLOW).unwrap().null_count(),
            100,
            "stored once, in the sub-columns"
        );
        let merged = tessera_kernels::attributes_column(&promoted).unwrap();
        assert_eq!(merged.value(0), r#"{"attr.code":500}"#);
        assert_eq!(merged.value(1), r#"{"attr.code":"x"}"#);
        let (again, paths) = promote(&promoted, &[], MAX_DEDICATED).unwrap();
        assert!(
            paths.is_empty(),
            "a path with sub-columns is promoted already"
        );
        assert_eq!(
            again.schema().fields().len(),
            promoted.schema().fields().len()
        );
    }
}
