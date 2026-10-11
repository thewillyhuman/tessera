// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Column promotion (section 8.2): when a file is written, an attribute
//! path gets a dedicated column when it is well known, when at least a
//! hundredth of the rows carry it, or when the layout asks for it (the
//! advisor's hot set and the sort key). At most [`MAX_DEDICATED`] paths
//! are promoted; the rest stay in `_overflow`, which keeps every
//! attribute either way so that a reader without the column loses
//! nothing (AD-3, AD-4).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use serde_json::Value;
use tessera_kernels::variant;
use tessera_types::event::OVERFLOW;

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
    /// The column type most of the values fit: integers as `Int64`,
    /// any float as `Float64`, booleans as `Boolean`, text otherwise.
    fn data_type(&self) -> DataType {
        let numbers = self.ints + self.floats;
        if self.texts == 0 && self.bools == 0 && numbers > 0 {
            if self.floats == 0 {
                DataType::Int64
            } else {
                DataType::Float64
            }
        } else if self.texts == 0 && numbers == 0 && self.bools > 0 {
            DataType::Boolean
        } else {
            DataType::Utf8
        }
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
                _ => entry.texts += 1,
            }
        }
    }
    let total = rows.num_rows() as f64;
    let existing = |path: &str| rows.schema().field_with_name(path).is_ok();
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
    if chosen.is_empty() {
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
        let data_type = seen.get(path).map_or(DataType::Utf8, Seen::data_type);
        let column = variant::extract(overflow, path, &data_type)?;
        fields.push(Field::new(path, data_type, true));
        columns.push(column);
    }
    let promoted = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    Ok((promoted, chosen))
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
}
