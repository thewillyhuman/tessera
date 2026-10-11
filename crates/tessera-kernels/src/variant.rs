// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Variant access (section 10.6): reading an attribute path as a typed
//! column.
//!
//! Until the advisor promotes a path to a dedicated column (M8), every
//! attribute lives in `_overflow` as a JSON object of path to scalar
//! (ST-13 fallback). This kernel reads one path out of that column as the
//! type the query asks for; values that do not fit the type are null, as
//! a cast of a dynamic value is in SQL.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use serde_json::Value;
use tessera_types::event::{EXTRACTED, LABEL_SCOPES, OVERFLOW, SEVERITY_TEXT, label_name};

use crate::KernelError;

/// Reads `path` from each JSON object of `overflow` as `data_type`.
///
/// # Errors
///
/// `Unsupported` for a type a variant cannot be read as.
pub fn extract(
    overflow: &StringArray,
    path: &str,
    data_type: &DataType,
) -> Result<ArrayRef, KernelError> {
    let values = (0..overflow.len()).map(|i| {
        if overflow.is_null(i) {
            return None;
        }
        serde_json::from_str::<Value>(overflow.value(i))
            .ok()
            .and_then(|object| object.get(path).cloned())
    });
    typed(values, data_type, &format!("attribute {path}"))
}

/// The label `name` of every row (QL-19, QL-20): a label a parser stage
/// derived (`_extracted`), else the attribute whose path maps to the
/// name, the more specific scope first, from the dedicated column when
/// the batch has one and from `_overflow` otherwise, else a well-known
/// column under its label name (`level` for the severity text).
///
/// # Errors
///
/// `Unsupported` for a type a label cannot be read as.
pub fn label(
    rows: &RecordBatch,
    name: &str,
    data_type: &DataType,
) -> Result<ArrayRef, KernelError> {
    let text = |column: &str| {
        rows.column_by_name(column)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
    };
    let (overflow, extracted, severity_text) =
        (text(OVERFLOW), text(EXTRACTED), text(SEVERITY_TEXT));
    // The dedicated column of the attribute under this label, by scope.
    let schema = rows.schema();
    let dedicated: Option<&ArrayRef> = LABEL_SCOPES.iter().find_map(|scope| {
        schema
            .fields()
            .iter()
            .position(|f| f.name().starts_with(scope) && label_name(f.name()) == name)
            .map(|i| rows.column(i))
    });
    let values = (0..rows.num_rows()).map(|i| {
        let from_extracted = extracted
            .filter(|e| !e.is_null(i))
            .and_then(|e| serde_json::from_str::<Value>(e.value(i)).ok())
            .and_then(|object| object.get(name).cloned());
        if from_extracted.is_some() {
            return from_extracted;
        }
        if let Some(value) = dedicated.and_then(|column| cell_json(column, i)) {
            return Some(value);
        }
        let from_attributes = overflow
            .filter(|o| !o.is_null(i))
            .and_then(|o| serde_json::from_str::<Value>(o.value(i)).ok())
            .and_then(|object| {
                let object = object.as_object()?;
                LABEL_SCOPES.iter().find_map(|scope| {
                    object
                        .iter()
                        .find(|(path, _)| path.starts_with(scope) && label_name(path) == name)
                        .map(|(_, v)| v.clone())
                })
            });
        from_attributes.or_else(|| match name {
            "level" => severity_text
                .filter(|s| !s.is_null(i))
                .map(|s| Value::String(s.value(i).to_owned())),
            _ => None,
        })
    });
    typed(values, data_type, name)
}

/// The value at `row` of a dedicated attribute column, as JSON.
fn cell_json(column: &ArrayRef, row: usize) -> Option<Value> {
    if column.is_null(row) {
        return None;
    }
    let any = column.as_any();
    match column.data_type() {
        DataType::Utf8 => any
            .downcast_ref::<StringArray>()
            .map(|a| Value::String(a.value(row).to_owned())),
        DataType::Int64 => any
            .downcast_ref::<Int64Array>()
            .map(|a| Value::from(a.value(row))),
        DataType::Float64 => any
            .downcast_ref::<Float64Array>()
            .and_then(|a| serde_json::Number::from_f64(a.value(row)))
            .map(Value::Number),
        DataType::Boolean => any
            .downcast_ref::<BooleanArray>()
            .map(|a| Value::Bool(a.value(row))),
        _ => None,
    }
}

/// Every label of one row by name: derived labels first, then attributes
/// under their label names with the more specific scope winning, then
/// `level` from the severity text.
#[must_use]
pub fn row_labels(
    overflow: Option<&str>,
    extracted: Option<&str>,
    severity_text: Option<&str>,
) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    if let Some(level) = severity_text {
        labels.insert("level".to_owned(), level.to_owned());
    }
    if let Some(Value::Object(object)) = overflow.and_then(|o| serde_json::from_str(o).ok()) {
        // Later scopes must not override earlier ones: insert in reverse.
        for scope in LABEL_SCOPES.iter().rev() {
            for (path, value) in &object {
                if path.starts_with(scope) {
                    if let Some(text) = as_text(value.clone()) {
                        labels.insert(label_name(path), text);
                    }
                }
            }
        }
    }
    if let Some(Value::Object(object)) = extracted.and_then(|e| serde_json::from_str(e).ok()) {
        for (name, value) in object {
            if let Some(text) = as_text(value) {
                labels.insert(name, text);
            }
        }
    }
    labels
}

/// `values` as an array of `data_type`.
fn typed(
    values: impl Iterator<Item = Option<Value>>,
    data_type: &DataType,
    what: &str,
) -> Result<ArrayRef, KernelError> {
    Ok(match data_type {
        DataType::Utf8 => Arc::new(values.map(|v| v.and_then(as_text)).collect::<StringArray>()),
        DataType::Int64 => Arc::new(values.map(|v| v.and_then(as_int)).collect::<Int64Array>()),
        DataType::Float64 => Arc::new(
            values
                .map(|v| v.and_then(as_float))
                .collect::<Float64Array>(),
        ),
        DataType::Boolean => Arc::new(
            values
                .map(|v| v.and_then(|v| v.as_bool()))
                .collect::<BooleanArray>(),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Arc::new(
            values
                .map(|v| v.and_then(as_int))
                .collect::<TimestampNanosecondArray>(),
        ),
        other => {
            return Err(KernelError::Unsupported(format!(
                "cannot read {what} as {other}"
            )));
        }
    })
}

fn as_text(value: Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s),
        other => Some(other.to_string()),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn truncated(value: f64) -> i64 {
    value as i64
}

fn as_int(value: Value) -> Option<i64> {
    match value {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(truncated)),
        Value::String(s) => s.parse().ok(),
        Value::Bool(b) => Some(i64::from(b)),
        _ => None,
    }
}

fn as_float(value: Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn overflow() -> StringArray {
        StringArray::from(vec![
            Some(r#"{"attr.n":2,"attr.method":"GET","attr.ok":true}"#),
            Some(r#"{"attr.n":"7","attr.method":5}"#),
            None,
            Some("not json"),
        ])
    }

    #[test]
    fn reads_a_path_as_text() {
        let array = extract(&overflow(), "attr.method", &DataType::Utf8).unwrap();
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(array.value(0), "GET");
        assert_eq!(array.value(1), "5");
        assert!(array.is_null(2));
        assert!(array.is_null(3));
    }

    #[test]
    fn reads_a_path_as_a_number_with_coercion() {
        let array = extract(&overflow(), "attr.n", &DataType::Int64).unwrap();
        let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(array.value(0), 2);
        assert_eq!(array.value(1), 7);
        let floats = extract(&overflow(), "attr.n", &DataType::Float64).unwrap();
        let floats = floats.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((floats.value(0) - 2.0).abs() < f64::EPSILON);
        let missing = extract(&overflow(), "attr.absent", &DataType::Int64).unwrap();
        assert_eq!(missing.null_count(), 4);
    }

    #[test]
    fn reads_booleans_and_rejects_other_types() {
        let array = extract(&overflow(), "attr.ok", &DataType::Boolean).unwrap();
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(array.value(0));
        assert!(array.is_null(1));
        assert!(matches!(
            extract(&overflow(), "attr.ok", &DataType::Binary),
            Err(KernelError::Unsupported(_))
        ));
    }

    /// A batch of the given text columns, each three rows.
    fn batch_of(columns: &[(&str, StringArray)]) -> RecordBatch {
        use arrow::datatypes::{Field, Schema};

        RecordBatch::try_new(
            Arc::new(Schema::new(
                columns
                    .iter()
                    .map(|(name, _)| Field::new(*name, DataType::Utf8, true))
                    .collect::<Vec<_>>(),
            )),
            columns
                .iter()
                .map(|(_, array)| Arc::new(array.clone()) as ArrayRef)
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn labels_resolve_by_scope_then_well_known_columns() {
        let overflow = StringArray::from(vec![
            Some(
                r#"{"resource.service.name":"api","attr.service.name":"shadow","attr.http.method":"GET"}"#,
            ),
            Some(r#"{"scope.service.name":"lib"}"#),
            None,
        ]);
        let severity = StringArray::from(vec![Some("ERROR"), None, Some("INFO")]);
        let rows = batch_of(&[
            (OVERFLOW, overflow.clone()),
            (SEVERITY_TEXT, severity.clone()),
        ]);
        let service = label(&rows, "service_name", &DataType::Utf8).unwrap();
        let service = service.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            service.value(0),
            "shadow",
            "attr wins over resource (QL-20)"
        );
        assert_eq!(service.value(1), "lib");
        assert!(service.is_null(2));
        let method = label(&rows, "http_method", &DataType::Utf8).unwrap();
        assert_eq!(
            method
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "GET"
        );
        let level = label(&rows, "level", &DataType::Utf8).unwrap();
        let level = level.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(level.value(0), "ERROR");
        assert!(level.is_null(1));
        assert_eq!(level.value(2), "INFO");
        let bare = batch_of(&[("body", StringArray::from(vec![Some("a"), Some("b")]))]);
        let none = label(&bare, "x", &DataType::Int64).unwrap();
        assert_eq!(none.null_count(), 2);
        let extracted = StringArray::from(vec![Some(r#"{"service_name":"parsed"}"#), None, None]);
        let rows = batch_of(&[(OVERFLOW, overflow), (EXTRACTED, extracted)]);
        let parsed = label(&rows, "service_name", &DataType::Utf8).unwrap();
        let parsed = parsed.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(parsed.value(0), "parsed", "derived labels win");
        assert_eq!(parsed.value(1), "lib");
    }

    #[test]
    fn row_labels_merge_every_source() {
        let labels = row_labels(
            Some(r#"{"resource.service.name":"api","attr.service.name":"shadow","scope.v":"1"}"#),
            Some(r#"{"status":"500","v":"2"}"#),
            Some("ERROR"),
        );
        let pairs: Vec<(&str, &str)> = labels
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(
            pairs,
            vec![
                ("level", "ERROR"),
                ("service_name", "shadow"),
                ("status", "500"),
                ("v", "2")
            ]
        );
        assert!(row_labels(None, None, None).is_empty());
    }

    #[test]
    fn a_label_comes_from_the_dedicated_column_when_the_batch_has_one() {
        use arrow::datatypes::{Field, Schema};

        let rows = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(OVERFLOW, DataType::Utf8, true),
                Field::new("attr.host", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec![
                    Some(r#"{"attr.host":"stale"}"#),
                    None,
                    Some(r#"{"resource.host":"r3"}"#),
                ])),
                Arc::new(StringArray::from(vec![Some("h1"), Some("h2"), None])),
            ],
        )
        .unwrap();
        let hosts = label(&rows, "host", &DataType::Utf8).unwrap();
        let hosts = hosts.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(hosts.value(0), "h1", "the column, not the stored text");
        assert_eq!(hosts.value(1), "h2");
        assert_eq!(hosts.value(2), "r3", "the overflow when the column is null");
    }
}
