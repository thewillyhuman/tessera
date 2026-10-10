// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Variant access (section 10.6): reading an attribute path as a typed
//! column.
//!
//! Until the advisor promotes a path to a dedicated column (M8), every
//! attribute lives in `_overflow` as a JSON object of path to scalar
//! (ST-13 fallback). This kernel reads one path out of that column as the
//! type the query asks for; values that do not fit the type are null, as
//! a cast of a dynamic value is in SQL.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use serde_json::Value;
use tessera_types::event::{SCOPES, label_name};

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

/// Reads the label `name` of each row (QL-19): the attribute whose path
/// maps to the name, the more specific scope first (QL-20), and `level`
/// for the severity text when no attribute claims it. `rows` is the
/// batch length, for batches without an overflow column.
///
/// # Errors
///
/// `Unsupported` for a type a label cannot be read as.
pub fn label(
    overflow: Option<&StringArray>,
    severity_text: Option<&StringArray>,
    name: &str,
    data_type: &DataType,
    rows: usize,
) -> Result<ArrayRef, KernelError> {
    let values = (0..rows).map(|i| {
        let from_attributes = overflow
            .filter(|o| !o.is_null(i))
            .and_then(|o| serde_json::from_str::<Value>(o.value(i)).ok())
            .and_then(|object| {
                let object = object.as_object()?;
                SCOPES.iter().find_map(|scope| {
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
        let service = label(
            Some(&overflow),
            Some(&severity),
            "service_name",
            &DataType::Utf8,
            3,
        )
        .unwrap();
        let service = service.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            service.value(0),
            "shadow",
            "attr wins over resource (QL-20)"
        );
        assert_eq!(service.value(1), "lib");
        assert!(service.is_null(2));
        let method = label(Some(&overflow), None, "http_method", &DataType::Utf8, 3).unwrap();
        assert_eq!(
            method
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "GET"
        );
        let level = label(
            Some(&overflow),
            Some(&severity),
            "level",
            &DataType::Utf8,
            3,
        )
        .unwrap();
        let level = level.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(level.value(0), "ERROR");
        assert!(level.is_null(1));
        assert_eq!(level.value(2), "INFO");
        let none = label(None, None, "x", &DataType::Int64, 2).unwrap();
        assert_eq!(none.null_count(), 2);
    }
}
