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
                "cannot read attribute {path} as {other}"
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
}
