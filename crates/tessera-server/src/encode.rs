// SPDX-License-Identifier: Apache-2.0

//! Arrow batches as JSON values, for the HTTP query API.

use arrow::array::{
    Array, ArrayRef, BooleanArray, DurationNanosecondArray, FixedSizeBinaryArray, Float64Array,
    Int32Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use serde::Serialize;
use serde_json::Value;
use tessera_types::Timestamp;
use tessera_types::event::OVERFLOW;

use crate::ApiError;

/// A column of a result.
#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct ColumnJson {
    /// The column name.
    pub name: String,
    /// The column type, in SQL terms.
    pub r#type: String,
}

/// The SQL name of an Arrow type.
#[must_use]
pub fn type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "boolean".into(),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "bigint".into(),
        DataType::Float32 | DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 => "text".into(),
        DataType::Timestamp(_, _) => "timestamp".into(),
        DataType::Duration(_) => "interval".into(),
        DataType::Binary | DataType::FixedSizeBinary(_) => "bytea".into(),
        DataType::Null => "null".into(),
        other => other.to_string().to_ascii_lowercase(),
    }
}

/// The columns and rows of `batches` as JSON.
///
/// # Errors
///
/// If a column has a type the API does not encode.
pub fn rows_json(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Result<(Vec<ColumnJson>, Vec<Vec<Value>>), ApiError> {
    let columns = schema
        .fields()
        .iter()
        .map(|f| ColumnJson {
            name: f.name().clone(),
            r#type: type_name(f.data_type()),
        })
        .collect();
    let mut rows = Vec::new();
    for batch in batches {
        let encoded: Vec<Vec<Value>> = batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, column)| column_json(field.name(), column))
            .collect::<Result<_, _>>()?;
        for i in 0..batch.num_rows() {
            rows.push(encoded.iter().map(|c| c[i].clone()).collect());
        }
    }
    Ok((columns, rows))
}

fn column_json(name: &str, array: &ArrayRef) -> Result<Vec<Value>, ApiError> {
    let n = array.len();
    let any = array.as_any();
    let values: Vec<Value> = match array.data_type() {
        DataType::Boolean => cells(any.downcast_ref::<BooleanArray>(), n, |a, i| {
            Value::Bool(a.value(i))
        }),
        DataType::Int32 => cells(any.downcast_ref::<Int32Array>(), n, |a, i| {
            Value::from(a.value(i))
        }),
        DataType::Int64 => cells(any.downcast_ref::<Int64Array>(), n, |a, i| {
            Value::from(a.value(i))
        }),
        DataType::Float64 => cells(any.downcast_ref::<Float64Array>(), n, |a, i| {
            serde_json::Number::from_f64(a.value(i)).map_or(Value::Null, Value::Number)
        }),
        DataType::Utf8 if name == OVERFLOW => {
            cells(any.downcast_ref::<StringArray>(), n, |a, i| {
                serde_json::from_str(a.value(i))
                    .unwrap_or_else(|_| Value::String(a.value(i).to_owned()))
            })
        }
        DataType::Utf8 => cells(any.downcast_ref::<StringArray>(), n, |a, i| {
            Value::String(a.value(i).to_owned())
        }),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            cells(any.downcast_ref::<TimestampNanosecondArray>(), n, |a, i| {
                Value::String(Timestamp::from_nanos(a.value(i)).to_string())
            })
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            cells(any.downcast_ref::<DurationNanosecondArray>(), n, |a, i| {
                Value::from(a.value(i))
            })
        }
        DataType::FixedSizeBinary(_) => {
            cells(any.downcast_ref::<FixedSizeBinaryArray>(), n, |a, i| {
                Value::String(hex(a.value(i)))
            })
        }
        DataType::Null => vec![Value::Null; n],
        other => {
            return Err(ApiError::bad_request(format!(
                "column {name} has type {other}, which the API cannot encode"
            )));
        }
    };
    Ok(values)
}

/// `f` over each non-null row of `array`, null elsewhere; all null if the
/// array is not of the expected type.
fn cells<A: Array>(array: Option<&A>, n: usize, f: impl Fn(&A, usize) -> Value) -> Vec<Value> {
    match array {
        Some(array) => (0..n)
            .map(|i| {
                if array.is_null(i) {
                    Value::Null
                } else {
                    f(array, i)
                }
            })
            .collect(),
        None => vec![Value::Null; n],
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};

    use super::*;

    #[test]
    fn encodes_every_supported_type_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i", DataType::Int64, true),
            Field::new("x", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("t", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new("id", DataType::FixedSizeBinary(2), true),
            Field::new(OVERFLOW, DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                Arc::new(Int64Array::from(vec![Some(7), None])),
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(TimestampNanosecondArray::from(vec![Some(0), None])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        vec![Some(vec![0xab, 0x01]), None].into_iter(),
                        2,
                    )
                    .unwrap(),
                ),
                Arc::new(StringArray::from(vec![Some(r#"{"attr.n":1}"#), None])),
            ],
        )
        .unwrap();
        let (columns, rows) = rows_json(&schema, &[batch]).unwrap();
        assert_eq!(columns[4].r#type, "timestamp");
        assert_eq!(columns[5].r#type, "bytea");
        assert_eq!(
            rows[0],
            vec![
                Value::Bool(true),
                Value::from(7),
                Value::from(1.5),
                Value::from("a"),
                Value::from("1970-01-01T00:00:00Z"),
                Value::from("ab01"),
                serde_json::json!({"attr.n": 1}),
            ]
        );
        assert!(rows[1].iter().all(Value::is_null));
    }
}
