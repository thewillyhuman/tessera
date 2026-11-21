// SPDX-License-Identifier: Apache-2.0

//! Structured log bodies at ingest (DM-10): a body that is a JSON
//! object or a logfmt line has its fields stored under `body.*` next
//! to the original text, as attributes of the row, so that queries
//! read them like any attribute without parsing the line again.

use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray};
use serde_json::{Map, Value};
use tessera_types::event::{BODY, OVERFLOW};

use crate::KernelError;
use crate::extract::{ERROR_LABEL, logfmt_labels};

/// Which bodies are parsed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct BodyParsing {
    /// Bodies that are JSON objects.
    pub json: bool,
    /// Bodies that are logfmt lines.
    pub logfmt: bool,
}

/// `rows` with the fields of every structured body added to the row's
/// attributes under `body.`, an attribute the row already has kept.
/// Rows of other kinds, and batches without a body column, come back
/// unchanged.
///
/// # Errors
///
/// If the rebuilt batch does not fit its schema.
pub fn parse_bodies(rows: &RecordBatch, parsing: BodyParsing) -> Result<RecordBatch, KernelError> {
    if !parsing.json && !parsing.logfmt {
        return Ok(rows.clone());
    }
    let text = |name: &str| {
        rows.schema().index_of(name).ok().and_then(|i| {
            rows.column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|a| (i, a.clone()))
        })
    };
    let (Some((_, body)), Some((overflow_index, overflow))) = (text(BODY), text(OVERFLOW)) else {
        return Ok(rows.clone());
    };
    let mut changed = false;
    let merged: StringArray = (0..rows.num_rows())
        .map(|i| {
            let current = (!overflow.is_null(i)).then(|| overflow.value(i).to_owned());
            if body.is_null(i) {
                return current;
            }
            let fields = fields_of(body.value(i), parsing);
            if fields.is_empty() {
                return current;
            }
            let mut object = current
                .as_deref()
                .and_then(|text| serde_json::from_str::<Map<String, Value>>(text).ok())
                .unwrap_or_default();
            for (path, value) in fields {
                object.entry(path).or_insert(value);
            }
            changed = true;
            serde_json::to_string(&object).ok()
        })
        .collect();
    if !changed {
        return Ok(rows.clone());
    }
    let mut columns = rows.columns().to_vec();
    columns[overflow_index] = Arc::new(merged);
    Ok(RecordBatch::try_new(rows.schema(), columns)?)
}

/// The `body.` paths of a structured line: a JSON object's members,
/// nested ones flattened with dots and lists kept as their JSON text;
/// or a logfmt line's pairs as text. Anything else has none.
fn fields_of(line: &str, parsing: BodyParsing) -> Vec<(String, Value)> {
    let trimmed = line.trim_start();
    if parsing.json
        && trimmed.starts_with('{')
        && let Ok(Value::Object(object)) = serde_json::from_str::<Value>(trimmed)
    {
        let mut fields = Vec::new();
        for (key, value) in object {
            flatten(&format!("body.{key}"), value, &mut fields);
        }
        return fields;
    }
    if parsing.logfmt && line.contains('=') {
        let pairs = logfmt_labels(line);
        if !pairs.contains_key(ERROR_LABEL) {
            return pairs
                .into_iter()
                .map(|(key, value)| (format!("body.{key}"), Value::String(value)))
                .collect();
        }
    }
    Vec::new()
}

fn flatten(path: &str, value: Value, into: &mut Vec<(String, Value)>) {
    match value {
        Value::Null => {}
        Value::Object(object) => {
            for (key, value) in object {
                flatten(&format!("{path}.{key}"), value, into);
            }
        }
        Value::Array(_) => into.push((path.to_owned(), Value::String(value.to_string()))),
        scalar => into.push((path.to_owned(), scalar)),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(bodies: &[Option<&str>], overflow: &[Option<&str>]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(BODY, DataType::Utf8, true),
                Field::new(OVERFLOW, DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(bodies.to_vec())),
                Arc::new(StringArray::from(overflow.to_vec())),
            ],
        )
        .unwrap()
    }

    fn overflow(rows: &RecordBatch, i: usize) -> Option<String> {
        let column = rows
            .column_by_name(OVERFLOW)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (!column.is_null(i)).then(|| column.value(i).to_owned())
    }

    #[test]
    fn json_bodies_add_their_fields_under_body_and_keep_existing_attributes() {
        let rows = batch(
            &[
                Some("{\"user\":\"ann\",\"http\":{\"status\":500},\"tags\":[1,2],\"none\":null}"),
                Some("plain text"),
                None,
            ],
            &[
                Some("{\"attr.k\":\"v\",\"body.user\":\"kept\"}"),
                None,
                None,
            ],
        );
        let parsed = parse_bodies(
            &rows,
            BodyParsing {
                json: true,
                logfmt: false,
            },
        )
        .unwrap();
        let first: Value = serde_json::from_str(&overflow(&parsed, 0).unwrap()).unwrap();
        assert_eq!(first["attr.k"], "v");
        assert_eq!(first["body.user"], "kept", "a stored attribute wins");
        assert_eq!(first["body.http.status"], 500);
        assert_eq!(first["body.tags"], "[1,2]");
        assert!(first.get("body.none").is_none());
        assert_eq!(overflow(&parsed, 1), None, "text is not structured");
        assert_eq!(overflow(&parsed, 2), None);
    }

    #[test]
    fn logfmt_bodies_are_parsed_only_when_asked() {
        let rows = batch(&[Some("level=info msg=\"hello world\" n=3")], &[None]);
        let off = parse_bodies(
            &rows,
            BodyParsing {
                json: true,
                logfmt: false,
            },
        )
        .unwrap();
        assert_eq!(overflow(&off, 0), None);
        let on = parse_bodies(
            &rows,
            BodyParsing {
                json: false,
                logfmt: true,
            },
        )
        .unwrap();
        let fields: Value = serde_json::from_str(&overflow(&on, 0).unwrap()).unwrap();
        assert_eq!(fields["body.msg"], "hello world");
        assert_eq!(fields["body.n"], "3", "logfmt values are text");
    }
}
