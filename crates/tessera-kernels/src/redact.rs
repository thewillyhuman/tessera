// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Applying redaction rules (SE-4) to a batch before it is written: the
//! attributes in `_overflow` that a rule names are dropped, hashed or
//! masked, and so is a log's `body` when a rule names it.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tessera_types::event::{BODY, OVERFLOW};
use tessera_types::{Redaction, RedactionAction};

use crate::KernelError;

/// `rows` with `rules` applied; the rows themselves when no rule names
/// anything the rows have.
///
/// # Errors
///
/// If a column cannot be rebuilt.
pub fn apply(rows: &RecordBatch, rules: &[Redaction]) -> Result<RecordBatch, KernelError> {
    if rules.is_empty() {
        return Ok(rows.clone());
    }
    let mut columns = rows.columns().to_vec();
    if let Ok(index) = rows.schema().index_of(OVERFLOW) {
        if let Some(overflow) = rows.column(index).as_any().downcast_ref::<StringArray>() {
            columns[index] = redact_overflow(overflow, rules);
        }
    }
    let body_rules: Vec<&Redaction> = rules.iter().filter(|r| r.path == BODY).collect();
    if let (Ok(index), Some(rule)) = (rows.schema().index_of(BODY), body_rules.first()) {
        if let Some(body) = rows.column(index).as_any().downcast_ref::<StringArray>() {
            let redacted: StringArray = (0..body.len())
                .map(|i| {
                    if body.is_null(i) {
                        return None;
                    }
                    redacted_text(body.value(i), rule.action)
                })
                .collect();
            columns[index] = Arc::new(redacted);
        }
    }
    Ok(RecordBatch::try_new(rows.schema(), columns)?)
}

/// The overflow column with the rules applied to every object.
fn redact_overflow(overflow: &StringArray, rules: &[Redaction]) -> ArrayRef {
    let redacted: StringArray = (0..overflow.len())
        .map(|i| {
            if overflow.is_null(i) {
                return None;
            }
            let text = overflow.value(i);
            let Ok(Value::Object(object)) = serde_json::from_str::<Value>(text) else {
                return Some(text.to_owned());
            };
            let mut out = Map::with_capacity(object.len());
            for (path, value) in object {
                match rules.iter().find(|r| r.matches(&path)) {
                    None => {
                        out.insert(path, value);
                    }
                    Some(rule) => match rule.action {
                        RedactionAction::Drop => {}
                        action => {
                            let text = match &value {
                                Value::String(s) => s.clone(),
                                other => other.to_string(),
                            };
                            if let Some(replaced) = redacted_text(&text, action) {
                                out.insert(path, Value::String(replaced));
                            }
                        }
                    },
                }
            }
            if out.is_empty() {
                None
            } else {
                Some(Value::Object(out).to_string())
            }
        })
        .collect();
    Arc::new(redacted)
}

/// `text` after `action`: nothing when dropped.
fn redacted_text(text: &str, action: RedactionAction) -> Option<String> {
    match action {
        RedactionAction::Drop => None,
        RedactionAction::Hash => {
            let digest = Sha256::digest(text.as_bytes());
            Some(digest.iter().fold(String::with_capacity(64), |mut hex, b| {
                use std::fmt::Write;
                let _ = write!(hex, "{b:02x}");
                hex
            }))
        }
        RedactionAction::Mask => Some("***".into()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tessera_types::event::Scalar;
    use tessera_types::{DatasetName, EventBatch, LogRow, TenantId, Timestamp};

    use super::*;

    fn rows() -> RecordBatch {
        let row = LogRow {
            timestamp: Timestamp::from_nanos(1),
            event_id: [1; 16],
            body: Some("card 4111".into()),
            attributes: BTreeMap::from([
                ("attr.user.email".to_owned(), Scalar::Text("a@b.c".into())),
                ("attr.user.id".to_owned(), Scalar::Int(7)),
                ("attr.http.method".to_owned(), Scalar::Text("GET".into())),
            ]),
            ..LogRow::default()
        };
        EventBatch::from_logs(TenantId::DEFAULT, DatasetName::new("p").unwrap(), &[row])
            .unwrap()
            .rows()
            .clone()
    }

    fn overflow(rows: &RecordBatch) -> Value {
        let column = rows
            .column_by_name(OVERFLOW)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        serde_json::from_str(column.value(0)).unwrap()
    }

    #[test]
    fn attributes_are_dropped_hashed_or_masked_and_the_rest_kept() {
        let rules = [
            Redaction {
                path: "attr.user.email".into(),
                action: RedactionAction::Hash,
            },
            Redaction {
                path: "attr.user.id".into(),
                action: RedactionAction::Drop,
            },
            Redaction {
                path: "body".into(),
                action: RedactionAction::Mask,
            },
        ];
        let redacted = apply(&rows(), &rules).unwrap();
        let attributes = overflow(&redacted);
        assert_eq!(attributes["attr.http.method"], "GET");
        assert!(attributes.get("attr.user.id").is_none(), "{attributes}");
        let hashed = attributes["attr.user.email"].as_str().unwrap();
        assert_eq!(hashed.len(), 64);
        assert_ne!(hashed, "a@b.c");
        let again = overflow(&apply(&rows(), &rules).unwrap());
        assert_eq!(again["attr.user.email"], hashed, "equal values hash alike");
        let body = redacted
            .column_by_name(BODY)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        assert_eq!(body.value(0), "***");
    }

    #[test]
    fn dropping_every_attribute_leaves_the_overflow_null() {
        let rules = [Redaction {
            path: "attr.*".into(),
            action: RedactionAction::Drop,
        }];
        let redacted = apply(&rows(), &rules).unwrap();
        assert!(redacted.column_by_name(OVERFLOW).unwrap().is_null(0));
    }
}
