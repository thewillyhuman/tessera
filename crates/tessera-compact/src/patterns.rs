// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Log patterns (section 13.2): at compaction every log body is reduced
//! to a template, `connection to <*> timed out after <*> ms`, and the
//! row receives the template's id. The reduction is a deterministic
//! function of the body, numbers, hex, addresses and long mixed tokens
//! masked, so a template keeps its id across files and compactions
//! (CO-5) with no shared state: the id is a stable hash of the template.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use tessera_types::event::{BODY, PATTERN_ID};
use tessera_types::stable_hash;

use crate::CompactError;

/// The wildcard a masked token becomes.
pub const WILDCARD: &str = "<*>";
/// Tokens beyond this many are not part of the template; the rest of
/// the body is one wildcard.
const MAX_TOKENS: usize = 32;

/// The template of `body`.
#[must_use]
pub fn template(body: &str) -> String {
    let mut tokens: Vec<&str> = Vec::new();
    for token in body.split_whitespace() {
        if tokens.len() == MAX_TOKENS {
            tokens.push(WILDCARD);
            break;
        }
        tokens.push(if is_variable(token) { WILDCARD } else { token });
    }
    tokens.join(" ")
}

/// Whether a token is a value rather than a word: it holds a digit, or
/// is long and not a plain word.
fn is_variable(token: &str) -> bool {
    let core = token.trim_matches(|c: char| !c.is_alphanumeric());
    if core.is_empty() {
        return false;
    }
    core.chars().any(|c| c.is_ascii_digit())
        || (core.len() > 24 && !core.chars().all(|c| c.is_alphabetic() || c == '_'))
}

/// The stable id of a template (CO-5).
#[must_use]
pub fn pattern_id(template: &str) -> i64 {
    // The high bit is cleared so the id fits a signed column as is.
    i64::try_from(stable_hash(template.as_bytes()) >> 1).unwrap_or(i64::MAX)
}

/// `rows` with a `pattern_id` column from their bodies, and the
/// templates seen with their ids.
///
/// # Errors
///
/// If the column cannot be added.
pub fn assign(rows: &RecordBatch) -> Result<(RecordBatch, Vec<(i64, String)>), CompactError> {
    let Some(bodies) = rows
        .column_by_name(BODY)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
    else {
        return Ok((rows.clone(), Vec::new()));
    };
    if rows.schema().field_with_name(PATTERN_ID).is_ok() {
        return Ok((rows.clone(), Vec::new()));
    }
    let mut seen: std::collections::BTreeMap<i64, String> = std::collections::BTreeMap::new();
    let ids: Vec<Option<i64>> = (0..bodies.len())
        .map(|i| {
            if bodies.is_null(i) {
                return None;
            }
            let template = template(bodies.value(i));
            let id = pattern_id(&template);
            seen.entry(id).or_insert(template);
            Some(id)
        })
        .collect();
    let mut fields: Vec<Field> = rows
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(PATTERN_ID, DataType::Int64, true));
    let mut columns: Vec<ArrayRef> = rows.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(ids)));
    let assigned = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    Ok((assigned, seen.into_iter().collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn templates_mask_values_and_keep_words() {
        assert_eq!(
            template("connection to 10.0.0.7:5432 timed out after 3000 ms"),
            "connection to <*> timed out after <*> ms"
        );
        assert_eq!(
            template("user 8f1c2a3b-9d4e-4f5a-8b6c-7d8e9f0a1b2c logged in"),
            "user <*> logged in"
        );
        assert_eq!(template("payment failed"), "payment failed");
        assert_eq!(
            pattern_id(&template("retry 1 of 5")),
            pattern_id(&template("retry 4 of 5")),
            "the same template has the same id, whatever the values"
        );
        assert_ne!(pattern_id("a b"), pattern_id("a c"));
        assert!(pattern_id("anything") >= 0);
    }

    #[test]
    fn rows_get_their_pattern_ids() {
        let schema = Arc::new(Schema::new(vec![Field::new(BODY, DataType::Utf8, true)]));
        let rows = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("retry 1 of 5"),
                Some("retry 2 of 5"),
                None,
                Some("payment failed"),
            ]))],
        )
        .unwrap();
        let (assigned, seen) = assign(&rows).unwrap();
        let ids = assigned
            .column_by_name(PATTERN_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        assert_eq!(ids.value(0), ids.value(1));
        assert!(ids.is_null(2));
        assert_ne!(ids.value(0), ids.value(3));
        assert_eq!(seen.len(), 2);
        assert!(seen.iter().any(|(_, t)| t == "retry <*> of <*>"));
        let (again, seen) = assign(&assigned).unwrap();
        assert_eq!(
            again.num_columns(),
            assigned.num_columns(),
            "already assigned"
        );
        assert!(seen.is_empty());
    }
}
