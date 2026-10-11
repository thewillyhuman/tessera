// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Reading a span's `links` column for `TraceQL`'s `link.` scope
//! (QL-15, DM-9): the column is a JSON list of objects with the linked
//! `trace_id` and `span_id` as hex and the link's `attributes` by key.

use serde_json::Value;

/// Whether any link in `links`, the column's JSON, has `field` equal to
/// `value`: `trace_id`, `span_id`, or `attributes.<name>`, compared as
/// text, a number or a boolean written out.
#[must_use]
pub fn matches(links: &str, field: &str, value: &str) -> bool {
    let Ok(Value::Array(links)) = serde_json::from_str::<Value>(links) else {
        return false;
    };
    links.iter().any(|link| {
        let found = match field.strip_prefix("attributes.") {
            Some(name) => link.get("attributes").and_then(|a| a.get(name)),
            None => link.get(field),
        };
        match found {
            Some(Value::String(text)) => text == value,
            Some(Value::Null) | None => false,
            Some(other) => {
                let rendered = other.to_string();
                rendered == value
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINKS: &str =
        r#"[{"trace_id":"0101","span_id":"02","attributes":{"rel":"parent","hops":2,"ok":true}}]"#;

    #[test]
    fn any_link_may_match_on_ids_or_attributes() {
        assert!(matches(LINKS, "trace_id", "0101"));
        assert!(matches(LINKS, "span_id", "02"));
        assert!(matches(LINKS, "attributes.rel", "parent"));
        assert!(matches(LINKS, "attributes.hops", "2"));
        assert!(matches(LINKS, "attributes.ok", "true"));
        assert!(!matches(LINKS, "attributes.rel", "child"));
        assert!(!matches(LINKS, "attributes.missing", "x"));
        assert!(!matches("not json", "span_id", "02"));
        assert!(!matches("[]", "span_id", "02"));
    }
}
