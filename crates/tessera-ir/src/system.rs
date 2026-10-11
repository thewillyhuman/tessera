// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `system` schema (CS-3): the control tables and the view over the
//! rule state logs (CQ-13) as the planner sees them. The typed tables
//! expose their records' fields as columns; the others are a key and a
//! JSON document. Where the rows come from is the node's business: the
//! engine asks its system-table provider at run time.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

/// The names of the tables, with `alert_instances` among them.
pub const TABLES: [&str; 11] = [
    "alert_rules",
    "alert_instances",
    "silences",
    "acknowledgments",
    "notification_routes",
    "rollups",
    "retention_policies",
    "layout_pins",
    "api_keys",
    "roles",
    "saved_queries",
];

fn text(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

fn int(name: &str) -> Field {
    Field::new(name, DataType::Int64, true)
}

fn time(name: &str) -> Field {
    Field::new(name, DataType::Timestamp(TimeUnit::Nanosecond, None), true)
}

/// The columns of `system.usage` (CO-4) besides `timestamp` and `phase`:
/// the column, the attribute of the `_system.usage` event it comes
/// from, and whether it is a number.
pub const USAGE_COLUMNS: [(&str, &str, bool); 18] = [
    ("tenant", "attr.tenant", false),
    ("dataset", "attr.dataset", false),
    ("kind", "attr.kind", false),
    ("service", "attr.service", false),
    ("team", "attr.team", false),
    ("bytes", "attr.bytes", true),
    ("events", "attr.events", true),
    ("principal", "attr.principal", false),
    ("language", "attr.language", false),
    ("source", "attr.source", false),
    ("fingerprint", "attr.fingerprint", false),
    ("cpu_ms", "attr.cpu_ms", true),
    ("requests", "attr.requests", true),
    ("pattern_id", "attr.pattern_id", true),
    ("pattern", "attr.pattern", false),
    ("name", "attr.name", false),
    ("action", "attr.action", false),
    ("limit", "attr.limit", true),
];

/// The schema of `system.log_patterns` (section 13.2): each pattern of a
/// dataset with the rows and bytes compaction has stored under it.
#[must_use]
pub fn log_patterns_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        text("tenant"),
        text("dataset"),
        int("pattern_id"),
        text("pattern"),
        Field::new("rows", DataType::Float64, true),
        Field::new("bytes", DataType::Float64, true),
    ]))
}

/// The schema of `system.usage`.
#[must_use]
pub fn usage_schema() -> SchemaRef {
    let mut fields = vec![time("timestamp"), text("phase")];
    for (name, _, numeric) in USAGE_COLUMNS {
        fields.push(if numeric { int(name) } else { text(name) });
    }
    Arc::new(Schema::new(fields))
}

/// The schema of `system.<table>`, or `None` for an unknown table.
#[must_use]
pub fn system_table_schema(table: &str) -> Option<SchemaRef> {
    let fields = match table {
        "alert_rules" => vec![
            text("name"),
            text("group"),
            text("language"),
            text("expr"),
            text("dataset"),
            int("every_seconds"),
            int("for_seconds"),
            text("severity"),
            text("labels"),
            text("annotations"),
            text("notify"),
        ],
        "api_keys" => vec![
            text("id"),
            text("name"),
            text("roles"),
            time("created_at"),
            time("expires_at"),
        ],
        "roles" => vec![
            text("name"),
            text("read"),
            text("write"),
            Field::new("admin", DataType::Boolean, true),
        ],
        "rollups" => vec![
            text("name"),
            text("expr"),
            text("dataset"),
            text("target"),
            int("window_seconds"),
            int("every_seconds"),
            text("origin"),
            Field::new("enabled", DataType::Boolean, true),
        ],
        "alert_instances" => vec![
            text("rule"),
            text("group"),
            text("fingerprint"),
            text("status"),
            time("since"),
            Field::new("value", DataType::Float64, true),
            text("labels"),
            int("transitions"),
            time("notified_at"),
        ],
        "silences" => vec![
            text("id"),
            text("matchers"),
            time("starts_at"),
            time("ends_at"),
            text("comment"),
        ],
        "acknowledgments" => vec![
            text("fingerprint"),
            time("until"),
            text("by"),
            text("comment"),
        ],
        "notification_routes" => vec![
            text("name"),
            text("kind"),
            text("url"),
            text("routing_key"),
            int("repeat_seconds"),
        ],
        "retention_policies" | "layout_pins" | "saved_queries" => {
            vec![text("key"), text("document")]
        }
        _ => return None,
    };
    Some(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_table_has_a_schema_and_nothing_else_does() {
        for table in TABLES {
            assert!(system_table_schema(table).is_some(), "{table}");
        }
        assert!(system_table_schema("logs").is_none());
        let rules = system_table_schema("alert_rules").unwrap();
        assert_eq!(
            rules.field_with_name("every_seconds").unwrap().data_type(),
            &DataType::Int64
        );
    }
}
