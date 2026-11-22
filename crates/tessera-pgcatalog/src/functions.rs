// SPDX-License-Identifier: Apache-2.0

//! The functions of `PostgreSQL` that catalog queries call: what the
//! session is, what an oid names, how a type is written, and the
//! many that ask about things Tessera does not have (privileges,
//! sizes, definitions), which answer as an empty database would.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use regex::Regex;
use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::{Value, ValueRef};
use rusqlite::{Connection, Result};

use crate::fill::{self, OWNER, PG_CATALOG, PUBLIC};
use crate::{Shape, types};

/// What the functions know of a shape.
struct Known {
    database: String,
    user: String,
    version: String,
    settings: Vec<(&'static str, String)>,
    /// Each schema's oid and name.
    namespaces: Vec<(i64, String)>,
    /// Each table's oid, its schema's oid and name, and its name.
    tables: Vec<(i64, i64, String, String)>,
}

/// The oids `PostgreSQL` gives the catalogs clients cast to `regclass`.
const CATALOGS: [(&str, i64); 14] = [
    ("pg_class", 1259),
    ("pg_namespace", 2615),
    ("pg_type", 1247),
    ("pg_attribute", 1249),
    ("pg_proc", 1255),
    ("pg_database", 1262),
    ("pg_authid", 1260),
    ("pg_constraint", 2606),
    ("pg_index", 2610),
    ("pg_am", 2601),
    ("pg_description", 2609),
    ("pg_attrdef", 2604),
    ("pg_trigger", 2620),
    ("pg_extension", 3079),
];

/// Whether `text` reads as true the way `PostgreSQL` reads a boolean.
pub(crate) fn truth(text: &str) -> bool {
    matches!(
        text.trim().to_ascii_lowercase().as_str(),
        "t" | "true" | "1" | "on" | "yes" | "y"
    )
}

fn text(value: ValueRef<'_>) -> Option<String> {
    match value {
        ValueRef::Null => None,
        ValueRef::Integer(n) => Some(n.to_string()),
        ValueRef::Real(x) => Some(x.to_string()),
        ValueRef::Text(bytes) | ValueRef::Blob(bytes) => {
            Some(String::from_utf8_lossy(bytes).into_owned())
        }
    }
}

fn integer(value: ValueRef<'_>) -> Option<i64> {
    match value {
        ValueRef::Integer(n) => Some(n),
        ValueRef::Text(bytes) => std::str::from_utf8(bytes).ok()?.trim().parse().ok(),
        _ => None,
    }
}

fn some_text(value: Option<String>) -> Value {
    value.map_or(Value::Null, Value::Text)
}

fn some_integer(value: Option<i64>) -> Value {
    value.map_or(Value::Null, Value::Integer)
}

/// The elements of an array, written as `PostgreSQL` writes one
/// (`{a,"b c"}`) or as a JSON list, which is what arrays are here.
pub(crate) fn elements(array: &str) -> Vec<String> {
    let array = array.trim();
    if array.starts_with('[') {
        return json_strings(array);
    }
    let Some(inner) = array.strip_prefix('{').and_then(|a| a.strip_suffix('}')) else {
        return vec![array.to_owned()];
    };
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut any = false;
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                quoted = !quoted;
                any = true;
            }
            '\\' if quoted => current.extend(chars.next()),
            ',' if !quoted => {
                out.push(std::mem::take(&mut current));
                any = false;
            }
            other => {
                current.push(other);
                any = true;
            }
        }
    }
    if any || !out.is_empty() {
        out.push(current);
    }
    out
}

/// The strings of a JSON list of scalars; numbers and the rest come
/// as they are written.
fn json_strings(list: &str) -> Vec<String> {
    let inner = list.trim().trim_start_matches('[').trim_end_matches(']');
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut any = false;
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                quoted = !quoted;
                any = true;
            }
            '\\' if quoted => current.extend(chars.next()),
            ',' if !quoted => {
                out.push(std::mem::take(&mut current).trim().to_owned());
                any = false;
            }
            other => {
                current.push(other);
                any |= !other.is_whitespace();
            }
        }
    }
    if any {
        out.push(current.trim().to_owned());
    }
    out
}

/// `elements` as a JSON list of strings.
fn json_list(elements: &[String]) -> String {
    let quoted: Vec<String> = elements
        .iter()
        .map(|e| format!("\"{}\"", e.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect();
    format!("[{}]", quoted.join(","))
}

impl Known {
    fn of(shape: &Shape) -> Known {
        Known {
            database: shape.database.clone(),
            user: shape.user.clone(),
            version: shape.version.clone(),
            settings: fill::settings(shape),
            namespaces: fill::namespaces(shape)
                .into_iter()
                .map(|(oid, name)| (oid, name.to_owned()))
                .collect(),
            tables: fill::tables(shape)
                .into_iter()
                .map(|placed| {
                    (
                        placed.oid,
                        placed.namespace,
                        placed.schema.to_owned(),
                        placed.table.name.clone(),
                    )
                })
                .collect(),
        }
    }

    fn setting(&self, name: &str) -> Option<String> {
        self.settings
            .iter()
            .find(|(known, _)| known.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.clone())
    }

    /// The oid of the relation named `name`, with or without its
    /// schema and quotes: a table, or one of the catalogs.
    fn relation(&self, name: &str) -> Option<i64> {
        let name = name.replace('"', "");
        let (schema, relation) = match name.rsplit_once('.') {
            Some((schema, relation)) => (Some(schema), relation),
            None => (None, name.as_str()),
        };
        let table = self
            .tables
            .iter()
            .filter(|(_, _, of, table)| table == relation && schema.is_none_or(|s| s == of))
            .min_by_key(|(_, namespace, _, _)| *namespace != PUBLIC);
        if let Some((oid, _, _, _)) = table {
            return Some(*oid);
        }
        CATALOGS
            .iter()
            .find(|(catalog, _)| *catalog == relation)
            .map(|(_, oid)| *oid)
    }

    /// Whether the relation `oid` is found without naming its schema.
    fn is_visible(&self, oid: i64) -> bool {
        self.tables
            .iter()
            .any(|(table, namespace, _, _)| *table == oid && *namespace == PUBLIC)
            || CATALOGS.iter().any(|(_, catalog)| *catalog == oid)
    }
}

/// Registers `name` taking `arguments` (any number when -1).
fn function<F>(connection: &Connection, name: &str, arguments: i32, body: F) -> Result<()>
where
    F: Fn(&Context<'_>) -> Value + Send + Sync + 'static,
{
    connection.create_scalar_function(name, arguments, FunctionFlags::SQLITE_UTF8, move |ctx| {
        Ok(body(ctx))
    })
}

/// Registers the functions for a connection that sees `shape`.
pub(crate) fn register(connection: &Connection, shape: &Shape) -> Result<()> {
    let known = Arc::new(Known::of(shape));
    session(connection, &known)?;
    names(connection, &known)?;
    arrays(connection)?;
    absent(connection)?;
    let patterns: Mutex<HashMap<String, Option<Regex>>> = Mutex::new(HashMap::new());
    function(connection, "regexp", 2, move |ctx| {
        let (Some(pattern), Some(value)) = (text(ctx.get_raw(0)), text(ctx.get_raw(1))) else {
            return Value::Null;
        };
        let mut patterns = patterns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let compiled = patterns
            .entry(pattern)
            .or_insert_with_key(|pattern| Regex::new(pattern).ok());
        Value::Integer(i64::from(
            compiled
                .as_ref()
                .is_some_and(|regex| regex.is_match(&value)),
        ))
    })
}

/// What the session is.
fn session(connection: &Connection, known: &Arc<Known>) -> Result<()> {
    let k = Arc::clone(known);
    function(connection, "version", 0, move |_| {
        Value::Text(format!(
            "PostgreSQL {} (Tessera {})",
            crate::SERVER_VERSION,
            k.version
        ))
    })?;
    for name in ["current_database", "current_catalog"] {
        let k = Arc::clone(known);
        function(connection, name, 0, move |_| {
            Value::Text(k.database.clone())
        })?;
    }
    for name in ["current_user", "session_user", "current_role", "user"] {
        let k = Arc::clone(known);
        function(connection, name, 0, move |_| Value::Text(k.user.clone()))?;
    }
    function(connection, "current_schema", 0, |_| {
        Value::Text("public".into())
    })?;
    function(connection, "current_schemas", 1, |ctx| {
        let implicit = text(ctx.get_raw(0)).is_some_and(|t| truth(&t));
        Value::Text(if implicit {
            "[\"pg_catalog\",\"public\"]".into()
        } else {
            "[\"public\"]".into()
        })
    })?;
    for arguments in [1, 2] {
        let k = Arc::clone(known);
        function(connection, "current_setting", arguments, move |ctx| {
            some_text(text(ctx.get_raw(0)).and_then(|name| k.setting(&name)))
        })?;
    }
    function(connection, "set_config", 3, |ctx| {
        some_text(text(ctx.get_raw(1)))
    })?;
    for (name, value) in [
        ("getdatabaseencoding", "UTF8"),
        ("pg_client_encoding", "UTF8"),
    ] {
        function(connection, name, 0, move |_| Value::Text(value.into()))?;
    }
    function(connection, "pg_encoding_to_char", 1, |_| {
        Value::Text("UTF8".into())
    })?;
    for (name, value) in [
        ("pg_backend_pid", 1),
        ("pg_is_in_recovery", 0),
        ("txid_current", 1),
        ("pg_my_temp_schema", 0),
        ("inet_server_port", 5432),
    ] {
        function(connection, name, 0, move |_| Value::Integer(value))?;
    }
    for name in ["inet_server_addr", "inet_client_addr", "pg_conf_load_time"] {
        function(connection, name, 0, |_| Value::Null)?;
    }
    connection.execute_batch(
        "CREATE TEMP VIEW pg_now AS SELECT strftime('%Y-%m-%d %H:%M:%f+00', 'now') AS now;",
    )?;
    for name in [
        "now",
        "clock_timestamp",
        "statement_timestamp",
        "transaction_timestamp",
        "pg_postmaster_start_time",
    ] {
        function(connection, name, 0, |_| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs());
            Value::Text(timestamp(now))
        })?;
    }
    Ok(())
}

/// Seconds since the epoch as `PostgreSQL` writes a timestamp in UTC.
fn timestamp(seconds: u64) -> String {
    let days = seconds / 86_400;
    let (hour, minute, second) = (seconds % 86_400 / 3_600, seconds % 3_600 / 60, seconds % 60);
    // Civil date from the day count (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}+00")
}

/// What an oid names and how a name is written.
fn names(connection: &Connection, known: &Arc<Known>) -> Result<()> {
    let k = Arc::clone(known);
    function(connection, "regclass", 1, move |ctx| {
        let value = ctx.get_raw(0);
        match value {
            ValueRef::Integer(oid) => Value::Integer(oid),
            other => some_integer(
                text(other).and_then(|name| name.trim().parse().ok().or_else(|| k.relation(&name))),
            ),
        }
    })?;
    let k = Arc::clone(known);
    function(connection, "to_regclass", 1, move |ctx| {
        some_integer(text(ctx.get_raw(0)).and_then(|name| k.relation(&name)))
    })?;
    // An oid reads as the type's name, a name as its oid.
    function(connection, "regtype", 1, |ctx| match ctx.get_raw(0) {
        ValueRef::Integer(0) => Value::Text("-".into()),
        ValueRef::Integer(oid) => some_text(types::display(oid)),
        other => some_integer(text(other).and_then(|name| types::by_name(&name))),
    })?;
    function(connection, "to_regtype", 1, |ctx| {
        some_integer(text(ctx.get_raw(0)).and_then(|name| types::by_name(&name)))
    })?;
    let k = Arc::clone(known);
    function(connection, "regnamespace", 1, move |ctx| {
        match ctx.get_raw(0) {
            ValueRef::Integer(oid) => Value::Integer(oid),
            other => some_integer(text(other).and_then(|name| {
                k.namespaces
                    .iter()
                    .find(|(_, known)| *known == name.replace('"', ""))
                    .map(|(oid, _)| *oid)
            })),
        }
    })?;
    function(connection, "pg_bool", 1, |ctx| match ctx.get_raw(0) {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(n) => Value::Integer(i64::from(n != 0)),
        other => Value::Integer(i64::from(text(other).is_some_and(|t| truth(&t)))),
    })?;
    for arguments in [1, 2] {
        function(connection, "format_type", arguments, |ctx| {
            some_text(integer(ctx.get_raw(0)).and_then(|oid| {
                types::display(oid).or_else(|| (oid != 0).then(|| "???".to_owned()))
            }))
        })?;
    }
    let k = Arc::clone(known);
    function(connection, "pg_get_userbyid", 1, move |ctx| {
        match integer(ctx.get_raw(0)) {
            Some(OWNER) => Value::Text(k.user.clone()),
            Some(oid) => Value::Text(format!("unknown (OID={oid})")),
            None => Value::Null,
        }
    })?;
    let k = Arc::clone(known);
    function(connection, "pg_table_is_visible", 1, move |ctx| {
        some_integer(integer(ctx.get_raw(0)).map(|oid| i64::from(k.is_visible(oid))))
    })?;
    function(connection, "pg_type_is_visible", 1, |ctx| {
        some_integer(
            integer(ctx.get_raw(0))
                .map(|oid| i64::from(types::by_oid(oid).is_some() || oid == PG_CATALOG)),
        )
    })?;
    function(connection, "quote_ident", 1, |ctx| {
        some_text(text(ctx.get_raw(0)).map(|name| {
            let plain = name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                && !name.starts_with(|c: char| c.is_ascii_digit())
                && !name.is_empty();
            if plain {
                name
            } else {
                format!("\"{}\"", name.replace('"', "\"\""))
            }
        }))
    })?;
    function(connection, "pg_size_pretty", 1, |ctx| {
        some_text(integer(ctx.get_raw(0)).map(|bytes| format!("{bytes} bytes")))
    })
}

/// Arrays, which are JSON lists here.
fn arrays(connection: &Connection) -> Result<()> {
    function(connection, "pg_array", 1, |ctx| {
        Value::Text(text(ctx.get_raw(0)).map_or_else(|| "[]".into(), |a| json_list(&elements(&a))))
    })?;
    for arguments in [2, 3] {
        function(connection, "array_to_string", arguments, |ctx| {
            let (Some(array), Some(separator)) = (text(ctx.get_raw(0)), text(ctx.get_raw(1)))
            else {
                return Value::Null;
            };
            Value::Text(elements(&array).join(&separator))
        })?;
    }
    for name in ["array_length", "array_upper"] {
        function(connection, name, 2, |ctx| {
            let count = text(ctx.get_raw(0)).map_or(0, |a| elements(&a).len());
            some_integer((count > 0).then(|| i64::try_from(count).unwrap_or(i64::MAX)))
        })?;
    }
    function(connection, "array_lower", 2, |ctx| {
        let count = text(ctx.get_raw(0)).map_or(0, |a| elements(&a).len());
        some_integer((count > 0).then_some(1))
    })?;
    function(connection, "string_to_array", 2, |ctx| {
        let (Some(whole), Some(separator)) = (text(ctx.get_raw(0)), text(ctx.get_raw(1))) else {
            return Value::Null;
        };
        let parts: Vec<String> = whole.split(separator.as_str()).map(str::to_owned).collect();
        Value::Text(json_list(&parts))
    })?;
    // The parts of a qualified name, quotes removed.
    function(connection, "parse_ident", 1, |ctx| {
        some_text(text(ctx.get_raw(0)).map(|name| {
            let parts: Vec<String> = name
                .split('.')
                .map(|part| part.trim().trim_matches('"').to_owned())
                .collect();
            json_list(&parts)
        }))
    })?;
    // The n-th of a list, from one; null past its end.
    function(connection, "pg_element", 2, |ctx| {
        let (Some(array), Some(position)) = (text(ctx.get_raw(0)), integer(ctx.get_raw(1))) else {
            return Value::Null;
        };
        let index = usize::try_from(position - 1).ok();
        some_text(index.and_then(|index| elements(&array).into_iter().nth(index)))
    })?;
    function(connection, "cardinality", 1, |ctx| {
        some_integer(
            text(ctx.get_raw(0)).map(|a| i64::try_from(elements(&a).len()).unwrap_or(i64::MAX)),
        )
    })
}

/// The functions that ask about what Tessera does not have. A
/// definition is null, a privilege is held, a size is zero.
fn absent(connection: &Connection) -> Result<()> {
    // `unnest` and `generate_subscripts` among the fields of a query
    // give a row per element; the catalogs they are asked of here,
    // the indexes and the constraints, have no row to begin with.
    const NULL: [(&str, &[i32]); 26] = [
        ("unnest", &[1]),
        ("generate_subscripts", &[2]),
        ("pg_get_expr", &[2, 3]),
        ("pg_get_constraintdef", &[1, 2]),
        ("pg_get_indexdef", &[1, 3]),
        ("pg_get_viewdef", &[1, 2]),
        ("pg_get_triggerdef", &[1, 2]),
        ("pg_get_ruledef", &[1, 2]),
        ("pg_get_functiondef", &[1]),
        ("pg_get_function_arguments", &[1]),
        ("pg_get_function_identity_arguments", &[1]),
        ("pg_get_function_result", &[1]),
        ("pg_get_partkeydef", &[1]),
        ("pg_get_partition_constraintdef", &[1]),
        ("pg_get_statisticsobjdef", &[1]),
        ("pg_get_statisticsobjdef_columns", &[1]),
        ("pg_get_serial_sequence", &[2]),
        ("obj_description", &[1, 2]),
        ("col_description", &[2]),
        ("shobj_description", &[2]),
        ("pg_tablespace_location", &[1]),
        ("pg_stat_get_last_analyze_time", &[1]),
        ("pg_stat_get_last_vacuum_time", &[1]),
        ("age", &[1]),
        ("pg_describe_object", &[3]),
        ("pg_get_function_sqlbody", &[1]),
    ];
    const TRUE: [(&str, &[i32]); 10] = [
        ("has_table_privilege", &[2, 3]),
        ("has_schema_privilege", &[2, 3]),
        ("has_database_privilege", &[2, 3]),
        ("has_column_privilege", &[3, 4]),
        ("has_any_column_privilege", &[2, 3]),
        ("has_function_privilege", &[2, 3]),
        ("has_sequence_privilege", &[2, 3]),
        ("has_tablespace_privilege", &[2, 3]),
        ("has_type_privilege", &[2, 3]),
        ("pg_has_role", &[2, 3]),
    ];
    const ZERO: [(&str, &[i32]); 21] = [
        ("pg_conversion_is_visible", &[1]),
        ("pg_operator_is_visible", &[1]),
        ("pg_collation_is_visible", &[1]),
        ("pg_opclass_is_visible", &[1]),
        ("pg_opfamily_is_visible", &[1]),
        ("pg_statistics_obj_is_visible", &[1]),
        ("pg_ts_config_is_visible", &[1]),
        ("pg_ts_dict_is_visible", &[1]),
        ("pg_ts_parser_is_visible", &[1]),
        ("pg_relation_size", &[1, 2]),
        ("pg_total_relation_size", &[1]),
        ("pg_table_size", &[1]),
        ("pg_indexes_size", &[1]),
        ("pg_database_size", &[1]),
        ("pg_tablespace_size", &[1]),
        ("pg_function_is_visible", &[1]),
        ("pg_is_other_temp_schema", &[1]),
        ("pg_relation_is_publishable", &[1]),
        ("pg_stat_get_numscans", &[1]),
        ("pg_stat_get_live_tuples", &[1]),
        ("pg_stat_get_dead_tuples", &[1]),
    ];
    for (name, counts) in NULL {
        for arguments in counts {
            function(connection, name, *arguments, |_| Value::Null)?;
        }
    }
    for (name, counts) in TRUE {
        for arguments in counts {
            function(connection, name, *arguments, |_| Value::Integer(1))?;
        }
    }
    for (name, counts) in ZERO {
        for arguments in counts {
            function(connection, name, *arguments, |_| Value::Integer(0))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrays_are_read_in_either_writing() {
        assert_eq!(elements("{r,p}"), ["r", "p"]);
        assert_eq!(elements("{\"a b\",c}"), ["a b", "c"]);
        assert_eq!(elements("{}"), Vec::<String>::new());
        assert_eq!(
            elements("[\"pg_catalog\",\"public\"]"),
            ["pg_catalog", "public"]
        );
        assert_eq!(elements("[1, 2]"), ["1", "2"]);
        assert_eq!(elements("[]"), Vec::<String>::new());
        assert_eq!(elements("x"), ["x"]);
        assert_eq!(json_list(&elements("{a,\"b\\\"c\"}")), "[\"a\",\"b\\\"c\"]");
    }

    #[test]
    fn a_timestamp_is_written_in_utc() {
        assert_eq!(timestamp(0), "1970-01-01 00:00:00+00");
        assert_eq!(timestamp(1_795_305_600), "2026-11-22 00:00:00+00");
        assert_eq!(timestamp(951_782_400 + 3_661), "2000-02-29 01:01:01+00");
    }
}
