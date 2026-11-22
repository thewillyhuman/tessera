// SPDX-License-Identifier: Apache-2.0

//! The catalogs laid out for a [`Shape`]: the tables of `schema.sql`
//! in two attached databases named as the schemas clients ask for,
//! with a row for every database, schema, table, column and type.

use rusqlite::{Connection, params};

use crate::types::{self, TYPES};
use crate::{Shape, Table};

/// The role that owns everything, as `PostgreSQL`'s bootstrap superuser
/// does.
pub(crate) const OWNER: i64 = 10;
/// The oid of `pg_catalog`.
pub(crate) const PG_CATALOG: i64 = 11;
/// The oid of `public`.
pub(crate) const PUBLIC: i64 = 2200;
/// The oid of `information_schema`.
pub(crate) const INFORMATION_SCHEMA: i64 = 13_000;
/// Where the oids of the databases begin.
const FIRST_DATABASE: i64 = 20_000;
/// Where the oids of the schemas besides `public` begin.
const FIRST_SCHEMA: i64 = 30_000;
/// Where the oids of the tables begin.
const FIRST_TABLE: i64 = 40_000;

/// A table and where the catalogs have it.
pub(crate) struct Placed<'a> {
    pub oid: i64,
    pub namespace: i64,
    pub schema: &'a str,
    pub table: &'a Table,
}

/// The oid of each schema of `shape`, with the catalogs' own two.
pub(crate) fn namespaces(shape: &Shape) -> Vec<(i64, &str)> {
    let mut found = vec![
        (PG_CATALOG, "pg_catalog"),
        (INFORMATION_SCHEMA, "information_schema"),
    ];
    let mut next = FIRST_SCHEMA;
    for schema in &shape.schemas {
        if schema.name == "public" {
            found.push((PUBLIC, "public"));
        } else {
            found.push((next, schema.name.as_str()));
            next += 1;
        }
    }
    found
}

/// Every table of `shape` with its oid, in the order of the shape.
pub(crate) fn tables(shape: &Shape) -> Vec<Placed<'_>> {
    let namespaces = namespaces(shape);
    let mut oid = FIRST_TABLE;
    let mut placed = Vec::new();
    for schema in &shape.schemas {
        let namespace = namespaces
            .iter()
            .find(|(_, name)| *name == schema.name)
            .map_or(PUBLIC, |(oid, _)| *oid);
        for table in &schema.tables {
            placed.push(Placed {
                oid,
                namespace,
                schema: &schema.name,
                table,
            });
            oid += 1;
        }
    }
    placed
}

/// The settings a session shows, by name.
pub(crate) fn settings(shape: &Shape) -> Vec<(&'static str, String)> {
    let fixed = [
        ("server_version", crate::SERVER_VERSION),
        ("server_version_num", "160000"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("IntervalStyle", "postgres"),
        ("TimeZone", "UTC"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        ("search_path", "\"$user\", public"),
        ("transaction_isolation", "read committed"),
        ("default_transaction_isolation", "read committed"),
        ("transaction_read_only", "off"),
        ("default_transaction_read_only", "off"),
        ("max_identifier_length", "63"),
        ("max_index_keys", "32"),
        ("max_connections", "100"),
        ("application_name", ""),
        ("is_superuser", "off"),
        ("extra_float_digits", "1"),
        ("bytea_output", "hex"),
        ("lc_collate", "C.UTF-8"),
        ("lc_ctype", "C.UTF-8"),
        ("statement_timeout", "0"),
        ("lock_timeout", "0"),
        ("ssl", "off"),
    ];
    let mut settings: Vec<(&'static str, String)> = fixed
        .into_iter()
        .map(|(name, value)| (name, value.to_owned()))
        .collect();
    settings.push(("session_authorization", shape.user.clone()));
    settings
}

/// Creates the catalogs in `connection` and fills them for `shape`.
pub(crate) fn lay_out(connection: &Connection, shape: &Shape) -> rusqlite::Result<()> {
    connection.execute_batch(
        "ATTACH DATABASE ':memory:' AS pg_catalog; \
         ATTACH DATABASE ':memory:' AS information_schema;",
    )?;
    connection.execute_batch(include_str!("schema.sql"))?;
    for (oid, name) in namespaces(shape) {
        connection.execute(
            "INSERT INTO pg_catalog.pg_namespace VALUES (?1, ?2, ?3, NULL)",
            params![oid, name, OWNER],
        )?;
        connection.execute(
            "INSERT INTO information_schema.schemata VALUES (?1, ?2, ?3, NULL, NULL, NULL, NULL)",
            params![shape.database, name, shape.user],
        )?;
    }
    for (database, oid) in shape.databases.iter().zip(FIRST_DATABASE..) {
        connection.execute(
            "INSERT INTO pg_catalog.pg_database VALUES \
             (?1, ?2, ?3, 6, 'c', 0, 1, -1, '0', '0', 1663, 'C.UTF-8', 'C.UTF-8', \
              NULL, NULL, NULL, NULL)",
            params![oid, database, OWNER],
        )?;
    }
    roles(connection, shape)?;
    fixed(connection, shape)?;
    for placed in tables(shape) {
        table(connection, shape, &placed)?;
    }
    Ok(())
}

/// The one role there is: the connection's user.
fn roles(connection: &Connection, shape: &Shape) -> rusqlite::Result<()> {
    connection.execute(
        "INSERT INTO pg_catalog.pg_roles VALUES \
         (?1, 0, 1, 0, 0, 1, 0, -1, '********', NULL, 0, NULL, ?2)",
        params![shape.user, OWNER],
    )?;
    connection.execute(
        "INSERT INTO pg_catalog.pg_authid VALUES (?2, ?1, 0, 1, 0, 0, 1, 0, 0, -1, NULL, NULL)",
        params![shape.user, OWNER],
    )?;
    for view in ["pg_user", "pg_shadow"] {
        connection.execute(
            &format!(
                "INSERT INTO pg_catalog.{view} VALUES (?1, ?2, 0, 0, 0, 0, '********', NULL, NULL)"
            ),
            params![shape.user, OWNER],
        )?;
    }
    Ok(())
}

/// What is the same for every shape: the types, the settings, the
/// access methods and the tablespaces.
fn fixed(connection: &Connection, shape: &Shape) -> rusqlite::Result<()> {
    for found in &TYPES {
        let is_array = found.name.starts_with('_');
        let function = |suffix: &str| {
            if is_array {
                format!("array_{suffix}")
            } else {
                format!("{}{suffix}", found.name)
            }
        };
        connection.execute(
            "INSERT INTO pg_catalog.pg_type VALUES \
             (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, 1, ',', 0, NULL, ?9, ?10, ?11, ?12, ?13, ?14, \
              '-', '-', '-', 'i', ?15, 0, 0, -1, 0, 0, NULL, NULL, NULL)",
            params![
                found.oid,
                found.name,
                PG_CATALOG,
                OWNER,
                found.len,
                (1..=8).contains(&found.len),
                if found.category == 'P' { "p" } else { "b" },
                found.category.to_string(),
                found.elem,
                found.array,
                function("in"),
                function("out"),
                function("recv"),
                function("send"),
                if found.len > 0 { "p" } else { "x" },
            ],
        )?;
    }
    for (name, setting) in settings(shape) {
        connection.execute(
            "INSERT INTO pg_catalog.pg_settings \
             (name, setting, category, short_desc, context, vartype, source, boot_val, \
              reset_val, pending_restart) \
             VALUES (?1, ?2, 'Preset Options', '', 'internal', 'string', 'default', ?2, ?2, 0)",
            params![name, setting],
        )?;
    }
    connection.execute_batch(
        "INSERT INTO pg_catalog.pg_am VALUES \
           (2, 'heap', 'heap_tableam_handler', 't'), (403, 'btree', 'bthandler', 'i'); \
         INSERT INTO pg_catalog.pg_tablespace VALUES \
           (1663, 'pg_default', 10, NULL, NULL), (1664, 'pg_global', 10, NULL, NULL); \
         INSERT INTO pg_catalog.pg_language VALUES \
           (12, 'internal', 10, 0, 0, 0, 0, 0, NULL), (14, 'sql', 10, 0, 1, 0, 0, 0, NULL); \
         INSERT INTO pg_catalog.pg_get_keywords (word, catcode, barelabel, catdesc, baredesc) \
         VALUES ('analyze', 'R', 1, 'reserved', 'can be bare label'), \
                ('ilike', 'T', 1, 'reserved (can be function or type name)', 'can be bare label'), \
                ('limit', 'R', 0, 'reserved', 'requires AS'), \
                ('offset', 'R', 0, 'reserved', 'requires AS'), \
                ('returning', 'R', 0, 'reserved', 'requires AS');",
    )
}

/// The rows that describe one table.
fn table(connection: &Connection, shape: &Shape, placed: &Placed<'_>) -> rusqlite::Result<()> {
    let name = &placed.table.name;
    connection.execute(
        "INSERT INTO pg_catalog.pg_class VALUES \
         (?1, ?2, ?3, 0, 0, ?4, 2, ?1, 0, 0, -1, 0, 0, 0, 0, 'p', 'r', ?5, 0, 0, 0, 0, 0, 0, 1, \
          'd', 0, 0, '0', '0', NULL, NULL, NULL)",
        params![
            placed.oid,
            name,
            placed.namespace,
            OWNER,
            i64::try_from(placed.table.columns.len()).unwrap_or(i64::MAX),
        ],
    )?;
    connection.execute(
        "INSERT INTO pg_catalog.pg_tables VALUES (?1, ?2, ?3, NULL, 0, 0, 0, 0)",
        params![placed.schema, name, shape.user],
    )?;
    connection.execute(
        "INSERT INTO information_schema.tables VALUES \
         (?1, ?2, ?3, 'BASE TABLE', NULL, NULL, NULL, NULL, NULL, 'NO', 'NO', NULL)",
        params![shape.database, placed.schema, name],
    )?;
    for (column, position) in placed.table.columns.iter().zip(1i64..) {
        let oid = column.column_type.oid();
        let Some(found) = types::by_oid(oid) else {
            continue;
        };
        connection.execute(
            "INSERT INTO pg_catalog.pg_attribute VALUES \
             (?1, ?2, ?3, -1, ?4, ?5, 0, -1, -1, ?6, 'i', ?7, '', ?8, 0, 0, '', '', 0, 1, 0, 0, \
              NULL, NULL, NULL, NULL)",
            params![
                placed.oid,
                column.name,
                oid,
                found.len,
                position,
                (1..=8).contains(&found.len),
                if found.len > 0 { "p" } else { "x" },
                !column.nullable,
            ],
        )?;
        let numeric = match found.name {
            "int8" => Some((64, 0)),
            "float8" => Some((53, -1)),
            _ => None,
        };
        connection.execute(
            "INSERT INTO information_schema.columns \
             (table_catalog, table_schema, table_name, column_name, ordinal_position, \
              is_nullable, data_type, numeric_precision, numeric_precision_radix, \
              numeric_scale, datetime_precision, udt_catalog, udt_schema, udt_name, \
              dtd_identifier, is_self_referencing, is_identity, is_generated, is_updatable) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?1, 'pg_catalog', ?12, \
                     ?13, 'NO', 'NO', 'NEVER', 'NO')",
            params![
                shape.database,
                placed.schema,
                name,
                column.name,
                position,
                if column.nullable { "YES" } else { "NO" },
                types::display(oid),
                numeric.map(|(precision, _)| precision),
                numeric.map(|_| 2),
                numeric.and_then(|(_, scale)| (scale >= 0).then_some(scale)),
                (found.name == "timestamptz").then_some(6),
                found.name,
                position.to_string(),
            ],
        )?;
    }
    Ok(())
}
