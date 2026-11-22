// SPDX-License-Identifier: Apache-2.0

//! `PostgreSQL`'s system catalogs for Tessera (spec section 16).
//!
//! SQL clients do not only run the queries they are given: to connect
//! and to show what there is to query, `psql`, the JDBC and Python
//! drivers and the tools built on them (`DBeaver`, Grafana, `SQLAlchemy`)
//! ask `pg_catalog` and `information_schema` which databases, schemas,
//! tables, columns and types exist, with whatever SQL `PostgreSQL`
//! accepts. Tessera's own SQL is not that language and its tables are
//! not stored that way, so these questions are answered here, apart
//! from the query engine: from a [`Shape`], which says what a
//! connection can see, the catalogs are laid out in an in-memory
//! `SQLite` database, the statement is rewritten from `PostgreSQL`'s
//! dialect into its ([`rewrite`]), and what it returns is the answer.
//!
//! The catalogs describe; they hold no row of the tables themselves.
//! A statement that reads a table's rows is not one for this crate,
//! and [`concerns`] says which are.

mod fill;
mod functions;
pub mod rewrite;
mod types;

use rusqlite::Connection;
use rusqlite::types::ValueRef;

/// What a connection can see: the database it is connected to, the
/// others it could connect to, and the tables with their columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shape {
    /// The database of the connection.
    pub database: String,
    /// Every database the connection's user can connect to, the
    /// connection's own among them.
    pub databases: Vec<String>,
    /// The user of the connection.
    pub user: String,
    /// The version of the server, as `version()` appends it.
    pub version: String,
    /// The schemas, `public` first by convention.
    pub schemas: Vec<Schema>,
}

/// A schema and its tables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    /// The schema's name.
    pub name: String,
    /// Its tables.
    pub tables: Vec<Table>,
}

/// A table and its columns, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    /// The table's name.
    pub name: String,
    /// Its columns.
    pub columns: Vec<Column>,
}

/// A column of a table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    /// The column's name.
    pub name: String,
    /// The type its values travel as.
    pub column_type: ColumnType,
    /// Whether it can be null.
    pub nullable: bool,
}

/// The types a column of a table can have on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnType {
    /// `boolean`.
    Bool,
    /// `bigint`.
    Int8,
    /// `double precision`.
    Float8,
    /// `text`.
    Text,
    /// `timestamp with time zone`.
    Timestamptz,
    /// `bytea`.
    Bytea,
}

impl ColumnType {
    /// The type's oid in `pg_type`.
    #[must_use]
    pub fn oid(self) -> i64 {
        match self {
            ColumnType::Bool => 16,
            ColumnType::Bytea => 17,
            ColumnType::Int8 => 20,
            ColumnType::Text => 25,
            ColumnType::Float8 => 701,
            ColumnType::Timestamptz => 1184,
        }
    }
}

/// The type of a field of an answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldType {
    /// `boolean`.
    Bool,
    /// `smallint`.
    Int2,
    /// `integer`.
    Int4,
    /// `bigint`.
    Int8,
    /// `oid`.
    Oid,
    /// `real`.
    Float4,
    /// `double precision`.
    Float8,
    /// `text`.
    Text,
    /// `name`.
    Name,
}

impl FieldType {
    /// The type a catalog column declared as `declared` has; text for
    /// what has no closer counterpart.
    fn declared(declared: &str) -> FieldType {
        match declared.to_ascii_uppercase().as_str() {
            "BOOL" => FieldType::Bool,
            "INT2" => FieldType::Int2,
            "INT4" => FieldType::Int4,
            "INT8" => FieldType::Int8,
            "OID" => FieldType::Oid,
            "FLOAT4" => FieldType::Float4,
            "FLOAT8" => FieldType::Float8,
            "NAME" => FieldType::Name,
            _ => FieldType::Text,
        }
    }

    /// The type of a value that is no catalog column: what an
    /// expression gave.
    fn of(value: &ValueRef<'_>) -> Option<FieldType> {
        match value {
            ValueRef::Null => None,
            ValueRef::Integer(_) => Some(FieldType::Int8),
            ValueRef::Real(_) => Some(FieldType::Float8),
            ValueRef::Text(_) | ValueRef::Blob(_) => Some(FieldType::Text),
        }
    }
}

/// A value of an answer or of a parameter.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// SQL `NULL`.
    Null,
    /// A boolean.
    Bool(bool),
    /// An integer.
    Int(i64),
    /// A floating-point number.
    Float(f64),
    /// Text.
    Text(String),
}

impl Value {
    /// `value` as a field of type `field` holds it.
    fn read(value: ValueRef<'_>, field: FieldType) -> Value {
        let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
        match (field, value) {
            (FieldType::Bool, ValueRef::Integer(n)) => Value::Bool(n != 0),
            (FieldType::Bool, ValueRef::Text(bytes)) => Value::Bool(functions::truth(&text(bytes))),
            (
                FieldType::Int2 | FieldType::Int4 | FieldType::Int8 | FieldType::Oid,
                ValueRef::Integer(n),
            ) => Value::Int(n),
            (
                FieldType::Int2 | FieldType::Int4 | FieldType::Int8 | FieldType::Oid,
                ValueRef::Text(bytes),
            ) => text(bytes).trim().parse().map_or(Value::Null, Value::Int),
            #[allow(clippy::cast_precision_loss)]
            (FieldType::Float4 | FieldType::Float8, ValueRef::Integer(n)) => Value::Float(n as f64),
            (FieldType::Float4 | FieldType::Float8, ValueRef::Real(x)) => Value::Float(x),
            (FieldType::Text | FieldType::Name, ValueRef::Integer(n)) => Value::Text(n.to_string()),
            (FieldType::Text | FieldType::Name, ValueRef::Real(x)) => Value::Text(x.to_string()),
            (FieldType::Text | FieldType::Name, ValueRef::Text(bytes) | ValueRef::Blob(bytes)) => {
                Value::Text(text(bytes))
            }
            // A null, a number that is not one, or a blob where none
            // belongs.
            _ => Value::Null,
        }
    }

    fn to_sql(&self) -> rusqlite::types::Value {
        match self {
            Value::Null => rusqlite::types::Value::Null,
            Value::Bool(b) => rusqlite::types::Value::Integer(i64::from(*b)),
            Value::Int(n) => rusqlite::types::Value::Integer(*n),
            Value::Float(x) => rusqlite::types::Value::Real(*x),
            Value::Text(text) => rusqlite::types::Value::Text(text.clone()),
        }
    }
}

/// A field of an answer: its name and type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    /// The field's name.
    pub name: String,
    /// The type of its values.
    pub field_type: FieldType,
}

/// What a catalog statement returns.
#[derive(Clone, Debug, PartialEq)]
pub struct Answer {
    /// The fields, in order.
    pub fields: Vec<Field>,
    /// The rows, each with a value per field.
    pub rows: Vec<Vec<Value>>,
}

/// Why a catalog statement was not answered.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// The statement is not one the catalogs can answer: it is not
    /// `PostgreSQL` the rewrite knows, or it names what they lack.
    #[error("{message} (catalog query, read as: {rewritten})")]
    Statement {
        /// What the store said.
        message: String,
        /// The statement as it was given to the store.
        rewritten: String,
    },
    /// The catalogs could not be laid out.
    #[error("the system catalogs could not be built: {0}")]
    Build(String),
}

/// The version of `PostgreSQL` the catalogs are those of: what
/// `server_version` shows and what a server says at startup.
pub const SERVER_VERSION: &str = "16.0";

/// The functions that answer for the session or the catalogs and take
/// brackets: a statement that calls one is a catalog statement.
const FUNCTIONS: [&str; 22] = [
    "version",
    "current_schema",
    "current_schemas",
    "current_database",
    "current_setting",
    "set_config",
    "format_type",
    "obj_description",
    "col_description",
    "shobj_description",
    "has_table_privilege",
    "has_schema_privilege",
    "has_database_privilege",
    "txid_current",
    "inet_server_addr",
    "inet_server_port",
    "inet_client_addr",
    "getdatabaseencoding",
    "quote_ident",
    "to_regclass",
    "to_regtype",
    "current_user",
];

/// The session's values that are written without brackets.
const VALUES: [&str; 5] = [
    "current_user",
    "session_user",
    "current_catalog",
    "current_role",
    "current_schema",
];

/// Whether `sql` is a statement for the catalogs: it shows a setting,
/// names `pg_catalog`, `information_schema` or a table or function
/// whose name begins `pg_`, or asks for a value of the session. What
/// stands inside a string or a quoted name does not count, nor does a
/// name after a dot other than those schemas', which is a column.
#[must_use]
pub fn concerns(sql: &str) -> bool {
    let words = rewrite::words(sql);
    if words.first().is_some_and(|w| w.text == "show") {
        return true;
    }
    words.iter().any(|word| {
        let text = word.text.as_str();
        if text == "pg_catalog" || text == "information_schema" {
            return true;
        }
        if word.qualified {
            return false;
        }
        text.starts_with("pg_")
            || (word.called && FUNCTIONS.contains(&text))
            || (!word.called && VALUES.contains(&text))
    })
}

/// Answers the catalog statement `sql` for a connection that sees
/// `shape`, its `$n` parameters bound to `parameters` in order; the
/// ones past the end are null.
///
/// The types of the fields are the catalog columns' where a field is
/// one, and what the first row holds where it is an expression; a
/// field no row gives a value for is text.
///
/// # Errors
///
/// `Statement` when the statement cannot be answered, with the text
/// it was rewritten to.
pub fn answer(shape: &Shape, sql: &str, parameters: &[Value]) -> Result<Answer, CatalogError> {
    if let Some(empty) = known_empty(sql) {
        return Ok(empty);
    }
    let build = |error: rusqlite::Error| CatalogError::Build(error.to_string());
    let connection = Connection::open_in_memory().map_err(build)?;
    fill::lay_out(&connection, shape).map_err(build)?;
    functions::register(&connection, shape).map_err(build)?;
    let rewritten = rewrite::rewrite(sql);
    run(&connection, &rewritten, parameters).map_err(|error| CatalogError::Statement {
        message: error.to_string(),
        rewritten,
    })
}

/// The answer to a statement that is known to return no row here and
/// is written in a way the rewrite does not follow: the JDBC driver
/// asks for a table's primary key and indexes by expanding an array
/// into rows and reading the row's fields, and Tessera's tables have
/// neither a key nor an index. Its fields are those the statement
/// names, as text.
fn known_empty(sql: &str) -> Option<Answer> {
    if !sql.contains("_pg_expandarray") {
        return None;
    }
    Some(Answer {
        fields: rewrite::field_names(sql)
            .into_iter()
            .map(|name| Field {
                name,
                field_type: FieldType::Text,
            })
            .collect(),
        rows: Vec::new(),
    })
}

fn run(connection: &Connection, sql: &str, parameters: &[Value]) -> rusqlite::Result<Answer> {
    let mut statement = connection.prepare(sql)?;
    let declared: Vec<(String, Option<FieldType>)> = statement
        .columns()
        .iter()
        .map(|column| {
            (
                column.name().to_owned(),
                column.decl_type().map(FieldType::declared),
            )
        })
        .collect();
    for position in 1..=statement.parameter_count() {
        let value = parameters.get(position - 1).unwrap_or(&Value::Null);
        statement.raw_bind_parameter(position, value.to_sql())?;
    }
    let mut raw: Vec<Vec<rusqlite::types::Value>> = Vec::new();
    let mut rows = statement.raw_query();
    while let Some(row) = rows.next()? {
        let mut values = Vec::with_capacity(declared.len());
        for i in 0..declared.len() {
            values.push(row.get::<_, rusqlite::types::Value>(i)?);
        }
        raw.push(values);
    }
    let fields: Vec<Field> = declared
        .into_iter()
        .enumerate()
        .map(|(i, (name, declared))| Field {
            name,
            field_type: declared
                .or_else(|| {
                    raw.iter()
                        .find_map(|row| FieldType::of(&ValueRef::from(&row[i])))
                })
                .unwrap_or(FieldType::Text),
        })
        .collect();
    let rows = raw
        .iter()
        .map(|row| {
            row.iter()
                .zip(&fields)
                .map(|(value, field)| Value::read(ValueRef::from(value), field.field_type))
                .collect()
        })
        .collect();
    Ok(Answer { fields, rows })
}
