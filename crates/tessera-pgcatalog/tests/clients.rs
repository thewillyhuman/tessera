// SPDX-License-Identifier: Apache-2.0

//! The catalogs as SQL clients ask of them (spec section 16): the
//! statements `psql`, the JDBC driver, `SQLAlchemy`, `psycopg` and
//! `asyncpg` send to connect and to show what there is, recorded from
//! the clients; those Grafana's data source and `DBeaver` are known
//! to send, which were written down and not recorded; and what a few
//! of them are to return.

use tessera_pgcatalog::{
    Answer, CatalogError, Column, ColumnType, FieldType, Schema, Shape, Table, Value, answer,
    concerns,
};

fn column(name: &str, column_type: ColumnType, nullable: bool) -> Column {
    Column {
        name: name.into(),
        column_type,
        nullable,
    }
}

/// A connection to `default` that also sees `prod`, with two tables
/// in `public` and one in `system`.
fn shape() -> Shape {
    Shape {
        database: "default".into(),
        databases: vec!["default".into(), "prod".into()],
        user: "me".into(),
        version: "9.9.9".into(),
        schemas: vec![
            Schema {
                name: "public".into(),
                tables: vec![
                    Table {
                        name: "logs".into(),
                        columns: vec![
                            column("timestamp", ColumnType::Timestamptz, false),
                            column("event_id", ColumnType::Bytea, false),
                            column("severity_number", ColumnType::Int8, true),
                            column("body", ColumnType::Text, true),
                        ],
                    },
                    Table {
                        name: "metrics".into(),
                        columns: vec![
                            column("timestamp", ColumnType::Timestamptz, false),
                            column("value", ColumnType::Float8, true),
                            column("is_monotonic", ColumnType::Bool, true),
                        ],
                    },
                ],
            },
            Schema {
                name: "system".into(),
                tables: vec![Table {
                    name: "usage".into(),
                    columns: vec![column("tenant", ColumnType::Text, true)],
                }],
            },
        ],
    }
}

fn ask(sql: &str) -> Answer {
    answer(&shape(), sql, &[]).unwrap_or_else(|error| panic!("{error}"))
}

/// The rows of `answer` as text, nulls as `∅`.
fn rows(answer: &Answer) -> Vec<Vec<String>> {
    answer
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    Value::Null => "∅".to_owned(),
                    Value::Bool(b) => b.to_string(),
                    Value::Int(n) => n.to_string(),
                    Value::Float(x) => x.to_string(),
                    Value::Text(text) => text.clone(),
                })
                .collect()
        })
        .collect()
}

fn names(answer: &Answer) -> Vec<&str> {
    answer.fields.iter().map(|f| f.name.as_str()).collect()
}

/// Every statement the clients were recorded sending is answered:
/// what a client cannot ask, it cannot show.
#[test]
fn every_statement_the_clients_send_is_answered() {
    let recorded = [
        ("psql", include_str!("statements/psql.sql")),
        ("jdbc", include_str!("statements/jdbc.sql")),
        ("python", include_str!("statements/python.sql")),
        ("tools", include_str!("statements/tools.sql")),
    ];
    let shape = shape();
    let mut failures = Vec::new();
    let mut asked = 0;
    for (client, statements) in recorded {
        for statement in statements.split("\n;;;\n") {
            let statement = statement.trim();
            if statement.is_empty() {
                continue;
            }
            asked += 1;
            assert!(
                concerns(statement),
                "{client}: not for the catalogs: {statement}"
            );
            if let Err(error) = answer(&shape, statement, &[]) {
                failures.push(format!("{client}: {error}"));
            }
        }
    }
    assert!(asked > 80, "{asked} statements");
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

#[test]
fn tables_are_listed_with_their_schema_and_owner() {
    // What `\dt` asks, in short.
    let listed = ask("SELECT n.nspname as \"Schema\", c.relname as \"Name\", \
         pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\" \
         FROM pg_catalog.pg_class c \
         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind IN ('r','p','') AND n.nspname !~ '^pg_toast' \
           AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 1,2;");
    assert_eq!(names(&listed), ["Schema", "Name", "Owner"]);
    assert_eq!(
        rows(&listed),
        [["public", "logs", "me"], ["public", "metrics", "me"]],
        "the tables of system are not on the search path"
    );
    let system = ask(
        "SELECT table_schema, table_name FROM information_schema.tables \
         WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY 1, 2",
    );
    assert_eq!(
        rows(&system),
        [
            ["public", "logs"],
            ["public", "metrics"],
            ["system", "usage"]
        ]
    );
}

#[test]
fn columns_are_described_with_their_types() {
    // What `\d logs` asks for the columns, in short.
    let described = ask(
        "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull, a.attnum \
         FROM pg_catalog.pg_attribute a \
         WHERE a.attrelid = 'public.logs'::regclass AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY a.attnum",
    );
    assert_eq!(
        names(&described),
        ["attname", "format_type", "attnotnull", "attnum"]
    );
    assert_eq!(
        rows(&described),
        [
            ["timestamp", "timestamp with time zone", "true", "1"],
            ["event_id", "bytea", "true", "2"],
            ["severity_number", "bigint", "false", "3"],
            ["body", "text", "false", "4"],
        ]
    );
    let standard = ask(
        "SELECT column_name, data_type, is_nullable, udt_name FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'metrics' ORDER BY ordinal_position",
    );
    assert_eq!(
        rows(&standard),
        [
            ["timestamp", "timestamp with time zone", "NO", "timestamptz"],
            ["value", "double precision", "YES", "float8"],
            ["is_monotonic", "boolean", "YES", "bool"],
        ]
    );
}

#[test]
fn a_field_has_the_type_of_its_catalog_column() {
    let typed = answer(
        &shape(),
        "SELECT oid, relname, relhasindex, relnatts, reltuples, relkind, \
                relnatts + 1, relname || '!', NULL \
         FROM pg_class WHERE relname = $1",
        &[Value::Text("logs".into())],
    )
    .unwrap();
    let of_fields: Vec<FieldType> = typed.fields.iter().map(|f| f.field_type).collect();
    assert_eq!(
        of_fields,
        [
            FieldType::Oid,
            FieldType::Name,
            FieldType::Bool,
            FieldType::Int2,
            FieldType::Float4,
            FieldType::Text,
            // What is computed has the type of what it gave.
            FieldType::Int8,
            FieldType::Text,
            FieldType::Text,
        ]
    );
    assert_eq!(
        typed.rows,
        [[
            Value::Int(40_000),
            Value::Text("logs".into()),
            Value::Bool(false),
            Value::Int(4),
            Value::Float(-1.0),
            Value::Text("r".into()),
            Value::Int(5),
            Value::Text("logs!".into()),
            Value::Null,
        ]]
    );
    // A parameter the client did not send is null.
    let none = answer(&shape(), "SELECT relname FROM pg_class WHERE oid = $1", &[]).unwrap();
    assert!(none.rows.is_empty());
}

#[test]
fn the_session_says_what_it_is() {
    let session = ask(
        "SELECT version(), current_database(), current_schema(), current_user, session_user, \
                current_setting('server_version_num')::int / 100",
    );
    assert_eq!(
        names(&session),
        [
            "version",
            "current_database",
            "current_schema",
            "current_user",
            "session_user",
            "?column?"
        ]
    );
    assert_eq!(
        rows(&session),
        [[
            "PostgreSQL 16.0 (Tessera 9.9.9)",
            "default",
            "public",
            "me",
            "me",
            "1600"
        ]]
    );
    let shown = ask("SHOW server_version");
    assert_eq!(
        (names(&shown), rows(&shown)),
        (vec!["server_version"], vec![vec!["16.0".to_owned()]])
    );
    assert_eq!(
        rows(&ask("show transaction isolation level")),
        [["read committed"]]
    );
    let databases = ask("SELECT datname FROM pg_database WHERE datallowconn ORDER BY 1");
    assert_eq!(rows(&databases), [["default"], ["prod"]]);
}

#[test]
fn what_tessera_has_none_of_is_empty_not_an_error() {
    for nothing in [
        "SELECT * FROM pg_catalog.pg_index WHERE indrelid = 40000",
        "SELECT conname FROM pg_constraint WHERE conrelid = 'logs'::regclass",
        "SELECT extname FROM pg_extension",
        "SELECT * FROM information_schema.views",
        // The JDBC driver's question for a primary key, which expands
        // an array into rows in a way only it writes.
        "SELECT result.TABLE_NAME, result.COLUMN_NAME, result.KEY_SEQ FROM (SELECT ct.relname AS \
         TABLE_NAME, a.attname AS COLUMN_NAME, (information_schema._pg_expandarray(i.indkey)).n AS \
         KEY_SEQ FROM pg_catalog.pg_index i) result",
    ] {
        let answer = ask(nothing);
        assert!(answer.rows.is_empty(), "{nothing}");
        assert!(!answer.fields.is_empty(), "{nothing}");
    }
    let key = ask(
        "SELECT result.TABLE_NAME, result.KEY_SEQ FROM (SELECT (information_schema.\
         _pg_expandarray(i.indkey)).n AS KEY_SEQ FROM pg_catalog.pg_index i) result",
    );
    assert_eq!(names(&key), ["table_name", "key_seq"]);
}

#[test]
fn a_statement_the_catalogs_cannot_answer_says_how_it_was_read() {
    let error = answer(&shape(), "SELECT nope FROM pg_catalog.pg_class", &[]).unwrap_err();
    let CatalogError::Statement { message, rewritten } = &error else {
        panic!("{error}");
    };
    assert!(message.contains("nope"), "{message}");
    assert_eq!(rewritten, "select nope from pg_catalog . pg_class");
}

#[test]
fn only_statements_about_the_catalogs_or_the_session_are_theirs() {
    for theirs in [
        "SHOW search_path",
        "select version()",
        "SELECT current_schema(), session_user",
        "select relname from pg_class",
        "SELECT * FROM information_schema.tables",
        "select 1 from pg_catalog.pg_namespace",
        "SELECT current_setting('server_version_num')::int/100 as version",
    ] {
        assert!(concerns(theirs), "{theirs}");
    }
    for not_theirs in [
        "SELECT count(*) FROM logs",
        "SELECT body FROM logs WHERE body LIKE 'pg_%' OR body = 'select version()'",
        "SELECT version FROM logs",
        "SELECT attr.pg_stat, l.current_user FROM logs l",
        "SELECT \"pg_class\" FROM logs",
        "select * from system.usage",
    ] {
        assert!(!concerns(not_theirs), "{not_theirs}");
    }
}
