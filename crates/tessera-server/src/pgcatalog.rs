// SPDX-License-Identifier: Apache-2.0

//! The system catalogs over the `PostgreSQL` protocol (section 16):
//! what a connection can see, as [`tessera_pgcatalog`] is to describe
//! it, and its answers as rows on the wire.
//!
//! A database is a dataset of the connection's tenant. Every database
//! has the same tables: the views over a dataset's events in the
//! schema `public`, and the tenant's control tables in `system`.

use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use futures::stream;
use pgwire::api::Type;
use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response};
use pgwire::error::PgWireResult;
use tessera_catalog::Principal;
use tessera_ir::{BindContext, Statement};
use tessera_object::Key;
use tessera_opt::optimize;
use tessera_pgcatalog::{Answer, Column, ColumnType, FieldType, Schema, Shape, Table, Value};
use tessera_sql::Parameter;
use tessera_types::DatasetName;

use crate::AppState;

/// The views over a dataset's events, the tables of `public`.
const EVENT_TABLES: [&str; 4] = ["logs", "spans", "metrics", "alerts"];

/// The tables of `system` besides the control tables.
const SYSTEM_TABLES: [&str; 2] = ["usage", "log_patterns"];

/// What the connection of `principal` to the database of `ctx` sees.
/// The datasets it may read are asked of the store, which is one
/// listing of the names under its tenant.
pub(crate) async fn shape(
    state: &AppState,
    principal: &Principal,
    ctx: &BindContext,
    user: &str,
) -> Shape {
    let database = ctx.dataset.to_string();
    let mut databases = datasets(state, ctx).await;
    databases.retain(|dataset| principal.may_read(dataset));
    if !databases.contains(&database) {
        databases.push(database.clone());
        databases.sort();
    }
    let tables = |names: &[&str], prefix: &str| -> Vec<Table> {
        names
            .iter()
            .filter_map(|name| {
                let schema = schema_of(state, ctx, &format!("SELECT * FROM {prefix}{name}"))?;
                Some(Table {
                    name: (*name).to_owned(),
                    columns: columns(&schema),
                })
            })
            .collect()
    };
    let system: Vec<&str> = tessera_ir::system::TABLES
        .iter()
        .chain(&SYSTEM_TABLES)
        .copied()
        .collect();
    Shape {
        database,
        databases,
        user: user.to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        schemas: vec![
            Schema {
                name: "public".into(),
                tables: tables(&EVENT_TABLES, ""),
            },
            Schema {
                name: "system".into(),
                tables: tables(&system, "system."),
            },
        ],
    }
}

/// The names of the tenant's datasets: what lies under its prefix and
/// is named as a dataset is. A store that cannot be listed leaves the
/// connection its own database.
async fn datasets(state: &AppState, ctx: &BindContext) -> Vec<String> {
    let Ok(prefix) = Key::new(ctx.tenant.as_str()) else {
        return Vec::new();
    };
    match state.pools.root().store().list_names(&prefix).await {
        Ok(names) => names
            .into_iter()
            .filter(|name| DatasetName::new(name.clone()).is_ok())
            .collect(),
        Err(error) => {
            tracing::debug!(error = %error, "datasets not listed for the catalogs");
            Vec::new()
        }
    }
}

/// The schema of what `query` returns, without running it; none for a
/// table the planner does not know.
fn schema_of(state: &AppState, ctx: &BindContext, query: &str) -> Option<SchemaRef> {
    let Statement::Query(plan) = tessera_sql::describe(query, ctx).ok()?.statement else {
        return None;
    };
    let plan = optimize(plan, state.engine.registry()).ok()?;
    plan.schema(state.engine.registry()).ok()
}

fn columns(schema: &SchemaRef) -> Vec<Column> {
    schema
        .fields()
        .iter()
        .map(|field| Column {
            name: field.name().clone(),
            column_type: column_type(field.data_type()),
            nullable: field.is_nullable(),
        })
        .collect()
}

/// The type a column of `data_type` travels as, which is what the
/// listener's own rows are sent as.
fn column_type(data_type: &DataType) -> ColumnType {
    match data_type {
        DataType::Boolean => ColumnType::Bool,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Duration(_) => ColumnType::Int8,
        DataType::Float32 | DataType::Float64 => ColumnType::Float8,
        DataType::Timestamp(_, _) => ColumnType::Timestamptz,
        DataType::Binary | DataType::FixedSizeBinary(_) => ColumnType::Bytea,
        _ => ColumnType::Text,
    }
}

/// A bound parameter as the catalogs take it.
pub(crate) fn value(parameter: &Parameter) -> Value {
    match parameter {
        Parameter::Null => Value::Null,
        Parameter::Bool(b) => Value::Bool(*b),
        Parameter::Int(n) => Value::Int(*n),
        Parameter::Float(x) => Value::Float(*x),
        Parameter::Text(text) => Value::Text(text.clone()),
        Parameter::Timestamp(at) => Value::Text(at.to_string()),
    }
}

fn wire_type(field_type: FieldType) -> Type {
    match field_type {
        FieldType::Bool => Type::BOOL,
        FieldType::Int2 => Type::INT2,
        FieldType::Int4 => Type::INT4,
        FieldType::Int8 => Type::INT8,
        FieldType::Oid => Type::OID,
        FieldType::Float4 => Type::FLOAT4,
        FieldType::Float8 => Type::FLOAT8,
        FieldType::Text => Type::TEXT,
        FieldType::Name => Type::NAME,
    }
}

/// The row description of `answer` in `format`.
pub(crate) fn fields(answer: &Answer, format: &Format) -> Vec<FieldInfo> {
    answer
        .fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            FieldInfo::new(
                field.name.clone(),
                None,
                None,
                wire_type(field.field_type),
                format.format_for(i),
            )
        })
        .collect()
}

/// `answer` as the rows of a query's response in `format`.
pub(crate) fn response(answer: &Answer, format: &Format) -> PgWireResult<Response> {
    let fields = Arc::new(fields(answer, format));
    let mut encoder = DataRowEncoder::new(Arc::clone(&fields));
    let mut rows = Vec::with_capacity(answer.rows.len());
    for row in &answer.rows {
        for (value, field) in row.iter().zip(&answer.fields) {
            encode(&mut encoder, value, field.field_type)?;
        }
        rows.push(Ok(encoder.take_row()));
    }
    Ok(Response::Query(QueryResponse::new(
        fields,
        stream::iter(rows),
    )))
}

/// Encodes `value` as a field of `field_type`: the catalogs read
/// every value as its field's type, so the two agree.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn encode(encoder: &mut DataRowEncoder, value: &Value, field_type: FieldType) -> PgWireResult<()> {
    match (value, field_type) {
        (Value::Bool(b), _) => encoder.encode_field(b),
        (Value::Int(n), FieldType::Int2) => encoder.encode_field(&(*n as i16)),
        (Value::Int(n), FieldType::Int4) => encoder.encode_field(&(*n as i32)),
        (Value::Int(n), FieldType::Oid) => encoder.encode_field(&(*n as u32)),
        (Value::Int(n), _) => encoder.encode_field(n),
        (Value::Float(x), FieldType::Float4) => encoder.encode_field(&(*x as f32)),
        (Value::Float(x), _) => encoder.encode_field(x),
        (Value::Text(text), _) => encoder.encode_field(text),
        (Value::Null, _) => encoder.encode_field(&None::<i64>),
    }
}
