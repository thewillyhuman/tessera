// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `PostgreSQL` wire protocol (section 16): enough for `psql`, common
//! drivers and Grafana's `PostgreSQL` data source to run SQL queries.
//!
//! The database a client connects to is the dataset its unqualified
//! table names read. Both the simple and the extended query protocol
//! are served; a prepared statement is described by planning it without
//! running it. Parameters (`$1`) are not supported yet. Session
//! statements drivers send on connect (`SET`, `BEGIN`, `COMMIT`, ...)
//! are acknowledged and ignored. Timestamps travel with `PostgreSQL`'s
//! microsecond precision.
//!
//! Authentication (SE-1) is not implemented; `tessera run` binds the
//! listener to the loopback interface.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use async_trait::async_trait;
use futures::stream;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, METADATA_DATABASE, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;
use tessera_exec::{QueryError, explain_text, query};
use tessera_ir::{BindContext, Frontend, Plan, Statement};
use tessera_opt::optimize;
use tessera_sql::SqlFrontend;
use tessera_types::{DatasetName, TenantId};
use tokio::net::TcpListener;

use crate::AppState;

/// The handlers of one node.
pub struct PgServer {
    state: AppState,
    parser: Arc<NoopQueryParser>,
}

impl PgServer {
    /// Handlers over `state`.
    #[must_use]
    pub fn new(state: AppState) -> Arc<Self> {
        Arc::new(PgServer {
            state,
            parser: Arc::new(NoopQueryParser::new()),
        })
    }

    fn context<C: ClientInfo>(&self, client: &C) -> PgWireResult<BindContext> {
        let database = client
            .metadata()
            .get(METADATA_DATABASE)
            .cloned()
            .unwrap_or_else(|| "default".into());
        let dataset = DatasetName::new(database).map_err(|e| user_error("3D000", e.to_string()))?;
        Ok(BindContext {
            tenant: TenantId::DEFAULT,
            dataset,
            now: self.state.env.clock.now(),
        })
    }

    /// Runs `text` and encodes the result in `format`.
    async fn execute<C: ClientInfo>(
        &self,
        client: &C,
        text: &str,
        format: &Format,
    ) -> PgWireResult<Response> {
        if let Some(tag) = session_statement(text) {
            return Ok(Response::Execution(Tag::new(tag)));
        }
        let ctx = self.context(client)?;
        let response = query::run(&self.state.engine, &SqlFrontend, text, &ctx)
            .await
            .map_err(|e| query_error(&e, text))?;
        match response {
            tessera_exec::Response::Rows(output) => {
                let fields = Arc::new(fields(&output.schema, format));
                let rows = encode_rows(&fields, &output.batches)?;
                Ok(Response::Query(QueryResponse::new(
                    fields,
                    stream::iter(rows),
                )))
            }
            tessera_exec::Response::Explain { plan, analysis } => {
                let fields = Arc::new(vec![FieldInfo::new(
                    "QUERY PLAN".into(),
                    None,
                    None,
                    Type::TEXT,
                    format.format_for(0),
                )]);
                let mut encoder = DataRowEncoder::new(Arc::clone(&fields));
                let mut rows = Vec::new();
                for line in explain_text(&plan, analysis.as_ref()).lines() {
                    encoder.encode_field(&line)?;
                    rows.push(Ok(encoder.take_row()));
                }
                Ok(Response::Query(QueryResponse::new(
                    fields,
                    stream::iter(rows),
                )))
            }
            tessera_exec::Response::Control(statement) => {
                let message = crate::control::execute(&self.state, &ctx.tenant, statement)
                    .await
                    .map_err(|e| user_error("42000", e.message().to_owned()))?;
                Ok(Response::Execution(Tag::new(&message)))
            }
        }
    }

    /// The row description of `text` without running it.
    fn describe<C: ClientInfo>(
        &self,
        client: &C,
        text: &str,
        format: &Format,
    ) -> PgWireResult<Vec<FieldInfo>> {
        if session_statement(text).is_some() {
            return Ok(Vec::new());
        }
        let ctx = self.context(client)?;
        let statement = SqlFrontend
            .lower(text, &ctx)
            .map_err(|e| query_error(&QueryError::Syntax(e), text))?;
        let plan: Plan = match statement {
            Statement::Query(plan) => plan,
            Statement::Control(_) => return Ok(Vec::new()),
            Statement::Explain { .. } => {
                return Ok(vec![FieldInfo::new(
                    "QUERY PLAN".into(),
                    None,
                    None,
                    Type::TEXT,
                    format.format_for(0),
                )]);
            }
        };
        let plan = optimize(plan, self.state.engine.registry())
            .map_err(|e| query_error(&QueryError::Plan(e), text))?;
        let schema = plan
            .schema(self.state.engine.registry())
            .map_err(|e| user_error("42P18", e.to_string()))?;
        Ok(fields(&schema, format))
    }
}

/// The tag to acknowledge a statement that only concerns the session.
fn session_statement(text: &str) -> Option<&'static str> {
    let first = text
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == ';')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    match first.as_str() {
        "SET" => Some("SET"),
        "RESET" => Some("RESET"),
        "BEGIN" | "START" => Some("BEGIN"),
        "COMMIT" | "END" => Some("COMMIT"),
        "ROLLBACK" => Some("ROLLBACK"),
        "DISCARD" => Some("DISCARD ALL"),
        "" => Some(""),
        _ => None,
    }
}

fn user_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}

/// SQLSTATE codes: syntax errors are `42601`, unknown names `42703`,
/// storage failures `58030` (I/O error), anything else `XX000`.
fn query_error(error: &QueryError, text: &str) -> PgWireError {
    match error {
        QueryError::Syntax(diagnostic) => user_error("42601", diagnostic.describe(text)),
        QueryError::Plan(error) => user_error("42703", error.to_string()),
        QueryError::Engine(error) => {
            tracing::warn!(error = %error, "query failed over the postgresql protocol");
            user_error("58030", error.to_string())
        }
    }
}

/// The `PostgreSQL` type of an Arrow type. Durations travel as their
/// nanosecond count.
fn pg_type(data_type: &DataType) -> Type {
    match data_type {
        DataType::Boolean => Type::BOOL,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Duration(_) => Type::INT8,
        DataType::Float32 | DataType::Float64 => Type::FLOAT8,
        DataType::Timestamp(_, _) => Type::TIMESTAMPTZ,
        DataType::Binary | DataType::FixedSizeBinary(_) => Type::BYTEA,
        _ => Type::TEXT,
    }
}

fn fields(schema: &SchemaRef, format: &Format) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            FieldInfo::new(
                f.name().clone(),
                None,
                None,
                pg_type(f.data_type()),
                format.format_for(i),
            )
        })
        .collect()
}

fn system_time(nanos: i64) -> SystemTime {
    if nanos >= 0 {
        UNIX_EPOCH + Duration::from_nanos(nanos.unsigned_abs())
    } else {
        UNIX_EPOCH - Duration::from_nanos(nanos.unsigned_abs())
    }
}

fn encode_rows(
    fields: &Arc<Vec<FieldInfo>>,
    batches: &[RecordBatch],
) -> PgWireResult<Vec<PgWireResult<DataRow>>> {
    let mut rows = Vec::new();
    let mut encoder = DataRowEncoder::new(Arc::clone(fields));
    for batch in batches {
        for i in 0..batch.num_rows() {
            for column in batch.columns() {
                encode_cell(&mut encoder, column, i)?;
            }
            rows.push(Ok(encoder.take_row()));
        }
    }
    Ok(rows)
}

fn encode_cell(encoder: &mut DataRowEncoder, column: &ArrayRef, i: usize) -> PgWireResult<()> {
    let any = column.as_any();
    if column.is_null(i) {
        return encoder.encode_field(&None::<i64>);
    }
    match column.data_type() {
        DataType::Boolean => {
            encoder.encode_field(&any.downcast_ref::<BooleanArray>().map(|a| a.value(i)))
        }
        DataType::Int32 => encoder.encode_field(
            &any.downcast_ref::<Int32Array>()
                .map(|a| i64::from(a.value(i))),
        ),
        DataType::Int64 | DataType::Duration(TimeUnit::Nanosecond) => {
            let value = any
                .downcast_ref::<Int64Array>()
                .map(|a| a.value(i))
                .or_else(|| {
                    any.downcast_ref::<arrow::array::DurationNanosecondArray>()
                        .map(|a| a.value(i))
                });
            encoder.encode_field(&value)
        }
        DataType::Float64 => {
            encoder.encode_field(&any.downcast_ref::<Float64Array>().map(|a| a.value(i)))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => encoder.encode_field(
            &any.downcast_ref::<TimestampNanosecondArray>()
                .map(|a| system_time(a.value(i))),
        ),
        DataType::FixedSizeBinary(_) => encoder.encode_field(
            &any.downcast_ref::<FixedSizeBinaryArray>()
                .map(|a| a.value(i)),
        ),
        DataType::Utf8 => {
            encoder.encode_field(&any.downcast_ref::<StringArray>().map(|a| a.value(i)))
        }
        other => encoder.encode_field(&Some(format!("<{other}>"))),
    }
}

#[async_trait]
impl SimpleQueryHandler for PgServer {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let mut responses = Vec::new();
        for statement in query.split(';').filter(|s| !s.trim().is_empty()) {
            responses.push(
                self.execute(client, statement, &Format::UnifiedText)
                    .await?,
            );
        }
        if responses.is_empty() {
            responses.push(Response::EmptyQuery);
        }
        Ok(responses)
    }
}

#[async_trait]
impl ExtendedQueryHandler for PgServer {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.parser)
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        self.execute(
            client,
            &portal.statement.statement,
            &portal.result_column_format,
        )
        .await
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let fields = self.describe(client, &statement.statement, &Format::UnifiedBinary)?;
        Ok(DescribeStatementResponse::new(Vec::new(), fields))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let fields = self.describe(
            client,
            &portal.statement.statement,
            &portal.result_column_format,
        )?;
        Ok(DescribePortalResponse::new(fields))
    }
}

impl NoopStartupHandler for PgServer {}

/// The handler set of one connection.
struct Handlers(Arc<PgServer>);

impl PgWireServerHandlers for Handlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.0)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.0)
    }

    fn startup_handler(&self) -> Arc<impl pgwire::api::auth::StartupHandler> {
        Arc::clone(&self.0)
    }
}

/// Serves connections on `listener` until `shutdown` resolves.
pub async fn serve(listener: TcpListener, state: AppState, shutdown: impl Future<Output = ()>) {
    let server = PgServer::new(state);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((socket, peer)) => {
                    let handlers = Handlers(Arc::clone(&server));
                    tokio::spawn(async move {
                        if let Err(error) = process_socket(socket, None, handlers).await {
                            tracing::debug!(%peer, error = %error, "postgresql connection ended");
                        }
                    });
                }
                Err(error) => tracing::warn!(error = %error, "postgresql accept failed"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use tokio_postgres::NoTls;
    use tower::ServiceExt;

    use super::*;
    use crate::otlp::testing::export;

    /// A node serving the protocol on a free loopback port, with three
    /// logs ingested into `prod`.
    async fn node() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let state = crate::testing::state().await;
        let app = crate::router(state.clone());
        let body = serde_json::to_vec(&export(&["first", "second", "third"])).unwrap();
        let write = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(header::CONTENT_TYPE, "application/json")
            .header(crate::otlp::DATASET_HEADER, "prod")
            .body(Body::from(body))
            .unwrap();
        assert_eq!(app.oneshot(write).await.unwrap().status(), StatusCode::OK);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve(listener, state, std::future::pending()));
        (address, server)
    }

    async fn connect(address: std::net::SocketAddr, database: &str) -> tokio_postgres::Client {
        let config = format!(
            "host={} port={} user=tessera dbname={database}",
            address.ip(),
            address.port()
        );
        let (client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
        tokio::spawn(connection);
        client
    }

    #[tokio::test]
    async fn serves_simple_and_extended_queries() {
        let (address, server) = node().await;
        let client = connect(address, "prod").await;

        // Simple protocol: text results.
        let rows = client
            .simple_query("SELECT body, attr.n AS n FROM logs ORDER BY timestamp DESC LIMIT 2")
            .await
            .unwrap();
        let bodies: Vec<String> = rows
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some(row.get(0).unwrap().to_owned())
                }
                _ => None,
            })
            .collect();
        assert_eq!(bodies, vec!["third", "second"]);

        // Extended protocol: binary results, described before running.
        let statement = client
            .prepare("SELECT count(*) AS n, max(timestamp) AS latest, min(body) FROM logs")
            .await
            .unwrap();
        assert_eq!(
            statement.columns()[0].type_(),
            &tokio_postgres::types::Type::FLOAT8
        );
        assert_eq!(
            statement.columns()[1].type_(),
            &tokio_postgres::types::Type::TIMESTAMPTZ
        );
        let row = client.query_one(&statement, &[]).await.unwrap();
        let n: f64 = row.get(0);
        let latest: SystemTime = row.get(1);
        let first: String = row.get(2);
        assert!((n - 3.0).abs() < f64::EPSILON);
        // The protocol carries microseconds: the two nanoseconds are lost.
        assert_eq!(latest, UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        assert_eq!(first, "first");

        // Session statements are acknowledged.
        client
            .simple_query("SET extra_float_digits = 3")
            .await
            .unwrap();
        client.batch_execute("BEGIN; COMMIT").await.unwrap();

        // EXPLAIN comes back as QUERY PLAN lines.
        let rows = client
            .query("EXPLAIN SELECT * FROM logs", &[])
            .await
            .unwrap();
        let first_line: String = rows[0].get(0);
        assert_eq!(first_line, "Scan: prod/log [all time) default");
        server.abort();
    }

    #[tokio::test]
    async fn the_database_is_the_dataset_and_errors_carry_sqlstate() {
        let (address, server) = node().await;
        let client = connect(address, "staging").await;
        let rows = client
            .query("SELECT count(*) FROM logs", &[])
            .await
            .unwrap();
        let n: f64 = rows[0].get(0);
        assert!(n.abs() < f64::EPSILON, "staging has no rows");
        let rows = client
            .query("SELECT count(*) FROM prod.logs", &[])
            .await
            .unwrap();
        let n: f64 = rows[0].get(0);
        assert!((n - 3.0).abs() < f64::EPSILON);

        let error = client
            .query("SELECT nope FROM logs", &[])
            .await
            .unwrap_err();
        let db_error = error.as_db_error().unwrap();
        assert_eq!(db_error.code().code(), "42601");
        assert_eq!(
            db_error.message(),
            "unknown column nope at line 1, column 8"
        );
        let error = client.simple_query("SELEC 1").await.unwrap_err();
        assert_eq!(error.as_db_error().unwrap().code().code(), "42601");
        server.abort();
    }
}
