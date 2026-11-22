// SPDX-License-Identifier: Apache-2.0

//! The `PostgreSQL` wire protocol (section 16): enough for `psql`, common
//! drivers and the tools built on them (Grafana's `PostgreSQL` data
//! source, `DBeaver`, `SQLAlchemy`) to connect, to browse what there is
//! and to run SQL queries.
//!
//! The database a client connects to is the dataset its unqualified
//! table names read. Both the simple and the extended query protocol
//! are served; a prepared statement is described by planning it without
//! running it, and its `$n` parameters bind with the types the
//! statement implies. Session statements drivers send on connect
//! (`SET`, `BEGIN`, `COMMIT`, ...) are acknowledged and ignored.
//! Timestamps travel with `PostgreSQL`'s microsecond precision.
//!
//! What a client asks of the system catalogs, `pg_catalog` and
//! `information_schema`, and of the session (`SHOW`, `version()`,
//! `current_schema()`) is not Tessera's SQL and is answered by
//! [`tessera_pgcatalog`], from what the connection can see: the
//! datasets its key may read as databases, the views over a dataset's
//! events in the schema `public` and the tenant's control tables in
//! `system`.
//!
//! Where anonymous access is off, the password is an API key (SE-1)
//! and the user name is not looked at.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream;
use pgwire::api::auth::{self, DefaultServerParameterProvider, LoginInfo, StartupHandler};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, METADATA_DATABASE, PgWireServerHandlers, Type};
use pgwire::api::{PgWireConnectionState, PidSecretKeyGenerator, RandomPidSecretKeyGenerator};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::{TlsAcceptor, process_socket};
use tessera_catalog::Principal;
use tessera_exec::{QueryError, RunOptions, explain_text, query};
use tessera_ir::{BindContext, Plan, Statement};
use tessera_opt::optimize;
use tessera_sql::{BoundSql, Parameter};
use tessera_types::{DatasetName, TenantId, Timestamp};
use tokio::net::TcpListener;

use crate::AppState;

/// The handlers of one node.
pub struct PgServer {
    state: AppState,
    parser: Arc<Parser>,
}

/// A statement as the extended protocol stores it: its text, and the
/// parameter types the client declared, none where it left them to
/// the server.
#[derive(Clone, Debug)]
pub struct Prepared {
    text: String,
    declared: Vec<Option<Type>>,
}

/// Keeps the text and the declared parameter types; the handlers
/// describe statements themselves, with the client's context.
#[derive(Debug, Default)]
pub struct Parser;

#[async_trait]
impl QueryParser for Parser {
    type Statement = Prepared;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Option<Prepared>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(Some(Prepared {
            text: sql.to_owned(),
            declared: types.to_vec(),
        }))
    }

    fn get_parameter_types(&self, statement: &Prepared) -> PgWireResult<Vec<Type>> {
        Ok(statement
            .declared
            .iter()
            .map(|declared| declared.clone().unwrap_or(Type::UNKNOWN))
            .collect())
    }

    fn get_result_schema(
        &self,
        _statement: &Prepared,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        Ok(Vec::new())
    }
}

impl PgServer {
    /// Handlers over `state`.
    #[must_use]
    pub fn new(state: AppState) -> Arc<Self> {
        Arc::new(PgServer {
            state,
            parser: Arc::new(Parser),
        })
    }

    /// `prepared` with its parameters unbound: the type of each, what
    /// the client declared, else what the statement implies, else text;
    /// and the statement, none for one that only concerns the session.
    async fn lower_unbound<C: ClientInfo>(
        &self,
        client: &C,
        prepared: &Prepared,
    ) -> PgWireResult<(Vec<Type>, Option<Statement>)> {
        if session_statement(&prepared.text).is_some() {
            return Ok((Vec::new(), None));
        }
        if tessera_pgcatalog::concerns(&prepared.text) {
            // The catalogs take any value: each parameter is what the
            // client declared, else text.
            let types = (0..tessera_pgcatalog::rewrite::parameters(&prepared.text))
                .map(|i| match prepared.declared.get(i) {
                    Some(Some(declared)) if *declared != Type::UNKNOWN => declared.clone(),
                    _ => Type::TEXT,
                })
                .collect();
            return Ok((types, None));
        }
        let (ctx, _) = self.context(client).await?;
        let described = tessera_sql::describe(&prepared.text, &ctx)
            .map_err(|e| query_error(&QueryError::Syntax(e), &prepared.text))?;
        let types = described
            .parameters
            .iter()
            .enumerate()
            .map(|(i, inferred)| match prepared.declared.get(i) {
                Some(Some(declared)) if *declared != Type::UNKNOWN => declared.clone(),
                _ => inferred.as_ref().map_or(Type::TEXT, pg_type),
            })
            .collect();
        Ok((types, Some(described.statement)))
    }

    /// The principal of a connection: the key it gave as its password,
    /// or anonymous.
    async fn principal<C: ClientInfo>(&self, client: &C) -> PgWireResult<Principal> {
        match client.metadata().get(KEY_METADATA) {
            Some(key) => crate::auth::with_key(&self.state, key).await,
            None => crate::auth::anonymous(&self.state),
        }
        .map_err(|e| user_error("28000", e.message().to_owned()))
    }

    async fn context<C: ClientInfo>(&self, client: &C) -> PgWireResult<(BindContext, Principal)> {
        let principal = self.principal(client).await?;
        let database = client
            .metadata()
            .get(METADATA_DATABASE)
            .cloned()
            .unwrap_or_else(|| "default".into());
        let dataset = DatasetName::new(database).map_err(|e| user_error("3D000", e.to_string()))?;
        let tenant = if principal.cluster_admin {
            TenantId::DEFAULT
        } else {
            principal.tenant.clone()
        };
        let mut ctx = BindContext::new(tenant, dataset, self.state.env.clock.now());
        ctx.tenants = crate::params::tenants(&self.state, &principal);
        ctx.prices = crate::params::prices(&self.state);
        Ok((ctx, principal))
    }

    /// What the system catalogs answer to `text` for this connection
    /// (see [`tessera_pgcatalog`]), its parameters bound to
    /// `parameters`: nulls where a client only asks what the statement
    /// returns.
    async fn catalog<C: ClientInfo>(
        &self,
        client: &C,
        text: &str,
        parameters: &[Parameter],
    ) -> PgWireResult<tessera_pgcatalog::Answer> {
        let (ctx, principal) = self.context(client).await?;
        let user = LoginInfo::from_client_info(client)
            .user()
            .map_or_else(|| principal.name.clone(), str::to_owned);
        tracing::debug!(statement = %text, "catalog statement");
        let shape = crate::pgcatalog::shape(&self.state, &principal, &ctx, &user).await;
        let values: Vec<_> = parameters.iter().map(crate::pgcatalog::value).collect();
        let statement = text.to_owned();
        // The catalogs are built and asked on a thread of their own:
        // neither waits for anything, both take a moment.
        tokio::task::spawn_blocking(move || tessera_pgcatalog::answer(&shape, &statement, &values))
            .await
            .map_err(|e| user_error("XX000", e.to_string()))?
            .map_err(|e| {
                tracing::debug!(error = %e, statement = %text, "catalog statement not answered");
                user_error("42601", e.to_string())
            })
    }

    /// Runs `text` with its `$n` parameters bound to `parameters` and
    /// encodes the result in `format`.
    async fn execute<C: ClientInfo>(
        &self,
        client: &C,
        text: &str,
        format: &Format,
        parameters: Vec<Parameter>,
    ) -> PgWireResult<Response> {
        if let Some(tag) = session_statement(text) {
            return Ok(Response::Execution(Tag::new(tag)));
        }
        if tessera_pgcatalog::concerns(text) {
            let answer = self.catalog(client, text, &parameters).await?;
            return crate::pgcatalog::response(&answer, format);
        }
        let (ctx, principal) = self.context(client).await?;
        let started = std::time::Instant::now();
        let reader = principal.clone();
        let options = RunOptions {
            readable: Some(Arc::new(move |dataset: &DatasetName| {
                reader.may_read(dataset.as_str())
            })),
            row_filter: crate::params::row_filter(&principal, &ctx),
            limits: Some(crate::params::limits(&self.state, &ctx.tenant)),
            ..RunOptions::default()
        };
        let frontend = BoundSql { parameters };
        let response = query::run_with(&self.state.engine, &frontend, text, &ctx, options)
            .await
            .map_err(|e| query_error(&e, text))?;
        match response {
            tessera_exec::Response::Rows(output) => {
                crate::workload::record_uses(
                    &self.state,
                    output.uses.clone(),
                    "sql",
                    "ad hoc",
                    started.elapsed(),
                    output.fingerprint,
                    principal.team(),
                );
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
                if !principal.is_admin() {
                    return Err(user_error(
                        "42501",
                        "these credentials do not administer the tenant".into(),
                    ));
                }
                let message = crate::control::execute(&self.state, &ctx.tenant, statement)
                    .await
                    .map_err(|e| user_error("42000", e.message().to_owned()))?;
                crate::audit::record(&self.state, &principal, &ctx.tenant, "control", &message);
                Ok(Response::Execution(Tag::new(&message)))
            }
        }
    }

    /// The row description of `statement`, lowered from `text`, without
    /// running it.
    fn fields_of(
        &self,
        statement: Option<Statement>,
        text: &str,
        format: &Format,
    ) -> PgWireResult<Vec<FieldInfo>> {
        let plan: Plan = match statement {
            Some(Statement::Query(plan)) => plan,
            None | Some(Statement::Control(_)) => return Ok(Vec::new()),
            Some(Statement::Explain { .. }) => {
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
/// insufficient privilege `42501`, storage failures `58030` (I/O
/// error), anything else `XX000`.
fn query_error(error: &QueryError, text: &str) -> PgWireError {
    match error {
        QueryError::Syntax(diagnostic) => user_error("42601", diagnostic.describe(text)),
        QueryError::Plan(error) => user_error("42703", error.to_string()),
        QueryError::Forbidden { .. } => user_error("42501", error.to_string()),
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
                self.execute(client, statement, &Format::UnifiedText, Vec::new())
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
    type Statement = Prepared;
    type QueryParser = Parser;

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
        let prepared = &portal.statement.statement;
        let (types, _) = self.lower_unbound(client, prepared).await?;
        let parameters = bind(portal, &types)?;
        self.execute(
            client,
            &prepared.text,
            &portal.result_column_format,
            parameters,
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
        let prepared = &statement.statement;
        let (types, lowered) = self.lower_unbound(client, prepared).await?;
        let fields = if tessera_pgcatalog::concerns(&prepared.text) {
            let answer = self.catalog(client, &prepared.text, &[]).await?;
            crate::pgcatalog::fields(&answer, &Format::UnifiedBinary)
        } else {
            self.fields_of(lowered, &prepared.text, &Format::UnifiedBinary)?
        };
        Ok(DescribeStatementResponse::new(types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let prepared = &portal.statement.statement;
        let (types, lowered) = self.lower_unbound(client, prepared).await?;
        let format = &portal.result_column_format;
        let fields = if tessera_pgcatalog::concerns(&prepared.text) {
            let parameters = bind(portal, &types)?;
            let answer = self.catalog(client, &prepared.text, &parameters).await?;
            crate::pgcatalog::fields(&answer, format)
        } else {
            self.fields_of(lowered, &prepared.text, format)?
        };
        Ok(DescribePortalResponse::new(fields))
    }
}

/// The values a portal binds, read in `types`, one per parameter.
fn bind(portal: &Portal<Prepared>, types: &[Type]) -> PgWireResult<Vec<Parameter>> {
    if portal.parameters.len() != types.len() {
        return Err(user_error(
            "08P01",
            format!(
                "bind message supplies {} parameters, but the statement requires {}",
                portal.parameters.len(),
                types.len()
            ),
        ));
    }
    types
        .iter()
        .enumerate()
        .map(|(index, pg_type)| parameter(portal, index, pg_type))
        .collect()
}

/// The parameter at `index`, read as `pg_type` in the portal's format:
/// booleans, integers, floats, timestamps and text; a null is a null.
fn parameter(portal: &Portal<Prepared>, index: usize, pg_type: &Type) -> PgWireResult<Parameter> {
    let value = match pg_type.name() {
        "bool" => portal
            .parameter::<bool>(index, pg_type)?
            .map(Parameter::Bool),
        "int2" => portal
            .parameter::<i16>(index, pg_type)?
            .map(|v| Parameter::Int(i64::from(v))),
        "int4" => portal
            .parameter::<i32>(index, pg_type)?
            .map(|v| Parameter::Int(i64::from(v))),
        "int8" => portal.parameter::<i64>(index, pg_type)?.map(Parameter::Int),
        "float4" => portal
            .parameter::<f32>(index, pg_type)?
            .map(|v| Parameter::Float(f64::from(v))),
        "float8" => portal
            .parameter::<f64>(index, pg_type)?
            .map(Parameter::Float),
        "timestamp" | "timestamptz" => portal
            .parameter::<SystemTime>(index, pg_type)?
            .map(|t| Parameter::Timestamp(Timestamp::from_system_time(t))),
        "text" | "varchar" | "bpchar" | "name" | "unknown" => portal
            .parameter::<String>(index, pg_type)?
            .map(Parameter::Text),
        other => {
            return Err(user_error(
                "0A000",
                format!(
                    "parameters of type {other} are not supported; cast the value in the query"
                ),
            ));
        }
    };
    Ok(value.unwrap_or(Parameter::Null))
}

/// The connection metadata entry that holds the key a client gave.
const KEY_METADATA: &str = "tessera.key";

#[async_trait]
impl StartupHandler for PgServer {
    /// Without anonymous access, the client is asked for a cleartext
    /// password, which must be an API key; the key is kept with the
    /// connection and resolved per statement, so a revoked key stops
    /// working within the keep time.
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                auth::protocol_negotiation(client, startup).await?;
                auth::save_startup_parameters_to_metadata(client, startup);
                if self.state.auth.allows_anonymous() {
                    self.finish(client).await?;
                } else {
                    client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    client
                        .send(PgWireBackendMessage::Authentication(
                            Authentication::CleartextPassword,
                        ))
                        .await?;
                }
            }
            PgWireFrontendMessage::PasswordMessageFamily(password) => {
                let password = password.into_password()?;
                if let Err(error) = crate::auth::with_key(&self.state, &password.password).await {
                    tracing::debug!(error = %error.message(), "postgresql login refused");
                    let user = LoginInfo::from_client_info(client)
                        .user()
                        .unwrap_or_default()
                        .to_owned();
                    return Err(PgWireError::InvalidPassword(user));
                }
                client
                    .metadata_mut()
                    .insert(KEY_METADATA.into(), password.password);
                self.finish(client).await?;
            }
            _ => {}
        }
        Ok(())
    }
}

impl PgServer {
    async fn finish<C>(&self, client: &mut C) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let (pid, secret_key) = RandomPidSecretKeyGenerator::default().generate(client);
        client.set_pid_and_secret_key(pid, secret_key);
        // The version the catalogs answer with: drivers choose what
        // they ask by it.
        let mut parameters = DefaultServerParameterProvider::default();
        tessera_pgcatalog::SERVER_VERSION.clone_into(&mut parameters.server_version);
        auth::finish_authentication(client, &parameters).await
    }
}

/// The handler set of one connection.
struct Handlers(Arc<PgServer>);

impl PgWireServerHandlers for Handlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.0)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.0)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::clone(&self.0)
    }
}

/// Serves connections on `listener` until `shutdown` resolves, over
/// TLS when `acceptor` is given (SE-3).
pub async fn serve(
    listener: TcpListener,
    state: AppState,
    acceptor: Option<TlsAcceptor>,
    shutdown: impl Future<Output = ()>,
) {
    let server = PgServer::new(state);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            accepted = listener.accept() => match accepted {
                Ok((socket, peer)) => {
                    let handlers = Handlers(Arc::clone(&server));
                    let acceptor = acceptor.clone();
                    tokio::spawn(async move {
                        if let Err(error) = process_socket(socket, acceptor, handlers).await {
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
        let server = tokio::spawn(serve(listener, state, None, std::future::pending()));
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
    async fn without_anonymous_access_the_password_must_be_an_api_key() {
        let mut state = crate::testing::state().await;
        let issued = tessera_catalog::auth::issue(
            &tessera_catalog::TenantName::new("default").unwrap(),
            "psql",
            vec![tessera_catalog::AccessRole::READER.to_owned()],
            None,
            state.env.clock.now(),
            &mut state.env.random.clone(),
        );
        state
            .controls
            .open(&TenantId::DEFAULT)
            .put(
                tessera_catalog::Table::ApiKeys,
                &issued.record.id,
                serde_json::to_value(&issued.record).unwrap(),
            )
            .await
            .unwrap();
        state.auth = Arc::new(crate::Auth::new(false));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve(listener, state, None, std::future::pending()));

        let base = format!("host={} port={} dbname=prod", address.ip(), address.port());
        let refused = tokio_postgres::connect(&format!("{base} user=tessera"), NoTls).await;
        assert!(refused.is_err(), "no password");
        let refused = tokio_postgres::connect(
            &format!("{base} user=tessera password=tsk_default_x_y"),
            NoTls,
        )
        .await;
        assert!(refused.is_err(), "a wrong key");
        let (client, connection) = tokio_postgres::connect(
            &format!("{base} user=tessera password={}", issued.text),
            NoTls,
        )
        .await
        .unwrap();
        tokio::spawn(connection);
        let rows = client
            .simple_query("SELECT count(*) FROM logs")
            .await
            .unwrap();
        assert!(!rows.is_empty());
        let denied = client.simple_query("CREATE ALERT a AS SELECT 1").await;
        assert!(denied.is_err(), "a reader does not administer");
        server.abort();
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

    /// Section 16: a client that browses asks the catalogs, with
    /// `PostgreSQL`'s own SQL, what databases, tables and columns
    /// there are, and reads the answers by their types.
    #[tokio::test]
    async fn clients_browse_the_catalogs() {
        let (address, server) = node().await;
        let client = connect(address, "prod").await;

        let shown = client.simple_query("SHOW server_version").await.unwrap();
        let version = shown.iter().find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
            _ => None,
        });
        assert_eq!(version.as_deref(), Some("16.0"));

        // Extended protocol, a parameter, and fields of the catalogs'
        // own types: an oid, a name, a boolean.
        let tables = client
            .query(
                "SELECT c.oid, c.relname, c.relhasindex \
                 FROM pg_catalog.pg_class c \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relkind = 'r' ORDER BY c.relname",
                &[&"public"],
            )
            .await
            .unwrap();
        let names: Vec<String> = tables.iter().map(|row| row.get(1)).collect();
        assert_eq!(names, ["alerts", "logs", "metrics", "spans"]);
        let oid: u32 = tables[1].get(0);
        let indexed: bool = tables[1].get(2);
        assert!(oid > 0 && !indexed);

        // The connection's dataset is its database; the columns are
        // the ones a query on the table returns.
        let database: String = client
            .query_one("SELECT current_database()", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(database, "prod");
        let column = client
            .query_one(
                "SELECT data_type, is_nullable FROM information_schema.columns \
                 WHERE table_schema = 'public' AND table_name = 'logs' AND column_name = 'body'",
                &[],
            )
            .await
            .unwrap();
        let (data_type, nullable): (String, String) = (column.get(0), column.get(1));
        assert_eq!((data_type.as_str(), nullable.as_str()), ("text", "YES"));
        let system = client
            .query(
                "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'system' \
                 AND tablename IN ('usage', 'alert_rules')",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(system.len(), 2);

        // A query on a table is the engine's, whatever its strings say.
        let rows = client
            .query("SELECT body FROM logs WHERE body LIKE 'pg_%'", &[])
            .await
            .unwrap();
        assert!(rows.is_empty());
        // What the catalogs cannot answer is a syntax error to the client.
        let error = client
            .query("SELECT nope FROM pg_catalog.pg_class", &[])
            .await
            .unwrap_err();
        assert_eq!(error.as_db_error().unwrap().code().code(), "42601");
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

    #[tokio::test]
    async fn parameters_bind_over_the_extended_protocol() {
        let (address, server) = node().await;
        let client = connect(address, "prod").await;

        // Text against a text column.
        let rows = client
            .query("SELECT body FROM logs WHERE body = $1", &[&"second"])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let body: String = rows[0].get(0);
        assert_eq!(body, "second");

        // A timestamp against the timestamp column, inferred for the
        // client, which then sends it in binary.
        let since = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let rows = client
            .query(
                "SELECT body FROM logs WHERE timestamp > $1 ORDER BY timestamp",
                &[&since],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 2, "the two rows after the epoch second");

        // A cast says the type; the value is computed with.
        let row = client
            .query_one("SELECT $1::int + 1 AS n FROM logs LIMIT 1", &[&41i64])
            .await
            .unwrap();
        let n: i64 = row.get(0);
        assert_eq!(n, 42);

        // A declared type wins over inference, and a null binds as NULL.
        let statement = client
            .prepare_typed(
                "SELECT count(*) AS n FROM logs WHERE body = $1 OR $2 IS NULL",
                &[
                    tokio_postgres::types::Type::TEXT,
                    tokio_postgres::types::Type::INT8,
                ],
            )
            .await
            .unwrap();
        assert_eq!(
            statement.params(),
            &[
                tokio_postgres::types::Type::TEXT,
                tokio_postgres::types::Type::INT8
            ]
        );
        let row = client
            .query_one(&statement, &[&"nothing", &None::<i64>])
            .await
            .unwrap();
        let n: f64 = row.get(0);
        assert!(
            (n - 3.0).abs() < f64::EPSILON,
            "NULL IS NULL keeps every row"
        );

        // Too few values is the client's error, caught before the query.
        let refused = client
            .query(
                "SELECT body FROM logs WHERE body = $1 AND $2 IS NULL",
                &[&"x"],
            )
            .await;
        assert!(refused.is_err());
        server.abort();
    }
}
