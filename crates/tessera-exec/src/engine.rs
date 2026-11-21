// SPDX-License-Identifier: Apache-2.0

//! The engine: runs an optimized plan on one node.
//!
//! Each node of the plan becomes a pipeline over the batches its inputs
//! produced; scans fetch their files through pruning and decode them on
//! the CPU pool, and every operator runs there too. The result carries
//! an [`Analysis`] for `EXPLAIN ANALYZE` (QE-4, QE-8).
//!
//! An aggregate, a top-k or a limit over one scan runs in two stages
//! when the node has peers (QE-14): the partial half on every node over
//! its share of the files, the final half over the partial outputs. A
//! grouped aggregate's final half runs on the workers themselves when
//! the transport allows: each partial output stays on its node as an
//! exchange, split by group key, and each worker folds one partition
//! fetched from every producer, so that no node holds every partial
//! output. A join whose two sides are scans runs the same way, each
//! side an exchange split by its join keys and each worker joining
//! one partition of both, and so does a trace group, its spans split
//! by trace id. The other stages fold on the coordinator.
//!
//! Pipelines are not yet fused across nodes nor split into morsels
//! across stages.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampNanosecondArray, new_null_array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use tessera_format::{FileFormat, FormatError};
use tessera_ir::{
    Asof, BinaryOp, Expr, JoinKind, JoinSide, Literal, NamedExpr, Plan, PlanError, Registry,
    SortKey,
};
use tessera_kernels::Grouper;
use tessera_kernels::promql::Params;
use tessera_manifest::{DatasetLogs, ManifestError, Manifests, View, view};
use tessera_object::{PoolError, Pools};
use tessera_types::event::{OBSERVED_TIMESTAMP, TRACE_ID};
use tessera_types::{
    DatasetName, Env, FileMeta, Kind, Random, SharedRandom, TenantId, TimeRange, Timestamp,
    WindowSchedule,
};

use crate::ExecError;
use crate::batch::Batch;
use crate::cache::{self, ResultCache, ResultKey};
use crate::exchange::{self, Exchanges};
use crate::memory::MemoryPool;
use crate::operator::{
    BuildSide, CollectSink, Extract, Filter, Format, HashAggregate, HashJoin, JoinInput, Limit,
    Operator, Project, RangeVector, Sort, Steps, TopK, TraceGroup, Window, merge_sorted,
    partitioned,
};
use crate::pipeline::Pipeline;
use crate::promql;
use crate::remote::{
    Exchange, FinalInput, FinalTask, LeafTask, Peer, Peers, RemoteScan, StageTask,
};
use crate::rollups::RollupSource;
use crate::scan::{self, ScanAnalysis, Share};
use crate::scheduler::{CpuPool, TaskError};
use crate::system::SystemTables;
use crate::workload::ScanUse;

/// Why a query failed.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// The plan is not well formed.
    #[error("{0}")]
    Plan(#[from] PlanError),
    /// An operator failed.
    #[error("{0}")]
    Exec(#[from] ExecError),
    /// The query used more than its limits allow (QE-18).
    #[error("query limit: {0}")]
    Limit(String),
    /// A kernel failed outside an operator.
    #[error("{0}")]
    Kernel(#[from] tessera_kernels::KernelError),
    /// Arrow rejected an operation.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// A manifest, registry or segment could not be read.
    #[error("{0}")]
    Manifest(#[from] ManifestError),
    /// The dataset log could not be read.
    #[error("{0}")]
    Dataset(#[from] tessera_manifest::DatasetLogError),
    /// A file could not be read.
    #[error("{0}")]
    Format(#[from] FormatError),
    /// A file names a pool that is not registered.
    #[error("{0}")]
    Pool(#[from] PoolError),
    /// A CPU task did not complete.
    #[error("{0}")]
    Task(#[from] TaskError),
    /// An object could not be read.
    #[error("{0}")]
    Store(#[from] tessera_object::ObjectError),
}

/// What a query did, for `EXPLAIN ANALYZE`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Analysis {
    /// One entry per scan, in plan order.
    pub scans: Vec<ScanAnalysis>,
    /// Whether the rows came from the result cache, with no scan run.
    pub result_cached: bool,
    /// Bytes blocking operators spilled to local disk (QE-11).
    pub spilled_bytes: u64,
    /// The stages split across nodes (QE-14), in plan order.
    pub stages: Vec<StageAnalysis>,
}

/// A stage whose partial half ran on the nodes that scan its files
/// (QE-14), for `EXPLAIN ANALYZE`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StageAnalysis {
    /// What was split: `aggregate`, `join`, `trace-group`, `sort`,
    /// `top-k` or `limit`.
    pub kind: &'static str,
    /// Partial stages sent to peers.
    pub remote: usize,
    /// Partial stages a peer could not run, run here instead.
    pub failed: usize,
    /// Final partitions run on peers over an exchange.
    pub finals: usize,
    /// Final partitions a peer could not run, run here instead.
    pub final_failed: usize,
    /// Kept outputs a producer no longer had, recomputed here from
    /// its files.
    pub recomputed: usize,
}

/// The rows of a query and what producing them took.
#[derive(Debug)]
pub struct QueryOutput {
    /// The schema of the rows.
    pub schema: SchemaRef,
    /// The rows.
    pub batches: Vec<RecordBatch>,
    /// What the query did.
    pub analysis: Analysis,
    /// What it asked of each scanned table (AD-1); filled by
    /// [`crate::query::run`], empty straight from the engine.
    pub uses: Vec<ScanUse>,
    /// The plan's fingerprint (CO-3); filled by [`crate::query::run`],
    /// zero straight from the engine.
    pub fingerprint: u64,
}

impl QueryOutput {
    /// The total number of rows.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }
}

/// Limits a query runs under (QE-18).
#[derive(Clone, Copy, Debug)]
pub struct QueryLimits {
    /// Bytes a query's blocking operators may hold.
    pub memory_bytes: usize,
    /// Bytes a query may read from files and the tail, or no bound.
    pub scan_bytes: Option<u64>,
    /// How long a query may run, or no bound.
    pub wall_time: Option<Duration>,
}

impl Default for QueryLimits {
    fn default() -> Self {
        QueryLimits {
            memory_bytes: 1 << 30,
            scan_bytes: None,
            wall_time: None,
        }
    }
}

/// What one query may use while it runs (QE-18): the memory its
/// blocking operators share and the bytes its scans may read in all.
struct Grant {
    memory: Arc<MemoryPool>,
    scan_bytes: Option<u64>,
}

/// How long a table's tombstones are kept before they are read again.
const TOMBSTONE_TTL: Duration = Duration::from_secs(1);

/// A table: the tenant, the dataset and the kind.
type TableKey = (TenantId, DatasetName, Kind);
/// A table's tombstone filters and when they were read.
type KeptFilters = (Instant, Arc<Vec<Expr>>);

/// The engine of one node.
pub struct Engine {
    pub(crate) pools: Arc<Pools>,
    pub(crate) format: Arc<dyn FileFormat>,
    pub(crate) cpu: Arc<CpuPool>,
    pub(crate) peers: Option<(Arc<dyn Peers>, Arc<dyn RemoteScan>)>,
    /// Where the `system` tables' rows come from.
    system_tables: Option<Arc<dyn SystemTables>>,
    /// Where the rollups the planner may read come from (AD-13).
    rollups: Option<Arc<dyn RollupSource>>,
    /// The manifests this node has opened (section 10.8).
    pub(crate) manifests: Manifests,
    /// The dataset logs, for tombstones (SE-5).
    datasets: DatasetLogs,
    /// Where blocking operators spill (QE-11); none means they fail
    /// past their grant.
    spill_dir: Option<PathBuf>,
    /// The tombstones read in the last second, per table.
    tombstones: Mutex<HashMap<TableKey, KeptFilters>>,
    /// Results of closed-range queries (section 10.8).
    results: ResultCache,
    /// The partial outputs kept for the workers of final stages (QE-14).
    exchanges: Exchanges,
    /// Where exchange ids come from.
    random: SharedRandom,
    limits: QueryLimits,
    registry: Registry,
}

impl Engine {
    /// An engine over `pools`, reading files with `format` and running CPU
    /// work on `cpu`. Datasets use the default partition window until the
    /// dataset log carries one.
    #[must_use]
    pub fn new(
        pools: Arc<Pools>,
        format: Arc<dyn FileFormat>,
        env: Env,
        cpu: Arc<CpuPool>,
        limits: QueryLimits,
    ) -> Self {
        let manifests = Manifests::new(
            Arc::clone(pools.root().store()),
            env.clone(),
            tessera_manifest::manifests::DEFAULT_CAPACITY,
        );
        let random = env.random.clone();
        let datasets = DatasetLogs::new(Arc::clone(pools.root().store()), env);
        Engine {
            pools,
            format,
            cpu,
            peers: None,
            system_tables: None,
            rollups: None,
            manifests,
            datasets,
            spill_dir: None,
            tombstones: Mutex::new(HashMap::new()),
            results: ResultCache::new(cache::DEFAULT_BYTES),
            exchanges: Exchanges::default(),
            random,
            limits,
            registry: Registry::builtin(),
        }
    }

    /// The engine with a provider of the `system` tables' rows (CS-3).
    #[must_use]
    pub fn with_system_tables(mut self, provider: Arc<dyn SystemTables>) -> Self {
        self.system_tables = Some(provider);
        self
    }

    /// The engine with a provider of the rollups queries may read (AD-13).
    #[must_use]
    pub fn with_rollups(mut self, provider: Arc<dyn RollupSource>) -> Self {
        self.rollups = Some(provider);
        self
    }

    /// The provider of rollups, if any.
    #[must_use]
    pub fn rollups(&self) -> Option<&Arc<dyn RollupSource>> {
        self.rollups.as_ref()
    }

    /// The conjuncts that hide the deleted rows of a table (SE-5): for
    /// each tombstone, not (its predicate and observed before it). Read
    /// from the dataset log at most once a second per table.
    ///
    /// # Errors
    ///
    /// If the dataset log cannot be read: deleted rows are never served
    /// for want of knowing about them.
    pub async fn tombstone_filters(
        &self,
        tenant: &TenantId,
        dataset: &DatasetName,
        kind: Kind,
    ) -> Result<Arc<Vec<Expr>>, EngineError> {
        let key = (tenant.clone(), dataset.clone(), kind);
        {
            let cached = self
                .tombstones
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some((at, filters)) = cached.get(&key)
                && at.elapsed() < TOMBSTONE_TTL
            {
                return Ok(Arc::clone(filters));
            }
        }
        let log = self.datasets.open(tenant, dataset);
        let filters: Vec<Expr> = log
            .tombstones(kind)
            .await?
            .into_iter()
            .map(|t| {
                let predicate: Expr = serde_json::from_value(t.predicate).map_err(|source| {
                    EngineError::Dataset(tessera_manifest::DatasetLogError::Corrupt {
                        key: format!("tombstone {}", t.id),
                        source,
                    })
                })?;
                Ok(Expr::Not(Box::new(
                    predicate.and(
                        Expr::column(OBSERVED_TIMESTAMP)
                            .binary(BinaryOp::LtEq, Expr::literal(t.created_at)),
                    ),
                )))
            })
            .collect::<Result<_, EngineError>>()?;
        let filters = Arc::new(filters);
        self.tombstones
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key, (Instant::now(), Arc::clone(&filters)));
        Ok(filters)
    }

    /// The engine keeping at most `bytes` of closed-range results.
    #[must_use]
    pub fn with_result_cache(mut self, bytes: usize) -> Self {
        self.results = ResultCache::new(bytes);
        self
    }

    /// The engine with other nodes to share leaf tasks with (QE-14):
    /// `peers` says which node reads which file and `remote` reaches it.
    #[must_use]
    pub fn with_peers(mut self, peers: Arc<dyn Peers>, remote: Arc<dyn RemoteScan>) -> Self {
        self.peers = Some((peers, remote));
        self
    }

    /// Runs a stage sent by another node (QE-14): its plan over its
    /// files, under this node's own limits, and returns the output, or
    /// keeps it under the task's exchange and returns nothing.
    ///
    /// # Errors
    ///
    /// If the plan is malformed, a file cannot be read or an operator
    /// fails.
    pub async fn stage(&self, task: &StageTask) -> Result<Vec<RecordBatch>, EngineError> {
        let grant = self.own_grant();
        let mut analysis = Analysis::default();
        let share = Share {
            files: task.files.clone(),
            tail: Vec::new(),
            analysis: ScanAnalysis::default(),
        };
        let rows = self
            .execute_in(&task.plan, &grant, &mut analysis, Some(&share))
            .await?;
        let Some(exchange) = &task.exchange else {
            return Ok(rows);
        };
        let schema = task.plan.schema(&self.registry)?;
        self.exchanges.keep(exchange, schema, rows)?;
        Ok(Vec::new())
    }

    /// The rows a stage of this node kept under exchange `id` for
    /// `partition`, with their schema, taken once (QE-14).
    #[must_use]
    pub fn fetch(&self, id: &str, partition: u32) -> Option<(SchemaRef, Vec<RecordBatch>)> {
        self.exchanges.take(id, partition)
    }

    /// Runs a final stage sent by a coordinator over one partition of
    /// its inputs' exchanges (QE-14): the task's plan folds, per input,
    /// `rows`, the partition this node kept, and the partition fetched
    /// from each producer.
    ///
    /// # Errors
    ///
    /// If a producer no longer has its partition, the plan is
    /// malformed or an operator fails.
    pub async fn finalize(
        &self,
        task: &FinalTask,
        rows: Vec<Vec<RecordBatch>>,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let mut gathered: Vec<Vec<RecordBatch>> = rows;
        gathered.resize_with(task.inputs.len(), Vec::new);
        // The producers first: a fetch that fails leaves this node's
        // own partitions in place for the coordinator to take.
        for (input, rows) in task.inputs.iter().zip(&mut gathered) {
            for producer in &input.producers {
                let Some((_, transport)) = &self.peers else {
                    return Err(unsupported(
                        "a final stage needs a transport to its producers",
                    ));
                };
                let fetched = transport
                    .fetch(producer, &input.exchange.id, task.partition)
                    .await
                    .map_err(|e| {
                        unsupported(format!(
                            "exchange {}: producer {}: {e}",
                            input.exchange.id, producer.node
                        ))
                    })?;
                rows.extend(fetched);
            }
        }
        for (input, rows) in task.inputs.iter().zip(&mut gathered) {
            if let Some((_, mine)) = self.exchanges.take(&input.exchange.id, task.partition) {
                rows.extend(mine);
            }
        }
        self.fold(&task.plan, gathered, &self.own_grant()).await
    }

    /// `plan` over `rows`, one list per input: a join of its two sides,
    /// an operator over its one input otherwise.
    async fn fold(
        &self,
        plan: &Plan,
        rows: Vec<Vec<RecordBatch>>,
        grant: &Grant,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let mut rows = rows.into_iter();
        match plan {
            Plan::Join {
                left,
                right,
                on,
                kind,
                asof,
            } => {
                let left_rows = rows.next().unwrap_or_default();
                let right_rows = rows.next().unwrap_or_default();
                self.join_rows(
                    left,
                    right,
                    on,
                    *kind,
                    asof.as_ref(),
                    left_rows,
                    right_rows,
                    grant,
                )
                .await
            }
            Plan::TraceGroup { spanset, .. } => {
                let operator =
                    Box::new(TraceGroup::new(spanset.clone(), Arc::clone(&grant.memory)));
                self.pipe(operator, rows.next().unwrap_or_default()).await
            }
            other => {
                let Some(input) = other.inputs().into_iter().next() else {
                    return Err(unsupported("a final stage needs an input"));
                };
                let operator = self.operator(other, input, &grant.memory)?;
                self.pipe(operator, rows.next().unwrap_or_default()).await
            }
        }
    }

    /// A grant under this node's own limits, for work sent by another
    /// node.
    fn own_grant(&self) -> Grant {
        Grant {
            memory: MemoryPool::with_spill_dir(self.limits.memory_bytes, self.spill_dir.clone()),
            scan_bytes: self.limits.scan_bytes,
        }
    }

    /// An exchange id no other stage of this node shares.
    fn exchange_id(&self) -> String {
        format!("{:032x}", self.random.fork().next_u128())
    }

    /// Runs a leaf task sent by another node: reads its files and
    /// returns the rows in the scan's schema.
    ///
    /// # Errors
    ///
    /// If a file cannot be read or the predicates cannot be evaluated.
    pub async fn leaf(&self, task: &LeafTask) -> Result<Vec<RecordBatch>, EngineError> {
        let mut analysis = ScanAnalysis::default();
        scan::read_files(
            self,
            &task.scan,
            task.files.clone(),
            &[],
            &mut analysis,
            None,
        )
        .await
    }

    /// Lets blocking operators spill under `dir` when a query's memory
    /// grant runs out (QE-11); the directory is created, and a
    /// directory that cannot be is logged and left unused.
    #[must_use]
    pub fn with_spill_dir(mut self, dir: PathBuf) -> Self {
        match std::fs::create_dir_all(&dir) {
            Ok(()) => self.spill_dir = Some(dir),
            Err(error) => {
                tracing::warn!(dir = %dir.display(), error = %error, "spill directory unusable; queries past their memory grant fail");
            }
        }
        self
    }

    /// The function registry the engine runs.
    #[must_use]
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The consistent view of `dataset` and `kind` within `range`
    /// (section 6.8), read here: the files of the partitions the range
    /// overlaps and the tail rows no partition has adopted, as they are,
    /// with where each writer's log was read to (section 11.7).
    ///
    /// # Errors
    ///
    /// If the registry, a manifest, a file or a segment cannot be read.
    pub async fn view(
        &self,
        tenant: &TenantId,
        dataset: &DatasetName,
        kind: Kind,
        range: TimeRange,
    ) -> Result<View, EngineError> {
        let found = view::sources(
            &self.manifests,
            tenant,
            dataset,
            kind,
            self.window_schedule(tenant, dataset).await?,
            range,
        )
        .await?;
        Ok(view::read_sources(&self.pools, &self.format, found).await?)
    }

    /// The partition windows of `dataset` over time (MF-2), from its
    /// dataset log.
    pub(crate) async fn window_schedule(
        &self,
        tenant: &TenantId,
        dataset: &DatasetName,
    ) -> Result<WindowSchedule, EngineError> {
        Ok(self.datasets.open(tenant, dataset).window().await?)
    }

    /// The node's default limits on a query.
    #[must_use]
    pub fn limits(&self) -> &QueryLimits {
        &self.limits
    }

    /// Forgets what the engine holds for tables nobody read in `idle`
    /// (CT-6): dataset logs and tombstone filters. Manifests are bounded
    /// by their capacity already, and results by their bytes. Returns
    /// how many dataset logs were forgotten.
    pub fn release_idle(&self, idle: Duration) -> usize {
        self.tombstones
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|_, (at, _)| at.elapsed() < idle);
        self.datasets.release_idle(idle)
    }

    /// Runs an optimized `plan` under the node's default limits.
    ///
    /// # Errors
    ///
    /// If the plan is malformed, storage cannot be read, or an operator
    /// fails or exhausts its memory.
    pub async fn run(&self, plan: &Plan) -> Result<QueryOutput, EngineError> {
        self.run_with(plan, &self.limits).await
    }

    /// Runs an optimized `plan` under `limits` (QE-18): the memory its
    /// blocking operators share, the bytes its scans may read and how
    /// long it may run.
    ///
    /// # Errors
    ///
    /// If the plan is malformed, storage cannot be read, an operator
    /// fails, or a limit is exceeded.
    pub async fn run_with(
        &self,
        plan: &Plan,
        limits: &QueryLimits,
    ) -> Result<QueryOutput, EngineError> {
        let schema = plan.schema(&self.registry)?;
        let key = if cache::is_closed(plan, self.manifests.env().clock.now()) {
            Some(self.result_key(plan).await?)
        } else {
            None
        };
        if let Some(kept) = key.as_ref().and_then(|key| self.results.get(key)) {
            return Ok(QueryOutput {
                schema: Arc::clone(&kept.schema),
                batches: kept.batches.clone(),
                analysis: Analysis {
                    scans: Vec::new(),
                    result_cached: true,
                    spilled_bytes: 0,
                    stages: Vec::new(),
                },
                uses: Vec::new(),
                fingerprint: 0,
            });
        }
        let grant = Grant {
            memory: MemoryPool::with_spill_dir(limits.memory_bytes, self.spill_dir.clone()),
            scan_bytes: limits.scan_bytes,
        };
        let mut analysis = Analysis::default();
        let execution = self.execute(plan, &grant, &mut analysis);
        let batches = match limits.wall_time {
            Some(allowed) => tokio::time::timeout(allowed, execution)
                .await
                .map_err(|_| {
                    EngineError::Limit(format!(
                        "ran longer than the {} s a query may take",
                        allowed.as_secs_f64()
                    ))
                })??,
            None => execution.await?,
        };
        analysis.spilled_bytes = grant.memory.spilled_bytes();
        if let Some(key) = key {
            self.results
                .insert(key, Arc::clone(&schema), batches.clone());
        }
        Ok(QueryOutput {
            schema,
            batches,
            analysis,
            uses: Vec::new(),
            fingerprint: 0,
        })
    }

    /// What a result of `plan` depends on: the plan and the versions of
    /// every manifest its scans read.
    async fn result_key(&self, plan: &Plan) -> Result<ResultKey, EngineError> {
        let mut versions = Vec::new();
        for scan in cache::scans(plan) {
            versions.extend(
                self.manifests
                    .versions(&scan.tenant, &scan.dataset, scan.kind, scan.time_range)
                    .await?,
            );
        }
        versions.sort();
        Ok(ResultKey {
            fingerprint: cache::fingerprint(plan),
            versions,
        })
    }

    /// Runs `plan` within `grant`.
    fn execute<'a>(
        &'a self,
        plan: &'a Plan,
        grant: &'a Grant,
        analysis: &'a mut Analysis,
    ) -> futures::future::BoxFuture<'a, Result<Vec<RecordBatch>, EngineError>> {
        self.execute_in(plan, grant, analysis, None)
    }

    /// Runs `plan` within `grant`; with `share`, as a stage whose scan
    /// reads the share and nothing else (QE-14).
    fn execute_in<'a>(
        &'a self,
        plan: &'a Plan,
        grant: &'a Grant,
        analysis: &'a mut Analysis,
        share: Option<&'a Share>,
    ) -> futures::future::BoxFuture<'a, Result<Vec<RecordBatch>, EngineError>> {
        Box::pin(async move {
            if share.is_none()
                && let Some(batches) = self.staged(plan, grant, analysis).await?
            {
                return Ok(batches);
            }
            match plan {
                Plan::Scan(scan) => self.scan_node(scan, grant, analysis, share).await,
                Plan::Values { schema, rows } => Ok(vec![values(schema, rows)?]),
                Plan::SystemTable { tenant, table } => {
                    let Some(provider) = &self.system_tables else {
                        return Err(EngineError::Exec(ExecError::Unsupported(
                            "system tables are not available on this engine".into(),
                        )));
                    };
                    provider
                        .rows(tenant, table)
                        .await
                        .map_err(|e| EngineError::Exec(ExecError::Unsupported(e)))
                }
                Plan::Union { inputs } => {
                    let mut batches = Vec::new();
                    for input in inputs {
                        batches.extend(self.execute_in(input, grant, analysis, share).await?);
                    }
                    Ok(batches)
                }
                Plan::Join {
                    left,
                    right,
                    on,
                    kind,
                    asof,
                } => {
                    self.join(left, right, on, *kind, asof.as_ref(), grant, analysis)
                        .await
                }
                Plan::TraceGroup {
                    candidates,
                    spans,
                    spanset,
                } => {
                    // Two phases (QL-13): the candidates' trace ids, then
                    // every span of those traces.
                    let ids = match candidates {
                        Some(candidates) => Some(trace_ids(
                            &self.execute_in(candidates, grant, analysis, share).await?,
                        )),
                        None => None,
                    };
                    let spans = match ids {
                        Some(ids) if ids.is_empty() => return Ok(Vec::new()),
                        Some(ids) => Cow::Owned(with_in_list(
                            spans.as_ref().clone(),
                            Expr::column(TRACE_ID),
                            ids,
                        )),
                        None => Cow::Borrowed(spans.as_ref()),
                    };
                    let batches = self.execute_in(&spans, grant, analysis, share).await?;
                    let operator =
                        Box::new(TraceGroup::new(spanset.clone(), Arc::clone(&grant.memory)));
                    self.pipe(operator, batches).await
                }
                Plan::PromQL {
                    inputs,
                    expr,
                    start,
                    end,
                    step,
                } => {
                    let mut series = Vec::with_capacity(inputs.len());
                    for input in inputs {
                        let batches = self.execute_in(input, grant, analysis, share).await?;
                        series.push(promql::series_of(&batches)?);
                    }
                    let params = Params {
                        start: start.as_nanos(),
                        end: end.as_nanos(),
                        step: i64::try_from(step.as_nanos()).unwrap_or(i64::MAX),
                    };
                    let expr = expr.clone();
                    let task = self
                        .cpu
                        .spawn(move || promql::evaluate(&expr, &series, params));
                    Ok(vec![task.await??])
                }
                other => {
                    let input = other.inputs()[0];
                    let operator = self.operator(other, input, &grant.memory)?;
                    let batches = self.execute_in(input, grant, analysis, share).await?;
                    self.pipe(operator, batches).await
                }
            }
        })
    }

    /// Runs `plan` as a partial stage on the nodes that scan its files
    /// and a final one here (QE-14), when it is an aggregate, a sort, a
    /// top-k or a limit over one scan and this node has peers: each
    /// peer returns its share's partial result, this node's share runs
    /// here with the tail, and the final operator folds them, a sort's
    /// by merging the shares each node ordered. A peer that
    /// fails has its share run here. `None` when the plan is not one
    /// to split.
    async fn staged(
        &self,
        plan: &Plan,
        grant: &Grant,
        analysis: &mut Analysis,
    ) -> Result<Option<Vec<RecordBatch>>, EngineError> {
        let Some((_, transport)) = &self.peers else {
            return Ok(None);
        };
        match plan {
            Plan::Join {
                left, right, on, ..
            } => {
                return self
                    .join_staged(plan, left, right, on, grant, analysis)
                    .await;
            }
            Plan::TraceGroup {
                candidates, spans, ..
            } => {
                return self
                    .trace_staged(plan, candidates.as_deref(), spans, grant, analysis)
                    .await;
            }
            _ => {}
        }
        let Some(stage) = stage_of(plan, &self.registry) else {
            return Ok(None);
        };
        let Some(scan) = fragment_scan(&stage.partial) else {
            return Ok(None);
        };
        let limit = scan_limit(grant, analysis);
        let mut scanned = ScanAnalysis::default();
        let split = scan::split(self, scan, &mut scanned, limit).await?;
        let partial = with_scan(&stage.partial, &split.scan);
        // A grouped aggregate folds on the workers when the transport
        // lets them keep and hand out their outputs: one partition of
        // the groups per node, the coordinator's own included.
        let exchange = match &stage.keys {
            Some(keys) if transport.supports_exchange() && !split.remote.is_empty() => {
                Some(Exchange {
                    id: self.exchange_id(),
                    partitions: u32::try_from(split.remote.len() + 1).unwrap_or(u32::MAX),
                    keys: keys.clone(),
                })
            }
            _ => None,
        };
        let mut report = StageAnalysis {
            kind: stage.kind,
            ..StageAnalysis::default()
        };
        let (mut runs, producers, mine) = self
            .partial_stages(
                &partial,
                split,
                scanned,
                exchange.as_ref(),
                grant,
                analysis,
                &mut report,
            )
            .await?;
        let output = match exchange {
            Some(exchange) if !producers.is_empty() => {
                let workers: Vec<Peer> = producers.iter().map(|(p, _)| p.clone()).collect();
                let input = ExchangeInput {
                    exchange,
                    partial: partial.clone(),
                    producers,
                    mine,
                };
                self.exchanged(
                    &stage.final_plan,
                    vec![input],
                    workers,
                    grant,
                    analysis,
                    &mut report,
                )
                .await?
            }
            _ => {
                runs.push(mine);
                if let Plan::Sort { keys, .. } = &stage.final_plan {
                    self.merge_sorted(&stage.final_plan, &partial, keys, runs, grant)
                        .await?
                } else {
                    let batches = runs.into_iter().flatten().collect();
                    let operator = self.operator(&stage.final_plan, &partial, &grant.memory)?;
                    self.pipe(operator, batches).await?
                }
            }
        };
        analysis.stages.push(report);
        Ok(Some(output))
    }

    /// The final half of a sort stage: `runs`, the shares each ordered
    /// on `keys` by the node that read it, merged into one order. The
    /// merge holds every row at once, so it reserves them in the grant
    /// as the sort it stands for would; when the grant refuses, that
    /// sort runs over the shares instead, and spills (QE-11).
    async fn merge_sorted(
        &self,
        sort: &Plan,
        input: &Plan,
        keys: &[SortKey],
        runs: Vec<Vec<RecordBatch>>,
        grant: &Grant,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let Some(schema) = runs.iter().flatten().next().map(RecordBatch::schema) else {
            return Ok(Vec::new());
        };
        let bytes: usize = runs
            .iter()
            .flatten()
            .map(RecordBatch::get_array_memory_size)
            .sum();
        if grant.memory.reserve(bytes).is_err() {
            let operator = self.operator(sort, input, &grant.memory)?;
            return self
                .pipe(operator, runs.into_iter().flatten().collect())
                .await;
        }
        let keys = keys.to_vec();
        let merged = self
            .cpu
            .spawn(move || merge_sorted(runs, &keys, &schema))
            .await;
        grant.memory.release(bytes);
        Ok(merged??)
    }

    /// Runs a stage's partial half over `split`: on each peer over its
    /// share, kept under `exchange` when there is one, and here over
    /// this node's share and the tail; a peer that fails has its share
    /// run here. Returns the rows each peer returned, the peers that
    /// kept their output with their files, and this node's own rows.
    #[allow(clippy::too_many_arguments)]
    async fn partial_stages(
        &self,
        partial: &Plan,
        split: scan::Split,
        mut scanned: ScanAnalysis,
        exchange: Option<&Exchange>,
        grant: &Grant,
        analysis: &mut Analysis,
        report: &mut StageAnalysis,
    ) -> Result<PartialOutputs, EngineError> {
        let Some((_, transport)) = &self.peers else {
            return Err(unsupported("a partial stage needs peers"));
        };
        let mut local = split.local;
        let mut runs = Vec::new();
        let mut producers: Vec<(Peer, Vec<FileMeta>)> = Vec::new();
        for (peer, files) in split.remote {
            let task = StageTask {
                plan: partial.clone(),
                files,
                exchange: exchange.cloned(),
            };
            report.remote += 1;
            scanned.remote_tasks += 1;
            match transport.stage(&peer, &task).await {
                Ok(rows) => {
                    scanned.files_remote += task.files.len();
                    if exchange.is_some() {
                        producers.push((peer, task.files));
                    } else {
                        runs.push(rows);
                    }
                }
                Err(error) => {
                    tracing::warn!(peer = %peer.node, error = %error, "stage failed; running here");
                    report.failed += 1;
                    scanned.remote_failed += 1;
                    local.extend(task.files);
                }
            }
        }
        let share = Share {
            files: local,
            tail: split.tail,
            analysis: scanned,
        };
        let mine = self
            .execute_in(partial, grant, analysis, Some(&share))
            .await?;
        Ok((runs, producers, mine))
    }

    /// A join whose two sides are fragments over scans runs on the
    /// workers (QE-14): each side's partial stages keep their rows as
    /// an exchange split by the side's join keys, and every producer
    /// joins one partition of both sides, the coordinator the last.
    /// The runtime filter an inner join applies to its left side on
    /// one node is not applied here. `None` when the transport has no
    /// exchange, the join has no keys, a side is not a fragment any
    /// node can run a share of, or no peer owns a file of either side.
    async fn join_staged(
        &self,
        plan: &Plan,
        left: &JoinSide,
        right: &JoinSide,
        on: &[(Expr, Expr)],
        grant: &Grant,
        analysis: &mut Analysis,
    ) -> Result<Option<Vec<RecordBatch>>, EngineError> {
        let Some((_, transport)) = &self.peers else {
            return Ok(None);
        };
        if !transport.supports_exchange() || on.is_empty() {
            return Ok(None);
        }
        let (Some(left_scan), Some(right_scan)) =
            (fragment_scan_of(&left.plan), fragment_scan_of(&right.plan))
        else {
            return Ok(None);
        };
        let limit = scan_limit(grant, analysis);
        let mut left_scanned = ScanAnalysis::default();
        let left_split = scan::split(self, left_scan, &mut left_scanned, limit).await?;
        let mut right_scanned = ScanAnalysis::default();
        let right_split = scan::split(self, right_scan, &mut right_scanned, limit).await?;
        let mut workers: Vec<Peer> = Vec::new();
        for (peer, _) in left_split.remote.iter().chain(&right_split.remote) {
            if !workers.iter().any(|w| w.node == peer.node) {
                workers.push(peer.clone());
            }
        }
        if workers.is_empty() {
            return Ok(None);
        }
        let partitions = u32::try_from(workers.len() + 1).unwrap_or(u32::MAX);
        let id = self.exchange_id();
        let mut report = StageAnalysis {
            kind: "join",
            ..StageAnalysis::default()
        };
        let sides = [
            (left, left_split, left_scanned, "l", 0),
            (right, right_split, right_scanned, "r", 1),
        ];
        let mut inputs = Vec::new();
        for (side, split, scanned, suffix, which) in sides {
            let partial = with_scan(&side.plan, &split.scan);
            let exchange = Exchange {
                id: format!("{id}-{suffix}"),
                partitions,
                keys: on
                    .iter()
                    .map(|keys| {
                        if which == 0 {
                            keys.0.clone()
                        } else {
                            keys.1.clone()
                        }
                    })
                    .collect(),
            };
            let (_, producers, mine) = self
                .partial_stages(
                    &partial,
                    split,
                    scanned,
                    Some(&exchange),
                    grant,
                    analysis,
                    &mut report,
                )
                .await?;
            inputs.push(ExchangeInput {
                exchange,
                partial,
                producers,
                mine,
            });
        }
        let output = self
            .exchanged(plan, inputs, workers, grant, analysis, &mut report)
            .await?;
        analysis.stages.push(report);
        Ok(Some(output))
    }

    /// A trace group whose span plan is a fragment over a scan runs on
    /// the workers (QE-14): the candidates' trace ids are read here,
    /// every span of those traces is read on the nodes that own the
    /// files and kept as an exchange split by trace id, so that a trace
    /// stays whole, and every producer groups one partition. `None`
    /// when the transport has no exchange or the span plan is not a
    /// fragment; when no peer owns a file of it, the group runs here
    /// over the candidates already read.
    async fn trace_staged(
        &self,
        plan: &Plan,
        candidates: Option<&Plan>,
        spans: &Plan,
        grant: &Grant,
        analysis: &mut Analysis,
    ) -> Result<Option<Vec<RecordBatch>>, EngineError> {
        let Some((_, transport)) = &self.peers else {
            return Ok(None);
        };
        if !transport.supports_exchange() || fragment_scan_of(spans).is_none() {
            return Ok(None);
        }
        let ids = match candidates {
            Some(candidates) => Some(trace_ids(
                &self.execute_in(candidates, grant, analysis, None).await?,
            )),
            None => None,
        };
        let spans = match ids {
            Some(ids) if ids.is_empty() => return Ok(Some(Vec::new())),
            Some(ids) => with_in_list(spans.clone(), Expr::column(TRACE_ID), ids),
            None => spans.clone(),
        };
        let Some(scan) = fragment_scan_of(&spans) else {
            return Ok(None);
        };
        let limit = scan_limit(grant, analysis);
        let mut scanned = ScanAnalysis::default();
        let split = scan::split(self, scan, &mut scanned, limit).await?;
        let partial = with_scan(&spans, &split.scan);
        let exchange = Exchange {
            id: self.exchange_id(),
            partitions: u32::try_from(split.remote.len() + 1).unwrap_or(u32::MAX),
            keys: vec![Expr::column(TRACE_ID)],
        };
        let mut report = StageAnalysis {
            kind: "trace-group",
            ..StageAnalysis::default()
        };
        let (_, producers, mine) = self
            .partial_stages(
                &partial,
                split,
                scanned,
                Some(&exchange),
                grant,
                analysis,
                &mut report,
            )
            .await?;
        let output = if producers.is_empty() {
            self.fold(plan, vec![mine], grant).await?
        } else {
            let workers: Vec<Peer> = producers.iter().map(|(p, _)| p.clone()).collect();
            let input = ExchangeInput {
                exchange: Exchange {
                    partitions: u32::try_from(workers.len() + 1).unwrap_or(u32::MAX),
                    ..exchange
                },
                partial,
                producers,
                mine,
            };
            self.exchanged(plan, vec![input], workers, grant, analysis, &mut report)
                .await?
        };
        analysis.stages.push(report);
        Ok(Some(output))
    }

    /// The final half of a stage over its inputs' exchanges (QE-14):
    /// each worker folds one partition, fetching every input's part
    /// from its producers and taking the coordinator's part with the
    /// task; the coordinator folds the last partition itself. A
    /// partition a worker cannot fold is folded here, and a producer
    /// that no longer has its output has its share recomputed here
    /// from its files (QE-17), so the result is the same either way.
    async fn exchanged(
        &self,
        final_plan: &Plan,
        mut inputs: Vec<ExchangeInput>,
        workers: Vec<Peer>,
        grant: &Grant,
        analysis: &mut Analysis,
        report: &mut StageAnalysis,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let Some((_, transport)) = &self.peers else {
            return Err(unsupported("an exchange needs peers"));
        };
        let partitions = workers.len() + 1;
        // The coordinator's own parts, by input and partition.
        let mut parts: Vec<Vec<Vec<RecordBatch>>> = Vec::new();
        for input in &mut inputs {
            parts.push(exchange::partition(
                std::mem::take(&mut input.mine),
                &input.exchange.keys,
                input.exchange.partitions,
            )?);
        }
        let mut output = Vec::new();
        let mut here: Vec<usize> = Vec::new();
        for (index, worker) in workers.iter().enumerate() {
            let task = FinalTask {
                plan: final_plan.clone(),
                partition: u32::try_from(index).unwrap_or(u32::MAX),
                inputs: inputs
                    .iter()
                    .map(|input| FinalInput {
                        exchange: input.exchange.clone(),
                        producers: input
                            .producers
                            .iter()
                            .filter(|(producer, _)| producer.node != worker.node)
                            .map(|(producer, _)| producer.clone())
                            .collect(),
                    })
                    .collect(),
            };
            report.finals += 1;
            let rows: Vec<Vec<RecordBatch>> = parts
                .iter_mut()
                .map(|part| std::mem::take(&mut part[index]))
                .collect();
            match transport.finalize(worker, &task, &rows).await {
                Ok(folded) => output.extend(folded),
                Err(error) => {
                    tracing::warn!(peer = %worker.node, error = %error, "final stage failed; running here");
                    report.final_failed += 1;
                    for (part, rows) in parts.iter_mut().zip(rows) {
                        part[index] = rows;
                    }
                    here.push(index);
                }
            }
        }
        here.push(partitions - 1);
        // A producer whose kept output is gone is recomputed once, every
        // partition of it kept for the ones still to fold here.
        let mut recomputed: Vec<Vec<Option<Vec<Vec<RecordBatch>>>>> = inputs
            .iter()
            .map(|input| vec![None; input.producers.len()])
            .collect();
        for index in here {
            let mut gathered: Vec<Vec<RecordBatch>> = Vec::new();
            for (which, input) in inputs.iter().enumerate() {
                let mut rows = std::mem::take(&mut parts[which][index]);
                for (producer, (peer, files)) in input.producers.iter().enumerate() {
                    if let Some(again) = &mut recomputed[which][producer] {
                        rows.extend(std::mem::take(&mut again[index]));
                        continue;
                    }
                    let partition = u32::try_from(index).unwrap_or(u32::MAX);
                    match transport.fetch(peer, &input.exchange.id, partition).await {
                        Ok(fetched) => rows.extend(fetched),
                        Err(error) => {
                            tracing::warn!(peer = %peer.node, error = %error, "kept output lost; recomputing here");
                            report.recomputed += 1;
                            let share = Share {
                                files: files.clone(),
                                tail: Vec::new(),
                                analysis: ScanAnalysis::default(),
                            };
                            let output = self
                                .execute_in(&input.partial, grant, analysis, Some(&share))
                                .await?;
                            let mut again = exchange::partition(
                                output,
                                &input.exchange.keys,
                                input.exchange.partitions,
                            )?;
                            rows.extend(std::mem::take(&mut again[index]));
                            recomputed[which][producer] = Some(again);
                        }
                    }
                }
                gathered.push(rows);
            }
            output.extend(self.fold(final_plan, gathered, grant).await?);
        }
        Ok(output)
    }

    /// A scan node: the scan runs within what the query's earlier scans
    /// left of its bytes, and its analysis joins the query's.
    async fn scan_node(
        &self,
        scan: &tessera_ir::Scan,
        grant: &Grant,
        analysis: &mut Analysis,
        share: Option<&Share>,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let limit = scan_limit(grant, analysis);
        let mut scanned = share.map_or_else(ScanAnalysis::default, |s| s.analysis.clone());
        let result = match share {
            Some(share) => scan::read_share(self, scan, share, &mut scanned, limit).await,
            None => scan::run(self, scan, &mut scanned, limit).await,
        };
        analysis.scans.push(scanned);
        result
    }

    /// The operator that turns `input`'s rows into `plan`'s, for the
    /// single-input nodes.
    fn operator(
        &self,
        plan: &Plan,
        input: &Plan,
        memory: &Arc<MemoryPool>,
    ) -> Result<Box<dyn Operator>, EngineError> {
        let registry = &self.registry;
        Ok(match plan {
            Plan::Filter { predicate, .. } => Box::new(Filter::new(predicate.clone())),
            Plan::Project { exprs, .. } => {
                Box::new(Project::new(exprs.clone(), plan.schema(registry)?))
            }
            Plan::Extract { source, stage, .. } => Box::new(Extract::new(
                source.clone(),
                stage.clone(),
                plan.schema(registry)?,
            )),
            Plan::Format {
                line,
                labels,
                drop,
                keep,
                ..
            } => Box::new(Format::new(
                line.clone(),
                labels.clone(),
                drop.clone(),
                keep.clone(),
                plan.schema(registry)?,
            )),
            Plan::Aggregate { .. }
            | Plan::PartialAggregate { .. }
            | Plan::FinalAggregate { .. } => self.aggregation(plan, input, memory)?,
            Plan::Sort { keys, .. } => Box::new(Sort::new(
                keys.clone(),
                input.schema(registry)?,
                Arc::clone(memory),
            )),
            Plan::Window {
                partition_by,
                order_by,
                functions,
                ..
            } => Box::new(Window::new(
                partition_by.clone(),
                order_by.clone(),
                functions.clone(),
                input.schema(registry)?,
                plan.schema(registry)?,
                Arc::clone(memory),
            )),
            Plan::TopK {
                keys,
                k,
                partition_by,
                ..
            } => Box::new(TopK::new(
                keys.clone(),
                *k,
                partition_by.clone(),
                input.schema(registry)?,
                Arc::clone(memory),
            )),
            Plan::Limit { offset, count, .. } => Box::new(Limit::new(*offset, *count)),
            Plan::RangeVector {
                function,
                value,
                range,
                step,
                start,
                end,
                grouping,
                ..
            } => Box::new(RangeVector::new(
                *function,
                value.clone(),
                Steps {
                    range: *range,
                    step: *step,
                    start: *start,
                    end: *end,
                },
                grouping.clone(),
                Arc::clone(memory),
            )),
            Plan::Scan(_)
            | Plan::Values { .. }
            | Plan::SystemTable { .. }
            | Plan::Union { .. }
            | Plan::Join { .. }
            | Plan::TraceGroup { .. }
            | Plan::PromQL { .. } => {
                return Err(EngineError::Exec(ExecError::Unsupported(
                    "not a single-input node".into(),
                )));
            }
        })
    }

    /// The aggregate operator of `plan`, whole or one half of a split
    /// one (QE-14).
    fn aggregation(
        &self,
        plan: &Plan,
        input: &Plan,
        memory: &Arc<MemoryPool>,
    ) -> Result<Box<dyn Operator>, EngineError> {
        let registry = &self.registry;
        Ok(match plan {
            Plan::Aggregate {
                group_by,
                aggregates,
                ..
            } => Box::new(HashAggregate::new(
                group_by.clone(),
                aggregates.clone(),
                input.weighted(registry)?,
                &input.schema(registry)?,
                plan.schema(registry)?,
                Arc::clone(memory),
            )?),
            Plan::PartialAggregate {
                group_by,
                aggregates,
                ..
            } => Box::new(HashAggregate::partial(
                group_by.clone(),
                aggregates.clone(),
                input.weighted(registry)?,
                &input.schema(registry)?,
                plan.schema(registry)?,
                Arc::clone(memory),
            )?),
            Plan::FinalAggregate {
                input: states,
                group_by,
                aggregates,
            } => {
                let Plan::PartialAggregate { input: rows, .. } = states.as_ref() else {
                    return Err(EngineError::Exec(ExecError::Unsupported(
                        "a final aggregate reads a partial aggregate".into(),
                    )));
                };
                Box::new(HashAggregate::merging(
                    group_by.clone(),
                    aggregates.clone(),
                    &rows.schema(registry)?,
                    &input.schema(registry)?,
                    plan.schema(registry)?,
                    Arc::clone(memory),
                )?)
            }
            _ => {
                return Err(EngineError::Exec(ExecError::Unsupported(
                    "not an aggregate".into(),
                )));
            }
        })
    }

    /// Runs a join (DM-2). The right side runs first: under an inner
    /// join its key values narrow the left side's read when they are few
    /// (the runtime filter of QE-6). The smaller side is then hashed and
    /// the other probes it (QE-12), except that a left join always
    /// probes with its left side, every row of which comes out, and so
    /// does an `ASOF JOIN`, which picks a right row for each left one.
    #[allow(
        clippy::too_many_arguments,
        reason = "the two sides, what joins them and the run's context"
    )]
    async fn join(
        &self,
        left: &JoinSide,
        right: &JoinSide,
        on: &[(Expr, Expr)],
        kind: JoinKind,
        asof: Option<&Asof>,
        grant: &Grant,
        analysis: &mut Analysis,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let left_keys: Vec<Expr> = on.iter().map(|(l, _)| l.clone()).collect();
        let right_keys: Vec<Expr> = on.iter().map(|(_, r)| r.clone()).collect();
        let right_rows = self.execute(&right.plan, grant, analysis).await?;
        let left_plan = match kind {
            JoinKind::Inner => with_runtime_filter(
                left.plan.as_ref().clone(),
                &left_keys,
                &right_keys,
                &right_rows,
            )?,
            JoinKind::Left => left.plan.as_ref().clone(),
        };
        let left_rows = self.execute(&left_plan, grant, analysis).await?;
        self.join_rows(left, right, on, kind, asof, left_rows, right_rows, grant)
            .await
    }

    /// Joins the rows of both sides, hashing the smaller side of an
    /// inner join; past the grant, one bucket of keys at a time through
    /// local disk (QE-11).
    #[allow(clippy::too_many_arguments)]
    async fn join_rows(
        &self,
        left: &JoinSide,
        right: &JoinSide,
        on: &[(Expr, Expr)],
        kind: JoinKind,
        asof: Option<&Asof>,
        left_rows: Vec<RecordBatch>,
        right_rows: Vec<RecordBatch>,
        grant: &Grant,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let left_keys: Vec<Expr> = on.iter().map(|(l, _)| l.clone()).collect();
        let right_keys: Vec<Expr> = on.iter().map(|(_, r)| r.clone()).collect();
        let rows_of =
            |batches: &[RecordBatch]| batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let build_left =
            kind == JoinKind::Inner && asof.is_none() && rows_of(&left_rows) < rows_of(&right_rows);
        let left_input = JoinInput {
            schema: left.plan.schema(&self.registry)?,
            keys: left_keys,
            time: asof.map(|a| a.left.clone()),
            name: left.name.clone(),
        };
        let right_input = JoinInput {
            schema: right.plan.schema(&self.registry)?,
            keys: right_keys,
            time: asof.map(|a| a.right.clone()),
            name: right.name.clone(),
        };
        let matching = asof.map(|a| a.matching);
        let (build_side, build_rows, build, probe_rows, probe) = if build_left {
            (
                BuildSide::Left,
                left_rows,
                left_input,
                right_rows,
                right_input,
            )
        } else {
            (
                BuildSide::Right,
                right_rows,
                right_input,
                left_rows,
                left_input,
            )
        };
        let pool = Arc::clone(&grant.memory);
        let whole = {
            let build_rows = build_rows.clone();
            let (build, probe, pool) = (build.clone(), probe.clone(), Arc::clone(&pool));
            self.cpu
                .spawn(move || {
                    HashJoin::new(
                        kind,
                        build_side,
                        matching,
                        &build_rows,
                        &build,
                        &probe,
                        pool,
                    )
                })
                .await?
        };
        match whole {
            Ok(operator) => self.pipe(Box::new(operator), probe_rows).await,
            // Without a spill directory the grant stands.
            Err(ExecError::Memory(failure)) if self.spill_dir.is_some() => {
                let dir = self.spill_dir.clone().unwrap_or_default();
                tracing::debug!(%failure, "join past its grant; partitioning through disk");
                let task = self.cpu.spawn(move || {
                    partitioned(
                        kind, build_side, matching, build_rows, &build, probe_rows, &probe, &pool,
                        &dir,
                    )
                });
                Ok(task.await??)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Runs `operator` over `batches` on the CPU pool.
    async fn pipe(
        &self,
        operator: Box<dyn Operator>,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let task = self.cpu.spawn(move || {
            let mut pipeline = Pipeline::new(vec![operator]);
            let mut sink = CollectSink::new();
            pipeline.run(batches.into_iter().map(|b| Ok(Batch::new(b))), &mut sink)?;
            Ok::<_, ExecError>(sink.into_batches())
        });
        Ok(task.await??)
    }
}

fn unsupported(message: impl Into<String>) -> EngineError {
    EngineError::Exec(ExecError::Unsupported(message.into()))
}

/// What a query may still read (QE-18), given what its earlier scans
/// spent.
fn scan_limit(grant: &Grant, analysis: &Analysis) -> Option<scan::ScanLimit> {
    grant.scan_bytes.map(|bytes| scan::ScanLimit {
        bytes,
        spent: analysis
            .scans
            .iter()
            .map(|s| s.bytes_read + s.bytes_from_tail)
            .sum(),
    })
}

/// A plan split in two (QE-14): the partial half every share of the
/// scan runs, and the final half that folds their outputs.
struct Stage {
    kind: &'static str,
    partial: Plan,
    final_plan: Plan,
    /// The expressions over the partial output that choose a worker
    /// when the final half runs on the workers: a grouped aggregate's
    /// keys.
    keys: Option<Vec<Expr>>,
}

/// What a stage's partial half yields: the rows each peer returned,
/// one run per peer, the peers that kept their output with their
/// files, and this node's own rows.
type PartialOutputs = (
    Vec<Vec<RecordBatch>>,
    Vec<(Peer, Vec<FileMeta>)>,
    Vec<RecordBatch>,
);

/// One input of a final stage run over an exchange: what its partial
/// stages kept, the coordinator's own rows of it, and how to recompute
/// a producer's share.
struct ExchangeInput {
    exchange: Exchange,
    partial: Plan,
    producers: Vec<(Peer, Vec<FileMeta>)>,
    mine: Vec<RecordBatch>,
}

/// How `plan` splits, if it does: an aggregate whose functions merge
/// becomes a partial and a final aggregate; a sort is run on every
/// share and the ordered shares merged; a top-k or a limit without an
/// offset is run on every share and once more over the union.
fn stage_of(plan: &Plan, registry: &Registry) -> Option<Stage> {
    match plan {
        Plan::Aggregate {
            input,
            group_by,
            aggregates,
        } => {
            let rows = input.schema(registry).ok()?;
            for aggregate in aggregates {
                tessera_ir::aggregate_state_types(aggregate, &rows, registry).ok()?;
            }
            let partial = Plan::PartialAggregate {
                input: input.clone(),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
            };
            let keys = group_by
                .iter()
                .map(|k| NamedExpr::new(Expr::column(&k.name), k.name.clone()))
                .collect();
            let final_plan = Plan::FinalAggregate {
                input: Box::new(partial.clone()),
                group_by: keys,
                aggregates: aggregates.clone(),
            };
            let names: Vec<Expr> = group_by.iter().map(|k| Expr::column(&k.name)).collect();
            Some(Stage {
                kind: "aggregate",
                partial,
                final_plan,
                keys: (!names.is_empty()).then_some(names),
            })
        }
        Plan::Sort { .. } => Some(Stage {
            kind: "sort",
            partial: plan.clone(),
            final_plan: plan.clone(),
            keys: None,
        }),
        Plan::TopK { .. } => Some(Stage {
            kind: "top-k",
            partial: plan.clone(),
            final_plan: plan.clone(),
            keys: None,
        }),
        Plan::Limit {
            offset: 0,
            count: Some(_),
            ..
        } => Some(Stage {
            kind: "limit",
            partial: plan.clone(),
            final_plan: plan.clone(),
            keys: None,
        }),
        _ => None,
    }
}

/// The scan a stage's partial half reads, when the half is one node
/// over a chain of row-wise operators over one scan that any node can
/// run its share of; a scan that stops early on its own stays whole.
fn fragment_scan(partial: &Plan) -> Option<&tessera_ir::Scan> {
    fragment_scan_of(partial.inputs().into_iter().next()?)
}

/// The scan `plan` reads, when it is a chain of row-wise operators
/// over one scan that any node can run its share of.
fn fragment_scan_of(plan: &Plan) -> Option<&tessera_ir::Scan> {
    let mut node = plan;
    loop {
        match node {
            Plan::Scan(scan) if scan.newest_first_limit.is_none() => return Some(scan),
            Plan::Filter { input, .. }
            | Plan::Project { input, .. }
            | Plan::Extract { input, .. }
            | Plan::Format { input, .. } => node = input,
            _ => return None,
        }
    }
}

/// `plan`, a fragment [`fragment_scan`] accepted, with its scan
/// replaced by `scan`.
fn with_scan(plan: &Plan, scan: &tessera_ir::Scan) -> Plan {
    let rebuilt = |input: &Plan| Box::new(with_scan(input, scan));
    match plan {
        Plan::Scan(_) => Plan::Scan(scan.clone()),
        Plan::Filter { input, predicate } => Plan::Filter {
            input: rebuilt(input),
            predicate: predicate.clone(),
        },
        Plan::Project { input, exprs } => Plan::Project {
            input: rebuilt(input),
            exprs: exprs.clone(),
        },
        Plan::Extract {
            input,
            source,
            stage,
        } => Plan::Extract {
            input: rebuilt(input),
            source: source.clone(),
            stage: stage.clone(),
        },
        Plan::Format {
            input,
            line,
            labels,
            drop,
            keep,
        } => Plan::Format {
            input: rebuilt(input),
            line: line.clone(),
            labels: labels.clone(),
            drop: drop.clone(),
            keep: keep.clone(),
        },
        Plan::PartialAggregate {
            input,
            group_by,
            aggregates,
        } => Plan::PartialAggregate {
            input: rebuilt(input),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
        },
        Plan::Sort { input, keys } => Plan::Sort {
            input: rebuilt(input),
            keys: keys.clone(),
        },
        Plan::TopK {
            input,
            keys,
            k,
            partition_by,
        } => Plan::TopK {
            input: rebuilt(input),
            keys: keys.clone(),
            k: *k,
            partition_by: partition_by.clone(),
        },
        Plan::Limit {
            input,
            offset,
            count,
        } => Plan::Limit {
            input: rebuilt(input),
            offset: *offset,
            count: *count,
        },
        other => other.clone(),
    }
}

/// A batch of constant rows.
/// The distinct trace ids of `batches`, as byte literals.
fn trace_ids(batches: &[RecordBatch]) -> Vec<Literal> {
    let mut ids = BTreeSet::new();
    for batch in batches {
        let Some(column) = batch
            .column_by_name(TRACE_ID)
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
        else {
            continue;
        };
        ids.extend(column.iter().flatten().map(<[u8]>::to_vec));
    }
    ids.into_iter().map(Literal::Bytes).collect()
}

/// `plan` restricted to the rows where `expr` is one of `values`: a
/// predicate on the scan when the plan is one, so that pruning sees it;
/// a filter otherwise.
fn with_in_list(plan: Plan, expr: Expr, values: Vec<Literal>) -> Plan {
    let in_list = Expr::InList {
        expr: Box::new(expr),
        list: values,
        negated: false,
    };
    match plan {
        Plan::Scan(mut scan) => {
            scan.predicates.push(in_list);
            Plan::Scan(scan)
        }
        other => other.filter(in_list),
    }
}

/// The most key values a runtime filter carries; past that, reading
/// the probe side whole costs less than testing every row against them.
const RUNTIME_FILTER_KEYS: usize = 10_000;

/// `plan`, the probe side of an inner join, restricted to the key
/// values the build side holds (QE-6): one in-list per key that is a
/// column, which the scan's pruning then sees. A key that is an
/// expression, or whose values are too many or of a type no literal
/// holds, leaves the plan alone.
fn with_runtime_filter(
    plan: Plan,
    probe_keys: &[Expr],
    build_keys: &[Expr],
    build: &[RecordBatch],
) -> Result<Plan, EngineError> {
    let mut plan = plan;
    for (probe, key) in probe_keys.iter().zip(build_keys) {
        if !matches!(probe, Expr::Column(_)) {
            continue;
        }
        if let Some(values) = distinct_literals(key, build)? {
            plan = with_in_list(plan, probe.clone(), values);
        }
    }
    Ok(plan)
}

/// The distinct non-null values `key` takes over `batches`, as
/// literals; `None` when there are more than [`RUNTIME_FILTER_KEYS`] or
/// no literal holds their type.
fn distinct_literals(
    key: &Expr,
    batches: &[RecordBatch],
) -> Result<Option<Vec<Literal>>, EngineError> {
    let values = batches
        .iter()
        .map(|batch| tessera_kernels::evaluate(key, batch))
        .collect::<Result<Vec<ArrayRef>, _>>()?;
    let Some(first) = values.first() else {
        return Ok(Some(Vec::new()));
    };
    let mut distinct = Grouper::new(vec![first.data_type().clone()])?;
    for array in &values {
        distinct.group_ids(std::slice::from_ref(array))?;
        if distinct.len() > RUNTIME_FILTER_KEYS {
            return Ok(None);
        }
    }
    let keys = distinct.keys()?;
    Ok(keys.first().and_then(literals_of))
}

/// The non-null values of `array` as literals, for the types a literal
/// holds.
fn literals_of(array: &ArrayRef) -> Option<Vec<Literal>> {
    let any = array.as_any();
    Some(match array.data_type() {
        DataType::Utf8 => any
            .downcast_ref::<StringArray>()?
            .iter()
            .flatten()
            .map(Literal::from)
            .collect(),
        DataType::Int64 => any
            .downcast_ref::<Int64Array>()?
            .iter()
            .flatten()
            .map(Literal::Int)
            .collect(),
        DataType::Int32 => any
            .downcast_ref::<Int32Array>()?
            .iter()
            .flatten()
            .map(|v| Literal::Int(i64::from(v)))
            .collect(),
        DataType::Float64 => any
            .downcast_ref::<Float64Array>()?
            .iter()
            .flatten()
            .map(Literal::Float)
            .collect(),
        DataType::Boolean => any
            .downcast_ref::<BooleanArray>()?
            .iter()
            .flatten()
            .map(Literal::Bool)
            .collect(),
        DataType::FixedSizeBinary(_) => any
            .downcast_ref::<FixedSizeBinaryArray>()?
            .iter()
            .flatten()
            .map(|v| Literal::Bytes(v.to_vec()))
            .collect(),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => any
            .downcast_ref::<TimestampNanosecondArray>()?
            .iter()
            .flatten()
            .map(|v| Literal::Timestamp(Timestamp::from_nanos(v)))
            .collect(),
        _ => return None,
    })
}

fn values(schema: &SchemaRef, rows: &[Vec<Literal>]) -> Result<RecordBatch, EngineError> {
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let cells = rows.iter().map(|row| row.get(i).unwrap_or(&Literal::Null));
            column(field.data_type(), cells, rows.len())
        })
        .collect::<Vec<ArrayRef>>();
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

fn column<'a>(
    data_type: &DataType,
    cells: impl Iterator<Item = &'a Literal>,
    len: usize,
) -> ArrayRef {
    match data_type {
        DataType::Int64 => Arc::new(
            cells
                .map(|c| match c {
                    Literal::Int(i) => Some(*i),
                    _ => None,
                })
                .collect::<Int64Array>(),
        ),
        DataType::Float64 => Arc::new(
            cells
                .map(|c| match c {
                    Literal::Float(f) => Some(*f),
                    #[allow(clippy::cast_precision_loss)]
                    Literal::Int(i) => Some(*i as f64),
                    _ => None,
                })
                .collect::<Float64Array>(),
        ),
        DataType::Utf8 => Arc::new(
            cells
                .map(|c| match c {
                    Literal::Text(s) => Some(s.clone()),
                    _ => None,
                })
                .collect::<StringArray>(),
        ),
        DataType::Boolean => Arc::new(
            cells
                .map(|c| match c {
                    Literal::Bool(b) => Some(*b),
                    _ => None,
                })
                .collect::<BooleanArray>(),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Arc::new(
            cells
                .map(|c| match c {
                    Literal::Timestamp(t) => Some(t.as_nanos()),
                    _ => None,
                })
                .collect::<TimestampNanosecondArray>(),
        ),
        other => new_null_array(other, len),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use arrow::datatypes::{Field, Schema};

    use super::*;

    #[test]
    fn values_build_typed_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("x", DataType::Float64, true),
        ]));
        let batch = values(
            &schema,
            &[
                vec![Literal::Int(1), Literal::Text("a".into()), Literal::Int(2)],
                vec![Literal::Null, Literal::Null, Literal::Float(0.5)],
            ],
        )
        .unwrap();
        assert_eq!(batch.num_rows(), 2);
        let n = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(n.value(0), 1);
        assert!(n.is_null(1));
        let x = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((x.value(0) - 2.0).abs() < f64::EPSILON);
        assert_eq!(values(&schema, &[]).unwrap().num_rows(), 0);
    }
}
