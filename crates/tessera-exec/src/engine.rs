// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The engine: runs an optimized plan on one node.
//!
//! Each node of the plan becomes a pipeline over the batches its inputs
//! produced; scans fetch their files through pruning and decode them on
//! the CPU pool, and every operator runs there too. The result carries
//! an [`Analysis`] for `EXPLAIN ANALYZE` (QE-4, QE-8).
//!
//! Pipelines are not yet fused across nodes nor split into morsels
//! across stages; that arrives with distributed execution (M6), where
//! stages and exchanges are introduced.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampNanosecondArray, new_null_array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use tessera_format::{FileFormat, FormatError};
use tessera_ir::{BinaryOp, Expr, JoinKind, JoinSide, Literal, Plan, PlanError, Registry};
use tessera_kernels::Grouper;
use tessera_kernels::promql::Params;
use tessera_manifest::{DatasetLogs, ManifestError, Manifests};
use tessera_object::{PoolError, Pools};
use tessera_types::event::{OBSERVED_TIMESTAMP, TRACE_ID};
use tessera_types::{DatasetName, Env, Kind, TenantId, Timestamp, Window};

use crate::ExecError;
use crate::batch::Batch;
use crate::cache::{self, ResultCache, ResultKey};
use crate::memory::MemoryPool;
use crate::operator::{
    BuildSide, CollectSink, Extract, Filter, Format, HashAggregate, HashJoin, JoinInput, Limit,
    Operator, Project, RangeVector, Sort, Steps, TopK, TraceGroup,
};
use crate::pipeline::Pipeline;
use crate::promql;
use crate::remote::{LeafTask, Peers, RemoteScan};
use crate::rollups::RollupSource;
use crate::scan::{self, ScanAnalysis};
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
}

impl Default for QueryLimits {
    fn default() -> Self {
        QueryLimits {
            memory_bytes: 1 << 30,
        }
    }
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
    pub(crate) partition_window: Duration,
    pub(crate) peers: Option<(Arc<dyn Peers>, Arc<dyn RemoteScan>)>,
    /// Where the `system` tables' rows come from.
    system_tables: Option<Arc<dyn SystemTables>>,
    /// Where the rollups the planner may read come from (AD-13).
    rollups: Option<Arc<dyn RollupSource>>,
    /// The manifests this node has opened (section 10.8).
    pub(crate) manifests: Manifests,
    /// The dataset logs, for tombstones (SE-5).
    datasets: DatasetLogs,
    /// The tombstones read in the last second, per table.
    tombstones: Mutex<HashMap<TableKey, KeptFilters>>,
    /// Results of closed-range queries (section 10.8).
    results: ResultCache,
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
        let datasets = DatasetLogs::new(Arc::clone(pools.root().store()), env);
        Engine {
            pools,
            format,
            cpu,
            partition_window: Window::DEFAULT_LENGTH,
            peers: None,
            system_tables: None,
            rollups: None,
            manifests,
            datasets,
            tombstones: Mutex::new(HashMap::new()),
            results: ResultCache::new(cache::DEFAULT_BYTES),
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
            if let Some((at, filters)) = cached.get(&key) {
                if at.elapsed() < TOMBSTONE_TTL {
                    return Ok(Arc::clone(filters));
                }
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

    /// Runs a leaf task sent by another node: reads its files and
    /// returns the rows in the scan's schema.
    ///
    /// # Errors
    ///
    /// If a file cannot be read or the predicates cannot be evaluated.
    pub async fn leaf(&self, task: &LeafTask) -> Result<Vec<RecordBatch>, EngineError> {
        let mut analysis = ScanAnalysis::default();
        scan::read_files(self, &task.scan, task.files.clone(), &[], &mut analysis).await
    }

    /// The function registry the engine runs.
    #[must_use]
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Runs an optimized `plan`.
    ///
    /// # Errors
    ///
    /// If the plan is malformed, storage cannot be read, or an operator
    /// fails or exhausts its memory.
    pub async fn run(&self, plan: &Plan) -> Result<QueryOutput, EngineError> {
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
                },
                uses: Vec::new(),
                fingerprint: 0,
            });
        }
        let memory = MemoryPool::new(self.limits.memory_bytes);
        let mut analysis = Analysis::default();
        let batches = self.execute(plan, &memory, &mut analysis).await?;
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

    fn execute<'a>(
        &'a self,
        plan: &'a Plan,
        memory: &'a Arc<MemoryPool>,
        analysis: &'a mut Analysis,
    ) -> futures::future::BoxFuture<'a, Result<Vec<RecordBatch>, EngineError>> {
        Box::pin(async move {
            match plan {
                Plan::Scan(scan) => {
                    let mut scanned = ScanAnalysis::default();
                    let result = scan::run(self, scan, &mut scanned).await;
                    analysis.scans.push(scanned);
                    result
                }
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
                        batches.extend(self.execute(input, memory, analysis).await?);
                    }
                    Ok(batches)
                }
                Plan::Join {
                    left,
                    right,
                    on,
                    kind,
                } => self.join(left, right, on, *kind, memory, analysis).await,
                Plan::TraceGroup {
                    candidates,
                    spans,
                    spanset,
                } => {
                    // Two phases (QL-13): the candidates' trace ids, then
                    // every span of those traces.
                    let ids = match candidates {
                        Some(candidates) => Some(trace_ids(
                            &self.execute(candidates, memory, analysis).await?,
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
                    let batches = self.execute(&spans, memory, analysis).await?;
                    let operator = Box::new(TraceGroup::new(spanset.clone(), Arc::clone(memory)));
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
                        let batches = self.execute(input, memory, analysis).await?;
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
                    let operator = self.operator(other, input, memory)?;
                    let batches = self.execute(input, memory, analysis).await?;
                    self.pipe(operator, batches).await
                }
            }
        })
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
            Plan::Sort { keys, .. } => Box::new(Sort::new(
                keys.clone(),
                input.schema(registry)?,
                Arc::clone(memory),
            )),
            Plan::TopK { keys, k, .. } => Box::new(TopK::new(
                keys.clone(),
                *k,
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

    /// Runs a join (DM-2). The right side runs first: under an inner
    /// join its key values narrow the left side's read when they are few
    /// (the runtime filter of QE-6). The smaller side is then hashed and
    /// the other probes it (QE-12), except that a left join always
    /// probes with its left side, every row of which comes out.
    async fn join(
        &self,
        left: &JoinSide,
        right: &JoinSide,
        on: &[(Expr, Expr)],
        kind: JoinKind,
        memory: &Arc<MemoryPool>,
        analysis: &mut Analysis,
    ) -> Result<Vec<RecordBatch>, EngineError> {
        let left_keys: Vec<Expr> = on.iter().map(|(l, _)| l.clone()).collect();
        let right_keys: Vec<Expr> = on.iter().map(|(_, r)| r.clone()).collect();
        let right_rows = self.execute(&right.plan, memory, analysis).await?;
        let left_plan = match kind {
            JoinKind::Inner => with_runtime_filter(
                left.plan.as_ref().clone(),
                &left_keys,
                &right_keys,
                &right_rows,
            )?,
            JoinKind::Left => left.plan.as_ref().clone(),
        };
        let left_rows = self.execute(&left_plan, memory, analysis).await?;
        let rows_of =
            |batches: &[RecordBatch]| batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let build_left = kind == JoinKind::Inner && rows_of(&left_rows) < rows_of(&right_rows);
        let left_input = JoinInput {
            schema: left.plan.schema(&self.registry)?,
            keys: left_keys,
            name: left.name.clone(),
        };
        let right_input = JoinInput {
            schema: right.plan.schema(&self.registry)?,
            keys: right_keys,
            name: right.name.clone(),
        };
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
        let pool = Arc::clone(memory);
        let operator = self
            .cpu
            .spawn(move || HashJoin::new(kind, build_side, &build_rows, &build, &probe, pool))
            .await??;
        self.pipe(Box::new(operator), probe_rows).await
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
