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

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, new_null_array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use tessera_format::{FileFormat, FormatError};
use tessera_ir::{Literal, Plan, PlanError, Registry};
use tessera_manifest::ManifestError;
use tessera_object::{PoolError, Pools};
use tessera_types::{Env, Window};

use crate::ExecError;
use crate::batch::Batch;
use crate::memory::MemoryPool;
use crate::operator::{
    CollectSink, Extract, Filter, Format, HashAggregate, Limit, Operator, Project, Sort, TopK,
};
use crate::pipeline::Pipeline;
use crate::scan::{self, ScanAnalysis};
use crate::scheduler::{CpuPool, TaskError};

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

/// The engine of one node.
pub struct Engine {
    pub(crate) pools: Arc<Pools>,
    pub(crate) format: Arc<dyn FileFormat>,
    pub(crate) env: Env,
    pub(crate) cpu: Arc<CpuPool>,
    pub(crate) partition_window: Duration,
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
        Engine {
            pools,
            format,
            env,
            cpu,
            partition_window: Window::DEFAULT_LENGTH,
            limits,
            registry: Registry::builtin(),
        }
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
        let memory = MemoryPool::new(self.limits.memory_bytes);
        let mut analysis = Analysis::default();
        let batches = self.execute(plan, &memory, &mut analysis).await?;
        Ok(QueryOutput {
            schema,
            batches,
            analysis,
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
                Plan::Union { inputs } => {
                    let mut batches = Vec::new();
                    for input in inputs {
                        batches.extend(self.execute(input, memory, analysis).await?);
                    }
                    Ok(batches)
                }
                Plan::Filter { input, predicate } => {
                    let batches = self.execute(input, memory, analysis).await?;
                    self.pipe(Box::new(Filter::new(predicate.clone())), batches)
                        .await
                }
                Plan::Extract {
                    input,
                    source,
                    stage,
                } => {
                    let batches = self.execute(input, memory, analysis).await?;
                    let schema = plan.schema(&self.registry)?;
                    let operator = Extract::new(source.clone(), stage.clone(), schema);
                    self.pipe(Box::new(operator), batches).await
                }
                Plan::Format {
                    input,
                    line,
                    labels,
                    drop,
                    keep,
                } => {
                    let batches = self.execute(input, memory, analysis).await?;
                    let schema = plan.schema(&self.registry)?;
                    let operator = Format::new(
                        line.clone(),
                        labels.clone(),
                        drop.clone(),
                        keep.clone(),
                        schema,
                    );
                    self.pipe(Box::new(operator), batches).await
                }
                Plan::Project { input, exprs } => {
                    let batches = self.execute(input, memory, analysis).await?;
                    let schema = plan.schema(&self.registry)?;
                    self.pipe(Box::new(Project::new(exprs.clone(), schema)), batches)
                        .await
                }
                Plan::Aggregate {
                    input,
                    group_by,
                    aggregates,
                } => {
                    let weighted = input.weighted(&self.registry)?;
                    let input_schema = input.schema(&self.registry)?;
                    let batches = self.execute(input, memory, analysis).await?;
                    let schema = plan.schema(&self.registry)?;
                    let operator = HashAggregate::new(
                        group_by.clone(),
                        aggregates.clone(),
                        weighted,
                        &input_schema,
                        schema,
                        Arc::clone(memory),
                    )?;
                    self.pipe(Box::new(operator), batches).await
                }
                Plan::Sort { input, keys } => {
                    let schema = input.schema(&self.registry)?;
                    let batches = self.execute(input, memory, analysis).await?;
                    let operator = Sort::new(keys.clone(), schema, Arc::clone(memory));
                    self.pipe(Box::new(operator), batches).await
                }
                Plan::TopK { input, keys, k } => {
                    let schema = input.schema(&self.registry)?;
                    let batches = self.execute(input, memory, analysis).await?;
                    let operator = TopK::new(keys.clone(), *k, schema, Arc::clone(memory));
                    self.pipe(Box::new(operator), batches).await
                }
                Plan::Limit {
                    input,
                    offset,
                    count,
                } => {
                    let batches = self.execute(input, memory, analysis).await?;
                    self.pipe(Box::new(Limit::new(*offset, *count)), batches)
                        .await
                }
            }
        })
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
