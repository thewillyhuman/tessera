// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `HashAggregate`: group rows by key expressions and fold each group
//! through its accumulators. Blocking; accounts its state, and when the
//! grant runs out spills every group's running state to local disk by
//! bucket of its key, to fold each bucket back on its own at the end
//! (QE-11), so that the memory a bucket takes is a fraction of what the
//! groups would.
//!
//! With no keys there is one group, and an empty input still yields one
//! row (a count of zero, other aggregates null), as SQL requires.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use tessera_ir::{AggregateExpr, Expr, Literal, NamedExpr, state_column};
use tessera_kernels::{Accumulator, Grouper, accumulator, evaluate};
use tessera_types::event::SAMPLE_RATE;

use crate::ExecError;
use crate::batch::Batch;
use crate::memory::{MemoryPool, OutOfMemory};
use crate::operator::sort::emit;
use crate::operator::{Flow, Operator, Sink};
use crate::spill::SpillFile;

/// How many buckets spilled groups are split into: folding one back
/// takes this fraction of the memory the groups would.
const BUCKETS: u32 = 16;

/// Which half of an aggregation the operator does (QE-14).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Rows in, results out.
    Full,
    /// Rows in, states out: the keys, then each aggregate's state
    /// columns, for a `Final` operator elsewhere.
    Partial,
    /// States in, results out.
    Final,
}

/// Grouped aggregation in a hash table.
pub struct HashAggregate {
    group_by: Vec<NamedExpr>,
    aggregates: Vec<AggregateExpr>,
    weighted: bool,
    mode: Mode,
    /// How many state columns each aggregate takes, in `Final` mode.
    state_widths: Vec<usize>,
    schema: SchemaRef,
    memory: Arc<MemoryPool>,
    grouper: Option<Grouper>,
    accumulators: Vec<Box<dyn Accumulator>>,
    rows_seen: usize,
    reserved: usize,
    /// What rebuilds the grouper and the accumulators after a spill.
    shape: Shape,
    /// The schema of a spilled run: the keys, then every state column;
    /// none when a function has no state to spill.
    states_schema: Option<SchemaRef>,
    /// The runs spilled so far, by bucket.
    spilled: Vec<Vec<SpillFile>>,
}

/// The types the grouper and the accumulators are built from.
struct Shape {
    key_types: Vec<DataType>,
    arg_types: Vec<Option<DataType>>,
    parameters: Vec<Vec<Literal>>,
}

impl Shape {
    fn grouper(&self) -> Result<Option<Grouper>, ExecError> {
        if self.key_types.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Grouper::new(self.key_types.clone())?))
        }
    }

    fn accumulators(
        &self,
        aggregates: &[AggregateExpr],
    ) -> Result<Vec<Box<dyn Accumulator>>, ExecError> {
        aggregates
            .iter()
            .zip(&self.arg_types)
            .zip(&self.parameters)
            .map(|((a, arg_type), parameters)| {
                Ok(accumulator(&a.function, arg_type.as_ref(), parameters)?)
            })
            .collect()
    }
}

/// A rough size of one group's state, for accounting.
const BYTES_PER_GROUP: usize = 64;

impl HashAggregate {
    /// Groups by `group_by` and computes `aggregates`, producing `schema`
    /// (keys first, then aggregates). `weighted` says whether the input
    /// carries `sample_rate` (QE-13).
    ///
    /// # Errors
    ///
    /// If an aggregate function is unknown or its argument type unsupported.
    pub fn new(
        group_by: Vec<NamedExpr>,
        aggregates: Vec<AggregateExpr>,
        weighted: bool,
        input: &SchemaRef,
        schema: SchemaRef,
        memory: Arc<MemoryPool>,
    ) -> Result<Self, ExecError> {
        Self::build(
            Mode::Full,
            group_by,
            aggregates,
            weighted,
            input,
            input,
            schema,
            memory,
        )
    }

    /// The first half of an aggregation split across nodes (QE-14):
    /// as [`HashAggregate::new`], but `schema` is the states' (keys,
    /// then each aggregate's state columns) and the operator emits them.
    ///
    /// # Errors
    ///
    /// If an aggregate function is unknown or cannot be merged.
    pub fn partial(
        group_by: Vec<NamedExpr>,
        aggregates: Vec<AggregateExpr>,
        weighted: bool,
        input: &SchemaRef,
        schema: SchemaRef,
        memory: Arc<MemoryPool>,
    ) -> Result<Self, ExecError> {
        Self::build(
            Mode::Partial,
            group_by,
            aggregates,
            weighted,
            input,
            input,
            schema,
            memory,
        )
    }

    /// The second half: folds states in the `states` schema, which a
    /// partial operator produced from rows in the `rows` schema, into
    /// the results `schema`. The keys are the states' key columns.
    ///
    /// # Errors
    ///
    /// If an aggregate function is unknown or cannot be merged.
    pub fn merging(
        group_by: Vec<NamedExpr>,
        aggregates: Vec<AggregateExpr>,
        rows: &SchemaRef,
        states: &SchemaRef,
        schema: SchemaRef,
        memory: Arc<MemoryPool>,
    ) -> Result<Self, ExecError> {
        Self::build(
            Mode::Final,
            group_by,
            aggregates,
            false,
            rows,
            states,
            schema,
            memory,
        )
    }

    /// `args` types the aggregates' arguments, `keys` the group keys.
    #[allow(clippy::too_many_arguments, reason = "every part of one operator")]
    fn build(
        mode: Mode,
        group_by: Vec<NamedExpr>,
        aggregates: Vec<AggregateExpr>,
        weighted: bool,
        args: &SchemaRef,
        keys: &SchemaRef,
        schema: SchemaRef,
        memory: Arc<MemoryPool>,
    ) -> Result<Self, ExecError> {
        let registry = tessera_ir::Registry::builtin();
        // How many state columns each aggregate has: what a final
        // operator folds, and what a spill writes; a function without
        // a state leaves the operator unable to spill.
        let widths = aggregates
            .iter()
            .map(|a| {
                tessera_ir::aggregate_state_types(a, args, &registry)
                    .map(|types| types.len())
                    .map_err(|e| ExecError::Unsupported(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>();
        let state_widths = match (mode, widths) {
            (Mode::Final, widths) => widths?,
            (_, Ok(widths)) => widths,
            (_, Err(_)) => Vec::new(),
        };
        let arg_types = aggregates
            .iter()
            .map(|a| {
                a.args
                    .first()
                    .map(|arg| tessera_ir::expr_type(arg, args, &registry))
                    .transpose()
                    .map_err(|e| ExecError::Unsupported(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let parameters = aggregates
            .iter()
            .map(|a| {
                a.args
                    .iter()
                    .skip(1)
                    .filter_map(|arg| match arg {
                        Expr::Literal(literal) => Some(literal.clone()),
                        _ => None,
                    })
                    .collect::<Vec<Literal>>()
            })
            .collect();
        let key_types = group_by
            .iter()
            .map(|k| {
                tessera_ir::expr_type(&k.expr, keys, &registry)
                    .map_err(|e| ExecError::Unsupported(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let states_schema = if mode == Mode::Final {
            Some(Arc::clone(keys))
        } else if state_widths.is_empty() && !aggregates.is_empty() {
            None
        } else {
            states_schema(&group_by, &key_types, &aggregates, args, &registry)
        };
        let shape = Shape {
            key_types,
            arg_types,
            parameters,
        };
        Ok(HashAggregate {
            grouper: shape.grouper()?,
            accumulators: shape.accumulators(&aggregates)?,
            group_by,
            aggregates,
            weighted,
            mode,
            state_widths,
            schema,
            memory,
            rows_seen: 0,
            reserved: 0,
            shape,
            states_schema,
            spilled: Vec::new(),
        })
    }

    /// The group id of each row of `rows`.
    fn group_ids(&mut self, rows: &RecordBatch) -> Result<Vec<u32>, ExecError> {
        match &mut self.grouper {
            Some(grouper) => {
                let keys = self
                    .group_by
                    .iter()
                    .map(|k| evaluate(&k.expr, rows))
                    .collect::<Result<Vec<ArrayRef>, _>>()?;
                Ok(grouper.group_ids(&keys)?)
            }
            None => Ok(vec![0; rows.num_rows()]),
        }
    }

    /// Folds `rows` into the groups: rows of input in `Full` and
    /// `Partial` mode, states in `Final` mode.
    fn fold(&mut self, groups: &[u32], rows: &RecordBatch) -> Result<(), ExecError> {
        if self.mode == Mode::Final {
            self.merge_states(groups, rows, self.group_by.len())
        } else {
            let sample_rates = if self.weighted {
                rows.column_by_name(SAMPLE_RATE)
                    .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
                    .cloned()
            } else {
                None
            };
            for (aggregate, accumulator) in self.aggregates.iter().zip(&mut self.accumulators) {
                let values = aggregate
                    .args
                    .first()
                    .map(|arg| evaluate(arg, rows))
                    .transpose()?;
                accumulator.update(groups, values.as_ref(), sample_rates.as_ref())?;
            }
            Ok(())
        }
    }

    /// Merges the state columns of `rows`, which follow the first
    /// `offset` columns, each aggregate's in turn.
    fn merge_states(
        &mut self,
        groups: &[u32],
        rows: &RecordBatch,
        offset: usize,
    ) -> Result<(), ExecError> {
        let mut offset = offset;
        for (width, accumulator) in self.state_widths.iter().zip(&mut self.accumulators) {
            let states = rows.columns().get(offset..offset + width).ok_or_else(|| {
                ExecError::Unsupported("aggregate states are missing columns".into())
            })?;
            accumulator.merge(groups, states)?;
            offset += width;
        }
        Ok(())
    }

    /// Every group's key and running state as one batch in the states
    /// schema.
    fn states(&mut self, schema: &SchemaRef) -> Result<RecordBatch, ExecError> {
        let groups = self.groups();
        let mut columns: Vec<ArrayRef> = match &self.grouper {
            Some(grouper) => grouper.keys()?,
            None => Vec::new(),
        };
        for accumulator in &mut self.accumulators {
            columns.extend(accumulator.state(groups)?);
        }
        Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
    }

    /// Spills every group's state to disk by bucket and starts over
    /// empty; without a spill directory, a states schema or any group,
    /// `failure` stands.
    fn spill(&mut self, failure: OutOfMemory) -> Result<(), ExecError> {
        let dir = self.memory.spill_dir().map(std::path::Path::to_path_buf);
        let (Some(dir), Some(schema)) = (dir, self.states_schema.clone()) else {
            return Err(failure.into());
        };
        if self.grouper.as_ref().is_some_and(Grouper::is_empty) || self.rows_seen == 0 {
            return Err(failure.into());
        }
        let states = self.states(&schema)?;
        let buckets = self
            .grouper
            .as_ref()
            .map_or_else(|| vec![0], |g| g.buckets(BUCKETS));
        self.spilled
            .resize_with(usize::try_from(BUCKETS).unwrap_or(16), Vec::new);
        for (bucket, runs) in self.spilled.iter_mut().enumerate() {
            let indices: UInt32Array = buckets
                .iter()
                .enumerate()
                .filter(|(_, b)| usize::try_from(**b).unwrap_or(0) == bucket)
                .map(|(group, _)| u32::try_from(group).unwrap_or(u32::MAX))
                .collect();
            if indices.is_empty() {
                continue;
            }
            let columns = states
                .columns()
                .iter()
                .map(|c| take(c, &indices, None).map_err(ExecError::from))
                .collect::<Result<Vec<_>, _>>()?;
            let rows = RecordBatch::try_new(Arc::clone(&schema), columns)?;
            let file = SpillFile::write(&dir, &schema, &[rows])?;
            self.memory.note_spilled(file.bytes());
            runs.push(file);
        }
        self.grouper = self.shape.grouper()?;
        self.accumulators = self.shape.accumulators(&self.aggregates)?;
        self.memory.release(self.reserved);
        self.reserved = 0;
        Ok(())
    }

    /// Reserves what the groups take, spilling first when the grant
    /// refuses.
    fn account(&mut self) -> Result<(), ExecError> {
        let needed = self.groups() * BYTES_PER_GROUP * (1 + self.aggregates.len());
        if needed > self.reserved {
            if let Err(failure) = self.memory.reserve(needed - self.reserved) {
                self.spill(failure)?;
                return Ok(());
            }
            self.reserved = needed;
        }
        Ok(())
    }

    /// The results of the groups held, keys first, as the mode gives
    /// them: states in `Partial` mode, values otherwise.
    fn result(&mut self) -> Result<RecordBatch, ExecError> {
        let groups = self.groups();
        let mut columns: Vec<ArrayRef> = match &self.grouper {
            Some(grouper) => grouper.keys()?,
            None => Vec::new(),
        };
        for accumulator in &mut self.accumulators {
            if self.mode == Mode::Partial {
                columns.extend(accumulator.state(groups)?);
            } else {
                columns.push(accumulator.finish(groups)?);
            }
        }
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }

    /// Folds the spilled runs back one bucket at a time, emitting each
    /// bucket's results.
    fn finish_spilled(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        let Some(schema) = self.states_schema.clone() else {
            return Err(ExecError::Unsupported(
                "spilled states without a schema".into(),
            ));
        };
        // What is held joins the runs, so that every bucket is whole.
        let last = OutOfMemory {
            limit: self.memory.limit(),
            used: self.memory.used(),
            requested: 0,
        };
        if self.groups() > 0 && self.rows_seen > 0 {
            self.spill(last)?;
        }
        let key_columns = self.group_by.len();
        let keys: Vec<NamedExpr> = schema
            .fields()
            .iter()
            .take(key_columns)
            .map(|f| NamedExpr::new(Expr::column(f.name()), f.name().clone()))
            .collect();
        let buckets = std::mem::take(&mut self.spilled);
        for runs in buckets {
            if runs.is_empty() {
                continue;
            }
            self.grouper = self.shape.grouper()?;
            self.accumulators = self.shape.accumulators(&self.aggregates)?;
            for run in &runs {
                for rows in run.read()? {
                    let rows = rows?;
                    let groups = match &mut self.grouper {
                        Some(grouper) => {
                            let columns = keys
                                .iter()
                                .map(|k| evaluate(&k.expr, &rows))
                                .collect::<Result<Vec<ArrayRef>, _>>()?;
                            grouper.group_ids(&columns)?
                        }
                        None => vec![0; rows.num_rows()],
                    };
                    self.merge_states(&groups, &rows, key_columns)?;
                    let needed = self.groups() * BYTES_PER_GROUP * (1 + self.aggregates.len());
                    if needed > self.reserved {
                        self.memory.reserve(needed - self.reserved)?;
                        self.reserved = needed;
                    }
                }
            }
            drop(runs);
            let result = self.result()?;
            self.memory.release(self.reserved);
            self.reserved = 0;
            emit(&result, out)?;
        }
        Ok(())
    }

    fn groups(&self) -> usize {
        match &self.grouper {
            Some(grouper) => grouper.len(),
            None => 1,
        }
    }
}

impl Operator for HashAggregate {
    fn push(&mut self, input: Batch, _: &mut dyn Sink) -> Result<Flow, ExecError> {
        let rows = input.materialize()?;
        if rows.num_rows() == 0 {
            return Ok(Flow::Continue);
        }
        self.rows_seen += rows.num_rows();
        let groups = self.group_ids(&rows)?;
        self.fold(&groups, &rows)?;
        self.account()?;
        Ok(Flow::Continue)
    }

    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        if self.spilled.iter().any(|runs| !runs.is_empty()) {
            return self.finish_spilled(out);
        }
        let result = self.result()?;
        self.memory.release(self.reserved);
        self.reserved = 0;
        emit(&result, out)
    }
}

/// The schema of a spilled run of `Full` or `Partial` mode: the keys,
/// then each aggregate's state columns; none when an aggregate has no
/// state to spill.
fn states_schema(
    group_by: &[NamedExpr],
    key_types: &[DataType],
    aggregates: &[AggregateExpr],
    args: &SchemaRef,
    registry: &tessera_ir::Registry,
) -> Option<SchemaRef> {
    let mut fields: Vec<Field> = group_by
        .iter()
        .zip(key_types)
        .map(|(k, t)| Field::new(&k.name, t.clone(), true))
        .collect();
    for aggregate in aggregates {
        let types = tessera_ir::aggregate_state_types(aggregate, args, registry).ok()?;
        for (i, t) in types.into_iter().enumerate() {
            fields.push(Field::new(state_column(&aggregate.name, i), t, true));
        }
    }
    Some(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Float64Array, Int64Array, StringArray};
    use tessera_ir::{BinaryOp, Expr, Plan, Registry, Scan};
    use tessera_types::{DatasetName, Kind, TenantId};

    use super::*;
    use crate::operator::testing::{rows, run, schema};

    fn output_schema(group_by: &[NamedExpr], aggregates: &[AggregateExpr]) -> SchemaRef {
        // Type the output exactly as the planner would, over the test schema.
        let mut scan = Scan::all(TenantId::DEFAULT, DatasetName::new("t").unwrap(), Kind::Log);
        scan.projection = Some(vec![
            "timestamp".into(),
            "severity_number".into(),
            "body".into(),
            "sample_rate".into(),
        ]);
        Plan::Aggregate {
            input: Box::new(Plan::Scan(scan)),
            group_by: group_by.to_vec(),
            aggregates: aggregates.to_vec(),
        }
        .schema(&Registry::builtin())
        .unwrap()
    }

    #[test]
    fn groups_across_batches_and_counts_with_weights() {
        let group_by = vec![NamedExpr::new(
            Expr::column("severity_number").binary(BinaryOp::GtEq, Expr::literal(10)),
            "high",
        )];
        let aggregates = vec![
            AggregateExpr {
                function: "count".into(),
                args: vec![],
                name: "n".into(),
            },
            AggregateExpr {
                function: "max".into(),
                args: vec![Expr::column("body")],
                name: "last_body".into(),
            },
        ];
        let out_schema = output_schema(&group_by, &aggregates);
        let mut aggregate = HashAggregate::new(
            group_by,
            aggregates,
            true,
            &schema(),
            out_schema,
            MemoryPool::unbounded(),
        )
        .unwrap();
        let out = run(&mut aggregate, vec![rows(&[1, 2, 3]), rows(&[4, 5])]);
        assert_eq!(out.len(), 1);
        let out = &out[0];
        assert_eq!(out.num_rows(), 2);
        let counts = out
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        // severities are 0,10,20 then 0,10: "high" false twice, true three times
        assert_eq!(counts.values().to_vec(), vec![2.0, 3.0]);
        let bodies = out
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(bodies.value(0), "row0");
        assert_eq!(bodies.value(1), "row2");
    }

    #[test]
    fn an_empty_input_without_keys_yields_one_row() {
        let aggregates = vec![
            AggregateExpr {
                function: "count_raw".into(),
                args: vec![],
                name: "n".into(),
            },
            AggregateExpr {
                function: "min".into(),
                args: vec![Expr::column("timestamp")],
                name: "first".into(),
            },
        ];
        let out_schema = output_schema(&[], &aggregates);
        let mut aggregate = HashAggregate::new(
            vec![],
            aggregates,
            false,
            &schema(),
            out_schema,
            MemoryPool::unbounded(),
        )
        .unwrap();
        let out = run(&mut aggregate, vec![]);
        assert_eq!(out.len(), 1);
        let counts = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.value(0), 0);
        assert!(out[0].column(1).is_null(0));
    }

    #[test]
    fn an_empty_input_with_keys_yields_no_rows() {
        let group_by = vec![NamedExpr::new(Expr::column("body"), "body")];
        let aggregates = vec![AggregateExpr {
            function: "count_raw".into(),
            args: vec![],
            name: "n".into(),
        }];
        let out_schema = output_schema(&group_by, &aggregates);
        let mut aggregate = HashAggregate::new(
            group_by,
            aggregates,
            false,
            &schema(),
            out_schema,
            MemoryPool::unbounded(),
        )
        .unwrap();
        let out = run(&mut aggregate, vec![]);
        assert!(out.is_empty());
    }

    #[test]
    fn rejects_unknown_aggregates() {
        let aggregates = vec![AggregateExpr {
            function: "median".into(),
            args: vec![Expr::column("timestamp")],
            name: "m".into(),
        }];
        let result = HashAggregate::new(
            vec![],
            aggregates,
            false,
            &schema(),
            schema(),
            MemoryPool::unbounded(),
        );
        assert!(result.is_err());
    }

    /// QE-14: the states of two partial operators, folded by a final
    /// one, give what one full operator gives.
    #[test]
    fn partial_states_fold_into_the_full_result() {
        let group_by = vec![NamedExpr::new(
            Expr::column("severity_number").binary(BinaryOp::GtEq, Expr::literal(10)),
            "high",
        )];
        let aggregates = vec![
            AggregateExpr {
                function: "count".into(),
                args: vec![],
                name: "n".into(),
            },
            AggregateExpr {
                function: "avg".into(),
                args: vec![Expr::column("severity_number")],
                name: "mean".into(),
            },
            AggregateExpr {
                function: "max".into(),
                args: vec![Expr::column("body")],
                name: "last_body".into(),
            },
        ];
        let full_schema = output_schema(&group_by, &aggregates);
        let mut full = HashAggregate::new(
            group_by.clone(),
            aggregates.clone(),
            true,
            &schema(),
            full_schema.clone(),
            MemoryPool::unbounded(),
        )
        .unwrap();
        let expected = run(&mut full, vec![rows(&[1, 2, 3]), rows(&[4, 5])]);

        let mut scan = Scan::all(TenantId::DEFAULT, DatasetName::new("t").unwrap(), Kind::Log);
        scan.projection = Some(vec![
            "timestamp".into(),
            "severity_number".into(),
            "body".into(),
            "sample_rate".into(),
        ]);
        let partial_plan = Plan::PartialAggregate {
            input: Box::new(Plan::Scan(scan)),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
        };
        let states_schema = partial_plan.schema(&Registry::builtin()).unwrap();
        assert_eq!(states_schema.fields().len(), 1 + 1 + 2 + 1);
        let mut states = Vec::new();
        for input in [vec![rows(&[1, 2, 3])], vec![rows(&[4, 5])]] {
            let mut partial = HashAggregate::partial(
                group_by.clone(),
                aggregates.clone(),
                true,
                &schema(),
                states_schema.clone(),
                MemoryPool::unbounded(),
            )
            .unwrap();
            states.extend(run(&mut partial, input));
        }
        let keys = vec![NamedExpr::new(Expr::column("high"), "high")];
        let mut merging = HashAggregate::merging(
            keys,
            aggregates,
            &schema(),
            &states_schema,
            full_schema,
            MemoryPool::unbounded(),
        )
        .unwrap();
        let got = run(&mut merging, states);
        let sorted = |batches: &[RecordBatch]| -> Vec<(bool, f64, f64, String)> {
            let mut out: Vec<_> = batches
                .iter()
                .flat_map(|b| {
                    let high = b
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .unwrap();
                    let n = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
                    let mean = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
                    let body = b.column(3).as_any().downcast_ref::<StringArray>().unwrap();
                    (0..b.num_rows())
                        .map(|i| {
                            (
                                high.value(i),
                                n.value(i),
                                mean.value(i),
                                body.value(i).to_owned(),
                            )
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
            out.sort_by(|a, b| a.partial_cmp(b).unwrap());
            out
        };
        assert_eq!(sorted(&got), sorted(&expected));
    }

    /// QE-11: past its grant the aggregate spills its groups' states by
    /// bucket and folds them back, giving what an unbounded run gives,
    /// with the files gone afterwards.
    #[test]
    fn spills_group_states_and_folds_them_back() {
        let dir =
            std::env::temp_dir().join(format!("tessera-aggregate-spill-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let group_by = vec![NamedExpr::new(Expr::column("body"), "body")];
        // An average first, whose state is two columns, so that the fold
        // back reads the states at the right offsets.
        let aggregates = vec![
            AggregateExpr {
                function: "avg".into(),
                args: vec![Expr::column("severity_number")],
                name: "mean".into(),
            },
            AggregateExpr {
                function: "count".into(),
                args: vec![],
                name: "n".into(),
            },
        ];
        let out_schema = output_schema(&group_by, &aggregates);
        // Each batch repeats the bodies row0..row4, so five groups grow
        // across forty batches.
        let inputs: Vec<RecordBatch> = (0..40)
            .map(|i| rows(&[i, i + 1, i + 2, i + 3, i + 4]))
            .collect();
        let mut unbounded = HashAggregate::new(
            group_by.clone(),
            aggregates.clone(),
            true,
            &schema(),
            out_schema.clone(),
            MemoryPool::unbounded(),
        )
        .unwrap();
        let expected = run(&mut unbounded, inputs.clone());

        let pool = MemoryPool::with_spill_dir(BYTES_PER_GROUP * 3 * 2, Some(dir.clone()));
        let mut spilling = HashAggregate::new(
            group_by,
            aggregates,
            true,
            &schema(),
            out_schema,
            Arc::clone(&pool),
        )
        .unwrap();
        let got = run(&mut spilling, inputs);
        let sorted = |batches: &[RecordBatch]| -> Vec<(String, f64, f64)> {
            let mut out: Vec<_> = batches
                .iter()
                .flat_map(|b| {
                    let body = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                    let mean = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
                    let n = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
                    (0..b.num_rows())
                        .map(|i| (body.value(i).to_owned(), mean.value(i), n.value(i)))
                        .collect::<Vec<_>>()
                })
                .collect();
            out.sort_by(|a, b| a.partial_cmp(b).unwrap());
            out
        };
        assert_eq!(sorted(&got), sorted(&expected));
        assert_eq!(sorted(&got).len(), 5);
        assert!(pool.spilled_bytes() > 0, "states went to disk");
        assert_eq!(pool.used(), 0);
        assert!(
            std::fs::read_dir(&dir).unwrap().next().is_none(),
            "spill files are removed"
        );
    }
}
