// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `HashAggregate`: group rows by key expressions and fold each group
//! through its accumulators. Blocking; accounts its state.
//!
//! With no keys there is one group, and an empty input still yields one
//! row (a count of zero, other aggregates null), as SQL requires.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use tessera_ir::{AggregateExpr, NamedExpr};
use tessera_kernels::{Accumulator, Grouper, accumulator, evaluate};
use tessera_types::event::SAMPLE_RATE;

use crate::ExecError;
use crate::batch::Batch;
use crate::memory::MemoryPool;
use crate::operator::sort::emit;
use crate::operator::{Flow, Operator, Sink};

/// Grouped aggregation in a hash table.
pub struct HashAggregate {
    group_by: Vec<NamedExpr>,
    aggregates: Vec<AggregateExpr>,
    weighted: bool,
    schema: SchemaRef,
    memory: Arc<MemoryPool>,
    grouper: Option<Grouper>,
    accumulators: Vec<Box<dyn Accumulator>>,
    rows_seen: usize,
    reserved: usize,
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
        let registry = tessera_ir::Registry::builtin();
        let accumulators = aggregates
            .iter()
            .map(|a| {
                let arg_type = a
                    .args
                    .first()
                    .map(|arg| tessera_ir::expr_type(arg, input, &registry))
                    .transpose()
                    .map_err(|e| ExecError::Unsupported(e.to_string()))?;
                Ok(accumulator(&a.function, arg_type.as_ref())?)
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        let grouper = if group_by.is_empty() {
            None
        } else {
            let key_types = group_by
                .iter()
                .map(|k| {
                    tessera_ir::expr_type(&k.expr, input, &registry)
                        .map_err(|e| ExecError::Unsupported(e.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Some(Grouper::new(key_types)?)
        };
        Ok(HashAggregate {
            group_by,
            aggregates,
            weighted,
            schema,
            memory,
            grouper,
            accumulators,
            rows_seen: 0,
            reserved: 0,
        })
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
        let groups = match &mut self.grouper {
            Some(grouper) => {
                let keys = self
                    .group_by
                    .iter()
                    .map(|k| evaluate(&k.expr, &rows))
                    .collect::<Result<Vec<ArrayRef>, _>>()?;
                grouper.group_ids(&keys)?
            }
            None => vec![0; rows.num_rows()],
        };
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
                .map(|arg| evaluate(arg, &rows))
                .transpose()?;
            accumulator.update(&groups, values.as_ref(), sample_rates.as_ref())?;
        }
        let needed = self.groups() * BYTES_PER_GROUP * (1 + self.aggregates.len());
        if needed > self.reserved {
            self.memory.reserve(needed - self.reserved)?;
            self.reserved = needed;
        }
        Ok(Flow::Continue)
    }

    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        let groups = self.groups();
        let mut columns: Vec<ArrayRef> = match &self.grouper {
            Some(grouper) => grouper.keys()?,
            None => Vec::new(),
        };
        for accumulator in &mut self.accumulators {
            columns.push(accumulator.finish(groups)?);
        }
        let result = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        self.memory.release(self.reserved);
        self.reserved = 0;
        emit(&result, out)
    }
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
}
