// SPDX-License-Identifier: Apache-2.0

//! `Project`: compute output columns from each input row.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use tessera_ir::NamedExpr;
use tessera_kernels::evaluate;

use crate::ExecError;
use crate::batch::Batch;
use crate::operator::{Flow, Operator, Sink};

/// Evaluates expressions over the selected rows of each batch.
#[derive(Debug)]
pub struct Project {
    exprs: Vec<NamedExpr>,
    schema: SchemaRef,
}

impl Project {
    /// A projection producing `schema`, one field per expression.
    #[must_use]
    pub fn new(exprs: Vec<NamedExpr>, schema: SchemaRef) -> Self {
        Project { exprs, schema }
    }
}

impl Operator for Project {
    fn push(&mut self, input: Batch, out: &mut dyn Sink) -> Result<Flow, ExecError> {
        let rows = input.materialize()?;
        let columns = self
            .exprs
            .iter()
            .map(|e| evaluate(&e.expr, &rows))
            .collect::<Result<Vec<_>, _>>()?;
        let projected = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        out.push(Batch::new(projected))
    }

    fn finish(&mut self, _: &mut dyn Sink) -> Result<(), ExecError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, BooleanArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use tessera_ir::{BinaryOp, Expr};

    use super::*;
    use crate::operator::CollectSink;
    use crate::operator::testing::rows;

    #[test]
    fn evaluates_over_the_selected_rows_only() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "double",
            DataType::Int64,
            true,
        )]));
        let mut project = Project::new(
            vec![NamedExpr::new(
                Expr::column("severity_number").binary(BinaryOp::Mul, Expr::literal(2)),
                "double",
            )],
            schema,
        );
        let input = Batch::new(rows(&[1, 2, 3]))
            .select(&BooleanArray::from(vec![true, false, true]))
            .unwrap();
        let mut sink = CollectSink::new();
        project.push(input, &mut sink).unwrap();
        let out = &sink.batches()[0];
        let doubled = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(doubled.len(), 2);
        assert_eq!(doubled.values().to_vec(), vec![0, 40]);
    }
}
