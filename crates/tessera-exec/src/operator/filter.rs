// SPDX-License-Identifier: Apache-2.0

//! `Filter`: keep the rows where a predicate holds, as a selection.

use tessera_ir::Expr;
use tessera_kernels::evaluate_predicate;

use crate::ExecError;
use crate::batch::Batch;
use crate::operator::{Flow, Operator, Sink};

/// Narrows each batch's selection to the rows satisfying the predicate.
#[derive(Debug)]
pub struct Filter {
    predicate: Expr,
}

impl Filter {
    /// A filter on `predicate`, which must be boolean.
    #[must_use]
    pub fn new(predicate: Expr) -> Self {
        Filter { predicate }
    }
}

impl Operator for Filter {
    fn push(&mut self, input: Batch, out: &mut dyn Sink) -> Result<Flow, ExecError> {
        let keep = evaluate_predicate(&self.predicate, input.rows())?;
        let selected = input.select(&keep)?;
        if selected.is_empty() {
            return Ok(Flow::Continue);
        }
        out.push(selected)
    }

    fn finish(&mut self, _: &mut dyn Sink) -> Result<(), ExecError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tessera_ir::BinaryOp;

    use super::*;
    use crate::operator::testing::{rows, run, timestamps};

    #[test]
    fn keeps_only_matching_rows() {
        let mut filter =
            Filter::new(Expr::column("severity_number").binary(BinaryOp::GtEq, Expr::literal(20)));
        let out = run(&mut filter, vec![rows(&[1, 2, 3, 4]), rows(&[5])]);
        assert_eq!(timestamps(&out), vec![3, 4]);
    }
}
