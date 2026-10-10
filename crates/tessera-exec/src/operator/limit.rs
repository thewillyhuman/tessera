// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `Limit`: skip `offset` rows, then pass at most `count` and stop the
//! producer early.

use arrow::array::BooleanArray;

use crate::ExecError;
use crate::batch::Batch;
use crate::operator::{Flow, Operator, Sink};

/// Passes a window of the selected rows through.
#[derive(Debug)]
pub struct Limit {
    to_skip: usize,
    remaining: Option<usize>,
}

impl Limit {
    /// Skips `offset` rows and keeps at most `count`, or every remaining
    /// row when `count` is `None`.
    #[must_use]
    pub fn new(offset: usize, count: Option<usize>) -> Self {
        Limit {
            to_skip: offset,
            remaining: count,
        }
    }
}

impl Operator for Limit {
    fn push(&mut self, input: Batch, out: &mut dyn Sink) -> Result<Flow, ExecError> {
        if self.remaining == Some(0) {
            return Ok(Flow::Done);
        }
        let selected = input.num_selected();
        let skip = self.to_skip.min(selected);
        self.to_skip -= skip;
        let take = match self.remaining {
            Some(remaining) => remaining.min(selected - skip),
            None => selected - skip,
        };
        if let Some(remaining) = &mut self.remaining {
            *remaining -= take;
        }
        if take == 0 {
            return Ok(if self.remaining == Some(0) {
                Flow::Done
            } else {
                Flow::Continue
            });
        }
        let batch = if skip == 0 && take == selected {
            input
        } else {
            // Keep the `take` selected rows after the first `skip`.
            let mut seen = 0;
            let keep: BooleanArray = (0..input.rows().num_rows())
                .map(|i| {
                    let selected = input.selection().is_none_or(|s| s.value(i));
                    if !selected {
                        return false;
                    }
                    seen += 1;
                    seen > skip && seen <= skip + take
                })
                .collect();
            input.select(&keep)?
        };
        let flow = out.push(batch)?;
        Ok(if self.remaining == Some(0) {
            Flow::Done
        } else {
            flow
        })
    }

    fn finish(&mut self, _: &mut dyn Sink) -> Result<(), ExecError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::CollectSink;
    use crate::operator::testing::{rows, run, timestamps};

    #[test]
    fn skips_and_stops_across_batches() {
        let mut limit = Limit::new(2, Some(3));
        let out = run(
            &mut limit,
            vec![rows(&[1, 2, 3]), rows(&[4, 5]), rows(&[6])],
        );
        assert_eq!(timestamps(&out), vec![3, 4, 5]);
    }

    #[test]
    fn reports_done_when_satisfied() {
        let mut limit = Limit::new(0, Some(1));
        let mut sink = CollectSink::new();
        assert_eq!(
            limit.push(Batch::new(rows(&[1, 2])), &mut sink).unwrap(),
            Flow::Done
        );
        assert_eq!(sink.num_rows(), 1);
        assert_eq!(
            limit.push(Batch::new(rows(&[3])), &mut sink).unwrap(),
            Flow::Done
        );
        assert_eq!(sink.num_rows(), 1);
    }

    #[test]
    fn an_offset_alone_passes_the_rest() {
        let mut limit = Limit::new(1, None);
        let out = run(&mut limit, vec![rows(&[1, 2]), rows(&[3])]);
        assert_eq!(timestamps(&out), vec![2, 3]);
    }
}
