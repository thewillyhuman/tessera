// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Aggregate functions as accumulators over dense group ids.
//!
//! One accumulator per aggregate expression keeps one state per group.
//! Batch execution feeds it rows with their group ids; the incremental
//! runtime (M7) feeds it the same way, so there is one implementation
//! per function (QE-20).
//!
//! Weights (QE-13): a row with `sample_rate = r < 1` stands for `1 / r`
//! similar rows that were dropped. `count`, `sum` and `avg` scale each
//! row by that weight; the `_raw` variants count what is stored.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, ListArray, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Float64Type, TimeUnit};
use tessera_ir::Literal;

use crate::KernelError;

/// The running state of one aggregate function over every group.
pub trait Accumulator: Send {
    /// Folds one row per element of `groups` into the group's state.
    /// `values` is the argument column, or `None` for `count(*)`;
    /// `sample_rates` is the `sample_rate` column when the input is
    /// weighted.
    ///
    /// # Errors
    ///
    /// If the values are not of the accumulator's type.
    fn update(
        &mut self,
        groups: &[u32],
        values: Option<&ArrayRef>,
        sample_rates: Option<&Float64Array>,
    ) -> Result<(), KernelError>;

    /// The result for every group id below `groups`, in order.
    ///
    /// # Errors
    ///
    /// If the result array cannot be built.
    fn finish(&mut self, groups: usize) -> Result<ArrayRef, KernelError>;

    /// The running state of every group id below `groups` as columns,
    /// for an accumulator of the same function on another node to
    /// fold with [`Accumulator::merge`] (QE-14). The columns are the
    /// ones `tessera_ir::aggregate_state_types` names for the function.
    ///
    /// # Errors
    ///
    /// If the state arrays cannot be built.
    fn state(&mut self, groups: usize) -> Result<Vec<ArrayRef>, KernelError>;

    /// Folds one state row per element of `groups` into the group's
    /// state; `states` are the columns [`Accumulator::state`] gave.
    ///
    /// # Errors
    ///
    /// If the columns are not the function's state.
    fn merge(&mut self, groups: &[u32], states: &[ArrayRef]) -> Result<(), KernelError>;
}

/// The accumulator for the function `name` over an argument of type
/// `input` (`None` for `count(*)`), with the constant `parameters`
/// that follow the argument, such as the quantile of `quantile`.
///
/// # Errors
///
/// `Unsupported` if the function, the argument type or the parameters
/// are not known.
pub fn accumulator(
    name: &str,
    input: Option<&DataType>,
    parameters: &[Literal],
) -> Result<Box<dyn Accumulator>, KernelError> {
    let unsupported = || {
        KernelError::Unsupported(format!(
            "{name} does not accept {}",
            input.map_or_else(|| "no argument".to_owned(), ToString::to_string)
        ))
    };
    let numeric = |t: Option<&DataType>| {
        matches!(
            t,
            Some(DataType::Int32 | DataType::Int64 | DataType::Float64)
        )
    };
    match name {
        "count" => Ok(Box::new(Count {
            weighted: true,
            counts: Vec::new(),
        })),
        "count_raw" => Ok(Box::new(Count {
            weighted: false,
            counts: Vec::new(),
        })),
        "sum" if numeric(input) => Ok(Box::new(Sum {
            weighted: true,
            integer: false,
            sums: Vec::new(),
        })),
        "sum_raw" if numeric(input) => Ok(Box::new(Sum {
            weighted: false,
            integer: !matches!(input, Some(DataType::Float64)),
            sums: Vec::new(),
        })),
        "avg" if numeric(input) => Ok(Box::new(Avg {
            weighted: true,
            sums: Vec::new(),
        })),
        "avg_raw" if numeric(input) => Ok(Box::new(Avg {
            weighted: false,
            sums: Vec::new(),
        })),
        "stddev" | "stddev_raw" | "stdvar" | "stdvar_raw" if numeric(input) => {
            Ok(Box::new(Variance {
                weighted: !name.ends_with("_raw"),
                root: name.starts_with("stddev"),
                moments: Vec::new(),
            }))
        }
        "quantile" if numeric(input) => {
            let q = match parameters {
                [q] => number(q),
                _ => None,
            }
            .ok_or_else(|| KernelError::Unsupported("quantile needs a constant quantile".into()))?;
            Ok(Box::new(Quantile {
                q,
                values: Vec::new(),
            }))
        }
        "min" | "max" => {
            let input = input.ok_or_else(unsupported)?;
            let keep_smaller = name == "min";
            match input {
                DataType::Int32
                | DataType::Int64
                | DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Box::new(ExtremeInt {
                    keep_smaller,
                    output: input.clone(),
                    values: Vec::new(),
                })),
                DataType::Float64 => Ok(Box::new(ExtremeFloat {
                    keep_smaller,
                    values: Vec::new(),
                })),
                DataType::Utf8 => Ok(Box::new(ExtremeText {
                    keep_smaller,
                    values: Vec::new(),
                })),
                _ => Err(unsupported()),
            }
        }
        _ => Err(unsupported()),
    }
}

/// A constant parameter as a number.
#[allow(clippy::cast_precision_loss)]
fn number(literal: &Literal) -> Option<f64> {
    match literal {
        Literal::Float(f) => Some(*f),
        Literal::Int(i) => Some(*i as f64),
        _ => None,
    }
}

/// A sum or count of integers kept in a float, back as an integer. Values
/// beyond 2^53 lose precision there, as `sum` over bigint does in
/// floating point everywhere; a dedicated integer path is a follow-up.
#[allow(clippy::cast_possible_truncation)]
fn whole(value: f64) -> i64 {
    value.round() as i64
}

/// The weight of row `i`: `1 / sample_rate`, or 1 when unweighted or the
/// rate is missing or not positive.
fn weight(sample_rates: Option<&Float64Array>, i: usize) -> f64 {
    match sample_rates {
        Some(rates) if !rates.is_null(i) && rates.value(i) > 0.0 => 1.0 / rates.value(i),
        _ => 1.0,
    }
}

fn grow<T: Clone>(states: &mut Vec<T>, groups: &[u32], empty: T) {
    let needed = groups.iter().map(|g| *g as usize + 1).max().unwrap_or(0);
    if states.len() < needed {
        states.resize(needed, empty);
    }
}

fn as_floats(values: &ArrayRef) -> Result<Float64Array, KernelError> {
    let array = cast(values, &DataType::Float64)?;
    Ok(array
        .as_any()
        .downcast_ref::<Float64Array>()
        .cloned()
        .unwrap_or_else(|| Float64Array::from(Vec::<f64>::new())))
}

/// State column `i` as floats.
fn state_floats(states: &[ArrayRef], i: usize) -> Result<Float64Array, KernelError> {
    let column = states
        .get(i)
        .ok_or_else(|| KernelError::Unsupported(format!("state column {i} is missing")))?;
    as_floats(column)
}

fn as_ints(values: &ArrayRef) -> Result<Int64Array, KernelError> {
    let array = cast(values, &DataType::Int64)?;
    Ok(array
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .unwrap_or_else(|| Int64Array::from(Vec::<i64>::new())))
}

struct Count {
    weighted: bool,
    counts: Vec<f64>,
}

impl Accumulator for Count {
    fn state(&mut self, groups: usize) -> Result<Vec<ArrayRef>, KernelError> {
        self.counts.resize(groups, 0.0);
        Ok(vec![Arc::new(Float64Array::from(self.counts.clone()))])
    }

    fn merge(&mut self, groups: &[u32], states: &[ArrayRef]) -> Result<(), KernelError> {
        let counts = state_floats(states, 0)?;
        grow(&mut self.counts, groups, 0.0);
        for (i, group) in groups.iter().enumerate() {
            if !counts.is_null(i) {
                self.counts[*group as usize] += counts.value(i);
            }
        }
        Ok(())
    }

    fn update(
        &mut self,
        groups: &[u32],
        values: Option<&ArrayRef>,
        sample_rates: Option<&Float64Array>,
    ) -> Result<(), KernelError> {
        grow(&mut self.counts, groups, 0.0);
        for (i, group) in groups.iter().enumerate() {
            if values.is_some_and(|v| v.is_null(i)) {
                continue;
            }
            let w = if self.weighted {
                weight(sample_rates, i)
            } else {
                1.0
            };
            self.counts[*group as usize] += w;
        }
        Ok(())
    }

    fn finish(&mut self, groups: usize) -> Result<ArrayRef, KernelError> {
        self.counts.resize(groups, 0.0);
        if self.weighted {
            Ok(Arc::new(Float64Array::from(self.counts.clone())))
        } else {
            // Unweighted counts are whole numbers and fit exactly.
            Ok(Arc::new(Int64Array::from_iter_values(
                self.counts.iter().map(|c| whole(*c)),
            )))
        }
    }
}

struct Sum {
    weighted: bool,
    integer: bool,
    sums: Vec<Option<f64>>,
}

impl Accumulator for Sum {
    fn state(&mut self, groups: usize) -> Result<Vec<ArrayRef>, KernelError> {
        self.sums.resize(groups, None);
        Ok(vec![Arc::new(Float64Array::from(self.sums.clone()))])
    }

    fn merge(&mut self, groups: &[u32], states: &[ArrayRef]) -> Result<(), KernelError> {
        let sums = state_floats(states, 0)?;
        grow(&mut self.sums, groups, None);
        for (i, group) in groups.iter().enumerate() {
            if sums.is_null(i) {
                continue;
            }
            let slot = &mut self.sums[*group as usize];
            *slot = Some(slot.unwrap_or(0.0) + sums.value(i));
        }
        Ok(())
    }

    fn update(
        &mut self,
        groups: &[u32],
        values: Option<&ArrayRef>,
        sample_rates: Option<&Float64Array>,
    ) -> Result<(), KernelError> {
        let values =
            values.ok_or_else(|| KernelError::Unsupported("sum needs an argument".into()))?;
        grow(&mut self.sums, groups, None);
        let floats = as_floats(values)?;
        for (i, group) in groups.iter().enumerate() {
            if floats.is_null(i) {
                continue;
            }
            let w = if self.weighted {
                weight(sample_rates, i)
            } else {
                1.0
            };
            let slot = &mut self.sums[*group as usize];
            *slot = Some(slot.unwrap_or(0.0) + floats.value(i) * w);
        }
        Ok(())
    }

    fn finish(&mut self, groups: usize) -> Result<ArrayRef, KernelError> {
        self.sums.resize(groups, None);
        if self.integer {
            Ok(Arc::new(
                self.sums
                    .iter()
                    .map(|s| s.map(whole))
                    .collect::<Int64Array>(),
            ))
        } else {
            Ok(Arc::new(Float64Array::from(self.sums.clone())))
        }
    }
}

struct Avg {
    weighted: bool,
    sums: Vec<(f64, f64)>,
}

/// The population variance, or its square root, by Welford's running
/// moments: per group the total weight, the mean and the sum of
/// squared deviations.
struct Variance {
    weighted: bool,
    root: bool,
    moments: Vec<(f64, f64, f64)>,
}

impl Accumulator for Variance {
    fn state(&mut self, groups: usize) -> Result<Vec<ArrayRef>, KernelError> {
        self.moments.resize(groups, (0.0, 0.0, 0.0));
        let column = |pick: fn(&(f64, f64, f64)) -> f64| -> ArrayRef {
            Arc::new(Float64Array::from_iter_values(
                self.moments.iter().map(pick),
            ))
        };
        Ok(vec![column(|m| m.0), column(|m| m.1), column(|m| m.2)])
    }

    /// Chan's parallel combination of two sets' weight, mean and second
    /// moment.
    fn merge(&mut self, groups: &[u32], states: &[ArrayRef]) -> Result<(), KernelError> {
        let totals = state_floats(states, 0)?;
        let means = state_floats(states, 1)?;
        let m2s = state_floats(states, 2)?;
        grow(&mut self.moments, groups, (0.0, 0.0, 0.0));
        for (i, group) in groups.iter().enumerate() {
            if totals.is_null(i) || totals.value(i) <= 0.0 {
                continue;
            }
            let other = totals.value(i);
            let (total, mean, m2) = &mut self.moments[*group as usize];
            let combined = *total + other;
            let delta = means.value(i) - *mean;
            *m2 += m2s.value(i) + delta * delta * *total * other / combined;
            *mean += delta * other / combined;
            *total = combined;
        }
        Ok(())
    }

    fn update(
        &mut self,
        groups: &[u32],
        values: Option<&ArrayRef>,
        sample_rates: Option<&Float64Array>,
    ) -> Result<(), KernelError> {
        let values =
            values.ok_or_else(|| KernelError::Unsupported("stddev needs an argument".into()))?;
        grow(&mut self.moments, groups, (0.0, 0.0, 0.0));
        let floats = as_floats(values)?;
        for (i, group) in groups.iter().enumerate() {
            if floats.is_null(i) {
                continue;
            }
            let w = if self.weighted {
                weight(sample_rates, i)
            } else {
                1.0
            };
            if w <= 0.0 {
                continue;
            }
            let x = floats.value(i);
            let (total, mean, m2) = &mut self.moments[*group as usize];
            *total += w;
            let delta = x - *mean;
            *mean += delta * w / *total;
            *m2 += w * delta * (x - *mean);
        }
        Ok(())
    }

    fn finish(&mut self, groups: usize) -> Result<ArrayRef, KernelError> {
        self.moments.resize(groups, (0.0, 0.0, 0.0));
        let root = self.root;
        Ok(Arc::new(
            self.moments
                .iter()
                .map(|(total, _, m2)| {
                    (*total > 0.0).then(|| {
                        let variance = m2 / total;
                        if root { variance.sqrt() } else { variance }
                    })
                })
                .collect::<Float64Array>(),
        ))
    }
}

impl Accumulator for Avg {
    fn state(&mut self, groups: usize) -> Result<Vec<ArrayRef>, KernelError> {
        self.sums.resize(groups, (0.0, 0.0));
        Ok(vec![
            Arc::new(Float64Array::from_iter_values(
                self.sums.iter().map(|s| s.0),
            )),
            Arc::new(Float64Array::from_iter_values(
                self.sums.iter().map(|s| s.1),
            )),
        ])
    }

    fn merge(&mut self, groups: &[u32], states: &[ArrayRef]) -> Result<(), KernelError> {
        let sums = state_floats(states, 0)?;
        let weights = state_floats(states, 1)?;
        grow(&mut self.sums, groups, (0.0, 0.0));
        for (i, group) in groups.iter().enumerate() {
            if sums.is_null(i) || weights.is_null(i) {
                continue;
            }
            let slot = &mut self.sums[*group as usize];
            slot.0 += sums.value(i);
            slot.1 += weights.value(i);
        }
        Ok(())
    }

    fn update(
        &mut self,
        groups: &[u32],
        values: Option<&ArrayRef>,
        sample_rates: Option<&Float64Array>,
    ) -> Result<(), KernelError> {
        let values =
            values.ok_or_else(|| KernelError::Unsupported("avg needs an argument".into()))?;
        grow(&mut self.sums, groups, (0.0, 0.0));
        let floats = as_floats(values)?;
        for (i, group) in groups.iter().enumerate() {
            if floats.is_null(i) {
                continue;
            }
            let w = if self.weighted {
                weight(sample_rates, i)
            } else {
                1.0
            };
            let slot = &mut self.sums[*group as usize];
            slot.0 += floats.value(i) * w;
            slot.1 += w;
        }
        Ok(())
    }

    fn finish(&mut self, groups: usize) -> Result<ArrayRef, KernelError> {
        self.sums.resize(groups, (0.0, 0.0));
        Ok(Arc::new(
            self.sums
                .iter()
                .map(|(sum, weight)| (*weight > 0.0).then(|| sum / weight))
                .collect::<Float64Array>(),
        ))
    }
}

/// `min`/`max` over integers and timestamps, kept as 64-bit and cast back
/// to the input type at the end: an extremum has the type of its input.
struct ExtremeInt {
    keep_smaller: bool,
    output: DataType,
    values: Vec<Option<i64>>,
}

impl Accumulator for ExtremeInt {
    fn state(&mut self, groups: usize) -> Result<Vec<ArrayRef>, KernelError> {
        Ok(vec![self.finish(groups)?])
    }

    fn merge(&mut self, groups: &[u32], states: &[ArrayRef]) -> Result<(), KernelError> {
        self.update(groups, states.first(), None)
    }

    fn update(
        &mut self,
        groups: &[u32],
        values: Option<&ArrayRef>,
        _: Option<&Float64Array>,
    ) -> Result<(), KernelError> {
        let values =
            values.ok_or_else(|| KernelError::Unsupported("min/max need an argument".into()))?;
        grow(&mut self.values, groups, None);
        let ints = as_ints(values)?;
        for (i, group) in groups.iter().enumerate() {
            if ints.is_null(i) {
                continue;
            }
            let v = ints.value(i);
            let slot = &mut self.values[*group as usize];
            *slot = Some(match *slot {
                Some(current) if self.keep_smaller => current.min(v),
                Some(current) => current.max(v),
                None => v,
            });
        }
        Ok(())
    }

    fn finish(&mut self, groups: usize) -> Result<ArrayRef, KernelError> {
        self.values.resize(groups, None);
        let ints: ArrayRef = Arc::new(Int64Array::from(self.values.clone()));
        if self.output == DataType::Int64 {
            Ok(ints)
        } else {
            Ok(cast(&ints, &self.output)?)
        }
    }
}

struct ExtremeFloat {
    keep_smaller: bool,
    values: Vec<Option<f64>>,
}

impl Accumulator for ExtremeFloat {
    fn state(&mut self, groups: usize) -> Result<Vec<ArrayRef>, KernelError> {
        Ok(vec![self.finish(groups)?])
    }

    fn merge(&mut self, groups: &[u32], states: &[ArrayRef]) -> Result<(), KernelError> {
        self.update(groups, states.first(), None)
    }

    fn update(
        &mut self,
        groups: &[u32],
        values: Option<&ArrayRef>,
        _: Option<&Float64Array>,
    ) -> Result<(), KernelError> {
        let values =
            values.ok_or_else(|| KernelError::Unsupported("min/max need an argument".into()))?;
        grow(&mut self.values, groups, None);
        let floats = as_floats(values)?;
        for (i, group) in groups.iter().enumerate() {
            if floats.is_null(i) {
                continue;
            }
            let v = floats.value(i);
            let slot = &mut self.values[*group as usize];
            *slot = Some(match *slot {
                Some(current) if self.keep_smaller => current.min(v),
                Some(current) => current.max(v),
                None => v,
            });
        }
        Ok(())
    }

    fn finish(&mut self, groups: usize) -> Result<ArrayRef, KernelError> {
        self.values.resize(groups, None);
        Ok(Arc::new(Float64Array::from(self.values.clone())))
    }
}

/// The exact `q`-th quantile of the stored values, interpolated between
/// ranks as Prometheus does; null for a group without values.
struct Quantile {
    q: f64,
    values: Vec<Vec<f64>>,
}

impl Accumulator for Quantile {
    fn state(&mut self, groups: usize) -> Result<Vec<ArrayRef>, KernelError> {
        self.values.resize(groups, Vec::new());
        let lists = ListArray::from_iter_primitive::<Float64Type, _, _>(
            self.values
                .iter()
                .map(|values| Some(values.iter().map(|v| Some(*v)))),
        );
        Ok(vec![Arc::new(lists)])
    }

    fn merge(&mut self, groups: &[u32], states: &[ArrayRef]) -> Result<(), KernelError> {
        let lists = states
            .first()
            .and_then(|s| s.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| KernelError::Unsupported("a quantile state is a list".into()))?;
        grow(&mut self.values, groups, Vec::new());
        for (i, group) in groups.iter().enumerate() {
            if lists.is_null(i) {
                continue;
            }
            let values = lists.value(i);
            let floats = as_floats(&values)?;
            self.values[*group as usize].extend(floats.iter().flatten());
        }
        Ok(())
    }

    fn update(
        &mut self,
        groups: &[u32],
        values: Option<&ArrayRef>,
        _: Option<&Float64Array>,
    ) -> Result<(), KernelError> {
        let values =
            values.ok_or_else(|| KernelError::Unsupported("quantile needs an argument".into()))?;
        grow(&mut self.values, groups, Vec::new());
        let floats = as_floats(values)?;
        for (i, group) in groups.iter().enumerate() {
            if floats.is_null(i) {
                continue;
            }
            self.values[*group as usize].push(floats.value(i));
        }
        Ok(())
    }

    fn finish(&mut self, groups: usize) -> Result<ArrayRef, KernelError> {
        self.values.resize(groups, Vec::new());
        let q = self.q;
        let out: Float64Array = self
            .values
            .iter_mut()
            .map(|values| (!values.is_empty()).then(|| crate::promql::quantile(q, values)))
            .collect();
        Ok(Arc::new(out))
    }
}

struct ExtremeText {
    keep_smaller: bool,
    values: Vec<Option<String>>,
}

impl Accumulator for ExtremeText {
    fn state(&mut self, groups: usize) -> Result<Vec<ArrayRef>, KernelError> {
        Ok(vec![self.finish(groups)?])
    }

    fn merge(&mut self, groups: &[u32], states: &[ArrayRef]) -> Result<(), KernelError> {
        self.update(groups, states.first(), None)
    }

    fn update(
        &mut self,
        groups: &[u32],
        values: Option<&ArrayRef>,
        _: Option<&Float64Array>,
    ) -> Result<(), KernelError> {
        let values =
            values.ok_or_else(|| KernelError::Unsupported("min/max need an argument".into()))?;
        let text = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                KernelError::Unsupported("min/max over text need a text column".into())
            })?;
        grow(&mut self.values, groups, None);
        for (i, group) in groups.iter().enumerate() {
            if text.is_null(i) {
                continue;
            }
            let v = text.value(i);
            let slot = &mut self.values[*group as usize];
            let replace = match slot.as_deref() {
                Some(current) if self.keep_smaller => v < current,
                Some(current) => v > current,
                None => true,
            };
            if replace {
                *slot = Some(v.to_owned());
            }
        }
        Ok(())
    }

    fn finish(&mut self, groups: usize) -> Result<ArrayRef, KernelError> {
        self.values.resize(groups, None);
        Ok(Arc::new(StringArray::from(self.values.clone())))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::TimestampNanosecondArray;

    use super::*;

    fn ints(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn floats(out: &ArrayRef) -> Vec<Option<f64>> {
        out.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    fn longs(out: &ArrayRef) -> Vec<Option<i64>> {
        out.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    #[test]
    fn count_scales_by_the_inverse_sample_rate() {
        let rates = Float64Array::from(vec![1.0, 0.5, 0.25, 1.0]);
        let mut weighted = accumulator("count", None, &[]).unwrap();
        weighted.update(&[0, 0, 1, 1], None, Some(&rates)).unwrap();
        assert_eq!(
            floats(&weighted.finish(2).unwrap()),
            vec![Some(3.0), Some(5.0)]
        );
        let mut raw = accumulator("count_raw", None, &[]).unwrap();
        raw.update(&[0, 0, 1, 1], None, Some(&rates)).unwrap();
        assert_eq!(longs(&raw.finish(2).unwrap()), vec![Some(2), Some(2)]);
    }

    #[test]
    fn quantiles_interpolate_between_ranks() {
        let mut median =
            accumulator("quantile", Some(&DataType::Int64), &[Literal::Float(0.5)]).unwrap();
        median
            .update(
                &[0, 0, 0, 0, 1],
                Some(&ints(&[Some(1), Some(2), Some(3), Some(4), None])),
                None,
            )
            .unwrap();
        assert_eq!(
            floats(&median.finish(2).unwrap()),
            vec![Some(2.5), None],
            "between the second and third of four; none for an empty group"
        );
        assert!(accumulator("quantile", Some(&DataType::Int64), &[]).is_err());
    }

    #[test]
    fn count_of_a_column_skips_nulls() {
        let mut raw = accumulator("count_raw", Some(&DataType::Int64), &[]).unwrap();
        raw.update(&[0, 0, 0], Some(&ints(&[Some(1), None, Some(3)])), None)
            .unwrap();
        assert_eq!(longs(&raw.finish(1).unwrap()), vec![Some(2)]);
    }

    #[test]
    fn sums_and_averages_follow_weights() {
        let rates = Float64Array::from(vec![1.0, 0.5, 1.0]);
        let values = ints(&[Some(10), Some(20), None]);
        let mut sum = accumulator("sum", Some(&DataType::Int64), &[]).unwrap();
        sum.update(&[0, 0, 1], Some(&values), Some(&rates)).unwrap();
        assert_eq!(floats(&sum.finish(2).unwrap()), vec![Some(50.0), None]);
        let mut sum_raw = accumulator("sum_raw", Some(&DataType::Int32), &[]).unwrap();
        sum_raw
            .update(&[0, 0, 1], Some(&values), Some(&rates))
            .unwrap();
        assert_eq!(longs(&sum_raw.finish(2).unwrap()), vec![Some(30), None]);
        let mut avg = accumulator("avg", Some(&DataType::Int64), &[]).unwrap();
        avg.update(&[0, 0, 1], Some(&values), Some(&rates)).unwrap();
        // (10 * 1 + 20 * 2) / (1 + 2)
        assert_eq!(
            floats(&avg.finish(2).unwrap()),
            vec![Some(50.0 / 3.0), None]
        );
        let mut avg_raw = accumulator("avg_raw", Some(&DataType::Int64), &[]).unwrap();
        avg_raw
            .update(&[0, 0, 1], Some(&values), Some(&rates))
            .unwrap();
        assert_eq!(floats(&avg_raw.finish(2).unwrap()), vec![Some(15.0), None]);
    }

    /// `stddev` and `stdvar` are the population moments; the weighted
    /// forms count each row `1 / sample_rate` times.
    #[test]
    fn deviation_and_variance_are_population_moments() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(2.0),
            Some(4.0),
            Some(4.0),
            Some(4.0),
            Some(5.0),
            Some(5.0),
            Some(7.0),
            Some(9.0),
            None,
        ]));
        let groups = [0u32; 9];
        let mut stddev = accumulator("stddev_raw", Some(&DataType::Float64), &[]).unwrap();
        stddev.update(&groups, Some(&values), None).unwrap();
        assert_eq!(floats(&stddev.finish(2).unwrap()), vec![Some(2.0), None]);
        let mut stdvar = accumulator("stdvar_raw", Some(&DataType::Float64), &[]).unwrap();
        stdvar.update(&groups, Some(&values), None).unwrap();
        assert_eq!(floats(&stdvar.finish(1).unwrap()), vec![Some(4.0)]);
        // Two rows at rate 0.5 weigh like four rows of each value.
        let pair: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0), Some(3.0)]));
        let rates = Float64Array::from(vec![Some(0.5), Some(0.5)]);
        let mut weighted = accumulator("stdvar", Some(&DataType::Float64), &[]).unwrap();
        weighted.update(&[0, 0], Some(&pair), Some(&rates)).unwrap();
        assert_eq!(floats(&weighted.finish(1).unwrap()), vec![Some(1.0)]);
    }

    #[test]
    fn extrema_keep_the_input_type() {
        let mut min = accumulator("min", Some(&DataType::Int32), &[]).unwrap();
        let narrow: ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![Some(5), Some(3)]));
        min.update(&[0, 0], Some(&narrow), None).unwrap();
        let out = min.finish(1).unwrap();
        assert_eq!(out.data_type(), &DataType::Int32);
        assert_eq!(
            out.as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(0),
            3
        );
    }

    #[test]
    fn min_and_max_cover_numbers_text_and_time() {
        let mut min = accumulator("min", Some(&DataType::Int64), &[]).unwrap();
        min.update(&[0, 0, 1], Some(&ints(&[Some(5), Some(3), None])), None)
            .unwrap();
        assert_eq!(longs(&min.finish(2).unwrap()), vec![Some(3), None]);
        let mut max = accumulator("max", Some(&DataType::Utf8), &[]).unwrap();
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("b"), Some("c"), Some("a")]));
        max.update(&[0, 0, 0], Some(&text), None).unwrap();
        let out = max.finish(1).unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "c"
        );
        let ts_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let mut latest = accumulator("max", Some(&ts_type), &[]).unwrap();
        let times: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![1, 9, 4]));
        latest.update(&[0, 0, 0], Some(&times), None).unwrap();
        let out = latest.finish(1).unwrap();
        assert_eq!(out.data_type(), &ts_type);
        assert_eq!(
            out.as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(0),
            9
        );
        let mut fmax = accumulator("max", Some(&DataType::Float64), &[]).unwrap();
        let f: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 0.5]));
        fmax.update(&[0, 0], Some(&f), None).unwrap();
        assert_eq!(floats(&fmax.finish(1).unwrap()), vec![Some(1.5)]);
    }

    #[test]
    fn unknown_functions_and_types_are_rejected() {
        assert!(accumulator("median", Some(&DataType::Int64), &[]).is_err());
        assert!(accumulator("sum", Some(&DataType::Utf8), &[]).is_err());
        assert!(accumulator("min", Some(&DataType::Binary), &[]).is_err());
        assert!(accumulator("min", None, &[]).is_err());
    }

    #[test]
    fn groups_never_seen_get_empty_results() {
        let mut sum = accumulator("sum_raw", Some(&DataType::Int64), &[]).unwrap();
        sum.update(&[1], Some(&ints(&[Some(4)])), None).unwrap();
        assert_eq!(longs(&sum.finish(3).unwrap()), vec![None, Some(4), None]);
    }

    /// One accumulator's behaviour under a split: the function, its
    /// argument type, its parameters and its argument column.
    type Case = (
        &'static str,
        Option<DataType>,
        Vec<Literal>,
        Option<ArrayRef>,
    );

    /// Asserts that folding two halves' states gives what one accumulator
    /// over everything gives.
    fn assert_merge_matches((name, input, parameters, values): Case) {
        let rates = Float64Array::from(vec![0.5, 1.0, 0.25, 1.0, 0.5, 1.0]);
        let groups = [0u32, 1, 0, 1, 0, 1];
        let mut whole = accumulator(name, input.as_ref(), &parameters).unwrap();
        whole
            .update(&groups, values.as_ref(), Some(&rates))
            .unwrap();
        let expected = whole.finish(2).unwrap();

        let slice = |from: usize| values.as_ref().map(|a| a.slice(from, 3));
        let mut merged = accumulator(name, input.as_ref(), &parameters).unwrap();
        for from in [0, 3] {
            let mut half = accumulator(name, input.as_ref(), &parameters).unwrap();
            half.update(
                &groups[from..from + 3],
                slice(from).as_ref(),
                Some(&rates.slice(from, 3)),
            )
            .unwrap();
            let states = half.state(2).unwrap();
            merged.merge(&[0, 1], &states).unwrap();
        }
        let got = merged.finish(2).unwrap();
        if got.data_type() == &DataType::Float64 {
            let got = got.as_any().downcast_ref::<Float64Array>().unwrap();
            let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
            for g in 0..2 {
                match (got.is_null(g), expected.is_null(g)) {
                    (true, true) => {}
                    (false, false) => assert!(
                        (got.value(g) - expected.value(g)).abs() < 1e-9,
                        "{name} group {g}: {got:?} vs {expected:?}"
                    ),
                    _ => panic!("{name} group {g}: {got:?} vs {expected:?}"),
                }
            }
        } else {
            assert_eq!(&got, &expected, "{name}");
        }
    }

    /// Six optional floats as a column.
    fn column(values: [Option<f64>; 6]) -> ArrayRef {
        Arc::new(Float64Array::from(values.to_vec()))
    }

    fn six(values: [f64; 6]) -> ArrayRef {
        column(values.map(Some))
    }

    /// QE-14: counts, sums and moments merge their states exactly.
    #[test]
    fn merged_sums_and_moments_equal_one_pass() {
        let f64s = Some(DataType::Float64);
        let i64s = Some(DataType::Int64);
        let numbers = six([1.0, 2.0, 3.0, 4.0, 9.0, 6.0]);
        let cases: Vec<Case> = vec![
            ("count", None, vec![], None),
            (
                "count_raw",
                i64s.clone(),
                vec![],
                Some(ints(&[Some(1), None, Some(3), Some(4), Some(5), Some(6)])),
            ),
            (
                "sum",
                f64s.clone(),
                vec![],
                Some(column([
                    Some(1.0),
                    Some(2.0),
                    None,
                    Some(4.0),
                    Some(5.0),
                    Some(6.0),
                ])),
            ),
            (
                "sum_raw",
                i64s,
                vec![],
                Some(ints(&[
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(4),
                    Some(5),
                    Some(6),
                ])),
            ),
            (
                "avg",
                f64s.clone(),
                vec![],
                Some(column([
                    Some(1.0),
                    Some(2.0),
                    Some(3.0),
                    None,
                    Some(5.0),
                    Some(6.0),
                ])),
            ),
            ("stddev", f64s.clone(), vec![], Some(Arc::clone(&numbers))),
            ("stdvar_raw", f64s, vec![], Some(numbers)),
        ];
        for case in cases {
            assert_merge_matches(case);
        }
    }

    /// QE-14: quantiles and extremes merge their states exactly.
    #[test]
    fn merged_quantiles_and_extremes_equal_one_pass() {
        let f64s = Some(DataType::Float64);
        let text: ArrayRef = Arc::new(StringArray::from(vec![
            Some("b"),
            None,
            Some("c"),
            Some("a"),
            Some("a"),
            Some("z"),
        ]));
        let cases: Vec<Case> = vec![
            (
                "quantile",
                f64s.clone(),
                vec![Literal::Float(0.5)],
                Some(column([
                    Some(1.0),
                    Some(2.0),
                    Some(3.0),
                    Some(4.0),
                    Some(9.0),
                    None,
                ])),
            ),
            (
                "min",
                Some(DataType::Int64),
                vec![],
                Some(ints(&[Some(5), None, Some(3), Some(4), Some(1), Some(6)])),
            ),
            (
                "max",
                f64s,
                vec![],
                Some(column([
                    Some(5.0),
                    None,
                    Some(3.0),
                    Some(4.0),
                    Some(1.0),
                    Some(6.0),
                ])),
            ),
            ("max", Some(DataType::Utf8), vec![], Some(text)),
        ];
        for case in cases {
            assert_merge_matches(case);
        }
    }
}
