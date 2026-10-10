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

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};

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
}

/// The accumulator for the function `name` over an argument of type
/// `input` (`None` for `count(*)`).
///
/// # Errors
///
/// `Unsupported` if the function or the argument type is not known.
pub fn accumulator(
    name: &str,
    input: Option<&DataType>,
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
        "min" | "max" => {
            let input = input.ok_or_else(unsupported)?;
            let keep_smaller = name == "min";
            match input {
                DataType::Int32
                | DataType::Int64
                | DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Box::new(ExtremeInt {
                    keep_smaller,
                    output: widened(input),
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

/// A sum or count of integers kept in a float, back as an integer. Values
/// beyond 2^53 lose precision there, as `sum` over bigint does in
/// floating point everywhere; a dedicated integer path is a follow-up.
#[allow(clippy::cast_possible_truncation)]
fn whole(value: f64) -> i64 {
    value.round() as i64
}

fn widened(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Int32 => DataType::Int64,
        other => other.clone(),
    }
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

impl Accumulator for Avg {
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

struct ExtremeInt {
    keep_smaller: bool,
    output: DataType,
    values: Vec<Option<i64>>,
}

impl Accumulator for ExtremeInt {
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

struct ExtremeText {
    keep_smaller: bool,
    values: Vec<Option<String>>,
}

impl Accumulator for ExtremeText {
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
        let mut weighted = accumulator("count", None).unwrap();
        weighted.update(&[0, 0, 1, 1], None, Some(&rates)).unwrap();
        assert_eq!(
            floats(&weighted.finish(2).unwrap()),
            vec![Some(3.0), Some(5.0)]
        );
        let mut raw = accumulator("count_raw", None).unwrap();
        raw.update(&[0, 0, 1, 1], None, Some(&rates)).unwrap();
        assert_eq!(longs(&raw.finish(2).unwrap()), vec![Some(2), Some(2)]);
    }

    #[test]
    fn count_of_a_column_skips_nulls() {
        let mut raw = accumulator("count_raw", Some(&DataType::Int64)).unwrap();
        raw.update(&[0, 0, 0], Some(&ints(&[Some(1), None, Some(3)])), None)
            .unwrap();
        assert_eq!(longs(&raw.finish(1).unwrap()), vec![Some(2)]);
    }

    #[test]
    fn sums_and_averages_follow_weights() {
        let rates = Float64Array::from(vec![1.0, 0.5, 1.0]);
        let values = ints(&[Some(10), Some(20), None]);
        let mut sum = accumulator("sum", Some(&DataType::Int64)).unwrap();
        sum.update(&[0, 0, 1], Some(&values), Some(&rates)).unwrap();
        assert_eq!(floats(&sum.finish(2).unwrap()), vec![Some(50.0), None]);
        let mut sum_raw = accumulator("sum_raw", Some(&DataType::Int32)).unwrap();
        sum_raw
            .update(&[0, 0, 1], Some(&values), Some(&rates))
            .unwrap();
        assert_eq!(longs(&sum_raw.finish(2).unwrap()), vec![Some(30), None]);
        let mut avg = accumulator("avg", Some(&DataType::Int64)).unwrap();
        avg.update(&[0, 0, 1], Some(&values), Some(&rates)).unwrap();
        // (10 * 1 + 20 * 2) / (1 + 2)
        assert_eq!(
            floats(&avg.finish(2).unwrap()),
            vec![Some(50.0 / 3.0), None]
        );
        let mut avg_raw = accumulator("avg_raw", Some(&DataType::Int64)).unwrap();
        avg_raw
            .update(&[0, 0, 1], Some(&values), Some(&rates))
            .unwrap();
        assert_eq!(floats(&avg_raw.finish(2).unwrap()), vec![Some(15.0), None]);
    }

    #[test]
    fn min_and_max_cover_numbers_text_and_time() {
        let mut min = accumulator("min", Some(&DataType::Int64)).unwrap();
        min.update(&[0, 0, 1], Some(&ints(&[Some(5), Some(3), None])), None)
            .unwrap();
        assert_eq!(longs(&min.finish(2).unwrap()), vec![Some(3), None]);
        let mut max = accumulator("max", Some(&DataType::Utf8)).unwrap();
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("b"), Some("c"), Some("a")]));
        max.update(&[0, 0, 0], Some(&text), None).unwrap();
        let out = max.finish(1).unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "c"
        );
        let ts_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let mut latest = accumulator("max", Some(&ts_type)).unwrap();
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
        let mut fmax = accumulator("max", Some(&DataType::Float64)).unwrap();
        let f: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 0.5]));
        fmax.update(&[0, 0], Some(&f), None).unwrap();
        assert_eq!(floats(&fmax.finish(1).unwrap()), vec![Some(1.5)]);
    }

    #[test]
    fn unknown_functions_and_types_are_rejected() {
        assert!(accumulator("median", Some(&DataType::Int64)).is_err());
        assert!(accumulator("sum", Some(&DataType::Utf8)).is_err());
        assert!(accumulator("min", Some(&DataType::Binary)).is_err());
        assert!(accumulator("min", None).is_err());
    }

    #[test]
    fn groups_never_seen_get_empty_results() {
        let mut sum = accumulator("sum_raw", Some(&DataType::Int64)).unwrap();
        sum.update(&[1], Some(&ints(&[Some(4)])), None).unwrap();
        assert_eq!(longs(&sum.finish(3).unwrap()), vec![None, Some(4), None]);
    }
}
