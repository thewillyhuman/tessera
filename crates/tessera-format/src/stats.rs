// SPDX-License-Identifier: Apache-2.0

//! Per-column statistics for manifest entries (ST-12): min, max, null
//! count and octiles of every column whose type has an order, and the
//! distinct non-null values of every comparable column, exact up to
//! [`DISTINCT_CAP`] (AD-6). Columns of other types get a null count only.

use std::collections::HashSet;
use std::hash::{BuildHasher, RandomState};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::row::{RowConverter, SortField};
use tessera_types::ColumnStats;
use tessera_types::event::Scalar;

/// Distinct values are counted exactly up to this many; the cap then
/// stands for "many", which is what a sort-key choice needs to know.
pub const DISTINCT_CAP: u64 = 65_536;

/// The octiles divide the ordered values into this many parts.
const OCTILES: usize = 8;

/// Statistics of one column.
#[must_use]
pub fn column_stats(name: &str, array: &ArrayRef) -> ColumnStats {
    let (min, max) = min_max(array);
    ColumnStats {
        name: name.to_owned(),
        min,
        max,
        null_count: array.null_count() as u64,
        distinct: distinct(array),
        quantiles: quantiles(array),
    }
}

/// The octiles of `array`'s non-null values (AD-6): the value at each
/// eighth of their order, 1/8 to 7/8; none for fewer than eight values
/// or a type without an order.
fn quantiles(array: &ArrayRef) -> Vec<Scalar> {
    let present = array.len() - array.null_count();
    if present < OCTILES {
        return Vec::new();
    }
    let last_nulls = SortOptions {
        descending: false,
        nulls_first: false,
    };
    let Ok(sorted) = arrow::compute::sort(array, Some(last_nulls)) else {
        return Vec::new();
    };
    (1..OCTILES)
        .filter_map(|eighth| scalar_at(&sorted, eighth * present / OCTILES))
        .collect()
}

/// The value of `array` at `index` as a scalar, for the ordered types.
fn scalar_at(array: &ArrayRef, index: usize) -> Option<Scalar> {
    let any = array.as_any();
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => any
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|a| Scalar::Int(a.value(index))),
        DataType::Int64 => any
            .downcast_ref::<Int64Array>()
            .map(|a| Scalar::Int(a.value(index))),
        DataType::Int32 => any
            .downcast_ref::<Int32Array>()
            .map(|a| Scalar::Int(i64::from(a.value(index)))),
        DataType::Float64 => any
            .downcast_ref::<Float64Array>()
            .map(|a| Scalar::Float(a.value(index))),
        DataType::Utf8 => any
            .downcast_ref::<StringArray>()
            .map(|a| Scalar::Text(a.value(index).to_owned())),
        _ => None,
    }
}

/// The distinct non-null values of `array`, told apart by a 64-bit
/// hash of their comparable form, up to [`DISTINCT_CAP`]; none when
/// the type has no comparable form.
fn distinct(array: &ArrayRef) -> Option<u64> {
    let converter = RowConverter::new(vec![SortField::new(array.data_type().clone())]).ok()?;
    let rows = converter.convert_columns(&[Arc::clone(array)]).ok()?;
    let hasher = RandomState::new();
    let mut seen: HashSet<u64> = HashSet::new();
    for (i, row) in rows.iter().enumerate() {
        if array.is_null(i) {
            continue;
        }
        seen.insert(hasher.hash_one(row.as_ref()));
        if seen.len() as u64 >= DISTINCT_CAP {
            return Some(DISTINCT_CAP);
        }
    }
    Some(seen.len() as u64)
}

fn min_max(array: &ArrayRef) -> (Option<Scalar>, Option<Scalar>) {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let values = array.as_any().downcast_ref::<TimestampNanosecondArray>();
            values.map_or((None, None), |a| {
                let (lo, hi) = (arrow::compute::min(a), arrow::compute::max(a));
                (lo.map(Scalar::Int), hi.map(Scalar::Int))
            })
        }
        DataType::Int64 => {
            let values = array.as_any().downcast_ref::<Int64Array>();
            values.map_or((None, None), |a| {
                (
                    arrow::compute::min(a).map(Scalar::Int),
                    arrow::compute::max(a).map(Scalar::Int),
                )
            })
        }
        DataType::Int32 => {
            let values = array.as_any().downcast_ref::<Int32Array>();
            values.map_or((None, None), |a| {
                let lo = arrow::compute::min(a).map(|v| Scalar::Int(i64::from(v)));
                let hi = arrow::compute::max(a).map(|v| Scalar::Int(i64::from(v)));
                (lo, hi)
            })
        }
        DataType::Float64 => {
            let values = array.as_any().downcast_ref::<Float64Array>();
            values.map_or((None, None), |a| {
                (
                    arrow::compute::min(a).map(Scalar::Float),
                    arrow::compute::max(a).map(Scalar::Float),
                )
            })
        }
        DataType::Utf8 => {
            let values = array.as_any().downcast_ref::<StringArray>();
            values.map_or((None, None), |a| {
                let lo = arrow::compute::min_string(a).map(|v| Scalar::Text(v.to_owned()));
                let hi = arrow::compute::max_string(a).map(|v| Scalar::Text(v.to_owned()));
                (lo, hi)
            })
        }
        _ => (None, None),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn integers_and_strings_get_min_and_max() {
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), None, Some(-1)]));
        let stats = column_stats("n", &ints);
        assert_eq!(
            (stats.min, stats.max, stats.null_count),
            (Some(Scalar::Int(-1)), Some(Scalar::Int(3)), 1)
        );
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("b"), Some("a")]));
        let stats = column_stats("s", &text);
        assert_eq!(
            (stats.min, stats.max),
            (
                Some(Scalar::Text("a".into())),
                Some(Scalar::Text("b".into()))
            )
        );
    }

    #[test]
    fn an_all_null_column_has_no_bounds() {
        let nulls: ArrayRef = Arc::new(Float64Array::from(vec![None, None]));
        let stats = column_stats("f", &nulls);
        assert_eq!((stats.min, stats.max, stats.null_count), (None, None, 2));
        assert_eq!(stats.distinct, Some(0));
    }

    /// AD-6: distinct non-null values, exact until the cap.
    #[test]
    fn distinct_values_are_counted_up_to_the_cap() {
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), None, Some(-1), Some(3)]));
        assert_eq!(column_stats("n", &ints).distinct, Some(2));
        let text: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a"]));
        assert_eq!(column_stats("s", &text).distinct, Some(2));
        let past_cap = i64::try_from(DISTINCT_CAP).unwrap() + 7;
        let many: ArrayRef = Arc::new(Int64Array::from_iter_values(0..past_cap));
        assert_eq!(column_stats("m", &many).distinct, Some(DISTINCT_CAP));
    }

    /// AD-6: the octiles mark where a column's values lie, nulls left
    /// out, and fewer than eight values give none.
    #[test]
    fn octiles_mark_the_spread_of_the_values() {
        let hundred: ArrayRef = Arc::new(Int64Array::from_iter_values((0..100).rev()));
        assert_eq!(
            column_stats("n", &hundred).quantiles,
            [12, 25, 37, 50, 62, 75, 87].map(Scalar::Int)
        );
        let with_nulls: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
        ]));
        assert_eq!(
            column_stats("n", &with_nulls).quantiles,
            [2, 3, 4, 5, 6, 7, 8].map(Scalar::Int)
        );
        let few: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        assert!(column_stats("n", &few).quantiles.is_empty());
        let text: ArrayRef = Arc::new(StringArray::from(
            (0..16).map(|i| format!("{i:02}")).collect::<Vec<_>>(),
        ));
        assert_eq!(
            column_stats("s", &text).quantiles[0],
            Scalar::Text("02".into())
        );
    }

    #[test]
    fn timestamps_are_reported_as_nanoseconds() {
        let ts: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![5, 2, 9]));
        let stats = column_stats("timestamp", &ts);
        assert_eq!(
            (stats.min, stats.max),
            (Some(Scalar::Int(2)), Some(Scalar::Int(9)))
        );
    }
}
