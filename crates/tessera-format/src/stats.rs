// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Per-column statistics for manifest entries (ST-12): min, max and null
//! count of every column whose type has an order. Columns of other types
//! get a null count only.

use arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use tessera_types::ColumnStats;
use tessera_types::event::Scalar;

/// Statistics of one column.
#[must_use]
pub fn column_stats(name: &str, array: &ArrayRef) -> ColumnStats {
    let (min, max) = min_max(array);
    ColumnStats {
        name: name.to_owned(),
        min,
        max,
        null_count: array.null_count() as u64,
    }
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
