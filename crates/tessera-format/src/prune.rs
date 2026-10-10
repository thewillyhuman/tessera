// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Row-group pruning against Parquet column statistics (stage 6 of
//! section 10.4).
//!
//! A row group is skipped when some bound cannot be met by any value in
//! it: its recorded maximum is below the bound's minimum, or its minimum
//! above the bound's maximum. Statistics that are missing, or of a type
//! that is not compared, never prune, so pruning is always safe.

use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use tessera_types::event::Scalar;

use crate::scan::Bound;

/// Whether `row_group` can hold a row satisfying every bound. `column`
/// maps a column name to its index in the row group.
#[must_use]
pub fn may_match(
    row_group: &RowGroupMetaData,
    bounds: &[Bound],
    column: impl Fn(&str) -> Option<usize>,
) -> bool {
    bounds.iter().all(|bound| {
        let Some(index) = column(&bound.column) else {
            return true;
        };
        let Some(stats) = row_group.column(index).statistics() else {
            return true;
        };
        !excludes(stats, bound)
    })
}

/// A statistic or bound value that orders with numbers of either kind.
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug)]
enum Number {
    Int(i64),
    Float(f64),
}

impl Number {
    /// Whether `self < other`. Integers compare exactly; a mixed pair
    /// compares in floating point, whose rounding is monotonic, so a
    /// row group is never pruned that could hold a qualifying value.
    #[allow(clippy::cast_precision_loss)]
    fn below(self, other: Number) -> bool {
        match (self, other) {
            (Number::Int(a), Number::Int(b)) => a < b,
            (Number::Float(a), Number::Float(b)) => a < b,
            (Number::Int(a), Number::Float(b)) => (a as f64) < b,
            (Number::Float(a), Number::Int(b)) => a < b as f64,
        }
    }
}

fn number(scalar: &Scalar) -> Option<Number> {
    match scalar {
        Scalar::Int(i) => Some(Number::Int(*i)),
        Scalar::Float(f) => Some(Number::Float(*f)),
        Scalar::Bool(_) | Scalar::Text(_) => None,
    }
}

fn excludes(stats: &Statistics, bound: &Bound) -> bool {
    match stats {
        Statistics::Int64(s) => excludes_numbers(
            s.min_opt().map(|v| Number::Int(*v)),
            s.max_opt().map(|v| Number::Int(*v)),
            bound,
        ),
        Statistics::Int32(s) => excludes_numbers(
            s.min_opt().map(|v| Number::Int(i64::from(*v))),
            s.max_opt().map(|v| Number::Int(i64::from(*v))),
            bound,
        ),
        Statistics::Double(s) => excludes_numbers(
            s.min_opt().map(|v| Number::Float(*v)),
            s.max_opt().map(|v| Number::Float(*v)),
            bound,
        ),
        Statistics::Float(s) => excludes_numbers(
            s.min_opt().map(|v| Number::Float(f64::from(*v))),
            s.max_opt().map(|v| Number::Float(f64::from(*v))),
            bound,
        ),
        Statistics::ByteArray(s) => {
            let min = s.min_opt().and_then(|v| v.as_utf8().ok());
            let max = s.max_opt().and_then(|v| v.as_utf8().ok());
            let below = match (&bound.min, max) {
                (Some(Scalar::Text(wanted)), Some(max)) => max < wanted.as_str(),
                _ => false,
            };
            let above = match (&bound.max, min) {
                (Some(Scalar::Text(wanted)), Some(min)) => min > wanted.as_str(),
                _ => false,
            };
            below || above
        }
        Statistics::Boolean(_) | Statistics::Int96(_) | Statistics::FixedLenByteArray(_) => false,
    }
}

fn excludes_numbers(min: Option<Number>, max: Option<Number>, bound: &Bound) -> bool {
    let below = match (bound.min.as_ref().and_then(number), max) {
        (Some(wanted), Some(max)) => max.below(wanted),
        _ => false,
    };
    let above = match (bound.max.as_ref().and_then(number), min) {
        (Some(wanted), Some(min)) => wanted.below(min),
        _ => false,
    };
    below || above
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_numbers_order_safely() {
        assert!(Number::Int(1).below(Number::Float(1.5)));
        assert!(!Number::Int(2).below(Number::Float(1.5)));
        assert!(Number::Float(0.5).below(Number::Int(1)));
        assert!(!Number::Int(i64::MAX).below(Number::Int(i64::MAX)));
        assert!(Number::Int(i64::MAX - 1).below(Number::Int(i64::MAX)));
    }

    #[test]
    fn bounds_without_numbers_never_prune() {
        let bound = Bound {
            column: "x".into(),
            min: Some(Scalar::Text("a".into())),
            max: Some(Scalar::Bool(true)),
        };
        assert!(!excludes_numbers(
            Some(Number::Int(0)),
            Some(Number::Int(1)),
            &bound
        ));
    }
}
