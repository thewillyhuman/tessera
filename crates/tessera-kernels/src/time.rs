// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Time kernels: `time_bucket`.

use std::sync::Arc;

use arrow::array::{ArrayRef, TimestampNanosecondArray};

use crate::KernelError;

/// Floors each timestamp to a multiple of `interval` nanoseconds since the
/// Unix epoch, as SQL `time_bucket(interval, timestamp)`.
///
/// # Errors
///
/// `Unsupported` if `interval` is not positive.
pub fn time_bucket(
    interval: i64,
    timestamps: &TimestampNanosecondArray,
) -> Result<ArrayRef, KernelError> {
    if interval <= 0 {
        return Err(KernelError::Unsupported(
            "time_bucket needs a positive interval".into(),
        ));
    }
    let buckets: TimestampNanosecondArray = timestamps
        .iter()
        .map(|t| t.map(|t| t.div_euclid(interval) * interval))
        .collect();
    Ok(Arc::new(buckets.with_timezone_opt(timestamps.timezone())))
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;

    use super::*;

    #[test]
    fn buckets_floor_to_the_interval() {
        let times =
            TimestampNanosecondArray::from(vec![Some(59), Some(60), Some(121), None, Some(-1)]);
        let buckets = time_bucket(60, &times).unwrap();
        let buckets = buckets
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(buckets.value(0), 0);
        assert_eq!(buckets.value(1), 60);
        assert_eq!(buckets.value(2), 120);
        assert!(buckets.is_null(3));
        assert_eq!(buckets.value(4), -60);
        assert!(time_bucket(0, &times).is_err());
    }
}
