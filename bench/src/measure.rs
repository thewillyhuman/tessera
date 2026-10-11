// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Percentiles of latency samples.

use std::time::Duration;

/// The percentiles of a sample.
#[derive(Clone, Copy, Debug, Default)]
pub struct Percentiles {
    /// The median.
    pub p50: Duration,
    /// The 95th percentile.
    pub p95: Duration,
    /// The 99th percentile.
    pub p99: Duration,
    /// The largest.
    pub max: Duration,
}

/// The percentiles of `samples`, by nearest rank; zero without samples.
pub fn percentiles(samples: &[Duration]) -> Percentiles {
    if samples.is_empty() {
        return Percentiles::default();
    }
    let mut sorted = samples.to_vec();
    sorted.sort();
    let at = |p: f64| {
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let index = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[index.min(sorted.len() - 1)]
    };
    Percentiles {
        p50: at(50.0),
        p95: at(95.0),
        p99: at(99.0),
        max: sorted[sorted.len() - 1],
    }
}
