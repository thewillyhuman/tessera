// SPDX-License-Identifier: Apache-2.0

//! The recent ingest volume of each dataset (IN-9): a smoothed rate of
//! bytes per second, from which the size of the dataset's preferred set
//! of ingesters follows. A quiet dataset keeps one writer; a dataset
//! sending more than one ingester comfortably takes gets another per
//! such share, up to the cluster's cap.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use tessera_types::{DatasetName, TenantId, Timestamp};

/// Bytes per second one ingester takes comfortably: the share of a
/// dataset's volume that earns it one more preferred ingester.
pub const BYTES_PER_INGESTER: f64 = 16.0 * 1024.0 * 1024.0;
/// How long a burst takes to fade to half its weight in the rate.
const HALF_LIFE_SECONDS: f64 = 30.0;

/// A dataset's smoothed rate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Volume {
    /// Bytes per second, smoothed.
    pub bytes_per_second: f64,
    /// When it was last observed.
    at: Timestamp,
}

impl Volume {
    /// How many preferred ingesters the volume earns, at least one and
    /// at most `cap`.
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn preferred(&self, cap: usize) -> usize {
        let shares = (self.bytes_per_second / BYTES_PER_INGESTER).ceil().max(1.0) as usize;
        shares.clamp(1, cap.max(1))
    }
}

/// The volumes of every dataset written through this node.
#[derive(Default)]
pub struct Volumes {
    rates: Mutex<HashMap<(TenantId, DatasetName), Volume>>,
}

impl Volumes {
    /// Records `bytes` of `dataset` arriving at `now` and returns the
    /// dataset's volume with them: the rate since the last write,
    /// weighed in against the smoothed one by the time elapsed.
    #[allow(clippy::cast_precision_loss)]
    pub fn observe(
        &self,
        tenant: &TenantId,
        dataset: &DatasetName,
        bytes: usize,
        now: Timestamp,
    ) -> Volume {
        let mut rates = self.rates.lock().unwrap_or_else(PoisonError::into_inner);
        let volume = rates
            .entry((tenant.clone(), dataset.clone()))
            .or_insert(Volume {
                bytes_per_second: 0.0,
                at: now,
            });
        let elapsed = (now.as_nanos().saturating_sub(volume.at.as_nanos()) as f64 / 1e9).max(0.001);
        let instant = bytes as f64 / elapsed;
        let decay = 0.5_f64.powf(elapsed / HALF_LIFE_SECONDS);
        volume.bytes_per_second = volume.bytes_per_second * decay + instant * (1.0 - decay);
        volume.at = now;
        *volume
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_nanos(seconds * 1_000_000_000)
    }

    #[test]
    fn a_quiet_dataset_keeps_one_ingester_and_a_loud_one_earns_more() {
        let volumes = Volumes::default();
        let tenant = TenantId::DEFAULT;
        let dataset = DatasetName::new("prod").unwrap();
        let mut volume = volumes.observe(&tenant, &dataset, 1024, at(1));
        assert_eq!(volume.preferred(8), 1);
        // Two hundred megabytes a second for a minute.
        for second in 2..60 {
            volume = volumes.observe(&tenant, &dataset, 200 * 1024 * 1024, at(second));
        }
        assert!(
            volume.bytes_per_second > 100.0 * 1024.0 * 1024.0,
            "{volume:?}"
        );
        assert_eq!(volume.preferred(8), 8, "capped");
        assert!(
            (9..=13).contains(&volume.preferred(32)),
            "one per sixteen megabytes a second of the smoothed rate: {volume:?}"
        );
        // Silence for a few minutes fades it.
        let faded = volumes.observe(&tenant, &dataset, 0, at(400));
        assert_eq!(faded.preferred(8), 1, "{faded:?}");
        let other = volumes.observe(&tenant, &DatasetName::new("dev").unwrap(), 10, at(400));
        assert_eq!(other.preferred(8), 1, "datasets are rated apart");
    }
}
