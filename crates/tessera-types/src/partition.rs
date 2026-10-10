// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Partitions (spec section 6.6): one tenant, dataset, kind and event-time
//! window. The unit of manifest, compaction and retention.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{DatasetName, Kind, TenantId, Timestamp};

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// An event-time window: a start aligned to a multiple of its length since
/// the Unix epoch. The length travels with the window so that partitions
/// keep the window they were created with when the dataset's window
/// changes (MF-2).
///
/// Formats as `{start seconds:012}-{length seconds:07}`, which sorts by
/// start and is a valid key component.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct Window {
    start_seconds: u64,
    length_seconds: u64,
}

/// Why a string is not a window.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid window {0:?}: expected '{{start}}-{{length}}' in seconds")]
pub struct InvalidWindow(String);

impl Window {
    /// The default window length (MF-1).
    pub const DEFAULT_LENGTH: Duration = Duration::from_secs(86_400);
    /// The shortest window the advisor may choose (MF-2).
    pub const MIN_LENGTH: Duration = Duration::from_secs(5 * 60);
    /// The longest window the advisor may choose (MF-2).
    pub const MAX_LENGTH: Duration = Duration::from_secs(30 * 86_400);

    /// The window of the given length that contains `at`, or `None` for
    /// times before the epoch or a zero length.
    #[must_use]
    pub fn containing(at: Timestamp, length: Duration) -> Option<Self> {
        let length_seconds = length.as_secs();
        if length_seconds == 0 {
            return None;
        }
        let seconds = u64::try_from(at.as_nanos().div_euclid(NANOS_PER_SECOND)).ok()?;
        Some(Window {
            start_seconds: seconds - seconds % length_seconds,
            length_seconds,
        })
    }

    /// The first instant of the window.
    #[must_use]
    pub fn start(self) -> Timestamp {
        Timestamp::from_nanos(
            i64::try_from(self.start_seconds)
                .unwrap_or(i64::MAX)
                .saturating_mul(NANOS_PER_SECOND),
        )
    }

    /// The first instant after the window.
    #[must_use]
    pub fn end(self) -> Timestamp {
        let end = self.start_seconds.saturating_add(self.length_seconds);
        Timestamp::from_nanos(
            i64::try_from(end)
                .unwrap_or(i64::MAX)
                .saturating_mul(NANOS_PER_SECOND),
        )
    }

    /// The window's length.
    #[must_use]
    pub fn length(self) -> Duration {
        Duration::from_secs(self.length_seconds)
    }

    /// Whether `at` lies in the window.
    #[must_use]
    pub fn contains(self, at: Timestamp) -> bool {
        self.start() <= at && at < self.end()
    }
}

impl fmt::Display for Window {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:012}-{:07}", self.start_seconds, self.length_seconds)
    }
}

impl FromStr for Window {
    type Err = InvalidWindow;

    fn from_str(value: &str) -> Result<Self, InvalidWindow> {
        let invalid = || InvalidWindow(value.to_owned());
        let (start, length) = value.split_once('-').ok_or_else(invalid)?;
        let start_seconds: u64 = start.parse().map_err(|_| invalid())?;
        let length_seconds: u64 = length.parse().map_err(|_| invalid())?;
        if length_seconds == 0 || start_seconds % length_seconds != 0 {
            return Err(invalid());
        }
        Ok(Window {
            start_seconds,
            length_seconds,
        })
    }
}

/// One partition: the unit of manifest, compaction and retention (ST-11).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct PartitionId {
    /// The tenant.
    pub tenant: TenantId,
    /// The dataset.
    pub dataset: DatasetName,
    /// The signal.
    pub kind: Kind,
    /// The event-time window.
    pub window: Window,
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}",
            self.tenant, self.dataset, self.kind, self.window
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_nanos(seconds * NANOS_PER_SECOND)
    }

    #[test]
    fn a_window_is_aligned_to_its_length() {
        let window = Window::containing(at(100_000), Window::DEFAULT_LENGTH).unwrap();
        assert_eq!(window.start(), at(86_400));
        assert_eq!(window.end(), at(172_800));
        assert!(window.contains(at(100_000)));
        assert!(window.contains(at(86_400)));
        assert!(!window.contains(at(172_800)));
    }

    #[test]
    fn the_same_window_is_found_for_every_instant_in_it() {
        let length = Duration::from_secs(300);
        let first = Window::containing(at(600), length).unwrap();
        for seconds in 600..900 {
            assert_eq!(Window::containing(at(seconds), length), Some(first));
        }
        assert_ne!(Window::containing(at(900), length), Some(first));
    }

    #[test]
    fn times_before_the_epoch_and_zero_lengths_have_no_window() {
        assert_eq!(
            Window::containing(Timestamp::from_nanos(-1), Window::DEFAULT_LENGTH),
            None
        );
        assert_eq!(Window::containing(at(1), Duration::ZERO), None);
    }

    #[test]
    fn window_round_trips_through_its_key_form() {
        let window = Window::containing(at(100_000), Window::DEFAULT_LENGTH).unwrap();
        assert_eq!(window.to_string(), "000000086400-0086400");
        assert_eq!(window.to_string().parse::<Window>().unwrap(), window);
        assert!("12-5".parse::<Window>().is_err(), "unaligned start");
        assert!("12".parse::<Window>().is_err());
        assert!("0-0".parse::<Window>().is_err());
    }

    #[test]
    fn windows_order_by_start() {
        let early = Window::containing(at(0), Window::DEFAULT_LENGTH).unwrap();
        let late = Window::containing(at(200_000), Window::DEFAULT_LENGTH).unwrap();
        assert!(early < late);
        assert!(early.to_string() < late.to_string());
    }

    #[test]
    fn partition_id_displays_its_path() {
        let partition = PartitionId {
            tenant: TenantId::DEFAULT,
            dataset: DatasetName::new("prod").unwrap(),
            kind: Kind::Log,
            window: Window::containing(at(0), Window::DEFAULT_LENGTH).unwrap(),
        };
        assert_eq!(
            partition.to_string(),
            "default/prod/log/000000000000-0086400"
        );
    }
}
