// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Partitions (spec section 6.6): one tenant, dataset, kind and event-time
//! window. The unit of manifest, compaction and retention.

use std::collections::BTreeMap;
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

/// One change of a dataset's partition window (MF-2): the length in
/// force from `from` on.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct WindowChange {
    /// When the length takes effect.
    pub from: Timestamp,
    /// The window length from then on.
    pub length: Duration,
}

/// A dataset's partition windows over time (MF-2): the window length in
/// force at every instant, so that a timestamp maps to its partition by
/// the dataset log alone. A change applies from the end of the window
/// in force when it is made, so that the partitions that have started
/// keep their window; the windows of each length are aligned to the
/// epoch as [`Window::containing`] places them.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct WindowSchedule {
    /// The changes, by `from`; the first from the beginning of time.
    changes: Vec<WindowChange>,
}

impl WindowSchedule {
    /// One length for all time.
    #[must_use]
    pub fn of(length: Duration) -> Self {
        WindowSchedule {
            changes: vec![WindowChange {
                from: Timestamp::MIN,
                length,
            }],
        }
    }

    /// The changes, in order of effect.
    #[must_use]
    pub fn changes(&self) -> &[WindowChange] {
        &self.changes
    }

    /// The length in force at `at`.
    #[must_use]
    pub fn length_at(&self, at: Timestamp) -> Duration {
        self.changes
            .iter()
            .rev()
            .find(|c| c.from <= at)
            .map_or(Window::DEFAULT_LENGTH, |c| c.length)
    }

    /// The length the schedule ends in: what windows not started yet
    /// will have.
    #[must_use]
    pub fn latest(&self) -> Duration {
        self.changes
            .last()
            .map_or(Window::DEFAULT_LENGTH, |c| c.length)
    }

    /// The window that contains `at`, or `None` before the epoch.
    #[must_use]
    pub fn containing(&self, at: Timestamp) -> Option<Window> {
        Window::containing(at, self.length_at(at))
    }

    /// Schedules `length` for the windows that have not started by
    /// `now`: from the end of the window containing `now`, a change
    /// still pending after `now` replaced. Returns whether the schedule
    /// changed: not when it already ended in `length`.
    pub fn set(&mut self, length: Duration, now: Timestamp) -> bool {
        if self.latest() == length {
            return false;
        }
        let in_force = self.length_at(now);
        self.changes.retain(|c| c.from <= now);
        if self.latest() != length {
            let from = Window::containing(now, in_force).map_or(Timestamp::UNIX_EPOCH, Window::end);
            self.changes.push(WindowChange { from, length });
        }
        true
    }
}

impl Default for WindowSchedule {
    fn default() -> Self {
        WindowSchedule::of(Window::DEFAULT_LENGTH)
    }
}

impl From<Duration> for WindowSchedule {
    fn from(length: Duration) -> Self {
        WindowSchedule::of(length)
    }
}

/// The window schedules of several datasets, with a default for the
/// ones not named: what places the rows of a writer log, which may
/// hold several datasets, in their partitions.
#[derive(Clone, Debug, Default)]
pub struct Windows {
    by_dataset: BTreeMap<(TenantId, DatasetName), WindowSchedule>,
    default: WindowSchedule,
}

impl Windows {
    /// Schedules with `default` for every dataset not named.
    #[must_use]
    pub fn new(default: WindowSchedule) -> Self {
        Windows {
            by_dataset: BTreeMap::new(),
            default,
        }
    }

    /// Names `dataset`'s schedule.
    pub fn insert(&mut self, tenant: TenantId, dataset: DatasetName, schedule: WindowSchedule) {
        self.by_dataset.insert((tenant, dataset), schedule);
    }

    /// The schedule of `dataset`.
    #[must_use]
    pub fn schedule_for(&self, tenant: &TenantId, dataset: &DatasetName) -> &WindowSchedule {
        self.by_dataset
            .get(&(tenant.clone(), dataset.clone()))
            .unwrap_or(&self.default)
    }
}

impl From<Duration> for Windows {
    fn from(length: Duration) -> Self {
        Windows::new(WindowSchedule::of(length))
    }
}

impl From<WindowSchedule> for Windows {
    fn from(schedule: WindowSchedule) -> Self {
        Windows::new(schedule)
    }
}

impl From<&Windows> for Windows {
    fn from(windows: &Windows) -> Self {
        windows.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_nanos(seconds * NANOS_PER_SECOND)
    }

    /// MF-2: a change takes effect at the end of the window in force,
    /// earlier windows keep their length, and a pending change gives
    /// way to a newer one.
    #[test]
    fn a_schedule_changes_the_window_from_the_next_boundary() {
        let hour = Duration::from_secs(3_600);
        let mut schedule = WindowSchedule::default();
        assert_eq!(schedule.length_at(at(100_000)), Window::DEFAULT_LENGTH);
        assert!(!schedule.set(Window::DEFAULT_LENGTH, at(100_000)));
        assert!(schedule.set(hour, at(100_000)));
        let change = *schedule.changes().last().unwrap();
        assert_eq!(
            change.from,
            at(172_800),
            "the end of the day containing now"
        );
        assert!(
            !schedule.set(hour, at(100_100)),
            "asking again changes nothing"
        );
        assert_eq!(schedule.length_at(at(172_799)), Window::DEFAULT_LENGTH);
        assert_eq!(schedule.length_at(at(172_800)), hour);
        assert_eq!(schedule.latest(), hour);
        assert_eq!(
            schedule.containing(at(100_000)),
            Window::containing(at(100_000), Window::DEFAULT_LENGTH)
        );
        assert_eq!(
            schedule.containing(at(180_000)),
            Window::containing(at(180_000), hour)
        );
        // Changing one's mind before the change took effect replaces it,
        // and going back to the length in force drops it.
        let minutes = Duration::from_secs(300);
        assert!(schedule.set(minutes, at(100_500)));
        assert_eq!(schedule.changes().last().unwrap().from, at(172_800));
        assert_eq!(schedule.changes().len(), 2);
        assert_eq!(schedule.latest(), minutes);
        assert!(schedule.set(Window::DEFAULT_LENGTH, at(100_600)));
        assert_eq!(schedule.changes().len(), 1);
        assert!(schedule.set(minutes, at(100_700)));
        // Once in effect, a change stays and the next one follows it.
        assert!(schedule.set(hour, at(172_900)));
        let later = *schedule.changes().last().unwrap();
        assert_eq!(later.from, at(173_100), "the end of the five-minute window");
        assert_eq!(schedule.length_at(at(173_000)), minutes);
        assert_eq!(schedule.length_at(at(173_100)), hour);
        let json = serde_json::to_string(&schedule).unwrap();
        assert_eq!(
            serde_json::from_str::<WindowSchedule>(&json).unwrap(),
            schedule
        );
    }

    #[test]
    fn windows_name_each_datasets_schedule_and_default_the_rest() {
        let hour = Duration::from_secs(3_600);
        let mut windows = Windows::from(Window::DEFAULT_LENGTH);
        let prod = DatasetName::new("prod").unwrap();
        windows.insert(TenantId::DEFAULT, prod.clone(), WindowSchedule::of(hour));
        assert_eq!(
            windows.schedule_for(&TenantId::DEFAULT, &prod).latest(),
            hour
        );
        let other = DatasetName::new("other").unwrap();
        assert_eq!(
            windows.schedule_for(&TenantId::DEFAULT, &other).latest(),
            Window::DEFAULT_LENGTH
        );
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
