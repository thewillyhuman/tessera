// SPDX-License-Identifier: Apache-2.0

//! Event time and the injected clock.
//!
//! Every component reads time through a [`Clock`] (RS-2) so that it can run
//! under deterministic simulation with a [`ManualClock`].

use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const SECONDS_PER_DAY: i64 = 86_400;

/// A point in time with nanosecond precision, as nanoseconds since the Unix
/// epoch. This is the type of the `timestamp` column (spec section 4.2).
///
/// Formats as RFC 3339 in UTC, with a nine-digit fraction when it is not
/// zero.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Timestamp(i64);

impl Timestamp {
    /// 1970-01-01T00:00:00Z.
    pub const UNIX_EPOCH: Timestamp = Timestamp(0);
    /// The earliest representable time.
    pub const MIN: Timestamp = Timestamp(i64::MIN);
    /// The latest representable time.
    pub const MAX: Timestamp = Timestamp(i64::MAX);

    /// [`Timestamp::MIN`], for serde defaults.
    #[must_use]
    pub const fn min_value() -> Self {
        Timestamp::MIN
    }

    /// [`Timestamp::MAX`], for serde defaults.
    #[must_use]
    pub const fn max_value() -> Self {
        Timestamp::MAX
    }

    /// Wraps a count of nanoseconds since the Unix epoch.
    #[must_use]
    pub const fn from_nanos(nanos: i64) -> Self {
        Timestamp(nanos)
    }

    /// Nanoseconds since the Unix epoch.
    #[must_use]
    pub const fn as_nanos(self) -> i64 {
        self.0
    }

    /// `self + duration`, or `None` if the result does not fit.
    #[must_use]
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        let nanos = i64::try_from(duration.as_nanos()).ok()?;
        self.0.checked_add(nanos).map(Timestamp)
    }

    /// `self - earlier`, or `None` if `earlier` is later than `self`.
    #[must_use]
    pub fn duration_since(self, earlier: Timestamp) -> Option<Duration> {
        let nanos = self.0.checked_sub(earlier.0)?;
        u64::try_from(nanos).ok().map(Duration::from_nanos)
    }

    /// Converts a system time, saturating at the representable range.
    #[must_use]
    pub fn from_system_time(time: SystemTime) -> Self {
        match time.duration_since(UNIX_EPOCH) {
            Ok(after) => Timestamp(i64::try_from(after.as_nanos()).unwrap_or(i64::MAX)),
            Err(before) => {
                let nanos = i64::try_from(before.duration().as_nanos()).unwrap_or(i64::MAX);
                Timestamp(-nanos)
            }
        }
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let seconds = self.0.div_euclid(NANOS_PER_SECOND);
        let nanos = self.0.rem_euclid(NANOS_PER_SECOND);
        let days = seconds.div_euclid(SECONDS_PER_DAY);
        let second_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
        let (year, month, day) = civil_from_days(days);
        write!(
            f,
            "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}",
            second_of_day / 3600,
            second_of_day % 3600 / 60,
            second_of_day % 60
        )?;
        if nanos != 0 {
            write!(f, ".{nanos:09}")?;
        }
        f.write_str("Z")
    }
}

/// A timestamp that could not be parsed.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
#[error("invalid timestamp {0:?}: expected RFC 3339 such as 2024-01-31T12:00:00Z")]
pub struct InvalidTimestamp(String);

impl FromStr for Timestamp {
    type Err = InvalidTimestamp;

    /// Parses RFC 3339 in the forms `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS`
    /// with an optional fraction and an optional `Z` or `±HH:MM` offset;
    /// a space may replace the `T`, as `PostgreSQL` accepts.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || InvalidTimestamp(text.to_owned());
        let (date, rest) = text.split_at(text.len().min(10));
        let mut parts = date.split('-');
        let year: i64 = parts
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(invalid)?;
        let month: i64 = parts
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(invalid)?;
        let day: i64 = parts
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(invalid)?;
        if parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            return Err(invalid());
        }
        let (mut seconds, mut nanos, mut offset) = (0i64, 0i64, 0i64);
        let rest =
            rest.strip_prefix(['T', 't', ' '])
                .or(if rest.is_empty() { Some("") } else { None });
        let rest = rest.ok_or_else(invalid)?;
        if !rest.is_empty() {
            let (clock, zone) = match rest.find(['Z', 'z', '+', '-']) {
                Some(i) => rest.split_at(i),
                None => (rest, ""),
            };
            let (hms, fraction) = clock.split_once('.').unwrap_or((clock, ""));
            let mut fields = hms.split(':');
            let hour: i64 = fields
                .next()
                .and_then(|p| p.parse().ok())
                .ok_or_else(invalid)?;
            let minute: i64 = fields
                .next()
                .and_then(|p| p.parse().ok())
                .ok_or_else(invalid)?;
            let second: i64 = fields
                .next()
                .map_or(Some(0), |p| p.parse().ok())
                .ok_or_else(invalid)?;
            if fields.next().is_some() || hour > 23 || minute > 59 || second > 60 {
                return Err(invalid());
            }
            seconds = hour * 3600 + minute * 60 + second;
            if !fraction.is_empty() {
                if fraction.len() > 9 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(invalid());
                }
                let digits = u32::try_from(fraction.len()).map_err(|_| invalid())?;
                let scale = 10i64.pow(9 - digits);
                nanos = fraction.parse::<i64>().map_err(|_| invalid())? * scale;
            }
            offset = match zone {
                "" | "Z" | "z" => 0,
                signed => {
                    let sign = if signed.starts_with('-') { -1 } else { 1 };
                    let (h, m) = signed[1..].split_once(':').ok_or_else(invalid)?;
                    let h: i64 = h.parse().map_err(|_| invalid())?;
                    let m: i64 = m.parse().map_err(|_| invalid())?;
                    sign * (h * 3600 + m * 60)
                }
            };
        }
        let days = days_from_civil(year, month, day);
        let total = (days * SECONDS_PER_DAY + seconds - offset)
            .checked_mul(NANOS_PER_SECOND)
            .and_then(|n| n.checked_add(nanos))
            .ok_or_else(invalid)?;
        Ok(Timestamp(total))
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date: the inverse of
/// [`civil_from_days`], from the same source.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Proleptic Gregorian date for a count of days since 1970-01-01.
///
/// This is the `civil_from_days` algorithm of Howard Hinnant [1], valid for
/// the whole `i64` range of days used here.
///
/// [1] <https://howardhinnant.github.io/date_algorithms.html>
fn civil_from_days(days: i64) -> (i64, u8, u8) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153; // March = 0
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    // Month and day are bounded by the algorithm; the casts cannot truncate.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    (year, month as u8, day as u8)
}

/// A source of the current time.
///
/// A half-open range of event time, `[start, end)`.
///
/// Every plan node carries one (QE-3) and pruning starts from it (section
/// 10.4). [`TimeRange::ALL`] covers every representable time.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct TimeRange {
    /// The first time in the range.
    pub start: Timestamp,
    /// The first time after the range.
    pub end: Timestamp,
}

impl TimeRange {
    /// Every representable time.
    pub const ALL: TimeRange = TimeRange {
        start: Timestamp::MIN,
        end: Timestamp::MAX,
    };

    /// `[start, end)`. An `end` at or before `start` gives an empty range.
    #[must_use]
    pub const fn new(start: Timestamp, end: Timestamp) -> Self {
        TimeRange { start, end }
    }

    /// Whether no time lies in the range.
    #[must_use]
    pub fn is_empty(self) -> bool {
        self.end <= self.start
    }

    /// Whether `at` lies in the range.
    #[must_use]
    pub fn contains(self, at: Timestamp) -> bool {
        self.start <= at && at < self.end
    }

    /// The times in both ranges.
    #[must_use]
    pub fn intersect(self, other: TimeRange) -> TimeRange {
        TimeRange {
            start: self.start.max(other.start),
            end: self.end.min(other.end),
        }
    }

    /// Whether the closed interval `[min, max]` shares a time with the
    /// range: the test applied to a file's recorded bounds.
    #[must_use]
    pub fn overlaps_closed(self, min: Timestamp, max: Timestamp) -> bool {
        min < self.end && max >= self.start
    }
}

impl fmt::Display for TimeRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.start == Timestamp::MIN, self.end == Timestamp::MAX) {
            (true, true) => write!(f, "[all time)"),
            (true, false) => write!(f, "[.., {})", self.end),
            (false, true) => write!(f, "[{}, ..)", self.start),
            (false, false) => write!(f, "[{}, {})", self.start, self.end),
        }
    }
}

/// Production code uses [`SystemClock`]; tests and the simulation harness
/// inject a [`ManualClock`] so that time only moves when the test says so.
pub trait Clock: Send + Sync {
    /// The current time.
    fn now(&self) -> Timestamp;
}

/// The operating system clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_system_time(SystemTime::now())
    }
}

/// A clock that only moves when [`advance`](Self::advance) or
/// [`set`](Self::set) is called. Shared between threads by reference.
#[derive(Debug)]
pub struct ManualClock {
    now: AtomicI64,
}

impl ManualClock {
    /// A clock stopped at `start`.
    #[must_use]
    pub fn new(start: Timestamp) -> Self {
        ManualClock {
            now: AtomicI64::new(start.0),
        }
    }

    /// Moves the clock forward by `by`, saturating at the representable range.
    pub fn advance(&self, by: Duration) {
        let nanos = i64::try_from(by.as_nanos()).unwrap_or(i64::MAX);
        // The closure always returns `Some`, so the update cannot fail.
        let _ = self
            .now
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |now| {
                Some(now.saturating_add(nanos))
            });
    }

    /// Sets the clock to `to`, which may be earlier than now.
    pub fn set(&self, to: Timestamp) {
        self.now.store(to.0, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.now.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn formats_unix_epoch_as_rfc3339() {
        assert_eq!(Timestamp::UNIX_EPOCH.to_string(), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn formats_nanoseconds_when_present() {
        let ts = Timestamp::from_nanos(1_700_000_000_123_456_789);
        assert_eq!(ts.to_string(), "2023-11-14T22:13:20.123456789Z");
    }

    #[test]
    fn formats_times_before_the_epoch() {
        let ts = Timestamp::from_nanos(-1);
        assert_eq!(ts.to_string(), "1969-12-31T23:59:59.999999999Z");
    }

    #[test]
    fn formats_leap_day() {
        let ts = Timestamp::from_nanos(951_782_400_000_000_000); // 2000-02-29
        assert_eq!(ts.to_string(), "2000-02-29T00:00:00Z");
    }

    #[test]
    fn adds_a_duration() {
        let ts = Timestamp::from_nanos(10).checked_add(Duration::from_nanos(5));
        assert_eq!(ts, Some(Timestamp::from_nanos(15)));
    }

    #[test]
    fn addition_overflow_is_detected() {
        let ts = Timestamp::from_nanos(i64::MAX).checked_add(Duration::from_nanos(1));
        assert_eq!(ts, None);
    }

    #[test]
    fn measures_duration_since_an_earlier_time() {
        let earlier = Timestamp::from_nanos(100);
        let later = Timestamp::from_nanos(250);
        assert_eq!(
            later.duration_since(earlier),
            Some(Duration::from_nanos(150))
        );
        assert_eq!(earlier.duration_since(later), None);
    }

    #[test]
    fn manual_clock_only_moves_when_told() {
        let clock = ManualClock::new(Timestamp::from_nanos(1_000));
        assert_eq!(clock.now(), Timestamp::from_nanos(1_000));
        clock.advance(Duration::from_nanos(500));
        assert_eq!(clock.now(), Timestamp::from_nanos(1_500));
        clock.set(Timestamp::from_nanos(7));
        assert_eq!(clock.now(), Timestamp::from_nanos(7));
    }

    #[test]
    fn timestamp_serializes_as_nanoseconds() {
        let ts = Timestamp::from_nanos(1_500);
        assert_eq!(serde_json::to_string(&ts).unwrap(), "1500");
        assert_eq!(serde_json::from_str::<Timestamp>("1500").unwrap(), ts);
    }

    #[test]
    fn system_clock_is_after_2020() {
        let year_2020 = Timestamp::from_nanos(1_577_836_800_000_000_000);
        assert!(SystemClock.now() > year_2020);
    }

    #[test]
    fn a_range_is_half_open() {
        let range = TimeRange::new(Timestamp::from_nanos(10), Timestamp::from_nanos(20));
        assert!(range.contains(Timestamp::from_nanos(10)));
        assert!(range.contains(Timestamp::from_nanos(19)));
        assert!(!range.contains(Timestamp::from_nanos(20)));
        assert!(!range.is_empty());
        assert!(TimeRange::new(Timestamp::from_nanos(5), Timestamp::from_nanos(5)).is_empty());
    }

    #[test]
    fn intersection_keeps_the_common_times() {
        let a = TimeRange::new(Timestamp::from_nanos(0), Timestamp::from_nanos(20));
        let b = TimeRange::new(Timestamp::from_nanos(10), Timestamp::from_nanos(30));
        let both = a.intersect(b);
        assert_eq!(both.start, Timestamp::from_nanos(10));
        assert_eq!(both.end, Timestamp::from_nanos(20));
        assert!(
            a.intersect(TimeRange::new(
                Timestamp::from_nanos(50),
                Timestamp::from_nanos(60)
            ))
            .is_empty()
        );
    }

    #[test]
    fn file_bounds_overlap_when_they_touch_the_range() {
        let range = TimeRange::new(Timestamp::from_nanos(10), Timestamp::from_nanos(20));
        assert!(range.overlaps_closed(Timestamp::from_nanos(0), Timestamp::from_nanos(10)));
        assert!(range.overlaps_closed(Timestamp::from_nanos(19), Timestamp::from_nanos(99)));
        assert!(!range.overlaps_closed(Timestamp::from_nanos(20), Timestamp::from_nanos(99)));
        assert!(!range.overlaps_closed(Timestamp::from_nanos(0), Timestamp::from_nanos(9)));
    }

    #[test]
    fn ranges_display_their_open_ends() {
        assert_eq!(TimeRange::ALL.to_string(), "[all time)");
        let until = TimeRange::new(Timestamp::MIN, Timestamp::UNIX_EPOCH);
        assert_eq!(until.to_string(), "[.., 1970-01-01T00:00:00Z)");
    }

    #[test]
    fn parses_what_it_formats() {
        for nanos in [0, 1, -1, 1_700_000_000_123_456_789, 951_782_400_000_000_000] {
            let ts = Timestamp::from_nanos(nanos);
            assert_eq!(ts.to_string().parse::<Timestamp>().unwrap(), ts);
        }
    }

    #[test]
    fn parses_dates_spaces_fractions_and_offsets() {
        assert_eq!(
            "1970-01-02".parse::<Timestamp>().unwrap(),
            Timestamp::from_nanos(86_400 * 1_000_000_000)
        );
        assert_eq!(
            "1970-01-01 00:00:01.5".parse::<Timestamp>().unwrap(),
            Timestamp::from_nanos(1_500_000_000)
        );
        assert_eq!(
            "1970-01-01T01:00:00+01:00".parse::<Timestamp>().unwrap(),
            Timestamp::UNIX_EPOCH
        );
        assert_eq!(
            "1969-12-31T23:00:00-01:00".parse::<Timestamp>().unwrap(),
            Timestamp::UNIX_EPOCH
        );
        assert_eq!(
            "2023-11-14T22:13".parse::<Timestamp>().unwrap(),
            Timestamp::from_nanos(1_699_999_980_000_000_000)
        );
    }

    #[test]
    fn rejects_malformed_timestamps() {
        for text in [
            "",
            "2024",
            "2024-13-01",
            "2024-01-01X10:00",
            "2024-01-01T25:00:00",
            "2024-01-01T10:00:00.1234567890",
            "not a date",
        ] {
            assert!(text.parse::<Timestamp>().is_err(), "{text:?} parsed");
        }
    }
}
