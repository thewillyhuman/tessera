// SPDX-License-Identifier: Apache-2.0

//! Range-vector functions over one series (section 10.6, "series-at-a-
//! time range vectors"): the samples of a series, sorted by time, are
//! evaluated at every step over the window `(t - range, t]` with `LogQL`
//! semantics. One implementation serves batch queries and the
//! incremental runtime (QE-20).

use tessera_ir::RangeFunction;

/// One sample: event time in nanoseconds and its value.
pub type Sample = (i64, f64);

/// Evaluates `function` over `samples` (sorted by time) at every step from
/// `start` to `end` inclusive, each over `(t - range, t]`. Steps whose
/// window holds no sample produce nothing, except for `AbsentOverTime`,
/// which produces 1 at those steps and nothing elsewhere.
#[must_use]
pub fn evaluate(
    function: RangeFunction,
    samples: &[Sample],
    start: i64,
    end: i64,
    step: i64,
    range: i64,
) -> Vec<Sample> {
    let step = step.max(1);
    let mut out = Vec::new();
    let mut lo = 0usize;
    let mut hi = 0usize;
    let mut t = start;
    while t <= end {
        let window_start = t - range;
        while lo < samples.len() && samples[lo].0 <= window_start {
            lo += 1;
        }
        while hi < samples.len() && samples[hi].0 <= t {
            hi += 1;
        }
        let window = &samples[lo.min(hi)..hi];
        match (function, window.is_empty()) {
            (RangeFunction::AbsentOverTime, true) => out.push((t, 1.0)),
            (RangeFunction::AbsentOverTime, false) | (_, true) => {}
            (_, false) => {
                if let Some(value) = apply(function, window, range) {
                    out.push((t, value));
                }
            }
        }
        t = t.saturating_add(step);
        if step == 0 {
            break;
        }
    }
    out
}

/// One series over one part of a window (QE-14): what a node that
/// scans says of the samples it holds in a slice of time, in place of
/// the samples. The parts of a window, merged in time order, give
/// every function that is made of its parts ([`in_parts`]).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Part {
    /// How many samples.
    pub count: f64,
    /// Their sum.
    pub sum: f64,
    /// The sum of the squares of their distances from their mean.
    pub spread: f64,
    /// The smallest.
    pub min: f64,
    /// The largest.
    pub max: f64,
    /// The first in time.
    pub first: f64,
    /// The last in time.
    pub last: f64,
}

impl Part {
    /// The part that holds `samples`, which are in time order; none
    /// for no sample.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn of(samples: &[Sample]) -> Option<Part> {
        let (first, last) = (samples.first()?.1, samples.last()?.1);
        let values = || samples.iter().map(|(_, v)| *v);
        let count = samples.len() as f64;
        let sum: f64 = values().sum();
        let mean = sum / count;
        Some(Part {
            count,
            sum,
            spread: values().map(|v| (v - mean) * (v - mean)).sum(),
            min: values().fold(f64::INFINITY, f64::min),
            max: values().fold(f64::NEG_INFINITY, f64::max),
            first,
            last,
        })
    }

    /// This part and `later`, a part of the same series after it, as
    /// one. The spread of the two is theirs plus what the distance
    /// between their means adds, which loses no more precision than
    /// the parts' own spreads have.
    #[must_use]
    pub fn then(self, later: Part) -> Part {
        let count = self.count + later.count;
        let apart = later.sum / later.count - self.sum / self.count;
        Part {
            count,
            sum: self.sum + later.sum,
            spread: self.spread + later.spread + apart * apart * self.count * later.count / count,
            min: self.min.min(later.min),
            max: self.max.max(later.max),
            first: self.first,
            last: later.last,
        }
    }
}

/// Whether `function` over a window is made of the [`Part`]s the
/// window is cut into. A quantile is not: it needs every value. Nor is
/// absence, which is said of a whole selection, not of a series.
#[must_use]
pub fn in_parts(function: RangeFunction) -> bool {
    !matches!(
        function,
        RangeFunction::QuantileOverTime(_) | RangeFunction::AbsentOverTime
    )
}

/// The parts of `samples` (sorted by time): one for every `width` from
/// `start` to `end` inclusive, each over `(t - width, t]`, that holds
/// a sample.
#[must_use]
pub fn parts(samples: &[Sample], start: i64, end: i64, width: i64) -> Vec<(i64, Part)> {
    let width = width.max(1);
    let mut out = Vec::new();
    let mut lo = 0usize;
    let mut t = start;
    while t <= end && lo < samples.len() {
        while lo < samples.len() && samples[lo].0 <= t - width {
            lo += 1;
        }
        let mut hi = lo;
        while hi < samples.len() && samples[hi].0 <= t {
            hi += 1;
        }
        if let Some(part) = Part::of(&samples[lo..hi]) {
            out.push((t, part));
        }
        lo = hi;
        let Some(next) = t.checked_add(width) else {
            break;
        };
        t = next;
    }
    out
}

/// Evaluates `function` over `parts` (sorted by time) at every step
/// from `start` to `end` inclusive, each over the parts that end in
/// `(t - range, t]`, merged. Steps whose window holds no part produce
/// nothing, and nothing is produced for a function that is not made
/// of its parts.
#[must_use]
pub fn evaluate_parts(
    function: RangeFunction,
    parts: &[(i64, Part)],
    start: i64,
    end: i64,
    step: i64,
    range: i64,
) -> Vec<Sample> {
    let step = step.max(1);
    let mut out = Vec::new();
    let mut lo = 0usize;
    let mut hi = 0usize;
    let mut t = start;
    while t <= end {
        while lo < parts.len() && parts[lo].0 <= t - range {
            lo += 1;
        }
        while hi < parts.len() && parts[hi].0 <= t {
            hi += 1;
        }
        let merged = parts[lo.min(hi)..hi]
            .iter()
            .map(|(_, part)| *part)
            .reduce(Part::then);
        if let Some(value) = merged.and_then(|whole| of_parts(function, whole, range)) {
            out.push((t, value));
        }
        let Some(next) = t.checked_add(step) else {
            break;
        };
        t = next;
    }
    out
}

/// `function` over a window whose parts merge into `whole`.
#[allow(clippy::cast_precision_loss)]
fn of_parts(function: RangeFunction, whole: Part, range: i64) -> Option<f64> {
    let seconds = range as f64 / 1e9;
    Some(match function {
        RangeFunction::CountOverTime => whole.count,
        RangeFunction::BytesOverTime | RangeFunction::SumOverTime => whole.sum,
        RangeFunction::Rate | RangeFunction::BytesRate => whole.sum / seconds,
        RangeFunction::AvgOverTime => whole.sum / whole.count,
        RangeFunction::MinOverTime => whole.min,
        RangeFunction::MaxOverTime => whole.max,
        RangeFunction::FirstOverTime => whole.first,
        RangeFunction::LastOverTime => whole.last,
        RangeFunction::StdvarOverTime => whole.spread / whole.count,
        RangeFunction::StddevOverTime => (whole.spread / whole.count).sqrt(),
        RangeFunction::QuantileOverTime(_) | RangeFunction::AbsentOverTime => return None,
    })
}

#[allow(clippy::cast_precision_loss)]
fn apply(function: RangeFunction, window: &[Sample], range: i64) -> Option<f64> {
    let values = || window.iter().map(|(_, v)| *v);
    let n = window.len() as f64;
    let seconds = range as f64 / 1e9;
    Some(match function {
        RangeFunction::CountOverTime => n,
        RangeFunction::BytesOverTime | RangeFunction::SumOverTime => values().sum(),
        // What the samples carry, per second: one each where the caller
        // counts rows, which sums to their number exactly.
        RangeFunction::Rate | RangeFunction::BytesRate => values().sum::<f64>() / seconds,
        RangeFunction::AvgOverTime => values().sum::<f64>() / n,
        RangeFunction::MinOverTime => values().fold(f64::INFINITY, f64::min),
        RangeFunction::MaxOverTime => values().fold(f64::NEG_INFINITY, f64::max),
        RangeFunction::FirstOverTime => window.first()?.1,
        RangeFunction::LastOverTime => window.last()?.1,
        RangeFunction::StdvarOverTime | RangeFunction::StddevOverTime => {
            let mean = values().sum::<f64>() / n;
            let variance = values().map(|v| (v - mean) * (v - mean)).sum::<f64>() / n;
            if function == RangeFunction::StddevOverTime {
                variance.sqrt()
            } else {
                variance
            }
        }
        RangeFunction::QuantileOverTime(q) => {
            let mut sorted: Vec<f64> = values().collect();
            sorted.sort_by(f64::total_cmp);
            quantile(&sorted, q)
        }
        RangeFunction::AbsentOverTime => return None,
    })
}

/// The `q`-quantile of sorted values with linear interpolation, as
/// Prometheus and Loki compute it.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn quantile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    if q <= 0.0 {
        return sorted[0];
    }
    if q >= 1.0 {
        return sorted[sorted.len() - 1];
    }
    let rank = q * (sorted.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = (lower + 1).min(sorted.len() - 1);
    let weight = rank - lower as f64;
    sorted[lower] * (1.0 - weight) + sorted[upper] * weight
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: i64 = 1_000_000_000;

    fn samples() -> Vec<Sample> {
        vec![(S, 2.0), (2 * S, 4.0), (4 * S, 6.0), (9 * S, 8.0)]
    }

    #[test]
    fn counts_and_rates_slide_over_the_window() {
        let out = evaluate(
            RangeFunction::CountOverTime,
            &samples(),
            2 * S,
            10 * S,
            2 * S,
            3 * S,
        );
        assert_eq!(
            out,
            vec![(2 * S, 2.0), (4 * S, 2.0), (6 * S, 1.0), (10 * S, 1.0)]
        );
        let ones: Vec<Sample> = samples().iter().map(|(t, _)| (*t, 1.0)).collect();
        let rate = evaluate(RangeFunction::Rate, &ones, 4 * S, 4 * S, S, 4 * S);
        assert_eq!(rate, vec![(4 * S, 3.0 / 4.0)], "rows counted as one each");
    }

    /// A rate is the sum of what the samples carry, per second: one
    /// each where rows are counted, and where the samples are counts
    /// already, of the parts a window was split into, their total.
    #[test]
    fn a_rate_is_the_sum_of_the_values_per_second() {
        let rate = evaluate(RangeFunction::Rate, &samples(), 4 * S, 4 * S, S, 4 * S);
        assert_eq!(rate, vec![(4 * S, 12.0 / 4.0)]);
    }

    /// QE-14: a window cut into parts, each summarized where its
    /// samples are, gives what the samples give: exactly where a
    /// function counts, picks or adds whole numbers, and to the last
    /// digits where the order of a sum or the way a spread is put
    /// together shows.
    #[test]
    fn the_parts_of_a_window_give_what_its_samples_give() {
        // Whole values at uneven times, by a fixed sequence.
        let mut state = 7u64;
        let mut next = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            (state >> 33) % 97
        };
        let mut samples: Vec<Sample> = Vec::new();
        let mut at = 0i64;
        for _ in 0..400 {
            at += 1 + i64::try_from(next()).unwrap() * 7_000_000;
            #[allow(clippy::cast_precision_loss)]
            samples.push((at, next() as f64 - 40.0));
        }
        let end = samples.last().unwrap().0 + S;
        let functions = [
            RangeFunction::CountOverTime,
            RangeFunction::Rate,
            RangeFunction::BytesOverTime,
            RangeFunction::BytesRate,
            RangeFunction::SumOverTime,
            RangeFunction::AvgOverTime,
            RangeFunction::MinOverTime,
            RangeFunction::MaxOverTime,
            RangeFunction::FirstOverTime,
            RangeFunction::LastOverTime,
            RangeFunction::StdvarOverTime,
            RangeFunction::StddevOverTime,
        ];
        // The window as long as the step, three steps long, a step and
        // a half long, and shorter than the step.
        for (range, step, width) in [
            (S, S, S),
            (3 * S, S, S),
            (3 * S / 2, S, S / 2),
            (S / 2, S, S / 2),
        ] {
            let start = range;
            let cut = parts(&samples, start - range + width, end, width);
            for function in functions {
                assert!(in_parts(function));
                let whole = evaluate(function, &samples, start, end, step, range);
                let merged = evaluate_parts(function, &cut, start, end, step, range);
                assert_eq!(whole.len(), merged.len(), "{function} {range} {step}");
                for ((t, expected), (u, found)) in whole.iter().zip(&merged) {
                    assert_eq!(t, u, "{function}");
                    let close = (expected - found).abs() <= 1e-9 * expected.abs().max(1.0);
                    assert!(
                        close,
                        "{function} at {t}: {expected} whole, {found} in parts"
                    );
                    let exact = !matches!(
                        function,
                        RangeFunction::StdvarOverTime | RangeFunction::StddevOverTime
                    );
                    if exact {
                        assert_eq!(expected.to_bits(), found.to_bits(), "{function} at {t}");
                    }
                }
            }
        }
        assert!(!in_parts(RangeFunction::QuantileOverTime(0.5)));
        assert!(!in_parts(RangeFunction::AbsentOverTime));
    }

    #[test]
    fn unwrapped_functions_use_the_values() {
        let s = samples();
        let at = |f| evaluate(f, &s, 4 * S, 4 * S, S, 10 * S)[0].1;
        assert!((at(RangeFunction::SumOverTime) - 12.0).abs() < f64::EPSILON);
        assert!((at(RangeFunction::AvgOverTime) - 4.0).abs() < f64::EPSILON);
        assert!((at(RangeFunction::MinOverTime) - 2.0).abs() < f64::EPSILON);
        assert!((at(RangeFunction::MaxOverTime) - 6.0).abs() < f64::EPSILON);
        assert!((at(RangeFunction::FirstOverTime) - 2.0).abs() < f64::EPSILON);
        assert!((at(RangeFunction::LastOverTime) - 6.0).abs() < f64::EPSILON);
        assert!((at(RangeFunction::StdvarOverTime) - 8.0 / 3.0).abs() < 1e-9);
        assert!((at(RangeFunction::StddevOverTime) - (8.0f64 / 3.0).sqrt()).abs() < 1e-9);
        assert!((at(RangeFunction::QuantileOverTime(0.5)) - 4.0).abs() < f64::EPSILON);
        assert!((at(RangeFunction::QuantileOverTime(0.25)) - 3.0).abs() < f64::EPSILON);
        assert!((at(RangeFunction::BytesRate) - 1.2).abs() < 1e-9);
    }

    #[test]
    fn absence_is_reported_where_nothing_was_seen() {
        let out = evaluate(
            RangeFunction::AbsentOverTime,
            &samples(),
            5 * S,
            9 * S,
            2 * S,
            S,
        );
        assert_eq!(out, vec![(5 * S, 1.0), (7 * S, 1.0)]);
        assert!(evaluate(RangeFunction::CountOverTime, &[], 0, 10, 5, 5).is_empty());
    }
}
