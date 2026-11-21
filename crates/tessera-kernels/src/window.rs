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

#[allow(clippy::cast_precision_loss)]
fn apply(function: RangeFunction, window: &[Sample], range: i64) -> Option<f64> {
    let values = || window.iter().map(|(_, v)| *v);
    let n = window.len() as f64;
    let seconds = range as f64 / 1e9;
    Some(match function {
        RangeFunction::CountOverTime => n,
        RangeFunction::Rate => n / seconds,
        RangeFunction::BytesOverTime | RangeFunction::SumOverTime => values().sum(),
        RangeFunction::BytesRate => values().sum::<f64>() / seconds,
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
        let rate = evaluate(RangeFunction::Rate, &samples(), 4 * S, 4 * S, S, 4 * S);
        assert_eq!(rate, vec![(4 * S, 3.0 / 4.0)]);
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
