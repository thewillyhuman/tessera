// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Native histograms (QL-5): the sample type a stored histogram point
//! takes in `PromQL`, with Prometheus' bucket layout and algorithms.
//!
//! A histogram counts observations in exponential buckets at a
//! `schema`: bucket `i` covers `(base^(i-1), base^i]` with `base =
//! 2^(2^-schema)`, so that schema 0 doubles per bucket and every step
//! up halves each bucket. Negative observations mirror the layout, and
//! observations within `zero_threshold` of zero land in the zero
//! bucket. A histogram with custom buckets (schema [`CUSTOM_SCHEMA`],
//! what a classic histogram's bounds become) has positive buckets only:
//! bucket `i` reaches up to `custom_values[i]` and the last to `+Inf`.
//!
//! Counts are floats, as a rate makes them, and buckets are held
//! sparsely by index. The bounds, the quantile, the fraction and the
//! variance are Prometheus 3.15's (`model/histogram`,
//! `promql/quantile.go`), so that a query answers as Prometheus would.

use std::collections::BTreeMap;

/// The schema of a histogram with custom buckets.
pub const CUSTOM_SCHEMA: i32 = -53;
/// The coarsest exponential schema.
pub const MIN_SCHEMA: i32 = -4;
/// The finest exponential schema.
pub const MAX_SCHEMA: i32 = 8;

/// What a histogram sample says about counter resets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ResetHint {
    /// A reset is found by comparing with the previous sample.
    #[default]
    Unknown,
    /// The first sample after a counter reset.
    Reset,
    /// Certainly no reset since the previous sample.
    NoReset,
    /// A gauge histogram, which does not reset.
    Gauge,
}

impl ResetHint {
    /// The name a stored point carries.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ResetHint::Unknown => "unknown",
            ResetHint::Reset => "reset",
            ResetHint::NoReset => "no_reset",
            ResetHint::Gauge => "gauge",
        }
    }

    /// The hint with the given name, if any.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        [
            ResetHint::Unknown,
            ResetHint::Reset,
            ResetHint::NoReset,
            ResetHint::Gauge,
        ]
        .into_iter()
        .find(|hint| hint.as_str() == name)
    }
}

/// A native histogram.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Histogram {
    /// What the sample says about counter resets.
    pub hint: ResetHint,
    /// The exponential schema, or [`CUSTOM_SCHEMA`].
    pub schema: i32,
    /// Half the width of the zero bucket.
    pub zero_threshold: f64,
    /// The observations in the zero bucket.
    pub zero_count: f64,
    /// The observations in all.
    pub count: f64,
    /// The sum of the observations; `NaN` once one was `NaN`.
    pub sum: f64,
    /// The positive buckets by index.
    pub positive: BTreeMap<i32, f64>,
    /// The negative buckets by index.
    pub negative: BTreeMap<i32, f64>,
    /// The upper bounds of custom buckets; empty otherwise.
    pub custom_values: Vec<f64>,
}

/// One bucket with its bounds, each inclusive or not, and its count.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Bucket {
    /// The lower bound.
    pub lower: f64,
    /// The upper bound.
    pub upper: f64,
    /// Whether the lower bound belongs to the bucket.
    pub lower_inclusive: bool,
    /// Whether the upper bound belongs to the bucket.
    pub upper_inclusive: bool,
    /// The observations in the bucket.
    pub count: f64,
    /// The index within the schema; meaningless for the zero bucket.
    pub index: i32,
}

impl Bucket {
    /// The share of the bucket's width at or below `v`, which lies
    /// strictly inside finite bounds: linear for custom buckets and
    /// the zero bucket, on a logarithmic scale otherwise, where
    /// exponential bounds are evenly spaced.
    #[must_use]
    pub fn fraction_below(&self, v: f64, linear: bool) -> f64 {
        if linear {
            return (v - self.lower) / (self.upper - self.lower);
        }
        let log_lower = self.lower.abs().log2();
        let log_upper = self.upper.abs().log2();
        let log_v = v.abs().log2();
        if v > 0.0 {
            (log_v - log_lower) / (log_upper - log_lower)
        } else {
            1.0 - ((log_v - log_upper) / (log_lower - log_upper))
        }
    }
}

impl Histogram {
    /// Whether the buckets are custom rather than exponential.
    #[must_use]
    pub fn uses_custom_buckets(&self) -> bool {
        self.schema == CUSTOM_SCHEMA
    }

    /// The upper bound of positive bucket `index`; a negative bucket's
    /// bounds are the same mirrored.
    #[must_use]
    pub fn bound(&self, index: i32) -> f64 {
        bound(index, self.schema, &self.custom_values)
    }

    /// The zero bucket.
    #[must_use]
    pub fn zero_bucket(&self) -> Bucket {
        Bucket {
            lower: -self.zero_threshold,
            upper: self.zero_threshold,
            lower_inclusive: true,
            upper_inclusive: true,
            count: self.zero_count,
            index: 0,
        }
    }

    fn side_bucket(&self, positive: bool, index: i32, count: f64) -> Bucket {
        let (lower, upper) = if positive {
            (self.bound(index - 1), self.bound(index))
        } else {
            (-self.bound(index), -self.bound(index - 1))
        };
        let (lower_inclusive, upper_inclusive) = if self.uses_custom_buckets() {
            (index == 0, true)
        } else {
            (lower < 0.0, upper > 0.0)
        };
        Bucket {
            lower,
            upper,
            lower_inclusive,
            upper_inclusive,
            count,
            index,
        }
    }

    /// Every bucket in ascending order of value: the negative buckets
    /// from the most negative, the zero bucket when it counts anything,
    /// then the positive buckets upward. A bucket that overlaps the
    /// zero bucket is cut at the zero threshold.
    #[must_use]
    pub fn buckets(&self) -> Vec<Bucket> {
        let threshold = self.zero_threshold;
        let mut out = Vec::with_capacity(self.negative.len() + self.positive.len() + 1);
        for (&index, &count) in self.negative.iter().rev() {
            let mut bucket = self.side_bucket(false, index, count);
            if bucket.upper < 0.0 && bucket.upper > -threshold {
                bucket.upper = -threshold;
            }
            out.push(bucket);
        }
        if self.zero_count > 0.0 {
            out.push(self.zero_bucket());
        }
        for (&index, &count) in &self.positive {
            let mut bucket = self.side_bucket(true, index, count);
            if bucket.lower > 0.0 && bucket.lower < threshold {
                bucket.lower = threshold;
            }
            out.push(bucket);
        }
        out
    }

    /// Scales every count and the sum by `factor`; a negative factor
    /// makes a gauge.
    pub fn mul(&mut self, factor: f64) -> &mut Self {
        self.zero_count *= factor;
        self.count *= factor;
        self.sum *= factor;
        for count in self.positive.values_mut().chain(self.negative.values_mut()) {
            *count *= factor;
        }
        if factor < 0.0 {
            self.hint = ResetHint::Gauge;
        }
        self
    }

    /// Divides every count and the sum by `scalar`; dividing by zero
    /// leaves no buckets, a negative divisor makes a gauge.
    pub fn div(&mut self, scalar: f64) -> &mut Self {
        self.zero_count /= scalar;
        self.count /= scalar;
        self.sum /= scalar;
        if scalar == 0.0 {
            self.positive.clear();
            self.negative.clear();
            return self;
        }
        for count in self.positive.values_mut().chain(self.negative.values_mut()) {
            *count /= scalar;
        }
        if scalar < 0.0 {
            self.hint = ResetHint::Gauge;
        }
        self
    }

    /// Drops the empty buckets.
    pub fn compact(&mut self) -> &mut Self {
        self.positive.retain(|_, count| *count != 0.0);
        self.negative.retain(|_, count| *count != 0.0);
        self
    }

    /// Whether both histograms hold the same data: the same layout,
    /// empty buckets included, and counts that agree bit for bit.
    #[must_use]
    #[allow(clippy::float_cmp)]
    pub fn equals(&self, other: &Self) -> bool {
        if self.schema != other.schema
            || self.count.to_bits() != other.count.to_bits()
            || self.sum.to_bits() != other.sum.to_bits()
        {
            return false;
        }
        if self.uses_custom_buckets() && self.custom_values != other.custom_values {
            return false;
        }
        if self.zero_threshold != other.zero_threshold
            || self.zero_count.to_bits() != other.zero_count.to_bits()
        {
            return false;
        }
        buckets_match(&self.negative, &other.negative)
            && buckets_match(&self.positive, &other.positive)
    }

    /// Merges the buckets down to the coarser exponential schema
    /// `target`; nothing changes for custom buckets or a target that
    /// is not coarser.
    pub fn reduce_resolution(&mut self, target: i32) {
        if self.uses_custom_buckets() || target == CUSTOM_SCHEMA || target >= self.schema {
            return;
        }
        self.positive = reduced(&self.positive, self.schema, target);
        self.negative = reduced(&self.negative, self.schema, target);
        self.schema = target;
    }

    /// The `q` quantile, interpolated within the bucket that holds its
    /// rank: on a logarithmic scale for exponential buckets, linearly
    /// for custom buckets and inside the zero bucket. `NaN` without
    /// observations or for a `NaN` `q`, `-Inf` below 0, `+Inf` above 1.
    #[must_use]
    pub fn quantile(&self, q: f64) -> f64 {
        if q < 0.0 {
            return f64::NEG_INFINITY;
        }
        if q > 1.0 {
            return f64::INFINITY;
        }
        if self.count == 0.0 || q.is_nan() {
            return f64::NAN;
        }
        // NaN observations count without a bucket: they keep the walk
        // forward, as a low quantile does; a high one walks down.
        let forward = self.sum.is_nan() || q < 0.5;
        let mut ordered = self.buckets();
        if !forward {
            ordered.reverse();
        }
        let mut rank = if forward {
            q * self.count
        } else {
            (1.0 - q) * self.count
        };
        let mut count = 0.0;
        let mut bucket = Bucket::default();
        for candidate in ordered {
            bucket = candidate;
            if bucket.count == 0.0 {
                continue;
            }
            count += bucket.count;
            if count >= rank {
                break;
            }
        }
        if !self.uses_custom_buckets() && bucket.lower < 0.0 && bucket.upper > 0.0 {
            // In the zero bucket, a one-sided histogram is bounded by zero.
            if self.negative.is_empty() && !self.positive.is_empty() {
                bucket.lower = 0.0;
            } else if self.positive.is_empty() && !self.negative.is_empty() {
                bucket.upper = 0.0;
            }
        } else if self.uses_custom_buckets() {
            if bucket.lower == f64::NEG_INFINITY {
                if bucket.upper <= 0.0 {
                    return bucket.upper;
                }
                bucket.lower = 0.0;
            } else if bucket.upper == f64::INFINITY {
                return bucket.lower;
            }
        }
        if count > self.count {
            count = self.count;
        }
        if count < rank {
            // Only NaN observations reach past the last bucket.
            return if self.sum.is_nan() {
                f64::NAN
            } else {
                bucket.upper
            };
        }
        if forward {
            rank -= count - bucket.count;
        } else {
            rank = count - rank;
        }
        let fraction = rank / bucket.count;
        if self.uses_custom_buckets() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
            return bucket.lower + (bucket.upper - bucket.lower) * fraction;
        }
        let log_lower = bucket.lower.abs().log2();
        let log_upper = bucket.upper.abs().log2();
        if bucket.lower > 0.0 {
            (log_lower + (log_upper - log_lower) * fraction).exp2()
        } else {
            -(log_upper + (log_lower - log_upper) * (1.0 - fraction)).exp2()
        }
    }

    /// The share of observations between `lower` and `upper`, the
    /// inverse of [`Histogram::quantile`] under the same interpolation.
    /// `NaN` without observations or for a `NaN` bound, 0 when `lower`
    /// is not below `upper`; `NaN` observations belong to no range.
    #[must_use]
    pub fn fraction(&self, lower: f64, upper: f64) -> f64 {
        if self.count == 0.0 || lower.is_nan() || upper.is_nan() {
            return f64::NAN;
        }
        if lower >= upper {
            return 0.0;
        }
        let all = self.buckets();
        let mut walk = Ranks::default();
        let mut visited = 0;
        for &bucket in &all {
            visited += 1;
            if walk.visit(self, bucket, lower, upper) {
                break;
            }
        }
        let count = if self.sum.is_nan() {
            walk.count + all[visited..].iter().map(|b| b.count).sum::<f64>()
        } else {
            self.count
        };
        let lower_rank = walk.lower.filter(|rank| *rank <= count).unwrap_or(count);
        let upper_rank = walk.upper.filter(|rank| *rank <= count).unwrap_or(count);
        (upper_rank - lower_rank) / self.count
    }

    /// The variance of the observations, each taken at the middle of
    /// its bucket: the geometric mean of exponential bounds, zero in
    /// the zero bucket, the arithmetic mean of custom bounds.
    #[must_use]
    pub fn variance(&self) -> f64 {
        let mean = self.sum / self.count;
        let (mut variance, mut compensation) = (0.0, 0.0);
        for bucket in self.buckets() {
            if bucket.count == 0.0 {
                continue;
            }
            let value = if self.uses_custom_buckets() {
                f64::midpoint(bucket.upper, bucket.lower)
            } else if bucket.lower <= 0.0 && bucket.upper >= 0.0 {
                0.0
            } else {
                let magnitude = (bucket.upper * bucket.lower).sqrt();
                if bucket.upper < 0.0 {
                    -magnitude
                } else {
                    magnitude
                }
            };
            let delta = value - mean;
            (variance, compensation) =
                kahan_add(bucket.count * delta * delta, variance, compensation);
        }
        variance += compensation;
        variance / self.count
    }
}

/// The walk of [`Histogram::fraction`] over the buckets: the rank at
/// which each bound was found.
#[derive(Default)]
struct Ranks {
    /// The observations before the current bucket.
    rank: f64,
    /// The observations up to and including the current bucket.
    count: f64,
    lower: Option<f64>,
    upper: Option<f64>,
}

impl Ranks {
    /// Accounts for `bucket`; true once both bounds are placed.
    fn visit(&mut self, histogram: &Histogram, mut bucket: Bucket, lower: f64, upper: f64) -> bool {
        self.count += bucket.count;
        let zero_bucket = bucket.lower <= 0.0 && bucket.upper >= 0.0;
        if zero_bucket {
            // A one-sided histogram is bounded by zero in the zero bucket.
            if histogram.negative.is_empty() && !histogram.positive.is_empty() {
                bucket.lower = 0.0;
            } else if histogram.positive.is_empty() && !histogram.negative.is_empty() {
                bucket.upper = 0.0;
            }
        }
        if self.lower.is_none() && bucket.lower >= lower {
            self.lower = Some(self.rank);
        }
        if self.upper.is_none() && bucket.lower >= upper {
            self.upper = Some(self.rank);
        }
        if self.lower.is_some() && self.upper.is_some() {
            return true;
        }
        let interpolate = |v: f64| {
            if histogram.uses_custom_buckets() || zero_bucket {
                // A bucket from -Inf cannot be interpolated into: it
                // counts whole, as the +Inf one adds nothing.
                if bucket.lower == f64::NEG_INFINITY {
                    return bucket.count;
                }
                self.rank + bucket.count * bucket.fraction_below(v, true)
            } else {
                self.rank + bucket.count * bucket.fraction_below(v, false)
            }
        };
        if self.lower.is_none() && bucket.lower < lower && bucket.upper > lower {
            self.lower = Some(interpolate(lower));
        }
        if self.upper.is_none() && bucket.lower < upper && bucket.upper > upper {
            self.upper = Some(interpolate(upper));
        }
        if self.lower.is_some() && self.upper.is_some() {
            return true;
        }
        self.rank += bucket.count;
        false
    }
}

/// Kahan-Neumaier summation: `sum + inc` with the running
/// compensation `c`.
pub(crate) fn kahan_add(inc: f64, sum: f64, c: f64) -> (f64, f64) {
    let t = sum + inc;
    let c = if t.is_infinite() {
        0.0
    } else if sum.abs() >= inc.abs() {
        c + ((sum - t) + inc)
    } else {
        c + ((inc - t) + sum)
    };
    (t, c)
}

fn buckets_match(a: &BTreeMap<i32, f64>, b: &BTreeMap<i32, f64>) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|((i, x), (j, y))| i == j && x.to_bits() == y.to_bits())
}

/// The index at schema `to` of bucket `index` at the finer schema
/// `from`.
#[must_use]
pub fn target_index(index: i32, from: i32, to: i32) -> i32 {
    let shift = u32::try_from((from - to).clamp(0, 31)).unwrap_or(0);
    ((index.wrapping_sub(1)) >> shift).wrapping_add(1)
}

fn reduced(buckets: &BTreeMap<i32, f64>, from: i32, to: i32) -> BTreeMap<i32, f64> {
    let mut out: BTreeMap<i32, f64> = BTreeMap::new();
    for (&index, &count) in buckets {
        *out.entry(target_index(index, from, to)).or_insert(0.0) += count;
    }
    out
}

/// The upper bound of positive bucket `index` under `schema`: a custom
/// value, `-Inf` below the first and `+Inf` past the last, or the
/// exponential bound.
#[must_use]
pub fn bound(index: i32, schema: i32, custom_values: &[f64]) -> f64 {
    if schema == CUSTOM_SCHEMA {
        if index == -1 {
            return f64::NEG_INFINITY;
        }
        return usize::try_from(index)
            .ok()
            .and_then(|i| custom_values.get(i))
            .copied()
            .unwrap_or(f64::INFINITY);
    }
    exponential_bound(index, schema)
}

/// `2^(index * 2^-schema)`, read from the table for positive schemas.
/// The last bucket before the one of `±Inf` observations ends at the
/// largest finite number rather than overflowing.
#[allow(clippy::float_cmp)]
fn exponential_bound(index: i32, schema: i32) -> f64 {
    let schema = schema.clamp(MIN_SCHEMA, MAX_SCHEMA);
    if schema < 0 {
        let exp = i64::from(index) << -schema;
        if exp == 1024 {
            return f64::MAX;
        }
        return ldexp(1.0, exp);
    }
    let fraction_index = index & ((1 << schema) - 1);
    let fraction = BOUNDS[schema.unsigned_abs() as usize][fraction_index.unsigned_abs() as usize];
    let exp = i64::from(index >> schema) + 1;
    if fraction == 0.5 && exp == 1025 {
        return f64::MAX;
    }
    ldexp(fraction, exp)
}

/// `fraction * 2^exp`, exact as a power of two is.
fn ldexp(fraction: f64, exp: i64) -> f64 {
    if exp > 2100 {
        return fraction * f64::INFINITY;
    }
    if exp < -2200 {
        return 0.0;
    }
    let mut value = fraction;
    let mut exp = exp;
    while exp > 1023 {
        value *= f64::from_bits(0x7FE0_0000_0000_0000);
        exp -= 1023;
    }
    while exp < -1022 {
        value *= f64::from_bits(0x0010_0000_0000_0000);
        exp += 1022;
    }
    let bits = u64::try_from(exp + 1023).unwrap_or(0) << 52;
    value * f64::from_bits(bits)
}

/// Prometheus' table of bucket bounds in `[0.5, 1)` for schemas 0 to 8,
/// taken verbatim so that bounds agree to the last bit.
#[allow(clippy::unreadable_literal)]
const BOUNDS: [&[f64]; 9] = [
    // Schema 0.
    &[0.5],
    // Schema 1.
    &[0.5, 0.7071067811865475],
    // Schema 2.
    &[
        0.5,
        0.5946035575013605,
        0.7071067811865475,
        0.8408964152537144,
    ],
    // Schema 3.
    &[
        0.5,
        0.5452538663326288,
        0.5946035575013605,
        0.6484197773255048,
        0.7071067811865475,
        0.7711054127039704,
        0.8408964152537144,
        0.9170040432046711,
    ],
    // Schema 4.
    &[
        0.5,
        0.5221368912137069,
        0.5452538663326288,
        0.5693943173783458,
        0.5946035575013605,
        0.620928906036742,
        0.6484197773255048,
        0.6771277734684463,
        0.7071067811865475,
        0.7384130729697496,
        0.7711054127039704,
        0.805245165974627,
        0.8408964152537144,
        0.8781260801866495,
        0.9170040432046711,
        0.9576032806985735,
    ],
    // Schema 5.
    &[
        0.5,
        0.5109485743270583,
        0.5221368912137069,
        0.5335702003384117,
        0.5452538663326288,
        0.5571933712979462,
        0.5693943173783458,
        0.5818624293887887,
        0.5946035575013605,
        0.6076236799902344,
        0.620928906036742,
        0.6345254785958666,
        0.6484197773255048,
        0.6626183215798706,
        0.6771277734684463,
        0.6919549409819159,
        0.7071067811865475,
        0.7225904034885232,
        0.7384130729697496,
        0.7545822137967112,
        0.7711054127039704,
        0.7879904225539431,
        0.805245165974627,
        0.8228777390769823,
        0.8408964152537144,
        0.8593096490612387,
        0.8781260801866495,
        0.8973545375015533,
        0.9170040432046711,
        0.9370838170551498,
        0.9576032806985735,
        0.9785720620876999,
    ],
    // Schema 6.
    &[
        0.5,
        0.5054446430258502,
        0.5109485743270583,
        0.5165124395106142,
        0.5221368912137069,
        0.5278225891802786,
        0.5335702003384117,
        0.5393803988785598,
        0.5452538663326288,
        0.5511912916539204,
        0.5571933712979462,
        0.5632608093041209,
        0.5693943173783458,
        0.5755946149764913,
        0.5818624293887887,
        0.5881984958251406,
        0.5946035575013605,
        0.6010783657263515,
        0.6076236799902344,
        0.6142402680534349,
        0.620928906036742,
        0.6276903785123455,
        0.6345254785958666,
        0.6414350080393891,
        0.6484197773255048,
        0.6554806057623822,
        0.6626183215798706,
        0.6698337620266515,
        0.6771277734684463,
        0.6845012114872953,
        0.6919549409819159,
        0.6994898362691555,
        0.7071067811865475,
        0.7148066691959849,
        0.7225904034885232,
        0.7304588970903234,
        0.7384130729697496,
        0.7464538641456323,
        0.7545822137967112,
        0.762799075372269,
        0.7711054127039704,
        0.7795022001189185,
        0.7879904225539431,
        0.7965710756711334,
        0.805245165974627,
        0.8140137109286738,
        0.8228777390769823,
        0.8318382901633681,
        0.8408964152537144,
        0.8500531768592616,
        0.8593096490612387,
        0.8686669176368529,
        0.8781260801866495,
        0.8876882462632604,
        0.8973545375015533,
        0.9071260877501991,
        0.9170040432046711,
        0.9269895625416926,
        0.9370838170551498,
        0.9472879907934827,
        0.9576032806985735,
        0.9680308967461471,
        0.9785720620876999,
        0.9892280131939752,
    ],
    // Schema 7.
    &[
        0.5,
        0.5027149505564014,
        0.5054446430258502,
        0.5081891574554764,
        0.5109485743270583,
        0.5137229745593818,
        0.5165124395106142,
        0.5193170509806894,
        0.5221368912137069,
        0.5249720429003435,
        0.5278225891802786,
        0.5306886136446309,
        0.5335702003384117,
        0.5364674337629877,
        0.5393803988785598,
        0.5423091811066545,
        0.5452538663326288,
        0.5482145409081883,
        0.5511912916539204,
        0.5541842058618393,
        0.5571933712979462,
        0.5602188762048033,
        0.5632608093041209,
        0.5663192597993595,
        0.5693943173783458,
        0.572486072215902,
        0.5755946149764913,
        0.5787200368168754,
        0.5818624293887887,
        0.585021884841625,
        0.5881984958251406,
        0.5913923554921704,
        0.5946035575013605,
        0.5978321960199137,
        0.6010783657263515,
        0.6043421618132907,
        0.6076236799902344,
        0.6109230164863786,
        0.6142402680534349,
        0.6175755319684665,
        0.620928906036742,
        0.6243004885946023,
        0.6276903785123455,
        0.6310986751971253,
        0.6345254785958666,
        0.637970889198196,
        0.6414350080393891,
        0.6449179367033329,
        0.6484197773255048,
        0.6519406325959679,
        0.6554806057623822,
        0.659039800633032,
        0.6626183215798706,
        0.6662162735415805,
        0.6698337620266515,
        0.6734708931164728,
        0.6771277734684463,
        0.6808045103191123,
        0.6845012114872953,
        0.688217985377265,
        0.6919549409819159,
        0.6957121878859629,
        0.6994898362691555,
        0.7032879969095076,
        0.7071067811865475,
        0.7109463010845827,
        0.7148066691959849,
        0.718687998724491,
        0.7225904034885232,
        0.7265139979245261,
        0.7304588970903234,
        0.7344252166684908,
        0.7384130729697496,
        0.7424225829363761,
        0.7464538641456323,
        0.7505070348132126,
        0.7545822137967112,
        0.7586795205991071,
        0.762799075372269,
        0.7669409989204777,
        0.7711054127039704,
        0.7752924388424999,
        0.7795022001189185,
        0.7837348199827764,
        0.7879904225539431,
        0.7922691326262467,
        0.7965710756711334,
        0.8008963778413465,
        0.805245165974627,
        0.8096175675974316,
        0.8140137109286738,
        0.8184337248834821,
        0.8228777390769823,
        0.8273458838280969,
        0.8318382901633681,
        0.8363550898207981,
        0.8408964152537144,
        0.8454623996346523,
        0.8500531768592616,
        0.8546688815502312,
        0.8593096490612387,
        0.8639756154809185,
        0.8686669176368529,
        0.8733836930995842,
        0.8781260801866495,
        0.8828942179666361,
        0.8876882462632604,
        0.8925083056594671,
        0.8973545375015533,
        0.9022270839033115,
        0.9071260877501991,
        0.9120516927035263,
        0.9170040432046711,
        0.9219832844793128,
        0.9269895625416926,
        0.9320230241988943,
        0.9370838170551498,
        0.9421720895161669,
        0.9472879907934827,
        0.9524316709088368,
        0.9576032806985735,
        0.9628029718180622,
        0.9680308967461471,
        0.9732872087896164,
        0.9785720620876999,
        0.9838856116165875,
        0.9892280131939752,
        0.9945994234836328,
    ],
    // Schema 8.
    &[
        0.5,
        0.5013556375251013,
        0.5027149505564014,
        0.5040779490592088,
        0.5054446430258502,
        0.5068150424757447,
        0.5081891574554764,
        0.509566998038869,
        0.5109485743270583,
        0.5123338964485679,
        0.5137229745593818,
        0.5151158188430205,
        0.5165124395106142,
        0.5179128468009786,
        0.5193170509806894,
        0.520725062344158,
        0.5221368912137069,
        0.5235525479396449,
        0.5249720429003435,
        0.526395386502313,
        0.5278225891802786,
        0.5292536613972564,
        0.5306886136446309,
        0.5321274564422321,
        0.5335702003384117,
        0.5350168559101208,
        0.5364674337629877,
        0.5379219445313954,
        0.5393803988785598,
        0.5408428074966075,
        0.5423091811066545,
        0.5437795304588847,
        0.5452538663326288,
        0.5467321995364429,
        0.5482145409081883,
        0.549700901315111,
        0.5511912916539204,
        0.5526857228508706,
        0.5541842058618393,
        0.5556867516724088,
        0.5571933712979462,
        0.5587040757836845,
        0.5602188762048033,
        0.5617377836665098,
        0.5632608093041209,
        0.564787964283144,
        0.5663192597993595,
        0.5678547070789026,
        0.5693943173783458,
        0.5709381019847808,
        0.572486072215902,
        0.5740382394200894,
        0.5755946149764913,
        0.5771552102951081,
        0.5787200368168754,
        0.5802891060137493,
        0.5818624293887887,
        0.5834400184762408,
        0.585021884841625,
        0.5866080400818185,
        0.5881984958251406,
        0.5897932637314379,
        0.5913923554921704,
        0.5929957828304968,
        0.5946035575013605,
        0.5962156912915756,
        0.5978321960199137,
        0.5994530835371903,
        0.6010783657263515,
        0.6027080545025619,
        0.6043421618132907,
        0.6059806996384005,
        0.6076236799902344,
        0.6092711149137041,
        0.6109230164863786,
        0.6125793968185725,
        0.6142402680534349,
        0.6159056423670379,
        0.6175755319684665,
        0.6192499490999082,
        0.620928906036742,
        0.622612415087629,
        0.6243004885946023,
        0.6259931389331581,
        0.6276903785123455,
        0.6293922197748583,
        0.6310986751971253,
        0.6328097572894031,
        0.6345254785958666,
        0.6362458516947014,
        0.637970889198196,
        0.6397006037528346,
        0.6414350080393891,
        0.6431741147730128,
        0.6449179367033329,
        0.6466664866145447,
        0.6484197773255048,
        0.6501778216898253,
        0.6519406325959679,
        0.6537082229673385,
        0.6554806057623822,
        0.6572577939746774,
        0.659039800633032,
        0.6608266388015788,
        0.6626183215798706,
        0.6644148621029772,
        0.6662162735415805,
        0.6680225691020727,
        0.6698337620266515,
        0.6716498655934177,
        0.6734708931164728,
        0.6752968579460171,
        0.6771277734684463,
        0.6789636531064505,
        0.6808045103191123,
        0.6826503586020058,
        0.6845012114872953,
        0.6863570825438342,
        0.688217985377265,
        0.690083933630119,
        0.6919549409819159,
        0.6938310211492645,
        0.6957121878859629,
        0.6975984549830999,
        0.6994898362691555,
        0.7013863456101023,
        0.7032879969095076,
        0.7051948041086352,
        0.7071067811865475,
        0.7090239421602076,
        0.7109463010845827,
        0.7128738720527471,
        0.7148066691959849,
        0.7167447066838943,
        0.718687998724491,
        0.7206365595643126,
        0.7225904034885232,
        0.7245495448210174,
        0.7265139979245261,
        0.7284837772007218,
        0.7304588970903234,
        0.7324393720732029,
        0.7344252166684908,
        0.7364164454346837,
        0.7384130729697496,
        0.7404151139112358,
        0.7424225829363761,
        0.7444354947621984,
        0.7464538641456323,
        0.7484777058836176,
        0.7505070348132126,
        0.7525418658117031,
        0.7545822137967112,
        0.7566280937263048,
        0.7586795205991071,
        0.7607365094544071,
        0.762799075372269,
        0.7648672334736434,
        0.7669409989204777,
        0.7690203869158282,
        0.7711054127039704,
        0.7731960915705107,
        0.7752924388424999,
        0.7773944698885442,
        0.7795022001189185,
        0.7816156449856788,
        0.7837348199827764,
        0.7858597406461707,
        0.7879904225539431,
        0.7901268813264122,
        0.7922691326262467,
        0.7944171921585818,
        0.7965710756711334,
        0.7987307989543135,
        0.8008963778413465,
        0.8030678282083853,
        0.805245165974627,
        0.8074284071024302,
        0.8096175675974316,
        0.8118126635086642,
        0.8140137109286738,
        0.8162207259936375,
        0.8184337248834821,
        0.820652723822003,
        0.8228777390769823,
        0.8251087869603088,
        0.8273458838280969,
        0.8295890460808079,
        0.8318382901633681,
        0.8340936325652911,
        0.8363550898207981,
        0.8386226785089391,
        0.8408964152537144,
        0.8431763167241966,
        0.8454623996346523,
        0.8477546807446661,
        0.8500531768592616,
        0.8523579048290255,
        0.8546688815502312,
        0.8569861239649629,
        0.8593096490612387,
        0.8616394738731368,
        0.8639756154809185,
        0.8663180910111553,
        0.8686669176368529,
        0.871022112577578,
        0.8733836930995842,
        0.8757516765159389,
        0.8781260801866495,
        0.8805069215187917,
        0.8828942179666361,
        0.8852879870317771,
        0.8876882462632604,
        0.890095013257712,
        0.8925083056594671,
        0.8949281411607002,
        0.8973545375015533,
        0.8997875124702672,
        0.9022270839033115,
        0.9046732696855155,
        0.9071260877501991,
        0.909585556079304,
        0.9120516927035263,
        0.9145245157024483,
        0.9170040432046711,
        0.9194902933879467,
        0.9219832844793128,
        0.9244830347552253,
        0.9269895625416926,
        0.92950288621441,
        0.9320230241988943,
        0.9345499949706191,
        0.9370838170551498,
        0.93962450902828,
        0.9421720895161669,
        0.9447265771954693,
        0.9472879907934827,
        0.9498563490882775,
        0.9524316709088368,
        0.9550139751351947,
        0.9576032806985735,
        0.9601996065815236,
        0.9628029718180622,
        0.9654133954938133,
        0.9680308967461471,
        0.9706554947643201,
        0.9732872087896164,
        0.9759260581154889,
        0.9785720620876999,
        0.9812252401044634,
        0.9838856116165875,
        0.9865531961276168,
        0.9892280131939752,
        0.9919100824251095,
        0.9945994234836328,
        0.9972960560854698,
    ],
];

#[cfg(test)]
mod tests {
    #![allow(clippy::unreadable_literal)]

    use super::*;

    /// A histogram as Prometheus' test format writes one: `buckets`
    /// from index 0 upward and `n_buckets` likewise on the negative
    /// side; `zero` is the zero bucket's count and threshold.
    fn exponential(
        schema: i32,
        count: f64,
        sum: f64,
        zero: (f64, f64),
        buckets: &[f64],
        n_buckets: &[f64],
    ) -> Histogram {
        let side = |counts: &[f64]| {
            counts
                .iter()
                .enumerate()
                .map(|(i, c)| (i32::try_from(i).unwrap(), *c))
                .collect()
        };
        Histogram {
            schema,
            zero_threshold: zero.1,
            zero_count: zero.0,
            count,
            sum,
            positive: side(buckets),
            negative: side(n_buckets),
            ..Histogram::default()
        }
    }

    fn with_bounds(count: f64, sum: f64, bounds: &[f64], buckets: &[f64]) -> Histogram {
        let mut histogram = exponential(CUSTOM_SCHEMA, count, sum, (0.0, 0.0), buckets, &[]);
        histogram.custom_values = bounds.to_vec();
        histogram
    }

    #[track_caller]
    fn assert_near(actual: f64, expected: f64) {
        if expected.is_nan() || expected.is_infinite() {
            assert!(
                actual.to_bits() == expected.to_bits(),
                "expected {expected}, got {actual}"
            );
            return;
        }
        let tolerance = 1e-12 * expected.abs().max(1.0);
        assert!(
            (actual - expected).abs() <= tolerance,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn bounds_follow_prometheus_schemas() {
        for schema in MIN_SCHEMA..=MAX_SCHEMA {
            assert_near(bound(0, schema, &[]), 1.0);
        }
        assert_near(bound(1, 0, &[]), 2.0);
        assert_near(bound(-1, 0, &[]), 0.5);
        assert_near(bound(3, 0, &[]), 8.0);
        assert_near(bound(1, -1, &[]), 4.0);
        assert_near(bound(-1, -2, &[]), 1.0 / 16.0);
        assert_near(bound(1, 1, &[]), 2f64.sqrt());
        assert_near(bound(2, 1, &[]), 2.0);
        assert_near(bound(1, 3, &[]), 2f64.powf(0.125));
        assert_near(bound(-9, 3, &[]), 2f64.powf(-9.0 / 8.0));
        assert_near(bound(1024, 0, &[]), f64::MAX);
        assert_near(bound(1025, 0, &[]), f64::INFINITY);
        assert_near(bound(64, -4, &[]), f64::MAX);
        assert_near(bound(-1075, 0, &[]), 0.0);
        assert_near(bound(-1, CUSTOM_SCHEMA, &[1.0, 2.0]), f64::NEG_INFINITY);
        assert_near(bound(0, CUSTOM_SCHEMA, &[1.0, 2.0]), 1.0);
        assert_near(bound(2, CUSTOM_SCHEMA, &[1.0, 2.0]), f64::INFINITY);
        assert_eq!(target_index(3, 1, 0), 2);
        assert_eq!(target_index(-1, 1, 0), 0);
        assert_eq!(target_index(5, 2, 0), 2);
    }

    #[test]
    fn buckets_list_negative_zero_and_positive_in_order() {
        let histogram = Histogram {
            schema: 0,
            zero_threshold: 1.5,
            zero_count: 1.0,
            count: 6.0,
            sum: 0.0,
            positive: BTreeMap::from([(1, 2.0)]),
            negative: BTreeMap::from([(1, 3.0), (2, 0.0)]),
            ..Histogram::default()
        };
        let buckets = histogram.buckets();
        let shown: Vec<String> = buckets
            .iter()
            .map(|b| {
                format!(
                    "{}{},{}{}:{}",
                    if b.lower_inclusive { '[' } else { '(' },
                    b.lower,
                    b.upper,
                    if b.upper_inclusive { ']' } else { ')' },
                    b.count
                )
            })
            .collect();
        assert_eq!(
            shown,
            vec!["[-4,-2):0", "[-2,-1.5):3", "[-1.5,1.5]:1", "(1.5,2]:2"],
            "buckets next to the zero bucket are cut at its threshold"
        );
        let custom = with_bounds(4.0, 5.0, &[5.0, 10.0], &[1.0, 2.0, 1.0]);
        let bounds: Vec<(f64, f64, bool)> = custom
            .buckets()
            .iter()
            .map(|b| (b.lower, b.upper, b.lower_inclusive))
            .collect();
        assert_eq!(
            bounds,
            vec![
                (f64::NEG_INFINITY, 5.0, true),
                (5.0, 10.0, false),
                (10.0, f64::INFINITY, false)
            ]
        );
    }

    /// The quantiles of Prometheus' `native_histograms.test`, with its
    /// exponential interpolation inside a bucket.
    #[test]
    fn quantiles_interpolate_exponentially() {
        let single = exponential(0, 4.0, 5.0, (0.0, 0.0), &[1.0, 2.0, 1.0], &[]);
        assert_near(single.quantile(0.5), std::f64::consts::SQRT_2);
        let positive = exponential(
            0,
            12.0,
            100.0,
            (2.0, 0.001),
            &[2.0, 3.0, 0.0, 1.0, 4.0],
            &[],
        );
        assert_near(positive.quantile(1.001), f64::INFINITY);
        assert_near(positive.quantile(1.0), 16.0);
        assert_near(positive.quantile(0.99), 15.67072476139083);
        assert_near(positive.quantile(0.9), 12.99603834169977);
        assert_near(positive.quantile(0.6), 4.594793419988138);
        assert_near(positive.quantile(0.5), 1.5874010519681994);
        assert_near(positive.quantile(0.1), 0.0006);
        assert_near(positive.quantile(0.0), 0.0);
        assert_near(positive.quantile(-1.0), f64::NEG_INFINITY);
        assert_near(positive.quantile(f64::NAN), f64::NAN);
        let negative = exponential(
            0,
            12.0,
            100.0,
            (2.0, 0.001),
            &[],
            &[2.0, 3.0, 0.0, 1.0, 4.0],
        );
        assert_near(negative.quantile(1.0), 0.0);
        assert_near(negative.quantile(0.99), -0.00006);
        assert_near(negative.quantile(0.5), -1.5874010519681996);
        assert_near(negative.quantile(0.1), -12.996038341699768);
        assert_near(negative.quantile(0.0), -16.0);
        let both = exponential(
            0,
            24.0,
            100.0,
            (4.0, 0.001),
            &[2.0, 3.0, 0.0, 1.0, 4.0],
            &[2.0, 3.0, 0.0, 1.0, 4.0],
        );
        assert_near(both.quantile(0.99), 15.34822590920423);
        assert_near(both.quantile(0.7), 1.2030250360821164);
        assert_near(both.quantile(0.55), 0.0006);
        assert_near(both.quantile(0.5), 0.0);
        assert_near(both.quantile(0.45), -0.0006);
        assert_near(both.quantile(0.3), -1.2030250360821169);
        assert_near(both.quantile(0.01), -15.34822590920423);
        let balanced = exponential(0, 8.0, 0.0, (0.0, 0.0), &[1.0, 2.0, 1.0], &[1.0, 2.0, 1.0]);
        assert_near(balanced.quantile(0.5), 0.5);
        let zero_only = exponential(0, 1.0, 0.25, (1.0, 0.5), &[], &[]);
        assert_near(zero_only.quantile(0.5), 0.0);
        assert_near(Histogram::default().quantile(0.5), f64::NAN);
        let custom = with_bounds(4.0, 5.0, &[5.0, 10.0], &[1.0, 2.0, 1.0]);
        assert_near(custom.quantile(0.5), 7.5);
        assert_near(custom.quantile(0.1), 2.0);
        assert_near(custom.quantile(1.0), 10.0);
        for (schema, expected) in [(-1, 2.0), (0, 2f64.sqrt()), (1, 2f64.powf(0.25))] {
            let varied = exponential(schema, 5.0, 4.0, (0.0, 0.0), &[0.0, 5.0], &[]);
            assert_near(varied.quantile(0.5), expected);
            let mirrored = exponential(schema, 5.0, 4.0, (0.0, 0.0), &[], &[0.0, 5.0]);
            assert_near(mirrored.quantile(0.5), -expected);
        }
    }

    /// The fractions of Prometheus' `native_histograms.test`.
    #[test]
    fn fractions_invert_quantiles() {
        let positive = exponential(
            0,
            12.0,
            100.0,
            (2.0, 0.001),
            &[2.0, 3.0, 0.0, 1.0, 4.0],
            &[],
        );
        assert_near(positive.fraction(0.0, f64::INFINITY), 1.0);
        assert_near(positive.fraction(f64::NEG_INFINITY, 0.0), 0.0);
        assert_near(positive.fraction(0.0, 0.001), 0.16666666666666666);
        assert_near(positive.fraction(0.001, f64::INFINITY), 0.8333333333333334);
        assert_near(positive.fraction(0.0, 0.0005), 0.08333333333333333);
        assert_near(positive.quantile(0.08333333333333333), 0.0005);
        assert_near(positive.fraction(1.0, 2.0), 0.25);
        assert_near(positive.fraction(0.0, 1.5), 0.4795739585136224);
        assert_near(positive.fraction(1.5, 2.0), 0.10375937481971091);
        assert_near(positive.fraction(1.0, 8.0), 0.3333333333333333);
        assert_near(positive.fraction(0.0, 6.0), 0.6320802083934297);
        assert_near(positive.quantile(0.6320802083934297), 6.0);
        assert_near(positive.fraction(-2.0, -1.0), 0.0);
        assert_near(positive.fraction(42.0, 3.0), 0.0);
        assert_near(positive.fraction(0.0, 0.0), 0.0);
        assert_near(positive.fraction(f64::NAN, 42.0), f64::NAN);
        assert_near(positive.fraction(f64::NEG_INFINITY, f64::INFINITY), 1.0);
        let both = exponential(
            0,
            24.0,
            100.0,
            (4.0, 0.001),
            &[2.0, 3.0, 0.0, 1.0, 4.0],
            &[2.0, 3.0, 0.0, 1.0, 4.0],
        );
        assert_near(both.fraction(-0.0005, 0.0005), 0.08333333333333333);
        assert_near(both.fraction(f64::NEG_INFINITY, 0.0005), 0.5416666666666666);
        assert_near(both.fraction(1.5, 2.0), 0.051879687409855414);
        assert_near(both.fraction(-6.0, -1.5), 0.07625312493990362);
        assert_near(both.fraction(-6.0, -1.0), 0.14937343753004817);
        let negative = exponential(
            0,
            12.0,
            100.0,
            (2.0, 0.001),
            &[],
            &[2.0, 3.0, 0.0, 1.0, 4.0],
        );
        assert_near(
            negative.fraction(f64::NEG_INFINITY, -6.0),
            0.36791979160657035,
        );
        assert_near(negative.quantile(0.36791979160657035), -6.0);
        assert_near(negative.fraction(-8.0, -1.0), 0.3333333333333333);
        let custom = with_bounds(4.0, 5.0, &[5.0, 10.0], &[1.0, 2.0, 1.0]);
        assert_near(custom.fraction(5.0, 10.0), 0.5);
        assert_near(custom.fraction(f64::NEG_INFINITY, 5.0), 0.25);
        // Nothing in the +Inf bucket lies below a finite bound.
        assert_near(custom.fraction(7.5, 20.0), 0.25);
        assert_near(Histogram::default().fraction(3.0, 42.0), f64::NAN);
        let empty_custom = with_bounds(0.0, 0.0, &[-2.0, 3.0], &[]);
        assert_near(empty_custom.fraction(0.0, 8.0), f64::NAN);
    }

    /// `NaN` observations raise the count but sit in no bucket.
    #[test]
    fn nan_observations_belong_to_no_bucket() {
        let all_nan = exponential(0, 3.0, f64::NAN, (0.0, 0.0), &[], &[]);
        assert_near(all_nan.quantile(0.8), f64::NAN);
        assert_near(all_nan.quantile(0.4), f64::NAN);
        assert_near(all_nan.fraction(f64::NEG_INFINITY, f64::INFINITY), 0.0);
        let some_nan = exponential(0, 15.0, f64::NAN, (0.0, 0.0), &[12.0], &[]);
        assert_near(some_nan.quantile(1.0), f64::NAN);
        assert_near(some_nan.quantile(0.8), 1.0);
        assert_near(some_nan.quantile(0.4), 0.7071067811865475);
        assert_near(
            some_nan.fraction(f64::NEG_INFINITY, 0.7071067811865475),
            0.4,
        );
        assert_near(some_nan.fraction(f64::NEG_INFINITY, f64::INFINITY), 0.8);
    }

    #[test]
    fn variance_takes_each_bucket_at_its_middle() {
        let low = exponential(
            2,
            4.0,
            10.0,
            (0.0, 0.0),
            &[1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0],
            &[],
        );
        assert_near(low.variance(), 1.163807968526718);
        assert_near(low.variance().sqrt(), 1.0787993180043811);
        let negative = exponential(
            0,
            10.0,
            -112946.0,
            (0.0, 0.0),
            &[],
            &[
                0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0, 1.0, 0.0,
                0.0, 1.0,
            ],
        );
        assert_near(negative.variance(), 759352122.1939945);
        let tens = exponential(
            0,
            10.0,
            -100.0,
            (0.0, 0.0),
            &[],
            &[0.0, 0.0, 0.0, 0.0, 10.0],
        );
        assert_near(tens.variance(), 1.725830020304794);
        let nan = exponential(3, 7.0, f64::NAN, (1.0, 0.0), &[1.0], &[1.0]);
        assert_near(nan.variance(), f64::NAN);
        let custom = with_bounds(4.0, 5.0, &[5.0, 10.0], &[0.0, 4.0, 0.0]);
        assert_near(custom.variance(), (7.5 - 1.25) * (7.5 - 1.25));
    }

    #[test]
    fn scaling_compaction_and_equality() {
        let base = exponential(
            0,
            30.0,
            33.0,
            (3.0, 0.001),
            &[3.0, 3.0, 3.0],
            &[6.0, 6.0, 6.0],
        );
        let mut tripled = base.clone();
        tripled.mul(3.0);
        assert_near(tripled.count, 90.0);
        assert_near(tripled.sum, 99.0);
        assert_near(tripled.zero_count, 9.0);
        assert_near(tripled.positive[&1], 9.0);
        assert_near(tripled.negative[&2], 18.0);
        assert_eq!(tripled.hint, ResetHint::Unknown);
        let mut negated = base.clone();
        negated.mul(-1.0);
        assert_eq!(
            negated.hint,
            ResetHint::Gauge,
            "a negative factor makes a gauge"
        );
        let mut third = base.clone();
        third.div(3.0);
        assert_near(third.count, 10.0);
        assert_near(third.positive[&0], 1.0);
        let mut by_zero = base.clone();
        by_zero.div(0.0);
        assert_near(by_zero.count, f64::INFINITY);
        assert_near(by_zero.zero_count, f64::INFINITY);
        assert!(by_zero.positive.is_empty() && by_zero.negative.is_empty());
        let mut sparse = exponential(0, 3.0, 3.0, (0.0, 0.0), &[1.0, 0.0, 2.0], &[0.0]);
        sparse.compact();
        assert_eq!(sparse.positive, BTreeMap::from([(0, 1.0), (2, 2.0)]));
        assert!(sparse.negative.is_empty());
        assert!(base.equals(&base.clone()));
        assert!(!base.equals(&tripled));
        let mut with_empty = base.clone();
        with_empty.positive.insert(7, 0.0);
        assert!(
            !base.equals(&with_empty),
            "an empty bucket is a different layout"
        );
        let mut nan_sum = base.clone();
        nan_sum.sum = f64::NAN;
        assert!(nan_sum.equals(&nan_sum.clone()), "NaN compares by bits");
        let mut wider_zero = base.clone();
        wider_zero.zero_threshold = 0.01;
        assert!(!base.equals(&wider_zero));
        let a = with_bounds(4.0, 5.0, &[5.0, 10.0], &[1.0, 2.0, 1.0]);
        let mut b = a.clone();
        b.custom_values = vec![5.0, 20.0];
        assert!(!a.equals(&b));
        let mut reduced = exponential(1, 6.0, 6.0, (0.0, 0.0), &[1.0, 1.0, 1.0, 1.0], &[2.0, 2.0]);
        reduced.positive.insert(-1, 1.0);
        reduced.reduce_resolution(0);
        assert_eq!(reduced.schema, 0);
        assert_eq!(
            reduced.positive,
            BTreeMap::from([(0, 2.0), (1, 2.0), (2, 1.0)]),
            "indices -1 and 0 fold into 0, 1 and 2 into 1, 3 into 2"
        );
        assert_eq!(reduced.negative, BTreeMap::from([(0, 2.0), (1, 2.0)]));
        let mut unchanged = a.clone();
        unchanged.reduce_resolution(0);
        assert_eq!(unchanged, a, "custom buckets keep their schema");
    }
}
