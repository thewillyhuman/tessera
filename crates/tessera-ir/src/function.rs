// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The function registry.
//!
//! The registry names each scalar and aggregate function and gives its
//! result type. Each function has exactly one implementation, in the
//! kernels, used by batch execution and by the incremental runtime alike
//! (QE-20); this is what makes differential and incremental-versus-batch
//! tests meaningful.
//!
//! Aggregates are weight-aware (QE-13): over rows with `sample_rate < 1`,
//! `count`, `sum` and `avg` scale each row by `1 / sample_rate`; the
//! `_raw` variants ignore weights.

use std::collections::BTreeMap;

use arrow::datatypes::{DataType, TimeUnit};

/// How a function's result type follows from its argument types, or
/// `None` when the function does not accept those types.
pub type ReturnType = fn(&[DataType]) -> Option<DataType>;

/// A scalar function.
#[derive(Clone, Debug)]
pub struct ScalarFunction {
    /// The name, lowercase.
    pub name: &'static str,
    /// The typing rule.
    pub return_type: ReturnType,
}

/// An aggregate function.
#[derive(Clone, Debug)]
pub struct AggregateFunction {
    /// The name, lowercase.
    pub name: &'static str,
    /// The typing rule.
    pub return_type: ReturnType,
    /// Whether rows are scaled by `1 / sample_rate`.
    pub weighted: bool,
}

/// The functions a query may call.
#[derive(Clone, Debug, Default)]
pub struct Registry {
    scalars: BTreeMap<&'static str, ScalarFunction>,
    aggregates: BTreeMap<&'static str, AggregateFunction>,
}

fn timestamp() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

fn is_numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int32 | DataType::Int64 | DataType::Float64
    )
}

/// The type a sum of integers is kept in: 32-bit inputs widen to 64 bits.
fn widened(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Int32 => DataType::Int64,
        other => other.clone(),
    }
}

fn count(args: &[DataType]) -> Option<DataType> {
    (args.len() <= 1).then_some(DataType::Int64)
}

fn weighted_count(args: &[DataType]) -> Option<DataType> {
    (args.len() <= 1).then_some(DataType::Float64)
}

fn numeric_widened(args: &[DataType]) -> Option<DataType> {
    match args {
        [t] if is_numeric(t) => Some(widened(t)),
        _ => None,
    }
}

fn weighted_sum(args: &[DataType]) -> Option<DataType> {
    match args {
        [t] if is_numeric(t) => Some(DataType::Float64),
        _ => None,
    }
}

fn average(args: &[DataType]) -> Option<DataType> {
    match args {
        [t] if is_numeric(t) => Some(DataType::Float64),
        _ => None,
    }
}

fn extremum(args: &[DataType]) -> Option<DataType> {
    match args {
        [t] if is_numeric(t) || *t == DataType::Utf8 || *t == timestamp() => Some(t.clone()),
        _ => None,
    }
}

fn time_bucket(args: &[DataType]) -> Option<DataType> {
    match args {
        [DataType::Duration(_), t] if *t == timestamp() => Some(timestamp()),
        _ => None,
    }
}

/// `tumble(timestamp, interval)`: the start of the tumbling window a
/// time falls in (section 9.5), `time_bucket` with its arguments in
/// the order windows are written.
fn tumble(args: &[DataType]) -> Option<DataType> {
    match args {
        [t, DataType::Duration(_)] if *t == timestamp() => Some(timestamp()),
        _ => None,
    }
}

/// `slide(timestamp, size)` types like `tumble`: a sliding window of
/// `size` that an alert rule advances by its evaluation interval
/// (CQ-7); the incremental evaluator alone runs it.
///
/// `hop(timestamp, size, step)`: a hopping window of `size` starting
/// every `step` (CQ-7). A row belongs to several windows, so only the
/// incremental evaluator runs it; the type is the window start.
fn hop(args: &[DataType]) -> Option<DataType> {
    match args {
        [t, DataType::Duration(_), DataType::Duration(_)] if *t == timestamp() => Some(timestamp()),
        _ => None,
    }
}

fn now(args: &[DataType]) -> Option<DataType> {
    args.is_empty().then(timestamp)
}

fn text_to_text(args: &[DataType]) -> Option<DataType> {
    match args {
        [DataType::Utf8] => Some(DataType::Utf8),
        _ => None,
    }
}

fn text_to_int(args: &[DataType]) -> Option<DataType> {
    match args {
        [DataType::Utf8] => Some(DataType::Int64),
        _ => None,
    }
}

fn links_predicate(args: &[DataType]) -> Option<DataType> {
    match args {
        [DataType::Utf8, DataType::Utf8, DataType::Utf8] => Some(DataType::Boolean),
        _ => None,
    }
}

fn text_predicate(args: &[DataType]) -> Option<DataType> {
    match args {
        [DataType::Utf8, DataType::Utf8] => Some(DataType::Boolean),
        _ => None,
    }
}

fn text_to_float(args: &[DataType]) -> Option<DataType> {
    match args {
        [DataType::Utf8] => Some(DataType::Float64),
        _ => None,
    }
}

fn prometheus_name(args: &[DataType]) -> Option<DataType> {
    match args {
        [
            DataType::Utf8,
            DataType::Utf8,
            DataType::Utf8,
            DataType::Boolean,
        ] => Some(DataType::Utf8),
        _ => None,
    }
}

fn coalesce(args: &[DataType]) -> Option<DataType> {
    let first = args.first()?;
    args.iter().all(|t| t == first).then(|| first.clone())
}

/// The scalar functions of the core engine.
const SCALARS: &[ScalarFunction] = &[
    ScalarFunction {
        name: "time_bucket",
        return_type: time_bucket,
    },
    ScalarFunction {
        name: "tumble",
        return_type: tumble,
    },
    ScalarFunction {
        name: "hop",
        return_type: hop,
    },
    ScalarFunction {
        name: "slide",
        return_type: tumble,
    },
    ScalarFunction {
        name: "prometheus_name",
        return_type: prometheus_name,
    },
    ScalarFunction {
        name: "now",
        return_type: now,
    },
    ScalarFunction {
        name: "lower",
        return_type: text_to_text,
    },
    ScalarFunction {
        name: "upper",
        return_type: text_to_text,
    },
    ScalarFunction {
        name: "length",
        return_type: text_to_int,
    },
    ScalarFunction {
        name: "coalesce",
        return_type: coalesce,
    },
    // Text predicates (section 9.7): a substring, a token by the
    // rules of QL-18, and a regular expression.
    ScalarFunction {
        name: "contains",
        return_type: text_predicate,
    },
    ScalarFunction {
        name: "has_token",
        return_type: text_predicate,
    },
    ScalarFunction {
        name: "regexp_match",
        return_type: text_predicate,
    },
    // `LogQL`'s `ip()` label filter: the text is one address, a CIDR
    // block or an inclusive range.
    ScalarFunction {
        name: "ip_matches",
        return_type: text_predicate,
    },
    // `TraceQL`'s `link.` scope: whether a span's links JSON has a link
    // whose field equals a value (QL-15).
    ScalarFunction {
        name: "links_match",
        return_type: links_predicate,
    },
    // Log-line helpers (LogQL section 9.4): strip ANSI colour codes;
    // read a duration (`1.5s`, `250ms`) as seconds; read a byte
    // size (`2KB`, `1.5MiB`) as bytes.
    ScalarFunction {
        name: "decolorize",
        return_type: text_to_text,
    },
    ScalarFunction {
        name: "parse_duration",
        return_type: text_to_float,
    },
    ScalarFunction {
        name: "parse_bytes",
        return_type: text_to_float,
    },
];

/// The aggregate functions of the core engine.
const AGGREGATES: &[AggregateFunction] = &[
    AggregateFunction {
        name: "count",
        return_type: weighted_count,
        weighted: true,
    },
    AggregateFunction {
        name: "count_raw",
        return_type: count,
        weighted: false,
    },
    AggregateFunction {
        name: "sum",
        return_type: weighted_sum,
        weighted: true,
    },
    AggregateFunction {
        name: "sum_raw",
        return_type: numeric_widened,
        weighted: false,
    },
    AggregateFunction {
        name: "avg",
        return_type: average,
        weighted: true,
    },
    AggregateFunction {
        name: "avg_raw",
        return_type: average,
        weighted: false,
    },
    // Population standard deviation and variance, as PromQL and LogQL
    // define `stddev` and `stdvar`; the weighted forms scale each row
    // by its sample rate (QE-13).
    AggregateFunction {
        name: "stddev",
        return_type: average,
        weighted: true,
    },
    AggregateFunction {
        name: "stddev_raw",
        return_type: average,
        weighted: false,
    },
    AggregateFunction {
        name: "stdvar",
        return_type: average,
        weighted: true,
    },
    AggregateFunction {
        name: "stdvar_raw",
        return_type: average,
        weighted: false,
    },
    AggregateFunction {
        name: "min",
        return_type: extremum,
        weighted: false,
    },
    AggregateFunction {
        name: "max",
        return_type: extremum,
        weighted: false,
    },
];

impl Registry {
    /// The functions of the core engine (M2).
    #[must_use]
    pub fn builtin() -> Self {
        let mut registry = Registry::default();
        for scalar in SCALARS {
            registry.scalars.insert(scalar.name, scalar.clone());
        }
        for aggregate in AGGREGATES {
            registry
                .aggregates
                .insert(aggregate.name, aggregate.clone());
        }
        registry
    }

    /// The scalar function with this name, if any.
    #[must_use]
    pub fn scalar(&self, name: &str) -> Option<&ScalarFunction> {
        self.scalars.get(name)
    }

    /// The aggregate function with this name, if any.
    #[must_use]
    pub fn aggregate(&self, name: &str) -> Option<&AggregateFunction> {
        self.aggregates.get(name)
    }

    /// Whether `name` is an aggregate function.
    #[must_use]
    pub fn is_aggregate(&self, name: &str) -> bool {
        self.aggregates.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weighted_aggregates_have_raw_variants() {
        let registry = Registry::builtin();
        assert!(registry.aggregate("count").unwrap().weighted);
        assert!(!registry.aggregate("count_raw").unwrap().weighted);
        assert!(registry.aggregate("sum").unwrap().weighted);
        assert!(!registry.aggregate("min").unwrap().weighted);
        assert!(registry.is_aggregate("avg"));
        assert!(!registry.is_aggregate("lower"));
    }

    #[test]
    fn typing_rules_reject_wrong_arguments() {
        let registry = Registry::builtin();
        let sum = registry.aggregate("sum").unwrap();
        assert_eq!(
            (sum.return_type)(&[DataType::Int64]),
            Some(DataType::Float64)
        );
        assert_eq!((sum.return_type)(&[DataType::Utf8]), None);
        let sum_raw = registry.aggregate("sum_raw").unwrap();
        assert_eq!(
            (sum_raw.return_type)(&[DataType::Int64]),
            Some(DataType::Int64)
        );
        assert_eq!(
            (sum_raw.return_type)(&[DataType::Int32]),
            Some(DataType::Int64)
        );
        let bucket = registry.scalar("time_bucket").unwrap();
        assert_eq!(
            (bucket.return_type)(&[
                DataType::Duration(TimeUnit::Nanosecond),
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            ]),
            Some(DataType::Timestamp(TimeUnit::Nanosecond, None))
        );
        assert_eq!((bucket.return_type)(&[DataType::Int64]), None);
        let naming = registry.scalar("prometheus_name").unwrap();
        assert_eq!(
            (naming.return_type)(&[
                DataType::Utf8,
                DataType::Utf8,
                DataType::Utf8,
                DataType::Boolean
            ]),
            Some(DataType::Utf8)
        );
        assert_eq!((naming.return_type)(&[DataType::Utf8]), None);
        assert!(registry.scalar("nope").is_none());
        let contains = registry.scalar("contains").unwrap();
        assert_eq!(
            (contains.return_type)(&[DataType::Utf8, DataType::Utf8]),
            Some(DataType::Boolean)
        );
        assert_eq!((contains.return_type)(&[DataType::Utf8]), None);
        assert!(registry.scalar("has_token").is_some());
        assert!(registry.scalar("regexp_match").is_some());
        assert_eq!(
            (registry.scalar("parse_duration").unwrap().return_type)(&[DataType::Utf8]),
            Some(DataType::Float64)
        );
        assert!(registry.scalar("decolorize").is_some());
        assert!(registry.scalar("parse_bytes").is_some());
    }
}
