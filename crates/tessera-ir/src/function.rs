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
    matches!(data_type, DataType::Int64 | DataType::Float64)
}

fn count(args: &[DataType]) -> Option<DataType> {
    (args.len() <= 1).then_some(DataType::Int64)
}

fn weighted_count(args: &[DataType]) -> Option<DataType> {
    (args.len() <= 1).then_some(DataType::Float64)
}

fn numeric_passthrough(args: &[DataType]) -> Option<DataType> {
    match args {
        [t] if is_numeric(t) => Some(t.clone()),
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

fn coalesce(args: &[DataType]) -> Option<DataType> {
    let first = args.first()?;
    args.iter().all(|t| t == first).then(|| first.clone())
}

impl Registry {
    /// The functions of the core engine (M2).
    #[must_use]
    pub fn builtin() -> Self {
        let mut registry = Registry::default();
        for scalar in [
            ScalarFunction {
                name: "time_bucket",
                return_type: time_bucket,
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
        ] {
            registry.scalars.insert(scalar.name, scalar);
        }
        for aggregate in [
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
                return_type: numeric_passthrough,
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
        ] {
            registry.aggregates.insert(aggregate.name, aggregate);
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
        let bucket = registry.scalar("time_bucket").unwrap();
        assert_eq!(
            (bucket.return_type)(&[
                DataType::Duration(TimeUnit::Nanosecond),
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            ]),
            Some(DataType::Timestamp(TimeUnit::Nanosecond, None))
        );
        assert_eq!((bucket.return_type)(&[DataType::Int64]), None);
        assert!(registry.scalar("nope").is_none());
    }
}
