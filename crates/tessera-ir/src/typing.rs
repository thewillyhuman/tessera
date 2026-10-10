// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The type of an expression over an input schema.
//!
//! Types are Arrow's. The rules are those of SQL where they apply:
//! comparisons and logic yield booleans, integer arithmetic stays integer,
//! mixed arithmetic is floating point, and time arithmetic follows
//! timestamps and durations.

use arrow::datatypes::{DataType, Schema, TimeUnit};

use crate::expr::{BinaryOp, Expr};
use crate::function::Registry;

/// Why an expression has no type.
#[derive(Clone, PartialEq, Eq, Debug, thiserror::Error)]
pub enum TypeError {
    /// A column is not in the input.
    #[error("unknown column {0}")]
    UnknownColumn(String),
    /// No scalar function has this name.
    #[error("unknown function {0}")]
    UnknownFunction(String),
    /// No aggregate function has this name.
    #[error("unknown aggregate function {0}")]
    UnknownAggregate(String),
    /// The operands do not fit the operator or function.
    #[error("{0}")]
    Mismatch(String),
}

fn timestamp() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

fn duration() -> DataType {
    DataType::Duration(TimeUnit::Nanosecond)
}

fn is_numeric(t: &DataType) -> bool {
    matches!(t, DataType::Int32 | DataType::Int64 | DataType::Float64)
}

fn is_integer(t: &DataType) -> bool {
    matches!(t, DataType::Int32 | DataType::Int64)
}

/// Whether values of the two types can be compared.
#[must_use]
pub fn comparable(left: &DataType, right: &DataType) -> bool {
    left == right
        || *left == DataType::Null
        || *right == DataType::Null
        || (is_numeric(left) && is_numeric(right))
}

/// The type of `expr` over `input`.
///
/// # Errors
///
/// If a column or function is unknown or the operands do not fit.
pub fn expr_type(expr: &Expr, input: &Schema, registry: &Registry) -> Result<DataType, TypeError> {
    match expr {
        Expr::Column(name) => input
            .field_with_name(name)
            .map(|f| f.data_type().clone())
            .map_err(|_| TypeError::UnknownColumn(name.clone())),
        Expr::Path { data_type, .. } => Ok(data_type.clone()),
        Expr::Literal(value) => Ok(value.data_type().unwrap_or(DataType::Null)),
        Expr::Binary { op, left, right } => {
            let left = expr_type(left, input, registry)?;
            let right = expr_type(right, input, registry)?;
            binary_type(*op, &left, &right)
        }
        Expr::Not(inner) => {
            let t = expr_type(inner, input, registry)?;
            if t == DataType::Boolean || t == DataType::Null {
                Ok(DataType::Boolean)
            } else {
                Err(TypeError::Mismatch(format!("NOT needs a boolean, got {t}")))
            }
        }
        Expr::Negate(inner) => {
            let t = expr_type(inner, input, registry)?;
            if is_integer(&t) {
                Ok(DataType::Int64)
            } else if is_numeric(&t) {
                Ok(t)
            } else {
                Err(TypeError::Mismatch(format!("- needs a number, got {t}")))
            }
        }
        Expr::IsNull { expr, .. } => {
            expr_type(expr, input, registry)?;
            Ok(DataType::Boolean)
        }
        Expr::Like { expr, .. } => {
            let t = expr_type(expr, input, registry)?;
            if t == DataType::Utf8 || t == DataType::Null {
                Ok(DataType::Boolean)
            } else {
                Err(TypeError::Mismatch(format!("LIKE needs text, got {t}")))
            }
        }
        Expr::InList { expr, list, .. } => {
            let t = expr_type(expr, input, registry)?;
            for item in list {
                let item = item.data_type().unwrap_or(DataType::Null);
                if !comparable(&t, &item) {
                    return Err(TypeError::Mismatch(format!(
                        "cannot compare {t} with {item} in IN"
                    )));
                }
            }
            Ok(DataType::Boolean)
        }
        Expr::Call { name, args } => {
            let function = registry
                .scalar(name)
                .ok_or_else(|| TypeError::UnknownFunction(name.clone()))?;
            let types = args
                .iter()
                .map(|a| expr_type(a, input, registry))
                .collect::<Result<Vec<_>, _>>()?;
            (function.return_type)(&types).ok_or_else(|| {
                let shown: Vec<String> = types.iter().map(ToString::to_string).collect();
                TypeError::Mismatch(format!("{name} does not accept ({})", shown.join(", ")))
            })
        }
        Expr::Cast { expr, to } => {
            expr_type(expr, input, registry)?;
            Ok(to.clone())
        }
    }
}

fn binary_type(op: BinaryOp, left: &DataType, right: &DataType) -> Result<DataType, TypeError> {
    let boolean_or_null = |t: &DataType| *t == DataType::Boolean || *t == DataType::Null;
    if op.is_logical() {
        return if boolean_or_null(left) && boolean_or_null(right) {
            Ok(DataType::Boolean)
        } else {
            Err(TypeError::Mismatch(format!(
                "{} needs booleans, got {left} and {right}",
                op.symbol()
            )))
        };
    }
    if op.is_comparison() {
        return if comparable(left, right) {
            Ok(DataType::Boolean)
        } else {
            Err(TypeError::Mismatch(format!(
                "cannot compare {left} with {right}"
            )))
        };
    }
    match (op, left, right) {
        (_, l, r) if is_integer(l) && is_integer(r) => Ok(DataType::Int64),
        (_, l, r) if is_numeric(l) && is_numeric(r) => Ok(DataType::Float64),
        (BinaryOp::Add | BinaryOp::Sub, l, r) if *l == timestamp() && *r == duration() => {
            Ok(timestamp())
        }
        (BinaryOp::Add, l, r) if *l == duration() && *r == timestamp() => Ok(timestamp()),
        (BinaryOp::Sub, l, r) if *l == timestamp() && *r == timestamp() => Ok(duration()),
        (BinaryOp::Add | BinaryOp::Sub, l, r) if *l == duration() && *r == duration() => {
            Ok(duration())
        }
        _ => Err(TypeError::Mismatch(format!(
            "{} does not apply to {left} and {right}",
            op.symbol()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow::datatypes::Field;
    use tessera_types::Timestamp;

    use super::*;
    use crate::expr::Literal;

    fn input() -> Schema {
        Schema::new(vec![
            Field::new("timestamp", timestamp(), false),
            Field::new("severity_number", DataType::Int32, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("sample_rate", DataType::Float64, false),
        ])
    }

    fn typed(expr: &Expr) -> Result<DataType, TypeError> {
        expr_type(expr, &input(), &Registry::builtin())
    }

    #[test]
    fn columns_take_the_input_type() {
        assert_eq!(typed(&Expr::column("body")), Ok(DataType::Utf8));
        assert_eq!(
            typed(&Expr::column("nope")),
            Err(TypeError::UnknownColumn("nope".into()))
        );
    }

    #[test]
    fn comparisons_and_logic_are_boolean() {
        let cmp = Expr::column("sample_rate").binary(BinaryOp::Lt, Expr::literal(1));
        assert_eq!(typed(&cmp), Ok(DataType::Boolean));
        let both = cmp.clone().and(Expr::Not(Box::new(cmp)));
        assert_eq!(typed(&both), Ok(DataType::Boolean));
        let bad = Expr::column("body").binary(BinaryOp::Eq, Expr::literal(1));
        assert!(matches!(typed(&bad), Err(TypeError::Mismatch(_))));
        let null = Expr::column("body").binary(BinaryOp::Eq, Expr::Literal(Literal::Null));
        assert_eq!(typed(&null), Ok(DataType::Boolean));
    }

    #[test]
    fn arithmetic_follows_sql_rules() {
        let int = Expr::literal(1).binary(BinaryOp::Add, Expr::literal(2));
        assert_eq!(typed(&int), Ok(DataType::Int64));
        let widened = Expr::column("severity_number").binary(BinaryOp::Add, Expr::literal(2));
        assert_eq!(typed(&widened), Ok(DataType::Int64));
        assert_eq!(
            typed(&Expr::Negate(Box::new(Expr::column("severity_number")))),
            Ok(DataType::Int64)
        );
        let mixed = Expr::literal(1).binary(BinaryOp::Mul, Expr::column("sample_rate"));
        assert_eq!(typed(&mixed), Ok(DataType::Float64));
        let shifted =
            Expr::column("timestamp").binary(BinaryOp::Sub, Expr::literal(Duration::from_secs(1)));
        assert_eq!(typed(&shifted), Ok(timestamp()));
        let elapsed =
            Expr::column("timestamp").binary(BinaryOp::Sub, Expr::literal(Timestamp::UNIX_EPOCH));
        assert_eq!(typed(&elapsed), Ok(duration()));
        let bad = Expr::column("body").binary(BinaryOp::Add, Expr::literal(1));
        assert!(matches!(typed(&bad), Err(TypeError::Mismatch(_))));
    }

    #[test]
    fn calls_are_typed_by_the_registry() {
        let call = Expr::Call {
            name: "length".into(),
            args: vec![Expr::column("body")],
        };
        assert_eq!(typed(&call), Ok(DataType::Int64));
        let wrong = Expr::Call {
            name: "length".into(),
            args: vec![Expr::column("timestamp")],
        };
        assert!(matches!(typed(&wrong), Err(TypeError::Mismatch(_))));
        let unknown = Expr::Call {
            name: "nope".into(),
            args: vec![],
        };
        assert_eq!(
            typed(&unknown),
            Err(TypeError::UnknownFunction("nope".into()))
        );
    }
}
