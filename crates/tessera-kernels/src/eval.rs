// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Expression evaluation over a record batch.
//!
//! Literals stay scalars until an operation needs a column, so a
//! comparison against a constant never allocates the constant per row.
//! Operands are coerced as SQL does: integers widen, integer and float
//! meet in float, and `NULL` takes the other side's type. Logic is
//! three-valued; a predicate treats an unknown result as false.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Datum, DurationNanosecondArray, Float64Array,
    Int64Array, RecordBatch, Scalar, StringArray, TimestampNanosecondArray, UInt32Array,
    new_null_array,
};
use arrow::compute::kernels::regexp::regexp_is_match_scalar;
use arrow::compute::kernels::zip::zip;
use arrow::compute::kernels::{cmp, comparison, numeric};
use arrow::compute::{and_kleene, cast, is_not_null, is_null, not, or_kleene, take};
use arrow::datatypes::{DataType, TimeUnit};
use tessera_ir::{BinaryOp, Expr, Literal};
use tessera_types::event::{OVERFLOW, SEVERITY_TEXT};

use crate::{KernelError, time, variant};

/// A value during evaluation: a column of the batch's length, or one
/// value standing for every row.
#[derive(Clone, Debug)]
enum Value {
    Column(ArrayRef),
    Constant(ArrayRef),
}

impl Value {
    fn data_type(&self) -> &DataType {
        match self {
            Value::Column(a) | Value::Constant(a) => a.data_type(),
        }
    }

    fn array(&self) -> &ArrayRef {
        match self {
            Value::Column(a) | Value::Constant(a) => a,
        }
    }

    fn is_constant(&self) -> bool {
        matches!(self, Value::Constant(_))
    }

    fn with_array(&self, array: ArrayRef) -> Value {
        match self {
            Value::Column(_) => Value::Column(array),
            Value::Constant(_) => Value::Constant(array),
        }
    }

    fn cast(&self, to: &DataType) -> Result<Value, KernelError> {
        if self.data_type() == to {
            return Ok(self.clone());
        }
        Ok(self.with_array(cast(self.array(), to)?))
    }

    /// The value as a column of `rows` rows.
    fn into_column(self, rows: usize) -> Result<ArrayRef, KernelError> {
        match self {
            Value::Column(a) => Ok(a),
            Value::Constant(a) => {
                let indices = UInt32Array::from(vec![0; rows]);
                Ok(take(&a, &indices, None)?)
            }
        }
    }

    fn datum(&self) -> Box<dyn Datum + '_> {
        match self {
            Value::Column(a) => Box::new(a.as_ref()),
            Value::Constant(a) => Box::new(Scalar::new(a.clone())),
        }
    }
}

/// Evaluates `expr` to a column of `batch`'s length.
///
/// # Errors
///
/// If a column is missing, a function is unknown to the kernels, or the
/// operands do not fit.
pub fn evaluate(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef, KernelError> {
    value(expr, batch)?.into_column(batch.num_rows())
}

/// Evaluates a boolean `expr`, turning unknown (null) into false, so the
/// result can select rows.
///
/// # Errors
///
/// As [`evaluate`], or if the expression is not boolean.
pub fn evaluate_predicate(expr: &Expr, batch: &RecordBatch) -> Result<BooleanArray, KernelError> {
    let array = evaluate(expr, batch)?;
    let booleans = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            KernelError::Unsupported(format!(
                "predicate {expr} is {}, not boolean",
                array.data_type()
            ))
        })?;
    if booleans.null_count() == 0 {
        return Ok(booleans.clone());
    }
    Ok(arrow::compute::prep_null_mask_filter(booleans))
}

fn literal(value: &Literal) -> ArrayRef {
    match value {
        Literal::Null => new_null_array(&DataType::Null, 1),
        Literal::Bool(b) => Arc::new(BooleanArray::from(vec![*b])),
        Literal::Int(i) => Arc::new(Int64Array::from(vec![*i])),
        Literal::Float(x) => Arc::new(Float64Array::from(vec![*x])),
        Literal::Text(s) => Arc::new(StringArray::from(vec![s.as_str()])),
        Literal::Timestamp(t) => Arc::new(TimestampNanosecondArray::from(vec![t.as_nanos()])),
        Literal::Duration(d) => Arc::new(DurationNanosecondArray::from(vec![
            i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        ])),
        Literal::Bytes(b) => Arc::new(BinaryArray::from(vec![b.as_slice()])),
    }
}

fn value(expr: &Expr, batch: &RecordBatch) -> Result<Value, KernelError> {
    match expr {
        Expr::Column(name) => batch
            .column_by_name(name)
            .map(|c| Value::Column(Arc::clone(c)))
            .ok_or_else(|| KernelError::MissingColumn(name.clone())),
        Expr::Path { path, data_type } => {
            let overflow = batch
                .column_by_name(OVERFLOW)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| KernelError::MissingColumn(OVERFLOW.to_owned()))?;
            Ok(Value::Column(variant::extract(overflow, path, data_type)?))
        }
        Expr::Label { name, data_type } => {
            let overflow = batch
                .column_by_name(OVERFLOW)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let severity = batch
                .column_by_name(SEVERITY_TEXT)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            Ok(Value::Column(variant::label(
                overflow,
                severity,
                name,
                data_type,
                batch.num_rows(),
            )?))
        }
        Expr::Literal(lit) => Ok(Value::Constant(literal(lit))),
        Expr::Binary { op, left, right } => {
            let left = value(left, batch)?;
            let right = value(right, batch)?;
            binary(*op, &left, &right)
        }
        Expr::Not(inner) => {
            let inner = value(inner, batch)?.cast(&DataType::Boolean)?;
            let booleans = as_booleans(inner.array())?;
            Ok(inner.with_array(Arc::new(not(booleans)?)))
        }
        Expr::Negate(inner) => {
            let inner = value(inner, batch)?;
            let inner = widen(&inner)?;
            Ok(inner.with_array(numeric::neg(inner.array())?))
        }
        Expr::IsNull { expr, negated } => {
            let inner = value(expr, batch)?;
            let result: ArrayRef = if *negated {
                Arc::new(is_not_null(inner.array())?)
            } else {
                Arc::new(is_null(inner.array())?)
            };
            Ok(inner.with_array(result))
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let text = value(expr, batch)?.cast(&DataType::Utf8)?;
            let pattern = StringArray::from(vec![pattern.as_str()]);
            let pattern = Scalar::new(&pattern);
            let matched = match (negated, case_insensitive) {
                (false, false) => comparison::like(text.datum().as_ref(), &pattern)?,
                (false, true) => comparison::ilike(text.datum().as_ref(), &pattern)?,
                (true, false) => comparison::nlike(text.datum().as_ref(), &pattern)?,
                (true, true) => comparison::nilike(text.datum().as_ref(), &pattern)?,
            };
            Ok(text.with_array(Arc::new(matched)))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let tested = value(expr, batch)?;
            let mut found: Option<Value> = None;
            for item in list {
                let equal = binary(BinaryOp::Eq, &tested, &Value::Constant(literal(item)))?;
                found = Some(match found {
                    None => equal,
                    Some(acc) => binary(BinaryOp::Or, &acc, &equal)?,
                });
            }
            let found = found.unwrap_or_else(|| {
                tested.with_array(Arc::new(BooleanArray::from(vec![
                    false;
                    tested.array().len()
                ])))
            });
            if *negated {
                let booleans = as_booleans(found.array())?;
                Ok(found.with_array(Arc::new(not(booleans)?)))
            } else {
                Ok(found)
            }
        }
        Expr::Call { name, args } => call(name, args, batch),
        Expr::Cast { expr, to } => value(expr, batch)?.cast(to),
    }
}

fn as_booleans(array: &ArrayRef) -> Result<&BooleanArray, KernelError> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            KernelError::Unsupported(format!("expected a boolean, got {}", array.data_type()))
        })
}

fn is_integer(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn is_numeric(t: &DataType) -> bool {
    is_integer(t) || matches!(t, DataType::Float32 | DataType::Float64)
}

/// Integers as 64-bit, floats as 64-bit; anything else unchanged.
fn widen(v: &Value) -> Result<Value, KernelError> {
    match v.data_type() {
        t if is_integer(t) => v.cast(&DataType::Int64),
        DataType::Float32 => v.cast(&DataType::Float64),
        _ => Ok(v.clone()),
    }
}

/// The type two operands meet in, as SQL coerces them.
fn common_type(left: &DataType, right: &DataType) -> Option<DataType> {
    match (left, right) {
        (DataType::Null, other) | (other, DataType::Null) => Some(other.clone()),
        (l, r) if l == r => Some(l.clone()),
        (l, r) if is_integer(l) && is_integer(r) => Some(DataType::Int64),
        (l, r) if is_numeric(l) && is_numeric(r) => Some(DataType::Float64),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, _),
            DataType::Timestamp(TimeUnit::Nanosecond, _),
        ) => Some(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        (DataType::FixedSizeBinary(n), DataType::Binary)
        | (DataType::Binary, DataType::FixedSizeBinary(n)) => Some(DataType::FixedSizeBinary(*n)),
        _ => None,
    }
}

/// Whether a comparison of these operands is decided by their types: a
/// fixed-width binary column against bytes of another width never
/// matches.
fn width_mismatch(left: &Value, right: &Value) -> bool {
    let width = |v: &Value| match v.data_type() {
        DataType::FixedSizeBinary(n) => Some(usize::try_from(*n).unwrap_or(0)),
        DataType::Binary => v
            .array()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .filter(|a| a.len() == 1 && !a.is_null(0))
            .map(|a| a.value(0).len()),
        _ => None,
    };
    matches!((width(left), width(right)), (Some(a), Some(b)) if a != b)
}

fn binary(op: BinaryOp, left: &Value, right: &Value) -> Result<Value, KernelError> {
    let constant = left.is_constant() && right.is_constant();
    let wrap = |array: ArrayRef| {
        if constant {
            Value::Constant(array)
        } else {
            Value::Column(array)
        }
    };
    if op.is_logical() {
        let left = left.cast(&DataType::Boolean)?;
        let right = right.cast(&DataType::Boolean)?;
        let rows = left.array().len().max(right.array().len());
        let l = left.into_column(rows)?;
        let r = right.into_column(rows)?;
        let result = match op {
            BinaryOp::And => and_kleene(as_booleans(&l)?, as_booleans(&r)?)?,
            _ => or_kleene(as_booleans(&l)?, as_booleans(&r)?)?,
        };
        return Ok(wrap(Arc::new(result)));
    }
    if op.is_comparison() {
        if width_mismatch(left, right) {
            let rows = left.array().len().max(right.array().len());
            let equal = op == BinaryOp::Eq;
            return Ok(wrap(Arc::new(BooleanArray::from(vec![!equal; rows]))));
        }
        let target = common_type(left.data_type(), right.data_type()).ok_or_else(|| {
            KernelError::Unsupported(format!(
                "cannot compare {} with {}",
                left.data_type(),
                right.data_type()
            ))
        })?;
        let left = left.cast(&target)?;
        let right = right.cast(&target)?;
        let (l, r) = (left.datum(), right.datum());
        let result = match op {
            BinaryOp::Eq => cmp::eq(l.as_ref(), r.as_ref())?,
            BinaryOp::NotEq => cmp::neq(l.as_ref(), r.as_ref())?,
            BinaryOp::Lt => cmp::lt(l.as_ref(), r.as_ref())?,
            BinaryOp::LtEq => cmp::lt_eq(l.as_ref(), r.as_ref())?,
            BinaryOp::Gt => cmp::gt(l.as_ref(), r.as_ref())?,
            _ => cmp::gt_eq(l.as_ref(), r.as_ref())?,
        };
        return Ok(wrap(Arc::new(result)));
    }
    let (left, right) = arithmetic_operands(left, right)?;
    let (l, r) = (left.datum(), right.datum());
    let result = match op {
        BinaryOp::Add => numeric::add(l.as_ref(), r.as_ref())?,
        BinaryOp::Sub => numeric::sub(l.as_ref(), r.as_ref())?,
        BinaryOp::Mul => numeric::mul(l.as_ref(), r.as_ref())?,
        BinaryOp::Div => numeric::div(l.as_ref(), r.as_ref())?,
        BinaryOp::Rem => numeric::rem(l.as_ref(), r.as_ref())?,
        _ => unreachable!("logical and comparison operators are handled above"),
    };
    Ok(wrap(result))
}

/// Numeric operands meet in their common type; time arithmetic keeps
/// timestamps and durations as they are.
fn arithmetic_operands(left: &Value, right: &Value) -> Result<(Value, Value), KernelError> {
    if is_numeric(left.data_type()) && is_numeric(right.data_type()) {
        let target = common_type(left.data_type(), right.data_type()).unwrap_or(DataType::Float64);
        return Ok((left.cast(&target)?, right.cast(&target)?));
    }
    Ok((left.clone(), right.clone()))
}

fn call(name: &str, args: &[Expr], batch: &RecordBatch) -> Result<Value, KernelError> {
    let values = args
        .iter()
        .map(|a| value(a, batch))
        .collect::<Result<Vec<_>, _>>()?;
    match (name, values.as_slice()) {
        ("time_bucket", [interval, timestamps]) => {
            let interval = interval
                .array()
                .as_any()
                .downcast_ref::<DurationNanosecondArray>()
                .filter(|a| a.len() == 1 && !a.is_null(0))
                .map(|a| a.value(0))
                .ok_or_else(|| {
                    KernelError::Unsupported("time_bucket needs a constant interval".into())
                })?;
            let timestamps = timestamps.cast(&DataType::Timestamp(TimeUnit::Nanosecond, None))?;
            let array = timestamps
                .array()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| KernelError::Unsupported("time_bucket needs timestamps".into()))?;
            Ok(timestamps.with_array(time::time_bucket(interval, array)?))
        }
        ("lower", [text]) => map_text(text, str::to_lowercase),
        ("upper", [text]) => map_text(text, str::to_uppercase),
        ("length", [text]) => {
            let text = text.cast(&DataType::Utf8)?;
            let lengths = arrow::compute::kernels::length::length(text.array())?;
            Ok(text.with_array(cast(&lengths, &DataType::Int64)?))
        }
        ("contains", [text, needle]) => {
            let text = text.cast(&DataType::Utf8)?;
            let needle = needle.cast(&DataType::Utf8)?;
            let matched = comparison::contains(text.datum().as_ref(), needle.datum().as_ref())?;
            Ok(text.with_array(Arc::new(matched)))
        }
        ("has_token", [text, token]) => {
            let token = constant_text(token, "has_token")?.to_lowercase();
            let text = text.cast(&DataType::Utf8)?;
            let strings = text
                .array()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| KernelError::Unsupported("has_token needs text".into()))?;
            let matched: BooleanArray = strings
                .iter()
                .map(|s| s.map(|s| tessera_format::text::has_token(s, &token)))
                .collect();
            Ok(text.with_array(Arc::new(matched)))
        }
        ("regexp_match", [text, pattern]) => {
            let pattern = constant_text(pattern, "regexp_match")?;
            let text = text.cast(&DataType::Utf8)?;
            let strings = text
                .array()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| KernelError::Unsupported("regexp_match needs text".into()))?;
            let matched = regexp_is_match_scalar(strings, &pattern, None)?;
            Ok(text.with_array(Arc::new(matched)))
        }
        ("coalesce", [first, rest @ ..]) => {
            let mut acc = first.clone();
            for next in rest {
                let next = next.cast(acc.data_type())?;
                let rows = acc.array().len().max(next.array().len());
                let a = acc.into_column(rows)?;
                let n = next.into_column(rows)?;
                acc = Value::Column(zip(&is_not_null(&a)?, &a, &n)?);
            }
            Ok(acc)
        }
        _ => Err(KernelError::Unsupported(format!(
            "function {name} with {} arguments",
            values.len()
        ))),
    }
}

/// The text of a constant argument.
fn constant_text(value: &Value, function: &str) -> Result<String, KernelError> {
    value
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .filter(|a| value.is_constant() && a.len() == 1 && !a.is_null(0))
        .map(|a| a.value(0).to_owned())
        .ok_or_else(|| {
            KernelError::Unsupported(format!("{function} needs a constant text pattern"))
        })
}

fn map_text(text: &Value, f: impl Fn(&str) -> String) -> Result<Value, KernelError> {
    let text = text.cast(&DataType::Utf8)?;
    let strings = text
        .array()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| KernelError::Unsupported("expected text".into()))?;
    let mapped: StringArray = strings.iter().map(|s| s.map(&f)).collect();
    Ok(text.with_array(Arc::new(mapped)))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow::array::Int32Array;
    use arrow::datatypes::{Field, Schema};
    use tessera_types::Timestamp;

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("severity_number", DataType::Int32, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("_overflow", DataType::Utf8, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![100, 160, 250])),
                Arc::new(Int32Array::from(vec![Some(9), Some(17), None])),
                Arc::new(StringArray::from(vec![
                    Some("Hello"),
                    Some("timeout"),
                    None,
                ])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"attr.n":1,"attr.m":"GET"}"#),
                    Some(r#"{"attr.n":2}"#),
                    None,
                ])),
            ],
        )
        .unwrap()
    }

    fn booleans(expr: &Expr) -> Vec<bool> {
        evaluate_predicate(expr, &batch())
            .unwrap()
            .iter()
            .map(|b| b.unwrap_or(false))
            .collect()
    }

    #[test]
    fn compares_narrow_columns_with_wide_literals() {
        let expr = Expr::column("severity_number").binary(BinaryOp::GtEq, Expr::literal(17));
        assert_eq!(booleans(&expr), vec![false, true, false]);
        let expr = Expr::column("severity_number").binary(BinaryOp::Lt, Expr::literal(10.5));
        assert_eq!(booleans(&expr), vec![true, false, false]);
    }

    #[test]
    fn logic_is_three_valued_and_predicates_drop_unknowns() {
        let high = Expr::column("severity_number").binary(BinaryOp::GtEq, Expr::literal(17));
        let null_or_high = Expr::IsNull {
            expr: Box::new(Expr::column("severity_number")),
            negated: false,
        }
        .or(high.clone());
        assert_eq!(booleans(&null_or_high), vec![false, true, true]);
        let not_high = Expr::Not(Box::new(high));
        // Row 3 is NULL >= 17 = unknown; NOT unknown is unknown, dropped.
        assert_eq!(booleans(&not_high), vec![true, false, false]);
        let array = evaluate(
            &Expr::Not(Box::new(
                Expr::column("severity_number").binary(BinaryOp::GtEq, Expr::literal(17)),
            )),
            &batch(),
        )
        .unwrap();
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn reads_attribute_paths_as_typed_values() {
        let n = Expr::Path {
            path: "attr.n".into(),
            data_type: DataType::Int64,
        }
        .binary(BinaryOp::Eq, Expr::literal(2));
        assert_eq!(booleans(&n), vec![false, true, false]);
        let m = Expr::Path {
            path: "attr.m".into(),
            data_type: DataType::Utf8,
        }
        .binary(BinaryOp::Eq, Expr::literal("GET"));
        assert_eq!(booleans(&m), vec![true, false, false]);
    }

    #[test]
    fn like_in_and_text_functions() {
        let like = Expr::Like {
            expr: Box::new(Expr::column("body")),
            pattern: "%time%".into(),
            negated: false,
            case_insensitive: false,
        };
        assert_eq!(booleans(&like), vec![false, true, false]);
        let case_insensitive = Expr::Like {
            expr: Box::new(Expr::column("body")),
            pattern: "hello".into(),
            negated: false,
            case_insensitive: true,
        };
        assert_eq!(booleans(&case_insensitive), vec![true, false, false]);
        let in_list = Expr::InList {
            expr: Box::new(Expr::column("severity_number")),
            list: vec![Literal::Int(9), Literal::Int(5)],
            negated: false,
        };
        assert_eq!(booleans(&in_list), vec![true, false, false]);
        let not_in = Expr::InList {
            expr: Box::new(Expr::column("severity_number")),
            list: vec![Literal::Int(9)],
            negated: true,
        };
        assert_eq!(booleans(&not_in), vec![false, true, false]);
        let upper = Expr::Call {
            name: "upper".into(),
            args: vec![Expr::column("body")],
        };
        let out = evaluate(&upper, &batch()).unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "HELLO"
        );
        let length = Expr::Call {
            name: "length".into(),
            args: vec![Expr::column("body")],
        };
        let out = evaluate(&length, &batch()).unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<Int64Array>().unwrap().value(1),
            7
        );
        let coalesce = Expr::Call {
            name: "coalesce".into(),
            args: vec![Expr::column("body"), Expr::literal("-")],
        };
        let out = evaluate(&coalesce, &batch()).unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<StringArray>().unwrap().value(2),
            "-"
        );
    }

    #[test]
    fn arithmetic_and_time_bucket() {
        let doubled = Expr::column("severity_number").binary(BinaryOp::Mul, Expr::literal(2));
        let out = evaluate(&doubled, &batch()).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(1), 34);
        assert!(out.is_null(2));
        let halved = Expr::column("severity_number").binary(BinaryOp::Div, Expr::literal(2.0));
        let out = evaluate(&halved, &batch()).unwrap();
        assert!(
            (out.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                - 4.5)
                .abs()
                < f64::EPSILON
        );
        let bucket = Expr::Call {
            name: "time_bucket".into(),
            args: vec![
                Expr::literal(Duration::from_nanos(100)),
                Expr::column("timestamp"),
            ],
        };
        let out = evaluate(&bucket, &batch()).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(out.values().to_vec(), vec![100, 100, 200]);
        let later =
            Expr::column("timestamp").binary(BinaryOp::Add, Expr::literal(Duration::from_nanos(5)));
        let out = evaluate(&later, &batch()).unwrap();
        assert_eq!(
            out.as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(0),
            105
        );
        let after = Expr::column("timestamp")
            .binary(BinaryOp::Gt, Expr::literal(Timestamp::from_nanos(150)));
        assert_eq!(booleans(&after), vec![false, true, true]);
    }

    #[test]
    fn constants_expand_to_the_batch_length() {
        let out = evaluate(&Expr::literal(7), &batch()).unwrap();
        assert_eq!(out.len(), 3);
        let out = evaluate(
            &Expr::literal(1).binary(BinaryOp::Add, Expr::literal(2)),
            &batch(),
        )
        .unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<Int64Array>().unwrap().value(2),
            3
        );
    }

    #[test]
    fn errors_name_the_problem() {
        assert!(matches!(
            evaluate(&Expr::column("nope"), &batch()),
            Err(KernelError::MissingColumn(c)) if c == "nope"
        ));
        assert!(evaluate_predicate(&Expr::column("body"), &batch()).is_err());
        let call = Expr::Call {
            name: "now".into(),
            args: vec![],
        };
        assert!(matches!(
            evaluate(&call, &batch()),
            Err(KernelError::Unsupported(_))
        ));
    }

    #[test]
    fn text_predicates_and_labels() {
        let contains = Expr::Call {
            name: "contains".into(),
            args: vec![Expr::column("body"), Expr::literal("ime")],
        };
        assert_eq!(booleans(&contains), vec![false, true, false]);
        let token = Expr::Call {
            name: "has_token".into(),
            args: vec![Expr::column("body"), Expr::literal("Hello")],
        };
        assert_eq!(booleans(&token), vec![true, false, false]);
        let partial = Expr::Call {
            name: "has_token".into(),
            args: vec![Expr::column("body"), Expr::literal("hell")],
        };
        assert_eq!(booleans(&partial), vec![false, false, false]);
        let regex = Expr::Call {
            name: "regexp_match".into(),
            args: vec![Expr::column("body"), Expr::literal("^t.*t$")],
        };
        assert_eq!(booleans(&regex), vec![false, true, false]);
        let label = Expr::Label {
            name: "m".into(),
            data_type: DataType::Utf8,
        }
        .binary(BinaryOp::Eq, Expr::literal("GET"));
        assert_eq!(booleans(&label), vec![true, false, false]);
        let number = Expr::Label {
            name: "n".into(),
            data_type: DataType::Int64,
        }
        .binary(BinaryOp::GtEq, Expr::literal(2));
        assert_eq!(booleans(&number), vec![false, true, false]);
    }

    #[test]
    fn byte_literals_compare_with_fixed_binary_columns() {
        let schema = Schema::new(vec![Field::new("id", DataType::FixedSizeBinary(2), true)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(
                arrow::array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![Some(vec![1u8, 2]), Some(vec![3u8, 4]), None].into_iter(),
                    2,
                )
                .unwrap(),
            )],
        )
        .unwrap();
        let hit =
            Expr::column("id").binary(BinaryOp::Eq, Expr::Literal(Literal::Bytes(vec![3, 4])));
        let keep: Vec<bool> = evaluate_predicate(&hit, &batch)
            .unwrap()
            .iter()
            .map(|b| b.unwrap_or(false))
            .collect();
        assert_eq!(keep, vec![false, true, false]);
        let wrong_width =
            Expr::column("id").binary(BinaryOp::Eq, Expr::Literal(Literal::Bytes(vec![3])));
        let keep: Vec<bool> = evaluate_predicate(&wrong_width, &batch)
            .unwrap()
            .iter()
            .map(|b| b.unwrap_or(false))
            .collect();
        assert_eq!(keep, vec![false, false, false]);
    }
}
