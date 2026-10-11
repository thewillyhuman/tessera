// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! What the planner knows about a rollup (section 8.7, AD-13): the shape
//! of its query, where its output is and the time it covers, so that an
//! aggregation of the same shape reads the output instead of the raw
//! rows. The evaluator that maintains rollups builds these; the
//! optimizer consumes them.

use std::time::Duration;

use arrow::datatypes::DataType;
use tessera_types::{DatasetName, Kind, TimeRange};

use crate::expr::Expr;
use crate::plan::AggregateExpr;

/// One grouping key of a rollup, with the type its output holds.
#[derive(Clone, Debug, PartialEq)]
pub struct RollupKey {
    /// The output column name, under which the value is stored.
    pub name: String,
    /// The key over the source rows.
    pub expr: Expr,
    /// The type of the key.
    pub data_type: DataType,
}

/// One aggregate of a rollup, with the type its output holds.
#[derive(Clone, Debug, PartialEq)]
pub struct RollupAggregate {
    /// The aggregate over the source rows; its name is the output
    /// column the value is stored under.
    pub aggregate: AggregateExpr,
    /// The type of the aggregate.
    pub data_type: DataType,
}

/// The type a rollup's output holds for a value of `data_type`: the
/// output is stored as event attributes, which are read back as text,
/// 64-bit integers, 64-bit floats, booleans or timestamps, so narrower
/// numbers widen.
#[must_use]
pub fn stored_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => DataType::Int64,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => DataType::Float64,
        other => other.clone(),
    }
}

/// A rollup as the planner sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct RollupEntry {
    /// The rollup's name, also the `name` of its output events.
    pub name: String,
    /// The dataset holding the output, as events.
    pub target: DatasetName,
    /// The source dataset.
    pub dataset: DatasetName,
    /// The source kind.
    pub kind: Kind,
    /// The row filter of the query, over the source schema.
    pub predicate: Option<Expr>,
    /// The grouping keys other than the window.
    pub keys: Vec<RollupKey>,
    /// The window length.
    pub window: Duration,
    /// The aggregates.
    pub aggregates: Vec<RollupAggregate>,
    /// The time the output covers: whole windows from the first one
    /// evaluated to the progress mark.
    pub covered: TimeRange,
}
