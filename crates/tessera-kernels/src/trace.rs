// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The shape of a trace from its spans: the parent of each span among
//! them, their nested-set numbers from a depth-first walk with children
//! in start order, and the trace-level columns, the start, the duration
//! and the root span's name and service.
//!
//! The trace group computes it per trace at query time (QL-13), and
//! compaction to L3 stores it, for the traces that lie whole within a
//! partition, as columns a query then reads instead of computing
//! (QL-14); both go through here so that the numbers agree (AD-8).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use tessera_ir::trace::{
    NEST_LEFT, NEST_PARENT, NEST_RIGHT, ROOT_NAME, ROOT_SERVICE, TRACE_DURATION_NS, TRACE_START,
};
use tessera_types::event::{DURATION_NS, NAME, PARENT_SPAN_ID, SPAN_ID, TIMESTAMP};

use crate::KernelError;

/// What the spans of one trace say about it.
#[derive(Clone, Debug, PartialEq)]
pub struct Shape {
    /// The index of each span's parent among the trace's spans; none
    /// for a root, or a span whose parent the trace does not hold.
    pub parent: Vec<Option<usize>>,
    /// Each span's nested-set numbers: left, right and the parent's
    /// left, `-1` for a root.
    pub nested: Vec<(i64, i64, i64)>,
    /// The earliest span start.
    pub start: i64,
    /// From the earliest start to the latest end, in nanoseconds.
    pub duration: i64,
    /// The name of the first span without a parent.
    pub root_name: Option<String>,
    /// That span's `resource.service.name`.
    pub root_service: Option<String>,
    /// Whether some span has no parent: a trace without one is still
    /// missing its root.
    pub has_root: bool,
}

/// The shape of the trace whose spans `spans` are, in the order given;
/// the trace group gives them in start order.
///
/// # Errors
///
/// If the rows lack the span id, parent id or timestamp columns.
pub fn shape(spans: &RecordBatch) -> Result<Shape, KernelError> {
    let n = spans.num_rows();
    let ids = fixed(spans, SPAN_ID)?;
    let parents = fixed(spans, PARENT_SPAN_ID)?;
    let by_id: HashMap<&[u8], usize> = (0..n).map(|i| (ids.value(i), i)).collect();
    let parent: Vec<Option<usize>> = (0..n)
        .map(|i| {
            if parents.is_null(i) {
                None
            } else {
                by_id.get(parents.value(i)).copied()
            }
        })
        .collect();
    let timestamps = spans
        .column_by_name(TIMESTAMP)
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
        .ok_or_else(|| KernelError::MissingColumn(TIMESTAMP.to_owned()))?;
    let durations = spans
        .column_by_name(DURATION_NS)
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
    let start = timestamps.values().iter().copied().min().unwrap_or(0);
    let end = (0..n)
        .map(|i| {
            let duration = durations
                .filter(|d| !d.is_null(i))
                .map_or(0, |d| d.value(i));
            timestamps.value(i).saturating_add(duration)
        })
        .max()
        .unwrap_or(start);
    let root = (0..n).find(|&i| parents.is_null(i));
    let names = spans
        .column_by_name(NAME)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let merged = tessera_types::event::attributes_column(spans);
    let overflow = merged.as_ref();
    let root_name = root.and_then(|r| {
        names
            .filter(|a| !a.is_null(r))
            .map(|a| a.value(r).to_owned())
    });
    let root_service = root
        .and_then(|r| overflow.filter(|a| !a.is_null(r)).map(|a| a.value(r)))
        .and_then(service_name);
    Ok(Shape {
        nested: nested_set(&parent),
        parent,
        start,
        duration: end.saturating_sub(start),
        root_name,
        root_service,
        has_root: root.is_some(),
    })
}

/// The nested-set numbers of a forest given by each node's parent: a
/// depth-first walk from every root in index order, children in index
/// order, numbering from one; a node whose ancestry never reaches a
/// root, as in a cycle, is walked from its first node as a root.
#[must_use]
pub fn nested_set(parent: &[Option<usize>]) -> Vec<(i64, i64, i64)> {
    let n = parent.len();
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (span, p) in parent.iter().enumerate() {
        if let Some(p) = p {
            children[*p].push(span);
        }
    }
    let mut numbers = vec![(0i64, 0i64, -1i64); n];
    let mut next = 1i64;
    let roots = (0..n).filter(|&span| parent[span].is_none());
    let unreached = (0..n).filter(|&span| parent[span].is_some());
    for root in roots.chain(unreached) {
        if numbers[root].0 != 0 {
            continue;
        }
        numbers[root].0 = next;
        next += 1;
        let mut stack: Vec<(usize, usize)> = vec![(root, 0)];
        while let Some((span, child)) = stack.last_mut() {
            let span = *span;
            let Some(&next_child) = children[span].get(*child) else {
                numbers[span].1 = next;
                next += 1;
                stack.pop();
                continue;
            };
            *child += 1;
            if numbers[next_child].0 == 0 {
                numbers[next_child].0 = next;
                numbers[next_child].2 = numbers[span].0;
                next += 1;
                stack.push((next_child, 0));
            }
        }
    }
    numbers
}

/// `resource.service.name` from an `_overflow` value.
#[must_use]
pub fn service_name(overflow: &str) -> Option<String> {
    let object: serde_json::Value = serde_json::from_str(overflow).ok()?;
    match object.get("resource.service.name")? {
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// The trace-level columns, in the order they are added.
const TRACE_FIELDS: [(&str, DataType); 7] = [
    (TRACE_START, DataType::Timestamp(TimeUnit::Nanosecond, None)),
    (TRACE_DURATION_NS, DataType::Int64),
    (ROOT_NAME, DataType::Utf8),
    (ROOT_SERVICE, DataType::Utf8),
    (NEST_LEFT, DataType::Int64),
    (NEST_RIGHT, DataType::Int64),
    (NEST_PARENT, DataType::Int64),
];

/// `spans` with the trace-level columns of `shape` added, one value for
/// every row, replacing columns of those names the rows already carry.
///
/// # Errors
///
/// If the batch cannot be built.
pub fn with_trace_columns(spans: &RecordBatch, shape: &Shape) -> Result<RecordBatch, KernelError> {
    let n = spans.num_rows();
    let values: [ArrayRef; 7] = [
        Arc::new(TimestampNanosecondArray::from(vec![shape.start; n])),
        Arc::new(Int64Array::from(vec![shape.duration; n])),
        Arc::new(StringArray::from(vec![shape.root_name.as_deref(); n])),
        Arc::new(StringArray::from(vec![shape.root_service.as_deref(); n])),
        Arc::new(Int64Array::from_iter_values(
            shape.nested.iter().map(|x| x.0),
        )),
        Arc::new(Int64Array::from_iter_values(
            shape.nested.iter().map(|x| x.1),
        )),
        Arc::new(Int64Array::from_iter_values(
            shape.nested.iter().map(|x| x.2),
        )),
    ];
    let mut fields: Vec<Field> = spans
        .schema()
        .fields()
        .iter()
        .filter(|f| !TRACE_FIELDS.iter().any(|(name, _)| name == f.name()))
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns: Vec<ArrayRef> = spans
        .schema()
        .fields()
        .iter()
        .zip(spans.columns())
        .filter(|(f, _)| !TRACE_FIELDS.iter().any(|(name, _)| name == f.name()))
        .map(|(_, c)| Arc::clone(c))
        .collect();
    for ((name, data_type), column) in TRACE_FIELDS.iter().zip(values) {
        fields.push(Field::new(*name, data_type.clone(), true));
        columns.push(column);
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn fixed<'a>(rows: &'a RecordBatch, name: &str) -> Result<&'a FixedSizeBinaryArray, KernelError> {
    rows.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| KernelError::MissingColumn(name.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_set_numbers_a_forest_depth_first() {
        // 0 is the root of 1 and 3; 1 holds 2.
        let numbers = nested_set(&[None, Some(0), Some(1), Some(0)]);
        assert_eq!(numbers, vec![(1, 8, -1), (2, 5, 1), (3, 4, 2), (6, 7, 1)]);
        assert_eq!(
            nested_set(&[Some(1), Some(0)]),
            vec![(1, 4, -1), (2, 3, 1)],
            "a cycle is walked from its first span"
        );
        assert!(nested_set(&[]).is_empty());
    }

    #[test]
    fn the_service_comes_from_the_resource_attribute() {
        assert_eq!(
            service_name(r#"{"resource.service.name":"api","attr.x":1}"#),
            Some("api".into())
        );
        assert_eq!(service_name(r#"{"attr.x":1}"#), None);
        assert_eq!(service_name("not json"), None);
    }
}
