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
use arrow::compute::{SortColumn, lexsort_to_indices, take_record_batch};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use tessera_ir::trace::{
    NEST_LEFT, NEST_PARENT, NEST_RIGHT, ROOT_NAME, ROOT_SERVICE, TRACE_DURATION_NS, TRACE_START,
};
use tessera_types::event::{DURATION_NS, NAME, PARENT_SPAN_ID, SPAN_ID, TIMESTAMP, TRACE_ID};

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
    replace_trace_columns(spans, values)
}

/// What a completed trace stores per span, or nothing for a span of a
/// trace that is not complete.
#[derive(Default)]
struct Stored {
    start: Vec<Option<i64>>,
    duration: Vec<Option<i64>>,
    root_name: Vec<Option<String>>,
    root_service: Vec<Option<String>>,
    nested: Vec<Option<(i64, i64, i64)>>,
}

impl Stored {
    fn push(&mut self, shape: Option<&Shape>, span: usize) {
        self.start.push(shape.map(|s| s.start));
        self.duration.push(shape.map(|s| s.duration));
        self.root_name.push(shape.and_then(|s| s.root_name.clone()));
        self.root_service
            .push(shape.and_then(|s| s.root_service.clone()));
        self.nested.push(shape.map(|s| s.nested[span]));
    }
}

/// How many traces `complete` found whole, and how many it did not.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Completion {
    /// Traces that got their columns.
    pub complete: usize,
    /// Traces missing their root or touching the window's edges.
    pub incomplete: usize,
}

/// `rows`, the spans of one partition, sorted by trace then start and
/// given the trace-level columns of the traces that lie whole within
/// the partition's window (QL-14): a trace whose root is among its
/// spans and that starts after `window_start` and ends before
/// `window_end`, strictly, since a span on an edge may have a sibling
/// in the next partition. The spans of the other traces get null
/// columns, and a query numbers them itself (QL-13).
///
/// # Errors
///
/// If the rows lack the span columns or cannot be sorted.
pub fn complete(
    rows: &RecordBatch,
    window_start: i64,
    window_end: i64,
) -> Result<(RecordBatch, Completion), KernelError> {
    let sorted = {
        let by = |name: &str| -> Result<SortColumn, KernelError> {
            Ok(SortColumn {
                values: Arc::clone(
                    rows.column_by_name(name)
                        .ok_or_else(|| KernelError::MissingColumn(name.to_owned()))?,
                ),
                options: None,
            })
        };
        let indices = lexsort_to_indices(&[by(TRACE_ID)?, by(TIMESTAMP)?], None)?;
        take_record_batch(rows, &indices)?
    };
    let ids = fixed(&sorted, TRACE_ID)?;
    let mut stored = Stored::default();
    let mut completion = Completion::default();
    let mut start = 0;
    while start < sorted.num_rows() {
        let mut end = start + 1;
        while end < sorted.num_rows() && ids.value(end) == ids.value(start) {
            end += 1;
        }
        let trace = sorted.slice(start, end - start);
        let shape = shape(&trace)?;
        let whole = shape.has_root
            && shape.start > window_start
            && shape.start.saturating_add(shape.duration) < window_end;
        if whole {
            completion.complete += 1;
        } else {
            completion.incomplete += 1;
        }
        for span in 0..(end - start) {
            stored.push(whole.then_some(&shape), span);
        }
        start = end;
    }
    let pick = |i: usize| -> ArrayRef {
        Arc::new(Int64Array::from(
            stored
                .nested
                .iter()
                .map(|n| n.map(|n| [n.0, n.1, n.2][i]))
                .collect::<Vec<_>>(),
        ))
    };
    let values: [ArrayRef; 7] = [
        Arc::new(TimestampNanosecondArray::from(stored.start.clone())),
        Arc::new(Int64Array::from(stored.duration.clone())),
        Arc::new(StringArray::from(stored.root_name.clone())),
        Arc::new(StringArray::from(stored.root_service.clone())),
        pick(0),
        pick(1),
        pick(2),
    ];
    Ok((replace_trace_columns(&sorted, values)?, completion))
}

/// `spans` with the trace-level columns set to `values`, replacing
/// columns of those names the rows already carry.
fn replace_trace_columns(
    spans: &RecordBatch,
    values: [ArrayRef; 7],
) -> Result<RecordBatch, KernelError> {
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

    /// QL-14: trace 1 is whole (root and child inside the window), trace
    /// 2 has no root, trace 3 starts on the window's first instant.
    #[test]
    fn completion_stores_the_shape_of_the_traces_within_the_window() {
        use tessera_types::event::Scalar;
        use tessera_types::{
            DatasetName, EventBatch, SpanKind, SpanRow, StatusCode, TenantId, Timestamp,
        };

        let span = |trace: u8, id: u8, parent: Option<u8>, at: i64, name: &str| SpanRow {
            timestamp: Timestamp::from_nanos(at),
            observed_timestamp: Timestamp::from_nanos(at),
            event_id: [id; 16],
            trace_id: [trace; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: name.into(),
            span_kind: SpanKind::Server,
            duration_ns: 1_000,
            status_code: StatusCode::Ok,
            status_message: None,
            links: None,
            attributes: std::collections::BTreeMap::from([(
                "resource.service.name".to_owned(),
                Scalar::Text("api".into()),
            )]),
        };
        let rows = EventBatch::from_spans(
            TenantId::DEFAULT,
            DatasetName::new("prod").unwrap(),
            &[
                span(3, 5, None, 100, "edge"),
                span(1, 2, Some(1), 1_500, "child"),
                span(1, 1, None, 1_000, "root"),
                span(2, 4, Some(9), 2_000, "orphan"),
            ],
        )
        .unwrap()
        .rows()
        .clone();
        let (stored, completion) = complete(&rows, 100, 10_000).unwrap();
        assert_eq!(
            completion,
            Completion {
                complete: 1,
                incomplete: 2
            }
        );
        let ids = fixed(&stored, SPAN_ID).unwrap();
        let order: Vec<u8> = (0..stored.num_rows()).map(|i| ids.value(i)[0]).collect();
        assert_eq!(order, [1, 2, 4, 5], "by trace, then by start");
        let longs = |name: &str| {
            stored
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        };
        assert_eq!(longs(NEST_LEFT), [Some(1), Some(2), None, None]);
        assert_eq!(longs(NEST_RIGHT), [Some(4), Some(3), None, None]);
        assert_eq!(longs(NEST_PARENT), [Some(-1), Some(1), None, None]);
        assert_eq!(
            longs(TRACE_DURATION_NS),
            [Some(1_500), Some(1_500), None, None]
        );
        let names = stored
            .column_by_name(ROOT_NAME)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "root");
        assert!(names.is_null(2), "no root: nothing stored");
        let services = stored
            .column_by_name(ROOT_SERVICE)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(services.value(1), "api");
    }
}
