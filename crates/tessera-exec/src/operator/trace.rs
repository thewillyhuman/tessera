// SPDX-License-Identifier: Apache-2.0

//! `TraceGroup`: group spans by trace and evaluate a spanset expression
//! on each trace (QL-13). Blocking; the input is kept in trace order
//! against the pool, spilled to local disk past the grant (QE-11), and
//! read back one trace at a time.
//!
//! The input is every span of the candidate traces. Each trace's spans
//! are sorted by start time and given the trace-level columns
//! (`trace_start`, `trace_duration_ns`, `root_name`, `root_service`)
//! and their nested-set numbers (`nest_left`, `nest_right`,
//! `nest_parent`) so that conditions can read them; the tree is the
//! `parent_span_id` links among the trace's own spans. Structural
//! relations are checked pair by pair between the two sides' spans,
//! walking the parent chain for descendant and ancestor.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt32Array, new_null_array,
};
use arrow::compute::{cast, concat_batches, take};
use arrow::datatypes::DataType;
use tessera_ir::trace::{
    MATCHED, NEST_LEFT, NEST_PARENT, NEST_RIGHT, ROOT_NAME, ROOT_SERVICE, SPANSET, SPANSET_LABELS,
    TRACE_DURATION_NS, TRACE_START,
};
use tessera_ir::{
    BinaryOp, Expr, Literal, Relation, SortKey, SpanAggregate, Spanset, Structural,
    trace_group_schema,
};
use tessera_kernels::{evaluate, evaluate_predicate, trace};
use tessera_types::event::{TIMESTAMP, TRACE_ID};

use crate::ExecError;
use crate::batch::Batch;
use crate::memory::MemoryPool;
use crate::operator::sort::{SortedBuffer, emit};
use crate::operator::{Flow, Operator, Sink};

/// Groups spans by trace and evaluates a spanset expression.
pub struct TraceGroup {
    spanset: Spanset,
    memory: Arc<MemoryPool>,
    /// The input in trace order, once its schema is known.
    buffer: Option<SortedBuffer>,
}

impl TraceGroup {
    /// Evaluates `spanset` on every trace of the input.
    #[must_use]
    pub fn new(spanset: Spanset, memory: Arc<MemoryPool>) -> Self {
        TraceGroup {
            spanset,
            memory,
            buffer: None,
        }
    }

    /// Evaluates the traces of the buffer one at a time: the sorted
    /// batches are cut at trace boundaries, the trace that straddles
    /// two batches carried into the next.
    fn emit_traces(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        let Some(buffer) = self.buffer.as_mut() else {
            return Ok(());
        };
        let mut carried: Option<RecordBatch> = None;
        for batch in buffer.drain()? {
            let batch = batch?;
            let spans = match carried.take() {
                Some(carry) => concat_batches(&carry.schema(), &[carry, batch])?,
                None => batch,
            };
            let n = spans.num_rows();
            if n == 0 {
                continue;
            }
            let ids = fixed(&spans, TRACE_ID)?;
            let mut last_start = n - 1;
            while last_start > 0 && ids.value(last_start - 1) == ids.value(n - 1) {
                last_start -= 1;
            }
            if last_start > 0 {
                let rows = self.evaluate_all(&spans.slice(0, last_start))?;
                emit(&rows, out)?;
            }
            carried = Some(spans.slice(last_start, n - last_start));
        }
        if let Some(last) = carried {
            let rows = self.evaluate_all(&last)?;
            emit(&rows, out)?;
        }
        Ok(())
    }

    /// The output rows for every trace of `spans`, which are sorted by
    /// trace id then start time.
    fn evaluate_all(&self, spans: &RecordBatch) -> Result<RecordBatch, ExecError> {
        let ids = fixed(spans, TRACE_ID)?;
        let mut output = Output::default();
        let mut start = 0;
        while start < spans.num_rows() {
            let id = ids.value(start);
            let mut end = start + 1;
            while end < spans.num_rows() && ids.value(end) == id {
                end += 1;
            }
            let trace = Trace::new(&spans.slice(start, end - start))?;
            let every_span: BTreeSet<usize> = (0..trace.rows.num_rows()).collect();
            for (index, group) in evaluate_spanset(&self.spanset, &trace, &every_span)?
                .into_iter()
                .enumerate()
            {
                output.add(&trace, start, index, &group);
            }
            start = end;
        }
        output.build(spans)
    }
}

impl Operator for TraceGroup {
    fn push(&mut self, input: Batch, _: &mut dyn Sink) -> Result<Flow, ExecError> {
        let rows = input.materialize()?;
        let buffer = self.buffer.get_or_insert_with(|| {
            // Trace id then start time, so a trace's spans are
            // contiguous and in order.
            SortedBuffer::new(
                vec![
                    SortKey::ascending(Expr::column(TRACE_ID)),
                    SortKey::ascending(Expr::column(TIMESTAMP)),
                ],
                rows.schema(),
                Arc::clone(&self.memory),
            )
        });
        buffer.push(rows)?;
        Ok(Flow::Continue)
    }

    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        let result = self.emit_traces(out);
        if let Some(buffer) = self.buffer.as_mut() {
            buffer.release();
        }
        result
    }
}

fn fixed<'a>(rows: &'a RecordBatch, name: &str) -> Result<&'a FixedSizeBinaryArray, ExecError> {
    rows.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| ExecError::Unsupported(format!("trace group input has no {name}")))
}

#[cfg(test)]
fn text<'a>(rows: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    rows.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
}

/// The spans of one trace with the trace-level columns, and its tree.
struct Trace {
    /// The spans in start order, plus the trace-level columns.
    rows: RecordBatch,
    /// The index of each span's parent among the trace's spans.
    parent: Vec<Option<usize>>,
    /// Each span's nested-set numbers: left, right and parent.
    nested: Vec<(i64, i64, i64)>,
    /// The trace's start.
    start: i64,
    /// The trace's duration in nanoseconds.
    duration: i64,
    /// The root span's name.
    root_name: Option<String>,
    /// The root span's `resource.service.name`.
    root_service: Option<String>,
}

impl Trace {
    /// The trace of `spans`: its shape as a completed trace stored it
    /// (QL-14) when every span carries the stored columns, computed
    /// from the spans otherwise (QL-13).
    fn new(spans: &RecordBatch) -> Result<Self, ExecError> {
        let (shape, rows) = if let Some(stored) = trace::Shape::from_stored(spans)? {
            (stored, spans.clone())
        } else {
            let shape = trace::shape(spans)?;
            let rows = trace::with_trace_columns(spans, &shape)?;
            (shape, rows)
        };
        Ok(Trace {
            rows,
            parent: shape.parent,
            nested: shape.nested,
            start: shape.start,
            duration: shape.duration,
            root_name: shape.root_name,
            root_service: shape.root_service,
        })
    }

    /// Whether `ancestor` is a proper ancestor of `span`.
    fn is_ancestor(&self, ancestor: usize, span: usize) -> bool {
        let mut current = self.parent[span];
        // A cycle cannot be longer than the trace.
        for _ in 0..self.parent.len() {
            match current {
                None => return false,
                Some(p) if p == ancestor => return true,
                Some(p) => current = self.parent[p],
            }
        }
        false
    }

    /// Whether `right` stands in `relation` to `left`.
    fn related(&self, relation: Relation, left: usize, right: usize) -> bool {
        match relation {
            Relation::Descendant => self.is_ancestor(left, right),
            Relation::Child => self.parent[right] == Some(left),
            Relation::Sibling => {
                left != right
                    && self.parent[right].is_some()
                    && self.parent[right] == self.parent[left]
            }
            Relation::Ancestor => self.is_ancestor(right, left),
            Relation::Parent => self.parent[left] == Some(right),
        }
    }
}

/// One spanset of a trace: the spans, as indices among the trace's
/// spans, and the labels `by` gave it.
#[derive(Clone, Debug, Default, PartialEq)]
struct Group {
    spans: BTreeSet<usize>,
    labels: BTreeMap<String, String>,
}

/// Every span of `groups`.
fn all_spans(groups: &[Group]) -> BTreeSet<usize> {
    groups
        .iter()
        .flat_map(|g| g.spans.iter().copied())
        .collect()
}

/// One group of `spans`, or none when there are none.
fn single(spans: BTreeSet<usize>) -> Vec<Group> {
    if spans.is_empty() {
        Vec::new()
    } else {
        vec![Group {
            spans,
            labels: BTreeMap::new(),
        }]
    }
}

/// The spansets `spanset` yields on `trace`, from the spans in `input`:
/// every span of the trace at the top, the spans of one spanset of the
/// left side under a pipe.
fn evaluate_spanset(
    spanset: &Spanset,
    trace: &Trace,
    input: &BTreeSet<usize>,
) -> Result<Vec<Group>, ExecError> {
    Ok(match spanset {
        Spanset::Filter(None) => single(input.clone()),
        Spanset::Filter(Some(condition)) => {
            let keep = evaluate_predicate(condition, &trace.rows)?;
            single(
                input
                    .iter()
                    .copied()
                    .filter(|&i| keep.is_valid(i) && keep.value(i))
                    .collect(),
            )
        }
        Spanset::Pipe(left, right) => {
            let mut piped = Vec::new();
            for group in evaluate_spanset(left, trace, input)? {
                for mut inner in evaluate_spanset(right, trace, &group.spans)? {
                    for (label, value) in &group.labels {
                        inner
                            .labels
                            .entry(label.clone())
                            .or_insert_with(|| value.clone());
                    }
                    piped.push(inner);
                }
            }
            piped
        }
        Spanset::And(left, right) => {
            let (l, r) = (
                evaluate_spanset(left, trace, input)?,
                evaluate_spanset(right, trace, input)?,
            );
            if l.is_empty() || r.is_empty() {
                Vec::new()
            } else {
                single(all_spans(&l).union(&all_spans(&r)).copied().collect())
            }
        }
        Spanset::Or(left, right) => {
            let (l, r) = (
                evaluate_spanset(left, trace, input)?,
                evaluate_spanset(right, trace, input)?,
            );
            single(all_spans(&l).union(&all_spans(&r)).copied().collect())
        }
        Spanset::Structural { left, op, right } => {
            let (l, r) = (
                evaluate_spanset(left, trace, input)?,
                evaluate_spanset(right, trace, input)?,
            );
            if l.is_empty() || r.is_empty() {
                Vec::new()
            } else {
                single(related_spans(trace, *op, &all_spans(&l), &all_spans(&r)))
            }
        }
        Spanset::AggregateFilter {
            input: inner,
            function,
            arg,
            op,
            value,
        } => {
            let groups = evaluate_spanset(inner, trace, input)?;
            let values = match arg {
                Some(arg) => Some(floats(arg, &trace.rows)?),
                None => None,
            };
            let threshold = number(value)?;
            groups
                .into_iter()
                .filter(|group| {
                    statistic(*function, &group.spans, values.as_ref())
                        .is_some_and(|stat| compare(*op, stat, threshold))
                })
                .collect()
        }
        Spanset::By {
            input: inner,
            key,
            label,
        } => {
            let groups = evaluate_spanset(inner, trace, input)?;
            let keys = cast(&evaluate(key, &trace.rows)?, &DataType::Utf8)?;
            let keys = keys
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ExecError::Unsupported("by key is not text".into()))?;
            groups
                .into_iter()
                .flat_map(|group| split_by(&group, keys, label))
                .collect()
        }
        Spanset::Coalesce(inner) => single(all_spans(&evaluate_spanset(inner, trace, input)?)),
    })
}

/// The spans a structural operator keeps: the `right` spans that stand
/// (or, negated, do not stand) in the relation to some `left` span, plus
/// the left spans that took part for the union forms.
fn related_spans(
    trace: &Trace,
    op: Structural,
    left_spans: &BTreeSet<usize>,
    right_spans: &BTreeSet<usize>,
) -> BTreeSet<usize> {
    let mut keep = BTreeSet::new();
    for &right in right_spans {
        let mut matched = false;
        for &left in left_spans {
            if trace.related(op.relation, left, right) {
                matched = true;
                if op.union {
                    keep.insert(left);
                } else {
                    break;
                }
            }
        }
        if matched != op.negated {
            keep.insert(right);
        }
    }
    keep
}

/// `group` split by the value of `keys`, each part labelled `label`;
/// the spans without a value come last, unlabelled.
fn split_by(group: &Group, keys: &StringArray, label: &str) -> Vec<Group> {
    let mut by_value: BTreeMap<Option<String>, BTreeSet<usize>> = BTreeMap::new();
    for &span in &group.spans {
        let value = (!keys.is_null(span)).then(|| keys.value(span).to_owned());
        by_value.entry(value).or_default().insert(span);
    }
    let (without, with): (Vec<_>, Vec<_>) =
        by_value.into_iter().partition(|(value, _)| value.is_none());
    with.into_iter()
        .chain(without)
        .map(|(value, spans)| {
            let mut labels = group.labels.clone();
            if let Some(value) = value {
                labels.insert(label.to_owned(), value);
            }
            Group { spans, labels }
        })
        .collect()
}

/// `expr` over `rows` as floats.
fn floats(expr: &Expr, rows: &RecordBatch) -> Result<Float64Array, ExecError> {
    let array = cast(&evaluate(expr, rows)?, &DataType::Float64)?;
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .cloned()
        .ok_or_else(|| ExecError::Unsupported(format!("{expr} is not numeric")))
}

/// A threshold as a number; durations in nanoseconds.
fn number(value: &Literal) -> Result<f64, ExecError> {
    #[allow(clippy::cast_precision_loss)]
    Ok(match value {
        Literal::Int(i) => *i as f64,
        Literal::Float(f) => *f,
        Literal::Duration(d) => d.as_nanos() as f64,
        other => {
            return Err(ExecError::Unsupported(format!(
                "cannot compare a span statistic with {other}"
            )));
        }
    })
}

/// `function` over the `values` of `spans`; `None` when there is nothing
/// to aggregate.
#[allow(clippy::cast_precision_loss)]
fn statistic(
    function: SpanAggregate,
    spans: &BTreeSet<usize>,
    values: Option<&Float64Array>,
) -> Option<f64> {
    if function == SpanAggregate::Count {
        return Some(spans.len() as f64);
    }
    let values = values?;
    let present: Vec<f64> = spans
        .iter()
        .filter(|&&i| !values.is_null(i))
        .map(|&i| values.value(i))
        .collect();
    if present.is_empty() {
        return None;
    }
    Some(match function {
        SpanAggregate::Count => unreachable!("handled above"),
        SpanAggregate::Sum => present.iter().sum(),
        SpanAggregate::Avg => present.iter().sum::<f64>() / present.len() as f64,
        SpanAggregate::Min => present.iter().copied().fold(f64::INFINITY, f64::min),
        SpanAggregate::Max => present.iter().copied().fold(f64::NEG_INFINITY, f64::max),
    })
}

fn compare(op: BinaryOp, left: f64, right: f64) -> bool {
    match op {
        BinaryOp::Eq => (left - right).abs() < f64::EPSILON,
        BinaryOp::NotEq => (left - right).abs() >= f64::EPSILON,
        BinaryOp::Lt => left < right,
        BinaryOp::LtEq => left <= right,
        BinaryOp::Gt => left > right,
        BinaryOp::GtEq => left >= right,
        _ => false,
    }
}

/// The output rows under construction: for every matched span, its row
/// in the sorted input and its trace's and spanset's columns.
#[derive(Default)]
struct Output {
    rows: Vec<u32>,
    trace_start: Vec<i64>,
    trace_duration: Vec<i64>,
    root_name: Vec<Option<String>>,
    root_service: Vec<Option<String>>,
    nested: Vec<(i64, i64, i64)>,
    spanset: Vec<i64>,
    labels: Vec<Option<String>>,
    matched: Vec<i64>,
}

impl Output {
    /// Adds the spans of `group`, the `index`-th spanset of `trace`,
    /// whose spans start at row `offset` of the sorted input.
    fn add(&mut self, trace: &Trace, offset: usize, index: usize, group: &Group) {
        let labels = (!group.labels.is_empty())
            .then(|| serde_json::to_string(&group.labels).unwrap_or_default());
        for &span in &group.spans {
            self.rows
                .push(u32::try_from(offset + span).unwrap_or(u32::MAX));
            self.trace_start.push(trace.start);
            self.trace_duration.push(trace.duration);
            self.root_name.push(trace.root_name.clone());
            self.root_service.push(trace.root_service.clone());
            self.nested.push(trace.nested[span]);
            self.spanset.push(i64::try_from(index).unwrap_or(i64::MAX));
            self.labels.push(labels.clone());
            self.matched
                .push(i64::try_from(group.spans.len()).unwrap_or(i64::MAX));
        }
    }

    /// The rows in the trace group schema, taking span columns from the
    /// sorted input.
    fn build(self, spans: &RecordBatch) -> Result<RecordBatch, ExecError> {
        let schema = trace_group_schema();
        let indices = UInt32Array::from(self.rows);
        let n = indices.len();
        let columns = schema
            .fields()
            .iter()
            .map(|field| -> Result<ArrayRef, ExecError> {
                Ok(match field.name().as_str() {
                    TRACE_START => {
                        Arc::new(TimestampNanosecondArray::from(self.trace_start.clone()))
                    }
                    TRACE_DURATION_NS => Arc::new(Int64Array::from(self.trace_duration.clone())),
                    ROOT_NAME => Arc::new(StringArray::from(self.root_name.clone())),
                    ROOT_SERVICE => Arc::new(StringArray::from(self.root_service.clone())),
                    NEST_LEFT => Arc::new(Int64Array::from_iter_values(
                        self.nested.iter().map(|n| n.0),
                    )),
                    NEST_RIGHT => Arc::new(Int64Array::from_iter_values(
                        self.nested.iter().map(|n| n.1),
                    )),
                    NEST_PARENT => Arc::new(Int64Array::from_iter_values(
                        self.nested.iter().map(|n| n.2),
                    )),
                    SPANSET => Arc::new(Int64Array::from(self.spanset.clone())),
                    SPANSET_LABELS => Arc::new(StringArray::from(self.labels.clone())),
                    MATCHED => Arc::new(Int64Array::from(self.matched.clone())),
                    name => match spans.column_by_name(name) {
                        Some(column) => take(column, &indices, None)?,
                        None => new_null_array(field.data_type(), n),
                    },
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow::compute::take_record_batch;
    use tessera_ir::trace_group_schema;
    use tessera_types::event::{DURATION_NS, NAME, SPAN_ID, STATUS_CODE, Scalar};
    use tessera_types::{
        DatasetName, EventBatch, SpanKind, SpanRow, StatusCode, TenantId, Timestamp,
    };

    use super::*;
    use crate::operator::testing::run;

    /// Two traces. Trace 1: `GET /` (api, 100ns) with children `db`
    /// (db, error, 40ns) and `auth` (auth, 10ns); `db` has child
    /// `cache` (db, 5ns). Trace 2: `GET /` (api) with child `db` (ok).
    fn spans() -> RecordBatch {
        let span = |trace: u8,
                    id: u8,
                    parent: Option<u8>,
                    name: &str,
                    service: &str,
                    start: i64,
                    duration: i64,
                    error: bool| SpanRow {
            timestamp: Timestamp::from_nanos(1_000 + start),
            observed_timestamp: Timestamp::from_nanos(2_000),
            event_id: [id; 16],
            trace_id: [trace; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: name.into(),
            span_kind: SpanKind::Server,
            duration_ns: duration,
            status_code: if error {
                StatusCode::Error
            } else {
                StatusCode::Ok
            },
            status_message: None,
            links: None,
            attributes: BTreeMap::from([(
                "resource.service.name".to_owned(),
                Scalar::Text(service.into()),
            )]),
        };
        let rows = vec![
            span(2, 6, Some(5), "db", "db", 5, 10, false),
            span(1, 3, Some(2), "cache", "db", 20, 5, false),
            span(1, 1, None, "GET /", "api", 0, 100, false),
            span(1, 2, Some(1), "db", "db", 10, 40, true),
            span(2, 5, None, "GET /", "api", 0, 50, false),
            span(1, 4, Some(1), "auth", "auth", 60, 10, false),
        ];
        EventBatch::from_spans(TenantId::DEFAULT, DatasetName::new("prod").unwrap(), &rows)
            .unwrap()
            .rows()
            .clone()
    }

    fn filter(condition: Expr) -> Spanset {
        Spanset::Filter(Some(condition))
    }

    fn name_is(name: &str) -> Spanset {
        filter(Expr::column(NAME).binary(BinaryOp::Eq, Expr::literal(name)))
    }

    fn structural(
        left: Spanset,
        relation: Relation,
        negated: bool,
        union: bool,
        right: Spanset,
    ) -> Spanset {
        Spanset::Structural {
            left: Box::new(left),
            op: Structural {
                relation,
                negated,
                union,
            },
            right: Box::new(right),
        }
    }

    /// `(trace, span id)` of every output row, with the spanset index.
    fn matched(spanset: Spanset) -> Vec<(u8, u8, i64)> {
        let mut operator = TraceGroup::new(spanset, MemoryPool::unbounded());
        let out = run(&mut operator, vec![spans()]);
        let mut rows = Vec::new();
        for batch in &out {
            assert_eq!(batch.schema(), trace_group_schema());
            let traces = fixed(batch, TRACE_ID).unwrap();
            let ids = fixed(batch, SPAN_ID).unwrap();
            let sets = batch
                .column_by_name(SPANSET)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((traces.value(i)[0], ids.value(i)[0], sets.value(i)));
            }
        }
        rows
    }

    /// QE-11: past the grant the spans go to disk in trace order and
    /// come back one trace at a time, with the same result.
    #[test]
    fn spills_spans_and_still_groups_whole_traces() {
        let all = spans();
        let one_each: Vec<RecordBatch> = (0..all.num_rows()).map(|i| all.slice(i, 1)).collect();
        let dir = std::env::temp_dir().join(format!("tessera-trace-spill-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let one = one_each[0].get_array_memory_size();
        let pool = MemoryPool::with_spill_dir(one * 2, Some(dir.clone()));
        let mut spilling = TraceGroup::new(name_is("db"), Arc::clone(&pool));
        let spilled = run(&mut spilling, one_each.clone());
        let mut plain = TraceGroup::new(name_is("db"), MemoryPool::unbounded());
        let expected = run(&mut plain, vec![all]);
        let rows_of = |batches: &[RecordBatch]| -> Vec<(u8, u8, i64)> {
            let mut rows = Vec::new();
            for batch in batches {
                let traces = fixed(batch, TRACE_ID).unwrap();
                let ids = fixed(batch, SPAN_ID).unwrap();
                let sets = batch
                    .column_by_name(SPANSET)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    rows.push((traces.value(i)[0], ids.value(i)[0], sets.value(i)));
                }
            }
            rows
        };
        let mut spilled_rows = rows_of(&spilled);
        let mut expected_rows = rows_of(&expected);
        spilled_rows.sort_unstable();
        expected_rows.sort_unstable();
        assert_eq!(spilled_rows, expected_rows);
        assert_eq!(spilled_rows.len(), 2, "db in both traces");
        assert!(pool.spilled_bytes() > 0, "runs went to disk");
        assert_eq!(pool.used(), 0);
        assert!(std::fs::read_dir(&dir).unwrap().next().is_none());
        std::fs::remove_dir(&dir).unwrap();

        let mut refused = TraceGroup::new(name_is("db"), MemoryPool::new(one));
        let mut sink = crate::operator::CollectSink::new();
        refused
            .push(Batch::new(one_each[0].clone()), &mut sink)
            .unwrap();
        let error = refused
            .push(Batch::new(one_each[1].clone()), &mut sink)
            .unwrap_err();
        assert!(
            matches!(error, ExecError::Memory(_)),
            "no directory, no spill: {error}"
        );
    }

    /// QL-14: the numbers of a depth-first walk, children in start
    /// order, restarting at one for every trace.
    #[test]
    fn nested_set_numbers_follow_a_depth_first_walk_of_each_trace() {
        let mut operator = TraceGroup::new(Spanset::Filter(None), MemoryPool::unbounded());
        let out = run(&mut operator, vec![spans()]);
        let batch = concat_batches(&trace_group_schema(), &out).unwrap();
        let ids = fixed(&batch, SPAN_ID).unwrap();
        let longs = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        };
        let (left, right, parent) = (longs(NEST_LEFT), longs(NEST_RIGHT), longs(NEST_PARENT));
        let mut numbered: Vec<(u8, i64, i64, i64)> = (0..batch.num_rows())
            .map(|i| (ids.value(i)[0], left[i], right[i], parent[i]))
            .collect();
        numbered.sort_unstable();
        assert_eq!(
            numbered,
            vec![
                (1, 1, 8, -1), // GET / of trace 1: the root
                (2, 2, 5, 1),  // db, its first child, holding cache
                (3, 3, 4, 2),  // cache, a leaf under db
                (4, 6, 7, 1),  // auth, the second child
                (5, 1, 4, -1), // GET / of trace 2
                (6, 2, 3, 1),  // db under it
            ]
        );
    }

    #[test]
    fn a_filter_keeps_the_matching_spans_with_their_trace_columns() {
        let mut operator = TraceGroup::new(name_is("db"), MemoryPool::unbounded());
        let out = run(&mut operator, vec![spans()]);
        let batch = concat_batches(&trace_group_schema(), &out).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let root_name = text(&batch, ROOT_NAME).unwrap();
        let root_service = text(&batch, ROOT_SERVICE).unwrap();
        let duration = batch
            .column_by_name(TRACE_DURATION_NS)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column_by_name(MATCHED)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..2 {
            assert_eq!(root_name.value(i), "GET /");
            assert_eq!(root_service.value(i), "api");
            assert_eq!(counts.value(i), 1);
        }
        assert_eq!(duration.value(0), 100, "trace 1 spans 0..100");
        assert_eq!(duration.value(1), 50, "trace 2 spans 0..50");
        assert!(batch.column_by_name(SPANSET_LABELS).unwrap().is_null(0));
        assert_eq!(matched(Spanset::Filter(None)).len(), 6);
        assert!(matched(name_is("nothing")).is_empty());
    }

    #[test]
    fn structural_operators_follow_the_tree() {
        let error = filter(Expr::column(STATUS_CODE).binary(BinaryOp::Eq, Expr::literal("error")));
        assert_eq!(
            matched(structural(
                name_is("GET /"),
                Relation::Descendant,
                false,
                false,
                error.clone()
            )),
            vec![(1, 2, 0)]
        );
        assert_eq!(
            matched(structural(
                name_is("GET /"),
                Relation::Descendant,
                false,
                false,
                name_is("cache")
            )),
            vec![(1, 3, 0)],
            "through a span neither side matched"
        );
        assert_eq!(
            matched(structural(
                name_is("GET /"),
                Relation::Child,
                false,
                false,
                name_is("cache")
            )),
            vec![]
        );
        assert_eq!(
            matched(structural(
                name_is("db"),
                Relation::Parent,
                false,
                false,
                Spanset::Filter(None)
            )),
            vec![(1, 1, 0), (2, 5, 0)]
        );
        assert_eq!(
            matched(structural(
                name_is("cache"),
                Relation::Ancestor,
                false,
                false,
                Spanset::Filter(None)
            )),
            vec![(1, 1, 0), (1, 2, 0)]
        );
        assert_eq!(
            matched(structural(
                name_is("db"),
                Relation::Sibling,
                false,
                false,
                Spanset::Filter(None)
            )),
            vec![(1, 4, 0)]
        );
        assert_eq!(
            matched(structural(
                name_is("GET /"),
                Relation::Child,
                true,
                false,
                Spanset::Filter(None)
            )),
            vec![(1, 1, 0), (1, 3, 0), (2, 5, 0)],
            "negated: the roots and the grandchild are not children of a root"
        );
        assert_eq!(
            matched(structural(
                name_is("GET /"),
                Relation::Child,
                false,
                true,
                error
            )),
            vec![(1, 1, 0), (1, 2, 0)],
            "union: the parent that took part comes along"
        );
        assert!(
            matched(structural(
                name_is("nothing"),
                Relation::Child,
                true,
                false,
                Spanset::Filter(None)
            ))
            .is_empty(),
            "both sides must match in the trace"
        );
    }

    #[test]
    fn logical_operators_combine_the_sides_per_trace() {
        let error = filter(Expr::column(STATUS_CODE).binary(BinaryOp::Eq, Expr::literal("error")));
        assert_eq!(
            matched(Spanset::And(
                Box::new(name_is("auth")),
                Box::new(error.clone())
            )),
            vec![(1, 2, 0), (1, 4, 0)]
        );
        assert_eq!(
            matched(Spanset::Or(Box::new(name_is("auth")), Box::new(error))),
            vec![(1, 2, 0), (1, 4, 0)]
        );
        assert_eq!(
            matched(Spanset::Or(
                Box::new(name_is("auth")),
                Box::new(name_is("db"))
            )),
            vec![(1, 2, 0), (1, 4, 0), (2, 6, 0)]
        );
        let trace_level = filter(
            Expr::column(ROOT_NAME)
                .binary(BinaryOp::Eq, Expr::literal("GET /"))
                .and(Expr::column(TRACE_DURATION_NS).binary(BinaryOp::Gt, Expr::literal(60))),
        );
        assert_eq!(matched(trace_level).len(), 4, "every span of trace 1");
    }

    #[test]
    fn aggregate_filters_keep_spansets_by_their_statistics() {
        let count = |op, n| Spanset::AggregateFilter {
            input: Box::new(Spanset::Filter(None)),
            function: SpanAggregate::Count,
            arg: None,
            op,
            value: Literal::Int(n),
        };
        assert_eq!(matched(count(BinaryOp::Gt, 2)).len(), 4);
        assert_eq!(matched(count(BinaryOp::Eq, 2)).len(), 2);
        let avg = Spanset::AggregateFilter {
            input: Box::new(name_is("db")),
            function: SpanAggregate::Avg,
            arg: Some(Expr::column(DURATION_NS)),
            op: BinaryOp::GtEq,
            value: Literal::Duration(std::time::Duration::from_nanos(40)),
        };
        assert_eq!(matched(avg), vec![(1, 2, 0)]);
        let max = Spanset::AggregateFilter {
            input: Box::new(Spanset::Filter(None)),
            function: SpanAggregate::Max,
            arg: Some(Expr::column(DURATION_NS)),
            op: BinaryOp::Lt,
            value: Literal::Int(60),
        };
        assert_eq!(matched(max).len(), 2, "trace 2 only");
    }

    #[test]
    fn by_splits_spansets_and_labels_them() {
        let by = Spanset::By {
            input: Box::new(Spanset::Filter(None)),
            key: Expr::Path {
                path: "resource.service.name".into(),
                data_type: DataType::Utf8,
            },
            label: "by(resource.service.name)".into(),
        };
        let rows = matched(by.clone());
        assert_eq!(
            rows,
            vec![
                (1, 1, 0),
                (1, 4, 1),
                (1, 2, 2),
                (1, 3, 2),
                (2, 5, 0),
                (2, 6, 1)
            ],
            "api, auth, db in trace 1; api, db in trace 2"
        );
        let mut operator = TraceGroup::new(by.clone(), MemoryPool::unbounded());
        let out = run(&mut operator, vec![spans()]);
        let batch = concat_batches(&trace_group_schema(), &out).unwrap();
        let labels = text(&batch, SPANSET_LABELS).unwrap();
        assert_eq!(labels.value(0), r#"{"by(resource.service.name)":"api"}"#);
        let matched_counts = batch
            .column_by_name(MATCHED)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(matched_counts.value(2), 2, "the db spanset has two spans");
        assert_eq!(matched(Spanset::Coalesce(Box::new(by))).len(), 6);
    }

    #[test]
    fn a_pipe_filters_the_spans_of_each_spanset() {
        let error = filter(Expr::column(STATUS_CODE).binary(BinaryOp::Eq, Expr::literal("error")));
        assert_eq!(
            matched(Spanset::Pipe(
                Box::new(name_is("db")),
                Box::new(error.clone())
            )),
            vec![(1, 2, 0)]
        );
        let by_service = Spanset::By {
            input: Box::new(Spanset::Filter(None)),
            key: Expr::Path {
                path: "resource.service.name".into(),
                data_type: DataType::Utf8,
            },
            label: "by(resource.service.name)".into(),
        };
        let mut operator = TraceGroup::new(
            Spanset::Pipe(Box::new(by_service), Box::new(name_is("cache"))),
            MemoryPool::unbounded(),
        );
        let out = run(&mut operator, vec![spans()]);
        let batch = concat_batches(&trace_group_schema(), &out).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            text(&batch, SPANSET_LABELS).unwrap().value(0),
            r#"{"by(resource.service.name)":"db"}"#,
            "the labels of the piped spanset survive"
        );
        assert!(
            matched(Spanset::Pipe(
                Box::new(name_is("auth")),
                Box::new(Spanset::Structural {
                    left: Box::new(Spanset::Filter(None)),
                    op: Structural {
                        relation: Relation::Child,
                        negated: false,
                        union: false,
                    },
                    right: Box::new(Spanset::Filter(None)),
                })
            ))
            .is_empty(),
            "a lone span has no child among the piped spans"
        );
    }

    #[test]
    fn input_arrives_in_any_order_and_memory_is_released() {
        let memory = MemoryPool::unbounded();
        let mut operator = TraceGroup::new(name_is("db"), Arc::clone(&memory));
        let all = spans();
        let out = run(&mut operator, vec![all.slice(0, 2), all.slice(2, 4)]);
        assert_eq!(
            concat_batches(&trace_group_schema(), &out)
                .unwrap()
                .num_rows(),
            2
        );
        assert_eq!(memory.used(), 0);
        let mut empty = TraceGroup::new(name_is("db"), memory);
        assert!(run(&mut empty, vec![]).is_empty());
    }

    /// QL-14: spans that carry a stored shape are not numbered again;
    /// the stored numbers, however they came, are the ones answered.
    #[test]
    fn a_stored_shape_is_read_rather_than_computed() {
        let all = spans();
        let trace_1 = {
            let ids = fixed(&all, TRACE_ID).unwrap();
            let keep: Vec<u32> = (0..all.num_rows())
                .filter(|&i| ids.value(i)[0] == 1)
                .map(|i| u32::try_from(i).unwrap())
                .collect();
            take_record_batch(&all, &UInt32Array::from(keep)).unwrap()
        };
        let mut shape = trace::shape(&trace_1).unwrap();
        for numbers in &mut shape.nested {
            numbers.0 += 100;
            numbers.1 += 100;
            if numbers.2 >= 0 {
                numbers.2 += 100;
            }
        }
        let stored = trace::with_trace_columns(&trace_1, &shape).unwrap();
        let mut operator = TraceGroup::new(Spanset::Filter(None), MemoryPool::unbounded());
        let out = run(&mut operator, vec![stored]);
        let batch = concat_batches(&trace_group_schema(), &out).unwrap();
        let left = batch
            .column_by_name(NEST_LEFT)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(
            left.values().iter().all(|l| *l > 100),
            "the stored numbers: {:?}",
            left.values()
        );
        let mut operator = TraceGroup::new(Spanset::Filter(None), MemoryPool::unbounded());
        let out = run(&mut operator, vec![spans()]);
        let batch = concat_batches(&trace_group_schema(), &out).unwrap();
        let left = batch
            .column_by_name(NEST_LEFT)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(left.values().iter().all(|l| *l < 100), "computed");
    }
}
