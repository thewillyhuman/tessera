// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `TraceGroup`: group spans by trace and evaluate a spanset expression
//! on each trace (QL-13). Blocking; every input row is held until the
//! end of input and accounted against the pool.
//!
//! The input is every span of the candidate traces. Each trace's spans
//! are sorted by start time and given the trace-level columns
//! (`trace_start`, `trace_duration_ns`, `root_name`, `root_service`) so
//! that conditions can read them; the tree is the `parent_span_id`
//! links among the trace's own spans. Structural relations are checked
//! pair by pair between the two sides' spans, walking the parent chain
//! for descendant and ancestor.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt32Array, new_null_array,
};
use arrow::compute::{
    SortColumn, SortOptions, cast, concat_batches, lexsort_to_indices, take, take_record_batch,
};
use arrow::datatypes::{DataType, Field, Schema};
use tessera_ir::trace::{
    MATCHED, ROOT_NAME, ROOT_SERVICE, SPANSET, SPANSET_LABELS, TRACE_DURATION_NS, TRACE_START,
};
use tessera_ir::{
    BinaryOp, Expr, Literal, Relation, SpanAggregate, Spanset, Structural, trace_group_schema,
};
use tessera_kernels::{evaluate, evaluate_predicate};
use tessera_types::event::{
    DURATION_NS, NAME, OVERFLOW, PARENT_SPAN_ID, SPAN_ID, TIMESTAMP, TRACE_ID,
};

use crate::ExecError;
use crate::batch::Batch;
use crate::memory::MemoryPool;
use crate::operator::sort::emit;
use crate::operator::{Flow, Operator, Sink};

/// Groups spans by trace and evaluates a spanset expression.
pub struct TraceGroup {
    spanset: Spanset,
    memory: Arc<MemoryPool>,
    buffered: Vec<RecordBatch>,
    reserved: usize,
}

impl TraceGroup {
    /// Evaluates `spanset` on every trace of the input.
    #[must_use]
    pub fn new(spanset: Spanset, memory: Arc<MemoryPool>) -> Self {
        TraceGroup {
            spanset,
            memory,
            buffered: Vec::new(),
            reserved: 0,
        }
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
        let bytes = rows.get_array_memory_size();
        self.memory.reserve(bytes)?;
        self.reserved += bytes;
        self.buffered.push(rows);
        Ok(Flow::Continue)
    }

    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        let result = match self.buffered.first() {
            None => Ok(()),
            Some(first) => {
                let all = concat_batches(&first.schema(), &self.buffered)?;
                self.buffered.clear();
                sorted_by_trace(&all)
                    .and_then(|spans| self.evaluate_all(&spans))
                    .and_then(|rows| emit(&rows, out))
            }
        };
        self.memory.release(self.reserved);
        self.reserved = 0;
        result
    }
}

/// `spans` ordered by trace id, then start time.
fn sorted_by_trace(spans: &RecordBatch) -> Result<RecordBatch, ExecError> {
    let key = |name: &str| -> Result<SortColumn, ExecError> {
        Ok(SortColumn {
            values: spans
                .column_by_name(name)
                .ok_or_else(|| ExecError::Unsupported(format!("trace group input has no {name}")))?
                .clone(),
            options: Some(SortOptions::default()),
        })
    };
    let indices = lexsort_to_indices(&[key(TRACE_ID)?, key(TIMESTAMP)?], None)?;
    Ok(take_record_batch(spans, &indices)?)
}

fn fixed<'a>(rows: &'a RecordBatch, name: &str) -> Result<&'a FixedSizeBinaryArray, ExecError> {
    rows.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| ExecError::Unsupported(format!("trace group input has no {name}")))
}

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
    fn new(spans: &RecordBatch) -> Result<Self, ExecError> {
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
            .ok_or_else(|| ExecError::Unsupported("trace group input has no timestamp".into()))?;
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
        let names = text(spans, NAME);
        let overflow = text(spans, OVERFLOW);
        let root_name = root.and_then(|r| {
            names
                .filter(|a| !a.is_null(r))
                .map(|a| a.value(r).to_owned())
        });
        let root_service = root
            .and_then(|r| overflow.filter(|a| !a.is_null(r)).map(|a| a.value(r)))
            .and_then(service_name);
        let rows = with_trace_columns(
            spans,
            start,
            end.saturating_sub(start),
            root_name.as_deref(),
            root_service.as_deref(),
        )?;
        Ok(Trace {
            rows,
            parent,
            start,
            duration: end.saturating_sub(start),
            root_name,
            root_service,
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

/// `resource.service.name` from an `_overflow` value.
fn service_name(overflow: &str) -> Option<String> {
    let object: serde_json::Value = serde_json::from_str(overflow).ok()?;
    match object.get("resource.service.name")? {
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// `spans` with the trace-level columns appended, one value for every
/// row.
fn with_trace_columns(
    spans: &RecordBatch,
    start: i64,
    duration: i64,
    root_name: Option<&str>,
    root_service: Option<&str>,
) -> Result<RecordBatch, ExecError> {
    let n = spans.num_rows();
    let mut fields: Vec<Field> = spans
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns: Vec<ArrayRef> = spans.columns().to_vec();
    fields.push(Field::new(
        TRACE_START,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        false,
    ));
    columns.push(Arc::new(TimestampNanosecondArray::from(vec![start; n])));
    fields.push(Field::new(TRACE_DURATION_NS, DataType::Int64, false));
    columns.push(Arc::new(Int64Array::from(vec![duration; n])));
    fields.push(Field::new(ROOT_NAME, DataType::Utf8, true));
    columns.push(Arc::new(StringArray::from(vec![root_name; n])));
    fields.push(Field::new(ROOT_SERVICE, DataType::Utf8, true));
    columns.push(Arc::new(StringArray::from(vec![root_service; n])));
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
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

    use tessera_ir::trace_group_schema;
    use tessera_types::event::{STATUS_CODE, Scalar};
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
}
