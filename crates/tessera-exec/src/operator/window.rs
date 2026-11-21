// SPDX-License-Identifier: Apache-2.0

//! `Window`: the input's rows with a column per window function
//! (QE-20), each computed over the rows of equal partition values in
//! the window's order.
//!
//! The rows are ordered by the partition expressions and then the
//! order keys through the sorted buffer of the sort operator, so that
//! past the grant they go through disk (QE-11); on `finish` the merged
//! run is cut into partitions, each held whole while its columns are
//! computed, since a lead or a last value looks ahead. With an order a
//! row's frame is the rows up to its last peer, the default frame of
//! SQL; without one it is the whole partition. A function may name
//! its frame in rows instead, counted from the row whatever its
//! peers. An aggregate runs through the accumulator the group-by
//! uses: fed the rows as the frames reach them and read after each
//! while the frames only grow, and with each row's frame as a group
//! of its own when they move at both ends, which costs the frame's
//! length per row.

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, UInt32Array,
};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{concat, concat_batches, partition, take};
use arrow::datatypes::SchemaRef;
use tessera_ir::{Expr, Frame, FrameBound, Literal, Registry, SortKey, WindowExpr, WindowFunction};
use tessera_kernels::{Accumulator, accumulator, evaluate};
use tessera_types::event::SAMPLE_RATE;

use crate::ExecError;
use crate::batch::Batch;
use crate::memory::MemoryPool;
use crate::operator::sort::{OUTPUT_ROWS, Run, SortedBuffer, emit};
use crate::operator::{Flow, Operator, Sink};

/// Adds the columns of its window functions to every row.
pub struct Window {
    partition_by: Vec<Expr>,
    order_by: Vec<SortKey>,
    functions: Vec<WindowExpr>,
    input: SchemaRef,
    schema: SchemaRef,
    buffer: SortedBuffer,
    /// Whether the input carries `sample_rate`, which weights the
    /// aggregates that are (QE-13).
    weighted: bool,
}

impl Window {
    /// The window `functions` over rows of `input`, partitioned by
    /// `partition_by` in `order_by` order, producing `schema`: the
    /// input's columns, then one per function.
    #[must_use]
    pub fn new(
        partition_by: Vec<Expr>,
        order_by: Vec<SortKey>,
        functions: Vec<WindowExpr>,
        input: SchemaRef,
        schema: SchemaRef,
        memory: Arc<MemoryPool>,
    ) -> Self {
        let keys = partition_by
            .iter()
            .cloned()
            .map(SortKey::ascending)
            .chain(order_by.iter().cloned())
            .collect();
        let weighted = input.column_with_name(SAMPLE_RATE).is_some();
        Window {
            partition_by,
            order_by,
            functions,
            buffer: SortedBuffer::new(keys, Arc::clone(&input), memory),
            input,
            schema,
            weighted,
        }
    }

    /// Cuts `run`, in partition and then window order, into partitions
    /// and emits each with its columns.
    fn cut(&self, run: Run, out: &mut dyn Sink) -> Result<(), ExecError> {
        let mut pending: Vec<RecordBatch> = Vec::new();
        let mut last: Option<Vec<ArrayRef>> = None;
        for rows in run {
            let rows = rows?;
            if rows.num_rows() == 0 {
                continue;
            }
            let keys = self.partition_keys(&rows)?;
            let ranges = if keys.is_empty() {
                std::iter::once(0..rows.num_rows()).collect()
            } else {
                partition(&keys)?.ranges()
            };
            for range in ranges {
                let first: Vec<ArrayRef> = keys.iter().map(|k| k.slice(range.start, 1)).collect();
                if let Some(last) = &last
                    && !same_partition(last, &first)?
                {
                    self.emit_partition(&concat_batches(&self.input, &pending)?, out)?;
                    pending.clear();
                }
                pending.push(rows.slice(range.start, range.len()));
                last = Some(keys.iter().map(|k| k.slice(range.end - 1, 1)).collect());
            }
        }
        if !pending.is_empty() {
            self.emit_partition(&concat_batches(&self.input, &pending)?, out)?;
        }
        Ok(())
    }

    fn partition_keys(&self, rows: &RecordBatch) -> Result<Vec<ArrayRef>, ExecError> {
        self.partition_by
            .iter()
            .map(|expr| evaluate(expr, rows).map_err(ExecError::from))
            .collect()
    }

    /// Emits `rows`, one partition in window order, with its columns.
    fn emit_partition(&self, rows: &RecordBatch, out: &mut dyn Sink) -> Result<(), ExecError> {
        let peers = self.peers(rows)?;
        let mut columns = rows.columns().to_vec();
        for function in &self.functions {
            columns.push(self.compute(function, rows, &peers)?);
        }
        let output = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        for offset in (0..output.num_rows()).step_by(OUTPUT_ROWS) {
            let length = OUTPUT_ROWS.min(output.num_rows() - offset);
            emit(&output.slice(offset, length), out)?;
        }
        Ok(())
    }

    /// The ranges of rows equal in the order keys: every row's peers,
    /// all rows at once without an order.
    fn peers(&self, rows: &RecordBatch) -> Result<Vec<Range<usize>>, ExecError> {
        if self.order_by.is_empty() {
            return Ok(std::iter::once(0..rows.num_rows()).collect());
        }
        let columns: Vec<ArrayRef> = self
            .order_by
            .iter()
            .map(|key| evaluate(&key.expr, rows))
            .collect::<Result<_, _>>()?;
        Ok(partition(&columns)?.ranges())
    }

    /// The column of `window` over the partition `rows`.
    fn compute(
        &self,
        window: &WindowExpr,
        rows: &RecordBatch,
        peers: &[Range<usize>],
    ) -> Result<ArrayRef, ExecError> {
        let value = |index: usize| {
            window.args.get(index).map_or_else(
                || {
                    Err(ExecError::Unsupported(format!(
                        "{} takes a value",
                        window.function.name()
                    )))
                },
                |arg| evaluate(arg, rows).map_err(ExecError::from),
            )
        };
        Ok(match &window.function {
            WindowFunction::RowNumber => numbered(peers, |range, _| range.start..range.end),
            WindowFunction::Rank => numbered(peers, |range, _| {
                let first = range.start;
                first..first
            }),
            WindowFunction::DenseRank => numbered(peers, |_, index| index..index),
            WindowFunction::Lag => shifted(window, &value(0)?, false)?,
            WindowFunction::Lead => shifted(window, &value(0)?, true)?,
            WindowFunction::FirstValue => {
                let frames = frames(window.frame.as_ref(), peers, rows.num_rows());
                at_frames(&value(0)?, &frames, |rows| rows.start)?
            }
            WindowFunction::LastValue => {
                let frames = frames(window.frame.as_ref(), peers, rows.num_rows());
                at_frames(&value(0)?, &frames, |rows| rows.end - 1)?
            }
            WindowFunction::Aggregate(name) => {
                let input = self.aggregate_input(name, window, rows)?;
                let frames = frames(window.frame.as_ref(), peers, rows.num_rows());
                let grows = window
                    .frame
                    .is_none_or(|frame| frame.start == FrameBound::UnboundedPreceding);
                if grows {
                    input.growing(&frames)?
                } else {
                    input.moving(&frames)?
                }
            }
        })
    }

    /// What the aggregate `name` of `window` reads of the partition
    /// `rows`.
    fn aggregate_input(
        &self,
        name: &str,
        window: &WindowExpr,
        rows: &RecordBatch,
    ) -> Result<AggregateInput, ExecError> {
        let value = window
            .args
            .first()
            .map(|arg| evaluate(arg, rows))
            .transpose()?;
        let parameters: Vec<Literal> = window
            .args
            .iter()
            .skip(1)
            .filter_map(|arg| match arg {
                Expr::Literal(literal) => Some(literal.clone()),
                _ => None,
            })
            .collect();
        let weighted = self.weighted
            && Registry::builtin()
                .aggregate(name)
                .is_some_and(|function| function.weighted);
        let sample_rates = if weighted {
            rows.column_by_name(SAMPLE_RATE)
                .and_then(|c| c.as_any().downcast_ref::<Float64Array>().cloned())
        } else {
            None
        };
        Ok(AggregateInput {
            name: name.to_owned(),
            value,
            parameters,
            sample_rates,
        })
    }
}

/// What an aggregate window function reads of one partition: its
/// argument over the partition's rows, its constant parameters and,
/// when it is weighted, the rows' sample rates (QE-13).
struct AggregateInput {
    name: String,
    value: Option<ArrayRef>,
    parameters: Vec<Literal>,
    sample_rates: Option<Float64Array>,
}

/// How many rows of frames one accumulator is fed at a time when
/// each row's frame is its own group, which bounds what is held.
const FRAME_ROWS: usize = 64 * 1024;

impl AggregateInput {
    /// A fresh accumulator of the function, the one the group-by uses.
    fn accumulator(&self) -> Result<Box<dyn Accumulator>, ExecError> {
        Ok(accumulator(
            &self.name,
            self.value.as_ref().map(Array::data_type),
            &self.parameters,
        )?)
    }

    /// The aggregate of every row over its frame, where the frames
    /// start at the partition's first row and never shrink: one
    /// accumulator is fed the rows as the frames reach them and read
    /// whenever a frame has grown.
    fn growing(&self, frames: &[Range<usize>]) -> Result<ArrayRef, ExecError> {
        let mut accumulator = self.accumulator()?;
        let mut fed = 0;
        let mut results: Vec<ArrayRef> = Vec::new();
        let mut result_of = Vec::with_capacity(frames.len());
        for frame in frames {
            if frame.end > fed || results.is_empty() {
                let rows = fed..frame.end.max(fed);
                let groups = vec![0; rows.len()];
                let values = self.value.as_ref().map(|v| v.slice(rows.start, rows.len()));
                let rates = self
                    .sample_rates
                    .as_ref()
                    .map(|r| r.slice(rows.start, rows.len()));
                accumulator.update(&groups, values.as_ref(), rates.as_ref())?;
                fed = rows.end;
                results.push(accumulator.finish(1)?);
            }
            result_of.push(u32::try_from(results.len() - 1).unwrap_or(u32::MAX));
        }
        let results: Vec<&dyn Array> = results.iter().map(AsRef::as_ref).collect();
        if results.is_empty() {
            return Ok(self.accumulator()?.finish(0)?);
        }
        Ok(take(
            &concat(&results)?,
            &UInt32Array::from(result_of),
            None,
        )?)
    }

    /// The aggregate of every row over its frame, where the frames
    /// move at both ends: each row's frame is a group of its own, a
    /// run of rows at a time.
    fn moving(&self, frames: &[Range<usize>]) -> Result<ArrayRef, ExecError> {
        let index = |row: usize| u32::try_from(row).unwrap_or(u32::MAX);
        let mut pieces: Vec<ArrayRef> = Vec::new();
        let mut row = 0;
        while row < frames.len() {
            let first = row;
            let mut groups: Vec<u32> = Vec::new();
            let mut rows: Vec<u32> = Vec::new();
            while row < frames.len() && (rows.len() < FRAME_ROWS || row == first) {
                let frame = frames[row].clone();
                groups.extend(std::iter::repeat_n(index(row - first), frame.len()));
                rows.extend(frame.map(index));
                row += 1;
            }
            let rows = UInt32Array::from(rows);
            let values = self
                .value
                .as_ref()
                .map(|v| take(v, &rows, None))
                .transpose()?;
            let rates = match &self.sample_rates {
                Some(rates) => take(rates, &rows, None)?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .cloned(),
                None => None,
            };
            let mut accumulator = self.accumulator()?;
            accumulator.update(&groups, values.as_ref(), rates.as_ref())?;
            pieces.push(accumulator.finish(row - first)?);
        }
        if pieces.is_empty() {
            return Ok(self.accumulator()?.finish(0)?);
        }
        let pieces: Vec<&dyn Array> = pieces.iter().map(AsRef::as_ref).collect();
        Ok(concat(&pieces)?)
    }
}

/// The rows each row of a partition of `length` rows is computed
/// over: its `frame` when the function names one, else the rows up to
/// its last peer, which without an order is the whole partition.
fn frames(frame: Option<&Frame>, peers: &[Range<usize>], length: usize) -> Vec<Range<usize>> {
    match frame {
        Some(frame) => (0..length).map(|row| frame.rows(row, length)).collect(),
        None => peers
            .iter()
            .flat_map(|peers| std::iter::repeat_n(0..peers.end, peers.len()))
            .collect(),
    }
}

/// The value at one row of every row's frame, the row `pick` chooses
/// of the frame's rows: null where the frame is empty.
fn at_frames(
    value: &ArrayRef,
    frames: &[Range<usize>],
    pick: impl Fn(&Range<usize>) -> usize,
) -> Result<ArrayRef, ExecError> {
    let indices: UInt32Array = frames
        .iter()
        .map(|rows| (!rows.is_empty()).then(|| u32::try_from(pick(rows)).unwrap_or(u32::MAX)))
        .collect();
    Ok(take(value, &indices, None)?)
}

/// Whether the one-row partition keys `a` and `b` are equal, nulls
/// alike: true without keys.
fn same_partition(a: &[ArrayRef], b: &[ArrayRef]) -> Result<bool, ExecError> {
    if a.is_empty() {
        return Ok(true);
    }
    let both: Vec<ArrayRef> = a
        .iter()
        .zip(b)
        .map(|(x, y)| concat(&[x.as_ref(), y.as_ref()]))
        .collect::<Result<_, _>>()?;
    Ok(partition(&both)?.ranges().len() == 1)
}

/// A number per row from its peer group: `of` gives, for a range of
/// peers and its index, the range of numbers the rows take in turn,
/// from zero, or one number for them all when the range is empty.
fn numbered(peers: &[Range<usize>], of: impl Fn(&Range<usize>, usize) -> Range<usize>) -> ArrayRef {
    let number = |n: usize| i64::try_from(n + 1).unwrap_or(i64::MAX);
    let values = peers.iter().enumerate().flat_map(|(index, range)| {
        let numbers = of(range, index);
        if numbers.is_empty() {
            std::iter::repeat_n(number(numbers.start), range.len()).collect::<Vec<_>>()
        } else {
            numbers.map(number).collect()
        }
    });
    Arc::new(Int64Array::from_iter_values(values))
}

/// `value` moved by the window's offset, back or `forward`: null where
/// the partition ends, or the window's default when it has one.
fn shifted(window: &WindowExpr, value: &ArrayRef, forward: bool) -> Result<ArrayRef, ExecError> {
    let offset = match window.args.get(1) {
        None => 1,
        Some(Expr::Literal(Literal::Int(offset))) => *offset,
        Some(other) => {
            return Err(ExecError::Unsupported(format!(
                "{} takes a whole number as its offset, not {other}",
                window.function.name()
            )));
        }
    };
    let length = i64::try_from(value.len()).unwrap_or(i64::MAX);
    let indices: UInt32Array = (0..length)
        .map(|i| {
            let j = if forward { i + offset } else { i - offset };
            (0..length)
                .contains(&j)
                .then(|| u32::try_from(j).unwrap_or(u32::MAX))
        })
        .collect();
    let taken = take(value, &indices, None)?;
    let Some(default) = window.args.get(2) else {
        return Ok(taken);
    };
    let batch = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("value", value.data_type().clone(), true),
        ])),
        vec![Arc::clone(value)],
    )?;
    let default = evaluate(default, &batch)?;
    let missing: BooleanArray = (0..indices.len())
        .map(|i| Some(indices.is_null(i)))
        .collect();
    Ok(zip(&missing, &default, &taken)?)
}

impl Operator for Window {
    fn push(&mut self, input: Batch, _: &mut dyn Sink) -> Result<Flow, ExecError> {
        self.buffer.push(input.materialize()?)?;
        Ok(Flow::Continue)
    }

    fn finish(&mut self, out: &mut dyn Sink) -> Result<(), ExecError> {
        let result = self.buffer.drain().and_then(|run| self.cut(run, out));
        self.buffer.release();
        result
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};

    use super::*;
    use crate::operator::testing::{rows, run, schema};

    fn function(function: WindowFunction, args: Vec<Expr>, name: &str) -> WindowExpr {
        WindowExpr {
            function,
            args,
            frame: None,
            name: name.into(),
        }
    }

    fn framed(
        function: WindowFunction,
        args: Vec<Expr>,
        (start, end): (FrameBound, FrameBound),
        name: &str,
    ) -> WindowExpr {
        WindowExpr {
            frame: Some(Frame::new(start, end).unwrap()),
            ..self::function(function, args, name)
        }
    }

    /// The output schema: the input's fields then one per function,
    /// typed as the test expects.
    fn output(functions: &[(&str, DataType)]) -> SchemaRef {
        let mut fields: Vec<Field> = schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        for (name, data_type) in functions {
            fields.push(Field::new(*name, data_type.clone(), true));
        }
        Arc::new(Schema::new(fields))
    }

    fn ints(batches: &[RecordBatch], column: &str) -> Vec<Option<i64>> {
        batches
            .iter()
            .flat_map(|b| {
                let array = cast(b.column_by_name(column).unwrap(), &DataType::Int64).unwrap();
                array.as_primitive::<Int64Type>().iter().collect::<Vec<_>>()
            })
            .collect()
    }

    fn floats(batches: &[RecordBatch], column: &str) -> Vec<Option<f64>> {
        batches
            .iter()
            .flat_map(|b| {
                b.column_by_name(column)
                    .unwrap()
                    .as_primitive::<Float64Type>()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Two batches of three: severities 0, 10, 20 at times 1 to 3 and
    /// again at 4 to 6, so each severity is a partition of two rows.
    fn input() -> Vec<RecordBatch> {
        vec![rows(&[1, 2, 3]), rows(&[4, 5, 6])]
    }

    /// QE-20: within each partition, in order, a row is numbered, sees
    /// its neighbours, the partition's first row and its own last peer,
    /// and the aggregates of the rows so far; partitions come out in
    /// key order.
    #[test]
    fn numbers_rows_within_each_partition_in_order() {
        let timestamp = DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None);
        let mut window = Window::new(
            vec![Expr::column("severity_number")],
            vec![SortKey::ascending(Expr::column("timestamp"))],
            vec![
                function(WindowFunction::RowNumber, Vec::new(), "n"),
                function(WindowFunction::Lag, vec![Expr::column("timestamp")], "prev"),
                function(
                    WindowFunction::Lead,
                    vec![Expr::column("timestamp")],
                    "next",
                ),
                function(
                    WindowFunction::FirstValue,
                    vec![Expr::column("timestamp")],
                    "first",
                ),
                function(
                    WindowFunction::LastValue,
                    vec![Expr::column("timestamp")],
                    "last",
                ),
                function(
                    WindowFunction::Aggregate("count".into()),
                    Vec::new(),
                    "so_far",
                ),
                function(
                    WindowFunction::Aggregate("sum".into()),
                    vec![Expr::column("sample_rate")],
                    "total",
                ),
            ],
            schema(),
            output(&[
                ("n", DataType::Int64),
                ("prev", timestamp.clone()),
                ("next", timestamp.clone()),
                ("first", timestamp.clone()),
                ("last", timestamp),
                ("so_far", DataType::Float64),
                ("total", DataType::Float64),
            ]),
            MemoryPool::unbounded(),
        );
        let out = run(&mut window, input());
        assert_eq!(ints(&out, "timestamp"), [1, 4, 2, 5, 3, 6].map(Some));
        assert_eq!(ints(&out, "n"), [1, 2, 1, 2, 1, 2].map(Some));
        assert_eq!(
            ints(&out, "prev"),
            [None, Some(1), None, Some(2), None, Some(3)]
        );
        assert_eq!(
            ints(&out, "next"),
            [Some(4), None, Some(5), None, Some(6), None]
        );
        assert_eq!(ints(&out, "first"), [1, 1, 2, 2, 3, 3].map(Some));
        assert_eq!(ints(&out, "last"), [1, 4, 2, 5, 3, 6].map(Some));
        assert_eq!(
            floats(&out, "so_far"),
            [1.0, 2.0, 1.0, 2.0, 1.0, 2.0].map(Some)
        );
        assert_eq!(
            floats(&out, "total"),
            [1.0, 2.0, 1.0, 2.0, 1.0, 2.0].map(Some)
        );
    }

    /// QE-20: a frame in rows moves with the row. Over times 1 to 6
    /// with severities 0, 10, 20, 0, 10, 20: the sum of a row and the
    /// one before, the count of the rows from two before to one
    /// before, none for the first row, the running sum, the sum of
    /// the whole partition, and the first and last rows of a frame of
    /// three centred on the row.
    #[test]
    fn a_frame_in_rows_moves_with_the_row() {
        use FrameBound::{
            CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding,
        };
        let severity = || vec![Expr::column("severity_number")];
        let sum = || WindowFunction::Aggregate("sum".into());
        let timestamp = DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None);
        let mut window = Window::new(
            Vec::new(),
            vec![SortKey::ascending(Expr::column("timestamp"))],
            vec![
                framed(sum(), severity(), (Preceding(1), CurrentRow), "moving"),
                framed(
                    WindowFunction::Aggregate("count".into()),
                    Vec::new(),
                    (Preceding(2), Preceding(1)),
                    "before",
                ),
                framed(
                    sum(),
                    severity(),
                    (UnboundedPreceding, CurrentRow),
                    "running",
                ),
                framed(
                    sum(),
                    severity(),
                    (UnboundedPreceding, UnboundedFollowing),
                    "whole",
                ),
                framed(
                    WindowFunction::FirstValue,
                    vec![Expr::column("timestamp")],
                    (Preceding(1), Following(1)),
                    "first",
                ),
                framed(
                    WindowFunction::LastValue,
                    vec![Expr::column("timestamp")],
                    (Preceding(1), Following(1)),
                    "last",
                ),
                framed(
                    WindowFunction::LastValue,
                    vec![Expr::column("timestamp")],
                    (Following(2), Following(3)),
                    "ahead",
                ),
            ],
            schema(),
            output(&[
                ("moving", DataType::Float64),
                ("before", DataType::Float64),
                ("running", DataType::Float64),
                ("whole", DataType::Float64),
                ("first", timestamp.clone()),
                ("last", timestamp.clone()),
                ("ahead", timestamp),
            ]),
            MemoryPool::unbounded(),
        );
        let out = run(&mut window, input());
        assert_eq!(ints(&out, "timestamp"), [1, 2, 3, 4, 5, 6].map(Some));
        assert_eq!(
            floats(&out, "moving"),
            [0.0, 10.0, 30.0, 20.0, 10.0, 30.0].map(Some)
        );
        assert_eq!(
            floats(&out, "before"),
            [0.0, 1.0, 2.0, 2.0, 2.0, 2.0].map(Some)
        );
        assert_eq!(
            floats(&out, "running"),
            [0.0, 10.0, 30.0, 30.0, 40.0, 60.0].map(Some)
        );
        assert_eq!(floats(&out, "whole"), [60.0; 6].map(Some));
        assert_eq!(ints(&out, "first"), [1, 1, 2, 3, 4, 5].map(Some));
        assert_eq!(ints(&out, "last"), [2, 3, 4, 5, 6, 6].map(Some));
        assert_eq!(
            ints(&out, "ahead"),
            [Some(4), Some(5), Some(6), Some(6), None, None],
            "a frame past the partition is empty"
        );
    }

    /// A frame counts rows, not peers: two rows equal in the order
    /// keys share the default frame and differ under `ROWS`. Frames
    /// stay within their partition.
    #[test]
    fn a_frame_in_rows_tells_peers_apart_and_stays_in_its_partition() {
        use FrameBound::{CurrentRow, Preceding, UnboundedPreceding};
        let count = || WindowFunction::Aggregate("count".into());
        let mut window = Window::new(
            Vec::new(),
            vec![SortKey::ascending(Expr::column("severity_number"))],
            vec![
                function(count(), Vec::new(), "peers"),
                framed(
                    count(),
                    Vec::new(),
                    (UnboundedPreceding, CurrentRow),
                    "rows",
                ),
            ],
            schema(),
            output(&[("peers", DataType::Float64), ("rows", DataType::Float64)]),
            MemoryPool::unbounded(),
        );
        let out = run(&mut window, input());
        assert_eq!(
            floats(&out, "peers"),
            [2.0, 2.0, 4.0, 4.0, 6.0, 6.0].map(Some)
        );
        assert_eq!(
            floats(&out, "rows"),
            [1.0, 2.0, 3.0, 4.0, 5.0, 6.0].map(Some)
        );

        let mut window = Window::new(
            vec![Expr::column("severity_number")],
            vec![SortKey::ascending(Expr::column("timestamp"))],
            vec![framed(
                count(),
                Vec::new(),
                (Preceding(5), CurrentRow),
                "rows",
            )],
            schema(),
            output(&[("rows", DataType::Float64)]),
            MemoryPool::unbounded(),
        );
        let out = run(&mut window, input());
        assert_eq!(
            floats(&out, "rows"),
            [1.0, 2.0, 1.0, 2.0, 1.0, 2.0].map(Some)
        );
    }

    /// Rows equal in the order keys are peers: they share a rank, a
    /// dense rank, a last value and an aggregate's frame, while their
    /// row numbers differ.
    #[test]
    fn peers_share_their_rank_and_frame() {
        let mut window = Window::new(
            Vec::new(),
            vec![SortKey::ascending(Expr::column("severity_number"))],
            vec![
                function(WindowFunction::Rank, Vec::new(), "rank"),
                function(WindowFunction::DenseRank, Vec::new(), "dense"),
                function(WindowFunction::RowNumber, Vec::new(), "n"),
                function(
                    WindowFunction::LastValue,
                    vec![Expr::column("timestamp")],
                    "last",
                ),
                function(
                    WindowFunction::Aggregate("count".into()),
                    Vec::new(),
                    "so_far",
                ),
            ],
            schema(),
            output(&[
                ("rank", DataType::Int64),
                ("dense", DataType::Int64),
                ("n", DataType::Int64),
                (
                    "last",
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                ),
                ("so_far", DataType::Float64),
            ]),
            MemoryPool::unbounded(),
        );
        let out = run(&mut window, input());
        assert_eq!(
            ints(&out, "severity_number"),
            [0, 0, 10, 10, 20, 20].map(Some)
        );
        assert_eq!(ints(&out, "rank"), [1, 1, 3, 3, 5, 5].map(Some));
        assert_eq!(ints(&out, "dense"), [1, 1, 2, 2, 3, 3].map(Some));
        assert_eq!(ints(&out, "n"), [1, 2, 3, 4, 5, 6].map(Some));
        assert_eq!(
            floats(&out, "so_far"),
            [2.0, 2.0, 4.0, 4.0, 6.0, 6.0].map(Some)
        );
        let last = ints(&out, "last");
        let times = ints(&out, "timestamp");
        for pair in [0, 2, 4] {
            assert_eq!(last[pair], last[pair + 1]);
            assert!(last[pair] == times[pair] || last[pair] == times[pair + 1]);
        }
    }

    /// `lag` and `lead` take an offset and a default; without an order
    /// an aggregate sees the whole partition.
    #[test]
    fn offsets_defaults_and_whole_partitions() {
        let mut window = Window::new(
            Vec::new(),
            vec![SortKey::ascending(Expr::column("timestamp"))],
            vec![
                function(
                    WindowFunction::Lag,
                    vec![
                        Expr::column("sample_rate"),
                        Expr::literal(2),
                        Expr::literal(0.0),
                    ],
                    "two_back",
                ),
                function(
                    WindowFunction::Lead,
                    vec![Expr::column("sample_rate"), Expr::literal(1)],
                    "ahead",
                ),
            ],
            schema(),
            output(&[
                ("two_back", DataType::Float64),
                ("ahead", DataType::Float64),
            ]),
            MemoryPool::unbounded(),
        );
        let out = run(&mut window, input());
        assert_eq!(
            floats(&out, "two_back"),
            [0.0, 0.0, 1.0, 1.0, 1.0, 1.0].map(Some)
        );
        assert_eq!(
            floats(&out, "ahead"),
            [Some(1.0), Some(1.0), Some(1.0), Some(1.0), Some(1.0), None]
        );
        let mut whole = Window::new(
            vec![Expr::column("severity_number")],
            Vec::new(),
            vec![function(
                WindowFunction::Aggregate("count".into()),
                Vec::new(),
                "all",
            )],
            schema(),
            output(&[("all", DataType::Float64)]),
            MemoryPool::unbounded(),
        );
        let out = run(&mut whole, input());
        assert_eq!(floats(&out, "all"), [2.0; 6].map(Some));
    }

    /// QE-11: past its grant the rows go through sorted runs on disk and
    /// the columns come out the same, with the files gone afterwards.
    #[test]
    fn spills_past_its_grant() {
        let dir = std::env::temp_dir().join(format!("tessera-window-spill-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let one = rows(&[1, 2, 3]).get_array_memory_size();
        let pool = MemoryPool::with_spill_dir(one * 3 / 2, Some(dir.clone()));
        let mut window = Window::new(
            vec![Expr::column("severity_number")],
            vec![SortKey::ascending(Expr::column("timestamp"))],
            vec![function(WindowFunction::RowNumber, Vec::new(), "n")],
            schema(),
            output(&[("n", DataType::Int64)]),
            Arc::clone(&pool),
        );
        let out = run(
            &mut window,
            (0..4)
                .map(|i| rows(&[3 * i + 1, 3 * i + 2, 3 * i + 3]))
                .collect(),
        );
        assert_eq!(
            ints(&out, "n"),
            [1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4].map(Some)
        );
        assert!(pool.spilled_bytes() > 0, "rows went to disk");
        assert_eq!(pool.used(), 0);
        assert!(
            std::fs::read_dir(&dir).unwrap().next().is_none(),
            "spill files are removed"
        );
    }
}
