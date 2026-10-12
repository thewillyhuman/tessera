// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! `HashJoin`: the pairs of rows of two inputs with equal keys (DM-2).
//!
//! One side, the build side, is held whole in a hash table keyed by its
//! join keys; the other, the probe side, streams through
//! [`Operator::push`], each batch looking its keys up and emitting the
//! pairs it finds. Null keys match nothing, as in SQL. The output puts
//! the left side's columns before the right side's whichever side was
//! built, under the names the plan's schema gives them.
//!
//! An `ASOF JOIN` (section 9.2) pairs a probe row with one build row of
//! its key, the nearest in time on the side [`AsofMatch`] asks for: the
//! build rows of each key are kept in time order and a probe row finds
//! its row by binary search. The right side is always built, since it
//! is the right row that is chosen for each left one.
//!
//! A build side the memory grant does not cover goes through
//! [`partitioned`] instead (QE-11): both sides are split by a hash of
//! their keys into runs on local disk and each bucket is joined on its
//! own, the table in memory a bucket's rather than a side's.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, UInt32Array};
use arrow::compute::{cast, concat_batches, take, take_record_batch};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use tessera_ir::{AsofMatch, Expr, JoinKind, qualify};
use tessera_kernels::{Grouper, buckets_of, evaluate};

use crate::ExecError;
use crate::batch::Batch;
use crate::memory::MemoryPool;
use crate::operator::sort::emit_flow;
use crate::operator::{CollectSink, Flow, Operator, Sink};
use crate::spill::SpillFile;

/// How many buckets a partitioned join splits its sides into.
const BUCKETS: u32 = 16;

/// Which input the hash table holds.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BuildSide {
    /// The left input is hashed and the right probes it.
    Left,
    /// The right input is hashed and the left probes it.
    Right,
}

/// One input of a join as the operator needs to know it.
#[derive(Clone, Debug)]
pub struct JoinInput {
    /// The input's schema, which its batches follow.
    pub schema: SchemaRef,
    /// The keys, evaluated over the input's own rows.
    pub keys: Vec<Expr>,
    /// The time an `ASOF JOIN` matches on, over the input's own rows.
    pub time: Option<Expr>,
    /// The name qualifying the input's columns in the output.
    pub name: String,
}

/// Joins a built input with the batches pushed through.
pub struct HashJoin {
    schema: SchemaRef,
    kind: JoinKind,
    build_side: BuildSide,
    build: RecordBatch,
    table: Grouper,
    /// The build rows of each key, by the key's group id; in time order
    /// under an `ASOF JOIN`.
    rows_by_key: Vec<Vec<u32>>,
    probe_keys: Vec<Expr>,
    /// Under an `ASOF JOIN`: which build row a probe row takes, the
    /// build rows' times and the probe rows' time.
    asof: Option<AsofMatch>,
    times: Vec<Option<i64>>,
    probe_time: Option<Expr>,
    memory: Arc<MemoryPool>,
    reserved: usize,
}

impl HashJoin {
    /// A join holding `rows`, the `build` input's rows, hashed by its
    /// keys, to be probed by the other input's batches. A left join
    /// builds its right side, since every probe row must come out, and
    /// so does an `ASOF JOIN`, whose `asof` says which right row of a
    /// key each left row takes by the inputs' times.
    ///
    /// # Errors
    ///
    /// If the keys or times do not evaluate or the memory grant does
    /// not cover the build side.
    pub fn new(
        kind: JoinKind,
        build_side: BuildSide,
        asof: Option<AsofMatch>,
        rows: &[RecordBatch],
        build: &JoinInput,
        probe: &JoinInput,
        memory: Arc<MemoryPool>,
    ) -> Result<Self, ExecError> {
        if kind == JoinKind::Left && build_side == BuildSide::Left {
            return Err(ExecError::Unsupported(
                "a left join probes with its left side".into(),
            ));
        }
        if asof.is_some() && build_side == BuildSide::Left {
            return Err(ExecError::Unsupported(
                "an asof join probes with its left side".into(),
            ));
        }
        let rows = match rows.first() {
            Some(first) => concat_batches(&first.schema(), rows)?,
            None => RecordBatch::new_empty(Arc::clone(&build.schema)),
        };
        let per_row = size_of::<u32>() + if asof.is_some() { size_of::<i64>() } else { 0 };
        let reserved = rows.get_array_memory_size() + rows.num_rows() * per_row;
        memory.reserve(reserved)?;
        let times = match (asof, &build.time) {
            (Some(_), Some(time)) => times_of(time, &rows)?,
            (Some(_), None) => {
                return Err(ExecError::Unsupported(
                    "an asof join needs a time on each side".into(),
                ));
            }
            (None, _) => Vec::new(),
        };
        let keys = evaluate_keys(&build.keys, &rows)?;
        let mut table = Grouper::new(keys.iter().map(|k| k.data_type().clone()).collect())?;
        let ids = table.group_ids(&keys)?;
        let mut rows_by_key: Vec<Vec<u32>> = vec![Vec::new(); table.len()];
        for (row, id) in ids.into_iter().enumerate() {
            let no_time = times.get(row).is_some_and(Option::is_none);
            if no_time || keys.iter().any(|k| k.is_null(row)) {
                continue;
            }
            rows_by_key[id as usize].push(u32::try_from(row).unwrap_or(u32::MAX));
        }
        if asof.is_some() {
            for of_key in &mut rows_by_key {
                of_key.sort_by_key(|row| times[*row as usize]);
            }
        }
        let (left, right) = match build_side {
            BuildSide::Left => ((build, rows.schema()), (probe, Arc::clone(&probe.schema))),
            BuildSide::Right => ((probe, Arc::clone(&probe.schema)), (build, rows.schema())),
        };
        Ok(HashJoin {
            schema: output_schema(left, right, kind),
            kind,
            build_side,
            build: rows,
            table,
            rows_by_key,
            probe_keys: probe.keys.clone(),
            asof,
            times,
            probe_time: probe.time.clone(),
            memory,
            reserved,
        })
    }

    /// The output schema.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// The build rows a probe row of key `id` at `time` pairs with:
    /// every row of the key, or under an `ASOF JOIN` the one nearest
    /// in time on the asked side, a tie going to the later build row.
    fn matches(&self, id: u32, time: Option<i64>) -> &[u32] {
        let of_key = &self.rows_by_key[id as usize];
        let Some(matching) = self.asof else {
            return of_key;
        };
        let Some(time) = time else {
            return &[];
        };
        let at = |row: &u32| self.times[*row as usize].unwrap_or(i64::MIN);
        let found = match matching {
            AsofMatch::AtOrBefore => of_key[..of_key.partition_point(|r| at(r) <= time)].last(),
            AsofMatch::Before => of_key[..of_key.partition_point(|r| at(r) < time)].last(),
            AsofMatch::AtOrAfter => of_key.get(of_key.partition_point(|r| at(r) < time)),
            AsofMatch::After => of_key.get(of_key.partition_point(|r| at(r) <= time)),
        };
        found.map_or(&[], std::slice::from_ref)
    }
}

/// `time` over `rows` as integers: nanoseconds for a timestamp, the
/// value for an integer; none where it is null.
fn times_of(time: &Expr, rows: &RecordBatch) -> Result<Vec<Option<i64>>, ExecError> {
    let values = evaluate(time, rows)?;
    let values = cast(&values, &DataType::Int64)?;
    Ok(values.as_primitive::<Int64Type>().iter().collect())
}

/// Joins `build_rows` and `probe_rows` one bucket of keys at a time
/// through `dir` on local disk (QE-11): each side is split by a hash of
/// its keys into one run per bucket, the rows in memory are let go, and
/// each bucket's build rows are hashed on their own and probed by the
/// bucket's probe rows. The output is what [`HashJoin`] over the whole
/// would give, in another order.
///
/// # Errors
///
/// If a run cannot be written or read, the keys do not evaluate, or a
/// bucket's build side still does not fit the grant.
#[allow(
    clippy::too_many_arguments,
    reason = "the two sides and where to spill"
)]
pub fn partitioned(
    kind: JoinKind,
    build_side: BuildSide,
    asof: Option<AsofMatch>,
    build_rows: Vec<RecordBatch>,
    build: &JoinInput,
    probe_rows: Vec<RecordBatch>,
    probe: &JoinInput,
    memory: &Arc<MemoryPool>,
    dir: &Path,
) -> Result<Vec<RecordBatch>, ExecError> {
    let build_runs = split_to_disk(build_rows, build, memory, dir)?;
    let probe_runs = split_to_disk(probe_rows, probe, memory, dir)?;
    let mut output = Vec::new();
    for (build_run, probe_run) in build_runs.into_iter().zip(probe_runs) {
        let Some(probe_run) = probe_run else {
            // Every output row comes from a probe row.
            continue;
        };
        let hashed: Vec<RecordBatch> = match &build_run {
            Some(run) => run.read()?.collect::<Result<_, _>>()?,
            None => Vec::new(),
        };
        let mut join = HashJoin::new(
            kind,
            build_side,
            asof,
            &hashed,
            build,
            probe,
            Arc::clone(memory),
        )?;
        drop(hashed);
        let mut sink = CollectSink::new();
        for rows in probe_run.read()? {
            join.push(Batch::new(rows?), &mut sink)?;
        }
        join.finish(&mut sink)?;
        output.extend(sink.into_batches());
    }
    Ok(output)
}

/// Writes `rows` to `dir` as one run per bucket of their keys; `None`
/// for a bucket without rows.
fn split_to_disk(
    rows: Vec<RecordBatch>,
    input: &JoinInput,
    memory: &Arc<MemoryPool>,
    dir: &Path,
) -> Result<Vec<Option<SpillFile>>, ExecError> {
    let buckets = usize::try_from(BUCKETS).unwrap_or(16);
    let mut by_bucket: Vec<Vec<RecordBatch>> = vec![Vec::new(); buckets];
    for batch in rows {
        if batch.num_rows() == 0 {
            continue;
        }
        let keys = evaluate_keys(&input.keys, &batch)?;
        let assigned = buckets_of(&keys, BUCKETS)?;
        for (bucket, rows) in by_bucket.iter_mut().enumerate() {
            let indices: UInt32Array = assigned
                .iter()
                .enumerate()
                .filter(|(_, b)| usize::try_from(**b).unwrap_or(0) == bucket)
                .map(|(row, _)| u32::try_from(row).unwrap_or(u32::MAX))
                .collect();
            if !indices.is_empty() {
                rows.push(take_record_batch(&batch, &indices)?);
            }
        }
    }
    by_bucket
        .into_iter()
        .map(|batches| {
            if batches.is_empty() {
                return Ok(None);
            }
            let file = SpillFile::write(dir, &input.schema, &batches)?;
            memory.note_spilled(file.bytes());
            Ok(Some(file))
        })
        .collect()
}

/// The joined schema: the left columns then the right, qualified; the
/// right ones nullable under a left join.
fn output_schema(
    (left, left_schema): (&JoinInput, SchemaRef),
    (right, right_schema): (&JoinInput, SchemaRef),
    kind: JoinKind,
) -> SchemaRef {
    let qualified = |schema: &Schema, name: &str, nullable: bool| {
        schema
            .fields()
            .iter()
            .map(|f| {
                let field = f.as_ref().clone().with_name(qualify(name, f.name()));
                if nullable {
                    field.with_nullable(true)
                } else {
                    field
                }
            })
            .collect::<Vec<Field>>()
    };
    let mut fields = qualified(&left_schema, &left.name, false);
    fields.extend(qualified(
        &right_schema,
        &right.name,
        kind == JoinKind::Left,
    ));
    Arc::new(Schema::new(fields))
}

fn evaluate_keys(keys: &[Expr], rows: &RecordBatch) -> Result<Vec<ArrayRef>, ExecError> {
    keys.iter()
        .map(|key| evaluate(key, rows).map_err(ExecError::from))
        .collect()
}

fn taken(rows: &RecordBatch, indices: &UInt32Array) -> Result<Vec<ArrayRef>, ExecError> {
    rows.columns()
        .iter()
        .map(|column| take(column, indices, None).map_err(ExecError::from))
        .collect()
}

impl Operator for HashJoin {
    fn push(&mut self, input: Batch, out: &mut dyn Sink) -> Result<Flow, ExecError> {
        let rows = input.materialize()?;
        if rows.num_rows() == 0 {
            return Ok(Flow::Continue);
        }
        let keys = evaluate_keys(&self.probe_keys, &rows)?;
        let times = match (self.asof, &self.probe_time) {
            (Some(_), Some(time)) => times_of(time, &rows)?,
            _ => Vec::new(),
        };
        let mut probe_rows: Vec<u32> = Vec::new();
        let mut build_rows: Vec<Option<u32>> = Vec::new();
        for (row, key) in self.table.lookup(&keys)?.into_iter().enumerate() {
            let matched = key.map_or(&[][..], |id| {
                self.matches(id, times.get(row).copied().flatten())
            });
            let row = u32::try_from(row).unwrap_or(u32::MAX);
            if matched.is_empty() {
                if self.kind == JoinKind::Left {
                    probe_rows.push(row);
                    build_rows.push(None);
                }
                continue;
            }
            for &found in matched {
                probe_rows.push(row);
                build_rows.push(Some(found));
            }
        }
        if probe_rows.is_empty() {
            return Ok(Flow::Continue);
        }
        let probe_columns = taken(&rows, &UInt32Array::from(probe_rows))?;
        let build_columns = taken(&self.build, &UInt32Array::from(build_rows))?;
        let columns = match self.build_side {
            BuildSide::Left => [build_columns, probe_columns].concat(),
            BuildSide::Right => [probe_columns, build_columns].concat(),
        };
        let output = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        emit_flow(&output, out)
    }

    fn finish(&mut self, _: &mut dyn Sink) -> Result<(), ExecError> {
        self.memory.release(self.reserved);
        self.reserved = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int32Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, TimeUnit};

    use super::*;
    use crate::operator::testing::{rows, run, schema};

    /// Owners of severities: 0 and 10 have one, 20 has two, 30 none.
    fn owners() -> (SchemaRef, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("severity", DataType::Int32, true),
            Field::new("owner", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![
                    Some(0),
                    Some(10),
                    Some(20),
                    Some(20),
                    None,
                ])),
                Arc::new(StringArray::from(vec!["ann", "bob", "cy", "di", "nobody"])),
            ],
        )
        .unwrap();
        (schema, vec![batch])
    }

    fn join(kind: JoinKind, build_side: BuildSide) -> HashJoin {
        let (owner_schema, owner_rows) = owners();
        let logs = JoinInput {
            schema: schema(),
            keys: vec![Expr::column("severity_number")],
            time: None,
            name: "l".into(),
        };
        let owners = JoinInput {
            schema: owner_schema,
            keys: vec![Expr::column("severity")],
            time: None,
            name: "o".into(),
        };
        let memory = MemoryPool::unbounded();
        match build_side {
            BuildSide::Right => {
                HashJoin::new(kind, build_side, None, &owner_rows, &owners, &logs, memory).unwrap()
            }
            BuildSide::Left => HashJoin::new(
                kind,
                build_side,
                None,
                &[rows(&[1, 2, 3, 4])],
                &logs,
                &owners,
                memory,
            )
            .unwrap(),
        }
    }

    fn strings(batches: &[RecordBatch], column: &str) -> Vec<Option<String>> {
        batches
            .iter()
            .flat_map(|b| {
                b.column_by_name(column)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|s| s.map(str::to_owned))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn an_inner_join_pairs_every_match_and_skips_null_keys() {
        let mut join = join(JoinKind::Inner, BuildSide::Right);
        let out = run(&mut join, vec![rows(&[1, 2, 3, 4])]);
        let schema = out[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names[0], "l.timestamp");
        assert_eq!(names[4], "o.severity");
        assert_eq!(
            strings(&out, "l.body"),
            ["row0", "row1", "row2", "row2"].map(|s| Some(s.to_owned()))
        );
        assert_eq!(
            strings(&out, "o.owner"),
            ["ann", "bob", "cy", "di"].map(|s| Some(s.to_owned()))
        );
    }

    #[test]
    fn a_left_join_keeps_unmatched_rows_with_nulls() {
        let mut join = join(JoinKind::Left, BuildSide::Right);
        let out = run(&mut join, vec![rows(&[1, 2, 3, 4]), rows(&[5])]);
        let owners = strings(&out, "o.owner");
        assert_eq!(
            owners.len(),
            6,
            "two rows for severity 20, one for the rest"
        );
        assert_eq!(owners[4], None, "severity 30 has no owner");
        assert_eq!(
            owners[5].as_deref(),
            Some("ann"),
            "the next batch probes too"
        );
        assert!(
            out[0]
                .schema()
                .field_with_name("o.owner")
                .unwrap()
                .is_nullable()
        );
    }

    #[test]
    fn building_the_left_side_keeps_the_left_columns_first() {
        let mut join = join(JoinKind::Inner, BuildSide::Left);
        let (_, owner_rows) = owners();
        let out = run(&mut join, owner_rows);
        assert_eq!(out[0].schema().field(0).name(), "l.timestamp");
        assert_eq!(
            strings(&out, "o.owner"),
            ["ann", "bob", "cy", "di"].map(|s| Some(s.to_owned()))
        );
    }

    #[test]
    fn a_left_join_cannot_build_its_left_side_and_memory_is_released() {
        let (owner_schema, _) = owners();
        let logs = JoinInput {
            schema: schema(),
            keys: vec![Expr::column("severity_number")],
            time: None,
            name: "l".into(),
        };
        let owners = JoinInput {
            schema: owner_schema,
            keys: vec![Expr::column("severity")],
            time: None,
            name: "o".into(),
        };
        let memory = MemoryPool::unbounded();
        assert!(
            HashJoin::new(
                JoinKind::Left,
                BuildSide::Left,
                None,
                &[],
                &logs,
                &owners,
                Arc::clone(&memory)
            )
            .is_err()
        );
        let mut join = HashJoin::new(
            JoinKind::Inner,
            BuildSide::Right,
            None,
            &[],
            &owners,
            &logs,
            Arc::clone(&memory),
        )
        .unwrap();
        assert!(
            run(&mut join, vec![rows(&[1])]).is_empty(),
            "nothing matches an empty build side"
        );
        assert_eq!(memory.used(), 0);
    }

    /// QE-11: joining one bucket at a time through disk gives the rows a
    /// whole join gives, and leaves no files behind.
    #[test]
    fn a_partitioned_join_matches_a_whole_one() {
        let dir = std::env::temp_dir().join(format!("tessera-join-spill-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (owner_schema, owner_rows) = owners();
        let logs = JoinInput {
            schema: schema(),
            keys: vec![Expr::column("severity_number")],
            time: None,
            name: "l".into(),
        };
        let owners = JoinInput {
            schema: owner_schema,
            keys: vec![Expr::column("severity")],
            time: None,
            name: "o".into(),
        };
        for kind in [JoinKind::Inner, JoinKind::Left] {
            let mut whole = HashJoin::new(
                kind,
                BuildSide::Right,
                None,
                &owner_rows,
                &owners,
                &logs,
                MemoryPool::unbounded(),
            )
            .unwrap();
            let expected = run(&mut whole, vec![rows(&[1, 2, 3, 4]), rows(&[5, 6])]);
            let pool = MemoryPool::unbounded();
            let got = partitioned(
                kind,
                BuildSide::Right,
                None,
                owner_rows.clone(),
                &owners,
                vec![rows(&[1, 2, 3, 4]), rows(&[5, 6])],
                &logs,
                &pool,
                &dir,
            )
            .unwrap();
            let mut expected_owners = strings(&expected, "o.owner");
            let mut got_owners = strings(&got, "o.owner");
            expected_owners.sort();
            got_owners.sort();
            assert_eq!(got_owners, expected_owners, "{kind:?}");
            assert!(pool.spilled_bytes() > 0);
            assert_eq!(pool.used(), 0);
        }
        assert!(
            std::fs::read_dir(&dir).unwrap().next().is_none(),
            "spill files are removed"
        );
    }

    /// Section 9.2: an `ASOF JOIN` pairs a probe row with the one build
    /// row of its key nearest in time on the asked side, none when no
    /// row lies on that side, a tie going to the later build row.
    #[test]
    fn an_asof_join_takes_the_nearest_row_in_time() {
        let owner_schema = Arc::new(Schema::new(vec![
            Field::new("severity", DataType::Int32, true),
            Field::new(
                "since",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("owner", DataType::Utf8, true),
        ]));
        let owner_rows = RecordBatch::try_new(
            Arc::clone(&owner_schema),
            vec![
                Arc::new(Int32Array::from(vec![0, 0, 10, 20, 20])),
                Arc::new(TimestampNanosecondArray::from(vec![0, 5, 100, 2, 3])),
                Arc::new(StringArray::from(vec!["ann", "amy", "bob", "cy", "dee"])),
            ],
        )
        .unwrap();
        let owners = JoinInput {
            schema: owner_schema,
            keys: vec![Expr::column("severity")],
            time: Some(Expr::column("since")),
            name: "o".into(),
        };
        let logs = JoinInput {
            schema: schema(),
            keys: vec![Expr::column("severity_number")],
            time: Some(Expr::column("timestamp")),
            name: "l".into(),
        };
        let owners_by = |kind, matching| {
            let mut join = HashJoin::new(
                kind,
                BuildSide::Right,
                Some(matching),
                std::slice::from_ref(&owner_rows),
                &owners,
                &logs,
                MemoryPool::unbounded(),
            )
            .unwrap();
            strings(
                &run(&mut join, vec![rows(&[1, 2, 3, 4]), rows(&[6, 7])]),
                "o.owner",
            )
        };
        let names = |names: &[&str]| {
            names
                .iter()
                .map(|s| Some((*s).to_owned()))
                .collect::<Vec<_>>()
        };
        // Severities 0, 10, 20 and 30 at times 1 to 4, then 0 and 10 at 6 and 7.
        assert_eq!(
            owners_by(JoinKind::Inner, AsofMatch::AtOrBefore),
            names(&["ann", "dee", "amy"])
        );
        assert_eq!(
            owners_by(JoinKind::Left, AsofMatch::AtOrBefore),
            vec![
                Some("ann".to_owned()),
                None,
                Some("dee".to_owned()),
                None,
                Some("amy".to_owned()),
                None
            ]
        );
        assert_eq!(
            owners_by(JoinKind::Inner, AsofMatch::Before),
            names(&["ann", "cy", "amy"])
        );
        assert_eq!(
            owners_by(JoinKind::Inner, AsofMatch::AtOrAfter),
            names(&["amy", "bob", "dee", "bob"])
        );
        assert_eq!(
            owners_by(JoinKind::Inner, AsofMatch::After),
            names(&["amy", "bob", "bob"])
        );
        assert!(
            HashJoin::new(
                JoinKind::Inner,
                BuildSide::Left,
                Some(AsofMatch::AtOrBefore),
                &[rows(&[1])],
                &logs,
                &owners,
                MemoryPool::unbounded(),
            )
            .is_err(),
            "an asof join builds its right side"
        );
    }
}
