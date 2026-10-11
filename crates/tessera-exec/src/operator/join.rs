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
//! A build side the memory grant does not cover goes through
//! [`partitioned`] instead (QE-11): both sides are split by a hash of
//! their keys into runs on local disk and each bucket is joined on its
//! own, the table in memory a bucket's rather than a side's.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::{concat_batches, take, take_record_batch};
use arrow::datatypes::{Field, Schema, SchemaRef};
use tessera_ir::{Expr, JoinKind, qualify};
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
    /// The build rows of each key, by the key's group id.
    rows_by_key: Vec<Vec<u32>>,
    probe_keys: Vec<Expr>,
    memory: Arc<MemoryPool>,
    reserved: usize,
}

impl HashJoin {
    /// A join holding `rows`, the `build` input's rows, hashed by its
    /// keys, to be probed by the other input's batches. A left join
    /// builds its right side, since every probe row must come out.
    ///
    /// # Errors
    ///
    /// If the keys do not evaluate or the memory grant does not cover
    /// the build side.
    pub fn new(
        kind: JoinKind,
        build_side: BuildSide,
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
        let rows = match rows.first() {
            Some(first) => concat_batches(&first.schema(), rows)?,
            None => RecordBatch::new_empty(Arc::clone(&build.schema)),
        };
        let reserved = rows.get_array_memory_size() + rows.num_rows() * size_of::<u32>();
        memory.reserve(reserved)?;
        let keys = evaluate_keys(&build.keys, &rows)?;
        let mut table = Grouper::new(keys.iter().map(|k| k.data_type().clone()).collect())?;
        let ids = table.group_ids(&keys)?;
        let mut rows_by_key: Vec<Vec<u32>> = vec![Vec::new(); table.len()];
        for (row, id) in ids.into_iter().enumerate() {
            if keys.iter().any(|k| k.is_null(row)) {
                continue;
            }
            rows_by_key[id as usize].push(u32::try_from(row).unwrap_or(u32::MAX));
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
            memory,
            reserved,
        })
    }

    /// The output schema.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
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
        let mut join = HashJoin::new(kind, build_side, &hashed, build, probe, Arc::clone(memory))?;
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
        let mut probe_rows: Vec<u32> = Vec::new();
        let mut build_rows: Vec<Option<u32>> = Vec::new();
        for (row, key) in self.table.lookup(&keys)?.into_iter().enumerate() {
            let row = u32::try_from(row).unwrap_or(u32::MAX);
            match key {
                Some(id) => {
                    for &matched in &self.rows_by_key[id as usize] {
                        probe_rows.push(row);
                        build_rows.push(Some(matched));
                    }
                }
                None if self.kind == JoinKind::Left => {
                    probe_rows.push(row);
                    build_rows.push(None);
                }
                None => {}
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
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::DataType;

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
            name: "l".into(),
        };
        let owners = JoinInput {
            schema: owner_schema,
            keys: vec![Expr::column("severity")],
            name: "o".into(),
        };
        let memory = MemoryPool::unbounded();
        match build_side {
            BuildSide::Right => {
                HashJoin::new(kind, build_side, &owner_rows, &owners, &logs, memory).unwrap()
            }
            BuildSide::Left => HashJoin::new(
                kind,
                build_side,
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
            name: "l".into(),
        };
        let owners = JoinInput {
            schema: owner_schema,
            keys: vec![Expr::column("severity")],
            name: "o".into(),
        };
        let memory = MemoryPool::unbounded();
        assert!(
            HashJoin::new(
                JoinKind::Left,
                BuildSide::Left,
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
            name: "l".into(),
        };
        let owners = JoinInput {
            schema: owner_schema,
            keys: vec![Expr::column("severity")],
            name: "o".into(),
        };
        for kind in [JoinKind::Inner, JoinKind::Left] {
            let mut whole = HashJoin::new(
                kind,
                BuildSide::Right,
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
}
