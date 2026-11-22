// SPDX-License-Identifier: Apache-2.0

//! Compaction and collection over real storage: L1 files written by the
//! write path become one indexed L2 file, duplicates disappear, the view
//! stays the same, and garbage is collected only when it is safe.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, FixedSizeBinaryArray};
use tessera_compact::{Checkpoints, Compactor, Outcome, Policy, collect_files, collect_log, plan};
use tessera_format::{ParquetFormat, Sidecar};
use tessera_manifest::{
    DatasetLogs, LayoutRecord, Manifest, intake, list_partitions, registry, view,
};
use tessera_object::{MemoryStore, ObjectStore, Pools, layout};
use tessera_types::event::{EVENT_ID, Scalar};
use tessera_types::{
    DatasetName, Env, EventBatch, IndexKind, Kind, Level, LogRow, PoolId, Seq, TenantId, Timestamp,
    Window, WriterId,
};
use tessera_wal::{EntryBody, FlushTarget, WriterLog, reader, write_l1_files};

fn dataset() -> DatasetName {
    DatasetName::new("prod").unwrap()
}

fn batch(ids: &[u8]) -> EventBatch {
    let rows: Vec<LogRow> = ids
        .iter()
        .map(|i| LogRow {
            timestamp: Timestamp::from_nanos(i64::from(*i) * 1_000),
            event_id: [*i; 16],
            severity_number: Some(if i % 2 == 0 { 9 } else { 17 }),
            body: Some(format!("row {i} checkout")),
            trace_id: Some([*i; 16]),
            attributes: BTreeMap::from([("attr.n".to_owned(), Scalar::Int(i64::from(*i)))]),
            ..LogRow::default()
        })
        .collect();
    EventBatch::from_logs(TenantId::DEFAULT, dataset(), &rows).unwrap()
}

struct Node {
    store: Arc<MemoryStore>,
    env: Env,
    pools: Arc<Pools>,
    log: WriterLog,
}

/// A node with `segments` flushed as one L1 file each and adopted.
async fn node(segments: &[&[u8]], start: Timestamp) -> Node {
    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, start);
    let mut log = WriterLog::start(store.clone(), env.clone());
    registry::register(
        &*store,
        &env,
        &TenantId::DEFAULT,
        &dataset(),
        log.writer_id(),
    )
    .await
    .unwrap();
    let target = FlushTarget {
        store: store.clone(),
        pool: PoolId::new("root").unwrap(),
        format: Arc::new(ParquetFormat::default()),
    };
    let mut after = Seq::ZERO;
    for ids in segments {
        let through = log.append_segment(&[batch(ids)]).await.unwrap();
        let files = write_l1_files(
            &target,
            &env,
            log.writer_id(),
            after,
            through,
            Window::DEFAULT_LENGTH,
            vec![batch(ids)],
        )
        .await
        .unwrap();
        log.append(EntryBody::Flush {
            tenant: TenantId::DEFAULT,
            dataset: dataset(),
            after,
            through,
            files,
        })
        .await
        .unwrap();
        after = through;
    }
    intake::run(
        store.clone(),
        &env,
        &TenantId::DEFAULT,
        &dataset(),
        log.writer_id(),
    )
    .await
    .unwrap();
    let pools = Arc::new(Pools::new(PoolId::new("root").unwrap(), store.clone()));
    Node {
        store,
        env,
        pools,
        log,
    }
}

/// CT-7: compaction places a tenant's files in the pool its settings
/// name and leaves pattern mining out when the tenant switched it off.
#[tokio::test(flavor = "multi_thread")]
async fn a_tenants_files_go_to_its_pool_and_its_switches_hold() {
    use tessera_catalog::{Features, TenantSettings, TenantSettingsSource};

    struct Cold;

    impl TenantSettingsSource for Cold {
        fn settings_of(&self, _: &TenantId) -> TenantSettings {
            TenantSettings {
                storage_pool: Some(PoolId::new("cold").unwrap()),
                features: Features {
                    patterns: Some(false),
                    ..Features::default()
                },
                ..TenantSettings::default()
            }
        }
    }

    let node = node(
        &[&[1, 2], &[3, 4], &[5, 6], &[7, 8]],
        Timestamp::from_nanos(0),
    )
    .await;
    let cold = Arc::new(MemoryStore::new());
    let mut pools = Pools::new(PoolId::new("root").unwrap(), node.store.clone());
    pools
        .add(PoolId::new("cold").unwrap(), cold.clone(), 1)
        .unwrap();
    let pools = Arc::new(pools);
    let manifest = manifest(&node).await;
    let before = manifest.snapshot().await.unwrap();
    let job = plan(
        manifest.id(),
        before.state(),
        node.env.clock.now(),
        &Policy::default(),
    )
    .unwrap();
    let compactor = Compactor::new(
        Arc::clone(&pools),
        Arc::new(ParquetFormat::default()),
        node.env.clone(),
    )
    .with_tenants(Arc::new(Cold));
    let outcome = compactor
        .run(
            &manifest,
            &job,
            &LayoutRecord::default_for(job.partition.kind),
            None,
            &[],
        )
        .await
        .unwrap();
    let Outcome::Replaced { output, .. } = outcome else {
        panic!("expected a replacement, got {outcome:?}");
    };
    let after = manifest.snapshot().await.unwrap();
    let file = after.state().file(output).unwrap().clone();
    assert_eq!(file.pool.as_str(), "cold", "placed in the tenant's pool");
    let key = layout::data_file(&file.partition, file.file_id);
    assert!(
        cold.get(&key).await.is_ok(),
        "the file is in the cold store"
    );
    assert!(
        !file.columns.iter().any(|c| c.name == "pattern_id"),
        "no pattern column: {:?}",
        file.columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
    );
    let format: Arc<dyn tessera_format::FileFormat> = Arc::new(ParquetFormat::default());
    let view = view::read(
        &pools,
        &format,
        &node.env,
        &TenantId::DEFAULT,
        &dataset(),
        Kind::Log,
        Window::DEFAULT_LENGTH,
    )
    .await
    .unwrap();
    assert_eq!(
        view.rows
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        8
    );
}

async fn manifest(node: &Node) -> Manifest {
    let partitions = list_partitions(&*node.store, &TenantId::DEFAULT, &dataset(), Kind::Log)
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    let store: Arc<dyn ObjectStore> = node.store.clone();
    Manifest::open(store, partitions[0].clone(), node.env.clone())
}

async fn event_ids(node: &Node) -> Vec<u8> {
    let format: Arc<dyn tessera_format::FileFormat> = Arc::new(ParquetFormat::default());
    let read = view::read(
        &node.pools,
        &format,
        &node.env,
        &TenantId::DEFAULT,
        &dataset(),
        Kind::Log,
        Window::DEFAULT_LENGTH,
    )
    .await
    .unwrap();
    let mut ids: Vec<u8> = read
        .rows
        .iter()
        .flat_map(|b| {
            let column = b
                .column_by_name(EVENT_ID)
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .clone();
            (0..column.len()).map(move |i| column.value(i)[0])
        })
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn l1_files_merge_into_one_indexed_l2_file_without_duplicates() {
    let node = node(
        &[&[1, 2, 3], &[3, 4], &[5, 6, 1], &[7]],
        Timestamp::UNIX_EPOCH,
    )
    .await;
    let manifest = manifest(&node).await;
    let before = manifest.snapshot().await.unwrap();
    assert_eq!(before.state().files().count(), 4);
    assert_eq!(
        event_ids(&node).await,
        vec![1, 1, 2, 3, 3, 4, 5, 6, 7],
        "duplicates are visible before compaction (IN-12)"
    );

    let job = plan(
        manifest.id(),
        before.state(),
        node.env.clock.now(),
        &Policy::default(),
    )
    .unwrap();
    assert_eq!(job.inputs.len(), 4);
    let compactor = Compactor::new(
        node.pools.clone(),
        Arc::new(ParquetFormat::default()),
        node.env.clone(),
    );
    let outcome = compactor
        .run(
            &manifest,
            &job,
            &LayoutRecord::default_for(job.partition.kind),
            None,
            &[],
        )
        .await
        .unwrap();
    let Outcome::Replaced {
        output,
        rows,
        duplicates,
    } = outcome
    else {
        panic!("expected a replacement, got {outcome:?}");
    };
    assert_eq!((rows, duplicates), (7, 2));

    let after = manifest.snapshot().await.unwrap();
    let files: Vec<_> = after.state().files().collect();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].file_id, output);
    assert_eq!(files[0].level, Level::L2);
    assert_eq!(
        files[0].sort_key,
        vec!["resource.service.name", "severity_number", "timestamp"]
    );
    assert!(
        files[0]
            .columns
            .iter()
            .any(|c| c.name == "resource.service.name"),
        "the service is a column of its own even where no row has it"
    );
    assert!(files[0].has_index(IndexKind::Term, "body"));
    assert!(files[0].has_index(IndexKind::Trigram, "body"));
    assert!(files[0].has_index(IndexKind::Bloom, "trace_id"));
    assert_eq!(after.state().tombstones().count(), 4);
    assert_eq!(event_ids(&node).await, vec![1, 2, 3, 4, 5, 6, 7]);

    let sidecar = node
        .store
        .get(&layout::sidecar(manifest.id(), output))
        .await
        .unwrap();
    let sidecar = Sidecar::decode(sidecar).unwrap();
    assert!(
        sidecar
            .row_groups_with_tokens("body", "checkout")
            .unwrap()
            .to_vec()
            .contains(&0)
    );
    assert_eq!(sidecar.may_contain("trace_id", &[7u8; 16]), Some(true));
    assert_eq!(sidecar.may_contain("trace_id", &[200u8; 16]), Some(false));

    assert!(
        plan(
            manifest.id(),
            after.state(),
            node.env.clock.now(),
            &Policy::default()
        )
        .is_none(),
        "nothing left to compact"
    );
}

/// A layout may name what the rows have no column for: a label, which
/// is read from several columns and was adopted as a sort key from the
/// predicates of a workload. Such a key orders nothing in the file;
/// refused, it stopped the partition's compaction for good, and the
/// tenant's round with it. A path no row carries is a column all the
/// same, promoted empty, and stays in the key.
#[tokio::test]
async fn a_sort_key_the_rows_have_no_column_for_is_left_out() {
    let node = node(&[&[1, 2], &[3], &[4], &[5]], Timestamp::UNIX_EPOCH).await;
    let manifest = manifest(&node).await;
    let before = manifest.snapshot().await.unwrap();
    let job = plan(
        manifest.id(),
        before.state(),
        node.env.clock.now(),
        &Policy::default(),
    )
    .unwrap();
    let record = LayoutRecord {
        sort_key: vec![
            "label:run".into(),
            "attr.absent".into(),
            "severity_number".into(),
        ],
        ..LayoutRecord::default_for(job.partition.kind)
    };
    let compactor = Compactor::new(
        node.pools.clone(),
        Arc::new(ParquetFormat::default()),
        node.env.clone(),
    );
    let outcome = compactor
        .run(&manifest, &job, &record, None, &[])
        .await
        .unwrap();
    assert!(
        matches!(outcome, Outcome::Replaced { rows: 5, .. }),
        "{outcome:?}"
    );

    let after = manifest.snapshot().await.unwrap();
    let files: Vec<_> = after.state().files().collect();
    assert_eq!(files.len(), 1);
    assert_eq!(
        files[0].sort_key,
        vec!["attr.absent", "severity_number", "timestamp"],
        "the file says what it is sorted by"
    );
    assert_eq!(files[0].layout_epoch, record.epoch);
    assert_eq!(event_ids(&node).await, vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn a_superseded_job_leaves_the_manifest_alone() {
    let node = node(&[&[1], &[2], &[3], &[4]], Timestamp::UNIX_EPOCH).await;
    let manifest = manifest(&node).await;
    let state = manifest.snapshot().await.unwrap();
    let job = plan(
        manifest.id(),
        state.state(),
        node.env.clock.now(),
        &Policy::default(),
    )
    .unwrap();
    let compactor = Compactor::new(
        node.pools.clone(),
        Arc::new(ParquetFormat::default()),
        node.env.clone(),
    );
    assert!(matches!(
        compactor
            .run(
                &manifest,
                &job,
                &LayoutRecord::default_for(job.partition.kind),
                None,
                &[],
            )
            .await
            .unwrap(),
        Outcome::Replaced { .. }
    ));
    // The same job again: its inputs are gone.
    assert_eq!(
        compactor
            .run(
                &manifest,
                &job,
                &LayoutRecord::default_for(job.partition.kind),
                None,
                &[],
            )
            .await
            .unwrap(),
        Outcome::Superseded
    );
    let after = manifest.snapshot().await.unwrap();
    assert_eq!(after.state().files().count(), 1);
    assert_eq!(event_ids(&node).await, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn removed_files_are_deleted_after_the_retention_window() {
    let node = node(&[&[1], &[2], &[3], &[4]], Timestamp::UNIX_EPOCH).await;
    let manifest = manifest(&node).await;
    let state = manifest.snapshot().await.unwrap();
    let inputs: Vec<_> = state.state().files().map(|f| f.file_id).collect();
    let job = plan(
        manifest.id(),
        state.state(),
        node.env.clock.now(),
        &Policy::default(),
    )
    .unwrap();
    Compactor::new(
        node.pools.clone(),
        Arc::new(ParquetFormat::default()),
        node.env.clone(),
    )
    .run(
        &manifest,
        &job,
        &LayoutRecord::default_for(job.partition.kind),
        None,
        &[],
    )
    .await
    .unwrap();
    let retention = Duration::from_secs(24 * 3_600);
    let too_early = node
        .env
        .clock
        .now()
        .checked_add(Duration::from_secs(3_600))
        .unwrap();
    assert_eq!(
        collect_files(&manifest, &node.pools, too_early, retention)
            .await
            .unwrap(),
        0
    );
    for id in &inputs {
        node.store
            .head(&layout::data_file(manifest.id(), *id))
            .await
            .unwrap();
    }
    let late_enough = node.env.clock.now().checked_add(retention).unwrap();
    assert_eq!(
        collect_files(&manifest, &node.pools, late_enough, retention)
            .await
            .unwrap(),
        4
    );
    for id in &inputs {
        assert!(
            node.store
                .head(&layout::data_file(manifest.id(), *id))
                .await
                .is_err()
        );
    }
    assert_eq!(
        manifest
            .snapshot()
            .await
            .unwrap()
            .state()
            .tombstones()
            .count(),
        0
    );
    assert_eq!(event_ids(&node).await, vec![1, 2, 3, 4]);
}

/// CO-7: a trace does not stop at a partition's window, and an alert
/// that names one is in no span. The traces a sampling is to spare
/// are read from the error spans of other span files and from the
/// alert events: the ids, of the rows that qualify, and nothing else.
#[tokio::test]
async fn the_traces_to_spare_are_read_from_error_spans_and_alerts() {
    use tessera_types::event::EventRow;
    use tessera_types::{SpanRow, StatusCode};

    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
    let log = WriterLog::start(store.clone(), env.clone());
    let format: Arc<dyn tessera_format::FileFormat> = Arc::new(ParquetFormat::default());
    let target = FlushTarget {
        store: store.clone(),
        pool: PoolId::new("root").unwrap(),
        format: Arc::clone(&format),
    };
    // Four traces of three spans; one span of trace 2 fails.
    let spans: Vec<SpanRow> = (0..12u8)
        .map(|i| SpanRow {
            timestamp: Timestamp::from_nanos(i64::from(i)),
            event_id: [i; 16],
            trace_id: [i / 3; 16],
            span_id: [i; 8],
            name: "op".into(),
            status_code: if i == 7 {
                StatusCode::Error
            } else {
                StatusCode::Ok
            },
            ..SpanRow::default()
        })
        .collect();
    // Two alerts, one of which names a trace.
    let alerts: Vec<EventRow> = [Some([9u8; 16]), None]
        .into_iter()
        .zip(0u8..)
        .map(|(trace_id, i)| EventRow {
            timestamp: Timestamp::from_nanos(i64::from(i)),
            observed_timestamp: Timestamp::from_nanos(i64::from(i)),
            event_id: [100 + i; 16],
            trace_id,
            span_id: None,
            name: "HighErrorRate".into(),
            attributes: BTreeMap::new(),
        })
        .collect();
    let written = |batch: EventBatch| {
        let (target, env, writer) = (&target, &env, log.writer_id());
        async move {
            write_l1_files(
                target,
                env,
                writer,
                Seq::ZERO,
                Seq::new(1),
                Window::DEFAULT_LENGTH,
                vec![batch],
            )
            .await
            .unwrap()
        }
    };
    let span_files =
        written(EventBatch::from_spans(TenantId::DEFAULT, dataset(), &spans).unwrap()).await;
    let alert_files =
        written(EventBatch::from_alerts(TenantId::DEFAULT, dataset(), &alerts).unwrap()).await;
    assert_eq!((span_files.len(), alert_files.len()), (1, 1));

    let pools = Pools::new(PoolId::new("root").unwrap(), store.clone());
    let spared = tessera_compact::spared::traces(&pools, &format, &span_files, &alert_files)
        .await
        .unwrap();
    assert_eq!(
        spared,
        tessera_compact::Spared::from([[2; 16], [9; 16]]),
        "the trace with an error span and the one an alert names"
    );
    let none = tessera_compact::spared::traces(&pools, &format, &[], &[])
        .await
        .unwrap();
    assert!(none.is_empty());
}

const GRACE: Duration = Duration::from_secs(3_600);

/// A collection pass over the node's writer log at `now`, with no rule
/// group holding a position.
async fn collect(node: &Node, now: Timestamp) -> tessera_compact::LogCollection {
    collect_held(node, now, &Checkpoints::none()).await
}

/// A collection pass over the node's writer log at `now` with the
/// positions rule groups hold.
async fn collect_held(
    node: &Node,
    now: Timestamp,
    held: &Checkpoints,
) -> tessera_compact::LogCollection {
    let store: Arc<dyn ObjectStore> = node.store.clone();
    let datasets = DatasetLogs::new(Arc::clone(&store), node.env.clone());
    collect_log(
        &store,
        &node.env,
        node.log.writer_id(),
        now,
        GRACE,
        &datasets,
        held,
    )
    .await
    .unwrap()
}

/// Flushes the tail segment `(3, 5]` holding row 9 and adopts it.
async fn flush_tail(node: &mut Node) {
    let target = FlushTarget {
        store: node.store.clone(),
        pool: PoolId::new("root").unwrap(),
        format: Arc::new(ParquetFormat::default()),
    };
    let writer = node.log.writer_id();
    let files = write_l1_files(
        &target,
        &node.env,
        writer,
        Seq::new(3),
        Seq::new(5),
        Window::DEFAULT_LENGTH,
        vec![batch(&[9])],
    )
    .await
    .unwrap();
    node.log
        .append(EntryBody::Flush {
            tenant: TenantId::DEFAULT,
            dataset: dataset(),
            after: Seq::new(3),
            through: Seq::new(5),
            files,
        })
        .await
        .unwrap();
    intake::run(
        node.store.clone(),
        &node.env,
        &TenantId::DEFAULT,
        &dataset(),
        writer,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn adopted_segments_are_collected_after_the_grace_period() {
    let mut node = node(&[&[1, 2], &[3]], Timestamp::UNIX_EPOCH).await;
    // A third segment stays unadopted in the tail.
    node.log.append_segment(&[batch(&[9])]).await.unwrap();
    let writer: WriterId = node.log.writer_id();
    let later = node.env.clock.now().checked_add(2 * GRACE).unwrap();

    // Segments 1 and 3 and flush records 2 and 4 go; segment 5 stays.
    let collected = collect(&node, later).await;
    assert_eq!((collected.deleted, collected.log_gone), (4, false));
    assert_eq!(
        reader::list(&*node.store, writer).await.unwrap(),
        vec![Seq::new(5)]
    );
    assert_eq!(
        event_ids(&node).await,
        vec![1, 2, 3, 9],
        "the view still sees every row"
    );

    // Adopting the tail segment does not make it collectable while it is
    // the newest entry of a live log; its flush record becomes the newest.
    flush_tail(&mut node).await;
    let collected = collect(&node, later).await;
    assert_eq!(
        collected.deleted, 1,
        "segment 5 goes, its flush record stays"
    );
    assert_eq!(
        reader::list(&*node.store, writer).await.unwrap(),
        vec![Seq::new(6)]
    );

    // Not yet aged: nothing goes.
    let soon = node
        .env
        .clock
        .now()
        .checked_add(Duration::from_secs(1))
        .unwrap();
    assert_eq!(collect(&node, soon).await.deleted, 0);
}

/// GC-2 (c): an adopted entry a rule group has not read stays until the
/// group reads past it, or until the hard cap, after which the group
/// rebuilds its windows from the files instead (section 11.7).
#[tokio::test]
async fn an_entry_a_rule_group_has_not_read_stays_until_the_cap() {
    let mut node = node(&[&[1, 2], &[3]], Timestamp::UNIX_EPOCH).await;
    node.log.append_segment(&[batch(&[9])]).await.unwrap();
    let writer: WriterId = node.log.writer_id();
    let mut held = Checkpoints::none();
    held.hold(writer, Seq::new(1));

    let later = node.env.clock.now().checked_add(2 * GRACE).unwrap();
    let collected = collect_held(&node, later, &held).await;
    assert_eq!(
        collected.deleted, 1,
        "segment 1 was read; flush record 2 is the first entry the group has not"
    );
    assert_eq!(
        reader::list(&*node.store, writer).await.unwrap(),
        vec![Seq::new(2), Seq::new(3), Seq::new(4), Seq::new(5)]
    );

    let past_cap = later.checked_add(Checkpoints::DEFAULT_CAP).unwrap();
    let collected = collect_held(&node, past_cap, &held).await;
    assert_eq!(collected.deleted, 3, "the cap passed: only the tail stays");
    assert_eq!(
        reader::list(&*node.store, writer).await.unwrap(),
        vec![Seq::new(5)]
    );
}

#[tokio::test]
async fn an_ended_and_adopted_log_is_removed_whole() {
    let mut node = node(&[&[1, 2], &[3]], Timestamp::UNIX_EPOCH).await;
    node.log.append_segment(&[batch(&[9])]).await.unwrap();
    let writer: WriterId = node.log.writer_id();
    flush_tail(&mut node).await;
    node.log.end().await.unwrap();
    intake::run(
        node.store.clone(),
        &node.env,
        &TenantId::DEFAULT,
        &dataset(),
        writer,
    )
    .await
    .unwrap();
    let later = node.env.clock.now().checked_add(2 * GRACE).unwrap();
    let collected = collect(&node, later).await;
    assert!(collected.log_gone, "{collected:?}");
    assert!(reader::list(&*node.store, writer).await.unwrap().is_empty());
    assert_eq!(event_ids(&node).await, vec![1, 2, 3, 9]);
}
