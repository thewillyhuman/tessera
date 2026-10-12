// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Leaf tasks shared with peers: files assigned to another node are read
//! there (QE-15), a peer that fails is read around (QE-17), and the rows
//! are the same either way. A grouped aggregate, a join of two scans
//! and a trace group fold on the workers over an exchange of their
//! partial outputs (QE-14), and fold here when a worker or a kept
//! output is lost.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use futures::future::BoxFuture;
use tessera_exec::{
    CpuPool, Engine, FinalTask, LeafTask, Peer, Peers, QueryLimits, RemoteError, RemoteScan,
    StageAnalysis, StageTask,
};
use tessera_format::ParquetFormat;
use tessera_ir::{
    AggregateExpr, BinaryOp, Expr, JoinKind, JoinSide, NamedExpr, Plan, Registry, Relation, Scan,
    SortKey, Spanset, Structural,
};
use tessera_manifest::{intake, registry};
use tessera_object::{MemoryStore, Pools};
use tessera_opt::optimize;
use tessera_types::event::{NAME, Scalar};
use tessera_types::{
    DatasetName, Env, EventBatch, FileId, Kind, LogRow, NodeId, PoolId, SeededRandom, Seq, SpanRow,
    TenantId, Timestamp, Window,
};
use tessera_wal::{EntryBody, FlushTarget, WriterLog, write_l1_files};

const DAY: i64 = 86_400_000_000_000;

fn dataset() -> DatasetName {
    DatasetName::new("prod").unwrap()
}

fn batch(day: i64, from: u8, count: u8) -> EventBatch {
    let rows: Vec<LogRow> = (from..from + count)
        .map(|i| LogRow {
            timestamp: Timestamp::from_nanos(day * DAY + i64::from(i) * 1_000),
            event_id: [i; 16],
            body: Some(format!("row{i}")),
            attributes: BTreeMap::from([("attr.n".to_owned(), Scalar::Int(i64::from(i)))]),
            ..LogRow::default()
        })
        .collect();
    EventBatch::from_logs(TenantId::DEFAULT, dataset(), &rows).unwrap()
}

/// Four files: two on day 0, two on day 1.
async fn populate() -> (Arc<MemoryStore>, Env) {
    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
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
    for segment in [
        vec![batch(0, 0, 5)],
        vec![batch(0, 10, 5)],
        vec![batch(1, 20, 5)],
        vec![batch(1, 30, 5)],
    ] {
        let through = log.append_segment(&segment).await.unwrap();
        let files = write_l1_files(
            &target,
            &env,
            log.writer_id(),
            after,
            through,
            Window::DEFAULT_LENGTH,
            segment,
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
    (store, env)
}

fn engine(store: &Arc<MemoryStore>, env: &Env) -> Engine {
    let pools = Pools::new(PoolId::new("root").unwrap(), store.clone());
    Engine::new(
        Arc::new(pools),
        Arc::new(ParquetFormat::default()),
        env.clone(),
        CpuPool::new(2),
        QueryLimits::default(),
    )
}

/// Assigns every other file to the peer.
struct HalfToPeer {
    peer: Peer,
}

impl Peers for HalfToPeer {
    fn owner_of(&self, file: FileId) -> Option<Peer> {
        file.to_string().as_bytes()[0]
            .is_multiple_of(2)
            .then(|| self.peer.clone())
    }
}

/// A peer that runs tasks on its own engine over the same store, and
/// counts them; it fails when told to.
struct PeerEngine {
    engine: Engine,
    tasks: Mutex<Vec<LeafTask>>,
    stages: Mutex<Vec<StageTask>>,
    fail: bool,
}

impl PeerEngine {
    fn new(engine: Engine, fail: bool) -> Arc<Self> {
        Arc::new(PeerEngine {
            engine,
            tasks: Mutex::new(Vec::new()),
            stages: Mutex::new(Vec::new()),
            fail,
        })
    }
}

impl RemoteScan for PeerEngine {
    fn leaf<'a>(
        &'a self,
        _peer: &'a Peer,
        task: &'a LeafTask,
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, RemoteError>> {
        Box::pin(async move {
            self.tasks.lock().unwrap().push(task.clone());
            if self.fail {
                return Err(RemoteError("peer down".into()));
            }
            self.engine
                .leaf(task)
                .await
                .map_err(|e| RemoteError(e.to_string()))
        })
    }

    fn stage<'a>(
        &'a self,
        _peer: &'a Peer,
        task: &'a StageTask,
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, RemoteError>> {
        Box::pin(async move {
            self.stages.lock().unwrap().push(task.clone());
            if self.fail {
                return Err(RemoteError("peer down".into()));
            }
            self.engine
                .stage(task)
                .await
                .map_err(|e| RemoteError(e.to_string()))
        })
    }
}

fn peer() -> Peer {
    peer_seeded(7)
}

fn peer_seeded(seed: u64) -> Peer {
    let mut random = SeededRandom::new(seed);
    Peer {
        node: NodeId::random(&mut random),
        http: format!("http://peer{seed}"),
    }
}

/// Files spread over this node and two peers by their id.
struct ThreeWay {
    peers: [Peer; 2],
}

impl Peers for ThreeWay {
    fn owner_of(&self, file: FileId) -> Option<Peer> {
        match file.to_string().as_bytes()[1] % 3 {
            0 => None,
            1 => Some(self.peers[0].clone()),
            _ => Some(self.peers[1].clone()),
        }
    }
}

/// A transport between in-process engines, one per node: stages keep
/// their outputs on the peer, partitions are fetched from it and final
/// stages run there, unless the peer is told to fail at one of them.
#[derive(Default)]
struct Mesh {
    engines: Mutex<HashMap<String, Arc<Engine>>>,
    fail_final: Mutex<Vec<String>>,
    fail_fetch: Mutex<Vec<String>>,
    calls: Mutex<Vec<String>>,
}

impl Mesh {
    fn add(&self, peer: &Peer, engine: Arc<Engine>) {
        self.engines
            .lock()
            .unwrap()
            .insert(peer.node.to_string(), engine);
    }

    fn engine(&self, peer: &Peer) -> Result<Arc<Engine>, RemoteError> {
        self.engines
            .lock()
            .unwrap()
            .get(&peer.node.to_string())
            .cloned()
            .ok_or_else(|| RemoteError(format!("no such node {}", peer.node)))
    }

    fn record(&self, call: &str, peer: &Peer) {
        self.calls
            .lock()
            .unwrap()
            .push(format!("{call} {}", peer.http));
    }

    fn told_to_fail(list: &Mutex<Vec<String>>, peer: &Peer) -> bool {
        list.lock().unwrap().contains(&peer.node.to_string())
    }

    fn calls(&self, call: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.starts_with(call))
            .count()
    }
}

impl RemoteScan for Mesh {
    fn leaf<'a>(
        &'a self,
        peer: &'a Peer,
        task: &'a LeafTask,
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, RemoteError>> {
        Box::pin(async move {
            self.engine(peer)?
                .leaf(task)
                .await
                .map_err(|e| RemoteError(e.to_string()))
        })
    }

    fn stage<'a>(
        &'a self,
        peer: &'a Peer,
        task: &'a StageTask,
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, RemoteError>> {
        Box::pin(async move {
            self.record("stage", peer);
            self.engine(peer)?
                .stage(task)
                .await
                .map_err(|e| RemoteError(e.to_string()))
        })
    }

    fn supports_exchange(&self) -> bool {
        true
    }

    fn fetch<'a>(
        &'a self,
        peer: &'a Peer,
        id: &'a str,
        partition: u32,
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, RemoteError>> {
        Box::pin(async move {
            self.record("fetch", peer);
            if Self::told_to_fail(&self.fail_fetch, peer) {
                return Err(RemoteError("peer lost its output".into()));
            }
            self.engine(peer)?
                .fetch(id, partition)
                .map(|(_, rows)| rows)
                .ok_or_else(|| RemoteError("nothing kept".into()))
        })
    }

    fn finalize<'a>(
        &'a self,
        peer: &'a Peer,
        task: &'a FinalTask,
        rows: &'a [Vec<RecordBatch>],
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>, RemoteError>> {
        Box::pin(async move {
            self.record("final", peer);
            if Self::told_to_fail(&self.fail_final, peer) {
                return Err(RemoteError("peer down".into()));
            }
            self.engine(peer)?
                .finalize(task, rows.to_vec())
                .await
                .map_err(|e| RemoteError(e.to_string()))
        })
    }
}

/// A coordinator with two peers over `mesh`, every file assigned to
/// one of the three.
fn three_nodes(store: &Arc<MemoryStore>, env: &Env, mesh: &Arc<Mesh>) -> (Engine, [Peer; 2]) {
    let peers = [peer_seeded(7), peer_seeded(8)];
    let view = Arc::new(ThreeWay {
        peers: peers.clone(),
    });
    for peer in &peers {
        let worker = engine(store, env).with_peers(
            Arc::clone(&view) as Arc<dyn Peers>,
            Arc::clone(mesh) as Arc<dyn RemoteScan>,
        );
        mesh.add(peer, Arc::new(worker));
    }
    let coordinator = engine(store, env).with_peers(view, Arc::clone(mesh) as Arc<dyn RemoteScan>);
    (coordinator, peers)
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[tokio::test]
async fn files_assigned_to_a_peer_are_read_there() {
    let (store, env) = populate().await;
    let remote = PeerEngine::new(engine(&store, &env), false);
    let engine = engine(&store, &env).with_peers(
        Arc::new(HalfToPeer { peer: peer() }),
        Arc::clone(&remote) as Arc<dyn RemoteScan>,
    );
    let scan = Scan::all(TenantId::DEFAULT, dataset(), Kind::Log);
    let plan = optimize(Plan::Scan(scan), &Registry::builtin()).unwrap();
    let output = engine.run(&plan).await.unwrap();
    assert_eq!(output.num_rows(), 20);
    let scanned = &output.analysis.scans[0];
    let tasks = remote.tasks.lock().unwrap();
    assert_eq!(scanned.remote_tasks, 1, "one task per peer");
    assert_eq!(scanned.files_remote, tasks[0].files.len());
    assert!(
        scanned.files_remote > 0 && scanned.files_remote < 4,
        "{scanned:?}"
    );
    assert_eq!(scanned.files_read + scanned.files_remote, 4);
    assert_eq!(scanned.remote_failed, 0);
    assert_eq!(scanned.rows_from_files, 20);
}

#[tokio::test]
async fn a_failing_peer_is_read_around() {
    let (store, env) = populate().await;
    let remote = PeerEngine::new(engine(&store, &env), true);
    let engine = engine(&store, &env).with_peers(
        Arc::new(HalfToPeer { peer: peer() }),
        Arc::clone(&remote) as Arc<dyn RemoteScan>,
    );
    let scan = Scan::all(TenantId::DEFAULT, dataset(), Kind::Log);
    let plan = optimize(Plan::Scan(scan), &Registry::builtin()).unwrap();
    let output = engine.run(&plan).await.unwrap();
    assert_eq!(output.num_rows(), 20, "the same rows, read here");
    let scanned = &output.analysis.scans[0];
    assert_eq!(scanned.remote_failed, 1);
    assert_eq!(scanned.files_remote, 0);
    assert_eq!(scanned.files_read, 4);

    // A newest-first limit stays local: stopping early needs one reader.
    let mut scan = Scan::all(TenantId::DEFAULT, dataset(), Kind::Log);
    scan.newest_first_limit = Some(3);
    let plan = optimize(Plan::Scan(scan), &Registry::builtin()).unwrap();
    let output = engine.run(&plan).await.unwrap();
    assert_eq!(output.analysis.scans[0].remote_tasks, 0);
    assert!(total_rows(&output.batches) >= 3);
}

/// The rows of `batches` as text, sorted, for comparing results whose
/// row order is unspecified.
fn sorted_rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(|i| {
                b.columns()
                    .iter()
                    .map(|c| arrow::util::display::array_value_to_string(c, i).unwrap())
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect();
    rows.sort();
    rows
}

fn grouped_plan() -> Plan {
    let scan = Scan::all(TenantId::DEFAULT, dataset(), Kind::Log);
    let plan = Plan::Aggregate {
        input: Box::new(Plan::Scan(scan)),
        group_by: vec![NamedExpr::new(
            Expr::column("timestamp").binary(
                tessera_ir::BinaryOp::Lt,
                Expr::literal(Timestamp::from_nanos(DAY)),
            ),
            "first_day",
        )],
        aggregates: vec![
            AggregateExpr {
                function: "count".into(),
                args: vec![],
                name: "n".into(),
            },
            AggregateExpr {
                function: "max".into(),
                args: vec![Expr::column("body")],
                name: "last".into(),
            },
            AggregateExpr {
                function: "avg".into(),
                args: vec![Expr::column("sample_rate")],
                name: "mean".into(),
            },
        ],
    };
    optimize(plan, &Registry::builtin()).unwrap()
}

/// QE-14: an aggregate's partial half runs on the peer over its files,
/// its states come back, and the result is what one node gives.
#[tokio::test]
async fn an_aggregate_runs_its_partial_stage_on_the_peers() {
    let (store, env) = populate().await;
    let alone = engine(&store, &env);
    let expected = alone.run(&grouped_plan()).await.unwrap();
    assert!(expected.analysis.stages.is_empty(), "no peers, no stages");

    let remote = PeerEngine::new(engine(&store, &env), false);
    let engine = engine(&store, &env).with_peers(
        Arc::new(HalfToPeer { peer: peer() }),
        Arc::clone(&remote) as Arc<dyn RemoteScan>,
    );
    let output = engine.run(&grouped_plan()).await.unwrap();
    assert_eq!(sorted_rows(&output.batches), sorted_rows(&expected.batches));
    assert_eq!(output.num_rows(), 2, "one group per day");
    assert_eq!(
        output.analysis.stages,
        vec![StageAnalysis {
            kind: "aggregate",
            remote: 1,
            failed: 0,
            ..StageAnalysis::default()
        }]
    );
    let stages = remote.stages.lock().unwrap();
    assert_eq!(stages.len(), 1);
    assert!(
        matches!(stages[0].plan, Plan::PartialAggregate { .. }),
        "{}",
        stages[0].plan
    );
    assert!(
        remote.tasks.lock().unwrap().is_empty(),
        "no plain leaf task"
    );
    let scanned = &output.analysis.scans[0];
    assert_eq!(scanned.files_remote, stages[0].files.len());
    assert!(scanned.files_remote > 0 && scanned.files_read + scanned.files_remote == 4);
    assert!(
        tessera_exec::explain_text(&grouped_plan(), Some(&output.analysis))
            .contains("Stage aggregate: partial on 1 peers, 0 run here after a failure")
    );
}

/// QE-14, QE-17: a peer that cannot run its stage has its share run on
/// the coordinator, and the result does not change.
#[tokio::test]
async fn a_failing_peer_has_its_stage_run_here() {
    let (store, env) = populate().await;
    let expected = engine(&store, &env).run(&grouped_plan()).await.unwrap();
    let remote = PeerEngine::new(engine(&store, &env), true);
    let engine = engine(&store, &env).with_peers(
        Arc::new(HalfToPeer { peer: peer() }),
        Arc::clone(&remote) as Arc<dyn RemoteScan>,
    );
    let output = engine.run(&grouped_plan()).await.unwrap();
    assert_eq!(sorted_rows(&output.batches), sorted_rows(&expected.batches));
    assert_eq!(output.analysis.stages[0].failed, 1);
    assert_eq!(output.analysis.scans[0].files_read, 4);
}

/// QE-14: a top-k and a limit run on every share and once more over
/// the union; an aggregate over a limit stays whole, since its input
/// cannot be split.
#[tokio::test]
async fn top_k_and_limits_split_and_an_aggregate_over_a_limit_does_not() {
    let (store, env) = populate().await;
    let scan = || Plan::Scan(Scan::all(TenantId::DEFAULT, dataset(), Kind::Log));
    let registry = Registry::builtin();
    let top = optimize(
        scan().top_k(
            vec![SortKey::descending(Expr::column("timestamp"))],
            3,
            vec![],
        ),
        &registry,
    )
    .unwrap();
    // Unoptimized: the optimizer turns a bare limit into a newest-first
    // scan, which stays on one node.
    let limited = scan().limit(0, Some(5));
    let counted_after_limit = optimize(
        Plan::Aggregate {
            input: Box::new(scan().limit(0, Some(5))),
            group_by: vec![],
            aggregates: vec![AggregateExpr {
                function: "count".into(),
                args: vec![],
                name: "n".into(),
            }],
        },
        &registry,
    )
    .unwrap();
    let alone = engine(&store, &env);
    let remote = PeerEngine::new(engine(&store, &env), false);
    let engine = engine(&store, &env).with_peers(
        Arc::new(HalfToPeer { peer: peer() }),
        Arc::clone(&remote) as Arc<dyn RemoteScan>,
    );

    let expected = alone.run(&top).await.unwrap();
    let output = engine.run(&top).await.unwrap();
    assert_eq!(sorted_rows(&output.batches), sorted_rows(&expected.batches));
    assert_eq!(output.num_rows(), 3);
    assert_eq!(output.analysis.stages[0].kind, "top-k");

    let output = engine.run(&limited).await.unwrap();
    assert_eq!(output.num_rows(), 5);
    assert_eq!(output.analysis.stages[0].kind, "limit");

    let output = engine.run(&counted_after_limit).await.unwrap();
    assert_eq!(
        sorted_rows(&output.batches),
        vec!["5.0".to_owned()],
        "a weighted count"
    );
    assert!(
        output.analysis.stages.is_empty(),
        "{:?}",
        output.analysis.stages
    );
    assert_eq!(remote.stages.lock().unwrap().len(), 2);
}

/// QE-14: with a transport that keeps stage outputs, a grouped
/// aggregate's final half runs on the workers, each over one partition
/// of the groups fetched from the others, and the coordinator folds
/// only its own partition.
#[tokio::test]
async fn a_grouped_aggregate_folds_on_the_workers() {
    let (store, env) = populate().await;
    let expected = engine(&store, &env).run(&grouped_plan()).await.unwrap();
    let mesh = Arc::new(Mesh::default());
    let (coordinator, _) = three_nodes(&store, &env, &mesh);
    let output = coordinator.run(&grouped_plan()).await.unwrap();
    assert_eq!(sorted_rows(&output.batches), sorted_rows(&expected.batches));
    let stage = &output.analysis.stages[0];
    assert_eq!(stage.kind, "aggregate");
    assert!(stage.remote >= 1, "{stage:?}");
    assert_eq!(
        stage.finals, stage.remote,
        "one final partition per producer: {stage:?}"
    );
    assert_eq!(stage.final_failed, 0);
    assert_eq!(stage.recomputed, 0);
    assert_eq!(mesh.calls("final"), stage.finals);
    assert!(
        mesh.calls("fetch") >= stage.remote,
        "the coordinator fetched its partition from every producer"
    );
    assert!(
        tessera_exec::explain_text(&grouped_plan(), Some(&output.analysis)).contains("final on"),
        "{}",
        tessera_exec::explain_text(&grouped_plan(), Some(&output.analysis))
    );

    // An aggregate without keys has one group: it folds here.
    let counted = optimize(
        Plan::Aggregate {
            input: Box::new(Plan::Scan(Scan::all(
                TenantId::DEFAULT,
                dataset(),
                Kind::Log,
            ))),
            group_by: vec![],
            aggregates: vec![AggregateExpr {
                function: "count".into(),
                args: vec![],
                name: "n".into(),
            }],
        },
        &Registry::builtin(),
    )
    .unwrap();
    let output = coordinator.run(&counted).await.unwrap();
    assert_eq!(sorted_rows(&output.batches), vec!["20.0".to_owned()]);
    assert_eq!(output.analysis.stages[0].finals, 0);
}

/// QE-14, QE-17: a worker that cannot fold its partition has it folded
/// on the coordinator, from the outputs the others kept.
#[tokio::test]
async fn a_worker_that_cannot_fold_has_its_partition_folded_here() {
    let (store, env) = populate().await;
    let expected = engine(&store, &env).run(&grouped_plan()).await.unwrap();
    let mesh = Arc::new(Mesh::default());
    let (coordinator, peers) = three_nodes(&store, &env, &mesh);
    mesh.fail_final
        .lock()
        .unwrap()
        .push(peers[0].node.to_string());
    let output = coordinator.run(&grouped_plan()).await.unwrap();
    assert_eq!(sorted_rows(&output.batches), sorted_rows(&expected.batches));
    let stage = &output.analysis.stages[0];
    assert_eq!(stage.final_failed, 1, "{stage:?}");
    assert_eq!(stage.recomputed, 0);
}

/// QE-17: a producer whose kept output is gone has its share recomputed
/// on the coordinator from its files, once, and the result holds.
#[tokio::test]
async fn a_lost_output_is_recomputed_here() {
    let (store, env) = populate().await;
    let expected = engine(&store, &env).run(&grouped_plan()).await.unwrap();
    let mesh = Arc::new(Mesh::default());
    let (coordinator, peers) = three_nodes(&store, &env, &mesh);
    mesh.fail_fetch
        .lock()
        .unwrap()
        .push(peers[0].node.to_string());
    let output = coordinator.run(&grouped_plan()).await.unwrap();
    assert_eq!(sorted_rows(&output.batches), sorted_rows(&expected.batches));
    let stage = &output.analysis.stages[0];
    assert_eq!(stage.recomputed, 1, "{stage:?}");
    assert!(
        stage.final_failed >= 1,
        "the other worker could not fetch from the lost producer either: {stage:?}"
    );
}

/// A self join of the logs on their number: every row meets itself.
fn joined_plan() -> Plan {
    let scan = || Plan::Scan(Scan::all(TenantId::DEFAULT, dataset(), Kind::Log));
    let n = Expr::Path {
        path: "attr.n".into(),
        data_type: DataType::Int64,
    };
    optimize(
        Plan::Join {
            left: JoinSide::new(scan(), "l"),
            right: JoinSide::new(scan(), "r"),
            on: vec![(n.clone(), n)],
            kind: JoinKind::Inner,
            asof: None,
        }
        .project(vec![
            NamedExpr::new(Expr::column("l.body"), "left"),
            NamedExpr::new(Expr::column("r.body"), "right"),
        ]),
        &Registry::builtin(),
    )
    .unwrap()
}

/// QE-14: a join of two scans runs on the workers, each side an
/// exchange split by its join keys and each worker joining one
/// partition of both; a lost output is made up for here.
#[tokio::test]
async fn a_join_of_two_scans_folds_on_the_workers() {
    let (store, env) = populate().await;
    let expected = engine(&store, &env).run(&joined_plan()).await.unwrap();
    assert_eq!(expected.num_rows(), 20, "every row meets itself");
    let mesh = Arc::new(Mesh::default());
    let (coordinator, peers) = three_nodes(&store, &env, &mesh);
    let output = coordinator.run(&joined_plan()).await.unwrap();
    assert_eq!(sorted_rows(&output.batches), sorted_rows(&expected.batches));
    let stage = &output.analysis.stages[0];
    assert_eq!(stage.kind, "join");
    assert!(stage.remote >= 2, "a stage per side and peer: {stage:?}");
    assert!(stage.finals >= 1 && stage.final_failed == 0, "{stage:?}");

    mesh.fail_fetch
        .lock()
        .unwrap()
        .push(peers[1].node.to_string());
    let output = coordinator.run(&joined_plan()).await.unwrap();
    assert_eq!(sorted_rows(&output.batches), sorted_rows(&expected.batches));
    let stage = &output.analysis.stages[0];
    assert_eq!(
        stage.recomputed, 2,
        "the lost producer's share of each side, once: {stage:?}"
    );
}

/// Trace `trace`: a root `GET /` with a child `db`, which has a child
/// `cache`, all starting at `start`.
fn trace(trace: u8, start: i64) -> EventBatch {
    let span = |id: u8, parent: Option<u8>, name: &str, offset: i64| SpanRow {
        timestamp: Timestamp::from_nanos(start + offset),
        observed_timestamp: Timestamp::from_nanos(start + 100),
        event_id: [trace * 16 + id; 16],
        trace_id: [trace; 16],
        span_id: [id; 8],
        parent_span_id: parent.map(|p| [p; 8]),
        name: name.into(),
        duration_ns: 50 - offset,
        ..SpanRow::default()
    };
    EventBatch::from_spans(
        TenantId::DEFAULT,
        dataset(),
        &[
            span(1, None, "GET /", 0),
            span(2, Some(1), "db", 10),
            span(3, Some(2), "cache", 20),
        ],
    )
    .unwrap()
}

/// Six traces, one file each, and a seventh in the tail.
async fn populate_spans() -> (Arc<MemoryStore>, Env) {
    let store = Arc::new(MemoryStore::new());
    let env = Env::simulated(1, Timestamp::UNIX_EPOCH);
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
    for id in 1..=6u8 {
        let segment = vec![trace(id, i64::from(id) * 1_000)];
        let through = log.append_segment(&segment).await.unwrap();
        let files = write_l1_files(
            &target,
            &env,
            log.writer_id(),
            after,
            through,
            Window::DEFAULT_LENGTH,
            segment,
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
    log.append_segment(&[trace(7, 70_000)]).await.unwrap();
    (store, env)
}

/// The `cache` spans below a `GET /` span, with their candidates
/// narrowed by the spanset's filters as the `TraceQL` lowering does.
fn grouped_traces() -> Plan {
    let spans = Scan::all(TenantId::DEFAULT, dataset(), Kind::Span);
    let name_is = |name: &str| {
        Spanset::Filter(Some(
            Expr::column(NAME).binary(BinaryOp::Eq, Expr::literal(name)),
        ))
    };
    let spanset = Spanset::Structural {
        left: Box::new(name_is("GET /")),
        op: Structural {
            relation: Relation::Descendant,
            negated: false,
            union: false,
        },
        right: Box::new(name_is("cache")),
    };
    let mut candidates = spans.clone();
    candidates.predicates = vec![spanset.candidates().expect("a filter to narrow by")];
    optimize(
        Plan::TraceGroup {
            candidates: Some(Box::new(Plan::Scan(candidates))),
            spans: Box::new(Plan::Scan(spans)),
            spanset,
        },
        &Registry::builtin(),
    )
    .unwrap()
}

/// QE-14: a trace group's span phase runs on the workers, every span
/// of a trace in one partition, and each worker groups its traces.
#[tokio::test]
async fn a_trace_group_folds_on_the_workers() {
    let (store, env) = populate_spans().await;
    let expected = engine(&store, &env).run(&grouped_traces()).await.unwrap();
    assert_eq!(expected.num_rows(), 7, "one cache span per trace");
    let mesh = Arc::new(Mesh::default());
    let (coordinator, _) = three_nodes(&store, &env, &mesh);
    let output = coordinator.run(&grouped_traces()).await.unwrap();
    assert_eq!(sorted_rows(&output.batches), sorted_rows(&expected.batches));
    let stage = &output.analysis.stages[0];
    assert_eq!(stage.kind, "trace-group");
    assert!(
        stage.remote >= 1 && stage.finals == stage.remote,
        "{stage:?}"
    );
    assert_eq!(stage.final_failed, 0);
}
