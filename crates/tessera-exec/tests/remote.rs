// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! Leaf tasks shared with peers: files assigned to another node are read
//! there (QE-15), a peer that fails is read around (QE-17), and the rows
//! are the same either way.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use futures::future::BoxFuture;
use tessera_exec::{CpuPool, Engine, LeafTask, Peer, Peers, QueryLimits, RemoteError, RemoteScan};
use tessera_format::ParquetFormat;
use tessera_ir::{Plan, Registry, Scan};
use tessera_manifest::{intake, registry};
use tessera_object::{MemoryStore, Pools};
use tessera_opt::optimize;
use tessera_types::event::Scalar;
use tessera_types::{
    DatasetName, Env, EventBatch, FileId, Kind, LogRow, NodeId, PoolId, SeededRandom, Seq,
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
        (file.to_string().as_bytes()[0] % 2 == 0).then(|| self.peer.clone())
    }
}

/// A peer that runs tasks on its own engine over the same store, and
/// counts them; it fails when told to.
struct PeerEngine {
    engine: Engine,
    tasks: Mutex<Vec<LeafTask>>,
    fail: bool,
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
}

fn peer() -> Peer {
    let mut random = SeededRandom::new(7);
    Peer {
        node: NodeId::random(&mut random),
        http: "http://peer".into(),
    }
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[tokio::test]
async fn files_assigned_to_a_peer_are_read_there() {
    let (store, env) = populate().await;
    let remote = Arc::new(PeerEngine {
        engine: engine(&store, &env),
        tasks: Mutex::new(Vec::new()),
        fail: false,
    });
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
    let remote = Arc::new(PeerEngine {
        engine: engine(&store, &env),
        tasks: Mutex::new(Vec::new()),
        fail: true,
    });
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
