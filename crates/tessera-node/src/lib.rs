// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! One Tessera node (AR-1): opens the root pool, the catalog, the ingest
//! service, the query engine and the HTTP API, keeps the catalog fresh
//! and checkpointed, runs intake, recovery, compaction and garbage
//! collection in the background, and stops cleanly.
//!
//! [`Node::start`] and [`Node::stop`] are the node as a value, so that a
//! test can run several in one process; [`run`] is the binary's loop
//! from start to Ctrl-C.

mod background;
mod heartbeat;
mod leases;
mod peers;

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tessera_catalog::{Catalog, PoolConfig};
use tessera_cluster::{Admission, Limits, Membership, Registry, Role, registry};
use tessera_commit::LeaseManager;
use tessera_exec::{CpuPool, Engine, QueryLimits};
use tessera_format::{FileFormat, ParquetFormat};
use tessera_ingest::{IngestConfig, IngestHandle, Ingester};
use tessera_object::{LocalStore, ObjectStore, PoolState, Pools, layout};
use tessera_server::{AppState, Cluster, HttpRemote};
use tessera_types::{Env, PoolId};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub use leases::{Leases, Owned};
pub use peers::MembershipPeers;

/// How often a node re-reads the catalog, so that a change made elsewhere
/// takes effect within the bound of CT-3.
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);
/// How often the catalog is checkpointed, if it changed.
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);
/// How often the ingester does its time-based work.
const INGEST_TICK: Duration = Duration::from_millis(50);
/// How many ingesters a dataset's writes are spread over (IN-9). The
/// spec lets the set grow with the dataset's volume; two is the start.
const PREFERRED_INGESTERS: usize = 2;

/// What a node needs to start (OP-11).
#[derive(Clone, Debug)]
pub struct Config {
    /// The directory that acts as the root pool.
    pub data_dir: PathBuf,
    /// Where the HTTP API listens; port zero picks a free one.
    pub listen: SocketAddr,
    /// Where the `PostgreSQL` wire protocol listens; port zero picks a
    /// free one.
    pub pg_listen: SocketAddr,
    /// The roles this node runs; every role by default.
    pub roles: BTreeSet<Role>,
    /// The base URL peers reach this node's HTTP API at; by default the
    /// listen address, which is right only when it is a real address.
    pub advertise: Option<String>,
    /// How often the node announces itself and re-reads the registry.
    pub heartbeat: Duration,
    /// How long a slot lease lasts (OP-3).
    pub lease: Duration,
    /// How often held leases are renewed and free ones tried (OP-3).
    pub renewal: Duration,
    /// The ingester's tunables.
    pub ingest: IngestConfig,
}

impl Config {
    /// Every role, loopback listeners, the default tunables, against
    /// `data_dir`.
    #[must_use]
    pub fn single_node(data_dir: PathBuf) -> Self {
        let loopback = |port| SocketAddr::from(([127, 0, 0, 1], port));
        Config {
            data_dir,
            listen: loopback(8080),
            pg_listen: loopback(5432),
            roles: Role::ALL.into_iter().collect(),
            advertise: None,
            heartbeat: registry::HEARTBEAT,
            lease: LeaseManager::DEFAULT_DURATION,
            renewal: LeaseManager::DEFAULT_RENEWAL,
            ingest: IngestConfig::default(),
        }
    }
}

/// Why the node could not start or stopped.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// The data directory cannot be used.
    #[error("cannot open data directory {path}: {source}")]
    DataDir {
        /// The directory.
        path: PathBuf,
        /// The failure.
        #[source]
        source: std::io::Error,
    },
    /// The catalog cannot be opened.
    #[error("cannot open the catalog: {0}")]
    Catalog(#[from] tessera_catalog::CatalogError),
    /// The node cannot announce itself.
    #[error("cannot join the cluster: {0}")]
    Registry(#[from] tessera_cluster::RegistryError),
    /// A listener cannot be bound or served.
    #[error("cannot serve on {address}: {source}")]
    Serve {
        /// The address.
        address: SocketAddr,
        /// The failure.
        #[source]
        source: std::io::Error,
    },
}

/// A running node.
pub struct Node {
    state: AppState,
    http: SocketAddr,
    pg: SocketAddr,
    ingest: IngestHandle,
    ingest_task: JoinHandle<()>,
    registry: Arc<Registry>,
    membership: Arc<Membership>,
    leases: Option<Arc<Leases>>,
    lease_loop: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
    stop_servers: Vec<oneshot::Sender<()>>,
    servers: Vec<JoinHandle<()>>,
    loops: Vec<JoinHandle<()>>,
}

impl Node {
    /// Opens everything and starts serving.
    ///
    /// # Errors
    ///
    /// If the data directory, the catalog or a listener cannot be opened.
    pub async fn start(config: Config, env: Env) -> Result<Node, RunError> {
        let (store, catalog, pool) = open(&config, &env).await?;
        let mut loops = vec![tokio::spawn(keep_fresh(Arc::clone(&catalog)))];

        let format: Arc<dyn FileFormat> = Arc::new(ParquetFormat);
        let pools = Arc::new(Pools::new(pool.clone(), Arc::clone(&store)));
        let ingester = Ingester::start(
            Arc::clone(&store),
            env.clone(),
            config.ingest,
            pool.clone(),
            Arc::clone(&format),
        );
        tracing::info!(writer = %ingester.writer_id(), "ingest started");
        let (ingest, ingest_task) = tessera_ingest::spawn(ingester, INGEST_TICK);

        let listener = bind(config.listen).await?;
        let http = listener.local_addr().unwrap_or(config.listen);
        tracing::info!(address = %http, "http api listening");
        let pg_listener = bind(config.pg_listen).await?;
        let pg = pg_listener.local_addr().unwrap_or(config.pg_listen);
        tracing::info!(address = %pg, "postgresql protocol listening");

        let advertise = config.advertise.unwrap_or_else(|| format!("http://{http}"));
        let (registry, membership) = join(
            &store,
            &env,
            config.roles.clone(),
            advertise,
            &mut loops,
            config.heartbeat,
        )
        .await?;
        let leases = config.roles.contains(&Role::Compact).then(|| {
            Arc::new(Leases {
                manager: LeaseManager::new(
                    Arc::clone(&store),
                    layout::leases(),
                    env.clone(),
                    config.lease,
                ),
                catalog: Arc::clone(&catalog),
                membership: Arc::clone(&membership),
                owned: Owned::default(),
            })
        });
        let mut lease_loop = None;
        if let Some(leases) = &leases {
            let (stop, stopped) = oneshot::channel();
            let task = tokio::spawn(leases::run(Arc::clone(leases), config.renewal, stopped));
            lease_loop = Some((stop, task));
            loops.push(tokio::spawn(background::run(background::Background {
                store: Arc::clone(&store),
                env: env.clone(),
                catalog: Arc::clone(&catalog),
                format: Arc::clone(&format),
                pool,
                pools: Arc::clone(&pools),
                owned: leases.owned.clone(),
                membership: Arc::clone(&membership),
            })));
        }

        let remote = HttpRemote::new();
        let engine = engine(&pools, &format, &env, &membership, &remote);
        let state = AppState {
            catalog: Arc::clone(&catalog),
            ingest: ingest.clone(),
            pools,
            format,
            engine: Arc::new(engine),
            env,
            cluster: Some(Arc::new(Cluster {
                membership: Arc::clone(&membership),
                remote,
                admission: Arc::new(Admission::new(Limits::default())),
                preferred_ingesters: PREFERRED_INGESTERS,
            })),
        };
        let (stop_servers, servers) = serve(state.clone(), listener, pg_listener);
        Ok(Node {
            state,
            http,
            pg,
            ingest,
            ingest_task,
            registry,
            membership,
            leases,
            lease_loop,
            stop_servers,
            servers,
            loops,
        })
    }

    /// The cluster as this node sees it.
    #[must_use]
    pub fn membership(&self) -> &Arc<Membership> {
        &self.membership
    }

    /// The slots this node owns; none without the `compact` role.
    #[must_use]
    pub fn owned(&self) -> Owned {
        self.leases
            .as_ref()
            .map(|leases| leases.owned.clone())
            .unwrap_or_default()
    }

    /// Where the HTTP API listens.
    #[must_use]
    pub fn http_address(&self) -> SocketAddr {
        self.http
    }

    /// Where the `PostgreSQL` wire protocol listens.
    #[must_use]
    pub fn pg_address(&self) -> SocketAddr {
        self.pg
    }

    /// The node's identity.
    #[must_use]
    pub fn env(&self) -> &Env {
        &self.state.env
    }

    /// What the handlers share, for callers in the same process.
    #[must_use]
    pub fn state(&self) -> &AppState {
        &self.state
    }

    /// Stops serving, ends the background work and closes the ingester,
    /// so that nothing is left for recovery.
    pub async fn stop(self) {
        for stop in self.stop_servers {
            let _ = stop.send(());
        }
        for server in self.servers {
            let _ = server.await;
        }
        for task in &self.loops {
            task.abort();
        }
        if let Some((stop, task)) = self.lease_loop {
            let _ = stop.send(());
            let _ = task.await;
        }
        if let Err(error) = self.ingest.shutdown().await {
            tracing::warn!(error = %error, "ingest shutdown left work for recovery");
        }
        if let Err(error) = self.registry.withdraw().await {
            tracing::warn!(error = %error, "the registry still lists this node");
        }
        tracing::info!("stopped");
    }

    /// Drops the node as a crash would: nothing is closed, released or
    /// withdrawn, so the rest of the cluster must notice by itself.
    pub fn abandon(self) {
        for task in self.servers.iter().chain(&self.loops) {
            task.abort();
        }
        if let Some((_, task)) = &self.lease_loop {
            task.abort();
        }
        self.ingest_task.abort();
    }
}

/// Runs a node until Ctrl-C.
///
/// # Errors
///
/// If the data directory, the catalog or a listener cannot be opened.
pub async fn run(config: Config) -> Result<(), RunError> {
    let node = Node::start(config, Env::system()).await?;
    shutdown_signal().await;
    node.stop().await;
    Ok(())
}

/// Opens the root pool and the catalog in it.
async fn open(
    config: &Config,
    env: &Env,
) -> Result<(Arc<dyn ObjectStore>, Arc<Catalog>, PoolId), RunError> {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::open(&config.data_dir).await.map_err(
        |source| RunError::DataDir {
            path: config.data_dir.clone(),
            source,
        },
    )?);
    let pool = PoolId::new("root").unwrap_or_else(|_| unreachable!("a literal valid pool id"));
    let root_pool = PoolConfig {
        id: pool.clone(),
        url: format!(
            "file://{}",
            config
                .data_dir
                .canonicalize()
                .unwrap_or(config.data_dir.clone())
                .display()
        ),
        weight: 1,
        state: PoolState::Active,
    };
    let catalog = Arc::new(Catalog::open(Arc::clone(&store), root_pool, env.clone()).await?);
    tracing::info!(
        node = %env.node,
        data_dir = %config.data_dir.display(),
        version = %catalog.cached().version(),
        "catalog open"
    );
    Ok((store, catalog, pool))
}

/// The query engine of this node, reading files on peers chosen by
/// rendezvous hashing over the live query nodes (QE-15).
fn engine(
    pools: &Arc<Pools>,
    format: &Arc<dyn FileFormat>,
    env: &Env,
    membership: &Arc<Membership>,
    remote: &HttpRemote,
) -> Engine {
    Engine::new(
        Arc::clone(pools),
        Arc::clone(format),
        env.clone(),
        CpuPool::for_host(),
        QueryLimits::default(),
    )
    .with_peers(
        Arc::new(MembershipPeers::new(Arc::clone(membership))),
        Arc::new(remote.clone()),
    )
}

/// Announces the node under `roles` and `advertise`, and keeps the
/// registry and the membership view fresh from a heartbeat loop.
async fn join(
    store: &Arc<dyn ObjectStore>,
    env: &Env,
    roles: BTreeSet<Role>,
    advertise: String,
    loops: &mut Vec<JoinHandle<()>>,
    heartbeat: Duration,
) -> Result<(Arc<Registry>, Arc<Membership>), RunError> {
    let registry = Arc::new(Registry::new(
        Arc::clone(store),
        env.clone(),
        roles.clone(),
        advertise.clone(),
    ));
    let own = registry.announce().await?;
    tracing::info!(?roles, %advertise, "joined the cluster");
    let membership = Arc::new(Membership::new(env.clone(), own));
    loops.push(tokio::spawn(heartbeat::run(
        Arc::clone(&registry),
        Arc::clone(&membership),
        heartbeat,
    )));
    Ok((registry, membership))
}

/// Serves the HTTP API and the `PostgreSQL` protocol until told to stop:
/// one sender and one task per server.
fn serve(
    state: AppState,
    listener: tokio::net::TcpListener,
    pg_listener: tokio::net::TcpListener,
) -> (Vec<oneshot::Sender<()>>, Vec<JoinHandle<()>>) {
    let (stop_pg, pg_stopped) = oneshot::channel::<()>();
    let pg_server = tokio::spawn(tessera_server::pg::serve(
        pg_listener,
        state.clone(),
        async {
            let _ = pg_stopped.await;
        },
    ));
    let (stop_http, http_stopped) = oneshot::channel::<()>();
    let app = tessera_server::router(state);
    let http_server = tokio::spawn(async move {
        let served = axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = http_stopped.await;
            })
            .await;
        if let Err(error) = served {
            tracing::error!(error = %error, "http api stopped with an error");
        }
    });
    (vec![stop_http, stop_pg], vec![http_server, pg_server])
}

async fn bind(address: SocketAddr) -> Result<tokio::net::TcpListener, RunError> {
    tokio::net::TcpListener::bind(address)
        .await
        .map_err(|source| RunError::Serve { address, source })
}

/// Re-reads the catalog every second and checkpoints it every minute if
/// it changed. Failures are logged and retried on the next tick.
async fn keep_fresh(catalog: Arc<Catalog>) {
    let mut checkpointed = catalog.cached().version();
    let mut last_checkpoint = tokio::time::Instant::now();
    let mut ticks = tokio::time::interval(REFRESH_INTERVAL);
    loop {
        ticks.tick().await;
        let version = match catalog.snapshot().await {
            Ok(snapshot) => snapshot.version(),
            Err(error) => {
                tracing::warn!(error = %error, "catalog refresh failed");
                continue;
            }
        };
        if version > checkpointed && last_checkpoint.elapsed() >= CHECKPOINT_INTERVAL {
            match catalog.checkpoint().await {
                Ok(()) => {
                    tracing::debug!(%version, "catalog checkpointed");
                    checkpointed = version;
                }
                Err(error) => tracing::warn!(error = %error, "catalog checkpoint failed"),
            }
            last_checkpoint = tokio::time::Instant::now();
        }
    }
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(error = %error, "cannot listen for ctrl-c; running until killed");
        std::future::pending::<()>().await;
    }
    tracing::info!("shutdown requested");
}
