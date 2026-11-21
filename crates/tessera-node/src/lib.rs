// SPDX-License-Identifier: Apache-2.0

//! One Tessera node (AR-1): opens the root pool, the catalog, the ingest
//! service, the query engine and the HTTP API, keeps the catalog fresh
//! and checkpointed, runs intake, recovery, compaction and garbage
//! collection in the background, and stops cleanly.
//!
//! [`Node::start`] and [`Node::stop`] are the node as a value, so that a
//! test can run several in one process; [`run`] is the binary's loop
//! from start to Ctrl-C.

mod advisor;
mod background;
mod budgets;
mod evaluator;
mod health;
mod heartbeat;
mod leases;
pub mod observe;
mod peers;
mod retention;
pub mod tls;
mod watch;

pub use tessera_server::oidc::Oidc;
pub use tessera_types::Keyring;
pub use tls::Tls;

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tessera_catalog::{Catalog, ControlStores, PoolConfig};
use tessera_cluster::{
    Admission, Ledger, Limits, Load, Membership, NodeRecord, Registry, Role, registry,
};
use tessera_commit::LeaseManager;
use tessera_eval::HttpNotifier;
use tessera_exec::{CpuPool, Engine, QueryLimits};
use tessera_format::{FileFormat, ParquetFormat};
use tessera_iceberg::Exporter;
use tessera_ingest::{IngestConfig, IngestHandle, Ingester};
use tessera_manifest::{DatasetLogs, Manifests};
use tessera_object::{
    CachedStore, LocalStore, MeteredStore, ObjectStore, PoolState, Pools, RemoteStore, SealedStore,
    StoreRequests, layout,
};
use tessera_server::{
    AppState, Auth, Cluster, HttpRemote, IngestMeter, RollupEntries, SystemTableRows,
};
use tessera_types::{Env, KeyPolicy, Keys, Meter, PoolId};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub use evaluator::StateSink;
pub use leases::{Leases, Owned};
pub use peers::MembershipPeers;

/// How often a node re-reads the catalog, so that a change made elsewhere
/// takes effect within the bound of CT-3.
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);
/// How often a node releases the state of tenants nobody used (CT-6).
const RELEASE_INTERVAL: Duration = Duration::from_secs(60);
/// How long a tenant's state stays loaded after its last use (CT-6).
const IDLE: Duration = Duration::from_secs(600);
/// How often the catalog is checkpointed, if it changed.
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);
/// How often the ingester does its time-based work.
const INGEST_TICK: Duration = Duration::from_millis(50);
/// The dataset of the `_system` tenant that holds the workload log.
pub const WORKLOAD_DATASET: &str = "queries";
/// The most ingesters a dataset's writes are spread over (IN-9): the
/// set grows with the dataset's recent volume up to this.
const PREFERRED_INGESTERS: usize = 8;

/// What a node needs to start (OP-11).
#[derive(Clone, Debug)]
pub struct Config {
    /// The directory that acts as the root pool when `root_pool` names
    /// none.
    pub data_dir: PathBuf,
    /// The root pool as a URL, `s3://<bucket>` or
    /// `s3://<bucket>/<prefix>` (ST-1), reached as the `AWS_*`
    /// variables say; none, and the data directory is the root pool.
    pub root_pool: Option<String>,
    /// Where queries spill when a memory grant runs out (QE-11).
    pub spill_dir: PathBuf,
    /// The cache of object reads on local disk (section 10.8); none
    /// reads everything from the root pool, which is right when that
    /// is a local directory.
    pub cache: Option<DiskCache>,
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
    /// Base URLs of nodes to gossip with on every heartbeat, whatever
    /// the registry says (section 14.1): a static seed list.
    pub seeds: Vec<String>,
    /// A `host:port` whose addresses, looked up on every heartbeat, are
    /// nodes to gossip with (section 14.1): a DNS seed, such as a
    /// headless service.
    pub seed_dns: Option<String>,
    /// How often the node announces itself and re-reads the registry.
    pub heartbeat: Duration,
    /// How long a slot lease lasts (OP-3).
    pub lease: Duration,
    /// How often held leases are renewed and free ones tried (OP-3).
    pub renewal: Duration,
    /// How often the background work of the `compact` role runs.
    pub background: Duration,
    /// How often the `eval` role evaluates its rule groups.
    pub eval: Duration,
    /// How often the advisor runs on the `compact` role.
    pub advise: Duration,
    /// The ingester's tunables.
    pub ingest: IngestConfig,
    /// Whether requests without an API key are served as a cluster
    /// administrator (CT-5); on for a single node, off for a cluster
    /// that anyone can reach.
    pub allow_anonymous: bool,
    /// The secret the nodes of a cluster present to each other on the
    /// `/internal` routes (SE-3); none leaves them open.
    pub cluster_secret: Option<String>,
    /// The certificate both listeners serve TLS with and the CA peers
    /// are checked against (SE-3); none serves plain connections.
    pub tls: Option<Tls>,
    /// The identity provider bearer tokens that are not API keys are
    /// checked against (SE-1); none refuses such tokens.
    pub oidc: Option<Oidc>,
    /// The named tenant keys this node holds (SE-2); a tenant's
    /// `encryption_key` setting names one, and its blocks and files are
    /// sealed under it. Empty, every tenant is stored plain and one
    /// naming a key is refused.
    pub keyring: Keyring,
    /// How often the node writes its load signals to `_system.metrics`
    /// (section 14.7).
    pub metrics: Duration,
    /// The URI external engines read the data directory at, such as
    /// `s3://bucket/prefix` or `file:///var/lib/tessera`. Given, the
    /// `compact` role publishes Apache Iceberg metadata for every
    /// dataset under it (OP-9); none, and nothing is exported.
    pub export_location: Option<String>,
    /// How often the Iceberg metadata is brought up to date.
    pub export_interval: Duration,
}

/// Where a node keeps what it read of the objects that never change,
/// and how much of it (section 10.8). The directory is the cache's
/// alone: it is emptied at every start.
#[derive(Clone, Debug)]
pub struct DiskCache {
    /// The directory.
    pub dir: PathBuf,
    /// The most bytes kept.
    pub bytes: u64,
}

impl Config {
    /// Every role, loopback listeners, the default tunables, against
    /// `data_dir`.
    #[must_use]
    pub fn single_node(data_dir: PathBuf) -> Self {
        let loopback = |port| SocketAddr::from(([127, 0, 0, 1], port));
        Config {
            data_dir,
            root_pool: None,
            spill_dir: std::env::temp_dir().join("tessera-spill"),
            cache: None,
            listen: loopback(8080),
            pg_listen: loopback(5432),
            roles: Role::ALL.into_iter().collect(),
            advertise: None,
            seeds: Vec::new(),
            seed_dns: None,
            heartbeat: registry::HEARTBEAT,
            lease: LeaseManager::DEFAULT_DURATION,
            renewal: LeaseManager::DEFAULT_RENEWAL,
            background: background::DEFAULT_INTERVAL,
            eval: evaluator::DEFAULT_INTERVAL,
            advise: advisor::DEFAULT_INTERVAL,
            ingest: IngestConfig::default(),
            allow_anonymous: true,
            cluster_secret: None,
            tls: None,
            oidc: None,
            keyring: Keyring::default(),
            metrics: observe::DEFAULT_INTERVAL,
            export_location: None,
            export_interval: background::EXPORT_EVERY,
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
    /// The cache directory cannot be used.
    #[error("cannot open the cache directory {path}: {source}")]
    Cache {
        /// The directory.
        path: PathBuf,
        /// The failure.
        #[source]
        source: std::io::Error,
    },
    /// The root pool's URL names no store this build opens.
    #[error("cannot open the root pool: {0}")]
    RootPool(#[from] tessera_object::OpenError),
    /// The root pool's store is not one to be trusted with the data.
    #[error("cannot use the root pool: {0}")]
    Probe(#[from] tessera_object::ProbeError),
    /// The catalog cannot be opened.
    #[error("cannot open the catalog: {0}")]
    Catalog(#[from] tessera_catalog::CatalogError),
    /// The node cannot announce itself.
    #[error("cannot join the cluster: {0}")]
    Registry(#[from] tessera_cluster::RegistryError),
    /// A live node cannot read what this build writes (OP-7).
    #[error(
        "this build writes data format {writes}, which node {node} cannot read (it reads through {reads_through}); upgrade that node first"
    )]
    Format {
        /// The format this build writes.
        writes: u32,
        /// The node that cannot read it.
        node: tessera_types::NodeId,
        /// The newest format it reads.
        reads_through: u32,
    },
    /// A task the node is made of ended while the node ran.
    #[error("a task of the node ended before the node was stopped")]
    Broken,
    /// TLS cannot be set up from the configured files.
    #[error("tls: {0}")]
    Tls(#[from] tls::TlsError),
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
    controls: Arc<ControlStores>,
    http: SocketAddr,
    pg: SocketAddr,
    ingest: IngestHandle,
    ingest_tasks: Vec<JoinHandle<()>>,
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
        let (store, catalog, pool, cache, requests) = open(&config, &env).await?;
        let (env, store) = keyed(&config, env, &catalog, store);
        let mut loops = vec![tokio::spawn(keep_fresh(Arc::clone(&catalog)))];

        let format: Arc<dyn FileFormat> = Arc::new(
            ParquetFormat::with_footer_cache(tessera_format::footer::DEFAULT_CAPACITY)
                .with_keys(Arc::clone(&env.keys)),
        );
        let pools = Arc::new(Pools::new(pool.clone(), Arc::clone(&store)));
        let ingesters = ingesters(&store, &env, config.ingest, &pool, &format);
        let (ingest, ingest_tasks) = tessera_ingest::spawn_all(ingesters, INGEST_TICK);

        let listener = bind(config.listen).await?;
        let http = listener.local_addr().unwrap_or(config.listen);
        tracing::info!(address = %http, "http api listening");
        let pg_listener = bind(config.pg_listen).await?;
        let pg = pg_listener.local_addr().unwrap_or(config.pg_listen);
        tracing::info!(address = %pg, "postgresql protocol listening");

        let acceptor = config.tls.as_ref().map(tls::acceptor).transpose()?;
        let scheme = if acceptor.is_some() { "https" } else { "http" };
        let advertise = config
            .advertise
            .clone()
            .unwrap_or_else(|| format!("{scheme}://{http}"));
        let gossip = heartbeat::Gossip {
            remote: peer_client(&config)?,
            seeds: config.seeds.clone(),
            dns: config.seed_dns.clone(),
            scheme,
        };
        let (registry, membership) = join(
            &store,
            &env,
            config.roles.clone(),
            advertise,
            &mut loops,
            config.heartbeat,
            gossip,
        )
        .await?;
        let shared = Shared {
            store,
            env,
            catalog,
            format,
            pool,
            pools,
            membership,
            load: Load::new(),
        };
        loops
            .extend(cache.map(|cache| tokio::spawn(report_cache(cache, Arc::clone(&shared.load)))));
        loops.push(tokio::spawn(report_requests(
            requests,
            Arc::clone(&shared.load),
        )));
        let meter: Arc<dyn Meter> = Arc::new(IngestMeter::new(ingest.clone(), shared.env.clone()));
        let (leases, lease_loop) = shared.own_slots(&config, &mut loops, meter);
        let (state, engine, controls) =
            shared.app_state(ingest.clone(), &config, peer_client(&config)?);
        shared.evaluate_rules(
            &config,
            leases.as_ref(),
            &state,
            Arc::clone(&engine),
            &controls,
            &mut loops,
        );
        shared.advise_layouts(&config, leases.as_ref(), engine, &controls, &mut loops);
        loops.push(tokio::spawn(release_idle(state.clone())));
        loops.push(tokio::spawn(budgets::run(state.clone(), config.heartbeat)));
        loops.push(tokio::spawn(observe::run_metrics(
            state.clone(),
            config.metrics,
        )));
        if let Some(receiver) = observe::take_logs() {
            loops.push(tokio::spawn(observe::run_logs(state.clone(), receiver)));
        }
        match health::ensure_rules(&controls).await {
            Ok(added) if added > 0 => tracing::info!(added, "default health rules added"),
            Ok(_) => {}
            Err(error) => tracing::warn!(error = %error, "default health rules not added"),
        }
        let membership = shared.membership;
        let (stop_servers, servers) = serve(state.clone(), listener, pg_listener, acceptor);
        Ok(Node {
            state,
            controls,
            http,
            pg,
            ingest,
            ingest_tasks,
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

    /// The tenants' control stores, as this node opens them.
    #[must_use]
    pub fn controls(&self) -> &Arc<ControlStores> {
        &self.controls
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

    /// Whether every task the node is made of is still running: the
    /// servers, the ingesters, the lease loop and the loops of its
    /// roles. None ends while the node is healthy.
    #[must_use]
    pub fn is_whole(&self) -> bool {
        let lease_loop = self.lease_loop.as_ref().map(|(_, task)| task);
        watch::all_running(
            self.loops
                .iter()
                .chain(&self.servers)
                .chain(&self.ingest_tasks)
                .chain(lease_loop),
        )
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
        for task in &self.ingest_tasks {
            task.abort();
        }
    }
}

/// The node's ingesters, one per writer the configuration asks for,
/// each with a writer log of its own (ST-5).
fn ingesters(
    store: &Arc<dyn ObjectStore>,
    env: &Env,
    config: IngestConfig,
    pool: &PoolId,
    format: &Arc<dyn FileFormat>,
) -> Vec<Ingester> {
    let ingesters: Vec<Ingester> = (0..config.writers.max(1))
        .map(|_| {
            Ingester::start(
                Arc::clone(store),
                env.clone(),
                config,
                pool.clone(),
                Arc::clone(format),
            )
        })
        .collect();
    for ingester in &ingesters {
        tracing::info!(writer = %ingester.writer_id(), "ingest started");
    }
    ingesters
}

/// Runs a node until the process is interrupted or terminated, or
/// until one of the node's own tasks ends, then stops it as
/// [`Node::stop`] does.
///
/// # Errors
///
/// If the data directory, the catalog or a listener cannot be opened,
/// or `Broken` if the node stopped because a task of its own ended.
pub async fn run(config: Config) -> Result<(), RunError> {
    let shutdown = shutdown_signal();
    let node = Node::start(config, Env::system()).await?;
    let broken = watch::broken_before(shutdown, || node.is_whole(), watch::EVERY).await;
    if broken {
        tracing::error!("a task of the node ended; stopping for the supervisor to start it again");
    }
    node.stop().await;
    if broken {
        return Err(RunError::Broken);
    }
    Ok(())
}

/// The environment with the node's tenant keys, named for each tenant
/// by the catalog, and `store` sealing with them what the tenants'
/// prefixes hold (SE-2). The sealing sits over the cache, which thus
/// keeps what it holds as the store does. The catalog was opened
/// under it: it is the cluster's and stays plain.
fn keyed(
    config: &Config,
    env: Env,
    catalog: &Arc<Catalog>,
    store: Arc<dyn ObjectStore>,
) -> (Env, Arc<dyn ObjectStore>) {
    if !config.keyring.is_empty() {
        tracing::info!(keys = ?config.keyring.names().collect::<Vec<_>>(), "keyring loaded");
    }
    let policy: Arc<dyn KeyPolicy> = catalog.clone();
    let env = env.with_keys(Keys::new(config.keyring.clone(), policy));
    let store = Arc::new(SealedStore::new(store, Arc::clone(&env.keys)));
    (env, store)
}

/// The root pool's store with the node's cache in front of it, when
/// it has one, and the cache to report the use of.
async fn cached(
    config: &Config,
    (store, url): (Arc<dyn ObjectStore>, String),
) -> Result<(RootStore, Option<Arc<CachedStore>>), RunError> {
    let Some(DiskCache { dir, bytes }) = &config.cache else {
        return Ok(((store, url), None));
    };
    let cache = CachedStore::open(store, dir, *bytes)
        .await
        .map_err(|source| RunError::Cache {
            path: dir.clone(),
            source,
        })?;
    let cache = Arc::new(cache);
    tracing::info!(dir = %dir.display(), bytes, "object cache open");
    let store: Arc<dyn ObjectStore> = cache.clone();
    Ok(((store, url), Some(cache)))
}

/// Copies what the root pool's store was asked into the node's load
/// signals every second.
async fn report_requests(requests: Arc<StoreRequests>, load: Arc<Load>) {
    let mut ticks = tokio::time::interval(Duration::from_secs(1));
    loop {
        ticks.tick().await;
        load.set_store_requests(requests.by_operation());
    }
}

/// Copies the cache's use into the node's load signals every second.
async fn report_cache(cache: Arc<CachedStore>, load: Arc<Load>) {
    let mut ticks = tokio::time::interval(Duration::from_secs(1));
    loop {
        ticks.tick().await;
        let used = cache.stats();
        load.set_object_cache(tessera_cluster::ObjectCache {
            hits: used.hits,
            misses: used.misses,
            bytes: used.bytes,
        });
    }
}

/// A root pool's store and the URL the catalog records it under.
type RootStore = (Arc<dyn ObjectStore>, String);

/// What a node opens first: the root pool's store, the catalog in
/// it, the pool's id, the cache in front of the store, if any, and
/// the count of what the store is asked.
type Opened = (
    Arc<dyn ObjectStore>,
    Arc<Catalog>,
    PoolId,
    Option<Arc<CachedStore>>,
    Arc<StoreRequests>,
);

/// Opens the root pool, behind the node's cache when it has one, and
/// the catalog in it.
async fn open(config: &Config, env: &Env) -> Result<Opened, RunError> {
    // Counted on the store itself, under the cache: what the cache
    // serves is not a request.
    let (store, url) = root_store(config).await?;
    let metered = MeteredStore::new(store);
    let requests = metered.requests();
    let ((store, url), cache) = cached(config, (Arc::new(metered), url)).await?;
    // Before anything is written: a store that creates an object
    // twice would let two nodes win the same commit (ST-3).
    tessera_object::probe::create_if_absent(&*store, &layout::probe(env.node)).await?;
    let pool = PoolId::new("root").unwrap_or_else(|_| unreachable!("a literal valid pool id"));
    let root_pool = PoolConfig {
        id: pool.clone(),
        url: url.clone(),
        weight: 1,
        state: PoolState::Active,
    };
    let catalog = Arc::new(Catalog::open(Arc::clone(&store), root_pool, env.clone()).await?);
    tracing::info!(
        node = %env.node,
        root_pool = %url,
        version = %catalog.cached().version(),
        "catalog open"
    );
    Ok((store, catalog, pool, cache, requests))
}

/// The store of the root pool and the URL the catalog records it
/// under: the bucket `root_pool` names, or the data directory.
async fn root_store(config: &Config) -> Result<RootStore, RunError> {
    if let Some(url) = &config.root_pool {
        return Ok((Arc::new(RemoteStore::s3(url)?), url.clone()));
    }
    let directory = &config.data_dir;
    let store = LocalStore::open(directory)
        .await
        .map_err(|source| RunError::DataDir {
            path: directory.clone(),
            source,
        })?;
    let path = directory.canonicalize().unwrap_or(directory.clone());
    Ok((Arc::new(store), format!("file://{}", path.display())))
}

/// What the node's loops share.
struct Shared {
    store: Arc<dyn ObjectStore>,
    env: Env,
    catalog: Arc<Catalog>,
    format: Arc<dyn FileFormat>,
    pool: PoolId,
    pools: Arc<Pools>,
    membership: Arc<Membership>,
    /// The node's load signals (OP-5), set by every loop.
    load: Arc<Load>,
}

impl Shared {
    /// Starts the lease loop for the kinds of work the node's roles own
    /// (section 14.2) and, on a compact node, the background work over
    /// the owned slots. Returns the leases and the loop's stop handle.
    fn own_slots(
        &self,
        config: &Config,
        loops: &mut Vec<JoinHandle<()>>,
        meter: Arc<dyn Meter>,
    ) -> LeaseLoop {
        let kinds = leases::kinds_of(&config.roles);
        if kinds.is_empty() {
            return (None, None);
        }
        let leases = Arc::new(Leases {
            manager: LeaseManager::new(
                Arc::clone(&self.store),
                layout::leases(),
                self.env.clone(),
                config.lease,
            ),
            catalog: Arc::clone(&self.catalog),
            membership: Arc::clone(&self.membership),
            owned: Owned::default(),
            kinds,
            load: Arc::clone(&self.load),
        });
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(leases::run(Arc::clone(&leases), config.renewal, stopped));
        if config.roles.contains(&Role::Compact) {
            loops.push(tokio::spawn(background::run(background::Background {
                store: Arc::clone(&self.store),
                env: self.env.clone(),
                catalog: Arc::clone(&self.catalog),
                format: Arc::clone(&self.format),
                pool: self.pool.clone(),
                pools: Arc::clone(&self.pools),
                owned: leases.owned.clone(),
                membership: Arc::clone(&self.membership),
                interval: config.background,
                datasets: Arc::new(DatasetLogs::new(Arc::clone(&self.store), self.env.clone())),
                manifests: Arc::new(Manifests::new(
                    Arc::clone(&self.store),
                    self.env.clone(),
                    tessera_manifest::manifests::DEFAULT_CAPACITY,
                )),
                meter,
                controls: Arc::new(ControlStores::new(
                    Arc::clone(&self.store),
                    self.env.clone(),
                )),
                load: Arc::clone(&self.load),
                export: config.export_location.as_deref().map(|location| {
                    Exporter::new(Arc::clone(&self.store), location, self.env.clone())
                }),
                export_interval: config.export_interval,
            })));
        }
        (Some(leases), Some((stop, task)))
    }

    /// The handlers' shared state: the engine with its peers and system
    /// tables, the control stores and the cluster links.
    fn app_state(
        &self,
        ingest: IngestHandle,
        config: &Config,
        remote: HttpRemote,
    ) -> (AppState, Arc<Engine>, Arc<ControlStores>) {
        let controls = Arc::new(ControlStores::new(
            Arc::clone(&self.store),
            self.env.clone(),
        ));
        let engine = Arc::new(
            engine(
                &self.pools,
                &self.format,
                &self.env,
                &self.membership,
                &remote,
                config.spill_dir.clone(),
            )
            .with_system_tables(Arc::new(SystemTableRows::new(
                Arc::clone(&controls),
                Arc::clone(&self.store),
                self.env.clone(),
                self.catalog.clone(),
            )))
            .with_rollups(Arc::new(RollupEntries::new(
                Arc::clone(&controls),
                Arc::clone(&self.store),
                self.env.clone(),
            ))),
        );
        let state = AppState {
            catalog: Arc::clone(&self.catalog),
            ingest,
            pools: Arc::clone(&self.pools),
            format: Arc::clone(&self.format),
            engine: Arc::clone(&engine),
            env: self.env.clone(),
            controls: Arc::clone(&controls),
            cluster: Some(Arc::new(Cluster {
                membership: Arc::clone(&self.membership),
                remote,
                admission: Arc::new(Admission::new(Limits::default())),
                preferred_ingesters: PREFERRED_INGESTERS,
                volumes: tessera_server::Volumes::default(),
            })),
            budgets: Arc::new(Ledger::new()),
            auth: Arc::new(
                Auth::new(config.allow_anonymous)
                    .with_cluster_secret(config.cluster_secret.clone())
                    .with_oidc(config.oidc.clone()),
            ),
            load: Arc::clone(&self.load),
        };
        (state, engine, controls)
    }

    /// Starts the advisor's loop on a compact node (section 8), over the
    /// datasets whose partition slots this node owns.
    fn advise_layouts(
        &self,
        config: &Config,
        leases: Option<&Arc<Leases>>,
        engine: Arc<Engine>,
        controls: &Arc<ControlStores>,
        loops: &mut Vec<JoinHandle<()>>,
    ) {
        let Some(leases) = leases.filter(|_| config.roles.contains(&Role::Compact)) else {
            return;
        };
        loops.push(tokio::spawn(advisor::run(advisor::Advisor {
            store: Arc::clone(&self.store),
            env: self.env.clone(),
            catalog: Arc::clone(&self.catalog),
            controls: Arc::clone(controls),
            datasets: Arc::new(DatasetLogs::new(Arc::clone(&self.store), self.env.clone())),
            engine,
            owned: leases.owned.clone(),
            interval: config.advise,
        })));
    }

    /// Starts the `eval` role's loop over the rule groups this node owns
    /// (section 11.3), delivering through the node's own write path.
    fn evaluate_rules(
        &self,
        config: &Config,
        leases: Option<&Arc<Leases>>,
        state: &AppState,
        engine: Arc<Engine>,
        controls: &Arc<ControlStores>,
        loops: &mut Vec<JoinHandle<()>>,
    ) {
        let Some(leases) = leases.filter(|_| config.roles.contains(&Role::Eval)) else {
            return;
        };
        loops.push(tokio::spawn(evaluator::run(evaluator::Evaluator {
            store: Arc::clone(&self.store),
            env: self.env.clone(),
            catalog: Arc::clone(&self.catalog),
            controls: Arc::clone(controls),
            engine,
            owned: leases.owned.clone(),
            state: state.clone(),
            sink: Arc::new(evaluator::StateSink::new(state.clone())),
            notifier: Arc::new(HttpNotifier::new()),
            interval: config.eval,
            load: Arc::clone(&self.load),
        })));
    }
}

/// The leases a node holds and the handle that stops their loop.
type LeaseLoop = (
    Option<Arc<Leases>>,
    Option<(oneshot::Sender<()>, JoinHandle<()>)>,
);

/// The query engine of this node, reading files on peers chosen by
/// rendezvous hashing over the live query nodes (QE-15).
fn engine(
    pools: &Arc<Pools>,
    format: &Arc<dyn FileFormat>,
    env: &Env,
    membership: &Arc<Membership>,
    remote: &HttpRemote,
    spill_dir: PathBuf,
) -> Engine {
    Engine::new(
        Arc::clone(pools),
        Arc::clone(format),
        env.clone(),
        CpuPool::for_host(),
        QueryLimits::default(),
    )
    .with_spill_dir(spill_dir)
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
    gossip: heartbeat::Gossip,
) -> Result<(Arc<Registry>, Arc<Membership>), RunError> {
    let registry = Arc::new(Registry::new(
        Arc::clone(store),
        env.clone(),
        roles.clone(),
        advertise.clone(),
    ));
    let own = registry.announce().await?;
    let now = env.clock.now();
    let others: Vec<NodeRecord> = registry
        .list()
        .await?
        .into_iter()
        .filter(|r| r.node != own.node)
        .collect();
    if let Some(reader) = tessera_cluster::oldest_reader(&others, now)
        .filter(|r| r.reads_through < registry::DATA_FORMAT)
    {
        registry.withdraw().await?;
        return Err(RunError::Format {
            writes: registry::DATA_FORMAT,
            node: reader.node,
            reads_through: reader.reads_through,
        });
    }
    tracing::info!(?roles, %advertise, "joined the cluster");
    let membership = Arc::new(Membership::new(env.clone(), own));
    loops.push(tokio::spawn(heartbeat::run(
        Arc::clone(&registry),
        Arc::clone(&membership),
        heartbeat,
        gossip,
    )));
    Ok((registry, membership))
}

/// The client of the other nodes' internal routes: presenting the
/// cluster secret and trusting the cluster's CA (SE-3).
fn peer_client(config: &Config) -> Result<HttpRemote, RunError> {
    let ca = config.tls.as_ref().map(tls::ca_pem).transpose()?.flatten();
    HttpRemote::new()
        .with_secret(config.cluster_secret.clone())
        .with_root_ca(ca)
        .map_err(|error| {
            RunError::Tls(tls::TlsError::Ca {
                path: config
                    .tls
                    .as_ref()
                    .and_then(|t| t.ca.clone())
                    .unwrap_or_default(),
                source: std::io::Error::other(error.to_string()),
            })
        })
}

/// Serves the HTTP API and the `PostgreSQL` protocol until told to stop:
/// one sender and one task per server.
fn serve(
    state: AppState,
    listener: tokio::net::TcpListener,
    pg_listener: tokio::net::TcpListener,
    acceptor: Option<tokio_rustls::TlsAcceptor>,
) -> (Vec<oneshot::Sender<()>>, Vec<JoinHandle<()>>) {
    let (stop_pg, pg_stopped) = oneshot::channel::<()>();
    let pg_server = tokio::spawn(tessera_server::pg::serve(
        pg_listener,
        state.clone(),
        acceptor.clone(),
        async {
            let _ = pg_stopped.await;
        },
    ));
    let (stop_http, http_stopped) = oneshot::channel::<()>();
    let app = tessera_server::router(state);
    let http_server = tokio::spawn(async move {
        if let Some(acceptor) = acceptor {
            tls::serve_https(listener, acceptor, app, http_stopped).await;
            return;
        }
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
/// Releases the state of tenants nobody used for [`IDLE`] every
/// [`RELEASE_INTERVAL`] (CT-6): dataset logs, tombstone filters,
/// control stores and cached principals. A tenant that sends and
/// queries nothing then costs storage only.
async fn release_idle(state: AppState) {
    let mut ticks = tokio::time::interval(RELEASE_INTERVAL);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        let logs = state.engine.release_idle(IDLE);
        let stores = state.controls.release_idle(IDLE);
        state.auth.forget_expired();
        if logs + stores > 0 {
            tracing::debug!(logs, stores, "idle tenant state released");
        }
    }
}

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

/// Starts listening for the signals that ask the process to stop, an
/// interrupt from a terminal and the termination a supervisor such as
/// Kubernetes sends before it kills, and returns what completes at the
/// first of them. The listening starts with the call, not with the
/// wait, so that a signal arriving while the node starts is kept.
#[cfg(unix)]
fn shutdown_signal() -> impl Future<Output = ()> {
    use tokio::signal::unix::{SignalKind, signal};

    let signals = signal(SignalKind::interrupt())
        .and_then(|interrupt| Ok((interrupt, signal(SignalKind::terminate())?)));
    async move {
        match signals {
            Ok((mut interrupt, mut terminate)) => {
                let interrupted = Box::pin(interrupt.recv());
                let terminated = Box::pin(terminate.recv());
                futures::future::select(interrupted, terminated).await;
            }
            Err(error) => {
                tracing::warn!(error = %error, "cannot listen for signals; running until killed");
                std::future::pending::<()>().await;
            }
        }
        tracing::info!("shutdown requested");
    }
}

/// Completes at Ctrl-C, the one request to stop there is off Unix.
#[cfg(not(unix))]
fn shutdown_signal() -> impl Future<Output = ()> {
    async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %error, "cannot listen for ctrl-c; running until killed");
            std::future::pending::<()>().await;
        }
        tracing::info!("shutdown requested");
    }
}
