// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `tessera` binary (AR-1): one executable for every role.
//!
//! `tessera run` starts a node against a local directory or an
//! S3-compatible bucket, by default with every role: the catalog and its administration API, OTLP
//! ingest, the query engine with SQL over HTTP and the `PostgreSQL` wire
//! protocol, intake and compaction. The start-up configuration is what
//! OP-11 allows: the root pool, the roles, the listen addresses and the
//! address peers reach the node at; the root pool's registry is the
//! membership seed.

use std::io::IsTerminal;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tessera_cluster::Role;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Tessera: a unified observability database.
#[derive(Parser, Debug)]
#[command(name = "tessera", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Starts a node. With no options: every role, in one process, against
    /// a local directory.
    Run(RunArgs),
}

/// Options of `tessera run`.
#[derive(clap::Args, Debug)]
struct RunArgs {
    /// The directory that acts as the root pool.
    #[arg(long, default_value = "./tessera-data")]
    data_dir: PathBuf,
    /// The root pool as a bucket, or a prefix in one, in place of a
    /// directory: `s3://<bucket>` or `s3://<bucket>/<prefix>`. The
    /// region, the credentials and, for a store other than Amazon's,
    /// the endpoint are read from the `AWS_*` variables as the AWS
    /// tools read them.
    #[arg(long, conflicts_with = "data_dir")]
    root_pool: Option<String>,
    /// Where queries spill when a memory grant runs out; the system's
    /// temporary directory by default.
    #[arg(long, default_value_os_t = std::env::temp_dir().join("tessera-spill"))]
    spill_dir: PathBuf,
    /// A directory to keep on local disk what was read of the objects
    /// that never change: data files, their indexes and writer-log
    /// entries. With a bucket as the root pool, each is then asked of
    /// it once. The directory is the cache's alone and is emptied at
    /// every start.
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    /// The most bytes the cache keeps; eight gibibytes by default.
    #[arg(long, default_value_t = 8 << 30, requires = "cache_dir")]
    cache_bytes: u64,
    /// Where the HTTP API listens. Loopback by default, since requests
    /// without an API key are served unless --no-anonymous is given.
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    /// Where the `PostgreSQL` wire protocol listens. Loopback by default,
    /// for the same reason as --listen.
    #[arg(long, default_value = "127.0.0.1:5432")]
    pg_listen: SocketAddr,
    /// The roles this node runs, comma-separated: ingest, query, compact,
    /// eval. Every role by default.
    #[arg(long, value_delimiter = ',', default_values_t = Role::ALL)]
    roles: Vec<Role>,
    /// The base URL peers reach this node's HTTP API at. Needed when the
    /// listen address is not one they can use, such as 0.0.0.0.
    #[arg(long)]
    advertise: Option<String>,
    /// A node to gossip membership with on every heartbeat, by its base
    /// URL; repeatable. The registry in the root pool seeds membership
    /// without it.
    #[arg(long = "seed")]
    seeds: Vec<String>,
    /// A `host:port` whose addresses are nodes to gossip membership
    /// with, looked up on every heartbeat: a headless service.
    #[arg(long)]
    seed_dns: Option<String>,
    /// The URI external engines read the data directory at, such as
    /// `s3://bucket/prefix` or `file:///var/lib/tessera`. Given, the node
    /// publishes Apache Iceberg metadata for every dataset under
    /// `<tenant>/<dataset>/iceberg/<kind>`; without it nothing is exported.
    #[arg(long)]
    export_location: Option<String>,
    /// Refuse requests without an API key. By default a request without
    /// one acts as a cluster administrator, which suits a single node on
    /// the loopback interface and nothing anyone else can reach.
    #[arg(long)]
    no_anonymous: bool,
    /// Writer logs the node appends to, one task each; writes go to them
    /// in turn, so ingest takes as many cores.
    #[arg(long, default_value_t = 1)]
    ingest_writers: usize,
    /// The secret the nodes of a cluster present to each other on the
    /// `/internal` routes (SE-3); the same on every node. Without it
    /// those routes are open, which suits a private network only.
    #[arg(long, env = "TESSERA_CLUSTER_SECRET")]
    cluster_secret: Option<String>,
    /// A file of named tenant keys, one `name = <64 hex digits>` per
    /// line (SE-2). A tenant whose `encryption_key` setting names one
    /// has its blocks and files sealed under it; keep the file out of
    /// the data directory and the same on every node.
    #[arg(long, env = "TESSERA_KEYRING")]
    keyring: Option<PathBuf>,
    /// The PEM certificate chain the HTTP API and the `PostgreSQL`
    /// protocol serve TLS with (SE-3), with `--tls-key`. Without them
    /// connections are plain.
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<std::path::PathBuf>,
    /// The PEM private key of `--tls-cert`.
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<std::path::PathBuf>,
    /// A PEM CA certificate the other nodes' certificates are checked
    /// against, for a cluster with its own CA; the system's roots
    /// otherwise.
    #[arg(long)]
    tls_ca: Option<std::path::PathBuf>,
    /// A PEM CA certificate client certificates are checked against
    /// (SE-1). A client presenting a certificate it signed is the tenant
    /// the subject's organization (O) names, with the roles its
    /// organizational units (OU) name; one presenting none still
    /// connects and needs a key or a token.
    #[arg(long, requires = "tls_cert")]
    tls_client_ca: Option<std::path::PathBuf>,
    /// The `OpenID` Connect issuer bearer tokens are checked against
    /// (SE-1), as its tokens name it; its discovery document is read
    /// under it. Without it, only API keys are accepted.
    #[arg(long, requires = "oidc_audience")]
    oidc_issuer: Option<String>,
    /// The audience a token must carry.
    #[arg(long, requires = "oidc_issuer")]
    oidc_audience: Option<String>,
    /// The claim naming the tenant a token acts for.
    #[arg(long, default_value = "tenant")]
    oidc_tenant_claim: String,
    /// The claim listing the roles a token acts with.
    #[arg(long, default_value = "roles")]
    oidc_roles_claim: String,
    /// How long the ingester waits for more rows before writing a
    /// segment, in milliseconds (ST-6). Acknowledgment latency is this
    /// plus the store's write; a longer wait makes larger segments.
    #[arg(long, default_value_t = 50)]
    linger_ms: u64,
}

/// The keyring at `path`; none without a path.
fn read_keyring(path: Option<&std::path::Path>) -> Result<tessera_node::Keyring, String> {
    let Some(path) = path else {
        return Ok(tessera_node::Keyring::default());
    };
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    tessera_node::Keyring::parse(&text).map_err(|e| format!("{}: {e}", path.display()))
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        // Colours are for a person at a terminal; a supervisor's log
        // store would keep the escapes as noise.
        .with_ansi(std::io::stdout().is_terminal())
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("TESSERA_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .finish()
        .with(tessera_node::observe::install())
        .init();
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Run(args) => {
            let keyring = match read_keyring(args.keyring.as_deref()) {
                Ok(keyring) => keyring,
                Err(error) => {
                    tracing::error!(error = %error, "cannot read the keyring");
                    return ExitCode::FAILURE;
                }
            };
            tessera_node::run(tessera_node::Config {
                listen: args.listen,
                pg_listen: args.pg_listen,
                roles: args.roles.into_iter().collect(),
                advertise: args.advertise,
                seeds: args.seeds,
                seed_dns: args.seed_dns,
                export_location: args.export_location,
                allow_anonymous: !args.no_anonymous,
                cluster_secret: args.cluster_secret,
                tls: args
                    .tls_cert
                    .zip(args.tls_key)
                    .map(|(cert, key)| tessera_node::Tls {
                        cert,
                        key,
                        ca: args.tls_ca,
                        client_ca: args.tls_client_ca,
                    }),
                oidc: args
                    .oidc_issuer
                    .zip(args.oidc_audience)
                    .map(|(issuer, audience)| tessera_node::Oidc {
                        issuer,
                        audience,
                        tenant_claim: args.oidc_tenant_claim,
                        roles_claim: args.oidc_roles_claim,
                    }),
                ingest: tessera_ingest::IngestConfig {
                    linger: std::time::Duration::from_millis(args.linger_ms),
                    writers: args.ingest_writers.max(1),
                    ..tessera_ingest::IngestConfig::default()
                },
                keyring,
                spill_dir: args.spill_dir,
                root_pool: args.root_pool,
                cache: args.cache_dir.map(|dir| tessera_node::DiskCache {
                    dir,
                    bytes: args.cache_bytes,
                }),
                ..tessera_node::Config::single_node(args.data_dir)
            })
            .await
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %error, "tessera stopped with an error");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_has_loopback_and_local_directory_defaults() {
        let cli = Cli::try_parse_from(["tessera", "run"]).unwrap();
        let Command::Run(args) = cli.command;
        assert_eq!(args.listen, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(args.pg_listen, "127.0.0.1:5432".parse().unwrap());
        assert_eq!(args.data_dir, PathBuf::from("./tessera-data"));
        assert_eq!(args.roles, Role::ALL);
        assert_eq!(args.advertise, None);
    }

    #[test]
    fn roles_are_a_comma_separated_list() {
        let cli = Cli::try_parse_from([
            "tessera",
            "run",
            "--roles",
            "query,compact",
            "--advertise",
            "http://node-1:8080",
        ])
        .unwrap();
        let Command::Run(args) = cli.command;
        assert_eq!(args.roles, [Role::Query, Role::Compact]);
        assert_eq!(args.advertise.as_deref(), Some("http://node-1:8080"));
        let Command::Run(seeded) = Cli::try_parse_from([
            "tessera",
            "run",
            "--seed",
            "http://a:8080",
            "--seed",
            "http://b:8080",
            "--seed-dns",
            "tessera:8080",
        ])
        .unwrap()
        .command;
        assert_eq!(seeded.seeds, ["http://a:8080", "http://b:8080"]);
        assert_eq!(seeded.seed_dns.as_deref(), Some("tessera:8080"));
        assert!(Cli::try_parse_from(["tessera", "run", "--roles", "boss"]).is_err());
        let Command::Run(args) = Cli::try_parse_from(["tessera", "run", "--linger-ms", "200"])
            .unwrap()
            .command;
        assert_eq!(args.linger_ms, 200);
    }

    #[test]
    fn run_accepts_overrides() {
        let cli = Cli::try_parse_from([
            "tessera",
            "run",
            "--data-dir",
            "/tmp/x",
            "--listen",
            "0.0.0.0:9000",
        ])
        .unwrap();
        let Command::Run(args) = cli.command;
        assert_eq!(args.listen.port(), 9000);
        assert_eq!(args.data_dir, PathBuf::from("/tmp/x"));
    }

    #[test]
    fn the_root_pool_is_a_directory_or_a_bucket() {
        let bucket = ["tessera", "run", "--root-pool", "s3://logs/prod"];
        let Command::Run(args) = Cli::try_parse_from(bucket).unwrap().command;
        assert_eq!(args.root_pool.as_deref(), Some("s3://logs/prod"));
        let both = [
            "tessera",
            "run",
            "--root-pool",
            "s3://logs",
            "--data-dir",
            "/tmp/x",
        ];
        assert!(Cli::try_parse_from(both).is_err());
    }

    #[test]
    fn the_cache_is_a_directory_and_a_size() {
        let Command::Run(none) = Cli::try_parse_from(["tessera", "run"]).unwrap().command;
        assert_eq!(none.cache_dir, None);
        let sized = [
            "tessera",
            "run",
            "--cache-dir",
            "/var/cache/tessera",
            "--cache-bytes",
            "1024",
        ];
        let Command::Run(args) = Cli::try_parse_from(sized).unwrap().command;
        assert_eq!(args.cache_dir, Some(PathBuf::from("/var/cache/tessera")));
        assert_eq!(args.cache_bytes, 1024);
        let no_directory = ["tessera", "run", "--cache-bytes", "1024"];
        assert!(Cli::try_parse_from(no_directory).is_err());
    }

    #[test]
    fn a_subcommand_is_required() {
        assert!(Cli::try_parse_from(["tessera"]).is_err());
    }
}
