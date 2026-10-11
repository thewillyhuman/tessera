// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `tessera` binary (AR-1): one executable for every role.
//!
//! `tessera run` starts a node against a local directory, by default
//! with every role: the catalog and its administration API, OTLP
//! ingest, the query engine with SQL over HTTP and the `PostgreSQL` wire
//! protocol, intake and compaction. The start-up configuration is what
//! OP-11 allows: the root pool, the roles, the listen addresses and the
//! address peers reach the node at; the root pool's registry is the
//! membership seed.

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
    /// Refuse requests without an API key. By default a request without
    /// one acts as a cluster administrator, which suits a single node on
    /// the loopback interface and nothing anyone else can reach.
    #[arg(long)]
    no_anonymous: bool,
    /// The secret the nodes of a cluster present to each other on the
    /// `/internal` routes (SE-3); the same on every node. Without it
    /// those routes are open, which suits a private network only.
    #[arg(long, env = "TESSERA_CLUSTER_SECRET")]
    cluster_secret: Option<String>,
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
    /// How long the ingester waits for more rows before writing a
    /// segment, in milliseconds (ST-6). Acknowledgment latency is this
    /// plus the store's write; a longer wait makes larger segments.
    #[arg(long, default_value_t = 50)]
    linger_ms: u64,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
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
            tessera_node::run(tessera_node::Config {
                listen: args.listen,
                pg_listen: args.pg_listen,
                roles: args.roles.into_iter().collect(),
                advertise: args.advertise,
                allow_anonymous: !args.no_anonymous,
                cluster_secret: args.cluster_secret,
                tls: args
                    .tls_cert
                    .zip(args.tls_key)
                    .map(|(cert, key)| tessera_node::Tls {
                        cert,
                        key,
                        ca: args.tls_ca,
                    }),
                ingest: tessera_ingest::IngestConfig {
                    linger: std::time::Duration::from_millis(args.linger_ms),
                    ..tessera_ingest::IngestConfig::default()
                },
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
    fn a_subcommand_is_required() {
        assert!(Cli::try_parse_from(["tessera"]).is_err());
    }
}
