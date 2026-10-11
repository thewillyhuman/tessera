// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The `tessera` binary (AR-1): one executable for every role.
//!
//! `tessera run` starts every role in one process against a local
//! directory: the catalog and its administration API, OTLP ingest, the
//! query engine with SQL over HTTP and the `PostgreSQL` wire protocol.
//! Roles and the rest of the start-up configuration of OP-11 arrive with
//! their milestones.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

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
    /// Where the HTTP API listens. Loopback by default: there is no
    /// authentication yet.
    #[arg(long, default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    /// Where the `PostgreSQL` wire protocol listens. Loopback by default:
    /// there is no authentication yet.
    #[arg(long, default_value = "127.0.0.1:5432")]
    pg_listen: SocketAddr,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("TESSERA_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Run(args) => {
            tessera_node::run(tessera_node::Config {
                listen: args.listen,
                pg_listen: args.pg_listen,
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
