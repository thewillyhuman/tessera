// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The binary as a supervisor runs it: started, then told to stop.

#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// OP-4: removing a node is stopping it, and what stops a process in
/// a container is the termination signal. The node must take it as it
/// takes an interrupt: close its writer log, withdraw from the registry
/// and exit by itself, rather than be killed with its work left for
/// recovery.
#[test]
fn a_terminated_node_stops_by_itself() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = Command::new(env!("CARGO_BIN_EXE_tessera"))
        .args([
            "run",
            "--listen",
            "127.0.0.1:0",
            "--pg-listen",
            "127.0.0.1:0",
        ])
        .arg("--data-dir")
        .arg(dir.path().join("data"))
        .arg("--spill-dir")
        .arg(dir.path().join("spill"))
        .env("NO_COLOR", "1")
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(node.stdout.take().unwrap())
        .lines()
        .map(Result::unwrap);
    assert!(
        lines.any(|line| line.contains("postgresql protocol listening")),
        "the node started"
    );

    let sent = Command::new("kill")
        .args(["-TERM", &node.id().to_string()])
        .status()
        .unwrap();
    assert!(sent.success());

    let deadline = Instant::now() + Duration::from_secs(30);
    let exit = loop {
        if let Some(exit) = node.try_wait().unwrap() {
            break exit;
        }
        assert!(Instant::now() < deadline, "the node did not stop");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(exit.success(), "killed rather than stopped: {exit}");
    assert!(
        lines.any(|line| line.contains("stopped")),
        "the node said it stopped"
    );
}
