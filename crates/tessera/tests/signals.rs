// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The binary as a supervisor runs it: started with its output on a
//! pipe, then told to stop.

#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

/// A node on free ports over a directory of its own, and its output.
struct Running {
    node: Child,
    output: BufReader<ChildStdout>,
    _dir: tempfile::TempDir,
}

impl Running {
    fn start() -> Running {
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
            .env_remove("NO_COLOR")
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let output = BufReader::new(node.stdout.take().unwrap());
        Running {
            node,
            output,
            _dir: dir,
        }
    }

    /// The lines the node writes up to the one that holds `text`.
    fn lines_until(&mut self, text: &str) -> Vec<String> {
        let mut lines = Vec::new();
        for line in (&mut self.output).lines().map(Result::unwrap) {
            let last = line.contains(text);
            lines.push(line);
            if last {
                return lines;
            }
        }
        panic!("the node never wrote {text:?}: {lines:?}");
    }

    fn terminate(&self) {
        let sent = Command::new("kill")
            .args(["-TERM", &self.node.id().to_string()])
            .status()
            .unwrap();
        assert!(sent.success());
    }
}

/// OP-4: removing a node is stopping it, and what stops a process in
/// a container is the termination signal. The node must take it as it
/// takes an interrupt: close its writer log, withdraw from the registry
/// and exit by itself, rather than be killed with its work left for
/// recovery.
#[test]
fn a_terminated_node_stops_by_itself() {
    let mut running = Running::start();
    running.lines_until("postgresql protocol listening");
    running.terminate();

    let deadline = Instant::now() + Duration::from_secs(30);
    let exit = loop {
        if let Some(exit) = running.node.try_wait().unwrap() {
            break exit;
        }
        assert!(Instant::now() < deadline, "the node did not stop");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(exit.success(), "killed rather than stopped: {exit}");
    running.lines_until("stopped");
}

/// What a supervisor collects from the node's output is text for a log
/// store, not for a terminal: it carries no colours.
#[test]
fn a_log_written_to_a_pipe_carries_no_terminal_escapes() {
    let mut running = Running::start();
    let lines = running.lines_until("postgresql protocol listening");
    running.terminate();
    running.node.wait().unwrap();
    let escaped: Vec<&String> = lines.iter().filter(|l| l.contains('\u{1b}')).collect();
    assert!(escaped.is_empty(), "{escaped:?}");
}
