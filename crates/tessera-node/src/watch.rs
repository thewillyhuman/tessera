// SPDX-License-Identifier: Apache-2.0

//! Watching a node's own tasks.
//!
//! A node is a set of tasks that each run until the node stops: the
//! servers, the ingesters, the heartbeat, the lease loop and the loops
//! of its roles. One that ends before that, by a panic, leaves a node
//! that still answers its health check while part of it is gone: it
//! may hold leases it no longer works on, or serve without announcing
//! itself. Such a node is better stopped, in order, and started again
//! by whatever supervises it than left running.

use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use futures::future::{Either, select};
use tokio::task::JoinHandle;

/// How often a node looks at its tasks.
pub(crate) const EVERY: Duration = Duration::from_secs(1);

/// Whether every one of `tasks` is still running.
pub(crate) fn all_running<'a>(tasks: impl IntoIterator<Item = &'a JoinHandle<()>>) -> bool {
    tasks.into_iter().all(|task| !task.is_finished())
}

/// Waits for `stop` to complete, looking every `every` at whether the
/// node is still `whole`. Returns whether the wait ended because it
/// was not.
pub(crate) async fn broken_before(
    stop: impl Future<Output = ()>,
    whole: impl Fn() -> bool,
    every: Duration,
) -> bool {
    let mut stop = pin!(stop);
    let mut looks = tokio::time::interval(every);
    loop {
        let look = pin!(looks.tick());
        match select(&mut stop, look).await {
            Either::Left(_) => return false,
            Either::Right(_) if whole() => {}
            Either::Right(_) => return true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_task_that_ends_breaks_the_node() {
        let forever = tokio::spawn(std::future::pending::<()>());
        let failing = tokio::spawn(async { panic!("a loop that panics") });
        let tasks = [forever, failing];
        let broken = broken_before(std::future::pending(), || all_running(&tasks), EVERY).await;
        assert!(broken);
    }

    #[tokio::test(start_paused = true)]
    async fn a_whole_node_waits_for_the_request_to_stop() {
        let tasks = [tokio::spawn(std::future::pending::<()>())];
        let stop = tokio::time::sleep(Duration::from_secs(60));
        let broken = broken_before(stop, || all_running(&tasks), EVERY).await;
        assert!(!broken);
    }
}
