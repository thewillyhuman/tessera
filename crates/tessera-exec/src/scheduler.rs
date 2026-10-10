// SPDX-License-Identifier: LicenseRef-Tessera-Pending

//! The CPU pool (RS-4, QE-9).
//!
//! Decode, kernels, sort and scan run as tasks on dedicated threads,
//! never on the async I/O runtime. A task is a closure; its result comes
//! back through a [`Task`] future, so async code awaits CPU work without
//! blocking a runtime thread.
//!
//! Tasks are queued and taken by the first free worker, which is fair
//! and bounded; a per-worker deque with stealing is the follow-up when
//! profiling shows the shared queue contended.

use std::collections::VecDeque;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::thread::JoinHandle;

use tokio::sync::oneshot;

type Job = Box<dyn FnOnce() + Send>;

struct Queue {
    jobs: Mutex<(VecDeque<Job>, bool)>,
    available: Condvar,
}

/// A pool of threads running CPU-bound tasks.
pub struct CpuPool {
    queue: Arc<Queue>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

/// The task did not produce a result.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
pub enum TaskError {
    /// The task panicked; the worker survived.
    #[error("the task panicked")]
    Panicked,
    /// The pool was shut down before the task ran.
    #[error("the pool was shut down")]
    Cancelled,
}

/// The pending result of a task.
pub struct Task<T> {
    receiver: oneshot::Receiver<Result<T, TaskError>>,
}

impl<T> Future for Task<T> {
    type Output = Result<T, TaskError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(TaskError::Cancelled)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl CpuPool {
    /// A pool of `threads` workers, at least one.
    ///
    /// # Panics
    ///
    /// If the operating system refuses to create a thread, which leaves
    /// the node unable to run any query.
    #[must_use]
    pub fn new(threads: usize) -> Arc<Self> {
        let queue = Arc::new(Queue {
            jobs: Mutex::new((VecDeque::new(), false)),
            available: Condvar::new(),
        });
        let workers = (0..threads.max(1))
            .map(|i| {
                let queue = Arc::clone(&queue);
                std::thread::Builder::new()
                    .name(format!("tessera-cpu-{i}"))
                    .spawn(move || worker(&queue))
                    .expect("spawning a CPU worker thread")
            })
            .collect();
        Arc::new(CpuPool {
            queue,
            workers: Mutex::new(workers),
        })
    }

    /// A pool with one worker per available CPU.
    #[must_use]
    pub fn for_host() -> Arc<Self> {
        let threads = std::thread::available_parallelism().map_or(1, usize::from);
        Self::new(threads)
    }

    /// How many workers the pool has.
    #[must_use]
    pub fn threads(&self) -> usize {
        self.workers.lock().map_or(0, |w| w.len())
    }

    /// Queues `f` and returns its pending result.
    pub fn spawn<T, F>(&self, f: F) -> Task<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let job: Job = Box::new(move || {
            let result = catch_unwind(AssertUnwindSafe(f)).map_err(|_| TaskError::Panicked);
            // The caller may have gone away; nobody needs the result then.
            let _ = sender.send(result);
        });
        if let Ok(mut guard) = self.queue.jobs.lock() {
            if guard.1 {
                // Shut down: dropping the sender cancels the task.
                return Task { receiver };
            }
            guard.0.push_back(job);
        }
        self.queue.available.notify_one();
        Task { receiver }
    }
}

fn worker(queue: &Queue) {
    loop {
        let job = {
            let Ok(mut guard) = queue.jobs.lock() else {
                return;
            };
            loop {
                if let Some(job) = guard.0.pop_front() {
                    break job;
                }
                if guard.1 {
                    return;
                }
                guard = match queue.available.wait(guard) {
                    Ok(guard) => guard,
                    Err(_) => return,
                };
            }
        };
        job();
    }
}

impl Drop for CpuPool {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.queue.jobs.lock() {
            guard.1 = true;
            guard.0.clear();
        }
        self.queue.available.notify_all();
        if let Ok(mut workers) = self.workers.lock() {
            for worker in workers.drain(..) {
                let _ = worker.join();
            }
        }
    }
}

impl std::fmt::Debug for CpuPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CpuPool")
            .field("threads", &self.threads())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runs_tasks_on_its_own_threads_and_returns_results() {
        let pool = CpuPool::new(2);
        assert_eq!(pool.threads(), 2);
        let tasks: Vec<_> = (0..8)
            .map(|i| {
                pool.spawn(move || {
                    let name = std::thread::current().name().unwrap_or("").to_owned();
                    (i * i, name)
                })
            })
            .collect();
        for (i, task) in tasks.into_iter().enumerate() {
            let (square, name) = task.await.unwrap();
            assert_eq!(square, i * i);
            assert!(name.starts_with("tessera-cpu-"), "{name}");
        }
    }

    #[tokio::test]
    async fn a_panicking_task_fails_alone() {
        let pool = CpuPool::new(1);
        let bad = pool.spawn(|| panic!("boom"));
        assert_eq!(bad.await.unwrap_err(), TaskError::Panicked);
        let good = pool.spawn(|| 7);
        assert_eq!(good.await.unwrap(), 7);
    }

    #[tokio::test]
    async fn shutdown_cancels_queued_tasks() {
        let pool = CpuPool::new(1);
        let (started, has_started) = std::sync::mpsc::channel();
        let slow = pool.spawn(move || {
            started.send(()).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(50));
        });
        has_started.recv().unwrap();
        let queued = pool.spawn(|| 1);
        drop(pool);
        assert!(slow.await.is_ok());
        assert_eq!(queued.await.unwrap_err(), TaskError::Cancelled);
    }
}
