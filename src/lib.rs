//! # *SACS* - Simple Asynchronous Cron Scheduler
//!
//! `SACS` is a easy to use, small and lightweight task scheduler and executor for Tokio runtime.

mod event;
mod executor;
mod job;
mod queue;
/// Scheduler it heart of the `SACS`. This is an entry point to schedule and control on `Task`s and whole jobs runtime.
pub mod scheduler;
/// `Task` object represents single job with schedule. Use it to create workload of different types and post it to `Scheduler`.
pub mod task;
mod worker;

use job::JobId;
use std::{future::Future, pin::Pin, sync::Arc};
use task::TaskId;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, error::SendError, Receiver, Sender},
    RwLock,
};

const DEFAULT_CONTROL_CHANNEL_SIZE: usize = 16;

/// Convenient alias for `Result`.
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) type AsyncJob =
    dyn FnMut(JobId) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync;
pub(crate) type AsyncJobBoxed = Arc<RwLock<Box<AsyncJob>>>;

/// Represents `SACS` specific errors.
#[derive(Debug, Error)]
pub enum Error {
    /// Error during shutdown of any component due to `Tokio` runtime or `thread` error.
    #[error("unable to complete shutdown request")]
    IncompleteShutdown,
    /// Internal error, defines some kind of buggy state in events queue
    #[error("inconsistent indexes in the events queue")]
    InconsistentQueueContent,
    /// Job with such `Id` doesn't exist
    #[error("there's no job with id `{0:?}`")]
    IncorrectJobId(JobId),
    /// Task with such `Id` doesn't exist
    #[error("there's no task with id `{0:?}`")]
    IncorrectTaskId(TaskId),
    /// Invalid Cron expression provided during `Task` creation
    #[error("cron expression is invalid")]
    InvalidCronExpression(#[from] cron::error::Error),
    /// Unable to receive change state event due to closed or errored channel
    #[error("unable to receive event from control channel")]
    ReceivingChangeStateEvent,
    /// Unable to send change state event due to closed or errored channel
    #[error("unable to send event ot control channel")]
    SendingChangeStateEvent,
    /// Unable to complete state change request because shutdown has been initiated
    #[error("shutdown request received")]
    ShutdownRequested,
    /// Incorrect operation with system time
    #[error("unable to convert system time")]
    SystemTimeOperation(#[from] std::time::SystemTimeError),
}

struct ControlChannel<T> {
    sender: Sender<T>,
    receiver: RwLock<Receiver<T>>,
}

impl<T> ControlChannel<T> {
    fn new(size: usize) -> Self {
        let (sender, receiver) = mpsc::channel::<T>(size);
        Self {
            sender,
            receiver: RwLock::new(receiver),
        }
    }

    async fn send(&self, event: T) -> Result<(), SendError<T>> {
        self.sender.send(event).await
    }

    fn sender(&self) -> Sender<T> {
        self.sender.clone()
    }

    async fn receive(&self) -> Option<T> {
        self.receiver.write().await.recv().await
    }

    async fn send_event(&self, event: T) -> Result<()> {
        self.send(event)
            .await
            .map_err(|_e| Error::SendingChangeStateEvent)
    }
}

impl<T> Default for ControlChannel<T> {
    fn default() -> Self {
        Self::new(DEFAULT_CONTROL_CHANNEL_SIZE)
    }
}

/// Type of `Tokio` runtime to use for jobs worker.
#[derive(Debug, Default)]
pub enum WorkerType {
    /// Use current runtime instead of creating new one.
    ///
    /// This is the simplest and lightest worker because it uses runtime of the calling context.
    /// This is default type.
    #[default]
    CurrentRuntime,
    /// Creates new thread and runs new `Tokio` runtime of `CurrentThread` type. Single thread worker.
    CurrentThread,
    /// Creates new thread and runs new `Tokio` runtime of `MultiThread` type.
    ///
    /// Multi thread worker. Number of threads to use can be specified via parameter.
    /// Uses `Tokio` default (number of CPU cores) if `None`.
    MultiThread(Option<usize>),
}
