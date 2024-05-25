//! # **SACS** - Simple Asynchronous Cron Scheduler
//!
//! `SACS` is easy to use, lightweight scheduler and executor of repeatable async tasks for `Tokio` runtime.
//!
//! ## Features
//!
//! - Runs tasks with different types of schedule: once, with delay, by interval, with cron schedule.
//! - Uses current `Tokio` runtime or creates new one with a specified type, number of threads and limited parallelism.
//! - Allows task cancellation and getting current state of the task.
//! - Lightweight, small, easy to use.
//!
//! ## Quick start
//!
//! Just create `Scheduler` and add `Task` to it. Refer to [`scheduler`] and [`task`] modules documentation
//! for more examples and details of possible usage.
//!
//! ```rust
//! use sacs::{
//!     scheduler::{Scheduler, ShutdownOpts, TaskScheduler},
//!     task::{CronOpts, Task, TaskSchedule},
//!     Result,
//! };
//! use std::time::Duration;
//! use tracing::info;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     tracing_subscriber::fmt::init();
//!
//!     // Create scheduler with default config
//!     let scheduler = Scheduler::default();
//!
//!     // Create a task with cron schedule: repeat it every 3 seconds
//!     let cron = TaskSchedule::Cron("*/3 * * * * *".try_into()?, CronOpts::default());
//!     let task = Task::new(cron, |id| {
//!         Box::pin(async move {
//!             info!("Job {id} started.");
//!             // Actual async workload here
//!             tokio::time::sleep(Duration::from_secs(2)).await;
//!             // ...
//!             info!("Job {id} finished.");
//!         })
//!     });
//!
//!     // Post task to the scheduler and forget it :)
//!     let _task_id = scheduler.add(task).await?;
//!
//!     // ... and do any other async work in parallel
//!     tokio::time::sleep(Duration::from_secs(10)).await;
//!
//!     // It's not mandatory, but good to shut down scheduler
//!     // Wait for completion of all running jobs
//!     scheduler.shutdown(ShutdownOpts::WaitForFinish).await
//! }
//! ```

mod event;
mod executor;
pub mod job;
mod queue;
pub mod scheduler;
pub mod task;
mod worker;

use job::JobId;
use std::fmt::Debug;
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
    #[error("there's already scheduled task with id `{0:?}`")]
    DuplicatedTaskId(TaskId),
    #[error("cron expression is invalid")]
    InvalidCronExpression(#[from] cron::error::Error),
    /// Unable to receive change state event due to the closed channel or error
    #[error("unable to receive event from control channel")]
    ReceivingChangeStateEvent,
    /// Unable to send change state event due to the closed channel or error
    #[error("unable to send event ot control channel")]
    SendingChangeStateEvent,
    /// Unable to complete state change request because shutdown has been initiated
    #[error("shutdown request received")]
    ShutdownRequested,
    /// Incorrect operation with system time
    #[error("unable to convert system time")]
    SystemTimeOperation(#[from] std::time::SystemTimeError),
}

#[derive(Debug)]
struct ControlChannel<T: Debug> {
    sender: Sender<T>,
    receiver: RwLock<Receiver<T>>,
}

impl<T: Debug> ControlChannel<T> {
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

impl<T: Debug> Default for ControlChannel<T> {
    fn default() -> Self {
        Self::new(DEFAULT_CONTROL_CHANNEL_SIZE)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn constructors() {
        let cc = ControlChannel::<bool>::default();
        assert_eq!(cc.sender.capacity(), DEFAULT_CONTROL_CHANNEL_SIZE);
    }
}
