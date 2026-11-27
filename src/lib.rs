#![deny(unsafe_code, warnings, missing_docs)]

//! # **SACS** - Simple Asynchronous Cron Scheduler
//!
//! `SACS` is easy to use, lightweight scheduler and executor of repeatable async tasks for `Tokio` runtime.
//!
//! ## Features
//!
//! - Runs tasks with different types of schedule: once, with delay, by interval, with a cron schedule.
//! - Uses current `Tokio` runtime or creates new one with the specified type, number of threads and limited parallelism.
//! - Allows task cancellation, getting current state and runtime statistics of the task.
//! - Task execution time may be limited.
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
//!
//! # Cron schedules with timezone
//! A cron expression without a timezone is evaluated in the system's local timezone. To pin a schedule to a specific
//! timezone, prefix the expression with `TZ=`, for example `TZ=Europe/Kyiv 0 12 * * *` or `TZ=UTC @daily`,
//! see [`CronSchedule`](task::CronSchedule) and [`cron-lite`](https://docs.rs/cron-lite/) documentation for details.
//!
//! Timezone names are resolved at runtime from the operating system's timezone database (on Unix,
//! `/usr/share/zoneinfo` or the directory set in the `TZDIR` environment variable). Make sure `tzdata` is installed
//! wherever your application runs: minimal container images (distroless, scratch, Alpine without the `tzdata` package)
//! don't have it, and any `TZ=` schedule fails to parse there with [`Error::InvalidCronExpression`].
//!
//! # Feature flags
//! - `async-trait` - declares [`TaskScheduler`](scheduler::TaskScheduler) methods as `async fn` instead of
//!   functions returning `impl Future + Send`.

mod event;
mod executor;
pub mod job;
mod queue;
pub mod scheduler;
pub mod task;
mod worker;

use job::JobId;
use std::fmt::Debug;
use std::time::SystemTime;
use std::{future::Future, pin::Pin, sync::Arc};
use task::TaskId;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex,
};

const DEFAULT_CONTROL_CHANNEL_SIZE: usize = 16;

/// Convenient alias for `Result`.
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) type AsyncJob =
    dyn FnMut(JobId) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync;
pub(crate) type AsyncJobBoxed = Arc<tokio::sync::RwLock<Box<AsyncJob>>>;

/// Represents `SACS` specific errors.
#[derive(Debug, Error)]
#[non_exhaustive]
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
    /// Task with such `Id` already exists, duplicates isn't allowed
    #[error("there's already scheduled task with id `{0:?}`")]
    DuplicatedTaskId(TaskId),
    /// Invalid Cron expression provided during `Task` creation
    #[error("cron expression is invalid")]
    InvalidCronExpression(#[from] cron_lite::error::CronError),
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
    /// Task's cron schedule has no upcoming occurrence (e.g. it points to an impossible
    /// calendar date, or lies entirely in the past), so it can never be scheduled.
    #[error("cron schedule has no upcoming occurrence")]
    NoUpcomingEvent,
    /// Task's schedule requires time arithmetic (delay or interval) that overflows the
    /// representable range of `SystemTime`.
    #[error("task schedule duration overflows representable system time range")]
    ScheduleOverflow,
    /// Requested worker configuration is invalid, e.g. zero worker threads.
    #[error("invalid worker configuration")]
    InvalidWorkerConfig,
    /// Scheduler's background loop has terminated, so it can't accept new requests
    /// and its state should be considered stale.
    #[error("scheduler has terminated and can't process requests anymore")]
    SchedulerTerminated,
}

#[derive(Debug)]
struct ControlChannel<T: Debug> {
    sender: Sender<T>,
    receiver: Mutex<Receiver<T>>,
}

impl<T: Debug> ControlChannel<T> {
    fn new(size: usize) -> Self {
        let (sender, receiver) = mpsc::channel::<T>(size);
        Self {
            sender,
            receiver: Mutex::new(receiver),
        }
    }

    fn sender(&self) -> Sender<T> {
        self.sender.clone()
    }

    async fn receive(&self) -> Option<T> {
        self.receiver.lock().await.recv().await
    }

    /// Best-effort, non-blocking send for fungible "something changed, wake up and
    /// reconsider" notifications, where losing one under backpressure is harmless because
    /// the receiver always re-derives its own next action from shared state rather than
    /// from the notification's payload (see `Queue`'s wake-up channel). Never blocks the
    /// sender.
    fn notify(&self, event: T) {
        let _ = self.sender.try_send(event);
    }
}

impl<T: Debug> Default for ControlChannel<T> {
    fn default() -> Self {
        Self::new(DEFAULT_CONTROL_CHANNEL_SIZE)
    }
}

/// Format used to render timestamps in `Debug` output, e.g. `2026-11-27 10:15:00.123 +02:00`.
const DEBUG_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f %:z";

/// Renders `time` in the system's local timezone for `Debug` output.
///
/// Falls back to the `SystemTime`'s own `Debug` representation if `time` is outside the range
/// supported by `jiff`.
pub(crate) fn format_local_time(time: SystemTime) -> String {
    match jiff::Timestamp::try_from(time) {
        Ok(ts) => ts
            .to_zoned_datetime(jiff::tz::TimeZone::system())
            .strftime(DEBUG_TIME_FORMAT)
            .to_string(),
        Err(_) => format!("{time:?}"),
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

    #[test]
    fn format_local_time_round_trips() {
        let time = SystemTime::UNIX_EPOCH + std::time::Duration::new(1_764_236_100, 123_000_000);
        let formatted = format_local_time(time);

        let parsed = jiff::fmt::strtime::parse(DEBUG_TIME_FORMAT, &formatted)
            .and_then(|tm| tm.to_timestamp())
            .unwrap();
        assert_eq!(SystemTime::try_from(parsed).unwrap(), time);
    }

    #[test]
    fn format_local_time_out_of_range_falls_back_to_debug() {
        // ~35k years after the epoch: representable by `SystemTime`, but beyond `jiff`'s range.
        let Some(time) =
            SystemTime::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(1 << 40))
        else {
            return;
        };
        assert_eq!(format_local_time(time), format!("{time:?}"));
    }
}
