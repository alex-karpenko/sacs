# **SACS** - Simple Asynchronous Cron Scheduler

`SACS` is easy to use, lightweight scheduler and executor of repeatable async tasks for `Tokio` runtime.

<p>
<a href="https://github.com/alex-karpenko/sacs/actions/workflows/ci.yaml" rel="nofollow"><img src="https://img.shields.io/github/actions/workflow/status/alex-karpenko/sacs/ci.yaml?label=ci" alt="CI status"></a>
<a href="https://github.com/alex-karpenko/sacs/actions/workflows/audit.yaml" rel="nofollow"><img src="https://img.shields.io/github/actions/workflow/status/alex-karpenko/sacs/audit.yaml?label=audit" alt="Audit status"></a>
<a href="https://github.com/alex-karpenko/sacs/actions/workflows/publish.yaml" rel="nofollow"><img src="https://img.shields.io/github/actions/workflow/status/alex-karpenko/sacs/publish.yaml?label=publish" alt="Crates.io publishing status"></a>
<a href="https://docs.rs/sacs" rel="nofollow"><img src="https://img.shields.io/docsrs/sacs" alt="docs.rs status"></a>
<a href="https://crates.io/crates/sacs" rel="nofollow"><img src="https://img.shields.io/crates/v/sacs" alt="Version at Crates.io"></a>
<a href="https://github.com/alex-karpenko/sacs/blob/HEAD/LICENSE" rel="nofollow"><img src="https://img.shields.io/crates/l/sacs" alt="License"></a>
</p>
<!-- <a href="https://crates.io/crates/sacs" rel="nofollow"><img alt="Crates.io MSRV" src="https://img.shields.io/crates/msrv/sacs"></a> -->

## Features

- Run tasks with different types of schedule: once, with delay, by interval, with a cron schedule.
- Uses current `Tokio` runtime or creates new one with specified type, number of threads and limited parallelism.
- Allows task cancellation, getting current state and runtime statistics of the task.
- Task execution time may be limited.
- Lightweight, small, easy to use.

## Quick start

Just create [`Scheduler`](https://docs.rs/sacs/latest/sacs/scheduler/struct.Scheduler.html) and
add [`Task`](https://docs.rs/sacs/latest/sacs/task/struct.Task.html) to it.
Refer to the [`crate's documentation`](https://docs.rs/sacs/latest/sacs/) for more examples and details of possible usage.

```rust
use sacs::{
    scheduler::{Scheduler, ShutdownOpts, TaskScheduler},
    task::{CronOpts, Task, TaskSchedule},
    Result,
};
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // Create scheduler with default config
    let scheduler = Scheduler::default();

    // Create a task with cron schedule: repeat it every 3 seconds
    let cron = TaskSchedule::Cron("*/3 * * * * *".try_into()?, CronOpts::default());
    let task = Task::new(cron, |id| {
        Box::pin(async move {
            info!("Job {id} started.");
            // Actual async workload here
            tokio::time::sleep(Duration::from_secs(2)).await;
            // ...
            info!("Job {id} finished.");
        })
    });

    // Post task to the scheduler and forget it :)
    let _task_id = scheduler.add(task).await?;

    // ... and do any other async work in parallel
    tokio::time::sleep(Duration::from_secs(10)).await;

    // It's not mandatory, but good to shut down scheduler
    // Wait for completion of all running jobs
    scheduler.shutdown(ShutdownOpts::WaitForFinish).await
}
```

## Cron schedules with timezone

A cron expression without a timezone is evaluated in the system's local timezone. To pin a schedule to a specific
timezone, prefix the expression with `TZ=`, for example `TZ=Europe/Kyiv 0 12 * * *` or `TZ=UTC @daily`,
see [`cron-lite`](https://docs.rs/cron-lite/) documentation for details.

Timezone names are resolved at runtime from the operating system's timezone database (on Unix, `/usr/share/zoneinfo`
or the directory set in the `TZDIR` environment variable). Make sure `tzdata` is installed wherever your application
runs: minimal container images (distroless, scratch, Alpine without the `tzdata` package) don't have it, and any `TZ=`
schedule fails to parse there with `Error::InvalidCronExpression`.

## Feature flags

- `async-trait` - declares `TaskScheduler` methods as `async fn` instead of functions returning `impl Future + Send`.

## Breaking changes & migration

`0.10.0` introduces a few breaking changes. The highlights:

- [`Error`](https://docs.rs/sacs/latest/sacs/enum.Error.html) is now `#[non_exhaustive]` --
  an exhaustive `match` on it needs a wildcard arm.
- The `cron-lite` dependency was updated to `0.5.0`; its `CronError` variants changed shape
  (tuple to struct fields) and it is also `#[non_exhaustive]` now. Update any code that
  destructures `Error::InvalidCronExpression(cron_lite::error::CronError::...)`.
- `chrono` and `chrono-tz` were replaced by [`jiff`](https://docs.rs/jiff), and the `tz` feature
  was removed: `TZ=`-prefixed cron schedules are always supported now. Remove `features = ["tz"]`
  from your `Cargo.toml`, otherwise Cargo rejects the dependency. Timezone data no longer ships
  inside the binary; it's read from the operating system at runtime, see
  [Cron schedules with timezone](#cron-schedules-with-timezone).
- [`TaskScheduler::cancel`](https://docs.rs/sacs/latest/sacs/scheduler/trait.TaskScheduler.html#tymethod.cancel)
  now returns `Err(Error::IncorrectTaskId)` for an unknown or already-finished task id, matching
  its documented contract, instead of always returning `Ok(())`.
- A few new `Error` variants (`NoUpcomingEvent`, `ScheduleOverflow`, `InvalidWorkerConfig`,
  `SchedulerTerminated`) are now returned from `add()`/`cancel()`/`status()`/`statistics()` in
  cases that previously panicked or produced a misleading error.


## License

This project is licensed under the [MIT license](LICENSE).
