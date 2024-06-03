# **SACS** - Simple Asynchronous Cron Scheduler

`SACS` is easy to use, lightweight scheduler and executor of repeatable async tasks for `Tokio` runtime.

<p>
<a href="https://github.com/alex-karpenko/sacs/actions/workflows/ci.yaml" rel="nofollow"><img src="https://img.shields.io/github/actions/workflow/status/alex-karpenko/sacs/ci.yaml?label=ci" alt="CI status"></a>
<a href="https://github.com/alex-karpenko/sacs/actions/workflows/audit.yaml" rel="nofollow"><img src="https://img.shields.io/github/actions/workflow/status/alex-karpenko/sacs/audit.yaml?label=audit" alt="Audit status"></a>
<a href="https://github.com/alex-karpenko/sacs/actions/workflows/publish.yaml" rel="nofollow"><img src="https://img.shields.io/github/actions/workflow/status/alex-karpenko/sacs/publish.yaml?label=publish" alt="Crates.io publishing status"></a>
<a href="https://docs.rs/sacs" rel="nofollow"><img src="https://img.shields.io/docsrs/sacs" alt="docs.rs status"></a>
<a href="https://crates.io/crates/sacs" rel="nofollow"><img src="https://img.shields.io/crates/v/sacs" alt="Version at Crates.io"></a>
<a href="https://crates.io/crates/sacs" rel="nofollow"><img alt="Crates.io MSRV" src="https://img.shields.io/crates/msrv/sacs"></a>
<a href="https://github.com/alex-karpenko/sacs/blob/HEAD/LICENSE" rel="nofollow"><img src="https://img.shields.io/crates/l/sacs" alt="License"></a>
</p>

## Features

- Runs tasks with different types of schedule: once, with delay, by interval, with cron schedule.
- Uses current `Tokio` runtime or creates new one with specified type, number of threads and limited parallelism.
- Allows task cancellation and getting current state of task.
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

    // Create task with cron schedule: repeat it every 3 seconds
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

    // It's not mandatory but good to shutdown scheduler
    // Wait for completion of all running jobs
    scheduler.shutdown(ShutdownOpts::WaitForFinish).await
}
```

## TODO

- [x] Make [`TaskId`](https://docs.rs/sacs/latest/sacs/task/struct.TaskId.html) and
[`JobId`](https://docs.rs/sacs/latest/sacs/job/struct.JobId.html) more flexible and
convenient to create and refer tasks.
- [x] Tracing.
- [ ] Task with limited execution time.
- [ ] More examples.

## License

This project is licensed under the [MIT license](LICENSE).
