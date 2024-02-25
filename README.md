# **SACS** - Simple Asynchronous Cron Scheduler

`SACS` is easy to use, lightweight scheduler and executor of repeatable async tasks for `Tokio` runtime.

## Features

- Runs tasks with different types of schedule: once, with delay, by interval, with cron schedule.
- Uses current `Tokio` runtime or creates new one with specified type, number of threads and limited parallelism.
- Allows task cancellation and getting current state of task.
- Lightweight, small, easy to use.

## Quick start

Just create `Scheduler` and add `Task` to it.

Refer to the crate's documentation for more examples and details of possible usage.

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

## License

This project is licensed under the [MIT license](LICENSE).
