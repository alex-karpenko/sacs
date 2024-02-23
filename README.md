# **SACS** - Simple Asynchronous Cron Scheduler

`SACS` is easy to use, lightweight scheduler and executor of repeatable async tasks for Tokio runtime.

## Features

- Runs tasks with different types of schedule: once, with delay, by interval, with cron schedule.
- Uses current Tokio runtime or creates new one with specified type, number of threads and restricted parallelism.
- Allows task cancellation and getting current state of task.
- Lightweight, small, easy to use.

## Quick start

Just create `Scheduler` and add `Task`s to it.

```rust
use sacs::{
    scheduler::{Scheduler, TaskScheduler},
    task::{Task, TaskSchedule},
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
    let cron = TaskSchedule::RepeatByCron("*/3 * * * * *".try_into()?);
    let task = Task::new(cron, |id| {
        Box::pin(async move {
            info!("Job {id} started.");
            // Actual async workload here
            tokio::time::sleep(Duration::from_secs(2)).await;
            // ...
            info!("Job {id} finished.");
        })
    });

    // Post task to scheduler and forget it :)
    scheduler.add(task).await;

    // ... and do any other async work
    tokio::time::sleep(Duration::from_secs(10)).await;

    // It's not mandatory but good to shutdown scheduler.
    // Wait for completion of all running jobs
    scheduler.shutdown(true).await
}
```

Refer to [`Scheduler`](scheduler/struct.Scheduler.html) and [`Task`](task/struct.Task.html) documentation for more examples and details of possible variants of configuration.

## License

This project is licensed under the [MIT license](LICENSE).
