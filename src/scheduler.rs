//! [`Scheduler`] is the most essential structure of `SACS`, its heart.
//! This is an entry point to schedule tasks and control on tasks and on whole jobs runtime.
//!
//! All other module's content works for [`Scheduler`].
use crate::{
    event::Event,
    executor::{Executor, JobExecutor},
    job::{Job, JobId, JobState},
    queue::{EventTimeQueue, Queue},
    task::{Task, TaskId, TaskStatus},
    ControlChannel, Error, Result,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    select,
    sync::{mpsc::Sender, RwLock},
    task::yield_now,
};
use tracing::{debug, instrument, warn};

/// Default maximin number of jobs to run on the single worker
pub(crate) const DEFAULT_MAX_PARALLEL_JOBS: usize = 16;
const SCHEDULER_CONTROL_CHANNEL_SIZE: usize = 1024;

#[cfg(feature = "async-trait")]
#[allow(async_fn_in_trait)]
pub trait TaskScheduler {
    /// Posts new [`Task`] to the `Scheduler`.
    async fn add(&self, task: Task) -> Result<TaskId>;
    /// Cancels existing [`Task`] with respect to [`CancelOpts`].
    async fn cancel(&self, id: TaskId, opts: CancelOpts) -> Result<()>;
    /// Returns current status of the [`Task`] and removes the task from the scheduler if it's finished.
    async fn status(&self, id: &TaskId) -> Result<TaskStatus>;
    /// Shuts down the scheduler with respect to [`ShutdownOpts`].
    async fn shutdown(self, opts: ShutdownOpts) -> Result<()>;
}

/// Base [`Scheduler`] behavior
#[cfg(not(feature = "async-trait"))]
pub trait TaskScheduler {
    /// Posts new [`Task`] to the `Scheduler`.
    fn add(&self, task: Task) -> impl std::future::Future<Output = Result<TaskId>> + Send;
    /// Cancels existing [`Task`] with respect to [`CancelOpts`].
    fn cancel(
        &self,
        id: TaskId,
        opts: CancelOpts,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    /// Returns current status of the [`Task`] and removes the task from the scheduler if it's finished.
    fn status(&self, id: &TaskId) -> impl std::future::Future<Output = Result<TaskStatus>> + Send;
    /// Shuts down the scheduler with respect to [`ShutdownOpts`].
    fn shutdown(self, opts: ShutdownOpts) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// The main work horse of `SACS`. Provides everything needed to run and control [`Tasks`](Task).
///
/// ## Overview
///
/// [`Scheduler`] runs as independent `Tokio` task (so you don't need to pool it using await/select/join), and it's responsible for:
/// - provisioning new and removing existing tasks on request
/// - starting jobs according to the task's schedule
/// - collecting and providing task's status
/// - clean scheduler's state from orphaned task's data
/// - shutdown
///
/// [`Scheduler`] uses some kind of its own "executor" engine under the hood to start jobs on `Tokio` runtime with respect
/// to provided constraints: runtime type, number of threads and parallelism (maximum number of simultaneously running jobs).
///
/// New scheduler can be created using convenient [SchedulerBuilder] or using [`Scheduler::default()`] method or using a long version
/// of the trivial constructor [`Scheduler::new()`]
///
/// Each [`Scheduler`] has at least three configuration parameters:
/// - [`WorkerType`] - type of `Tokio` runtime to use for workload: `CurrentRuntime`, `CurrentThread` or `MultiThread`
/// - [`WorkerParallelism`] - limits number of simultaneously running jobs, or makes it unlimited
/// - [`GarbageCollector`] - provide way to clean statuses of orphaned tasks to avoid uncontrolled memory consumption
///
/// You can use reasonable default parameters for trivial schedulers or provide yours own (via builder or constructor)
/// to tune behavior according to your needs. You can run as many schedulers as you need with different configurations.
///
/// ## Examples
///
/// The simplest config using default constructor:
/// - use current `Tokio` runtime
/// - limit workers to 16 jobs
/// - without garbage collector
/// ```rust
/// use sacs::{Result, scheduler::{Scheduler, ShutdownOpts, TaskScheduler}};
///
/// #[tokio::main]
/// async fn default_scheduler() -> Result<()> {
///     let scheduler = Scheduler::default();
///     // ...
///     scheduler.shutdown(ShutdownOpts::IgnoreRunning).await
/// }
/// ```
///
/// Use separate `MultiThread` `Tokio` runtime, collect finished tasks immediately:
/// ```rust
/// use sacs::{
///     scheduler::{RuntimeThreads, Scheduler, SchedulerBuilder, GarbageCollector,
///                 ShutdownOpts, TaskScheduler, WorkerType},
///     Result,
/// };
///
/// #[tokio::main]
/// async fn multi_thread_scheduler() -> Result<()> {
///     let scheduler = SchedulerBuilder::new()
///         .worker_type(WorkerType::MultiThread(RuntimeThreads::CpuCores))
///         .garbage_collector(GarbageCollector::Immediate)
///         .build();
///     // ...
///     scheduler.shutdown(ShutdownOpts::WaitForFinish).await
/// }
/// ```
///
/// Use separate `MultiThread` `Tokio` runtime with:
/// - four threads
/// - unlimited number of jobs
/// - garbage collector with 12-hour expiration time, run it every 15 minutes
/// ```
/// use sacs::{
///     scheduler::{GarbageCollector, RuntimeThreads, Scheduler, SchedulerBuilder,
///                 ShutdownOpts, TaskScheduler, WorkerParallelism, WorkerType},
///     Result,
/// };
/// use std::time::Duration;
///
/// #[tokio::main]
/// async fn specific_scheduler() -> Result<()> {
///     let scheduler = SchedulerBuilder::new()
///         .worker_type(WorkerType::MultiThread(RuntimeThreads::Limited(4)))
///         .parallelism(WorkerParallelism::Unlimited)
///         .garbage_collector(GarbageCollector::periodic(
///             Duration::from_secs(12 * 60 * 60), // expire after
///             Duration::from_secs(15 * 60),      // interval
///         ))
///         .build();
///     // ...
///     scheduler
///         .shutdown(ShutdownOpts::WaitFor(Duration::from_secs(60)))
///         .await
/// }
/// ```
///
pub struct Scheduler {
    tasks: Arc<RwLock<HashMap<TaskId, Task>>>,
    channel: Sender<ChangeStateEvent>,
    handler: tokio::task::JoinHandle<Result<()>>,
}

#[derive(Debug)]
enum ChangeStateEvent {
    Shutdown(ShutdownOpts),
    EnqueueTask(Task),
    DropTask(TaskId, CancelOpts),
}

/// Type of `Tokio` runtime to use for jobs worker.
#[derive(Debug, Default)]
pub enum WorkerType {
    /// Use current runtime instead of creating new one.
    ///
    /// This is the simplest and lightest worker because it uses runtime of the calling context.
    /// This is a default type.
    #[default]
    CurrentRuntime,
    /// Creates new thread and runs new `Tokio` runtime of `CurrentThread` type. Single thread worker.
    CurrentThread,
    /// Creates new thread and runs new `Tokio` runtime of `MultiThread` type.
    ///
    /// This is a multi-thread worker.
    /// The number of threads to use can be specified via parameter.
    /// Default is `Tokio` default - number of CPU cores.
    MultiThread(RuntimeThreads),
}

/// Thread number limit for [`Scheduler`] with [`MultiThread`](WorkerType::MultiThread) `Tokio` runtime
#[derive(Debug, Default)]
pub enum RuntimeThreads {
    /// Limits number of threads to number of actual CPU Cores.
    #[default]
    CpuCores,
    /// Sets limit to specified number.
    Limited(usize),
}

/// Limit of simultaneously running jobs per [`Scheduler`].
#[derive(Debug)]
pub enum WorkerParallelism {
    /// No limits, use the whole potential of your machine.
    Unlimited,
    /// Run no more simultaneous jobs than specified (default with 16 jobs).
    Limited(usize),
}

impl Default for WorkerParallelism {
    fn default() -> Self {
        Self::Limited(DEFAULT_MAX_PARALLEL_JOBS)
    }
}

/// Define [`Task`] cancellation behavior.
#[derive(Debug, Default, Clone)]
pub enum CancelOpts {
    /// Orphans task and lets it continue working (default).
    #[default]
    Ignore,
    /// Cancel it.
    Kill,
}

/// Define how to shut down [`Scheduler`] with running tasks.
#[derive(Debug, Default, Clone)]
pub enum ShutdownOpts {
    /// Lets running tasks continue working.
    IgnoreRunning,
    /// Cancel tasks with respect to [`CancelOpts`].
    CancelTasks(CancelOpts),
    /// Wait until all tasks finish (default).
    #[default]
    WaitForFinish,
    /// Wait until all tasks finish but no more than specified time. Returns [`Error::IncompleteShutdown`] in case of timeout.
    WaitFor(Duration),
}

/// Define parameters of orphaned task's garbage collector.
#[derive(Debug, Default, PartialEq)]
pub enum GarbageCollector {
    /// Don't collect garbage (default).
    #[default]
    Disabled,
    /// Don't preserve finished tasks status at all (clean status right after finishing).
    Immediate,
    /// Run garbage collector every `interval`
    /// time and cleanup tasks which have been finished more than `expire_after` time ago.
    Periodic {
        expire_after: Duration,
        interval: Duration,
    },
}

impl GarbageCollector {
    /// Helper constructor to disable garbage collector.
    pub fn disabled() -> Self {
        Self::Disabled
    }

    /// Helper constructor to crete immediate garbage collector.
    pub fn immediate() -> Self {
        Self::Immediate
    }

    /// Helper constructor to create garbage collector config.
    pub fn periodic(expire_after: Duration, interval: Duration) -> Self {
        Self::Periodic {
            expire_after,
            interval,
        }
    }
}

impl GarbageCollector {
    #[instrument("collect garbage", skip_all)]
    async fn collect_garbage(tasks: Arc<RwLock<HashMap<TaskId, Task>>>, expire_after: Duration) {
        let mut tasks = tasks.write().await;
        let expired_at = SystemTime::now().checked_sub(expire_after).unwrap();

        let to_remove: Vec<TaskId> = tasks
            .iter()
            .filter(|(_id, task)| task.state.finished())
            .filter(|(_id, task)| {
                task.state
                    .last_finished_at()
                    .expect("finished task has no `finished_at` time set, looks like a BUG")
                    <= expired_at
            })
            .map(|(id, _task)| id.clone())
            .collect();

        to_remove.iter().for_each(|id| {
            debug!(task_id = %id, "remove expired task");
            tasks.remove(id);
        });
    }
}

/// Convenient way to create customized [`Scheduler`].
///
/// It can be used instead of [`Scheduler::new()`] to create Scheduler with
/// expected parameters.
///
/// ## Examples
///
/// ```rust
/// use sacs::{
///     scheduler::{GarbageCollector, RuntimeThreads, Scheduler, SchedulerBuilder,
///                 ShutdownOpts, TaskScheduler, WorkerParallelism, WorkerType},
///     Result,
/// };
/// use std::time::Duration;
///
/// #[tokio::main]
/// async fn specific_scheduler() -> Result<()> {
///     let scheduler = SchedulerBuilder::new()
///         .worker_type(WorkerType::MultiThread(RuntimeThreads::Limited(4)))
///         .parallelism(WorkerParallelism::Unlimited)
///         .garbage_collector(GarbageCollector::Periodic {
///             expire_after: Duration::from_secs(12 * 60 * 60), // 12 hours
///             interval: Duration::from_secs(15 * 60),  // 15 minutes
///         })
///         .build();
///     // ...
///     scheduler
///         .shutdown(ShutdownOpts::WaitFor(Duration::from_secs(60)))
///         .await
/// }
///```
#[derive(Debug, Default)]
pub struct SchedulerBuilder {
    worker_type: WorkerType,
    parallelism: WorkerParallelism,
    garbage_collector: GarbageCollector,
}

impl SchedulerBuilder {
    /// Returns builder instance.
    pub fn new() -> Self {
        Self {
            worker_type: WorkerType::default(),
            parallelism: WorkerParallelism::default(),
            garbage_collector: GarbageCollector::default(),
        }
    }

    /// Set type of worker's runtime using [`WorkerType`].
    pub fn worker_type(self, worker_type: WorkerType) -> Self {
        Self {
            worker_type,
            ..self
        }
    }

    /// Set worker's parallelism using [`WorkerParallelism`].
    pub fn parallelism(self, parallelism: WorkerParallelism) -> Self {
        Self {
            parallelism,
            ..self
        }
    }

    /// Define parameters of garbage collector using [`GarbageCollector`].
    pub fn garbage_collector(self, garbage_collector: GarbageCollector) -> Self {
        Self {
            garbage_collector,
            ..self
        }
    }

    /// Build [`Scheduler`] instance.
    pub fn build(self) -> Scheduler {
        Scheduler::new(self.worker_type, self.parallelism, self.garbage_collector)
    }
}

impl Scheduler {
    /// Basic [`Scheduler`] constructor. Using of [`SchedulerBuilder`] is another way to construct it with custom parameters.
    pub fn new(
        worker_type: WorkerType,
        parallelism: WorkerParallelism,
        garbage_collector: GarbageCollector,
    ) -> Self {
        debug!(
            ?worker_type,
            ?parallelism,
            ?garbage_collector,
            "construct new scheduler"
        );

        let channel = ControlChannel::<ChangeStateEvent>::new(SCHEDULER_CONTROL_CHANNEL_SIZE);
        let tasks = Arc::new(RwLock::new(HashMap::new()));

        Self {
            tasks: tasks.clone(),
            channel: channel.sender(),
            handler: tokio::task::spawn(Scheduler::work(
                worker_type,
                parallelism,
                tasks.clone(),
                channel,
                garbage_collector,
            )),
        }
    }

    #[instrument("scheduler loop", skip_all)]
    async fn work(
        worker_type: WorkerType,
        parallelism: WorkerParallelism,
        tasks: Arc<RwLock<HashMap<TaskId, Task>>>,
        channel: ControlChannel<ChangeStateEvent>,
        garbage_collector: GarbageCollector,
    ) -> Result<()> {
        let queue = Queue::default();
        let executor = Executor::new(worker_type, parallelism);
        let mut jobs: HashMap<JobId, TaskId> = HashMap::new();

        match garbage_collector {
            GarbageCollector::Disabled => {}
            GarbageCollector::Immediate => {}
            GarbageCollector::Periodic {
                expire_after,
                interval,
            } => {
                // Prepare GC task
                let tasks = tasks.clone();
                let task = Task::new(
                    crate::task::TaskSchedule::IntervalDelayed(interval, interval),
                    move |id| {
                        let tasks = tasks.clone();
                        Box::pin(async move {
                            debug!(job_id = %id, "collecting garbage");
                            GarbageCollector::collect_garbage(tasks, expire_after).await;
                        })
                    },
                );
                // Enqueue GC task, it will be processed in the first run of events loop
                debug!(task = ?task, "schedule GC task");
                channel
                    .send(ChangeStateEvent::EnqueueTask(task))
                    .await
                    .map_err(|_e| Error::SendingChangeStateEvent)?;
            }
        }

        loop {
            debug!("scheduler loop iteration");
            select! {
                biased;
                event = channel.receive() => {
                    if let Some(event) = event {
                        debug!(event = ?event, "control event received");
                        match event {
                            ChangeStateEvent::Shutdown(opts) => {
                                queue.shutdown().await;
                                executor.shutdown(opts).await?;
                                tasks.write().await.clear();
                                return Ok(())
                            },
                            ChangeStateEvent::EnqueueTask(mut task) => {
                                let event_id = task.id.clone();
                                let task_id = task.id.clone();
                                let at = task.schedule.initial_run_time();
                                queue.insert(Event::new(event_id, at)).await?;
                                task.state.enqueued();
                                let mut tasks = tasks.write().await;
                                tasks.insert(task_id, task);
                            },
                            ChangeStateEvent::DropTask(id, opts) => {
                                let mut tasks = tasks.write().await;
                                let task = tasks.get(&id);
                                if let Some(task) = task {
                                    let event_id = id.clone().into();
                                    queue.pop(&event_id).await?;
                                    match opts {
                                        CancelOpts::Ignore => {},
                                        CancelOpts::Kill => {
                                            for job in task.state.jobs() {
                                                executor.cancel(&job).await?;
                                            }
                                        },
                                    }
                                    tasks.remove(&id);
                                }
                            }
                        }
                    } else {
                        warn!("empty events channel");
                    }
                },
                event = queue.next() => {
                    if let Ok(event) = event {
                        debug!(event= ?event, "queue event received");
                        let mut tasks = tasks.write().await;
                        let task = tasks.get_mut(&event.id.into());

                        if let Some(task) = task {
                            let job_id = JobId::new(task.id());
                            let job = task.job.clone();
                            let job = Job::new(job_id, job, task.timeout);
                            let job_id = executor.enqueue(job).await?;
                            jobs.insert(job_id.clone(), task.id.clone());
                            task.state.scheduled(job_id);
                            let at = task.schedule.after_start_run_time();
                            if let Some(at) = at {
                                let event_id = task.id.clone();
                                queue.insert(Event::new(event_id, at)).await?;
                                task.state.enqueued();
                            }
                        }
                        } else {
                            warn!(event = ?event, "error from queue received, exiting");
                            return Err(event.err().unwrap())
                        }
                    },
                job_id = executor.work() => {
                    if let Ok(job_id) = job_id {
                        debug!(job_id = %job_id, "executor event received");
                        let mut tasks = tasks.write().await;
                        let job_state = executor.state(&job_id).await?;
                        let task_id = jobs.get(&job_id);
                        if let Some(task_id) = task_id {
                            let is_finished = {
                                let task = tasks.get_mut(task_id);
                                if let Some(task) = task {
                                    debug!(job_state = ?job_state, "job state changed");
                                    let mut at = None;
                                    match job_state {
                                        JobState::Running => { task.state.started(job_id); },
                                        JobState::Completed => {
                                            task.state.completed(&job_id);
                                            at = task.schedule.after_finish_run_time();
                                        },
                                        JobState::Cancelled => {
                                            task.state.cancelled(&job_id);
                                            at = task.schedule.after_finish_run_time();
                                        },
                                        JobState::Timeout => {
                                            task.state.timeout(&job_id);
                                            at = task.schedule.after_finish_run_time();
                                        },
                                        JobState::Pending | JobState::Starting => {},
                                    };
                                    if let Some(at) = at {
                                        let event_id = task.id.clone();
                                        queue.insert(Event::new(event_id, at)).await?;
                                        task.state.enqueued();
                                    }
                                    task.state.finished()
                                } else {
                                    false
                                }
                            };
                            // Remove the task from state if GC is Immediate
                            if is_finished && garbage_collector == GarbageCollector::Immediate {
                                debug!(task_id = %task_id, "remove finished task");
                                tasks.remove(task_id);
                            }
                        }
                    } else {
                        warn!(job_id = ?job_id, "error from executor received");
                    }
                }
            }
        }
    }

    async fn send_event(&self, event: ChangeStateEvent) -> Result<()> {
        self.channel
            .send(event)
            .await
            .map_err(|_e| Error::SendingChangeStateEvent)
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new(
            WorkerType::default(),
            WorkerParallelism::default(),
            GarbageCollector::default(),
        )
    }
}

impl TaskScheduler for Scheduler {
    /// Post new [`Task`] to scheduler.
    ///
    /// Right after that task will be staring to execute according to its schedule.
    ///
    /// Returns [`TaskId`] of the scheduled task or [`Error::DuplicatedTaskId`] if the task with the same `TaskId`
    /// is already present, even if it's finished but not removed by getting its status or by garbage collector.
    async fn add(&self, task: Task) -> Result<TaskId> {
        debug!(task_id = %task.id(), "add task");
        let id = task.id();
        yield_now().await; // to avoid scheduling several tasks within a single async scheduler cycle
        if self.tasks.read().await.get(&id).is_some() {
            Err(Error::DuplicatedTaskId(id))
        } else {
            self.send_event(ChangeStateEvent::EnqueueTask(task)).await?;
            Ok(id)
        }
    }

    /// Removes [`Task`] with specified [`TaskId`] from the scheduler with respect to [`CancelOpts`]:
    /// the task can be killed or left to continue working up to finish.
    ///
    /// Returns [`Error::IncorrectTaskId`] if the task is not scheduled or cleaned by garbage collector.
    async fn cancel(&self, id: TaskId, opts: CancelOpts) -> Result<()> {
        debug!(task_id = %id, ?opts, "cancel task");
        self.send_event(ChangeStateEvent::DropTask(id, opts)).await
    }

    /// Returns current [`status`](TaskStatus) of the task.
    ///
    /// If the task is finished, then its status will be removed from the scheduler after
    /// this method call, so the following calls with the same `TaskId` will fail with [`Error::IncorrectTaskId`].
    async fn status(&self, id: &TaskId) -> Result<TaskStatus> {
        debug!(task_id = %id, "task status requested");
        let mut tasks = self.tasks.write().await;
        let task = tasks.get_mut(id);

        if let Some(task) = task {
            let status = task.state.status();
            if task.state.finished() {
                debug!(task_id = %id, "remove finished task");
                tasks.remove(id);
            }
            return Ok(status);
        }

        Err(Error::IncorrectTaskId(id.clone()))
    }

    /// Starts process of scheduler shutdown:
    /// - remove awaiting tasks from the queue
    /// - shuts down executing engine with respect to [`ShutdownOpts`].
    ///
    /// Can return [`Error::IncompleteShutdown`] in case of errors.
    async fn shutdown(self, opts: ShutdownOpts) -> Result<()> {
        debug!(?opts, "shutdown requested");
        self.send_event(ChangeStateEvent::Shutdown(opts.clone()))
            .await?;

        match opts {
            ShutdownOpts::IgnoreRunning => Ok(()),
            ShutdownOpts::CancelTasks(_) | ShutdownOpts::WaitForFinish => {
                futures::join!(self.handler)
                    .0
                    .map_err(|_e| Error::IncompleteShutdown)?
            }
            ShutdownOpts::WaitFor(timeout) => {
                debug!(?timeout, "wait for task completion");
                select! {
                    res = self.handler => {
                        res.map_err(|_e| Error::IncompleteShutdown)?
                    },
                    _ = tokio::time::sleep(timeout) => {
                        Err(Error::IncompleteShutdown)
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::task::{CronOpts, TaskSchedule};
    use std::time::UNIX_EPOCH;

    async fn basic_test_suite(
        scheduler: Scheduler,
        schedules: Vec<TaskSchedule>,
        durations: &[Duration],
        suite_duration: Duration,
    ) -> Result<(Vec<String>, Vec<String>)> {
        assert_eq!(
            schedules.len(),
            durations.len(),
            "schedulers and durations arrays size mismatched"
        );

        let logs = Arc::new(RwLock::new(Vec::<String>::new()));
        let jobs = Arc::new(RwLock::new(Vec::<JobId>::new()));

        for s in 0..schedules.len() {
            let log = logs.clone();
            let jobs = jobs.clone();
            let task_duration = durations[s];
            let task = Task::new(schedules[s].clone(), move |id| {
                let log = log.clone();
                let jobs = jobs.clone();
                Box::pin(async move {
                    jobs.write().await.push(id.clone());
                    log.write().await.push(format!("{},start,{id}", s));
                    tokio::time::sleep(task_duration).await;
                    log.write().await.push(format!("{},finish,{id}", s));
                })
            });
            scheduler.add(task).await?;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        tokio::time::sleep(suite_duration).await;
        scheduler.shutdown(ShutdownOpts::WaitForFinish).await?;

        let logs: Vec<String> = logs.read().await.iter().map(String::from).collect();
        let jobs: Vec<String> = jobs.read().await.iter().map(|s| format!("{s}")).collect();

        Ok((logs, jobs))
    }

    #[tokio::test]
    async fn once_1_worker() {
        let schedules: Vec<TaskSchedule> =
            Vec::from([TaskSchedule::Once, TaskSchedule::Once, TaskSchedule::Once]);
        let durations = [
            Duration::from_secs(3),
            Duration::from_secs(3),
            Duration::from_secs(1),
        ];
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Limited(1),
            GarbageCollector::default(),
        );

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(5))
                .await
                .unwrap();

        assert_eq!(logs.len(), 4);
        assert_eq!(jobs.len(), 2);

        let mut expected = vec![];
        for j in jobs.iter().enumerate() {
            expected.push(format!("{},start,{}", j.0, j.1));
            expected.push(format!("{},finish,{}", j.0, j.1));
        }
        assert_eq!(logs, expected);
    }

    #[tokio::test]
    async fn once_2_workers() {
        let schedules: Vec<TaskSchedule> = Vec::from([
            TaskSchedule::Once,
            TaskSchedule::Once,
            TaskSchedule::Once,
            TaskSchedule::Once,
            TaskSchedule::Once,
        ]);
        let durations = [
            Duration::from_millis(2950),
            Duration::from_millis(3000),
            Duration::from_millis(2950),
            Duration::from_millis(3000),
            Duration::from_millis(1),
        ];
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Limited(2),
            GarbageCollector::default(),
        );

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(5))
                .await
                .unwrap();

        assert_eq!(logs.len(), 8);
        assert_eq!(jobs.len(), 4);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("1,start,{}", jobs[1]),
            format!("0,finish,{}", jobs[0]),
            format!("2,start,{}", jobs[2]),
            format!("1,finish,{}", jobs[1]),
            format!("3,start,{}", jobs[3]),
            format!("2,finish,{}", jobs[2]),
            format!("3,finish,{}", jobs[3]),
        ]);
        assert_eq!(logs, expected);
    }

    #[tokio::test]
    async fn once_unlimited_workers() {
        let schedules: Vec<TaskSchedule> = Vec::from([
            TaskSchedule::Once,
            TaskSchedule::Once,
            TaskSchedule::Once,
            TaskSchedule::Once,
            TaskSchedule::Once,
        ]);
        let durations = [
            Duration::from_millis(1000),
            Duration::from_millis(1200),
            Duration::from_millis(1300),
            Duration::from_millis(1400),
            Duration::from_millis(2000),
        ];
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Unlimited,
            GarbageCollector::default(),
        );

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(1))
                .await
                .unwrap();

        assert_eq!(logs.len(), 10);
        assert_eq!(jobs.len(), 5);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("1,start,{}", jobs[1]),
            format!("2,start,{}", jobs[2]),
            format!("3,start,{}", jobs[3]),
            format!("4,start,{}", jobs[4]),
            format!("0,finish,{}", jobs[0]),
            format!("1,finish,{}", jobs[1]),
            format!("2,finish,{}", jobs[2]),
            format!("3,finish,{}", jobs[3]),
            format!("4,finish,{}", jobs[4]),
        ]);
        assert_eq!(logs, expected);
    }

    #[tokio::test]
    async fn cron() {
        let schedules: Vec<TaskSchedule> = Vec::from([
            TaskSchedule::Cron("*/2 * * * * *".try_into().unwrap(), CronOpts::default()),
            TaskSchedule::Cron("*/5 * * * * *".try_into().unwrap(), CronOpts::default()),
        ]);
        let durations = [Duration::from_millis(1200), Duration::from_millis(3500)];
        let scheduler = SchedulerBuilder::new()
            .parallelism(WorkerParallelism::Limited(2))
            .build();

        // wait for the next 10-seconds interval
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let wait_for = 10 - now % 10 - 1;
        tokio::time::sleep(Duration::from_secs(wait_for)).await;

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(7))
                .await
                .unwrap();

        assert_eq!(logs.len(), 12);
        assert_eq!(jobs.len(), 6);

        let expected1: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),  // 0
            format!("1,start,{}", jobs[1]),  // 0
            format!("0,finish,{}", jobs[0]), // 1200
            format!("0,start,{}", jobs[2]),  // 2000
            format!("0,finish,{}", jobs[2]), // 3200
            format!("1,finish,{}", jobs[1]), // 3500
            format!("0,start,{}", jobs[3]),  // 4000
            format!("1,start,{}", jobs[4]),  // 5000
            format!("0,finish,{}", jobs[3]), // 5200
            format!("0,start,{}", jobs[5]),  // 6000
            format!("0,finish,{}", jobs[5]), // 7200
            format!("1,finish,{}", jobs[4]), // 8500
        ]);
        let expected2: Vec<String> = Vec::from([
            format!("1,start,{}", jobs[0]),  // 0
            format!("0,start,{}", jobs[1]),  // 0
            format!("0,finish,{}", jobs[1]), // 1200
            format!("0,start,{}", jobs[2]),  // 2000
            format!("0,finish,{}", jobs[2]), // 3200
            format!("1,finish,{}", jobs[0]), // 3500
            format!("0,start,{}", jobs[3]),  // 4000
            format!("1,start,{}", jobs[4]),  // 5000
            format!("0,finish,{}", jobs[3]), // 5200
            format!("0,start,{}", jobs[5]),  // 6000
            format!("0,finish,{}", jobs[5]), // 7200
            format!("1,finish,{}", jobs[4]), // 8500
        ]);
        let debug = format!(
            "jobs={:?}\nlogs={:?}\nexpected1={:?}\nexpected2={:?}",
            jobs, logs, expected1, expected2
        );
        assert!((logs == expected1) || (logs == expected2), "{debug}");
    }

    #[tokio::test]
    async fn cron_at_start() {
        let schedules: Vec<TaskSchedule> = Vec::from([TaskSchedule::Cron(
            "*/5 * * * * *".try_into().unwrap(),
            CronOpts {
                at_start: true,
                concurrent: false,
            },
        )]);
        let durations = [Duration::from_millis(1000)];
        let scheduler = SchedulerBuilder::new()
            .parallelism(WorkerParallelism::Unlimited)
            .build();

        // wait for the next 5-seconds interval
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let wait_for = 5 - now % 5 + 1;
        tokio::time::sleep(Duration::from_secs(wait_for)).await;

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(6))
                .await
                .unwrap();

        assert_eq!(logs.len(), 4);
        assert_eq!(jobs.len(), 2);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("0,finish,{}", jobs[0]),
            format!("0,start,{}", jobs[1]),
            format!("0,finish,{}", jobs[1]),
        ]);
        let debug = format!("jobs={:?}\nlogs={:?}\nexpected={:?}", jobs, logs, expected);
        assert_eq!(logs, expected, "{debug}");
    }

    #[tokio::test]
    async fn cron_non_concurrent() {
        let schedules: Vec<TaskSchedule> = Vec::from([TaskSchedule::Cron(
            "*/5 * * * * *".try_into().unwrap(),
            CronOpts {
                at_start: true,
                concurrent: false,
            },
        )]);
        let durations = [Duration::from_millis(7000)];
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Unlimited,
            GarbageCollector::default(),
        );

        // wait for the next 5-seconds interval
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let wait_for = 5 - now % 5 + 1;
        tokio::time::sleep(Duration::from_secs(wait_for)).await;

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(6))
                .await
                .unwrap();

        assert_eq!(logs.len(), 2);
        assert_eq!(jobs.len(), 1);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("0,finish,{}", jobs[0]),
        ]);
        let debug = format!("jobs={:?}\nlogs={:?}\nexpected={:?}", jobs, logs, expected);
        assert_eq!(logs, expected, "{debug}");
    }

    #[tokio::test]
    async fn cron_concurrent() {
        tracing_subscriber::fmt::init();
        let schedules: Vec<TaskSchedule> = Vec::from([TaskSchedule::Cron(
            "*/3 * * * * *".try_into().unwrap(),
            CronOpts {
                at_start: true,
                concurrent: true,
            },
        )]);
        let durations = [Duration::from_millis(3900)];
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Unlimited,
            GarbageCollector::default(),
        );

        // wait for the next 3-seconds interval
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let wait_for = 3 - now % 3 + 1;
        tokio::time::sleep(Duration::from_secs(wait_for)).await;

        let (logs, jobs) = basic_test_suite(
            scheduler,
            schedules,
            &durations,
            Duration::from_millis(5000),
        )
        .await
        .unwrap();

        assert_eq!(logs.len(), 6);
        assert_eq!(jobs.len(), 3);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("0,start,{}", jobs[1]),
            format!("0,finish,{}", jobs[0]),
            format!("0,start,{}", jobs[2]),
            format!("0,finish,{}", jobs[1]),
            format!("0,finish,{}", jobs[2]),
        ]);
        let debug = format!("jobs={:?}\nlogs={:?}\nexpected={:?}", jobs, logs, expected);
        assert_eq!(logs, expected, "{debug}");
    }

    #[tokio::test]
    async fn once_delayed_4_workers() {
        let schedules: Vec<TaskSchedule> = Vec::from([
            TaskSchedule::Once,
            TaskSchedule::OnceDelayed(Duration::from_millis(500)),
            TaskSchedule::OnceDelayed(Duration::from_secs(1)),
            TaskSchedule::OnceDelayed(Duration::from_millis(3300)),
        ]);
        let durations = [
            Duration::from_secs(3),
            Duration::from_secs(3),
            Duration::from_secs(1),
            Duration::from_secs(1),
        ];
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Limited(4),
            GarbageCollector::default(),
        );

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(4))
                .await
                .unwrap();

        assert_eq!(logs.len(), 8);
        assert_eq!(jobs.len(), 4);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("1,start,{}", jobs[1]),
            format!("2,start,{}", jobs[2]),
            format!("2,finish,{}", jobs[2]),
            format!("0,finish,{}", jobs[0]),
            format!("3,start,{}", jobs[3]),
            format!("1,finish,{}", jobs[1]),
            format!("3,finish,{}", jobs[3]),
        ]);
        assert_eq!(logs, expected);
    }

    #[tokio::test]
    async fn interval_4_workers() {
        let schedules: Vec<TaskSchedule> = Vec::from([
            TaskSchedule::Interval(Duration::from_secs(3)),
            TaskSchedule::Interval(Duration::from_secs(1)),
            TaskSchedule::IntervalDelayed(Duration::from_millis(2100), Duration::from_millis(2100)),
            TaskSchedule::IntervalDelayed(Duration::from_millis(5900), Duration::from_millis(5900)),
        ]);
        let durations = [
            Duration::from_millis(900),
            Duration::from_secs(2),
            Duration::from_secs(5),
            Duration::from_secs(1),
        ];
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Limited(4),
            GarbageCollector::default(),
        );

        let (logs, jobs) = basic_test_suite(
            scheduler,
            schedules,
            &durations,
            Duration::from_millis(7200),
        )
        .await
        .unwrap();

        assert_eq!(logs.len(), 14);
        assert_eq!(jobs.len(), 7);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("1,start,{}", jobs[1]),
            format!("0,finish,{}", jobs[0]),
            format!("1,finish,{}", jobs[1]),
            format!("2,start,{}", jobs[2]),
            format!("1,start,{}", jobs[3]),
            format!("0,start,{}", jobs[4]),
            format!("0,finish,{}", jobs[4]),
            format!("1,finish,{}", jobs[3]),
            format!("3,start,{}", jobs[5]),
            format!("1,start,{}", jobs[6]),
            format!("3,finish,{}", jobs[5]),
            format!("2,finish,{}", jobs[2]),
            format!("1,finish,{}", jobs[6]),
        ]);
        assert_eq!(logs, expected);
    }

    #[tokio::test]
    async fn garbage_collector_periodic() {
        let scheduler = SchedulerBuilder::new()
            .garbage_collector(GarbageCollector::periodic(
                Duration::from_millis(2500),
                Duration::from_millis(500),
            ))
            .build();

        // Every 100ms work for 2s
        let task_1 = Task::new(TaskSchedule::Interval(Duration::from_millis(100)), |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
            })
        });
        // Once work for 4s
        let task_2 = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(4)).await;
            })
        });
        // Once work for 8s
        let task_3 = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(8)).await;
            })
        });

        let id_1 = scheduler.add(task_1).await.unwrap();
        let id_2 = scheduler.add(task_2).await.unwrap();
        let id_3 = scheduler.add(task_3).await.unwrap();

        tokio::time::sleep(Duration::from_millis(3500)).await;
        assert_eq!(scheduler.status(&id_1).await.unwrap(), TaskStatus::Running);
        assert_eq!(scheduler.status(&id_2).await.unwrap(), TaskStatus::Running);
        assert_eq!(scheduler.status(&id_3).await.unwrap(), TaskStatus::Running);

        tokio::time::sleep(Duration::from_millis(4400)).await;
        assert_eq!(scheduler.status(&id_1).await.unwrap(), TaskStatus::Running);
        assert!(scheduler.status(&id_2).await.is_err());
        assert_eq!(scheduler.status(&id_3).await.unwrap(), TaskStatus::Running);

        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert_eq!(scheduler.status(&id_1).await.unwrap(), TaskStatus::Running);
        assert!(scheduler.status(&id_2).await.is_err());
        assert_eq!(scheduler.status(&id_3).await.unwrap(), TaskStatus::Finished);

        scheduler
            .shutdown(ShutdownOpts::CancelTasks(CancelOpts::Kill))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn garbage_collector_immediate() {
        let scheduler = SchedulerBuilder::new()
            .garbage_collector(GarbageCollector::immediate())
            .build();

        // Every 100ms work for 2s
        let task_1 = Task::new(TaskSchedule::Interval(Duration::from_millis(100)), |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
            })
        });
        // Once work for 2s
        let task_2 = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
            })
        });
        // Once work for 3s
        let task_3 = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(3)).await;
            })
        });
        // Once work for 4s with 3s timeout
        let task_4 = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(4)).await;
            })
        })
        .with_timeout(Duration::from_secs(3));

        let id_1 = scheduler.add(task_1).await.unwrap();
        let id_2 = scheduler.add(task_2).await.unwrap();
        let id_3 = scheduler.add(task_3).await.unwrap();
        let id_4 = scheduler.add(task_4).await.unwrap();

        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert_eq!(scheduler.status(&id_1).await.unwrap(), TaskStatus::Running);
        assert!(scheduler.status(&id_2).await.is_err());
        assert_eq!(scheduler.status(&id_3).await.unwrap(), TaskStatus::Running);
        assert_eq!(scheduler.status(&id_4).await.unwrap(), TaskStatus::Running);

        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert_eq!(scheduler.status(&id_1).await.unwrap(), TaskStatus::Running);
        assert!(scheduler.status(&id_2).await.is_err());
        assert!(scheduler.status(&id_3).await.is_err());
        assert!(scheduler.status(&id_4).await.is_err());

        scheduler
            .shutdown(ShutdownOpts::CancelTasks(CancelOpts::Kill))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn reject_duplicated_task() {
        let scheduler = SchedulerBuilder::new()
            .worker_type(WorkerType::CurrentThread)
            .garbage_collector(GarbageCollector::disabled())
            .build();

        // Every 100ms work for 2s
        let task_1 = Task::new(TaskSchedule::Interval(Duration::from_millis(100)), |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
            })
        })
        .with_id("TASK_ID");

        // Clone with duplicated TaskId
        let task_2 = task_1.clone();

        let _id1 = scheduler.add(task_1).await.unwrap();
        let id2 = scheduler.add(task_2).await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(id2.is_err());
        let err = id2.err().unwrap();
        match err {
            Error::DuplicatedTaskId(id) => {
                assert_eq!(id, TaskId::from("TASK_ID"))
            }
            _ => unreachable!("Incorrect error type or TaskId"),
        }

        scheduler
            .shutdown(ShutdownOpts::IgnoreRunning)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_with_timeout() {
        let scheduler = Scheduler::default();

        // Every 100ms work for 2s
        let task = Task::new(TaskSchedule::Interval(Duration::from_millis(100)), |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
            })
        });
        scheduler.add(task).await.unwrap();
        yield_now().await;

        scheduler
            .shutdown(ShutdownOpts::WaitFor(Duration::from_secs(3)))
            .await
            .unwrap();

        let scheduler = Scheduler::default();

        // Long task
        let task = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
            })
        });
        scheduler.add(task).await.unwrap();
        yield_now().await;

        match scheduler
            .shutdown(ShutdownOpts::WaitFor(Duration::from_secs(1)))
            .await
        {
            Ok(_) => unreachable!("unexpected Ok result"),
            Err(e) => match e {
                Error::IncompleteShutdown => {}
                _ => unreachable!("failed with unexpected error"),
            },
        }
    }

    #[tokio::test]
    async fn task_with_timeout() {
        let scheduler = Scheduler::default();

        // Once work for 2s, with 5s timeout
        let task1 = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
            })
        })
        .with_id("OK")
        .with_timeout(Duration::from_secs(5));

        // Once work for 2s, with 1s timeout
        let task2 = task1
            .clone()
            .with_id("FAILED")
            .with_timeout(Duration::from_secs(1));

        // Every 1s, with 2s duration and 1s timeout
        let task3 = Task::new(TaskSchedule::Interval(Duration::from_secs(1)), |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
            })
        })
        .with_id("FAILING")
        .with_timeout(Duration::from_secs(1));

        let id1 = scheduler.add(task1).await.unwrap();
        let id2 = scheduler.add(task2).await.unwrap();
        let id3 = scheduler.add(task3).await.unwrap();

        tokio::time::sleep(Duration::from_millis(800)).await;
        assert_eq!(scheduler.status(&id1).await.unwrap(), TaskStatus::Running);
        assert_eq!(scheduler.status(&id2).await.unwrap(), TaskStatus::Running);
        assert_eq!(scheduler.status(&id3).await.unwrap(), TaskStatus::Running);

        tokio::time::sleep(Duration::from_millis(800)).await;
        assert_eq!(scheduler.status(&id1).await.unwrap(), TaskStatus::Running);
        assert_eq!(scheduler.status(&id2).await.unwrap(), TaskStatus::Finished);
        assert_eq!(scheduler.status(&id3).await.unwrap(), TaskStatus::Waiting);

        tokio::time::sleep(Duration::from_millis(800)).await;
        assert_eq!(scheduler.status(&id1).await.unwrap(), TaskStatus::Finished);
        assert!(scheduler.status(&id2).await.is_err());
        assert_eq!(scheduler.status(&id3).await.unwrap(), TaskStatus::Running);

        tokio::time::sleep(Duration::from_millis(800)).await;
        assert!(scheduler.status(&id1).await.is_err());
        assert!(scheduler.status(&id2).await.is_err());
        assert_eq!(scheduler.status(&id3).await.unwrap(), TaskStatus::Waiting);

        let _ = scheduler
            .shutdown(ShutdownOpts::CancelTasks(CancelOpts::Kill))
            .await;
    }
}
