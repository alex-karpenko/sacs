//! [`Scheduler`] is the most essential structure of `SACS`, its heart.
//! This is an entry point to schedule tasks and control on tasks and on whole jobs runtime.
//!
//! All other module's content works for [`Scheduler`].
use crate::{
    event::Event,
    executor::{Executor, JobExecutor},
    job::{Job, JobId, JobState},
    queue::{EventTimeQueue, Queue},
    task::{Task, TaskId, TaskStatistics, TaskStatus},
    ControlChannel, Error, Result,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use tokio::{
    select,
    sync::{mpsc::Sender, oneshot, RwLock},
};
use tracing::{debug, error, instrument, warn};

/// Default maximin number of jobs to run on the single worker
pub(crate) const DEFAULT_MAX_PARALLEL_JOBS: usize = 16;
const SCHEDULER_CONTROL_CHANNEL_SIZE: usize = 1024;

#[cfg(feature = "async-trait")]
#[allow(async_fn_in_trait)]
/// Base [`Scheduler`] behavior
pub trait TaskScheduler {
    /// Posts new [`Task`] to the `Scheduler`.
    async fn add(&self, task: Task) -> Result<TaskId>;
    /// Cancels existing [`Task`] with respect to [`CancelOpts`].
    async fn cancel(&self, id: TaskId, opts: CancelOpts) -> Result<()>;
    /// Returns current status of the [`Task`] and removes the task from the scheduler if it's finished.
    async fn status(&self, id: &TaskId) -> Result<TaskStatus>;
    /// Returns various statistics about the [`Task`] state. Please refer to the [`TaskStatistics`] for details.
    async fn statistics(&self, id: &TaskId) -> Result<TaskStatistics>;
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
    /// Returns various statistics about the [`Task`] state. Please refer to the [`TaskStatistics`] for details.
    fn statistics(
        &self,
        id: &TaskId,
    ) -> impl std::future::Future<Output = Result<TaskStatistics>> + Send;
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
#[derive(Debug)]
pub struct Scheduler {
    tasks: Arc<RwLock<HashMap<TaskId, Task>>>,
    channel: Sender<ChangeStateEvent>,
    handler: tokio::task::JoinHandle<Result<()>>,
    // Only read back via `jobs_index_size()`, which is `#[cfg(test)]`-only.
    #[allow(dead_code)]
    jobs_index_size: Arc<AtomicUsize>,
}

enum ChangeStateEvent {
    Shutdown(ShutdownOpts),
    EnqueueTask(Task, SystemTime, oneshot::Sender<Result<TaskId>>),
    DropTask(TaskId, CancelOpts, oneshot::Sender<Result<()>>),
}

// `oneshot::Sender` isn't `Debug`, so this is implemented manually to keep tracing usable.
impl std::fmt::Debug for ChangeStateEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeStateEvent::Shutdown(opts) => f.debug_tuple("Shutdown").field(opts).finish(),
            ChangeStateEvent::EnqueueTask(task, at, _) => f
                .debug_tuple("EnqueueTask")
                .field(&task.id())
                .field(at)
                .finish(),
            ChangeStateEvent::DropTask(id, opts, _) => {
                f.debug_tuple("DropTask").field(id).field(opts).finish()
            }
        }
    }
}

/// Type of `Tokio` runtime to use for jobs worker.
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuntimeThreads {
    /// Limits number of threads to number of actual CPU Cores.
    #[default]
    CpuCores,
    /// Sets limit to specified number.
    Limited(usize),
}

/// Limit of simultaneously running jobs per [`Scheduler`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CancelOpts {
    /// Orphans task and lets it continue working (default).
    #[default]
    Ignore,
    /// Cancel it.
    Kill,
}

/// Define how to shut down [`Scheduler`] with running tasks.
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ShutdownOpts {
    /// Lets running tasks continue working without blocking the caller of
    /// [`shutdown`](TaskScheduler::shutdown).
    ///
    /// # Note
    /// The worker thread backing running jobs is detached, not joined, so jobs are still
    /// lost if the whole process exits before they finish. If your process may exit shortly
    /// after shutdown, use [`ShutdownOpts::WaitForFinish`] or [`ShutdownOpts::WaitFor`]
    /// instead.
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
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GarbageCollector {
    /// Don't collect garbage (default).
    #[default]
    Disabled,
    /// Don't preserve finished tasks status at all (clean status right after finishing).
    Immediate,
    /// Run garbage collector every `interval`
    /// time and cleanup tasks which have been finished more than `expire_after` time ago.
    Periodic {
        /// Maximum Time-to-live of the finished tasks' state.
        expire_after: Duration,
        /// Interval between the garbage collector runs.
        interval: Duration,
    },
}

impl GarbageCollector {
    /// Helper constructor to disable garbage collector.
    pub fn disabled() -> Self {
        Self::Disabled
    }

    /// Helper constructor to create immediate garbage collector.
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
    #[instrument("collect garbage", skip_all, level = "debug")]
    async fn collect_garbage(tasks: Arc<RwLock<HashMap<TaskId, Task>>>, expire_after: Duration) {
        let mut tasks = tasks.write().await;
        // An `expire_after` that reaches before the representable `SystemTime` range (an
        // absurdly large TTL, freely constructible via the public builder) means "nothing
        // can be expired yet" rather than a panic.
        let expired_at = SystemTime::now()
            .checked_sub(expire_after)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let to_remove: Vec<TaskId> = tasks
            .iter()
            .filter(|(_id, task)| task.state.is_task_finished())
            .filter_map(|(id, task)| match task.state.last_finished_at() {
                Some(last_finished_at) if last_finished_at <= expired_at => Some(id.clone()),
                Some(_) => None,
                None => {
                    // Every terminal `TaskState` transition sets `last_finished_at`, so this
                    // should be unreachable; treat it as a recoverable state-machine defect
                    // (skip the task, keep the collector running) rather than a panic.
                    warn!(task_id = %id, "finished task has no `last_finished_at` set, skipping");
                    None
                }
            })
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
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    ///
    /// # Panics
    /// Panics if the requested [`WorkerType`] is invalid (e.g. zero worker threads) or if the
    /// underlying `Tokio` runtime can't be constructed. Use [`SchedulerBuilder::try_build`]
    /// for a fallible alternative.
    pub fn build(self) -> Scheduler {
        Scheduler::new(self.worker_type, self.parallelism, self.garbage_collector)
    }

    /// Fallible counterpart of [`SchedulerBuilder::build`]. Returns
    /// [`Error::InvalidWorkerConfig`] instead of panicking when the requested worker
    /// configuration is invalid or the underlying `Tokio` runtime can't be constructed.
    pub fn try_build(self) -> Result<Scheduler> {
        Scheduler::try_new(self.worker_type, self.parallelism, self.garbage_collector)
    }
}

impl Scheduler {
    /// Basic [`Scheduler`] constructor. Using of [`SchedulerBuilder`] is another way to construct it with custom parameters.
    ///
    /// # Panics
    /// Panics if the requested [`WorkerType`] is invalid (e.g. zero worker threads) or if the
    /// underlying `Tokio` runtime can't be constructed (e.g. due to OS resource limits). Use
    /// [`Scheduler::try_new`] for a fallible alternative.
    pub fn new(
        worker_type: WorkerType,
        parallelism: WorkerParallelism,
        garbage_collector: GarbageCollector,
    ) -> Self {
        Self::try_new(worker_type, parallelism, garbage_collector)
            .expect("invalid worker configuration or unable to construct Tokio runtime")
    }

    /// Fallible counterpart of [`Scheduler::new`]. Returns [`Error::InvalidWorkerConfig`]
    /// instead of panicking when the requested worker configuration is invalid or the
    /// underlying `Tokio` runtime can't be constructed.
    pub fn try_new(
        worker_type: WorkerType,
        parallelism: WorkerParallelism,
        garbage_collector: GarbageCollector,
    ) -> Result<Self> {
        debug!(
            ?worker_type,
            ?parallelism,
            ?garbage_collector,
            "construct new scheduler"
        );

        let executor = Executor::try_new(worker_type, parallelism)?;
        let channel = ControlChannel::<ChangeStateEvent>::new(SCHEDULER_CONTROL_CHANNEL_SIZE);
        let tasks = Arc::new(RwLock::new(HashMap::new()));
        let jobs_index_size = Arc::new(AtomicUsize::new(0));

        Ok(Self {
            tasks: tasks.clone(),
            channel: channel.sender(),
            jobs_index_size: jobs_index_size.clone(),
            handler: tokio::task::spawn(Scheduler::work(
                executor,
                tasks.clone(),
                channel,
                garbage_collector,
                jobs_index_size,
            )),
        })
    }

    /// Returns `true` if the scheduler's background loop is still running.
    ///
    /// A `false` result means the loop has terminated (normally, from a panic, or from an
    /// unrecoverable internal error) and every subsequent [`add`](TaskScheduler::add),
    /// [`cancel`](TaskScheduler::cancel), [`status`](TaskScheduler::status) and
    /// [`statistics`](TaskScheduler::statistics) call will fail with
    /// [`Error::SchedulerTerminated`] instead of silently serving stale state.
    pub fn is_alive(&self) -> bool {
        !self.handler.is_finished()
    }

    #[cfg(test)]
    pub(crate) fn jobs_index_size(&self) -> usize {
        self.jobs_index_size.load(Ordering::Relaxed)
    }

    #[instrument("scheduler loop", skip_all, level = "debug")]
    async fn work(
        executor: Executor,
        tasks: Arc<RwLock<HashMap<TaskId, Task>>>,
        channel: ControlChannel<ChangeStateEvent>,
        garbage_collector: GarbageCollector,
        jobs_index_size: Arc<AtomicUsize>,
    ) -> Result<()> {
        let queue = Queue::default();
        let mut jobs: HashMap<JobId, TaskId> = HashMap::new();

        match garbage_collector {
            GarbageCollector::Disabled => {}
            GarbageCollector::Immediate => {}
            GarbageCollector::Periodic {
                expire_after,
                interval,
            } => {
                // Prepare GC task
                let gc_tasks = tasks.clone();
                let mut task = Task::new(
                    crate::task::TaskSchedule::IntervalDelayed(interval, interval),
                    move |id| {
                        let gc_tasks = gc_tasks.clone();
                        Box::pin(async move {
                            debug!(job_id = %id, "collecting garbage");
                            GarbageCollector::collect_garbage(gc_tasks, expire_after).await;
                        })
                    },
                );
                // Install the GC task directly into the queue/tasks map instead of going
                // through the usual oneshot-based admission (as `add()` does): the control
                // loop below hasn't started yet, so nothing would be there to reply to the
                // oneshot, and awaiting one here would just hang forever.
                match task.schedule.initial_run_time() {
                    Ok(at) => {
                        debug!(task_id = %task.id(), "schedule GC task");
                        let event_id = task.id.clone();
                        let task_id = task.id.clone();
                        queue.insert(Event::new(event_id, at)).await?;
                        task.state.task_enqueued();
                        tasks.write().await.insert(task_id, task);
                    }
                    Err(e) => {
                        error!(
                            error = %e,
                            ?interval,
                            "garbage collector schedule is invalid, garbage collector is disabled"
                        );
                    }
                }
            }
        }

        loop {
            debug!("scheduler loop iteration");
            // Deliberately not `biased`: a sustained burst on any one of these three
            // sources (e.g. many concurrent `add()` calls, or many ready queue events)
            // must not be able to starve the other two forever. Fair (pseudo-random)
            // selection guarantees every ready branch eventually gets polled, which matters
            // here because processing one branch can itself depend on the other two making
            // progress (e.g. dispatching a job depends on the executor eventually reporting
            // capacity back).
            select! {
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
                            ChangeStateEvent::EnqueueTask(mut task, at, reply) => {
                                // Atomic admission: the duplicate-id check and the insert
                                // happen under a single `tasks` write lock, processed solely
                                // by this loop, so two concurrent `add()` calls with the same
                                // explicit `TaskId` can no longer both observe an absent
                                // entry and both succeed.
                                let task_id = task.id.clone();
                                let mut tasks_guard = tasks.write().await;
                                if tasks_guard.contains_key(&task_id) {
                                    let _ = reply.send(Err(Error::DuplicatedTaskId(task_id)));
                                } else {
                                    let event_id = task_id.clone();
                                    match queue.insert(Event::new(event_id, at)).await {
                                        Ok(()) => {
                                            task.state.task_enqueued();
                                            tasks_guard.insert(task_id.clone(), task);
                                            let _ = reply.send(Ok(task_id));
                                        }
                                        Err(e) => {
                                            let _ = reply.send(Err(e));
                                        }
                                    }
                                }
                            },
                            ChangeStateEvent::DropTask(id, opts, reply) => {
                                let mut tasks_guard = tasks.write().await;
                                let jobs_to_cancel = tasks_guard.get(&id).map(|task| task.state.jobs());
                                if let Some(jobs_to_cancel) = jobs_to_cancel {
                                    let event_id = id.clone().into();
                                    let result: Result<()> = async {
                                        queue.pop(&event_id).await?;
                                        if let CancelOpts::Kill = opts {
                                            for job in jobs_to_cancel {
                                                executor.cancel(&job).await?;
                                                // The pending-job cancel path (see
                                                // `Executor::cancel`) transitions state and
                                                // requeues directly, without emitting an
                                                // executor event -- so the executor-event
                                                // branch's `jobs.remove()` cleanup below
                                                // will never see it. The owning task is
                                                // being torn down regardless (it's about to
                                                // be removed from `tasks`), so clean up the
                                                // scheduler's own job index eagerly here
                                                // instead of leaking this entry for the life
                                                // of the scheduler. Idempotent: if this job
                                                // was actually dispatched and its
                                                // `JobCanceled` event arrives later, that
                                                // cleanup path's own
                                                // `jobs.remove(..).is_some()` guard just
                                                // finds nothing to do.
                                                if jobs.remove(&job).is_some() {
                                                    jobs_index_size
                                                        .fetch_sub(1, Ordering::Relaxed);
                                                }
                                            }
                                        }
                                        Ok(())
                                    }.await;
                                    match result {
                                        Ok(()) => {
                                            tasks_guard.remove(&id);
                                            let _ = reply.send(Ok(()));
                                        }
                                        Err(e) => {
                                            let _ = reply.send(Err(e));
                                        }
                                    }
                                } else {
                                    let _ = reply.send(Err(Error::IncorrectTaskId(id)));
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
                            if jobs.insert(job_id.clone(), task.id.clone()).is_none() {
                                jobs_index_size.fetch_add(1, Ordering::Relaxed);
                            }
                            task.state.job_scheduled(job_id);
                            let at = task.schedule.after_start_run_time();
                            if let Some(at) = at {
                                let event_id = task.id.clone();
                                queue.insert(Event::new(event_id, at)).await?;
                                task.state.task_enqueued();
                            }
                        }
                        } else {
                            warn!(event = ?event, "error from queue received, exiting");
                            return Err(event.err().unwrap())
                        }
                    },
                event = executor.next_event() => {
                    match event {
                        Ok(event) => {
                            let job_id = match executor.apply(event).await {
                                Ok(job_id) => job_id,
                                Err(e) => {
                                    warn!(error = %e, "error applying executor event");
                                    continue;
                                }
                            };
                            debug!(job_id = %job_id, "executor event received");
                            // `executor.state()` can legitimately miss an id if the job's
                            // terminal state was already consumed by a previous iteration;
                            // that's a lost-race, not a scheduler-ending bug, so it's logged
                            // and skipped instead of propagated with `?` as before.
                            let job_state = match executor.state(&job_id).await {
                                Ok(state) => state,
                                Err(e) => {
                                    warn!(job_id = %job_id, error = %e, "unknown job id reported by executor, ignoring");
                                    continue;
                                }
                            };
                            let mut tasks = tasks.write().await;
                            let task_id = jobs.get(&job_id);
                            if let Some(task_id) = task_id {
                                let is_finished = {
                                    let task = tasks.get_mut(task_id);
                                    if let Some(task) = task {
                                        debug!(job_state = ?job_state, "job state changed");
                                        let mut at = None;
                                        match job_state {
                                            JobState::Running => { task.state.job_started(job_id.clone()); },
                                            JobState::Completed => {
                                                task.state.job_completed(&job_id);
                                                at = task.schedule.after_finish_run_time();
                                            },
                                            JobState::Canceled => {
                                                task.state.job_canceled(&job_id);
                                                at = task.schedule.after_finish_run_time();
                                            },
                                            JobState::Timeout => {
                                                task.state.job_timeout(&job_id);
                                                at = task.schedule.after_finish_run_time();
                                            },
                                            JobState::Error => {
                                                task.state.job_error(&job_id);
                                                at = task.schedule.after_finish_run_time();
                                            },
                                            JobState::Pending | JobState::Starting => {},
                                        };
                                        if let Some(at) = at {
                                            let event_id = task.id.clone();
                                            queue.insert(Event::new(event_id, at)).await?;
                                            task.state.task_enqueued();
                                        }
                                        task.state.is_task_finished()
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
                            // Regardless of whether the owning task is still around, drop
                            // this job's entry from the scheduler's own `jobs` index once
                            // it reaches a terminal state -- otherwise this map grows
                            // without bound for the life of the scheduler, unseen by any
                            // `GarbageCollector` setting (which only ever prunes `tasks`).
                            if job_state.finished() && jobs.remove(&job_id).is_some() {
                                jobs_index_size.fetch_sub(1, Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "error from executor received");
                        }
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
    /// Returns [`TaskId`] of the scheduled task, [`Error::DuplicatedTaskId`] if the task with
    /// the same `TaskId` is already present (even if it's finished but not removed by getting
    /// its status or by garbage collector), [`Error::ScheduleOverflow`] if the task's schedule
    /// requires time arithmetic that overflows the representable `SystemTime` range,
    /// [`Error::NoUpcomingEvent`] if a cron schedule has no future occurrence at all (e.g. it
    /// points to an impossible calendar date), or [`Error::SchedulerTerminated`] if the
    /// scheduler's background loop has already terminated.
    ///
    /// Admission is atomic with respect to duplicate ids: this method only returns once the
    /// scheduler's background loop has itself checked for and rejected duplicates, so two
    /// concurrent `add()` calls with the same explicit `TaskId` can never both succeed.
    async fn add(&self, task: Task) -> Result<TaskId> {
        debug!(task_id = %task.id(), "add task");
        if !self.is_alive() {
            return Err(Error::SchedulerTerminated);
        }
        let at = task.schedule.initial_run_time()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send_event(ChangeStateEvent::EnqueueTask(task, at, reply_tx))
            .await
            .map_err(|_e| Error::SchedulerTerminated)?;
        reply_rx.await.map_err(|_e| Error::SchedulerTerminated)?
    }

    /// Removes [`Task`] with specified [`TaskId`] from the scheduler with respect to [`CancelOpts`]:
    /// the task can be killed or left to continue working up to finish.
    ///
    /// This method only returns once the scheduler's background loop has itself processed the
    /// cancellation, so a subsequent `add()` of the same `TaskId`, or a subsequent `status()`
    /// call, is guaranteed to observe the task as already gone.
    ///
    /// Returns [`Error::IncorrectTaskId`] if the task is not scheduled or cleaned by garbage
    /// collector, or [`Error::SchedulerTerminated`] if the scheduler's background loop has
    /// already terminated.
    async fn cancel(&self, id: TaskId, opts: CancelOpts) -> Result<()> {
        debug!(task_id = %id, ?opts, "cancel task");
        if !self.is_alive() {
            return Err(Error::SchedulerTerminated);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        self.send_event(ChangeStateEvent::DropTask(id, opts, reply_tx))
            .await
            .map_err(|_e| Error::SchedulerTerminated)?;
        reply_rx.await.map_err(|_e| Error::SchedulerTerminated)?
    }

    /// Returns current [`status`](TaskStatus) of the task.
    ///
    /// # Warning
    /// If the task is finished, then its status will be removed from the scheduler after
    /// this method call, so the following calls with the same `TaskId` will fail with [`Error::IncorrectTaskId`].
    /// This is deliberate (not idempotent by design): if you also need [`statistics`](TaskScheduler::statistics)
    /// for the same finished task, query it *before* calling `status`, since `status` may
    /// already have removed the task's state.
    ///
    /// Returns [`Error::SchedulerTerminated`] instead of stale data if the scheduler's
    /// background loop has already terminated.
    async fn status(&self, id: &TaskId) -> Result<TaskStatus> {
        debug!(task_id = %id, "task status requested");
        if !self.is_alive() {
            return Err(Error::SchedulerTerminated);
        }
        let mut tasks = self.tasks.write().await;
        let task = tasks.get_mut(id);

        if let Some(task) = task {
            let status = task.state.status();
            if task.state.is_task_finished() {
                debug!(task_id = %id, "remove finished task");
                tasks.remove(id);
            }
            return Ok(status);
        }

        Err(Error::IncorrectTaskId(id.clone()))
    }

    /// Returns various statistics about scheduled the task.
    ///
    /// Please refer to the [`TaskStatistics`] documentation for details.
    /// Returns [`Error::IncorrectTaskId`] if the task is not scheduled or cleaned by garbage
    /// collector, or [`Error::SchedulerTerminated`] instead of stale data if the scheduler's
    /// background loop has already terminated.
    async fn statistics(&self, id: &TaskId) -> Result<TaskStatistics> {
        debug!(task_id = %id, "task statistics requested");
        if !self.is_alive() {
            return Err(Error::SchedulerTerminated);
        }
        let tasks = self.tasks.read().await;
        let task = tasks.get(id);

        if let Some(task) = task {
            Ok(task.statistics())
        } else {
            Err(Error::IncorrectTaskId(id.clone()))
        }
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
    use ntest::timeout;

    use super::*;
    use crate::task::{CronOpts, TaskSchedule, TaskState};
    use std::time::UNIX_EPOCH;
    use tokio::task::yield_now;

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
                    log.write().await.push(format!("{s},start,{id}"));
                    tokio::time::sleep(task_duration).await;
                    log.write().await.push(format!("{s},finish,{id}"));
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
    #[timeout(10000)]
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
    #[timeout(10000)]
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
    #[timeout(10000)]
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
    #[timeout(20000)]
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
            "jobs={jobs:?}\nlogs={logs:?}\nexpected1={expected1:?}\nexpected2={expected2:?}"
        );
        assert!((logs == expected1) || (logs == expected2), "{debug}");
    }

    #[tokio::test]
    #[timeout(20000)]
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
        let debug = format!("jobs={jobs:?}\nlogs={logs:?}\nexpected={expected:?}");
        assert_eq!(logs, expected, "{debug}");
    }

    #[tokio::test]
    #[timeout(20000)]
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
        let debug = format!("jobs={jobs:?}\nlogs={logs:?}\nexpected={expected:?}");
        assert_eq!(logs, expected, "{debug}");
    }

    #[tokio::test]
    #[timeout(20000)]
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
        let debug = format!("jobs={jobs:?}\nlogs={logs:?}\nexpected={expected:?}");
        assert_eq!(logs, expected, "{debug}");
    }

    #[tokio::test]
    #[timeout(10000)]
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
    #[timeout(10000)]
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
    #[timeout(10000)]
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
    #[timeout(5000)]
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
    #[timeout(5000)]
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
    #[timeout(5000)]
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
    #[timeout(5000)]
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

    #[tokio::test]
    #[timeout(5000)]
    async fn task_cancellation() {
        let scheduler = Scheduler::default();

        let task1 = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
            })
        })
        .with_id("OK");

        let task2 = task1.clone().with_id("CANCELED KILLED");
        let task3 = task1.clone().with_id("CANCELED IGNORED");

        let id1 = scheduler.add(task1).await.unwrap();
        let id2 = scheduler.add(task2).await.unwrap();
        let id3 = scheduler.add(task3).await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        assert_eq!(
            scheduler.statistics(&id1).await.unwrap(),
            TaskStatistics {
                running: 1,
                ..Default::default()
            }
        );
        assert_eq!(
            scheduler.statistics(&id2).await.unwrap(),
            TaskStatistics {
                running: 1,
                ..Default::default()
            }
        );
        assert_eq!(
            scheduler.statistics(&id3).await.unwrap(),
            TaskStatistics {
                running: 1,
                ..Default::default()
            }
        );
        assert_eq!(scheduler.status(&id1).await.unwrap(), TaskStatus::Running);
        assert_eq!(scheduler.status(&id2).await.unwrap(), TaskStatus::Running);
        assert_eq!(scheduler.status(&id3).await.unwrap(), TaskStatus::Running);

        scheduler
            .cancel(id2.clone(), CancelOpts::Kill)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            scheduler.statistics(&id1).await.unwrap(),
            TaskStatistics {
                running: 1,
                ..Default::default()
            }
        );
        assert!(scheduler.statistics(&id2).await.is_err());
        assert_eq!(
            scheduler.statistics(&id3).await.unwrap(),
            TaskStatistics {
                running: 1,
                ..Default::default()
            }
        );
        assert_eq!(scheduler.status(&id1).await.unwrap(), TaskStatus::Running);
        assert!(scheduler.status(&id2).await.is_err());
        assert_eq!(scheduler.status(&id3).await.unwrap(), TaskStatus::Running);

        scheduler
            .cancel(id3.clone(), CancelOpts::Ignore)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            scheduler.statistics(&id1).await.unwrap(),
            TaskStatistics {
                running: 1,
                ..Default::default()
            }
        );
        assert!(scheduler.statistics(&id3).await.is_err());

        assert_eq!(scheduler.status(&id1).await.unwrap(), TaskStatus::Running);
        assert!(scheduler.status(&id3).await.is_err());

        tokio::time::sleep(Duration::from_millis(400)).await;

        assert_eq!(
            scheduler.statistics(&id1).await.unwrap(),
            TaskStatistics {
                completed: 1,
                ..Default::default()
            }
        );

        assert_eq!(scheduler.status(&id1).await.unwrap(), TaskStatus::Finished);

        assert!(scheduler.status(&id1).await.is_err());

        let _ = scheduler
            .shutdown(ShutdownOpts::CancelTasks(CancelOpts::Kill))
            .await;
    }

    #[test]
    fn default_garbage_collector_value_should_be_disabled() {
        assert_eq!(GarbageCollector::disabled(), GarbageCollector::default());
        assert_eq!(GarbageCollector::default(), GarbageCollector::Disabled);
    }

    #[test]
    fn scheduler_builder_should_provide_default_values() {
        let builder = SchedulerBuilder::new();
        assert_eq!(builder.worker_type, WorkerType::CurrentRuntime);
        assert_eq!(builder.garbage_collector, GarbageCollector::Disabled);
        assert_eq!(builder.parallelism, WorkerParallelism::Limited(16));
    }

    #[test]
    fn scheduler_builder_should_provide_custom_worker_type() {
        let builder = SchedulerBuilder::new();
        assert_eq!(builder.worker_type, WorkerType::CurrentRuntime);
        assert_eq!(
            builder
                .clone()
                .worker_type(WorkerType::CurrentThread)
                .worker_type,
            WorkerType::CurrentThread
        );
        assert_eq!(
            builder
                .clone()
                .worker_type(WorkerType::MultiThread(RuntimeThreads::CpuCores))
                .worker_type,
            WorkerType::MultiThread(RuntimeThreads::CpuCores)
        );
        assert_eq!(
            builder
                .worker_type(WorkerType::MultiThread(RuntimeThreads::Limited(8)))
                .worker_type,
            WorkerType::MultiThread(RuntimeThreads::Limited(8))
        );
    }

    #[test]
    fn scheduler_builder_should_provide_custom_garbage_collector() {
        let builder = SchedulerBuilder::new();
        assert_eq!(builder.garbage_collector, GarbageCollector::Disabled);
        assert_eq!(
            builder
                .clone()
                .garbage_collector(GarbageCollector::Immediate)
                .garbage_collector,
            GarbageCollector::Immediate
        );
        assert_eq!(
            builder
                .clone()
                .garbage_collector(GarbageCollector::immediate())
                .garbage_collector,
            GarbageCollector::Immediate
        );

        assert_eq!(
            builder
                .clone()
                .garbage_collector(GarbageCollector::Periodic {
                    expire_after: Duration::from_secs(30),
                    interval: Duration::from_secs(10)
                })
                .garbage_collector,
            GarbageCollector::Periodic {
                expire_after: Duration::from_secs(30),
                interval: Duration::from_secs(10)
            }
        );
        assert_eq!(
            builder
                .clone()
                .garbage_collector(GarbageCollector::periodic(
                    Duration::from_secs(120),
                    Duration::from_millis(1000)
                ))
                .garbage_collector,
            GarbageCollector::Periodic {
                expire_after: Duration::from_secs(120),
                interval: Duration::from_millis(1000)
            }
        );
    }

    #[test]
    fn scheduler_builder_should_provide_custom_parallelism() {
        let builder = SchedulerBuilder::new();
        assert_eq!(builder.parallelism, WorkerParallelism::Limited(16));
        assert_eq!(
            builder
                .clone()
                .parallelism(WorkerParallelism::Limited(8))
                .parallelism,
            WorkerParallelism::Limited(8)
        );
        assert_eq!(
            builder
                .clone()
                .parallelism(WorkerParallelism::Unlimited)
                .parallelism,
            WorkerParallelism::Unlimited
        );
    }

    // N3: the scheduler's internal `jobs` index must not grow without bound -- it needs to
    // shrink back down as jobs finish, regardless of `GarbageCollector` setting (which only
    // ever prunes `tasks`, never this map). `jobs_index_size()` is `pub(crate)`, so this has
    // to be a unit test rather than living in `tests/regressions.rs`.
    #[tokio::test]
    #[timeout(15000)]
    async fn n3_job_index_does_not_grow() {
        let scheduler = SchedulerBuilder::new()
            .garbage_collector(GarbageCollector::disabled())
            .build();

        let task = Task::new(TaskSchedule::Interval(Duration::from_millis(100)), |_id| {
            Box::pin(async move {})
        });
        let id = scheduler.add(task).await.unwrap();

        // Let roughly 20+ executions happen.
        tokio::time::sleep(Duration::from_millis(2500)).await;

        // A leak would have grown this to ~20+ entries; it must stay bounded to (at most) the
        // handful of jobs that can legitimately be in flight at once.
        assert!(
            scheduler.jobs_index_size() <= 2,
            "jobs index leaked: {} entries",
            scheduler.jobs_index_size()
        );

        scheduler.cancel(id, CancelOpts::Kill).await.unwrap();
        let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
    }

    // N3 (related): a job cancelled while still `pending` in the executor (never dispatched
    // to a worker, e.g. under saturated `WorkerParallelism::Limited`) must not leak its entry
    // in the scheduler's `jobs` index either. `Executor::cancel`'s pending-job branch
    // transitions state and requeues directly instead of emitting an executor event (to avoid
    // a self-send deadlock against the very scheduler-loop task that would need to drain it),
    // so the scheduler's `DropTask` handler must clean up this index itself instead of relying
    // on the executor-event branch, which will never see it.
    #[tokio::test]
    #[timeout(10000)]
    async fn cancel_pending_job_does_not_leak_jobs_index() {
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Limited(1),
            GarbageCollector::default(),
        );

        // Occupies the only worker slot so the next job stays `pending` in the executor.
        let blocker = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
            })
        });
        scheduler.add(blocker).await.unwrap();

        let victim = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
        let victim_id = victim.id();
        scheduler.add(victim).await.unwrap();

        // Give the loop a moment to enqueue both jobs (insert into the `jobs` index) before
        // cancelling.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(scheduler.jobs_index_size(), 2);

        scheduler.cancel(victim_id, CancelOpts::Kill).await.unwrap();

        assert_eq!(
            scheduler.jobs_index_size(),
            1,
            "the pending-cancelled victim's job entry must not leak in the scheduler's jobs index"
        );

        let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
    }

    // O5: an oversized `expire_after` (trivially constructible via the public builder) must
    // not panic the garbage collector; it should just mean "nothing can be expired yet".
    // `GarbageCollector::collect_garbage` isn't `pub`, so this has to be a unit test.
    #[tokio::test]
    #[timeout(5000)]
    async fn o5_gc_huge_ttl_does_not_panic() {
        let tasks = Arc::new(RwLock::new(HashMap::new()));
        let mut task = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
        let job_id = JobId::new(task.id());
        task.state.task_enqueued();
        task.state.job_scheduled(job_id.clone());
        task.state.job_started(job_id.clone());
        task.state.job_completed(&job_id);
        assert!(task.state.is_task_finished());
        tasks.write().await.insert(task.id(), task);

        // `expire_after` reaches before `SystemTime::UNIX_EPOCH`: `checked_sub` would return
        // `None`, which the old code `.unwrap()`-ed straight into a panic.
        GarbageCollector::collect_garbage(tasks.clone(), Duration::MAX).await;

        // An oversized TTL means "nothing can be expired yet": the task must still be there.
        assert_eq!(tasks.read().await.len(), 1);
    }

    // N7: once the scheduler's background loop has terminated (here simulated the same way a
    // panic or an unrecoverable internal error would end it -- the loop task simply stops
    // running), every operation must report `Error::SchedulerTerminated` instead of hanging,
    // silently serving stale state, or reporting `SendingChangeStateEvent` (which names the
    // wrong cause). This needs the private `handler` field, so it's a unit test.
    #[tokio::test]
    #[timeout(5000)]
    async fn n7_operations_after_termination_report_scheduler_terminated() {
        let scheduler = Scheduler::default();
        assert!(scheduler.is_alive());

        scheduler.handler.abort();
        // Give the abort a moment to actually land.
        while !scheduler.handler.is_finished() {
            yield_now().await;
        }
        assert!(!scheduler.is_alive());

        let status = scheduler.status(&TaskId::from("anything")).await;
        assert!(
            matches!(status, Err(Error::SchedulerTerminated)),
            "{status:?}"
        );

        let stats = scheduler.statistics(&TaskId::from("anything")).await;
        assert!(
            matches!(stats, Err(Error::SchedulerTerminated)),
            "{stats:?}"
        );

        let task = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
        let add_result = scheduler.add(task).await;
        assert!(
            matches!(add_result, Err(Error::SchedulerTerminated)),
            "{add_result:?}"
        );

        let cancel_result = scheduler
            .cancel(TaskId::from("anything"), CancelOpts::Kill)
            .await;
        assert!(
            matches!(cancel_result, Err(Error::SchedulerTerminated)),
            "{cancel_result:?}"
        );
    }

    // O5: a finished task whose `last_finished_at` is unset (a state-machine defect that
    // shouldn't occur via normal transitions, but the old code `.expect()`-ed it into a
    // panic) must be skipped, not crash the collector, and must stay in `tasks`.
    #[tokio::test]
    #[timeout(5000)]
    async fn o5_gc_skips_task_without_finished_at() {
        let tasks = Arc::new(RwLock::new(HashMap::new()));
        let mut task = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
        task.state = TaskState::test_finished_without_last_finished_at();
        assert!(task.state.is_task_finished());
        assert_eq!(task.state.last_finished_at(), None);
        tasks.write().await.insert(task.id(), task);

        GarbageCollector::collect_garbage(tasks.clone(), Duration::from_millis(1)).await;

        assert_eq!(
            tasks.read().await.len(),
            1,
            "a finished task with no `last_finished_at` must be skipped, not removed"
        );
    }

    #[tokio::test]
    #[timeout(5000)]
    async fn o5_gc_still_collects_expired() {
        let tasks = Arc::new(RwLock::new(HashMap::new()));
        let mut task = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
        let job_id = JobId::new(task.id());
        task.state.task_enqueued();
        task.state.job_scheduled(job_id.clone());
        task.state.job_started(job_id.clone());
        task.state.job_completed(&job_id);
        tasks.write().await.insert(task.id(), task);

        tokio::time::sleep(Duration::from_millis(50)).await;
        GarbageCollector::collect_garbage(tasks.clone(), Duration::from_millis(1)).await;

        assert!(tasks.read().await.is_empty());
    }
}
