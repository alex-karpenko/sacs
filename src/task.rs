//! [`Task`] object represents a single job with schedule.
//! Use it to create workload of different types and post it to `Scheduler`.
//!
//! Module contains everything related to [`Task`], its [`TaskSchedule`] and [`state`](TaskStatus).
//!
use crate::{event::EventId, format_local_time, job::JobId, AsyncJobBoxed, Error};
use cron_lite::Schedule;
use futures::Future;
use jiff::ZonedDateTime;
use std::{
    collections::BTreeSet,
    fmt::Display,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use uuid::Uuid;

/// `Task` represents a single job with its schedule, attributes, and state.
#[derive(Clone)]
pub struct Task {
    pub(crate) id: TaskId,
    pub(crate) job: AsyncJobBoxed,
    pub(crate) schedule: TaskSchedule,
    pub(crate) state: TaskState,
    pub(crate) timeout: Option<Duration>,
}

impl Task {
    /// Creates new Task with specified schedule, job function and default [`TaskId`].
    ///
    /// # Note on the job factory
    ///
    /// The `job` factory is `FnMut`, stored behind a shared lock so it may keep and mutate
    /// its own state across runs. That lock is held only while *constructing* the future
    /// (i.e. while the factory closure itself runs), not while the returned future executes,
    /// so it never serializes job execution. It does, however, serialize future
    /// *construction*: with [`CronOpts::concurrent`] set to `true`, several runs of the same
    /// task execute concurrently, but each run's factory invocation still happens one at a
    /// time. If the factory panics, the worker catches it and reports the job as
    /// [`errored`](TaskStatus::Finished) instead of taking down the whole scheduler -- except
    /// under a `panic = "abort"` build profile, where any panic still aborts the process
    /// regardless.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use sacs::task::{CronOpts, Task, TaskSchedule};
    /// use std::time::Duration;
    ///
    /// let schedule = TaskSchedule::Cron("*/5 * * * * *".try_into().unwrap(), CronOpts::default());
    /// let task = Task::new(schedule, |id| {
    ///     Box::pin(async move {
    ///         // Actual async workload here
    ///         tokio::time::sleep(Duration::from_secs(1)).await;
    ///         // ...
    ///         println!("Job {id} finished.");
    ///         })
    ///     });
    /// ```
    pub fn new<T>(schedule: TaskSchedule, job: T) -> Self
    where
        T: 'static,
        T: FnMut(JobId) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
    {
        Self {
            id: TaskId::new(),
            job: Arc::new(RwLock::new(Box::new(job))),
            schedule,
            state: TaskState::default(),
            timeout: None,
        }
    }

    /// Set explicit [`TaskId`] to the existing [`Task`].
    ///
    /// This method is useful if you need to know [`TaskId`] before the task was scheduled.
    ///
    /// # Note:
    /// **_If you provide explicit [`TaskId`] value, your responsibility is to ensure the uniqueness of the [`TaskId`]
    /// within instance of `Scheduler`._**
    ///
    /// This method consumes `Task` instance and returns the same instance with specified `Id`.
    /// Since [`Task`] is cloneable, this method can be used to create several identical tasks with different `TaskId`s.
    ///
    /// # Examples:
    ///
    /// ```rust
    /// use sacs::task::{CronOpts, Task, TaskId, TaskSchedule};
    /// use std::time::Duration;
    /// use uuid::Uuid;
    ///
    /// let schedule = TaskSchedule::Cron("*/5 * * * * *".try_into().unwrap(), CronOpts::default());
    /// let task1_id = Uuid::new_v4();
    /// let task_id = task1_id.clone();
    ///
    /// let task1 = Task::new(schedule.clone(), move |id| {
    ///     let task_id = task_id.clone();
    ///     Box::pin(async move {
    ///         println!("TaskId={task_id}.");
    ///         // Actual async workload here
    ///         tokio::time::sleep(Duration::from_secs(1)).await;
    ///         // ...
    ///         println!("Job {id} finished.");
    ///         })
    ///     }).with_id(task1_id);
    ///
    /// let task2 = task1.clone().with_id(Uuid::new_v4());
    ///
    /// let task3 = Task::new(schedule, |id| {
    ///     Box::pin(async move {
    ///         // Actual async workload here
    ///         tokio::time::sleep(Duration::from_secs(1)).await;
    ///         // ...
    ///         println!("Job {id} finished.");
    ///         })
    ///     }).with_id("This is abstract unique task id");
    /// ```
    pub fn with_id(self, id: impl Into<TaskId>) -> Self {
        Self {
            id: id.into(),
            ..self
        }
    }

    /// Set specific [`TaskSchedule`] to the existing [`Task`].
    ///
    /// Method is useful if you need to create a new task based on existing (not scheduled yet) [`Task`].
    ///
    /// Method consumes `Task` instance and returns the same instance with specified [`TaskSchedule`].
    /// Since [`Task`] is cloneable, this method can be used to create several identical tasks with different schedules.
    ///
    /// Be careful: [`Task::clone()`] doesn't change `TaskId`,
    /// so it's your responsibility to ensure the uniqueness of task's Id before
    /// posting it to `Scheduler`.
    /// Anyway, `Scheduler::add()` method rejects new `Task` if the same (with the same `TaskId`) is already present,
    /// even if it's finished but not removed by getting its status or by garbage collector.
    ///
    /// # Examples:
    ///
    /// ```rust
    /// use sacs::task::{Task, TaskSchedule};
    /// use std::time::Duration;
    ///
    /// let task1 = Task::new(TaskSchedule::Once, move |id| {
    ///     Box::pin(async move {
    ///         println!("Starting job {id}.");
    ///         // Actual async workload here
    ///         tokio::time::sleep(Duration::from_secs(1)).await;
    ///         // ...
    ///         println!("Job {id} finished.");
    ///         })
    ///     });
    ///
    /// let task2 = task1.clone()
    ///         .with_schedule(TaskSchedule::OnceDelayed(Duration::from_secs(5)))
    ///         .with_id("Execute once with 5s delay");
    ///
    /// let task3 = task1.clone()
    ///         .with_schedule(TaskSchedule::IntervalDelayed(
    ///             Duration::from_secs(2),
    ///             Duration::from_secs(1),
    ///         ))
    ///         .with_id("Repeats every 2s");
    /// ```
    pub fn with_schedule(self, schedule: impl Into<TaskSchedule>) -> Self {
        Self {
            schedule: schedule.into(),
            ..self
        }
    }

    /// Add execution time limit to the existing [`Task`].
    ///
    /// Method is useful to constrain execution time when long-running time is evidence of the potential issue.
    ///
    /// Method consumes `Task` instance and returns the same instance with time constraint added.
    /// Since [`Task`] is cloneable, this method can be used to create several identical tasks with different limits.
    ///
    /// Be careful: [`Task::clone()`] doesn't change `TaskId`,
    /// so it's your responsibility to ensure the uniqueness of task's Id before
    /// posting it to `Scheduler`.
    /// Anyway, `Scheduler::add()` method rejects new `Task` if the same (with the same `TaskId`) is already present,
    /// even if it's finished but not removed by getting its status or by garbage collector.
    ///
    /// # Examples:
    ///
    /// ```rust
    /// use sacs::task::{Task, TaskSchedule};
    /// use std::time::Duration;
    ///
    /// let task1 = Task::new(TaskSchedule::Once, move |id| {
    ///     Box::pin(async move {
    ///         println!("Starting job {id}.");
    ///         // Actual async workload here
    ///         tokio::time::sleep(Duration::from_secs(2)).await;
    ///         // ...
    ///         println!("Job {id} finished.");
    ///         })
    ///     })
    ///     .with_timeout(Duration::from_secs(5))
    ///     .with_id("Execute once with 5s timeout, should succeed");
    ///
    /// let task2 = task1.clone()
    ///         .with_timeout(Duration::from_secs(1))
    ///         .with_id("Execute once with 1s timeout, should fail");
    /// ```
    pub fn with_timeout(self, timeout: impl Into<Duration>) -> Self {
        Self {
            timeout: Some(timeout.into()),
            ..self
        }
    }

    /// Create a new [`Task`] with a specified schedule, job function and explicit [`TaskId`].
    ///
    /// This method is useful if you need to know [`TaskId`] before a task is scheduled.
    /// Or need to use [`TaskId`] within job context: for a particular task its TaskId is constant value,
    /// but JobId varies for each task run.
    ///
    /// # Note:
    /// **_If you provide explicit [`TaskId`] value, your responsibility is to ensure the uniqueness of the [`TaskId`]
    /// within instance of `Scheduler`._**
    ///
    /// # Examples:
    ///
    /// ```rust
    /// use sacs::task::{CronOpts, Task, TaskId, TaskSchedule};
    /// use std::time::Duration;
    /// use uuid::Uuid;
    ///
    /// let schedule = TaskSchedule::Cron("*/5 * * * * *".try_into().unwrap(), CronOpts::default());
    /// let task1_id = Uuid::new_v4();
    /// let task_id = task1_id.clone();
    ///
    /// let task1 = Task::new_with_id(schedule.clone(), move |id| {
    ///     let task_id = task_id.clone();
    ///     Box::pin(async move {
    ///         println!("TaskId={task_id}.");
    ///         // Actual async workload here
    ///         tokio::time::sleep(Duration::from_secs(1)).await;
    ///         // ...
    ///         println!("Job {id} finished.");
    ///         })
    ///     }, task1_id.into());
    ///
    /// let task2 = Task::new_with_id(schedule, |id| {
    ///     Box::pin(async move {
    ///         // Actual async workload here
    ///         tokio::time::sleep(Duration::from_secs(1)).await;
    ///         // ...
    ///         println!("Job {id} finished.");
    ///         })
    ///     }, Uuid::new_v4().into());
    /// ```
    #[deprecated(since = "0.4.2", note = "please use `with_id` method instead.")]
    pub fn new_with_id<T>(schedule: TaskSchedule, job: T, id: TaskId) -> Self
    where
        T: 'static,
        T: FnMut(JobId) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
    {
        Self {
            id,
            job: Arc::new(RwLock::new(Box::new(job))),
            schedule,
            state: TaskState::default(),
            timeout: None,
        }
    }

    /// Returns task's [`TaskId`], it can be used to `drop` task or to get its `status` or `statistics`.
    pub fn id(&self) -> TaskId {
        self.id.clone()
    }

    /// Returns [`TaskSchedule`] associated with the task.
    pub fn schedule(&self) -> TaskSchedule {
        self.schedule.clone()
    }

    /// Returns configured Tasks' timeout wrapped in `Option`.
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Returns task's status.
    pub fn status(&self) -> TaskStatus {
        self.state.status()
    }

    /// Returns various statistics related to the current Tasks' state,
    /// please refer to the [`TaskStatistics`] definition for details.
    pub fn statistics(&self) -> TaskStatistics {
        self.state.statistics.clone()
    }
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("id", &self.id)
            .field("schedule", &self.schedule)
            .field("state", &self.state)
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// Unique identifier of [`Task`] which can be used to address the task in `Scheduler`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId {
    pub(crate) id: String,
}

impl TaskId {
    /// Constructs new unique `TaskId`.
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4().into(),
        }
    }
}
impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<&str> for TaskId {
    fn from(value: &str) -> Self {
        Self { id: value.into() }
    }
}

impl From<String> for TaskId {
    fn from(value: String) -> Self {
        Self { id: value }
    }
}

impl From<&String> for TaskId {
    fn from(value: &String) -> Self {
        Self {
            id: value.to_owned(),
        }
    }
}

impl From<Uuid> for TaskId {
    fn from(value: Uuid) -> Self {
        Self { id: value.into() }
    }
}

impl From<&Uuid> for TaskId {
    fn from(value: &Uuid) -> Self {
        Self {
            id: value.to_string(),
        }
    }
}

impl From<TaskId> for String {
    fn from(value: TaskId) -> Self {
        value.id
    }
}

impl From<&TaskId> for String {
    fn from(value: &TaskId) -> Self {
        value.id.to_owned()
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

/// Defines task's schedule with parameters.
///
/// ## Overview
///
/// Possible schedule types are:
/// - `Once`: the simplest one-shot task without schedule.
///   It starts immediately after adding to Scheduler and doesn't repeat after finishing.
/// - `OnceDelayed`: The same as `Once` but it starts after specified delay.
/// - `Interval`: this is the simplest repeatable task, scheduler starts it immediately after adding, waits for finish
///   and starts the next instance after specified interval.
///   So there can be a single working job only with this type of schedule.
/// - `IntervalDelayed`: its behavior is similar to `Interval` but scheduler starts first job with some specified delay.
/// - `Cron`: the most flexible schedule type which uses well-known cron [`expressions`](CronSchedule) to define time to run
///   and [`CronOpts`] parameter which defines the behavior of task right after adding to scheduler (start the first job immediately or
///   strictly according to the schedule) and allows or restricts concurrent running of jobs.
///
/// ## Note on wall-clock time
///
/// `OnceDelayed`, `Interval` and `IntervalDelayed` compute their next run time relative to
/// wall-clock (`SystemTime::now()`), not a monotonic clock. A system clock step (e.g. NTP
/// correction) will shift the effective wait accordingly: stepping the clock backwards makes
/// the next run happen later than intended, stepping it forwards makes it happen earlier.
/// `Cron` schedules are wall-clock by nature and unaffected by this note.
///
/// ## Examples
///
/// ```rust
/// use sacs::task::*;
/// use std::time::Duration;
///
/// let once = TaskSchedule::Once;
/// let once_after_5m = TaskSchedule::OnceDelayed(Duration::from_secs(5 * 60));
/// let interval_5s = TaskSchedule::Interval(Duration::from_secs(5));
/// let interval_after_15s = TaskSchedule::IntervalDelayed(Duration::from_secs(15), Duration::from_secs(5));
///
/// // Every workday, every morning, every 15 minutes.
/// // Run the next job even if the previous job is still running.
/// let cron = TaskSchedule::Cron(
///             "*/15 8-11 * * Mon-Fri".try_into().unwrap(),
///             CronOpts {
///                 at_start: false,
///                 concurrent: true,
///             });
/// ```
///
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskSchedule {
    /// Starts the job immediately and runs it once (no repetitions).
    Once,
    /// Starts the job after specified delay and runs it once (no repetitions).
    OnceDelayed(Duration),
    /// Starts the first job immediately and after finishing of the previous job repeats it every specified interval.
    Interval(Duration),
    /// Starts the first job with specified delay and after finishing of the first job repeats it every specified interval.
    IntervalDelayed(Duration, Duration),
    /// Runs job(s) repeatedly according to [`cron schedule`](CronSchedule) with respect to [`options`](CronOpts).
    /// See examples above and documentation of [`CronSchedule`] and [`CronOpts`] for details.
    Cron(CronSchedule, CronOpts),
}

impl TaskSchedule {
    /// Computes the time of the first run of the task and, where applicable, validates that
    /// every duration the schedule will ever need (not just the initial one) fits within the
    /// representable `SystemTime` range.
    ///
    /// Returns [`Error::ScheduleOverflow`] if any required time arithmetic overflows, and
    /// [`Error::NoUpcomingEvent`] if a cron schedule (not starting immediately) has no future
    /// occurrence at all (e.g. it points to an impossible calendar date).
    pub(crate) fn initial_run_time(&self) -> crate::Result<SystemTime> {
        let now = SystemTime::now();
        match self {
            TaskSchedule::Once => Ok(now),
            TaskSchedule::OnceDelayed(delay) => {
                now.checked_add(*delay).ok_or(Error::ScheduleOverflow)
            }
            TaskSchedule::Interval(interval) => {
                // Validate the periodic interval up front too: it isn't used for the initial
                // run, but it will overflow at the very first reschedule otherwise.
                now.checked_add(*interval).ok_or(Error::ScheduleOverflow)?;
                Ok(now)
            }
            TaskSchedule::IntervalDelayed(interval, delay) => {
                let at = now.checked_add(*delay).ok_or(Error::ScheduleOverflow)?;
                now.checked_add(*interval).ok_or(Error::ScheduleOverflow)?;
                Ok(at)
            }
            TaskSchedule::Cron(schedule, opts) => {
                if opts.at_start {
                    Ok(now)
                } else {
                    schedule.upcoming().ok_or(Error::NoUpcomingEvent)
                }
            }
        }
    }

    pub(crate) fn after_start_run_time(&self) -> Option<SystemTime> {
        match self {
            TaskSchedule::Cron(schedule, opts) => {
                if opts.concurrent {
                    schedule.upcoming()
                } else {
                    None
                }
            }
            TaskSchedule::Once => None,
            TaskSchedule::OnceDelayed(_) => None,
            TaskSchedule::Interval(_) => None,
            TaskSchedule::IntervalDelayed(_, _) => None,
        }
    }

    /// Computes the time of the next run after a job finished, or `None` if the task
    /// shouldn't be rescheduled anymore (either by design, e.g. `Once`, or because its
    /// schedule ran out, e.g. a cron expression with no more upcoming occurrences, or a
    /// duration that overflows the representable `SystemTime` range).
    pub(crate) fn after_finish_run_time(&self) -> Option<SystemTime> {
        match self {
            TaskSchedule::Interval(interval) => {
                let at = SystemTime::now().checked_add(*interval);
                if at.is_none() {
                    warn!(
                        ?interval,
                        "interval overflows system time range, task won't be rescheduled"
                    );
                }
                at
            }
            TaskSchedule::IntervalDelayed(interval, _delay) => {
                let at = SystemTime::now().checked_add(*interval);
                if at.is_none() {
                    warn!(
                        ?interval,
                        "interval overflows system time range, task won't be rescheduled"
                    );
                }
                at
            }
            TaskSchedule::Once => None,
            TaskSchedule::OnceDelayed(_) => None,
            TaskSchedule::Cron(schedule, opts) => {
                if opts.concurrent {
                    None
                } else {
                    schedule.upcoming()
                }
            }
        }
    }
}

/// Defines specific behavior of cron schedule.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CronOpts {
    /// If `true` then the first job will be scheduled right after adding the task to scheduler even if
    /// this time is out of schedule. Default is `false`.
    pub at_start: bool,
    /// If `true` then scheduler will run a new job every moment of schedule regardless of completion
    /// of previous run.
    /// If `false` (default), then scheduler prohibits
    /// running several tasks at the same time guaranteeing that a single job only will be running.
    pub concurrent: bool,
}

/// Represents the current state of [`Task`] instance.
///
/// Each task in every moment of time has some determined state:
/// - just created task which wasn't pushed to scheduler is `New`.
/// - when scheduler got the task it plans when the task should be started and puts it to `Waiting`
///   until time to run arrived.
/// - when start time arrived,
///   the scheduler posts job (instance of a task) to execution engine and moves it `Scheduled` state.
/// - when executor started the job (this moment depends on the amount of free execution resources),
///   it moves the task to `Running` state.
/// - when the task is completed, it may be rescheduled (if it's repeatable) and moved to `Waiting` or `Scheduled`,
///   or may be `Finished` if that's kind of one-shot task.
#[derive(Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TaskStatus {
    /// Just created, not added to `Scheduler`.
    #[default]
    New,
    /// Scheduler waits for the specified moment of time to run the job.
    Waiting,
    /// Job instance has been scheduled to execute but hasn't been run yet.
    Scheduled,
    /// It works right now.
    Running,
    /// All jobs of the task have been finished (completed, canceled or timed out), and no more jobs will be scheduled anymore.
    Finished,
}

/// Struct represents various statistic numbers about [`Task`].
#[derive(Default, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskStatistics {
    /// Number of jobs in waiting state: they will be running when time to run arrived.
    pub waiting: usize,
    /// Number of scheduled but not yet running jobs.
    pub scheduled: usize,
    /// Number of jobs in running state.
    pub running: usize,
    /// Number of successfully completed jobs.
    pub completed: usize,
    /// Number of canceled jobs.
    pub canceled: usize,
    /// Number of jobs killed because the run time limit exceeded.
    pub timeouts: usize,
    /// Number of jobs finished unsuccessfully because of runtime errors.
    pub errors: usize,
}

#[derive(Default, Clone)]
pub(crate) struct TaskState {
    statistics: TaskStatistics,
    scheduled_jobs: BTreeSet<JobId>,
    running_jobs: BTreeSet<JobId>,
    last_finished_at: Option<SystemTime>,
}

impl std::fmt::Debug for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let last_finished_at = if let Some(last_finished_at) = self.last_finished_at {
            format_local_time(last_finished_at)
        } else {
            "None".to_string()
        };

        f.debug_struct("TaskState")
            .field("waiting", &self.statistics.waiting)
            .field("scheduled", &self.statistics.scheduled)
            .field("running", &self.statistics.running)
            .field("completed", &self.statistics.completed)
            .field("canceled", &self.statistics.canceled)
            .field("timeouts", &self.statistics.timeouts)
            .field("errors", &self.statistics.errors)
            .field("scheduled_jobs", &self.scheduled_jobs)
            .field("running_jobs", &self.running_jobs)
            .field("last_finished_at", &last_finished_at)
            .finish()
    }
}

impl TaskState {
    pub(crate) fn status(&self) -> TaskStatus {
        if self.statistics.running > 0 {
            TaskStatus::Running
        } else if self.statistics.scheduled > 0 {
            TaskStatus::Scheduled
        } else if self.statistics.waiting > 0 {
            TaskStatus::Waiting
        } else if self.statistics.completed > 0
            || self.statistics.canceled > 0
            || self.statistics.timeouts > 0
            || self.statistics.errors > 0
        {
            TaskStatus::Finished
        } else {
            TaskStatus::New
        }
    }

    pub(crate) fn task_enqueued(&mut self) -> &Self {
        self.statistics.waiting += 1;
        debug!(status = ?self.status(), "task enqueued");
        self
    }

    pub(crate) fn job_scheduled(&mut self, id: JobId) -> &Self {
        debug_assert!(
            self.statistics.waiting > 0,
            "job_scheduled: waiting counter underflow"
        );
        self.statistics.waiting = self.statistics.waiting.saturating_sub(1);
        self.statistics.scheduled += 1;
        self.scheduled_jobs.insert(id);
        debug!(status = ?self.status(), "job scheduled");
        self
    }

    pub(crate) fn job_started(&mut self, id: JobId) -> &Self {
        debug_assert!(
            self.statistics.scheduled > 0,
            "job_started: scheduled counter underflow"
        );
        self.statistics.scheduled = self.statistics.scheduled.saturating_sub(1);
        self.statistics.running += 1;
        self.scheduled_jobs.remove(&id);
        self.running_jobs.insert(id);
        debug!(status = ?self.status(), "job started");
        self
    }

    pub(crate) fn job_completed(&mut self, id: &JobId) -> &Self {
        debug_assert!(
            self.statistics.running > 0,
            "job_completed: running counter underflow"
        );
        self.statistics.running = self.statistics.running.saturating_sub(1);
        self.statistics.completed += 1;
        self.running_jobs.remove(id);
        self.last_finished_at = Some(SystemTime::now());
        debug!(status = ?self.status(), "job completed");
        self
    }

    pub(crate) fn job_canceled(&mut self, id: &JobId) -> &Self {
        self.statistics.canceled += 1;
        if self.running_jobs.remove(id) {
            debug_assert!(
                self.statistics.running > 0,
                "job_canceled: running counter underflow"
            );
            self.statistics.running = self.statistics.running.saturating_sub(1);
        } else {
            self.scheduled_jobs.remove(id);
            debug_assert!(
                self.statistics.scheduled > 0,
                "job_canceled: scheduled counter underflow"
            );
            self.statistics.scheduled = self.statistics.scheduled.saturating_sub(1);
        }
        self.last_finished_at = Some(SystemTime::now());
        debug!(status = ?self.status(), "job canceled");
        self
    }

    pub(crate) fn job_timeout(&mut self, id: &JobId) -> &Self {
        debug_assert!(
            self.statistics.running > 0,
            "job_timeout: running counter underflow"
        );
        self.statistics.running = self.statistics.running.saturating_sub(1);
        self.statistics.timeouts += 1;
        self.running_jobs.remove(id);
        self.last_finished_at = Some(SystemTime::now());
        debug!(status = ?self.status(), "job killed because timeout");
        self
    }

    pub(crate) fn job_error(&mut self, id: &JobId) -> &Self {
        debug_assert!(
            self.statistics.running > 0,
            "job_error: running counter underflow"
        );
        self.statistics.running = self.statistics.running.saturating_sub(1);
        self.statistics.errors += 1;
        self.running_jobs.remove(id);
        self.last_finished_at = Some(SystemTime::now());
        debug!(status = ?self.status(), "job finished with error");
        self
    }

    pub(crate) fn is_task_finished(&self) -> bool {
        self.statistics.waiting == 0
            && self.statistics.scheduled == 0
            && self.statistics.running == 0
            && (self.statistics.completed > 0
                || self.statistics.canceled > 0
                || self.statistics.timeouts > 0
                || self.statistics.errors > 0)
            && self.scheduled_jobs.is_empty()
            && self.running_jobs.is_empty()
    }

    pub(crate) fn last_finished_at(&self) -> Option<SystemTime> {
        self.last_finished_at
    }

    pub(crate) fn jobs(&self) -> BTreeSet<JobId> {
        let mut jobs = BTreeSet::new();
        jobs.extend(self.scheduled_jobs.clone());
        jobs.extend(self.running_jobs.clone());
        jobs
    }

    /// Test-only seam: every real terminal transition (`job_completed`/`job_canceled`/
    /// `job_timeout`/`job_error`) also sets `last_finished_at`, so a finished task without
    /// it is currently unreachable through normal state transitions. This constructs that
    /// otherwise-unreachable state directly, so the garbage collector's defensive handling
    /// of it (see `GarbageCollector::collect_garbage`) can be regression-tested.
    #[cfg(test)]
    pub(crate) fn test_finished_without_last_finished_at() -> Self {
        let mut state = Self::default();
        state.statistics.completed = 1;
        state
    }
}

/// Defines cron expression used by schedule.
///
/// It uses seven items expression: seconds, minutes, hours, days (month), months, days (week), years.
/// Seconds and years can be omitted: if an expression has five items, it runs at second 0 every year;
/// if it has six items - the first item defines seconds.
///
/// For more information about valid cron expressions,
/// please refer to [cron-lite](https://docs.rs/cron-lite/) crate documentation.
///
/// An expression without a timezone is evaluated in the system's local timezone. To use a specific timezone,
/// prefix the expression with `TZ=<timezone name>`, e.g. `TZ=Europe/Kyiv 0 12 * * *`. Timezone names are resolved
/// at runtime from the operating system's timezone database, so the parsing of such an expression fails with
/// [`Error::InvalidCronExpression`] if the timezone is unknown or the database (e.g. `tzdata` package) is missing,
/// see the [crate documentation](crate#cron-schedules-with-timezone) for details.
///
/// ## Examples
///
/// ```rust
/// use sacs::task::CronSchedule;
///
/// let every_minute_1: CronSchedule = "* * * * *".try_into().unwrap();
/// let every_minute_2: CronSchedule = "0 * * * * *".try_into().unwrap();
/// assert_eq!(every_minute_1, every_minute_2);
///
/// let every_5th_second: CronSchedule = "*/5 * * * * *".try_into().unwrap();
/// let every_business_hour: CronSchedule = "0 9-18 * * Mon-Fri".try_into().unwrap();
///
/// // Every even year every 10 seconds at 9:00-9:01am on January 1st.
/// let something_senseless: CronSchedule = "*/10 0 9 1 1 ? */2".try_into().unwrap();
///
/// // Every day at noon, Kyiv time, regardless of the system's local timezone.
/// let kyiv_noon: CronSchedule = "TZ=Europe/Kyiv 0 12 * * *".try_into().unwrap();
/// ```
///
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CronSchedule {
    schedule: Box<Schedule>,
}

impl Display for CronSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.schedule)
    }
}

impl CronSchedule {
    /// Returns the next occurrence of the schedule, or `None` if there is none (e.g. an
    /// impossible calendar date, a past-only year, or any schedule beyond the underlying
    /// cron engine's representable range).
    ///
    /// A schedule without an explicit `TZ=` prefix is evaluated in the system's local timezone.
    fn upcoming(&self) -> Option<SystemTime> {
        self.schedule
            .upcoming(&ZonedDateTime::now())
            .and_then(|next| SystemTime::try_from(next).ok())
    }
}

impl TryFrom<String> for CronSchedule {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        // Try to convert into schedule
        let schedule = Schedule::new(value)?;
        Ok(Self {
            schedule: Box::new(schedule),
        })
    }
}

impl std::str::FromStr for CronSchedule {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value.to_string())
    }
}

impl TryFrom<&str> for CronSchedule {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from(value.to_string())
    }
}

impl TryFrom<&String> for CronSchedule {
    type Error = Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::try_from(value.to_string())
    }
}

impl From<EventId> for TaskId {
    fn from(value: EventId) -> Self {
        Self { id: value.id }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::Result;

    #[test]
    fn cron_with_seconds() {
        let cron: Result<CronSchedule> = " *    * * * * *".try_into();
        assert!(cron.is_ok());
        assert!(" *    * * * * *".parse::<CronSchedule>().is_ok());
        assert!("not a cron".parse::<CronSchedule>().is_err());

        let cron: Result<CronSchedule> = " 5  * * * * * ".try_into();
        assert!(cron.is_ok());

        let cron: Result<CronSchedule> = " */5 * *  *  * * ".try_into();
        assert!(cron.is_ok());
    }

    #[test]
    fn cron_without_seconds() {
        let cron: Result<CronSchedule> = " * * *  * *".try_into();
        assert!(cron.is_ok());

        let cron: Result<CronSchedule> = " 12 * * *  * ".try_into();
        assert!(cron.is_ok());

        let cron: Result<CronSchedule> = " */10 3 *  * * ".try_into();
        assert!(cron.is_ok());
    }

    #[test]
    fn wrong_cron_expression() {
        let cron: Result<CronSchedule> = "* * * * ".try_into();
        assert!(cron.is_err());

        let cron: Result<CronSchedule> = "* 24 * * * ".try_into();
        assert!(cron.is_err());

        let cron: Result<CronSchedule> = "* * 0,32 * * ".try_into();
        assert!(cron.is_err());

        let cron: Result<CronSchedule> = "* * * 13 * ".try_into();
        assert!(cron.is_err());
    }

    #[test]
    fn task_state_finished_status_completed() {
        let job = JobId::new("job");
        let mut state = TaskState::default();
        assert_eq!(state.status(), TaskStatus::New);

        state.task_enqueued();
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Waiting);

        state.job_scheduled(job.clone());
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Scheduled);

        state.job_started(job.clone());
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Running);

        state.job_completed(&job);
        assert!(state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Finished);
    }

    #[test]
    fn task_state_finished_status_canceled_started() {
        let job = JobId::new("job");
        let mut state = TaskState::default();
        assert_eq!(state.status(), TaskStatus::New);

        state.task_enqueued();
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Waiting);

        state.job_scheduled(job.clone());
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Scheduled);

        state.job_started(job.clone());
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Running);

        state.job_canceled(&job);
        assert!(state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Finished);
    }

    #[test]
    fn task_state_finished_status_canceled_scheduled() {
        let job = JobId::new("job");
        let mut state = TaskState::default();
        assert_eq!(state.status(), TaskStatus::New);

        state.task_enqueued();
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Waiting);

        state.job_scheduled(job.clone());
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Scheduled);

        state.job_canceled(&job);
        assert!(state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Finished);
    }

    #[test]
    fn task_state_finished_status_timeout() {
        let job = JobId::new("job");
        let mut state = TaskState::default();
        assert_eq!(state.status(), TaskStatus::New);

        state.task_enqueued();
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Waiting);

        state.job_scheduled(job.clone());
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Scheduled);

        state.job_started(job.clone());
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Running);

        state.job_timeout(&job);
        assert!(state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Finished);
    }

    #[test]
    fn task_state_finished_status_error() {
        let job = JobId::new("job");
        let mut state = TaskState::default();
        assert_eq!(state.status(), TaskStatus::New);

        state.task_enqueued();
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Waiting);

        state.job_scheduled(job.clone());
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Scheduled);

        state.job_started(job.clone());
        assert!(!state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Running);

        state.job_error(&job);
        assert!(state.is_task_finished());
        assert_eq!(state.status(), TaskStatus::Finished);
    }

    #[test]
    fn task_state_transition() {
        let job1 = JobId::new("task1");
        let job2 = JobId::new("task2");
        let job3 = JobId::new("task3");
        let job4 = JobId::new("task4");
        let job5 = JobId::new("task5");

        let mut state = TaskState::default();
        assert_eq!(state.status(), TaskStatus::New);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(format!("{state:?}"), String::from("TaskState { waiting: 0, scheduled: 0, running: 0, completed: 0, canceled: 0, timeouts: 0, errors: 0, scheduled_jobs: {}, running_jobs: {}, last_finished_at: \"None\" }"));
        assert_eq!(state.statistics, TaskStatistics::default());

        state.task_enqueued();
        assert_eq!(state.status(), TaskStatus::Waiting);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 1,
                ..Default::default()
            }
        );

        state.task_enqueued();
        assert_eq!(state.status(), TaskStatus::Waiting);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 2,
                ..Default::default()
            }
        );

        state.task_enqueued();
        assert_eq!(state.status(), TaskStatus::Waiting);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 3,
                ..Default::default()
            }
        );

        state.task_enqueued();
        assert_eq!(state.status(), TaskStatus::Waiting);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 4,
                ..Default::default()
            }
        );

        state.task_enqueued();
        assert_eq!(state.status(), TaskStatus::Waiting);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 5,
                ..Default::default()
            }
        );

        state.job_scheduled(job1.clone());
        assert_eq!(state.status(), TaskStatus::Scheduled);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(format!("{state:?}"), format!("TaskState {{ waiting: 4, scheduled: 1, running: 0, completed: 0, canceled: 0, timeouts: 0, errors: 0, scheduled_jobs: {{JobId {{ task_id: TaskId {{ id: \"task1\" }}, id: {} }}}}, running_jobs: {{}}, last_finished_at: \"None\" }}", job1.id.to_string()));
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 4,
                scheduled: 1,
                ..Default::default()
            }
        );

        let jobs = state.jobs();
        let expected = BTreeSet::<JobId>::from([job1.clone()]);
        assert_eq!(jobs, expected);

        state.job_started(job1.clone());
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 4,
                scheduled: 0,
                running: 1,
                ..Default::default()
            }
        );

        let jobs = state.jobs();
        let expected = BTreeSet::<JobId>::from([job1.clone()]);
        assert_eq!(jobs, expected);

        state.job_scheduled(job5.clone());
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 3,
                scheduled: 1,
                running: 1,
                ..Default::default()
            }
        );

        let jobs = state.jobs();
        let expected = BTreeSet::<JobId>::from([job1.clone(), job5.clone()]);

        assert_eq!(jobs, expected);
        state.job_started(job5.clone());
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 3,
                scheduled: 0,
                running: 2,
                ..Default::default()
            }
        );

        let jobs = state.jobs();
        let expected = BTreeSet::<JobId>::from([job1.clone(), job5.clone()]);
        assert_eq!(jobs, expected);

        state.job_scheduled(job2.clone());
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 2,
                scheduled: 1,
                running: 2,
                ..Default::default()
            }
        );

        let jobs = state.jobs();
        let expected = BTreeSet::<JobId>::from([job1.clone(), job2.clone(), job5.clone()]);
        assert_eq!(jobs, expected);

        state.job_scheduled(job3.clone());
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 1,
                scheduled: 2,
                running: 2,
                ..Default::default()
            }
        );

        let jobs = state.jobs();
        let expected =
            BTreeSet::<JobId>::from([job1.clone(), job2.clone(), job3.clone(), job5.clone()]);
        assert_eq!(jobs, expected);

        state.job_started(job3.clone());
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 1,
                scheduled: 1,
                running: 3,
                ..Default::default()
            }
        );

        let jobs = state.jobs();
        let expected =
            BTreeSet::<JobId>::from([job1.clone(), job2.clone(), job3.clone(), job5.clone()]);
        assert_eq!(jobs, expected);

        state.job_scheduled(job4.clone());
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 0,
                scheduled: 2,
                running: 3,
                ..Default::default()
            }
        );

        let jobs = state.jobs();
        let expected = BTreeSet::<JobId>::from([
            job1.clone(),
            job2.clone(),
            job3.clone(),
            job4.clone(),
            job5.clone(),
        ]);
        assert_eq!(jobs, expected);

        state.job_started(job4.clone());
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_none());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 0,
                scheduled: 1,
                running: 4,
                ..Default::default()
            }
        );

        let jobs = state.jobs();
        let expected = BTreeSet::<JobId>::from([
            job1.clone(),
            job2.clone(),
            job3.clone(),
            job4.clone(),
            job5.clone(),
        ]);
        assert_eq!(jobs, expected);

        state.job_canceled(&job2);
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_some());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 0,
                scheduled: 0,
                running: 4,
                canceled: 1,
                ..Default::default()
            }
        );

        let jobs = state.jobs();
        let expected =
            BTreeSet::<JobId>::from([job1.clone(), job3.clone(), job4.clone(), job5.clone()]);
        assert_eq!(jobs, expected);

        state.job_timeout(&job3);
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_some());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 0,
                scheduled: 0,
                running: 3,
                canceled: 1,
                timeouts: 1,
                errors: 0,
                completed: 0
            }
        );

        let jobs = state.jobs();
        let expected = BTreeSet::<JobId>::from([job1.clone(), job4.clone(), job5.clone()]);
        assert_eq!(jobs, expected);

        state.job_error(&job4);
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_some());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 0,
                scheduled: 0,
                running: 2,
                canceled: 1,
                timeouts: 1,
                errors: 1,
                completed: 0
            }
        );

        let jobs = state.jobs();
        let expected = BTreeSet::<JobId>::from([job1.clone(), job5.clone()]);
        assert_eq!(jobs, expected);

        state.job_canceled(&job5);
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.is_task_finished());
        assert!(state.last_finished_at().is_some());
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 0,
                scheduled: 0,
                running: 1,
                canceled: 2,
                timeouts: 1,
                errors: 1,
                completed: 0
            }
        );

        let jobs = state.jobs();
        let expected = BTreeSet::<JobId>::from([job1.clone()]);
        assert_eq!(jobs, expected);

        state.job_completed(&job1);
        assert_eq!(state.status(), TaskStatus::Finished);
        assert!(state.is_task_finished());
        assert!(state.last_finished_at().is_some());
        assert_eq!(format!("{state:?}"), format!("TaskState {{ waiting: 0, scheduled: 0, running: 0, completed: 1, canceled: 2, timeouts: 1, errors: 1, scheduled_jobs: {{}}, running_jobs: {{}}, last_finished_at: \"{}\" }}", format_local_time(state.last_finished_at().unwrap())));
        assert_eq!(
            state.statistics,
            TaskStatistics {
                waiting: 0,
                scheduled: 0,
                running: 0,
                canceled: 2,
                timeouts: 1,
                errors: 1,
                completed: 1
            }
        );

        let jobs = state.jobs();
        let expected = BTreeSet::<JobId>::from([]);
        assert_eq!(jobs, expected);
    }

    #[test]
    fn cron_opts_default() {
        assert_eq!(
            CronOpts::default(),
            CronOpts {
                at_start: false,
                concurrent: false
            }
        );
    }

    #[test]
    fn type_convertors() {
        let uuid_id = Uuid::new_v4();
        let str_id = uuid_id.to_string();

        assert_eq!(
            TaskId::from(String::from("TASK_ID")).id,
            String::from("TASK_ID")
        );
        assert_eq!(
            TaskId::from(&String::from("TASK_ID")).id,
            String::from("TASK_ID")
        );

        assert_eq!(TaskId::from("TASK_ID").id, String::from("TASK_ID"));

        assert_eq!(TaskId::from(uuid_id).id, str_id);
        assert_eq!(TaskId::from(&uuid_id).id, str_id);

        assert_eq!(String::from(TaskId::from(uuid_id)), str_id);
        assert_eq!(String::from(&TaskId::from(uuid_id)), str_id);

        assert_ne!(TaskId::default(), TaskId::default());
    }

    #[test]
    fn constructors() {
        let task = Task::new(TaskSchedule::OnceDelayed(Duration::from_secs(1)), |_id| {
            Box::pin(async move {})
        });

        assert_eq!(
            task.clone().with_id("TEST").id().to_string(),
            String::from("TEST")
        );
        assert_eq!(
            task.clone().with_schedule(TaskSchedule::Once).schedule(),
            TaskSchedule::Once
        );
        assert_eq!(
            task.clone().with_timeout(Duration::from_secs(10)).timeout(),
            Some(Duration::from_secs(10))
        );
        assert_eq!(task.clone().timeout(), None);
        assert_eq!(task.status(), TaskStatus::New);

        let id = Uuid::new_v4();
        #[allow(deprecated)]
        let task = Task::new_with_id(
            TaskSchedule::OnceDelayed(Duration::from_secs(1)),
            |_id| Box::pin(async move {}),
            id.into(),
        );
        assert_eq!(task.id().to_string(), id.to_string());
        assert_eq!(task.status(), TaskStatus::New);

        assert_eq!(
            CronSchedule::try_from("1 2 3 4 ?").unwrap(),
            CronSchedule::try_from(String::from("0 1 2 3 4 ? *")).unwrap()
        );
        assert_eq!(
            CronSchedule::try_from("1 2 ? 4 5").unwrap(),
            CronSchedule::try_from(&String::from("0 1 2 ? 4 5 *")).unwrap()
        );
    }

    #[test]
    fn task_status_reflects_real_task_state() {
        let mut task = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
        let task_id = task.id();
        let job_id = JobId::new(task_id);

        assert_eq!(task.status(), TaskStatus::New);

        task.state.task_enqueued();
        assert_eq!(task.status(), TaskStatus::Waiting);

        task.state.job_scheduled(job_id.clone());
        assert_eq!(task.status(), TaskStatus::Scheduled);

        task.state.job_started(job_id.clone());
        assert_eq!(task.status(), TaskStatus::Running);

        task.state.job_completed(&job_id);
        assert_eq!(task.status(), TaskStatus::Finished);
    }

    #[test]
    fn debug_formatter() {
        let task1 = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {})).with_id("TEST");
        let task2 = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}))
            .with_id("TEST_WITH_TIMEOUT")
            .with_timeout(Duration::from_secs(1));

        assert_eq!(format!("{task1:?}"), "Task { id: TaskId { id: \"TEST\" }, schedule: Once, state: TaskState { waiting: 0, scheduled: 0, running: 0, completed: 0, canceled: 0, timeouts: 0, errors: 0, scheduled_jobs: {}, running_jobs: {}, last_finished_at: \"None\" }, timeout: None }");
        assert_eq!(format!("{task2:?}"), "Task { id: TaskId { id: \"TEST_WITH_TIMEOUT\" }, schedule: Once, state: TaskState { waiting: 0, scheduled: 0, running: 0, completed: 0, canceled: 0, timeouts: 0, errors: 0, scheduled_jobs: {}, running_jobs: {}, last_finished_at: \"None\" }, timeout: Some(1s) }");

        assert_eq!(
            format!("{}", CronSchedule::try_from("1 2 3 4 ?").unwrap()),
            String::from("0 1 2 3 4 ? *")
        );
    }

    // O2: a cron expression that parses but has no future occurrence must not panic.
    #[test]
    fn o2_impossible_calendar_date_upcoming_is_none() {
        // February 30th never exists.
        let cron: CronSchedule = "0 0 0 30 2 *".try_into().unwrap();
        assert_eq!(cron.upcoming(), None);
    }

    #[test]
    fn cron_schedule_with_timezone_fires_at_zone_local_time() {
        for zone in ["UTC", "Europe/Kyiv", "America/New_York", "Asia/Tokyo"] {
            let cron: CronSchedule = format!("TZ={zone} 30 12 * * *")
                .as_str()
                .try_into()
                .unwrap();
            assert_eq!(cron.to_string(), format!("TZ={zone} 0 30 12 * * * *"));

            let next = cron.upcoming().unwrap();
            assert!(next > SystemTime::now());
            assert!(next <= SystemTime::now() + Duration::from_secs(25 * 60 * 60));

            let tz = jiff::tz::TimeZone::get(zone).unwrap();
            let next = jiff::Timestamp::try_from(next)
                .unwrap()
                .to_zoned_datetime(tz);
            assert_eq!(
                (next.hour(), next.minute(), next.second()),
                (12, 30, 0),
                "{zone}: {next}"
            );
        }
    }

    #[test]
    fn cron_schedule_with_unknown_timezone_is_rejected() {
        let result: Result<CronSchedule> = "TZ=Nowhere/Atlantis 0 12 * * *".try_into();
        assert!(
            matches!(
                result,
                Err(Error::InvalidCronExpression(
                    cron_lite::error::CronError::InvalidTimeZone(ref tz)
                )) if tz == "Nowhere/Atlantis"
            ),
            "{result:?}"
        );
    }

    #[test]
    fn o2_past_only_year_upcoming_is_none() {
        let cron: CronSchedule = "0 0 0 1 1 ? 2020".try_into().unwrap();
        assert_eq!(cron.upcoming(), None);
    }

    #[test]
    fn o2_initial_run_time_rejects_no_upcoming_cron() {
        let schedule = TaskSchedule::Cron(
            "0 0 0 30 2 *".try_into().unwrap(),
            CronOpts {
                at_start: false,
                concurrent: false,
            },
        );
        assert!(matches!(
            schedule.initial_run_time(),
            Err(Error::NoUpcomingEvent)
        ));
    }

    #[test]
    fn o2_at_start_bypasses_no_upcoming_cron_on_admission() {
        // With `at_start: true`, admission doesn't need an upcoming occurrence: the first
        // job runs immediately, and the task simply finishes afterwards (no reschedule).
        let schedule = TaskSchedule::Cron(
            "0 0 0 30 2 *".try_into().unwrap(),
            CronOpts {
                at_start: true,
                concurrent: false,
            },
        );
        assert!(schedule.initial_run_time().is_ok());
        assert_eq!(schedule.after_finish_run_time(), None);
    }

    // O1: duration arithmetic overflow must be rejected, not panic.
    #[test]
    fn o1_once_delayed_overflow_is_rejected() {
        let schedule = TaskSchedule::OnceDelayed(Duration::MAX);
        assert!(matches!(
            schedule.initial_run_time(),
            Err(Error::ScheduleOverflow)
        ));
    }

    #[test]
    fn o1_interval_overflow_is_rejected_at_admission() {
        let schedule = TaskSchedule::Interval(Duration::MAX);
        assert!(matches!(
            schedule.initial_run_time(),
            Err(Error::ScheduleOverflow)
        ));
    }

    // N9: `IntervalDelayed`'s *interval* must be validated at admission too, not only its
    // delay -- otherwise a valid delay with an overflowing interval passes admission and
    // only fails (silently) at the first reschedule.
    #[test]
    fn n9_interval_delayed_validates_both_fields() {
        let schedule = TaskSchedule::IntervalDelayed(Duration::MAX, Duration::from_millis(10));
        assert!(matches!(
            schedule.initial_run_time(),
            Err(Error::ScheduleOverflow)
        ));
    }

    #[test]
    fn interval_delayed_valid_schedule_is_accepted() {
        let schedule =
            TaskSchedule::IntervalDelayed(Duration::from_secs(1), Duration::from_millis(10));
        assert!(schedule.initial_run_time().is_ok());
    }

    // N9: reschedule-time overflow degrades to "stop rescheduling", not a panic, matching
    // the admission-time error for the same input.
    #[test]
    fn interval_after_finish_overflow_returns_none() {
        let schedule = TaskSchedule::Interval(Duration::MAX);
        assert_eq!(schedule.after_finish_run_time(), None);
    }

    // N9: same degrade-to-"stop rescheduling" behavior for `IntervalDelayed`'s periodic
    // interval, not just its initial delay.
    #[test]
    fn interval_delayed_after_finish_overflow_returns_none() {
        let schedule = TaskSchedule::IntervalDelayed(Duration::MAX, Duration::from_millis(10));
        assert_eq!(schedule.after_finish_run_time(), None);
    }

    // Step 4b: an out-of-order transition (which normal scheduler flows never trigger) must
    // fail loudly in debug builds via `debug_assert!` ...
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "job_scheduled: waiting counter underflow")]
    fn task_state_counters_fail_loudly_in_debug_on_invalid_transition() {
        let job = JobId::new("job");
        let mut state = TaskState::default();
        // Calling job_scheduled without a preceding task_enqueued underflows `waiting`.
        state.job_scheduled(job);
    }

    // ... and degrade to saturating at 0 instead of wrapping/corrupting in release builds
    // (where `debug_assert!` compiles out). This is what actually protects production if
    // `overflow-checks` is off, as it is by default: an unchecked `-= 1` from 0 would
    // silently wrap `waiting` to `usize::MAX`, permanently corrupting the task's public
    // statistics and making `is_task_finished()` never become true again.
    #[test]
    #[cfg(not(debug_assertions))]
    fn task_state_counters_saturate_instead_of_underflow_in_release() {
        let job = JobId::new("job");
        let mut state = TaskState::default();
        state.job_scheduled(job);
        assert_eq!(state.statistics.waiting, 0);
        assert_eq!(state.statistics.scheduled, 1);
    }
}
