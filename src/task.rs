//! [`Task`] object represents a single job with schedule.
//! Use it to create workload of different types and post it to `Scheduler`.
//!
//! Module contains everything related to [`Task`], it's [`TaskSchedule`] and [`state`](TaskStatus).
//!
use crate::{event::EventId, job::JobId, AsyncJobBoxed, Error};
use chrono::{DateTime, Local};
use cron_lite::Schedule;
use futures::Future;
use std::{
    collections::BTreeSet,
    fmt::Display,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use tracing::debug;
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
    /// Or need to use [`TaskId`] within job context: for a particular task it's TaskId is constant value,
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

    /// Returns task's [`TaskId`], it can be used to `drop` task or to get it's `status` or `statistics`.
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
///   and [`CronOpts`] parameter which defines the behavior of task right after adding to scheduler (start first job immediately or
///   strictly according to the schedule) and allows or restricts concurrent running of jobs.
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
    pub(crate) fn initial_run_time(&self) -> SystemTime {
        match self {
            TaskSchedule::Once => SystemTime::now(),
            TaskSchedule::OnceDelayed(delay) => SystemTime::now().checked_add(*delay).unwrap(),
            TaskSchedule::Interval(_interval) => SystemTime::now(),
            TaskSchedule::IntervalDelayed(_interval, delay) => {
                SystemTime::now().checked_add(*delay).unwrap()
            }
            TaskSchedule::Cron(schedule, opts) => {
                if opts.at_start {
                    SystemTime::now()
                } else {
                    schedule.upcoming()
                }
            }
        }
    }

    pub(crate) fn after_start_run_time(&self) -> Option<SystemTime> {
        match self {
            TaskSchedule::Cron(schedule, opts) => {
                if opts.concurrent {
                    Some(schedule.upcoming())
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
    pub(crate) fn after_finish_run_time(&self) -> Option<SystemTime> {
        match self {
            TaskSchedule::Interval(interval) => Some(SystemTime::now().checked_add(*interval)?),
            TaskSchedule::IntervalDelayed(interval, _delay) => {
                Some(SystemTime::now().checked_add(*interval)?)
            }
            TaskSchedule::Once => None,
            TaskSchedule::OnceDelayed(_) => None,
            TaskSchedule::Cron(schedule, opts) => {
                if opts.concurrent {
                    None
                } else {
                    Some(schedule.upcoming())
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
            format!("{}", DateTime::<Local>::from(last_finished_at))
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
        self.statistics.waiting -= 1;
        self.statistics.scheduled += 1;
        self.scheduled_jobs.insert(id);
        debug!(status = ?self.status(), "job scheduled");
        self
    }

    pub(crate) fn job_started(&mut self, id: JobId) -> &Self {
        self.statistics.scheduled -= 1;
        self.statistics.running += 1;
        self.scheduled_jobs.remove(&id);
        self.running_jobs.insert(id);
        debug!(status = ?self.status(), "job started");
        self
    }

    pub(crate) fn job_completed(&mut self, id: &JobId) -> &Self {
        self.statistics.running -= 1;
        self.statistics.completed += 1;
        self.running_jobs.remove(id);
        self.last_finished_at = Some(SystemTime::now());
        debug!(status = ?self.status(), "job completed");
        self
    }

    pub(crate) fn job_canceled(&mut self, id: &JobId) -> &Self {
        self.statistics.canceled += 1;
        if self.running_jobs.remove(id) {
            self.statistics.running -= 1;
        } else {
            self.scheduled_jobs.remove(id);
            self.statistics.scheduled -= 1;
        }
        self.last_finished_at = Some(SystemTime::now());
        debug!(status = ?self.status(), "job canceled");
        self
    }

    pub(crate) fn job_timeout(&mut self, id: &JobId) -> &Self {
        self.statistics.running -= 1;
        self.statistics.timeouts += 1;
        self.running_jobs.remove(id);
        self.last_finished_at = Some(SystemTime::now());
        debug!(status = ?self.status(), "job killed because timeout");
        self
    }

    pub(crate) fn job_error(&mut self, id: &JobId) -> &Self {
        self.statistics.running -= 1;
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
/// It's possible to use cron expression with timezone if `tz` feature is enabled.
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
    fn upcoming(&self) -> SystemTime {
        let next: SystemTime = self.schedule.upcoming(&Local::now()).unwrap().into();
        next
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
        assert_eq!(format!("{state:?}"), format!("TaskState {{ waiting: 0, scheduled: 0, running: 0, completed: 1, canceled: 2, timeouts: 1, errors: 1, scheduled_jobs: {{}}, running_jobs: {{}}, last_finished_at: \"{}\" }}", DateTime::<Local>::from(state.last_finished_at().unwrap())));
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

        assert_eq!(format!("{task1:?}"), format!("Task {{ id: TaskId {{ id: \"TEST\" }}, schedule: Once, state: TaskState {{ waiting: 0, scheduled: 0, running: 0, completed: 0, canceled: 0, timeouts: 0, errors: 0, scheduled_jobs: {{}}, running_jobs: {{}}, last_finished_at: \"None\" }}, timeout: None }}"));
        assert_eq!(format!("{task2:?}"), format!("Task {{ id: TaskId {{ id: \"TEST_WITH_TIMEOUT\" }}, schedule: Once, state: TaskState {{ waiting: 0, scheduled: 0, running: 0, completed: 0, canceled: 0, timeouts: 0, errors: 0, scheduled_jobs: {{}}, running_jobs: {{}}, last_finished_at: \"None\" }}, timeout: Some(1s) }}"));

        assert_eq!(
            format!("{}", CronSchedule::try_from("1 2 3 4 ?").unwrap()),
            String::from("0 1 2 3 4 ? *")
        );
    }
}
