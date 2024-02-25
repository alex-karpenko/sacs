//! `Task` object represents single job with schedule. Use it to create workload of different types and post it to [`Scheduler`].
//!
//! Module contains everything related to [`Task`], it's [`TaskSchedule`] and [`state`](TaskStatus).
//!
use crate::{event::EventId, job::JobId, AsyncJobBoxed, Error};
use chrono::{DateTime, Local};
use cron::Schedule;
use futures::Future;
use std::{
    collections::BTreeSet,
    fmt::Display,
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use tracing::debug;
use uuid::Uuid;

/// `Task` represents single job with it's schedule, attributes and state.
pub struct Task {
    pub(crate) id: TaskId,
    pub(crate) job: AsyncJobBoxed,
    pub(crate) schedule: TaskSchedule,
    pub(crate) state: TaskState,
}

impl Task {
    /// Creates new Task with specified schedule and job function.
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
        }
    }

    /// Returns task's [`TaskId`], it can be used to [`drop`](Scheduler::drop()) task or to get it's [`status`](Scheduler::status()).
    pub fn id(&self) -> TaskId {
        self.id.clone()
    }

    /// Returns [`TaskSchedule`] associated with the task.
    pub fn schedule(&self) -> TaskSchedule {
        self.schedule.clone()
    }

    /// Returns task's status.
    pub fn status(&self) -> TaskStatus {
        self.state.status()
    }
}

impl std::fmt::Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("id", &self.id)
            .field("schedule", &self.schedule)
            .field("state", &self.state)
            .finish()
    }
}

/// Unique identifier of [`Task`] which can be used to address task in [`Scheduler`].
#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Hash)]
pub struct TaskId {
    pub(crate) id: Uuid,
}

impl TaskId {
    /// Constructs new unique `TaskId`.
    pub fn new() -> Self {
        Self { id: Uuid::new_v4() }
    }
}
impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Uuid> for TaskId {
    fn from(value: Uuid) -> Self {
        Self { id: value }
    }
}

impl From<&Uuid> for TaskId {
    fn from(value: &Uuid) -> Self {
        Self { id: *value }
    }
}

impl From<TaskId> for Uuid {
    fn from(value: TaskId) -> Self {
        value.id
    }
}

impl From<&TaskId> for Uuid {
    fn from(value: &TaskId) -> Self {
        value.id
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

#[derive(Clone, Debug)]
pub enum TaskSchedule {
    Once,
    OnceDelayed(Duration),
    Interval(Duration),
    IntervalDelayed(Duration),
    Cron(CronSchedule, CronOpts),
}

impl TaskSchedule {
    pub fn initial_run_time(&self) -> SystemTime {
        match self {
            TaskSchedule::Once => SystemTime::now(),
            TaskSchedule::OnceDelayed(delay) => SystemTime::now().checked_add(*delay).unwrap(),
            TaskSchedule::Interval(_interval) => SystemTime::now(),
            TaskSchedule::IntervalDelayed(interval) => {
                SystemTime::now().checked_add(*interval).unwrap()
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

    pub fn after_start_run_time(&self) -> Option<SystemTime> {
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
            TaskSchedule::IntervalDelayed(_) => None,
        }
    }
    pub fn after_finish_run_time(&self) -> Option<SystemTime> {
        match self {
            TaskSchedule::Interval(interval) => {
                Some(SystemTime::now().checked_add(*interval).unwrap())
            }
            TaskSchedule::IntervalDelayed(interval) => {
                Some(SystemTime::now().checked_add(*interval).unwrap())
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

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CronOpts {
    pub at_start: bool,
    pub concurrent: bool,
}

#[derive(Default, Clone, PartialEq, Debug)]
pub enum TaskStatus {
    #[default]
    New,
    Waiting,
    Scheduled,
    Running,
    Finished,
}

#[derive(Default)]
pub(crate) struct TaskState {
    waiting: usize,
    scheduled: usize,
    running: usize,
    completed: usize,
    cancelled: usize,
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
            .field("waiting", &self.waiting)
            .field("scheduled", &self.scheduled)
            .field("running", &self.running)
            .field("completed", &self.completed)
            .field("cancelled", &self.cancelled)
            .field("scheduled_jobs", &self.scheduled_jobs)
            .field("running_jobs", &self.running_jobs)
            .field("last_finished_at", &last_finished_at)
            .finish()
    }
}

impl TaskState {
    pub fn status(&self) -> TaskStatus {
        if self.running > 0 {
            TaskStatus::Running
        } else if self.scheduled > 0 {
            TaskStatus::Scheduled
        } else if self.waiting > 0 {
            TaskStatus::Waiting
        } else if (self.completed + self.cancelled) > 0 {
            TaskStatus::Finished
        } else {
            TaskStatus::New
        }
    }

    pub fn enqueued(&mut self) -> &Self {
        self.waiting += 1;
        debug!("enqueue: status={:?}, {self:?}", self.status());
        self
    }

    pub fn scheduled(&mut self, id: JobId) -> &Self {
        self.waiting -= 1;
        self.scheduled += 1;
        self.scheduled_jobs.insert(id);
        debug!("scheduled: status={:?}, {self:?}", self.status());
        self
    }

    pub fn started(&mut self, id: JobId) -> &Self {
        self.scheduled -= 1;
        self.running += 1;
        self.scheduled_jobs.remove(&id);
        self.running_jobs.insert(id);
        debug!("started: status={:?}, {self:?}", self.status());
        self
    }

    pub fn completed(&mut self, id: &JobId) -> &Self {
        self.running -= 1;
        self.completed += 1;
        self.running_jobs.remove(id);
        self.last_finished_at = Some(SystemTime::now());
        debug!("completed: status={:?}, {self:?}", self.status());
        self
    }

    pub fn cancelled(&mut self, id: &JobId) -> &Self {
        self.cancelled += 1;
        if self.running_jobs.remove(id) {
            self.running -= 1;
        } else {
            self.scheduled_jobs.remove(id);
            self.scheduled -= 1;
        }
        self.last_finished_at = Some(SystemTime::now());
        debug!("cancelled: status={:?}, {self:?}", self.status());
        self
    }

    pub fn finished(&self) -> bool {
        self.waiting == 0
            && self.scheduled == 0
            && self.running == 0
            && (self.completed + self.cancelled) > 0
            && self.scheduled_jobs.is_empty()
            && self.running_jobs.is_empty()
    }

    pub(crate) fn last_finished_at(&self) -> Option<SystemTime> {
        self.last_finished_at
    }

    pub fn jobs(&self) -> BTreeSet<JobId> {
        let mut jobs = BTreeSet::new();
        jobs.extend(self.scheduled_jobs.clone());
        jobs.extend(self.running_jobs.clone());
        jobs
    }
}

#[derive(Clone, Debug)]
pub struct CronSchedule {
    schedule: Schedule,
}

impl Display for CronSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.schedule)
    }
}

impl CronSchedule {
    fn upcoming(&self) -> SystemTime {
        let next: SystemTime = self.schedule.upcoming(Local).take(1).next().unwrap().into();
        next
    }
}

impl TryFrom<String> for CronSchedule {
    type Error = Error;

    fn try_from(value: String) -> std::prelude::v1::Result<Self, Self::Error> {
        // Sanitize it
        let value = value
            .split(' ')
            .map(String::from)
            .filter(|s| !s.is_empty())
            .collect::<Vec<String>>()
            .join(" ");
        // Add leading seconds if absent
        let value = if value.split(' ').count() == 5 {
            String::from("0 ") + &value
        } else {
            value
        };

        // Try to convert into schedule
        let schedule = Schedule::from_str(value.as_str())?;
        Ok(Self { schedule })
    }
}

impl TryFrom<&str> for CronSchedule {
    type Error = Error;

    fn try_from(value: &str) -> std::prelude::v1::Result<Self, Self::Error> {
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
    fn task_state_transition() {
        let job1 = JobId::new();
        let job2 = JobId::new();

        let mut state = TaskState::default();
        assert_eq!(state.status(), TaskStatus::New);
        assert!(!state.finished());
        assert!(state.last_finished_at().is_none());

        state.enqueued();
        assert_eq!(state.status(), TaskStatus::Waiting);
        assert!(!state.finished());
        assert!(state.last_finished_at().is_none());

        state.enqueued();
        assert_eq!(state.status(), TaskStatus::Waiting);
        assert!(!state.finished());
        assert!(state.last_finished_at().is_none());

        state.scheduled(job1.clone());
        assert_eq!(state.status(), TaskStatus::Scheduled);
        assert!(!state.finished());
        assert!(state.last_finished_at().is_none());

        state.started(job1.clone());
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.finished());
        assert!(state.last_finished_at().is_none());

        state.scheduled(job2.clone());
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.finished());
        assert!(state.last_finished_at().is_none());

        state.cancelled(&job2);
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.finished());
        assert!(state.last_finished_at().is_some());

        state.completed(&job1);
        assert_eq!(state.status(), TaskStatus::Finished);
        assert!(state.finished());
        assert!(state.last_finished_at().is_some());
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
}
