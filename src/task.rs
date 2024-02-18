use crate::{event::EventId, job::JobId, AsyncJobBoxed, Error, Result};
use chrono::Utc;
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

pub struct Task {
    pub(crate) id: TaskId,
    pub(crate) job: AsyncJobBoxed,
    pub(crate) schedule: TaskSchedule,
    pub(crate) state: TaskState,
}

impl Task {
    pub fn new<T>(schedule: TaskSchedule, job: T) -> Result<Self>
    where
        T: 'static,
        T: FnMut(JobId) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
    {
        Ok(Self {
            id: TaskId::default(),
            job: Arc::new(RwLock::new(Box::new(job))),
            schedule,
            state: TaskState::default(),
        })
    }

    pub fn id(&self) -> TaskId {
        self.id.clone()
    }

    pub fn schedule(&self) -> TaskSchedule {
        self.schedule.clone()
    }

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

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Hash)]
pub struct TaskId {
    pub(crate) id: Uuid,
}

impl Default for TaskId {
    fn default() -> Self {
        Self { id: Uuid::new_v4() }
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
    RepeatByInterval(Duration),
    RepeatByIntervalDelayed(Duration),
    RepeatByCron(CronSchedule),
}

impl TaskSchedule {
    pub fn initial_run_time(&self) -> SystemTime {
        match self {
            TaskSchedule::Once => SystemTime::now(),
            TaskSchedule::OnceDelayed(delay) => SystemTime::now().checked_add(*delay).unwrap(),
            TaskSchedule::RepeatByInterval(_interval) => SystemTime::now(),
            TaskSchedule::RepeatByIntervalDelayed(interval) => {
                SystemTime::now().checked_add(*interval).unwrap()
            }
            TaskSchedule::RepeatByCron(schedule) => schedule.upcoming(),
        }
    }

    pub fn after_start_run_time(&self) -> Option<SystemTime> {
        match self {
            TaskSchedule::RepeatByCron(schedule) => Some(schedule.upcoming()),
            TaskSchedule::Once => None,
            TaskSchedule::OnceDelayed(_) => None,
            TaskSchedule::RepeatByInterval(_) => None,
            TaskSchedule::RepeatByIntervalDelayed(_) => None,
        }
    }
    pub fn after_finish_run_time(&self) -> Option<SystemTime> {
        match self {
            TaskSchedule::RepeatByInterval(interval) => {
                Some(SystemTime::now().checked_add(*interval).unwrap())
            }
            TaskSchedule::RepeatByIntervalDelayed(interval) => {
                Some(SystemTime::now().checked_add(*interval).unwrap())
            }
            TaskSchedule::Once => None,
            TaskSchedule::OnceDelayed(_) => None,
            TaskSchedule::RepeatByCron(_) => None,
        }
    }
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

#[derive(Debug, Default)]
pub(crate) struct TaskState {
    waiting: usize,
    scheduled: usize,
    running: usize,
    completed: usize,
    cancelled: usize,
    scheduled_jobs: BTreeSet<JobId>,
    running_jobs: BTreeSet<JobId>,
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
        let next: SystemTime = self.schedule.upcoming(Utc).take(1).next().unwrap().into();
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

        state.enqueued();
        assert_eq!(state.status(), TaskStatus::Waiting);
        assert!(!state.finished());

        state.enqueued();
        assert_eq!(state.status(), TaskStatus::Waiting);
        assert!(!state.finished());

        state.scheduled(job1.clone());
        assert_eq!(state.status(), TaskStatus::Scheduled);
        assert!(!state.finished());

        state.started(job1.clone());
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.finished());

        state.scheduled(job2.clone());
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.finished());

        state.cancelled(&job2);
        assert_eq!(state.status(), TaskStatus::Running);
        assert!(!state.finished());

        state.completed(&job1);
        assert_eq!(state.status(), TaskStatus::Finished);
        assert!(state.finished());
    }
}
