//! Contains primitive [`JobId`] which uniquely identifies executing `Task` instance.
use crate::{task::TaskId, AsyncJobBoxed};
use std::{
    fmt::{Debug, Display},
    time::Duration,
};
use uuid::Uuid;

/// `JobId` uniquely identifies running instance of `Task`.
///
/// You don't need to construct this object manually:
/// - `task_id` is provided from `Scheduler` during planned starting of the `Task` instance,
/// - `job_id` is created automatically `Uuid`.
///
/// Executor creates `JobId` for each running job and provides it to job's closure as a parameter (`id` in the example below).
///
/// String representation of the `JobId` is `"{task_id}/{id}"`.
///
/// Common usage of `JobId` inside task closure is for logging.
///
/// # Examples
///
/// ```rust
/// use sacs::task::{Task, TaskSchedule};
/// use std::time::Duration;
///
/// let task = Task::new(TaskSchedule::Once, |id| {
///     Box::pin(async move {
///         println!("Starting job, TaskId={}, JobId={}.", id.task_id, id.id);
///         // Actual async workload here
///         tokio::time::sleep(Duration::from_secs(1)).await;
///         // ...
///         println!("Job {id} finished.");
///         })
///     });
/// ```
#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub struct JobId {
    /// ID of the `Task` which owns this Job, is provided from `Scheduler` during scheduled starting of the `Task` instance.
    pub task_id: TaskId,
    /// Unique ID of the running Job within particular `Task`.
    pub id: Uuid,
}

impl JobId {
    pub(crate) fn new(task_id: impl Into<TaskId>) -> Self {
        Self {
            id: Uuid::new_v4(),
            task_id: task_id.into(),
        }
    }
}

impl From<TaskId> for JobId {
    fn from(value: TaskId) -> Self {
        Self {
            id: Uuid::new_v4(),
            task_id: value,
        }
    }
}

impl From<&TaskId> for JobId {
    fn from(value: &TaskId) -> Self {
        Self {
            id: Uuid::new_v4(),
            task_id: value.to_owned(),
        }
    }
}

impl From<JobId> for String {
    fn from(value: JobId) -> Self {
        format!("{}/{}", value.task_id, value.id)
    }
}

impl Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.task_id, self.id)
    }
}

pub(crate) struct Job {
    id: JobId,
    job: AsyncJobBoxed,
    timeout: Option<Duration>,
}

impl Job {
    pub(crate) fn new(id: JobId, job: AsyncJobBoxed, timeout: Option<Duration>) -> Self {
        Self { id, job, timeout }
    }

    pub(crate) fn id(&self) -> JobId {
        self.id.clone()
    }

    pub(crate) fn job(&self) -> AsyncJobBoxed {
        self.job.clone()
    }

    pub(crate) fn timeout(&self) -> Option<Duration> {
        self.timeout
    }
}

impl Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("id", &self.id)
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) enum JobState {
    #[default]
    Pending,
    Starting,
    Running,
    Completed,
    Canceled,
    Timeout,
    Error,
}

impl JobState {
    pub fn finished(&self) -> bool {
        *self == JobState::Completed
            || *self == JobState::Canceled
            || *self == JobState::Timeout
            || *self == JobState::Error
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::task::{Task, TaskSchedule};

    #[test]
    fn job_state_finished() {
        assert!(!JobState::Pending.finished());
        assert!(!JobState::Starting.finished());
        assert!(!JobState::Running.finished());
        assert!(JobState::Completed.finished());
        assert!(JobState::Canceled.finished());
        assert!(JobState::Timeout.finished());
        assert!(JobState::Error.finished());
    }

    #[test]
    fn type_convertors() {
        let task_id = TaskId::from("TASK_ID");
        let job_id = JobId::from(task_id.clone());

        assert_eq!(JobId::from(task_id.clone()).task_id, task_id);
        assert_eq!(JobId::from(&task_id).task_id, task_id);

        assert_eq!(
            format!("{job_id}"),
            format!("{}/{}", job_id.task_id, job_id.id)
        );
        assert_eq!(
            String::from(job_id.clone()),
            format!("{}/{}", job_id.task_id, job_id.id)
        );
    }

    #[test]
    fn debug_formatter() {
        let task1 = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {})).with_id("TEST");
        let task2 = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}))
            .with_id("TEST_WITH_TIMEOUT")
            .with_timeout(Duration::from_secs(1));

        let job1 = Job::new(JobId::new(task1.id()), task1.job, None);
        let job2 = Job::new(
            JobId::new(task2.id()),
            task2.job,
            Some(Duration::from_secs(1)),
        );

        assert_eq!(format!("{job1:?}"), format!("Job {{ id: JobId {{ task_id: TaskId {{ id: \"TEST\" }}, id: {} }}, timeout: None }}",job1.id().id));
        assert_eq!(format!("{job2:?}"), format!("Job {{ id: JobId {{ task_id: TaskId {{ id: \"TEST_WITH_TIMEOUT\" }}, id: {} }}, timeout: Some(1s) }}", job2.id().id));
    }
}
