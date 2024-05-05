//! Contains primitive [`JobId`] which uniquely identifies executing `Task` instance.
use crate::{task::TaskId, AsyncJobBoxed};
use std::fmt::Display;
use uuid::Uuid;

/// `JobId` uniquely identifies running instance of `Task`.
///
/// You don't need to construct this object manually:
/// - `task_id` is provided from `Scheduler` during planned starting of the `Task` instance,
/// - `job_id` is a created automatically `Uuid`.
///
/// Executor creates `JobId` for each running job and provides it to job's closure as a parameter (`id` in the example below).
///
/// String representation of the `JobId` is `"{task_id}/{job_id}"`.
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
///         println!("Starting job, TaskId={}, JobId={}.", id.task_id, id.job_id);
///         // Actual async workload here
///         tokio::time::sleep(Duration::from_secs(1)).await;
///         // ...
///         println!("Job {id} finished.");
///         })
///     });
/// ```
#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Hash)]
pub struct JobId {
    /// ID of the `Task` which owns this Job, is provided from `Scheduler` during scheduled starting of the `Task` instance.
    pub task_id: TaskId,
    /// Unique ID of the running Job within particular `Task`.
    pub job_id: Uuid,
}

impl JobId {
    pub(crate) fn new(task_id: impl Into<TaskId>) -> Self {
        Self {
            job_id: Uuid::new_v4(),
            task_id: task_id.into(),
        }
    }
}

impl From<TaskId> for JobId {
    fn from(value: TaskId) -> Self {
        Self {
            job_id: Uuid::new_v4(),
            task_id: value,
        }
    }
}

impl From<&TaskId> for JobId {
    fn from(value: &TaskId) -> Self {
        Self {
            job_id: Uuid::new_v4(),
            task_id: value.to_owned(),
        }
    }
}

impl From<JobId> for String {
    fn from(value: JobId) -> Self {
        format!("{}/{}", value.task_id, value.job_id)
    }
}

impl Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.task_id, self.job_id)
    }
}

pub(crate) struct Job {
    id: JobId,
    job: AsyncJobBoxed,
}

impl Job {
    pub fn new(id: JobId, job: AsyncJobBoxed) -> Self {
        Self { id, job }
    }

    pub fn id(&self) -> JobId {
        self.id.clone()
    }

    pub fn job(&self) -> AsyncJobBoxed {
        self.job.clone()
    }
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job").field("id", &self.id).finish()
    }
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) enum JobState {
    #[default]
    Pending,
    Starting,
    Running,
    Completed,
    Cancelled,
}

impl JobState {
    pub fn finished(&self) -> bool {
        *self == JobState::Completed || *self == JobState::Cancelled
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn job_state_finished() {
        assert!(!JobState::Pending.finished());
        assert!(!JobState::Starting.finished());
        assert!(!JobState::Running.finished());
        assert!(JobState::Completed.finished());
        assert!(JobState::Cancelled.finished());
    }

    #[test]
    fn type_convertors() {
        let task_id = TaskId::from("TASK_ID");
        let job_id = JobId::from(task_id.clone());

        assert_eq!(JobId::from(task_id.clone()).task_id, task_id);
        assert_eq!(JobId::from(&task_id).task_id, task_id);

        assert_eq!(
            format!("{job_id}"),
            format!("{}/{}", task_id, job_id.job_id)
        );
        assert_eq!(job_id.to_string(), format!("{}/{}", task_id, job_id.job_id));
    }
}
