use crate::AsyncJobBoxed;
use std::fmt::Display;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, Clone, PartialOrd, Ord, Hash)]
pub struct JobId {
    id: Uuid,
}

impl JobId {
    pub fn new() -> Self {
        Self { id: Uuid::new_v4() }
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Uuid> for JobId {
    fn from(value: Uuid) -> Self {
        Self { id: value }
    }
}

impl From<&Uuid> for JobId {
    fn from(value: &Uuid) -> Self {
        Self { id: *value }
    }
}

impl From<JobId> for Uuid {
    fn from(value: JobId) -> Self {
        value.id
    }
}

impl From<&JobId> for Uuid {
    fn from(value: &JobId) -> Self {
        value.id
    }
}

impl Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

pub struct Job {
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
pub enum JobStatus {
    #[default]
    Pending,
    Starting,
    Running,
    Completed,
    Cancelled,
}

impl JobStatus {
    pub fn finished(&self) -> bool {
        *self == JobStatus::Completed || *self == JobStatus::Cancelled
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn job_status_finished() {
        assert!(!JobStatus::Pending.finished());
        assert!(!JobStatus::Starting.finished());
        assert!(!JobStatus::Running.finished());
        assert!(JobStatus::Completed.finished());
        assert!(JobStatus::Cancelled.finished());
    }
}
