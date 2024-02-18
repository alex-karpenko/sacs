use crate::{
    job::{Job, JobId, JobStatus},
    worker::{AsyncWorker, Worker},
    ControlChannel, Error, Result, WorkerType,
};
use std::collections::{HashMap, VecDeque};
use tokio::{select, sync::RwLock};
use tracing::{debug, warn};

/// Default maximin number of jobs to run on the single worker
pub(crate) const DEFAULT_MAX_PARALLEL_JOBS: usize = 16;
/// Default size of control channels
const EXECUTOR_CONTROL_CHANNEL_SIZE: usize = 1024;

pub(crate) trait JobExecutor {
    async fn enqueue(&self, job: Job) -> Result<JobId>;
    async fn cancel(&self, id: &JobId) -> Result<()>;
    async fn status(&self, id: &JobId) -> Result<JobStatus>;
    async fn work(&self) -> Result<JobId>;
    async fn shutdown(self, wait: bool) -> Result<()>;
}

pub(crate) struct Executor {
    max_parallel_jobs: usize,
    control_channel: ControlChannel<ChangeExecutorStateEvent>,
    worker: Worker,
    jobs: RwLock<ExecutorJobsState>,
}

#[derive(Default)]
struct ExecutorJobsState {
    pending: VecDeque<Job>,
    status: HashMap<JobId, JobStatus>,
}

impl ExecutorJobsState {
    pub fn clear(&mut self) {
        self.pending.clear();
        self.status.clear();
    }
}

impl Executor {
    pub fn new(worker_type: WorkerType, max_parallel_jobs: usize) -> Self {
        debug!("new: type={:?}, max jobs={max_parallel_jobs}", worker_type);
        let executor_channel = ControlChannel::new(EXECUTOR_CONTROL_CHANNEL_SIZE);
        let to_executor = executor_channel.sender();

        Self {
            max_parallel_jobs,
            control_channel: executor_channel,
            worker: Worker::new(worker_type, to_executor),
            jobs: RwLock::new(ExecutorJobsState::default()),
        }
    }

    async fn requeue_jobs(&self) -> Result<()> {
        let mut jobs = self.jobs.write().await;
        let running = jobs
            .status
            .iter()
            .filter(|(_id, status)| {
                **status == JobStatus::Running || **status == JobStatus::Starting
            })
            .count();

        if running < self.max_parallel_jobs {
            let free_slots = self.max_parallel_jobs - running;
            debug!("requeue_jobs: {} new jobs can be started", free_slots);
            for _ in 0..free_slots {
                if let Some(job) = jobs.pending.pop_front() {
                    let id = job.id();
                    jobs.status.insert(id, JobStatus::Starting);
                    self.worker.start(job).await?;
                } else {
                    break;
                }
            }
        }

        Ok(())
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new(WorkerType::CurrentThread, DEFAULT_MAX_PARALLEL_JOBS)
    }
}

impl JobExecutor for Executor {
    async fn work(&self) -> Result<JobId> {
        select! {
            event = self.control_channel.receive() => {
                debug!("work: control event={:?}", event);
                if let Some(event) = event {
                    match event {
                        ChangeExecutorStateEvent::JobStarted(id) => {
                            self.jobs.write().await.status.insert(id.clone(), JobStatus::Running);
                            Ok(id)
                        },
                        ChangeExecutorStateEvent::JobCancelled(id) => {
                            {
                                self.jobs.write().await.status.insert(id.clone(), JobStatus::Cancelled);
                            }
                            self.requeue_jobs().await?;
                            Ok(id)
                        },
                        ChangeExecutorStateEvent::JobCompleted(id) => {
                            {
                                self.jobs.write().await.status.insert(id.clone(), JobStatus::Completed);
                            }
                            self.requeue_jobs().await?;
                            Ok(id)
                        },
                    }
                } else {
                    warn!("work: empty control channel");
                    Err(Error::ReceivingChangeStateEvent)
                }
            }
        }
    }

    async fn enqueue(&self, job: Job) -> Result<JobId> {
        debug!("enqueue: job={:?}", job);
        let id = job.id();
        {
            let mut jobs = self.jobs.write().await;
            let id = job.id();

            jobs.pending.push_back(job);
            jobs.status.insert(id, JobStatus::Pending);
        }
        self.requeue_jobs().await?;

        Ok(id)
    }

    async fn cancel(&self, id: &JobId) -> Result<()> {
        debug!("cancel: job id={:?}", id);
        self.worker.cancel(id).await?;
        Ok(())
    }

    async fn status(&self, id: &JobId) -> Result<JobStatus> {
        debug!("status: id={:?}", id);
        let mut jobs = self.jobs.write().await;
        if let Some(status) = jobs.status.get(id) {
            let response = Ok(status.clone());
            if status.finished() {
                debug!("status: remove finished job status, id={:?}", id);
                jobs.status.remove(id);
            }
            response
        } else {
            Err(Error::IncorrectJobId(id.clone()))
        }
    }

    async fn shutdown(self, wait: bool) -> Result<()> {
        debug!("shutdown: requested");
        let result = self.worker.shutdown(wait).await;
        self.jobs.write().await.clear();

        result
    }
}

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum ChangeExecutorStateEvent {
    JobStarted(JobId),
    JobCancelled(JobId),
    JobCompleted(JobId),
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::task::{Task, TaskSchedule};
    use std::{sync::Arc, time::Duration};
    use tokio::join;

    #[tokio::test]
    async fn current_runtime_single_worker() {
        let executor = Executor::new(WorkerType::CurrentRuntime, 1);
        let completed = Arc::new(RwLock::<bool>::new(false));

        let task_0 = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
            })
        })
        .unwrap();
        let job_0 = task_0.job.clone();
        let job_id_0 = JobId::new();
        let job_0 = Job::new(job_id_0.clone(), job_0);

        let task_1 = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
            })
        })
        .unwrap();
        let job_1 = task_1.job.clone();
        let job_id_1 = JobId::new();
        let job_1 = Job::new(job_id_1.clone(), job_1);

        executor.enqueue(job_0).await.unwrap();
        executor.enqueue(job_1).await.unwrap();

        let _ = join!(
            // Start both jobs
            // 1st - Starting, 2nd - Pending (because single job worker)
            async {
                assert_eq!(
                    executor.status(&job_id_0).await.unwrap(),
                    JobStatus::Starting
                );
            },
            async {
                assert_eq!(
                    executor.status(&job_id_1).await.unwrap(),
                    JobStatus::Pending
                );
            },
            // wait 200ms
            // 1st - Running, 2nd - Pending
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                assert_eq!(
                    executor.status(&job_id_0).await.unwrap(),
                    JobStatus::Running
                );
            },
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                assert_eq!(
                    executor.status(&job_id_1).await.unwrap(),
                    JobStatus::Pending
                );
            },
            // wait 400ms and cancel 1st
            // 1st - Running, 2nd - Pending
            async {
                tokio::time::sleep(Duration::from_millis(400)).await;
                executor.cancel(&job_id_0).await.unwrap();
                assert_eq!(
                    executor.status(&job_id_0).await.unwrap(),
                    JobStatus::Running
                );
            },
            async {
                tokio::time::sleep(Duration::from_millis(400)).await;
                assert_eq!(
                    executor.status(&job_id_1).await.unwrap(),
                    JobStatus::Pending
                );
            },
            // wait 600ms
            // 1st - Cancelled, 2nd - Running
            async {
                tokio::time::sleep(Duration::from_millis(600)).await;
                assert_eq!(
                    executor.status(&job_id_0).await.unwrap(),
                    JobStatus::Cancelled
                );
            },
            async {
                tokio::time::sleep(Duration::from_millis(600)).await;
                assert_eq!(
                    executor.status(&job_id_1).await.unwrap(),
                    JobStatus::Running
                );
            },
            // wait 800ms
            // 1st - no status, 2nd - Running
            async {
                tokio::time::sleep(Duration::from_millis(800)).await;
                assert!(matches!(
                    executor.status(&job_id_0).await,
                    Err(Error::IncorrectJobId(id)) if id == job_id_0
                ));
            },
            async {
                tokio::time::sleep(Duration::from_millis(800)).await;
                assert_eq!(
                    executor.status(&job_id_1).await.unwrap(),
                    JobStatus::Running
                );
            },
            // wait 1500ms
            // 2nd - Completed
            async {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                assert_eq!(
                    executor.status(&job_id_1).await.unwrap(),
                    JobStatus::Completed
                );
            },
            // wait 1600ms
            // 2nd - no status
            async {
                tokio::time::sleep(Duration::from_millis(1600)).await;
                assert!(matches!(
                    executor.status(&job_id_1).await,
                    Err(Error::IncorrectJobId(id)) if id == job_id_1
                ));
                let mut completed = completed.write().await;
                *completed = true;
            },
            // work loop
            async {
                loop {
                    if *(completed.read().await) {
                        break;
                    };
                    select! {
                        _ = executor.work() => {},
                        _ = tokio::time::sleep(Duration::from_millis(1800)) => {
                            assert!(*(completed.read().await), "safety timer 1 exceeded");
                        }
                    }
                }
            },
            // safety timer
            async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let completed = completed.read().await;
                assert!(*completed, "safety timer 2 exceeded");
            }
        );
    }
}
