use crate::{
    job::{Job, JobId, JobState},
    scheduler::{ShutdownOpts, WorkerParallelism, WorkerType},
    worker::{AsyncWorker, Worker},
    ControlChannel, Error, Result,
};
use std::collections::{HashMap, VecDeque};
use tokio::{select, sync::RwLock};
use tracing::{debug, instrument, warn};

/// Default size of control channels
const EXECUTOR_CONTROL_CHANNEL_SIZE: usize = 1024;

pub(crate) trait JobExecutor {
    async fn enqueue(&self, job: Job) -> Result<JobId>;
    async fn cancel(&self, id: &JobId) -> Result<()>;
    async fn state(&self, id: &JobId) -> Result<JobState>;
    async fn work(&self) -> Result<JobId>;
    async fn shutdown(self, opts: ShutdownOpts) -> Result<()>;
}

pub(crate) struct Executor {
    parallelism: WorkerParallelism,
    control_channel: ControlChannel<ChangeExecutorStateEvent>,
    worker: Worker,
    jobs: RwLock<ExecutorJobsState>,
}

#[derive(Default)]
struct ExecutorJobsState {
    pending: VecDeque<Job>,
    state: HashMap<JobId, JobState>,
}

impl ExecutorJobsState {
    pub fn clear(&mut self) {
        self.pending.clear();
        self.state.clear();
    }
}

impl Executor {
    pub fn new(worker_type: WorkerType, parallelism: WorkerParallelism) -> Self {
        debug!(?worker_type, ?parallelism, "construct new executor");
        let executor_channel = ControlChannel::new(EXECUTOR_CONTROL_CHANNEL_SIZE);
        let to_executor = executor_channel.sender();

        Self {
            parallelism,
            control_channel: executor_channel,
            worker: Worker::new(worker_type, to_executor),
            jobs: RwLock::new(ExecutorJobsState::default()),
        }
    }

    #[instrument("process pending jobs", skip_all)]
    async fn requeue_jobs(&self) -> Result<()> {
        let mut jobs = self.jobs.write().await;
        // Nothing to execute
        if jobs.pending.is_empty() {
            debug!("jobs queue is empty");
            return Ok(());
        }

        let jobs_to_run = match self.parallelism {
            WorkerParallelism::Unlimited => jobs.pending.len(),
            WorkerParallelism::Limited(limit) => {
                let running = jobs
                    .state
                    .iter()
                    .filter(|(_id, state)| {
                        **state == JobState::Running || **state == JobState::Starting
                    })
                    .count();

                if running < limit {
                    limit - running
                } else {
                    0
                }
            }
        };

        debug!(count = jobs_to_run, "dispatch pending jobs");
        for _ in 0..jobs_to_run {
            if let Some(job) = jobs.pending.pop_front() {
                let id = job.id();
                debug!(job_id = %id, "start job");
                jobs.state.insert(id, JobState::Starting);
                self.worker.start(job).await?;
            } else {
                break;
            }
        }

        Ok(())
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new(WorkerType::CurrentThread, WorkerParallelism::default())
    }
}

impl JobExecutor for Executor {
    #[instrument("waiting for executor event", skip_all)]
    async fn work(&self) -> Result<JobId> {
        select! {
            event = self.control_channel.receive() => {
                debug!(?event, "event received");
                if let Some(event) = event {
                    match event {
                        ChangeExecutorStateEvent::JobStarted(id) => {
                            self.jobs.write().await.state.insert(id.clone(), JobState::Running);
                            Ok(id)
                        },
                        ChangeExecutorStateEvent::JobCancelled(id) => {
                            {
                                self.jobs.write().await.state.insert(id.clone(), JobState::Cancelled);
                            }
                            self.requeue_jobs().await?;
                            Ok(id)
                        },
                        ChangeExecutorStateEvent::JobTimeouted(id) => {
                            {
                                self.jobs.write().await.state.insert(id.clone(), JobState::Timeouted);
                            }
                            self.requeue_jobs().await?;
                            Ok(id)
                        },
                        ChangeExecutorStateEvent::JobCompleted(id) => {
                            {
                                self.jobs.write().await.state.insert(id.clone(), JobState::Completed);
                            }
                            self.requeue_jobs().await?;
                            Ok(id)
                        },
                    }
                } else {
                    warn!("control channel error, exiting");
                    Err(Error::ReceivingChangeStateEvent)
                }
            }
        }
    }

    async fn enqueue(&self, job: Job) -> Result<JobId> {
        debug!(job_id = %job.id(), "enqueue job");
        let id = job.id();
        {
            let mut jobs = self.jobs.write().await;
            let id = job.id();

            jobs.pending.push_back(job);
            jobs.state.insert(id, JobState::Pending);
        }
        self.requeue_jobs().await?;

        Ok(id)
    }

    async fn cancel(&self, id: &JobId) -> Result<()> {
        debug!(job_id = %id, "cancel job");
        self.worker.cancel(id).await?;
        Ok(())
    }

    async fn state(&self, id: &JobId) -> Result<JobState> {
        debug!(job_id = %id, "job state requested");
        let mut jobs = self.jobs.write().await;
        if let Some(state) = jobs.state.get(id) {
            let response = Ok(state.clone());
            if state.finished() {
                debug!(job_id = %id, "remove finished job state");
                jobs.state.remove(id);
            }
            response
        } else {
            Err(Error::IncorrectJobId(id.clone()))
        }
    }

    async fn shutdown(self, opts: ShutdownOpts) -> Result<()> {
        debug!(?opts, "shutdown requested");
        let result = self.worker.shutdown(opts).await;
        self.jobs.write().await.clear();

        result
    }
}

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum ChangeExecutorStateEvent {
    JobStarted(JobId),
    JobCancelled(JobId),
    JobTimeouted(JobId),
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
        let executor = Executor::new(WorkerType::CurrentRuntime, WorkerParallelism::Limited(1));
        let completed = Arc::new(RwLock::<bool>::new(false));

        let task_0 = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
            })
        });
        let job_0 = task_0.job.clone();
        let job_id_0 = JobId::new("task 0 id");
        let job_0 = Job::new(job_id_0.clone(), job_0, None);

        let task_1 = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
            })
        });
        let job_1 = task_1.job.clone();
        let job_id_1 = JobId::new("task 1 id");
        let job_1 = Job::new(job_id_1.clone(), job_1, None);

        executor.enqueue(job_0).await.unwrap();
        executor.enqueue(job_1).await.unwrap();

        let _ = join!(
            // Start both jobs
            // 1st - Starting, 2nd - Pending (because single job worker)
            async {
                assert_eq!(executor.state(&job_id_0).await.unwrap(), JobState::Starting);
            },
            async {
                assert_eq!(executor.state(&job_id_1).await.unwrap(), JobState::Pending);
            },
            // wait 200ms
            // 1st - Running, 2nd - Pending
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                assert_eq!(executor.state(&job_id_0).await.unwrap(), JobState::Running);
            },
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                assert_eq!(executor.state(&job_id_1).await.unwrap(), JobState::Pending);
            },
            // wait 400ms and cancel 1st,
            // 1st - Running, 2nd - Pending
            async {
                tokio::time::sleep(Duration::from_millis(400)).await;
                executor.cancel(&job_id_0).await.unwrap();
                assert_eq!(executor.state(&job_id_0).await.unwrap(), JobState::Running);
            },
            async {
                tokio::time::sleep(Duration::from_millis(400)).await;
                assert_eq!(executor.state(&job_id_1).await.unwrap(), JobState::Pending);
            },
            // wait 600ms
            // 1st - Canceled, 2nd - Running
            async {
                tokio::time::sleep(Duration::from_millis(600)).await;
                assert_eq!(
                    executor.state(&job_id_0).await.unwrap(),
                    JobState::Cancelled
                );
            },
            async {
                tokio::time::sleep(Duration::from_millis(600)).await;
                assert_eq!(executor.state(&job_id_1).await.unwrap(), JobState::Running);
            },
            // wait 800ms
            // 1st - no state, 2nd - Running
            async {
                tokio::time::sleep(Duration::from_millis(800)).await;
                assert!(matches!(
                    executor.state(&job_id_0).await,
                    Err(Error::IncorrectJobId(id)) if id == job_id_0
                ));
            },
            async {
                tokio::time::sleep(Duration::from_millis(800)).await;
                assert_eq!(executor.state(&job_id_1).await.unwrap(), JobState::Running);
            },
            // wait 1500ms
            // 2nd - Completed
            async {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                assert_eq!(
                    executor.state(&job_id_1).await.unwrap(),
                    JobState::Completed
                );
            },
            // wait 1600ms
            // 2nd - no state
            async {
                tokio::time::sleep(Duration::from_millis(1600)).await;
                assert!(matches!(
                    executor.state(&job_id_1).await,
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
