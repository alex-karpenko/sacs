use crate::{
    job::{Job, JobId, JobState},
    scheduler::{ShutdownOpts, WorkerParallelism, WorkerType},
    worker::{AsyncWorker, Worker},
    ControlChannel, Error, Result,
};
use std::collections::{HashMap, VecDeque};
use tokio::sync::RwLock;
use tracing::{debug, instrument, warn};

/// Default size of control channels
const EXECUTOR_CONTROL_CHANNEL_SIZE: usize = 1024;

pub(crate) trait JobExecutor {
    /// Blocks until the next executor event arrives. Cancel-safe: it only awaits the
    /// receive itself, no state mutation happens before an event is actually returned.
    async fn next_event(&self) -> Result<ChangeExecutorStateEvent>;
    /// Applies a previously received event to the executor's state. Must be run to
    /// completion (never as the polled branch of a `select!`), because it isn't cancel-safe.
    async fn apply(&self, event: ChangeExecutorStateEvent) -> Result<JobId>;
    async fn enqueue(&self, job: Job) -> Result<JobId>;
    async fn cancel(&self, id: &JobId) -> Result<()>;
    async fn state(&self, id: &JobId) -> Result<JobState>;
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
    /// Fallible counterpart of [`Executor::new`]. Returns [`Error::InvalidWorkerConfig`]
    /// instead of panicking when the requested worker configuration is invalid or the
    /// underlying `Tokio` runtime can't be constructed.
    pub fn try_new(worker_type: WorkerType, parallelism: WorkerParallelism) -> Result<Self> {
        debug!(?worker_type, ?parallelism, "construct new executor");
        let executor_channel = ControlChannel::new(EXECUTOR_CONTROL_CHANNEL_SIZE);
        let to_executor = executor_channel.sender();

        Ok(Self {
            parallelism,
            control_channel: executor_channel,
            worker: Worker::try_new(worker_type, to_executor)?,
            jobs: RwLock::new(ExecutorJobsState::default()),
        })
    }

    /// Basic constructor, panics on invalid configuration or if the underlying `Tokio`
    /// runtime can't be constructed. Use [`Executor::try_new`] for a fallible alternative.
    pub fn new(worker_type: WorkerType, parallelism: WorkerParallelism) -> Self {
        Self::try_new(worker_type, parallelism)
            .expect("invalid worker configuration or unable to construct Tokio runtime")
    }

    /// Pops as many pending jobs as current parallelism allows and dispatches them to the
    /// worker. The `jobs` lock is held only while deciding *which* jobs to start, not while
    /// dispatching them: dispatch uses a non-blocking `try_start`, so this function can never
    /// block indefinitely on a saturated worker channel while holding the lock (that shape
    /// previously could deadlock against the worker's own bounded reply channel, since both
    /// ends are drained by the very consumer that would be stuck here).
    #[instrument("process pending jobs", skip_all, level = "debug")]
    async fn requeue_jobs(&self) -> Result<()> {
        let to_start: Vec<Job> = {
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

                    limit.saturating_sub(running)
                }
            };

            debug!(count = jobs_to_run, "dispatch pending jobs");
            let mut to_start = Vec::with_capacity(jobs_to_run);
            for _ in 0..jobs_to_run {
                match jobs.pending.pop_front() {
                    Some(job) => to_start.push(job),
                    None => break,
                }
            }
            to_start
        };

        let mut iter = to_start.into_iter();
        while let Some(job) = iter.next() {
            let id = job.id();
            debug!(job_id = %id, "start job");
            match self.worker.try_start(job) {
                Ok(()) => {
                    self.jobs.write().await.state.insert(id, JobState::Starting);
                }
                Err(job) => {
                    // Worker's control channel is saturated. Put this job, and every other
                    // job in this batch that hasn't been tried yet, back at the front of the
                    // queue (preserving their original order) and stop dispatching for now
                    // instead of blocking this (single) consumer task on a full channel: the
                    // next executor event will trigger another `requeue_jobs` attempt.
                    //
                    // `iter` only yields the jobs from this batch that haven't been tried
                    // yet, so pushing them back (in reverse, since each `push_front` puts its
                    // argument at the very front) followed by the job that just failed
                    // restores the exact order they were in before this loop started.
                    let remaining = iter.len();
                    debug!(job_id = %id, remaining, "worker channel saturated, deferring batch");
                    let mut jobs = self.jobs.write().await;
                    for leftover in iter.rev() {
                        jobs.pending.push_front(leftover);
                    }
                    jobs.pending.push_front(job);
                    break;
                }
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
    #[instrument("waiting for executor event", skip_all, level = "debug")]
    async fn next_event(&self) -> Result<ChangeExecutorStateEvent> {
        self.control_channel.receive().await.ok_or_else(|| {
            warn!("control channel error, exiting");
            Error::ReceivingChangeStateEvent
        })
    }

    #[instrument("applying executor event", skip_all, level = "debug")]
    async fn apply(&self, event: ChangeExecutorStateEvent) -> Result<JobId> {
        debug!(?event, "event received");
        match event {
            ChangeExecutorStateEvent::JobStarted(id) => {
                self.jobs
                    .write()
                    .await
                    .state
                    .insert(id.clone(), JobState::Running);
                Ok(id)
            }
            ChangeExecutorStateEvent::JobCanceled(id) => {
                {
                    self.jobs
                        .write()
                        .await
                        .state
                        .insert(id.clone(), JobState::Canceled);
                }
                self.requeue_jobs().await?;
                Ok(id)
            }
            ChangeExecutorStateEvent::JobTimeout(id) => {
                {
                    self.jobs
                        .write()
                        .await
                        .state
                        .insert(id.clone(), JobState::Timeout);
                }
                self.requeue_jobs().await?;
                Ok(id)
            }
            ChangeExecutorStateEvent::JobError(id) => {
                {
                    self.jobs
                        .write()
                        .await
                        .state
                        .insert(id.clone(), JobState::Error);
                }
                self.requeue_jobs().await?;
                Ok(id)
            }
            ChangeExecutorStateEvent::JobCompleted(id) => {
                {
                    self.jobs
                        .write()
                        .await
                        .state
                        .insert(id.clone(), JobState::Completed);
                }
                self.requeue_jobs().await?;
                Ok(id)
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
        // A job still sitting in `pending` (because parallelism is saturated) was never
        // handed to the worker, so the worker's own cancellation lookup would simply miss
        // it: it would stay in `pending` and eventually run anyway once capacity frees up,
        // even though `cancel` already reported success. Handle that case ourselves before
        // delegating to the worker: transition its state directly under the same lock that
        // removes it from `pending`, instead of routing through `control_channel`. That
        // channel's sole consumer is the very scheduler-loop task that calls `cancel`, so a
        // blocking `send_event` there could deadlock this task against itself under
        // back-pressure -- the same shape `requeue_jobs` used to have against the worker
        // channel before it was switched to non-blocking dispatch.
        let was_pending = {
            let mut jobs = self.jobs.write().await;
            let before = jobs.pending.len();
            jobs.pending.retain(|job| job.id() != *id);
            let removed = before != jobs.pending.len();
            if removed {
                jobs.state.insert(id.clone(), JobState::Canceled);
            }
            removed
        };

        if was_pending {
            // Removing a job from `pending` never frees worker capacity by itself, but
            // calling `requeue_jobs` here keeps behavior identical to the `JobCanceled`
            // event path it replaces (which always re-evaluated dispatch after a state
            // change) and is a harmless no-op if nothing else is eligible to start.
            self.requeue_jobs().await
        } else {
            self.worker.cancel(id).await
        }
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
    JobCanceled(JobId),
    JobTimeout(JobId),
    JobError(JobId),
    JobCompleted(JobId),
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::task::{Task, TaskSchedule};
    use std::{sync::Arc, time::Duration};
    use tokio::{join, select};

    async fn drive(executor: &Executor) -> Result<JobId> {
        let event = executor.next_event().await?;
        executor.apply(event).await
    }

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
                assert_eq!(executor.state(&job_id_0).await.unwrap(), JobState::Canceled);
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
                        _ = drive(&executor) => {},
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

    #[tokio::test]
    async fn executor_jobs_state_clear() {
        let mut state = ExecutorJobsState {
            pending: VecDeque::new(),
            state: HashMap::new(),
        };

        // Create task and extract job form it
        let task = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
            })
        });
        let job = task.job.clone();
        let job_id = JobId::new("task id");
        let job = Job::new(job_id.clone(), job, None);

        // Push task and job to executor state
        state
            .state
            .insert(JobId::new(task.id()), JobState::Starting);

        state.pending.push_back(job);

        // Assert it's present
        assert!(!state.state.is_empty());
        assert!(!state.pending.is_empty());

        // Assert clearing
        state.clear();
        assert!(state.state.is_empty());
        assert!(state.pending.is_empty());
    }

    #[tokio::test]
    async fn default_constructs_a_usable_executor() {
        let executor = Executor::default();
        let task = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
        let job = Job::new(JobId::new(task.id()), task.job.clone(), None);
        let job_id = job.id();

        executor.enqueue(job).await.unwrap();
        let event = drive(&executor).await.unwrap();
        assert_eq!(event, job_id);
    }

    #[tokio::test]
    async fn cancel_removes_pending_job() {
        // Limited(1): first job occupies the only slot, second stays pending.
        let executor = Executor::new(WorkerType::CurrentRuntime, WorkerParallelism::Limited(1));

        let blocker = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(2)).await;
            })
        });
        let blocker_job_id = JobId::new("blocker");
        let blocker_job = Job::new(blocker_job_id.clone(), blocker.job.clone(), None);

        let victim = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
        let victim_job_id = JobId::new("victim");
        let victim_job = Job::new(victim_job_id.clone(), victim.job.clone(), None);

        executor.enqueue(blocker_job).await.unwrap();
        executor.enqueue(victim_job).await.unwrap();

        assert_eq!(
            executor.state(&victim_job_id).await.unwrap(),
            JobState::Pending
        );

        executor.cancel(&victim_job_id).await.unwrap();

        {
            let jobs = executor.jobs.read().await;
            assert!(jobs.pending.is_empty(), "victim must be gone from pending");
        }

        // The state transition happens synchronously inside `cancel`, under the same lock
        // that removes the job from `pending` -- no event needs to be drained for it.
        assert_eq!(
            executor.state(&victim_job_id).await.unwrap(),
            JobState::Canceled
        );
    }

    /// Regression test for the requeue_jobs fix: when the worker's control channel is
    /// saturated partway through dispatching a batch of runnable jobs, every job in that
    /// batch must end up either dispatched or still queued for retry -- none may be
    /// silently dropped. Reproduces the scenario directly (bypassing `enqueue`, which is
    /// the only way to reliably build up a multi-item backlog without racing a live
    /// worker thread) by pre-loading `pending` with more jobs than the worker's channel
    /// can hold, then triggering exactly one `requeue_jobs` call.
    #[tokio::test]
    async fn requeue_jobs_does_not_drop_jobs_on_saturated_worker_channel() {
        let executor = Executor::new(WorkerType::CurrentRuntime, WorkerParallelism::Unlimited);

        const N: usize = 3000;
        {
            let mut jobs = executor.jobs.write().await;
            for i in 0..N {
                let task = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
                let job_id = JobId::new(format!("job-{i}"));
                let job = Job::new(job_id.clone(), task.job.clone(), None);
                jobs.pending.push_back(job);
                jobs.state.insert(job_id, JobState::Pending);
            }
        }

        executor.requeue_jobs().await.unwrap();

        let jobs = executor.jobs.read().await;
        assert_eq!(jobs.state.len(), N, "no job's state entry may disappear");

        let pending_ids: std::collections::HashSet<_> =
            jobs.pending.iter().map(|job| job.id()).collect();
        for (id, state) in jobs.state.iter() {
            let dispatched = *state == JobState::Starting;
            let still_pending = pending_ids.contains(id);
            assert!(
                dispatched || still_pending,
                "job {id:?} is in state {state:?} but neither dispatched nor pending -- it was lost"
            );
        }
    }
}
