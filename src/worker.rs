use crate::{
    executor::ChangeExecutorStateEvent,
    job::{Job, JobId},
    scheduler::{CancelOpts, RuntimeThreads, ShutdownOpts, WorkerType},
    ControlChannel, Error, Result,
};
use std::{collections::HashMap, panic::AssertUnwindSafe};
use tokio::{
    join, select,
    sync::mpsc::Sender,
    task::{AbortHandle, JoinHandle, JoinSet},
};
use tracing::{debug, instrument, warn};

const WORKER_CONTROL_CHANNEL_SIZE: usize = 1024;

pub trait AsyncWorker {
    /// Cancel a single running job
    async fn cancel(&self, id: &JobId) -> Result<()>;
    /// Shutdown worker
    async fn shutdown(self, opts: ShutdownOpts) -> Result<()>;
}

#[derive(Debug)]
enum ChangeStateEvent {
    StartJob(Job),
    CancelJob(JobId),
    Shutdown(ShutdownOpts),
}

pub(crate) struct Worker {
    tokio_handler: Option<JoinHandle<()>>,
    thread_handler: Option<std::thread::JoinHandle<()>>,
    channel: Sender<ChangeStateEvent>,
}

#[derive(Debug)]
enum JobExecutionResult {
    Completed,
    Timeout,
}

impl Worker {
    /// Constructs a new worker. Returns [`Error::InvalidWorkerConfig`] instead of panicking
    /// when the requested worker configuration is invalid (e.g. zero worker threads) or the
    /// underlying `Tokio` runtime can't be constructed (e.g. due to OS resource limits).
    pub fn try_new(
        type_: WorkerType,
        executor_channel: Sender<ChangeExecutorStateEvent>,
    ) -> Result<Self> {
        let worker_channel = ControlChannel::<ChangeStateEvent>::new(WORKER_CONTROL_CHANNEL_SIZE);
        let worker_sender = worker_channel.sender();

        debug!(worker_type = ?type_, "construct new worker");
        match type_ {
            WorkerType::CurrentRuntime => Ok(Self {
                tokio_handler: Some(tokio::task::spawn(Self::worker(
                    executor_channel,
                    worker_channel,
                ))),
                thread_handler: None,
                channel: worker_sender,
            }),
            WorkerType::CurrentThread => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|_e| Error::InvalidWorkerConfig)?;
                Ok(Self {
                    tokio_handler: None,
                    thread_handler: Some(std::thread::spawn(move || {
                        rt.block_on(
                            async move { Self::worker(executor_channel, worker_channel).await },
                        );
                    })),
                    channel: worker_sender,
                })
            }
            WorkerType::MultiThread(threads) => {
                if let RuntimeThreads::Limited(0) = threads {
                    return Err(Error::InvalidWorkerConfig);
                }
                let mut rt = tokio::runtime::Builder::new_multi_thread();
                let rt = if let RuntimeThreads::Limited(threads) = threads {
                    rt.worker_threads(threads)
                } else {
                    &mut rt
                };
                let rt = rt
                    .enable_all()
                    .build()
                    .map_err(|_e| Error::InvalidWorkerConfig)?;
                Ok(Self {
                    tokio_handler: None,
                    thread_handler: Some(std::thread::spawn(move || {
                        rt.block_on(
                            async move { Self::worker(executor_channel, worker_channel).await },
                        );
                    })),
                    channel: worker_sender,
                })
            }
        }
    }

    #[instrument("worker loop", skip_all, level = "debug")]
    async fn worker(
        executor_channel: Sender<ChangeExecutorStateEvent>,
        worker_channel: ControlChannel<ChangeStateEvent>,
    ) {
        // Jobs run as tasks in this `JoinSet`, each carrying its own `JobId` in its output so
        // there's no need for a separate index vector to correlate a finished task back to a
        // job (as a plain `Vec<JoinHandle<_>>` would require). `JoinSet::join_next` also
        // returns `None` immediately on an empty set instead of panicking (as
        // `futures::future::select_all` would on an empty slice), so no permanently-pending
        // sentinel task is needed either.
        let mut jobs: JoinSet<(JobId, JobExecutionResult)> = JoinSet::new();
        let mut abort_handles: HashMap<JobId, AbortHandle> = HashMap::new();

        loop {
            // Deliberately not `biased`: a sustained burst of `StartJob` commands must not
            // be able to starve draining `jobs.join_next()` forever, since job completions
            // feed the executor's own dispatch loop (a stuck drain here can cascade into a
            // stall further up).
            select! {
                event = worker_channel.receive() => {
                    if let Some(event) = event {
                        debug!(?event, "event received");
                        match event {
                            ChangeStateEvent::StartJob(job) => {
                                let id = job.id();
                                let built = {
                                    let factory = job.job();
                                    let mut factory = factory.write().await;
                                    // A panic here -- as opposed to a panic in the *future*
                                    // the factory returns, which `tokio::spawn` already turns
                                    // into a normal `JobState::Error` -- would otherwise
                                    // unwind this whole loop and silently paralyze the
                                    // worker forever. `AssertUnwindSafe` is safe here: on
                                    // panic the half-built future is simply discarded and
                                    // `tokio::sync::RwLock` never poisons, so the lock stays
                                    // perfectly usable for the next job. Note this has no
                                    // effect under `panic = "abort"`.
                                    std::panic::catch_unwind(AssertUnwindSafe(|| (factory)(id.clone())))
                                };

                                match built {
                                    Ok(job_to_run) => {
                                        let abort_handle = if let Some(timeout) = job.timeout() {
                                            let id = job.id();
                                            jobs.spawn(async move {
                                                let result = select! {
                                                    _ = job_to_run => JobExecutionResult::Completed,
                                                    _ = tokio::time::sleep(timeout) => JobExecutionResult::Timeout,
                                                };
                                                (id, result)
                                            })
                                        } else {
                                            let id = job.id();
                                            jobs.spawn(async move {
                                                job_to_run.await;
                                                (id, JobExecutionResult::Completed)
                                            })
                                        };
                                        abort_handles.insert(job.id(), abort_handle);

                                        let _ = executor_channel
                                            .send(ChangeExecutorStateEvent::JobStarted(job.id()))
                                            .await
                                            .map_err(|_e| Error::SendingChangeStateEvent);
                                    },
                                    Err(_payload) => {
                                        warn!(job_id = %id, "task factory panicked, job will not start");
                                        // Two events, in this order, so the paired
                                        // `scheduled -> running -> errors` counters in
                                        // `TaskState` stay balanced with zero changes needed
                                        // there: `job_error` assumes the job reached
                                        // `Running`, so it must get there first.
                                        let _ = executor_channel
                                            .send(ChangeExecutorStateEvent::JobStarted(id.clone()))
                                            .await;
                                        let _ = executor_channel
                                            .send(ChangeExecutorStateEvent::JobError(id))
                                            .await;
                                    },
                                }
                            },
                            ChangeStateEvent::CancelJob(id) => {
                                if let Some(abort_handle) = abort_handles.remove(&id) {
                                    abort_handle.abort();
                                    let _ = executor_channel
                                        .send(ChangeExecutorStateEvent::JobCanceled(id))
                                        .await
                                        .map_err(|_e| Error::SendingChangeStateEvent);
                                }
                            },
                            ChangeStateEvent::Shutdown(opts) => {
                                match opts {
                                    // `IgnoreRunning` doesn't make the *caller* wait (see
                                    // `Worker::shutdown` below, which returns immediately for
                                    // this variant without joining this loop's thread), but
                                    // the loop itself must still wait for jobs here: for a
                                    // dedicated worker runtime (`CurrentThread`/`MultiThread`)
                                    // this function's return is what lets `rt.block_on`
                                    // return, which is what finally drops the runtime -- and
                                    // dropping a runtime aborts everything still running on
                                    // it. Waiting here keeps the (detached, unjoined) runtime
                                    // thread alive until its jobs actually finish, instead of
                                    // aborting them. The residual caveat -- jobs are still
                                    // lost if the whole process exits first -- is inherent
                                    // and documented on `ShutdownOpts::IgnoreRunning`.
                                    ShutdownOpts::IgnoreRunning | ShutdownOpts::WaitForFinish => {
                                        jobs.join_all().await;
                                    },
                                    ShutdownOpts::CancelTasks(cancel_opts) => {
                                        match cancel_opts {
                                            CancelOpts::Ignore => {},
                                            CancelOpts::Kill => {
                                                jobs.abort_all();
                                            },
                                        }
                                    },
                                    ShutdownOpts::WaitFor(timeout) => {
                                        select! {
                                            _ = jobs.join_all() => { debug!("shutdown completed") },
                                            _ = tokio::time::sleep(timeout) => { debug!("shutdown timed out") },
                                        }
                                    },
                                };
                                break
                            },
                        }
                    } else {
                        warn!("control channel error, exiting");
                        break
                    }
                },
                completed = jobs.join_next(), if !jobs.is_empty() => {
                    if let Some(completed) = completed {
                        match completed {
                            Ok((id, completion_result)) => {
                                abort_handles.remove(&id);
                                let job_completion_state = match completion_result {
                                    JobExecutionResult::Completed => {
                                        debug!(job_id = ?id, "job completed successfully");
                                        ChangeExecutorStateEvent::JobCompleted(id)
                                    },
                                    JobExecutionResult::Timeout => {
                                        debug!(job_id = ?id, "job killed because timeout");
                                        ChangeExecutorStateEvent::JobTimeout(id)
                                    },
                                };
                                let _ = executor_channel
                                    .send(job_completion_state)
                                    .await
                                    .map_err(|_e| Error::SendingChangeStateEvent);
                            },
                            Err(join_error) => {
                                // Either a genuine panic in the spawned task (extremely
                                // unlikely: the task body itself never panics, only user
                                // code inside the awaited future could, and that already
                                // unwinds within `tokio::spawn`'s own catch boundary), or an
                                // abort we issued ourselves via `CancelJob` -- which already
                                // sent `JobCanceled` at cancellation time and already removed
                                // the entry from `abort_handles`. Either way there's nothing
                                // left to reconcile here.
                                debug!(%join_error, "job task ended abnormally");
                            },
                        }
                    }
                }
            }
        }
    }

    /// Non-blocking dispatch: tries to hand the job to this worker's control channel without
    /// waiting for capacity. Returns the job back on `Err` if the channel is currently
    /// saturated (or closed), so the caller can decide how to handle backpressure instead of
    /// blocking indefinitely on a full channel.
    pub(crate) fn try_start(&self, job: Job) -> std::result::Result<(), Job> {
        match self.channel.try_send(ChangeStateEvent::StartJob(job)) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(ChangeStateEvent::StartJob(job)))
            | Err(tokio::sync::mpsc::error::TrySendError::Closed(ChangeStateEvent::StartJob(
                job,
            ))) => Err(job),
            Err(_) => unreachable!("only StartJob events are ever sent from try_start"),
        }
    }

    async fn send_event(&self, event: ChangeStateEvent) -> Result<()> {
        self.channel
            .send(event)
            .await
            .map_err(|_e| Error::SendingChangeStateEvent)
    }
}

impl AsyncWorker for Worker {
    async fn cancel(&self, id: &JobId) -> Result<()> {
        debug!(job_id = %id, "cancel job");
        self.send_event(ChangeStateEvent::CancelJob(id.clone()))
            .await
    }

    async fn shutdown(self, opts: ShutdownOpts) -> Result<()> {
        debug!(?opts, "shutdown requested");
        self.send_event(ChangeStateEvent::Shutdown(opts.clone()))
            .await?;

        // Define waiter func with respect to the type of runtime thread
        let worker_waiter = async {
            if let Some(handler) = self.tokio_handler {
                return futures::join!(handler)
                    .0
                    .map_err(|_e| Error::IncompleteShutdown);
            };
            if let Some(handler) = self.thread_handler {
                return handler.join().map_err(|_e| Error::IncompleteShutdown);
            };
            Ok(())
        };

        match opts {
            ShutdownOpts::IgnoreRunning => Ok(()),
            ShutdownOpts::CancelTasks(_) | ShutdownOpts::WaitForFinish => join!(worker_waiter).0,
            ShutdownOpts::WaitFor(timeout) => {
                debug!(?timeout, "wait for jobs completion");
                select! {
                    _ = tokio::time::sleep(timeout) => {Err(Error::IncompleteShutdown)},
                    result = worker_waiter => {result},
                }
            }
        }
    }
}
