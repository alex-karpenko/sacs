use crate::{
    executor::ChangeExecutorStateEvent,
    job::{Job, JobId},
    scheduler::{CancelOpts, RuntimeThreads, ShutdownOpts, WorkerType},
    ControlChannel, Error, Result,
};
use futures::future::select_all;
use std::collections::HashMap;
use tokio::{join, select, sync::mpsc::Sender, task::JoinHandle};
use tracing::{debug, instrument, warn};
use uuid::Uuid;

const WORKER_CONTROL_CHANNEL_SIZE: usize = 1024;

pub trait AsyncWorker {
    /// Start one more job
    async fn start(&self, job: Job) -> Result<JobId>;
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

pub struct Worker {
    tokio_handler: Option<JoinHandle<()>>,
    thread_handler: Option<std::thread::JoinHandle<()>>,
    channel: Sender<ChangeStateEvent>,
}

enum JobExecutionResult {
    Completed,
    Timeouted,
}

impl Worker {
    pub fn new(type_: WorkerType, executor_channel: Sender<ChangeExecutorStateEvent>) -> Self {
        let worker_channel = ControlChannel::<ChangeStateEvent>::new(WORKER_CONTROL_CHANNEL_SIZE);
        let worker_sender = worker_channel.sender();

        debug!(worker_type = ?type_, "construct new worker");
        match type_ {
            WorkerType::CurrentRuntime => Self {
                tokio_handler: Some(tokio::task::spawn(Self::worker(
                    executor_channel,
                    worker_channel,
                ))),
                thread_handler: None,
                channel: worker_sender,
            },
            WorkerType::CurrentThread => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                Self {
                    tokio_handler: None,
                    thread_handler: Some(std::thread::spawn(move || {
                        rt.block_on(
                            async move { Self::worker(executor_channel, worker_channel).await },
                        );
                    })),
                    channel: worker_sender,
                }
            }
            WorkerType::MultiThread(threads) => {
                let mut rt = tokio::runtime::Builder::new_multi_thread();
                let rt = if let RuntimeThreads::Limited(threads) = threads {
                    rt.worker_threads(threads)
                } else {
                    &mut rt
                };
                let rt = rt.enable_all().build().unwrap();
                Self {
                    tokio_handler: None,
                    thread_handler: Some(std::thread::spawn(move || {
                        rt.block_on(
                            async move { Self::worker(executor_channel, worker_channel).await },
                        );
                    })),
                    channel: worker_sender,
                }
            }
        }
    }

    #[instrument("worker loop", skip_all)]
    async fn worker(
        executor_channel: Sender<ChangeExecutorStateEvent>,
        worker_channel: ControlChannel<ChangeStateEvent>,
    ) {
        let mut ids: Vec<JobId> = Vec::new();
        let mut handlers: Vec<JoinHandle<JobExecutionResult>> = Vec::new();

        // Push a single always-pending job to avoid panics on empty select_all
        let fake_id = JobId::new(Uuid::new_v4());
        let fake_handler = tokio::task::spawn(Box::pin(async {
            futures::future::pending::<JobExecutionResult>().await
        }));
        ids.push(fake_id);
        handlers.push(fake_handler);

        loop {
            select! {
                biased;
                event = worker_channel.receive() => {
                    if let Some(event) = event {
                        debug!(?event, "event received");
                        match event {
                            ChangeStateEvent::StartJob(job) => {
                                let id = job.id();
                                let job_to_run = &job.job();
                                let job_to_run = (job_to_run.write().await)(id);

                                ids.push(job.id());

                                if let Some(timeout) = job.timeout() {
                                    let handler = tokio::task::spawn(Box::pin(async move {
                                        select! {
                                            _ = job_to_run => {JobExecutionResult::Completed},
                                            _ = tokio::time::sleep(timeout) => {JobExecutionResult::Timeouted}
                                            }
                                        }
                                    ));
                                    handlers.push(handler);
                                } else {
                                    let handler = tokio::task::spawn(Box::pin(async move {job_to_run.await; JobExecutionResult::Completed}));
                                    handlers.push(handler);
                                }

                                let _ = executor_channel
                                    .send(ChangeExecutorStateEvent::JobStarted(job.id()))
                                    .await
                                    .map_err(|_e| Error::SendingChangeStateEvent);
                            },
                            ChangeStateEvent::CancelJob(id) => {
                                let ids_map: HashMap<&JobId, usize> = ids.iter().enumerate().map(|(i, id)| (id, i)).collect();
                                let index = ids_map.get(&id);
                                if let Some(index) = index {
                                    let index = *index;
                                    ids.remove(index);
                                    let handler = handlers.remove(index);
                                    handler.abort();
                                    let _ = executor_channel
                                        .send(ChangeExecutorStateEvent::JobCancelled(id))
                                        .await
                                        .map_err(|_e| Error::SendingChangeStateEvent);
                                }
                            },
                            ChangeStateEvent::Shutdown(opts) => {
                                // Remove fake (forever pending) handler
                                handlers.remove(0);
                                if !handlers.is_empty() {
                                    match opts {
                                        ShutdownOpts::IgnoreRunning => {},
                                        ShutdownOpts::CancelTasks(cancel_opts) => {
                                            match cancel_opts {
                                                CancelOpts::Ignore => {},
                                                CancelOpts::Kill => {
                                                    for handler in handlers {
                                                        handler.abort();
                                                    }
                                                },
                                            }
                                        },
                                        ShutdownOpts::WaitForFinish => {
                                            futures::future::join_all(handlers).await;
                                        },
                                        ShutdownOpts::WaitFor(timeout) => {
                                            select! {
                                                _ = futures::future::join_all(handlers) => { debug!("shutdown completed") },
                                                _ = tokio::time::sleep(timeout) => { debug!("shutdown timed out") },
                                            }
                                        },
                                    };
                                }
                                break
                            },
                        }
                    } else {
                        warn!("control channel error, exiting");
                        break
                    }
                },
                completed = select_all(&mut handlers.iter_mut()) => {
                    let (result, index, _) = completed;
                    let id = ids.get(index);
                    debug!(job_id = ?id, "job completed");
                    if let Some(id) = id {
                        let id = id.clone();
                        ids.remove(index);
                        handlers.remove(index);

                        let job_completion_state = match result {
                            Ok(completion_result) => {
                                match completion_result {
                                    JobExecutionResult::Completed => ChangeExecutorStateEvent::JobCompleted(id),
                                    JobExecutionResult::Timeouted => ChangeExecutorStateEvent::JobTimeouted(id),
                                }
                            },
                            Err(_) => ChangeExecutorStateEvent::JobCompleted(id), // TODO: introduce new state to reflect error
                        };
                        let _ = executor_channel
                            .send(job_completion_state)
                            .await
                            .map_err(|_e| Error::SendingChangeStateEvent);
                    }
                }
            }
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
    async fn start(&self, job: Job) -> Result<JobId> {
        let id = job.id();
        debug!(job_id = %id, "start job");
        self.send_event(ChangeStateEvent::StartJob(job)).await?;
        Ok(id)
    }

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
