use crate::{
    executor::ChangeExecutorStateEvent,
    job::{Job, JobId},
    scheduler::{CancelOpts, RuntimeThreads, ShutdownOpts, WorkerType},
    ControlChannel, Error, Result,
};
use futures::future::select_all;
use std::collections::HashMap;
use tokio::{join, select, sync::mpsc::Sender, task::JoinHandle};
use tracing::{debug, warn};
use uuid::Uuid;

const WORKER_CONTROL_CHANNEL_SIZE: usize = 1024;

pub trait AsyncWorker {
    /// Start one more job
    async fn start(&self, job: Job) -> Result<JobId>;
    /// Cancel single running job
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
    tokio_handler: Option<tokio::task::JoinHandle<()>>,
    thread_handler: Option<std::thread::JoinHandle<()>>,
    channel: Sender<ChangeStateEvent>,
}

impl Worker {
    pub fn new(type_: WorkerType, executor_channel: Sender<ChangeExecutorStateEvent>) -> Self {
        let worker_channel = ControlChannel::<ChangeStateEvent>::new(WORKER_CONTROL_CHANNEL_SIZE);
        let worker_sender = worker_channel.sender();

        debug!("new: type={:?}", type_);
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

    async fn worker(
        executor_channel: Sender<ChangeExecutorStateEvent>,
        worker_channel: ControlChannel<ChangeStateEvent>,
    ) {
        debug!("worker: start events loop");

        let mut ids: Vec<JobId> = Vec::new();
        let mut handlers: Vec<JoinHandle<()>> = Vec::new();

        // Push single always-pending job to avoid panics on empty select_all
        let fake_id = JobId::new(Uuid::new_v4());
        let fake_handler =
            tokio::task::spawn(Box::pin(async { futures::future::pending::<()>().await }));
        ids.push(fake_id);
        handlers.push(fake_handler);

        loop {
            select! {
                biased;
                event = worker_channel.receive() => {
                    if let Some(event) = event {
                        debug!("worker: event={:?}", event);
                        match event {
                            ChangeStateEvent::StartJob(job) => {
                                let id = job.id();
                                let job_to_run = &job.job();
                                let mut job_to_run = job_to_run.write().await;
                                let job_to_run = (job_to_run)(id);
                                let handler = tokio::task::spawn(Box::pin(job_to_run));
                                ids.push(job.id());
                                handlers.push(handler);
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
                                    debug!("worker: shutdown with options={opts:?}");
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
                                                _ = futures::future::join_all(handlers) => {},
                                                _ = tokio::time::sleep(timeout) => {},
                                            };
                                        },
                                    };
                                }
                                break
                            },
                        }
                    } else {
                        warn!("worker: empty control channel, exiting");
                        break
                    }
                },
                completed = select_all(&mut handlers.iter_mut()) => {
                    let (_, index, _) = completed;
                    let id = ids.get(index);
                    debug!("worker: completed={:?}", id);
                    if let Some(id) = id {
                        let id = id.clone();
                        ids.remove(index);
                        handlers.remove(index);
                        let _ = executor_channel
                            .send(ChangeExecutorStateEvent::JobCompleted(id))
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
        self.send_event(ChangeStateEvent::StartJob(job)).await?;
        Ok(id)
    }

    async fn cancel(&self, id: &JobId) -> Result<()> {
        self.send_event(ChangeStateEvent::CancelJob(id.clone()))
            .await
    }

    async fn shutdown(self, opts: ShutdownOpts) -> Result<()> {
        self.send_event(ChangeStateEvent::Shutdown(opts.clone()))
            .await?;

        // Define waiter func with respect to type of runtime thread
        let worker_waiter = async {
            if let Some(handler) = self.tokio_handler {
                debug!("shutdown: waiting for async handler completion");
                return futures::join!(handler)
                    .0
                    .map_err(|_e| Error::IncompleteShutdown);
            };
            if let Some(handler) = self.thread_handler {
                debug!("shutdown: waiting for thread handler completion");
                return handler.join().map_err(|_e| Error::IncompleteShutdown);
            };
            Ok(())
        };

        match opts {
            ShutdownOpts::IgnoreRunning => Ok(()),
            ShutdownOpts::CancelTasks(_) | ShutdownOpts::WaitForFinish => join!(worker_waiter).0,
            ShutdownOpts::WaitFor(timeout) => {
                select! {
                    _ = tokio::time::sleep(timeout) => {Err(Error::IncompleteShutdown)},
                    result = worker_waiter => {result},
                }
            }
        }
    }
}
