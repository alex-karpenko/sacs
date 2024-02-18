mod event;
mod executor;
mod job;
mod queue;
pub mod scheduler;
pub mod task;
mod worker;

use job::JobId;
use std::{future::Future, pin::Pin, sync::Arc};
use task::TaskId;
use thiserror::Error;
use tokio::sync::{
    mpsc::{self, error::SendError, Receiver, Sender},
    RwLock,
};

const DEFAULT_CONTROL_CHANNEL_SIZE: usize = 16;

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub(crate) type AsyncJob =
    dyn FnMut(JobId) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync;
pub(crate) type AsyncJobBoxed = Arc<RwLock<Box<AsyncJob>>>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unable to complete shutdown request")]
    IncompleteShutdown,
    #[error("inconsistent indexes in the events queue")]
    InconsistentQueueContent,
    #[error("there's no job with id `{0:?}`")]
    IncorrectJobId(JobId),
    #[error("there's no task with id `{0:?}`")]
    IncorrectTaskId(TaskId),
    #[error("cron expression is invalid")]
    InvalidCronExpression(#[from] cron::error::Error),
    #[error("unable to receive event from control channel")]
    ReceivingChangeStateEvent,
    #[error("unable to send event ot control channel")]
    SendingChangeStateEvent,
    #[error("shutdown request received")]
    ShutdownRequested,
    #[error("unable to convert system time")]
    SystemTimeOperation(#[from] std::time::SystemTimeError),
}

struct ControlChannel<T> {
    sender: Sender<T>,
    receiver: RwLock<Receiver<T>>,
}

impl<T> ControlChannel<T> {
    fn new(size: usize) -> Self {
        let (sender, receiver) = mpsc::channel::<T>(size);
        Self {
            sender,
            receiver: RwLock::new(receiver),
        }
    }

    async fn send(&self, event: T) -> Result<(), SendError<T>> {
        self.sender.send(event).await
    }

    fn sender(&self) -> Sender<T> {
        self.sender.clone()
    }

    async fn receive(&self) -> Option<T> {
        self.receiver.write().await.recv().await
    }

    async fn send_event(&self, event: T) -> Result<()> {
        self.send(event)
            .await
            .map_err(|_e| Error::SendingChangeStateEvent)
    }
}

impl<T> Default for ControlChannel<T> {
    fn default() -> Self {
        Self::new(DEFAULT_CONTROL_CHANNEL_SIZE)
    }
}

#[derive(Debug)]
pub enum WorkerType {
    CurrentRuntime,
    CurrentThread,
    MultiThread(Option<usize>),
}
