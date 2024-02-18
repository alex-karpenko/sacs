use crate::{
    event::Event,
    executor::{Executor, JobExecutor, DEFAULT_MAX_PARALLEL_JOBS},
    job::{Job, JobId, JobStatus},
    queue::{EventTimeQueue, Queue},
    task::{Task, TaskId, TaskStatus},
    ControlChannel, Error, Result, WorkerType,
};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    select,
    sync::{mpsc::Sender, RwLock},
};
use tracing::{debug, warn};

const SCHEDULER_CONTROL_CHANNEL_SIZE: usize = 1024;

#[cfg(feature = "async-trait")]
#[allow(async_fn_in_trait)]
pub trait TaskScheduler {
    async fn add(&self, task: Task) -> Result<TaskId>;
    async fn drop(&self, id: TaskId, cancel: bool) -> Result<()>;
    async fn status(&self, id: TaskId) -> Result<TaskStatus>;
    async fn shutdown(self, wait: bool) -> Result<()>;
}

#[cfg(not(feature = "async-trait"))]
pub trait TaskScheduler {
    fn add(&self, task: Task) -> impl std::future::Future<Output = Result<TaskId>> + Send;
    fn drop(
        &self,
        id: TaskId,
        cancel: bool,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn status(&self, id: TaskId) -> impl std::future::Future<Output = Result<TaskStatus>> + Send;
    fn shutdown(self, wait: bool) -> impl std::future::Future<Output = Result<()>> + Send;
}

pub struct Scheduler {
    tasks: Arc<RwLock<HashMap<TaskId, Task>>>,
    channel: Sender<ChangeStateEvent>,
    handler: tokio::task::JoinHandle<Result<()>>,
}

#[derive(Debug)]
enum ChangeStateEvent {
    Shutdown(bool),
    EnqueueTask(Task),
    DropTask(TaskId, bool),
}

impl Scheduler {
    pub fn new(worker_type: WorkerType, max_parallel_jobs: usize) -> Self {
        debug!("new: type={:?}, max jobs={max_parallel_jobs}", worker_type);
        let channel = ControlChannel::<ChangeStateEvent>::new(SCHEDULER_CONTROL_CHANNEL_SIZE);
        let tasks = Arc::new(RwLock::new(HashMap::new()));

        Self {
            tasks: tasks.clone(),
            channel: channel.sender(),
            handler: tokio::task::spawn(Scheduler::work(
                worker_type,
                max_parallel_jobs,
                tasks.clone(),
                channel,
            )),
        }
    }

    async fn work(
        worker_type: WorkerType,
        max_parallel_jobs: usize,
        tasks: Arc<RwLock<HashMap<TaskId, Task>>>,
        channel: ControlChannel<ChangeStateEvent>,
    ) -> Result<()> {
        let queue = Queue::default();
        let executor = Executor::new(worker_type, max_parallel_jobs);
        let mut jobs: HashMap<JobId, TaskId> = HashMap::new();

        debug!("work: start events loop");
        loop {
            select! {
            biased;
            event = channel.receive() => {
                if let Some(event) = event {
                    debug!("work: control event={:?}", event);
                    match event {
                        ChangeStateEvent::Shutdown(wait) => {
                            queue.shutdown().await;
                            executor.shutdown(wait).await?;
                            return Ok(())
                        },
                        ChangeStateEvent::EnqueueTask(mut task) => {
                            let event_id = task.id.clone().into();
                            let task_id = task.id.clone();
                            let at = task.schedule.initial_run_time();
                            queue.insert(Event::new(event_id, at)).await?;
                            task.state.enqueued();
                            let mut tasks = tasks.write().await;
                            tasks.insert(task_id, task);
                        },
                        ChangeStateEvent::DropTask(id, cancel) => {
                            let tasks = tasks.read().await;
                            let task = tasks.get(&id);
                            if let Some(task) = task {
                                let event_id = id.into();
                                queue.pop(&event_id).await?;
                                if cancel {
                                    for job in task.state.jobs() {
                                        executor.cancel(&job).await?;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    warn!("work: empty events channel");
                }
            },
            event = queue.next() => {
                if let Ok(event) = event {
                    debug!("work: queue event={:?}", event);
                    let mut tasks = tasks.write().await;
                    let task = tasks.get_mut(&event.id.into());

                    if let Some(task) = task {
                        let job_id = JobId::new();
                        let job = task.job.clone();
                        let job = Job::new(job_id, job);
                        let job_id = executor.enqueue(job).await?;
                        jobs.insert(job_id.clone(), task.id.clone());
                        task.state.scheduled(job_id);
                        let at = task.schedule.after_start_run_time();
                        if let Some(at) = at {
                            let event_id = task.id.clone().into();
                            queue.insert(Event::new(event_id, at)).await?;
                            task.state.enqueued();
                        }
                    }
                    } else {
                        warn!("work: error from queue received={:?}, exiting", event);
                        return Err(event.err().unwrap())
                    }
                },
            job_id = executor.work() => {
                if let Ok(job_id) = job_id {
                    debug!("work: executor event={:?}", job_id);
                    let mut tasks = tasks.write().await;
                    let job_status = executor.status(&job_id).await?;
                    let task_id = jobs.get(&job_id);
                    if let Some(task_id) = task_id {
                        let task = tasks.get_mut(task_id);
                        if let Some(task) = task {
                            debug!("work: job status changed={:?}", job_status);
                            let mut at = None;
                            match job_status {
                                JobStatus::Running => { task.state.started(job_id); },
                                JobStatus::Completed => {
                                    task.state.completed(&job_id);
                                    at = task.schedule.after_finish_run_time();
                                },
                                JobStatus::Cancelled => {
                                    task.state.cancelled(&job_id);
                                    at = task.schedule.after_finish_run_time();
                                },
                                _ => {},
                            };
                            if let Some(at) = at {
                                let event_id = task.id.clone().into();
                                queue.insert(Event::new(event_id, at)).await?;
                                task.state.enqueued();
                            }
                        }
                    }
                } else {
                    warn!("work: error from executor received={:?}", job_id);
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

impl Default for Scheduler {
    fn default() -> Self {
        Self::new(WorkerType::CurrentRuntime, DEFAULT_MAX_PARALLEL_JOBS)
    }
}

impl TaskScheduler for Scheduler {
    async fn add(&self, task: Task) -> Result<TaskId> {
        let id = TaskId::new();
        self.send_event(ChangeStateEvent::EnqueueTask(task)).await?;
        Ok(id)
    }

    async fn drop(&self, id: TaskId, cancel: bool) -> Result<()> {
        self.send_event(ChangeStateEvent::DropTask(id, cancel))
            .await
    }

    async fn status(&self, id: TaskId) -> Result<TaskStatus> {
        let mut tasks = self.tasks.write().await;
        let task = tasks.get_mut(&id);

        if let Some(task) = task {
            let status = task.state.status();
            if task.state.finished() {
                debug!("status: remove finished task {id}");
                tasks.remove(&id);
            }
            return Ok(status);
        }

        Err(Error::IncorrectTaskId(id))
    }

    async fn shutdown(self, wait: bool) -> Result<()> {
        debug!("shutdown: requested");
        self.send_event(ChangeStateEvent::Shutdown(wait)).await?;
        if wait {
            debug!("shutdown: waiting for scheduler handler completion");
            return futures::join!(self.handler)
                .0
                .map_err(|_e| Error::IncompleteShutdown)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::task::TaskSchedule;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use uuid::Uuid;

    async fn basic_test_suite(
        scheduler: Scheduler,
        schedules: Vec<TaskSchedule>,
        durations: &[Duration],
        suite_duration: Duration,
    ) -> Result<(Vec<String>, Vec<String>)> {
        assert_eq!(
            schedules.len(),
            durations.len(),
            "schedulers and durations arrays size mismatched"
        );

        let logs = Arc::new(RwLock::new(Vec::<String>::new()));
        let jobs = Arc::new(RwLock::new(Vec::<Uuid>::new()));

        for s in 0..schedules.len() {
            let log = logs.clone();
            let jobs = jobs.clone();
            let task_duration = durations[s];
            let task = Task::new(schedules[s].clone(), move |id| {
                let log = log.clone();
                let jobs = jobs.clone();
                Box::pin(async move {
                    jobs.write().await.push(id.clone().into());
                    log.write().await.push(format!("{},start,{id}", s));
                    tokio::time::sleep(task_duration).await;
                    log.write().await.push(format!("{},finish,{id}", s));
                })
            });
            scheduler.add(task).await?;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        tokio::time::sleep(suite_duration).await;
        scheduler.shutdown(true).await?;

        let logs: Vec<String> = logs.read().await.iter().map(String::from).collect();
        let jobs: Vec<String> = jobs.read().await.iter().map(|s| format!("{s}")).collect();

        Ok((logs, jobs))
    }

    #[tokio::test]
    async fn once_1_worker() {
        let schedules: Vec<TaskSchedule> =
            Vec::from([TaskSchedule::Once, TaskSchedule::Once, TaskSchedule::Once]);
        let durations = [
            Duration::from_secs(3),
            Duration::from_secs(3),
            Duration::from_secs(1),
        ];
        let scheduler = Scheduler::new(WorkerType::CurrentRuntime, 1);

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(5))
                .await
                .unwrap();

        assert_eq!(logs.len(), 4);
        assert_eq!(jobs.len(), 2);

        let mut expected = vec![];
        for j in jobs.iter().enumerate() {
            expected.push(format!("{},start,{}", j.0, j.1));
            expected.push(format!("{},finish,{}", j.0, j.1));
        }
        assert_eq!(logs, expected);
    }

    #[tokio::test]
    async fn once_2_workers() {
        let schedules: Vec<TaskSchedule> = Vec::from([
            TaskSchedule::Once,
            TaskSchedule::Once,
            TaskSchedule::Once,
            TaskSchedule::Once,
            TaskSchedule::Once,
        ]);
        let durations = [
            Duration::from_secs(3),
            Duration::from_secs(3),
            Duration::from_secs(3),
            Duration::from_secs(3),
            Duration::from_secs(1),
        ];
        let scheduler = Scheduler::new(WorkerType::CurrentRuntime, 2);

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(5))
                .await
                .unwrap();

        assert_eq!(logs.len(), 8);
        assert_eq!(jobs.len(), 4);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("1,start,{}", jobs[1]),
            format!("0,finish,{}", jobs[0]),
            format!("2,start,{}", jobs[2]),
            format!("1,finish,{}", jobs[1]),
            format!("3,start,{}", jobs[3]),
            format!("2,finish,{}", jobs[2]),
            format!("3,finish,{}", jobs[3]),
        ]);
        assert_eq!(logs, expected);
    }

    #[tokio::test]
    async fn cron() {
        let schedules: Vec<TaskSchedule> = Vec::from([
            TaskSchedule::RepeatByCron("*/2 * * * * *".try_into().unwrap()),
            TaskSchedule::RepeatByCron("*/5 * * * * *".try_into().unwrap()),
        ]);
        let durations = [Duration::from_millis(1200), Duration::from_millis(3500)];
        let scheduler = Scheduler::new(WorkerType::CurrentRuntime, 2);

        // wait for next 10 seconds interval
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let wait_for = 10 - now % 10 - 1;
        tokio::time::sleep(Duration::from_secs(wait_for)).await;

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(7))
                .await
                .unwrap();

        assert_eq!(logs.len(), 12);
        assert_eq!(jobs.len(), 6);

        let expected1: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),  // 0
            format!("1,start,{}", jobs[1]),  // 0
            format!("0,finish,{}", jobs[0]), // 1200
            format!("0,start,{}", jobs[2]),  // 2000
            format!("0,finish,{}", jobs[2]), // 3200
            format!("1,finish,{}", jobs[1]), // 3500
            format!("0,start,{}", jobs[3]),  // 4000
            format!("1,start,{}", jobs[4]),  // 5000
            format!("0,finish,{}", jobs[3]), // 5200
            format!("0,start,{}", jobs[5]),  // 6000
            format!("0,finish,{}", jobs[5]), // 7200
            format!("1,finish,{}", jobs[4]), // 8500
        ]);
        let expected2: Vec<String> = Vec::from([
            format!("1,start,{}", jobs[0]),  // 0
            format!("0,start,{}", jobs[1]),  // 0
            format!("0,finish,{}", jobs[1]), // 1200
            format!("0,start,{}", jobs[2]),  // 2000
            format!("0,finish,{}", jobs[2]), // 3200
            format!("1,finish,{}", jobs[0]), // 3500
            format!("0,start,{}", jobs[3]),  // 4000
            format!("1,start,{}", jobs[4]),  // 5000
            format!("0,finish,{}", jobs[3]), // 5200
            format!("0,start,{}", jobs[5]),  // 6000
            format!("0,finish,{}", jobs[5]), // 7200
            format!("1,finish,{}", jobs[4]), // 8500
        ]);
        let debug = format!(
            "jobs={:?}\nlogs={:?}\nexpected1={:?}\nexpected2={:?}",
            jobs, logs, expected1, expected2
        );
        assert!((logs == expected1) || (logs == expected2), "{debug}");
    }

    #[tokio::test]
    async fn once_delayed_4_workers() {
        let schedules: Vec<TaskSchedule> = Vec::from([
            TaskSchedule::Once,
            TaskSchedule::OnceDelayed(Duration::from_millis(500)),
            TaskSchedule::OnceDelayed(Duration::from_secs(1)),
            TaskSchedule::OnceDelayed(Duration::from_millis(3300)),
        ]);
        let durations = [
            Duration::from_secs(3),
            Duration::from_secs(3),
            Duration::from_secs(1),
            Duration::from_secs(1),
        ];
        let scheduler = Scheduler::new(WorkerType::CurrentRuntime, 4);

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(4))
                .await
                .unwrap();

        assert_eq!(logs.len(), 8);
        assert_eq!(jobs.len(), 4);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("1,start,{}", jobs[1]),
            format!("2,start,{}", jobs[2]),
            format!("2,finish,{}", jobs[2]),
            format!("0,finish,{}", jobs[0]),
            format!("3,start,{}", jobs[3]),
            format!("1,finish,{}", jobs[1]),
            format!("3,finish,{}", jobs[3]),
        ]);
        assert_eq!(logs, expected);
    }

    #[tokio::test]
    async fn interval_4_workers() {
        let schedules: Vec<TaskSchedule> = Vec::from([
            TaskSchedule::RepeatByInterval(Duration::from_secs(3)),
            TaskSchedule::RepeatByInterval(Duration::from_secs(1)),
            TaskSchedule::RepeatByIntervalDelayed(Duration::from_millis(2100)),
            TaskSchedule::RepeatByIntervalDelayed(Duration::from_millis(5900)),
        ]);
        let durations = [
            Duration::from_millis(900),
            Duration::from_secs(2),
            Duration::from_secs(5),
            Duration::from_secs(1),
        ];
        let scheduler = Scheduler::new(WorkerType::CurrentRuntime, 4);

        let (logs, jobs) = basic_test_suite(
            scheduler,
            schedules,
            &durations,
            Duration::from_millis(7200),
        )
        .await
        .unwrap();

        assert_eq!(logs.len(), 14);
        assert_eq!(jobs.len(), 7);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("1,start,{}", jobs[1]),
            format!("0,finish,{}", jobs[0]),
            format!("1,finish,{}", jobs[1]),
            format!("2,start,{}", jobs[2]),
            format!("1,start,{}", jobs[3]),
            format!("0,start,{}", jobs[4]),
            format!("0,finish,{}", jobs[4]),
            format!("1,finish,{}", jobs[3]),
            format!("3,start,{}", jobs[5]),
            format!("1,start,{}", jobs[6]),
            format!("3,finish,{}", jobs[5]),
            format!("2,finish,{}", jobs[2]),
            format!("1,finish,{}", jobs[6]),
        ]);
        assert_eq!(logs, expected);
    }
}
