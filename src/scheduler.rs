use crate::{
    event::Event,
    executor::{Executor, JobExecutor},
    job::{Job, JobId, JobState},
    queue::{EventTimeQueue, Queue},
    task::{Task, TaskId, TaskStatus},
    ControlChannel, Error, Result,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    select,
    sync::{mpsc::Sender, RwLock},
};
use tracing::{debug, warn};

/// Default maximin number of jobs to run on the single worker
pub(crate) const DEFAULT_MAX_PARALLEL_JOBS: usize = 16;
const SCHEDULER_CONTROL_CHANNEL_SIZE: usize = 1024;

#[cfg(feature = "async-trait")]
#[allow(async_fn_in_trait)]
pub trait TaskScheduler {
    async fn add(&self, task: Task) -> Result<TaskId>;
    async fn drop(&self, id: TaskId, opts: CancelOpts) -> Result<()>;
    async fn status(&self, id: &TaskId) -> Result<TaskStatus>;
    async fn shutdown(self, opts: ShutdownOpts) -> Result<()>;
}

#[cfg(not(feature = "async-trait"))]
pub trait TaskScheduler {
    fn add(&self, task: Task) -> impl std::future::Future<Output = Result<TaskId>> + Send;
    fn drop(
        &self,
        id: TaskId,
        opts: CancelOpts,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn status(&self, id: &TaskId) -> impl std::future::Future<Output = Result<TaskStatus>> + Send;
    fn shutdown(self, opts: ShutdownOpts) -> impl std::future::Future<Output = Result<()>> + Send;
}

pub struct Scheduler {
    tasks: Arc<RwLock<HashMap<TaskId, Task>>>,
    channel: Sender<ChangeStateEvent>,
    handler: tokio::task::JoinHandle<Result<()>>,
}

#[derive(Debug)]
enum ChangeStateEvent {
    Shutdown(ShutdownOpts),
    EnqueueTask(Task),
    DropTask(TaskId, CancelOpts),
}

/// Type of `Tokio` runtime to use for jobs worker.
#[derive(Debug, Default)]
pub enum WorkerType {
    /// Use current runtime instead of creating new one.
    ///
    /// This is the simplest and lightest worker because it uses runtime of the calling context.
    /// This is default type.
    #[default]
    CurrentRuntime,
    /// Creates new thread and runs new `Tokio` runtime of `CurrentThread` type. Single thread worker.
    CurrentThread,
    /// Creates new thread and runs new `Tokio` runtime of `MultiThread` type.
    ///
    /// This is multi thread worker. Number of threads to use can be specified via parameter.
    /// Default is `Tokio` default - number of CPU cores.
    MultiThread(RuntimeThreads),
}

#[derive(Debug, Default)]
pub enum RuntimeThreads {
    #[default]
    CpuCores,
    Limited(usize),
}

#[derive(Debug)]
pub enum WorkerParallelism {
    Unlimited,
    Limited(usize),
}

impl Default for WorkerParallelism {
    fn default() -> Self {
        Self::Limited(DEFAULT_MAX_PARALLEL_JOBS)
    }
}

#[derive(Debug, Default, Clone)]
pub enum CancelOpts {
    #[default]
    Ignore,
    Kill,
}

#[derive(Debug, Default, Clone)]
pub enum ShutdownOpts {
    IgnoreRunning,
    CancelTasks(CancelOpts),
    #[default]
    WaitForFinish,
    WaitFor(Duration),
}

#[derive(Debug, Default)]
pub enum GarbageCollector {
    #[default]
    Disabled,
    Enabled {
        expire_after: Duration,
        interval: Duration,
    },
}

impl GarbageCollector {
    pub fn enabled(expire_after: Duration, interval: Duration) -> Self {
        Self::Enabled {
            expire_after,
            interval,
        }
    }

    pub fn disabled() -> Self {
        Self::Disabled
    }
}

impl GarbageCollector {
    async fn collect_garbage(tasks: Arc<RwLock<HashMap<TaskId, Task>>>, expire_after: Duration) {
        let mut tasks = tasks.write().await;
        let expired_at = SystemTime::now().checked_sub(expire_after).unwrap();

        let to_remove: Vec<TaskId> = tasks
            .iter()
            .filter(|(_id, task)| task.state.finished())
            .filter(|(_id, task)| {
                task.state
                    .last_finished_at()
                    .expect("finished task has no `finished_at` time set, looks like a BUG")
                    <= expired_at
            })
            .map(|(id, _task)| id.clone())
            .collect();

        to_remove.iter().for_each(|id| {
            debug!("collect_garbage: remove expired task={id}");
            tasks.remove(id);
        });
    }
}

#[derive(Debug, Default)]
pub struct SchedulerBuilder {
    worker_type: WorkerType,
    parallelism: WorkerParallelism,
    garbage_collector: GarbageCollector,
}

impl SchedulerBuilder {
    pub fn new() -> Self {
        Self {
            worker_type: WorkerType::default(),
            parallelism: WorkerParallelism::default(),
            garbage_collector: GarbageCollector::default(),
        }
    }

    pub fn worker_type(self, worker_type: WorkerType) -> Self {
        Self {
            worker_type,
            ..self
        }
    }

    pub fn parallelism(self, parallelism: WorkerParallelism) -> Self {
        Self {
            parallelism,
            ..self
        }
    }

    pub fn garbage_collector(self, garbage_collector: GarbageCollector) -> Self {
        Self {
            garbage_collector,
            ..self
        }
    }

    pub fn build(self) -> Scheduler {
        Scheduler::new(self.worker_type, self.parallelism, self.garbage_collector)
    }
}

impl Scheduler {
    pub fn new(
        worker_type: WorkerType,
        parallelism: WorkerParallelism,
        garbage_collector: GarbageCollector,
    ) -> Self {
        debug!("new: type={:?}, parallelism={parallelism:?}", worker_type);
        let channel = ControlChannel::<ChangeStateEvent>::new(SCHEDULER_CONTROL_CHANNEL_SIZE);
        let tasks = Arc::new(RwLock::new(HashMap::new()));

        Self {
            tasks: tasks.clone(),
            channel: channel.sender(),
            handler: tokio::task::spawn(Scheduler::work(
                worker_type,
                parallelism,
                tasks.clone(),
                channel,
                garbage_collector,
            )),
        }
    }

    async fn work(
        worker_type: WorkerType,
        parallelism: WorkerParallelism,
        tasks: Arc<RwLock<HashMap<TaskId, Task>>>,
        channel: ControlChannel<ChangeStateEvent>,
        garbage_collector: GarbageCollector,
    ) -> Result<()> {
        let queue = Queue::default();
        let executor = Executor::new(worker_type, parallelism);
        let mut jobs: HashMap<JobId, TaskId> = HashMap::new();

        match garbage_collector {
            GarbageCollector::Disabled => {}
            GarbageCollector::Enabled {
                expire_after,
                interval,
            } => {
                // Prepare GC task
                let tasks = tasks.clone();
                let task = Task::new(
                    crate::task::TaskSchedule::RepeatByIntervalDelayed(interval),
                    move |id| {
                        let tasks = tasks.clone();
                        Box::pin(async move {
                            debug!("garbage_collector: GC job {id} started.");
                            GarbageCollector::collect_garbage(tasks, expire_after).await;
                            debug!("garbage_collector: GC job {id} finished.");
                        })
                    },
                );
                // Enqueue GC task, it will be processed in first run of events loop
                channel
                    .send(ChangeStateEvent::EnqueueTask(task))
                    .await
                    .map_err(|_e| Error::SendingChangeStateEvent)?;
            }
        }

        debug!("work: start events loop");
        loop {
            select! {
                biased;
                event = channel.receive() => {
                    if let Some(event) = event {
                        debug!("work: control event={:?}", event);
                        match event {
                            ChangeStateEvent::Shutdown(opts) => {
                                queue.shutdown().await;
                                executor.shutdown(opts).await?;
                                tasks.write().await.clear();
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
                            ChangeStateEvent::DropTask(id, opts) => {
                                let tasks = tasks.read().await;
                                let task = tasks.get(&id);
                                if let Some(task) = task {
                                    let event_id = id.into();
                                    queue.pop(&event_id).await?;
                                    match opts {
                                        CancelOpts::Ignore => {},
                                        CancelOpts::Kill => {
                                            for job in task.state.jobs() {
                                                executor.cancel(&job).await?;
                                            }
                                        },
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
                        let job_state = executor.state(&job_id).await?;
                        let task_id = jobs.get(&job_id);
                        if let Some(task_id) = task_id {
                            let task = tasks.get_mut(task_id);
                            if let Some(task) = task {
                                debug!("work: job status changed={:?}", job_state);
                                let mut at = None;
                                match job_state {
                                    JobState::Running => { task.state.started(job_id); },
                                    JobState::Completed => {
                                        task.state.completed(&job_id);
                                        at = task.schedule.after_finish_run_time();
                                    },
                                    JobState::Cancelled => {
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
        Self::new(
            WorkerType::default(),
            WorkerParallelism::default(),
            GarbageCollector::default(),
        )
    }
}

impl TaskScheduler for Scheduler {
    async fn add(&self, task: Task) -> Result<TaskId> {
        let id = task.id();
        self.send_event(ChangeStateEvent::EnqueueTask(task)).await?;
        Ok(id)
    }

    async fn drop(&self, id: TaskId, opts: CancelOpts) -> Result<()> {
        self.send_event(ChangeStateEvent::DropTask(id, opts)).await
    }

    async fn status(&self, id: &TaskId) -> Result<TaskStatus> {
        let mut tasks = self.tasks.write().await;
        let task = tasks.get_mut(id);

        if let Some(task) = task {
            let status = task.state.status();
            if task.state.finished() {
                debug!("status: remove finished task {id}");
                tasks.remove(id);
            }
            return Ok(status);
        }

        Err(Error::IncorrectTaskId(id.clone()))
    }

    async fn shutdown(self, opts: ShutdownOpts) -> Result<()> {
        debug!("shutdown: requested with opts={opts:?}");
        self.send_event(ChangeStateEvent::Shutdown(opts.clone()))
            .await?;

        match opts {
            ShutdownOpts::IgnoreRunning => Ok(()),
            ShutdownOpts::CancelTasks(_) | ShutdownOpts::WaitForFinish => {
                futures::join!(self.handler)
                    .0
                    .map_err(|_e| Error::IncompleteShutdown)?
            }
            ShutdownOpts::WaitFor(timeout) => {
                select! {
                    res = self.handler => {
                        res.map_err(|_e| Error::IncompleteShutdown)?
                    },
                    _ = tokio::time::sleep(timeout) => {
                        Err(Error::IncompleteShutdown)
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::task::{CronOpts, TaskSchedule};
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
        scheduler.shutdown(ShutdownOpts::WaitForFinish).await?;

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
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Limited(1),
            GarbageCollector::default(),
        );

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
            Duration::from_millis(2950),
            Duration::from_millis(3000),
            Duration::from_millis(2950),
            Duration::from_millis(3000),
            Duration::from_millis(1),
        ];
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Limited(2),
            GarbageCollector::default(),
        );

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
    async fn once_unlimited_workers() {
        let schedules: Vec<TaskSchedule> = Vec::from([
            TaskSchedule::Once,
            TaskSchedule::Once,
            TaskSchedule::Once,
            TaskSchedule::Once,
            TaskSchedule::Once,
        ]);
        let durations = [
            Duration::from_millis(1000),
            Duration::from_millis(1200),
            Duration::from_millis(1300),
            Duration::from_millis(1400),
            Duration::from_millis(2000),
        ];
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Unlimited,
            GarbageCollector::default(),
        );

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(1))
                .await
                .unwrap();

        assert_eq!(logs.len(), 10);
        assert_eq!(jobs.len(), 5);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("1,start,{}", jobs[1]),
            format!("2,start,{}", jobs[2]),
            format!("3,start,{}", jobs[3]),
            format!("4,start,{}", jobs[4]),
            format!("0,finish,{}", jobs[0]),
            format!("1,finish,{}", jobs[1]),
            format!("2,finish,{}", jobs[2]),
            format!("3,finish,{}", jobs[3]),
            format!("4,finish,{}", jobs[4]),
        ]);
        assert_eq!(logs, expected);
    }

    #[tokio::test]
    async fn cron() {
        let schedules: Vec<TaskSchedule> = Vec::from([
            TaskSchedule::RepeatByCron("*/2 * * * * *".try_into().unwrap(), CronOpts::default()),
            TaskSchedule::RepeatByCron("*/5 * * * * *".try_into().unwrap(), CronOpts::default()),
        ]);
        let durations = [Duration::from_millis(1200), Duration::from_millis(3500)];
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Limited(2),
            GarbageCollector::default(),
        );

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
    async fn cron_at_start() {
        let schedules: Vec<TaskSchedule> = Vec::from([TaskSchedule::RepeatByCron(
            "*/5 * * * * *".try_into().unwrap(),
            CronOpts {
                at_start: true,
                concurrent: false,
            },
        )]);
        let durations = [Duration::from_millis(1000)];
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Unlimited,
            GarbageCollector::default(),
        );

        // wait for next 5 seconds interval
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let wait_for = 5 - now % 5 + 1;
        tokio::time::sleep(Duration::from_secs(wait_for)).await;

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(6))
                .await
                .unwrap();

        assert_eq!(logs.len(), 4);
        assert_eq!(jobs.len(), 2);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("0,finish,{}", jobs[0]),
            format!("0,start,{}", jobs[1]),
            format!("0,finish,{}", jobs[1]),
        ]);
        let debug = format!("jobs={:?}\nlogs={:?}\nexpected={:?}", jobs, logs, expected);
        assert!(logs == expected, "{debug}");
    }

    #[tokio::test]
    async fn cron_non_concurrent() {
        let schedules: Vec<TaskSchedule> = Vec::from([TaskSchedule::RepeatByCron(
            "*/5 * * * * *".try_into().unwrap(),
            CronOpts {
                at_start: true,
                concurrent: false,
            },
        )]);
        let durations = [Duration::from_millis(7000)];
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Unlimited,
            GarbageCollector::default(),
        );

        // wait for next 5 seconds interval
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let wait_for = 5 - now % 5 + 1;
        tokio::time::sleep(Duration::from_secs(wait_for)).await;

        let (logs, jobs) =
            basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(6))
                .await
                .unwrap();

        assert_eq!(logs.len(), 2);
        assert_eq!(jobs.len(), 1);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("0,finish,{}", jobs[0]),
        ]);
        let debug = format!("jobs={:?}\nlogs={:?}\nexpected={:?}", jobs, logs, expected);
        assert!(logs == expected, "{debug}");
    }

    #[tokio::test]
    async fn cron_concurrent() {
        tracing_subscriber::fmt::init();
        let schedules: Vec<TaskSchedule> = Vec::from([TaskSchedule::RepeatByCron(
            "*/3 * * * * *".try_into().unwrap(),
            CronOpts {
                at_start: true,
                concurrent: true,
            },
        )]);
        let durations = [Duration::from_millis(3900)];
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Unlimited,
            GarbageCollector::default(),
        );

        // wait for next 3 seconds interval
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let wait_for = 3 - now % 3 + 1;
        tokio::time::sleep(Duration::from_secs(wait_for)).await;

        let (logs, jobs) = basic_test_suite(
            scheduler,
            schedules,
            &durations,
            Duration::from_millis(5000),
        )
        .await
        .unwrap();

        assert_eq!(logs.len(), 6);
        assert_eq!(jobs.len(), 3);

        let expected: Vec<String> = Vec::from([
            format!("0,start,{}", jobs[0]),
            format!("0,start,{}", jobs[1]),
            format!("0,finish,{}", jobs[0]),
            format!("0,start,{}", jobs[2]),
            format!("0,finish,{}", jobs[1]),
            format!("0,finish,{}", jobs[2]),
        ]);
        let debug = format!("jobs={:?}\nlogs={:?}\nexpected={:?}", jobs, logs, expected);
        assert!(logs == expected, "{debug}");
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
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Limited(4),
            GarbageCollector::default(),
        );

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
        let scheduler = Scheduler::new(
            WorkerType::CurrentRuntime,
            WorkerParallelism::Limited(4),
            GarbageCollector::default(),
        );

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

    #[tokio::test]
    async fn garbage_collector() {
        let scheduler = SchedulerBuilder::new()
            .garbage_collector(GarbageCollector::enabled(
                Duration::from_millis(2500),
                Duration::from_millis(500),
            ))
            .build();

        // Every 100ms work for 2s
        let task_1 = Task::new(
            TaskSchedule::RepeatByInterval(Duration::from_millis(100)),
            |_id| {
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                })
            },
        );
        // Once work for 4s
        let task_2 = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(4)).await;
            })
        });
        // Once work for 8s
        let task_3 = Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(8)).await;
            })
        });

        let id_1 = scheduler.add(task_1).await.unwrap();
        let id_2 = scheduler.add(task_2).await.unwrap();
        let id_3 = scheduler.add(task_3).await.unwrap();

        tokio::time::sleep(Duration::from_millis(3500)).await;
        assert_eq!(scheduler.status(&id_1).await.unwrap(), TaskStatus::Running);
        assert_eq!(scheduler.status(&id_2).await.unwrap(), TaskStatus::Running);
        assert_eq!(scheduler.status(&id_3).await.unwrap(), TaskStatus::Running);

        tokio::time::sleep(Duration::from_millis(4400)).await;
        assert_eq!(scheduler.status(&id_1).await.unwrap(), TaskStatus::Running);
        assert!(scheduler.status(&id_2).await.is_err());
        assert_eq!(scheduler.status(&id_3).await.unwrap(), TaskStatus::Running);

        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert_eq!(scheduler.status(&id_1).await.unwrap(), TaskStatus::Running);
        assert!(scheduler.status(&id_2).await.is_err());
        assert_eq!(scheduler.status(&id_3).await.unwrap(), TaskStatus::Finished);

        scheduler
            .shutdown(ShutdownOpts::CancelTasks(CancelOpts::Kill))
            .await
            .unwrap();
    }
}
