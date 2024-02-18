use sacs::{
    scheduler::{Scheduler, TaskScheduler},
    task::{Task, TaskSchedule},
    Result, WorkerType,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;
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
        })?;
        scheduler.add(task).await?;
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    tokio::time::sleep(suite_duration).await;
    scheduler.shutdown(true).await?;

    let logs: Vec<String> = logs.read().await.iter().map(String::from).collect();
    let jobs: Vec<String> = jobs.read().await.iter().map(|s| format!("{s}")).collect();

    Ok((logs, jobs))
}

async fn single_worker(worker_type: WorkerType) {
    let schedules: Vec<TaskSchedule> = Vec::from([
        TaskSchedule::Once,
        TaskSchedule::OnceDelayed(Duration::from_secs(1)),
        TaskSchedule::RepeatByInterval(Duration::from_secs(2)),
        TaskSchedule::RepeatByIntervalDelayed(Duration::from_secs(3)),
        TaskSchedule::RepeatByCron("*/5 * * * * *".try_into().unwrap()),
    ]);
    let durations = [
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(3),
        Duration::from_secs(4),
        Duration::from_secs(5),
    ];
    let scheduler = Scheduler::new(worker_type, 1);

    let (logs, jobs) = basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(15))
        .await
        .unwrap();

    assert_eq!(logs.len(), jobs.len() * 2);
}

async fn four_workers(worker_type: WorkerType) {
    let schedules: Vec<TaskSchedule> = Vec::from([
        TaskSchedule::Once,
        TaskSchedule::OnceDelayed(Duration::from_secs(1)),
        TaskSchedule::RepeatByInterval(Duration::from_secs(2)),
        TaskSchedule::RepeatByIntervalDelayed(Duration::from_secs(3)),
        TaskSchedule::RepeatByCron("*/5 * * * * *".try_into().unwrap()),
    ]);
    let durations = [
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(3),
        Duration::from_secs(4),
        Duration::from_secs(5),
    ];
    let scheduler = Scheduler::new(worker_type, 1);

    let (logs, jobs) = basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(15))
        .await
        .unwrap();

    assert_eq!(logs.len(), jobs.len() * 2);
}

#[tokio::test]
async fn single_worker_current_runtime() {
    single_worker(WorkerType::CurrentRuntime).await;
}

#[tokio::test]
async fn single_worker_current_thread() {
    single_worker(WorkerType::CurrentThread).await;
}

#[tokio::test]
async fn single_worker_two_threads() {
    single_worker(WorkerType::MultiThread(Some(4))).await;
}

#[tokio::test]
async fn single_worker_all_cores_threads() {
    single_worker(WorkerType::MultiThread(None)).await;
}

#[tokio::test]
async fn four_workers_current_runtime() {
    four_workers(WorkerType::CurrentRuntime).await;
}

#[tokio::test]
async fn four_workers_current_thread() {
    four_workers(WorkerType::CurrentThread).await;
}

#[tokio::test]
async fn four_workers_two_threads() {
    four_workers(WorkerType::MultiThread(Some(4))).await;
}

#[tokio::test]
async fn four_workers_all_cores_threads() {
    four_workers(WorkerType::MultiThread(None)).await;
}
