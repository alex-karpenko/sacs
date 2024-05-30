use opentelemetry_otlp::WithExportConfig;
use sacs::{
    job::JobId,
    scheduler::{
        GarbageCollector, RuntimeThreads, Scheduler, ShutdownOpts, TaskScheduler,
        WorkerParallelism, WorkerType,
    },
    task::{CronOpts, Task, TaskSchedule},
    Result,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{OnceCell, RwLock};
use tracing::debug_span;
use tracing_subscriber::{filter, layer::SubscriberExt, Registry};

const DEFAULT_OPENTELEMETRY_ENDPOINT_URL: &str = "http://localhost:4317";
static TRACING_INITIALIZED: OnceCell<()> = OnceCell::const_new();

async fn init() {
    TRACING_INITIALIZED
        .get_or_init(|| async { init_tracing().await })
        .await;
}

async fn init_tracing() {
    // Setup tracing layers
    let telemetry = tracing_opentelemetry::layer().with_tracer(get_tracer().await);
    let console_logger = tracing_subscriber::fmt::layer().compact();
    let env_filter = filter::EnvFilter::try_from_default_env()
        .or(filter::EnvFilter::try_new("info"))
        .unwrap();

    // Decide on layers
    let collector = Registry::default()
        .with(telemetry)
        .with(console_logger)
        .with(env_filter);

    // Initialize tracing
    tracing::subscriber::set_global_default(collector).unwrap();
}

async fn get_tracer() -> opentelemetry_sdk::trace::Tracer {
    let otlp_endpoint = std::env::var("OPENTELEMETRY_ENDPOINT_URL")
        .unwrap_or(String::from(DEFAULT_OPENTELEMETRY_ENDPOINT_URL));

    let otlp_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(otlp_endpoint);

    let trace_config =
        opentelemetry_sdk::trace::config().with_resource(opentelemetry_sdk::Resource::new(vec![
            opentelemetry::KeyValue::new("service.name", "sacs-test-suite"),
        ]));

    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otlp_exporter)
        .with_trace_config(trace_config)
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .unwrap()
}

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
    let jobs = Arc::new(RwLock::new(Vec::<JobId>::new()));

    for s in 0..schedules.len() {
        let log = logs.clone();
        let jobs = jobs.clone();
        let task_duration = durations[s];
        let task = Task::new(schedules[s].clone(), move |id| {
            let log = log.clone();
            let jobs = jobs.clone();
            Box::pin(async move {
                jobs.write().await.push(id.clone());
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

async fn single_worker(worker_type: WorkerType) {
    let schedules: Vec<TaskSchedule> = Vec::from([
        TaskSchedule::Once,
        TaskSchedule::OnceDelayed(Duration::from_secs(1)),
        TaskSchedule::Interval(Duration::from_secs(2)),
        TaskSchedule::IntervalDelayed(Duration::from_secs(3), Duration::from_secs(3)),
        TaskSchedule::Cron("*/5 * * * * *".try_into().unwrap(), CronOpts::default()),
    ]);
    let durations = [
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(3),
        Duration::from_secs(4),
        Duration::from_secs(5),
    ];
    let scheduler = Scheduler::new(
        worker_type,
        WorkerParallelism::Limited(1),
        GarbageCollector::default(),
    );

    let (logs, jobs) = basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(15))
        .await
        .unwrap();

    assert_eq!(logs.len(), jobs.len() * 2);
}

async fn four_workers(worker_type: WorkerType) {
    let schedules: Vec<TaskSchedule> = Vec::from([
        TaskSchedule::Once,
        TaskSchedule::OnceDelayed(Duration::from_secs(1)),
        TaskSchedule::Interval(Duration::from_secs(2)),
        TaskSchedule::IntervalDelayed(Duration::from_secs(3), Duration::from_secs(3)),
        TaskSchedule::Cron("*/5 * * * * *".try_into().unwrap(), CronOpts::default()),
    ]);
    let durations = [
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(3),
        Duration::from_secs(4),
        Duration::from_secs(5),
    ];
    let scheduler = Scheduler::new(
        worker_type,
        WorkerParallelism::Limited(4),
        GarbageCollector::default(),
    );

    let (logs, jobs) = basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(15))
        .await
        .unwrap();

    assert_eq!(logs.len(), jobs.len() * 2);
}

async fn unlimited_workers(worker_type: WorkerType) {
    let schedules: Vec<TaskSchedule> = Vec::from([
        TaskSchedule::Once,
        TaskSchedule::OnceDelayed(Duration::from_secs(1)),
        TaskSchedule::Interval(Duration::from_secs(2)),
        TaskSchedule::IntervalDelayed(Duration::from_secs(3), Duration::from_secs(3)),
        TaskSchedule::Cron("*/5 * * * * *".try_into().unwrap(), CronOpts::default()),
    ]);
    let durations = [
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(3),
        Duration::from_secs(4),
        Duration::from_secs(5),
    ];
    let scheduler = Scheduler::new(
        worker_type,
        WorkerParallelism::Unlimited,
        GarbageCollector::default(),
    );

    let (logs, jobs) = basic_test_suite(scheduler, schedules, &durations, Duration::from_secs(15))
        .await
        .unwrap();

    assert_eq!(logs.len(), jobs.len() * 2);
}

#[tokio::test]
async fn single_worker_current_runtime() {
    init().await;
    let span = debug_span!("single worker current runtime");
    let _span = span.enter();

    single_worker(WorkerType::CurrentRuntime).await;
}

#[tokio::test]
async fn single_worker_current_thread() {
    init().await;
    let span = debug_span!("single worker current thread");
    let _span = span.enter();

    single_worker(WorkerType::CurrentThread).await;
}

#[tokio::test]
async fn single_worker_two_threads() {
    init().await;
    let span = debug_span!("single worker two threads");
    let _span = span.enter();

    single_worker(WorkerType::MultiThread(RuntimeThreads::Limited(4))).await;
}

#[tokio::test]
async fn single_worker_all_cores_threads() {
    init().await;
    let span = debug_span!("single worker all cores threads");
    let _span = span.enter();

    single_worker(WorkerType::MultiThread(RuntimeThreads::CpuCores)).await;
}

#[tokio::test]
async fn four_workers_current_runtime() {
    init().await;
    let span = debug_span!("four workers current runtime");
    let _span = span.enter();

    four_workers(WorkerType::CurrentRuntime).await;
}

#[tokio::test]
async fn four_workers_current_thread() {
    init().await;
    let span = debug_span!("four workers current thread");
    let _span = span.enter();

    four_workers(WorkerType::CurrentThread).await;
}

#[tokio::test]
async fn four_workers_two_threads() {
    init().await;
    let span = debug_span!("four workers two threads");
    let _span = span.enter();

    four_workers(WorkerType::MultiThread(RuntimeThreads::Limited(4))).await;
}

#[tokio::test]
async fn four_workers_all_cores_threads() {
    init().await;
    let span = debug_span!("four workers all cores threads");
    let _span = span.enter();

    four_workers(WorkerType::MultiThread(RuntimeThreads::CpuCores)).await;
}

#[tokio::test]
async fn unlimited_workers_current_runtime() {
    init().await;
    let span = debug_span!("unlimited workers current runtime");
    let _span = span.enter();

    unlimited_workers(WorkerType::CurrentRuntime).await;
}

#[tokio::test]
async fn unlimited_workers_current_thread() {
    init().await;
    let span = debug_span!("unlimited workers current thread");
    let _span = span.enter();

    unlimited_workers(WorkerType::CurrentThread).await;
}

#[tokio::test]
async fn unlimited_workers_two_threads() {
    init().await;
    let span = debug_span!("unlimited workers two threads");
    let _span = span.enter();

    unlimited_workers(WorkerType::MultiThread(RuntimeThreads::Limited(4))).await;
}

#[tokio::test]
async fn unlimited_workers_all_cores_threads() {
    init().await;
    let span = debug_span!("unlimited workers all cores threads");
    let _span = span.enter();

    unlimited_workers(WorkerType::MultiThread(RuntimeThreads::CpuCores)).await;
}
