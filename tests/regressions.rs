//! Regression tests for the findings from the 2026-08-30 code reviews
//! (`code-review/sacs/20260830-{opencode,antygravity,claude}-v1.md`) and the corresponding
//! fix plan (`20260830-regression-fix-plan-v1.md`). Test names are prefixed with the
//! finding id(s) they cover, matching the plan's finding-to-step coverage matrix.
//!
//! A handful of findings that need crate-private access (`pub(crate)` test seams, or private
//! functions) are covered as unit tests inside the relevant `src/*.rs` module instead; see
//! `n3_job_index_does_not_grow`, `o5_gc_*` and `n7_operations_after_termination_*` in
//! `src/scheduler.rs`.

use ntest::timeout;
use sacs::{
    scheduler::{
        CancelOpts, GarbageCollector, RuntimeThreads, Scheduler, SchedulerBuilder, ShutdownOpts,
        TaskScheduler, WorkerParallelism, WorkerType,
    },
    task::{CronOpts, CronSchedule, Task, TaskId, TaskSchedule, TaskStatus},
    Error, Result,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

// ---------------------------------------------------------------------------------------
// Step 0: cron-lite 0.4.0 upgrade -- parse-time amplification vector closed.
// ---------------------------------------------------------------------------------------

#[test]
fn step0_cron_pattern_flood_is_rejected() {
    let flood: String = (0..2000)
        .map(|i| (i % 60).to_string())
        .collect::<Vec<_>>()
        .join(",");
    let r: Result<CronSchedule> = format!("{flood} * * * * *").as_str().try_into();
    assert!(matches!(r, Err(Error::InvalidCronExpression(_))));
}

// ---------------------------------------------------------------------------------------
// Step 1 / O2: a cron expression with no upcoming occurrence must not panic the scheduler.
// ---------------------------------------------------------------------------------------

#[tokio::test]
#[timeout(10000)]
async fn o2_impossible_calendar_date_is_rejected() {
    let scheduler = Scheduler::default();

    // February 30th never exists.
    let cron = TaskSchedule::Cron("0 0 0 30 2 *".try_into().unwrap(), CronOpts::default());
    let task = Task::new(cron, |_id| Box::pin(async move {}));
    let result = scheduler.add(task).await;
    assert!(matches!(result, Err(Error::NoUpcomingEvent)), "{result:?}");

    // The scheduler loop must still be alive: prove it by running a normal task.
    let healthy = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
    let id = scheduler.add(healthy).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(scheduler.status(&id).await.unwrap(), TaskStatus::Finished);

    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

#[tokio::test]
#[timeout(10000)]
async fn o2_past_only_year_is_rejected() {
    let scheduler = Scheduler::default();

    let cron = TaskSchedule::Cron("0 0 0 1 1 ? 2020".try_into().unwrap(), CronOpts::default());
    let task = Task::new(cron, |_id| Box::pin(async move {}));
    let result = scheduler.add(task).await;
    assert!(matches!(result, Err(Error::NoUpcomingEvent)), "{result:?}");

    let healthy = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
    let id = scheduler.add(healthy).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(scheduler.status(&id).await.unwrap(), TaskStatus::Finished);

    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

#[tokio::test]
#[timeout(10000)]
async fn o2_finite_cron_finishes_normally() {
    let scheduler = Scheduler::default();
    let ran = Arc::new(AtomicUsize::new(0));

    // `at_start: true` bypasses `upcoming()` at admission, so this is accepted even though
    // the schedule itself (a past-only year) has no upcoming occurrence. After the first
    // (immediate) run, rescheduling must yield "no more events" rather than panicking, and
    // the task must settle into `Finished`.
    let ran_clone = ran.clone();
    let cron = TaskSchedule::Cron(
        "0 0 0 1 1 ? 2020".try_into().unwrap(),
        CronOpts {
            at_start: true,
            concurrent: false,
        },
    );
    let task = Task::new(cron, move |_id| {
        let ran = ran_clone.clone();
        Box::pin(async move {
            ran.fetch_add(1, Ordering::SeqCst);
        })
    });

    let id = scheduler.add(task).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert_eq!(ran.load(Ordering::SeqCst), 1);
    assert_eq!(scheduler.status(&id).await.unwrap(), TaskStatus::Finished);

    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

// ---------------------------------------------------------------------------------------
// Step 5 / O1 + N9: schedule duration overflow must be rejected, not panic.
// ---------------------------------------------------------------------------------------

#[tokio::test]
#[timeout(5000)]
async fn o1_once_delayed_overflow_is_rejected() {
    let scheduler = Scheduler::default();

    let task = Task::new(TaskSchedule::OnceDelayed(Duration::MAX), |_id| {
        Box::pin(async move {})
    });
    let result = scheduler.add(task).await;
    assert!(matches!(result, Err(Error::ScheduleOverflow)), "{result:?}");

    let healthy = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
    let id = scheduler.add(healthy).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(scheduler.status(&id).await.unwrap(), TaskStatus::Finished);

    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

#[tokio::test]
#[timeout(5000)]
async fn n9_interval_delayed_validates_both_fields_at_admission() {
    let scheduler = Scheduler::default();

    // Delay is valid, but the periodic interval overflows: a partial fix (validating only
    // the initial delay) would accept this and only fail -- silently -- at the first
    // reschedule. It must be rejected right here, at admission.
    let task = Task::new(
        TaskSchedule::IntervalDelayed(Duration::MAX, Duration::from_millis(10)),
        |_id| Box::pin(async move {}),
    );
    let result = scheduler.add(task).await;
    assert!(matches!(result, Err(Error::ScheduleOverflow)), "{result:?}");

    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

// ---------------------------------------------------------------------------------------
// Step 2 / N1: `CancelOpts::Kill` must cancel a job still pending in the executor, not let
// it run once capacity frees up.
// ---------------------------------------------------------------------------------------

#[tokio::test]
#[timeout(15000)]
async fn n1_cancel_removes_pending_job() {
    let scheduler = SchedulerBuilder::new()
        .parallelism(WorkerParallelism::Limited(1))
        .build();

    let blocker = Task::new(TaskSchedule::Once, |_id| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
        })
    });
    let victim_ran = Arc::new(AtomicUsize::new(0));
    let victim_ran_clone = victim_ran.clone();
    let victim = Task::new(TaskSchedule::Once, move |_id| {
        let victim_ran = victim_ran_clone.clone();
        Box::pin(async move {
            victim_ran.fetch_add(1, Ordering::SeqCst);
        })
    });

    scheduler.add(blocker).await.unwrap();
    // With `Limited(1)` and the blocker already occupying the only slot, this stays
    // `Pending` in the executor -- never handed to the worker.
    let victim_id = scheduler.add(victim).await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;
    scheduler.cancel(victim_id, CancelOpts::Kill).await.unwrap();

    // Let the blocker finish (freeing the slot) well past that point.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        victim_ran.load(Ordering::SeqCst),
        0,
        "a canceled pending job must never run"
    );

    let _ = scheduler
        .shutdown(ShutdownOpts::CancelTasks(CancelOpts::Kill))
        .await;
}

#[tokio::test]
#[timeout(15000)]
async fn n1_cancel_pending_frees_the_slot() {
    let scheduler = SchedulerBuilder::new()
        .parallelism(WorkerParallelism::Limited(1))
        .build();

    let blocker = Task::new(TaskSchedule::Once, |_id| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(800)).await;
        })
    });
    let victim = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
    let third_ran = Arc::new(AtomicUsize::new(0));
    let third_ran_clone = third_ran.clone();
    let third = Task::new(TaskSchedule::Once, move |_id| {
        let third_ran = third_ran_clone.clone();
        Box::pin(async move {
            third_ran.fetch_add(1, Ordering::SeqCst);
        })
    });

    scheduler.add(blocker).await.unwrap();
    let victim_id = scheduler.add(victim).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    scheduler.cancel(victim_id, CancelOpts::Kill).await.unwrap();
    let third_id = scheduler.add(third).await.unwrap();

    tokio::time::sleep(Duration::from_millis(2000)).await;
    // Proves the canceled job was actually drained from `pending`, not merely skipped: the
    // slot it would have wrongly occupied is available for the next task instead.
    assert_eq!(third_ran.load(Ordering::SeqCst), 1);
    assert_eq!(
        scheduler.status(&third_id).await.unwrap(),
        TaskStatus::Finished
    );

    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

// ---------------------------------------------------------------------------------------
// Step 3 / N4: a panic in the task *factory closure* must not kill the worker loop.
// ---------------------------------------------------------------------------------------

fn panicking_task() -> Task {
    Task::new(
        TaskSchedule::Once,
        |_id| -> Pin<Box<dyn Future<Output = ()> + Send>> {
            panic!("factory panics intentionally for test");
        },
    )
}

async fn assert_factory_panic_does_not_kill_worker(worker_type: WorkerType) {
    let scheduler = SchedulerBuilder::new().worker_type(worker_type).build();

    // Admission never invokes the factory, only the worker loop does when the job starts.
    let bad_id = scheduler.add(panicking_task()).await.unwrap();

    let healthy_ran = Arc::new(AtomicUsize::new(0));
    let healthy_ran_clone = healthy_ran.clone();
    let healthy = Task::new(TaskSchedule::Once, move |_id| {
        let healthy_ran = healthy_ran_clone.clone();
        Box::pin(async move {
            healthy_ran.fetch_add(1, Ordering::SeqCst);
        })
    });
    let healthy_id = scheduler.add(healthy).await.unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(
        healthy_ran.load(Ordering::SeqCst),
        1,
        "worker must survive a factory panic and keep running other jobs"
    );
    assert_eq!(
        scheduler.status(&healthy_id).await.unwrap(),
        TaskStatus::Finished
    );
    assert_eq!(
        scheduler.status(&bad_id).await.unwrap(),
        TaskStatus::Finished
    );

    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

#[tokio::test]
#[timeout(10000)]
async fn n4_factory_panic_does_not_kill_worker_current_runtime() {
    assert_factory_panic_does_not_kill_worker(WorkerType::CurrentRuntime).await;
}

#[tokio::test]
#[timeout(10000)]
async fn n4_factory_panic_does_not_kill_worker_current_thread() {
    assert_factory_panic_does_not_kill_worker(WorkerType::CurrentThread).await;
}

#[tokio::test]
#[timeout(10000)]
async fn n4_factory_panic_does_not_kill_worker_multi_thread() {
    assert_factory_panic_does_not_kill_worker(WorkerType::MultiThread(RuntimeThreads::CpuCores))
        .await;
}

// ---------------------------------------------------------------------------------------
// Step 4 / O3 + N2: duplicate `TaskId` admission must be atomic.
// ---------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[timeout(30000)]
async fn o3_concurrent_add_same_id() {
    for _ in 0..20 {
        let scheduler = Arc::new(Scheduler::default());
        let id = TaskId::from(format!("dup-{}", uuid::Uuid::new_v4()));

        let ran_a = Arc::new(AtomicUsize::new(0));
        let ran_b = Arc::new(AtomicUsize::new(0));

        let ran_a_clone = ran_a.clone();
        let task_a = Task::new(TaskSchedule::Once, move |_id| {
            let ran_a = ran_a_clone.clone();
            Box::pin(async move {
                ran_a.fetch_add(1, Ordering::SeqCst);
            })
        })
        .with_id(id.clone());

        let ran_b_clone = ran_b.clone();
        let task_b = Task::new(TaskSchedule::Once, move |_id| {
            let ran_b = ran_b_clone.clone();
            Box::pin(async move {
                ran_b.fetch_add(1, Ordering::SeqCst);
            })
        })
        .with_id(id.clone());

        let s1 = scheduler.clone();
        let s2 = scheduler.clone();
        let h1 = tokio::spawn(async move { s1.add(task_a).await });
        let h2 = tokio::spawn(async move { s2.add(task_b).await });
        let (r1, r2) = tokio::join!(h1, h2);
        let r1 = r1.unwrap();
        let r2 = r2.unwrap();

        let ok_count = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
        let err_count = [&r1, &r2]
            .iter()
            .filter(|r| matches!(r, Err(Error::DuplicatedTaskId(_))))
            .count();
        assert_eq!(ok_count, 1, "exactly one add must succeed: {r1:?} {r2:?}");
        assert_eq!(
            err_count, 1,
            "exactly one add must fail with DuplicatedTaskId: {r1:?} {r2:?}"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
        let total_runs = ran_a.load(Ordering::SeqCst) + ran_b.load(Ordering::SeqCst);
        assert_eq!(
            total_runs, 1,
            "the accepted closure must run exactly once, the rejected one never"
        );

        let scheduler = Arc::try_unwrap(scheduler).expect("no other Arc handles should remain");
        let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
    }
}

#[tokio::test]
#[timeout(5000)]
async fn o3_sequential_add_same_id_still_rejected() {
    let scheduler = Scheduler::default();
    let task = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {})).with_id("SEQ_ID");
    let task2 = task.clone();

    scheduler.add(task).await.unwrap();
    let result = scheduler.add(task2).await;
    assert!(
        matches!(result, Err(Error::DuplicatedTaskId(_))),
        "{result:?}"
    );

    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

// ---------------------------------------------------------------------------------------
// Step 6 / O4: `ShutdownOpts::IgnoreRunning` must let running jobs finish on every worker
// type, without blocking the caller.
// ---------------------------------------------------------------------------------------

async fn assert_ignore_running_lets_job_finish(worker_type: WorkerType) {
    let scheduler = SchedulerBuilder::new().worker_type(worker_type).build();
    let completed = Arc::new(AtomicUsize::new(0));
    let completed_clone = completed.clone();
    let task = Task::new(TaskSchedule::Once, move |_id| {
        let completed = completed_clone.clone();
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(700)).await;
            completed.fetch_add(1, Ordering::SeqCst);
        })
    });
    scheduler.add(task).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let start = std::time::Instant::now();
    scheduler
        .shutdown(ShutdownOpts::IgnoreRunning)
        .await
        .unwrap();
    assert!(
        start.elapsed() < Duration::from_millis(400),
        "IgnoreRunning must return promptly, took {:?}",
        start.elapsed()
    );

    tokio::time::sleep(Duration::from_millis(900)).await;
    assert_eq!(
        completed.load(Ordering::SeqCst),
        1,
        "job must be allowed to finish under IgnoreRunning"
    );
}

#[tokio::test]
#[timeout(10000)]
async fn o4_ignore_running_lets_jobs_finish_current_runtime() {
    assert_ignore_running_lets_job_finish(WorkerType::CurrentRuntime).await;
}

#[tokio::test]
#[timeout(10000)]
async fn o4_ignore_running_lets_jobs_finish_current_thread() {
    assert_ignore_running_lets_job_finish(WorkerType::CurrentThread).await;
}

#[tokio::test]
#[timeout(10000)]
async fn o4_ignore_running_lets_jobs_finish_multi_thread() {
    assert_ignore_running_lets_job_finish(WorkerType::MultiThread(RuntimeThreads::CpuCores)).await;
}

// ---------------------------------------------------------------------------------------
// Step 8 / A9 + A10: `cancel()` must report unknown ids and complete synchronously.
// ---------------------------------------------------------------------------------------

#[tokio::test]
#[timeout(5000)]
async fn a9_cancel_unknown_id_errors() {
    let scheduler = Scheduler::default();
    let result = scheduler
        .cancel(TaskId::from("does-not-exist"), CancelOpts::Kill)
        .await;
    assert!(
        matches!(result, Err(Error::IncorrectTaskId(_))),
        "{result:?}"
    );
    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

#[tokio::test]
#[timeout(5000)]
async fn a9_cancel_running_task_still_ok() {
    let scheduler = Scheduler::default();
    let task = Task::new(TaskSchedule::Once, |_id| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
        })
    });
    let id = scheduler.add(task).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let result = scheduler.cancel(id, CancelOpts::Kill).await;
    assert!(result.is_ok(), "{result:?}");
    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

#[tokio::test]
#[timeout(15000)]
async fn a10_cancel_then_readd_same_id() {
    let scheduler = Scheduler::default();

    fn make_task(id: TaskId) -> Task {
        Task::new(TaskSchedule::Once, |_id| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(300)).await;
            })
        })
        .with_id(id)
    }

    for i in 0..20 {
        let id = TaskId::from(format!("a10-{i}"));
        scheduler.add(make_task(id.clone())).await.unwrap();
        scheduler
            .cancel(id.clone(), CancelOpts::Kill)
            .await
            .unwrap();
        let readd = scheduler.add(make_task(id)).await;
        assert!(
            readd.is_ok(),
            "re-add right after cancel().await must succeed, not race: {readd:?}"
        );
    }

    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

#[tokio::test]
#[timeout(5000)]
async fn a10_cancel_then_status() {
    let scheduler = Scheduler::default();
    let task = Task::new(TaskSchedule::Once, |_id| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
        })
    });
    let id = scheduler.add(task).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    scheduler
        .cancel(id.clone(), CancelOpts::Kill)
        .await
        .unwrap();

    let status = scheduler.status(&id).await;
    assert!(
        matches!(status, Err(Error::IncorrectTaskId(_))),
        "status() right after cancel().await must already reflect removal: {status:?}"
    );

    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

// ---------------------------------------------------------------------------------------
// Step 10 / O6: fallible worker/scheduler construction.
// ---------------------------------------------------------------------------------------

#[test]
fn o6_try_build_rejects_zero_threads() {
    let result = SchedulerBuilder::new()
        .worker_type(WorkerType::MultiThread(RuntimeThreads::Limited(0)))
        .try_build();
    assert!(
        matches!(result, Err(Error::InvalidWorkerConfig)),
        "{result:?}"
    );
}

#[test]
#[should_panic(expected = "invalid worker configuration")]
fn o6_build_panics_on_zero_threads() {
    let _ = SchedulerBuilder::new()
        .worker_type(WorkerType::MultiThread(RuntimeThreads::Limited(0)))
        .build();
}

#[tokio::test]
#[timeout(10000)]
async fn o6_try_build_succeeds_for_valid_worker_types() {
    for worker_type in [
        WorkerType::CurrentRuntime,
        WorkerType::CurrentThread,
        WorkerType::MultiThread(RuntimeThreads::Limited(2)),
    ] {
        let scheduler = SchedulerBuilder::new().worker_type(worker_type).try_build();
        assert!(scheduler.is_ok());
        let _ = scheduler
            .unwrap()
            .shutdown(ShutdownOpts::IgnoreRunning)
            .await;
    }
}

// ---------------------------------------------------------------------------------------
// Step 11 / N5 + N6 + A14: no lost events and no deadlock under channel saturation.
// ---------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[timeout(60000)]
async fn n6_saturation_does_not_deadlock_unlimited() {
    let scheduler = Arc::new(
        SchedulerBuilder::new()
            .worker_type(WorkerType::MultiThread(RuntimeThreads::CpuCores))
            .parallelism(WorkerParallelism::Unlimited)
            .garbage_collector(GarbageCollector::immediate())
            .build(),
    );

    const N: usize = 2000;
    let completed = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let scheduler = scheduler.clone();
        let completed = completed.clone();
        handles.push(tokio::spawn(async move {
            let completed = completed.clone();
            let task = Task::new(TaskSchedule::Once, move |_id| {
                let completed = completed.clone();
                Box::pin(async move {
                    completed.fetch_add(1, Ordering::SeqCst);
                })
            });
            scheduler.add(task).await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }

    // A deadlock fails this via the test's #[timeout]; a lost event fails the count assert.
    while completed.load(Ordering::SeqCst) < N {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(completed.load(Ordering::SeqCst), N);

    let scheduler = Arc::try_unwrap(scheduler).expect("no other Arc handles should remain");
    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

#[tokio::test(flavor = "multi_thread")]
#[timeout(30000)]
async fn n6_saturation_does_not_deadlock_limited_parallelism() {
    let scheduler = Arc::new(
        SchedulerBuilder::new()
            .worker_type(WorkerType::MultiThread(RuntimeThreads::Limited(2)))
            .parallelism(WorkerParallelism::Limited(4))
            .build(),
    );

    const N: usize = 400;
    let completed = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let scheduler = scheduler.clone();
        let completed = completed.clone();
        handles.push(tokio::spawn(async move {
            let completed = completed.clone();
            let task = Task::new(TaskSchedule::Once, move |_id| {
                let completed = completed.clone();
                Box::pin(async move {
                    completed.fetch_add(1, Ordering::SeqCst);
                })
            });
            scheduler.add(task).await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }

    while completed.load(Ordering::SeqCst) < N {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(completed.load(Ordering::SeqCst), N);

    let scheduler = Arc::try_unwrap(scheduler).expect("no other Arc handles should remain");
    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

// ---------------------------------------------------------------------------------------
// Step 12 / N7: `executor.state()` returning an error the scheduler's executor-event branch
// doesn't recognize a job id for (e.g. its terminal state was already consumed by a
// previous loop iteration) must be logged and skipped, not propagated -- it must not end
// the scheduler's background loop. That specific race is narrow and not deterministically
// triggerable through the public API alone, so this stresses the same code region (heavy
// concurrent completion/cancellation churn under limited parallelism, repeated many times)
// and proves the scheduler is still alive *and* still actually processing tasks afterward --
// the property that would break if a future change turned that `continue` back into a `?`.
// See also `n7_operations_after_termination_report_scheduler_terminated` in
// `src/scheduler.rs`, which covers the companion "loop already dead" half of this finding.
// ---------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[timeout(30000)]
async fn n7_unknown_job_state_does_not_kill_loop() {
    let scheduler = Arc::new(
        SchedulerBuilder::new()
            .worker_type(WorkerType::MultiThread(RuntimeThreads::Limited(2)))
            .parallelism(WorkerParallelism::Limited(4))
            .build(),
    );

    const N: usize = 300;
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let scheduler = scheduler.clone();
        handles.push(tokio::spawn(async move {
            let task = Task::new(TaskSchedule::Once, |_id| Box::pin(async move {}));
            let id = scheduler.add(task).await?;
            if i % 2 == 0 {
                // Race a `Kill` against the job's own near-instant natural completion.
                let _ = scheduler.cancel(id, CancelOpts::Kill).await;
            }
            Result::<()>::Ok(())
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }

    assert!(
        scheduler.is_alive(),
        "scheduler must survive concurrent completion/cancellation churn"
    );

    // Being "alive" isn't enough on its own -- prove the loop is still actually processing
    // new work, not merely that its `JoinHandle` hasn't finished yet.
    let completed = Arc::new(AtomicUsize::new(0));
    let completed_clone = completed.clone();
    let proof_task = Task::new(TaskSchedule::Once, move |_id| {
        let completed = completed_clone.clone();
        Box::pin(async move {
            completed.fetch_add(1, Ordering::SeqCst);
        })
    });
    scheduler.add(proof_task).await.unwrap();
    while completed.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let scheduler = Arc::try_unwrap(scheduler).expect("no other Arc handles should remain");
    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

// ---------------------------------------------------------------------------------------
// Step 16 / N10: the `tz` feature is otherwise only compile-tested by feature-powerset CI;
// this proves a `TZ=`-prefixed cron schedule actually drives real scheduling end-to-end
// rather than merely parsing.
// ---------------------------------------------------------------------------------------

#[cfg(feature = "tz")]
#[tokio::test]
#[timeout(10000)]
async fn n10_tz_cron_schedule_drives_real_scheduling() {
    let scheduler = Scheduler::default();
    let ran = Arc::new(AtomicUsize::new(0));
    let ran_clone = ran.clone();

    let cron = TaskSchedule::Cron(
        "TZ=UTC * * * * * *".try_into().unwrap(),
        CronOpts::default(),
    );
    let task = Task::new(cron, move |_id| {
        let ran = ran_clone.clone();
        Box::pin(async move {
            ran.fetch_add(1, Ordering::SeqCst);
        })
    });
    scheduler.add(task).await.unwrap();

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        ran.load(Ordering::SeqCst) >= 1,
        "a TZ=-prefixed cron schedule must actually run jobs, not just parse"
    );

    let _ = scheduler.shutdown(ShutdownOpts::IgnoreRunning).await;
}

// ---------------------------------------------------------------------------------------
// Step 13 / A11: the `JoinSet`-based worker loop must handle many concurrent jobs and
// cancellations with exact bookkeeping -- no lost completions, no double-counted
// cancellations, no panics from the removed fake pending-task/index-vector machinery.
// ---------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
#[timeout(20000)]
async fn a11_cancel_under_load() {
    let scheduler = Arc::new(
        SchedulerBuilder::new()
            .worker_type(WorkerType::MultiThread(RuntimeThreads::CpuCores))
            .parallelism(WorkerParallelism::Unlimited)
            .build(),
    );

    const N: usize = 200;
    let completed = Arc::new(AtomicUsize::new(0));

    let mut ids = Vec::with_capacity(N);
    for _ in 0..N {
        let completed = completed.clone();
        let task = Task::new(TaskSchedule::Once, move |_id| {
            let completed = completed.clone();
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                completed.fetch_add(1, Ordering::SeqCst);
            })
        });
        ids.push(scheduler.add(task).await.unwrap());
    }

    // Cancel every third job shortly after admission, while the rest are still in flight.
    // Issued concurrently (not in a sequential loop) so the whole batch lands in a tight
    // window near t=20ms instead of being smeared across however long N/3 sequential
    // round-trips through the scheduler's single-threaded actor loop take: on a slow or
    // loaded CI runner that smear could eat into the 200ms job duration and let a
    // late-canceled job complete first, since `cancel()` on an already-finished-but-not-
    // yet-reaped task still reports `Ok` (see `ChangeStateEvent::DropTask` in
    // `src/scheduler.rs`, where a finished task's `jobs()` is empty so there's nothing to
    // cancel), which would double count that job in both `completed` and `canceled`.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let cancel_handles: Vec<_> = ids
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 3 == 0)
        .map(|(_, id)| {
            let scheduler = scheduler.clone();
            let id = id.clone();
            tokio::spawn(async move { scheduler.cancel(id, CancelOpts::Kill).await.is_ok() })
        })
        .collect();
    let mut canceled = 0;
    for h in cancel_handles {
        if h.await.unwrap() {
            canceled += 1;
        }
    }

    tokio::time::sleep(Duration::from_millis(800)).await;

    let expected_completed = N - canceled;
    assert_eq!(
        completed.load(Ordering::SeqCst),
        expected_completed,
        "exactly the non-canceled jobs must complete (canceled={canceled})"
    );

    let scheduler = Arc::try_unwrap(scheduler).expect("no other Arc handles should remain");
    let _ = scheduler
        .shutdown(ShutdownOpts::CancelTasks(CancelOpts::Kill))
        .await;
}
