use futures::StreamExt;
use tokio::task::JoinSet;
use utils::{
    completion::{self, Completion},
    id::TimelineId,
};

use super::*;
use crate::task_mgr::BACKGROUND_RUNTIME;
use crate::tenant::harness::TenantHarness;

/// This test demonstrates a previous hang when a eviction and deletion were requested at the same
/// time. Now both of them complete per Arc drop semantics.
#[tokio::test(start_paused = true)]
async fn evict_and_wait_on_wanted_deleted() {
    // this is the runtime on which Layer spawns the blocking tasks on
    let handle = BACKGROUND_RUNTIME.handle();

    let h = TenantHarness::create("evict_and_wait_on_wanted_deleted").unwrap();
    utils::logging::replace_panic_hook_with_tracing_panic_hook().forget();
    let (tenant, ctx) = h.load().await;

    let timeline = tenant
        .create_test_timeline(TimelineId::generate(), Lsn(0x10), 14, &ctx)
        .await
        .unwrap();

    let layer = {
        let mut layers = {
            let layers = timeline.layers.read().await;
            layers.resident_layers().collect::<Vec<_>>().await
        };

        assert_eq!(layers.len(), 1);

        layers.swap_remove(0)
    };

    // setup done

    let resident = layer.keep_resident().await.unwrap();

    {
        let mut evict_and_wait = std::pin::pin!(layer.evict_and_wait());

        // drive the future to await on the status channel
        tokio::time::timeout(std::time::Duration::from_secs(3600), &mut evict_and_wait)
            .await
            .expect_err("should had been a timeout since we are holding the layer resident");

        layer.delete_on_drop();

        drop(resident);

        // make sure the eviction task gets to run
        SpawnBlockingPoolHelper::consume_and_release_all_of_spawn_blocking_threads(handle).await;

        let resident = layer.keep_resident().await;
        assert!(
            matches!(resident, Ok(None)),
            "keep_resident should not have re-initialized: {resident:?}"
        );

        evict_and_wait
            .await
            .expect("evict_and_wait should had succeeded");

        // works as intended
    }

    // assert that once we remove the `layer` from the layer map and drop our reference,
    // the deletion of the layer in remote_storage happens.
    {
        let mut layers = timeline.layers.write().await;
        layers.finish_gc_timeline(&[layer]);
    }

    SpawnBlockingPoolHelper::consume_and_release_all_of_spawn_blocking_threads(handle).await;

    assert_eq!(1, LAYER_IMPL_METRICS.started_deletes.get());
    assert_eq!(1, LAYER_IMPL_METRICS.completed_deletes.get());
    assert_eq!(1, LAYER_IMPL_METRICS.started_evictions.get());
    assert_eq!(1, LAYER_IMPL_METRICS.completed_evictions.get());
}

/// This test shows that ensures we are able to read the layer while the layer eviction has been
/// started but not completed due to spawn_blocking pool being blocked.
///
/// Here `Layer::keep_resident` is used to "simulate" reads, because it cannot download.
#[tokio::test(start_paused = true)]
async fn residency_check_while_evict_and_wait_on_clogged_spawn_blocking() {
    // this is the runtime on which Layer spawns the blocking tasks on
    let handle = BACKGROUND_RUNTIME.handle();
    let h = TenantHarness::create("residency_check_while_evict_and_wait_on_clogged_spawn_blocking")
        .unwrap();
    let (tenant, ctx) = h.load().await;

    let timeline = tenant
        .create_test_timeline(TimelineId::generate(), Lsn(0x10), 14, &ctx)
        .await
        .unwrap();

    let layer = {
        let mut layers = {
            let layers = timeline.layers.read().await;
            layers.resident_layers().collect::<Vec<_>>().await
        };

        assert_eq!(layers.len(), 1);

        layers.swap_remove(0)
    };

    // setup done

    let resident = layer.keep_resident().await.unwrap();

    let mut evict_and_wait = std::pin::pin!(layer.evict_and_wait());

    // drive the future to await on the status channel
    tokio::time::timeout(std::time::Duration::from_secs(3600), &mut evict_and_wait)
        .await
        .expect_err("should had been a timeout since we are holding the layer resident");
    assert_eq!(1, LAYER_IMPL_METRICS.started_evictions.get());

    // clog up BACKGROUND_RUNTIME spawn_blocking
    let helper = SpawnBlockingPoolHelper::consume_all_spawn_blocking_threads(handle).await;

    // now the eviction cannot proceed because the threads are consumed while completion exists
    drop(resident);

    // because no actual eviction happened, we get to just reinitialize the DownloadedLayer
    layer
        .keep_resident()
        .await
        .expect("keep_resident should had reinitialized without downloading")
        .expect("ResidentLayer");

    // because the keep_resident check alters wanted evicted without sending a message, we will
    // never get completed
    let e = tokio::time::timeout(std::time::Duration::from_secs(3600), &mut evict_and_wait)
        .await
        .expect("no timeout, because keep_resident re-initialized")
        .expect_err("eviction should not have succeeded because re-initialized");

    // works as intended: evictions lose to "downloads"
    assert!(matches!(e, EvictionError::Downloaded), "{e:?}");
    assert_eq!(0, LAYER_IMPL_METRICS.completed_evictions.get());

    // this is not wrong: the eviction is technically still "on the way" as it's still queued
    // because spawn_blocking is clogged up
    assert_eq!(
        0,
        LAYER_IMPL_METRICS
            .cancelled_evictions
            .values()
            .map(|ctr| ctr.get())
            .sum::<u64>()
    );

    let mut second_eviction = std::pin::pin!(layer.evict_and_wait());

    tokio::time::timeout(std::time::Duration::from_secs(3600), &mut second_eviction)
        .await
        .expect_err("timeout because spawn_blocking is clogged");

    // in this case we don't leak started evictions, but I think there is still a chance of that
    // happening, because we could have upgrades race multiple evictions while only one of them
    // happens?
    assert_eq!(2, LAYER_IMPL_METRICS.started_evictions.get());

    helper.release().await;

    tokio::time::timeout(std::time::Duration::from_secs(3600), &mut second_eviction)
        .await
        .expect("eviction goes through now that spawn_blocking is unclogged")
        .expect("eviction should succeed, because version matches");

    assert_eq!(1, LAYER_IMPL_METRICS.completed_evictions.get());

    // now we finally can observe the original spawn_blocking failing
    // it would had been possible to observe it earlier, but here it is guaranteed to have
    // happened.
    assert_eq!(
        1,
        LAYER_IMPL_METRICS
            .cancelled_evictions
            .values()
            .map(|ctr| ctr.get())
            .sum::<u64>()
    );
}

struct SpawnBlockingPoolHelper {
    awaited_by_spawn_blocking_tasks: Completion,
    blocking_tasks: JoinSet<()>,
}

impl SpawnBlockingPoolHelper {
    /// All `crate::task_mgr::BACKGROUND_RUNTIME` spawn_blocking threads will be consumed until
    /// release is called.
    ///
    /// In the tests this can be used to ensure something cannot be started on the target runtimes
    /// spawn_blocking pool.
    ///
    /// This should be no issue nowdays, because nextest runs each test in it's own process.
    async fn consume_all_spawn_blocking_threads(handle: &tokio::runtime::Handle) -> Self {
        let (completion, barrier) = completion::channel();
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);

        let assumed_max_blocking_threads = 512;

        let mut blocking_tasks = JoinSet::new();

        for _ in 0..assumed_max_blocking_threads {
            let barrier = barrier.clone();
            let tx = tx.clone();
            blocking_tasks.spawn_blocking_on(
                move || {
                    tx.blocking_send(()).unwrap();
                    drop(tx);
                    tokio::runtime::Handle::current().block_on(barrier.wait());
                },
                handle,
            );
        }

        drop(barrier);

        for _ in 0..assumed_max_blocking_threads {
            rx.recv().await.unwrap();
        }

        SpawnBlockingPoolHelper {
            awaited_by_spawn_blocking_tasks: completion,
            blocking_tasks,
        }
    }

    /// Release all previously blocked spawn_blocking threads
    async fn release(self) {
        let SpawnBlockingPoolHelper {
            awaited_by_spawn_blocking_tasks,
            mut blocking_tasks,
        } = self;

        drop(awaited_by_spawn_blocking_tasks);

        while let Some(res) = blocking_tasks.join_next().await {
            res.expect("none of the tasks should had panicked");
        }
    }

    /// In the tests it is used as an easy way of making sure something scheduled on the target
    /// runtimes `spawn_blocking` has completed, because it must've been scheduled and completed
    /// before our tasks have a chance to schedule and complete.
    async fn consume_and_release_all_of_spawn_blocking_threads(handle: &tokio::runtime::Handle) {
        Self::consume_all_spawn_blocking_threads(handle)
            .await
            .release()
            .await
    }
}
