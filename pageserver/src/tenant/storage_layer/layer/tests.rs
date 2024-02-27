use futures::StreamExt;
use tokio::task::JoinSet;
use utils::{
    completion::{self, Completion},
    id::TimelineId,
};

use super::*;
use crate::tenant::harness::TenantHarness;

#[tokio::test(start_paused = true)]
async fn residency_check_while_evict_and_wait_on_clogged_spawn_blocking() {
    let h = TenantHarness::create("residency_check_while_evict_and_wait_on_clogged_spawn_blocking")
        .unwrap();
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

    let mut evict_and_wait = std::pin::pin!(layer.evict_and_wait());

    // drive the future to await on the status channel
    tokio::time::timeout(std::time::Duration::from_secs(3600), &mut evict_and_wait)
        .await
        .expect_err("should had been a timeout since we are holding the layer resident");
    assert_eq!(1, LAYER_IMPL_METRICS.started_evictions.get());

    // make the eviction attempt a noop because the layer is also wanted as deleted.
    // we could now evict wanted deleted layers because we unlink the layers from index_part.json
    // right away, but that has not been implemented. alternatively we could cause a version
    // mismatch by incrementing the ` layer.0.version.fetch_add(1, Ordering::Relaxed)`.
    //
    // testability would be simpler if there was a separate actor for these.

    // clog up BACKGROUND_RUNTIME spawn_blocking
    let (completion, mut js) = consume_all_background_runtime_spawn_blocking_threads().await;

    // now the eviction cannot proceed because the threads are consumed while completion exists
    drop(resident);

    // this resets the wanted eviction. in the bug situation, it could had been executed by
    // eviction_task or disk usage based eviction while eviction_task was waiting on
    // `Layer::evict_and_wait`.
    //
    // because no actual eviction happened, we get to just reinitialize the DownloadedLayer
    layer
        .keep_resident()
        .await
        .expect("keep_resident should had reinitialized without downloading");

    // because the keep_resident check alters wanted evicted without sending a message, we will never get completed
    let e = tokio::time::timeout(std::time::Duration::from_secs(3600), &mut evict_and_wait)
        .await
        .expect("no timeout, because keep_resident re-initialized")
        .expect_err("eviction should not have succeeded because re-initialized");

    // works as intended: evictions lose to "downloads"
    assert!(matches!(e, EvictionError::Downloaded), "{e:?}");
    assert_eq!(0, LAYER_IMPL_METRICS.completed_evictions.get());

    // this is not wrong: the eviction is technically still "on the way" it's still queued
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

    assert_eq!(2, LAYER_IMPL_METRICS.started_evictions.get());

    drop(completion);
    while let Some(res) = js.join_next().await {
        res.unwrap();
    }

    tokio::time::timeout(std::time::Duration::from_secs(3600), &mut second_eviction)
        .await
        .expect("eviction goes through now that spawn_blocking is unclogged")
        .expect("eviction should succeed, because version matches");

    assert_eq!(1, LAYER_IMPL_METRICS.completed_evictions.get());

    // now we finally can observe the original spawn_blocking failing
    assert_eq!(
        1,
        LAYER_IMPL_METRICS
            .cancelled_evictions
            .values()
            .map(|ctr| ctr.get())
            .sum::<u64>()
    );
}

#[tokio::test(start_paused = true)]
async fn evict_and_wait_on_wanted_deleted() {
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
        cycle_background_runtime_spawn_blocking().await;

        let resident = layer.keep_resident().await.unwrap();

        assert!(
            resident.is_none(),
            "keep_resident should not have re-initialized"
        );

        // because the keep_resident check alters wanted evicted without sending a message, we will never get completed
        tokio::time::timeout(std::time::Duration::from_secs(3600), &mut evict_and_wait)
            .await
            .expect("completion")
            .expect("evict_and_wait should had succeeded");

        // works as intended
    }

    {
        let mut layers = timeline.layers.write().await;
        // layer is moved to a temporary here, then dropped
        layers.finish_gc_timeline(&[layer]);
    }

    cycle_background_runtime_spawn_blocking().await;

    assert_eq!(1, LAYER_IMPL_METRICS.started_deletes.get());
    assert_eq!(1, LAYER_IMPL_METRICS.completed_deletes.get());

    assert_eq!(1, LAYER_IMPL_METRICS.started_evictions.get());
    assert_eq!(1, LAYER_IMPL_METRICS.completed_evictions.get());
}

/// All `crate::task_mgr::BACKGROUND_RUNTIME` spawn_blocking threads will be consumed momentarily
/// by this or it will not complete.
///
/// This should be no issue nowdays, because nextest runs each test in it's own process.
async fn consume_all_background_runtime_spawn_blocking_threads() -> (Completion, JoinSet<()>) {
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
            crate::task_mgr::BACKGROUND_RUNTIME.handle(),
        );
    }

    drop(barrier);

    for _ in 0..assumed_max_blocking_threads {
        rx.recv().await.unwrap();
    }

    (completion, blocking_tasks)
}

async fn cycle_background_runtime_spawn_blocking() {
    let (_, mut js) = consume_all_background_runtime_spawn_blocking_threads().await;
    while let Some(res) = js.join_next().await {
        res.unwrap();
    }
}
