use futures::StreamExt;
use pageserver_api::key::CONTROLFILE_KEY;
use tokio::task::JoinSet;
use tracing::Instrument;
use utils::{
    completion::{self, Completion},
    id::TimelineId,
};

use super::*;
use crate::{context::DownloadBehavior, task_mgr::BACKGROUND_RUNTIME};
use crate::{task_mgr::TaskKind, tenant::harness::TenantHarness};

/// Used in tests to advance a future to wanted await point, and not futher.
const ADVANCE: std::time::Duration = std::time::Duration::from_secs(3600);

/// Used in tests to indicate forever long timeout; has to be longer than the amount of ADVANCE
/// timeout uses to advance futures.
const FOREVER: std::time::Duration = std::time::Duration::from_secs(ADVANCE.as_secs() * 24 * 7);

/// Demonstrate the API and resident -> evicted -> resident -> deleted transitions.
#[tokio::test]
async fn smoke_test() {
    let handle = BACKGROUND_RUNTIME.handle();

    let h = TenantHarness::create("smoke_test").unwrap();
    let span = h.span();
    let download_span = span.in_scope(|| tracing::info_span!("downloading", timeline_id = 1));
    let (tenant, _) = h.load().await;

    let ctx = RequestContext::new(TaskKind::UnitTest, DownloadBehavior::Download);

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

    // all layers created at pageserver are like `layer`, initialized with strong
    // Arc<DownloadedLayer>.

    let img_before = {
        let mut data = ValueReconstructState::default();
        layer
            .get_value_reconstruct_data(CONTROLFILE_KEY, Lsn(0x10)..Lsn(0x11), &mut data, &ctx)
            .await
            .unwrap();
        data.img
            .take()
            .expect("tenant harness writes the control file")
    };

    // important part is evicting the layer, which can be done when there are no more ResidentLayer
    // instances -- there currently are none, only two `Layer` values, one in the layermap and on
    // in scope.
    layer.evict_and_wait(FOREVER).await.unwrap();

    // double-evict returns an error, which is valid if both eviction_task and disk usage based
    // eviction would both evict the same layer at the same time.

    let e = layer.evict_and_wait(FOREVER).await.unwrap_err();
    assert!(matches!(e, EvictionError::NotFound));

    // on accesses when the layer is evicted, it will automatically be downloaded.
    let img_after = {
        let mut data = ValueReconstructState::default();
        layer
            .get_value_reconstruct_data(CONTROLFILE_KEY, Lsn(0x10)..Lsn(0x11), &mut data, &ctx)
            .instrument(download_span.clone())
            .await
            .unwrap();
        data.img.take().unwrap()
    };

    assert_eq!(img_before, img_after);

    // evict_and_wait can timeout, but it doesn't cancel the evicting itself
    //
    // ZERO for timeout does not work reliably, so first take up all spawn_blocking slots to
    // artificially slow it down.
    let helper = SpawnBlockingPoolHelper::consume_all_spawn_blocking_threads(handle).await;

    match layer
        .evict_and_wait(std::time::Duration::ZERO)
        .await
        .unwrap_err()
    {
        EvictionError::Timeout => {
            // expected, but note that the eviction is "still ongoing"
            helper.release().await;
            // exhaust spawn_blocking pool to ensure it is now complete
            SpawnBlockingPoolHelper::consume_and_release_all_of_spawn_blocking_threads(handle)
                .await;
        }
        other => unreachable!("{other:?}"),
    }

    // only way to query if a layer is resident is to acquire a ResidentLayer instance.
    // Layer::keep_resident never downloads, but it might initialize if the layer file is found
    // downloaded locally.
    let none = layer.keep_resident().await.unwrap();
    assert!(
        none.is_none(),
        "Expected none, because eviction removed the local file, found: {none:?}"
    );

    // plain downloading is rarely needed
    layer
        .download_and_keep_resident()
        .instrument(download_span)
        .await
        .unwrap();

    // last important part is deletion on drop: gc and compaction use it for compacted L0 layers
    // or fully garbage collected layers. deletion means deleting the local file, and scheduling a
    // deletion of the already unlinked from index_part.json remote file.
    //
    // marking a layer to be deleted on drop is irreversible; there is no technical reason against
    // reversiblity, but currently it is not needed so it is not provided.
    layer.delete_on_drop();

    let path = layer.local_path().to_owned();

    // wait_drop produces an unconnected to Layer future which will resolve when the
    // LayerInner::drop has completed.
    let mut wait_drop = std::pin::pin!(layer.wait_drop());

    // paused time doesn't really work well with timeouts and evict_and_wait, so delay pausing
    // until here
    tokio::time::pause();
    tokio::time::timeout(ADVANCE, &mut wait_drop)
        .await
        .expect_err("should had timed out because two strong references exist");

    tokio::fs::metadata(&path)
        .await
        .expect("the local layer file still exists");

    let rtc = timeline.remote_client.as_ref().unwrap();

    {
        let layers = &[layer];
        let mut g = timeline.layers.write().await;
        g.finish_gc_timeline(layers);
        // this just updates the remote_physical_size for demonstration purposes
        rtc.schedule_gc_update(layers).unwrap();
    }

    // when strong references are dropped, the file is deleted and remote deletion is scheduled
    wait_drop.await;

    let e = tokio::fs::metadata(&path)
        .await
        .expect_err("the local file is deleted");
    assert_eq!(e.kind(), std::io::ErrorKind::NotFound);

    rtc.wait_completion().await.unwrap();

    assert_eq!(rtc.get_remote_physical_size(), 0);
}

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
        let mut evict_and_wait = std::pin::pin!(layer.evict_and_wait(FOREVER));

        // drive the future to await on the status channel
        tokio::time::timeout(ADVANCE, &mut evict_and_wait)
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
    let span = h.span();
    let download_span = span.in_scope(|| tracing::info_span!("downloading", timeline_id = 1));

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

    let mut evict_and_wait = std::pin::pin!(layer.evict_and_wait(FOREVER));

    // drive the future to await on the status channel
    tokio::time::timeout(ADVANCE, &mut evict_and_wait)
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
        .instrument(download_span)
        .await
        .expect("keep_resident should had reinitialized without downloading")
        .expect("ResidentLayer");

    // because the keep_resident check alters wanted evicted without sending a message, we will
    // never get completed
    let e = tokio::time::timeout(ADVANCE, &mut evict_and_wait)
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

    let mut second_eviction = std::pin::pin!(layer.evict_and_wait(FOREVER));

    // advance to the wait on the queue
    tokio::time::timeout(ADVANCE, &mut second_eviction)
        .await
        .expect_err("timeout because spawn_blocking is clogged");

    // in this case we don't leak started evictions, but I think there is still a chance of that
    // happening, because we could have upgrades race multiple evictions while only one of them
    // happens?
    assert_eq!(2, LAYER_IMPL_METRICS.started_evictions.get());

    helper.release().await;

    // the second_eviction gets to run here
    //
    // synchronize to be *strictly* after the second_eviction spawn_blocking run
    SpawnBlockingPoolHelper::consume_and_release_all_of_spawn_blocking_threads(handle).await;

    tokio::time::timeout(ADVANCE, &mut second_eviction)
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

/// The test ensures with a failpoint that a pending eviction is not cancelled by what is currently
/// a `Layer::keep_resident` call.
///
/// This matters because cancelling the eviction would leave us in a state where the file is on
/// disk but the layer internal state says it has not been initialized.
#[tokio::test(start_paused = true)]
async fn cancelled_get_or_maybe_download_does_not_cancel_eviction() {
    use failpoints::FailpointKind::AfterDeterminingLayerNeedsNoDownload;

    let handle = BACKGROUND_RUNTIME.handle();
    let h =
        TenantHarness::create("cancelled_get_or_maybe_download_does_not_cancel_eviction").unwrap();
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

    let helper = SpawnBlockingPoolHelper::consume_all_spawn_blocking_threads(handle).await;

    layer
        .0
        .enable_failpoint(AfterDeterminingLayerNeedsNoDownload);

    tokio::time::timeout(ADVANCE, layer.evict_and_wait(FOREVER))
        .await
        .expect_err("should had advanced to waiting on channel");

    let e = layer
        .0
        .get_or_maybe_download(false, None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            e,
            DownloadError::Failpoint(AfterDeterminingLayerNeedsNoDownload)
        ),
        "{e:?}"
    );

    assert!(
        // cannot use tokio::fs version while spawn_blocking is clogged
        layer.0.needs_download_blocking().unwrap().is_none(),
        "file is still on disk"
    );

    helper.release().await;

    SpawnBlockingPoolHelper::consume_and_release_all_of_spawn_blocking_threads(handle).await;

    // failpoint is still enabled, but it is not hit
    let e = layer
        .0
        .get_or_maybe_download(false, None)
        .await
        .unwrap_err();
    assert!(matches!(e, DownloadError::DownloadRequired), "{e:?}");
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

#[test]
fn spawn_blocking_pool_helper_actually_works() {
    // create a custom runtime for which we know and control how many blocking threads it has
    //
    // because the amount is not configurable for our helper, expect the same amount as
    // BACKGROUND_RUNTIME using the tokio defaults would have.
    let rt = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(512)
        .enable_all()
        .build()
        .unwrap();

    let handle = rt.handle();

    rt.block_on(async move {
        // this will not return until all threads are spun up and actually executing the code
        // waiting on `consumed` to be `SpawnBlockingPoolHelper::release`'d.
        let consumed = SpawnBlockingPoolHelper::consume_all_spawn_blocking_threads(handle).await;

        println!("consumed");

        let mut jh = std::pin::pin!(tokio::task::spawn_blocking(move || {
            // this will not get to run before we release
        }));

        println!("spawned");

        tokio::time::timeout(std::time::Duration::from_secs(1), &mut jh)
            .await
            .expect_err("the task should not have gotten to run yet");

        println!("tried to join");

        consumed.release().await;

        println!("released");

        tokio::time::timeout(std::time::Duration::from_secs(1), jh)
            .await
            .expect("no timeout")
            .expect("no join error");

        println!("joined");
    });
}
