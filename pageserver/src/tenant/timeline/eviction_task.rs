//! The per-timeline layer eviction task, which evicts data which has not been accessed for more
//! than a given threshold.
//!
//! Data includes all kinds of caches, namely:
//! - (in-memory layers)
//! - on-demand downloaded layer files on disk
//! - (cached layer file pages)
//! - derived data from layer file contents, namely:
//!     - initial logical size
//!     - partitioning
//!     - (other currently missing unknowns)
//!
//! Items with parentheses are not (yet) touched by this task.
//!
//! See write-up on restart on-demand download spike: <https://gist.github.com/problame/2265bf7b8dc398be834abfead36c76b5>
use std::{
    ops::ControlFlow,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::{
    context::{DownloadBehavior, RequestContext},
    task_mgr::{self, TaskKind, BACKGROUND_RUNTIME},
    tenant::{
        config::{EvictionPolicy, EvictionPolicyLayerAccessThreshold},
        tasks::{BackgroundLoopKind, RateLimitError},
        timeline::EvictionError,
    },
};

use utils::completion;

use super::Timeline;

impl Timeline {
    pub(super) fn launch_eviction_task(
        self: &Arc<Self>,
        background_tasks_can_start: Option<&completion::Barrier>,
    ) {
        let self_clone = Arc::clone(self);
        let background_tasks_can_start = background_tasks_can_start.cloned();
        task_mgr::spawn(
            BACKGROUND_RUNTIME.handle(),
            TaskKind::Eviction,
            Some(self.tenant_id),
            Some(self.timeline_id),
            &format!("layer eviction for {}/{}", self.tenant_id, self.timeline_id),
            false,
            async move {
                let cancel = task_mgr::shutdown_token();
                tokio::select! {
                    _ = cancel.cancelled() => { return Ok(()); }
                    _ = completion::Barrier::maybe_wait(background_tasks_can_start) => {}
                };

                self_clone.eviction_task(cancel).await;
                Ok(())
            },
        );
    }

    #[instrument(skip_all, fields(tenant_id = %self.tenant_id, timeline_id = %self.timeline_id))]
    async fn eviction_task(self: Arc<Self>, cancel: CancellationToken) {
        use crate::tenant::tasks::random_init_delay;
        {
            let policy = self.get_eviction_policy();
            let period = match policy {
                EvictionPolicy::LayerAccessThreshold(lat) => lat.period,
                EvictionPolicy::NoEviction => Duration::from_secs(10),
            };
            if random_init_delay(period, &cancel).await.is_err() {
                return;
            }
        }

        let ctx = RequestContext::new(TaskKind::Eviction, DownloadBehavior::Warn);
        loop {
            let policy = self.get_eviction_policy();
            let cf = self.eviction_iteration(&policy, &cancel, &ctx).await;

            match cf {
                ControlFlow::Break(()) => break,
                ControlFlow::Continue(sleep_until) => {
                    if tokio::time::timeout_at(sleep_until, cancel.cancelled())
                        .await
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        }
    }

    #[instrument(skip_all, fields(policy_kind = policy.discriminant_str()))]
    async fn eviction_iteration(
        self: &Arc<Self>,
        policy: &EvictionPolicy,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> ControlFlow<(), Instant> {
        debug!("eviction iteration: {policy:?}");
        match policy {
            EvictionPolicy::NoEviction => {
                // check again in 10 seconds; XXX config watch mechanism
                ControlFlow::Continue(Instant::now() + Duration::from_secs(10))
            }
            EvictionPolicy::LayerAccessThreshold(p) => {
                let start = Instant::now();
                match self.eviction_iteration_threshold(p, cancel, ctx).await {
                    ControlFlow::Break(()) => return ControlFlow::Break(()),
                    ControlFlow::Continue(()) => (),
                }
                let elapsed = start.elapsed();
                crate::tenant::tasks::warn_when_period_overrun(
                    elapsed,
                    p.period,
                    BackgroundLoopKind::Eviction,
                );
                crate::metrics::EVICTION_ITERATION_DURATION
                    .get_metric_with_label_values(&[
                        &format!("{}", p.period.as_secs()),
                        &format!("{}", p.threshold.as_secs()),
                    ])
                    .unwrap()
                    .observe(elapsed.as_secs_f64());
                ControlFlow::Continue(start + p.period)
            }
        }
    }

    async fn eviction_iteration_threshold(
        self: &Arc<Self>,
        p: &EvictionPolicyLayerAccessThreshold,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> ControlFlow<()> {
        let now = SystemTime::now();

        let _permit = match crate::tenant::tasks::concurrent_background_tasks_rate_limit(
            BackgroundLoopKind::Eviction,
            ctx,
            cancel,
        )
        .await
        {
            Ok(permit) => permit,
            Err(RateLimitError::Cancelled) => return ControlFlow::Break(()),
        };

        // If we evict layers but keep cached values derived from those layers, then
        // we face a storm of on-demand downloads after pageserver restart.
        // The reason is that the restart empties the caches, and so, the values
        // need to be re-computed by accessing layers, which we evicted while the
        // caches were filled.
        //
        // Solutions here would be one of the following:
        // 1. Have a persistent cache.
        // 2. Count every access to a cached value to the access stats of all layers
        //    that were accessed to compute the value in the first place.
        // 3. Invalidate the caches at a period of < p.threshold/2, so that the values
        //    get re-computed from layers, thereby counting towards layer access stats.
        //
        // We follow approach (4) here because in Neon prod deployment:
        // - page cache is quite small => high churn => low hit rate
        //   => eviction gets correct access stats
        // - value-level caches such as logical size & repatition have a high hit rate,
        //   especially for inactive tenants
        //   => eviction sees zero accesses for these
        //   => they cause the on-demand download storm on pageserver restart
        //
        // We should probably move to persistent caches in the future, or avoid
        // having inactive tenants attached to pageserver in the first place.

        #[allow(dead_code)]
        #[derive(Debug, Default)]
        struct EvictionStats {
            candidates: usize,
            evicted: usize,
            errors: usize,
            not_evictable: usize,
            skipped_for_shutdown: usize,
        }

        let mut stats = EvictionStats::default();
        // Gather layers for eviction.
        // NB: all the checks can be invalidated as soon as we release the layer map lock.
        // We don't want to hold the layer map lock during eviction.
        // So, we just need to deal with this.
        let candidates: Vec<_> = {
            let guard = self.layers.read().await;
            let layers = guard.layer_map();
            let mut candidates = Vec::new();
            for hist_layer in layers.iter_historic_layers() {
                let hist_layer = guard.get_from_desc(&hist_layer);

                // guard against eviction while we inspect it; it might be that eviction_task and
                // disk_usage_eviction_task both select the same layers to be evicted, and
                // seemingly free up double the space. both succeeding is of no consequence.
                let guard = match hist_layer.keep_resident().await {
                    Ok(Some(l)) => l,
                    Ok(None) => continue,
                    Err(e) => {
                        // these should not happen, but we cannot make them statically impossible right
                        // now.
                        tracing::warn!(layer=%hist_layer, "failed to keep the layer resident: {e:#}");
                        continue;
                    }
                };

                let last_activity_ts = hist_layer.access_stats().latest_activity().unwrap_or_else(|| {
                    // We only use this fallback if there's an implementation error.
                    // `latest_activity` already does rate-limited warn!() log.
                    debug!(layer=%hist_layer, "last_activity returns None, using SystemTime::now");
                    SystemTime::now()
                });

                let no_activity_for = match now.duration_since(last_activity_ts) {
                    Ok(d) => d,
                    Err(_e) => {
                        // We reach here if `now` < `last_activity_ts`, which can legitimately
                        // happen if there is an access between us getting `now`, and us getting
                        // the access stats from the layer.
                        //
                        // The other reason why it can happen is system clock skew because
                        // SystemTime::now() is not monotonic, so, even if there is no access
                        // to the layer after we get `now` at the beginning of this function,
                        // it could be that `now`  < `last_activity_ts`.
                        //
                        // To distinguish the cases, we would need to record `Instant`s in the
                        // access stats (i.e., monotonic timestamps), but then, the timestamps
                        // values in the access stats would need to be `Instant`'s, and hence
                        // they would be meaningless outside of the pageserver process.
                        // At the time of writing, the trade-off is that access stats are more
                        // valuable than detecting clock skew.
                        continue;
                    }
                };
                if no_activity_for > p.threshold {
                    candidates.push(guard.drop_eviction_guard())
                }
            }
            candidates
        };
        stats.candidates = candidates.len();

        let remote_client = match self.remote_client.as_ref() {
            None => {
                error!(
                    num_candidates = candidates.len(),
                    "no remote storage configured, cannot evict layers"
                );
                return ControlFlow::Continue(());
            }
            Some(c) => c,
        };

        let results = match self.evict_layer_batch(remote_client, &candidates).await {
            Err(pre_err) => {
                stats.errors += candidates.len();
                error!("could not do any evictions: {pre_err:#}");
                return ControlFlow::Continue(());
            }
            Ok(results) => results,
        };
        assert_eq!(results.len(), candidates.len());
        for result in results {
            match result {
                None => {
                    stats.skipped_for_shutdown += 1;
                }
                Some(Ok(())) => {
                    stats.evicted += 1;
                }
                Some(Err(EvictionError::NotFound | EvictionError::Downloaded)) => {
                    // compaction/gc removed the file while we were waiting on layer_removal_cs
                    stats.not_evictable += 1;
                }
            }
        }
        if stats.candidates == stats.not_evictable {
            debug!(stats=?stats, "eviction iteration complete");
        } else if stats.errors > 0 || stats.not_evictable > 0 {
            warn!(stats=?stats, "eviction iteration complete");
        } else {
            info!(stats=?stats, "eviction iteration complete");
        }
        ControlFlow::Continue(())
    }
}
