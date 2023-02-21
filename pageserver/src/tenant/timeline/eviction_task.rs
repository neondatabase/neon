//! The per-timeline layer eviction task.

use std::{
    ops::ControlFlow,
    sync::Arc,
    time::{Duration, SystemTime},
};

use either::Either;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::{
    task_mgr::{self, TaskKind, BACKGROUND_RUNTIME},
    tenant::{
        config::{EvictionPolicy, EvictionPolicyLayerAccessThreshold},
        storage_layer::PersistentLayer,
    },
};

use super::Timeline;

impl Timeline {
    pub(super) fn launch_eviction_task(self: &Arc<Self>) {
        let self_clone = Arc::clone(self);
        task_mgr::spawn(
            BACKGROUND_RUNTIME.handle(),
            TaskKind::Eviction,
            Some(self.tenant_id),
            Some(self.timeline_id),
            &format!("layer eviction for {}/{}", self.tenant_id, self.timeline_id),
            false,
            async move {
                self_clone.eviction_task(task_mgr::shutdown_token()).await;
                info!("eviction task finishing");
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
                info!("shutting down");
                return;
            }
        }

        loop {
            let policy = self.get_eviction_policy();
            let cf = self.eviction_iteration(&policy, cancel.clone()).await;

            match cf {
                ControlFlow::Break(()) => break,
                ControlFlow::Continue(sleep_until) => {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            info!("shutting down");
                            break;
                        }
                        _ = tokio::time::sleep_until(sleep_until) => { }
                    }
                }
            }
        }
    }

    #[instrument(skip_all, fields(policy_kind = policy.discriminant_str()))]
    async fn eviction_iteration(
        self: &Arc<Self>,
        policy: &EvictionPolicy,
        cancel: CancellationToken,
    ) -> ControlFlow<(), Instant> {
        debug!("eviction iteration: {policy:?}");
        match policy {
            EvictionPolicy::NoEviction => {
                // check again in 10 seconds; XXX config watch mechanism
                ControlFlow::Continue(Instant::now() + Duration::from_secs(10))
            }
            EvictionPolicy::LayerAccessThreshold(p) => {
                let start = Instant::now();
                match self.eviction_iteration_threshold(p, cancel).await {
                    ControlFlow::Break(()) => return ControlFlow::Break(()),
                    ControlFlow::Continue(()) => (),
                }
                let elapsed = start.elapsed();
                crate::tenant::tasks::warn_when_period_overrun(elapsed, p.period, "eviction");
                ControlFlow::Continue(start + p.period)
            }
        }
    }

    async fn eviction_iteration_threshold(
        self: &Arc<Self>,
        p: &EvictionPolicyLayerAccessThreshold,
        cancel: CancellationToken,
    ) -> ControlFlow<()> {
        let now = SystemTime::now();

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
        let candidates: Vec<Arc<dyn PersistentLayer>> = {
            let layers = self.layers.read().unwrap();
            let mut candidates = Vec::new();
            for hist_layer in layers.iter_historic_layers() {
                if hist_layer.is_remote_layer() {
                    continue;
                }
                let last_activity_ts = match hist_layer
                    .access_stats()
                    .most_recent_access_or_residence_event()
                {
                    Either::Left(mra) => mra.when,
                    Either::Right(re) => re.timestamp,
                };
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
                    candidates.push(hist_layer)
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

        let results = match self
            .evict_layer_batch(remote_client, &candidates[..], cancel)
            .await
        {
            Err(pre_err) => {
                stats.errors += candidates.len();
                error!("could not do any evictions: {pre_err:#}");
                return ControlFlow::Continue(());
            }
            Ok(results) => results,
        };
        assert_eq!(results.len(), candidates.len());
        for (l, result) in candidates.iter().zip(results) {
            match result {
                None => {
                    stats.skipped_for_shutdown += 1;
                }
                Some(Ok(true)) => {
                    debug!("evicted layer {l:?}");
                    stats.evicted += 1;
                }
                Some(Ok(false)) => {
                    debug!("layer is not evictable: {l:?}");
                    stats.not_evictable += 1;
                }
                Some(Err(e)) => {
                    // This variant is the case where an unexpected error happened during eviction.
                    // Expected errors that result in non-eviction are `Some(Ok(false))`.
                    // So, dump Debug here to gather as much info as possible in this rare case.
                    warn!("failed to evict layer {l:?}: {e:?}");
                    stats.errors += 1;
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
