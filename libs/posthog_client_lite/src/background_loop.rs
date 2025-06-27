//! A background loop that fetches feature flags from PostHog and updates the feature store.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use arc_swap::ArcSwap;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

use crate::{
    CaptureEvent, FeatureStore, LocalEvaluationResponse, PostHogClient, PostHogClientConfig,
};

/// A background loop that fetches feature flags from PostHog and updates the feature store.
pub struct FeatureResolverBackgroundLoop {
    posthog_client: PostHogClient,
    feature_store: ArcSwap<(SystemTime, Arc<FeatureStore>)>,
    cancel: CancellationToken,
}

impl FeatureResolverBackgroundLoop {
    pub fn new(config: PostHogClientConfig, shutdown_pageserver: CancellationToken) -> Self {
        Self {
            posthog_client: PostHogClient::new(config),
            feature_store: ArcSwap::new(Arc::new((
                SystemTime::UNIX_EPOCH,
                Arc::new(FeatureStore::new()),
            ))),
            cancel: shutdown_pageserver,
        }
    }

    /// Update the feature store with a new feature flag spec bypassing the normal refresh loop.
    pub fn update(&self, spec: String) -> anyhow::Result<()> {
        let resp: LocalEvaluationResponse = serde_json::from_str(&spec)?;
        self.update_feature_store_nofail(resp, "http_propagate");
        Ok(())
    }

    fn update_feature_store_nofail(&self, resp: LocalEvaluationResponse, source: &'static str) {
        let project_id = self.posthog_client.config.project_id.parse::<u64>().ok();
        match FeatureStore::new_with_flags(resp.flags, project_id) {
            Ok(feature_store) => {
                self.feature_store
                    .store(Arc::new((SystemTime::now(), Arc::new(feature_store))));
                tracing::info!("Feature flag updated from {}", source);
            }
            Err(e) => {
                tracing::warn!("Cannot process feature flag spec from {}: {}", source, e);
            }
        }
    }

    pub fn spawn(
        self: Arc<Self>,
        handle: &tokio::runtime::Handle,
        refresh_period: Duration,
        fake_tenants: Vec<CaptureEvent>,
    ) {
        let this = self.clone();
        let cancel = self.cancel.clone();

        // Main loop of updating the feature flags.
        handle.spawn(
            async move {
                tracing::info!(
                    "Starting PostHog feature resolver with refresh period: {:?}",
                    refresh_period
                );
                let mut ticker = tokio::time::interval(refresh_period);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {}
                        _ = cancel.cancelled() => break
                    }
                    {
                        let last_update = this.feature_store.load().0;
                        if let Ok(elapsed) = last_update.elapsed() {
                            if elapsed < refresh_period {
                                tracing::debug!(
                                    "Skipping feature flag refresh because it's too soon"
                                );
                                continue;
                            }
                        }
                    }
                    let resp = match this
                        .posthog_client
                        .get_feature_flags_local_evaluation()
                        .await
                    {
                        Ok(resp) => resp,
                        Err(e) => {
                            tracing::warn!("Cannot get feature flags: {}", e);
                            continue;
                        }
                    };
                    this.update_feature_store_nofail(resp, "refresh_loop");
                }
                tracing::info!("PostHog feature resolver stopped");
            }
            .instrument(info_span!("posthog_feature_resolver")),
        );

        // Report fake tenants to PostHog so that we have the combination of all the properties in the UI.
        // Do one report per pageserver restart.
        let this = self.clone();
        handle.spawn(
            async move {
                tracing::info!("Starting PostHog feature reporter");
                for tenant in &fake_tenants {
                    tracing::info!("Reporting fake tenant: {:?}", tenant);
                }
                if let Err(e) = this.posthog_client.capture_event_batch(&fake_tenants).await {
                    tracing::warn!("Cannot report fake tenants: {}", e);
                }
            }
            .instrument(info_span!("posthog_feature_reporter")),
        );
    }

    pub fn feature_store(&self) -> Arc<FeatureStore> {
        self.feature_store.load().1.clone()
    }
}
