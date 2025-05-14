use std::{collections::HashMap, sync::Arc, time::Duration};

use posthog_client_lite::{FeatureStore, PostHogClient, PostHogEvaluationError};
use tokio_util::sync::CancellationToken;
use utils::id::TenantId;

use crate::config::PageServerConf;

struct FeatureResolverInner {
    posthog_client: PostHogClient,
    feature_store: Arc<std::sync::Mutex<FeatureStore>>,
    cancel: CancellationToken,
}

impl FeatureResolverInner {
    fn new(conf: &'static PageServerConf, shutdown_pageserver: CancellationToken) -> Option<Self> {
        if let Some(ref posthog_config) = conf.posthog_config {
            let this = Self {
                posthog_client: PostHogClient::new(
                    posthog_config.server_api_key.clone(),
                    posthog_config.client_api_key.clone(),
                    posthog_config.project_id.clone(),
                    posthog_config.private_api_url.clone(),
                    posthog_config.public_api_url.clone(),
                ),
                feature_store: Arc::new(std::sync::Mutex::new(FeatureStore::new())),
                cancel: shutdown_pageserver,
            };
            Some(this)
        } else {
            None
        }
    }

    fn spawn(self: Arc<Self>, handle: &tokio::runtime::Handle) {
        let this = self.clone();
        let cancel = self.cancel.clone();
        handle.spawn(async move {
            tracing::info!("Starting PostHog feature resolver");
            'outer: loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                    _ = cancel.cancelled() => {
                        break 'outer;
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
                this.feature_store.lock().unwrap().set_flags(resp.flags);
            }
            tracing::info!("PostHog feature resolver stopped");
        });
    }
}

#[derive(Clone)]
pub struct FeatureResolver {
    inner: Option<Arc<FeatureResolverInner>>,
}

impl FeatureResolver {
    pub fn new_disabled() -> Self {
        Self { inner: None }
    }

    /// Always call this function within tokio context.
    pub fn spawn(
        conf: &'static PageServerConf,
        shutdown_pageserver: CancellationToken,
        handle: &tokio::runtime::Handle,
    ) -> anyhow::Result<Self> {
        // DO NOT block in this function: make it return as fast as possible to avoid startup delays.
        let inner = FeatureResolverInner::new(conf, shutdown_pageserver);
        let inner = inner.map(Arc::new);
        if let Some(inner) = inner.clone() {
            inner.spawn(handle);
        }
        Ok(FeatureResolver { inner })
    }

    /// Evaluate a multivariate feature flag. Currently, we do not support any properties.
    pub fn evaluate_multivariate(
        &self,
        flag_key: &str,
        tenant_id: TenantId,
    ) -> Result<String, PostHogEvaluationError> {
        if let Some(inner) = &self.inner {
            inner.feature_store.lock().unwrap().evaluate_multivariate(
                flag_key,
                &tenant_id.to_string(),
                &HashMap::new(),
            )
        } else {
            Err(PostHogEvaluationError::NotAvailable(
                "PostHog integration is not enabled".to_string(),
            ))
        }
    }
}
