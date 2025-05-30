use std::{collections::HashMap, sync::Arc, time::Duration};

use posthog_client_lite::{
    FeatureResolverBackgroundLoop, PostHogClientConfig, PostHogEvaluationError,
};
use tokio_util::sync::CancellationToken;
use utils::id::TenantId;

use crate::config::PageServerConf;

#[derive(Clone)]
pub struct FeatureResolver {
    inner: Option<Arc<FeatureResolverBackgroundLoop>>,
}

impl FeatureResolver {
    pub fn new_disabled() -> Self {
        Self { inner: None }
    }

    pub fn spawn(
        conf: &PageServerConf,
        shutdown_pageserver: CancellationToken,
        handle: &tokio::runtime::Handle,
    ) -> anyhow::Result<Self> {
        // DO NOT block in this function: make it return as fast as possible to avoid startup delays.
        if let Some(posthog_config) = &conf.posthog_config {
            let inner = FeatureResolverBackgroundLoop::new(
                PostHogClientConfig {
                    server_api_key: posthog_config.server_api_key.clone(),
                    client_api_key: posthog_config.client_api_key.clone(),
                    project_id: posthog_config.project_id.clone(),
                    private_api_url: posthog_config.private_api_url.clone(),
                    public_api_url: posthog_config.public_api_url.clone(),
                },
                shutdown_pageserver,
            );
            let inner = Arc::new(inner);
            // TODO: make this configurable
            inner.clone().spawn(handle, Duration::from_secs(60));
            Ok(FeatureResolver { inner: Some(inner) })
        } else {
            Ok(FeatureResolver { inner: None })
        }
    }

    /// Evaluate a multivariate feature flag. Currently, we do not support any properties.
    ///
    /// Error handling: the caller should inspect the error and decide the behavior when a feature flag
    /// cannot be evaluated (i.e., default to false if it cannot be resolved). The error should *not* be
    /// propagated beyond where the feature flag gets resolved.
    pub fn evaluate_multivariate(
        &self,
        flag_key: &str,
        tenant_id: TenantId,
    ) -> Result<String, PostHogEvaluationError> {
        if let Some(inner) = &self.inner {
            inner.feature_store().evaluate_multivariate(
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

    /// Evaluate a boolean feature flag. Currently, we do not support any properties.
    ///
    /// Returns `Ok(())` if the flag is evaluated to true, otherwise returns an error.
    ///
    /// Error handling: the caller should inspect the error and decide the behavior when a feature flag
    /// cannot be evaluated (i.e., default to false if it cannot be resolved). The error should *not* be
    /// propagated beyond where the feature flag gets resolved.
    pub fn evaluate_boolean(
        &self,
        flag_key: &str,
        tenant_id: TenantId,
    ) -> Result<(), PostHogEvaluationError> {
        if let Some(inner) = &self.inner {
            inner.feature_store().evaluate_boolean(
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

    pub fn is_feature_flag_boolean(&self, flag_key: &str) -> Result<bool, PostHogEvaluationError> {
        if let Some(inner) = &self.inner {
            inner.feature_store().is_feature_flag_boolean(flag_key)
        } else {
            Err(PostHogEvaluationError::NotAvailable(
                "PostHog integration is not enabled".to_string(),
            ))
        }
    }
}
