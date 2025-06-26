use std::{collections::HashMap, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use pageserver_api::config::NodeMetadata;
use posthog_client_lite::{
    CaptureEvent, FeatureResolverBackgroundLoop, PostHogClientConfig, PostHogEvaluationError,
    PostHogFlagFilterPropertyValue,
};
use remote_storage::RemoteStorageKind;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use utils::id::TenantId;

use crate::{config::PageServerConf, metrics::FEATURE_FLAG_EVALUATION};

const DEFAULT_POSTHOG_REFRESH_INTERVAL: Duration = Duration::from_secs(600);

#[derive(Clone)]
pub struct FeatureResolver {
    inner: Option<Arc<FeatureResolverBackgroundLoop>>,
    internal_properties: Option<Arc<HashMap<String, PostHogFlagFilterPropertyValue>>>,
    force_overrides_for_testing: Arc<ArcSwap<HashMap<String, String>>>,
}

impl FeatureResolver {
    pub fn new_disabled() -> Self {
        Self {
            inner: None,
            internal_properties: None,
            force_overrides_for_testing: Arc::new(ArcSwap::new(Arc::new(HashMap::new()))),
        }
    }

    pub fn update(&self, spec: String) -> anyhow::Result<()> {
        if let Some(inner) = &self.inner {
            inner.update(spec)?;
        }
        Ok(())
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

            // The properties shared by all tenants on this pageserver.
            let internal_properties = {
                let mut properties = HashMap::new();
                properties.insert(
                    "pageserver_id".to_string(),
                    PostHogFlagFilterPropertyValue::String(conf.id.to_string()),
                );
                if let Some(availability_zone) = &conf.availability_zone {
                    properties.insert(
                        "availability_zone".to_string(),
                        PostHogFlagFilterPropertyValue::String(availability_zone.clone()),
                    );
                }
                // Infer region based on the remote storage config.
                if let Some(remote_storage) = &conf.remote_storage_config {
                    match &remote_storage.storage {
                        RemoteStorageKind::AwsS3(config) => {
                            properties.insert(
                                "region".to_string(),
                                PostHogFlagFilterPropertyValue::String(format!(
                                    "aws-{}",
                                    config.bucket_region
                                )),
                            );
                        }
                        RemoteStorageKind::AzureContainer(config) => {
                            properties.insert(
                                "region".to_string(),
                                PostHogFlagFilterPropertyValue::String(format!(
                                    "azure-{}",
                                    config.container_region
                                )),
                            );
                        }
                        RemoteStorageKind::LocalFs { .. } => {
                            properties.insert(
                                "region".to_string(),
                                PostHogFlagFilterPropertyValue::String("local".to_string()),
                            );
                        }
                    }
                }
                // TODO: move this to a background task so that we don't block startup in case of slow disk
                let metadata_path = conf.metadata_path();
                match std::fs::read_to_string(&metadata_path) {
                    Ok(metadata_str) => match serde_json::from_str::<NodeMetadata>(&metadata_str) {
                        Ok(metadata) => {
                            properties.insert(
                                "hostname".to_string(),
                                PostHogFlagFilterPropertyValue::String(metadata.http_host),
                            );
                            if let Some(cplane_region) = metadata.other.get("region_id") {
                                if let Some(cplane_region) = cplane_region.as_str() {
                                    // This region contains the cell number
                                    properties.insert(
                                        "neon_region".to_string(),
                                        PostHogFlagFilterPropertyValue::String(
                                            cplane_region.to_string(),
                                        ),
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Failed to parse metadata.json: {}", e);
                        }
                    },
                    Err(e) => {
                        tracing::warn!("Failed to read metadata.json: {}", e);
                    }
                }
                Arc::new(properties)
            };
            let fake_tenants = {
                let mut tenants = Vec::new();
                for i in 0..10 {
                    let distinct_id = format!(
                        "fake_tenant_{}_{}_{}",
                        conf.availability_zone.as_deref().unwrap_or_default(),
                        conf.id,
                        i
                    );
                    let properties = Self::collect_properties_inner(
                        distinct_id.clone(),
                        Some(&internal_properties),
                    );
                    tenants.push(CaptureEvent {
                        event: "initial_tenant_report".to_string(),
                        distinct_id,
                        properties: json!({ "$set": properties }), // use `$set` to set the person properties instead of the event properties
                    });
                }
                tenants
            };
            inner.clone().spawn(
                handle,
                posthog_config
                    .refresh_interval
                    .unwrap_or(DEFAULT_POSTHOG_REFRESH_INTERVAL),
                fake_tenants,
            );
            Ok(FeatureResolver {
                inner: Some(inner),
                internal_properties: Some(internal_properties),
                force_overrides_for_testing: Arc::new(ArcSwap::new(Arc::new(HashMap::new()))),
            })
        } else {
            Ok(FeatureResolver {
                inner: None,
                internal_properties: None,
                force_overrides_for_testing: Arc::new(ArcSwap::new(Arc::new(HashMap::new()))),
            })
        }
    }

    fn collect_properties_inner(
        tenant_id: String,
        internal_properties: Option<&HashMap<String, PostHogFlagFilterPropertyValue>>,
    ) -> HashMap<String, PostHogFlagFilterPropertyValue> {
        let mut properties = HashMap::new();
        if let Some(internal_properties) = internal_properties {
            for (key, value) in internal_properties.iter() {
                properties.insert(key.clone(), value.clone());
            }
        }
        properties.insert(
            "tenant_id".to_string(),
            PostHogFlagFilterPropertyValue::String(tenant_id),
        );
        properties
    }

    /// Collect all properties availble for the feature flag evaluation.
    pub(crate) fn collect_properties(
        &self,
        tenant_id: TenantId,
    ) -> HashMap<String, PostHogFlagFilterPropertyValue> {
        Self::collect_properties_inner(tenant_id.to_string(), self.internal_properties.as_deref())
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
        let force_overrides = self.force_overrides_for_testing.load();
        if let Some(value) = force_overrides.get(flag_key) {
            return Ok(value.clone());
        }

        if let Some(inner) = &self.inner {
            let res = inner.feature_store().evaluate_multivariate(
                flag_key,
                &tenant_id.to_string(),
                &self.collect_properties(tenant_id),
            );
            match &res {
                Ok(value) => {
                    FEATURE_FLAG_EVALUATION
                        .with_label_values(&[flag_key, "ok", value])
                        .inc();
                }
                Err(e) => {
                    FEATURE_FLAG_EVALUATION
                        .with_label_values(&[flag_key, "error", e.as_variant_str()])
                        .inc();
                }
            }
            res
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
        let force_overrides = self.force_overrides_for_testing.load();
        if let Some(value) = force_overrides.get(flag_key) {
            return if value == "true" {
                Ok(())
            } else {
                Err(PostHogEvaluationError::NoConditionGroupMatched)
            };
        }

        if let Some(inner) = &self.inner {
            let res = inner.feature_store().evaluate_boolean(
                flag_key,
                &tenant_id.to_string(),
                &self.collect_properties(tenant_id),
            );
            match &res {
                Ok(()) => {
                    FEATURE_FLAG_EVALUATION
                        .with_label_values(&[flag_key, "ok", "true"])
                        .inc();
                }
                Err(e) => {
                    FEATURE_FLAG_EVALUATION
                        .with_label_values(&[flag_key, "error", e.as_variant_str()])
                        .inc();
                }
            }
            res
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
                "PostHog integration is not enabled, cannot auto-determine the flag type"
                    .to_string(),
            ))
        }
    }

    /// Force override a feature flag for testing. This is only for testing purposes. Assume the caller only call it
    /// from a single thread so it won't race.
    pub fn force_override_for_testing(&self, flag_key: &str, value: Option<&str>) {
        let mut force_overrides = self.force_overrides_for_testing.load().as_ref().clone();
        if let Some(value) = value {
            force_overrides.insert(flag_key.to_string(), value.to_string());
        } else {
            force_overrides.remove(flag_key);
        }
        self.force_overrides_for_testing
            .store(Arc::new(force_overrides));
    }
}
