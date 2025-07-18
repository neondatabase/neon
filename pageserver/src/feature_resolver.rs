use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use arc_swap::ArcSwap;
use pageserver_api::config::NodeMetadata;
use posthog_client_lite::{
    CaptureEvent, FeatureResolverBackgroundLoop, PostHogEvaluationError,
    PostHogFlagFilterPropertyValue,
};
use rand::Rng;
use remote_storage::RemoteStorageKind;
use serde_json::json;
use tokio_util::sync::CancellationToken;
use utils::id::TenantId;

use crate::{config::PageServerConf, metrics::FEATURE_FLAG_EVALUATION, tenant::TenantShard};

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
            let posthog_client_config = match posthog_config.clone().try_into_posthog_config() {
                Ok(config) => config,
                Err(e) => {
                    tracing::warn!(
                        "invalid posthog config, skipping posthog integration: {}",
                        e
                    );
                    return Ok(FeatureResolver {
                        inner: None,
                        internal_properties: None,
                        force_overrides_for_testing: Arc::new(ArcSwap::new(Arc::new(
                            HashMap::new(),
                        ))),
                    });
                }
            };
            let inner =
                FeatureResolverBackgroundLoop::new(posthog_client_config, shutdown_pageserver);
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

                    let tenant_properties = PerTenantProperties {
                        remote_size_mb: Some(rand::thread_rng().gen_range(100.0..1000000.00)),
                        db_count_max: Some(rand::thread_rng().gen_range(1..1000)),
                        rel_count_max: Some(rand::thread_rng().gen_range(1..1000)),
                    }
                    .into_posthog_properties();

                    let properties = Self::collect_properties_inner(
                        distinct_id.clone(),
                        Some(&internal_properties),
                        &tenant_properties,
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
        tenant_properties: &HashMap<String, PostHogFlagFilterPropertyValue>,
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
        for (key, value) in tenant_properties.iter() {
            properties.insert(key.clone(), value.clone());
        }
        properties
    }

    /// Collect all properties availble for the feature flag evaluation.
    pub(crate) fn collect_properties(
        &self,
        tenant_id: TenantId,
        tenant_properties: &HashMap<String, PostHogFlagFilterPropertyValue>,
    ) -> HashMap<String, PostHogFlagFilterPropertyValue> {
        Self::collect_properties_inner(
            tenant_id.to_string(),
            self.internal_properties.as_deref(),
            tenant_properties,
        )
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
        tenant_properties: &HashMap<String, PostHogFlagFilterPropertyValue>,
    ) -> Result<String, PostHogEvaluationError> {
        let force_overrides = self.force_overrides_for_testing.load();
        if let Some(value) = force_overrides.get(flag_key) {
            return Ok(value.clone());
        }

        if let Some(inner) = &self.inner {
            let res = inner.feature_store().evaluate_multivariate(
                flag_key,
                &tenant_id.to_string(),
                &self.collect_properties(tenant_id, tenant_properties),
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
        tenant_properties: &HashMap<String, PostHogFlagFilterPropertyValue>,
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
                &self.collect_properties(tenant_id, tenant_properties),
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

struct PerTenantProperties {
    pub remote_size_mb: Option<f64>,
    pub db_count_max: Option<usize>,
    pub rel_count_max: Option<usize>,
}

impl PerTenantProperties {
    pub fn into_posthog_properties(self) -> HashMap<String, PostHogFlagFilterPropertyValue> {
        let mut properties = HashMap::new();
        if let Some(remote_size_mb) = self.remote_size_mb {
            properties.insert(
                "tenant_remote_size_mb".to_string(),
                PostHogFlagFilterPropertyValue::Number(remote_size_mb),
            );
        }
        if let Some(db_count) = self.db_count_max {
            properties.insert(
                "tenant_db_count_max".to_string(),
                PostHogFlagFilterPropertyValue::Number(db_count as f64),
            );
        }
        if let Some(rel_count) = self.rel_count_max {
            properties.insert(
                "tenant_rel_count_max".to_string(),
                PostHogFlagFilterPropertyValue::Number(rel_count as f64),
            );
        }
        properties
    }
}

pub struct TenantFeatureResolver {
    inner: FeatureResolver,
    tenant_id: TenantId,
    cached_tenant_properties: ArcSwap<HashMap<String, PostHogFlagFilterPropertyValue>>,

    // Add feature flag on the critical path below.
    //
    // If a feature flag will be used on the critical path, we will update it in the tenant housekeeping loop insetad of
    // resolving directly by calling `evaluate_multivariate` or `evaluate_boolean`. Remember to update the flag in the
    // housekeeping loop. The user should directly read this atomic flag instead of using the set of evaluate functions.
    pub feature_test_remote_size_flag: AtomicBool,
}

impl TenantFeatureResolver {
    pub fn new(inner: FeatureResolver, tenant_id: TenantId) -> Self {
        Self {
            inner,
            tenant_id,
            cached_tenant_properties: ArcSwap::new(Arc::new(HashMap::new())),
            feature_test_remote_size_flag: AtomicBool::new(false),
        }
    }

    pub fn evaluate_multivariate(&self, flag_key: &str) -> Result<String, PostHogEvaluationError> {
        self.inner.evaluate_multivariate(
            flag_key,
            self.tenant_id,
            &self.cached_tenant_properties.load(),
        )
    }

    pub fn evaluate_boolean(&self, flag_key: &str) -> Result<(), PostHogEvaluationError> {
        self.inner.evaluate_boolean(
            flag_key,
            self.tenant_id,
            &self.cached_tenant_properties.load(),
        )
    }

    pub fn collect_properties(&self) -> HashMap<String, PostHogFlagFilterPropertyValue> {
        self.inner
            .collect_properties(self.tenant_id, &self.cached_tenant_properties.load())
    }

    pub fn is_feature_flag_boolean(&self, flag_key: &str) -> Result<bool, PostHogEvaluationError> {
        self.inner.is_feature_flag_boolean(flag_key)
    }

    /// Refresh the cached properties and flags on the critical path.
    pub fn refresh_properties_and_flags(&self, tenant_shard: &TenantShard) {
        // Any of the remote size is none => this property is none.
        let mut remote_size_mb = Some(0.0);
        // Any of the db or rel count is available => this property is available.
        let mut db_count_max = None;
        let mut rel_count_max = None;
        for timeline in tenant_shard.list_timelines() {
            let size = timeline.metrics.resident_physical_size_get();
            if size == 0 {
                remote_size_mb = None;
                break;
            }
            if let Some(ref mut remote_size_mb) = remote_size_mb {
                *remote_size_mb += size as f64 / 1024.0 / 1024.0;
            }
            if let Some((db_count, rel_count)) = *timeline.db_rel_count.load().as_ref() {
                if db_count_max.is_none() {
                    db_count_max = Some(db_count);
                }
                if rel_count_max.is_none() {
                    rel_count_max = Some(rel_count);
                }
                db_count_max = db_count_max.map(|max| max.max(db_count));
                rel_count_max = rel_count_max.map(|max| max.max(rel_count));
            }
        }
        self.cached_tenant_properties.store(Arc::new(
            PerTenantProperties {
                remote_size_mb,
                db_count_max,
                rel_count_max,
            }
            .into_posthog_properties(),
        ));

        // BEGIN: Update the feature flag on the critical path.
        self.feature_test_remote_size_flag.store(
            self.evaluate_boolean("test-remote-size-flag").is_ok(),
            std::sync::atomic::Ordering::Relaxed,
        );
        // END: Update the feature flag on the critical path.
    }
}
