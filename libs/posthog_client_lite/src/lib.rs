//! A lite version of the PostHog client that only supports local evaluation of feature flags.

mod background_loop;

pub use background_loop::FeatureResolverBackgroundLoop;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Digest;

#[derive(Debug, thiserror::Error)]
pub enum PostHogEvaluationError {
    /// The feature flag is not available, for example, because the local evaluation data is not populated yet.
    #[error("Feature flag not available: {0}")]
    NotAvailable(String),
    #[error("No condition group is matched")]
    NoConditionGroupMatched,
    /// Real errors, e.g., the rollout percentage does not add up to 100.
    #[error("Failed to evaluate feature flag: {0}")]
    Internal(String),
}

impl PostHogEvaluationError {
    pub fn as_variant_str(&self) -> &'static str {
        match self {
            PostHogEvaluationError::NotAvailable(_) => "not_available",
            PostHogEvaluationError::NoConditionGroupMatched => "no_condition_group_matched",
            PostHogEvaluationError::Internal(_) => "internal",
        }
    }
}

#[derive(Deserialize)]
pub struct LocalEvaluationResponse {
    pub flags: Vec<LocalEvaluationFlag>,
}

#[derive(Deserialize)]
pub struct LocalEvaluationFlag {
    #[allow(dead_code)]
    id: u64,
    team_id: u64,
    key: String,
    filters: LocalEvaluationFlagFilters,
    active: bool,
}

#[derive(Deserialize)]
pub struct LocalEvaluationFlagFilters {
    groups: Vec<LocalEvaluationFlagFilterGroup>,
    multivariate: Option<LocalEvaluationFlagMultivariate>,
}

#[derive(Deserialize)]
pub struct LocalEvaluationFlagFilterGroup {
    variant: Option<String>,
    properties: Option<Vec<LocalEvaluationFlagFilterProperty>>,
    rollout_percentage: i64,
}

#[derive(Deserialize)]
pub struct LocalEvaluationFlagFilterProperty {
    key: String,
    value: PostHogFlagFilterPropertyValue,
    operator: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum PostHogFlagFilterPropertyValue {
    String(String),
    Number(f64),
    Boolean(bool),
    List(Vec<String>),
}

#[derive(Deserialize)]
pub struct LocalEvaluationFlagMultivariate {
    variants: Vec<LocalEvaluationFlagMultivariateVariant>,
}

#[derive(Deserialize)]
pub struct LocalEvaluationFlagMultivariateVariant {
    key: String,
    rollout_percentage: i64,
}

pub struct FeatureStore {
    flags: HashMap<String, LocalEvaluationFlag>,
}

impl Default for FeatureStore {
    fn default() -> Self {
        Self::new()
    }
}

enum GroupEvaluationResult {
    MatchedAndOverride(String),
    MatchedAndEvaluate,
    Unmatched,
}

impl FeatureStore {
    pub fn new() -> Self {
        Self {
            flags: HashMap::new(),
        }
    }

    pub fn new_with_flags(
        flags: Vec<LocalEvaluationFlag>,
        project_id: Option<u64>,
    ) -> Result<Self, &'static str> {
        let mut store = Self::new();
        store.set_flags(flags, project_id)?;
        Ok(store)
    }

    pub fn set_flags(
        &mut self,
        flags: Vec<LocalEvaluationFlag>,
        project_id: Option<u64>,
    ) -> Result<(), &'static str> {
        self.flags.clear();
        for flag in flags {
            if let Some(project_id) = project_id {
                if flag.team_id != project_id {
                    return Err(
                        "Retrieved a spec with different project id, wrong config? Discarding the feature flags.",
                    );
                }
            }
            self.flags.insert(flag.key.clone(), flag);
        }
        Ok(())
    }

    /// Generate a consistent hash for a user ID (e.g., tenant ID).
    ///
    /// The implementation is different from PostHog SDK. In PostHog SDK, it is sha1 of `user_id.distinct_id.salt`.
    /// However, as we do not upload all of our tenant IDs to PostHog, we do not have the PostHog distinct_id for a
    /// tenant. Therefore, the way we compute it is sha256 of `user_id.feature_id.salt`.
    fn consistent_hash(user_id: &str, flag_key: &str, salt: &str) -> f64 {
        let mut hasher = sha2::Sha256::new();
        hasher.update(user_id);
        hasher.update(".");
        hasher.update(flag_key);
        hasher.update(".");
        hasher.update(salt);
        let hash = hasher.finalize();
        let hash_int = u64::from_le_bytes(hash[..8].try_into().unwrap());
        hash_int as f64 / u64::MAX as f64
    }

    /// Evaluate a condition. Returns an error if the condition cannot be evaluated due to parsing error or missing
    /// property.
    fn evaluate_condition(
        &self,
        operator: &str,
        provided: &PostHogFlagFilterPropertyValue,
        requested: &PostHogFlagFilterPropertyValue,
    ) -> Result<bool, PostHogEvaluationError> {
        match operator {
            "exact" => {
                let PostHogFlagFilterPropertyValue::String(provided) = provided else {
                    // Left should be a string
                    return Err(PostHogEvaluationError::Internal(format!(
                        "The left side of the condition is not a string: {provided:?}"
                    )));
                };
                let PostHogFlagFilterPropertyValue::List(requested) = requested else {
                    // Right should be a list of string
                    return Err(PostHogEvaluationError::Internal(format!(
                        "The right side of the condition is not a list: {requested:?}"
                    )));
                };
                Ok(requested.contains(provided))
            }
            "lt" | "gt" => {
                let PostHogFlagFilterPropertyValue::String(requested) = requested else {
                    // Right should be a string
                    return Err(PostHogEvaluationError::Internal(format!(
                        "The right side of the condition is not a string: {requested:?}"
                    )));
                };
                let Ok(requested) = requested.parse::<f64>() else {
                    return Err(PostHogEvaluationError::Internal(format!(
                        "Can not parse the right side of the condition as a number: {requested:?}"
                    )));
                };
                // Left can either be a number or a string
                let provided = match provided {
                    PostHogFlagFilterPropertyValue::Number(provided) => *provided,
                    PostHogFlagFilterPropertyValue::String(provided) => {
                        let Ok(provided) = provided.parse::<f64>() else {
                            return Err(PostHogEvaluationError::Internal(format!(
                                "Can not parse the left side of the condition as a number: {provided:?}"
                            )));
                        };
                        provided
                    }
                    _ => {
                        return Err(PostHogEvaluationError::Internal(format!(
                            "The left side of the condition is not a number or a string: {provided:?}"
                        )));
                    }
                };
                match operator {
                    "lt" => Ok(provided < requested),
                    "gt" => Ok(provided > requested),
                    op => Err(PostHogEvaluationError::Internal(format!(
                        "Unsupported operator: {op}"
                    ))),
                }
            }
            _ => Err(PostHogEvaluationError::Internal(format!(
                "Unsupported operator: {operator}"
            ))),
        }
    }

    /// Evaluate a percentage.
    fn evaluate_percentage(&self, mapped_user_id: f64, percentage: i64) -> bool {
        mapped_user_id <= percentage as f64 / 100.0
    }

    /// Evaluate a filter group for a feature flag. Returns an error if there are errors during the evaluation.
    ///
    /// Return values:
    /// Ok(GroupEvaluationResult::MatchedAndOverride(variant)): matched and evaluated to this value
    /// Ok(GroupEvaluationResult::MatchedAndEvaluate): condition matched but no variant override, use the global rollout percentage
    /// Ok(GroupEvaluationResult::Unmatched): condition unmatched
    fn evaluate_group(
        &self,
        group: &LocalEvaluationFlagFilterGroup,
        hash_on_group_rollout_percentage: f64,
        provided_properties: &HashMap<String, PostHogFlagFilterPropertyValue>,
    ) -> Result<GroupEvaluationResult, PostHogEvaluationError> {
        if let Some(ref properties) = group.properties {
            for property in properties {
                if let Some(value) = provided_properties.get(&property.key) {
                    // The user provided the property value
                    if !self.evaluate_condition(
                        property.operator.as_ref(),
                        value,
                        &property.value,
                    )? {
                        return Ok(GroupEvaluationResult::Unmatched);
                    }
                } else {
                    // We cannot evaluate, the property is not available
                    return Err(PostHogEvaluationError::NotAvailable(format!(
                        "The required property in the condition is not available: {}",
                        property.key
                    )));
                }
            }
        }

        // The group has no condition matchers or we matched the properties
        if self.evaluate_percentage(hash_on_group_rollout_percentage, group.rollout_percentage) {
            if let Some(ref variant_override) = group.variant {
                Ok(GroupEvaluationResult::MatchedAndOverride(
                    variant_override.clone(),
                ))
            } else {
                Ok(GroupEvaluationResult::MatchedAndEvaluate)
            }
        } else {
            Ok(GroupEvaluationResult::Unmatched)
        }
    }

    /// Evaluate a multivariate feature flag. Returns an error if the flag is not available or if there are errors
    /// during the evaluation.
    ///
    /// The parsing logic is as follows:
    ///
    /// * Match each filter group.
    ///   - If a group is matched, it will first determine whether the user is in the range of the group's rollout
    ///     percentage. We will generate a consistent hash for the user ID on the group rollout percentage. This hash
    ///     is shared across all groups.
    ///   - If the hash falls within the group's rollout percentage, return the variant if it's overridden, or
    ///   - Evaluate the variant using the global config and the global rollout percentage.
    /// * Otherwise, continue with the next group until all groups are evaluated and no group is within the
    ///   rollout percentage.
    /// * If there are no matching groups, return an error.
    ///
    /// Example: we have a multivariate flag with 3 groups of the configured global rollout percentage: A (10%), B (20%), C (70%).
    /// There is a single group with a condition that has a rollout percentage of 10% and it does not have a variant override.
    /// Then, we will have 1% of the users evaluated to A, 2% to B, and 7% to C.
    ///
    /// Error handling: the caller should inspect the error and decide the behavior when a feature flag
    /// cannot be evaluated (i.e., default to false if it cannot be resolved). The error should *not* be
    /// propagated beyond where the feature flag gets resolved.
    pub fn evaluate_multivariate(
        &self,
        flag_key: &str,
        user_id: &str,
        properties: &HashMap<String, PostHogFlagFilterPropertyValue>,
    ) -> Result<String, PostHogEvaluationError> {
        let hash_on_global_rollout_percentage =
            Self::consistent_hash(user_id, flag_key, "multivariate");
        let hash_on_group_rollout_percentage =
            Self::consistent_hash(user_id, flag_key, "within_group");
        self.evaluate_multivariate_inner(
            flag_key,
            hash_on_global_rollout_percentage,
            hash_on_group_rollout_percentage,
            properties,
        )
    }

    /// Evaluate a boolean feature flag. Returns  an error if the flag is not available or if there are errors
    /// during the evaluation.
    ///
    /// The parsing logic is as follows:
    ///
    /// * Generate a consistent hash for the tenant-feature.
    /// * Match each filter group.
    ///   - If a group is matched, it will first determine whether the user is in the range of the rollout
    ///     percentage.
    ///   - If the hash falls within the group's rollout percentage, return true.
    /// * Otherwise, continue with the next group until all groups are evaluated and no group is within the
    ///   rollout percentage.
    /// * If there are no matching groups, return an error.
    ///
    /// Returns `Ok(())` if the feature flag evaluates to true. In the future, it will return a payload.
    ///
    /// Error handling: the caller should inspect the error and decide the behavior when a feature flag
    /// cannot be evaluated (i.e., default to false if it cannot be resolved). The error should *not* be
    /// propagated beyond where the feature flag gets resolved.
    pub fn evaluate_boolean(
        &self,
        flag_key: &str,
        user_id: &str,
        properties: &HashMap<String, PostHogFlagFilterPropertyValue>,
    ) -> Result<(), PostHogEvaluationError> {
        let hash_on_global_rollout_percentage = Self::consistent_hash(user_id, flag_key, "boolean");
        self.evaluate_boolean_inner(flag_key, hash_on_global_rollout_percentage, properties)
    }

    /// Evaluate a multivariate feature flag. Note that we directly take the mapped user ID
    /// (a consistent hash ranging from 0 to 1) so that it is easier to use it in the tests
    /// and avoid duplicate computations.
    ///
    /// Use a different consistent hash for evaluating the group rollout percentage.
    /// The behavior: if the condition is set to rolling out to 10% of the users, and
    /// we set the variant A to 20% in the global config, then 2% of the total users will
    /// be evaluated to variant A.
    ///
    /// Note that the hash to determine group rollout percentage is shared across all groups. So if we have two
    /// exactly-the-same conditions with 10% and 20% rollout percentage respectively, a total of 20% of the users
    /// will be evaluated (versus 30% if group evaluation is done independently).
    pub(crate) fn evaluate_multivariate_inner(
        &self,
        flag_key: &str,
        hash_on_global_rollout_percentage: f64,
        hash_on_group_rollout_percentage: f64,
        properties: &HashMap<String, PostHogFlagFilterPropertyValue>,
    ) -> Result<String, PostHogEvaluationError> {
        if let Some(flag_config) = self.flags.get(flag_key) {
            if !flag_config.active {
                return Err(PostHogEvaluationError::NotAvailable(format!(
                    "The feature flag is not active: {flag_key}"
                )));
            }
            let Some(ref multivariate) = flag_config.filters.multivariate else {
                return Err(PostHogEvaluationError::Internal(format!(
                    "No multivariate available, should use evaluate_boolean?: {flag_key}"
                )));
            };
            // TODO: sort the groups so that variant overrides always get evaluated first and it follows the PostHog
            // Python SDK behavior; for now we do not configure conditions without variant overrides in Neon so it
            // does not matter.
            for group in &flag_config.filters.groups {
                match self.evaluate_group(group, hash_on_group_rollout_percentage, properties)? {
                    GroupEvaluationResult::MatchedAndOverride(variant) => return Ok(variant),
                    GroupEvaluationResult::MatchedAndEvaluate => {
                        let mut percentage = 0;
                        for variant in &multivariate.variants {
                            percentage += variant.rollout_percentage;
                            if self
                                .evaluate_percentage(hash_on_global_rollout_percentage, percentage)
                            {
                                return Ok(variant.key.clone());
                            }
                        }
                        // This should not happen because the rollout percentage always adds up to 100, but just in case that PostHog
                        // returned invalid spec, we return an error.
                        return Err(PostHogEvaluationError::Internal(format!(
                            "Rollout percentage does not add up to 100: {flag_key}"
                        )));
                    }
                    GroupEvaluationResult::Unmatched => continue,
                }
            }
            // If no group is matched, the feature is not available, and up to the caller to decide what to do.
            Err(PostHogEvaluationError::NoConditionGroupMatched)
        } else {
            // The feature flag is not available yet
            Err(PostHogEvaluationError::NotAvailable(format!(
                "Not found in the local evaluation spec: {flag_key}"
            )))
        }
    }

    /// Evaluate a multivariate feature flag. Note that we directly take the mapped user ID
    /// (a consistent hash ranging from 0 to 1) so that it is easier to use it in the tests
    /// and avoid duplicate computations.
    ///
    /// Use a different consistent hash for evaluating the group rollout percentage.
    /// The behavior: if the condition is set to rolling out to 10% of the users, and
    /// we set the variant A to 20% in the global config, then 2% of the total users will
    /// be evaluated to variant A.
    ///
    /// Note that the hash to determine group rollout percentage is shared across all groups. So if we have two
    /// exactly-the-same conditions with 10% and 20% rollout percentage respectively, a total of 20% of the users
    /// will be evaluated (versus 30% if group evaluation is done independently).
    pub(crate) fn evaluate_boolean_inner(
        &self,
        flag_key: &str,
        hash_on_global_rollout_percentage: f64,
        properties: &HashMap<String, PostHogFlagFilterPropertyValue>,
    ) -> Result<(), PostHogEvaluationError> {
        if let Some(flag_config) = self.flags.get(flag_key) {
            if !flag_config.active {
                return Err(PostHogEvaluationError::NotAvailable(format!(
                    "The feature flag is not active: {flag_key}"
                )));
            }
            if flag_config.filters.multivariate.is_some() {
                return Err(PostHogEvaluationError::Internal(format!(
                    "This looks like a multivariate flag, should use evaluate_multivariate?: {flag_key}"
                )));
            };
            // TODO: sort the groups so that variant overrides always get evaluated first and it follows the PostHog
            // Python SDK behavior; for now we do not configure conditions without variant overrides in Neon so it
            // does not matter.
            for group in &flag_config.filters.groups {
                match self.evaluate_group(group, hash_on_global_rollout_percentage, properties)? {
                    GroupEvaluationResult::MatchedAndOverride(_) => {
                        return Err(PostHogEvaluationError::Internal(format!(
                            "Boolean flag cannot have overrides: {flag_key}"
                        )));
                    }
                    GroupEvaluationResult::MatchedAndEvaluate => {
                        return Ok(());
                    }
                    GroupEvaluationResult::Unmatched => continue,
                }
            }
            // If no group is matched, the feature is not available, and up to the caller to decide what to do.
            Err(PostHogEvaluationError::NoConditionGroupMatched)
        } else {
            // The feature flag is not available yet
            Err(PostHogEvaluationError::NotAvailable(format!(
                "Not found in the local evaluation spec: {flag_key}"
            )))
        }
    }

    /// Infer whether a feature flag is a boolean flag by checking if it has a multivariate filter.
    pub fn is_feature_flag_boolean(&self, flag_key: &str) -> Result<bool, PostHogEvaluationError> {
        if let Some(flag_config) = self.flags.get(flag_key) {
            Ok(flag_config.filters.multivariate.is_none())
        } else {
            Err(PostHogEvaluationError::NotAvailable(format!(
                "Not found in the local evaluation spec: {flag_key}"
            )))
        }
    }
}

pub struct PostHogClientConfig {
    /// The server API key.
    pub server_api_key: String,
    /// The client API key.
    pub client_api_key: String,
    /// The project ID.
    pub project_id: String,
    /// The private API URL.
    pub private_api_url: String,
    /// The public API URL.
    pub public_api_url: String,
}

/// A lite PostHog client.
///
/// At the point of writing this code, PostHog does not have a functional Rust client with feature flag support.
/// This is a lite version that only supports local evaluation of feature flags and only supports those JSON specs
/// that will be used within Neon.
///
/// PostHog is designed as a browser-server system: the browser (client) side uses the client key and is exposed
/// to the end users; the server side uses a server key and is not exposed to the end users. The client and the
/// server has different API keys and provide a different set of APIs. In Neon, we only have the server (that is
/// pageserver), and it will use both the client API and the server API. So we need to store two API keys within
/// our PostHog client.
///
/// The server API is used to fetch the feature flag specs. The client API is used to capture events in case we
/// want to report the feature flag usage back to PostHog. The current plan is to use PostHog only as an UI to
/// configure feature flags so it is very likely that the client API will not be used.
pub struct PostHogClient {
    /// The config.
    config: PostHogClientConfig,
    /// The HTTP client.
    client: reqwest::Client,
}

#[derive(Serialize, Debug)]
pub struct CaptureEvent {
    pub event: String,
    pub distinct_id: String,
    pub properties: serde_json::Value,
}

impl PostHogClient {
    pub fn new(config: PostHogClientConfig) -> Self {
        let client = reqwest::Client::new();
        Self { config, client }
    }

    pub fn new_with_us_region(
        server_api_key: String,
        client_api_key: String,
        project_id: String,
    ) -> Self {
        Self::new(PostHogClientConfig {
            server_api_key,
            client_api_key,
            project_id,
            private_api_url: "https://us.posthog.com".to_string(),
            public_api_url: "https://us.i.posthog.com".to_string(),
        })
    }

    /// Check if the server API key is a feature flag secure API key. This key can only be
    /// used to fetch the feature flag specs and can only be used on a undocumented API
    /// endpoint.
    fn is_feature_flag_secure_api_key(&self) -> bool {
        self.config.server_api_key.starts_with("phs_")
    }

    /// Get the raw JSON spec, same as `get_feature_flags_local_evaluation` but without parsing.
    pub async fn get_feature_flags_local_evaluation_raw(&self) -> anyhow::Result<String> {
        // BASE_URL/api/projects/:project_id/feature_flags/local_evaluation
        // with bearer token of self.server_api_key
        // OR
        // BASE_URL/api/feature_flag/local_evaluation/
        // with bearer token of feature flag specific self.server_api_key
        let url = if self.is_feature_flag_secure_api_key() {
            // The new feature local evaluation secure API token
            format!(
                "{}/api/feature_flag/local_evaluation",
                self.config.private_api_url
            )
        } else {
            // The old personal API token
            format!(
                "{}/api/projects/{}/feature_flags/local_evaluation",
                self.config.private_api_url, self.config.project_id
            )
        };
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.config.server_api_key)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(anyhow::anyhow!(
                "Failed to get feature flags: {}, {}",
                status,
                body
            ));
        }
        Ok(body)
    }

    /// Fetch the feature flag specs from the server.
    ///
    /// This is unfortunately an undocumented API at:
    /// - <https://posthog.com/docs/api/feature-flags#get-api-projects-project_id-feature_flags-local_evaluation>
    /// - <https://posthog.com/docs/feature-flags/local-evaluation>
    ///
    /// The handling logic in [`FeatureStore`] mostly follows the Python API implementation.
    /// See `_compute_flag_locally` in <https://github.com/PostHog/posthog-python/blob/master/posthog/client.py>
    pub async fn get_feature_flags_local_evaluation(
        &self,
    ) -> Result<LocalEvaluationResponse, anyhow::Error> {
        let raw = self.get_feature_flags_local_evaluation_raw().await?;
        Ok(serde_json::from_str(&raw)?)
    }

    /// Capture an event. This will only be used to report the feature flag usage back to PostHog, though
    /// it also support a lot of other functionalities.
    ///
    /// <https://posthog.com/docs/api/capture>
    pub async fn capture_event(
        &self,
        event: &str,
        distinct_id: &str,
        properties: &serde_json::Value,
    ) -> anyhow::Result<()> {
        // PUBLIC_URL/capture/
        let url = format!("{}/capture/", self.config.public_api_url);
        let response = self
            .client
            .post(url)
            .body(serde_json::to_string(&json!({
                "api_key": self.config.client_api_key,
                "distinct_id": distinct_id,
                "event": event,
                "properties": properties,
            }))?)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(anyhow::anyhow!(
                "Failed to capture events: {}, {}",
                status,
                body
            ));
        }
        Ok(())
    }

    pub async fn capture_event_batch(&self, events: &[CaptureEvent]) -> anyhow::Result<()> {
        // PUBLIC_URL/batch/
        let url = format!("{}/batch/", self.config.public_api_url);
        let response = self
            .client
            .post(url)
            .body(serde_json::to_string(&json!({
                "api_key": self.config.client_api_key,
                "batch": events,
            }))?)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(anyhow::anyhow!(
                "Failed to capture events: {}, {}",
                status,
                body
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data() -> &'static str {
        r#"{
  "flags": [
    {
      "id": 141807,
      "team_id": 152860,
      "name": "",
      "key": "image-compaction-boundary",
      "filters": {
        "groups": [
          {
            "variant": null,
            "properties": [
              {
                "key": "plan_type",
                "type": "person",
                "value": [
                  "free"
                ],
                "operator": "exact"
              }
            ],
            "rollout_percentage": 40
          },
          {
            "variant": null,
            "properties": [],
            "rollout_percentage": 10
          }
        ],
        "payloads": {},
        "multivariate": null
      },
      "deleted": false,
      "active": true,
      "ensure_experience_continuity": false,
      "has_encrypted_payloads": false,
      "version": 1
    },
    {
      "id": 135586,
      "team_id": 152860,
      "name": "",
      "key": "boolean-flag",
      "filters": {
        "groups": [
          {
            "variant": null,
            "properties": [
              {
                "key": "plan_type",
                "type": "person",
                "value": [
                  "free"
                ],
                "operator": "exact"
              }
            ],
            "rollout_percentage": 47
          }
        ],
        "payloads": {},
        "multivariate": null
      },
      "deleted": false,
      "active": true,
      "ensure_experience_continuity": false,
      "has_encrypted_payloads": false,
      "version": 1
    },
    {
      "id": 132794,
      "team_id": 152860,
      "name": "",
      "key": "gc-compaction",
      "filters": {
        "groups": [
          {
            "variant": "enabled-stage-2",
            "properties": [
              {
                "key": "plan_type",
                "type": "person",
                "value": [
                  "free"
                ],
                "operator": "exact"
              },
              {
                "key": "pageserver_remote_size",
                "type": "person",
                "value": "10000000",
                "operator": "lt"
              }
            ],
             "rollout_percentage": 50
          },
          {
            "properties": [
              {
                "key": "plan_type",
                "type": "person",
                "value": [
                  "free"
                ],
                "operator": "exact"
              },
              {
                "key": "pageserver_remote_size",
                "type": "person",
                "value": "10000000",
                "operator": "lt"
              }
            ],
            "rollout_percentage": 80
          }
        ],
        "payloads": {},
        "multivariate": {
          "variants": [
            {
              "key": "disabled",
              "name": "",
              "rollout_percentage": 90
            },
            {
              "key": "enabled-stage-1",
              "name": "",
              "rollout_percentage": 10
            },
            {
              "key": "enabled-stage-2",
              "name": "",
              "rollout_percentage": 0
            },
            {
              "key": "enabled-stage-3",
              "name": "",
              "rollout_percentage": 0
            },
            {
              "key": "enabled",
              "name": "",
              "rollout_percentage": 0
            }
          ]
        }
      },
      "deleted": false,
      "active": true,
      "ensure_experience_continuity": false,
      "has_encrypted_payloads": false,
      "version": 7
    }
  ],
  "group_type_mapping": {},
  "cohorts": {}
}"#
    }

    #[test]
    fn parse_local_evaluation() {
        let data = data();
        let _: LocalEvaluationResponse = serde_json::from_str(data).unwrap();
    }

    #[test]
    fn evaluate_multivariate() {
        let mut store = FeatureStore::new();
        let response: LocalEvaluationResponse = serde_json::from_str(data()).unwrap();
        store.set_flags(response.flags, None).unwrap();

        // This lacks the required properties and cannot be evaluated.
        let variant =
            store.evaluate_multivariate_inner("gc-compaction", 1.00, 0.40, &HashMap::new());
        assert!(matches!(
            variant,
            Err(PostHogEvaluationError::NotAvailable(_))
        ),);

        let properties_unmatched = HashMap::from([
            (
                "plan_type".to_string(),
                PostHogFlagFilterPropertyValue::String("paid".to_string()),
            ),
            (
                "pageserver_remote_size".to_string(),
                PostHogFlagFilterPropertyValue::Number(1000.0),
            ),
        ]);

        // This does not match any group so there will be an error.
        let variant =
            store.evaluate_multivariate_inner("gc-compaction", 1.00, 0.40, &properties_unmatched);
        assert!(matches!(
            variant,
            Err(PostHogEvaluationError::NoConditionGroupMatched)
        ),);
        let variant =
            store.evaluate_multivariate_inner("gc-compaction", 0.80, 0.80, &properties_unmatched);
        assert!(matches!(
            variant,
            Err(PostHogEvaluationError::NoConditionGroupMatched)
        ),);

        let properties = HashMap::from([
            (
                "plan_type".to_string(),
                PostHogFlagFilterPropertyValue::String("free".to_string()),
            ),
            (
                "pageserver_remote_size".to_string(),
                PostHogFlagFilterPropertyValue::Number(1000.0),
            ),
        ]);

        // It matches the first group as 0.10 <= 0.50 and the properties are matched. Then it gets evaluated to the variant override.
        let variant = store.evaluate_multivariate_inner("gc-compaction", 0.10, 0.10, &properties);
        assert_eq!(variant.unwrap(), "enabled-stage-2".to_string());

        // It matches the second group as 0.50 <= 0.60 <= 0.80 and the properties are matched. Then it gets evaluated using the global percentage.
        let variant = store.evaluate_multivariate_inner("gc-compaction", 0.99, 0.60, &properties);
        assert_eq!(variant.unwrap(), "enabled-stage-1".to_string());
        let variant = store.evaluate_multivariate_inner("gc-compaction", 0.80, 0.60, &properties);
        assert_eq!(variant.unwrap(), "disabled".to_string());

        // It matches the group conditions but not the group rollout percentage.
        let variant = store.evaluate_multivariate_inner("gc-compaction", 1.00, 0.90, &properties);
        assert!(matches!(
            variant,
            Err(PostHogEvaluationError::NoConditionGroupMatched)
        ),);
    }

    #[test]
    fn evaluate_boolean_1() {
        // The `boolean-flag` feature flag only has one group that matches on the free user.

        let mut store = FeatureStore::new();
        let response: LocalEvaluationResponse = serde_json::from_str(data()).unwrap();
        store.set_flags(response.flags, None).unwrap();

        // This lacks the required properties and cannot be evaluated.
        let variant = store.evaluate_boolean_inner("boolean-flag", 1.00, &HashMap::new());
        assert!(matches!(
            variant,
            Err(PostHogEvaluationError::NotAvailable(_))
        ),);

        let properties_unmatched = HashMap::from([
            (
                "plan_type".to_string(),
                PostHogFlagFilterPropertyValue::String("paid".to_string()),
            ),
            (
                "pageserver_remote_size".to_string(),
                PostHogFlagFilterPropertyValue::Number(1000.0),
            ),
        ]);

        // This does not match any group so there will be an error.
        let variant = store.evaluate_boolean_inner("boolean-flag", 1.00, &properties_unmatched);
        assert!(matches!(
            variant,
            Err(PostHogEvaluationError::NoConditionGroupMatched)
        ),);

        let properties = HashMap::from([
            (
                "plan_type".to_string(),
                PostHogFlagFilterPropertyValue::String("free".to_string()),
            ),
            (
                "pageserver_remote_size".to_string(),
                PostHogFlagFilterPropertyValue::Number(1000.0),
            ),
        ]);

        // It matches the first group as 0.10 <= 0.50 and the properties are matched. Then it gets evaluated to the variant override.
        let variant = store.evaluate_boolean_inner("boolean-flag", 0.10, &properties);
        assert!(variant.is_ok());

        // It matches the group conditions but not the group rollout percentage.
        let variant = store.evaluate_boolean_inner("boolean-flag", 1.00, &properties);
        assert!(matches!(
            variant,
            Err(PostHogEvaluationError::NoConditionGroupMatched)
        ),);
    }

    #[test]
    fn evaluate_boolean_2() {
        // The `image-compaction-boundary` feature flag has one group that matches on the free user and a group that matches on all users.

        let mut store = FeatureStore::new();
        let response: LocalEvaluationResponse = serde_json::from_str(data()).unwrap();
        store.set_flags(response.flags, None).unwrap();

        // This lacks the required properties and cannot be evaluated.
        let variant =
            store.evaluate_boolean_inner("image-compaction-boundary", 1.00, &HashMap::new());
        assert!(matches!(
            variant,
            Err(PostHogEvaluationError::NotAvailable(_))
        ),);

        let properties_unmatched = HashMap::from([
            (
                "plan_type".to_string(),
                PostHogFlagFilterPropertyValue::String("paid".to_string()),
            ),
            (
                "pageserver_remote_size".to_string(),
                PostHogFlagFilterPropertyValue::Number(1000.0),
            ),
        ]);

        // This does not match the filtered group but the all user group.
        let variant =
            store.evaluate_boolean_inner("image-compaction-boundary", 1.00, &properties_unmatched);
        assert!(matches!(
            variant,
            Err(PostHogEvaluationError::NoConditionGroupMatched)
        ),);
        let variant =
            store.evaluate_boolean_inner("image-compaction-boundary", 0.05, &properties_unmatched);
        assert!(variant.is_ok());

        let properties = HashMap::from([
            (
                "plan_type".to_string(),
                PostHogFlagFilterPropertyValue::String("free".to_string()),
            ),
            (
                "pageserver_remote_size".to_string(),
                PostHogFlagFilterPropertyValue::Number(1000.0),
            ),
        ]);

        // It matches the first group as 0.30 <= 0.40 and the properties are matched. Then it gets evaluated to the variant override.
        let variant = store.evaluate_boolean_inner("image-compaction-boundary", 0.30, &properties);
        assert!(variant.is_ok());

        // It matches the group conditions but not the group rollout percentage.
        let variant = store.evaluate_boolean_inner("image-compaction-boundary", 1.00, &properties);
        assert!(matches!(
            variant,
            Err(PostHogEvaluationError::NoConditionGroupMatched)
        ),);

        // It matches the second "all" group conditions.
        let variant = store.evaluate_boolean_inner("image-compaction-boundary", 0.09, &properties);
        assert!(variant.is_ok());
    }
}
