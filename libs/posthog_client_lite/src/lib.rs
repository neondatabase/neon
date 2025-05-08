//! A lite version of the PostHog client that only supports local evaluation of feature flags.

use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Digest;

#[derive(Debug, thiserror::Error)]
pub enum PostHogEvaluationError {
    /// The feature flag is not available, for example, because the local evaluation data is not populated yet.
    #[error("Feature flag not available: {0}")]
    NotAvailable(&'static str),
    #[error("No condition group is matched")]
    NoConditionGroupMatched,
    /// Real errors, e.g., the rollout percentage does not add up to 100.
    #[error("Failed to evaluate feature flag: {0}")]
    Internal(&'static str),
}

#[derive(Deserialize)]
pub struct PostHogLocalEvaluationResponse {
    #[allow(dead_code)]
    flags: Vec<PostHogLocalEvaluationFlag>,
}

#[derive(Deserialize)]
pub struct PostHogLocalEvaluationFlag {
    key: String,
    filters: PostHogLocalEvaluationFlagFilters,
    active: bool,
}

#[derive(Deserialize)]
pub struct PostHogLocalEvaluationFlagFilters {
    groups: Vec<PostHogLocalEvaluationFlagFilterGroup>,
    multivariate: PostHogLocalEvaluationFlagMultivariate,
}

#[derive(Deserialize)]
pub struct PostHogLocalEvaluationFlagFilterGroup {
    variant: Option<String>,
    properties: Option<Vec<PostHogLocalEvaluationFlagFilterProperty>>,
    rollout_percentage: i64,
}

#[derive(Deserialize)]
pub struct PostHogLocalEvaluationFlagFilterProperty {
    key: String,
    value: PostHogFlagFilterPropertyValue,
    operator: String,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum PostHogFlagFilterPropertyValue {
    String(String),
    Number(f64),
    Boolean(bool),
    List(Vec<String>),
}

#[derive(Deserialize)]
pub struct PostHogLocalEvaluationFlagMultivariate {
    variants: Vec<PostHogLocalEvaluationFlagMultivariateVariant>,
}

#[derive(Deserialize)]
pub struct PostHogLocalEvaluationFlagMultivariateVariant {
    key: String,
    rollout_percentage: i64,
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
    /// The server API key.
    server_api_key: String,
    /// The client API key.
    client_api_key: String,
    /// The project ID.
    project_id: String,
    /// The private API URL.
    private_api_url: String,
    /// The public API URL.
    public_api_url: String,
    /// The HTTP client.
    client: Arc<reqwest::Client>,
}

pub struct FeatureStore {
    flags: HashMap<String, PostHogLocalEvaluationFlag>,
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

    pub fn set_flags(&mut self, flags: Vec<PostHogLocalEvaluationFlag>) {
        self.flags.clear();
        for flag in flags {
            if flag.active {
                self.flags.insert(flag.key.clone(), flag);
            }
        }
    }

    /// Generate a consistent hash for a user ID (e.g., tenant ID).
    ///
    /// The implementation is different from PostHog SDK. In PostHog SDK, it is sha1 of `user_id.distinct_id.salt`.
    /// However, as we do not upload all of our tenant IDs to PostHog, we do not have the PostHog distinct_id for a
    /// tenant. Therefore, the way we compute it is sha256 of `user_id.feature_id`.
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

    /// Evaluate a condition. Returns `None` if the condition cannot be evaluated due to parsing error or missing
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
                    return Err(PostHogEvaluationError::Internal(
                        "The left side of the condition is not a string",
                    ));
                };
                let PostHogFlagFilterPropertyValue::List(requested) = requested else {
                    // Right should be a list of string
                    return Err(PostHogEvaluationError::Internal(
                        "The right side of the condition is not a list",
                    ));
                };
                Ok(requested.contains(provided))
            }
            "lt" | "gt" => {
                let PostHogFlagFilterPropertyValue::String(requested) = requested else {
                    // Right should be a string
                    return Err(PostHogEvaluationError::Internal(
                        "The right side of the condition is not a string",
                    ));
                };
                let Ok(requested) = requested.parse::<f64>() else {
                    return Err(PostHogEvaluationError::Internal(
                        "Can not parse the right side of the condition as a number",
                    ));
                };
                // Left can either be a number or a string
                let provided = match provided {
                    PostHogFlagFilterPropertyValue::Number(provided) => *provided,
                    PostHogFlagFilterPropertyValue::String(provided) => {
                        let Ok(provided) = provided.parse::<f64>() else {
                            return Err(PostHogEvaluationError::Internal(
                                "Can not parse the left side of the condition as a number",
                            ));
                        };
                        provided
                    }
                    _ => {
                        return Err(PostHogEvaluationError::Internal(
                            "The left side of the condition is not a number or a string",
                        ));
                    }
                };
                match operator {
                    "lt" => Ok(provided < requested),
                    "gt" => Ok(provided > requested),
                    _ => Err(PostHogEvaluationError::Internal("Unreachable code path")),
                }
            }
            _ => Err(PostHogEvaluationError::Internal("Unsupported operator")),
        }
    }

    /// Evaluate a percentage.
    fn evaluate_percentage(&self, mapped_user_id: f64, percentage: i64) -> bool {
        mapped_user_id <= percentage as f64 / 100.0
    }

    /// Evaluate a filter group for a feature flag. Returns `None` if the group is not matched or if there are errors
    /// during the evaluation.
    ///
    /// Return values:
    /// Ok(Some(variant)): matched and evaluated to this value
    /// Ok(None): condition unmatched and not evaluated
    fn evaluate_group(
        &self,
        group: &PostHogLocalEvaluationFlagFilterGroup,
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
                    return Err(PostHogEvaluationError::NotAvailable(
                        "The required property in the condition is not available",
                    ));
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

    /// Evaluate a multivariate feature flag. Returns `None` if the flag is not available or if there are errors
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
    pub fn evaluate_multivariate(
        &self,
        flag_key: &str,
        user_id: &str,
    ) -> Result<String, PostHogEvaluationError> {
        let hash_on_global_rollout_percentage =
            Self::consistent_hash(user_id, flag_key, "multivariate");
        let hash_on_group_rollout_percentage =
            Self::consistent_hash(user_id, flag_key, "within_group");
        self.evaluate_multivariate_inner(
            flag_key,
            hash_on_global_rollout_percentage,
            hash_on_group_rollout_percentage,
            &HashMap::new(),
        )
    }

    /// Evaluate a multivariate feature flag. Returns `None` if the flag is not available or if there are errors
    /// during the evaluation. Note that we directly take the mapped user ID (a consistent hash ranging from 0 to 1)
    /// so that it is easier to use it in the tests and avoid duplicate computations.
    ///
    ///
    /// Use a different consistent hash for evaluating the group rollout percentage.
    /// The behavior: if the condition is set to rolling out to 10% of the users, and
    /// we set the variant A to 20% in the global config, then 2% of the total users will
    /// be evaluated to variant A.
    ///
    /// Note that the hash to determine group rollout percentage is shared across all groups. So if we have two
    /// exactly-the-same conditions with 10% and 20% rollout percentage respectively, a total of 20% of the users
    /// will be evaluated (versus 30% if group evaluation is done independently).
    pub fn evaluate_multivariate_inner(
        &self,
        flag_key: &str,
        hash_on_global_rollout_percentage: f64,
        hash_on_group_rollout_percentage: f64,
        properties: &HashMap<String, PostHogFlagFilterPropertyValue>,
    ) -> Result<String, PostHogEvaluationError> {
        if let Some(flag_config) = self.flags.get(flag_key) {
            // TODO: sort the groups so that variant overrides always get evaluated first and it follows the PostHog
            // Python SDK behavior; for now we do not configure conditions without variant overrides in Neon so it
            // does not matter.
            for group in &flag_config.filters.groups {
                match self.evaluate_group(group, hash_on_group_rollout_percentage, properties)? {
                    GroupEvaluationResult::MatchedAndOverride(variant) => return Ok(variant),
                    GroupEvaluationResult::MatchedAndEvaluate => {
                        let mut percentage = 0;
                        for variant in &flag_config.filters.multivariate.variants {
                            percentage += variant.rollout_percentage;
                            if self
                                .evaluate_percentage(hash_on_global_rollout_percentage, percentage)
                            {
                                return Ok(variant.key.clone());
                            }
                        }
                        // This should not happen because the rollout percentage always adds up to 100, but just in case,
                        // return None if we end up here.
                        return Err(PostHogEvaluationError::Internal(
                            "Rollout percentage does not add up to 100",
                        ));
                    }
                    GroupEvaluationResult::Unmatched => continue,
                }
            }
            // If no group is matched, the feature is not available, and up to the caller to decide what to do.
            Err(PostHogEvaluationError::NoConditionGroupMatched)
        } else {
            // The feature flag is not available yet
            Err(PostHogEvaluationError::NotAvailable(
                "Not found in the local evaluation spec",
            ))
        }
    }
}

impl PostHogClient {
    pub fn new(
        server_api_key: String,
        client_api_key: String,
        project_id: String,
        private_api_url: String,
        public_api_url: String,
    ) -> Self {
        let client = reqwest::Client::new();
        Self {
            server_api_key,
            client_api_key,
            project_id,
            private_api_url,
            public_api_url,
            client: Arc::new(client),
        }
    }

    pub fn new_with_us_region(
        server_api_key: String,
        client_api_key: String,
        project_id: String,
    ) -> Self {
        Self::new(
            server_api_key,
            client_api_key,
            project_id,
            "https://us.posthog.com".to_string(),
            "https://us.i.posthog.com".to_string(),
        )
    }

    pub async fn get_feature_flags_local_evaluation(
        &self,
    ) -> anyhow::Result<PostHogLocalEvaluationResponse> {
        // BASE_URL/api/projects/:project_id/feature_flags/local_evaluation
        // with bearer token of self.server_api_key
        let url = format!(
            "{}/api/projects/{}/feature_flags/local_evaluation",
            self.private_api_url, self.project_id
        );
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.server_api_key)
            .send()
            .await?;
        let body = response.text().await?;
        Ok(serde_json::from_str(&body)?)
    }

    pub async fn capture_event(
        &self,
        event: &str,
        distinct_id: &str,
        properties: &HashMap<String, PostHogFlagFilterPropertyValue>,
    ) -> anyhow::Result<()> {
        // PUBLIC_URL/capture/
        // with bearer token of self.client_api_key
        let url = format!("{}/capture/", self.public_api_url);
        self.client
            .post(url)
            .body(serde_json::to_string(&json!({
                "api_key": self.client_api_key,
                "distinct_id": distinct_id,
                "event": event,
                "properties": properties,
            }))?)
            .send()
            .await?;
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
                    "version": 6
                }
            ],
            "group_type_mapping": {},
            "cohorts": {}
        }"#
    }

    #[test]
    fn parse_local_evaluation() {
        let data = data();
        let _: PostHogLocalEvaluationResponse = serde_json::from_str(data).unwrap();
    }

    #[test]
    fn evaluate_multivariate() {
        let mut store = FeatureStore::new();
        let response: PostHogLocalEvaluationResponse = serde_json::from_str(data()).unwrap();
        store.set_flags(response.flags);

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
}
