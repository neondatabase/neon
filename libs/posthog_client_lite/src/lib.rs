//! A lite version of the PostHog client that only supports local evaluation of feature flags.

use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::Digest;

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
    rollout_percentage: f64,
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
    rollout_percentage: f64,
}

pub struct PostHogClient {
    server_api_key: String,
    client_api_key: String,
    project_id: String,
    private_api_url: String,
    public_api_url: String,
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

impl FeatureStore {
    pub fn new() -> Self {
        Self {
            flags: HashMap::new(),
        }
    }

    pub fn update_flags(&mut self, flags: Vec<PostHogLocalEvaluationFlag>) {
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
    /// tenant.
    fn consistent_hash(user_id: &str) -> f64 {
        let mut hasher = sha2::Sha256::new();
        hasher.update(user_id);
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
    ) -> Option<bool> {
        match operator {
            "exact" => {
                let PostHogFlagFilterPropertyValue::String(provided) = provided else {
                    // Left should be a string
                    return None;
                };
                let PostHogFlagFilterPropertyValue::List(requested) = requested else {
                    // Right should be a list of string
                    return None;
                };
                Some(requested.contains(provided))
            }
            "lt" | "gt" => {
                let PostHogFlagFilterPropertyValue::String(requested) = requested else {
                    // Right should be a string
                    return None;
                };
                let Ok(requested) = requested.parse::<f64>() else {
                    return None;
                };
                // Left can either be a number or a string
                let provided = match provided {
                    PostHogFlagFilterPropertyValue::Number(provided) => *provided,
                    PostHogFlagFilterPropertyValue::String(provided) => {
                        let Ok(provided) = provided.parse::<f64>() else {
                            return None;
                        };
                        provided
                    }
                    _ => return None,
                };
                match operator {
                    "lt" => Some(provided < requested),
                    "gt" => Some(provided > requested),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Evaluate a percentage.
    fn evaluate_percentage(&self, mapped_user_id: f64, percentage: f64) -> bool {
        mapped_user_id <= percentage
    }

    /// Evaluate a filter group for a feature flag. Returns `None` if the group is not matched or if there are errors
    /// during the evaluation.
    fn evaluate_group(
        &self,
        group: &PostHogLocalEvaluationFlagFilterGroup,
        mapped_user_id: f64,
        provided_properties: &HashMap<String, PostHogFlagFilterPropertyValue>,
    ) -> Option<String> {
        if let Some(ref properties) = group.properties {
            for property in properties {
                if let Some(value) = provided_properties.get(&property.key) {
                    // The user provided the property value
                    if self.evaluate_condition(property.operator.as_ref(), value, &property.value)
                        != Some(true)
                    {
                        return None;
                    }
                } else {
                    // We cannot evaluate, the property is not available
                    return None;
                }
            }
            // If all conditions are met, return the variant
            if self.evaluate_percentage(mapped_user_id, group.rollout_percentage / 100.0) {
                group.variant.clone()
            } else {
                None
            }
        } else {
            // No matchers, apply to all users
            if self.evaluate_percentage(mapped_user_id, group.rollout_percentage / 100.0) {
                group.variant.clone()
            } else {
                None
            }
        }
    }

    /// Evaluate a multivariate feature flag. Returns `None` if the flag is not available or if there are errors
    /// during the evaluation.
    ///
    /// The parsing logic is as follows:
    ///
    /// * Match each filter group. If any group is matched, check the rollout percentage against the precomputed
    ///   user ID on the consistent hash ring. If the user ID is in the range of the group's rollout percentage,
    ///   return the variant.
    /// * Otherwise, continue with the next group until all groups are evaluated and no group is within the
    ///   rollout percentage.
    /// * If there are no matching groups, evaluate by the global rollout percentage.
    pub fn evaluate_multivariate(&self, flag_key: &str, user_id: &str) -> Option<String> {
        let mapped_user_id = Self::consistent_hash(user_id);
        self.evaluate_multivariate_inner(flag_key, mapped_user_id, &HashMap::new())
    }

    /// Evaluate a multivariate feature flag. Returns `None` if the flag is not available or if there are errors
    /// during the evaluation. Note that we directly take the mapped user ID (a consistent hash ranging from 0 to 1)
    /// so that it is easier to use it in the tests and avoid duplicate computations.
    pub fn evaluate_multivariate_inner(
        &self,
        flag_key: &str,
        mapped_user_id: f64,
        properties: &HashMap<String, PostHogFlagFilterPropertyValue>,
    ) -> Option<String> {
        if let Some(flag_config) = self.flags.get(flag_key) {
            // First, try to evaluate each group
            for group in &flag_config.filters.groups {
                if let Some(variant) = self.evaluate_group(group, mapped_user_id, properties) {
                    return Some(variant);
                }
            }
            // If no group is matched, evaluate by the global rollout percentage
            let mut percentage = 0.0;
            for variant in &flag_config.filters.multivariate.variants {
                percentage += variant.rollout_percentage;
                if self.evaluate_percentage(mapped_user_id, percentage / 100.0) {
                    return Some(variant.key.clone());
                }
            }
            // This should not happen because the rollout percentage always adds up to 100, but just in case,
            // return None if we end up here.
            None
        } else {
            // The feature flag is not available yet
            None
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
                                "rollout_percentage": 66
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
        store.update_flags(response.flags);
        let variant = store.evaluate_multivariate_inner("gc-compaction", 1.00, &HashMap::new());
        assert_eq!(variant, Some("enabled-stage-1".to_string()));
        let variant = store.evaluate_multivariate_inner("gc-compaction", 0.80, &HashMap::new());
        assert_eq!(variant, Some("disabled".to_string()));
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
        let variant = store.evaluate_multivariate_inner("gc-compaction", 0.10, &properties);
        assert_eq!(variant, Some("enabled-stage-2".to_string()));
    }
}
