//! Wrapper around `std::env::var` for parsing environment variables.

use std::fmt::Display;
use std::str::FromStr;

/// For types `V` that implement [`FromStr`].
pub fn var<V, E>(varname: &str) -> Option<V>
where
    V: FromStr<Err = E>,
    E: Display,
{
    match std::env::var(varname) {
        Ok(s) => Some(
            s.parse()
                .map_err(|e| {
                    format!("failed to parse env var {varname} using FromStr::parse: {e:#}")
                })
                .unwrap(),
        ),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("env var {varname} is not unicode")
        }
    }
}

/// For types `V` that implement [`serde::de::DeserializeOwned`].
pub fn var_serde_json_string<V>(varname: &str) -> Option<V>
where
    V: serde::de::DeserializeOwned,
{
    match std::env::var(varname) {
        Ok(s) => Some({
            let value = serde_json::Value::String(s);
            serde_json::from_value(value)
                .map_err(|e| {
                    format!("failed to parse env var {varname} as a serde_json json string: {e:#}")
                })
                .unwrap()
        }),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("env var {varname} is not unicode")
        }
    }
}

/* BEGIN_HADRON */
pub enum DeploymentMode {
    Local,
    Dev,
    Staging,
    Prod,
}

pub fn get_deployment_mode() -> Option<DeploymentMode> {
    match std::env::var("DEPLOYMENT_MODE") {
        Ok(env) => match env.as_str() {
            "development" => Some(DeploymentMode::Dev),
            "staging" => Some(DeploymentMode::Staging),
            "production" => Some(DeploymentMode::Prod),
            _ => {
                tracing::error!("Unexpected DEPLOYMENT_MODE: {}", env);
                None
            }
        },
        Err(_) => {
            tracing::error!("DEPLOYMENT_MODE not set");
            None
        }
    }
}

pub fn is_dev_or_staging() -> bool {
    matches!(
        get_deployment_mode(),
        Some(DeploymentMode::Dev) | Some(DeploymentMode::Staging)
    )
}

pub enum TestingMode {
    Chaos,
    Stress,
}

pub fn get_test_mode() -> Option<TestingMode> {
    match std::env::var("HADRON_TEST_MODE") {
        Ok(env) => match env.as_str() {
            "chaos" => Some(TestingMode::Chaos),
            "stress" => Some(TestingMode::Stress),
            _ => {
                tracing::error!("Unexpected HADRON_TEST_MODE: {}", env);
                None
            }
        },
        Err(_) => {
            tracing::error!("HADRON_TEST_MODE not set");
            None
        }
    }
}

pub fn is_chaos_testing() -> bool {
    matches!(get_test_mode(), Some(TestingMode::Chaos))
}
/* END_HADRON */
