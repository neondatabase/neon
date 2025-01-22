//! Wrapper around `std::env::var` for parsing environment variables.

use std::{fmt::Display, str::FromStr};

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
