use std::{fmt::Display, str::FromStr};

pub fn var_or_else<V, E, D>(varname: &str, default: D) -> V
where
    V: FromStr<Err = E>,
    E: Display,
    D: FnOnce() -> V,
{
    match std::env::var(varname) {
        Ok(s) => s
            .parse()
            .map_err(|e| format!("failed to parse env var {varname}: {e:#}"))
            .unwrap(),
        Err(std::env::VarError::NotPresent) => default(),
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("env var {varname} is not unicode")
        }
    }
}

pub fn var<V, E>(varname: &str) -> Option<V>
where
    V: FromStr<Err = E>,
    E: Display,
{
    match std::env::var(varname) {
        Ok(s) => Some(
            s.parse()
                .map_err(|e| format!("failed to parse env var {varname}: {e:#}"))
                .unwrap(),
        ),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("env var {varname} is not unicode")
        }
    }
}
