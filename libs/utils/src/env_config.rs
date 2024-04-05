use std::{fmt::Display, str::FromStr};

pub fn var<V, E, D>(varname: &str, default: D) -> V
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

pub struct Bool(bool);

impl Bool {
    pub const fn new_const<const V: bool>() -> Self {
        Bool(V)
    }
}

impl FromStr for Bool {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(b) = s.parse() {
            return Ok(Bool(b));
        }
        Ok(Bool(match s {
            "0" => false,
            "1" => true,
            _ => return Err(format!("not a bool, accepting 0|1|{}|{}", false, true)),
        }))
    }
}

impl Into<bool> for Bool {
    fn into(self) -> bool {
        self.0
    }
}
