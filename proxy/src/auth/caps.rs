use std::{borrow::Cow, fmt::Display, fs, net::IpAddr};

use anyhow::Result;
use camino::Utf8Path;
use jsonwebtoken::{decode, Algorithm, DecodingKey, TokenData, Validation};
use serde::{Deserialize, Serialize};
use utils::http::error::ApiError;

use super::{check_peer_addr_is_in_list, IpPattern};

const TOKEN_ALGORITHM: Algorithm = Algorithm::EdDSA;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Scope {
    Connection,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct Claims {
    pub scope: Scope,
    pub allowed_ips: Option<Vec<IpPattern>>,
    pub endpoint_id: String,
}

impl Claims {
    pub fn check_ip(&self, ip: &IpAddr) -> Option<bool> {
        let allowed_ips = match &self.allowed_ips {
            None => return None,
            Some(allowed_ips) => allowed_ips,
        };
        if allowed_ips.is_empty() {
            return Some(true);
        }

        return Some(check_peer_addr_is_in_list(ip, &allowed_ips));
    }
}

pub struct CapsValidator {
    decoding_key: DecodingKey,
    validation: Validation,
}

impl CapsValidator {
    pub fn new(decoding_key: DecodingKey) -> Self {
        let mut validation = Validation::default();
        validation.algorithms = vec![TOKEN_ALGORITHM];
        Self {
            decoding_key,
            validation,
        }
    }

    pub fn from_key_path(key_path: &Utf8Path) -> Result<Self> {
        let metadata = key_path.metadata()?;
        let decoding_key = if metadata.is_file() {
            let public_key = fs::read(key_path)?;
            DecodingKey::from_ed_pem(&public_key)?
        } else {
            anyhow::bail!("path isn't a file")
        };

        Ok(Self::new(decoding_key))
    }

    pub fn decode(&self, token: &str) -> std::result::Result<TokenData<Claims>, CapsError> {
        return match decode(token, &self.decoding_key, &self.validation) {
            Ok(res) => Ok(res),
            Err(e) => Err(CapsError(Cow::Owned(e.to_string()))),
        };
    }
}

impl std::fmt::Debug for CapsValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CapsValidator")
            .field("validation", &self.validation)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct CapsError(pub Cow<'static, str>);

impl Display for CapsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<CapsError> for ApiError {
    fn from(_value: CapsError) -> Self {
        ApiError::Forbidden("neon_caps validation error".to_string())
    }
}
