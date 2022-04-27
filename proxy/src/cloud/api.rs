//! Declaration of Cloud API V2.

use crate::{auth, scram};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GetAuthInfoError {
    // We shouldn't include the actual secret here.
    #[error("Bad authentication secret")]
    BadSecret,

    #[error("Bad client credentials: {0:?}")]
    BadCredentials(crate::auth::ClientCredentials),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

// TODO: convert to an enum and describe possible sub-errors (see above)
#[derive(Debug, Error)]
#[error("Failed to wake up the compute node")]
pub struct WakeComputeError;

/// Opaque implementation of Cloud API.
pub type BoxedApi = Box<dyn Api + Send + Sync>;

/// Cloud API methods required by the proxy.
#[async_trait]
pub trait Api {
    /// Get authentication information for the given user.
    async fn get_auth_info(
        &self,
        creds: &auth::ClientCredentials,
    ) -> Result<AuthInfo, GetAuthInfoError>;

    /// Wake up the compute node and return the corresponding connection info.
    async fn wake_compute(
        &self,
        creds: &auth::ClientCredentials,
    ) -> Result<DatabaseInfo, WakeComputeError>;
}

/// Auth secret which is managed by the cloud.
pub enum AuthInfo {
    /// Md5 hash of user's password.
    Md5([u8; 16]),
    /// [SCRAM](crate::scram) authentication info.
    Scram(scram::ServerSecret),
}

/// Compute node connection params provided by the cloud.
/// Note how it implements serde traits, since we receive it over the wire.
#[derive(Serialize, Deserialize, Default)]
pub struct DatabaseInfo {
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub user: String,

    /// [Cloud API V1](super::legacy) returns cleartext password,
    /// but [Cloud API V2](super::api) implements [SCRAM](crate::scram)
    /// authentication, so we can leverage this method and cope without password.
    pub password: Option<String>,
}

// Manually implement debug to omit personal and sensitive info.
impl std::fmt::Debug for DatabaseInfo {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.debug_struct("DatabaseInfo")
            .field("host", &self.host)
            .field("port", &self.port)
            .finish()
    }
}

impl From<DatabaseInfo> for tokio_postgres::Config {
    fn from(db_info: DatabaseInfo) -> Self {
        let mut config = tokio_postgres::Config::new();

        config
            .host(&db_info.host)
            .port(db_info.port)
            .dbname(&db_info.dbname)
            .user(&db_info.user);

        if let Some(password) = db_info.password {
            config.password(password);
        }

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_db_info() -> anyhow::Result<()> {
        let _: DatabaseInfo = serde_json::from_value(json!({
            "host": "localhost",
            "port": 5432,
            "dbname": "postgres",
            "user": "john_doe",
            "password": "password",
        }))?;

        let _: DatabaseInfo = serde_json::from_value(json!({
            "host": "localhost",
            "port": 5432,
            "dbname": "postgres",
            "user": "john_doe",
        }))?;

        Ok(())
    }
}
