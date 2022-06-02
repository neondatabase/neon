//! Declaration of Cloud API V2.

use crate::{
    auth::{self, AuthFlow},
    compute, scram,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::auth::ClientCredentials;
use crate::stream::PqStream;

use tokio::io::{AsyncRead, AsyncWrite};
use utils::pq_proto::{BeMessage as Be, BeParameterStatusMessage};

#[derive(Debug, Error)]
pub enum ConsoleAuthError {
    // We shouldn't include the actual secret here.
    #[error("Bad authentication secret")]
    BadSecret,

    #[error("Bad client credentials: {0:?}")]
    BadCredentials(crate::auth::ClientCredentials),

    #[error("SNI info is missing. EITHER please upgrade the postgres client library OR pass ..&options=cluster:<project name>.. parameter")]
    SniMissingAndProjectNameMissing,

    #[error("Unexpected SNI content")]
    SniWrong,

    #[error(transparent)]
    BadUrl(#[from] url::ParseError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// HTTP status (other than 200) returned by the console.
    #[error("Console responded with an HTTP status: {0}")]
    HttpStatus(reqwest::StatusCode),

    #[error(transparent)]
    Transport(#[from] reqwest::Error),

    #[error("Console responded with a malformed JSON: '{0}'")]
    MalformedResponse(#[from] serde_json::Error),

    #[error("Console responded with a malformed compute address: '{0}'")]
    MalformedComputeAddress(String),
}

#[derive(Serialize, Deserialize, Debug)]
struct GetRoleSecretResponse {
    role_secret: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct GetWakeComputeResponse {
    address: String,
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

async fn get_auth_info(
    auth_endpoint: &str,
    user: &str,
    cluster: &str,
) -> Result<AuthInfo, ConsoleAuthError> {
    let mut url = reqwest::Url::parse(&format!("{auth_endpoint}/proxy_get_role_secret"))?;

    url.query_pairs_mut()
        .append_pair("project", cluster)
        .append_pair("role", user);

    // TODO: use a proper logger
    println!("cplane request: {}", url);

    let resp = reqwest::get(url).await?;
    if !resp.status().is_success() {
        return Err(ConsoleAuthError::HttpStatus(resp.status()));
    }

    let response: GetRoleSecretResponse = serde_json::from_str(resp.text().await?.as_str())?;

    scram::ServerSecret::parse(response.role_secret.as_str())
        .map(AuthInfo::Scram)
        .ok_or(ConsoleAuthError::BadSecret)
}

/// Wake up the compute node and return the corresponding connection info.
async fn wake_compute(
    auth_endpoint: &str,
    cluster: &str,
) -> Result<(String, u16), ConsoleAuthError> {
    let mut url = reqwest::Url::parse(&format!("{auth_endpoint}/proxy_wake_compute"))?;
    url.query_pairs_mut().append_pair("project", cluster);

    // TODO: use a proper logger
    println!("cplane request: {}", url);

    let resp = reqwest::get(url).await?;
    if !resp.status().is_success() {
        return Err(ConsoleAuthError::HttpStatus(resp.status()));
    }

    let response: GetWakeComputeResponse = serde_json::from_str(resp.text().await?.as_str())?;
    let (host, port) = response
        .address
        .split_once(':')
        .ok_or_else(|| ConsoleAuthError::MalformedComputeAddress(response.address.clone()))?;
    let port: u16 = port
        .parse()
        .map_err(|_| ConsoleAuthError::MalformedComputeAddress(response.address.clone()))?;

    Ok((host.to_string(), port))
}

pub async fn handle_user(
    auth_endpoint: &str,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    creds: &ClientCredentials,
) -> Result<compute::NodeInfo, crate::auth::AuthError> {
    // Determine cluster name from SNI (creds.sni_data) or from creds.cluster_option.
    let cluster = match &creds.sni_data {
        //if sni_data exists, use it
        Some(sni_data) => {
            sni_data
                .split_once('.')
                .ok_or(ConsoleAuthError::SniWrong)?
                .0
        }
        //otherwise use cluster_option if it was manually set thought ..&options=cluster:<name> parameter
        None => creds
            .cluster_option
            .as_ref()
            .ok_or(ConsoleAuthError::SniMissingAndProjectNameMissing)?
            .as_str(),
    };

    let user = creds.user.as_str();

    // Step 1: get the auth secret
    let auth_info = get_auth_info(auth_endpoint, user, cluster).await?;

    let flow = AuthFlow::new(client);
    let scram_keys = match auth_info {
        AuthInfo::Md5(_) => {
            // TODO: decide if we should support MD5 in api v2
            return Err(crate::auth::AuthErrorImpl::auth_failed("MD5 is not supported").into());
        }
        AuthInfo::Scram(secret) => {
            let scram = auth::Scram(&secret);
            Some(compute::ScramKeys {
                client_key: flow.begin(scram).await?.authenticate().await?.as_bytes(),
                server_key: secret.server_key.as_bytes(),
            })
        }
    };

    client
        .write_message_noflush(&Be::AuthenticationOk)?
        .write_message_noflush(&BeParameterStatusMessage::encoding())?;

    // Step 2: wake compute
    let (host, port) = wake_compute(auth_endpoint, cluster).await?;

    Ok(compute::NodeInfo {
        db_info: DatabaseInfo {
            host,
            port,
            dbname: creds.dbname.clone(),
            user: creds.user.clone(),
            password: None,
        },
        scram_keys,
    })
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
