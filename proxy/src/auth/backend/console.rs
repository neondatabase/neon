//! Cloud API V2.

use crate::{
    auth::{self, AuthFlow, ClientCredentials, DatabaseInfo},
    compute,
    error::UserFacingError,
    scram,
    stream::PqStream,
    url::ApiUrl,
};
use serde::{Deserialize, Serialize};
use std::{future::Future, io};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use utils::pq_proto::{BeMessage as Be, BeParameterStatusMessage};

pub type Result<T> = std::result::Result<T, ConsoleAuthError>;

#[derive(Debug, Error)]
pub enum ConsoleAuthError {
    #[error(transparent)]
    BadProjectName(#[from] auth::credentials::ClientCredsParseError),

    // We shouldn't include the actual secret here.
    #[error("Bad authentication secret")]
    BadSecret,

    #[error("Console responded with a malformed compute address: '{0}'")]
    BadComputeAddress(String),

    #[error("Console responded with a malformed JSON: '{0}'")]
    BadResponse(#[from] serde_json::Error),

    /// HTTP status (other than 200) returned by the console.
    #[error("Console responded with an HTTP status: {0}")]
    HttpStatus(reqwest::StatusCode),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl UserFacingError for ConsoleAuthError {
    fn to_string_client(&self) -> String {
        use ConsoleAuthError::*;
        match self {
            BadProjectName(e) => e.to_string_client(),
            _ => "Internal error".to_string(),
        }
    }
}

impl From<&auth::credentials::ClientCredsParseError> for ConsoleAuthError {
    fn from(e: &auth::credentials::ClientCredsParseError) -> Self {
        ConsoleAuthError::BadProjectName(e.clone())
    }
}

// TODO: convert into an enum with "error"
#[derive(Serialize, Deserialize, Debug)]
struct GetRoleSecretResponse {
    role_secret: String,
}

// TODO: convert into an enum with "error"
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

#[must_use]
pub(super) struct Api<'a> {
    endpoint: &'a ApiUrl,
    creds: &'a ClientCredentials,
}

impl<'a> Api<'a> {
    /// Construct an API object containing the auth parameters.
    pub(super) fn new(endpoint: &'a ApiUrl, creds: &'a ClientCredentials) -> Result<Self> {
        Ok(Self { endpoint, creds })
    }

    /// Authenticate the existing user or throw an error.
    pub(super) async fn handle_user(
        self,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
    ) -> auth::Result<compute::NodeInfo> {
        handle_user(client, &self, Self::get_auth_info, Self::wake_compute).await
    }

    async fn get_auth_info(&self) -> Result<AuthInfo> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut().push("proxy_get_role_secret");
        url.query_pairs_mut()
            .append_pair("project", self.creds.project_name.as_ref()?)
            .append_pair("role", &self.creds.user);

        // TODO: use a proper logger
        println!("cplane request: {url}");

        let resp = reqwest::get(url.into_inner()).await.map_err(io_error)?;
        if !resp.status().is_success() {
            return Err(ConsoleAuthError::HttpStatus(resp.status()));
        }

        let response: GetRoleSecretResponse =
            serde_json::from_str(&resp.text().await.map_err(io_error)?)?;

        scram::ServerSecret::parse(response.role_secret.as_str())
            .map(AuthInfo::Scram)
            .ok_or(ConsoleAuthError::BadSecret)
    }

    /// Wake up the compute node and return the corresponding connection info.
    async fn wake_compute(&self) -> Result<DatabaseInfo> {
        let mut url = self.endpoint.clone();
        url.path_segments_mut().push("proxy_wake_compute");
        let project_name = self.creds.project_name.as_ref()?;
        url.query_pairs_mut().append_pair("project", project_name);

        // TODO: use a proper logger
        println!("cplane request: {url}");

        let resp = reqwest::get(url.into_inner()).await.map_err(io_error)?;
        if !resp.status().is_success() {
            return Err(ConsoleAuthError::HttpStatus(resp.status()));
        }

        let response: GetWakeComputeResponse =
            serde_json::from_str(&resp.text().await.map_err(io_error)?)?;

        let (host, port) = parse_host_port(&response.address)
            .ok_or(ConsoleAuthError::BadComputeAddress(response.address))?;

        Ok(DatabaseInfo {
            host,
            port,
            dbname: self.creds.dbname.to_owned(),
            user: self.creds.user.to_owned(),
            password: None,
        })
    }
}

/// Common logic for user handling in API V2.
/// We reuse this for a mock API implementation in [`super::postgres`].
pub(super) async fn handle_user<'a, Endpoint, GetAuthInfo, WakeCompute>(
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    endpoint: &'a Endpoint,
    get_auth_info: impl FnOnce(&'a Endpoint) -> GetAuthInfo,
    wake_compute: impl FnOnce(&'a Endpoint) -> WakeCompute,
) -> auth::Result<compute::NodeInfo>
where
    GetAuthInfo: Future<Output = Result<AuthInfo>>,
    WakeCompute: Future<Output = Result<DatabaseInfo>>,
{
    let auth_info = get_auth_info(endpoint).await?;

    let flow = AuthFlow::new(client);
    let scram_keys = match auth_info {
        AuthInfo::Md5(_) => {
            // TODO: decide if we should support MD5 in api v2
            return Err(auth::AuthErrorImpl::auth_failed("MD5 is not supported").into());
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

    Ok(compute::NodeInfo {
        db_info: wake_compute(endpoint).await?,
        scram_keys,
    })
}

/// Upcast (almost) any error into an opaque [`io::Error`].
pub(super) fn io_error(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

fn parse_host_port(input: &str) -> Option<(String, u16)> {
    let (host, port) = input.split_once(':')?;
    Some((host.to_owned(), port.parse().ok()?))
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
