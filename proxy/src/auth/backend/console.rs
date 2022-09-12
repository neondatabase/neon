//! Cloud API V2.

use super::ConsoleReqExtra;
use crate::{
    auth::{self, AuthFlow, ClientCredentials},
    compute::{self, ComputeConnCfg},
    error::{io_error, UserFacingError},
    http, scram,
    stream::PqStream,
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

const REQUEST_FAILED: &str = "Console request failed";

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("Console responded with a malformed JSON: {0}")]
    BadResponse(#[from] serde_json::Error),

    /// HTTP status (other than 200) returned by the console.
    #[error("Console responded with an HTTP status: {0}")]
    HttpStatus(reqwest::StatusCode),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl UserFacingError for TransportError {
    fn to_string_client(&self) -> String {
        use TransportError::*;
        match self {
            HttpStatus(_) => self.to_string(),
            _ => REQUEST_FAILED.to_owned(),
        }
    }
}

// Helps eliminate graceless `.map_err` calls without introducing another ctor.
impl From<reqwest::Error> for TransportError {
    fn from(e: reqwest::Error) -> Self {
        io_error(e).into()
    }
}

#[derive(Debug, Error)]
pub enum GetAuthInfoError {
    // We shouldn't include the actual secret here.
    #[error("Console responded with a malformed auth secret")]
    BadSecret,

    #[error(transparent)]
    Transport(TransportError),
}

impl UserFacingError for GetAuthInfoError {
    fn to_string_client(&self) -> String {
        use GetAuthInfoError::*;
        match self {
            BadSecret => REQUEST_FAILED.to_owned(),
            Transport(e) => e.to_string_client(),
        }
    }
}

impl<E: Into<TransportError>> From<E> for GetAuthInfoError {
    fn from(e: E) -> Self {
        Self::Transport(e.into())
    }
}

#[derive(Debug, Error)]
pub enum WakeComputeError {
    // We shouldn't show users the address even if it's broken.
    #[error("Console responded with a malformed compute address: {0}")]
    BadComputeAddress(String),

    #[error(transparent)]
    Transport(TransportError),
}

impl UserFacingError for WakeComputeError {
    fn to_string_client(&self) -> String {
        use WakeComputeError::*;
        match self {
            BadComputeAddress(_) => REQUEST_FAILED.to_owned(),
            Transport(e) => e.to_string_client(),
        }
    }
}

impl<E: Into<TransportError>> From<E> for WakeComputeError {
    fn from(e: E) -> Self {
        Self::Transport(e.into())
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
    endpoint: &'a http::Endpoint,
    extra: &'a ConsoleReqExtra<'a>,
    creds: &'a ClientCredentials<'a>,
}

impl<'a> Api<'a> {
    /// Construct an API object containing the auth parameters.
    pub(super) fn new(
        endpoint: &'a http::Endpoint,
        extra: &'a ConsoleReqExtra<'a>,
        creds: &'a ClientCredentials,
    ) -> Self {
        Self {
            endpoint,
            extra,
            creds,
        }
    }

    /// Authenticate the existing user or throw an error.
    pub(super) async fn handle_user(
        self,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
    ) -> auth::Result<compute::NodeInfo> {
        handle_user(client, &self, Self::get_auth_info, Self::wake_compute).await
    }

    async fn get_auth_info(&self) -> Result<AuthInfo, GetAuthInfoError> {
        let req = self
            .endpoint
            .get("proxy_get_role_secret")
            .header("X-Request-ID", uuid::Uuid::new_v4().to_string())
            .query(&[("session_id", self.extra.session_id)])
            .query(&[
                ("application_name", self.extra.application_name),
                ("project", Some(self.creds.project().expect("impossible"))),
                ("role", Some(self.creds.user)),
            ])
            .build()?;

        // TODO: use a proper logger
        println!("cplane request: {}", req.url());

        let resp = self.endpoint.execute(req).await?;
        if !resp.status().is_success() {
            return Err(TransportError::HttpStatus(resp.status()).into());
        }

        let response: GetRoleSecretResponse = serde_json::from_str(&resp.text().await?)?;

        scram::ServerSecret::parse(&response.role_secret)
            .map(AuthInfo::Scram)
            .ok_or(GetAuthInfoError::BadSecret)
    }

    /// Wake up the compute node and return the corresponding connection info.
    pub(super) async fn wake_compute(&self) -> Result<ComputeConnCfg, WakeComputeError> {
        let req = self
            .endpoint
            .get("proxy_wake_compute")
            .header("X-Request-ID", uuid::Uuid::new_v4().to_string())
            .query(&[("session_id", self.extra.session_id)])
            .query(&[
                ("application_name", self.extra.application_name),
                ("project", Some(self.creds.project().expect("impossible"))),
            ])
            .build()?;

        // TODO: use a proper logger
        println!("cplane request: {}", req.url());

        let resp = self.endpoint.execute(req).await?;
        if !resp.status().is_success() {
            return Err(TransportError::HttpStatus(resp.status()).into());
        }

        let response: GetWakeComputeResponse = serde_json::from_str(&resp.text().await?)?;

        // Unfortunately, ownership won't let us use `Option::ok_or` here.
        let (host, port) = match parse_host_port(&response.address) {
            None => return Err(WakeComputeError::BadComputeAddress(response.address)),
            Some(x) => x,
        };

        let mut config = ComputeConnCfg::new();
        config
            .host(host)
            .port(port)
            .dbname(self.creds.dbname)
            .user(self.creds.user);

        Ok(config)
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
    GetAuthInfo: Future<Output = Result<AuthInfo, GetAuthInfoError>>,
    WakeCompute: Future<Output = Result<ComputeConnCfg, WakeComputeError>>,
{
    let auth_info = get_auth_info(endpoint).await?;

    let flow = AuthFlow::new(client);
    let scram_keys = match auth_info {
        AuthInfo::Md5(_) => {
            // TODO: decide if we should support MD5 in api v2
            return Err(auth::AuthError::bad_auth_method("MD5"));
        }
        AuthInfo::Scram(secret) => {
            let scram = auth::Scram(&secret);
            Some(compute::ScramKeys {
                client_key: flow.begin(scram).await?.authenticate().await?.as_bytes(),
                server_key: secret.server_key.as_bytes(),
            })
        }
    };

    let mut config = wake_compute(endpoint).await?;
    if let Some(keys) = scram_keys {
        config.auth_keys(tokio_postgres::config::AuthKeys::ScramSha256(keys));
    }

    Ok(compute::NodeInfo {
        reported_auth_ok: false,
        config,
    })
}

fn parse_host_port(input: &str) -> Option<(&str, u16)> {
    let (host, port) = input.split_once(':')?;
    Some((host, port.parse().ok()?))
}
