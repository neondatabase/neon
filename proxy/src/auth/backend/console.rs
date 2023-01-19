//! Cloud API V2.

use super::{ApiCaches, AuthSuccess, CachedNodeInfo, ConsoleReqExtra, NodeInfo};
use crate::{
    auth::{self, AuthFlow, ClientCredentials},
    compute,
    console::messages::{ConsoleError, GetRoleSecret, WakeCompute},
    error::{io_error, UserFacingError},
    http, sasl, scram,
    stream::PqStream,
};
use futures::TryFutureExt;
use reqwest::StatusCode as HttpStatusCode;
use std::future::Future;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{error, info, info_span, warn, Instrument};

/// A go-to error message which doesn't leak any detail.
const REQUEST_FAILED: &str = "Console request failed";

/// Common console API error.
#[derive(Debug, Error)]
pub enum ApiError {
    /// Error returned by the console itself.
    #[error("{REQUEST_FAILED} with {}: {}", .status, .text)]
    Console {
        status: HttpStatusCode,
        text: Box<str>,
    },

    /// Various IO errors like broken pipe or malformed payload.
    #[error("{REQUEST_FAILED}: {0}")]
    Transport(#[from] std::io::Error),
}

impl ApiError {
    /// Returns HTTP status code if it's the reason for failure.
    fn http_status_code(&self) -> Option<HttpStatusCode> {
        use ApiError::*;
        match self {
            Console { status, .. } => Some(*status),
            _ => None,
        }
    }
}

impl UserFacingError for ApiError {
    fn to_string_client(&self) -> String {
        use ApiError::*;
        match self {
            // To minimize risks, only select errors are forwarded to users.
            // Ask @neondatabase/control-plane for review before adding more.
            Console { status, .. } => match *status {
                HttpStatusCode::NOT_FOUND => {
                    // Status 404: failed to get a project-related resource.
                    format!("{REQUEST_FAILED}: endpoint cannot be found")
                }
                HttpStatusCode::NOT_ACCEPTABLE => {
                    // Status 406: endpoint is disabled (we don't allow connections).
                    format!("{REQUEST_FAILED}: endpoint is disabled")
                }
                HttpStatusCode::LOCKED => {
                    // Status 423: project might be in maintenance mode (or bad state).
                    format!("{REQUEST_FAILED}: endpoint is temporary unavailable")
                }
                _ => REQUEST_FAILED.to_owned(),
            },
            _ => REQUEST_FAILED.to_owned(),
        }
    }
}

// Helps eliminate graceless `.map_err` calls without introducing another ctor.
impl From<reqwest::Error> for ApiError {
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
    ApiError(ApiError),
}

// This allows more useful interactions than `#[from]`.
impl<E: Into<ApiError>> From<E> for GetAuthInfoError {
    fn from(e: E) -> Self {
        Self::ApiError(e.into())
    }
}

impl UserFacingError for GetAuthInfoError {
    fn to_string_client(&self) -> String {
        use GetAuthInfoError::*;
        match self {
            // We absolutely should not leak any secrets!
            BadSecret => REQUEST_FAILED.to_owned(),
            // However, API might return a meaningful error.
            ApiError(e) => e.to_string_client(),
        }
    }
}

#[derive(Debug, Error)]
pub enum WakeComputeError {
    #[error("Console responded with a malformed compute address: {0}")]
    BadComputeAddress(Box<str>),

    #[error(transparent)]
    ApiError(ApiError),
}

// This allows more useful interactions than `#[from]`.
impl<E: Into<ApiError>> From<E> for WakeComputeError {
    fn from(e: E) -> Self {
        Self::ApiError(e.into())
    }
}

impl UserFacingError for WakeComputeError {
    fn to_string_client(&self) -> String {
        use WakeComputeError::*;
        match self {
            // We shouldn't show user the address even if it's broken.
            // Besides, user is unlikely to care about this detail.
            BadComputeAddress(_) => REQUEST_FAILED.to_owned(),
            // However, API might return a meaningful error.
            ApiError(e) => e.to_string_client(),
        }
    }
}

/// Auth secret which is managed by the cloud.
pub enum AuthInfo {
    /// Md5 hash of user's password.
    Md5([u8; 16]),

    /// [SCRAM](crate::scram) authentication info.
    Scram(scram::ServerSecret),
}

pub struct Api<'a> {
    endpoint: &'a http::Endpoint,
    extra: &'a ConsoleReqExtra<'a>,
    creds: &'a ClientCredentials<'a>,

    /// For simplicity, caches should live forever.
    caches: &'static ApiCaches,
}

impl<'a> AsRef<ClientCredentials<'a>> for Api<'a> {
    fn as_ref(&self) -> &ClientCredentials<'a> {
        self.creds
    }
}

impl<'a> Api<'a> {
    /// Construct an API object containing the auth parameters.
    pub fn new(
        endpoint: &'a http::Endpoint,
        extra: &'a ConsoleReqExtra<'a>,
        creds: &'a ClientCredentials,
        caches: &'static ApiCaches,
    ) -> Self {
        Self {
            endpoint,
            extra,
            creds,
            caches,
        }
    }

    /// Authenticate the existing user or throw an error.
    pub async fn handle_user(
        &'a self,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin + Send>,
    ) -> auth::Result<AuthSuccess<CachedNodeInfo>> {
        handle_user(client, self, Self::get_auth_info, Self::wake_compute).await
    }
}

impl Api<'_> {
    async fn get_auth_info(&self) -> Result<Option<AuthInfo>, GetAuthInfoError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        async {
            let request = self
                .endpoint
                .get("proxy_get_role_secret")
                .header("X-Request-ID", &request_id)
                .query(&[("session_id", self.extra.session_id)])
                .query(&[
                    ("application_name", self.extra.application_name),
                    ("project", Some(self.creds.project().expect("impossible"))),
                    ("role", Some(self.creds.user)),
                ])
                .build()?;

            info!(url = request.url().as_str(), "sending http request");
            let response = self.endpoint.execute(request).await?;
            let body = match parse_body::<GetRoleSecret>(response).await {
                Ok(body) => body,
                // Error 404 is special: it's ok not to have a secret.
                Err(e) => match e.http_status_code() {
                    Some(HttpStatusCode::NOT_FOUND) => return Ok(None),
                    _otherwise => return Err(e.into()),
                },
            };

            let secret = scram::ServerSecret::parse(&body.role_secret)
                .map(AuthInfo::Scram)
                .ok_or(GetAuthInfoError::BadSecret)?;

            Ok(Some(secret))
        }
        .map_err(crate::error::log_error)
        .instrument(info_span!("get_auth_info", id = request_id))
        .await
    }

    async fn do_wake_compute(&self) -> Result<NodeInfo, WakeComputeError> {
        let project = self.creds.project().expect("impossible");
        let request_id = uuid::Uuid::new_v4().to_string();
        async {
            let request = self
                .endpoint
                .get("proxy_wake_compute")
                .header("X-Request-ID", &request_id)
                .query(&[("session_id", self.extra.session_id)])
                .query(&[
                    ("application_name", self.extra.application_name),
                    ("project", Some(project)),
                ])
                .build()?;

            info!(url = request.url().as_str(), "sending http request");
            let response = self.endpoint.execute(request).await?;
            let body = parse_body::<WakeCompute>(response).await?;

            // Unfortunately, ownership won't let us use `Option::ok_or` here.
            let (host, port) = match parse_host_port(&body.address) {
                None => return Err(WakeComputeError::BadComputeAddress(body.address)),
                Some(x) => x,
            };

            let mut config = compute::ConnCfg::new();
            config
                .host(host)
                .port(port)
                .dbname(self.creds.dbname)
                .user(self.creds.user);

            let node = NodeInfo {
                config,
                aux: body.aux.into(),
            };

            Ok(node)
        }
        .map_err(crate::error::log_error)
        .instrument(info_span!("wake_compute", id = request_id))
        .await
    }

    /// Wake up the compute node and return the corresponding connection info.
    pub async fn wake_compute(&self) -> Result<CachedNodeInfo, WakeComputeError> {
        let key = self.creds.project().expect("impossible");

        // Wake-up might not be needed if the node is still alive.
        if let Some(cached) = self.caches.node_info.get(key) {
            info!("found cached compute node info, skipping wake_compute");
            return Ok(cached);
        }

        let node = self.do_wake_compute().await?;
        let (_, cached) = self.caches.node_info.insert(key.into(), node);

        Ok(cached)
    }
}

/// Common logic for user handling in API V2.
/// We reuse this for a mock API implementation in [`super::postgres`].
pub(super) async fn handle_user<'a, Endpoint, GetAuthInfo, WakeCompute>(
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    endpoint: &'a Endpoint,
    get_auth_info: impl FnOnce(&'a Endpoint) -> GetAuthInfo,
    wake_compute: impl FnOnce(&'a Endpoint) -> WakeCompute,
) -> auth::Result<AuthSuccess<CachedNodeInfo>>
where
    Endpoint: AsRef<ClientCredentials<'a>>,
    GetAuthInfo: Future<Output = Result<Option<AuthInfo>, GetAuthInfoError>>,
    WakeCompute: Future<Output = Result<CachedNodeInfo, WakeComputeError>>,
{
    let creds = endpoint.as_ref();

    info!("fetching user's authentication info");
    let info = get_auth_info(endpoint).await?.unwrap_or_else(|| {
        // If we don't have an authentication secret, we mock one to
        // prevent malicious probing (possible due to missing protocol steps).
        // This mocked secret will never lead to successful authentication.
        info!("authentication info not found, mocking it");
        AuthInfo::Scram(scram::ServerSecret::mock(creds.user, rand::random()))
    });

    let flow = AuthFlow::new(client);
    let scram_keys = match info {
        AuthInfo::Md5(_) => {
            info!("auth endpoint chooses MD5");
            return Err(auth::AuthError::bad_auth_method("MD5"));
        }
        AuthInfo::Scram(secret) => {
            info!("auth endpoint chooses SCRAM");
            let scram = auth::Scram(&secret);
            let client_key = match flow.begin(scram).await?.authenticate().await? {
                sasl::Outcome::Success(key) => key,
                sasl::Outcome::Failure(reason) => {
                    info!("auth backend failed with an error: {reason}");
                    return Err(auth::AuthError::auth_failed(creds.user));
                }
            };

            Some(compute::ScramKeys {
                client_key: client_key.as_bytes(),
                server_key: secret.server_key.as_bytes(),
            })
        }
    };

    let mut node = wake_compute(endpoint).await?;
    if let Some(keys) = scram_keys {
        use tokio_postgres::config::AuthKeys;
        node.config.auth_keys(AuthKeys::ScramSha256(keys));
    }

    Ok(AuthSuccess {
        reported_auth_ok: false,
        value: node,
    })
}

/// Parse http response body, taking status code into account.
async fn parse_body<T: for<'a> serde::Deserialize<'a>>(
    response: reqwest::Response,
) -> Result<T, ApiError> {
    let status = response.status();
    if status.is_success() {
        // We shouldn't log raw body because it may contain secrets.
        info!("request succeeded, processing the body");
        return Ok(response.json().await?);
    }

    // Don't throw an error here because it's not as important
    // as the fact that the request itself has failed.
    let body = response.json().await.unwrap_or_else(|e| {
        warn!("failed to parse error body: {e}");
        ConsoleError {
            error: "reason unclear (malformed error message)".into(),
        }
    });

    let text = body.error;
    error!("console responded with an error ({status}): {text}");
    Err(ApiError::Console { status, text })
}

fn parse_host_port(input: &str) -> Option<(&str, u16)> {
    let (host, port) = input.split_once(':')?;
    Some((host, port.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_host_port() {
        let (host, port) = parse_host_port("127.0.0.1:5432").expect("failed to parse");
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 5432);
    }
}
