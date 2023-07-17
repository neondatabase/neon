//! Production console backend.

use super::{
    super::messages::{ConsoleError, GetRoleSecret, WakeCompute},
    errors::{ApiError, GetAuthInfoError, WakeComputeError},
    ApiCaches, AuthInfo, CachedNodeInfo, ConsoleReqExtra, NodeInfo,
};
use crate::{auth::ClientCredentials, compute, http, scram};
use async_trait::async_trait;
use futures::TryFutureExt;
use tokio_postgres::config::SslMode;
use tracing::{error, info, info_span, warn, Instrument};

#[derive(Clone)]
pub struct Api {
    endpoint: http::Endpoint,
    caches: &'static ApiCaches,
}

impl Api {
    /// Construct an API object containing the auth parameters.
    pub fn new(endpoint: http::Endpoint, caches: &'static ApiCaches) -> Self {
        Self { endpoint, caches }
    }

    pub fn url(&self) -> &str {
        self.endpoint.url().as_str()
    }

    async fn do_get_auth_info(
        &self,
        extra: &ConsoleReqExtra<'_>,
        creds: &ClientCredentials<'_>,
    ) -> Result<Option<AuthInfo>, GetAuthInfoError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        async {
            let request = self
                .endpoint
                .get("proxy_get_role_secret")
                .header("X-Request-ID", &request_id)
                .query(&[("session_id", extra.session_id)])
                .query(&[
                    ("application_name", extra.application_name),
                    ("project", Some(creds.project().expect("impossible"))),
                    ("role", Some(creds.user)),
                ])
                .build()?;

            info!(url = request.url().as_str(), "sending http request");
            let response = self.endpoint.execute(request).await?;
            let body = match parse_body::<GetRoleSecret>(response).await {
                Ok(body) => body,
                // Error 404 is special: it's ok not to have a secret.
                Err(e) => match e.http_status_code() {
                    Some(http::StatusCode::NOT_FOUND) => return Ok(None),
                    _otherwise => return Err(e.into()),
                },
            };

            let secret = scram::ServerSecret::parse(&body.role_secret)
                .map(AuthInfo::Scram)
                .ok_or(GetAuthInfoError::BadSecret)?;

            Ok(Some(secret))
        }
        .map_err(crate::error::log_error)
        .instrument(info_span!("http", id = request_id))
        .await
    }

    async fn do_wake_compute(
        &self,
        extra: &ConsoleReqExtra<'_>,
        creds: &ClientCredentials<'_>,
    ) -> Result<NodeInfo, WakeComputeError> {
        let project = creds.project().expect("impossible");
        let request_id = uuid::Uuid::new_v4().to_string();
        async {
            let request = self
                .endpoint
                .get("proxy_wake_compute")
                .header("X-Request-ID", &request_id)
                .query(&[("session_id", extra.session_id)])
                .query(&[
                    ("application_name", extra.application_name),
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

            // Don't set anything but host and port! This config will be cached.
            // We'll set username and such later using the startup message.
            // TODO: add more type safety (in progress).
            let mut config = compute::ConnCfg::new();
            config.host(host).port(port).ssl_mode(SslMode::Disable); // TLS is not configured on compute nodes.

            let node = NodeInfo {
                config,
                aux: body.aux.into(),
                allow_self_signed_compute: false,
            };

            Ok(node)
        }
        .map_err(crate::error::log_error)
        .instrument(info_span!("http", id = request_id))
        .await
    }
}

#[async_trait]
impl super::Api for Api {
    #[tracing::instrument(skip_all)]
    async fn get_auth_info(
        &self,
        extra: &ConsoleReqExtra<'_>,
        creds: &ClientCredentials,
    ) -> Result<Option<AuthInfo>, GetAuthInfoError> {
        self.do_get_auth_info(extra, creds).await
    }

    #[tracing::instrument(skip_all)]
    async fn wake_compute(
        &self,
        extra: &ConsoleReqExtra<'_>,
        creds: &ClientCredentials,
    ) -> Result<CachedNodeInfo, WakeComputeError> {
        let key = creds.project().expect("impossible");

        // Every time we do a wakeup http request, the compute node will stay up
        // for some time (highly depends on the console's scale-to-zero policy);
        // The connection info remains the same during that period of time,
        // which means that we might cache it to reduce the load and latency.
        if let Some(cached) = self.caches.node_info.get(key) {
            info!(key = key, "found cached compute node info");
            return Ok(cached);
        }

        let node = self.do_wake_compute(extra, creds).await?;
        let (_, cached) = self.caches.node_info.insert(key.into(), node);
        info!(key = key, "created a cache entry for compute node info");

        Ok(cached)
    }
}

/// Parse http response body, taking status code into account.
async fn parse_body<T: for<'a> serde::Deserialize<'a>>(
    response: http::Response,
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
