//! Production console backend.

use super::{
    super::messages::{ConsoleError, GetRoleSecret, WakeCompute},
    errors::{ApiError, GetAuthInfoError, WakeComputeError},
    ApiCaches, ConsoleReqExtra,
};
use super::{AuthInfo, AuthInfoCacheKey, CachedAuthInfo};
use super::{CachedNodeInfo, NodeInfo};
use crate::{auth::ClientCredentials, http, scram};
use async_trait::async_trait;
use futures::TryFutureExt;
use tracing::{error, info, info_span, warn, Instrument};

#[derive(Clone)]
pub struct Api {
    pub endpoint: http::Endpoint,
    pub caches: &'static ApiCaches,
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

            let node = NodeInfo {
                address: body.address,
                aux: body.aux,
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
        creds: &ClientCredentials<'_>,
    ) -> Result<Option<CachedAuthInfo>, GetAuthInfoError> {
        let key = AuthInfoCacheKey {
            project: creds.project().expect("impossible").into(),
            role: creds.user.into(),
        };

        // Check if we already have a cached auth info for this project + user combo.
        // Beware! We shouldn't flush this for unsuccessful auth attempts, otherwise
        // the cache makes no sense whatsoever in the presence of unfaithful clients.
        // Instead, we snoop an invalidation queue to keep the cache up-to-date.
        if let Some(cached) = self.caches.auth_info.get(&key) {
            info!(key = ?key, "found cached auth info");
            return Ok(Some(cached));
        }

        let info = self.do_get_auth_info(extra, creds).await?;

        Ok(info.map(|info| {
            info!(key = ?key, "creating a cache entry for auth info");
            let (_, cached) = self.caches.auth_info.insert(key.into(), info.into());
            cached
        }))
    }

    #[tracing::instrument(skip_all)]
    async fn wake_compute(
        &self,
        extra: &ConsoleReqExtra<'_>,
        creds: &ClientCredentials<'_>,
    ) -> Result<CachedNodeInfo, WakeComputeError> {
        let key: Box<str> = creds.project().expect("impossible").into();

        // Every time we do a wakeup http request, the compute node will stay up
        // for some time (highly depends on the console's scale-to-zero policy);
        // The connection info remains the same during that period of time,
        // which means that we might cache it to reduce the load and latency.
        if let Some(cached) = self.caches.node_info.get(&key) {
            info!(key = ?key, "found cached compute node info");
            return Ok(cached);
        }

        let info = self.do_wake_compute(extra, creds).await?;

        info!(key = ?key, "creating a cache entry for compute node info");
        let (_, cached) = self.caches.node_info.insert(key.into(), info.into());

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
