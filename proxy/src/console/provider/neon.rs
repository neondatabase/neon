//! Production console backend.

use super::{
    super::messages::{ConsoleError, GetRoleSecret, WakeCompute},
    errors::{ApiError, GetAuthInfoError, WakeComputeError},
    ApiCaches, ApiLocks, AuthInfo, AuthSecret, CachedAllowedIps, CachedNodeInfo, CachedRoleSecret,
    ConsoleReqExtra, NodeInfo,
};
use crate::{auth::backend::ComputeUserInfo, compute, http, scram};
use crate::{
    cache::Cached,
    context::RequestMonitoring,
    metrics::{ALLOWED_IPS_BY_CACHE_OUTCOME, ALLOWED_IPS_NUMBER},
};
use async_trait::async_trait;
use futures::TryFutureExt;
use itertools::Itertools;
use smol_str::SmolStr;
use std::sync::Arc;
use tokio::time::Instant;
use tokio_postgres::config::SslMode;
use tracing::{error, info, info_span, warn, Instrument};

#[derive(Clone)]
pub struct Api {
    endpoint: http::Endpoint,
    pub caches: &'static ApiCaches,
    locks: &'static ApiLocks,
    jwt: String,
}

impl Api {
    /// Construct an API object containing the auth parameters.
    pub fn new(
        endpoint: http::Endpoint,
        caches: &'static ApiCaches,
        locks: &'static ApiLocks,
    ) -> Self {
        let jwt: String = match std::env::var("NEON_PROXY_TO_CONTROLPLANE_TOKEN") {
            Ok(v) => v,
            Err(_) => "".to_string(),
        };
        Self {
            endpoint,
            caches,
            locks,
            jwt,
        }
    }

    pub fn url(&self) -> &str {
        self.endpoint.url().as_str()
    }

    async fn do_get_auth_info(
        &self,
        ctx: &mut RequestMonitoring,
        creds: &ComputeUserInfo,
    ) -> Result<AuthInfo, GetAuthInfoError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let application_name = ctx.console_application_name();
        async {
            let request = self
                .endpoint
                .get("proxy_get_role_secret")
                .header("X-Request-ID", &request_id)
                .header("Authorization", format!("Bearer {}", &self.jwt))
                .query(&[("session_id", ctx.session_id)])
                .query(&[
                    ("application_name", application_name.as_str()),
                    ("project", creds.endpoint.as_str()),
                    ("role", creds.inner.user.as_str()),
                ])
                .build()?;

            info!(url = request.url().as_str(), "sending http request");
            let start = Instant::now();
            let response = self.endpoint.execute(request).await?;
            info!(duration = ?start.elapsed(), "received http response");
            let body = match parse_body::<GetRoleSecret>(response).await {
                Ok(body) => body,
                // Error 404 is special: it's ok not to have a secret.
                Err(e) => match e.http_status_code() {
                    Some(http::StatusCode::NOT_FOUND) => return Ok(AuthInfo::default()),
                    _otherwise => return Err(e.into()),
                },
            };

            let secret = scram::ServerSecret::parse(&body.role_secret)
                .map(AuthSecret::Scram)
                .ok_or(GetAuthInfoError::BadSecret)?;
            let allowed_ips = body
                .allowed_ips
                .into_iter()
                .flatten()
                .map(SmolStr::from)
                .collect_vec();
            ALLOWED_IPS_NUMBER.observe(allowed_ips.len() as f64);
            Ok(AuthInfo {
                secret: Some(secret),
                allowed_ips,
                project_id: body.project_id.map(SmolStr::from),
            })
        }
        .map_err(crate::error::log_error)
        .instrument(info_span!("http", id = request_id))
        .await
    }

    async fn do_wake_compute(
        &self,
        ctx: &mut RequestMonitoring,
        extra: &ConsoleReqExtra,
        creds: &ComputeUserInfo,
    ) -> Result<NodeInfo, WakeComputeError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let application_name = ctx.console_application_name();
        async {
            let mut request_builder = self
                .endpoint
                .get("proxy_wake_compute")
                .header("X-Request-ID", &request_id)
                .header("Authorization", format!("Bearer {}", &self.jwt))
                .query(&[("session_id", ctx.session_id)])
                .query(&[
                    ("application_name", application_name.as_str()),
                    ("project", creds.endpoint.as_str()),
                ]);

            let options = extra.options_as_deep_object();
            if !options.is_empty() {
                request_builder = request_builder.query(&options);
            }

            let request = request_builder.build()?;

            info!(url = request.url().as_str(), "sending http request");
            let start = Instant::now();
            let response = self.endpoint.execute(request).await?;
            info!(duration = ?start.elapsed(), "received http response");
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
                aux: body.aux,
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
    async fn get_role_secret(
        &self,
        ctx: &mut RequestMonitoring,
        creds: &ComputeUserInfo,
    ) -> Result<Option<CachedRoleSecret>, GetAuthInfoError> {
        let ep = &creds.endpoint;
        let user = &creds.inner.user;
        if let Some(role_secret) = self.caches.project_info.get_role_secret(ep, user) {
            return Ok(Some(role_secret));
        }
        let auth_info = self.do_get_auth_info(ctx, creds).await?;
        let project_id = auth_info.project_id.unwrap_or(ep.clone());
        if let Some(secret) = &auth_info.secret {
            self.caches
                .project_info
                .insert_role_secret(&project_id, ep, user, secret.clone())
        }
        self.caches.project_info.insert_allowed_ips(
            &project_id,
            ep,
            Arc::new(auth_info.allowed_ips),
        );
        // When we just got a secret, we don't need to invalidate it.
        Ok(auth_info.secret.map(Cached::new_uncached))
    }

    async fn get_allowed_ips(
        &self,
        ctx: &mut RequestMonitoring,
        creds: &ComputeUserInfo,
    ) -> Result<CachedAllowedIps, GetAuthInfoError> {
        let ep = &creds.endpoint;
        if let Some(allowed_ips) = self.caches.project_info.get_allowed_ips(ep) {
            ALLOWED_IPS_BY_CACHE_OUTCOME
                .with_label_values(&["hit"])
                .inc();
            return Ok(allowed_ips);
        }
        ALLOWED_IPS_BY_CACHE_OUTCOME
            .with_label_values(&["miss"])
            .inc();
        let auth_info = self.do_get_auth_info(ctx, creds).await?;
        let allowed_ips = Arc::new(auth_info.allowed_ips);
        let user = &creds.inner.user;
        let project_id = auth_info.project_id.unwrap_or(ep.clone());
        if let Some(secret) = &auth_info.secret {
            self.caches
                .project_info
                .insert_role_secret(&project_id, ep, user, secret.clone())
        }
        self.caches
            .project_info
            .insert_allowed_ips(&project_id, ep, allowed_ips.clone());
        Ok(Cached::new_uncached(allowed_ips))
    }

    #[tracing::instrument(skip_all)]
    async fn wake_compute(
        &self,
        ctx: &mut RequestMonitoring,
        extra: &ConsoleReqExtra,
        creds: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, WakeComputeError> {
        let key = creds.inner.options.get_cache_key(&creds.endpoint);

        // Every time we do a wakeup http request, the compute node will stay up
        // for some time (highly depends on the console's scale-to-zero policy);
        // The connection info remains the same during that period of time,
        // which means that we might cache it to reduce the load and latency.
        if let Some(cached) = self.caches.node_info.get(&*key) {
            info!(key = &*key, "found cached compute node info");
            return Ok(cached);
        }

        let permit = self.locks.get_wake_compute_permit(&key).await?;

        // after getting back a permit - it's possible the cache was filled
        // double check
        if permit.should_check_cache() {
            if let Some(cached) = self.caches.node_info.get(&key) {
                info!(key = &*key, "found cached compute node info");
                return Ok(cached);
            }
        }

        let node = self.do_wake_compute(ctx, extra, creds).await?;
        let (_, cached) = self.caches.node_info.insert(key.clone(), node);
        info!(key = &*key, "created a cache entry for compute node info");

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
    let (host, port) = input.rsplit_once(':')?;
    let ipv6_brackets: &[_] = &['[', ']'];
    Some((host.trim_matches(ipv6_brackets), port.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_host_port_v4() {
        let (host, port) = parse_host_port("127.0.0.1:5432").expect("failed to parse");
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 5432);
    }

    #[test]
    fn test_parse_host_port_v6() {
        let (host, port) = parse_host_port("[2001:db8::1]:5432").expect("failed to parse");
        assert_eq!(host, "2001:db8::1");
        assert_eq!(port, 5432);
    }

    #[test]
    fn test_parse_host_port_url() {
        let (host, port) = parse_host_port("compute-foo-bar-1234.default.svc.cluster.local:5432")
            .expect("failed to parse");
        assert_eq!(host, "compute-foo-bar-1234.default.svc.cluster.local");
        assert_eq!(port, 5432);
    }
}
