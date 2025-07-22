//! Production console backend.

use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use ::http::HeaderName;
use ::http::header::AUTHORIZATION;
use bytes::Bytes;
use futures::TryFutureExt;
use hyper::StatusCode;
use postgres_client::config::SslMode;
use tokio::time::Instant;
use tracing::{Instrument, debug, info, info_span, warn};

use super::super::messages::{ControlPlaneErrorMessage, GetEndpointAccessControl, WakeCompute};
use crate::auth::backend::ComputeUserInfo;
use crate::auth::backend::jwt::AuthRule;
use crate::context::RequestContext;
use crate::control_plane::caches::ApiCaches;
use crate::control_plane::errors::{
    ControlPlaneError, GetAuthInfoError, GetEndpointJwksError, WakeComputeError,
};
use crate::control_plane::locks::ApiLocks;
use crate::control_plane::messages::{ColdStartInfo, EndpointJwksResponse};
use crate::control_plane::{
    AccessBlockerFlags, AuthInfo, AuthSecret, CachedNodeInfo, EndpointAccessControl, NodeInfo,
    RoleAccessControl,
};
use crate::metrics::Metrics;
use crate::proxy::retry::CouldRetry;
use crate::rate_limiter::WakeComputeRateLimiter;
use crate::types::{EndpointCacheKey, EndpointId, RoleName};
use crate::{compute, http, scram};

pub(crate) const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

#[derive(Clone)]
pub struct NeonControlPlaneClient {
    endpoint: http::Endpoint,
    pub caches: &'static ApiCaches,
    pub(crate) locks: &'static ApiLocks<EndpointCacheKey>,
    pub(crate) wake_compute_endpoint_rate_limiter: Arc<WakeComputeRateLimiter>,
    // put in a shared ref so we don't copy secrets all over in memory
    jwt: Arc<str>,
}

impl NeonControlPlaneClient {
    /// Construct an API object containing the auth parameters.
    pub fn new(
        endpoint: http::Endpoint,
        jwt: Arc<str>,
        caches: &'static ApiCaches,
        locks: &'static ApiLocks<EndpointCacheKey>,
        wake_compute_endpoint_rate_limiter: Arc<WakeComputeRateLimiter>,
    ) -> Self {
        Self {
            endpoint,
            caches,
            locks,
            wake_compute_endpoint_rate_limiter,
            jwt,
        }
    }

    pub(crate) fn url(&self) -> &str {
        self.endpoint.url().as_str()
    }

    async fn get_and_cache_auth_info<T>(
        &self,
        ctx: &RequestContext,
        endpoint: &EndpointId,
        role: &RoleName,
        cache_key: &EndpointId,
        extract: impl FnOnce(&EndpointAccessControl, &RoleAccessControl) -> T,
    ) -> Result<T, GetAuthInfoError> {
        match self.do_get_auth_req(ctx, endpoint, role).await {
            Ok(auth_info) => {
                let control = EndpointAccessControl {
                    allowed_ips: Arc::new(auth_info.allowed_ips),
                    allowed_vpce: Arc::new(auth_info.allowed_vpc_endpoint_ids),
                    flags: auth_info.access_blocker_flags,
                    rate_limits: auth_info.rate_limits,
                };
                let role_control = RoleAccessControl {
                    secret: auth_info.secret,
                };
                let res = extract(&control, &role_control);

                self.caches.project_info.insert_endpoint_access(
                    auth_info.account_id,
                    auth_info.project_id,
                    cache_key.into(),
                    role.into(),
                    control,
                    role_control,
                );

                if let Some(project_id) = auth_info.project_id {
                    ctx.set_project_id(project_id);
                }

                Ok(res)
            }
            Err(err) => match err {
                GetAuthInfoError::ApiError(ControlPlaneError::Message(ref msg)) => {
                    let retry_info = msg.status.as_ref().and_then(|s| s.details.retry_info);

                    // If we can retry this error, do not cache it,
                    // unless we were given a retry delay.
                    if msg.could_retry() && retry_info.is_none() {
                        return Err(err);
                    }

                    self.caches.project_info.insert_endpoint_access_err(
                        cache_key.into(),
                        role.into(),
                        msg.clone(),
                        retry_info.map(|r| Duration::from_millis(r.retry_delay_ms)),
                    );

                    Err(err)
                }
                err => Err(err),
            },
        }
    }

    async fn do_get_auth_req(
        &self,
        ctx: &RequestContext,
        endpoint: &EndpointId,
        role: &RoleName,
    ) -> Result<AuthInfo, GetAuthInfoError> {
        async {
            let response = {
                let request = self
                    .endpoint
                    .get_path("get_endpoint_access_control")
                    .header(X_REQUEST_ID, ctx.session_id().to_string())
                    .header(AUTHORIZATION, format!("Bearer {}", &self.jwt))
                    .query(&[("session_id", ctx.session_id())])
                    .query(&[
                        ("application_name", ctx.console_application_name().as_str()),
                        ("endpointish", endpoint.as_str()),
                        ("role", role.as_str()),
                    ])
                    .build()?;

                debug!(url = request.url().as_str(), "sending http request");
                let start = Instant::now();
                let _pause = ctx.latency_timer_pause_at(start, crate::metrics::Waiting::Cplane);
                let response = self.endpoint.execute(request).await?;

                info!(duration = ?start.elapsed(), "received http response");

                response
            };

            let body = match parse_body::<GetEndpointAccessControl>(
                response.status(),
                response.bytes().await?,
            ) {
                Ok(body) => body,
                // Error 404 is special: it's ok not to have a secret.
                // TODO(anna): retry
                Err(e) => {
                    return if e.get_reason().is_not_found() {
                        // TODO: refactor this because it's weird
                        // this is a failure to authenticate but we return Ok.
                        Ok(AuthInfo::default())
                    } else {
                        Err(e.into())
                    };
                }
            };

            let secret = if body.role_secret.is_empty() {
                None
            } else {
                let secret = scram::ServerSecret::parse(&body.role_secret)
                    .map(AuthSecret::Scram)
                    .ok_or(GetAuthInfoError::BadSecret)?;
                Some(secret)
            };
            let allowed_ips = body.allowed_ips.unwrap_or_default();
            Metrics::get()
                .proxy
                .allowed_ips_number
                .observe(allowed_ips.len() as f64);
            let allowed_vpc_endpoint_ids = body.allowed_vpc_endpoint_ids.unwrap_or_default();
            Metrics::get()
                .proxy
                .allowed_vpc_endpoint_ids
                .observe(allowed_vpc_endpoint_ids.len() as f64);
            let block_public_connections = body.block_public_connections.unwrap_or_default();
            let block_vpc_connections = body.block_vpc_connections.unwrap_or_default();
            Ok(AuthInfo {
                secret,
                allowed_ips,
                allowed_vpc_endpoint_ids,
                project_id: body.project_id,
                account_id: body.account_id,
                access_blocker_flags: AccessBlockerFlags {
                    public_access_blocked: block_public_connections,
                    vpc_access_blocked: block_vpc_connections,
                },
                rate_limits: body.rate_limits,
            })
        }
        .inspect_err(|e| tracing::debug!(error = ?e))
        .instrument(info_span!("do_get_auth_info"))
        .await
    }

    async fn do_get_endpoint_jwks(
        &self,
        ctx: &RequestContext,
        endpoint: &EndpointId,
    ) -> Result<Vec<AuthRule>, GetEndpointJwksError> {
        let request_id = ctx.session_id().to_string();
        async {
            let request = self
                .endpoint
                .get_with_url(|url| {
                    url.path_segments_mut()
                        .push("endpoints")
                        .push(endpoint.as_str())
                        .push("jwks");
                })
                .header(X_REQUEST_ID, &request_id)
                .header(AUTHORIZATION, format!("Bearer {}", &self.jwt))
                .query(&[("session_id", ctx.session_id())])
                .build()
                .map_err(GetEndpointJwksError::RequestBuild)?;

            debug!(url = request.url().as_str(), "sending http request");
            let start = Instant::now();
            let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Cplane);
            let response = self
                .endpoint
                .execute(request)
                .await
                .map_err(GetEndpointJwksError::RequestExecute)?;
            drop(pause);
            info!(duration = ?start.elapsed(), "received http response");

            let body = parse_body::<EndpointJwksResponse>(
                response.status(),
                response.bytes().await.map_err(ControlPlaneError::from)?,
            )?;

            let rules = body
                .jwks
                .into_iter()
                .map(|jwks| AuthRule {
                    id: jwks.id,
                    jwks_url: jwks.jwks_url,
                    audience: jwks.jwt_audience,
                    role_names: jwks.role_names,
                })
                .collect();

            Ok(rules)
        }
        .inspect_err(|e| tracing::debug!(error = ?e))
        .instrument(info_span!("do_get_endpoint_jwks"))
        .await
    }

    async fn do_wake_compute(
        &self,
        ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> Result<NodeInfo, WakeComputeError> {
        let request_id = ctx.session_id().to_string();
        let application_name = ctx.console_application_name();
        async {
            let mut request_builder = self
                .endpoint
                .get_path("wake_compute")
                .header("X-Request-ID", &request_id)
                .header("Authorization", format!("Bearer {}", &self.jwt))
                .query(&[("session_id", ctx.session_id())])
                .query(&[
                    ("application_name", application_name.as_str()),
                    ("endpointish", user_info.endpoint.as_str()),
                ]);

            let options = user_info.options.to_deep_object();
            if !options.is_empty() {
                request_builder = request_builder.query(&options);
            }

            let request = request_builder.build()?;

            debug!(url = request.url().as_str(), "sending http request");
            let start = Instant::now();
            let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Cplane);
            let response = self.endpoint.execute(request).await?;
            drop(pause);
            info!(duration = ?start.elapsed(), "received http response");
            let body = parse_body::<WakeCompute>(response.status(), response.bytes().await?)?;

            let Some((host, port)) = parse_host_port(&body.address) else {
                return Err(WakeComputeError::BadComputeAddress(body.address));
            };

            let host_addr = IpAddr::from_str(host).ok();

            let ssl_mode = match &body.server_name {
                Some(_) => SslMode::Require,
                None => SslMode::Disable,
            };
            let host = match body.server_name {
                Some(host) => host.into(),
                None => host.into(),
            };

            let node = NodeInfo {
                conn_info: compute::ConnectInfo {
                    host_addr,
                    host,
                    port,
                    ssl_mode,
                },
                aux: body.aux,
            };

            Ok(node)
        }
        .inspect_err(|e| tracing::debug!(error = ?e))
        .instrument(info_span!("do_wake_compute"))
        .await
    }
}

impl super::ControlPlaneApi for NeonControlPlaneClient {
    #[tracing::instrument(skip_all)]
    async fn get_role_access_control(
        &self,
        ctx: &RequestContext,
        endpoint: &EndpointId,
        role: &RoleName,
    ) -> Result<RoleAccessControl, GetAuthInfoError> {
        let key = endpoint.normalize();

        if let Some((role_control, ttl)) = self
            .caches
            .project_info
            .get_role_secret_with_ttl(&key, role)
        {
            return match role_control {
                Err(mut msg) => {
                    info!(key = &*key, "found cached get_role_access_control error");

                    // if retry_delay_ms is set change it to the remaining TTL
                    replace_retry_delay_ms(&mut msg, |_| ttl.as_millis() as u64);

                    Err(GetAuthInfoError::ApiError(ControlPlaneError::Message(msg)))
                }
                Ok(role_control) => {
                    debug!(key = &*key, "found cached role access control");
                    Ok(role_control)
                }
            };
        }

        self.get_and_cache_auth_info(ctx, endpoint, role, &key, |_, role_control| {
            role_control.clone()
        })
        .await
    }

    #[tracing::instrument(skip_all)]
    async fn get_endpoint_access_control(
        &self,
        ctx: &RequestContext,
        endpoint: &EndpointId,
        role: &RoleName,
    ) -> Result<EndpointAccessControl, GetAuthInfoError> {
        let key = endpoint.normalize();

        if let Some((control, ttl)) = self.caches.project_info.get_endpoint_access_with_ttl(&key) {
            return match control {
                Err(mut msg) => {
                    info!(
                        key = &*key,
                        "found cached get_endpoint_access_control error"
                    );

                    // if retry_delay_ms is set change it to the remaining TTL
                    replace_retry_delay_ms(&mut msg, |_| ttl.as_millis() as u64);

                    Err(GetAuthInfoError::ApiError(ControlPlaneError::Message(msg)))
                }
                Ok(control) => {
                    debug!(key = &*key, "found cached endpoint access control");
                    Ok(control)
                }
            };
        }

        self.get_and_cache_auth_info(ctx, endpoint, role, &key, |control, _| control.clone())
            .await
    }

    #[tracing::instrument(skip_all)]
    async fn get_endpoint_jwks(
        &self,
        ctx: &RequestContext,
        endpoint: &EndpointId,
    ) -> Result<Vec<AuthRule>, GetEndpointJwksError> {
        self.do_get_endpoint_jwks(ctx, endpoint).await
    }

    #[tracing::instrument(skip_all)]
    async fn wake_compute(
        &self,
        ctx: &RequestContext,
        user_info: &ComputeUserInfo,
    ) -> Result<CachedNodeInfo, WakeComputeError> {
        let key = user_info.endpoint_cache_key();

        macro_rules! check_cache {
            () => {
                if let Some(cached) = self.caches.node_info.get_with_created_at(&key) {
                    let (cached, (info, created_at)) = cached.take_value();
                    return match info {
                        Err(mut msg) => {
                            info!(key = &*key, "found cached wake_compute error");

                            // if retry_delay_ms is set, reduce it by the amount of time it spent in cache
                            replace_retry_delay_ms(&mut msg, |delay| {
                                delay.saturating_sub(created_at.elapsed().as_millis() as u64)
                            });

                            Err(WakeComputeError::ControlPlane(ControlPlaneError::Message(
                                msg,
                            )))
                        }
                        Ok(info) => {
                            debug!(key = &*key, "found cached compute node info");
                            ctx.set_project(info.aux.clone());
                            Ok(cached.map(|()| info))
                        }
                    };
                }
            };
        }

        // Every time we do a wakeup http request, the compute node will stay up
        // for some time (highly depends on the console's scale-to-zero policy);
        // The connection info remains the same during that period of time,
        // which means that we might cache it to reduce the load and latency.
        check_cache!();

        let permit = self.locks.get_permit(&key).await?;

        // after getting back a permit - it's possible the cache was filled
        // double check
        if permit.should_check_cache() {
            // TODO: if there is something in the cache, mark the permit as success.
            check_cache!();
        }

        // check rate limit
        if !self
            .wake_compute_endpoint_rate_limiter
            .check(user_info.endpoint.normalize_intern(), 1)
        {
            return Err(WakeComputeError::TooManyConnections);
        }

        let node = permit.release_result(self.do_wake_compute(ctx, user_info).await);
        match node {
            Ok(node) => {
                ctx.set_project(node.aux.clone());
                debug!(key = &*key, "created a cache entry for woken compute node");

                let mut stored_node = node.clone();
                // store the cached node as 'warm_cached'
                stored_node.aux.cold_start_info = ColdStartInfo::WarmCached;

                let (_, cached) = self.caches.node_info.insert_unit(key, Ok(stored_node));

                Ok(cached.map(|()| node))
            }
            Err(err) => match err {
                WakeComputeError::ControlPlane(ControlPlaneError::Message(ref msg)) => {
                    let retry_info = msg.status.as_ref().and_then(|s| s.details.retry_info);

                    // If we can retry this error, do not cache it,
                    // unless we were given a retry delay.
                    if msg.could_retry() && retry_info.is_none() {
                        return Err(err);
                    }

                    debug!(
                        key = &*key,
                        "created a cache entry for the wake compute error"
                    );

                    let ttl = retry_info.map_or(Duration::from_secs(30), |r| {
                        Duration::from_millis(r.retry_delay_ms)
                    });

                    self.caches.node_info.insert_ttl(key, Err(msg.clone()), ttl);

                    Err(err)
                }
                err => Err(err),
            },
        }
    }
}

fn replace_retry_delay_ms(msg: &mut ControlPlaneErrorMessage, f: impl FnOnce(u64) -> u64) {
    if let Some(status) = &mut msg.status
        && let Some(retry_info) = &mut status.details.retry_info
    {
        retry_info.retry_delay_ms = f(retry_info.retry_delay_ms);
    }
}

/// Parse http response body, taking status code into account.
fn parse_body<T: for<'a> serde::Deserialize<'a>>(
    status: StatusCode,
    body: Bytes,
) -> Result<T, ControlPlaneError> {
    if status.is_success() {
        // We shouldn't log raw body because it may contain secrets.
        info!("request succeeded, processing the body");
        return Ok(serde_json::from_slice(&body).map_err(std::io::Error::other)?);
    }

    // Log plaintext to be able to detect, whether there are some cases not covered by the error struct.
    info!("response_error plaintext: {:?}", body);

    // Don't throw an error here because it's not as important
    // as the fact that the request itself has failed.
    let mut body = serde_json::from_slice(&body).unwrap_or_else(|e| {
        warn!("failed to parse error body: {e}");
        Box::new(ControlPlaneErrorMessage {
            error: "reason unclear (malformed error message)".into(),
            http_status_code: status,
            status: None,
        })
    });
    body.http_status_code = status;

    warn!("console responded with an error ({status}): {body:?}");
    Err(ControlPlaneError::Message(body))
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
