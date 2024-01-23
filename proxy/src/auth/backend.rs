mod classic;
mod hacks;
mod link;

pub use link::LinkAuthError;
use tokio_postgres::config::AuthKeys;

use crate::auth::credentials::check_peer_addr_is_in_list;
use crate::auth::validate_password_and_exchange;
use crate::cache::Cached;
use crate::console::errors::GetAuthInfoError;
use crate::console::provider::ConsoleBackend;
use crate::console::AuthSecret;
use crate::context::RequestMonitoring;
use crate::proxy::connect_compute::handle_try_wake;
use crate::proxy::retry::retry_after;
use crate::proxy::NeonOptions;
use crate::stream::Stream;
use crate::{
    auth::{self, ComputeUserInfoMaybeEndpoint},
    config::AuthenticationConfig,
    console::{
        self,
        provider::{CachedAllowedIps, CachedNodeInfo},
        Api,
    },
    stream, url,
};
use crate::{scram, EndpointCacheKey, EndpointId, RoleName};
use futures::TryFutureExt;
use std::borrow::Cow;
use std::ops::ControlFlow;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{error, info, warn};

use super::IpPattern;

/// This type serves two purposes:
///
/// * When `T` is `()`, it's just a regular auth backend selector
///   which we use in [`crate::config::ProxyConfig`].
///
/// * However, when we substitute `T` with [`ComputeUserInfoMaybeEndpoint`],
///   this helps us provide the credentials only to those auth
///   backends which require them for the authentication process.
pub enum BackendType<'a, T> {
    /// Cloud API (V2).
    Console(Cow<'a, ConsoleBackend>, T),
    /// Authentication via a web browser.
    Link(Cow<'a, url::ApiUrl>),
    #[cfg(test)]
    /// Test backend.
    Test(&'a dyn TestBackend),
}

pub trait TestBackend: Send + Sync + 'static {
    fn wake_compute(&self) -> Result<CachedNodeInfo, console::errors::WakeComputeError>;
    fn get_allowed_ips(&self) -> Result<Vec<IpPattern>, console::errors::GetAuthInfoError>;
}

impl std::fmt::Display for BackendType<'_, ()> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use BackendType::*;
        match self {
            Console(api, _) => match &**api {
                ConsoleBackend::Console(endpoint) => {
                    fmt.debug_tuple("Console").field(&endpoint.url()).finish()
                }
                #[cfg(feature = "testing")]
                ConsoleBackend::Postgres(endpoint) => {
                    fmt.debug_tuple("Postgres").field(&endpoint.url()).finish()
                }
            },
            Link(url) => fmt.debug_tuple("Link").field(&url.as_str()).finish(),
            #[cfg(test)]
            Test(_) => fmt.debug_tuple("Test").finish(),
        }
    }
}

impl<T> BackendType<'_, T> {
    /// Very similar to [`std::option::Option::as_ref`].
    /// This helps us pass structured config to async tasks.
    pub fn as_ref(&self) -> BackendType<'_, &T> {
        use BackendType::*;
        match self {
            Console(c, x) => Console(Cow::Borrowed(c), x),
            Link(c) => Link(Cow::Borrowed(c)),
            #[cfg(test)]
            Test(x) => Test(*x),
        }
    }
}

impl<'a, T> BackendType<'a, T> {
    /// Very similar to [`std::option::Option::map`].
    /// Maps [`BackendType<T>`] to [`BackendType<R>`] by applying
    /// a function to a contained value.
    pub fn map<R>(self, f: impl FnOnce(T) -> R) -> BackendType<'a, R> {
        use BackendType::*;
        match self {
            Console(c, x) => Console(c, f(x)),
            Link(c) => Link(c),
            #[cfg(test)]
            Test(x) => Test(x),
        }
    }
}

impl<'a, T, E> BackendType<'a, Result<T, E>> {
    /// Very similar to [`std::option::Option::transpose`].
    /// This is most useful for error handling.
    pub fn transpose(self) -> Result<BackendType<'a, T>, E> {
        use BackendType::*;
        match self {
            Console(c, x) => x.map(|x| Console(c, x)),
            Link(c) => Ok(Link(c)),
            #[cfg(test)]
            Test(x) => Ok(Test(x)),
        }
    }
}

pub struct ComputeCredentials<T> {
    pub info: ComputeUserInfo,
    pub keys: T,
}

#[derive(Debug, Clone)]
pub struct ComputeUserInfoNoEndpoint {
    pub user: RoleName,
    pub options: NeonOptions,
}

#[derive(Debug, Clone)]
pub struct ComputeUserInfo {
    pub endpoint: EndpointId,
    pub user: RoleName,
    pub options: NeonOptions,
}

impl ComputeUserInfo {
    pub fn endpoint_cache_key(&self) -> EndpointCacheKey {
        self.options.get_cache_key(&self.endpoint)
    }
}

pub enum ComputeCredentialKeys {
    #[cfg(feature = "testing")]
    Password(Vec<u8>),
    AuthKeys(AuthKeys),
}

impl TryFrom<ComputeUserInfoMaybeEndpoint> for ComputeUserInfo {
    // user name
    type Error = ComputeUserInfoNoEndpoint;

    fn try_from(user_info: ComputeUserInfoMaybeEndpoint) -> Result<Self, Self::Error> {
        match user_info.endpoint_id {
            None => Err(ComputeUserInfoNoEndpoint {
                user: user_info.user,
                options: user_info.options,
            }),
            Some(endpoint) => Ok(ComputeUserInfo {
                endpoint,
                user: user_info.user,
                options: user_info.options,
            }),
        }
    }
}

/// True to its name, this function encapsulates our current auth trade-offs.
/// Here, we choose the appropriate auth flow based on circumstances.
///
/// All authentication flows will emit an AuthenticationOk message if successful.
async fn auth_quirks(
    ctx: &mut RequestMonitoring,
    api: &impl console::Api,
    user_info: ComputeUserInfoMaybeEndpoint,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    allow_cleartext: bool,
    config: &'static AuthenticationConfig,
) -> auth::Result<ComputeCredentials<ComputeCredentialKeys>> {
    // If there's no project so far, that entails that client doesn't
    // support SNI or other means of passing the endpoint (project) name.
    // We now expect to see a very specific payload in the place of password.
    let (info, unauthenticated_password) = match user_info.try_into() {
        Err(info) => {
            let res = hacks::password_hack_no_authentication(info, client, &mut ctx.latency_timer)
                .await?;
            ctx.set_endpoint_id(Some(res.info.endpoint.clone()));
            (res.info, Some(res.keys))
        }
        Ok(info) => (info, None),
    };

    info!("fetching user's authentication info");
    let allowed_ips = api.get_allowed_ips(ctx, &info).await?;

    // check allowed list
    if !check_peer_addr_is_in_list(&ctx.peer_addr, &allowed_ips) {
        return Err(auth::AuthError::ip_address_not_allowed());
    }
    let cached_secret = api.get_role_secret(ctx, &info).await?;

    let secret = cached_secret.value.clone().unwrap_or_else(|| {
        // If we don't have an authentication secret, we mock one to
        // prevent malicious probing (possible due to missing protocol steps).
        // This mocked secret will never lead to successful authentication.
        info!("authentication info not found, mocking it");
        AuthSecret::Scram(scram::ServerSecret::mock(&info.user, rand::random()))
    });
    match authenticate_with_secret(
        ctx,
        secret,
        info,
        client,
        unauthenticated_password,
        allow_cleartext,
        config,
    )
    .await
    {
        Ok(keys) => Ok(keys),
        Err(e) => {
            if e.is_auth_failed() {
                // The password could have been changed, so we invalidate the cache.
                cached_secret.invalidate();
            }
            Err(e)
        }
    }
}

async fn authenticate_with_secret(
    ctx: &mut RequestMonitoring,
    secret: AuthSecret,
    info: ComputeUserInfo,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    unauthenticated_password: Option<Vec<u8>>,
    allow_cleartext: bool,
    config: &'static AuthenticationConfig,
) -> auth::Result<ComputeCredentials<ComputeCredentialKeys>> {
    if let Some(password) = unauthenticated_password {
        let auth_outcome = validate_password_and_exchange(&password, secret)?;
        let keys = match auth_outcome {
            crate::sasl::Outcome::Success(key) => key,
            crate::sasl::Outcome::Failure(reason) => {
                info!("auth backend failed with an error: {reason}");
                return Err(auth::AuthError::auth_failed(&*info.user));
            }
        };

        // we have authenticated the password
        client.write_message_noflush(&pq_proto::BeMessage::AuthenticationOk)?;

        return Ok(ComputeCredentials { info, keys });
    }

    // -- the remaining flows are self-authenticating --

    // Perform cleartext auth if we're allowed to do that.
    // Currently, we use it for websocket connections (latency).
    if allow_cleartext {
        return hacks::authenticate_cleartext(info, client, &mut ctx.latency_timer, secret).await;
    }

    // Finally, proceed with the main auth flow (SCRAM-based).
    classic::authenticate(info, client, config, &mut ctx.latency_timer, secret).await
}

/// wake a compute (or retrieve an existing compute session from cache)
async fn wake_compute(
    ctx: &mut RequestMonitoring,
    api: &impl console::Api,
    compute_credentials: ComputeCredentials<ComputeCredentialKeys>,
) -> auth::Result<(CachedNodeInfo, ComputeUserInfo)> {
    let mut num_retries = 0;
    let mut node = loop {
        let wake_res = api.wake_compute(ctx, &compute_credentials.info).await;
        match handle_try_wake(wake_res, num_retries) {
            Err(e) => {
                error!(error = ?e, num_retries, retriable = false, "couldn't wake compute node");
                return Err(e.into());
            }
            Ok(ControlFlow::Continue(e)) => {
                warn!(error = ?e, num_retries, retriable = true, "couldn't wake compute node");
            }
            Ok(ControlFlow::Break(n)) => break n,
        }

        let wait_duration = retry_after(num_retries);
        num_retries += 1;
        tokio::time::sleep(wait_duration).await;
    };

    ctx.set_project(node.aux.clone());

    match compute_credentials.keys {
        #[cfg(feature = "testing")]
        ComputeCredentialKeys::Password(password) => node.config.password(password),
        ComputeCredentialKeys::AuthKeys(auth_keys) => node.config.auth_keys(auth_keys),
    };

    Ok((node, compute_credentials.info))
}

impl<'a> BackendType<'a, ComputeUserInfoMaybeEndpoint> {
    /// Get compute endpoint name from the credentials.
    pub fn get_endpoint(&self) -> Option<EndpointId> {
        use BackendType::*;

        match self {
            Console(_, user_info) => user_info.endpoint_id.clone(),
            Link(_) => Some("link".into()),
            #[cfg(test)]
            Test(_) => Some("test".into()),
        }
    }

    /// Get username from the credentials.
    pub fn get_user(&self) -> &str {
        use BackendType::*;

        match self {
            Console(_, user_info) => &user_info.user,
            Link(_) => "link",
            #[cfg(test)]
            Test(_) => "test",
        }
    }

    /// Authenticate the client via the requested backend, possibly using credentials.
    #[tracing::instrument(fields(allow_cleartext = allow_cleartext), skip_all)]
    pub async fn authenticate(
        self,
        ctx: &mut RequestMonitoring,
        client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
        allow_cleartext: bool,
        config: &'static AuthenticationConfig,
    ) -> auth::Result<(CachedNodeInfo, BackendType<'a, ComputeUserInfo>)> {
        use BackendType::*;

        let res = match self {
            Console(api, user_info) => {
                info!(
                    user = &*user_info.user,
                    project = user_info.endpoint(),
                    "performing authentication using the console"
                );

                let compute_credentials =
                    auth_quirks(ctx, &*api, user_info, client, allow_cleartext, config).await?;
                let (cache_info, user_info) = wake_compute(ctx, &*api, compute_credentials).await?;
                (cache_info, BackendType::Console(api, user_info))
            }
            // NOTE: this auth backend doesn't use client credentials.
            Link(url) => {
                info!("performing link authentication");

                let node_info = link::authenticate(&url, client).await?;

                (
                    CachedNodeInfo::new_uncached(node_info),
                    BackendType::Link(url),
                )
            }
            #[cfg(test)]
            Test(_) => {
                unreachable!("this function should never be called in the test backend")
            }
        };

        info!("user successfully authenticated");
        Ok(res)
    }
}

impl BackendType<'_, ComputeUserInfo> {
    pub async fn get_allowed_ips(
        &self,
        ctx: &mut RequestMonitoring,
    ) -> Result<CachedAllowedIps, GetAuthInfoError> {
        use BackendType::*;
        match self {
            Console(api, user_info) => api.get_allowed_ips(ctx, user_info).await,
            Link(_) => Ok(Cached::new_uncached(Arc::new(vec![]))),
            #[cfg(test)]
            Test(x) => Ok(Cached::new_uncached(Arc::new(x.get_allowed_ips()?))),
        }
    }

    /// When applicable, wake the compute node, gaining its connection info in the process.
    /// The link auth flow doesn't support this, so we return [`None`] in that case.
    pub async fn wake_compute(
        &self,
        ctx: &mut RequestMonitoring,
    ) -> Result<Option<CachedNodeInfo>, console::errors::WakeComputeError> {
        use BackendType::*;

        match self {
            Console(api, user_info) => api.wake_compute(ctx, user_info).map_ok(Some).await,
            Link(_) => Ok(None),
            #[cfg(test)]
            Test(x) => x.wake_compute().map(Some),
        }
    }
}
