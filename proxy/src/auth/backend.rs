mod classic;
mod hacks;
mod link;

pub use link::LinkAuthError;
use tokio_postgres::config::AuthKeys;

use crate::auth::credentials::check_peer_addr_is_in_list;
use crate::auth::validate_password_and_exchange;
use crate::cache::Cached;
use crate::console::errors::GetAuthInfoError;
use crate::console::provider::{CachedRoleSecret, ConsoleBackend};
use crate::console::{AuthSecret, NodeInfo};
use crate::context::RequestMonitoring;
use crate::proxy::connect_compute::ComputeConnectBackend;
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
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;

/// Alternative to [`std::borrow::Cow`] but doesn't need `T: ToOwned` as we don't need that functionality
pub enum MaybeOwned<'a, T> {
    Owned(T),
    Borrowed(&'a T),
}

impl<T> std::ops::Deref for MaybeOwned<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            MaybeOwned::Owned(t) => t,
            MaybeOwned::Borrowed(t) => t,
        }
    }
}

/// This type serves two purposes:
///
/// * When `T` is `()`, it's just a regular auth backend selector
///   which we use in [`crate::config::ProxyConfig`].
///
/// * However, when we substitute `T` with [`ComputeUserInfoMaybeEndpoint`],
///   this helps us provide the credentials only to those auth
///   backends which require them for the authentication process.
pub enum BackendType<'a, T, D> {
    /// Cloud API (V2).
    Console(MaybeOwned<'a, ConsoleBackend>, T),
    /// Authentication via a web browser.
    Link(MaybeOwned<'a, url::ApiUrl>, D),
}

pub trait TestBackend: Send + Sync + 'static {
    fn wake_compute(&self) -> Result<CachedNodeInfo, console::errors::WakeComputeError>;
    fn get_allowed_ips_and_secret(
        &self,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), console::errors::GetAuthInfoError>;
    fn get_role_secret(&self) -> Result<CachedRoleSecret, console::errors::GetAuthInfoError>;
}

impl std::fmt::Display for BackendType<'_, (), ()> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use BackendType::*;
        match self {
            Console(api, _) => match &**api {
                ConsoleBackend::Console(endpoint) => {
                    fmt.debug_tuple("Console").field(&endpoint.url()).finish()
                }
                #[cfg(any(test, feature = "testing"))]
                ConsoleBackend::Postgres(endpoint) => {
                    fmt.debug_tuple("Postgres").field(&endpoint.url()).finish()
                }
                #[cfg(test)]
                ConsoleBackend::Test(_) => fmt.debug_tuple("Test").finish(),
            },
            Link(url, _) => fmt.debug_tuple("Link").field(&url.as_str()).finish(),
        }
    }
}

impl<T, D> BackendType<'_, T, D> {
    /// Very similar to [`std::option::Option::as_ref`].
    /// This helps us pass structured config to async tasks.
    pub fn as_ref(&self) -> BackendType<'_, &T, &D> {
        use BackendType::*;
        match self {
            Console(c, x) => Console(MaybeOwned::Borrowed(c), x),
            Link(c, x) => Link(MaybeOwned::Borrowed(c), x),
        }
    }
}

impl<'a, T, D> BackendType<'a, T, D> {
    /// Very similar to [`std::option::Option::map`].
    /// Maps [`BackendType<T>`] to [`BackendType<R>`] by applying
    /// a function to a contained value.
    pub fn map<R>(self, f: impl FnOnce(T) -> R) -> BackendType<'a, R, D> {
        use BackendType::*;
        match self {
            Console(c, x) => Console(c, f(x)),
            Link(c, x) => Link(c, x),
        }
    }
}
impl<'a, T, D, E> BackendType<'a, Result<T, E>, D> {
    /// Very similar to [`std::option::Option::transpose`].
    /// This is most useful for error handling.
    pub fn transpose(self) -> Result<BackendType<'a, T, D>, E> {
        use BackendType::*;
        match self {
            Console(c, x) => x.map(|x| Console(c, x)),
            Link(c, x) => Ok(Link(c, x)),
        }
    }
}

pub struct ComputeCredentials {
    pub info: ComputeUserInfo,
    pub keys: ComputeCredentialKeys,
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
) -> auth::Result<ComputeCredentials> {
    // If there's no project so far, that entails that client doesn't
    // support SNI or other means of passing the endpoint (project) name.
    // We now expect to see a very specific payload in the place of password.
    let (info, unauthenticated_password) = match user_info.try_into() {
        Err(info) => {
            let res = hacks::password_hack_no_authentication(ctx, info, client).await?;

            ctx.set_endpoint_id(res.info.endpoint.clone());
            tracing::Span::current().record("ep", &tracing::field::display(&res.info.endpoint));
            let password = match res.keys {
                ComputeCredentialKeys::Password(p) => p,
                _ => unreachable!("password hack should return a password"),
            };
            (res.info, Some(password))
        }
        Ok(info) => (info, None),
    };

    info!("fetching user's authentication info");
    let (allowed_ips, maybe_secret) = api.get_allowed_ips_and_secret(ctx, &info).await?;

    // check allowed list
    if !check_peer_addr_is_in_list(&ctx.peer_addr, &allowed_ips) {
        return Err(auth::AuthError::ip_address_not_allowed(ctx.peer_addr));
    }
    let cached_secret = match maybe_secret {
        Some(secret) => secret,
        None => api.get_role_secret(ctx, &info).await?,
    };

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
) -> auth::Result<ComputeCredentials> {
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
        ctx.set_auth_method(crate::context::AuthMethod::Cleartext);
        return hacks::authenticate_cleartext(ctx, info, client, secret).await;
    }

    // Finally, proceed with the main auth flow (SCRAM-based).
    classic::authenticate(ctx, info, client, config, secret).await
}

impl<'a> BackendType<'a, ComputeUserInfoMaybeEndpoint, &()> {
    /// Get compute endpoint name from the credentials.
    pub fn get_endpoint(&self) -> Option<EndpointId> {
        use BackendType::*;

        match self {
            Console(_, user_info) => user_info.endpoint_id.clone(),
            Link(_, _) => Some("link".into()),
        }
    }

    /// Get username from the credentials.
    pub fn get_user(&self) -> &str {
        use BackendType::*;

        match self {
            Console(_, user_info) => &user_info.user,
            Link(_, _) => "link",
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
    ) -> auth::Result<BackendType<'a, ComputeCredentials, NodeInfo>> {
        use BackendType::*;

        let res = match self {
            Console(api, user_info) => {
                info!(
                    user = &*user_info.user,
                    project = user_info.endpoint(),
                    "performing authentication using the console"
                );

                let credentials =
                    auth_quirks(ctx, &*api, user_info, client, allow_cleartext, config).await?;
                BackendType::Console(api, credentials)
            }
            // NOTE: this auth backend doesn't use client credentials.
            Link(url, _) => {
                info!("performing link authentication");

                let info = link::authenticate(ctx, &url, client).await?;

                BackendType::Link(url, info)
            }
        };

        info!("user successfully authenticated");
        Ok(res)
    }
}

impl BackendType<'_, ComputeUserInfo, &()> {
    pub async fn get_role_secret(
        &self,
        ctx: &mut RequestMonitoring,
    ) -> Result<CachedRoleSecret, GetAuthInfoError> {
        use BackendType::*;
        match self {
            Console(api, user_info) => api.get_role_secret(ctx, user_info).await,
            Link(_, _) => Ok(Cached::new_uncached(None)),
        }
    }

    pub async fn get_allowed_ips_and_secret(
        &self,
        ctx: &mut RequestMonitoring,
    ) -> Result<(CachedAllowedIps, Option<CachedRoleSecret>), GetAuthInfoError> {
        use BackendType::*;
        match self {
            Console(api, user_info) => api.get_allowed_ips_and_secret(ctx, user_info).await,
            Link(_, _) => Ok((Cached::new_uncached(Arc::new(vec![])), None)),
        }
    }
}

#[async_trait::async_trait]
impl ComputeConnectBackend for BackendType<'_, ComputeCredentials, NodeInfo> {
    async fn wake_compute(
        &self,
        ctx: &mut RequestMonitoring,
    ) -> Result<CachedNodeInfo, console::errors::WakeComputeError> {
        use BackendType::*;

        match self {
            Console(api, creds) => api.wake_compute(ctx, &creds.info).await,
            Link(_, info) => Ok(Cached::new_uncached(info.clone())),
        }
    }

    fn get_keys(&self) -> Option<&ComputeCredentialKeys> {
        match self {
            BackendType::Console(_, creds) => Some(&creds.keys),
            BackendType::Link(_, _) => None,
        }
    }
}

#[async_trait::async_trait]
impl ComputeConnectBackend for BackendType<'_, ComputeCredentials, &()> {
    async fn wake_compute(
        &self,
        ctx: &mut RequestMonitoring,
    ) -> Result<CachedNodeInfo, console::errors::WakeComputeError> {
        use BackendType::*;

        match self {
            Console(api, creds) => api.wake_compute(ctx, &creds.info).await,
            Link(_, _) => unreachable!("link auth flow doesn't support waking the compute"),
        }
    }

    fn get_keys(&self) -> Option<&ComputeCredentialKeys> {
        match self {
            BackendType::Console(_, creds) => Some(&creds.keys),
            BackendType::Link(_, _) => None,
        }
    }
}
