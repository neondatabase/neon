mod classic;
mod hacks;
mod link;

use pq_proto::StartupMessageParams;
use smol_str::SmolStr;
use tokio_postgres::config::AuthKeys;

use crate::auth::backend::link::NeedsLinkAuthentication;
use crate::auth::credentials::check_peer_addr_is_in_list;
use crate::auth::validate_password_and_exchange;
use crate::cache::Cached;
use crate::cancellation::Session;
use crate::config::ProxyConfig;
use crate::console::errors::GetAuthInfoError;
use crate::console::provider::ConsoleBackend;
use crate::console::AuthSecret;
use crate::context::RequestMonitoring;
use crate::proxy::wake_compute::NeedsWakeCompute;
use crate::proxy::ClientMode;
use crate::proxy::NeonOptions;
use crate::rate_limiter::EndpointRateLimiter;
use crate::scram;
use crate::state_machine::{user_facing_error, DynStage, ResultExt, Stage, StageError};
use crate::stream::{PqStream, Stream};
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
use futures::TryFutureExt;
use std::borrow::Cow;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;

use self::hacks::NeedsPasswordHack;

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
    fn get_allowed_ips(&self) -> Result<Vec<SmolStr>, console::errors::GetAuthInfoError>;
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
    pub user: SmolStr,
    pub options: NeonOptions,
}

#[derive(Debug, Clone)]
pub struct ComputeUserInfo {
    pub endpoint: SmolStr,
    pub user: SmolStr,
    pub options: NeonOptions,
}

impl ComputeUserInfo {
    pub fn endpoint_cache_key(&self) -> SmolStr {
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
        match user_info.project {
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

struct NeedsAuthSecret<S> {
    stream: PqStream<Stream<S>>,
    api: Cow<'static, ConsoleBackend>,
    params: StartupMessageParams,
    allow_self_signed_compute: bool,
    allow_cleartext: bool,
    info: ComputeUserInfo,
    unauthenticated_password: Option<Vec<u8>>,
    config: &'static AuthenticationConfig,

    // monitoring
    ctx: RequestMonitoring,
    cancel_session: Session,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Stage for NeedsAuthSecret<S> {
    fn span(&self) -> tracing::Span {
        tracing::info_span!("get_auth_secret")
    }
    async fn run(self) -> Result<DynStage, StageError> {
        let Self {
            stream,
            api,
            params,
            allow_cleartext,
            allow_self_signed_compute,
            info,
            unauthenticated_password,
            config,
            mut ctx,
            cancel_session,
        } = self;

        info!("fetching user's authentication info");
        let (allowed_ips, stream) = api
            .get_allowed_ips(&mut ctx, &info)
            .await
            .send_error_to_user(&mut ctx, stream)?;

        // check allowed list
        if !check_peer_addr_is_in_list(&ctx.peer_addr, &allowed_ips) {
            return Err(user_facing_error(
                auth::AuthError::ip_address_not_allowed(),
                &mut ctx,
                stream,
            ));
        }
        let (cached_secret, mut stream) = api
            .get_role_secret(&mut ctx, &info)
            .await
            .send_error_to_user(&mut ctx, stream)?;

        let secret = cached_secret.value.clone().unwrap_or_else(|| {
            // If we don't have an authentication secret, we mock one to
            // prevent malicious probing (possible due to missing protocol steps).
            // This mocked secret will never lead to successful authentication.
            info!("authentication info not found, mocking it");
            AuthSecret::Scram(scram::ServerSecret::mock(&info.user, rand::random()))
        });

        let (keys, stream) = authenticate_with_secret(
            &mut ctx,
            secret,
            info,
            &mut stream,
            unauthenticated_password,
            allow_cleartext,
            config,
        )
        .await
        .map_err(|e| {
            if e.is_auth_failed() {
                // The password could have been changed, so we invalidate the cache.
                cached_secret.invalidate();
            }
            e
        })
        .send_error_to_user(&mut ctx, stream)?;

        Ok(Box::new(NeedsWakeCompute {
            stream,
            api,
            params,
            allow_self_signed_compute,
            creds: keys,
            ctx,
            cancel_session,
        }))
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

impl<'a> BackendType<'a, ComputeUserInfoMaybeEndpoint> {
    /// Get compute endpoint name from the credentials.
    pub fn get_endpoint(&self) -> Option<SmolStr> {
        use BackendType::*;

        match self {
            Console(_, user_info) => user_info.project.clone(),
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
}

pub struct NeedsAuthentication<S> {
    pub stream: PqStream<Stream<S>>,
    pub creds: BackendType<'static, auth::ComputeUserInfoMaybeEndpoint>,
    pub params: StartupMessageParams,
    pub endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    pub mode: ClientMode,
    pub config: &'static ProxyConfig,

    // monitoring
    pub ctx: RequestMonitoring,
    pub cancel_session: Session,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Stage for NeedsAuthentication<S> {
    fn span(&self) -> tracing::Span {
        tracing::info_span!("authenticate")
    }
    async fn run(self) -> Result<DynStage, StageError> {
        let Self {
            stream,
            creds,
            params,
            endpoint_rate_limiter,
            mode,
            config,
            mut ctx,
            cancel_session,
        } = self;

        // check rate limit
        if let Some(ep) = creds.get_endpoint() {
            if !endpoint_rate_limiter.check(ep) {
                return Err(user_facing_error(
                    auth::AuthError::too_many_connections(),
                    &mut ctx,
                    stream,
                ));
            }
        }

        let allow_self_signed_compute = mode.allow_self_signed_compute(config);
        let allow_cleartext = mode.allow_cleartext();

        match creds {
            BackendType::Console(api, creds) => {
                // If there's no project so far, that entails that client doesn't
                // support SNI or other means of passing the endpoint (project) name.
                // We now expect to see a very specific payload in the place of password.
                match creds.try_into() {
                    Err(info) => Ok(Box::new(NeedsPasswordHack {
                        stream,
                        api,
                        params,
                        allow_self_signed_compute,
                        info,
                        allow_cleartext,
                        config: &config.authentication_config,
                        ctx,
                        cancel_session,
                    })),
                    Ok(info) => Ok(Box::new(NeedsAuthSecret {
                        stream,
                        api,
                        params,
                        allow_self_signed_compute,
                        info,
                        unauthenticated_password: None,
                        allow_cleartext,
                        config: &config.authentication_config,
                        ctx,
                        cancel_session,
                    })),
                }
            }
            // NOTE: this auth backend doesn't use client credentials.
            BackendType::Link(link) => Ok(Box::new(NeedsLinkAuthentication {
                stream,
                link,
                params,
                allow_self_signed_compute,
                ctx,
                cancel_session,
            })),
            #[cfg(test)]
            BackendType::Test(_) => {
                unreachable!("this function should never be called in the test backend")
            }
        }
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
