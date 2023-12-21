mod classic;
mod hacks;
mod link;

pub use link::LinkAuthError;
use smol_str::SmolStr;
use tokio_postgres::config::AuthKeys;

use crate::auth::credentials::check_peer_addr_is_in_list;
use crate::auth::validate_password_and_exchange;
use crate::cache::Cached;
use crate::console::errors::GetAuthInfoError;
use crate::console::AuthSecret;
use crate::proxy::connect_compute::handle_try_wake;
use crate::proxy::retry::retry_after;
use crate::scram;
use crate::stream::Stream;
use crate::{
    auth::{self, ClientCredentials},
    config::AuthenticationConfig,
    console::{
        self,
        provider::{CachedAllowedIps, CachedNodeInfo, ConsoleReqExtra},
        Api,
    },
    metrics::LatencyTimer,
    stream, url,
};
use futures::TryFutureExt;
use std::borrow::Cow;
use std::net::IpAddr;
use std::ops::ControlFlow;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{error, info, warn};

/// This type serves two purposes:
///
/// * When `T` is `()`, it's just a regular auth backend selector
///   which we use in [`crate::config::ProxyConfig`].
///
/// * However, when we substitute `T` with [`ClientCredentials`],
///   this helps us provide the credentials only to those auth
///   backends which require them for the authentication process.
pub enum BackendType<'a, T> {
    /// Current Cloud API (V2).
    Console(Cow<'a, console::provider::neon::Api>, T),
    /// Local mock of Cloud API (V2).
    #[cfg(feature = "testing")]
    Postgres(Cow<'a, console::provider::mock::Api>, T),
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
            Console(endpoint, _) => fmt.debug_tuple("Console").field(&endpoint.url()).finish(),
            #[cfg(feature = "testing")]
            Postgres(endpoint, _) => fmt.debug_tuple("Postgres").field(&endpoint.url()).finish(),
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
            #[cfg(feature = "testing")]
            Postgres(c, x) => Postgres(Cow::Borrowed(c), x),
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
            #[cfg(feature = "testing")]
            Postgres(c, x) => Postgres(c, f(x)),
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
            #[cfg(feature = "testing")]
            Postgres(c, x) => x.map(|x| Postgres(c, x)),
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

pub struct ComputeUserInfoNoEndpoint {
    pub user: SmolStr,
    pub peer_addr: IpAddr,
    pub cache_key: SmolStr,
}

pub struct ComputeUserInfo {
    pub endpoint: SmolStr,
    pub inner: ComputeUserInfoNoEndpoint,
}

pub enum ComputeCredentialKeys {
    #[cfg(feature = "testing")]
    Password(Vec<u8>),
    AuthKeys(AuthKeys),
}

impl TryFrom<ClientCredentials> for ComputeUserInfo {
    // user name
    type Error = ComputeUserInfoNoEndpoint;

    fn try_from(creds: ClientCredentials) -> Result<Self, Self::Error> {
        let inner = ComputeUserInfoNoEndpoint {
            user: creds.user,
            peer_addr: creds.peer_addr,
            cache_key: creds.cache_key,
        };
        match creds.project {
            None => Err(inner),
            Some(endpoint) => Ok(ComputeUserInfo { endpoint, inner }),
        }
    }
}

/// True to its name, this function encapsulates our current auth trade-offs.
/// Here, we choose the appropriate auth flow based on circumstances.
///
/// All authentication flows will emit an AuthenticationOk message if successful.
async fn auth_quirks(
    api: &impl console::Api,
    extra: &ConsoleReqExtra,
    creds: ClientCredentials,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    allow_cleartext: bool,
    config: &'static AuthenticationConfig,
    latency_timer: &mut LatencyTimer,
) -> auth::Result<ComputeCredentials<ComputeCredentialKeys>> {
    // If there's no project so far, that entails that client doesn't
    // support SNI or other means of passing the endpoint (project) name.
    // We now expect to see a very specific payload in the place of password.
    let (info, unauthenticated_password) = match creds.try_into() {
        Err(info) => {
            let res = hacks::password_hack_no_authentication(info, client, latency_timer).await?;
            (res.info, Some(res.keys))
        }
        Ok(info) => (info, None),
    };

    info!("fetching user's authentication info");
    let allowed_ips = api.get_allowed_ips(extra, &info).await?;

    // check allowed list
    if !check_peer_addr_is_in_list(&info.inner.peer_addr, &allowed_ips) {
        return Err(auth::AuthError::ip_address_not_allowed());
    }
    let maybe_secret = api.get_role_secret(extra, &info).await?;

    let cached_secret = maybe_secret.unwrap_or_else(|| {
        // If we don't have an authentication secret, we mock one to
        // prevent malicious probing (possible due to missing protocol steps).
        // This mocked secret will never lead to successful authentication.
        info!("authentication info not found, mocking it");
        Cached::new_uncached(AuthSecret::Scram(scram::ServerSecret::mock(
            &info.inner.user,
            rand::random(),
        )))
    });
    match authenticate_with_secret(
        cached_secret.value.clone(),
        info,
        client,
        unauthenticated_password,
        allow_cleartext,
        config,
        latency_timer,
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
    secret: AuthSecret,
    info: ComputeUserInfo,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    unauthenticated_password: Option<Vec<u8>>,
    allow_cleartext: bool,
    config: &'static AuthenticationConfig,
    latency_timer: &mut LatencyTimer,
) -> auth::Result<ComputeCredentials<ComputeCredentialKeys>> {
    if let Some(password) = unauthenticated_password {
        let auth_outcome = validate_password_and_exchange(&password, secret)?;
        let keys = match auth_outcome {
            crate::sasl::Outcome::Success(key) => key,
            crate::sasl::Outcome::Failure(reason) => {
                info!("auth backend failed with an error: {reason}");
                return Err(auth::AuthError::auth_failed(&*info.inner.user));
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
        return hacks::authenticate_cleartext(info, client, latency_timer, secret).await;
    }

    // Finally, proceed with the main auth flow (SCRAM-based).
    classic::authenticate(info, client, config, latency_timer, secret).await
}

/// Authenticate the user and then wake a compute (or retrieve an existing compute session from cache)
/// only if authentication was successfuly.
async fn auth_and_wake_compute(
    api: &impl console::Api,
    extra: &ConsoleReqExtra,
    creds: ClientCredentials,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    allow_cleartext: bool,
    config: &'static AuthenticationConfig,
    latency_timer: &mut LatencyTimer,
) -> auth::Result<(CachedNodeInfo, ComputeUserInfo)> {
    let compute_credentials = auth_quirks(
        api,
        extra,
        creds,
        client,
        allow_cleartext,
        config,
        latency_timer,
    )
    .await?;

    let mut num_retries = 0;
    let mut node = loop {
        let wake_res = api.wake_compute(extra, &compute_credentials.info).await;
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

    match compute_credentials.keys {
        #[cfg(feature = "testing")]
        ComputeCredentialKeys::Password(password) => node.config.password(password),
        ComputeCredentialKeys::AuthKeys(auth_keys) => node.config.auth_keys(auth_keys),
    };

    Ok((node, compute_credentials.info))
}

impl<'a> BackendType<'a, ClientCredentials> {
    /// Get compute endpoint name from the credentials.
    pub fn get_endpoint(&self) -> Option<SmolStr> {
        use BackendType::*;

        match self {
            Console(_, creds) => creds.project.clone(),
            #[cfg(feature = "testing")]
            Postgres(_, creds) => creds.project.clone(),
            Link(_) => Some("link".into()),
            #[cfg(test)]
            Test(_) => Some("test".into()),
        }
    }

    /// Get username from the credentials.
    pub fn get_user(&self) -> &str {
        use BackendType::*;

        match self {
            Console(_, creds) => &creds.user,
            #[cfg(feature = "testing")]
            Postgres(_, creds) => &creds.user,
            Link(_) => "link",
            #[cfg(test)]
            Test(_) => "test",
        }
    }

    /// Authenticate the client via the requested backend, possibly using credentials.
    #[tracing::instrument(fields(allow_cleartext = allow_cleartext), skip_all)]
    pub async fn authenticate(
        self,
        extra: &ConsoleReqExtra,
        client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
        allow_cleartext: bool,
        config: &'static AuthenticationConfig,
        latency_timer: &mut LatencyTimer,
    ) -> auth::Result<(CachedNodeInfo, BackendType<'a, ComputeUserInfo>)> {
        use BackendType::*;

        let res = match self {
            Console(api, creds) => {
                info!(
                    user = &*creds.user,
                    project = creds.project(),
                    "performing authentication using the console"
                );

                let (cache_info, user_info) = auth_and_wake_compute(
                    &*api,
                    extra,
                    creds,
                    client,
                    allow_cleartext,
                    config,
                    latency_timer,
                )
                .await?;
                (cache_info, BackendType::Console(api, user_info))
            }
            #[cfg(feature = "testing")]
            Postgres(api, creds) => {
                info!(
                    user = &*creds.user,
                    project = creds.project(),
                    "performing authentication using a local postgres instance"
                );

                let (cache_info, user_info) = auth_and_wake_compute(
                    &*api,
                    extra,
                    creds,
                    client,
                    allow_cleartext,
                    config,
                    latency_timer,
                )
                .await?;
                (cache_info, BackendType::Postgres(api, user_info))
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
        extra: &ConsoleReqExtra,
    ) -> Result<CachedAllowedIps, GetAuthInfoError> {
        use BackendType::*;
        match self {
            Console(api, creds) => api.get_allowed_ips(extra, creds).await,
            #[cfg(feature = "testing")]
            Postgres(api, creds) => api.get_allowed_ips(extra, creds).await,
            Link(_) => Ok(Cached::new_uncached(Arc::new(vec![]))),
            #[cfg(test)]
            Test(x) => Ok(Cached::new_uncached(Arc::new(x.get_allowed_ips()?))),
        }
    }

    /// When applicable, wake the compute node, gaining its connection info in the process.
    /// The link auth flow doesn't support this, so we return [`None`] in that case.
    pub async fn wake_compute(
        &self,
        extra: &ConsoleReqExtra,
    ) -> Result<Option<CachedNodeInfo>, console::errors::WakeComputeError> {
        use BackendType::*;

        match self {
            Console(api, creds) => api.wake_compute(extra, creds).map_ok(Some).await,
            #[cfg(feature = "testing")]
            Postgres(api, creds) => api.wake_compute(extra, creds).map_ok(Some).await,
            Link(_) => Ok(None),
            #[cfg(test)]
            Test(x) => x.wake_compute().map(Some),
        }
    }
}
