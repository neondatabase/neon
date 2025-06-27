mod classic;
mod console_redirect;
mod hacks;
pub mod jwt;
pub mod local;

use std::sync::Arc;

pub use console_redirect::ConsoleRedirectBackend;
pub(crate) use console_redirect::ConsoleRedirectError;
use local::LocalBackend;
use postgres_client::config::AuthKeys;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, info};

use crate::auth::{self, ComputeUserInfoMaybeEndpoint, validate_password_and_exchange};
use crate::cache::Cached;
use crate::config::AuthenticationConfig;
use crate::context::RequestContext;
use crate::control_plane::client::ControlPlaneClient;
use crate::control_plane::errors::GetAuthInfoError;
use crate::control_plane::messages::EndpointRateLimitConfig;
use crate::control_plane::{
    self, AccessBlockerFlags, AuthSecret, CachedNodeInfo, ControlPlaneApi, EndpointAccessControl,
    RoleAccessControl,
};
use crate::intern::EndpointIdInt;
use crate::pqproto::BeMessage;
use crate::proxy::NeonOptions;
use crate::proxy::wake_compute::WakeComputeBackend;
use crate::rate_limiter::EndpointRateLimiter;
use crate::stream::Stream;
use crate::types::{EndpointCacheKey, EndpointId, RoleName};
use crate::{scram, stream};

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
pub enum Backend<'a, T> {
    /// Cloud API (V2).
    ControlPlane(MaybeOwned<'a, ControlPlaneClient>, T),
    /// Local proxy uses configured auth credentials and does not wake compute
    Local(MaybeOwned<'a, LocalBackend>),
}

impl std::fmt::Display for Backend<'_, ()> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ControlPlane(api, ()) => match &**api {
                ControlPlaneClient::ProxyV1(endpoint) => fmt
                    .debug_tuple("ControlPlane::ProxyV1")
                    .field(&endpoint.url())
                    .finish(),
                #[cfg(any(test, feature = "testing"))]
                ControlPlaneClient::PostgresMock(endpoint) => {
                    let url = endpoint.url();
                    match url::Url::parse(url) {
                        Ok(mut url) => {
                            let _ = url.set_password(Some("_redacted_"));
                            let url = url.as_str();
                            fmt.debug_tuple("ControlPlane::PostgresMock")
                                .field(&url)
                                .finish()
                        }
                        Err(_) => fmt
                            .debug_tuple("ControlPlane::PostgresMock")
                            .field(&url)
                            .finish(),
                    }
                }
                #[cfg(test)]
                ControlPlaneClient::Test(_) => fmt.debug_tuple("ControlPlane::Test").finish(),
            },
            Self::Local(_) => fmt.debug_tuple("Local").finish(),
        }
    }
}

impl<T> Backend<'_, T> {
    /// Very similar to [`std::option::Option::as_ref`].
    /// This helps us pass structured config to async tasks.
    pub(crate) fn as_ref(&self) -> Backend<'_, &T> {
        match self {
            Self::ControlPlane(c, x) => Backend::ControlPlane(MaybeOwned::Borrowed(c), x),
            Self::Local(l) => Backend::Local(MaybeOwned::Borrowed(l)),
        }
    }

    pub(crate) fn get_api(&self) -> &ControlPlaneClient {
        match self {
            Self::ControlPlane(api, _) => api,
            Self::Local(_) => panic!("Local backend has no API"),
        }
    }

    pub(crate) fn is_local_proxy(&self) -> bool {
        matches!(self, Self::Local(_))
    }
}

impl<'a, T> Backend<'a, T> {
    /// Very similar to [`std::option::Option::map`].
    /// Maps [`Backend<T>`] to [`Backend<R>`] by applying
    /// a function to a contained value.
    pub(crate) fn map<R>(self, f: impl FnOnce(T) -> R) -> Backend<'a, R> {
        match self {
            Self::ControlPlane(c, x) => Backend::ControlPlane(c, f(x)),
            Self::Local(l) => Backend::Local(l),
        }
    }
}
impl<'a, T, E> Backend<'a, Result<T, E>> {
    /// Very similar to [`std::option::Option::transpose`].
    /// This is most useful for error handling.
    pub(crate) fn transpose(self) -> Result<Backend<'a, T>, E> {
        match self {
            Self::ControlPlane(c, x) => x.map(|x| Backend::ControlPlane(c, x)),
            Self::Local(l) => Ok(Backend::Local(l)),
        }
    }
}

pub(crate) struct ComputeCredentials {
    pub(crate) info: ComputeUserInfo,
    pub(crate) keys: ComputeCredentialKeys,
}

#[derive(Debug, Clone)]
pub(crate) struct ComputeUserInfoNoEndpoint {
    pub(crate) user: RoleName,
    pub(crate) options: NeonOptions,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ComputeUserInfo {
    pub(crate) endpoint: EndpointId,
    pub(crate) user: RoleName,
    pub(crate) options: NeonOptions,
}

impl ComputeUserInfo {
    pub(crate) fn endpoint_cache_key(&self) -> EndpointCacheKey {
        self.options.get_cache_key(&self.endpoint)
    }
}

#[cfg_attr(test, derive(Debug))]
pub(crate) enum ComputeCredentialKeys {
    AuthKeys(AuthKeys),
    JwtPayload(Vec<u8>),
    None,
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
    ctx: &RequestContext,
    api: &impl control_plane::ControlPlaneApi,
    user_info: ComputeUserInfoMaybeEndpoint,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    allow_cleartext: bool,
    config: &'static AuthenticationConfig,
    endpoint_rate_limiter: Arc<EndpointRateLimiter>,
) -> auth::Result<ComputeCredentials> {
    // If there's no project so far, that entails that client doesn't
    // support SNI or other means of passing the endpoint (project) name.
    // We now expect to see a very specific payload in the place of password.
    let (info, unauthenticated_password) = match user_info.try_into() {
        Err(info) => {
            let (info, password) =
                hacks::password_hack_no_authentication(ctx, info, client).await?;
            ctx.set_endpoint_id(info.endpoint.clone());
            (info, Some(password))
        }
        Ok(info) => (info, None),
    };

    debug!("fetching authentication info and allowlists");

    let access_controls = api
        .get_endpoint_access_control(ctx, &info.endpoint, &info.user)
        .await?;

    access_controls.check(
        ctx,
        config.ip_allowlist_check_enabled,
        config.is_vpc_acccess_proxy,
    )?;

    access_controls.connection_attempt_rate_limit(ctx, &info.endpoint, &endpoint_rate_limiter)?;

    let role_access = api
        .get_role_access_control(ctx, &info.endpoint, &info.user)
        .await?;

    let secret = if let Some(secret) = role_access.secret {
        secret
    } else {
        // If we don't have an authentication secret, we mock one to
        // prevent malicious probing (possible due to missing protocol steps).
        // This mocked secret will never lead to successful authentication.
        info!("authentication info not found, mocking it");
        AuthSecret::Scram(scram::ServerSecret::mock(rand::random()))
    };

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
        Err(e) => Err(e),
    }
}

async fn authenticate_with_secret(
    ctx: &RequestContext,
    secret: AuthSecret,
    info: ComputeUserInfo,
    client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
    unauthenticated_password: Option<Vec<u8>>,
    allow_cleartext: bool,
    config: &'static AuthenticationConfig,
) -> auth::Result<ComputeCredentials> {
    if let Some(password) = unauthenticated_password {
        let ep = EndpointIdInt::from(&info.endpoint);

        let auth_outcome =
            validate_password_and_exchange(&config.thread_pool, ep, &password, secret).await?;
        let keys = match auth_outcome {
            crate::sasl::Outcome::Success(key) => key,
            crate::sasl::Outcome::Failure(reason) => {
                info!("auth backend failed with an error: {reason}");
                return Err(auth::AuthError::password_failed(&*info.user));
            }
        };

        // we have authenticated the password
        client.write_message(BeMessage::AuthenticationOk);

        return Ok(ComputeCredentials { info, keys });
    }

    // -- the remaining flows are self-authenticating --

    // Perform cleartext auth if we're allowed to do that.
    // Currently, we use it for websocket connections (latency).
    if allow_cleartext {
        ctx.set_auth_method(crate::context::AuthMethod::Cleartext);
        return hacks::authenticate_cleartext(ctx, info, client, secret, config).await;
    }

    // Finally, proceed with the main auth flow (SCRAM-based).
    classic::authenticate(ctx, info, client, config, secret).await
}

impl<'a> Backend<'a, ComputeUserInfoMaybeEndpoint> {
    /// Get username from the credentials.
    pub(crate) fn get_user(&self) -> &str {
        match self {
            Self::ControlPlane(_, user_info) => &user_info.user,
            Self::Local(_) => "local",
        }
    }

    /// Authenticate the client via the requested backend, possibly using credentials.
    #[tracing::instrument(fields(allow_cleartext = allow_cleartext), skip_all)]
    pub(crate) async fn authenticate(
        self,
        ctx: &RequestContext,
        client: &mut stream::PqStream<Stream<impl AsyncRead + AsyncWrite + Unpin>>,
        allow_cleartext: bool,
        config: &'static AuthenticationConfig,
        endpoint_rate_limiter: Arc<EndpointRateLimiter>,
    ) -> auth::Result<Backend<'a, ComputeCredentials>> {
        let res = match self {
            Self::ControlPlane(api, user_info) => {
                debug!(
                    user = &*user_info.user,
                    project = user_info.endpoint(),
                    "performing authentication using the console"
                );

                let auth_res = auth_quirks(
                    ctx,
                    &*api,
                    user_info.clone(),
                    client,
                    allow_cleartext,
                    config,
                    endpoint_rate_limiter,
                )
                .await;
                match auth_res {
                    Ok(credentials) => Ok(Backend::ControlPlane(api, credentials)),
                    Err(e) => {
                        // The password could have been changed, so we invalidate the cache.
                        // We should only invalidate the cache if the TTL might have expired.
                        if e.is_password_failed() {
                            #[allow(irrefutable_let_patterns)]
                            if let ControlPlaneClient::ProxyV1(api) = &*api {
                                if let Some(ep) = &user_info.endpoint_id {
                                    api.caches
                                        .project_info
                                        .maybe_invalidate_role_secret(ep, &user_info.user);
                                }
                            }
                        }

                        Err(e)
                    }
                }
            }
            Self::Local(_) => {
                return Err(auth::AuthError::bad_auth_method("invalid for local proxy"));
            }
        };

        // TODO: replace with some metric
        info!("user successfully authenticated");
        res
    }
}

impl Backend<'_, ComputeUserInfo> {
    pub(crate) async fn get_role_secret(
        &self,
        ctx: &RequestContext,
    ) -> Result<RoleAccessControl, GetAuthInfoError> {
        match self {
            Self::ControlPlane(api, user_info) => {
                api.get_role_access_control(ctx, &user_info.endpoint, &user_info.user)
                    .await
            }
            Self::Local(_) => Ok(RoleAccessControl { secret: None }),
        }
    }

    pub(crate) async fn get_endpoint_access_control(
        &self,
        ctx: &RequestContext,
    ) -> Result<EndpointAccessControl, GetAuthInfoError> {
        match self {
            Self::ControlPlane(api, user_info) => {
                api.get_endpoint_access_control(ctx, &user_info.endpoint, &user_info.user)
                    .await
            }
            Self::Local(_) => Ok(EndpointAccessControl {
                allowed_ips: Arc::new(vec![]),
                allowed_vpce: Arc::new(vec![]),
                flags: AccessBlockerFlags::default(),
                rate_limits: EndpointRateLimitConfig::default(),
            }),
        }
    }
}

#[async_trait::async_trait]
impl WakeComputeBackend for Backend<'_, ComputeUserInfo> {
    async fn wake_compute(
        &self,
        ctx: &RequestContext,
    ) -> Result<CachedNodeInfo, control_plane::errors::WakeComputeError> {
        match self {
            Self::ControlPlane(api, info) => api.wake_compute(ctx, info).await,
            Self::Local(local) => Ok(Cached::new_uncached(local.node_info.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unimplemented, clippy::unwrap_used)]

    use std::sync::Arc;

    use bytes::BytesMut;
    use control_plane::AuthSecret;
    use fallible_iterator::FallibleIterator;
    use once_cell::sync::Lazy;
    use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
    use postgres_protocol::message::backend::Message as PgMessage;
    use postgres_protocol::message::frontend;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

    use super::auth_quirks;
    use super::jwt::JwkCache;
    use crate::auth::{ComputeUserInfoMaybeEndpoint, IpPattern};
    use crate::config::AuthenticationConfig;
    use crate::context::RequestContext;
    use crate::control_plane::messages::EndpointRateLimitConfig;
    use crate::control_plane::{
        self, AccessBlockerFlags, CachedNodeInfo, EndpointAccessControl, RoleAccessControl,
    };
    use crate::proxy::NeonOptions;
    use crate::rate_limiter::EndpointRateLimiter;
    use crate::scram::ServerSecret;
    use crate::scram::threadpool::ThreadPool;
    use crate::stream::{PqStream, Stream};

    struct Auth {
        ips: Vec<IpPattern>,
        vpc_endpoint_ids: Vec<String>,
        access_blocker_flags: AccessBlockerFlags,
        secret: AuthSecret,
    }

    impl control_plane::ControlPlaneApi for Auth {
        async fn get_role_access_control(
            &self,
            _ctx: &RequestContext,
            _endpoint: &crate::types::EndpointId,
            _role: &crate::types::RoleName,
        ) -> Result<RoleAccessControl, control_plane::errors::GetAuthInfoError> {
            Ok(RoleAccessControl {
                secret: Some(self.secret.clone()),
            })
        }

        async fn get_endpoint_access_control(
            &self,
            _ctx: &RequestContext,
            _endpoint: &crate::types::EndpointId,
            _role: &crate::types::RoleName,
        ) -> Result<EndpointAccessControl, control_plane::errors::GetAuthInfoError> {
            Ok(EndpointAccessControl {
                allowed_ips: Arc::new(self.ips.clone()),
                allowed_vpce: Arc::new(self.vpc_endpoint_ids.clone()),
                flags: self.access_blocker_flags,
                rate_limits: EndpointRateLimitConfig::default(),
            })
        }

        async fn get_endpoint_jwks(
            &self,
            _ctx: &RequestContext,
            _endpoint: &crate::types::EndpointId,
        ) -> Result<Vec<super::jwt::AuthRule>, control_plane::errors::GetEndpointJwksError>
        {
            unimplemented!()
        }

        async fn wake_compute(
            &self,
            _ctx: &RequestContext,
            _user_info: &super::ComputeUserInfo,
        ) -> Result<CachedNodeInfo, control_plane::errors::WakeComputeError> {
            unimplemented!()
        }
    }

    static CONFIG: Lazy<AuthenticationConfig> = Lazy::new(|| AuthenticationConfig {
        jwks_cache: JwkCache::default(),
        thread_pool: ThreadPool::new(1),
        scram_protocol_timeout: std::time::Duration::from_secs(5),
        ip_allowlist_check_enabled: true,
        is_vpc_acccess_proxy: false,
        is_auth_broker: false,
        accept_jwts: false,
        console_redirect_confirmation_timeout: std::time::Duration::from_secs(5),
    });

    async fn read_message(r: &mut (impl AsyncRead + Unpin), b: &mut BytesMut) -> PgMessage {
        loop {
            r.read_buf(&mut *b).await.unwrap();
            if let Some(m) = PgMessage::parse(&mut *b).unwrap() {
                break m;
            }
        }
    }

    #[tokio::test]
    async fn auth_quirks_scram() {
        let (mut client, server) = tokio::io::duplex(1024);
        let mut stream = PqStream::new_skip_handshake(Stream::from_raw(server));

        let ctx = RequestContext::test();
        let api = Auth {
            ips: vec![],
            vpc_endpoint_ids: vec![],
            access_blocker_flags: AccessBlockerFlags::default(),
            secret: AuthSecret::Scram(ServerSecret::build("my-secret-password").await.unwrap()),
        };

        let user_info = ComputeUserInfoMaybeEndpoint {
            user: "conrad".into(),
            endpoint_id: Some("endpoint".into()),
            options: NeonOptions::default(),
        };

        let handle = tokio::spawn(async move {
            let mut scram = ScramSha256::new(b"my-secret-password", ChannelBinding::unsupported());

            let mut read = BytesMut::new();

            // server should offer scram
            match read_message(&mut client, &mut read).await {
                PgMessage::AuthenticationSasl(a) => {
                    let options: Vec<&str> = a.mechanisms().collect().unwrap();
                    assert_eq!(options, ["SCRAM-SHA-256"]);
                }
                _ => panic!("wrong message"),
            }

            // client sends client-first-message
            let mut write = BytesMut::new();
            frontend::sasl_initial_response("SCRAM-SHA-256", scram.message(), &mut write).unwrap();
            client.write_all(&write).await.unwrap();

            // server response with server-first-message
            match read_message(&mut client, &mut read).await {
                PgMessage::AuthenticationSaslContinue(a) => {
                    scram.update(a.data()).await.unwrap();
                }
                _ => panic!("wrong message"),
            }

            // client response with client-final-message
            write.clear();
            frontend::sasl_response(scram.message(), &mut write).unwrap();
            client.write_all(&write).await.unwrap();

            // server response with server-final-message
            match read_message(&mut client, &mut read).await {
                PgMessage::AuthenticationSaslFinal(a) => {
                    scram.finish(a.data()).unwrap();
                }
                _ => panic!("wrong message"),
            }
        });
        let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new_with_shards(
            EndpointRateLimiter::DEFAULT,
            64,
        ));

        let _creds = auth_quirks(
            &ctx,
            &api,
            user_info,
            &mut stream,
            false,
            &CONFIG,
            endpoint_rate_limiter,
        )
        .await
        .unwrap();

        // flush the final server message
        stream.flush().await.unwrap();

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn auth_quirks_cleartext() {
        let (mut client, server) = tokio::io::duplex(1024);
        let mut stream = PqStream::new_skip_handshake(Stream::from_raw(server));

        let ctx = RequestContext::test();
        let api = Auth {
            ips: vec![],
            vpc_endpoint_ids: vec![],
            access_blocker_flags: AccessBlockerFlags::default(),
            secret: AuthSecret::Scram(ServerSecret::build("my-secret-password").await.unwrap()),
        };

        let user_info = ComputeUserInfoMaybeEndpoint {
            user: "conrad".into(),
            endpoint_id: Some("endpoint".into()),
            options: NeonOptions::default(),
        };

        let handle = tokio::spawn(async move {
            let mut read = BytesMut::new();
            let mut write = BytesMut::new();

            // server should offer cleartext
            match read_message(&mut client, &mut read).await {
                PgMessage::AuthenticationCleartextPassword => {}
                _ => panic!("wrong message"),
            }

            // client responds with password
            write.clear();
            frontend::password_message(b"my-secret-password", &mut write).unwrap();
            client.write_all(&write).await.unwrap();
        });
        let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new_with_shards(
            EndpointRateLimiter::DEFAULT,
            64,
        ));

        let _creds = auth_quirks(
            &ctx,
            &api,
            user_info,
            &mut stream,
            true,
            &CONFIG,
            endpoint_rate_limiter,
        )
        .await
        .unwrap();

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn auth_quirks_password_hack() {
        let (mut client, server) = tokio::io::duplex(1024);
        let mut stream = PqStream::new_skip_handshake(Stream::from_raw(server));

        let ctx = RequestContext::test();
        let api = Auth {
            ips: vec![],
            vpc_endpoint_ids: vec![],
            access_blocker_flags: AccessBlockerFlags::default(),
            secret: AuthSecret::Scram(ServerSecret::build("my-secret-password").await.unwrap()),
        };

        let user_info = ComputeUserInfoMaybeEndpoint {
            user: "conrad".into(),
            endpoint_id: None,
            options: NeonOptions::default(),
        };

        let handle = tokio::spawn(async move {
            let mut read = BytesMut::new();

            // server should offer cleartext
            match read_message(&mut client, &mut read).await {
                PgMessage::AuthenticationCleartextPassword => {}
                _ => panic!("wrong message"),
            }

            // client responds with password
            let mut write = BytesMut::new();
            frontend::password_message(b"endpoint=my-endpoint;my-secret-password", &mut write)
                .unwrap();
            client.write_all(&write).await.unwrap();
        });

        let endpoint_rate_limiter = Arc::new(EndpointRateLimiter::new_with_shards(
            EndpointRateLimiter::DEFAULT,
            64,
        ));

        let creds = auth_quirks(
            &ctx,
            &api,
            user_info,
            &mut stream,
            true,
            &CONFIG,
            endpoint_rate_limiter,
        )
        .await
        .unwrap();

        assert_eq!(creds.info.endpoint, "my-endpoint");

        handle.await.unwrap();
    }
}
