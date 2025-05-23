use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use jose_jwk::jose_b64;
use postgres_client::maybe_tls_stream::MaybeTlsStream;
use rand_core::OsRng;
use tracing::field::display;
use tracing::{debug, info};

use super::AsyncRW;
use super::conn_pool::poll_client;
use super::conn_pool_lib::{Client, ConnInfo, EndpointConnPool, GlobalConnPool};
use super::http_conn_pool::{self, HttpConnPool, LocalProxyClient, poll_http2_client};
use super::local_conn_pool::{self, EXT_NAME, EXT_SCHEMA, EXT_VERSION, LocalConnPool};
use crate::auth::backend::local::StaticAuthRules;
use crate::auth::backend::{ComputeCredentials, ComputeUserInfo};
use crate::auth::{self, AuthError};
use crate::compute;
use crate::compute_ctl::{
    ComputeCtlError, ExtensionInstallRequest, Privilege, SetRoleGrantsRequest,
};
use crate::config::ProxyConfig;
use crate::context::RequestContext;
use crate::control_plane::client::ApiLockError;
use crate::control_plane::errors::{GetAuthInfoError, WakeComputeError};
use crate::error::{ErrorKind, ReportableError, UserFacingError};
use crate::intern::{EndpointIdInt, RoleNameInt};
use crate::pqproto::StartupMessageParams;
use crate::proxy::{connect_auth, connect_compute};
use crate::rate_limiter::EndpointRateLimiter;
use crate::types::{EndpointId, LOCAL_PROXY_SUFFIX};

pub(crate) struct PoolingBackend {
    pub(crate) http_conn_pool:
        Arc<GlobalConnPool<LocalProxyClient, HttpConnPool<LocalProxyClient>>>,
    pub(crate) local_pool: Arc<LocalConnPool<postgres_client::Client>>,
    pub(crate) pool:
        Arc<GlobalConnPool<postgres_client::Client, EndpointConnPool<postgres_client::Client>>>,

    pub(crate) config: &'static ProxyConfig,
    pub(crate) auth_backend: &'static crate::auth::Backend<'static, ()>,
    pub(crate) endpoint_rate_limiter: Arc<EndpointRateLimiter>,
}

impl PoolingBackend {
    pub(crate) async fn authenticate_with_password(
        &self,
        ctx: &RequestContext,
        user_info: &ComputeUserInfo,
        password: &[u8],
    ) -> Result<ComputeCredentials, AuthError> {
        ctx.set_auth_method(crate::context::AuthMethod::Cleartext);

        let user_info = user_info.clone();
        let backend = self.auth_backend.as_ref().map(|()| user_info.clone());
        let access_control = backend.get_endpoint_access_control(ctx).await?;
        access_control.check(
            ctx,
            self.config.authentication_config.ip_allowlist_check_enabled,
            self.config.authentication_config.is_vpc_acccess_proxy,
        )?;

        access_control.connection_attempt_rate_limit(
            ctx,
            &user_info.endpoint,
            &self.endpoint_rate_limiter,
        )?;

        let role_access = backend.get_role_secret(ctx).await?;
        let Some(secret) = role_access.secret else {
            // If we don't have an authentication secret, for the http flow we can just return an error.
            info!("authentication info not found");
            return Err(AuthError::password_failed(&*user_info.user));
        };

        let ep = EndpointIdInt::from(&user_info.endpoint);
        let role = RoleNameInt::from(&user_info.user);
        let auth_outcome = crate::auth::validate_password_and_exchange(
            &self.config.authentication_config.thread_pool,
            ep,
            role,
            password,
            secret,
        )
        .await?;
        let res = match auth_outcome {
            crate::sasl::Outcome::Success(key) => {
                info!("user successfully authenticated");
                Ok(key)
            }
            crate::sasl::Outcome::Failure(reason) => {
                info!("auth backend failed with an error: {reason}");
                Err(AuthError::password_failed(&*user_info.user))
            }
        };
        res.map(|key| ComputeCredentials {
            info: user_info,
            keys: key,
        })
    }

    pub(crate) async fn authenticate_with_jwt(
        &self,
        ctx: &RequestContext,
        user_info: &ComputeUserInfo,
        jwt: String,
    ) -> Result<ComputeCredentials, AuthError> {
        ctx.set_auth_method(crate::context::AuthMethod::Jwt);

        match &self.auth_backend {
            crate::auth::Backend::ControlPlane(console, ()) => {
                let keys = self
                    .config
                    .authentication_config
                    .jwks_cache
                    .check_jwt(
                        ctx,
                        user_info.endpoint.clone(),
                        &user_info.user,
                        &**console,
                        &jwt,
                    )
                    .await?;

                Ok(ComputeCredentials {
                    info: user_info.clone(),
                    keys,
                })
            }
            crate::auth::Backend::Local(_) => {
                let keys = self
                    .config
                    .authentication_config
                    .jwks_cache
                    .check_jwt(
                        ctx,
                        user_info.endpoint.clone(),
                        &user_info.user,
                        &StaticAuthRules,
                        &jwt,
                    )
                    .await?;

                Ok(ComputeCredentials {
                    info: user_info.clone(),
                    keys,
                })
            }
        }
    }

    // Wake up the destination if needed. Code here is a bit involved because
    // we reuse the code from the usual proxy and we need to prepare few structures
    // that this code expects.
    #[tracing::instrument(skip_all, fields(
        pid = tracing::field::Empty,
        compute_id = tracing::field::Empty,
        conn_id = tracing::field::Empty,
    ))]
    pub(crate) async fn connect_to_compute(
        &self,
        ctx: &RequestContext,
        conn_info: ConnInfo,
        keys: ComputeCredentials,
        force_new: bool,
    ) -> Result<Client<postgres_client::Client>, HttpConnError> {
        let maybe_client = if force_new {
            debug!("pool: pool is disabled");
            None
        } else {
            debug!("pool: looking for an existing connection");
            self.pool.get(ctx, &conn_info)?
        };

        if let Some(client) = maybe_client {
            return Ok(client);
        }
        let conn_id = uuid::Uuid::new_v4();
        tracing::Span::current().record("conn_id", display(conn_id));
        info!(%conn_id, "pool: opening a new connection '{conn_info}'");
        let backend = self.auth_backend.as_ref().map(|()| keys.info);

        let mut params = StartupMessageParams::default();
        params.insert("database", &conn_info.dbname);
        params.insert("user", &conn_info.user_info.user);

        let mut auth_info = compute::AuthInfo::with_auth_keys(keys.keys);
        auth_info.set_startup_params(&params, true);

        let node = connect_auth::connect_to_compute_and_auth(
            ctx,
            self.config,
            &backend,
            auth_info,
            connect_compute::TlsNegotiation::Postgres,
        )
        .await?;

        let (client, connection) = postgres_client::connect::managed(
            node.stream,
            Some(node.socket_addr.ip()),
            postgres_client::config::Host::Tcp(node.hostname.to_string()),
            node.socket_addr.port(),
            node.ssl_mode,
            Some(self.config.connect_to_compute.timeout),
        )
        .await?;

        Ok(poll_client(
            self.pool.clone(),
            ctx,
            conn_info,
            client,
            connection,
            conn_id,
            node.aux,
        ))
    }

    // Wake up the destination if needed
    #[tracing::instrument(skip_all, fields(
        compute_id = tracing::field::Empty,
        conn_id = tracing::field::Empty,
    ))]
    pub(crate) async fn connect_to_local_proxy(
        &self,
        ctx: &RequestContext,
        conn_info: ConnInfo,
    ) -> Result<http_conn_pool::Client<LocalProxyClient>, HttpConnError> {
        debug!("pool: looking for an existing connection");
        if let Ok(Some(client)) = self.http_conn_pool.get(ctx, &conn_info) {
            return Ok(client);
        }

        let conn_id = uuid::Uuid::new_v4();
        tracing::Span::current().record("conn_id", display(conn_id));
        debug!(%conn_id, "pool: opening a new connection '{conn_info}'");
        let backend = self.auth_backend.as_ref().map(|()| ComputeUserInfo {
            user: conn_info.user_info.user.clone(),
            endpoint: EndpointId::from(format!(
                "{}{LOCAL_PROXY_SUFFIX}",
                conn_info.user_info.endpoint.normalize()
            )),
            options: conn_info.user_info.options.clone(),
        });

        let node = connect_compute::connect_to_compute(
            ctx,
            self.config,
            &backend,
            connect_compute::TlsNegotiation::Direct,
        )
        .await?;

        let stream = match node.stream.into_framed().into_inner() {
            MaybeTlsStream::Raw(s) => Box::pin(s) as AsyncRW,
            MaybeTlsStream::Tls(s) => Box::pin(s) as AsyncRW,
        };

        let (client, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .timer(TokioTimer::new())
            .keep_alive_interval(Duration::from_secs(20))
            .keep_alive_while_idle(true)
            .keep_alive_timeout(Duration::from_secs(5))
            .handshake(TokioIo::new(stream))
            .await
            .map_err(LocalProxyConnError::H2)?;

        Ok(poll_http2_client(
            self.http_conn_pool.clone(),
            ctx,
            &conn_info,
            client,
            connection,
            conn_id,
            node.aux.clone(),
        ))
    }

    /// Connect to postgres over localhost.
    ///
    /// We expect postgres to be started here, so we won't do any retries.
    ///
    /// # Panics
    ///
    /// Panics if called with a non-local_proxy backend.
    #[tracing::instrument(skip_all, fields(
        pid = tracing::field::Empty,
        conn_id = tracing::field::Empty,
    ))]
    pub(crate) async fn connect_to_local_postgres(
        &self,
        ctx: &RequestContext,
        conn_info: ConnInfo,
        disable_pg_session_jwt: bool,
    ) -> Result<Client<postgres_client::Client>, HttpConnError> {
        if let Some(client) = self.local_pool.get(ctx, &conn_info)? {
            return Ok(client);
        }

        let local_backend = match &self.auth_backend {
            auth::Backend::ControlPlane(_, ()) => {
                unreachable!("only local_proxy can connect to local postgres")
            }
            auth::Backend::Local(local) => local,
        };

        if !self.local_pool.initialized(&conn_info) {
            // only install and grant usage one at a time.
            let _permit = local_backend
                .initialize
                .acquire()
                .await
                .expect("semaphore should never be closed");

            // check again for race
            if !self.local_pool.initialized(&conn_info) && !disable_pg_session_jwt {
                local_backend
                    .compute_ctl
                    .install_extension(&ExtensionInstallRequest {
                        extension: EXT_NAME,
                        database: conn_info.dbname.clone(),
                        version: EXT_VERSION,
                    })
                    .await?;

                local_backend
                    .compute_ctl
                    .grant_role(&SetRoleGrantsRequest {
                        schema: EXT_SCHEMA,
                        privileges: vec![Privilege::Usage],
                        database: conn_info.dbname.clone(),
                        role: conn_info.user_info.user.clone(),
                    })
                    .await?;

                self.local_pool.set_initialized(&conn_info);
            }
        }

        let conn_id = uuid::Uuid::new_v4();
        tracing::Span::current().record("conn_id", display(conn_id));
        info!(%conn_id, "local_pool: opening a new connection '{conn_info}'");

        let (key, jwk) = create_random_jwk();

        let mut config = local_backend
            .node_info
            .conn_info
            .to_postgres_client_config();
        config
            .user(&conn_info.user_info.user)
            .dbname(&conn_info.dbname);
        if !disable_pg_session_jwt {
            config.set_param(
                "options",
                &format!(
                    "-c pg_session_jwt.jwk={}",
                    serde_json::to_string(&jwk).expect("serializing jwk to json should not fail")
                ),
            );
        }

        let pause = ctx.latency_timer_pause(crate::metrics::Waiting::Compute);
        let (client, connection) = config.connect(&postgres_client::NoTls).await?;
        drop(pause);

        let pid = client.get_process_id();
        tracing::Span::current().record("pid", pid);

        let mut handle = local_conn_pool::poll_client(
            self.local_pool.clone(),
            ctx,
            conn_info,
            client,
            connection,
            key,
            conn_id,
            local_backend.node_info.aux.clone(),
        );

        {
            let (client, mut discard) = handle.inner();
            debug!("setting up backend session state");

            // initiates the auth session
            if !disable_pg_session_jwt
                && let Err(e) = client.batch_execute("select auth.init();").await
            {
                discard.discard();
                return Err(e.into());
            }

            info!("backend session state initialized");
        }

        Ok(handle)
    }
}

fn create_random_jwk() -> (SigningKey, jose_jwk::Key) {
    let key = SigningKey::generate(&mut OsRng);

    let jwk = jose_jwk::Key::Okp(jose_jwk::Okp {
        crv: jose_jwk::OkpCurves::Ed25519,
        x: jose_b64::serde::Bytes::from(key.verifying_key().to_bytes().to_vec()),
        d: None,
    });

    (key, jwk)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum HttpConnError {
    #[error("pooled connection closed at inconsistent state")]
    ConnectionClosedAbruptly(#[from] tokio::sync::watch::error::SendError<uuid::Uuid>),
    #[error("could not connect to compute")]
    ConnectError(#[from] compute::ConnectionError),
    #[error("could not connect to postgres in compute")]
    PostgresConnectionError(#[from] postgres_client::Error),
    #[error("could not connect to local-proxy in compute")]
    LocalProxyConnectionError(#[from] LocalProxyConnError),
    #[error("could not parse JWT payload")]
    JwtPayloadError(serde_json::Error),

    #[error("could not install extension: {0}")]
    ComputeCtl(#[from] ComputeCtlError),
    #[error("could not get auth info")]
    GetAuthInfo(#[from] GetAuthInfoError),
    #[error("user not authenticated")]
    AuthError(#[from] AuthError),
    #[error("wake_compute returned error")]
    WakeCompute(#[from] WakeComputeError),
    #[error("error acquiring resource permit: {0}")]
    TooManyConnectionAttempts(#[from] ApiLockError),
}

impl From<connect_auth::AuthError> for HttpConnError {
    fn from(value: connect_auth::AuthError) -> Self {
        match value {
            connect_auth::AuthError::Auth(compute::PostgresError::Postgres(error)) => {
                Self::PostgresConnectionError(error)
            }
            connect_auth::AuthError::Connect(error) => Self::ConnectError(error),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LocalProxyConnError {
    #[error("could not establish h2 connection")]
    H2(#[from] hyper::Error),
}

impl ReportableError for HttpConnError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            HttpConnError::ConnectError(_) => ErrorKind::Compute,
            HttpConnError::ConnectionClosedAbruptly(_) => ErrorKind::Compute,
            HttpConnError::PostgresConnectionError(p) => {
                if p.as_db_error().is_some() {
                    // postgres rejected the connection
                    ErrorKind::Postgres
                } else {
                    // couldn't even reach postgres
                    ErrorKind::Compute
                }
            }
            HttpConnError::LocalProxyConnectionError(_) => ErrorKind::Compute,
            HttpConnError::ComputeCtl(_) => ErrorKind::Service,
            HttpConnError::JwtPayloadError(_) => ErrorKind::User,
            HttpConnError::GetAuthInfo(a) => a.get_error_kind(),
            HttpConnError::AuthError(a) => a.get_error_kind(),
            HttpConnError::WakeCompute(w) => w.get_error_kind(),
            HttpConnError::TooManyConnectionAttempts(w) => w.get_error_kind(),
        }
    }
}

impl UserFacingError for HttpConnError {
    fn to_string_client(&self) -> String {
        match self {
            HttpConnError::ConnectError(p) => p.to_string_client(),
            HttpConnError::ConnectionClosedAbruptly(_) => self.to_string(),
            HttpConnError::PostgresConnectionError(p) => p.to_string(),
            HttpConnError::LocalProxyConnectionError(p) => p.to_string(),
            HttpConnError::ComputeCtl(_) => "could not set up the JWT authorization database extension".to_string(),
            HttpConnError::JwtPayloadError(p) => p.to_string(),
            HttpConnError::GetAuthInfo(c) => c.to_string_client(),
            HttpConnError::AuthError(c) => c.to_string_client(),
            HttpConnError::WakeCompute(c) => c.to_string_client(),
            HttpConnError::TooManyConnectionAttempts(_) => {
                "Failed to acquire permit to connect to the database. Too many database connection attempts are currently ongoing.".to_owned()
            }
        }
    }
}

impl ReportableError for LocalProxyConnError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            LocalProxyConnError::H2(_) => ErrorKind::Compute,
        }
    }
}

impl UserFacingError for LocalProxyConnError {
    fn to_string_client(&self) -> String {
        "Could not establish HTTP connection to the database".to_string()
    }
}
