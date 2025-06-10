use std::fmt;

use async_trait::async_trait;
use postgres_client::config::SslMode;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{info, info_span};

use crate::auth::backend::ComputeUserInfo;
use crate::cache::Cached;
use crate::compute::AuthInfo;
use crate::config::AuthenticationConfig;
use crate::context::RequestContext;
use crate::control_plane::client::cplane_proxy_v1;
use crate::control_plane::{self, CachedNodeInfo, NodeInfo};
use crate::error::{ReportableError, UserFacingError};
use crate::pqproto::BeMessage;
use crate::proxy::NeonOptions;
use crate::proxy::wake_compute::WakeComputeBackend;
use crate::stream::PqStream;
use crate::types::RoleName;
use crate::{auth, compute, waiters};

#[derive(Debug, Error)]
pub(crate) enum ConsoleRedirectError {
    #[error(transparent)]
    WaiterRegister(#[from] waiters::RegisterError),

    #[error(transparent)]
    WaiterWait(#[from] waiters::WaitError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Debug)]
pub struct ConsoleRedirectBackend {
    console_uri: reqwest::Url,
    api: cplane_proxy_v1::NeonControlPlaneClient,
}

impl fmt::Debug for cplane_proxy_v1::NeonControlPlaneClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NeonControlPlaneClient")
    }
}

impl UserFacingError for ConsoleRedirectError {
    fn to_string_client(&self) -> String {
        "Internal error".to_string()
    }
}

impl ReportableError for ConsoleRedirectError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            Self::WaiterRegister(_) => crate::error::ErrorKind::Service,
            Self::WaiterWait(_) => crate::error::ErrorKind::Service,
            Self::Io(_) => crate::error::ErrorKind::ClientDisconnect,
        }
    }
}

fn hello_message(
    redirect_uri: &reqwest::Url,
    session_id: &str,
    duration: std::time::Duration,
) -> String {
    let formatted_duration = humantime::format_duration(duration).to_string();
    format!(
        concat![
            "Welcome to Neon!\n",
            "Authenticate by visiting (will expire in {duration}):\n",
            "    {redirect_uri}{session_id}\n\n",
        ],
        duration = formatted_duration,
        redirect_uri = redirect_uri,
        session_id = session_id,
    )
}

pub(crate) fn new_psql_session_id() -> String {
    hex::encode(rand::random::<[u8; 8]>())
}

impl ConsoleRedirectBackend {
    pub fn new(console_uri: reqwest::Url, api: cplane_proxy_v1::NeonControlPlaneClient) -> Self {
        Self { console_uri, api }
    }

    pub(crate) fn get_api(&self) -> &cplane_proxy_v1::NeonControlPlaneClient {
        &self.api
    }

    pub(crate) async fn authenticate(
        &self,
        ctx: &RequestContext,
        auth_config: &'static AuthenticationConfig,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    ) -> auth::Result<(ConsoleRedirectNodeInfo, AuthInfo, ComputeUserInfo)> {
        authenticate(ctx, auth_config, &self.console_uri, client)
            .await
            .map(|(node_info, auth_info, user_info)| {
                (ConsoleRedirectNodeInfo(node_info), auth_info, user_info)
            })
    }
}

pub struct ConsoleRedirectNodeInfo(pub(super) NodeInfo);

#[async_trait]
impl WakeComputeBackend for ConsoleRedirectNodeInfo {
    async fn wake_compute(
        &self,
        _ctx: &RequestContext,
    ) -> Result<CachedNodeInfo, control_plane::errors::WakeComputeError> {
        Ok(Cached::new_uncached(self.0.clone()))
    }
}

async fn authenticate(
    ctx: &RequestContext,
    auth_config: &'static AuthenticationConfig,
    link_uri: &reqwest::Url,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> auth::Result<(NodeInfo, AuthInfo, ComputeUserInfo)> {
    ctx.set_auth_method(crate::context::AuthMethod::ConsoleRedirect);

    // registering waiter can fail if we get unlucky with rng.
    // just try again.
    let (psql_session_id, waiter) = loop {
        let psql_session_id = new_psql_session_id();

        if let Ok(waiter) = control_plane::mgmt::get_waiter(&psql_session_id) {
            break (psql_session_id, waiter);
        }
    };

    let span = info_span!("console_redirect", psql_session_id = &psql_session_id);
    let greeting = hello_message(
        link_uri,
        &psql_session_id,
        auth_config.console_redirect_confirmation_timeout,
    );

    // Give user a URL to spawn a new database.
    info!(parent: &span, "sending the auth URL to the user");
    client.write_message(BeMessage::AuthenticationOk);
    client.write_message(BeMessage::ParameterStatus {
        name: b"client_encoding",
        value: b"UTF8",
    });
    client.write_message(BeMessage::NoticeResponse(&greeting));
    client.flush().await?;

    // Wait for console response via control plane (see `mgmt`).
    info!(parent: &span, "waiting for console's reply...");
    let db_info = tokio::time::timeout(auth_config.console_redirect_confirmation_timeout, waiter)
        .await
        .map_err(|_elapsed| {
            auth::AuthError::confirmation_timeout(
                auth_config.console_redirect_confirmation_timeout.into(),
            )
        })?
        .map_err(ConsoleRedirectError::from)?;

    if auth_config.ip_allowlist_check_enabled {
        if let Some(allowed_ips) = &db_info.allowed_ips {
            if !auth::check_peer_addr_is_in_list(&ctx.peer_addr(), allowed_ips) {
                return Err(auth::AuthError::ip_address_not_allowed(ctx.peer_addr()));
            }
        }
    }

    // Check if the access over the public internet is allowed, otherwise block. Note that
    // the console redirect is not behind the VPC service endpoint, so we don't need to check
    // the VPC endpoint ID.
    if let Some(public_access_allowed) = db_info.public_access_allowed {
        if !public_access_allowed {
            return Err(auth::AuthError::NetworkNotAllowed);
        }
    }

    client.write_message(BeMessage::NoticeResponse("Connecting to database."));

    // Backwards compatibility. pg_sni_proxy uses "--" in domain names
    // while direct connections do not. Once we migrate to pg_sni_proxy
    // everywhere, we can remove this.
    let ssl_mode = if db_info.host.contains("--") {
        // we need TLS connection with SNI info to properly route it
        SslMode::Require
    } else {
        SslMode::Disable
    };

    let conn_info = compute::ConnectInfo {
        host: db_info.host.into(),
        port: db_info.port,
        ssl_mode,
        host_addr: None,
    };
    let auth_info =
        AuthInfo::for_console_redirect(&db_info.dbname, &db_info.user, db_info.password.as_deref());

    let user: RoleName = db_info.user.into();
    let user_info = ComputeUserInfo {
        endpoint: db_info.aux.endpoint_id.as_str().into(),
        user: user.clone(),
        options: NeonOptions::default(),
    };

    ctx.set_dbname(db_info.dbname.into());
    ctx.set_user(user);
    ctx.set_project(db_info.aux.clone());
    info!("woken up a compute node");

    Ok((
        NodeInfo {
            conn_info,
            aux: db_info.aux,
        },
        auth_info,
        user_info,
    ))
}
