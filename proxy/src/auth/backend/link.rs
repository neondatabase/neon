use crate::{
    auth, compute,
    console::{self, provider::NodeInfo},
    context::RequestMonitoring,
    error::{ReportableError, UserFacingError},
    stream::PqStream,
    waiters,
};
use pq_proto::BeMessage as Be;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::config::SslMode;
use tracing::{info, info_span};

#[derive(Debug, Error)]
pub enum LinkAuthError {
    /// Authentication error reported by the console.
    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    #[error(transparent)]
    WaiterRegister(#[from] waiters::RegisterError),

    #[error(transparent)]
    WaiterWait(#[from] waiters::WaitError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl UserFacingError for LinkAuthError {
    fn to_string_client(&self) -> String {
        use LinkAuthError::*;
        match self {
            AuthFailed(_) => self.to_string(),
            _ => "Internal error".to_string(),
        }
    }
}

impl ReportableError for LinkAuthError {
    fn get_error_type(&self) -> crate::error::ErrorKind {
        match self {
            LinkAuthError::AuthFailed(_) => crate::error::ErrorKind::User,
            LinkAuthError::WaiterRegister(_) => crate::error::ErrorKind::Service,
            LinkAuthError::WaiterWait(_) => crate::error::ErrorKind::Service,
            LinkAuthError::Io(_) => crate::error::ErrorKind::Disconnect,
        }
    }
}

fn hello_message(redirect_uri: &reqwest::Url, session_id: &str) -> String {
    format!(
        concat![
            "Welcome to Neon!\n",
            "Authenticate by visiting:\n",
            "    {redirect_uri}{session_id}\n\n",
        ],
        redirect_uri = redirect_uri,
        session_id = session_id,
    )
}

pub fn new_psql_session_id() -> String {
    hex::encode(rand::random::<[u8; 8]>())
}

pub(super) async fn authenticate(
    ctx: &mut RequestMonitoring,
    link_uri: &reqwest::Url,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> auth::Result<NodeInfo> {
    // registering waiter can fail if we get unlucky with rng.
    // just try again.
    let (psql_session_id, waiter) = loop {
        let psql_session_id = new_psql_session_id();

        match console::mgmt::get_waiter(&psql_session_id) {
            Ok(waiter) => break (psql_session_id, waiter),
            Err(_e) => continue,
        }
    };

    let span = info_span!("link", psql_session_id = &psql_session_id);
    let greeting = hello_message(link_uri, &psql_session_id);

    // Give user a URL to spawn a new database.
    info!(parent: &span, "sending the auth URL to the user");
    client
        .write_message_noflush(&Be::AuthenticationOk)?
        .write_message_noflush(&Be::CLIENT_ENCODING)?
        .write_message(&Be::NoticeResponse(&greeting))
        .await?;

    // Wait for web console response (see `mgmt`).
    info!(parent: &span, "waiting for console's reply...");
    let db_info = waiter.await.map_err(LinkAuthError::from)?;

    client.write_message_noflush(&Be::NoticeResponse("Connecting to database."))?;

    // This config should be self-contained, because we won't
    // take username or dbname from client's startup message.
    let mut config = compute::ConnCfg::new();
    config
        .host(&db_info.host)
        .port(db_info.port)
        .dbname(&db_info.dbname)
        .user(&db_info.user);

    ctx.set_user(db_info.user.into());
    ctx.set_project(db_info.aux.clone());
    tracing::Span::current().record("ep", &tracing::field::display(&db_info.aux.endpoint_id));

    // Backwards compatibility. pg_sni_proxy uses "--" in domain names
    // while direct connections do not. Once we migrate to pg_sni_proxy
    // everywhere, we can remove this.
    if db_info.host.contains("--") {
        // we need TLS connection with SNI info to properly route it
        config.ssl_mode(SslMode::Require);
    } else {
        config.ssl_mode(SslMode::Disable);
    }

    if let Some(password) = db_info.password {
        config.password(password.as_ref());
    }

    Ok(NodeInfo {
        config,
        aux: db_info.aux,
        allow_self_signed_compute: false, // caller may override
    })
}
