use super::AuthSuccess;
use crate::{
    auth, compute,
    console::{self, provider::NodeInfo},
    error::UserFacingError,
    stream::PqStream,
    waiters,
};
use pq_proto::BeMessage as Be;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
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
    link_uri: &reqwest::Url,
    client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> auth::Result<AuthSuccess<NodeInfo>> {
    let psql_session_id = new_psql_session_id();
    let span = info_span!("link", psql_session_id = &psql_session_id);
    let greeting = hello_message(link_uri, &psql_session_id);

    let db_info = console::mgmt::with_waiter(psql_session_id, |waiter| async {
        // Give user a URL to spawn a new database.
        info!(parent: &span, "sending the auth URL to the user");
        client
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&Be::CLIENT_ENCODING)?
            .write_message(&Be::NoticeResponse(&greeting))
            .await?;

        // Wait for web console response (see `mgmt`).
        info!(parent: &span, "waiting for console's reply...");
        waiter.await?.map_err(LinkAuthError::AuthFailed)
    })
    .await?;

    client.write_message_noflush(&Be::NoticeResponse("Connecting to database."))?;

    // This config should be self-contained, because we won't
    // take username or dbname from client's startup message.
    let mut config = compute::ConnCfg::new();
    config
        .host(&db_info.host)
        .port(db_info.port)
        .dbname(&db_info.dbname)
        .user(&db_info.user);

    // That is a hack to support new way of accessing compute without using a
    // NodePort. Now to access compute in cross-k8s setup (console->compute
    // and link-proxy->compute) we need to connect to the pg_sni_router service
    // using a TLS. Destination compute address is encoded in domain/SNI.
    //
    // However, for link-proxy it is hard add support for outgoing TLS connections
    // as our trick with stealing stream from tokio-postgres doesn't work with TLS.
    // So set sni_host option and use unencrupted connection instead. Once we add
    // encryption support for outgoing connections to the proxy, we can remove
    // this hack.
    if db_info.host.contains("cluster.local") {
        config.options(format!("sni_host={}", db_info.host).as_str());
    }

    if let Some(password) = db_info.password {
        config.password(password.as_ref());
    }

    Ok(AuthSuccess {
        reported_auth_ok: true,
        value: NodeInfo {
            config,
            aux: db_info.aux.into(),
        },
    })
}
