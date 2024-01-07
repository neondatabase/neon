use std::borrow::Cow;

use crate::{
    auth::BackendType,
    cancellation::Session,
    compute,
    console::{self, mgmt::ComputeReady, provider::NodeInfo, CachedNodeInfo},
    context::RequestMonitoring,
    proxy::connect_compute::{NeedsComputeConnection, TcpMechanism},
    state_machine::{DynStage, ResultExt, Stage, StageError},
    stream::{PqStream, Stream},
    waiters::Waiter,
};
use pq_proto::{BeMessage as Be, StartupMessageParams};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::config::SslMode;
use tracing::info;

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

pub struct NeedsLinkAuthentication<S> {
    pub stream: PqStream<Stream<S>>,
    pub link: Cow<'static, crate::url::ApiUrl>,
    pub params: StartupMessageParams,
    pub allow_self_signed_compute: bool,

    // monitoring
    pub ctx: RequestMonitoring,
    pub cancel_session: Session,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Stage for NeedsLinkAuthentication<S> {
    fn span(&self) -> tracing::Span {
        tracing::info_span!("link", psql_session_id = tracing::field::Empty)
    }
    async fn run(self) -> Result<DynStage, StageError> {
        let Self {
            mut stream,
            link,
            params,
            allow_self_signed_compute,
            mut ctx,
            cancel_session,
        } = self;

        // registering waiter can fail if we get unlucky with rng.
        // just try again.
        let (psql_session_id, waiter) = loop {
            let psql_session_id = new_psql_session_id();

            match console::mgmt::get_waiter(&psql_session_id) {
                Ok(waiter) => break (psql_session_id, waiter),
                Err(_e) => continue,
            }
        };
        tracing::Span::current().record("psql_session_id", &psql_session_id);
        let greeting = hello_message(&link, &psql_session_id);

        info!("sending the auth URL to the user");

        stream
            .write_message_noflush(&Be::AuthenticationOk)
            .and_then(|s| s.write_message_noflush(&Be::CLIENT_ENCODING))
            .and_then(|s| s.write_message_noflush(&Be::NoticeResponse(&greeting)))
            .no_user_error(&mut ctx, crate::error::ErrorKind::Service)?
            .flush()
            .await
            .no_user_error(&mut ctx, crate::error::ErrorKind::Disconnect)?;

        Ok(Box::new(NeedsLinkAuthenticationResponse {
            stream,
            link,
            params,
            allow_self_signed_compute,
            waiter,
            psql_session_id,
            ctx,
            cancel_session,
        }))
    }
}

struct NeedsLinkAuthenticationResponse<S> {
    stream: PqStream<Stream<S>>,
    link: Cow<'static, crate::url::ApiUrl>,
    params: StartupMessageParams,
    allow_self_signed_compute: bool,
    waiter: Waiter<'static, ComputeReady>,
    psql_session_id: String,

    // monitoring
    ctx: RequestMonitoring,
    cancel_session: Session,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Stage
    for NeedsLinkAuthenticationResponse<S>
{
    fn span(&self) -> tracing::Span {
        tracing::info_span!("link_wait", psql_session_id = self.psql_session_id)
    }
    async fn run(self) -> Result<DynStage, StageError> {
        let Self {
            mut stream,
            link,
            params,
            allow_self_signed_compute,
            waiter,
            psql_session_id: _,
            mut ctx,
            cancel_session,
        } = self;

        // Wait for web console response (see `mgmt`).
        info!("waiting for console's reply...");
        let db_info = waiter
            .await
            .no_user_error(&mut ctx, crate::error::ErrorKind::Service)?;

        stream
            .write_message_noflush(&Be::NoticeResponse("Connecting to database."))
            .no_user_error(&mut ctx, crate::error::ErrorKind::Service)?;

        // This config should be self-contained, because we won't
        // take username or dbname from client's startup message.
        let mut config = compute::ConnCfg::new();
        config
            .host(&db_info.host)
            .port(db_info.port)
            .dbname(&db_info.dbname)
            .user(&db_info.user);

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

        let node_info = CachedNodeInfo::new_uncached(NodeInfo {
            config,
            aux: db_info.aux,
            allow_self_signed_compute,
        });
        let user_info = BackendType::Link(link);

        Ok(Box::new(NeedsComputeConnection {
            stream,
            user_info,
            mechanism: TcpMechanism { params },
            node_info,
            ctx,
            cancel_session,
        }))
    }
}
