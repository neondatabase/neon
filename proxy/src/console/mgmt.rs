use crate::{
    console::messages::{DatabaseInfo, KickSession},
    waiters::{self, Waiter, Waiters},
};
use anyhow::Context;
use once_cell::sync::Lazy;
use postgres_backend::{self, AuthType, PostgresBackend, PostgresBackendTCP, QueryError};
use pq_proto::{BeMessage, SINGLE_COL_ROWDESC};
use std::future;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, info_span, Instrument};

static CPLANE_WAITERS: Lazy<Waiters<ComputeReady>> = Lazy::new(Default::default);

/// Give caller an opportunity to wait for the cloud's reply.
pub async fn with_waiter<R, T, E>(
    psql_session_id: impl Into<String>,
    action: impl FnOnce(Waiter<'static, ComputeReady>) -> R,
) -> Result<T, E>
where
    R: std::future::Future<Output = Result<T, E>>,
    E: From<waiters::RegisterError>,
{
    let waiter = CPLANE_WAITERS.register(psql_session_id.into())?;
    action(waiter).await
}

pub fn notify(psql_session_id: &str, msg: ComputeReady) -> Result<(), waiters::NotifyError> {
    CPLANE_WAITERS.notify(psql_session_id, msg)
}

/// Console management API listener task.
/// It spawns console response handlers needed for the link auth.
pub async fn task_main(listener: TcpListener) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("mgmt has shut down");
    }

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        info!("accepted connection from {peer_addr}");

        socket
            .set_nodelay(true)
            .context("failed to set client socket option")?;

        let span = info_span!("mgmt", peer = %peer_addr);

        tokio::task::spawn(
            async move {
                info!("serving a new console management API connection");

                // these might be long running connections, have a separate logging for cancelling
                // on shutdown and other ways of stopping.
                let cancelled = scopeguard::guard(tracing::Span::current(), |span| {
                    let _e = span.entered();
                    info!("console management API task cancelled");
                });

                if let Err(e) = handle_connection(socket).await {
                    error!("serving failed with an error: {e}");
                } else {
                    info!("serving completed");
                }

                // we can no longer get dropped
                scopeguard::ScopeGuard::into_inner(cancelled);
            }
            .instrument(span),
        );
    }
}

async fn handle_connection(socket: TcpStream) -> Result<(), QueryError> {
    let pgbackend = PostgresBackend::new(socket, AuthType::Trust, None)?;
    pgbackend.run(&mut MgmtHandler, future::pending::<()>).await
}

/// A message received by `mgmt` when a compute node is ready.
pub type ComputeReady = Result<DatabaseInfo, String>;

// TODO: replace with an http-based protocol.
struct MgmtHandler;
#[async_trait::async_trait]
impl postgres_backend::Handler<tokio::net::TcpStream> for MgmtHandler {
    async fn process_query(
        &mut self,
        pgb: &mut PostgresBackendTCP,
        query: &str,
    ) -> Result<(), QueryError> {
        try_process_query(pgb, query).map_err(|e| {
            error!("failed to process response: {e:?}");
            e
        })
    }
}

fn try_process_query(pgb: &mut PostgresBackendTCP, query: &str) -> Result<(), QueryError> {
    let resp: KickSession = serde_json::from_str(query).context("Failed to parse query as json")?;

    let span = info_span!("event", session_id = resp.session_id);
    let _enter = span.enter();
    info!("got response: {:?}", resp.result);

    match notify(resp.session_id, Ok(resp.result)) {
        Ok(()) => {
            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::DataRow(&[Some(b"ok")]))?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        }
        Err(e) => {
            error!("failed to deliver response to per-client task");
            pgb.write_message_noflush(&BeMessage::ErrorResponse(&e.to_string(), None))?;
        }
    }

    Ok(())
}
