//!
//! The Compute Service listens for compute connections, and serves requests like
//! the GetPage@LSN requests.
//!
//! We support two protocols:
//!
//! 1. Legacy, connection-oriented libpq based protocol. That's
//!    handled by the code in page_service.rs.
//!
//! 2. gRPC based protocol. See compute_service_grpc.rs.
//!
//! To make the transition smooth, without having to open up new firewall ports
//! etc, both protocols are served on the same port. When a new TCP connection
//! is accepted, we peek at the first few bytes incoming from the client to
//! determine which protocol it speaks.
//!
//! TODO: This gets easier once we drop the legacy protocol support. Or if we
//! open a separate port for them.

use std::sync::Arc;

use anyhow::Context;
use futures::FutureExt;
use pageserver_api::config::PageServicePipeliningConfig;
use postgres_backend::AuthType;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::auth::SwappableJwtAuth;
use utils::sync::gate::{Gate, GateGuard};

use crate::compute_service_grpc::launch_compute_service_grpc_server;
use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext, RequestContextBuilder};
use crate::page_service::libpq_page_service_conn_main;
use crate::task_mgr::{self, COMPUTE_REQUEST_RUNTIME, TaskKind};
use crate::tenant::mgr::TenantManager;

///////////////////////////////////////////////////////////////////////////////

pub type ConnectionHandlerResult = anyhow::Result<()>;

pub struct Connections {
    cancel: CancellationToken,
    tasks: tokio::task::JoinSet<ConnectionHandlerResult>,
    gate: Gate,
}

impl Connections {
    pub(crate) async fn shutdown(self) {
        let Self {
            cancel,
            mut tasks,
            gate,
        } = self;
        cancel.cancel();
        while let Some(res) = tasks.join_next().await {
            Self::handle_connection_completion(res);
        }
        gate.close().await;
    }

    fn handle_connection_completion(res: Result<anyhow::Result<()>, tokio::task::JoinError>) {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!("error in page_service connection task: {:?}", e),
            Err(e) => error!("page_service connection task panicked: {:?}", e),
        }
    }
}

pub struct Listener {
    cancel: CancellationToken,
    /// Cancel the listener task through `listen_cancel` to shut down the listener
    /// and get a handle on the existing connections.
    task: JoinHandle<Connections>,
}

pub fn spawn(
    conf: &'static PageServerConf,
    tenant_manager: Arc<TenantManager>,
    pg_auth: Option<Arc<SwappableJwtAuth>>,
    perf_trace_dispatch: Option<Dispatch>,
    tcp_listener: tokio::net::TcpListener,
    tls_config: Option<Arc<rustls::ServerConfig>>,
) -> Listener {
    let cancel = CancellationToken::new();
    let libpq_ctx = RequestContext::todo_child(
        TaskKind::LibpqEndpointListener,
        // listener task shouldn't need to download anything. (We will
        // create a separate sub-contexts for each connection, with their
        // own download behavior. This context is used only to listen and
        // accept connections.)
        DownloadBehavior::Error,
    );

    let task = COMPUTE_REQUEST_RUNTIME.spawn(task_mgr::exit_on_panic_or_error(
        "compute connection listener",
        compute_connection_listener_main(
            conf,
            tenant_manager,
            pg_auth,
            perf_trace_dispatch,
            tcp_listener,
            conf.pg_auth_type,
            tls_config,
            conf.page_service_pipelining.clone(),
            libpq_ctx,
            cancel.clone(),
        )
        .map(anyhow::Ok),
    ));

    Listener { cancel, task }
}

impl Listener {
    pub async fn stop_accepting(self) -> Connections {
        self.cancel.cancel();
        self.task
            .await
            .expect("unreachable: we wrap the listener task in task_mgr::exit_on_panic_or_error")
    }
}

/// Listener loop. Listens for connections, and launches a new handler
/// task for each.
///
/// Returns Ok(()) upon cancellation via `cancel`, returning the set of
/// open connections.
///
#[allow(clippy::too_many_arguments)]
pub async fn compute_connection_listener_main(
    conf: &'static PageServerConf,
    tenant_manager: Arc<TenantManager>,
    auth: Option<Arc<SwappableJwtAuth>>,
    perf_trace_dispatch: Option<Dispatch>,
    listener: tokio::net::TcpListener,
    auth_type: AuthType,
    tls_config: Option<Arc<rustls::ServerConfig>>,
    pipelining_config: PageServicePipeliningConfig,
    listener_ctx: RequestContext,
    listener_cancel: CancellationToken,
) -> Connections {
    let connections_cancel = CancellationToken::new();
    let connections_gate = Gate::default();
    let mut connection_handler_tasks = tokio::task::JoinSet::default();

    // The connection handling task passes the gRPC protocol
    // connections to this channel. The tonic gRPC server reads the
    // channel and takes over the connections from there.
    let (grpc_connections_tx, grpc_connections_rx) = tokio::sync::mpsc::channel(1000);

    // Set up the gRPC service
    launch_compute_service_grpc_server(
        grpc_connections_rx,
        conf,
        tenant_manager.clone(),
        auth.clone(),
        auth_type,
        connections_cancel.clone(),
        &listener_ctx,
    );

    // Main listener loop
    loop {
        let gate_guard = match connections_gate.enter() {
            Ok(guard) => guard,
            Err(_) => break,
        };

        let accepted = tokio::select! {
            biased;
            _ = listener_cancel.cancelled() => break,
            next = connection_handler_tasks.join_next(), if !connection_handler_tasks.is_empty() => {
                let res = next.expect("we dont poll while empty");
                Connections::handle_connection_completion(res);
                continue;
            }
            accepted = listener.accept() => accepted,
        };

        match accepted {
            Ok((socket, peer_addr)) => {
                // Connection established. Spawn a new task to handle it.
                debug!("accepted connection from {}", peer_addr);
                let local_auth = auth.clone();
                let connection_ctx = RequestContextBuilder::from(&listener_ctx)
                    .task_kind(TaskKind::PageRequestHandler)
                    .download_behavior(DownloadBehavior::Download)
                    .perf_span_dispatch(perf_trace_dispatch.clone())
                    .detached_child();

                connection_handler_tasks.spawn(page_service_conn_main(
                    conf,
                    tenant_manager.clone(),
                    local_auth,
                    socket,
                    auth_type,
                    tls_config.clone(),
                    pipelining_config.clone(),
                    connection_ctx,
                    connections_cancel.child_token(),
                    gate_guard,
                    grpc_connections_tx.clone(),
                ));
            }
            Err(err) => {
                // accept() failed. Log the error, and loop back to retry on next connection.
                error!("accept() failed: {:?}", err);
            }
        }
    }

    debug!("page_service listener loop terminated");

    Connections {
        cancel: connections_cancel,
        tasks: connection_handler_tasks,
        gate: connections_gate,
    }
}

/// Handle a new incoming connection.
///
/// This peeks at the first few incoming bytes and dispatches the connection
/// to the legacy libpq handler or the new gRPC handler accordingly.
#[instrument(skip_all, fields(peer_addr, application_name, compute_mode))]
#[allow(clippy::too_many_arguments)]
pub async fn page_service_conn_main(
    conf: &'static PageServerConf,
    tenant_manager: Arc<TenantManager>,
    auth: Option<Arc<SwappableJwtAuth>>,
    socket: tokio::net::TcpStream,
    auth_type: AuthType,
    tls_config: Option<Arc<rustls::ServerConfig>>,
    pipelining_config: PageServicePipeliningConfig,
    connection_ctx: RequestContext,
    cancel: CancellationToken,
    gate_guard: GateGuard,
    grpc_connections_tx: tokio::sync::mpsc::Sender<tokio::io::Result<tokio::net::TcpStream>>,
) -> ConnectionHandlerResult {
    let mut buf: [u8; 4] = [0; 4];

    // Peek
    socket.peek(&mut buf).await?;

    let mut grpc = false;
    if buf[0] == 0x16 {
        // looks like a TLS handshake. Assume gRPC.
        // XXX: Starting with v17, PostgreSQL also supports "direct TLS mode". But
        // the compute doesn't use it.
        grpc = true;
    }

    if buf[0] == b'G' || buf[0] == b'P' {
        // Looks like 'GET' or 'POST'
        // or 'PRI', indicating gRPC over HTTP/2 with prior knowledge
        grpc = true;
    }

    // Dispatch
    if grpc {
        socket
            .set_nodelay(true)
            .context("could not set TCP_NODELAY")?;
        grpc_connections_tx.send(Ok(socket)).await?;
        info!("connection sent to channel");
        Ok(())
    } else {
        libpq_page_service_conn_main(
            conf,
            tenant_manager,
            auth,
            socket,
            auth_type,
            tls_config,
            pipelining_config,
            connection_ctx,
            cancel,
            gate_guard,
        )
        .await
    }
}
