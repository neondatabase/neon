//! The Page Service listens for client connections and serves their GetPage@LSN
//! requests.

use std::any::Any;
use std::borrow::Cow;
use std::num::NonZeroUsize;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};
use std::{io, str};

use anyhow::{Context as _, bail};
use bytes::{Buf as _, BufMut as _, BytesMut};
use chrono::Utc;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use itertools::Itertools;
use jsonwebtoken::TokenData;
use once_cell::sync::OnceCell;
use pageserver_api::config::{
    GetVectoredConcurrentIo, PageServicePipeliningConfig, PageServicePipeliningConfigPipelined,
    PageServiceProtocolPipelinedBatchingStrategy, PageServiceProtocolPipelinedExecutionStrategy,
};
use pageserver_api::key::rel_block_to_key;
use pageserver_api::models::{PageTraceEvent, TenantState};
use pageserver_api::pagestream_api::{
    self, PagestreamBeMessage, PagestreamDbSizeRequest, PagestreamDbSizeResponse,
    PagestreamErrorResponse, PagestreamExistsRequest, PagestreamExistsResponse,
    PagestreamFeMessage, PagestreamGetPageRequest, PagestreamGetSlruSegmentRequest,
    PagestreamGetSlruSegmentResponse, PagestreamNblocksRequest, PagestreamNblocksResponse,
    PagestreamProtocolVersion, PagestreamRequest,
};
use pageserver_api::reltag::SlruKind;
use pageserver_api::shard::TenantShardId;
use pageserver_page_api as page_api;
use pageserver_page_api::proto;
use postgres_backend::{
    AuthType, PostgresBackend, PostgresBackendReader, QueryError, is_expected_io_error,
};
use postgres_ffi::BLCKSZ;
use postgres_ffi_types::constants::DEFAULTTABLESPACE_OID;
use pq_proto::framed::ConnectionError;
use pq_proto::{BeMessage, FeMessage, FeStartupPacket, RowDescriptor};
use smallvec::{SmallVec, smallvec};
use strum_macros::IntoStaticStr;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, BufWriter};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::service::Interceptor as _;
use tonic::transport::server::TcpConnectInfo;
use tracing::*;
use utils::auth::{Claims, Scope, SwappableJwtAuth};
use utils::id::{TenantId, TenantTimelineId, TimelineId};
use utils::logging::log_slow;
use utils::lsn::Lsn;
use utils::shard::ShardIndex;
use utils::simple_rcu::RcuReadGuard;
use utils::sync::gate::{Gate, GateGuard};
use utils::sync::spsc_fold;
use utils::{failpoint_support, span_record};

use crate::auth::check_permission;
use crate::basebackup::{self, BasebackupError};
use crate::config::PageServerConf;
use crate::context::{
    DownloadBehavior, PerfInstrumentFutureExt, RequestContext, RequestContextBuilder,
};
use crate::metrics::{
    self, COMPUTE_COMMANDS_COUNTERS, ComputeCommandKind, GetPageBatchBreakReason, LIVE_CONNECTIONS,
    MISROUTED_PAGESTREAM_REQUESTS, SmgrOpTimer, TimelineMetrics,
};
use crate::pgdatadir_mapping::{LsnRange, Version};
use crate::span::{
    debug_assert_current_span_has_tenant_and_timeline_id,
    debug_assert_current_span_has_tenant_and_timeline_id_no_shard_id,
};
use crate::task_mgr::{self, COMPUTE_REQUEST_RUNTIME, TaskKind};
use crate::tenant::mgr::{
    GetActiveTenantError, GetTenantError, ShardResolveResult, ShardSelector, TenantManager,
};
use crate::tenant::storage_layer::IoConcurrency;
use crate::tenant::timeline::handle::{Handle, HandleUpgradeError, WeakHandle};
use crate::tenant::timeline::{self, WaitLsnError, WaitLsnTimeout, WaitLsnWaiter};
use crate::tenant::{GetTimelineError, PageReconstructError, Timeline};
use crate::{CancellableTask, PERF_TRACE_TARGET, timed_after_cancellation};

/// How long we may wait for a [`crate::tenant::mgr::TenantSlot::InProgress`]` and/or a [`crate::tenant::TenantShard`] which
/// is not yet in state [`TenantState::Active`].
///
/// NB: this is a different value than [`crate::http::routes::ACTIVE_TENANT_TIMEOUT`].
/// HADRON: reduced timeout and we will retry in Cache::get().
const ACTIVE_TENANT_TIMEOUT: Duration = Duration::from_millis(5000);

/// Threshold at which to log slow GetPage requests.
const LOG_SLOW_GETPAGE_THRESHOLD: Duration = Duration::from_secs(30);

/// The idle time before sending TCP keepalive probes for gRPC connections. The
/// interval and timeout between each probe is configured via sysctl. This
/// allows detecting dead connections sooner.
const GRPC_TCP_KEEPALIVE_TIME: Duration = Duration::from_secs(60);

/// Whether to enable TCP nodelay for gRPC connections. This disables Nagle's
/// algorithm, which can cause latency spikes for small messages.
const GRPC_TCP_NODELAY: bool = true;

/// The interval between HTTP2 keepalive pings. This allows shutting down server
/// tasks when clients are unresponsive.
const GRPC_HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// The timeout for HTTP2 keepalive pings. Should be <= GRPC_KEEPALIVE_INTERVAL.
const GRPC_HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// Number of concurrent gRPC streams per TCP connection. We expect something
/// like 8 GetPage streams per connections, plus any unary requests.
const GRPC_MAX_CONCURRENT_STREAMS: u32 = 256;

///////////////////////////////////////////////////////////////////////////////

pub struct Listener {
    cancel: CancellationToken,
    /// Cancel the listener task through `listen_cancel` to shut down the listener
    /// and get a handle on the existing connections.
    task: JoinHandle<Connections>,
}

pub struct Connections {
    cancel: CancellationToken,
    tasks: tokio::task::JoinSet<ConnectionHandlerResult>,
    gate: Gate,
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
        "libpq listener",
        libpq_listener_main(
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

///
/// Main loop of the page service.
///
/// Listens for connections, and launches a new handler task for each.
///
/// Returns Ok(()) upon cancellation via `cancel`, returning the set of
/// open connections.
///
#[allow(clippy::too_many_arguments)]
pub async fn libpq_listener_main(
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

type ConnectionHandlerResult = anyhow::Result<()>;

/// Perf root spans start at the per-request level, after shard routing.
/// This struct carries connection-level information to the root perf span definition.
#[derive(Clone, Default)]
struct ConnectionPerfSpanFields {
    peer_addr: String,
    application_name: Option<String>,
    compute_mode: Option<String>,
}

#[instrument(skip_all, fields(peer_addr, application_name, compute_mode))]
#[allow(clippy::too_many_arguments)]
async fn page_service_conn_main(
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
) -> ConnectionHandlerResult {
    let _guard = LIVE_CONNECTIONS
        .with_label_values(&["page_service"])
        .guard();

    socket
        .set_nodelay(true)
        .context("could not set TCP_NODELAY")?;

    let socket_fd = socket.as_raw_fd();

    let peer_addr = socket.peer_addr().context("get peer address")?;

    let perf_span_fields = ConnectionPerfSpanFields {
        peer_addr: peer_addr.to_string(),
        application_name: None, // filled in later
        compute_mode: None,     // filled in later
    };
    tracing::Span::current().record("peer_addr", field::display(peer_addr));

    // setup read timeout of 10 minutes. the timeout is rather arbitrary for requirements:
    // - long enough for most valid compute connections
    // - less than infinite to stop us from "leaking" connections to long-gone computes
    //
    // no write timeout is used, because the kernel is assumed to error writes after some time.
    let mut socket = tokio_io_timeout::TimeoutReader::new(socket);

    let default_timeout_ms = 10 * 60 * 1000; // 10 minutes by default
    let socket_timeout_ms = (|| {
        fail::fail_point!("simulated-bad-compute-connection", |avg_timeout_ms| {
            // Exponential distribution for simulating
            // poor network conditions, expect about avg_timeout_ms to be around 15
            // in tests
            if let Some(avg_timeout_ms) = avg_timeout_ms {
                let avg = avg_timeout_ms.parse::<i64>().unwrap() as f32;
                let u = rand::random::<f32>();
                ((1.0 - u).ln() / (-avg)) as u64
            } else {
                default_timeout_ms
            }
        });
        default_timeout_ms
    })();

    // A timeout here does not mean the client died, it can happen if it's just idle for
    // a while: we will tear down this PageServerHandler and instantiate a new one if/when
    // they reconnect.
    socket.set_timeout(Some(std::time::Duration::from_millis(socket_timeout_ms)));
    let socket = Box::pin(socket);

    fail::fail_point!("ps::connection-start::pre-login");

    // XXX: pgbackend.run() should take the connection_ctx,
    // and create a child per-query context when it invokes process_query.
    // But it's in a shared crate, so, we store connection_ctx inside PageServerHandler
    // and create the per-query context in process_query ourselves.
    let mut conn_handler = PageServerHandler::new(
        tenant_manager,
        auth,
        pipelining_config,
        conf.get_vectored_concurrent_io,
        perf_span_fields,
        connection_ctx,
        cancel.clone(),
        gate_guard,
    );
    let pgbackend =
        PostgresBackend::new_from_io(socket_fd, socket, peer_addr, auth_type, tls_config)?;

    match pgbackend.run(&mut conn_handler, &cancel).await {
        Ok(()) => {
            // we've been requested to shut down
            Ok(())
        }
        Err(QueryError::Disconnected(ConnectionError::Io(io_error))) => {
            if is_expected_io_error(&io_error) {
                info!("Postgres client disconnected ({io_error})");
                Ok(())
            } else {
                let tenant_id = conn_handler.timeline_handles.as_ref().unwrap().tenant_id();
                Err(io_error).context(format!(
                    "Postgres connection error for tenant_id={tenant_id:?} client at peer_addr={peer_addr}"
                ))
            }
        }
        other => {
            let tenant_id = conn_handler.timeline_handles.as_ref().unwrap().tenant_id();
            other.context(format!(
                "Postgres query error for tenant_id={tenant_id:?} client peer_addr={peer_addr}"
            ))
        }
    }
}

/// Page service connection handler.
struct PageServerHandler {
    auth: Option<Arc<SwappableJwtAuth>>,
    claims: Option<Claims>,

    /// The context created for the lifetime of the connection
    /// services by this PageServerHandler.
    /// For each query received over the connection,
    /// `process_query` creates a child context from this one.
    connection_ctx: RequestContext,

    perf_span_fields: ConnectionPerfSpanFields,

    cancel: CancellationToken,

    /// None only while pagestream protocol is being processed.
    timeline_handles: Option<TimelineHandles>,

    pipelining_config: PageServicePipeliningConfig,
    get_vectored_concurrent_io: GetVectoredConcurrentIo,

    gate_guard: GateGuard,
}

struct TimelineHandles {
    wrapper: TenantManagerWrapper,
    /// Note on size: the typical size of this map is 1.  The largest size we expect
    /// to see is the number of shards divided by the number of pageservers (typically < 2),
    /// or the ratio used when splitting shards (i.e. how many children created from one)
    /// parent shard, where a "large" number might be ~8.
    handles: timeline::handle::Cache<TenantManagerTypes>,
}

impl TimelineHandles {
    fn new(tenant_manager: Arc<TenantManager>) -> Self {
        Self {
            wrapper: TenantManagerWrapper {
                tenant_manager,
                tenant_id: OnceCell::new(),
            },
            handles: Default::default(),
        }
    }
    async fn get(
        &mut self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
    ) -> Result<Handle<TenantManagerTypes>, GetActiveTimelineError> {
        if *self.wrapper.tenant_id.get_or_init(|| tenant_id) != tenant_id {
            return Err(GetActiveTimelineError::Tenant(
                GetActiveTenantError::SwitchedTenant,
            ));
        }
        self.handles
            .get(timeline_id, shard_selector, &self.wrapper)
            .await
            .map_err(|e| match e {
                timeline::handle::GetError::TenantManager(e) => e,
                timeline::handle::GetError::PerTimelineStateShutDown => {
                    trace!("per-timeline state shut down");
                    GetActiveTimelineError::Timeline(GetTimelineError::ShuttingDown)
                }
            })
    }

    fn tenant_id(&self) -> Option<TenantId> {
        self.wrapper.tenant_id.get().copied()
    }
}

pub(crate) struct TenantManagerWrapper {
    tenant_manager: Arc<TenantManager>,
    // We do not support switching tenant_id on a connection at this point.
    // We can can add support for this later if needed without changing
    // the protocol.
    tenant_id: once_cell::sync::OnceCell<TenantId>,
}

#[derive(Debug)]
pub(crate) struct TenantManagerTypes;

impl timeline::handle::Types for TenantManagerTypes {
    type TenantManagerError = GetActiveTimelineError;
    type TenantManager = TenantManagerWrapper;
    type Timeline = TenantManagerCacheItem;
}

pub(crate) struct TenantManagerCacheItem {
    pub(crate) timeline: Arc<Timeline>,
    // allow() for cheap propagation through RequestContext inside a task
    #[allow(clippy::redundant_allocation)]
    pub(crate) metrics: Arc<Arc<TimelineMetrics>>,
    #[allow(dead_code)] // we store it to keep the gate open
    pub(crate) gate_guard: GateGuard,
}

impl std::ops::Deref for TenantManagerCacheItem {
    type Target = Arc<Timeline>;
    fn deref(&self) -> &Self::Target {
        &self.timeline
    }
}

impl timeline::handle::Timeline<TenantManagerTypes> for TenantManagerCacheItem {
    fn shard_timeline_id(&self) -> timeline::handle::ShardTimelineId {
        Timeline::shard_timeline_id(&self.timeline)
    }

    fn per_timeline_state(&self) -> &timeline::handle::PerTimelineState<TenantManagerTypes> {
        &self.timeline.handles
    }

    fn get_shard_identity(&self) -> &pageserver_api::shard::ShardIdentity {
        Timeline::get_shard_identity(&self.timeline)
    }
}

impl timeline::handle::TenantManager<TenantManagerTypes> for TenantManagerWrapper {
    async fn resolve(
        &self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
    ) -> Result<TenantManagerCacheItem, GetActiveTimelineError> {
        let tenant_id = self.tenant_id.get().expect("we set this in get()");
        let timeout = ACTIVE_TENANT_TIMEOUT;
        let wait_start = Instant::now();
        let deadline = wait_start + timeout;
        let tenant_shard = loop {
            let resolved = self
                .tenant_manager
                .resolve_attached_shard(tenant_id, shard_selector);
            match resolved {
                ShardResolveResult::Found(tenant_shard) => break tenant_shard,
                ShardResolveResult::NotFound => {
                    return Err(GetActiveTimelineError::Tenant(
                        GetActiveTenantError::NotFound(GetTenantError::NotFound(*tenant_id)),
                    ));
                }
                ShardResolveResult::InProgress(barrier) => {
                    // We can't authoritatively answer right now: wait for InProgress state
                    // to end, then try again
                    tokio::select! {
                        _  = barrier.wait() => {
                            // The barrier completed: proceed around the loop to try looking up again
                        },
                        _ = tokio::time::sleep(deadline.duration_since(Instant::now())) => {
                            return Err(GetActiveTimelineError::Tenant(GetActiveTenantError::WaitForActiveTimeout {
                                latest_state: None,
                                wait_time: timeout,
                            }));
                        }
                    }
                }
            };
        };

        tracing::debug!("Waiting for tenant to enter active state...");
        tenant_shard
            .wait_to_become_active(deadline.duration_since(Instant::now()))
            .await
            .map_err(GetActiveTimelineError::Tenant)?;

        let timeline = tenant_shard
            .get_timeline(timeline_id, true)
            .map_err(GetActiveTimelineError::Timeline)?;

        let gate_guard = match timeline.gate.enter() {
            Ok(guard) => guard,
            Err(_) => {
                return Err(GetActiveTimelineError::Timeline(
                    GetTimelineError::ShuttingDown,
                ));
            }
        };

        let metrics = Arc::new(Arc::clone(&timeline.metrics));

        Ok(TenantManagerCacheItem {
            timeline,
            metrics,
            gate_guard,
        })
    }
}

#[derive(thiserror::Error, Debug)]
enum PageStreamError {
    /// We encountered an error that should prompt the client to reconnect:
    /// in practice this means we drop the connection without sending a response.
    #[error("Reconnect required: {0}")]
    Reconnect(Cow<'static, str>),

    /// We were instructed to shutdown while processing the query
    #[error("Shutting down")]
    Shutdown,

    /// Something went wrong reading a page: this likely indicates a pageserver bug
    #[error("Read error")]
    Read(#[source] PageReconstructError),

    /// Ran out of time waiting for an LSN
    #[error("LSN timeout: {0}")]
    LsnTimeout(WaitLsnError),

    /// The entity required to serve the request (tenant or timeline) is not found,
    /// or is not found in a suitable state to serve a request.
    #[error("Not found: {0}")]
    NotFound(Cow<'static, str>),

    /// Request asked for something that doesn't make sense, like an invalid LSN
    #[error("Bad request: {0}")]
    BadRequest(Cow<'static, str>),
}

impl From<PageStreamError> for tonic::Status {
    fn from(err: PageStreamError) -> Self {
        use tonic::Code;
        let message = err.to_string();
        let code = match err {
            PageStreamError::Reconnect(_) => Code::Unavailable,
            PageStreamError::Shutdown => Code::Unavailable,
            PageStreamError::Read(err) => match err {
                PageReconstructError::Cancelled => Code::Unavailable,
                PageReconstructError::MissingKey(_) => Code::NotFound,
                PageReconstructError::AncestorLsnTimeout(err) => tonic::Status::from(err).code(),
                PageReconstructError::Other(_) => Code::Internal,
                PageReconstructError::WalRedo(_) => Code::Internal,
            },
            PageStreamError::LsnTimeout(err) => tonic::Status::from(err).code(),
            PageStreamError::NotFound(_) => Code::NotFound,
            PageStreamError::BadRequest(_) => Code::InvalidArgument,
        };
        tonic::Status::new(code, message)
    }
}

impl From<PageReconstructError> for PageStreamError {
    fn from(value: PageReconstructError) -> Self {
        match value {
            PageReconstructError::Cancelled => Self::Shutdown,
            e => Self::Read(e),
        }
    }
}

impl From<GetActiveTimelineError> for PageStreamError {
    fn from(value: GetActiveTimelineError) -> Self {
        match value {
            GetActiveTimelineError::Tenant(GetActiveTenantError::Cancelled)
            | GetActiveTimelineError::Tenant(GetActiveTenantError::WillNotBecomeActive(
                TenantState::Stopping { .. },
            ))
            | GetActiveTimelineError::Timeline(GetTimelineError::ShuttingDown) => Self::Shutdown,
            GetActiveTimelineError::Tenant(e) => Self::NotFound(format!("{e}").into()),
            GetActiveTimelineError::Timeline(e) => Self::NotFound(format!("{e}").into()),
        }
    }
}

impl From<WaitLsnError> for PageStreamError {
    fn from(value: WaitLsnError) -> Self {
        match value {
            e @ WaitLsnError::Timeout(_) => Self::LsnTimeout(e),
            WaitLsnError::Shutdown => Self::Shutdown,
            e @ WaitLsnError::BadState { .. } => Self::Reconnect(format!("{e}").into()),
        }
    }
}

impl From<WaitLsnError> for QueryError {
    fn from(value: WaitLsnError) -> Self {
        match value {
            e @ WaitLsnError::Timeout(_) => Self::Other(anyhow::Error::new(e)),
            WaitLsnError::Shutdown => Self::Shutdown,
            WaitLsnError::BadState { .. } => Self::Reconnect,
        }
    }
}

#[derive(thiserror::Error, Debug)]
struct BatchedPageStreamError {
    req: PagestreamRequest,
    err: PageStreamError,
}

impl std::fmt::Display for BatchedPageStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.err.fmt(f)
    }
}

struct BatchedGetPageRequest {
    req: PagestreamGetPageRequest,
    timer: SmgrOpTimer,
    lsn_range: LsnRange,
    ctx: RequestContext,
    // If the request is perf enabled, this contains a context
    // with a perf span tracking the time spent waiting for the executor.
    batch_wait_ctx: Option<RequestContext>,
}

#[cfg(feature = "testing")]
struct BatchedTestRequest {
    req: pagestream_api::PagestreamTestRequest,
    timer: SmgrOpTimer,
}

/// NB: we only hold [`timeline::handle::WeakHandle`] inside this enum,
/// so that we don't keep the [`Timeline::gate`] open while the batch
/// is being built up inside the [`spsc_fold`] (pagestream pipelining).
#[derive(IntoStaticStr)]
#[allow(clippy::large_enum_variant)]
enum BatchedFeMessage {
    Exists {
        span: Span,
        timer: SmgrOpTimer,
        shard: WeakHandle<TenantManagerTypes>,
        req: PagestreamExistsRequest,
    },
    Nblocks {
        span: Span,
        timer: SmgrOpTimer,
        shard: WeakHandle<TenantManagerTypes>,
        req: PagestreamNblocksRequest,
    },
    GetPage {
        span: Span,
        shard: WeakHandle<TenantManagerTypes>,
        pages: SmallVec<[BatchedGetPageRequest; 1]>,
        batch_break_reason: GetPageBatchBreakReason,
    },
    DbSize {
        span: Span,
        timer: SmgrOpTimer,
        shard: WeakHandle<TenantManagerTypes>,
        req: PagestreamDbSizeRequest,
    },
    GetSlruSegment {
        span: Span,
        timer: SmgrOpTimer,
        shard: WeakHandle<TenantManagerTypes>,
        req: PagestreamGetSlruSegmentRequest,
    },
    #[cfg(feature = "testing")]
    Test {
        span: Span,
        shard: WeakHandle<TenantManagerTypes>,
        requests: Vec<BatchedTestRequest>,
    },
    RespondError {
        span: Span,
        error: BatchedPageStreamError,
    },
}

impl BatchedFeMessage {
    fn as_static_str(&self) -> &'static str {
        self.into()
    }

    fn observe_execution_start(&mut self, at: Instant) {
        match self {
            BatchedFeMessage::Exists { timer, .. }
            | BatchedFeMessage::Nblocks { timer, .. }
            | BatchedFeMessage::DbSize { timer, .. }
            | BatchedFeMessage::GetSlruSegment { timer, .. } => {
                timer.observe_execution_start(at);
            }
            BatchedFeMessage::GetPage { pages, .. } => {
                for page in pages {
                    page.timer.observe_execution_start(at);
                }
            }
            #[cfg(feature = "testing")]
            BatchedFeMessage::Test { requests, .. } => {
                for req in requests {
                    req.timer.observe_execution_start(at);
                }
            }
            BatchedFeMessage::RespondError { .. } => {}
        }
    }

    fn should_break_batch(
        &self,
        other: &BatchedFeMessage,
        max_batch_size: NonZeroUsize,
        batching_strategy: PageServiceProtocolPipelinedBatchingStrategy,
    ) -> Option<GetPageBatchBreakReason> {
        match (self, other) {
            (
                BatchedFeMessage::GetPage {
                    shard: accum_shard,
                    pages: accum_pages,
                    ..
                },
                BatchedFeMessage::GetPage {
                    shard: this_shard,
                    pages: this_pages,
                    ..
                },
            ) => {
                assert_eq!(this_pages.len(), 1);
                if accum_pages.len() >= max_batch_size.get() {
                    trace!(%max_batch_size, "stopping batching because of batch size");
                    assert_eq!(accum_pages.len(), max_batch_size.get());

                    return Some(GetPageBatchBreakReason::BatchFull);
                }
                if !accum_shard.is_same_handle_as(this_shard) {
                    trace!("stopping batching because timeline object mismatch");
                    // TODO: we _could_ batch & execute each shard seperately (and in parallel).
                    // But the current logic for keeping responses in order does not support that.

                    return Some(GetPageBatchBreakReason::NonUniformTimeline);
                }

                match batching_strategy {
                    PageServiceProtocolPipelinedBatchingStrategy::UniformLsn => {
                        if let Some(last_in_batch) = accum_pages.last() {
                            if last_in_batch.lsn_range.effective_lsn
                                != this_pages[0].lsn_range.effective_lsn
                            {
                                trace!(
                                    accum_lsn = %last_in_batch.lsn_range.effective_lsn,
                                    this_lsn = %this_pages[0].lsn_range.effective_lsn,
                                    "stopping batching because LSN changed"
                                );

                                return Some(GetPageBatchBreakReason::NonUniformLsn);
                            }
                        }
                    }
                    PageServiceProtocolPipelinedBatchingStrategy::ScatteredLsn => {
                        // The read path doesn't curently support serving the same page at different LSNs.
                        // While technically possible, it's uncertain if the complexity is worth it.
                        // Break the batch if such a case is encountered.
                        let same_page_different_lsn = accum_pages.iter().any(|batched| {
                            batched.req.rel == this_pages[0].req.rel
                                && batched.req.blkno == this_pages[0].req.blkno
                                && batched.lsn_range.effective_lsn
                                    != this_pages[0].lsn_range.effective_lsn
                        });

                        if same_page_different_lsn {
                            trace!(
                                rel=%this_pages[0].req.rel,
                                blkno=%this_pages[0].req.blkno,
                                lsn=%this_pages[0].lsn_range.effective_lsn,
                                "stopping batching because same page was requested at different LSNs"
                            );

                            return Some(GetPageBatchBreakReason::SamePageAtDifferentLsn);
                        }
                    }
                }

                None
            }
            #[cfg(feature = "testing")]
            (
                BatchedFeMessage::Test {
                    shard: accum_shard,
                    requests: accum_requests,
                    ..
                },
                BatchedFeMessage::Test {
                    shard: this_shard,
                    requests: this_requests,
                    ..
                },
            ) => {
                assert!(this_requests.len() == 1);
                if accum_requests.len() >= max_batch_size.get() {
                    trace!(%max_batch_size, "stopping batching because of batch size");
                    assert_eq!(accum_requests.len(), max_batch_size.get());
                    return Some(GetPageBatchBreakReason::BatchFull);
                }
                if !accum_shard.is_same_handle_as(this_shard) {
                    trace!("stopping batching because timeline object mismatch");
                    // TODO: we _could_ batch & execute each shard seperately (and in parallel).
                    // But the current logic for keeping responses in order does not support that.
                    return Some(GetPageBatchBreakReason::NonUniformTimeline);
                }
                let this_batch_key = this_requests[0].req.batch_key;
                let accum_batch_key = accum_requests[0].req.batch_key;
                if this_requests[0].req.batch_key != accum_requests[0].req.batch_key {
                    trace!(%accum_batch_key, %this_batch_key, "stopping batching because batch key changed");
                    return Some(GetPageBatchBreakReason::NonUniformKey);
                }
                None
            }
            (_, _) => Some(GetPageBatchBreakReason::NonBatchableRequest),
        }
    }
}

impl PageServerHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_manager: Arc<TenantManager>,
        auth: Option<Arc<SwappableJwtAuth>>,
        pipelining_config: PageServicePipeliningConfig,
        get_vectored_concurrent_io: GetVectoredConcurrentIo,
        perf_span_fields: ConnectionPerfSpanFields,
        connection_ctx: RequestContext,
        cancel: CancellationToken,
        gate_guard: GateGuard,
    ) -> Self {
        PageServerHandler {
            auth,
            claims: None,
            connection_ctx,
            perf_span_fields,
            timeline_handles: Some(TimelineHandles::new(tenant_manager)),
            cancel,
            pipelining_config,
            get_vectored_concurrent_io,
            gate_guard,
        }
    }

    /// This function always respects cancellation of any timeline in `[Self::shard_timelines]`.  Pass in
    /// a cancellation token at the next scope up (such as a tenant cancellation token) to ensure we respect
    /// cancellation if there aren't any timelines in the cache.
    ///
    /// If calling from a function that doesn't use the `[Self::shard_timelines]` cache, then pass in the
    /// timeline cancellation token.
    async fn flush_cancellable<IO>(
        &self,
        pgb: &mut PostgresBackend<IO>,
        cancel: &CancellationToken,
    ) -> Result<(), QueryError>
    where
        IO: AsyncRead + AsyncWrite + Send + Sync + Unpin,
    {
        tokio::select!(
            flush_r = pgb.flush() => {
                Ok(flush_r?)
            },
            _ = cancel.cancelled() => {
                Err(QueryError::Shutdown)
            }
        )
    }

    #[allow(clippy::too_many_arguments)]
    async fn pagestream_read_message<IO>(
        pgb: &mut PostgresBackendReader<IO>,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        timeline_handles: &mut TimelineHandles,
        conn_perf_span_fields: &ConnectionPerfSpanFields,
        cancel: &CancellationToken,
        ctx: &RequestContext,
        protocol_version: PagestreamProtocolVersion,
        parent_span: Span,
    ) -> Result<Option<BatchedFeMessage>, QueryError>
    where
        IO: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    {
        let msg = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return Err(QueryError::Shutdown)
            }
            msg = pgb.read_message() => { msg }
        };

        let received_at = Instant::now();

        let copy_data_bytes = match msg? {
            Some(FeMessage::CopyData(bytes)) => bytes,
            Some(FeMessage::Terminate) => {
                return Ok(None);
            }
            Some(m) => {
                return Err(QueryError::Other(anyhow::anyhow!(
                    "unexpected message: {m:?} during COPY"
                )));
            }
            None => {
                return Ok(None);
            } // client disconnected
        };
        trace!("query: {copy_data_bytes:?}");

        fail::fail_point!("ps::handle-pagerequest-message");

        // parse request
        let neon_fe_msg =
            PagestreamFeMessage::parse(&mut copy_data_bytes.reader(), protocol_version)?;

        let batched_msg = match neon_fe_msg {
            PagestreamFeMessage::Exists(req) => {
                let shard = timeline_handles
                    .get(tenant_id, timeline_id, ShardSelector::Zero)
                    .await?;
                debug_assert_current_span_has_tenant_and_timeline_id_no_shard_id();
                let span = tracing::info_span!(parent: &parent_span, "handle_get_rel_exists_request", rel = %req.rel, req_lsn = %req.hdr.request_lsn, shard_id = %shard.tenant_shard_id.shard_slug());
                let timer = Self::record_op_start_and_throttle(
                    &shard,
                    metrics::SmgrQueryType::GetRelExists,
                    received_at,
                )
                .await?;
                BatchedFeMessage::Exists {
                    span,
                    timer,
                    shard: shard.downgrade(),
                    req,
                }
            }
            PagestreamFeMessage::Nblocks(req) => {
                let shard = timeline_handles
                    .get(tenant_id, timeline_id, ShardSelector::Zero)
                    .await?;
                let span = tracing::info_span!(parent: &parent_span, "handle_get_nblocks_request", rel = %req.rel, req_lsn = %req.hdr.request_lsn, shard_id = %shard.tenant_shard_id.shard_slug());
                let timer = Self::record_op_start_and_throttle(
                    &shard,
                    metrics::SmgrQueryType::GetRelSize,
                    received_at,
                )
                .await?;
                BatchedFeMessage::Nblocks {
                    span,
                    timer,
                    shard: shard.downgrade(),
                    req,
                }
            }
            PagestreamFeMessage::DbSize(req) => {
                let shard = timeline_handles
                    .get(tenant_id, timeline_id, ShardSelector::Zero)
                    .await?;
                let span = tracing::info_span!(parent: &parent_span, "handle_db_size_request", dbnode = %req.dbnode, req_lsn = %req.hdr.request_lsn, shard_id = %shard.tenant_shard_id.shard_slug());
                let timer = Self::record_op_start_and_throttle(
                    &shard,
                    metrics::SmgrQueryType::GetDbSize,
                    received_at,
                )
                .await?;
                BatchedFeMessage::DbSize {
                    span,
                    timer,
                    shard: shard.downgrade(),
                    req,
                }
            }
            PagestreamFeMessage::GetSlruSegment(req) => {
                let shard = timeline_handles
                    .get(tenant_id, timeline_id, ShardSelector::Zero)
                    .await?;
                let span = tracing::info_span!(parent: &parent_span, "handle_get_slru_segment_request", kind = %req.kind, segno = %req.segno, req_lsn = %req.hdr.request_lsn, shard_id = %shard.tenant_shard_id.shard_slug());
                let timer = Self::record_op_start_and_throttle(
                    &shard,
                    metrics::SmgrQueryType::GetSlruSegment,
                    received_at,
                )
                .await?;
                BatchedFeMessage::GetSlruSegment {
                    span,
                    timer,
                    shard: shard.downgrade(),
                    req,
                }
            }
            PagestreamFeMessage::GetPage(req) => {
                // avoid a somewhat costly Span::record() by constructing the entire span in one go.
                macro_rules! mkspan {
                    (before shard routing) => {{
                        tracing::info_span!(
                            parent: &parent_span,
                            "handle_get_page_request",
                            request_id = %req.hdr.reqid,
                            rel = %req.rel,
                            blkno = %req.blkno,
                            req_lsn = %req.hdr.request_lsn,
                            not_modified_since_lsn = %req.hdr.not_modified_since,
                        )
                    }};
                    ($shard_id:expr) => {{
                        tracing::info_span!(
                            parent: &parent_span,
                            "handle_get_page_request",
                            request_id = %req.hdr.reqid,
                            rel = %req.rel,
                            blkno = %req.blkno,
                            req_lsn = %req.hdr.request_lsn,
                            not_modified_since_lsn = %req.hdr.not_modified_since,
                            shard_id = %$shard_id,
                        )
                    }};
                }

                macro_rules! respond_error {
                    ($span:expr, $error:expr) => {{
                        let error = BatchedFeMessage::RespondError {
                            span: $span,
                            error: BatchedPageStreamError {
                                req: req.hdr,
                                err: $error,
                            },
                        };
                        Ok(Some(error))
                    }};
                }

                let key = rel_block_to_key(req.rel, req.blkno);

                let res = timeline_handles
                    .get(tenant_id, timeline_id, ShardSelector::Page(key))
                    .await;

                let shard = match res {
                    Ok(tl) => tl,
                    Err(e) => {
                        let span = mkspan!(before shard routing);
                        match e {
                            GetActiveTimelineError::Tenant(GetActiveTenantError::NotFound(_)) => {
                                // We already know this tenant exists in general, because we resolved it at
                                // start of connection.  Getting a NotFound here indicates that the shard containing
                                // the requested page is not present on this node: the client's knowledge of shard->pageserver
                                // mapping is out of date.
                                //
                                // Closing the connection by returning ``::Reconnect` has the side effect of rate-limiting above message, via
                                // client's reconnect backoff, as well as hopefully prompting the client to load its updated configuration
                                // and talk to a different pageserver.
                                MISROUTED_PAGESTREAM_REQUESTS.inc();
                                return respond_error!(
                                    span,
                                    PageStreamError::Reconnect(
                                        "getpage@lsn request routed to wrong shard".into()
                                    )
                                );
                            }
                            e => {
                                return respond_error!(span, e.into());
                            }
                        }
                    }
                };

                let ctx = if shard.is_get_page_request_sampled() {
                    RequestContextBuilder::from(ctx)
                        .root_perf_span(|| {
                            info_span!(
                            target: PERF_TRACE_TARGET,
                            "GET_PAGE",
                            peer_addr = conn_perf_span_fields.peer_addr,
                            application_name = conn_perf_span_fields.application_name,
                            compute_mode = conn_perf_span_fields.compute_mode,
                            tenant_id = %tenant_id,
                            shard_id = %shard.get_shard_identity().shard_slug(),
                            timeline_id = %timeline_id,
                            lsn = %req.hdr.request_lsn,
                            not_modified_since_lsn = %req.hdr.not_modified_since,
                            request_id = %req.hdr.reqid,
                            key = %key,
                            )
                        })
                        .attached_child()
                } else {
                    ctx.attached_child()
                };

                // This ctx travels as part of the BatchedFeMessage through
                // batching into the request handler.
                // The request handler needs to do some per-request work
                // (relsize check) before dispatching the batch as a single
                // get_vectored call to the Timeline.
                // This ctx will be used for the reslize check, whereas the
                // get_vectored call will be a different ctx with separate
                // perf span.
                let ctx = ctx.with_scope_page_service_pagestream(&shard);

                // Similar game for this `span`: we funnel it through so that
                // request handler log messages contain the request-specific fields.
                let span = mkspan!(shard.tenant_shard_id.shard_slug());

                let timer = Self::record_op_start_and_throttle(
                    &shard,
                    metrics::SmgrQueryType::GetPageAtLsn,
                    received_at,
                )
                .maybe_perf_instrument(&ctx, |current_perf_span| {
                    info_span!(
                        target: PERF_TRACE_TARGET,
                        parent: current_perf_span,
                        "THROTTLE",
                    )
                })
                .await?;

                // We're holding the Handle
                let effective_lsn = match Self::effective_request_lsn(
                    &shard,
                    shard.get_last_record_lsn(),
                    req.hdr.request_lsn,
                    req.hdr.not_modified_since,
                    &shard.get_applied_gc_cutoff_lsn(),
                ) {
                    Ok(lsn) => lsn,
                    Err(e) => {
                        return respond_error!(span, e);
                    }
                };

                let batch_wait_ctx = if ctx.has_perf_span() {
                    Some(
                        RequestContextBuilder::from(&ctx)
                            .perf_span(|crnt_perf_span| {
                                info_span!(
                                    target: PERF_TRACE_TARGET,
                                    parent: crnt_perf_span,
                                    "WAIT_EXECUTOR",
                                )
                            })
                            .attached_child(),
                    )
                } else {
                    None
                };

                BatchedFeMessage::GetPage {
                    span,
                    shard: shard.downgrade(),
                    pages: smallvec![BatchedGetPageRequest {
                        req,
                        timer,
                        lsn_range: LsnRange {
                            effective_lsn,
                            request_lsn: req.hdr.request_lsn
                        },
                        ctx,
                        batch_wait_ctx,
                    }],
                    // The executor grabs the batch when it becomes idle.
                    // Hence, [`GetPageBatchBreakReason::ExecutorSteal`] is the
                    // default reason for breaking the batch.
                    batch_break_reason: GetPageBatchBreakReason::ExecutorSteal,
                }
            }
            #[cfg(feature = "testing")]
            PagestreamFeMessage::Test(req) => {
                let shard = timeline_handles
                    .get(tenant_id, timeline_id, ShardSelector::Zero)
                    .await?;
                let span = tracing::info_span!(parent: &parent_span, "handle_test_request", shard_id = %shard.tenant_shard_id.shard_slug());
                let timer = Self::record_op_start_and_throttle(
                    &shard,
                    metrics::SmgrQueryType::Test,
                    received_at,
                )
                .await?;
                BatchedFeMessage::Test {
                    span,
                    shard: shard.downgrade(),
                    requests: vec![BatchedTestRequest { req, timer }],
                }
            }
        };
        Ok(Some(batched_msg))
    }

    /// Starts a SmgrOpTimer at received_at and throttles the request.
    async fn record_op_start_and_throttle(
        shard: &Handle<TenantManagerTypes>,
        op: metrics::SmgrQueryType,
        received_at: Instant,
    ) -> Result<SmgrOpTimer, QueryError> {
        // It's important to start the smgr op metric recorder as early as possible
        // so that the _started counters are incremented before we do
        // any serious waiting, e.g., for throttle, batching, or actual request handling.
        let mut timer = shard.query_metrics.start_smgr_op(op, received_at);
        let now = Instant::now();
        timer.observe_throttle_start(now);
        let throttled = tokio::select! {
            res = shard.pagestream_throttle.throttle(1, now) => res,
            _ = shard.cancel.cancelled() => return Err(QueryError::Shutdown),
        };
        timer.observe_throttle_done(throttled);
        Ok(timer)
    }

    /// Post-condition: `batch` is Some()
    #[instrument(skip_all, level = tracing::Level::TRACE)]
    #[allow(clippy::boxed_local)]
    fn pagestream_do_batch(
        batching_strategy: PageServiceProtocolPipelinedBatchingStrategy,
        max_batch_size: NonZeroUsize,
        batch: &mut Result<BatchedFeMessage, QueryError>,
        this_msg: Result<BatchedFeMessage, QueryError>,
    ) -> Result<(), Result<BatchedFeMessage, QueryError>> {
        debug_assert_current_span_has_tenant_and_timeline_id_no_shard_id();

        let this_msg = match this_msg {
            Ok(this_msg) => this_msg,
            Err(e) => return Err(Err(e)),
        };

        let eligible_batch = match batch {
            Ok(b) => b,
            Err(_) => {
                return Err(Ok(this_msg));
            }
        };

        let batch_break =
            eligible_batch.should_break_batch(&this_msg, max_batch_size, batching_strategy);

        match batch_break {
            Some(reason) => {
                if let BatchedFeMessage::GetPage {
                    batch_break_reason, ..
                } = eligible_batch
                {
                    *batch_break_reason = reason;
                }

                Err(Ok(this_msg))
            }
            None => {
                // ok to batch
                match (eligible_batch, this_msg) {
                    (
                        BatchedFeMessage::GetPage {
                            pages: accum_pages, ..
                        },
                        BatchedFeMessage::GetPage {
                            pages: this_pages, ..
                        },
                    ) => {
                        accum_pages.extend(this_pages);
                        Ok(())
                    }
                    #[cfg(feature = "testing")]
                    (
                        BatchedFeMessage::Test {
                            requests: accum_requests,
                            ..
                        },
                        BatchedFeMessage::Test {
                            requests: this_requests,
                            ..
                        },
                    ) => {
                        accum_requests.extend(this_requests);
                        Ok(())
                    }
                    // Shape guaranteed by [`BatchedFeMessage::should_break_batch`]
                    _ => unreachable!(),
                }
            }
        }
    }

    #[instrument(level = tracing::Level::DEBUG, skip_all)]
    async fn pagestream_handle_batched_message<IO>(
        &mut self,
        pgb_writer: &mut PostgresBackend<IO>,
        batch: BatchedFeMessage,
        io_concurrency: IoConcurrency,
        cancel: &CancellationToken,
        protocol_version: PagestreamProtocolVersion,
        ctx: &RequestContext,
    ) -> Result<(), QueryError>
    where
        IO: AsyncRead + AsyncWrite + Send + Sync + Unpin,
    {
        let started_at = Instant::now();
        let batch = {
            let mut batch = batch;
            batch.observe_execution_start(started_at);
            batch
        };

        // Dispatch the batch to the appropriate request handler.
        let log_slow_name = batch.as_static_str();
        let (mut handler_results, span) = {
            // TODO: we unfortunately have to pin the future on the heap, since GetPage futures are huge and
            // won't fit on the stack.
            let mut boxpinned = Box::pin(Self::pagestream_dispatch_batched_message(
                batch,
                io_concurrency,
                ctx,
            ));
            log_slow(
                log_slow_name,
                LOG_SLOW_GETPAGE_THRESHOLD,
                boxpinned.as_mut(),
            )
            .await?
        };

        // We purposefully don't count flush time into the smgr operation timer.
        //
        // The reason is that current compute client will not perform protocol processing
        // if the postgres backend process is doing things other than `->smgr_read()`.
        // This is especially the case for prefetch.
        //
        // If the compute doesn't read from the connection, eventually TCP will backpressure
        // all the way into our flush call below.
        //
        // The timer's underlying metric is used for a storage-internal latency SLO and
        // we don't want to include latency in it that we can't control.
        // And as pointed out above, in this case, we don't control the time that flush will take.
        //
        // We put each response in the batch onto the wire in a separate pgb_writer.flush()
        // call, which (all unmeasured) adds syscall overhead but reduces time to first byte
        // and avoids building up a "giant" contiguous userspace buffer to hold the entire response.
        // TODO: vectored socket IO would be great, but pgb_writer doesn't support that.
        let flush_timers = {
            let flushing_start_time = Instant::now();
            let mut flush_timers = Vec::with_capacity(handler_results.len());
            for handler_result in &mut handler_results {
                let flush_timer = match handler_result {
                    Ok((_response, timer, _ctx)) => Some(
                        timer
                            .observe_execution_end(flushing_start_time)
                            .expect("we are the first caller"),
                    ),
                    Err(_) => {
                        // TODO: measure errors
                        None
                    }
                };
                flush_timers.push(flush_timer);
            }
            assert_eq!(flush_timers.len(), handler_results.len());
            flush_timers
        };

        // Map handler result to protocol behavior.
        // Some handler errors cause exit from pagestream protocol.
        // Other handler errors are sent back as an error message and we stay in pagestream protocol.
        for (handler_result, flushing_timer) in handler_results.into_iter().zip(flush_timers) {
            let (response_msg, ctx) = match handler_result {
                Err(e) => match &e.err {
                    PageStreamError::Shutdown => {
                        // If we fail to fulfil a request during shutdown, which may be _because_ of
                        // shutdown, then do not send the error to the client.  Instead just drop the
                        // connection.
                        span.in_scope(|| info!("dropping connection due to shutdown"));
                        return Err(QueryError::Shutdown);
                    }
                    PageStreamError::Reconnect(reason) => {
                        span.in_scope(|| info!("handler requested reconnect: {reason}"));
                        return Err(QueryError::Reconnect);
                    }
                    PageStreamError::Read(_)
                    | PageStreamError::LsnTimeout(_)
                    | PageStreamError::NotFound(_)
                    | PageStreamError::BadRequest(_) => {
                        // print the all details to the log with {:#}, but for the client the
                        // error message is enough.  Do not log if shutting down, as the anyhow::Error
                        // here includes cancellation which is not an error.
                        let full = utils::error::report_compact_sources(&e.err);
                        span.in_scope(|| {
                            error!("error reading relation or page version: {full:#}")
                        });

                        (
                            PagestreamBeMessage::Error(PagestreamErrorResponse {
                                req: e.req,
                                message: e.err.to_string(),
                            }),
                            None,
                        )
                    }
                },
                Ok((response_msg, _op_timer_already_observed, ctx)) => (response_msg, Some(ctx)),
            };

            let ctx = ctx.map(|req_ctx| {
                RequestContextBuilder::from(&req_ctx)
                    .perf_span(|crnt_perf_span| {
                        info_span!(
                            target: PERF_TRACE_TARGET,
                            parent: crnt_perf_span,
                            "FLUSH_RESPONSE",
                        )
                    })
                    .attached_child()
            });

            //
            // marshal & transmit response message
            //

            pgb_writer.write_message_noflush(&BeMessage::CopyData(
                &response_msg.serialize(protocol_version),
            ))?;

            failpoint_support::sleep_millis_async!("before-pagestream-msg-flush", cancel);

            // what we want to do
            let socket_fd = pgb_writer.socket_fd;
            let flush_fut = pgb_writer.flush();
            // metric for how long flushing takes
            let flush_fut = match flushing_timer {
                Some(flushing_timer) => futures::future::Either::Left(flushing_timer.measure(
                    Instant::now(),
                    flush_fut,
                    socket_fd,
                )),
                None => futures::future::Either::Right(flush_fut),
            };

            let flush_fut = if let Some(req_ctx) = ctx.as_ref() {
                futures::future::Either::Left(
                    flush_fut.maybe_perf_instrument(req_ctx, |current_perf_span| {
                        current_perf_span.clone()
                    }),
                )
            } else {
                futures::future::Either::Right(flush_fut)
            };

            // do it while respecting cancellation
            let _: () = async move {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        // We were requested to shut down.
                        info!("shutdown request received in page handler");
                        return Err(QueryError::Shutdown)
                    }
                    res = flush_fut => {
                        res?;
                    }
                }
                Ok(())
            }
            .await?;
        }
        Ok(())
    }

    /// Helper which dispatches a batched message to the appropriate handler.
    /// Returns a vec of results, along with the extracted trace span.
    async fn pagestream_dispatch_batched_message(
        batch: BatchedFeMessage,
        io_concurrency: IoConcurrency,
        ctx: &RequestContext,
    ) -> Result<
        (
            Vec<Result<(PagestreamBeMessage, SmgrOpTimer, RequestContext), BatchedPageStreamError>>,
            Span,
        ),
        QueryError,
    > {
        macro_rules! upgrade_handle_and_set_context {
            ($shard:ident) => {{
                let weak_handle = &$shard;
                let handle = weak_handle.upgrade()?;
                let ctx = ctx.with_scope_page_service_pagestream(&handle);
                (handle, ctx)
            }};
        }
        Ok(match batch {
            BatchedFeMessage::Exists {
                span,
                timer,
                shard,
                req,
            } => {
                fail::fail_point!("ps::handle-pagerequest-message::exists");
                let (shard, ctx) = upgrade_handle_and_set_context!(shard);
                (
                    vec![
                        Self::handle_get_rel_exists_request(&shard, &req, &ctx)
                            .instrument(span.clone())
                            .await
                            .map(|msg| (PagestreamBeMessage::Exists(msg), timer, ctx))
                            .map_err(|err| BatchedPageStreamError { err, req: req.hdr }),
                    ],
                    span,
                )
            }
            BatchedFeMessage::Nblocks {
                span,
                timer,
                shard,
                req,
            } => {
                fail::fail_point!("ps::handle-pagerequest-message::nblocks");
                let (shard, ctx) = upgrade_handle_and_set_context!(shard);
                (
                    vec![
                        Self::handle_get_nblocks_request(&shard, &req, &ctx)
                            .instrument(span.clone())
                            .await
                            .map(|msg| (PagestreamBeMessage::Nblocks(msg), timer, ctx))
                            .map_err(|err| BatchedPageStreamError { err, req: req.hdr }),
                    ],
                    span,
                )
            }
            BatchedFeMessage::GetPage {
                span,
                shard,
                pages,
                batch_break_reason,
            } => {
                fail::fail_point!("ps::handle-pagerequest-message::getpage");
                let (shard, ctx) = upgrade_handle_and_set_context!(shard);
                (
                    {
                        let npages = pages.len();
                        trace!(npages, "handling getpage request");
                        let res = Self::handle_get_page_at_lsn_request_batched(
                            &shard,
                            pages,
                            io_concurrency,
                            batch_break_reason,
                            &ctx,
                        )
                        .instrument(span.clone())
                        .await;
                        assert_eq!(res.len(), npages);
                        res
                    },
                    span,
                )
            }
            BatchedFeMessage::DbSize {
                span,
                timer,
                shard,
                req,
            } => {
                fail::fail_point!("ps::handle-pagerequest-message::dbsize");
                let (shard, ctx) = upgrade_handle_and_set_context!(shard);
                (
                    vec![
                        Self::handle_db_size_request(&shard, &req, &ctx)
                            .instrument(span.clone())
                            .await
                            .map(|msg| (PagestreamBeMessage::DbSize(msg), timer, ctx))
                            .map_err(|err| BatchedPageStreamError { err, req: req.hdr }),
                    ],
                    span,
                )
            }
            BatchedFeMessage::GetSlruSegment {
                span,
                timer,
                shard,
                req,
            } => {
                fail::fail_point!("ps::handle-pagerequest-message::slrusegment");
                let (shard, ctx) = upgrade_handle_and_set_context!(shard);
                (
                    vec![
                        Self::handle_get_slru_segment_request(&shard, &req, &ctx)
                            .instrument(span.clone())
                            .await
                            .map(|msg| (PagestreamBeMessage::GetSlruSegment(msg), timer, ctx))
                            .map_err(|err| BatchedPageStreamError { err, req: req.hdr }),
                    ],
                    span,
                )
            }
            #[cfg(feature = "testing")]
            BatchedFeMessage::Test {
                span,
                shard,
                requests,
            } => {
                fail::fail_point!("ps::handle-pagerequest-message::test");
                let (shard, ctx) = upgrade_handle_and_set_context!(shard);
                (
                    {
                        let npages = requests.len();
                        trace!(npages, "handling getpage request");
                        let res = Self::handle_test_request_batch(&shard, requests, &ctx)
                            .instrument(span.clone())
                            .await;
                        assert_eq!(res.len(), npages);
                        res
                    },
                    span,
                )
            }
            BatchedFeMessage::RespondError { span, error } => {
                // We've already decided to respond with an error, so we don't need to
                // call the handler.
                (vec![Err(error)], span)
            }
        })
    }

    /// Pagestream sub-protocol handler.
    ///
    /// It is a simple request-response protocol inside a COPYBOTH session.
    ///
    /// # Coding Discipline
    ///
    /// Coding discipline within this function: all interaction with the `pgb` connection
    /// needs to be sensitive to connection shutdown, currently signalled via [`Self::cancel`].
    /// This is so that we can shutdown page_service quickly.
    #[instrument(skip_all)]
    async fn handle_pagerequests<IO>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        protocol_version: PagestreamProtocolVersion,
        ctx: RequestContext,
    ) -> Result<(), QueryError>
    where
        IO: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    {
        debug_assert_current_span_has_tenant_and_timeline_id_no_shard_id();

        // switch client to COPYBOTH
        pgb.write_message_noflush(&BeMessage::CopyBothResponse)?;
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => {
                return Err(QueryError::Shutdown)
            }
            res = pgb.flush() => {
                res?;
            }
        }

        let io_concurrency = IoConcurrency::spawn_from_conf(
            self.get_vectored_concurrent_io,
            match self.gate_guard.try_clone() {
                Ok(guard) => guard,
                Err(_) => {
                    info!("shutdown request received in page handler");
                    return Err(QueryError::Shutdown);
                }
            },
        );

        let pgb_reader = pgb
            .split()
            .context("implementation error: split pgb into reader and writer")?;

        let timeline_handles = self
            .timeline_handles
            .take()
            .expect("implementation error: timeline_handles should not be locked");

        let request_span = info_span!("request");
        let ((pgb_reader, timeline_handles), result) = match self.pipelining_config.clone() {
            PageServicePipeliningConfig::Pipelined(pipelining_config) => {
                self.handle_pagerequests_pipelined(
                    pgb,
                    pgb_reader,
                    tenant_id,
                    timeline_id,
                    timeline_handles,
                    request_span,
                    pipelining_config,
                    protocol_version,
                    io_concurrency,
                    &ctx,
                )
                .await
            }
            PageServicePipeliningConfig::Serial => {
                self.handle_pagerequests_serial(
                    pgb,
                    pgb_reader,
                    tenant_id,
                    timeline_id,
                    timeline_handles,
                    request_span,
                    protocol_version,
                    io_concurrency,
                    &ctx,
                )
                .await
            }
        };

        debug!("pagestream subprotocol shut down cleanly");

        pgb.unsplit(pgb_reader)
            .context("implementation error: unsplit pgb")?;

        let replaced = self.timeline_handles.replace(timeline_handles);
        assert!(replaced.is_none());

        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_pagerequests_serial<IO>(
        &mut self,
        pgb_writer: &mut PostgresBackend<IO>,
        mut pgb_reader: PostgresBackendReader<IO>,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        mut timeline_handles: TimelineHandles,
        request_span: Span,
        protocol_version: PagestreamProtocolVersion,
        io_concurrency: IoConcurrency,
        ctx: &RequestContext,
    ) -> (
        (PostgresBackendReader<IO>, TimelineHandles),
        Result<(), QueryError>,
    )
    where
        IO: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    {
        let cancel = self.cancel.clone();

        let err = loop {
            let msg = Self::pagestream_read_message(
                &mut pgb_reader,
                tenant_id,
                timeline_id,
                &mut timeline_handles,
                &self.perf_span_fields,
                &cancel,
                ctx,
                protocol_version,
                request_span.clone(),
            )
            .await;
            let msg = match msg {
                Ok(msg) => msg,
                Err(e) => break e,
            };
            let msg = match msg {
                Some(msg) => msg,
                None => {
                    debug!("pagestream subprotocol end observed");
                    return ((pgb_reader, timeline_handles), Ok(()));
                }
            };

            let result = self
                .pagestream_handle_batched_message(
                    pgb_writer,
                    msg,
                    io_concurrency.clone(),
                    &cancel,
                    protocol_version,
                    ctx,
                )
                .await;
            match result {
                Ok(()) => {}
                Err(e) => break e,
            }
        };
        ((pgb_reader, timeline_handles), Err(err))
    }

    /// # Cancel-Safety
    ///
    /// May leak tokio tasks if not polled to completion.
    #[allow(clippy::too_many_arguments)]
    async fn handle_pagerequests_pipelined<IO>(
        &mut self,
        pgb_writer: &mut PostgresBackend<IO>,
        pgb_reader: PostgresBackendReader<IO>,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        mut timeline_handles: TimelineHandles,
        request_span: Span,
        pipelining_config: PageServicePipeliningConfigPipelined,
        protocol_version: PagestreamProtocolVersion,
        io_concurrency: IoConcurrency,
        ctx: &RequestContext,
    ) -> (
        (PostgresBackendReader<IO>, TimelineHandles),
        Result<(), QueryError>,
    )
    where
        IO: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    {
        //
        // Pipelined pagestream handling consists of
        // - a Batcher that reads requests off the wire and
        //   and batches them if possible,
        // - an Executor that processes the batched requests.
        //
        // The batch is built up inside an `spsc_fold` channel,
        // shared betwen Batcher (Sender) and Executor (Receiver).
        //
        // The Batcher continously folds client requests into the batch,
        // while the Executor can at any time take out what's in the batch
        // in order to process it.
        // This means the next batch builds up while the Executor
        // executes the last batch.
        //
        // CANCELLATION
        //
        // We run both Batcher and Executor futures to completion before
        // returning from this function.
        //
        // If Executor exits first, it signals cancellation to the Batcher
        // via a CancellationToken that is child of `self.cancel`.
        // If Batcher exits first, it signals cancellation to the Executor
        // by dropping the spsc_fold channel Sender.
        //
        // CLEAN SHUTDOWN
        //
        // Clean shutdown means that the client ends the COPYBOTH session.
        // In response to such a client message, the Batcher exits.
        // The Executor continues to run, draining the spsc_fold channel.
        // Once drained, the spsc_fold recv will fail with a distinct error
        // indicating that the sender disconnected.
        // The Executor exits with Ok(()) in response to that error.
        //
        // Server initiated shutdown is not clean shutdown, but instead
        // is an error Err(QueryError::Shutdown) that is propagated through
        // error propagation.
        //
        // ERROR PROPAGATION
        //
        // When the Batcher encounter an error, it sends it as a value
        // through the spsc_fold channel and exits afterwards.
        // When the Executor observes such an error in the channel,
        // it exits returning that error value.
        //
        // This design ensures that the Executor stage will still process
        // the batch that was in flight when the Batcher encountered an error,
        // thereby beahving identical to a serial implementation.

        let PageServicePipeliningConfigPipelined {
            max_batch_size,
            execution,
            batching: batching_strategy,
        } = pipelining_config;

        // Macro to _define_ a pipeline stage.
        macro_rules! pipeline_stage {
            ($name:literal, $cancel:expr, $make_fut:expr) => {{
                let cancel: CancellationToken = $cancel;
                let stage_fut = $make_fut(cancel.clone());
                async move {
                    scopeguard::defer! {
                        debug!("exiting");
                    }
                    timed_after_cancellation(stage_fut, $name, Duration::from_millis(100), &cancel)
                        .await
                }
                .instrument(tracing::info_span!($name))
            }};
        }

        //
        // Batcher
        //

        let perf_span_fields = self.perf_span_fields.clone();

        let cancel_batcher = self.cancel.child_token();
        let (mut batch_tx, mut batch_rx) = spsc_fold::channel();
        let batcher = pipeline_stage!("batcher", cancel_batcher.clone(), move |cancel_batcher| {
            let ctx = ctx.attached_child();
            async move {
                let mut pgb_reader = pgb_reader;
                let mut exit = false;
                while !exit {
                    let read_res = Self::pagestream_read_message(
                        &mut pgb_reader,
                        tenant_id,
                        timeline_id,
                        &mut timeline_handles,
                        &perf_span_fields,
                        &cancel_batcher,
                        &ctx,
                        protocol_version,
                        request_span.clone(),
                    )
                    .await;
                    let Some(read_res) = read_res.transpose() else {
                        debug!("client-initiated shutdown");
                        break;
                    };
                    exit |= read_res.is_err();
                    let could_send = batch_tx
                        .send(read_res, |batch, res| {
                            Self::pagestream_do_batch(batching_strategy, max_batch_size, batch, res)
                        })
                        .await;
                    exit |= could_send.is_err();
                }
                (pgb_reader, timeline_handles)
            }
        });

        //
        // Executor
        //

        let executor = pipeline_stage!("executor", self.cancel.clone(), move |cancel| {
            let ctx = ctx.attached_child();
            async move {
                let _cancel_batcher = cancel_batcher.drop_guard();
                loop {
                    let maybe_batch = batch_rx.recv().await;
                    let batch = match maybe_batch {
                        Ok(batch) => batch,
                        Err(spsc_fold::RecvError::SenderGone) => {
                            debug!("upstream gone");
                            return Ok(());
                        }
                    };
                    let mut batch = match batch {
                        Ok(batch) => batch,
                        Err(e) => {
                            return Err(e);
                        }
                    };

                    if let BatchedFeMessage::GetPage {
                        pages,
                        span: _,
                        shard: _,
                        batch_break_reason: _,
                    } = &mut batch
                    {
                        for req in pages {
                            req.batch_wait_ctx.take();
                        }
                    }

                    self.pagestream_handle_batched_message(
                        pgb_writer,
                        batch,
                        io_concurrency.clone(),
                        &cancel,
                        protocol_version,
                        &ctx,
                    )
                    .await?;
                }
            }
        });

        //
        // Execute the stages.
        //

        match execution {
            PageServiceProtocolPipelinedExecutionStrategy::ConcurrentFutures => {
                tokio::join!(batcher, executor)
            }
            PageServiceProtocolPipelinedExecutionStrategy::Tasks => {
                // These tasks are not tracked anywhere.
                let read_messages_task = tokio::spawn(batcher);
                let (read_messages_task_res, executor_res_) =
                    tokio::join!(read_messages_task, executor,);
                (
                    read_messages_task_res.expect("propagated panic from read_messages"),
                    executor_res_,
                )
            }
        }
    }

    /// Helper function to handle the LSN from client request.
    ///
    /// Each GetPage (and Exists and Nblocks) request includes information about
    /// which version of the page is being requested. The primary compute node
    /// will always request the latest page version, by setting 'request_lsn' to
    /// the last inserted or flushed WAL position, while a standby will request
    /// a version at the LSN that it's currently caught up to.
    ///
    /// In either case, if the page server hasn't received the WAL up to the
    /// requested LSN yet, we will wait for it to arrive. The return value is
    /// the LSN that should be used to look up the page versions.
    ///
    /// In addition to the request LSN, each request carries another LSN,
    /// 'not_modified_since', which is a hint to the pageserver that the client
    /// knows that the page has not been modified between 'not_modified_since'
    /// and the request LSN. This allows skipping the wait, as long as the WAL
    /// up to 'not_modified_since' has arrived. If the client doesn't have any
    /// information about when the page was modified, it will use
    /// not_modified_since == lsn. If the client lies and sends a too low
    /// not_modified_hint such that there are in fact later page versions, the
    /// behavior is undefined: the pageserver may return any of the page versions
    /// or an error.
    async fn wait_or_get_last_lsn(
        timeline: &Timeline,
        request_lsn: Lsn,
        not_modified_since: Lsn,
        latest_gc_cutoff_lsn: &RcuReadGuard<Lsn>,
        ctx: &RequestContext,
    ) -> Result<Lsn, PageStreamError> {
        let last_record_lsn = timeline.get_last_record_lsn();
        let effective_request_lsn = Self::effective_request_lsn(
            timeline,
            last_record_lsn,
            request_lsn,
            not_modified_since,
            latest_gc_cutoff_lsn,
        )?;

        if effective_request_lsn > last_record_lsn {
            timeline
                .wait_lsn(
                    not_modified_since,
                    crate::tenant::timeline::WaitLsnWaiter::PageService,
                    timeline::WaitLsnTimeout::Default,
                    ctx,
                )
                .await?;

            // Since we waited for 'effective_request_lsn' to arrive, that is now the last
            // record LSN. (Or close enough for our purposes; the last-record LSN can
            // advance immediately after we return anyway)
        }

        Ok(effective_request_lsn)
    }

    fn effective_request_lsn(
        timeline: &Timeline,
        last_record_lsn: Lsn,
        request_lsn: Lsn,
        not_modified_since: Lsn,
        latest_gc_cutoff_lsn: &RcuReadGuard<Lsn>,
    ) -> Result<Lsn, PageStreamError> {
        // Sanity check the request
        if request_lsn < not_modified_since {
            return Err(PageStreamError::BadRequest(
                format!(
                    "invalid request with request LSN {request_lsn} and not_modified_since {not_modified_since}",
                )
                .into(),
            ));
        }

        // Check explicitly for INVALID just to get a less scary error message if the request is obviously bogus
        if request_lsn == Lsn::INVALID {
            return Err(PageStreamError::BadRequest(
                "invalid LSN(0) in request".into(),
            ));
        }

        // Clients should only read from recent LSNs on their timeline, or from locations holding an LSN lease.
        //
        // We may have older data available, but we make a best effort to detect this case and return an error,
        // to distinguish a misbehaving client (asking for old LSN) from a storage issue (data missing at a legitimate LSN).
        if request_lsn < **latest_gc_cutoff_lsn && !timeline.is_gc_blocked_by_lsn_lease_deadline() {
            let gc_info = &timeline.gc_info.read().unwrap();
            if !gc_info.lsn_covered_by_lease(request_lsn) {
                return Err(
                    PageStreamError::BadRequest(format!(
                        "tried to request a page version that was garbage collected. requested at {} gc cutoff {}",
                        request_lsn, **latest_gc_cutoff_lsn
                    ).into())
                );
            }
        }

        if not_modified_since > last_record_lsn {
            Ok(not_modified_since)
        } else {
            // It might be better to use max(not_modified_since, latest_gc_cutoff_lsn)
            // here instead. That would give the same result, since we know that there
            // haven't been any modifications since 'not_modified_since'. Using an older
            // LSN might be faster, because that could allow skipping recent layers when
            // finding the page. However, we have historically used 'last_record_lsn', so
            // stick to that for now.
            Ok(std::cmp::min(last_record_lsn, request_lsn))
        }
    }

    /// Handles the lsn lease request.
    /// If a lease cannot be obtained, the client will receive NULL.
    #[instrument(skip_all, fields(shard_id, %lsn))]
    async fn handle_make_lsn_lease<IO>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<(), QueryError>
    where
        IO: AsyncRead + AsyncWrite + Send + Sync + Unpin,
    {
        let timeline = self
            .timeline_handles
            .as_mut()
            .unwrap()
            .get(
                tenant_shard_id.tenant_id,
                timeline_id,
                ShardSelector::Known(tenant_shard_id.to_index()),
            )
            .await?;
        set_tracing_field_shard_id(&timeline);

        let lease = timeline
            .renew_lsn_lease(lsn, timeline.get_lsn_lease_length(), ctx)
            .inspect_err(|e| {
                warn!("{e}");
            })
            .ok();
        let valid_until_str = lease.map(|l| {
            l.valid_until
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("valid_until is earlier than UNIX_EPOCH")
                .as_millis()
                .to_string()
        });

        info!(
            "acquired lease for {} until {}",
            lsn,
            valid_until_str.as_deref().unwrap_or("<unknown>")
        );

        let bytes = valid_until_str.as_ref().map(|x| x.as_bytes());

        pgb.write_message_noflush(&BeMessage::RowDescription(&[RowDescriptor::text_col(
            b"valid_until",
        )]))?
        .write_message_noflush(&BeMessage::DataRow(&[bytes]))?;

        Ok(())
    }

    #[instrument(skip_all, fields(shard_id))]
    async fn handle_get_rel_exists_request(
        timeline: &Timeline,
        req: &PagestreamExistsRequest,
        ctx: &RequestContext,
    ) -> Result<PagestreamExistsResponse, PageStreamError> {
        let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
        let lsn = Self::wait_or_get_last_lsn(
            timeline,
            req.hdr.request_lsn,
            req.hdr.not_modified_since,
            &latest_gc_cutoff_lsn,
            ctx,
        )
        .await?;

        let exists = timeline
            .get_rel_exists(
                req.rel,
                Version::LsnRange(LsnRange {
                    effective_lsn: lsn,
                    request_lsn: req.hdr.request_lsn,
                }),
                ctx,
            )
            .await?;

        Ok(PagestreamExistsResponse { req: *req, exists })
    }

    #[instrument(skip_all, fields(shard_id))]
    async fn handle_get_nblocks_request(
        timeline: &Timeline,
        req: &PagestreamNblocksRequest,
        ctx: &RequestContext,
    ) -> Result<PagestreamNblocksResponse, PageStreamError> {
        let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
        let lsn = Self::wait_or_get_last_lsn(
            timeline,
            req.hdr.request_lsn,
            req.hdr.not_modified_since,
            &latest_gc_cutoff_lsn,
            ctx,
        )
        .await?;

        let n_blocks = timeline
            .get_rel_size(
                req.rel,
                Version::LsnRange(LsnRange {
                    effective_lsn: lsn,
                    request_lsn: req.hdr.request_lsn,
                }),
                ctx,
            )
            .await?;

        Ok(PagestreamNblocksResponse {
            req: *req,
            n_blocks,
        })
    }

    #[instrument(skip_all, fields(shard_id))]
    async fn handle_db_size_request(
        timeline: &Timeline,
        req: &PagestreamDbSizeRequest,
        ctx: &RequestContext,
    ) -> Result<PagestreamDbSizeResponse, PageStreamError> {
        let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
        let lsn = Self::wait_or_get_last_lsn(
            timeline,
            req.hdr.request_lsn,
            req.hdr.not_modified_since,
            &latest_gc_cutoff_lsn,
            ctx,
        )
        .await?;

        let total_blocks = timeline
            .get_db_size(
                DEFAULTTABLESPACE_OID,
                req.dbnode,
                Version::LsnRange(LsnRange {
                    effective_lsn: lsn,
                    request_lsn: req.hdr.request_lsn,
                }),
                ctx,
            )
            .await?;
        let db_size = total_blocks as i64 * BLCKSZ as i64;

        Ok(PagestreamDbSizeResponse { req: *req, db_size })
    }

    #[instrument(skip_all)]
    async fn handle_get_page_at_lsn_request_batched(
        timeline: &Timeline,
        requests: SmallVec<[BatchedGetPageRequest; 1]>,
        io_concurrency: IoConcurrency,
        batch_break_reason: GetPageBatchBreakReason,
        ctx: &RequestContext,
    ) -> Vec<Result<(PagestreamBeMessage, SmgrOpTimer, RequestContext), BatchedPageStreamError>>
    {
        debug_assert_current_span_has_tenant_and_timeline_id();

        timeline
            .query_metrics
            .observe_getpage_batch_start(requests.len(), batch_break_reason);

        // If a page trace is running, submit an event for this request.
        if let Some(page_trace) = timeline.page_trace.load().as_ref() {
            let time = SystemTime::now();
            for batch in &requests {
                let key = rel_block_to_key(batch.req.rel, batch.req.blkno).to_compact();
                // Ignore error (trace buffer may be full or tracer may have disconnected).
                _ = page_trace.try_send(PageTraceEvent {
                    key,
                    effective_lsn: batch.lsn_range.effective_lsn,
                    time,
                });
            }
        }

        // If any request in the batch needs to wait for LSN, then do so now.
        let mut perf_instrument = false;
        let max_effective_lsn = requests
            .iter()
            .map(|req| {
                if req.ctx.has_perf_span() {
                    perf_instrument = true;
                }

                req.lsn_range.effective_lsn
            })
            .max()
            .expect("batch is never empty");

        let ctx = match perf_instrument {
            true => RequestContextBuilder::from(ctx)
                .root_perf_span(|| {
                    info_span!(
                        target: PERF_TRACE_TARGET,
                        "GET_VECTORED",
                        tenant_id = %timeline.tenant_shard_id.tenant_id,
                        timeline_id = %timeline.timeline_id,
                        shard = %timeline.tenant_shard_id.shard_slug(),
                        %max_effective_lsn
                    )
                })
                .attached_child(),
            false => ctx.attached_child(),
        };

        let last_record_lsn = timeline.get_last_record_lsn();
        if max_effective_lsn > last_record_lsn {
            if let Err(e) = timeline
                .wait_lsn(
                    max_effective_lsn,
                    crate::tenant::timeline::WaitLsnWaiter::PageService,
                    timeline::WaitLsnTimeout::Default,
                    &ctx,
                )
                .maybe_perf_instrument(&ctx, |current_perf_span| {
                    info_span!(
                        target: PERF_TRACE_TARGET,
                        parent: current_perf_span,
                        "WAIT_LSN",
                    )
                })
                .await
            {
                return Vec::from_iter(requests.into_iter().map(|req| {
                    Err(BatchedPageStreamError {
                        err: PageStreamError::from(e.clone()),
                        req: req.req.hdr,
                    })
                }));
            }
        }

        let results = timeline
            .get_rel_page_at_lsn_batched(
                requests.iter().map(|p| {
                    (
                        &p.req.rel,
                        &p.req.blkno,
                        p.lsn_range,
                        p.ctx.attached_child(),
                    )
                }),
                io_concurrency,
                &ctx,
            )
            .await;
        assert_eq!(results.len(), requests.len());

        // TODO: avoid creating the new Vec here
        Vec::from_iter(
            requests
                .into_iter()
                .zip(results.into_iter())
                .map(|(req, res)| {
                    res.map(|page| {
                        (
                            PagestreamBeMessage::GetPage(
                                pagestream_api::PagestreamGetPageResponse { req: req.req, page },
                            ),
                            req.timer,
                            req.ctx,
                        )
                    })
                    .map_err(|e| BatchedPageStreamError {
                        err: PageStreamError::from(e),
                        req: req.req.hdr,
                    })
                }),
        )
    }

    #[instrument(skip_all, fields(shard_id))]
    async fn handle_get_slru_segment_request(
        timeline: &Timeline,
        req: &PagestreamGetSlruSegmentRequest,
        ctx: &RequestContext,
    ) -> Result<PagestreamGetSlruSegmentResponse, PageStreamError> {
        let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
        let lsn = Self::wait_or_get_last_lsn(
            timeline,
            req.hdr.request_lsn,
            req.hdr.not_modified_since,
            &latest_gc_cutoff_lsn,
            ctx,
        )
        .await?;

        let kind = SlruKind::from_repr(req.kind)
            .ok_or(PageStreamError::BadRequest("invalid SLRU kind".into()))?;
        let segment = timeline.get_slru_segment(kind, req.segno, lsn, ctx).await?;

        Ok(PagestreamGetSlruSegmentResponse { req: *req, segment })
    }

    // NB: this impl mimics what we do for batched getpage requests.
    #[cfg(feature = "testing")]
    #[instrument(skip_all, fields(shard_id))]
    async fn handle_test_request_batch(
        timeline: &Timeline,
        requests: Vec<BatchedTestRequest>,
        _ctx: &RequestContext,
    ) -> Vec<Result<(PagestreamBeMessage, SmgrOpTimer, RequestContext), BatchedPageStreamError>>
    {
        // real requests would do something with the timeline
        let mut results = Vec::with_capacity(requests.len());
        for _req in requests.iter() {
            tokio::task::yield_now().await;

            results.push({
                if timeline.cancel.is_cancelled() {
                    Err(PageReconstructError::Cancelled)
                } else {
                    Ok(())
                }
            });
        }

        // TODO: avoid creating the new Vec here
        Vec::from_iter(
            requests
                .into_iter()
                .zip(results.into_iter())
                .map(|(req, res)| {
                    res.map(|()| {
                        (
                            PagestreamBeMessage::Test(pagestream_api::PagestreamTestResponse {
                                req: req.req.clone(),
                            }),
                            req.timer,
                            RequestContext::new(
                                TaskKind::PageRequestHandler,
                                DownloadBehavior::Warn,
                            ),
                        )
                    })
                    .map_err(|e| BatchedPageStreamError {
                        err: PageStreamError::from(e),
                        req: req.req.hdr,
                    })
                }),
        )
    }

    /// Note on "fullbackup":
    /// Full basebackups should only be used for debugging purposes.
    /// Originally, it was introduced to enable breaking storage format changes,
    /// but that is not applicable anymore.
    ///
    /// # Coding Discipline
    ///
    /// Coding discipline within this function: all interaction with the `pgb` connection
    /// needs to be sensitive to connection shutdown, currently signalled via [`Self::cancel`].
    /// This is so that we can shutdown page_service quickly.
    ///
    /// TODO: wrap the pgb that we pass to the basebackup handler so that it's sensitive
    /// to connection cancellation.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all, fields(shard_id, ?lsn, ?prev_lsn, %full_backup))]
    async fn handle_basebackup_request<IO>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        lsn: Option<Lsn>,
        prev_lsn: Option<Lsn>,
        full_backup: bool,
        gzip: bool,
        replica: bool,
        ctx: &RequestContext,
    ) -> Result<(), QueryError>
    where
        IO: AsyncRead + AsyncWrite + Send + Sync + Unpin,
    {
        let started = std::time::Instant::now();

        let timeline = self
            .timeline_handles
            .as_mut()
            .unwrap()
            .get(tenant_id, timeline_id, ShardSelector::Zero)
            .await?;
        set_tracing_field_shard_id(&timeline);
        let ctx = ctx.with_scope_timeline(&timeline);

        if timeline.is_archived() == Some(true) {
            tracing::info!(
                "timeline {tenant_id}/{timeline_id} is archived, but got basebackup request for it."
            );
            return Err(QueryError::NotFound("timeline is archived".into()));
        }

        let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
        if let Some(lsn) = lsn {
            // Backup was requested at a particular LSN. Wait for it to arrive.
            info!("waiting for {}", lsn);
            timeline
                .wait_lsn(
                    lsn,
                    crate::tenant::timeline::WaitLsnWaiter::PageService,
                    crate::tenant::timeline::WaitLsnTimeout::Default,
                    &ctx,
                )
                .await?;
            timeline
                .check_lsn_is_in_scope(lsn, &latest_gc_cutoff_lsn)
                .context("invalid basebackup lsn")?;
        }

        let lsn_awaited_after = started.elapsed();

        // switch client to COPYOUT
        pgb.write_message_noflush(&BeMessage::CopyOutResponse)
            .map_err(QueryError::Disconnected)?;
        self.flush_cancellable(pgb, &self.cancel).await?;

        let mut from_cache = false;

        // Send a tarball of the latest layer on the timeline. Compress if not
        // fullbackup. TODO Compress in that case too (tests need to be updated)
        if full_backup {
            let mut writer = pgb.copyout_writer();
            basebackup::send_basebackup_tarball(
                &mut writer,
                &timeline,
                lsn,
                prev_lsn,
                full_backup,
                replica,
                None,
                &ctx,
            )
            .await?;
        } else {
            let mut writer = BufWriter::new(pgb.copyout_writer());

            let cached = timeline
                .get_cached_basebackup_if_enabled(lsn, prev_lsn, full_backup, replica, gzip)
                .await;

            if let Some(mut cached) = cached {
                from_cache = true;
                tokio::io::copy(&mut cached, &mut writer)
                    .await
                    .map_err(|err| {
                        BasebackupError::Client(err, "handle_basebackup_request,cached,copy")
                    })?;
            } else {
                basebackup::send_basebackup_tarball(
                    &mut writer,
                    &timeline,
                    lsn,
                    prev_lsn,
                    full_backup,
                    replica,
                    // NB: using fast compression because it's on the critical path for compute
                    // startup. For an empty database, we get <100KB with this method. The
                    // Level::Best compression method gives us <20KB, but maybe we should add
                    // basebackup caching on compute shutdown first.
                    gzip.then_some(async_compression::Level::Fastest),
                    &ctx,
                )
                .await?;
            }
            writer
                .flush()
                .await
                .map_err(|err| BasebackupError::Client(err, "handle_basebackup_request,flush"))?;
        }

        pgb.write_message_noflush(&BeMessage::CopyDone)
            .map_err(QueryError::Disconnected)?;
        self.flush_cancellable(pgb, &timeline.cancel).await?;

        let basebackup_after = started
            .elapsed()
            .checked_sub(lsn_awaited_after)
            .unwrap_or(Duration::ZERO);

        info!(
            lsn_await_millis = lsn_awaited_after.as_millis(),
            basebackup_millis = basebackup_after.as_millis(),
            %from_cache,
            "basebackup complete"
        );

        Ok(())
    }

    // when accessing management api supply None as an argument
    // when using to authorize tenant pass corresponding tenant id
    fn check_permission(&self, tenant_id: Option<TenantId>) -> Result<(), QueryError> {
        if self.auth.is_none() {
            // auth is set to Trust, nothing to check so just return ok
            return Ok(());
        }
        // auth is some, just checked above, when auth is some
        // then claims are always present because of checks during connection init
        // so this expect won't trigger
        let claims = self
            .claims
            .as_ref()
            .expect("claims presence already checked");
        check_permission(claims, tenant_id).map_err(|e| QueryError::Unauthorized(e.0))
    }
}

/// `basebackup tenant timeline [lsn] [--gzip] [--replica]`
#[derive(Debug, Clone, Eq, PartialEq)]
struct BaseBackupCmd {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    lsn: Option<Lsn>,
    gzip: bool,
    replica: bool,
}

/// `fullbackup tenant timeline [lsn] [prev_lsn]`
#[derive(Debug, Clone, Eq, PartialEq)]
struct FullBackupCmd {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    lsn: Option<Lsn>,
    prev_lsn: Option<Lsn>,
}

/// `pagestream_v2 tenant timeline`
#[derive(Debug, Clone, Eq, PartialEq)]
struct PageStreamCmd {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    protocol_version: PagestreamProtocolVersion,
}

/// `lease lsn tenant timeline lsn`
#[derive(Debug, Clone, Eq, PartialEq)]
struct LeaseLsnCmd {
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    lsn: Lsn,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum PageServiceCmd {
    Set,
    PageStream(PageStreamCmd),
    BaseBackup(BaseBackupCmd),
    FullBackup(FullBackupCmd),
    LeaseLsn(LeaseLsnCmd),
}

impl PageStreamCmd {
    fn parse(query: &str, protocol_version: PagestreamProtocolVersion) -> anyhow::Result<Self> {
        let parameters = query.split_whitespace().collect_vec();
        if parameters.len() != 2 {
            bail!(
                "invalid number of parameters for pagestream command: {}",
                query
            );
        }
        let tenant_id = TenantId::from_str(parameters[0])
            .with_context(|| format!("Failed to parse tenant id from {}", parameters[0]))?;
        let timeline_id = TimelineId::from_str(parameters[1])
            .with_context(|| format!("Failed to parse timeline id from {}", parameters[1]))?;
        Ok(Self {
            tenant_id,
            timeline_id,
            protocol_version,
        })
    }
}

impl FullBackupCmd {
    fn parse(query: &str) -> anyhow::Result<Self> {
        let parameters = query.split_whitespace().collect_vec();
        if parameters.len() < 2 || parameters.len() > 4 {
            bail!(
                "invalid number of parameters for basebackup command: {}",
                query
            );
        }
        let tenant_id = TenantId::from_str(parameters[0])
            .with_context(|| format!("Failed to parse tenant id from {}", parameters[0]))?;
        let timeline_id = TimelineId::from_str(parameters[1])
            .with_context(|| format!("Failed to parse timeline id from {}", parameters[1]))?;
        // The caller is responsible for providing correct lsn and prev_lsn.
        let lsn = if let Some(lsn_str) = parameters.get(2) {
            Some(
                Lsn::from_str(lsn_str)
                    .with_context(|| format!("Failed to parse Lsn from {lsn_str}"))?,
            )
        } else {
            None
        };
        let prev_lsn = if let Some(prev_lsn_str) = parameters.get(3) {
            Some(
                Lsn::from_str(prev_lsn_str)
                    .with_context(|| format!("Failed to parse Lsn from {prev_lsn_str}"))?,
            )
        } else {
            None
        };
        Ok(Self {
            tenant_id,
            timeline_id,
            lsn,
            prev_lsn,
        })
    }
}

impl BaseBackupCmd {
    fn parse(query: &str) -> anyhow::Result<Self> {
        let parameters = query.split_whitespace().collect_vec();
        if parameters.len() < 2 {
            bail!(
                "invalid number of parameters for basebackup command: {}",
                query
            );
        }
        let tenant_id = TenantId::from_str(parameters[0])
            .with_context(|| format!("Failed to parse tenant id from {}", parameters[0]))?;
        let timeline_id = TimelineId::from_str(parameters[1])
            .with_context(|| format!("Failed to parse timeline id from {}", parameters[1]))?;
        let lsn;
        let flags_parse_from;
        if let Some(maybe_lsn) = parameters.get(2) {
            if *maybe_lsn == "latest" {
                lsn = None;
                flags_parse_from = 3;
            } else if maybe_lsn.starts_with("--") {
                lsn = None;
                flags_parse_from = 2;
            } else {
                lsn = Some(
                    Lsn::from_str(maybe_lsn)
                        .with_context(|| format!("Failed to parse lsn from {maybe_lsn}"))?,
                );
                flags_parse_from = 3;
            }
        } else {
            lsn = None;
            flags_parse_from = 2;
        }

        let mut gzip = false;
        let mut replica = false;

        for &param in &parameters[flags_parse_from..] {
            match param {
                "--gzip" => {
                    if gzip {
                        bail!("duplicate parameter for basebackup command: {param}")
                    }
                    gzip = true
                }
                "--replica" => {
                    if replica {
                        bail!("duplicate parameter for basebackup command: {param}")
                    }
                    replica = true
                }
                _ => bail!("invalid parameter for basebackup command: {param}"),
            }
        }
        Ok(Self {
            tenant_id,
            timeline_id,
            lsn,
            gzip,
            replica,
        })
    }
}

impl LeaseLsnCmd {
    fn parse(query: &str) -> anyhow::Result<Self> {
        let parameters = query.split_whitespace().collect_vec();
        if parameters.len() != 3 {
            bail!(
                "invalid number of parameters for lease lsn command: {}",
                query
            );
        }
        let tenant_shard_id = TenantShardId::from_str(parameters[0])
            .with_context(|| format!("Failed to parse tenant id from {}", parameters[0]))?;
        let timeline_id = TimelineId::from_str(parameters[1])
            .with_context(|| format!("Failed to parse timeline id from {}", parameters[1]))?;
        let lsn = Lsn::from_str(parameters[2])
            .with_context(|| format!("Failed to parse lsn from {}", parameters[2]))?;
        Ok(Self {
            tenant_shard_id,
            timeline_id,
            lsn,
        })
    }
}

impl PageServiceCmd {
    fn parse(query: &str) -> anyhow::Result<Self> {
        let query = query.trim();
        let Some((cmd, other)) = query.split_once(' ') else {
            bail!("cannot parse query: {query}")
        };
        match cmd.to_ascii_lowercase().as_str() {
            "pagestream_v2" => Ok(Self::PageStream(PageStreamCmd::parse(
                other,
                PagestreamProtocolVersion::V2,
            )?)),
            "pagestream_v3" => Ok(Self::PageStream(PageStreamCmd::parse(
                other,
                PagestreamProtocolVersion::V3,
            )?)),
            "basebackup" => Ok(Self::BaseBackup(BaseBackupCmd::parse(other)?)),
            "fullbackup" => Ok(Self::FullBackup(FullBackupCmd::parse(other)?)),
            "lease" => {
                let Some((cmd2, other)) = other.split_once(' ') else {
                    bail!("invalid lease command: {cmd}");
                };
                let cmd2 = cmd2.to_ascii_lowercase();
                if cmd2 == "lsn" {
                    Ok(Self::LeaseLsn(LeaseLsnCmd::parse(other)?))
                } else {
                    bail!("invalid lease command: {cmd}");
                }
            }
            "set" => Ok(Self::Set),
            _ => Err(anyhow::anyhow!("unsupported command {cmd} in {query}")),
        }
    }
}

/// Parse the startup options from the postgres wire protocol startup packet.
///
/// It takes a sequence of `-c option=X` or `-coption=X`. It parses the options string
/// by best effort and returns all the options parsed (key-value pairs) and a bool indicating
/// whether all options are successfully parsed. There could be duplicates in the options
/// if the caller passed such parameters.
fn parse_options(options: &str) -> (Vec<(String, String)>, bool) {
    let mut parsing_config = false;
    let mut has_error = false;
    let mut config = Vec::new();
    for item in options.split_whitespace() {
        if item == "-c" {
            if !parsing_config {
                parsing_config = true;
            } else {
                // "-c" followed with another "-c"
                tracing::warn!("failed to parse the startup options: {options}");
                has_error = true;
                break;
            }
        } else if item.starts_with("-c") || parsing_config {
            let Some((mut key, value)) = item.split_once('=') else {
                // "-c" followed with an invalid option
                tracing::warn!("failed to parse the startup options: {options}");
                has_error = true;
                break;
            };
            if !parsing_config {
                // Parse "-coptions=X"
                let Some(stripped_key) = key.strip_prefix("-c") else {
                    tracing::warn!("failed to parse the startup options: {options}");
                    has_error = true;
                    break;
                };
                key = stripped_key;
            }
            config.push((key.to_string(), value.to_string()));
            parsing_config = false;
        } else {
            tracing::warn!("failed to parse the startup options: {options}");
            has_error = true;
            break;
        }
    }
    if parsing_config {
        // "-c" without the option
        tracing::warn!("failed to parse the startup options: {options}");
        has_error = true;
    }
    (config, has_error)
}

impl<IO> postgres_backend::Handler<IO> for PageServerHandler
where
    IO: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
    fn check_auth_jwt(
        &mut self,
        _pgb: &mut PostgresBackend<IO>,
        jwt_response: &[u8],
    ) -> Result<(), QueryError> {
        // this unwrap is never triggered, because check_auth_jwt only called when auth_type is NeonJWT
        // which requires auth to be present
        let data: TokenData<Claims> = self
            .auth
            .as_ref()
            .unwrap()
            .decode(str::from_utf8(jwt_response).context("jwt response is not UTF-8")?)
            .map_err(|e| QueryError::Unauthorized(e.0))?;

        if matches!(data.claims.scope, Scope::Tenant) && data.claims.tenant_id.is_none() {
            return Err(QueryError::Unauthorized(
                "jwt token scope is Tenant, but tenant id is missing".into(),
            ));
        }

        debug!(
            "jwt scope check succeeded for scope: {:#?} by tenant id: {:?}",
            data.claims.scope, data.claims.tenant_id,
        );

        self.claims = Some(data.claims);
        Ok(())
    }

    fn startup(
        &mut self,
        _pgb: &mut PostgresBackend<IO>,
        sm: &FeStartupPacket,
    ) -> Result<(), QueryError> {
        fail::fail_point!("ps::connection-start::startup-packet");

        if let FeStartupPacket::StartupMessage { params, .. } = sm {
            if let Some(app_name) = params.get("application_name") {
                self.perf_span_fields.application_name = Some(app_name.to_string());
                Span::current().record("application_name", field::display(app_name));
            }
            if let Some(options) = params.get("options") {
                let (config, _) = parse_options(options);
                for (key, value) in config {
                    if key == "neon.compute_mode" {
                        self.perf_span_fields.compute_mode = Some(value.clone());
                        Span::current().record("compute_mode", field::display(value));
                    }
                }
            }
        };

        Ok(())
    }

    #[instrument(skip_all, fields(tenant_id, timeline_id))]
    async fn process_query(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        query_string: &str,
    ) -> Result<(), QueryError> {
        fail::fail_point!("simulated-bad-compute-connection", |_| {
            info!("Hit failpoint for bad connection");
            Err(QueryError::SimulatedConnectionError)
        });

        fail::fail_point!("ps::connection-start::process-query");

        let ctx = self.connection_ctx.attached_child();
        debug!("process query {query_string}");
        let query = PageServiceCmd::parse(query_string)?;
        match query {
            PageServiceCmd::PageStream(PageStreamCmd {
                tenant_id,
                timeline_id,
                protocol_version,
            }) => {
                tracing::Span::current()
                    .record("tenant_id", field::display(tenant_id))
                    .record("timeline_id", field::display(timeline_id));

                self.check_permission(Some(tenant_id))?;
                let command_kind = match protocol_version {
                    PagestreamProtocolVersion::V2 => ComputeCommandKind::PageStreamV2,
                    PagestreamProtocolVersion::V3 => ComputeCommandKind::PageStreamV3,
                };
                COMPUTE_COMMANDS_COUNTERS.for_command(command_kind).inc();

                self.handle_pagerequests(pgb, tenant_id, timeline_id, protocol_version, ctx)
                    .await?;
            }
            PageServiceCmd::BaseBackup(BaseBackupCmd {
                tenant_id,
                timeline_id,
                lsn,
                gzip,
                replica,
            }) => {
                tracing::Span::current()
                    .record("tenant_id", field::display(tenant_id))
                    .record("timeline_id", field::display(timeline_id));

                self.check_permission(Some(tenant_id))?;

                COMPUTE_COMMANDS_COUNTERS
                    .for_command(ComputeCommandKind::Basebackup)
                    .inc();
                let metric_recording = metrics::BASEBACKUP_QUERY_TIME.start_recording();
                let res = async {
                    self.handle_basebackup_request(
                        pgb,
                        tenant_id,
                        timeline_id,
                        lsn,
                        None,
                        false,
                        gzip,
                        replica,
                        &ctx,
                    )
                    .await?;
                    pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
                    Result::<(), QueryError>::Ok(())
                }
                .await;
                metric_recording.observe(&res);
                res?;
            }
            // same as basebackup, but result includes relational data as well
            PageServiceCmd::FullBackup(FullBackupCmd {
                tenant_id,
                timeline_id,
                lsn,
                prev_lsn,
            }) => {
                tracing::Span::current()
                    .record("tenant_id", field::display(tenant_id))
                    .record("timeline_id", field::display(timeline_id));

                self.check_permission(Some(tenant_id))?;

                COMPUTE_COMMANDS_COUNTERS
                    .for_command(ComputeCommandKind::Fullbackup)
                    .inc();

                // Check that the timeline exists
                self.handle_basebackup_request(
                    pgb,
                    tenant_id,
                    timeline_id,
                    lsn,
                    prev_lsn,
                    true,
                    false,
                    false,
                    &ctx,
                )
                .await?;
                pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
            }
            PageServiceCmd::Set => {
                // important because psycopg2 executes "SET datestyle TO 'ISO'"
                // on connect
                // TODO: allow setting options, i.e., application_name/compute_mode via SET commands
                pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
            }
            PageServiceCmd::LeaseLsn(LeaseLsnCmd {
                tenant_shard_id,
                timeline_id,
                lsn,
            }) => {
                tracing::Span::current()
                    .record("tenant_id", field::display(tenant_shard_id))
                    .record("timeline_id", field::display(timeline_id));

                self.check_permission(Some(tenant_shard_id.tenant_id))?;

                COMPUTE_COMMANDS_COUNTERS
                    .for_command(ComputeCommandKind::LeaseLsn)
                    .inc();

                match self
                    .handle_make_lsn_lease(pgb, tenant_shard_id, timeline_id, lsn, &ctx)
                    .await
                {
                    Ok(()) => {
                        pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?
                    }
                    Err(e) => {
                        error!("error obtaining lsn lease for {lsn}: {e:?}");
                        pgb.write_message_noflush(&BeMessage::ErrorResponse(
                            &e.to_string(),
                            Some(e.pg_error_code()),
                        ))?
                    }
                };
            }
        }

        Ok(())
    }
}

/// Serves the page service over gRPC. Dispatches to PageServerHandler for request processing.
///
/// TODO: rename to PageServiceHandler when libpq impl is removed.
pub struct GrpcPageServiceHandler {
    tenant_manager: Arc<TenantManager>,
    ctx: RequestContext,
    gate_guard: GateGuard,
    get_vectored_concurrent_io: GetVectoredConcurrentIo,
}

impl GrpcPageServiceHandler {
    /// Spawns a gRPC server for the page service.
    ///
    /// TODO: this doesn't support TLS. We need TLS reloading via ReloadingCertificateResolver, so we
    /// need to reimplement the TCP+TLS accept loop ourselves.
    pub fn spawn(
        tenant_manager: Arc<TenantManager>,
        auth: Option<Arc<SwappableJwtAuth>>,
        perf_trace_dispatch: Option<Dispatch>,
        get_vectored_concurrent_io: GetVectoredConcurrentIo,
        listener: std::net::TcpListener,
    ) -> anyhow::Result<CancellableTask> {
        let cancel = CancellationToken::new();
        let ctx = RequestContextBuilder::new(TaskKind::PageRequestHandler)
            .download_behavior(DownloadBehavior::Download)
            .perf_span_dispatch(perf_trace_dispatch)
            .detached_child();
        let gate = Gate::default();

        // Set up the TCP socket. We take a preconfigured TcpListener to bind the
        // port early during startup.
        let incoming = {
            let _runtime = COMPUTE_REQUEST_RUNTIME.enter(); // required by TcpListener::from_std
            listener.set_nonblocking(true)?;
            tonic::transport::server::TcpIncoming::from(tokio::net::TcpListener::from_std(
                listener,
            )?)
            .with_nodelay(Some(GRPC_TCP_NODELAY))
            .with_keepalive(Some(GRPC_TCP_KEEPALIVE_TIME))
        };

        // Set up the gRPC server.
        //
        // TODO: consider tuning window sizes.
        let mut server = tonic::transport::Server::builder()
            .http2_keepalive_interval(Some(GRPC_HTTP2_KEEPALIVE_INTERVAL))
            .http2_keepalive_timeout(Some(GRPC_HTTP2_KEEPALIVE_TIMEOUT))
            .max_concurrent_streams(Some(GRPC_MAX_CONCURRENT_STREAMS));

        // Main page service stack. Uses a mix of Tonic interceptors and Tower layers:
        //
        // * Interceptors: can inspect and modify the gRPC request. Sync code only, runs before service.
        //
        // * Layers: allow async code, can run code after the service response. However, only has access
        //   to the raw HTTP request/response, not the gRPC types.
        let page_service_handler = GrpcPageServiceHandler {
            tenant_manager,
            ctx,
            gate_guard: gate.enter().expect("gate was just created"),
            get_vectored_concurrent_io,
        };

        let observability_layer = ObservabilityLayer;
        let mut tenant_interceptor = TenantMetadataInterceptor;
        let mut auth_interceptor = TenantAuthInterceptor::new(auth);

        let page_service = tower::ServiceBuilder::new()
            // Create tracing span and record request start time.
            .layer(observability_layer)
            // Intercept gRPC requests.
            .layer(tonic::service::InterceptorLayer::new(move |mut req| {
                // Extract tenant metadata.
                req = tenant_interceptor.call(req)?;
                // Authenticate tenant JWT token.
                req = auth_interceptor.call(req)?;
                Ok(req)
            }))
            // Run the page service.
            .service(
                proto::PageServiceServer::new(page_service_handler)
                    // Support both gzip and zstd compression. The client decides what to use.
                    .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                    .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                    .send_compressed(tonic::codec::CompressionEncoding::Gzip)
                    .send_compressed(tonic::codec::CompressionEncoding::Zstd),
            );
        let server = server.add_service(page_service);

        // Reflection service for use with e.g. grpcurl.
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
            .build_v1()?;
        let server = server.add_service(reflection_service);

        // Spawn server task.
        let task_cancel = cancel.clone();
        let task = COMPUTE_REQUEST_RUNTIME.spawn(task_mgr::exit_on_panic_or_error(
            "grpc listener",
            async move {
                let result = server
                    .serve_with_incoming_shutdown(incoming, task_cancel.cancelled())
                    .await;
                if result.is_ok() {
                    // TODO: revisit shutdown logic once page service is implemented.
                    gate.close().await;
                }
                result
            },
        ));

        Ok(CancellableTask { task, cancel })
    }

    /// Errors if the request is executed on a non-zero shard. Only shard 0 has a complete view of
    /// relations and their sizes, as well as SLRU segments and similar data.
    #[allow(clippy::result_large_err)]
    fn ensure_shard_zero(timeline: &Handle<TenantManagerTypes>) -> Result<(), tonic::Status> {
        match timeline.get_shard_index().shard_number.0 {
            0 => Ok(()),
            shard => Err(tonic::Status::invalid_argument(format!(
                "request must execute on shard zero (is shard {shard})",
            ))),
        }
    }

    /// Generates a PagestreamRequest header from a ReadLsn and request ID.
    fn make_hdr(read_lsn: page_api::ReadLsn, req_id: u64) -> PagestreamRequest {
        PagestreamRequest {
            reqid: req_id,
            request_lsn: read_lsn.request_lsn,
            not_modified_since: read_lsn
                .not_modified_since_lsn
                .unwrap_or(read_lsn.request_lsn),
        }
    }

    /// Acquires a timeline handle for the given request.
    ///
    /// TODO: during shard splits, the compute may still be sending requests to the parent shard
    /// until the entire split is committed and the compute is notified. Consider installing a
    /// temporary shard router from the parent to the children while the split is in progress.
    ///
    /// TODO: consider moving this to a middleware layer; all requests need it. Needs to manage
    /// the TimelineHandles lifecycle.
    ///
    /// TODO: untangle acquisition from TenantManagerWrapper::resolve() and Cache::get(), to avoid
    /// the unnecessary overhead.
    async fn get_request_timeline(
        &self,
        req: &tonic::Request<impl Any>,
    ) -> Result<Handle<TenantManagerTypes>, GetActiveTimelineError> {
        let ttid = *extract::<TenantTimelineId>(req);
        let shard_index = *extract::<ShardIndex>(req);
        let shard_selector = ShardSelector::Known(shard_index);

        TimelineHandles::new(self.tenant_manager.clone())
            .get(ttid.tenant_id, ttid.timeline_id, shard_selector)
            .await
    }

    /// Starts a SmgrOpTimer at received_at, throttles the request, and records execution start.
    /// Only errors if the timeline is shutting down.
    ///
    /// TODO: move timer construction to ObservabilityLayer (see TODO there).
    /// TODO: decouple rate limiting (middleware?), and return SlowDown errors instead.
    async fn record_op_start_and_throttle(
        timeline: &Handle<TenantManagerTypes>,
        op: metrics::SmgrQueryType,
        received_at: Instant,
    ) -> Result<SmgrOpTimer, tonic::Status> {
        let mut timer = PageServerHandler::record_op_start_and_throttle(timeline, op, received_at)
            .await
            .map_err(|err| match err {
                // record_op_start_and_throttle() only returns Shutdown.
                QueryError::Shutdown => tonic::Status::unavailable(format!("{err}")),
                err => tonic::Status::internal(format!("unexpected error: {err}")),
            })?;
        timer.observe_execution_start(Instant::now());
        Ok(timer)
    }

    /// Processes a GetPage batch request, via the GetPages bidirectional streaming RPC.
    ///
    /// NB: errors returned from here are intercepted in get_pages(), and may be converted to a
    /// GetPageResponse with an appropriate status code to avoid terminating the stream.
    ///
    /// TODO: get_vectored() currently enforces a batch limit of 32. Postgres will typically send
    /// batches up to effective_io_concurrency = 100. Either we have to accept large batches, or
    /// split them up in the client or server.
    #[instrument(skip_all, fields(req_id, rel, blkno, blks, req_lsn, mod_lsn))]
    async fn get_page(
        ctx: &RequestContext,
        timeline: &WeakHandle<TenantManagerTypes>,
        req: proto::GetPageRequest,
        io_concurrency: IoConcurrency,
    ) -> Result<proto::GetPageResponse, tonic::Status> {
        let received_at = Instant::now();
        let timeline = timeline.upgrade()?;
        let ctx = ctx.with_scope_page_service_pagestream(&timeline);

        // Validate the request, decorate the span, and convert it to a Pagestream request.
        let req = page_api::GetPageRequest::try_from(req)?;

        span_record!(
            req_id = %req.request_id,
            rel = %req.rel,
            blkno = %req.block_numbers[0],
            blks = %req.block_numbers.len(),
            lsn = %req.read_lsn,
        );

        let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn(); // hold guard
        let effective_lsn = PageServerHandler::effective_request_lsn(
            &timeline,
            timeline.get_last_record_lsn(),
            req.read_lsn.request_lsn,
            req.read_lsn
                .not_modified_since_lsn
                .unwrap_or(req.read_lsn.request_lsn),
            &latest_gc_cutoff_lsn,
        )?;

        let mut batch = SmallVec::with_capacity(req.block_numbers.len());
        for blkno in req.block_numbers {
            // TODO: this creates one timer per page and throttles it. We should have a timer for
            // the entire batch, and throttle only the batch, but this is equivalent to what
            // PageServerHandler does already so we keep it for now.
            let timer = Self::record_op_start_and_throttle(
                &timeline,
                metrics::SmgrQueryType::GetPageAtLsn,
                received_at,
            )
            .await?;

            batch.push(BatchedGetPageRequest {
                req: PagestreamGetPageRequest {
                    hdr: Self::make_hdr(req.read_lsn, req.request_id),
                    rel: req.rel,
                    blkno,
                },
                lsn_range: LsnRange {
                    effective_lsn,
                    request_lsn: req.read_lsn.request_lsn,
                },
                timer,
                ctx: ctx.attached_child(),
                batch_wait_ctx: None, // TODO: add tracing
            });
        }

        // TODO: this does a relation size query for every page in the batch. Since this batch is
        // all for one relation, we could do this only once. However, this is not the case for the
        // libpq implementation.
        let results = PageServerHandler::handle_get_page_at_lsn_request_batched(
            &timeline,
            batch,
            io_concurrency,
            GetPageBatchBreakReason::BatchFull, // TODO: not relevant for gRPC batches
            &ctx,
        )
        .await;

        let mut resp = page_api::GetPageResponse {
            request_id: req.request_id,
            status_code: page_api::GetPageStatusCode::Ok,
            reason: None,
            page_images: Vec::with_capacity(results.len()),
        };

        for result in results {
            match result {
                Ok((PagestreamBeMessage::GetPage(r), _, _)) => resp.page_images.push(r.page),
                Ok((resp, _, _)) => {
                    return Err(tonic::Status::internal(format!(
                        "unexpected response: {resp:?}"
                    )));
                }
                Err(err) => return Err(err.err.into()),
            };
        }

        Ok(resp.into())
    }
}

/// Implements the gRPC page service.
///
/// TODO: cancellation.
/// TODO: when the libpq impl is removed, remove the Pagestream types and inline the handler code.
#[tonic::async_trait]
impl proto::PageService for GrpcPageServiceHandler {
    type GetBaseBackupStream = Pin<
        Box<dyn Stream<Item = Result<proto::GetBaseBackupResponseChunk, tonic::Status>> + Send>,
    >;

    type GetPagesStream =
        Pin<Box<dyn Stream<Item = Result<proto::GetPageResponse, tonic::Status>> + Send>>;

    #[instrument(skip_all, fields(rel, lsn))]
    async fn check_rel_exists(
        &self,
        req: tonic::Request<proto::CheckRelExistsRequest>,
    ) -> Result<tonic::Response<proto::CheckRelExistsResponse>, tonic::Status> {
        let received_at = extract::<ReceivedAt>(&req).0;
        let timeline = self.get_request_timeline(&req).await?;
        let ctx = self.ctx.with_scope_page_service_pagestream(&timeline);

        // Validate the request, decorate the span, and convert it to a Pagestream request.
        Self::ensure_shard_zero(&timeline)?;
        let req: page_api::CheckRelExistsRequest = req.into_inner().try_into()?;

        span_record!(rel=%req.rel, lsn=%req.read_lsn);

        let req = PagestreamExistsRequest {
            hdr: Self::make_hdr(req.read_lsn, 0),
            rel: req.rel,
        };

        // Execute the request and convert the response.
        let _timer = Self::record_op_start_and_throttle(
            &timeline,
            metrics::SmgrQueryType::GetRelExists,
            received_at,
        )
        .await?;

        let resp = PageServerHandler::handle_get_rel_exists_request(&timeline, &req, &ctx).await?;
        let resp: page_api::CheckRelExistsResponse = resp.exists;
        Ok(tonic::Response::new(resp.into()))
    }

    #[instrument(skip_all, fields(lsn))]
    async fn get_base_backup(
        &self,
        req: tonic::Request<proto::GetBaseBackupRequest>,
    ) -> Result<tonic::Response<Self::GetBaseBackupStream>, tonic::Status> {
        // Send chunks of 256 KB to avoid large memory allocations. pagebench basebackup shows this
        // to be the sweet spot where throughput is saturated.
        const CHUNK_SIZE: usize = 256 * 1024;

        let timeline = self.get_request_timeline(&req).await?;
        let ctx = self.ctx.with_scope_timeline(&timeline);

        // Validate the request and decorate the span.
        Self::ensure_shard_zero(&timeline)?;
        if timeline.is_archived() == Some(true) {
            return Err(tonic::Status::failed_precondition("timeline is archived"));
        }
        let req: page_api::GetBaseBackupRequest = req.into_inner().try_into()?;

        span_record!(lsn=?req.lsn);

        // Wait for the LSN to arrive, if given.
        if let Some(lsn) = req.lsn {
            let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
            timeline
                .wait_lsn(
                    lsn,
                    WaitLsnWaiter::PageService,
                    WaitLsnTimeout::Default,
                    &ctx,
                )
                .await?;
            timeline
                .check_lsn_is_in_scope(lsn, &latest_gc_cutoff_lsn)
                .map_err(|err| {
                    tonic::Status::invalid_argument(format!("invalid basebackup LSN: {err}"))
                })?;
        }

        // Spawn a task to run the basebackup.
        let span = Span::current();
        let (mut simplex_read, mut simplex_write) = tokio::io::simplex(CHUNK_SIZE);
        let jh = tokio::spawn(async move {
            let gzip_level = match req.compression {
                page_api::BaseBackupCompression::None => None,
                // NB: using fast compression because it's on the critical path for compute
                // startup. For an empty database, we get <100KB with this method. The
                // Level::Best compression method gives us <20KB, but maybe we should add
                // basebackup caching on compute shutdown first.
                page_api::BaseBackupCompression::Gzip => Some(async_compression::Level::Fastest),
            };

            // Check for a cached basebackup.
            let cached = timeline
                .get_cached_basebackup_if_enabled(
                    req.lsn,
                    None,
                    req.full,
                    req.replica,
                    gzip_level.is_some(),
                )
                .await;

            let result = if let Some(mut cached) = cached {
                // If we have a cached basebackup, send it.
                tokio::io::copy(&mut cached, &mut simplex_write)
                    .await
                    .map(|_| ())
                    .map_err(|err| BasebackupError::Client(err, "cached,copy"))
            } else {
                basebackup::send_basebackup_tarball(
                    &mut simplex_write,
                    &timeline,
                    req.lsn,
                    None,
                    req.full,
                    req.replica,
                    gzip_level,
                    &ctx,
                )
                .instrument(span) // propagate request span
                .await
            };
            simplex_write
                .shutdown()
                .await
                .map_err(|err| BasebackupError::Client(err, "simplex_write"))?;
            result
        });

        // Emit chunks of size CHUNK_SIZE.
        let chunks = async_stream::try_stream! {
            loop {
                let mut chunk = BytesMut::with_capacity(CHUNK_SIZE).limit(CHUNK_SIZE);
                loop {
                    let n = simplex_read.read_buf(&mut chunk).await.map_err(|err| {
                        tonic::Status::internal(format!("failed to read basebackup chunk: {err}"))
                    })?;
                    if n == 0 {
                        break; // full chunk or closed stream
                    }
                }
                let chunk = chunk.into_inner().freeze();
                if chunk.is_empty() {
                    break;
                }
                yield proto::GetBaseBackupResponseChunk::from(chunk);
            }
            // Wait for the basebackup task to exit and check for errors.
            jh.await.map_err(|err| {
                tonic::Status::internal(format!("basebackup failed: {err}"))
            })??;
        };

        Ok(tonic::Response::new(Box::pin(chunks)))
    }

    #[instrument(skip_all, fields(db_oid, lsn))]
    async fn get_db_size(
        &self,
        req: tonic::Request<proto::GetDbSizeRequest>,
    ) -> Result<tonic::Response<proto::GetDbSizeResponse>, tonic::Status> {
        let received_at = extract::<ReceivedAt>(&req).0;
        let timeline = self.get_request_timeline(&req).await?;
        let ctx = self.ctx.with_scope_page_service_pagestream(&timeline);

        // Validate the request, decorate the span, and convert it to a Pagestream request.
        Self::ensure_shard_zero(&timeline)?;
        let req: page_api::GetDbSizeRequest = req.into_inner().try_into()?;

        span_record!(db_oid=%req.db_oid, lsn=%req.read_lsn);

        let req = PagestreamDbSizeRequest {
            hdr: Self::make_hdr(req.read_lsn, 0),
            dbnode: req.db_oid,
        };

        // Execute the request and convert the response.
        let _timer = Self::record_op_start_and_throttle(
            &timeline,
            metrics::SmgrQueryType::GetDbSize,
            received_at,
        )
        .await?;

        let resp = PageServerHandler::handle_db_size_request(&timeline, &req, &ctx).await?;
        let resp = resp.db_size as page_api::GetDbSizeResponse;
        Ok(tonic::Response::new(resp.into()))
    }

    // NB: don't instrument this, instrument each streamed request.
    async fn get_pages(
        &self,
        req: tonic::Request<tonic::Streaming<proto::GetPageRequest>>,
    ) -> Result<tonic::Response<Self::GetPagesStream>, tonic::Status> {
        // Extract the timeline from the request and check that it exists.
        let ttid = *extract::<TenantTimelineId>(&req);
        let shard_index = *extract::<ShardIndex>(&req);
        let shard_selector = ShardSelector::Known(shard_index);

        let mut handles = TimelineHandles::new(self.tenant_manager.clone());
        handles
            .get(ttid.tenant_id, ttid.timeline_id, shard_selector)
            .await?;

        // Spawn an IoConcurrency sidecar, if enabled.
        let Ok(gate_guard) = self.gate_guard.try_clone() else {
            return Err(tonic::Status::unavailable("shutting down"));
        };
        let io_concurrency =
            IoConcurrency::spawn_from_conf(self.get_vectored_concurrent_io, gate_guard);

        // Spawn a task to handle the GetPageRequest stream.
        let span = Span::current();
        let ctx = self.ctx.attached_child();
        let mut reqs = req.into_inner();

        let resps = async_stream::try_stream! {
            let timeline = handles
                .get(ttid.tenant_id, ttid.timeline_id, shard_selector)
                .await?
                .downgrade();
            while let Some(req) = reqs.message().await? {
                let req_id = req.request_id;
                let result = Self::get_page(&ctx, &timeline, req, io_concurrency.clone())
                    .instrument(span.clone()) // propagate request span
                    .await;
                yield match result {
                    Ok(resp) => resp,
                    // Convert per-request errors to GetPageResponses as appropriate, or terminate
                    // the stream with a tonic::Status. Log the error regardless, since
                    // ObservabilityLayer can't automatically log stream errors.
                    Err(status) => {
                        // TODO: it would be nice if we could propagate the get_page() fields here.
                        span.in_scope(|| {
                            warn!("request failed with {:?}: {}", status.code(), status.message());
                        });
                        page_api::GetPageResponse::try_from_status(status, req_id)?.into()
                    }
                }
            }
        };

        Ok(tonic::Response::new(Box::pin(resps)))
    }

    #[instrument(skip_all, fields(rel, lsn))]
    async fn get_rel_size(
        &self,
        req: tonic::Request<proto::GetRelSizeRequest>,
    ) -> Result<tonic::Response<proto::GetRelSizeResponse>, tonic::Status> {
        let received_at = extract::<ReceivedAt>(&req).0;
        let timeline = self.get_request_timeline(&req).await?;
        let ctx = self.ctx.with_scope_page_service_pagestream(&timeline);

        // Validate the request, decorate the span, and convert it to a Pagestream request.
        Self::ensure_shard_zero(&timeline)?;
        let req: page_api::GetRelSizeRequest = req.into_inner().try_into()?;

        span_record!(rel=%req.rel, lsn=%req.read_lsn);

        let req = PagestreamNblocksRequest {
            hdr: Self::make_hdr(req.read_lsn, 0),
            rel: req.rel,
        };

        // Execute the request and convert the response.
        let _timer = Self::record_op_start_and_throttle(
            &timeline,
            metrics::SmgrQueryType::GetRelSize,
            received_at,
        )
        .await?;

        let resp = PageServerHandler::handle_get_nblocks_request(&timeline, &req, &ctx).await?;
        let resp: page_api::GetRelSizeResponse = resp.n_blocks;
        Ok(tonic::Response::new(resp.into()))
    }

    #[instrument(skip_all, fields(kind, segno, lsn))]
    async fn get_slru_segment(
        &self,
        req: tonic::Request<proto::GetSlruSegmentRequest>,
    ) -> Result<tonic::Response<proto::GetSlruSegmentResponse>, tonic::Status> {
        let received_at = extract::<ReceivedAt>(&req).0;
        let timeline = self.get_request_timeline(&req).await?;
        let ctx = self.ctx.with_scope_page_service_pagestream(&timeline);

        // Validate the request, decorate the span, and convert it to a Pagestream request.
        Self::ensure_shard_zero(&timeline)?;
        let req: page_api::GetSlruSegmentRequest = req.into_inner().try_into()?;

        span_record!(kind=%req.kind, segno=%req.segno, lsn=%req.read_lsn);

        let req = PagestreamGetSlruSegmentRequest {
            hdr: Self::make_hdr(req.read_lsn, 0),
            kind: req.kind as u8,
            segno: req.segno,
        };

        // Execute the request and convert the response.
        let _timer = Self::record_op_start_and_throttle(
            &timeline,
            metrics::SmgrQueryType::GetSlruSegment,
            received_at,
        )
        .await?;

        let resp =
            PageServerHandler::handle_get_slru_segment_request(&timeline, &req, &ctx).await?;
        let resp: page_api::GetSlruSegmentResponse = resp.segment;
        Ok(tonic::Response::new(resp.into()))
    }

    #[instrument(skip_all, fields(lsn))]
    async fn lease_lsn(
        &self,
        req: tonic::Request<proto::LeaseLsnRequest>,
    ) -> Result<tonic::Response<proto::LeaseLsnResponse>, tonic::Status> {
        let timeline = self.get_request_timeline(&req).await?;
        let ctx = self.ctx.with_scope_timeline(&timeline);

        // Validate and convert the request, and decorate the span.
        let req: page_api::LeaseLsnRequest = req.into_inner().try_into()?;

        span_record!(lsn=%req.lsn);

        // Attempt to acquire a lease. Return FailedPrecondition if the lease could not be granted.
        let lease_length = timeline.get_lsn_lease_length();
        let expires = match timeline.renew_lsn_lease(req.lsn, lease_length, &ctx) {
            Ok(lease) => lease.valid_until,
            Err(err) => return Err(tonic::Status::failed_precondition(format!("{err}"))),
        };

        // TODO: is this spammy? Move it compute-side?
        info!(
            "acquired lease for {} until {}",
            req.lsn,
            chrono::DateTime::<Utc>::from(expires).to_rfc3339()
        );

        Ok(tonic::Response::new(expires.into()))
    }
}

/// gRPC middleware layer that handles observability concerns:
///
/// * Creates and enters a tracing span.
/// * Records the request start time as a ReceivedAt request extension.
///
/// TODO: add perf tracing.
/// TODO: add timing and metrics.
/// TODO: add logging.
#[derive(Clone)]
struct ObservabilityLayer;

impl<S: tonic::server::NamedService> tower::Layer<S> for ObservabilityLayer {
    type Service = ObservabilityLayerService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Clone)]
struct ObservabilityLayerService<S> {
    inner: S,
}

#[derive(Clone, Copy)]
struct ReceivedAt(Instant);

impl<S: tonic::server::NamedService> tonic::server::NamedService for ObservabilityLayerService<S> {
    const NAME: &'static str = S::NAME; // propagate inner service name
}

impl<S, Req, Resp> tower::Service<http::Request<Req>> for ObservabilityLayerService<S>
where
    S: tower::Service<http::Request<Req>, Response = http::Response<Resp>> + Send,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&mut self, mut req: http::Request<Req>) -> Self::Future {
        // Record the request start time as a request extension.
        //
        // TODO: we should start a timer here instead, but it currently requires a timeline handle
        // and SmgrQueryType, which we don't have yet. Refactor it to provide it later.
        req.extensions_mut().insert(ReceivedAt(Instant::now()));

        // Extract the peer address and gRPC method.
        let peer = req
            .extensions()
            .get::<TcpConnectInfo>()
            .and_then(|info| info.remote_addr())
            .map(|addr| addr.to_string())
            .unwrap_or_default();

        let method = req
            .uri()
            .path()
            .split('/')
            .nth(2)
            .unwrap_or(req.uri().path())
            .to_string();

        // Create a basic tracing span.
        //
        // Enter the span for the current thread and instrument the future. It is not sufficient to
        // only instrument the future, since it only takes effect after the future is returned and
        // polled, not when the inner service is called below (e.g. during interceptor execution).
        let span = info_span!(
            "grpc:pageservice",
            // These will be populated by TenantMetadataInterceptor.
            tenant_id = field::Empty,
            timeline_id = field::Empty,
            shard_id = field::Empty,
            // NB: empty fields must be listed first above. Otherwise, the field names will be
            // clobbered when the empty fields are populated. They will be output last regardless.
            %peer,
            %method,
        );
        let _guard = span.enter();

        // Construct a future for calling the inner service, but don't await it. This avoids having
        // to clone the inner service into the future below.
        let call = self.inner.call(req);

        async move {
            // Await the inner service call.
            let result = call.await;

            // Log gRPC error statuses. This won't include request info from handler spans, but it
            // will catch all errors (even those emitted before handler spans are constructed). Only
            // unary request errors are logged here, not streaming response errors.
            if let Ok(ref resp) = result
                && let Some(status) = tonic::Status::from_header_map(resp.headers())
                && status.code() != tonic::Code::Ok
            {
                // TODO: it would be nice if we could propagate the handler span's request fields
                // here. This could e.g. be done by attaching the request fields to
                // tonic::Status::metadata via a proc macro.
                warn!(
                    "request failed with {:?}: {}",
                    status.code(),
                    status.message()
                );
            }

            result
        }
        .instrument(span.clone())
        .boxed()
    }

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
}

/// gRPC interceptor that decodes tenant metadata and stores it as request extensions of type
/// TenantTimelineId and ShardIndex.
#[derive(Clone)]
struct TenantMetadataInterceptor;

impl tonic::service::Interceptor for TenantMetadataInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        // Decode the tenant ID.
        let tenant_id = req
            .metadata()
            .get("neon-tenant-id")
            .ok_or_else(|| tonic::Status::invalid_argument("missing neon-tenant-id"))?
            .to_str()
            .map_err(|_| tonic::Status::invalid_argument("invalid neon-tenant-id"))?;
        let tenant_id = TenantId::from_str(tenant_id)
            .map_err(|_| tonic::Status::invalid_argument("invalid neon-tenant-id"))?;

        // Decode the timeline ID.
        let timeline_id = req
            .metadata()
            .get("neon-timeline-id")
            .ok_or_else(|| tonic::Status::invalid_argument("missing neon-timeline-id"))?
            .to_str()
            .map_err(|_| tonic::Status::invalid_argument("invalid neon-timeline-id"))?;
        let timeline_id = TimelineId::from_str(timeline_id)
            .map_err(|_| tonic::Status::invalid_argument("invalid neon-timeline-id"))?;

        // Decode the shard ID.
        let shard_id = req
            .metadata()
            .get("neon-shard-id")
            .ok_or_else(|| tonic::Status::invalid_argument("missing neon-shard-id"))?
            .to_str()
            .map_err(|_| tonic::Status::invalid_argument("invalid neon-shard-id"))?;
        let shard_id = ShardIndex::from_str(shard_id)
            .map_err(|_| tonic::Status::invalid_argument("invalid neon-shard-id"))?;

        // Stash them in the request.
        let extensions = req.extensions_mut();
        extensions.insert(TenantTimelineId::new(tenant_id, timeline_id));
        extensions.insert(shard_id);

        // Decorate the tracing span.
        span_record!(%tenant_id, %timeline_id, %shard_id);

        Ok(req)
    }
}

/// Authenticates gRPC page service requests.
#[derive(Clone)]
struct TenantAuthInterceptor {
    auth: Option<Arc<SwappableJwtAuth>>,
}

impl TenantAuthInterceptor {
    fn new(auth: Option<Arc<SwappableJwtAuth>>) -> Self {
        Self { auth }
    }
}

impl tonic::service::Interceptor for TenantAuthInterceptor {
    fn call(&mut self, req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        // Do nothing if auth is disabled.
        let Some(auth) = self.auth.as_ref() else {
            return Ok(req);
        };

        // Fetch the tenant ID from the request extensions (set by TenantMetadataInterceptor).
        let TenantTimelineId { tenant_id, .. } = *extract::<TenantTimelineId>(&req);

        // Fetch and decode the JWT token.
        let jwt = req
            .metadata()
            .get("authorization")
            .ok_or_else(|| tonic::Status::unauthenticated("no authorization header"))?
            .to_str()
            .map_err(|_| tonic::Status::invalid_argument("invalid authorization header"))?
            .strip_prefix("Bearer ")
            .ok_or_else(|| tonic::Status::invalid_argument("invalid authorization header"))?
            .trim();
        let jwtdata: TokenData<Claims> = auth
            .decode(jwt)
            .map_err(|err| tonic::Status::invalid_argument(format!("invalid JWT token: {err}")))?;
        let claims = jwtdata.claims;

        // Check if the token is valid for this tenant.
        check_permission(&claims, Some(tenant_id))
            .map_err(|err| tonic::Status::permission_denied(err.to_string()))?;

        // TODO: consider stashing the claims in the request extensions, if needed.

        Ok(req)
    }
}

/// Extracts the given type from the request extensions, or panics if it is missing.
fn extract<T: Send + Sync + 'static>(req: &tonic::Request<impl Any>) -> &T {
    extract_from(req.extensions())
}

/// Extract the given type from the request extensions, or panics if it is missing. This variant
/// can extract both from a tonic::Request and http::Request.
fn extract_from<T: Send + Sync + 'static>(ext: &http::Extensions) -> &T {
    let Some(value) = ext.get::<T>() else {
        let name = std::any::type_name::<T>();
        panic!("extension {name} should be set by middleware");
    };
    value
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum GetActiveTimelineError {
    #[error(transparent)]
    Tenant(GetActiveTenantError),
    #[error(transparent)]
    Timeline(#[from] GetTimelineError),
}

impl From<GetActiveTimelineError> for QueryError {
    fn from(e: GetActiveTimelineError) -> Self {
        match e {
            GetActiveTimelineError::Tenant(GetActiveTenantError::Cancelled) => QueryError::Shutdown,
            GetActiveTimelineError::Tenant(e) => e.into(),
            GetActiveTimelineError::Timeline(e) => QueryError::NotFound(format!("{e}").into()),
        }
    }
}

impl From<GetActiveTimelineError> for tonic::Status {
    fn from(err: GetActiveTimelineError) -> Self {
        let message = err.to_string();
        let code = match err {
            GetActiveTimelineError::Tenant(err) => tonic::Status::from(err).code(),
            GetActiveTimelineError::Timeline(err) => tonic::Status::from(err).code(),
        };
        tonic::Status::new(code, message)
    }
}

impl From<GetTimelineError> for tonic::Status {
    fn from(err: GetTimelineError) -> Self {
        use tonic::Code;
        let code = match &err {
            GetTimelineError::NotFound { .. } => Code::NotFound,
            GetTimelineError::NotActive { .. } => Code::Unavailable,
            GetTimelineError::ShuttingDown => Code::Unavailable,
        };
        tonic::Status::new(code, err.to_string())
    }
}

impl From<GetActiveTenantError> for QueryError {
    fn from(e: GetActiveTenantError) -> Self {
        match e {
            GetActiveTenantError::WaitForActiveTimeout { .. } => QueryError::Disconnected(
                ConnectionError::Io(io::Error::new(io::ErrorKind::TimedOut, e.to_string())),
            ),
            GetActiveTenantError::Cancelled
            | GetActiveTenantError::WillNotBecomeActive(TenantState::Stopping { .. }) => {
                QueryError::Shutdown
            }
            e @ GetActiveTenantError::NotFound(_) => QueryError::NotFound(format!("{e}").into()),
            e => QueryError::Other(anyhow::anyhow!(e)),
        }
    }
}

impl From<GetActiveTenantError> for tonic::Status {
    fn from(err: GetActiveTenantError) -> Self {
        use tonic::Code;
        let code = match &err {
            GetActiveTenantError::Broken(_) => Code::Internal,
            GetActiveTenantError::Cancelled => Code::Unavailable,
            GetActiveTenantError::NotFound(_) => Code::NotFound,
            GetActiveTenantError::SwitchedTenant => Code::Unavailable,
            GetActiveTenantError::WaitForActiveTimeout { .. } => Code::Unavailable,
            GetActiveTenantError::WillNotBecomeActive(_) => Code::Unavailable,
        };
        tonic::Status::new(code, err.to_string())
    }
}

impl From<HandleUpgradeError> for QueryError {
    fn from(e: HandleUpgradeError) -> Self {
        match e {
            HandleUpgradeError::ShutDown => QueryError::Shutdown,
        }
    }
}

impl From<HandleUpgradeError> for tonic::Status {
    fn from(err: HandleUpgradeError) -> Self {
        match err {
            HandleUpgradeError::ShutDown => tonic::Status::unavailable("timeline is shutting down"),
        }
    }
}

fn set_tracing_field_shard_id(timeline: &Timeline) {
    debug_assert_current_span_has_tenant_and_timeline_id_no_shard_id();
    tracing::Span::current().record(
        "shard_id",
        tracing::field::display(timeline.tenant_shard_id.shard_slug()),
    );
    debug_assert_current_span_has_tenant_and_timeline_id();
}

struct WaitedForLsn(Lsn);
impl From<WaitedForLsn> for Lsn {
    fn from(WaitedForLsn(lsn): WaitedForLsn) -> Self {
        lsn
    }
}

#[cfg(test)]
mod tests {
    use utils::shard::ShardCount;

    use super::*;

    #[test]
    fn pageservice_cmd_parse() {
        let tenant_id = TenantId::generate();
        let timeline_id = TimelineId::generate();
        let cmd =
            PageServiceCmd::parse(&format!("pagestream_v2 {tenant_id} {timeline_id}")).unwrap();
        assert_eq!(
            cmd,
            PageServiceCmd::PageStream(PageStreamCmd {
                tenant_id,
                timeline_id,
                protocol_version: PagestreamProtocolVersion::V2,
            })
        );
        let cmd = PageServiceCmd::parse(&format!("basebackup {tenant_id} {timeline_id}")).unwrap();
        assert_eq!(
            cmd,
            PageServiceCmd::BaseBackup(BaseBackupCmd {
                tenant_id,
                timeline_id,
                lsn: None,
                gzip: false,
                replica: false
            })
        );
        let cmd =
            PageServiceCmd::parse(&format!("basebackup {tenant_id} {timeline_id} --gzip")).unwrap();
        assert_eq!(
            cmd,
            PageServiceCmd::BaseBackup(BaseBackupCmd {
                tenant_id,
                timeline_id,
                lsn: None,
                gzip: true,
                replica: false
            })
        );
        let cmd =
            PageServiceCmd::parse(&format!("basebackup {tenant_id} {timeline_id} latest")).unwrap();
        assert_eq!(
            cmd,
            PageServiceCmd::BaseBackup(BaseBackupCmd {
                tenant_id,
                timeline_id,
                lsn: None,
                gzip: false,
                replica: false
            })
        );
        let cmd = PageServiceCmd::parse(&format!("basebackup {tenant_id} {timeline_id} 0/16ABCDE"))
            .unwrap();
        assert_eq!(
            cmd,
            PageServiceCmd::BaseBackup(BaseBackupCmd {
                tenant_id,
                timeline_id,
                lsn: Some(Lsn::from_str("0/16ABCDE").unwrap()),
                gzip: false,
                replica: false
            })
        );
        let cmd = PageServiceCmd::parse(&format!(
            "basebackup {tenant_id} {timeline_id} --replica --gzip"
        ))
        .unwrap();
        assert_eq!(
            cmd,
            PageServiceCmd::BaseBackup(BaseBackupCmd {
                tenant_id,
                timeline_id,
                lsn: None,
                gzip: true,
                replica: true
            })
        );
        let cmd = PageServiceCmd::parse(&format!(
            "basebackup {tenant_id} {timeline_id} 0/16ABCDE --replica --gzip"
        ))
        .unwrap();
        assert_eq!(
            cmd,
            PageServiceCmd::BaseBackup(BaseBackupCmd {
                tenant_id,
                timeline_id,
                lsn: Some(Lsn::from_str("0/16ABCDE").unwrap()),
                gzip: true,
                replica: true
            })
        );
        let cmd = PageServiceCmd::parse(&format!("fullbackup {tenant_id} {timeline_id}")).unwrap();
        assert_eq!(
            cmd,
            PageServiceCmd::FullBackup(FullBackupCmd {
                tenant_id,
                timeline_id,
                lsn: None,
                prev_lsn: None
            })
        );
        let cmd = PageServiceCmd::parse(&format!(
            "fullbackup {tenant_id} {timeline_id} 0/16ABCDE 0/16ABCDF"
        ))
        .unwrap();
        assert_eq!(
            cmd,
            PageServiceCmd::FullBackup(FullBackupCmd {
                tenant_id,
                timeline_id,
                lsn: Some(Lsn::from_str("0/16ABCDE").unwrap()),
                prev_lsn: Some(Lsn::from_str("0/16ABCDF").unwrap()),
            })
        );
        let tenant_shard_id = TenantShardId::unsharded(tenant_id);
        let cmd = PageServiceCmd::parse(&format!(
            "lease lsn {tenant_shard_id} {timeline_id} 0/16ABCDE"
        ))
        .unwrap();
        assert_eq!(
            cmd,
            PageServiceCmd::LeaseLsn(LeaseLsnCmd {
                tenant_shard_id,
                timeline_id,
                lsn: Lsn::from_str("0/16ABCDE").unwrap(),
            })
        );
        let tenant_shard_id = TenantShardId::split(&tenant_shard_id, ShardCount(8))[1];
        let cmd = PageServiceCmd::parse(&format!(
            "lease lsn {tenant_shard_id} {timeline_id} 0/16ABCDE"
        ))
        .unwrap();
        assert_eq!(
            cmd,
            PageServiceCmd::LeaseLsn(LeaseLsnCmd {
                tenant_shard_id,
                timeline_id,
                lsn: Lsn::from_str("0/16ABCDE").unwrap(),
            })
        );
        let cmd = PageServiceCmd::parse("set a = b").unwrap();
        assert_eq!(cmd, PageServiceCmd::Set);
        let cmd = PageServiceCmd::parse("SET foo").unwrap();
        assert_eq!(cmd, PageServiceCmd::Set);
    }

    #[test]
    fn pageservice_cmd_err_handling() {
        let tenant_id = TenantId::generate();
        let timeline_id = TimelineId::generate();
        let cmd = PageServiceCmd::parse("unknown_command");
        assert!(cmd.is_err());
        let cmd = PageServiceCmd::parse("pagestream_v2");
        assert!(cmd.is_err());
        let cmd = PageServiceCmd::parse(&format!("pagestream_v2 {tenant_id}xxx"));
        assert!(cmd.is_err());
        let cmd = PageServiceCmd::parse(&format!("pagestream_v2 {tenant_id}xxx {timeline_id}xxx"));
        assert!(cmd.is_err());
        let cmd = PageServiceCmd::parse(&format!(
            "basebackup {tenant_id} {timeline_id} --gzip --gzip"
        ));
        assert!(cmd.is_err());
        let cmd = PageServiceCmd::parse(&format!(
            "basebackup {tenant_id} {timeline_id} --gzip --unknown"
        ));
        assert!(cmd.is_err());
        let cmd = PageServiceCmd::parse(&format!(
            "basebackup {tenant_id} {timeline_id} --gzip 0/16ABCDE"
        ));
        assert!(cmd.is_err());
        let cmd = PageServiceCmd::parse(&format!("lease {tenant_id} {timeline_id} gzip 0/16ABCDE"));
        assert!(cmd.is_err());
    }

    #[test]
    fn test_parse_options() {
        let (config, has_error) = parse_options(" -c neon.compute_mode=primary ");
        assert!(!has_error);
        assert_eq!(
            config,
            vec![("neon.compute_mode".to_string(), "primary".to_string())]
        );

        let (config, has_error) = parse_options(" -c neon.compute_mode=primary -c foo=bar ");
        assert!(!has_error);
        assert_eq!(
            config,
            vec![
                ("neon.compute_mode".to_string(), "primary".to_string()),
                ("foo".to_string(), "bar".to_string()),
            ]
        );

        let (config, has_error) = parse_options(" -c neon.compute_mode=primary -cfoo=bar");
        assert!(!has_error);
        assert_eq!(
            config,
            vec![
                ("neon.compute_mode".to_string(), "primary".to_string()),
                ("foo".to_string(), "bar".to_string()),
            ]
        );

        let (_, has_error) = parse_options("-c");
        assert!(has_error);

        let (_, has_error) = parse_options("-c foo=bar -c -c");
        assert!(has_error);

        let (_, has_error) = parse_options("    ");
        assert!(!has_error);

        let (_, has_error) = parse_options(" -c neon.compute_mode");
        assert!(has_error);
    }
}
