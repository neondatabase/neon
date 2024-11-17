//! The Page Service listens for client connections and serves their GetPage@LSN
//! requests.

use anyhow::{bail, Context};
use async_compression::tokio::write::GzipEncoder;
use bytes::Buf;
use futures::FutureExt;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use pageserver_api::models::{self, TenantState};
use pageserver_api::models::{
    PagestreamBeMessage, PagestreamDbSizeRequest, PagestreamDbSizeResponse,
    PagestreamErrorResponse, PagestreamExistsRequest, PagestreamExistsResponse,
    PagestreamFeMessage, PagestreamGetPageRequest, PagestreamGetSlruSegmentRequest,
    PagestreamGetSlruSegmentResponse, PagestreamNblocksRequest, PagestreamNblocksResponse,
    PagestreamProtocolVersion,
};
use pageserver_api::shard::TenantShardId;
use postgres_backend::{is_expected_io_error, AuthType, PostgresBackend, QueryError};
use pq_proto::framed::ConnectionError;
use pq_proto::FeStartupPacket;
use pq_proto::{BeMessage, FeMessage, RowDescriptor};
use std::borrow::Cow;
use std::io;
use std::str;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::{
    auth::{Claims, Scope, SwappableJwtAuth},
    id::{TenantId, TimelineId},
    lsn::Lsn,
    simple_rcu::RcuReadGuard,
};

use crate::auth::check_permission;
use crate::basebackup;
use crate::basebackup::BasebackupError;
use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext};
use crate::metrics::{self};
use crate::metrics::{ComputeCommandKind, COMPUTE_COMMANDS_COUNTERS, LIVE_CONNECTIONS};
use crate::pgdatadir_mapping::Version;
use crate::span::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::span::debug_assert_current_span_has_tenant_and_timeline_id_no_shard_id;
use crate::task_mgr::TaskKind;
use crate::task_mgr::{self, COMPUTE_REQUEST_RUNTIME};
use crate::tenant::mgr::ShardSelector;
use crate::tenant::mgr::TenantManager;
use crate::tenant::mgr::{GetActiveTenantError, GetTenantError, ShardResolveResult};
use crate::tenant::timeline::{self, WaitLsnError};
use crate::tenant::GetTimelineError;
use crate::tenant::PageReconstructError;
use crate::tenant::Timeline;
use pageserver_api::key::rel_block_to_key;
use pageserver_api::reltag::{BlockNumber, RelTag, SlruKind};
use postgres_ffi::pg_constants::DEFAULTTABLESPACE_OID;
use postgres_ffi::BLCKSZ;

/// How long we may wait for a [`crate::tenant::mgr::TenantSlot::InProgress`]` and/or a [`crate::tenant::Tenant`] which
/// is not yet in state [`TenantState::Active`].
///
/// NB: this is a different value than [`crate::http::routes::ACTIVE_TENANT_TIMEOUT`].
const ACTIVE_TENANT_TIMEOUT: Duration = Duration::from_millis(30000);

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
}

pub fn spawn(
    conf: &'static PageServerConf,
    tenant_manager: Arc<TenantManager>,
    pg_auth: Option<Arc<SwappableJwtAuth>>,
    tcp_listener: tokio::net::TcpListener,
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
            tenant_manager,
            pg_auth,
            tcp_listener,
            conf.pg_auth_type,
            conf.server_side_batch_timeout,
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
        let Self { cancel, mut tasks } = self;
        cancel.cancel();
        while let Some(res) = tasks.join_next().await {
            Self::handle_connection_completion(res);
        }
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
pub async fn libpq_listener_main(
    tenant_manager: Arc<TenantManager>,
    auth: Option<Arc<SwappableJwtAuth>>,
    listener: tokio::net::TcpListener,
    auth_type: AuthType,
    server_side_batch_timeout: Option<Duration>,
    listener_ctx: RequestContext,
    listener_cancel: CancellationToken,
) -> Connections {
    let connections_cancel = CancellationToken::new();
    let mut connection_handler_tasks = tokio::task::JoinSet::default();

    loop {
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
                let connection_ctx = listener_ctx
                    .detached_child(TaskKind::PageRequestHandler, DownloadBehavior::Download);
                connection_handler_tasks.spawn(page_service_conn_main(
                    tenant_manager.clone(),
                    local_auth,
                    socket,
                    auth_type,
                    server_side_batch_timeout,
                    connection_ctx,
                    connections_cancel.child_token(),
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
    }
}

type ConnectionHandlerResult = anyhow::Result<()>;

#[instrument(skip_all, fields(peer_addr))]
async fn page_service_conn_main(
    tenant_manager: Arc<TenantManager>,
    auth: Option<Arc<SwappableJwtAuth>>,
    socket: tokio::net::TcpStream,
    auth_type: AuthType,
    server_side_batch_timeout: Option<Duration>,
    connection_ctx: RequestContext,
    cancel: CancellationToken,
) -> ConnectionHandlerResult {
    let _guard = LIVE_CONNECTIONS
        .with_label_values(&["page_service"])
        .guard();

    socket
        .set_nodelay(true)
        .context("could not set TCP_NODELAY")?;

    let peer_addr = socket.peer_addr().context("get peer address")?;
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
    let socket = std::pin::pin!(socket);

    fail::fail_point!("ps::connection-start::pre-login");

    // XXX: pgbackend.run() should take the connection_ctx,
    // and create a child per-query context when it invokes process_query.
    // But it's in a shared crate, so, we store connection_ctx inside PageServerHandler
    // and create the per-query context in process_query ourselves.
    let mut conn_handler = PageServerHandler::new(
        tenant_manager,
        auth,
        server_side_batch_timeout,
        connection_ctx,
        cancel.clone(),
    );
    let pgbackend = PostgresBackend::new_from_io(socket, peer_addr, auth_type, None)?;

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
                let tenant_id = conn_handler.timeline_handles.tenant_id();
                Err(io_error).context(format!(
                    "Postgres connection error for tenant_id={:?} client at peer_addr={}",
                    tenant_id, peer_addr
                ))
            }
        }
        other => {
            let tenant_id = conn_handler.timeline_handles.tenant_id();
            other.context(format!(
                "Postgres query error for tenant_id={:?} client peer_addr={}",
                tenant_id, peer_addr
            ))
        }
    }
}

struct PageServerHandler {
    auth: Option<Arc<SwappableJwtAuth>>,
    claims: Option<Claims>,

    /// The context created for the lifetime of the connection
    /// services by this PageServerHandler.
    /// For each query received over the connection,
    /// `process_query` creates a child context from this one.
    connection_ctx: RequestContext,

    cancel: CancellationToken,

    timeline_handles: TimelineHandles,

    /// Messages queued up for the next processing batch
    next_batch: Option<BatchedFeMessage>,

    /// See [`PageServerConf::server_side_batch_timeout`]
    server_side_batch_timeout: Option<Duration>,
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
    ) -> Result<timeline::handle::Handle<TenantManagerTypes>, GetActiveTimelineError> {
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
                timeline::handle::GetError::TimelineGateClosed => {
                    trace!("timeline gate closed");
                    GetActiveTimelineError::Timeline(GetTimelineError::ShuttingDown)
                }
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
    type Timeline = Arc<Timeline>;
}

impl timeline::handle::ArcTimeline<TenantManagerTypes> for Arc<Timeline> {
    fn gate(&self) -> &utils::sync::gate::Gate {
        &self.gate
    }

    fn shard_timeline_id(&self) -> timeline::handle::ShardTimelineId {
        Timeline::shard_timeline_id(self)
    }

    fn per_timeline_state(&self) -> &timeline::handle::PerTimelineState<TenantManagerTypes> {
        &self.handles
    }

    fn get_shard_identity(&self) -> &pageserver_api::shard::ShardIdentity {
        Timeline::get_shard_identity(self)
    }
}

impl timeline::handle::TenantManager<TenantManagerTypes> for TenantManagerWrapper {
    async fn resolve(
        &self,
        timeline_id: TimelineId,
        shard_selector: ShardSelector,
    ) -> Result<Arc<Timeline>, GetActiveTimelineError> {
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
        set_tracing_field_shard_id(&timeline);
        Ok(timeline)
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

enum BatchedFeMessage {
    Exists {
        span: Span,
        req: models::PagestreamExistsRequest,
    },
    Nblocks {
        span: Span,
        req: models::PagestreamNblocksRequest,
    },
    GetPage {
        span: Span,
        shard: timeline::handle::Handle<TenantManagerTypes>,
        effective_request_lsn: Lsn,
        pages: smallvec::SmallVec<[(RelTag, BlockNumber); 1]>,
    },
    DbSize {
        span: Span,
        req: models::PagestreamDbSizeRequest,
    },
    GetSlruSegment {
        span: Span,
        req: models::PagestreamGetSlruSegmentRequest,
    },
    RespondError {
        span: Span,
        error: PageStreamError,
    },
}

enum BatchOrEof {
    /// In the common case, this has one entry.
    /// At most, it has two entries: the first is the leftover batch, the second is an error.
    Batch(smallvec::SmallVec<[BatchedFeMessage; 1]>),
    Eof,
}

impl PageServerHandler {
    pub fn new(
        tenant_manager: Arc<TenantManager>,
        auth: Option<Arc<SwappableJwtAuth>>,
        server_side_batch_timeout: Option<Duration>,
        connection_ctx: RequestContext,
        cancel: CancellationToken,
    ) -> Self {
        PageServerHandler {
            auth,
            claims: None,
            connection_ctx,
            timeline_handles: TimelineHandles::new(tenant_manager),
            cancel,
            next_batch: None,
            server_side_batch_timeout,
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

    async fn read_batch_from_connection<IO>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        tenant_id: &TenantId,
        timeline_id: &TimelineId,
        ctx: &RequestContext,
    ) -> Result<Option<BatchOrEof>, QueryError>
    where
        IO: AsyncRead + AsyncWrite + Send + Sync + Unpin,
    {
        let mut batch = self.next_batch.take();
        let mut batch_started_at: Option<std::time::Instant> = None;

        let next_batch: Option<BatchedFeMessage> = loop {
            let sleep_fut = match (self.server_side_batch_timeout, batch_started_at) {
                (Some(batch_timeout), Some(started_at)) => futures::future::Either::Left(
                    tokio::time::sleep_until((started_at + batch_timeout).into()),
                ),
                _ => futures::future::Either::Right(futures::future::pending()),
            };

            let msg = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    return Err(QueryError::Shutdown)
                }
                msg = pgb.read_message() => {
                    msg
                }
                _ = sleep_fut => {
                    assert!(batch.is_some());
                    break None;
                }
            };
            let copy_data_bytes = match msg? {
                Some(FeMessage::CopyData(bytes)) => bytes,
                Some(FeMessage::Terminate) => {
                    return Ok(Some(BatchOrEof::Eof));
                }
                Some(m) => {
                    return Err(QueryError::Other(anyhow::anyhow!(
                        "unexpected message: {m:?} during COPY"
                    )));
                }
                None => {
                    return Ok(Some(BatchOrEof::Eof));
                } // client disconnected
            };
            trace!("query: {copy_data_bytes:?}");
            fail::fail_point!("ps::handle-pagerequest-message");

            // parse request
            let neon_fe_msg = PagestreamFeMessage::parse(&mut copy_data_bytes.reader())?;

            let this_msg = match neon_fe_msg {
                PagestreamFeMessage::Exists(req) => BatchedFeMessage::Exists {
                    span: tracing::info_span!("handle_get_rel_exists_request", rel = %req.rel, req_lsn = %req.request_lsn),
                    req,
                },
                PagestreamFeMessage::Nblocks(req) => BatchedFeMessage::Nblocks {
                    span: tracing::info_span!("handle_get_nblocks_request", rel = %req.rel, req_lsn = %req.request_lsn),
                    req,
                },
                PagestreamFeMessage::DbSize(req) => BatchedFeMessage::DbSize {
                    span: tracing::info_span!("handle_db_size_request", dbnode = %req.dbnode, req_lsn = %req.request_lsn),
                    req,
                },
                PagestreamFeMessage::GetSlruSegment(req) => BatchedFeMessage::GetSlruSegment {
                    span: tracing::info_span!("handle_get_slru_segment_request", kind = %req.kind, segno = %req.segno, req_lsn = %req.request_lsn),
                    req,
                },
                PagestreamFeMessage::GetPage(PagestreamGetPageRequest {
                    request_lsn,
                    not_modified_since,
                    rel,
                    blkno,
                }) => {
                    // shard_id is filled in by the handler
                    let span = tracing::info_span!(
                        "handle_get_page_at_lsn_request_batched",
                        %tenant_id, %timeline_id, shard_id = tracing::field::Empty, req_lsn = %request_lsn,
                        batch_size = tracing::field::Empty, batch_id = tracing::field::Empty
                    );

                    macro_rules! current_batch_and_error {
                        ($error:expr) => {{
                            let error = BatchedFeMessage::RespondError {
                                span,
                                error: $error,
                            };
                            let batch_and_error = match batch {
                                Some(b) => smallvec::smallvec![b, error],
                                None => smallvec::smallvec![error],
                            };
                            Ok(Some(BatchOrEof::Batch(batch_and_error)))
                        }};
                    }

                    let key = rel_block_to_key(rel, blkno);
                    let shard = match self
                        .timeline_handles
                        .get(*tenant_id, *timeline_id, ShardSelector::Page(key))
                        .instrument(span.clone())
                        .await
                    {
                        Ok(tl) => tl,
                        Err(GetActiveTimelineError::Tenant(GetActiveTenantError::NotFound(_))) => {
                            // We already know this tenant exists in general, because we resolved it at
                            // start of connection.  Getting a NotFound here indicates that the shard containing
                            // the requested page is not present on this node: the client's knowledge of shard->pageserver
                            // mapping is out of date.
                            //
                            // Closing the connection by returning ``::Reconnect` has the side effect of rate-limiting above message, via
                            // client's reconnect backoff, as well as hopefully prompting the client to load its updated configuration
                            // and talk to a different pageserver.
                            return current_batch_and_error!(PageStreamError::Reconnect(
                                "getpage@lsn request routed to wrong shard".into()
                            ));
                        }
                        Err(e) => {
                            return current_batch_and_error!(e.into());
                        }
                    };
                    let effective_request_lsn = match Self::wait_or_get_last_lsn(
                        &shard,
                        request_lsn,
                        not_modified_since,
                        &shard.get_latest_gc_cutoff_lsn(),
                        ctx,
                    )
                    // TODO: if we actually need to wait for lsn here, it delays the entire batch which doesn't need to wait
                    .await
                    {
                        Ok(lsn) => lsn,
                        Err(e) => {
                            return current_batch_and_error!(e);
                        }
                    };
                    BatchedFeMessage::GetPage {
                        span,
                        shard,
                        effective_request_lsn,
                        pages: smallvec::smallvec![(rel, blkno)],
                    }
                }
            };

            let batch_timeout = match self.server_side_batch_timeout {
                Some(value) => value,
                None => {
                    // Batching is not enabled - stop on the first message.
                    return Ok(Some(BatchOrEof::Batch(smallvec::smallvec![this_msg])));
                }
            };

            // check if we can batch
            match (&mut batch, this_msg) {
                (None, this_msg) => {
                    batch = Some(this_msg);
                }
                (
                    Some(BatchedFeMessage::GetPage {
                        span: _,
                        shard: accum_shard,
                        pages: accum_pages,
                        effective_request_lsn: accum_lsn,
                    }),
                    BatchedFeMessage::GetPage {
                        span: _,
                        shard: this_shard,
                        pages: this_pages,
                        effective_request_lsn: this_lsn,
                    },
                ) if async {
                    assert_eq!(this_pages.len(), 1);
                    if accum_pages.len() >= Timeline::MAX_GET_VECTORED_KEYS as usize {
                        assert_eq!(accum_pages.len(), Timeline::MAX_GET_VECTORED_KEYS as usize);
                        return false;
                    }
                    if (accum_shard.tenant_shard_id, accum_shard.timeline_id)
                        != (this_shard.tenant_shard_id, this_shard.timeline_id)
                    {
                        // TODO: we _could_ batch & execute each shard seperately (and in parallel).
                        // But the current logic for keeping responses in order does not support that.
                        return false;
                    }
                    // the vectored get currently only supports a single LSN, so, bounce as soon
                    // as the effective request_lsn changes
                    if *accum_lsn != this_lsn {
                        return false;
                    }
                    true
                }
                .await =>
                {
                    // ok to batch
                    accum_pages.extend(this_pages);
                }
                (Some(_), this_msg) => {
                    // by default, don't continue batching
                    break Some(this_msg);
                }
            }

            // batching impl piece
            let started_at = batch_started_at.get_or_insert_with(Instant::now);
            if started_at.elapsed() > batch_timeout {
                break None;
            }
        };

        self.next_batch = next_batch;
        Ok(batch.map(|b| BatchOrEof::Batch(smallvec::smallvec![b])))
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
        _protocol_version: PagestreamProtocolVersion,
        ctx: RequestContext,
    ) -> Result<(), QueryError>
    where
        IO: AsyncRead + AsyncWrite + Send + Sync + Unpin,
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

        // If [`PageServerHandler`] is reused for multiple pagestreams,
        // then make sure to not process requests from the previous ones.
        self.next_batch = None;

        loop {
            let maybe_batched = self
                .read_batch_from_connection(pgb, &tenant_id, &timeline_id, &ctx)
                .await?;
            let batched = match maybe_batched {
                Some(BatchOrEof::Batch(b)) => b,
                Some(BatchOrEof::Eof) => {
                    break;
                }
                None => {
                    continue;
                }
            };

            for batch in batched {
                // invoke handler function
                let (handler_results, span): (
                    Vec<Result<PagestreamBeMessage, PageStreamError>>,
                    _,
                ) = match batch {
                    BatchedFeMessage::Exists { span, req } => {
                        fail::fail_point!("ps::handle-pagerequest-message::exists");
                        (
                            vec![
                                self.handle_get_rel_exists_request(
                                    tenant_id,
                                    timeline_id,
                                    &req,
                                    &ctx,
                                )
                                .instrument(span.clone())
                                .await,
                            ],
                            span,
                        )
                    }
                    BatchedFeMessage::Nblocks { span, req } => {
                        fail::fail_point!("ps::handle-pagerequest-message::nblocks");
                        (
                            vec![
                                self.handle_get_nblocks_request(tenant_id, timeline_id, &req, &ctx)
                                    .instrument(span.clone())
                                    .await,
                            ],
                            span,
                        )
                    }
                    BatchedFeMessage::GetPage {
                        span,
                        shard,
                        effective_request_lsn,
                        pages,
                    } => {
                        fail::fail_point!("ps::handle-pagerequest-message::getpage");
                        (
                            {
                                let npages = pages.len();
                                let res = self
                                    .handle_get_page_at_lsn_request_batched(
                                        &shard,
                                        effective_request_lsn,
                                        pages,
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
                    BatchedFeMessage::DbSize { span, req } => {
                        fail::fail_point!("ps::handle-pagerequest-message::dbsize");
                        (
                            vec![
                                self.handle_db_size_request(tenant_id, timeline_id, &req, &ctx)
                                    .instrument(span.clone())
                                    .await,
                            ],
                            span,
                        )
                    }
                    BatchedFeMessage::GetSlruSegment { span, req } => {
                        fail::fail_point!("ps::handle-pagerequest-message::slrusegment");
                        (
                            vec![
                                self.handle_get_slru_segment_request(
                                    tenant_id,
                                    timeline_id,
                                    &req,
                                    &ctx,
                                )
                                .instrument(span.clone())
                                .await,
                            ],
                            span,
                        )
                    }
                    BatchedFeMessage::RespondError { span, error } => {
                        // We've already decided to respond with an error, so we don't need to
                        // call the handler.
                        (vec![Err(error)], span)
                    }
                };

                // Map handler result to protocol behavior.
                // Some handler errors cause exit from pagestream protocol.
                // Other handler errors are sent back as an error message and we stay in pagestream protocol.
                for handler_result in handler_results {
                    let response_msg = match handler_result {
                        Err(e) => match &e {
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
                                let full = utils::error::report_compact_sources(&e);
                                span.in_scope(|| {
                                    error!("error reading relation or page version: {full:#}")
                                });
                                PagestreamBeMessage::Error(PagestreamErrorResponse {
                                    message: e.to_string(),
                                })
                            }
                        },
                        Ok(response_msg) => response_msg,
                    };

                    // marshal & transmit response message
                    pgb.write_message_noflush(&BeMessage::CopyData(&response_msg.serialize()))?;
                }
                tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => {
                        // We were requested to shut down.
                        info!("shutdown request received in page handler");
                        return Err(QueryError::Shutdown)
                    }
                    res = pgb.flush() => {
                        res?;
                    }
                }
            }
        }
        Ok(())
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

        // Sanity check the request
        if request_lsn < not_modified_since {
            return Err(PageStreamError::BadRequest(
                format!(
                    "invalid request with request LSN {} and not_modified_since {}",
                    request_lsn, not_modified_since,
                )
                .into(),
            ));
        }

        if request_lsn < **latest_gc_cutoff_lsn {
            let gc_info = &timeline.gc_info.read().unwrap();
            if !gc_info.leases.contains_key(&request_lsn) {
                // The requested LSN is below gc cutoff and is not guarded by a lease.

                // Check explicitly for INVALID just to get a less scary error message if the
                // request is obviously bogus
                return Err(if request_lsn == Lsn::INVALID {
                    PageStreamError::BadRequest("invalid LSN(0) in request".into())
                } else {
                    PageStreamError::BadRequest(format!(
                        "tried to request a page version that was garbage collected. requested at {} gc cutoff {}",
                        request_lsn, **latest_gc_cutoff_lsn
                    ).into())
                });
            }
        }

        // Wait for WAL up to 'not_modified_since' to arrive, if necessary
        if not_modified_since > last_record_lsn {
            timeline
                .wait_lsn(
                    not_modified_since,
                    crate::tenant::timeline::WaitLsnWaiter::PageService,
                    ctx,
                )
                .await?;
            // Since we waited for 'not_modified_since' to arrive, that is now the last
            // record LSN. (Or close enough for our purposes; the last-record LSN can
            // advance immediately after we return anyway)
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
        let bytes = valid_until_str.as_ref().map(|x| x.as_bytes());

        pgb.write_message_noflush(&BeMessage::RowDescription(&[RowDescriptor::text_col(
            b"valid_until",
        )]))?
        .write_message_noflush(&BeMessage::DataRow(&[bytes]))?;

        Ok(())
    }

    #[instrument(skip_all, fields(shard_id))]
    async fn handle_get_rel_exists_request(
        &mut self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: &PagestreamExistsRequest,
        ctx: &RequestContext,
    ) -> Result<PagestreamBeMessage, PageStreamError> {
        let timeline = self
            .timeline_handles
            .get(tenant_id, timeline_id, ShardSelector::Zero)
            .await?;
        let _timer = timeline
            .query_metrics
            .start_timer(metrics::SmgrQueryType::GetRelExists, ctx);

        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        let lsn = Self::wait_or_get_last_lsn(
            &timeline,
            req.request_lsn,
            req.not_modified_since,
            &latest_gc_cutoff_lsn,
            ctx,
        )
        .await?;

        let exists = timeline
            .get_rel_exists(req.rel, Version::Lsn(lsn), ctx)
            .await?;

        Ok(PagestreamBeMessage::Exists(PagestreamExistsResponse {
            exists,
        }))
    }

    #[instrument(skip_all, fields(shard_id))]
    async fn handle_get_nblocks_request(
        &mut self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: &PagestreamNblocksRequest,
        ctx: &RequestContext,
    ) -> Result<PagestreamBeMessage, PageStreamError> {
        let timeline = self
            .timeline_handles
            .get(tenant_id, timeline_id, ShardSelector::Zero)
            .await?;

        let _timer = timeline
            .query_metrics
            .start_timer(metrics::SmgrQueryType::GetRelSize, ctx);

        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        let lsn = Self::wait_or_get_last_lsn(
            &timeline,
            req.request_lsn,
            req.not_modified_since,
            &latest_gc_cutoff_lsn,
            ctx,
        )
        .await?;

        let n_blocks = timeline
            .get_rel_size(req.rel, Version::Lsn(lsn), ctx)
            .await?;

        Ok(PagestreamBeMessage::Nblocks(PagestreamNblocksResponse {
            n_blocks,
        }))
    }

    #[instrument(skip_all, fields(shard_id))]
    async fn handle_db_size_request(
        &mut self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: &PagestreamDbSizeRequest,
        ctx: &RequestContext,
    ) -> Result<PagestreamBeMessage, PageStreamError> {
        let timeline = self
            .timeline_handles
            .get(tenant_id, timeline_id, ShardSelector::Zero)
            .await?;

        let _timer = timeline
            .query_metrics
            .start_timer(metrics::SmgrQueryType::GetDbSize, ctx);

        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        let lsn = Self::wait_or_get_last_lsn(
            &timeline,
            req.request_lsn,
            req.not_modified_since,
            &latest_gc_cutoff_lsn,
            ctx,
        )
        .await?;

        let total_blocks = timeline
            .get_db_size(DEFAULTTABLESPACE_OID, req.dbnode, Version::Lsn(lsn), ctx)
            .await?;
        let db_size = total_blocks as i64 * BLCKSZ as i64;

        Ok(PagestreamBeMessage::DbSize(PagestreamDbSizeResponse {
            db_size,
        }))
    }

    #[instrument(skip_all)]
    async fn handle_get_page_at_lsn_request_batched(
        &mut self,
        timeline: &Timeline,
        effective_lsn: Lsn,
        pages: smallvec::SmallVec<[(RelTag, BlockNumber); 1]>,
        ctx: &RequestContext,
    ) -> Vec<Result<PagestreamBeMessage, PageStreamError>> {
        debug_assert_current_span_has_tenant_and_timeline_id();
        let _timer = timeline.query_metrics.start_timer_many(
            metrics::SmgrQueryType::GetPageAtLsn,
            pages.len(),
            ctx,
        );

        let pages = timeline
            .get_rel_page_at_lsn_batched(pages, effective_lsn, ctx)
            .await;

        Vec::from_iter(pages.into_iter().map(|page| {
            page.map(|page| {
                PagestreamBeMessage::GetPage(models::PagestreamGetPageResponse { page })
            })
            .map_err(PageStreamError::from)
        }))
    }

    #[instrument(skip_all, fields(shard_id))]
    async fn handle_get_slru_segment_request(
        &mut self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: &PagestreamGetSlruSegmentRequest,
        ctx: &RequestContext,
    ) -> Result<PagestreamBeMessage, PageStreamError> {
        let timeline = self
            .timeline_handles
            .get(tenant_id, timeline_id, ShardSelector::Zero)
            .await?;

        let _timer = timeline
            .query_metrics
            .start_timer(metrics::SmgrQueryType::GetSlruSegment, ctx);

        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        let lsn = Self::wait_or_get_last_lsn(
            &timeline,
            req.request_lsn,
            req.not_modified_since,
            &latest_gc_cutoff_lsn,
            ctx,
        )
        .await?;

        let kind = SlruKind::from_repr(req.kind)
            .ok_or(PageStreamError::BadRequest("invalid SLRU kind".into()))?;
        let segment = timeline.get_slru_segment(kind, req.segno, lsn, ctx).await?;

        Ok(PagestreamBeMessage::GetSlruSegment(
            PagestreamGetSlruSegmentResponse { segment },
        ))
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
        fn map_basebackup_error(err: BasebackupError) -> QueryError {
            match err {
                BasebackupError::Client(e) => QueryError::Disconnected(ConnectionError::Io(e)),
                BasebackupError::Server(e) => QueryError::Other(e),
            }
        }

        let started = std::time::Instant::now();

        let timeline = self
            .timeline_handles
            .get(tenant_id, timeline_id, ShardSelector::Zero)
            .await?;

        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        if let Some(lsn) = lsn {
            // Backup was requested at a particular LSN. Wait for it to arrive.
            info!("waiting for {}", lsn);
            timeline
                .wait_lsn(
                    lsn,
                    crate::tenant::timeline::WaitLsnWaiter::PageService,
                    ctx,
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
                ctx,
            )
            .await
            .map_err(map_basebackup_error)?;
        } else {
            let mut writer = BufWriter::new(pgb.copyout_writer());
            if gzip {
                let mut encoder = GzipEncoder::with_quality(
                    &mut writer,
                    // NOTE using fast compression because it's on the critical path
                    //      for compute startup. For an empty database, we get
                    //      <100KB with this method. The Level::Best compression method
                    //      gives us <20KB, but maybe we should add basebackup caching
                    //      on compute shutdown first.
                    async_compression::Level::Fastest,
                );
                basebackup::send_basebackup_tarball(
                    &mut encoder,
                    &timeline,
                    lsn,
                    prev_lsn,
                    full_backup,
                    replica,
                    ctx,
                )
                .await
                .map_err(map_basebackup_error)?;
                // shutdown the encoder to ensure the gzip footer is written
                encoder
                    .shutdown()
                    .await
                    .map_err(|e| QueryError::Disconnected(ConnectionError::Io(e)))?;
            } else {
                basebackup::send_basebackup_tarball(
                    &mut writer,
                    &timeline,
                    lsn,
                    prev_lsn,
                    full_backup,
                    replica,
                    ctx,
                )
                .await
                .map_err(map_basebackup_error)?;
            }
            writer
                .flush()
                .await
                .map_err(|e| map_basebackup_error(BasebackupError::Client(e)))?;
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
    fn parse(query: &str) -> anyhow::Result<Self> {
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
            "pagestream_v2" => Ok(Self::PageStream(PageStreamCmd::parse(other)?)),
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

impl<IO> postgres_backend::Handler<IO> for PageServerHandler
where
    IO: AsyncRead + AsyncWrite + Send + Sync + Unpin,
{
    fn check_auth_jwt(
        &mut self,
        _pgb: &mut PostgresBackend<IO>,
        jwt_response: &[u8],
    ) -> Result<(), QueryError> {
        // this unwrap is never triggered, because check_auth_jwt only called when auth_type is NeonJWT
        // which requires auth to be present
        let data = self
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
        _sm: &FeStartupPacket,
    ) -> Result<(), QueryError> {
        fail::fail_point!("ps::connection-start::startup-packet");
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
            }) => {
                tracing::Span::current()
                    .record("tenant_id", field::display(tenant_id))
                    .record("timeline_id", field::display(timeline_id));

                self.check_permission(Some(tenant_id))?;

                COMPUTE_COMMANDS_COUNTERS
                    .for_command(ComputeCommandKind::PageStreamV2)
                    .inc();

                self.handle_pagerequests(
                    pgb,
                    tenant_id,
                    timeline_id,
                    PagestreamProtocolVersion::V2,
                    ctx,
                )
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
                let metric_recording = metrics::BASEBACKUP_QUERY_TIME.start_recording(&ctx);
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
                timeline_id
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
}
