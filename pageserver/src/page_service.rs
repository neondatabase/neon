//
//! The Page Service listens for client connections and serves their GetPage@LSN
//! requests.
//
//   It is possible to connect here using usual psql/pgbench/libpq. Following
// commands are supported now:
//     *status* -- show actual info about this pageserver,
//     *pagestream* -- enter mode where smgr and pageserver talk with their
//  custom protocol.
//

use anyhow::Context;
use bytes::Buf;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use pageserver_api::models::{
    PagestreamBeMessage, PagestreamDbSizeRequest, PagestreamDbSizeResponse,
    PagestreamErrorResponse, PagestreamExistsRequest, PagestreamExistsResponse,
    PagestreamFeMessage, PagestreamGetPageRequest, PagestreamGetPageResponse,
    PagestreamNblocksRequest, PagestreamNblocksResponse,
};
use pq_proto::ConnectionError;
use pq_proto::FeStartupPacket;
use pq_proto::{BeMessage, FeMessage, RowDescriptor};
use std::io;
use std::net::TcpListener;
use std::str;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::*;
use utils::id::ConnectionId;
use utils::postgres_backend_async::QueryError;
use utils::{
    auth::{Claims, JwtAuth, Scope},
    id::{TenantId, TimelineId},
    lsn::Lsn,
    postgres_backend::AuthType,
    postgres_backend_async::{self, PostgresBackend},
    simple_rcu::RcuReadGuard,
};

use crate::auth::check_permission;
use crate::basebackup;
use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext};
use crate::import_datadir::import_wal_from_tar;
use crate::metrics::{LIVE_CONNECTIONS_COUNT, SMGR_QUERY_TIME};
use crate::task_mgr;
use crate::task_mgr::TaskKind;
use crate::tenant::mgr;
use crate::tenant::{Tenant, Timeline};
use crate::trace::Tracer;

use postgres_ffi::pg_constants::DEFAULTTABLESPACE_OID;
use postgres_ffi::BLCKSZ;

fn copyin_stream(pgb: &mut PostgresBackend) -> impl Stream<Item = io::Result<Bytes>> + '_ {
    async_stream::try_stream! {
        loop {
            let msg = tokio::select! {
                biased;

                _ = task_mgr::shutdown_watcher() => {
                    // We were requested to shut down.
                    let msg = format!("pageserver is shutting down");
                    let _ = pgb.write_message(&BeMessage::ErrorResponse(&msg, None));
                    Err(QueryError::Other(anyhow::anyhow!(msg)))
                }

                msg = pgb.read_message() => { msg }
            };

            match msg {
                Ok(Some(message)) => {
                    let copy_data_bytes = match message {
                        FeMessage::CopyData(bytes) => bytes,
                        FeMessage::CopyDone => { break },
                        FeMessage::Sync => continue,
                        FeMessage::Terminate => {
                            let msg = "client terminated connection with Terminate message during COPY";
                            let query_error_error = QueryError::Disconnected(ConnectionError::Socket(io::Error::new(io::ErrorKind::ConnectionReset, msg)));
                            pgb.write_message(&BeMessage::ErrorResponse(msg, Some(query_error_error.pg_error_code())))?;
                            Err(io::Error::new(io::ErrorKind::ConnectionReset, msg))?;
                            break;
                        }
                        m => {
                            let msg = format!("unexpected message {m:?}");
                            pgb.write_message(&BeMessage::ErrorResponse(&msg, None))?;
                            Err(io::Error::new(io::ErrorKind::Other, msg))?;
                            break;
                        }
                    };

                    yield copy_data_bytes;
                }
                Ok(None) => {
                    let msg = "client closed connection during COPY";
                    let query_error_error = QueryError::Disconnected(ConnectionError::Socket(io::Error::new(io::ErrorKind::ConnectionReset, msg)));
                    pgb.write_message(&BeMessage::ErrorResponse(msg, Some(query_error_error.pg_error_code())))?;
                    pgb.flush().await?;
                    Err(io::Error::new(io::ErrorKind::ConnectionReset, msg))?;
                }
                Err(QueryError::Disconnected(ConnectionError::Socket(io_error))) => {
                    Err(io_error)?;
                }
                Err(other) => {
                    Err(io::Error::new(io::ErrorKind::Other, other))?;
                }
            };
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

///
/// Main loop of the page service.
///
/// Listens for connections, and launches a new handler task for each.
///
pub async fn libpq_listener_main(
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    listener: TcpListener,
    auth_type: AuthType,
    listener_ctx: RequestContext,
) -> anyhow::Result<()> {
    listener.set_nonblocking(true)?;
    let tokio_listener = tokio::net::TcpListener::from_std(listener)?;

    // Wait for a new connection to arrive, or for server shutdown.
    while let Some(res) = tokio::select! {
        biased;

        _ = task_mgr::shutdown_watcher() => {
            // We were requested to shut down.
            None
        }

        res = tokio_listener.accept() => {
            Some(res)
        }
    } {
        match res {
            Ok((socket, peer_addr)) => {
                // Connection established. Spawn a new task to handle it.
                debug!("accepted connection from {}", peer_addr);
                let local_auth = auth.clone();

                let connection_ctx = listener_ctx
                    .detached_child(TaskKind::PageRequestHandler, DownloadBehavior::Download);

                // PageRequestHandler tasks are not associated with any particular
                // timeline in the task manager. In practice most connections will
                // only deal with a particular timeline, but we don't know which one
                // yet.
                task_mgr::spawn(
                    &tokio::runtime::Handle::current(),
                    TaskKind::PageRequestHandler,
                    None,
                    None,
                    "serving compute connection task",
                    false,
                    page_service_conn_main(conf, local_auth, socket, auth_type, connection_ctx),
                );
            }
            Err(err) => {
                // accept() failed. Log the error, and loop back to retry on next connection.
                error!("accept() failed: {:?}", err);
            }
        }
    }

    debug!("page_service loop terminated");

    Ok(())
}

async fn page_service_conn_main(
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    socket: tokio::net::TcpStream,
    auth_type: AuthType,
    connection_ctx: RequestContext,
) -> anyhow::Result<()> {
    // Immediately increment the gauge, then create a job to decrement it on task exit.
    // One of the pros of `defer!` is that this will *most probably*
    // get called, even in presence of panics.
    let gauge = LIVE_CONNECTIONS_COUNT.with_label_values(&["page_service"]);
    gauge.inc();
    scopeguard::defer! {
        gauge.dec();
    }

    socket
        .set_nodelay(true)
        .context("could not set TCP_NODELAY")?;

    // XXX: pgbackend.run() should take the connection_ctx,
    // and create a child per-query context when it invokes process_query.
    // But it's in a shared crate, so, we store connection_ctx inside PageServerHandler
    // and create the per-query context in process_query ourselves.
    let mut conn_handler = PageServerHandler::new(conf, auth, connection_ctx);
    let pgbackend = PostgresBackend::new(socket, auth_type, None)?;

    let result = pgbackend
        .run(&mut conn_handler, task_mgr::shutdown_watcher)
        .await;
    match result {
        Ok(()) => {
            // we've been requested to shut down
            Ok(())
        }
        Err(QueryError::Disconnected(ConnectionError::Socket(io_error))) => {
            // `ConnectionReset` error happens when the Postgres client closes the connection.
            // As this disconnection happens quite often and is expected,
            // we decided to downgrade the logging level to `INFO`.
            // See: https://github.com/neondatabase/neon/issues/1683.
            if io_error.kind() == io::ErrorKind::ConnectionReset {
                info!("Postgres client disconnected");
                Ok(())
            } else {
                Err(io_error).context("Postgres connection error")
            }
        }
        other => other.context("Postgres query error"),
    }
}

struct PageRequestMetrics {
    get_rel_exists: metrics::Histogram,
    get_rel_size: metrics::Histogram,
    get_page_at_lsn: metrics::Histogram,
    get_db_size: metrics::Histogram,
}

impl PageRequestMetrics {
    fn new(tenant_id: &TenantId, timeline_id: &TimelineId) -> Self {
        let tenant_id = tenant_id.to_string();
        let timeline_id = timeline_id.to_string();

        let get_rel_exists =
            SMGR_QUERY_TIME.with_label_values(&["get_rel_exists", &tenant_id, &timeline_id]);

        let get_rel_size =
            SMGR_QUERY_TIME.with_label_values(&["get_rel_size", &tenant_id, &timeline_id]);

        let get_page_at_lsn =
            SMGR_QUERY_TIME.with_label_values(&["get_page_at_lsn", &tenant_id, &timeline_id]);

        let get_db_size =
            SMGR_QUERY_TIME.with_label_values(&["get_db_size", &tenant_id, &timeline_id]);

        Self {
            get_rel_exists,
            get_rel_size,
            get_page_at_lsn,
            get_db_size,
        }
    }
}

struct PageServerHandler {
    _conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    claims: Option<Claims>,

    /// The context created for the lifetime of the connection
    /// services by this PageServerHandler.
    /// For each query received over the connection,
    /// `process_query` creates a child context from this one.
    connection_ctx: RequestContext,
}

impl PageServerHandler {
    pub fn new(
        conf: &'static PageServerConf,
        auth: Option<Arc<JwtAuth>>,
        connection_ctx: RequestContext,
    ) -> Self {
        PageServerHandler {
            _conf: conf,
            auth,
            claims: None,
            connection_ctx,
        }
    }

    #[instrument(skip(self, pgb, ctx))]
    async fn handle_pagerequests(
        &self,
        pgb: &mut PostgresBackend,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        ctx: RequestContext,
    ) -> anyhow::Result<()> {
        // NOTE: pagerequests handler exits when connection is closed,
        //       so there is no need to reset the association
        task_mgr::associate_with(Some(tenant_id), Some(timeline_id));

        // Make request tracer if needed
        let tenant = get_active_tenant_with_timeout(tenant_id, &ctx).await?;
        let mut tracer = if tenant.get_trace_read_requests() {
            let connection_id = ConnectionId::generate();
            let path = tenant
                .conf
                .trace_path(&tenant_id, &timeline_id, &connection_id);
            Some(Tracer::new(path))
        } else {
            None
        };

        // Check that the timeline exists
        let timeline = tenant.get_timeline(timeline_id, true)?;

        // switch client to COPYBOTH
        pgb.write_message(&BeMessage::CopyBothResponse)?;
        pgb.flush().await?;

        let metrics = PageRequestMetrics::new(&tenant_id, &timeline_id);

        loop {
            let msg = tokio::select! {
                biased;

                _ = task_mgr::shutdown_watcher() => {
                    // We were requested to shut down.
                    info!("shutdown request received in page handler");
                    break;
                }

                msg = pgb.read_message() => { msg }
            };

            let copy_data_bytes = match msg? {
                Some(FeMessage::CopyData(bytes)) => bytes,
                Some(FeMessage::Terminate) => break,
                Some(m) => {
                    anyhow::bail!("unexpected message: {m:?} during COPY");
                }
                None => break, // client disconnected
            };

            trace!("query: {copy_data_bytes:?}");

            // Trace request if needed
            if let Some(t) = tracer.as_mut() {
                t.trace(&copy_data_bytes)
            }

            let neon_fe_msg = PagestreamFeMessage::parse(&mut copy_data_bytes.reader())?;

            // TODO: We could create a new per-request context here, with unique ID.
            // Currently we use the same per-timeline context for all requests

            let response = match neon_fe_msg {
                PagestreamFeMessage::Exists(req) => {
                    let _timer = metrics.get_rel_exists.start_timer();
                    self.handle_get_rel_exists_request(&timeline, &req, &ctx)
                        .await
                }
                PagestreamFeMessage::Nblocks(req) => {
                    let _timer = metrics.get_rel_size.start_timer();
                    self.handle_get_nblocks_request(&timeline, &req, &ctx).await
                }
                PagestreamFeMessage::GetPage(req) => {
                    let _timer = metrics.get_page_at_lsn.start_timer();
                    self.handle_get_page_at_lsn_request(&timeline, &req, &ctx)
                        .await
                }
                PagestreamFeMessage::DbSize(req) => {
                    let _timer = metrics.get_db_size.start_timer();
                    self.handle_db_size_request(&timeline, &req, &ctx).await
                }
            };

            let response = response.unwrap_or_else(|e| {
                // print the all details to the log with {:#}, but for the client the
                // error message is enough
                error!("error reading relation or page version: {:?}", e);
                PagestreamBeMessage::Error(PagestreamErrorResponse {
                    message: e.to_string(),
                })
            });

            pgb.write_message(&BeMessage::CopyData(&response.serialize()))?;
            pgb.flush().await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, pgb, ctx))]
    async fn handle_import_basebackup(
        &self,
        pgb: &mut PostgresBackend,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        base_lsn: Lsn,
        _end_lsn: Lsn,
        pg_version: u32,
        ctx: RequestContext,
    ) -> Result<(), QueryError> {
        task_mgr::associate_with(Some(tenant_id), Some(timeline_id));
        // Create empty timeline
        info!("creating new timeline");
        let tenant = get_active_tenant_with_timeout(tenant_id, &ctx).await?;
        let timeline = tenant.create_empty_timeline(timeline_id, base_lsn, pg_version, &ctx)?;

        // TODO mark timeline as not ready until it reaches end_lsn.
        // We might have some wal to import as well, and we should prevent compute
        // from connecting before that and writing conflicting wal.
        //
        // This is not relevant for pageserver->pageserver migrations, since there's
        // no wal to import. But should be fixed if we want to import from postgres.

        // TODO leave clean state on error. For now you can use detach to clean
        // up broken state from a failed import.

        // Import basebackup provided via CopyData
        info!("importing basebackup");
        pgb.write_message(&BeMessage::CopyInResponse)?;
        pgb.flush().await?;

        let mut copyin_stream = Box::pin(copyin_stream(pgb));
        timeline
            .import_basebackup_from_tar(&mut copyin_stream, base_lsn, &ctx)
            .await?;

        // Drain the rest of the Copy data
        let mut bytes_after_tar = 0;
        while let Some(bytes) = copyin_stream.next().await {
            bytes_after_tar += bytes?.len();
        }
        if bytes_after_tar > 0 {
            warn!("ignored {bytes_after_tar} unexpected bytes after the tar archive");
        }

        // TODO check checksum
        // Meanwhile you can verify client-side by taking fullbackup
        // and checking that it matches in size with what was imported.
        // It wouldn't work if base came from vanilla postgres though,
        // since we discard some log files.

        info!("done");
        Ok(())
    }

    #[instrument(skip(self, pgb, ctx))]
    async fn handle_import_wal(
        &self,
        pgb: &mut PostgresBackend,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        start_lsn: Lsn,
        end_lsn: Lsn,
        ctx: RequestContext,
    ) -> Result<(), QueryError> {
        task_mgr::associate_with(Some(tenant_id), Some(timeline_id));

        let timeline = get_active_timeline_with_timeout(tenant_id, timeline_id, &ctx).await?;
        let last_record_lsn = timeline.get_last_record_lsn();
        if last_record_lsn != start_lsn {
            return Err(QueryError::Other(
                anyhow::anyhow!("Cannot import WAL from Lsn {start_lsn} because timeline does not start from the same lsn: {last_record_lsn}"))
            );
        }

        // TODO leave clean state on error. For now you can use detach to clean
        // up broken state from a failed import.

        // Import wal provided via CopyData
        info!("importing wal");
        pgb.write_message(&BeMessage::CopyInResponse)?;
        pgb.flush().await?;
        let mut copyin_stream = Box::pin(copyin_stream(pgb));
        let mut reader = tokio_util::io::StreamReader::new(&mut copyin_stream);
        import_wal_from_tar(&timeline, &mut reader, start_lsn, end_lsn, &ctx).await?;
        info!("wal import complete");

        // Drain the rest of the Copy data
        let mut bytes_after_tar = 0;
        while let Some(bytes) = copyin_stream.next().await {
            bytes_after_tar += bytes?.len();
        }
        if bytes_after_tar > 0 {
            warn!("ignored {bytes_after_tar} unexpected bytes after the tar archive");
        }

        // TODO Does it make sense to overshoot?
        if timeline.get_last_record_lsn() < end_lsn {
            return Err(QueryError::Other(
                anyhow::anyhow!("Cannot import WAL from Lsn {start_lsn} because timeline does not start from the same lsn: {last_record_lsn}"))
            );
        }

        // Flush data to disk, then upload to s3. No need for a forced checkpoint.
        // We only want to persist the data, and it doesn't matter if it's in the
        // shape of deltas or images.
        info!("flushing layers");
        timeline.freeze_and_flush().await?;

        info!("done");
        Ok(())
    }

    /// Helper function to handle the LSN from client request.
    ///
    /// Each GetPage (and Exists and Nblocks) request includes information about
    /// which version of the page is being requested. The client can request the
    /// latest version of the page, or the version that's valid at a particular
    /// LSN. The primary compute node will always request the latest page
    /// version, while a standby will request a version at the LSN that it's
    /// currently caught up to.
    ///
    /// In either case, if the page server hasn't received the WAL up to the
    /// requested LSN yet, we will wait for it to arrive. The return value is
    /// the LSN that should be used to look up the page versions.
    async fn wait_or_get_last_lsn(
        timeline: &Timeline,
        mut lsn: Lsn,
        latest: bool,
        latest_gc_cutoff_lsn: &RcuReadGuard<Lsn>,
        ctx: &RequestContext,
    ) -> anyhow::Result<Lsn> {
        if latest {
            // Latest page version was requested. If LSN is given, it is a hint
            // to the page server that there have been no modifications to the
            // page after that LSN. If we haven't received WAL up to that point,
            // wait until it arrives.
            let last_record_lsn = timeline.get_last_record_lsn();

            // Note: this covers the special case that lsn == Lsn(0). That
            // special case means "return the latest version whatever it is",
            // and it's used for bootstrapping purposes, when the page server is
            // connected directly to the compute node. That is needed because
            // when you connect to the compute node, to receive the WAL, the
            // walsender process will do a look up in the pg_authid catalog
            // table for authentication. That poses a deadlock problem: the
            // catalog table lookup will send a GetPage request, but the GetPage
            // request will block in the page server because the recent WAL
            // hasn't been received yet, and it cannot be received until the
            // walsender completes the authentication and starts streaming the
            // WAL.
            if lsn <= last_record_lsn {
                lsn = last_record_lsn;
            } else {
                timeline.wait_lsn(lsn, ctx).await?;
                // Since we waited for 'lsn' to arrive, that is now the last
                // record LSN. (Or close enough for our purposes; the
                // last-record LSN can advance immediately after we return
                // anyway)
            }
        } else {
            if lsn == Lsn(0) {
                anyhow::bail!("invalid LSN(0) in request");
            }
            timeline.wait_lsn(lsn, ctx).await?;
        }
        anyhow::ensure!(
            lsn >= **latest_gc_cutoff_lsn,
            "tried to request a page version that was garbage collected. requested at {} gc cutoff {}",
            lsn, **latest_gc_cutoff_lsn
        );
        Ok(lsn)
    }

    #[instrument(skip(self, timeline, req, ctx), fields(rel = %req.rel, req_lsn = %req.lsn))]
    async fn handle_get_rel_exists_request(
        &self,
        timeline: &Timeline,
        req: &PagestreamExistsRequest,
        ctx: &RequestContext,
    ) -> anyhow::Result<PagestreamBeMessage> {
        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        let lsn =
            Self::wait_or_get_last_lsn(timeline, req.lsn, req.latest, &latest_gc_cutoff_lsn, ctx)
                .await?;

        let exists = timeline
            .get_rel_exists(req.rel, lsn, req.latest, ctx)
            .await?;

        Ok(PagestreamBeMessage::Exists(PagestreamExistsResponse {
            exists,
        }))
    }

    #[instrument(skip(self, timeline, req, ctx), fields(rel = %req.rel, req_lsn = %req.lsn))]
    async fn handle_get_nblocks_request(
        &self,
        timeline: &Timeline,
        req: &PagestreamNblocksRequest,
        ctx: &RequestContext,
    ) -> anyhow::Result<PagestreamBeMessage> {
        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        let lsn =
            Self::wait_or_get_last_lsn(timeline, req.lsn, req.latest, &latest_gc_cutoff_lsn, ctx)
                .await?;

        let n_blocks = timeline.get_rel_size(req.rel, lsn, req.latest, ctx).await?;

        Ok(PagestreamBeMessage::Nblocks(PagestreamNblocksResponse {
            n_blocks,
        }))
    }

    #[instrument(skip(self, timeline, req, ctx), fields(dbnode = %req.dbnode, req_lsn = %req.lsn))]
    async fn handle_db_size_request(
        &self,
        timeline: &Timeline,
        req: &PagestreamDbSizeRequest,
        ctx: &RequestContext,
    ) -> anyhow::Result<PagestreamBeMessage> {
        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        let lsn =
            Self::wait_or_get_last_lsn(timeline, req.lsn, req.latest, &latest_gc_cutoff_lsn, ctx)
                .await?;

        let total_blocks = timeline
            .get_db_size(DEFAULTTABLESPACE_OID, req.dbnode, lsn, req.latest, ctx)
            .await?;
        let db_size = total_blocks as i64 * BLCKSZ as i64;

        Ok(PagestreamBeMessage::DbSize(PagestreamDbSizeResponse {
            db_size,
        }))
    }

    #[instrument(skip(self, timeline, req, ctx), fields(rel = %req.rel, blkno = %req.blkno, req_lsn = %req.lsn))]
    async fn handle_get_page_at_lsn_request(
        &self,
        timeline: &Timeline,
        req: &PagestreamGetPageRequest,
        ctx: &RequestContext,
    ) -> anyhow::Result<PagestreamBeMessage> {
        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        let lsn =
            Self::wait_or_get_last_lsn(timeline, req.lsn, req.latest, &latest_gc_cutoff_lsn, ctx)
                .await?;
        /*
        // Add a 1s delay to some requests. The delay helps the requests to
        // hit the race condition from github issue #1047 more easily.
        use rand::Rng;
        if rand::thread_rng().gen::<u8>() < 5 {
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
        */

        let page = timeline
            .get_rel_page_at_lsn(req.rel, req.blkno, lsn, req.latest, ctx)
            .await?;

        Ok(PagestreamBeMessage::GetPage(PagestreamGetPageResponse {
            page,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, pgb, ctx))]
    async fn handle_basebackup_request(
        &mut self,
        pgb: &mut PostgresBackend,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        lsn: Option<Lsn>,
        prev_lsn: Option<Lsn>,
        full_backup: bool,
        ctx: RequestContext,
    ) -> anyhow::Result<()> {
        // check that the timeline exists
        let timeline = get_active_timeline_with_timeout(tenant_id, timeline_id, &ctx).await?;

        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        if let Some(lsn) = lsn {
            // Backup was requested at a particular LSN. Wait for it to arrive.
            info!("waiting for {}", lsn);
            timeline.wait_lsn(lsn, &ctx).await?;
            timeline
                .check_lsn_is_in_scope(lsn, &latest_gc_cutoff_lsn)
                .context("invalid basebackup lsn")?;
        }

        // switch client to COPYOUT
        pgb.write_message(&BeMessage::CopyOutResponse)?;
        pgb.flush().await?;

        // Send a tarball of the latest layer on the timeline
        {
            let mut writer = pgb.copyout_writer();
            basebackup::send_basebackup_tarball(
                &mut writer,
                &timeline,
                lsn,
                prev_lsn,
                full_backup,
                &ctx,
            )
            .await?;
        }

        pgb.write_message(&BeMessage::CopyDone)?;
        pgb.flush().await?;
        info!("basebackup complete");

        Ok(())
    }

    // when accessing management api supply None as an argument
    // when using to authorize tenant pass corresponding tenant id
    fn check_permission(&self, tenant_id: Option<TenantId>) -> anyhow::Result<()> {
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
        check_permission(claims, tenant_id)
    }
}

#[async_trait::async_trait]
impl postgres_backend_async::Handler for PageServerHandler {
    fn check_auth_jwt(
        &mut self,
        _pgb: &mut PostgresBackend,
        jwt_response: &[u8],
    ) -> Result<(), QueryError> {
        // this unwrap is never triggered, because check_auth_jwt only called when auth_type is NeonJWT
        // which requires auth to be present
        let data = self
            .auth
            .as_ref()
            .unwrap()
            .decode(str::from_utf8(jwt_response).context("jwt response is not UTF-8")?)?;

        if matches!(data.claims.scope, Scope::Tenant) && data.claims.tenant_id.is_none() {
            return Err(QueryError::Other(anyhow::anyhow!(
                "jwt token scope is Tenant, but tenant id is missing"
            )));
        }

        info!(
            "jwt auth succeeded for scope: {:#?} by tenant id: {:?}",
            data.claims.scope, data.claims.tenant_id,
        );

        self.claims = Some(data.claims);
        Ok(())
    }

    fn startup(
        &mut self,
        _pgb: &mut PostgresBackend,
        _sm: &FeStartupPacket,
    ) -> Result<(), QueryError> {
        Ok(())
    }

    async fn process_query(
        &mut self,
        pgb: &mut PostgresBackend,
        query_string: &str,
    ) -> Result<(), QueryError> {
        let ctx = self.connection_ctx.attached_child();
        debug!("process query {query_string:?}");

        if query_string.starts_with("pagestream ") {
            let (_, params_raw) = query_string.split_at("pagestream ".len());
            let params = params_raw.split(' ').collect::<Vec<_>>();
            if params.len() != 2 {
                return Err(QueryError::Other(anyhow::anyhow!(
                    "invalid param number for pagestream command"
                )));
            }
            let tenant_id = TenantId::from_str(params[0])
                .with_context(|| format!("Failed to parse tenant id from {}", params[0]))?;
            let timeline_id = TimelineId::from_str(params[1])
                .with_context(|| format!("Failed to parse timeline id from {}", params[1]))?;

            self.check_permission(Some(tenant_id))?;

            self.handle_pagerequests(pgb, tenant_id, timeline_id, ctx)
                .await?;
        } else if query_string.starts_with("basebackup ") {
            let (_, params_raw) = query_string.split_at("basebackup ".len());
            let params = params_raw.split_whitespace().collect::<Vec<_>>();

            if params.len() < 2 {
                return Err(QueryError::Other(anyhow::anyhow!(
                    "invalid param number for basebackup command"
                )));
            }

            let tenant_id = TenantId::from_str(params[0])
                .with_context(|| format!("Failed to parse tenant id from {}", params[0]))?;
            let timeline_id = TimelineId::from_str(params[1])
                .with_context(|| format!("Failed to parse timeline id from {}", params[1]))?;

            self.check_permission(Some(tenant_id))?;

            let lsn = if params.len() == 3 {
                Some(
                    Lsn::from_str(params[2])
                        .with_context(|| format!("Failed to parse Lsn from {}", params[2]))?,
                )
            } else {
                None
            };

            // Check that the timeline exists
            self.handle_basebackup_request(pgb, tenant_id, timeline_id, lsn, None, false, ctx)
                .await?;
            pgb.write_message(&BeMessage::CommandComplete(b"SELECT 1"))?;
        }
        // return pair of prev_lsn and last_lsn
        else if query_string.starts_with("get_last_record_rlsn ") {
            let (_, params_raw) = query_string.split_at("get_last_record_rlsn ".len());
            let params = params_raw.split_whitespace().collect::<Vec<_>>();

            if params.len() != 2 {
                return Err(QueryError::Other(anyhow::anyhow!(
                    "invalid param number for get_last_record_rlsn command"
                )));
            }

            let tenant_id = TenantId::from_str(params[0])
                .with_context(|| format!("Failed to parse tenant id from {}", params[0]))?;
            let timeline_id = TimelineId::from_str(params[1])
                .with_context(|| format!("Failed to parse timeline id from {}", params[1]))?;

            self.check_permission(Some(tenant_id))?;
            let timeline = get_active_timeline_with_timeout(tenant_id, timeline_id, &ctx).await?;

            let end_of_timeline = timeline.get_last_record_rlsn();

            pgb.write_message(&BeMessage::RowDescription(&[
                RowDescriptor::text_col(b"prev_lsn"),
                RowDescriptor::text_col(b"last_lsn"),
            ]))?
            .write_message(&BeMessage::DataRow(&[
                Some(end_of_timeline.prev.to_string().as_bytes()),
                Some(end_of_timeline.last.to_string().as_bytes()),
            ]))?
            .write_message(&BeMessage::CommandComplete(b"SELECT 1"))?;
        }
        // same as basebackup, but result includes relational data as well
        else if query_string.starts_with("fullbackup ") {
            let (_, params_raw) = query_string.split_at("fullbackup ".len());
            let params = params_raw.split_whitespace().collect::<Vec<_>>();

            if params.len() < 2 {
                return Err(QueryError::Other(anyhow::anyhow!(
                    "invalid param number for fullbackup command"
                )));
            }

            let tenant_id = TenantId::from_str(params[0])
                .with_context(|| format!("Failed to parse tenant id from {}", params[0]))?;
            let timeline_id = TimelineId::from_str(params[1])
                .with_context(|| format!("Failed to parse timeline id from {}", params[1]))?;

            // The caller is responsible for providing correct lsn and prev_lsn.
            let lsn = if params.len() > 2 {
                Some(
                    Lsn::from_str(params[2])
                        .with_context(|| format!("Failed to parse Lsn from {}", params[2]))?,
                )
            } else {
                None
            };
            let prev_lsn = if params.len() > 3 {
                Some(
                    Lsn::from_str(params[3])
                        .with_context(|| format!("Failed to parse Lsn from {}", params[3]))?,
                )
            } else {
                None
            };

            self.check_permission(Some(tenant_id))?;

            // Check that the timeline exists
            self.handle_basebackup_request(pgb, tenant_id, timeline_id, lsn, prev_lsn, true, ctx)
                .await?;
            pgb.write_message(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("import basebackup ") {
            // Import the `base` section (everything but the wal) of a basebackup.
            // Assumes the tenant already exists on this pageserver.
            //
            // Files are scheduled to be persisted to remote storage, and the
            // caller should poll the http api to check when that is done.
            //
            // Example import command:
            // 1. Get start/end LSN from backup_manifest file
            // 2. Run:
            // cat my_backup/base.tar | psql -h $PAGESERVER \
            //     -c "import basebackup $TENANT $TIMELINE $START_LSN $END_LSN $PG_VERSION"
            let (_, params_raw) = query_string.split_at("import basebackup ".len());
            let params = params_raw.split_whitespace().collect::<Vec<_>>();
            if params.len() != 5 {
                return Err(QueryError::Other(anyhow::anyhow!(
                    "invalid param number for import basebackup command"
                )));
            }
            let tenant_id = TenantId::from_str(params[0])
                .with_context(|| format!("Failed to parse tenant id from {}", params[0]))?;
            let timeline_id = TimelineId::from_str(params[1])
                .with_context(|| format!("Failed to parse timeline id from {}", params[1]))?;
            let base_lsn = Lsn::from_str(params[2])
                .with_context(|| format!("Failed to parse Lsn from {}", params[2]))?;
            let end_lsn = Lsn::from_str(params[3])
                .with_context(|| format!("Failed to parse Lsn from {}", params[3]))?;
            let pg_version = u32::from_str(params[4])
                .with_context(|| format!("Failed to parse pg_version from {}", params[4]))?;

            self.check_permission(Some(tenant_id))?;

            match self
                .handle_import_basebackup(
                    pgb,
                    tenant_id,
                    timeline_id,
                    base_lsn,
                    end_lsn,
                    pg_version,
                    ctx,
                )
                .await
            {
                Ok(()) => pgb.write_message(&BeMessage::CommandComplete(b"SELECT 1"))?,
                Err(e) => {
                    error!("error importing base backup between {base_lsn} and {end_lsn}: {e:?}");
                    pgb.write_message(&BeMessage::ErrorResponse(
                        &e.to_string(),
                        Some(e.pg_error_code()),
                    ))?
                }
            };
        } else if query_string.starts_with("import wal ") {
            // Import the `pg_wal` section of a basebackup.
            //
            // Files are scheduled to be persisted to remote storage, and the
            // caller should poll the http api to check when that is done.
            let (_, params_raw) = query_string.split_at("import wal ".len());
            let params = params_raw.split_whitespace().collect::<Vec<_>>();
            if params.len() != 4 {
                return Err(QueryError::Other(anyhow::anyhow!(
                    "invalid param number for import wal command"
                )));
            }
            let tenant_id = TenantId::from_str(params[0])
                .with_context(|| format!("Failed to parse tenant id from {}", params[0]))?;
            let timeline_id = TimelineId::from_str(params[1])
                .with_context(|| format!("Failed to parse timeline id from {}", params[1]))?;
            let start_lsn = Lsn::from_str(params[2])
                .with_context(|| format!("Failed to parse Lsn from {}", params[2]))?;
            let end_lsn = Lsn::from_str(params[3])
                .with_context(|| format!("Failed to parse Lsn from {}", params[3]))?;

            self.check_permission(Some(tenant_id))?;

            match self
                .handle_import_wal(pgb, tenant_id, timeline_id, start_lsn, end_lsn, ctx)
                .await
            {
                Ok(()) => pgb.write_message(&BeMessage::CommandComplete(b"SELECT 1"))?,
                Err(e) => {
                    error!("error importing WAL between {start_lsn} and {end_lsn}: {e:?}");
                    pgb.write_message(&BeMessage::ErrorResponse(
                        &e.to_string(),
                        Some(e.pg_error_code()),
                    ))?
                }
            };
        } else if query_string.to_ascii_lowercase().starts_with("set ") {
            // important because psycopg2 executes "SET datestyle TO 'ISO'"
            // on connect
            pgb.write_message(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("show ") {
            // show <tenant_id>
            let (_, params_raw) = query_string.split_at("show ".len());
            let params = params_raw.split(' ').collect::<Vec<_>>();
            if params.len() != 1 {
                return Err(QueryError::Other(anyhow::anyhow!(
                    "invalid param number for config command"
                )));
            }
            let tenant_id = TenantId::from_str(params[0])
                .with_context(|| format!("Failed to parse tenant id from {}", params[0]))?;

            self.check_permission(Some(tenant_id))?;

            let tenant = get_active_tenant_with_timeout(tenant_id, &ctx).await?;
            pgb.write_message(&BeMessage::RowDescription(&[
                RowDescriptor::int8_col(b"checkpoint_distance"),
                RowDescriptor::int8_col(b"checkpoint_timeout"),
                RowDescriptor::int8_col(b"compaction_target_size"),
                RowDescriptor::int8_col(b"compaction_period"),
                RowDescriptor::int8_col(b"compaction_threshold"),
                RowDescriptor::int8_col(b"gc_horizon"),
                RowDescriptor::int8_col(b"gc_period"),
                RowDescriptor::int8_col(b"image_creation_threshold"),
                RowDescriptor::int8_col(b"pitr_interval"),
            ]))?
            .write_message(&BeMessage::DataRow(&[
                Some(tenant.get_checkpoint_distance().to_string().as_bytes()),
                Some(
                    tenant
                        .get_checkpoint_timeout()
                        .as_secs()
                        .to_string()
                        .as_bytes(),
                ),
                Some(tenant.get_compaction_target_size().to_string().as_bytes()),
                Some(
                    tenant
                        .get_compaction_period()
                        .as_secs()
                        .to_string()
                        .as_bytes(),
                ),
                Some(tenant.get_compaction_threshold().to_string().as_bytes()),
                Some(tenant.get_gc_horizon().to_string().as_bytes()),
                Some(tenant.get_gc_period().as_secs().to_string().as_bytes()),
                Some(tenant.get_image_creation_threshold().to_string().as_bytes()),
                Some(tenant.get_pitr_interval().as_secs().to_string().as_bytes()),
            ]))?
            .write_message(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else {
            return Err(QueryError::Other(anyhow::anyhow!(
                "unknown command {query_string}"
            )));
        }

        Ok(())
    }
}

/// Get active tenant.
///
/// If the tenant is Loading, waits for it to become Active, for up to 30 s. That
/// ensures that queries don't fail immediately after pageserver startup, because
/// all tenants are still loading.
async fn get_active_tenant_with_timeout(
    tenant_id: TenantId,
    _ctx: &RequestContext, /* require get a context to support cancellation in the future */
) -> anyhow::Result<Arc<Tenant>> {
    let tenant = mgr::get_tenant(tenant_id, false).await?;
    match tokio::time::timeout(Duration::from_secs(30), tenant.wait_to_become_active()).await {
        Ok(wait_result) => wait_result
            // no .context(), the error message is good enough and some tests depend on it
            .map(move |()| tenant),
        Err(_) => anyhow::bail!("Timeout waiting for tenant {tenant_id} to become Active"),
    }
}

/// Shorthand for getting a reference to a Timeline of an Active tenant.
async fn get_active_timeline_with_timeout(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    ctx: &RequestContext,
) -> anyhow::Result<Arc<Timeline>> {
    get_active_tenant_with_timeout(tenant_id, ctx)
        .await
        .and_then(|tenant| tenant.get_timeline(timeline_id, true))
}
