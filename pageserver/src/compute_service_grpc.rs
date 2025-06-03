//!
//! Compute <-> Pageserver API handler. This is for the new gRPC-based protocol
//!
//! TODO:
//!
//! - Many of the API endpoints are still missing
//!
//! - This is very much not optimized.
//!
//! - Much of the code was copy-pasted from page_service.rs. Like the code to get the
//!   Timeline object, and the JWT auth. Could refactor and share.
//!
//!

use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use crate::TenantManager;
use crate::auth::check_permission;
use crate::basebackup;
use crate::basebackup::BasebackupError;
use crate::config::PageServerConf;
use crate::context::{DownloadBehavior, RequestContext, RequestContextBuilder};
use crate::task_mgr::TaskKind;
use crate::tenant::Timeline;
use crate::tenant::mgr::ShardResolveResult;
use crate::tenant::mgr::ShardSelector;
use crate::tenant::storage_layer::IoConcurrency;
use crate::tenant::timeline::WaitLsnTimeout;
use async_stream::try_stream;
use futures::Stream;
use tokio::io::{AsyncWriteExt, ReadHalf, SimplexStream};
use tokio::task::JoinHandle;
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::sync::CancellationToken;

use futures::stream::StreamExt;

use pageserver_page_api::proto::page_service_server::PageService;
use pageserver_page_api::proto::page_service_server::PageServiceServer;
use pageserver_page_api::*;

use anyhow::Context;
use bytes::BytesMut;
use jsonwebtoken::TokenData;
use tracing::Instrument;
use tracing::{debug, error};
use utils::auth::{Claims, SwappableJwtAuth};

use utils::id::{TenantId, TenantTimelineId, TimelineId};
use utils::lsn::Lsn;
use utils::shard::ShardIndex;
use utils::simple_rcu::RcuReadGuard;

use crate::tenant::PageReconstructError;

use postgres_ffi::BLCKSZ;

use tonic;
use tonic::codec::CompressionEncoding;
use tonic::service::interceptor::InterceptedService;

use crate::pgdatadir_mapping::Version;
use postgres_ffi::pg_constants::DEFAULTTABLESPACE_OID;

use postgres_backend::AuthType;

pub use pageserver_page_api::proto;

pub(super) fn launch_compute_service_grpc_server(
    tcp_connections_rx: tokio::sync::mpsc::Receiver<tokio::io::Result<tokio::net::TcpStream>>,
    conf: &'static PageServerConf,
    tenant_manager: Arc<TenantManager>,
    auth: Option<Arc<SwappableJwtAuth>>,
    auth_type: AuthType,
    connections_cancel: CancellationToken,
    listener_ctx: &RequestContext,
) {
    // Set up the gRPC service
    let service_ctx = RequestContextBuilder::from(listener_ctx)
        .task_kind(TaskKind::PageRequestHandler)
        .download_behavior(DownloadBehavior::Download)
        .attached_child();
    let service = crate::compute_service_grpc::PageServiceService {
        conf,
        tenant_mgr: tenant_manager.clone(),
        ctx: Arc::new(service_ctx),
    };
    let authenticator = PageServiceAuthenticator {
        auth: auth.clone(),
        auth_type,
    };

    let server = InterceptedService::new(
        PageServiceServer::new(service).send_compressed(CompressionEncoding::Gzip),
        authenticator,
    );

    let cc = connections_cancel.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::ReceiverStream::new(tcp_connections_rx),
                cc.cancelled(),
            )
            .await
    });
}

struct PageServiceService {
    conf: &'static PageServerConf,
    tenant_mgr: Arc<TenantManager>,
    ctx: Arc<RequestContext>,
}

/// An error happened in a get() operation.
impl From<PageReconstructError> for tonic::Status {
    fn from(e: PageReconstructError) -> Self {
        match e {
            PageReconstructError::Other(err) => tonic::Status::unknown(err.to_string()),
            PageReconstructError::AncestorLsnTimeout(_) => {
                tonic::Status::unavailable(e.to_string())
            }
            PageReconstructError::Cancelled => tonic::Status::aborted(e.to_string()),
            PageReconstructError::WalRedo(_) => tonic::Status::internal(e.to_string()),
            PageReconstructError::MissingKey(_) => tonic::Status::internal(e.to_string()),
        }
    }
}

fn convert_reltag(value: &RelTag) -> pageserver_api::reltag::RelTag {
    pageserver_api::reltag::RelTag {
        spcnode: value.spcnode,
        dbnode: value.dbnode,
        relnode: value.relnode,
        forknum: value.forknum,
    }
}

#[tonic::async_trait]
impl PageService for PageServiceService {
    type GetBaseBackupStream = GetBaseBackupStream;
    type GetPagesStream =
        Pin<Box<dyn Stream<Item = Result<proto::GetPageResponse, tonic::Status>> + Send>>;

    async fn check_rel_exists(
        &self,
        request: tonic::Request<proto::CheckRelExistsRequest>,
    ) -> std::result::Result<tonic::Response<proto::CheckRelExistsResponse>, tonic::Status> {
        let ttid = self.extract_ttid(request.metadata())?;
        let shard = self.extract_shard(request.metadata())?;
        let req: CheckRelExistsRequest = request.into_inner().try_into()?;

        let rel = convert_reltag(&req.rel);
        let span = tracing::info_span!("check_rel_exists", tenant_id = %ttid.tenant_id, timeline_id = %ttid.timeline_id, rel = %rel, req_lsn = %req.read_lsn.request_lsn);

        async {
            let timeline = self.get_timeline(ttid, shard).await?;
            let ctx = self.ctx.with_scope_timeline(&timeline);
            let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
            let lsn = Self::wait_or_get_last_lsn(
                &timeline,
                req.read_lsn.request_lsn,
                req.read_lsn
                    .not_modified_since_lsn
                    .unwrap_or(req.read_lsn.request_lsn),
                &latest_gc_cutoff_lsn,
                &ctx,
            )
            .await?;

            let exists = timeline.get_rel_exists(rel, Version::at(lsn), &ctx).await?;

            Ok(tonic::Response::new(proto::CheckRelExistsResponse {
                exists,
            }))
        }
        .instrument(span)
        .await
    }

    /// Returns size of a relation, as # of blocks
    async fn get_rel_size(
        &self,
        request: tonic::Request<proto::GetRelSizeRequest>,
    ) -> std::result::Result<tonic::Response<proto::GetRelSizeResponse>, tonic::Status> {
        let ttid = self.extract_ttid(request.metadata())?;
        let shard = self.extract_shard(request.metadata())?;
        let req: GetRelSizeRequest = request.into_inner().try_into()?;
        let rel = convert_reltag(&req.rel);

        let span = tracing::info_span!("get_rel_size", tenant_id = %ttid.tenant_id, timeline_id = %ttid.timeline_id, rel = %rel, req_lsn = %req.read_lsn.request_lsn);

        async {
            let timeline = self.get_timeline(ttid, shard).await?;
            let ctx = self.ctx.with_scope_timeline(&timeline);
            let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
            let lsn = Self::wait_or_get_last_lsn(
                &timeline,
                req.read_lsn.request_lsn,
                req.read_lsn
                    .not_modified_since_lsn
                    .unwrap_or(req.read_lsn.request_lsn),
                &latest_gc_cutoff_lsn,
                &ctx,
            )
            .await?;

            let num_blocks = timeline.get_rel_size(rel, Version::at(lsn), &ctx).await?;

            Ok(tonic::Response::new(proto::GetRelSizeResponse {
                num_blocks,
            }))
        }
        .instrument(span)
        .await
    }

    // TODO: take and emit model types
    async fn get_pages(
        &self,
        request: tonic::Request<tonic::Streaming<proto::GetPageRequest>>,
    ) -> Result<tonic::Response<Self::GetPagesStream>, tonic::Status> {
        let ttid = self.extract_ttid(request.metadata())?;
        let shard = self.extract_shard(request.metadata())?;
        let timeline = self.get_timeline(ttid, shard).await?;
        let ctx = self.ctx.with_scope_timeline(&timeline);
        let conf = self.conf;

        let mut request_stream = request.into_inner();

        let response_stream = try_stream! {
            while let Some(request) = request_stream.message().await? {

                let guard = timeline
                    .gate
                    .enter()
                    .or(Err(tonic::Status::unavailable("timeline is shutting down")))?;

                let request: GetPageRequest = request.try_into()?;
                let rel = convert_reltag(&request.rel);

                let span = tracing::info_span!("get_pages", tenant_id = %ttid.tenant_id, timeline_id = %ttid.timeline_id, shard_id = %shard, rel = %rel, req_lsn = %request.read_lsn.request_lsn);
                let result: Result<Vec<bytes::Bytes>, tonic::Status> = async {
                    let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
                    let lsn = Self::wait_or_get_last_lsn(
                        &timeline,
                        request.read_lsn.request_lsn,
                        request.read_lsn
                            .not_modified_since_lsn
                            .unwrap_or(request.read_lsn.request_lsn),
                        &latest_gc_cutoff_lsn,
                        &ctx,
                    )
                        .await?;

                    let io_concurrency = IoConcurrency::spawn_from_conf(conf.get_vectored_concurrent_io, guard);

                    // TODO: use get_rel_page_at_lsn_batched
                    let mut page_images = Vec::with_capacity(request.block_numbers.len());
                    for blkno in request.block_numbers {
                        let page_image = timeline
                            .get_rel_page_at_lsn(
                                rel,
                                blkno,
                                Version::at(lsn),
                                &ctx,
                                io_concurrency.clone(),
                            )
                            .await?;

                        page_images.push(page_image);
                    }
                    Ok(page_images)
                }
                .instrument(span)
                    .await;
                let page_images = result?;
                yield proto::GetPageResponse {
                    request_id: request.request_id,
                    status_code: proto::GetPageStatusCode::Ok as i32,
                    reason: "".to_string(),
                    page_image: page_images,
                };
            }
        };

        Ok(tonic::Response::new(
            Box::pin(response_stream) as Self::GetPagesStream
        ))
    }

    async fn get_db_size(
        &self,
        request: tonic::Request<proto::GetDbSizeRequest>,
    ) -> Result<tonic::Response<proto::GetDbSizeResponse>, tonic::Status> {
        let ttid = self.extract_ttid(request.metadata())?;
        let shard = self.extract_shard(request.metadata())?;
        let req: GetDbSizeRequest = request.into_inner().try_into()?;

        let span = tracing::info_span!("get_db_size", tenant_id = %ttid.tenant_id, timeline_id = %ttid.timeline_id, db_oid = %req.db_oid, req_lsn = %req.read_lsn.request_lsn);

        async {
            let timeline = self.get_timeline(ttid, shard).await?;
            let ctx = self.ctx.with_scope_timeline(&timeline);
            let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
            let lsn = Self::wait_or_get_last_lsn(
                &timeline,
                req.read_lsn.request_lsn,
                req.read_lsn
                    .not_modified_since_lsn
                    .unwrap_or(req.read_lsn.request_lsn),
                &latest_gc_cutoff_lsn,
                &ctx,
            )
            .await?;

            let total_blocks = timeline
                .get_db_size(DEFAULTTABLESPACE_OID, req.db_oid, Version::at(lsn), &ctx)
                .await?;

            Ok(tonic::Response::new(proto::GetDbSizeResponse {
                num_bytes: total_blocks as u64 * BLCKSZ as u64,
            }))
        }
        .instrument(span)
        .await
    }

    async fn get_base_backup(
        &self,
        request: tonic::Request<proto::GetBaseBackupRequest>,
    ) -> Result<tonic::Response<Self::GetBaseBackupStream>, tonic::Status> {
        let ttid = self.extract_ttid(request.metadata())?;
        let shard = self.extract_shard(request.metadata())?;
        let req: GetBaseBackupRequest = request.into_inner().try_into()?;

        let timeline = self.get_timeline(ttid, shard).await?;

        let ctx = self.ctx.with_scope_timeline(&timeline);
        let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
        let lsn = Self::wait_or_get_last_lsn(
            &timeline,
            req.read_lsn.request_lsn,
            req.read_lsn
                .not_modified_since_lsn
                .unwrap_or(req.read_lsn.request_lsn),
            &latest_gc_cutoff_lsn,
            &ctx,
        )
        .await?;

        let span = tracing::info_span!("get_base_backup", tenant_id = %ttid.tenant_id, timeline_id = %ttid.timeline_id, req_lsn = %req.read_lsn.request_lsn);

        tracing::info!("starting basebackup");

        #[allow(dead_code)]
        enum TestMode {
            /// Create real basebackup, in streaming fashion
            Streaming,
            /// Create real basebackup, but fully materialize it in the 'simplex' pipe buffer first
            Materialize,
            /// Create a dummy all-zeros basebackup, in streaming fashion
            DummyStreaming,
            /// Create a dummy all-zeros basebackup, but fully materialize it first
            DummyMaterialize,
        }
        let mode = TestMode::Streaming;

        let buf_size = match mode {
            TestMode::Streaming | TestMode::DummyStreaming => 64 * 1024,
            TestMode::Materialize | TestMode::DummyMaterialize => 64 * 1024 * 1024,
        };

        let (simplex_read, mut simplex_write) = tokio::io::simplex(buf_size);

        let basebackup_task = match mode {
            TestMode::DummyStreaming => {
                tokio::spawn(
                    async move {
                        // hold onto the guard for as long as the basebackup runs
                        let _latest_gc_cutoff_lsn = latest_gc_cutoff_lsn;

                        let zerosbuf: [u8; 1024] = [0; 1024];
                        let nbytes = 16900000;
                        let mut bytes_written = 0;
                        while bytes_written < nbytes {
                            let s = std::cmp::min(1024, nbytes - bytes_written);
                            let _ = simplex_write.write_all(&zerosbuf[0..s]).await;
                            bytes_written += s;
                        }
                        simplex_write
                            .shutdown()
                            .await
                            .context("shutdown of basebackup pipe")?;

                        Ok(())
                    }
                    .instrument(span),
                )
            }
            TestMode::DummyMaterialize => {
                let zerosbuf: [u8; 1024] = [0; 1024];
                let nbytes = 16900000;
                let mut bytes_written = 0;
                while bytes_written < nbytes {
                    let s = std::cmp::min(1024, nbytes - bytes_written);
                    let _ = simplex_write.write_all(&zerosbuf[0..s]).await;
                    bytes_written += s;
                }
                simplex_write
                    .shutdown()
                    .await
                    .expect("shutdown of basebackup pipe");
                tracing::info!("basebackup (dummy) materialized");
                let result = Ok(());

                tokio::spawn(std::future::ready(result))
            }
            TestMode::Materialize => {
                let result = basebackup::send_basebackup_tarball(
                    &mut simplex_write,
                    &timeline,
                    Some(lsn),
                    None,
                    false,
                    req.replica,
                    &ctx,
                )
                .await;
                simplex_write
                    .shutdown()
                    .await
                    .expect("shutdown of basebackup pipe");
                tracing::info!("basebackup materialized");

                // Launch a task that writes the basebackup tarball to the simplex pipe
                tokio::spawn(std::future::ready(result))
            }
            TestMode::Streaming => {
                tokio::spawn(
                    async move {
                        // hold onto the guard for as long as the basebackup runs
                        let _latest_gc_cutoff_lsn = latest_gc_cutoff_lsn;

                        let result = basebackup::send_basebackup_tarball(
                            &mut simplex_write,
                            &timeline,
                            Some(lsn),
                            None,
                            false,
                            req.replica,
                            &ctx,
                        )
                        .await;
                        simplex_write
                            .shutdown()
                            .await
                            .context("shutdown of basebackup pipe")?;
                        result
                    }
                    .instrument(span),
                )
            }
        };

        let response = new_basebackup_response_stream(simplex_read, basebackup_task);

        Ok(tonic::Response::new(response))
    }

    async fn get_slru_segment(
        &self,
        request: tonic::Request<proto::GetSlruSegmentRequest>,
    ) -> Result<tonic::Response<proto::GetSlruSegmentResponse>, tonic::Status> {
        let ttid = self.extract_ttid(request.metadata())?;
        let shard = self.extract_shard(request.metadata())?;
        let req: GetSlruSegmentRequest = request.into_inner().try_into()?;

        let span = tracing::info_span!("get_slru_segment", tenant_id = %ttid.tenant_id, timeline_id = %ttid.timeline_id, kind = %req.kind, segno = %req.segno, req_lsn = %req.read_lsn.request_lsn);

        async {
            let timeline = self.get_timeline(ttid, shard).await?;
            let ctx = self.ctx.with_scope_timeline(&timeline);
            let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
            let lsn = Self::wait_or_get_last_lsn(
                &timeline,
                req.read_lsn.request_lsn,
                req.read_lsn
                    .not_modified_since_lsn
                    .unwrap_or(req.read_lsn.request_lsn),
                &latest_gc_cutoff_lsn,
                &ctx,
            )
            .await?;

            let segment = timeline
                .get_slru_segment(req.kind, req.segno, lsn, &ctx)
                .await?;

            Ok(tonic::Response::new(proto::GetSlruSegmentResponse {
                segment,
            }))
        }
        .instrument(span)
        .await
    }
}

/// NB: this is a different value than [`crate::http::routes::ACTIVE_TENANT_TIMEOUT`].
/// NB: and also different from page_service::ACTIVE_TENANT_TIMEOUT
const ACTIVE_TENANT_TIMEOUT: Duration = Duration::from_millis(30000);

impl PageServiceService {
    async fn get_timeline(
        &self,
        ttid: TenantTimelineId,
        shard: ShardIndex,
    ) -> Result<Arc<Timeline>, tonic::Status> {
        let timeout = ACTIVE_TENANT_TIMEOUT;
        let wait_start = Instant::now();
        let deadline = wait_start + timeout;

        let tenant_shard = loop {
            let resolved = self
                .tenant_mgr
                .resolve_attached_shard(&ttid.tenant_id, ShardSelector::Known(shard));

            match resolved {
                ShardResolveResult::Found(tenant_shard) => break tenant_shard,
                ShardResolveResult::NotFound => {
                    return Err(tonic::Status::not_found("tenant not found"));
                }
                ShardResolveResult::InProgress(barrier) => {
                    // We can't authoritatively answer right now: wait for InProgress state
                    // to end, then try again
                    tokio::select! {
                        _  = barrier.wait() => {
                            // The barrier completed: proceed around the loop to try looking up again
                        },
                        _ = tokio::time::sleep(deadline.duration_since(Instant::now())) => {
                            return Err(tonic::Status::unavailable("tenant is in InProgress state"));
                        }
                    }
                }
            }
        };

        tracing::debug!("Waiting for tenant to enter active state...");
        tenant_shard
            .wait_to_become_active(deadline.duration_since(Instant::now()))
            .await
            .map_err(|e| {
                tonic::Status::unavailable(format!("tenant is not in active state: {e}"))
            })?;

        let timeline = tenant_shard
            .get_timeline(ttid.timeline_id, true)
            .map_err(|e| tonic::Status::unavailable(format!("could not get timeline: {e}")))?;

        // FIXME: need to do something with the 'gate' here?

        Ok(timeline)
    }

    /// Extract TenantTimelineId from the request metadata
    ///
    /// Note: the interceptor has already authenticated the request
    ///
    /// TOOD: Could we use "binary" metadata for these, for efficiency? gRPC has such a concept
    fn extract_ttid(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<TenantTimelineId, tonic::Status> {
        let tenant_id = metadata
            .get("neon-tenant-id")
            .ok_or(tonic::Status::invalid_argument(
                "neon-tenant-id metadata missing",
            ))?;
        let tenant_id = tenant_id.to_str().map_err(|_| {
            tonic::Status::invalid_argument("invalid UTF-8 characters in neon-tenant-id metadata")
        })?;
        let tenant_id = TenantId::from_str(tenant_id)
            .map_err(|_| tonic::Status::invalid_argument("invalid neon-tenant-id metadata"))?;

        let timeline_id =
            metadata
                .get("neon-timeline-id")
                .ok_or(tonic::Status::invalid_argument(
                    "neon-timeline-id metadata missing",
                ))?;
        let timeline_id = timeline_id.to_str().map_err(|_| {
            tonic::Status::invalid_argument("invalid UTF-8 characters in neon-timeline-id metadata")
        })?;
        let timeline_id = TimelineId::from_str(timeline_id)
            .map_err(|_| tonic::Status::invalid_argument("invalid neon-timelineid metadata"))?;

        Ok(TenantTimelineId::new(tenant_id, timeline_id))
    }

    /// Extract ShardSelector from the request metadata.
    fn extract_shard(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<ShardIndex, tonic::Status> {
        let shard_id = metadata
            .get("neon-shard-id")
            .ok_or(tonic::Status::invalid_argument(
                "neon-shard-id metadata missing",
            ))?
            .to_str()
            .map_err(|_| {
                tonic::Status::invalid_argument(
                    "invalid UTF-8 characters in shard-selector metadata",
                )
            })?;
        ShardIndex::from_str(shard_id)
            .map_err(|err| tonic::Status::invalid_argument(format!("invalid neon-shard-id: {err}")))
    }

    // XXX: copied from PageServerHandler
    async fn wait_or_get_last_lsn(
        timeline: &Timeline,
        request_lsn: Lsn,
        not_modified_since: Lsn,
        latest_gc_cutoff_lsn: &RcuReadGuard<Lsn>,
        ctx: &RequestContext,
    ) -> Result<Lsn, tonic::Status> {
        let last_record_lsn = timeline.get_last_record_lsn();

        // Sanity check the request
        if request_lsn < not_modified_since {
            return Err(tonic::Status::invalid_argument(format!(
                "invalid request with request LSN {} and not_modified_since {}",
                request_lsn, not_modified_since,
            )));
        }

        // Check explicitly for INVALID just to get a less scary error message if the request is obviously bogus
        if request_lsn == Lsn::INVALID {
            return Err(tonic::Status::invalid_argument("invalid LSN(0) in request"));
        }

        // Clients should only read from recent LSNs on their timeline, or from locations holding an LSN lease.
        //
        // We may have older data available, but we make a best effort to detect this case and return an error,
        // to distinguish a misbehaving client (asking for old LSN) from a storage issue (data missing at a legitimate LSN).
        if request_lsn < **latest_gc_cutoff_lsn && !timeline.is_gc_blocked_by_lsn_lease_deadline() {
            let gc_info = &timeline.gc_info.read().unwrap();
            if !gc_info.lsn_covered_by_lease(request_lsn) {
                return Err(tonic::Status::not_found(format!(
                    "tried to request a page version that was garbage collected. requested at {} gc cutoff {}",
                    request_lsn, **latest_gc_cutoff_lsn
                )));
            }
        }

        // Wait for WAL up to 'not_modified_since' to arrive, if necessary
        if not_modified_since > last_record_lsn {
            timeline
                .wait_lsn(
                    not_modified_since,
                    crate::tenant::timeline::WaitLsnWaiter::PageService,
                    WaitLsnTimeout::Default,
                    ctx,
                )
                .await
                .map_err(|_| {
                    tonic::Status::unavailable("not_modified_since LSN not arrived yet")
                })?;
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
}

#[derive(Clone)]
pub struct PageServiceAuthenticator {
    pub auth: Option<Arc<SwappableJwtAuth>>,
    pub auth_type: AuthType,
}

impl tonic::service::Interceptor for PageServiceAuthenticator {
    fn call(
        &mut self,
        req: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        // Check the tenant_id in any case
        let tenant_id =
            req.metadata()
                .get("neon-tenant-id")
                .ok_or(tonic::Status::invalid_argument(
                    "neon-tenant-id metadata missing",
                ))?;
        let tenant_id = tenant_id.to_str().map_err(|_| {
            tonic::Status::invalid_argument("invalid UTF-8 characters in neon-tenant-id metadata")
        })?;
        let tenant_id = TenantId::from_str(tenant_id)
            .map_err(|_| tonic::Status::invalid_argument("invalid neon-tenant-id metadata"))?;

        // when accessing management api supply None as an argument
        // when using to authorize tenant pass corresponding tenant id
        let auth = if let Some(auth) = &self.auth {
            auth
        } else {
            // auth is set to Trust, nothing to check so just return ok
            return Ok(req);
        };

        let authorization = req
            .metadata()
            .get("authorization")
            .ok_or(tonic::Status::unauthenticated("no authorization header"))?
            .to_str()
            .map_err(|_| {
                tonic::Status::invalid_argument(
                    "invalid UTF-8 characters in authorization metadata",
                )
            })?;
        if &authorization[0..7] != "Bearer " {
            return Err(tonic::Status::unauthenticated(
                "authorization header must start with 'Bearer '",
            ));
        }
        let jwt = &authorization[7..].trim();

        let jwtdata: TokenData<utils::auth::Claims> = auth
            .decode(jwt)
            .map_err(|err| tonic::Status::unauthenticated(format!("invalid JWT token: {}", err)))?;
        let claims: Claims = jwtdata.claims;

        if matches!(claims.scope, utils::auth::Scope::Tenant) && claims.tenant_id.is_none() {
            return Err(tonic::Status::unauthenticated(
                "jwt token scope is Tenant, but tenant id is missing",
            ));
        }

        debug!(
            "jwt scope check succeeded for scope: {:#?} by tenant id: {:?}",
            claims.scope, claims.tenant_id,
        );

        // The token is valid. Check if it's allowed to access the tenant ID
        // given in the request.

        check_permission(&claims, Some(tenant_id))
            .map_err(|err| tonic::Status::permission_denied(err.to_string()))?;

        // All checks out
        Ok(req)
    }
}

/// Stream of GetBaseBackupResponseChunk messages.
///
/// The first part of the Chain chunks the tarball. The second part checks the return value
/// of the send_basebackup_tarball Future that created the tarball.
type GetBaseBackupStream = futures::stream::Chain<BasebackupChunkedStream, CheckResultStream>;

fn new_basebackup_response_stream(
    simplex_read: ReadHalf<SimplexStream>,
    basebackup_task: JoinHandle<Result<(), BasebackupError>>,
) -> GetBaseBackupStream {
    let framed = FramedRead::new(simplex_read, GetBaseBackupResponseDecoder {});

    framed.chain(CheckResultStream { basebackup_task })
}

/// Stream that uses GetBaseBackupResponseDecoder
type BasebackupChunkedStream =
    tokio_util::codec::FramedRead<ReadHalf<SimplexStream>, GetBaseBackupResponseDecoder>;

struct GetBaseBackupResponseDecoder;
impl Decoder for GetBaseBackupResponseDecoder {
    type Item = proto::GetBaseBackupResponseChunk;
    type Error = tonic::Status;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 64 * 1024 {
            return Ok(None);
        }

        let item = proto::GetBaseBackupResponseChunk {
            chunk: bytes::Bytes::from(std::mem::take(src)),
        };

        Ok(Some(item))
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let item = proto::GetBaseBackupResponseChunk {
            chunk: bytes::Bytes::from(std::mem::take(src)),
        };

        Ok(Some(item))
    }
}

struct CheckResultStream {
    basebackup_task: tokio::task::JoinHandle<Result<(), BasebackupError>>,
}
impl futures::Stream for CheckResultStream {
    type Item = Result<proto::GetBaseBackupResponseChunk, tonic::Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        ctx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let task = Pin::new(&mut self.basebackup_task);
        match task.poll(ctx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(Ok(()))) => Poll::Ready(None),
            Poll::Ready(Ok(Err(basebackup_err))) => {
                error!(error=%basebackup_err, "error getting basebackup");
                Poll::Ready(Some(Err(tonic::Status::internal(
                    "could not get basebackup",
                ))))
            }
            Poll::Ready(Err(join_err)) => {
                error!(error=%join_err, "JoinError getting basebackup");
                Poll::Ready(Some(Err(tonic::Status::internal(
                    "could not get basebackup",
                ))))
            }
        }
    }
}
