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

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use crate::TenantManager;
use crate::auth::check_permission;
use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::tenant::Timeline;
use crate::tenant::mgr::ShardResolveResult;
use crate::tenant::mgr::ShardSelector;
use crate::tenant::storage_layer::IoConcurrency;
use crate::tenant::timeline::WaitLsnTimeout;
use tokio_util::sync::CancellationToken;

use pageserver_data_api::model;
use pageserver_data_api::proto::page_service_server::PageService;

use tracing::Instrument;
use tracing::debug;
use utils::auth::SwappableJwtAuth;

use utils::id::{TenantId, TenantTimelineId, TimelineId};
use utils::lsn::Lsn;
use utils::simple_rcu::RcuReadGuard;

use crate::tenant::PageReconstructError;

use postgres_ffi::BLCKSZ;

use tonic;

use pageserver_api::key::rel_block_to_key;

use crate::pgdatadir_mapping::Version;
use postgres_ffi::pg_constants::DEFAULTTABLESPACE_OID;

use postgres_backend::AuthType;

pub use pageserver_data_api::proto;

pub struct PageServiceService {
    pub conf: &'static PageServerConf,

    pub cancel: CancellationToken,

    pub tenant_mgr: Arc<TenantManager>,

    pub ctx: Arc<RequestContext>,
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

fn convert_reltag(value: &model::RelTag) -> pageserver_api::reltag::RelTag {
    pageserver_api::reltag::RelTag {
        spcnode: value.spc_oid,
        dbnode: value.db_oid,
        relnode: value.rel_number,
        forknum: value.fork_number,
    }
}

#[tonic::async_trait]
impl PageService for PageServiceService {
    async fn rel_exists(
        &self,
        request: tonic::Request<proto::RelExistsRequest>,
    ) -> std::result::Result<tonic::Response<proto::RelExistsResponse>, tonic::Status> {
        let ttid = self.extract_ttid(request.metadata())?;
        let req: model::RelExistsRequest = request.get_ref().try_into()?;

        let rel = convert_reltag(&req.rel);
        let span = tracing::info_span!("rel_exists", tenant_id = %ttid.tenant_id, timeline_id = %ttid.timeline_id, rel = %rel, req_lsn = %req.common.request_lsn);

        async {
            let timeline = self.get_timeline(ttid, ShardSelector::Zero).await?;
            let ctx = self.ctx.with_scope_timeline(&timeline);
            let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
            let lsn = Self::wait_or_get_last_lsn(
                &timeline,
                req.common.request_lsn,
                req.common.not_modified_since_lsn,
                &latest_gc_cutoff_lsn,
                &ctx,
            )
            .await?;

            let exists = timeline
                .get_rel_exists(rel, Version::Lsn(lsn), &ctx)
                .await?;

            Ok(tonic::Response::new(proto::RelExistsResponse { exists }))
        }
        .instrument(span)
        .await
    }

    /// Returns size of a relation, as # of blocks
    async fn rel_size(
        &self,
        request: tonic::Request<proto::RelSizeRequest>,
    ) -> std::result::Result<tonic::Response<proto::RelSizeResponse>, tonic::Status> {
        let ttid = self.extract_ttid(request.metadata())?;
        let req: model::RelSizeRequest = request.get_ref().try_into()?;
        let rel = convert_reltag(&req.rel);

        let span = tracing::info_span!("rel_size", tenant_id = %ttid.tenant_id, timeline_id = %ttid.timeline_id, rel = %rel, req_lsn = %req.common.request_lsn);

        async {
            let timeline = self.get_timeline(ttid, ShardSelector::Zero).await?;
            let ctx = self.ctx.with_scope_timeline(&timeline);
            let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
            let lsn = Self::wait_or_get_last_lsn(
                &timeline,
                req.common.request_lsn,
                req.common.not_modified_since_lsn,
                &latest_gc_cutoff_lsn,
                &ctx,
            )
            .await?;

            let num_blocks = timeline.get_rel_size(rel, Version::Lsn(lsn), &ctx).await?;

            Ok(tonic::Response::new(proto::RelSizeResponse { num_blocks }))
        }
        .instrument(span)
        .await
    }

    async fn get_page(
        &self,
        request: tonic::Request<proto::GetPageRequest>,
    ) -> std::result::Result<tonic::Response<proto::GetPageResponse>, tonic::Status> {
        let ttid = self.extract_ttid(request.metadata())?;
        let req: model::GetPageRequest = request.get_ref().try_into()?;

        // Calculate shard number.
        //
        // FIXME: this should probably be part of the data_api crate.
        let rel = convert_reltag(&req.rel);
        let key = rel_block_to_key(rel, req.block_number);
        let timeline = self.get_timeline(ttid, ShardSelector::Page(key)).await?;

        let ctx = self.ctx.with_scope_timeline(&timeline);
        let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
        let lsn = Self::wait_or_get_last_lsn(
            &timeline,
            req.common.request_lsn,
            req.common.not_modified_since_lsn,
            &latest_gc_cutoff_lsn,
            &ctx,
        )
        .await?;

        let shard_id = timeline.tenant_shard_id.shard_number;
        let span = tracing::info_span!("get_page", tenant_id = %ttid.tenant_id, shard_id = %shard_id, timeline_id = %ttid.timeline_id, rel = %rel, block_number = %req.block_number, req_lsn = %req.common.request_lsn);

        async {
            let gate_guard = match timeline.gate.enter() {
                Ok(guard) => guard,
                Err(_) => {
                    return Err(tonic::Status::unavailable("timeline is shutting down"));
                }
            };

            let io_concurrency = IoConcurrency::spawn_from_conf(self.conf, gate_guard);

            let page_image = timeline
                .get_rel_page_at_lsn(
                    rel,
                    req.block_number,
                    Version::Lsn(lsn),
                    &ctx,
                    io_concurrency,
                )
                .await?;

            Ok(tonic::Response::new(proto::GetPageResponse {
                page_image: page_image.to_vec(),
            }))
        }
        .instrument(span)
        .await
    }

    async fn db_size(
        &self,
        request: tonic::Request<proto::DbSizeRequest>,
    ) -> Result<tonic::Response<proto::DbSizeResponse>, tonic::Status> {
        let ttid = self.extract_ttid(request.metadata())?;
        let req: model::DbSizeRequest = request.get_ref().try_into()?;

        let span = tracing::info_span!("get_page", tenant_id = %ttid.tenant_id, timeline_id = %ttid.timeline_id, db_oid = %req.db_oid, req_lsn = %req.common.request_lsn);

        async {
            let timeline = self.get_timeline(ttid, ShardSelector::Zero).await?;
            let ctx = self.ctx.with_scope_timeline(&timeline);
            let latest_gc_cutoff_lsn = timeline.get_applied_gc_cutoff_lsn();
            let lsn = Self::wait_or_get_last_lsn(
                &timeline,
                req.common.request_lsn,
                req.common.not_modified_since_lsn,
                &latest_gc_cutoff_lsn,
                &ctx,
            )
            .await?;

            let total_blocks = timeline
                .get_db_size(DEFAULTTABLESPACE_OID, req.db_oid, Version::Lsn(lsn), &ctx)
                .await?;

            Ok(tonic::Response::new(proto::DbSizeResponse {
                num_bytes: total_blocks as u64 * BLCKSZ as u64,
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
        shard_selector: ShardSelector,
    ) -> Result<Arc<Timeline>, tonic::Status> {
        let timeout = ACTIVE_TENANT_TIMEOUT;
        let wait_start = Instant::now();
        let deadline = wait_start + timeout;

        let tenant_shard = loop {
            let resolved = self
                .tenant_mgr
                .resolve_attached_shard(&ttid.tenant_id, shard_selector);

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

        let jwt = req
            .metadata()
            .get("neon-auth-token")
            .ok_or(tonic::Status::unauthenticated("no neon-auth-token"))?;
        let jwt = jwt.to_str().map_err(|_| {
            tonic::Status::invalid_argument("invalid UTF-8 characters in neon-auth-token metadata")
        })?;

        let jwt = auth
            .decode(jwt)
            .map_err(|err| tonic::Status::unauthenticated(format!("invalid JWT token: {}", err)))?;
        let claims = jwt.claims;

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
