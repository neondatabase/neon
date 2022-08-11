use hyper::{Body, Request, Response, StatusCode, Uri};

use anyhow::Context;
use once_cell::sync::Lazy;
use postgres_ffi::WAL_SEGMENT_SIZE;
use serde::Serialize;
use serde::Serializer;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::Arc;
use tokio::task::JoinError;

use crate::safekeeper::ServerInfo;
use crate::safekeeper::Term;
use crate::safekeeper::TermHistory;

use crate::timelines_global_map::TimelineDeleteForceResult;
use crate::GlobalTimelines;
use crate::SafeKeeperConf;
use etcd_broker::subscription_value::SkTimelineInfo;
use utils::{
    auth::JwtAuth,
    http::{
        endpoint::{self, auth_middleware, check_permission_with},
        error::ApiError,
        json::{json_request, json_response},
        request::{ensure_no_body, parse_request_param},
        RequestExt, RouterBuilder,
    },
    id::{NodeId, TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
};

use super::models::TimelineCreateRequest;

#[derive(Debug, Serialize)]
struct SafekeeperStatus {
    id: NodeId,
}

/// Healthcheck handler.
async fn status_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;
    let conf = get_conf(&request);
    let status = SafekeeperStatus { id: conf.my_id };
    json_response(StatusCode::OK, status)
}

fn get_conf(request: &Request<Body>) -> &SafeKeeperConf {
    request
        .data::<Arc<SafeKeeperConf>>()
        .expect("unknown state type")
        .as_ref()
}

/// Serialize through Display trait.
fn display_serialize<S, F>(z: &F, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    F: Display,
{
    s.serialize_str(&format!("{}", z))
}

/// Augment AcceptorState with epoch for convenience
#[derive(Debug, Serialize)]
struct AcceptorStateStatus {
    term: Term,
    epoch: Term,
    term_history: TermHistory,
}

/// Info about timeline on safekeeper ready for reporting.
#[derive(Debug, Serialize)]
struct TimelineStatus {
    #[serde(serialize_with = "display_serialize")]
    tenant_id: TenantId,
    #[serde(serialize_with = "display_serialize")]
    timeline_id: TimelineId,
    acceptor_state: AcceptorStateStatus,
    pg_info: ServerInfo,
    #[serde(serialize_with = "display_serialize")]
    flush_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    timeline_start_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    local_start_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    commit_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    backup_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    peer_horizon_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    remote_consistent_lsn: Lsn,
}

fn check_permission(request: &Request<Body>, tenant_id: Option<TenantId>) -> Result<(), ApiError> {
    check_permission_with(request, |claims| {
        crate::auth::check_permission(claims, tenant_id)
    })
}

/// Report info about timeline.
async fn timeline_status_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let ttid = TenantTimelineId::new(
        parse_request_param(&request, "tenant_id")?,
        parse_request_param(&request, "timeline_id")?,
    );
    check_permission(&request, Some(ttid.tenant_id))?;

    let tli = GlobalTimelines::get(ttid)
        // FIXME: Currently, the only errors from `GlobalTimelines::get` will be client errors
        // because the provided timeline isn't there. However, the method can in theory change and
        // fail from internal errors later. Remove this comment once it the method returns
        // something other than `anyhow::Result`.
        .map_err(ApiError::InternalServerError)?;
    let (inmem, state) = tli.get_state();
    let flush_lsn = tli.get_flush_lsn();

    let acc_state = AcceptorStateStatus {
        term: state.acceptor_state.term,
        epoch: state.acceptor_state.get_epoch(flush_lsn),
        term_history: state.acceptor_state.term_history,
    };

    // Note: we report in memory values which can be lost.
    let status = TimelineStatus {
        tenant_id: ttid.tenant_id,
        timeline_id: ttid.timeline_id,
        acceptor_state: acc_state,
        pg_info: state.server,
        flush_lsn,
        timeline_start_lsn: state.timeline_start_lsn,
        local_start_lsn: state.local_start_lsn,
        commit_lsn: inmem.commit_lsn,
        backup_lsn: inmem.backup_lsn,
        peer_horizon_lsn: inmem.peer_horizon_lsn,
        remote_consistent_lsn: inmem.remote_consistent_lsn,
    };
    json_response(StatusCode::OK, status)
}

async fn timeline_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let request_data: TimelineCreateRequest = json_request(&mut request).await?;

    let ttid = TenantTimelineId {
        tenant_id: request_data.tenant_id,
        timeline_id: request_data.timeline_id,
    };
    check_permission(&request, Some(ttid.tenant_id))?;

    let server_info = ServerInfo {
        pg_version: request_data.pg_version,
        system_id: request_data.system_id.unwrap_or(0),
        wal_seg_size: request_data.wal_seg_size.unwrap_or(WAL_SEGMENT_SIZE as u32),
    };
    let local_start_lsn = request_data.local_start_lsn.unwrap_or_else(|| {
        request_data
            .commit_lsn
            .segment_lsn(server_info.wal_seg_size as usize)
    });
    tokio::task::spawn_blocking(move || {
        GlobalTimelines::create(ttid, server_info, request_data.commit_lsn, local_start_lsn)
    })
    .await
    .map_err(|e| ApiError::InternalServerError(e.into()))?
    .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

/// Deactivates the timeline and removes its data directory.
async fn timeline_delete_force_handler(
    mut request: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let ttid = TenantTimelineId::new(
        parse_request_param(&request, "tenant_id")?,
        parse_request_param(&request, "timeline_id")?,
    );
    check_permission(&request, Some(ttid.tenant_id))?;
    ensure_no_body(&mut request).await?;
    let resp = tokio::task::spawn_blocking(move || {
        // FIXME: `delete_force` can fail from both internal errors and bad requests. Add better
        // error handling here when we're able to.
        GlobalTimelines::delete_force(&ttid).map_err(ApiError::InternalServerError)
    })
    .await
    .map_err(|e: JoinError| ApiError::InternalServerError(e.into()))??;
    json_response(StatusCode::OK, resp)
}

/// Deactivates all timelines for the tenant and removes its data directory.
/// See `timeline_delete_force_handler`.
async fn tenant_delete_force_handler(
    mut request: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;
    ensure_no_body(&mut request).await?;
    let delete_info = tokio::task::spawn_blocking(move || {
        // FIXME: `delete_force_all_for_tenant` can return an error for multiple different reasons;
        // Using an `InternalServerError` should be fixed when the types support it
        GlobalTimelines::delete_force_all_for_tenant(&tenant_id)
            .map_err(ApiError::InternalServerError)
    })
    .await
    .map_err(|e: JoinError| ApiError::InternalServerError(e.into()))??;
    json_response(
        StatusCode::OK,
        delete_info
            .iter()
            .map(|(ttid, resp)| (format!("{}", ttid.timeline_id), *resp))
            .collect::<HashMap<String, TimelineDeleteForceResult>>(),
    )
}

/// Used only in tests to hand craft required data.
async fn record_safekeeper_info(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let ttid = TenantTimelineId::new(
        parse_request_param(&request, "tenant_id")?,
        parse_request_param(&request, "timeline_id")?,
    );
    check_permission(&request, Some(ttid.tenant_id))?;
    let safekeeper_info: SkTimelineInfo = json_request(&mut request).await?;

    let tli = GlobalTimelines::get(ttid)
        // `GlobalTimelines::get` returns an error when it can't find the timeline.
        .with_context(|| {
            format!(
                "Couldn't get timeline {} for tenant {}",
                ttid.timeline_id, ttid.tenant_id
            )
        })
        .map_err(ApiError::NotFound)?;
    tli.record_safekeeper_info(&safekeeper_info, NodeId(1))
        .await
        .map_err(ApiError::InternalServerError)?;

    json_response(StatusCode::OK, ())
}

/// Safekeeper http router.
pub fn make_router(
    conf: SafeKeeperConf,
    auth: Option<Arc<JwtAuth>>,
) -> RouterBuilder<hyper::Body, ApiError> {
    let mut router = endpoint::make_router();
    if auth.is_some() {
        router = router.middleware(auth_middleware(|request| {
            #[allow(clippy::mutable_key_type)]
            static ALLOWLIST_ROUTES: Lazy<HashSet<Uri>> =
                Lazy::new(|| ["/v1/status"].iter().map(|v| v.parse().unwrap()).collect());
            if ALLOWLIST_ROUTES.contains(request.uri()) {
                None
            } else {
                // Option<Arc<JwtAuth>> is always provided as data below, hence unwrap().
                request.data::<Option<Arc<JwtAuth>>>().unwrap().as_deref()
            }
        }))
    }

    // NB: on any changes do not forget to update the OpenAPI spec
    // located nearby (/safekeeper/src/http/openapi_spec.yaml).
    router
        .data(Arc::new(conf))
        .data(auth)
        .get("/v1/status", status_handler)
        // Will be used in the future instead of implicit timeline creation
        .post("/v1/tenant/timeline", timeline_create_handler)
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id",
            timeline_status_handler,
        )
        .delete(
            "/v1/tenant/:tenant_id/timeline/:timeline_id",
            timeline_delete_force_handler,
        )
        .delete("/v1/tenant/:tenant_id", tenant_delete_force_handler)
        // for tests
        .post(
            "/v1/record_safekeeper_info/:tenant_id/:timeline_id",
            record_safekeeper_info,
        )
}
