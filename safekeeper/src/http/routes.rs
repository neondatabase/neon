use etcd_broker::SkTimelineInfo;
use hyper::{Body, Request, Response, StatusCode};

use serde::Serialize;
use serde::Serializer;
use std::fmt::Display;
use std::sync::Arc;

use crate::safekeeper::Term;
use crate::safekeeper::TermHistory;
use crate::timeline::GlobalTimelines;
use crate::SafeKeeperConf;
use utils::{
    http::{
        endpoint,
        error::ApiError,
        json::{json_request, json_response},
        request::parse_request_param,
        RequestExt, RouterBuilder,
    },
    lsn::Lsn,
    zid::{ZNodeId, ZTenantId, ZTenantTimelineId, ZTimelineId},
};

use super::models::TimelineCreateRequest;

#[derive(Debug, Serialize)]
struct SafekeeperStatus {
    id: ZNodeId,
}

/// Healthcheck handler.
async fn status_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
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
    tenant_id: ZTenantId,
    #[serde(serialize_with = "display_serialize")]
    timeline_id: ZTimelineId,
    acceptor_state: AcceptorStateStatus,
    #[serde(serialize_with = "display_serialize")]
    timeline_start_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    local_start_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    commit_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    s3_wal_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    peer_horizon_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    remote_consistent_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    flush_lsn: Lsn,
}

/// Report info about timeline.
async fn timeline_status_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let zttid = ZTenantTimelineId::new(
        parse_request_param(&request, "tenant_id")?,
        parse_request_param(&request, "timeline_id")?,
    );

    let tli = GlobalTimelines::get(get_conf(&request), zttid, false).map_err(ApiError::from_err)?;
    let (inmem, state) = tli.get_state();
    let flush_lsn = tli.get_end_of_wal();

    let acc_state = AcceptorStateStatus {
        term: state.acceptor_state.term,
        epoch: state.acceptor_state.get_epoch(flush_lsn),
        term_history: state.acceptor_state.term_history,
    };

    // Note: we report in memory values which can be lost.
    let status = TimelineStatus {
        tenant_id: zttid.tenant_id,
        timeline_id: zttid.timeline_id,
        acceptor_state: acc_state,
        timeline_start_lsn: state.timeline_start_lsn,
        local_start_lsn: state.local_start_lsn,
        commit_lsn: inmem.commit_lsn,
        s3_wal_lsn: inmem.s3_wal_lsn,
        peer_horizon_lsn: inmem.peer_horizon_lsn,
        remote_consistent_lsn: inmem.remote_consistent_lsn,
        flush_lsn,
    };
    json_response(StatusCode::OK, status)
}

async fn timeline_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let request_data: TimelineCreateRequest = json_request(&mut request).await?;

    let zttid = ZTenantTimelineId {
        tenant_id: request_data.tenant_id,
        timeline_id: request_data.timeline_id,
    };
    GlobalTimelines::create(get_conf(&request), zttid, request_data.peer_ids)
        .map_err(ApiError::from_err)?;

    json_response(StatusCode::CREATED, ())
}

/// Used only in tests to hand craft required data.
async fn record_safekeeper_info(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let zttid = ZTenantTimelineId::new(
        parse_request_param(&request, "tenant_id")?,
        parse_request_param(&request, "timeline_id")?,
    );
    let safekeeper_info: SkTimelineInfo = json_request(&mut request).await?;

    let tli = GlobalTimelines::get(get_conf(&request), zttid, false).map_err(ApiError::from_err)?;
    tli.record_safekeeper_info(&safekeeper_info, ZNodeId(1))?;

    json_response(StatusCode::OK, ())
}

/// Safekeeper http router.
pub fn make_router(conf: SafeKeeperConf) -> RouterBuilder<hyper::Body, ApiError> {
    let router = endpoint::make_router();
    router
        .data(Arc::new(conf))
        .get("/v1/status", status_handler)
        .get(
            "/v1/timeline/:tenant_id/:timeline_id",
            timeline_status_handler,
        )
        .post("/v1/timeline", timeline_create_handler)
        // for tests
        .post(
            "/v1/record_safekeeper_info/:tenant_id/:timeline_id",
            record_safekeeper_info,
        )
}
