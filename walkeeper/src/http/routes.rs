use hyper::{Body, Request, Response, StatusCode};
use serde::Serialize;
use serde::Serializer;
use std::fmt::Display;
use std::sync::Arc;
use zenith_utils::http::{RequestExt, RouterBuilder};
use zenith_utils::lsn::Lsn;
use zenith_utils::zid::ZNodeId;
use zenith_utils::zid::ZTenantTimelineId;

use crate::safekeeper::Term;
use crate::safekeeper::TermHistory;
use crate::timeline::CreateControlFile;
use crate::timeline::GlobalTimelines;
use crate::SafeKeeperConf;
use zenith_utils::http::endpoint;
use zenith_utils::http::error::ApiError;
use zenith_utils::http::json::json_response;
use zenith_utils::http::request::parse_request_param;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

#[derive(Debug, Serialize)]
struct SafekeeperStatus {
    id: ZNodeId,
}

/// Healthcheck handler.
async fn status_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let conf = get_conf(&request);
    let status = SafekeeperStatus { id: conf.my_id };
    Ok(json_response(StatusCode::OK, status)?)
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
    commit_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    truncate_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    flush_lsn: Lsn,
}

/// Report info about timeline.
async fn timeline_status_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let zttid = ZTenantTimelineId::new(
        parse_request_param(&request, "tenant_id")?,
        parse_request_param(&request, "timeline_id")?,
    );

    let tli = GlobalTimelines::get(get_conf(&request), zttid, CreateControlFile::False)
        .map_err(ApiError::from_err)?;
    let sk_state = tli.get_info();
    let flush_lsn = tli.get_end_of_wal();

    let acc_state = AcceptorStateStatus {
        term: sk_state.acceptor_state.term,
        epoch: sk_state.acceptor_state.get_epoch(flush_lsn),
        term_history: sk_state.acceptor_state.term_history,
    };

    let status = TimelineStatus {
        tenant_id: zttid.tenant_id,
        timeline_id: zttid.timeline_id,
        acceptor_state: acc_state,
        commit_lsn: sk_state.commit_lsn,
        truncate_lsn: sk_state.truncate_lsn,
        flush_lsn,
    };
    Ok(json_response(StatusCode::OK, status)?)
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
}
