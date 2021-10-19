use hyper::{Body, Request, Response, StatusCode};
use routerify::ext::RequestExt;
use routerify::RouterBuilder;
use serde::Serialize;
use serde::Serializer;
use std::fmt::Display;
use std::sync::Arc;
use zenith_utils::lsn::Lsn;

use crate::safekeeper::AcceptorState;
use crate::timeline::CreateControlFile;
use crate::timeline::GlobalTimelines;
use crate::WalAcceptorConf;
use zenith_utils::http::endpoint;
use zenith_utils::http::error::ApiError;
use zenith_utils::http::json::json_response;
use zenith_utils::http::request::parse_request_param;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

/// Healthcheck handler.
async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    Ok(json_response(StatusCode::OK, "")?)
}

fn get_conf(request: &Request<Body>) -> &WalAcceptorConf {
    request
        .data::<Arc<WalAcceptorConf>>()
        .expect("unknown state type")
        .as_ref()
}

fn display_serialize<S, F>(z: &F, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    F: Display,
{
    s.serialize_str(&format!("{}", z))
}

/// Info about timeline on safekeeper ready for reporting.
#[derive(Debug, Serialize)]
struct TimelineStatus {
    #[serde(serialize_with = "display_serialize")]
    tenant_id: ZTenantId,
    #[serde(serialize_with = "display_serialize")]
    timeline_id: ZTimelineId,
    acceptor_state: AcceptorState,
    #[serde(serialize_with = "display_serialize")]
    commit_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    truncate_lsn: Lsn,
    #[serde(serialize_with = "display_serialize")]
    flush_lsn: Lsn,
}

/// Report info about timeline.
async fn timeline_status_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;

    let tli = GlobalTimelines::get(
        get_conf(&request),
        tenant_id,
        timeline_id,
        CreateControlFile::False,
    )
    .map_err(ApiError::from_err)?;
    let sk_state = tli.get_info();
    let (flush_lsn, _) = tli.get_end_of_wal();

    let status = TimelineStatus {
        tenant_id,
        timeline_id,
        acceptor_state: sk_state.acceptor_state,
        commit_lsn: sk_state.commit_lsn,
        truncate_lsn: sk_state.truncate_lsn,
        flush_lsn,
    };
    Ok(json_response(StatusCode::OK, status)?)
}

/// Safekeeper http router.
pub fn make_router(conf: WalAcceptorConf) -> RouterBuilder<hyper::Body, ApiError> {
    let router = endpoint::make_router();
    router
        .data(Arc::new(conf))
        .get("/v1/status", status_handler)
        .get(
            "/v1/timeline/:tenant_id/:timeline_id",
            timeline_status_handler,
        )
}
