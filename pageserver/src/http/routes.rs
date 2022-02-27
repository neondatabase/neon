use std::sync::Arc;

use anyhow::Result;
use hyper::StatusCode;
use hyper::{Body, Request, Response, Uri};
use tracing::*;
use zenith_utils::auth::JwtAuth;
use zenith_utils::http::endpoint::attach_openapi_ui;
use zenith_utils::http::endpoint::auth_middleware;
use zenith_utils::http::endpoint::check_permission;
use zenith_utils::http::error::ApiError;
use zenith_utils::http::{
    endpoint,
    error::HttpErrorBody,
    json::{json_request, json_response},
    request::parse_request_param,
};
use zenith_utils::http::{RequestExt, RouterBuilder};
use zenith_utils::zid::{HexZTimelineId, ZTimelineId};

use super::models::StatusResponse;
use super::models::TenantCreateRequest;
use super::models::TenantCreateResponse;
use super::models::TimelineCreateRequest;
use crate::repository::RepositoryTimeline;
use crate::timelines::TimelineInfo;
use crate::{config::PageServerConf, tenant_mgr, timelines, ZTenantId};

#[derive(Debug)]
struct State {
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    allowlist_routes: Vec<Uri>,
}

impl State {
    fn new(conf: &'static PageServerConf, auth: Option<Arc<JwtAuth>>) -> Self {
        let allowlist_routes = ["/v1/status", "/v1/doc", "/swagger.yml"]
            .iter()
            .map(|v| v.parse().unwrap())
            .collect::<Vec<_>>();
        Self {
            conf,
            auth,
            allowlist_routes,
        }
    }
}

#[inline(always)]
fn get_state(request: &Request<Body>) -> &State {
    request
        .data::<Arc<State>>()
        .expect("unknown state type")
        .as_ref()
}

#[inline(always)]
fn get_config(request: &Request<Body>) -> &'static PageServerConf {
    get_state(request).conf
}

// healthcheck handler
async fn status_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let config = get_config(&request);
    Ok(json_response(
        StatusCode::OK,
        StatusResponse { id: config.id },
    )?)
}

async fn timeline_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    let request_data: TimelineCreateRequest = json_request(&mut request).await?;

    check_permission(&request, Some(tenant_id))?;

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("/timeline_create", tenant = %tenant_id, new_timeline = ?request_data.new_timeline_id, lsn=?request_data.ancestor_start_lsn).entered();
        timelines::create_timeline(
            get_config(&request),
            tenant_id,
            request_data.new_timeline_id,
            request_data.ancestor_timeline_id,
            request_data.ancestor_start_lsn,
        )
    })
    .await
    .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::CREATED, response_data)?)
}

async fn timeline_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;
    let include_non_incremental_logical_size = get_include_non_incremental_logical_size(&request);
    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("timeline_list", tenant = %tenant_id).entered();
        crate::timelines::get_timelines(tenant_id, include_non_incremental_logical_size)
    })
    .await
    .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::OK, response_data)?)
}

// Gate non incremental logical size calculation behind a flag
// after pgbench -i -s100 calculation took 28ms so if multiplied by the number of timelines
// and tenants it can take noticeable amount of time. Also the value currently used only in tests
fn get_include_non_incremental_logical_size(request: &Request<Body>) -> bool {
    request
        .uri()
        .query()
        .map(|v| {
            url::form_urlencoded::parse(v.as_bytes())
                .into_owned()
                .any(|(param, _)| param == "include-non-incremental-logical-size")
        })
        .unwrap_or(false)
}

async fn timeline_detail_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter =
            info_span!("timeline_detail_handler", tenant = %tenant_id, timeline = %timeline_id)
                .entered();
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)?;
        let include_non_incremental_logical_size =
            get_include_non_incremental_logical_size(&request);
        Ok::<_, anyhow::Error>(TimelineInfo::from_repo_timeline(
            tenant_id,
            repo.get_timeline(timeline_id)?,
            include_non_incremental_logical_size,
        ))
    })
    .await
    .map_err(ApiError::from_err)??;

    Ok(json_response(StatusCode::OK, response_data)?)
}

async fn timeline_attach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;

    tokio::task::spawn_blocking(move || {
        let _enter =
            info_span!("timeline_attach_handler", tenant = %tenant_id, timeline = %timeline_id)
                .entered();
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)?;
        match repo.get_timeline(timeline_id)? {
            RepositoryTimeline::Local { .. } => {
                anyhow::bail!("Timeline with id {} is already local", timeline_id)
            }
            RepositoryTimeline::Remote {
                id: _,
                disk_consistent_lsn: _,
            } => {
                // FIXME (rodionov) get timeline already schedules timeline for download, and duplicate tasks can cause errors
                //  first should be fixed in https://github.com/zenithdb/zenith/issues/997
                // TODO (rodionov) change timeline state to awaits download (incapsulate it somewhere in the repo)
                // TODO (rodionov) can we safely request replication on the timeline before sync is completed? (can be implemented on top of the #997)
                Ok(())
            }
        }
    })
    .await
    .map_err(ApiError::from_err)??;

    Ok(json_response(StatusCode::ACCEPTED, ())?)
}

async fn timeline_detach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;

    tokio::task::spawn_blocking(move || {
        let _enter =
            info_span!("timeline_detach_handler", tenant = %tenant_id, timeline = %timeline_id)
                .entered();
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)?;
        repo.detach_timeline(timeline_id)
    })
    .await
    .map_err(ApiError::from_err)??;

    Ok(json_response(StatusCode::OK, ())?)
}

async fn tenant_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // check for management permission
    check_permission(&request, None)?;

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_list").entered();
        crate::tenant_mgr::list_tenants()
    })
    .await
    .map_err(ApiError::from_err)??;

    Ok(json_response(StatusCode::OK, response_data)?)
}

async fn tenant_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // check for management permission
    check_permission(&request, None)?;

    let request_data: TenantCreateRequest = json_request(&mut request).await?;

    let initial_timeline_id = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_create", tenant = ?request_data.new_tenant_id, initial_timeline = ?request_data.initial_timeline_id).entered();
        tenant_mgr::create_repository_for_tenant(
            get_config(&request),
            request_data.new_tenant_id,
            request_data.initial_timeline_id,
        ).map(|new_ids| TenantCreateResponse {
            tenant_id: new_ids.tenant_id,
            timeline_id: new_ids.timeline_id,
        })
    })
    .await
    .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::CREATED, initial_timeline_id)?)
}

async fn handler_404(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(
        StatusCode::NOT_FOUND,
        HttpErrorBody::from_msg("page not found".to_owned()),
    )
}

pub fn make_router(
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
) -> RouterBuilder<hyper::Body, ApiError> {
    let spec = include_bytes!("openapi_spec.yml");
    let mut router = attach_openapi_ui(endpoint::make_router(), spec, "/swagger.yml", "/v1/doc");
    if auth.is_some() {
        router = router.middleware(auth_middleware(|request| {
            let state = get_state(request);
            if state.allowlist_routes.contains(request.uri()) {
                None
            } else {
                state.auth.as_deref()
            }
        }))
    }

    router
        .data(Arc::new(State::new(conf, auth)))
        .get("/v1/status", status_handler)
        .get("/v1/tenant", tenant_list_handler)
        .post("/v1/tenant", tenant_create_handler)
        .get("/v1/tenant/:tenant_id/timeline", timeline_list_handler)
        .post("/v1/tenant/:tenant_id/timeline", timeline_create_handler)
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id",
            timeline_detail_handler,
        )
        .post(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/attach",
            timeline_attach_handler,
        )
        .post(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/detach",
            timeline_detach_handler,
        )
        .any(handler_404)
}
