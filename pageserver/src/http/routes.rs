use std::sync::Arc;

use anyhow::{Context, Result};
use hyper::StatusCode;
use hyper::{Body, Request, Response, Uri};
use serde::Serialize;
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
    request::get_request_param,
    request::parse_request_param,
};
use zenith_utils::http::{RequestExt, RouterBuilder};
use zenith_utils::lsn::Lsn;
use zenith_utils::zid::HexZTimelineId;
use zenith_utils::zid::ZTimelineId;

use super::models::BranchCreateRequest;
use super::models::StatusResponse;
use super::models::TenantCreateRequest;
use crate::branches::BranchInfo;
use crate::repository::RepositoryTimeline;
use crate::repository::TimelineSyncState;
use crate::{branches, config::PageServerConf, tenant_mgr, ZTenantId};

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

async fn branch_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let request_data: BranchCreateRequest = json_request(&mut request).await?;

    check_permission(&request, Some(request_data.tenant_id))?;

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("/branch_create", name = %request_data.name, tenant = %request_data.tenant_id, startpoint=%request_data.start_point).entered();
        branches::create_branch(
            get_config(&request),
            &request_data.name,
            &request_data.start_point,
            &request_data.tenant_id,
        )
    })
    .await
    .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::CREATED, response_data)?)
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

async fn branch_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenantid: ZTenantId = parse_request_param(&request, "tenant_id")?;

    let include_non_incremental_logical_size = get_include_non_incremental_logical_size(&request);

    check_permission(&request, Some(tenantid))?;

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("branch_list", tenant = %tenantid).entered();
        crate::branches::get_branches(
            get_config(&request),
            &tenantid,
            include_non_incremental_logical_size,
        )
    })
    .await
    .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::OK, response_data)?)
}

async fn branch_detail_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenantid: ZTenantId = parse_request_param(&request, "tenant_id")?;
    let branch_name: String = get_request_param(&request, "branch_name")?.to_string();
    let conf = get_state(&request).conf;
    let path = conf.branch_path(&branch_name, &tenantid);

    let include_non_incremental_logical_size = get_include_non_incremental_logical_size(&request);

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("branch_detail", tenant = %tenantid, branch=%branch_name).entered();
        let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
        BranchInfo::from_path(path, &repo, include_non_incremental_logical_size)
    })
    .await
    .map_err(ApiError::from_err)??;

    Ok(json_response(StatusCode::OK, response_data)?)
}

async fn timeline_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let conf = get_state(&request).conf;
    let timelines_dir = conf.timelines_path(&tenant_id);

    let mut timelines_dir_contents =
        tokio::fs::read_dir(&timelines_dir).await.with_context(|| {
            format!(
                "Failed to list timelines dir '{}' contents",
                timelines_dir.display()
            )
        })?;

    let mut local_timelines = Vec::new();
    while let Some(entry) = timelines_dir_contents.next_entry().await.with_context(|| {
        format!(
            "Failed to list timelines dir '{}' contents",
            timelines_dir.display()
        )
    })? {
        let entry_path = entry.path();
        let entry_type = entry.file_type().await.with_context(|| {
            format!(
                "Failed to get file type of timeline dirs' entry '{}'",
                entry_path.display()
            )
        })?;

        if entry_type.is_dir() {
            match entry.file_name().to_string_lossy().parse::<ZTimelineId>() {
                Ok(timeline_id) => local_timelines.push(timeline_id.to_string()),
                Err(e) => error!(
                    "Failed to get parse timeline id from timeline dirs' entry '{}': {}",
                    entry_path.display(),
                    e
                ),
            }
        }
    }

    Ok(json_response(StatusCode::OK, local_timelines)?)
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum TimelineInfo {
    Local {
        #[serde(with = "hex")]
        timeline_id: ZTimelineId,
        #[serde(with = "hex")]
        tenant_id: ZTenantId,
        ancestor_timeline_id: Option<HexZTimelineId>,
        last_record_lsn: Lsn,
        prev_record_lsn: Lsn,
        disk_consistent_lsn: Lsn,
        timeline_state: Option<TimelineSyncState>,
    },
    Remote {
        #[serde(with = "hex")]
        timeline_id: ZTimelineId,
        #[serde(with = "hex")]
        tenant_id: ZTenantId,
    },
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
        Ok::<_, anyhow::Error>(match repo.get_timeline(timeline_id)?.local_timeline() {
            None => TimelineInfo::Remote {
                timeline_id,
                tenant_id,
            },
            Some(timeline) => TimelineInfo::Local {
                timeline_id,
                tenant_id,
                ancestor_timeline_id: timeline
                    .get_ancestor_timeline_id()
                    .map(HexZTimelineId::from),
                disk_consistent_lsn: timeline.get_disk_consistent_lsn(),
                last_record_lsn: timeline.get_last_record_lsn(),
                prev_record_lsn: timeline.get_prev_record_lsn(),
                timeline_state: repo.get_timeline_state(timeline_id),
            },
        })
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
            RepositoryTimeline::Local(_) => {
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

    tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_create", tenant = %request_data.tenant_id).entered();
        tenant_mgr::create_repository_for_tenant(get_config(&request), request_data.tenant_id)
    })
    .await
    .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::CREATED, ())?)
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
        .get("/v1/timeline/:tenant_id", timeline_list_handler)
        .get(
            "/v1/timeline/:tenant_id/:timeline_id",
            timeline_detail_handler,
        )
        .post(
            "/v1/timeline/:tenant_id/:timeline_id/attach",
            timeline_attach_handler,
        )
        .post(
            "/v1/timeline/:tenant_id/:timeline_id/detach",
            timeline_detach_handler,
        )
        .get("/v1/branch/:tenant_id", branch_list_handler)
        .get("/v1/branch/:tenant_id/:branch_name", branch_detail_handler)
        .post("/v1/branch", branch_create_handler)
        .get("/v1/tenant", tenant_list_handler)
        .post("/v1/tenant", tenant_create_handler)
        .any(handler_404)
}
