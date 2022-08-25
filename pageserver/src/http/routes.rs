use std::sync::Arc;

use anyhow::{Context, Result};
use hyper::StatusCode;
use hyper::{Body, Request, Response, Uri};
use tracing::*;

use super::models::{
    StatusResponse, TenantConfigRequest, TenantCreateRequest, TenantCreateResponse, TenantInfo,
    TimelineCreateRequest, TimelineInfo,
};
use crate::layered_repository::Timeline;
use crate::tenant_config::TenantConfOpt;
use crate::{config::PageServerConf, tenant_mgr, timelines};
use utils::{
    auth::JwtAuth,
    http::{
        endpoint::{self, attach_openapi_ui, auth_middleware, check_permission},
        error::{ApiError, HttpErrorBody},
        json::{json_request, json_response},
        request::parse_request_param,
        RequestExt, RouterBuilder,
    },
    lsn::Lsn,
    zid::{ZTenantId, ZTimelineId},
};

struct State {
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    allowlist_routes: Vec<Uri>,
}

impl State {
    fn new(conf: &'static PageServerConf, auth: Option<Arc<JwtAuth>>) -> anyhow::Result<Self> {
        let allowlist_routes = ["/v1/status", "/v1/doc", "/swagger.yml"]
            .iter()
            .map(|v| v.parse().unwrap())
            .collect::<Vec<_>>();

        Ok(Self {
            conf,
            auth,
            allowlist_routes,
        })
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

// Helper functions to construct a LocalTimelineInfo struct for a timeline

fn build_timeline_info(
    timeline: &Timeline,
    include_non_incremental_logical_size: bool,
    include_non_incremental_physical_size: bool,
) -> anyhow::Result<TimelineInfo> {
    let last_record_lsn = timeline.get_last_record_lsn();
    let (wal_source_connstr, last_received_msg_lsn, last_received_msg_ts) = {
        let guard = timeline.last_received_wal.lock().unwrap();
        if let Some(info) = guard.as_ref() {
            (
                Some(info.wal_source_connstr.clone()),
                Some(info.last_received_msg_lsn),
                Some(info.last_received_msg_ts),
            )
        } else {
            (None, None, None)
        }
    };

    let info = TimelineInfo {
        tenant_id: timeline.tenant_id,
        timeline_id: timeline.timeline_id,
        ancestor_timeline_id: timeline.get_ancestor_timeline_id(),
        ancestor_lsn: {
            match timeline.get_ancestor_lsn() {
                Lsn(0) => None,
                lsn @ Lsn(_) => Some(lsn),
            }
        },
        disk_consistent_lsn: timeline.get_disk_consistent_lsn(),
        remote_consistent_lsn: timeline.get_remote_consistent_lsn().unwrap_or(Lsn(0)),
        last_record_lsn,
        prev_record_lsn: Some(timeline.get_prev_record_lsn()),
        latest_gc_cutoff_lsn: *timeline.get_latest_gc_cutoff_lsn(),
        current_logical_size: Some(timeline.get_current_logical_size()),
        current_physical_size: Some(timeline.get_physical_size()),
        current_logical_size_non_incremental: if include_non_incremental_logical_size {
            Some(timeline.get_current_logical_size_non_incremental(last_record_lsn)?)
        } else {
            None
        },
        current_physical_size_non_incremental: if include_non_incremental_physical_size {
            Some(timeline.get_physical_size_non_incremental()?)
        } else {
            None
        },
        wal_source_connstr,
        last_received_msg_lsn,
        last_received_msg_ts,
    };
    Ok(info)
}

// healthcheck handler
async fn status_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let config = get_config(&request);
    json_response(StatusCode::OK, StatusResponse { id: config.id })
}

async fn timeline_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    let request_data: TimelineCreateRequest = json_request(&mut request).await?;
    check_permission(&request, Some(tenant_id))?;

    match timelines::create_timeline(
        get_config(&request),
        tenant_id,
        request_data.new_timeline_id.map(ZTimelineId::from),
        request_data.ancestor_timeline_id.map(ZTimelineId::from),
        request_data.ancestor_start_lsn,
    )
    .await
    {
        Ok(Some(new_timeline)) => {
            // Created. Construct a TimelineInfo for it.
            let info = build_timeline_info(&new_timeline, false, false)?;
            json_response(StatusCode::CREATED, info)
        }
        Ok(None) => json_response(StatusCode::CONFLICT, ()), // timeline already exists
        Err(err) => Err(err).map_err(ApiError::from_err)?,
    }
}

async fn timeline_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    let include_non_incremental_logical_size =
        query_param_present(&request, "include-non-incremental-logical-size");
    let include_non_incremental_physical_size =
        query_param_present(&request, "include-non-incremental-physical-size");
    check_permission(&request, Some(tenant_id))?;

    let timeline_infos = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("timeline_list", tenant_id = %tenant_id).entered();
        let repo = tenant_mgr::get_tenant(tenant_id)?;

        let mut result = Vec::new();
        for timeline in repo.list_timelines() {
            let info = build_timeline_info(
                &timeline,
                include_non_incremental_logical_size,
                include_non_incremental_physical_size,
            )?;
            result.push(info);
        }
        Ok::<_, anyhow::Error>(result)
    })
    .await
    .map_err(ApiError::from_err)??;

    json_response(StatusCode::OK, timeline_infos)
}

/// Checks if a query param is present in the request's URL
fn query_param_present(request: &Request<Body>, param: &str) -> bool {
    request
        .uri()
        .query()
        .map(|v| {
            url::form_urlencoded::parse(v.as_bytes())
                .into_owned()
                .any(|(p, _)| p == param)
        })
        .unwrap_or(false)
}

async fn timeline_detail_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;
    let include_non_incremental_logical_size =
        query_param_present(&request, "include-non-incremental-logical-size");
    let include_non_incremental_physical_size =
        query_param_present(&request, "include-non-incremental-physical-size");
    check_permission(&request, Some(tenant_id))?;

    let timeline_info = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("timeline_detail_handler", tenant_id = %tenant_id).entered();
        let repo = tenant_mgr::get_tenant(tenant_id)?;
        let timeline = repo.get_timeline(timeline_id).ok_or_else(|| {
            ApiError::NotFound(format!(
                "Timeline {timeline_id} not found for tenant {tenant_id}"
            ))
        })?;
        let info = build_timeline_info(
            &timeline,
            include_non_incremental_logical_size,
            include_non_incremental_physical_size,
        )?;
        Ok::<_, anyhow::Error>(info)
    })
    .await
    .map_err(ApiError::from_err)??;

    json_response(StatusCode::OK, timeline_info)
}

// TODO makes sense to provide tenant config right away the same way as it handled in tenant_create
async fn tenant_attach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    info!("Handling tenant attach {}", tenant_id);

    let conf = get_config(&request);

    tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_attach_handler", tenant_id = %tenant_id).entered();
        if let Err(err) = tenant_mgr::attach_tenant(conf, tenant_id) {
            // FIXME: distinguish between "Tenant is already present locally" and other errors
            error!("could not attach tenant: {:?}", err);
            return Err(err);
        }
        Ok(())
    })
    .await
    .map_err(ApiError::from_err)??;

    json_response(StatusCode::ACCEPTED, ())
}

async fn timeline_delete_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;
    check_permission(&request, Some(tenant_id))?;

    let repo = tenant_mgr::get_tenant(tenant_id)?;
    if let Err(err) = repo.delete_timeline(timeline_id).await {
        error!("could not delete timeline: {:?}", err);
        return Err(err).map_err(ApiError::from_err)?;
    }

    json_response(StatusCode::OK, ())
}

async fn tenant_detach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    if let Err(err) = tenant_mgr::detach_tenant(tenant_id).await {
        // FIXME: distinguish between "Tenant is already present locally" and other errors
        error!("could not detach tenant: {:?}", err);
        return Err(err).map_err(ApiError::from_err);
    }

    json_response(StatusCode::OK, ())
}

async fn tenant_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_list").entered();
        tenant_mgr::list_tenants()
            .iter()
            .map(|(id, state)| TenantInfo {
                id: *id,
                state: *state,
                current_physical_size: None,
            })
            .collect::<Vec<TenantInfo>>()
    })
    .await
    .map_err(ApiError::from_err)?;

    json_response(StatusCode::OK, response_data)
}

async fn tenant_status(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let tenant_info = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_status_handler", tenant = %tenant_id).entered();
        let repo = tenant_mgr::get_tenant(tenant_id)?;

        // Calculate total physical size of all timelines
        let mut current_physical_size = 0;
        for timeline in repo.list_timelines().iter() {
            current_physical_size += timeline.get_physical_size();
        }

        let tenant_info = TenantInfo {
            id: tenant_id,
            state: repo.get_state(),
            current_physical_size: Some(current_physical_size),
        };

        Ok::<_, anyhow::Error>(tenant_info)
    })
    .await
    .map_err(ApiError::from_err)??;

    json_response(StatusCode::OK, tenant_info)
}

async fn tenant_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permission(&request, None)?;

    let request_data: TenantCreateRequest = json_request(&mut request).await?;

    let mut tenant_conf = TenantConfOpt::default();
    if let Some(gc_period) = request_data.gc_period {
        tenant_conf.gc_period =
            Some(humantime::parse_duration(&gc_period).map_err(ApiError::from_err)?);
    }
    tenant_conf.gc_horizon = request_data.gc_horizon;
    tenant_conf.image_creation_threshold = request_data.image_creation_threshold;

    if let Some(pitr_interval) = request_data.pitr_interval {
        tenant_conf.pitr_interval =
            Some(humantime::parse_duration(&pitr_interval).map_err(ApiError::from_err)?);
    }

    if let Some(walreceiver_connect_timeout) = request_data.walreceiver_connect_timeout {
        tenant_conf.walreceiver_connect_timeout = Some(
            humantime::parse_duration(&walreceiver_connect_timeout).map_err(ApiError::from_err)?,
        );
    }
    if let Some(lagging_wal_timeout) = request_data.lagging_wal_timeout {
        tenant_conf.lagging_wal_timeout =
            Some(humantime::parse_duration(&lagging_wal_timeout).map_err(ApiError::from_err)?);
    }
    if let Some(max_lsn_wal_lag) = request_data.max_lsn_wal_lag {
        tenant_conf.max_lsn_wal_lag = Some(max_lsn_wal_lag);
    }

    tenant_conf.checkpoint_distance = request_data.checkpoint_distance;
    if let Some(checkpoint_timeout) = request_data.checkpoint_timeout {
        tenant_conf.checkpoint_timeout =
            Some(humantime::parse_duration(&checkpoint_timeout).map_err(ApiError::from_err)?);
    }

    tenant_conf.compaction_target_size = request_data.compaction_target_size;
    tenant_conf.compaction_threshold = request_data.compaction_threshold;

    if let Some(compaction_period) = request_data.compaction_period {
        tenant_conf.compaction_period =
            Some(humantime::parse_duration(&compaction_period).map_err(ApiError::from_err)?);
    }

    let target_tenant_id = request_data
        .new_tenant_id
        .map(ZTenantId::from)
        .unwrap_or_else(ZTenantId::generate);

    let new_tenant_id = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_create", tenant = ?target_tenant_id).entered();
        let conf = get_config(&request);

        tenant_mgr::create_tenant(conf, tenant_conf, target_tenant_id)
    })
    .await
    .map_err(ApiError::from_err)??;

    Ok(match new_tenant_id {
        Some(id) => json_response(StatusCode::CREATED, TenantCreateResponse(id))?,
        None => json_response(StatusCode::CONFLICT, ())?,
    })
}

async fn tenant_config_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let request_data: TenantConfigRequest = json_request(&mut request).await?;
    let tenant_id = request_data.tenant_id;
    check_permission(&request, Some(tenant_id))?;

    let mut tenant_conf: TenantConfOpt = Default::default();
    if let Some(gc_period) = request_data.gc_period {
        tenant_conf.gc_period =
            Some(humantime::parse_duration(&gc_period).map_err(ApiError::from_err)?);
    }
    tenant_conf.gc_horizon = request_data.gc_horizon;
    tenant_conf.image_creation_threshold = request_data.image_creation_threshold;

    if let Some(pitr_interval) = request_data.pitr_interval {
        tenant_conf.pitr_interval =
            Some(humantime::parse_duration(&pitr_interval).map_err(ApiError::from_err)?);
    }
    if let Some(walreceiver_connect_timeout) = request_data.walreceiver_connect_timeout {
        tenant_conf.walreceiver_connect_timeout = Some(
            humantime::parse_duration(&walreceiver_connect_timeout).map_err(ApiError::from_err)?,
        );
    }
    if let Some(lagging_wal_timeout) = request_data.lagging_wal_timeout {
        tenant_conf.lagging_wal_timeout =
            Some(humantime::parse_duration(&lagging_wal_timeout).map_err(ApiError::from_err)?);
    }
    if let Some(max_lsn_wal_lag) = request_data.max_lsn_wal_lag {
        tenant_conf.max_lsn_wal_lag = Some(max_lsn_wal_lag);
    }

    tenant_conf.checkpoint_distance = request_data.checkpoint_distance;
    if let Some(checkpoint_timeout) = request_data.checkpoint_timeout {
        tenant_conf.checkpoint_timeout =
            Some(humantime::parse_duration(&checkpoint_timeout).map_err(ApiError::from_err)?);
    }
    tenant_conf.compaction_target_size = request_data.compaction_target_size;
    tenant_conf.compaction_threshold = request_data.compaction_threshold;

    if let Some(compaction_period) = request_data.compaction_period {
        tenant_conf.compaction_period =
            Some(humantime::parse_duration(&compaction_period).map_err(ApiError::from_err)?);
    }

    tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_config", tenant = ?tenant_id).entered();

        tenant_mgr::update_tenant_config(tenant_conf, tenant_id)
    })
    .await
    .map_err(ApiError::from_err)??;

    json_response(StatusCode::OK, ())
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
) -> anyhow::Result<RouterBuilder<hyper::Body, ApiError>> {
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

    Ok(router
        .data(Arc::new(
            State::new(conf, auth).context("Failed to initialize router state")?,
        ))
        .get("/v1/status", status_handler)
        .get("/v1/tenant", tenant_list_handler)
        .post("/v1/tenant", tenant_create_handler)
        .get("/v1/tenant/:tenant_id", tenant_status)
        .put("/v1/tenant/config", tenant_config_handler)
        .get("/v1/tenant/:tenant_id/timeline", timeline_list_handler)
        .post("/v1/tenant/:tenant_id/timeline", timeline_create_handler)
        .post("/v1/tenant/:tenant_id/attach", tenant_attach_handler)
        .post("/v1/tenant/:tenant_id/detach", tenant_detach_handler)
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id",
            timeline_detail_handler,
        )
        .delete(
            "/v1/tenant/:tenant_id/timeline/:timeline_id",
            timeline_delete_handler,
        )
        // for backward compatibility
        .post(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/detach",
            timeline_delete_handler,
        )
        .any(handler_404))
}
