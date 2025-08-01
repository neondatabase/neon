use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use anyhow::Context;
use control_plane::storage_controller::{AttachHookRequest, InspectRequest};
use futures::Future;
use http_utils::endpoint::{
    self, auth_middleware, check_permission_with, profile_cpu_handler, profile_heap_handler,
    request_span,
};
use http_utils::error::ApiError;
use http_utils::failpoints::failpoints_handler;
use http_utils::json::{json_request, json_response};
use http_utils::request::{must_get_query_param, parse_query_param, parse_request_param};
use http_utils::{RequestExt, RouterBuilder};
use hyper::header::CONTENT_TYPE;
use hyper::{Body, Request, Response, StatusCode, Uri};
use metrics::{BuildInfo, NeonMetrics};
use pageserver_api::controller_api::{
    MetadataHealthListOutdatedRequest, MetadataHealthListOutdatedResponse,
    MetadataHealthListUnhealthyResponse, MetadataHealthUpdateRequest, MetadataHealthUpdateResponse,
    NodeAvailability, NodeConfigureRequest, NodeRegisterRequest, SafekeeperSchedulingPolicyRequest,
    ShardsPreferredAzsRequest, TenantCreateRequest, TenantPolicyRequest, TenantShardMigrateRequest,
    TimelineImportRequest, TimelineSafekeeperMigrateRequest,
};
use pageserver_api::models::{
    DetachBehavior, LsnLeaseRequest, TenantConfigPatchRequest, TenantConfigRequest,
    TenantLocationConfigRequest, TenantShardSplitRequest, TenantTimeTravelRequest,
    TimelineArchivalConfigRequest, TimelineCreateRequest,
};
use pageserver_api::shard::TenantShardId;
use pageserver_api::upcall_api::{
    PutTimelineImportStatusRequest, ReAttachRequest, TimelineImportStatusRequest, ValidateRequest,
};
use pageserver_client::{BlockUnblock, mgmt_api};

use routerify::Middleware;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use utils::auth::{Scope, SwappableJwtAuth};
use utils::id::{NodeId, TenantId, TimelineId};

use crate::http;
use crate::metrics::{
    HttpRequestLatencyLabelGroup, HttpRequestStatusLabelGroup, METRICS_REGISTRY,
    PageserverRequestLabelGroup,
};
use crate::persistence::SafekeeperUpsert;
use crate::reconciler::ReconcileError;
use crate::service::{
    LeadershipStatus, RECONCILE_TIMEOUT, STARTUP_RECONCILE_TIMEOUT, Service,
    TenantMutationLocations,
};

/// State available to HTTP request handlers
pub struct HttpState {
    service: Arc<crate::service::Service>,
    auth: Option<Arc<SwappableJwtAuth>>,
    rate_limiter: governor::DefaultKeyedRateLimiter<TenantId>,
    neon_metrics: NeonMetrics,
    allowlist_routes: &'static [&'static str],
}

impl HttpState {
    pub fn new(
        service: Arc<crate::service::Service>,
        auth: Option<Arc<SwappableJwtAuth>>,
        build_info: BuildInfo,
    ) -> Self {
        let quota = governor::Quota::per_second(service.get_config().tenant_rate_limit);
        Self {
            service,
            auth,
            rate_limiter: governor::RateLimiter::keyed(quota),
            neon_metrics: NeonMetrics::new(build_info),
            allowlist_routes: &[
                "/status",
                "/live",
                "/ready",
                "/metrics",
                "/profile/cpu",
                "/profile/heap",
            ],
        }
    }
}

#[inline(always)]
fn get_state(request: &Request<Body>) -> &HttpState {
    request
        .data::<Arc<HttpState>>()
        .expect("unknown state type")
        .as_ref()
}

/// Rate limits tenant requests.
///
/// TODO: this should be a request middleware, but requires us to extract the tenant ID from
/// different URLs in a systematic way.
///
/// TODO: consider returning a 429 response if these start piling up.
async fn maybe_rate_limit(request: &Request<Body>, tenant_id: TenantId) {
    // Check if the tenant should be rate-limited.
    let rate_limiter = &get_state(request).rate_limiter;
    if rate_limiter.check_key(&tenant_id).is_ok() {
        return;
    }

    // Measure the rate limiting delay.
    let _timer = METRICS_REGISTRY
        .metrics_group
        .storage_controller_http_request_rate_limited
        .start_timer();

    // Log rate limited tenants once every 10 seconds.
    static LOG_RATE_LIMITER: LazyLock<governor::DefaultKeyedRateLimiter<TenantId>> =
        LazyLock::new(|| {
            let quota = governor::Quota::with_period(Duration::from_secs(10)).unwrap();
            governor::RateLimiter::keyed(quota)
        });

    if LOG_RATE_LIMITER.check_key(&tenant_id).is_ok() {
        warn!("tenant {tenant_id} is rate limited")
    }

    // Wait for quota.
    rate_limiter.until_key_ready(&tenant_id).await;
}

/// Pageserver calls into this on startup, to learn which tenants it should attach
async fn handle_re_attach(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::GenerationsApi)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let reattach_req = json_request::<ReAttachRequest>(&mut req).await?;
    let state = get_state(&req);
    json_response(StatusCode::OK, state.service.re_attach(reattach_req).await?)
}

/// Pageserver calls into this before doing deletions, to confirm that it still
/// holds the latest generation for the tenants with deletions enqueued
async fn handle_validate(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::GenerationsApi)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let validate_req = json_request::<ValidateRequest>(&mut req).await?;
    let state = get_state(&req);
    json_response(StatusCode::OK, state.service.validate(validate_req).await?)
}

async fn handle_get_timeline_import_status(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::GenerationsApi)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let get_req = json_request::<TimelineImportStatusRequest>(&mut req).await?;

    let state = get_state(&req);

    json_response(
        StatusCode::OK,
        state
            .service
            .handle_timeline_shard_import_progress(get_req)
            .await?,
    )
}

async fn handle_put_timeline_import_status(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::GenerationsApi)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let put_req = json_request::<PutTimelineImportStatusRequest>(&mut req).await?;

    let state = get_state(&req);
    json_response(
        StatusCode::OK,
        state
            .service
            .handle_timeline_shard_import_progress_upcall(put_req)
            .await?,
    )
}

/// Call into this before attaching a tenant to a pageserver, to acquire a generation number
/// (in the real control plane this is unnecessary, because the same program is managing
///  generation numbers and doing attachments).
async fn handle_attach_hook(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let attach_req = json_request::<AttachHookRequest>(&mut req).await?;
    let state = get_state(&req);

    json_response(
        StatusCode::OK,
        state
            .service
            .attach_hook(attach_req)
            .await
            .map_err(ApiError::InternalServerError)?,
    )
}

async fn handle_inspect(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let inspect_req = json_request::<InspectRequest>(&mut req).await?;

    let state = get_state(&req);

    json_response(StatusCode::OK, state.service.inspect(inspect_req))
}

async fn handle_tenant_create(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::PageServerApi)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let create_req = json_request::<TenantCreateRequest>(&mut req).await?;

    json_response(
        StatusCode::CREATED,
        service.tenant_create(create_req).await?,
    )
}

async fn handle_tenant_location_config(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&req, "tenant_shard_id")?;
    check_permissions(&req, Scope::PageServerApi)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let config_req = json_request::<TenantLocationConfigRequest>(&mut req).await?;
    json_response(
        StatusCode::OK,
        service
            .tenant_location_config(tenant_shard_id, config_req)
            .await?,
    )
}

async fn handle_tenant_config_patch(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::PageServerApi)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let config_req = json_request::<TenantConfigPatchRequest>(&mut req).await?;

    json_response(
        StatusCode::OK,
        service.tenant_config_patch(config_req).await?,
    )
}

async fn handle_tenant_config_set(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::PageServerApi)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let config_req = json_request::<TenantConfigRequest>(&mut req).await?;

    json_response(StatusCode::OK, service.tenant_config_set(config_req).await?)
}

async fn handle_tenant_config_get(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(_req) => {}
    };

    json_response(StatusCode::OK, service.tenant_config_get(tenant_id)?)
}

async fn handle_tenant_time_travel_remote_storage(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let time_travel_req = json_request::<TenantTimeTravelRequest>(&mut req).await?;

    let timestamp_raw = must_get_query_param(&req, "travel_to")?;
    let _timestamp = humantime::parse_rfc3339(&timestamp_raw).map_err(|_e| {
        ApiError::BadRequest(anyhow::anyhow!(
            "Invalid time for travel_to: {timestamp_raw:?}"
        ))
    })?;

    let done_if_after_raw = must_get_query_param(&req, "done_if_after")?;
    let _done_if_after = humantime::parse_rfc3339(&done_if_after_raw).map_err(|_e| {
        ApiError::BadRequest(anyhow::anyhow!(
            "Invalid time for done_if_after: {done_if_after_raw:?}"
        ))
    })?;

    service
        .tenant_time_travel_remote_storage(
            &time_travel_req,
            tenant_id,
            timestamp_raw,
            done_if_after_raw,
        )
        .await?;
    json_response(StatusCode::OK, ())
}

fn map_reqwest_hyper_status(status: reqwest::StatusCode) -> Result<hyper::StatusCode, ApiError> {
    hyper::StatusCode::from_u16(status.as_u16())
        .context("invalid status code")
        .map_err(ApiError::InternalServerError)
}

async fn handle_tenant_secondary_download(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let wait = parse_query_param(&req, "wait_ms")?.map(Duration::from_millis);
    maybe_rate_limit(&req, tenant_id).await;

    match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(_req) => {}
    };

    let (status, progress) = service.tenant_secondary_download(tenant_id, wait).await?;
    json_response(map_reqwest_hyper_status(status)?, progress)
}

async fn handle_tenant_delete(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(_req) => {}
    };

    let status_code = service
        .tenant_delete(tenant_id)
        .await
        .and_then(map_reqwest_hyper_status)?;

    if status_code == StatusCode::NOT_FOUND {
        // The pageserver uses 404 for successful deletion, but we use 200
        json_response(StatusCode::OK, ())
    } else {
        json_response(status_code, ())
    }
}

async fn handle_tenant_timeline_create(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let create_req = json_request::<TimelineCreateRequest>(&mut req).await?;
    json_response(
        StatusCode::CREATED,
        service
            .tenant_timeline_create(tenant_id, create_req)
            .await?,
    )
}

async fn handle_tenant_timeline_delete(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&req, "timeline_id")?;

    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(_req) => {}
    };

    service
        .maybe_delete_timeline_import(tenant_id, timeline_id)
        .await?;

    // For timeline deletions, which both implement an "initially return 202, then 404 once
    // we're done" semantic, we wrap with a retry loop to expose a simpler API upstream.
    async fn deletion_wrapper<R, F>(service: Arc<Service>, f: F) -> Result<Response<Body>, ApiError>
    where
        R: std::future::Future<Output = Result<StatusCode, ApiError>> + Send + 'static,
        F: Fn(Arc<Service>) -> R + Send + Sync + 'static,
    {
        // On subsequent retries, wait longer.
        // Enable callers with a 25 second request timeout to reliably get a response
        const MAX_WAIT: Duration = Duration::from_secs(25);
        const MAX_RETRY_PERIOD: Duration = Duration::from_secs(5);

        let started_at = Instant::now();

        // To keep deletion reasonably snappy for small tenants, initially check after 1 second if deletion
        // completed.
        let mut retry_period = Duration::from_secs(1);

        loop {
            let status = f(service.clone()).await?;
            match status {
                StatusCode::ACCEPTED => {
                    tracing::info!("Deletion accepted, waiting to try again...");
                    tokio::time::sleep(retry_period).await;
                    retry_period = MAX_RETRY_PERIOD;
                }
                StatusCode::CONFLICT => {
                    tracing::info!("Deletion already in progress, waiting to try again...");
                    tokio::time::sleep(retry_period).await;
                }
                StatusCode::NOT_FOUND => {
                    tracing::info!("Deletion complete");
                    return json_response(StatusCode::OK, ());
                }
                _ => {
                    tracing::warn!("Unexpected status {status}");
                    return json_response(status, ());
                }
            }

            let now = Instant::now();
            if now + retry_period > started_at + MAX_WAIT {
                tracing::info!("Deletion timed out waiting for 404");
                // REQUEST_TIMEOUT would be more appropriate, but CONFLICT is already part of
                // the pageserver's swagger definition for this endpoint, and has the same desired
                // effect of causing the control plane to retry later.
                return json_response(StatusCode::CONFLICT, ());
            }
        }
    }

    deletion_wrapper(service, move |service| async move {
        service
            .tenant_timeline_delete(tenant_id, timeline_id)
            .await
            .and_then(map_reqwest_hyper_status)
    })
    .await
}

async fn handle_tenant_timeline_archival_config(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&req, "timeline_id")?;

    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let create_req = json_request::<TimelineArchivalConfigRequest>(&mut req).await?;

    service
        .tenant_timeline_archival_config(tenant_id, timeline_id, create_req)
        .await?;

    json_response(StatusCode::OK, ())
}

async fn handle_tenant_timeline_detach_ancestor(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&req, "timeline_id")?;
    let behavior: Option<DetachBehavior> = parse_query_param(&req, "detach_behavior")?;

    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(_req) => {}
    };

    let res = service
        .tenant_timeline_detach_ancestor(tenant_id, timeline_id, behavior)
        .await?;

    json_response(StatusCode::OK, res)
}

async fn handle_tenant_timeline_block_unblock_gc(
    service: Arc<Service>,
    req: Request<Body>,
    dir: BlockUnblock,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    let timeline_id: TimelineId = parse_request_param(&req, "timeline_id")?;

    service
        .tenant_timeline_block_unblock_gc(tenant_id, timeline_id, dir)
        .await?;

    json_response(StatusCode::OK, ())
}

async fn handle_tenant_timeline_download_heatmap_layers(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&req, "tenant_shard_id")?;

    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_shard_id.tenant_id).await;

    let timeline_id: TimelineId = parse_request_param(&req, "timeline_id")?;
    let concurrency: Option<usize> = parse_query_param(&req, "concurrency")?;
    let recurse = parse_query_param(&req, "recurse")?.unwrap_or(false);

    service
        .tenant_timeline_download_heatmap_layers(tenant_shard_id, timeline_id, concurrency, recurse)
        .await?;

    json_response(StatusCode::OK, ())
}

async fn handle_tenant_timeline_safekeeper_migrate(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    // TODO(diko): it's not PS operation, there should be a different permission scope.
    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    let timeline_id: TimelineId = parse_request_param(&req, "timeline_id")?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let migrate_req = json_request::<TimelineSafekeeperMigrateRequest>(&mut req).await?;

    service
        .tenant_timeline_safekeeper_migrate(tenant_id, timeline_id, migrate_req)
        .await?;

    json_response(StatusCode::OK, ())
}

async fn handle_tenant_timeline_safekeeper_migrate_abort(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&req, "timeline_id")?;
    // TODO(diko): it's not PS operation, there should be a different permission scope.
    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    service
        .tenant_timeline_safekeeper_migrate_abort(tenant_id, timeline_id)
        .await?;

    json_response(StatusCode::OK, ())
}

async fn handle_tenant_timeline_lsn_lease(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&req, "timeline_id")?;

    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let lsn_lease_request = json_request::<LsnLeaseRequest>(&mut req).await?;

    service
        .tenant_timeline_lsn_lease(tenant_id, timeline_id, lsn_lease_request.lsn)
        .await?;

    json_response(StatusCode::OK, ())
}

// For metric labels where we would like to include the approximate path, but exclude high-cardinality fields like query parameters
// and tenant/timeline IDs.  Since we are proxying to arbitrary paths, we don't have routing templates to
// compare to, so we can just filter out our well known ID format with regexes.
fn path_without_ids(path: &str) -> String {
    static ID_REGEX: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    ID_REGEX
        .get_or_init(|| regex::Regex::new(r"([0-9a-fA-F]{32}(-[0-9]{4})?|\?.*)").unwrap())
        .replace_all(path, "")
        .to_string()
}

async fn handle_tenant_timeline_passthrough(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_or_shard_id: TenantShardId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_or_shard_id.tenant_id).await;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let Some(path) = req.uri().path_and_query() else {
        // This should never happen, our request router only calls us if there is a path
        return Err(ApiError::BadRequest(anyhow::anyhow!("Missing path")));
    };

    let method = match *req.method() {
        hyper::Method::GET => reqwest::Method::GET,
        hyper::Method::POST => reqwest::Method::POST,
        hyper::Method::PUT => reqwest::Method::PUT,
        hyper::Method::DELETE => reqwest::Method::DELETE,
        hyper::Method::PATCH => reqwest::Method::PATCH,
        _ => return Err(ApiError::BadRequest(anyhow::anyhow!("Unsupported method"))),
    };

    tracing::info!(
        "Proxying request for tenant {} ({})",
        tenant_or_shard_id.tenant_id,
        path
    );

    let tenant_shard_id = if tenant_or_shard_id.is_unsharded() {
        // If the request contains only tenant ID, find the node that holds shard zero
        let (_, shard_id) = service
            .tenant_shard0_node(tenant_or_shard_id.tenant_id)
            .await?;
        shard_id
    } else {
        tenant_or_shard_id
    };

    let service_inner = service.clone();

    service.tenant_shard_remote_mutation(tenant_shard_id, |locations| async move {
        let TenantMutationLocations(locations) = locations;
        if locations.is_empty() {
            return Err(ApiError::NotFound(anyhow::anyhow!("Tenant {} not found", tenant_or_shard_id.tenant_id).into()));
        }

        let (tenant_or_shard_id, locations) = locations.into_iter().next().unwrap();
        let node = locations.latest.node;

        // Callers will always pass an unsharded tenant ID.  Before proxying, we must
        // rewrite this to a shard-aware shard zero ID.
        let path = format!("{path}");
        let tenant_str = tenant_or_shard_id.tenant_id.to_string();
        let tenant_shard_str = format!("{tenant_shard_id}");
        let path = path.replace(&tenant_str, &tenant_shard_str);

        let latency = &METRICS_REGISTRY
            .metrics_group
            .storage_controller_passthrough_request_latency;

        let path_label = path_without_ids(&path)
            .split('/')
            .filter(|token| !token.is_empty())
            .collect::<Vec<_>>()
            .join("_");
        let labels = PageserverRequestLabelGroup {
            pageserver_id: &node.get_id().to_string(),
            path: &path_label,
            method: crate::metrics::Method::Get,
        };

        let _timer = latency.start_timer(labels.clone());

        let client = mgmt_api::Client::new(
            service_inner.get_http_client().clone(),
            node.base_url(),
            service_inner.get_config().pageserver_jwt_token.as_deref(),
        );
        let resp = client.op_raw(method, path).await.map_err(|e|
            // We return 503 here because if we can't successfully send a request to the pageserver,
            // either we aren't available or the pageserver is unavailable.
            ApiError::ResourceUnavailable(format!("Error sending pageserver API request to {node}: {e}").into()))?;

        if !resp.status().is_success() {
            let error_counter = &METRICS_REGISTRY
                .metrics_group
                .storage_controller_passthrough_request_error;
            error_counter.inc(labels);
        }
        let resp_staus = resp.status();

        // We have a reqest::Response, would like a http::Response
        let mut builder = hyper::Response::builder().status(map_reqwest_hyper_status(resp_staus)?);
        for (k, v) in resp.headers() {
            builder = builder.header(k.as_str(), v.as_bytes());
        }
        let resp_bytes = resp
            .bytes()
            .await
            .map_err(|e| ApiError::InternalServerError(e.into()))?;
        // Inspect 404 errors: at this point, we know that the tenant exists, but the pageserver we route
        // the request to might not yet be ready. Therefore, if it is a _tenant_ not found error, we can
        // convert it into a 503. TODO: we should make this part of the check in `tenant_shard_remote_mutation`.
        // However, `tenant_shard_remote_mutation` currently cannot inspect the HTTP error response body,
        // so we have to do it here instead.
        if resp_staus == reqwest::StatusCode::NOT_FOUND {
            let resp_str = std::str::from_utf8(&resp_bytes)
                .map_err(|e| ApiError::InternalServerError(e.into()))?;
            // We only handle "tenant not found" errors; other 404s like timeline not found should
            // be forwarded as-is.
            if Service::is_tenant_not_found_error(resp_str, tenant_or_shard_id.tenant_id) {
                // Rather than retry here, send the client a 503 to prompt a retry: this matches
                // the pageserver's use of 503, and all clients calling this API should retry on 503.
                return Err(ApiError::ResourceUnavailable(
                    format!(
                        "Pageserver {node} returned tenant 404 due to ongoing migration, retry later"
                    )
                    .into(),
                ));
            }
        }
        let response = builder
            .body(Body::from(resp_bytes))
            .map_err(|e| ApiError::InternalServerError(e.into()))?;
        Ok(response)
    }).await?
}

async fn handle_tenant_locate(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;

    check_permissions(&req, Scope::Admin)?;
    // NB: don't rate limit: admin operation.

    match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(_req) => {}
    };

    json_response(StatusCode::OK, service.tenant_locate(tenant_id)?)
}

async fn handle_tenant_describe(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::Scrubber)?;
    // NB: don't rate limit: scrubber operation.

    match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(_req) => {}
    };

    json_response(StatusCode::OK, service.tenant_describe(tenant_id)?)
}

/* BEGIN_HADRON */
async fn handle_tenant_timeline_describe(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Scrubber)?;

    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&req, "timeline_id")?;
    match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(_req) => {}
    };

    json_response(
        StatusCode::OK,
        service
            .tenant_timeline_describe(tenant_id, timeline_id)
            .await?,
    )
}
/* END_HADRON */

async fn handle_tenant_list(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let limit: Option<usize> = parse_query_param(&req, "limit")?;
    let start_after: Option<TenantId> = parse_query_param(&req, "start_after")?;
    tracing::info!("start_after: {:?}", start_after);

    match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(_req) => {}
    };

    json_response(StatusCode::OK, service.tenant_list(limit, start_after))
}

async fn handle_node_register(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Infra)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let register_req = json_request::<NodeRegisterRequest>(&mut req).await?;
    let state = get_state(&req);
    state.service.node_register(register_req).await?;
    json_response(StatusCode::OK, ())
}

async fn handle_node_list(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Infra)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let mut nodes = state.service.node_list().await?;
    nodes.sort_by_key(|n| n.get_id());
    let api_nodes = nodes.into_iter().map(|n| n.describe()).collect::<Vec<_>>();

    json_response(StatusCode::OK, api_nodes)
}

async fn handle_node_drop(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let node_id: NodeId = parse_request_param(&req, "node_id")?;
    json_response(StatusCode::OK, state.service.node_drop(node_id).await?)
}

async fn handle_node_delete_old(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let node_id: NodeId = parse_request_param(&req, "node_id")?;
    json_response(
        StatusCode::OK,
        state.service.node_delete_old(node_id).await?,
    )
}

async fn handle_tombstone_list(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let mut nodes = state.service.tombstone_list().await?;
    nodes.sort_by_key(|n| n.get_id());
    let api_nodes = nodes.into_iter().map(|n| n.describe()).collect::<Vec<_>>();

    json_response(StatusCode::OK, api_nodes)
}

async fn handle_tombstone_delete(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let node_id: NodeId = parse_request_param(&req, "node_id")?;
    json_response(
        StatusCode::OK,
        state.service.tombstone_delete(node_id).await?,
    )
}

async fn handle_node_configure(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let node_id: NodeId = parse_request_param(&req, "node_id")?;
    let config_req = json_request::<NodeConfigureRequest>(&mut req).await?;
    if node_id != config_req.node_id {
        return Err(ApiError::BadRequest(anyhow::anyhow!(
            "Path and body node_id differ"
        )));
    }
    let state = get_state(&req);

    json_response(
        StatusCode::OK,
        state
            .service
            .external_node_configure(
                config_req.node_id,
                config_req.availability.map(NodeAvailability::from),
                config_req.scheduling,
            )
            .await?,
    )
}

async fn handle_node_status(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Infra)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let node_id: NodeId = parse_request_param(&req, "node_id")?;

    let node_status = state.service.get_node(node_id).await?.describe();

    json_response(StatusCode::OK, node_status)
}

async fn handle_node_shards(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let state = get_state(&req);
    let node_id: NodeId = parse_request_param(&req, "node_id")?;

    let node_status = state.service.get_node_shards(node_id).await?;

    json_response(StatusCode::OK, node_status)
}

async fn handle_get_leader(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let leader = state.service.get_leader().await.map_err(|err| {
        ApiError::InternalServerError(anyhow::anyhow!(
            "Failed to read leader from database: {err}"
        ))
    })?;

    json_response(StatusCode::OK, leader)
}

async fn handle_node_delete(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let node_id: NodeId = parse_request_param(&req, "node_id")?;
    let force: bool = parse_query_param(&req, "force")?.unwrap_or(false);
    json_response(
        StatusCode::OK,
        state.service.start_node_delete(node_id, force).await?,
    )
}

async fn handle_cancel_node_delete(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Infra)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let node_id: NodeId = parse_request_param(&req, "node_id")?;
    json_response(
        StatusCode::ACCEPTED,
        state.service.cancel_node_delete(node_id).await?,
    )
}

async fn handle_node_drain(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Infra)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let node_id: NodeId = parse_request_param(&req, "node_id")?;

    state.service.start_node_drain(node_id).await?;

    json_response(StatusCode::ACCEPTED, ())
}

async fn handle_cancel_node_drain(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Infra)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let node_id: NodeId = parse_request_param(&req, "node_id")?;

    state.service.cancel_node_drain(node_id).await?;

    json_response(StatusCode::ACCEPTED, ())
}

async fn handle_node_fill(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Infra)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let node_id: NodeId = parse_request_param(&req, "node_id")?;

    state.service.start_node_fill(node_id).await?;

    json_response(StatusCode::ACCEPTED, ())
}

async fn handle_cancel_node_fill(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Infra)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let node_id: NodeId = parse_request_param(&req, "node_id")?;

    state.service.cancel_node_fill(node_id).await?;

    json_response(StatusCode::ACCEPTED, ())
}

async fn handle_safekeeper_list(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Infra)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let safekeepers = state.service.safekeepers_list().await?;
    json_response(StatusCode::OK, safekeepers)
}

async fn handle_metadata_health_update(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Scrubber)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let update_req = json_request::<MetadataHealthUpdateRequest>(&mut req).await?;
    let state = get_state(&req);

    state.service.metadata_health_update(update_req).await?;

    json_response(StatusCode::OK, MetadataHealthUpdateResponse {})
}

async fn handle_metadata_health_list_unhealthy(
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let unhealthy_tenant_shards = state.service.metadata_health_list_unhealthy().await?;

    json_response(
        StatusCode::OK,
        MetadataHealthListUnhealthyResponse {
            unhealthy_tenant_shards,
        },
    )
}

async fn handle_metadata_health_list_outdated(
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let list_outdated_req = json_request::<MetadataHealthListOutdatedRequest>(&mut req).await?;
    let state = get_state(&req);
    let health_records = state
        .service
        .metadata_health_list_outdated(list_outdated_req.not_scrubbed_for)
        .await?;

    json_response(
        StatusCode::OK,
        MetadataHealthListOutdatedResponse { health_records },
    )
}

async fn handle_tenant_shard_split(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;
    // NB: don't rate limit: admin operation.

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let split_req = json_request::<TenantShardSplitRequest>(&mut req).await?;

    json_response(
        StatusCode::OK,
        service.tenant_shard_split(tenant_id, split_req).await?,
    )
}

async fn handle_tenant_shard_migrate(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;
    // NB: don't rate limit: admin operation.

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let tenant_shard_id: TenantShardId = parse_request_param(&req, "tenant_shard_id")?;
    let migrate_req = json_request::<TenantShardMigrateRequest>(&mut req).await?;
    json_response(
        StatusCode::OK,
        service
            .tenant_shard_migrate(tenant_shard_id, migrate_req)
            .await?,
    )
}

async fn handle_tenant_shard_migrate_secondary(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;
    // NB: don't rate limit: admin operation.

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let tenant_shard_id: TenantShardId = parse_request_param(&req, "tenant_shard_id")?;
    let migrate_req = json_request::<TenantShardMigrateRequest>(&mut req).await?;
    json_response(
        StatusCode::OK,
        service
            .tenant_shard_migrate_secondary(tenant_shard_id, migrate_req)
            .await?,
    )
}

async fn handle_tenant_shard_cancel_reconcile(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;
    // NB: don't rate limit: admin operation.

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let tenant_shard_id: TenantShardId = parse_request_param(&req, "tenant_shard_id")?;
    json_response(
        StatusCode::OK,
        service
            .tenant_shard_cancel_reconcile(tenant_shard_id)
            .await?,
    )
}

async fn handle_tenant_update_policy(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;
    // NB: don't rate limit: admin operation.

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let update_req = json_request::<TenantPolicyRequest>(&mut req).await?;
    let state = get_state(&req);

    json_response(
        StatusCode::OK,
        state
            .service
            .tenant_update_policy(tenant_id, update_req)
            .await?,
    )
}

async fn handle_update_preferred_azs(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let azs_req = json_request::<ShardsPreferredAzsRequest>(&mut req).await?;
    let state = get_state(&req);

    json_response(
        StatusCode::OK,
        state.service.update_shards_preferred_azs(azs_req).await?,
    )
}

async fn handle_step_down(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::ControllerPeer)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let result = state.service.step_down().await;

    json_response(StatusCode::OK, result)
}

async fn handle_tenant_drop(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);

    json_response(StatusCode::OK, state.service.tenant_drop(tenant_id).await?)
}

async fn handle_tenant_import(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);

    json_response(
        StatusCode::OK,
        state.service.tenant_import(tenant_id).await?,
    )
}

async fn handle_timeline_import(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&req, "timeline_id")?;
    check_permissions(&req, Scope::PageServerApi)?;
    maybe_rate_limit(&req, tenant_id).await;

    let mut req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let import_req = json_request::<TimelineImportRequest>(&mut req).await?;

    let state = get_state(&req);

    if import_req.tenant_id != tenant_id || import_req.timeline_id != timeline_id {
        return Err(ApiError::BadRequest(anyhow::anyhow!(
            "tenant id or timeline id mismatch: url={tenant_id}/{timeline_id}, body={}/{}",
            import_req.tenant_id,
            import_req.timeline_id
        )));
    }

    json_response(
        StatusCode::OK,
        state.service.timeline_import(import_req).await?,
    )
}

async fn handle_tenant_timeline_locate(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let timeline_id: TimelineId = parse_request_param(&req, "timeline_id")?;

    check_permissions(&req, Scope::Admin)?;
    maybe_rate_limit(&req, tenant_id).await;

    match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(_req) => {}
    };

    json_response(
        StatusCode::OK,
        service
            .tenant_timeline_locate(tenant_id, timeline_id)
            .await?,
    )
}

async fn handle_tenants_dump(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    state.service.tenants_dump()
}

async fn handle_scheduler_dump(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    state.service.scheduler_dump()
}

async fn handle_consistency_check(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);

    json_response(StatusCode::OK, state.service.consistency_check().await?)
}

async fn handle_reconcile_all(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);

    json_response(StatusCode::OK, state.service.reconcile_all_now().await?)
}

/// Status endpoint is just used for checking that our HTTP listener is up
///
/// This serves as our k8s startup probe.
async fn handle_status(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(_req) => {}
    };

    json_response(StatusCode::OK, ())
}

/// Liveness endpoint indicates that this storage controller is in a state
/// where it can fulfill it's responsibilties. Namely, startup has finished
/// and it is the current leader.
///
/// This serves as our k8s liveness probe.
async fn handle_live(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let live = state.service.startup_complete.is_ready()
        && state.service.get_leadership_status() == LeadershipStatus::Leader;

    if live {
        json_response(StatusCode::OK, ())
    } else {
        json_response(StatusCode::SERVICE_UNAVAILABLE, ())
    }
}

/// Readiness endpoint indicates when we're done doing startup I/O (e.g. reconciling
/// with remote pageserver nodes).  This is intended for use as a kubernetes readiness probe.
async fn handle_ready(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    if state.service.startup_complete.is_ready() {
        json_response(StatusCode::OK, ())
    } else {
        json_response(StatusCode::SERVICE_UNAVAILABLE, ())
    }
}

impl From<ReconcileError> for ApiError {
    fn from(value: ReconcileError) -> Self {
        ApiError::Conflict(format!("Reconciliation error: {value}"))
    }
}

/// Return the safekeeper record by instance id, or 404.
///
/// Not used by anything except manual testing.
async fn handle_get_safekeeper(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Infra)?;

    let id = parse_request_param::<i64>(&req, "id")?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);

    let res = state.service.get_safekeeper(id).await;

    match res {
        Ok(b) => json_response(StatusCode::OK, b),
        Err(crate::persistence::DatabaseError::Query(diesel::result::Error::NotFound)) => {
            Err(ApiError::NotFound("unknown instance id".into()))
        }
        Err(other) => Err(other.into()),
    }
}

/// Used as part of deployment scripts.
///
/// Assumes information is only relayed to storage controller after first selecting an unique id on
/// control plane database, which means we have an id field in the request and payload.
async fn handle_upsert_safekeeper(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Infra)?;

    let body = json_request::<SafekeeperUpsert>(&mut req).await?;
    let id = parse_request_param::<i64>(&req, "id")?;

    if id != body.id {
        // it should be repeated
        return Err(ApiError::BadRequest(anyhow::anyhow!(
            "id mismatch: url={id:?}, body={:?}",
            body.id
        )));
    }

    if id <= 0 {
        return Err(ApiError::BadRequest(anyhow::anyhow!(
            "id not allowed to be zero or negative: {id}"
        )));
    }

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);

    state.service.upsert_safekeeper(body).await?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}

/// Sets the scheduling policy of the specified safekeeper
async fn handle_safekeeper_scheduling_policy(
    mut req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let body = json_request::<SafekeeperSchedulingPolicyRequest>(&mut req).await?;
    let id = parse_request_param::<i64>(&req, "id")?;

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);

    state
        .service
        .set_safekeeper_scheduling_policy(id, body.scheduling_policy)
        .await?;

    json_response(StatusCode::OK, ())
}

/// Common wrapper for request handlers that call into Service and will operate on tenants: they must only
/// be allowed to run if Service has finished its initial reconciliation.
async fn tenant_service_handler<R, H>(
    request: Request<Body>,
    handler: H,
    request_name: RequestName,
) -> R::Output
where
    R: std::future::Future<Output = Result<Response<Body>, ApiError>> + Send + 'static,
    H: FnOnce(Arc<Service>, Request<Body>) -> R + Send + Sync + 'static,
{
    let state = get_state(&request);
    let service = state.service.clone();

    let startup_complete = service.startup_complete.clone();
    if tokio::time::timeout(STARTUP_RECONCILE_TIMEOUT, startup_complete.wait())
        .await
        .is_err()
    {
        // This shouldn't happen: it is the responsibilty of [`Service::startup_reconcile`] to use appropriate
        // timeouts around its remote calls, to bound its runtime.
        return Err(ApiError::Timeout(
            "Timed out waiting for service readiness".into(),
        ));
    }

    named_request_span(
        request,
        |request| async move { handler(service, request).await },
        request_name,
    )
    .await
}

/// Check if the required scope is held in the request's token, or if the request has
/// a token with 'admin' scope then always permit it.
fn check_permissions(request: &Request<Body>, required_scope: Scope) -> Result<(), ApiError> {
    check_permission_with(request, |claims| {
        match crate::auth::check_permission(claims, required_scope) {
            Err(e) => match crate::auth::check_permission(claims, Scope::Admin) {
                Ok(()) => Ok(()),
                Err(_) => Err(e),
            },
            Ok(()) => Ok(()),
        }
    })
}

#[derive(Clone, Debug)]
struct RequestMeta {
    method: hyper::http::Method,
    at: Instant,
}

pub fn prologue_leadership_status_check_middleware<
    B: hyper::body::HttpBody + Send + Sync + 'static,
>() -> Middleware<B, ApiError> {
    Middleware::pre(move |req| async move {
        let state = get_state(&req);
        let leadership_status = state.service.get_leadership_status();

        enum AllowedRoutes {
            All,
            Some(&'static [&'static str]),
        }

        let allowed_routes = match leadership_status {
            LeadershipStatus::Leader => AllowedRoutes::All,
            LeadershipStatus::SteppedDown => AllowedRoutes::All,
            LeadershipStatus::Candidate => AllowedRoutes::Some(&[
                "/ready",
                "/status",
                "/metrics",
                "/profile/cpu",
                "/profile/heap",
            ]),
        };

        match allowed_routes {
            AllowedRoutes::All => Ok(req),
            AllowedRoutes::Some(allowed) if allowed.contains(&req.uri().path()) => Ok(req),
            _ => {
                tracing::info!(
                    "Request {} not allowed due to current leadership state",
                    req.uri()
                );

                Err(ApiError::ResourceUnavailable(
                    format!("Current leadership status is {leadership_status}").into(),
                ))
            }
        }
    })
}

fn prologue_metrics_middleware<B: hyper::body::HttpBody + Send + Sync + 'static>()
-> Middleware<B, ApiError> {
    Middleware::pre(move |req| async move {
        let meta = RequestMeta {
            method: req.method().clone(),
            at: Instant::now(),
        };

        req.set_context(meta);

        Ok(req)
    })
}

fn epilogue_metrics_middleware<B: hyper::body::HttpBody + Send + Sync + 'static>()
-> Middleware<B, ApiError> {
    Middleware::post_with_info(move |resp, req_info| async move {
        let request_name = match req_info.context::<RequestName>() {
            Some(name) => name,
            None => {
                return Ok(resp);
            }
        };

        if let Some(meta) = req_info.context::<RequestMeta>() {
            let status = &crate::metrics::METRICS_REGISTRY
                .metrics_group
                .storage_controller_http_request_status;
            let latency = &crate::metrics::METRICS_REGISTRY
                .metrics_group
                .storage_controller_http_request_latency;

            status.inc(HttpRequestStatusLabelGroup {
                path: request_name.0,
                method: meta.method.clone().into(),
                status: crate::metrics::StatusCode(resp.status()),
            });

            latency.observe(
                HttpRequestLatencyLabelGroup {
                    path: request_name.0,
                    method: meta.method.into(),
                },
                meta.at.elapsed().as_secs_f64(),
            );
        }
        Ok(resp)
    })
}

pub async fn measured_metrics_handler(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    pub const TEXT_FORMAT: &str = "text/plain; version=0.0.4";

    let req = match maybe_forward(req).await {
        ForwardOutcome::Forwarded(res) => {
            return res;
        }
        ForwardOutcome::NotForwarded(req) => req,
    };

    let state = get_state(&req);
    let payload = crate::metrics::METRICS_REGISTRY.encode(&state.neon_metrics);
    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, TEXT_FORMAT)
        .body(payload.into())
        .unwrap();

    Ok(response)
}

#[derive(Clone)]
struct RequestName(&'static str);

async fn named_request_span<R, H>(
    request: Request<Body>,
    handler: H,
    name: RequestName,
) -> R::Output
where
    R: Future<Output = Result<Response<Body>, ApiError>> + Send + 'static,
    H: FnOnce(Request<Body>) -> R + Send + Sync + 'static,
{
    request.set_context(name);
    request_span(request, handler).await
}

enum ForwardOutcome {
    Forwarded(Result<Response<Body>, ApiError>),
    NotForwarded(Request<Body>),
}

/// Potentially forward the request to the current storage controler leader.
/// More specifically we forward when:
/// 1. Request is not one of:
///    ["/control/v1/step_down", "/status", "/ready", "/metrics", "/profile/cpu", "/profile/heap"]
/// 2. Current instance is in [`LeadershipStatus::SteppedDown`] state
/// 3. There is a leader in the database to forward to
/// 4. Leader from step (3) is not the current instance
///
/// Why forward?
/// It turns out that we can't rely on external orchestration to promptly route trafic to the
/// new leader. This is downtime inducing. Forwarding provides a safe way out.
///
/// Why is it safe?
/// If a storcon instance is persisted in the database, then we know that it is the current leader.
/// There's one exception: time between handling step-down request and the new leader updating the
/// database.
///
/// Let's treat the happy case first. The stepped down node does not produce any side effects,
/// since all request handling happens on the leader.
///
/// As for the edge case, we are guaranteed to always have a maximum of two running instances.
/// Hence, if we are in the edge case scenario the leader persisted in the database is the
/// stepped down instance that received the request. Condition (4) above covers this scenario.
async fn maybe_forward(req: Request<Body>) -> ForwardOutcome {
    const NOT_FOR_FORWARD: &[&str] = &[
        "/control/v1/step_down",
        "/status",
        "/live",
        "/ready",
        "/metrics",
        "/profile/cpu",
        "/profile/heap",
    ];

    let uri = req.uri();
    let uri_for_forward = !NOT_FOR_FORWARD.contains(&uri.path());

    // Fast return before trying to take any Service locks, if we will never forward anyway
    if !uri_for_forward {
        return ForwardOutcome::NotForwarded(req);
    }

    let state = get_state(&req);
    let leadership_status = state.service.get_leadership_status();

    if leadership_status != LeadershipStatus::SteppedDown {
        return ForwardOutcome::NotForwarded(req);
    }

    let leader = state.service.get_leader().await;
    let leader = {
        match leader {
            Ok(Some(leader)) => leader,
            Ok(None) => {
                return ForwardOutcome::Forwarded(Err(ApiError::ResourceUnavailable(
                    "No leader to forward to while in stepped down state".into(),
                )));
            }
            Err(err) => {
                return ForwardOutcome::Forwarded(Err(ApiError::InternalServerError(
                    anyhow::anyhow!(
                        "Failed to get leader for forwarding while in stepped down state: {err}"
                    ),
                )));
            }
        }
    };

    let cfg = state.service.get_config();
    if let Some(ref self_addr) = cfg.address_for_peers {
        let leader_addr = match Uri::from_str(leader.address.as_str()) {
            Ok(uri) => uri,
            Err(err) => {
                return ForwardOutcome::Forwarded(Err(ApiError::InternalServerError(
                    anyhow::anyhow!(
                        "Failed to parse leader uri for forwarding while in stepped down state: {err}"
                    ),
                )));
            }
        };

        if *self_addr == leader_addr {
            return ForwardOutcome::Forwarded(Err(ApiError::ResourceUnavailable(
                "Leader is stepped down instance".into(),
            )));
        }
    }

    tracing::info!("Forwarding {} to leader at {}", uri, leader.address);

    // Use [`RECONCILE_TIMEOUT`] as the max amount of time a request should block for and
    // include some leeway to get the timeout for proxied requests.
    const PROXIED_REQUEST_TIMEOUT: Duration = Duration::from_secs(RECONCILE_TIMEOUT.as_secs() + 10);

    let client = state.service.get_http_client().clone();

    let request: reqwest::Request = match convert_request(
        req,
        &client,
        leader.address,
        PROXIED_REQUEST_TIMEOUT,
    )
    .await
    {
        Ok(r) => r,
        Err(err) => {
            return ForwardOutcome::Forwarded(Err(ApiError::InternalServerError(anyhow::anyhow!(
                "Failed to convert request for forwarding while in stepped down state: {err}"
            ))));
        }
    };

    let response = match client.execute(request).await {
        Ok(r) => r,
        Err(err) => {
            return ForwardOutcome::Forwarded(Err(ApiError::InternalServerError(anyhow::anyhow!(
                "Failed to forward while in stepped down state: {err}"
            ))));
        }
    };

    ForwardOutcome::Forwarded(convert_response(response).await)
}

/// Convert a [`reqwest::Response`] to a [hyper::Response`] by passing through
/// a stable representation (string, bytes or integer)
///
/// Ideally, we would not have to do this since both types use the http crate
/// under the hood. However, they use different versions of the crate and keeping
/// second order dependencies in sync is difficult.
async fn convert_response(resp: reqwest::Response) -> Result<hyper::Response<Body>, ApiError> {
    use std::str::FromStr;

    let mut builder = hyper::Response::builder().status(resp.status().as_u16());
    for (key, value) in resp.headers().into_iter() {
        let key = hyper::header::HeaderName::from_str(key.as_str()).map_err(|err| {
            ApiError::InternalServerError(anyhow::anyhow!("Response conversion failed: {err}"))
        })?;

        let value = hyper::header::HeaderValue::from_bytes(value.as_bytes()).map_err(|err| {
            ApiError::InternalServerError(anyhow::anyhow!("Response conversion failed: {err}"))
        })?;

        builder = builder.header(key, value);
    }

    let body = http::Body::wrap_stream(resp.bytes_stream());

    builder.body(body).map_err(|err| {
        ApiError::InternalServerError(anyhow::anyhow!("Response conversion failed: {err}"))
    })
}

/// Convert a [`reqwest::Request`] to a [hyper::Request`] by passing through
/// a stable representation (string, bytes or integer)
///
/// See [`convert_response`] for why we are doing it this way.
async fn convert_request(
    req: hyper::Request<Body>,
    client: &reqwest::Client,
    to_address: String,
    timeout: Duration,
) -> Result<reqwest::Request, ApiError> {
    use std::str::FromStr;

    let (parts, body) = req.into_parts();
    let method = reqwest::Method::from_str(parts.method.as_str()).map_err(|err| {
        ApiError::InternalServerError(anyhow::anyhow!("Request conversion failed: {err}"))
    })?;

    let path_and_query = parts.uri.path_and_query().ok_or_else(|| {
        ApiError::InternalServerError(anyhow::anyhow!(
            "Request conversion failed: no path and query"
        ))
    })?;

    let uri = reqwest::Url::from_str(
        format!(
            "{}{}",
            to_address.trim_end_matches("/"),
            path_and_query.as_str()
        )
        .as_str(),
    )
    .map_err(|err| {
        ApiError::InternalServerError(anyhow::anyhow!("Request conversion failed: {err}"))
    })?;

    let mut headers = reqwest::header::HeaderMap::new();
    for (key, value) in parts.headers.into_iter() {
        let key = match key {
            Some(k) => k,
            None => {
                continue;
            }
        };

        let key = reqwest::header::HeaderName::from_str(key.as_str()).map_err(|err| {
            ApiError::InternalServerError(anyhow::anyhow!("Request conversion failed: {err}"))
        })?;

        let value = reqwest::header::HeaderValue::from_bytes(value.as_bytes()).map_err(|err| {
            ApiError::InternalServerError(anyhow::anyhow!("Request conversion failed: {err}"))
        })?;

        headers.insert(key, value);
    }

    let body = hyper::body::to_bytes(body).await.map_err(|err| {
        ApiError::InternalServerError(anyhow::anyhow!("Request conversion failed: {err}"))
    })?;

    client
        .request(method, uri)
        .headers(headers)
        .body(body)
        .timeout(timeout)
        .build()
        .map_err(|err| {
            ApiError::InternalServerError(anyhow::anyhow!("Request conversion failed: {err}"))
        })
}

pub fn make_router(
    service: Arc<Service>,
    auth: Option<Arc<SwappableJwtAuth>>,
    build_info: BuildInfo,
) -> RouterBuilder<hyper::Body, ApiError> {
    let mut router = endpoint::make_router()
        .middleware(prologue_leadership_status_check_middleware())
        .middleware(prologue_metrics_middleware())
        .middleware(epilogue_metrics_middleware());
    if auth.is_some() {
        router = router.middleware(auth_middleware(|request| {
            let state = get_state(request);
            if state.allowlist_routes.contains(&request.uri().path()) {
                None
            } else {
                state.auth.as_deref()
            }
        }));
    }

    router
        .data(Arc::new(HttpState::new(service, auth, build_info)))
        // Non-prefixed generic endpoints (status, metrics, profiling)
        .get("/metrics", |r| {
            named_request_span(r, measured_metrics_handler, RequestName("metrics"))
        })
        .get("/status", |r| {
            named_request_span(r, handle_status, RequestName("status"))
        })
        .get("/live", |r| {
            named_request_span(r, handle_live, RequestName("live"))
        })
        .get("/ready", |r| {
            named_request_span(r, handle_ready, RequestName("ready"))
        })
        .get("/profile/cpu", |r| {
            named_request_span(r, profile_cpu_handler, RequestName("profile_cpu"))
        })
        .get("/profile/heap", |r| {
            named_request_span(r, profile_heap_handler, RequestName("profile_heap"))
        })
        // Upcalls for the pageserver: point the pageserver's `control_plane_api` config to this prefix
        .post("/upcall/v1/re-attach", |r| {
            named_request_span(r, handle_re_attach, RequestName("upcall_v1_reattach"))
        })
        .post("/upcall/v1/validate", |r| {
            named_request_span(r, handle_validate, RequestName("upcall_v1_validate"))
        })
        .get("/upcall/v1/timeline_import_status", |r| {
            named_request_span(
                r,
                handle_get_timeline_import_status,
                RequestName("upcall_v1_timeline_import_status"),
            )
        })
        .post("/upcall/v1/timeline_import_status", |r| {
            named_request_span(
                r,
                handle_put_timeline_import_status,
                RequestName("upcall_v1_timeline_import_status"),
            )
        })
        // Test/dev/debug endpoints
        .post("/debug/v1/attach-hook", |r| {
            named_request_span(r, handle_attach_hook, RequestName("debug_v1_attach_hook"))
        })
        .post("/debug/v1/inspect", |r| {
            named_request_span(r, handle_inspect, RequestName("debug_v1_inspect"))
        })
        .post("/debug/v1/tenant/:tenant_id/drop", |r| {
            named_request_span(r, handle_tenant_drop, RequestName("debug_v1_tenant_drop"))
        })
        .post("/debug/v1/node/:node_id/drop", |r| {
            named_request_span(r, handle_node_drop, RequestName("debug_v1_node_drop"))
        })
        .delete("/debug/v1/tombstone/:node_id", |r| {
            named_request_span(
                r,
                handle_tombstone_delete,
                RequestName("debug_v1_tombstone_delete"),
            )
        })
        .get("/debug/v1/tombstone", |r| {
            named_request_span(
                r,
                handle_tombstone_list,
                RequestName("debug_v1_tombstone_list"),
            )
        })
        .post("/debug/v1/tenant/:tenant_id/import", |r| {
            named_request_span(
                r,
                handle_tenant_import,
                RequestName("debug_v1_tenant_import"),
            )
        })
        .get("/debug/v1/tenant", |r| {
            named_request_span(r, handle_tenants_dump, RequestName("debug_v1_tenant"))
        })
        .get("/debug/v1/tenant/:tenant_id/locate", |r| {
            tenant_service_handler(
                r,
                handle_tenant_locate,
                RequestName("debug_v1_tenant_locate"),
            )
        })
        .post(
            "/debug/v1/tenant/:tenant_id/timeline/:timeline_id/import",
            |r| {
                named_request_span(
                    r,
                    handle_timeline_import,
                    RequestName("debug_v1_timeline_import"),
                )
            },
        )
        .get(
            "/debug/v1/tenant/:tenant_id/timeline/:timeline_id/locate",
            |r| {
                tenant_service_handler(
                    r,
                    handle_tenant_timeline_locate,
                    RequestName("v1_tenant_timeline_locate"),
                )
            },
        )
        .get("/debug/v1/scheduler", |r| {
            named_request_span(r, handle_scheduler_dump, RequestName("debug_v1_scheduler"))
        })
        .post("/debug/v1/consistency_check", |r| {
            named_request_span(
                r,
                handle_consistency_check,
                RequestName("debug_v1_consistency_check"),
            )
        })
        .post("/debug/v1/reconcile_all", |r| {
            request_span(r, handle_reconcile_all)
        })
        .put("/debug/v1/failpoints", |r| {
            request_span(r, |r| failpoints_handler(r, CancellationToken::new()))
        })
        // Node operations
        .post("/control/v1/node", |r| {
            named_request_span(r, handle_node_register, RequestName("control_v1_node"))
        })
        // This endpoint is deprecated and will be removed in a future version.
        // Use PUT /control/v1/node/:node_id/delete instead.
        .delete("/control/v1/node/:node_id", |r| {
            named_request_span(
                r,
                handle_node_delete_old,
                RequestName("control_v1_node_delete"),
            )
        })
        .get("/control/v1/node", |r| {
            named_request_span(r, handle_node_list, RequestName("control_v1_node"))
        })
        .put("/control/v1/node/:node_id/config", |r| {
            named_request_span(
                r,
                handle_node_configure,
                RequestName("control_v1_node_config"),
            )
        })
        .get("/control/v1/node/:node_id", |r| {
            named_request_span(r, handle_node_status, RequestName("control_v1_node_status"))
        })
        .get("/control/v1/node/:node_id/shards", |r| {
            named_request_span(
                r,
                handle_node_shards,
                RequestName("control_v1_node_describe"),
            )
        })
        .get("/control/v1/leader", |r| {
            named_request_span(r, handle_get_leader, RequestName("control_v1_get_leader"))
        })
        .put("/control/v1/node/:node_id/delete", |r| {
            named_request_span(
                r,
                handle_node_delete,
                RequestName("control_v1_start_node_delete"),
            )
        })
        .delete("/control/v1/node/:node_id/delete", |r| {
            named_request_span(
                r,
                handle_cancel_node_delete,
                RequestName("control_v1_cancel_node_delete"),
            )
        })
        .put("/control/v1/node/:node_id/drain", |r| {
            named_request_span(r, handle_node_drain, RequestName("control_v1_node_drain"))
        })
        .delete("/control/v1/node/:node_id/drain", |r| {
            named_request_span(
                r,
                handle_cancel_node_drain,
                RequestName("control_v1_cancel_node_drain"),
            )
        })
        .put("/control/v1/node/:node_id/fill", |r| {
            named_request_span(r, handle_node_fill, RequestName("control_v1_node_fill"))
        })
        .delete("/control/v1/node/:node_id/fill", |r| {
            named_request_span(
                r,
                handle_cancel_node_fill,
                RequestName("control_v1_cancel_node_fill"),
            )
        })
        // Metadata health operations
        .post("/control/v1/metadata_health/update", |r| {
            named_request_span(
                r,
                handle_metadata_health_update,
                RequestName("control_v1_metadata_health_update"),
            )
        })
        .get("/control/v1/metadata_health/unhealthy", |r| {
            named_request_span(
                r,
                handle_metadata_health_list_unhealthy,
                RequestName("control_v1_metadata_health_list_unhealthy"),
            )
        })
        .post("/control/v1/metadata_health/outdated", |r| {
            named_request_span(
                r,
                handle_metadata_health_list_outdated,
                RequestName("control_v1_metadata_health_list_outdated"),
            )
        })
        // Safekeepers
        .get("/control/v1/safekeeper", |r| {
            named_request_span(
                r,
                handle_safekeeper_list,
                RequestName("control_v1_safekeeper_list"),
            )
        })
        .get("/control/v1/safekeeper/:id", |r| {
            named_request_span(r, handle_get_safekeeper, RequestName("v1_safekeeper"))
        })
        .post("/control/v1/safekeeper/:id", |r| {
            // id is in the body
            named_request_span(
                r,
                handle_upsert_safekeeper,
                RequestName("v1_safekeeper_post"),
            )
        })
        .post("/control/v1/safekeeper/:id/scheduling_policy", |r| {
            named_request_span(
                r,
                handle_safekeeper_scheduling_policy,
                RequestName("v1_safekeeper_scheduling_policy"),
            )
        })
        // Tenant Shard operations
        .put("/control/v1/tenant/:tenant_shard_id/migrate", |r| {
            tenant_service_handler(
                r,
                handle_tenant_shard_migrate,
                RequestName("control_v1_tenant_migrate"),
            )
        })
        .put(
            "/control/v1/tenant/:tenant_shard_id/migrate_secondary",
            |r| {
                tenant_service_handler(
                    r,
                    handle_tenant_shard_migrate_secondary,
                    RequestName("control_v1_tenant_migrate_secondary"),
                )
            },
        )
        .put(
            "/control/v1/tenant/:tenant_shard_id/cancel_reconcile",
            |r| {
                tenant_service_handler(
                    r,
                    handle_tenant_shard_cancel_reconcile,
                    RequestName("control_v1_tenant_cancel_reconcile"),
                )
            },
        )
        .put("/control/v1/tenant/:tenant_id/shard_split", |r| {
            tenant_service_handler(
                r,
                handle_tenant_shard_split,
                RequestName("control_v1_tenant_shard_split"),
            )
        })
        .get("/control/v1/tenant/:tenant_id", |r| {
            tenant_service_handler(
                r,
                handle_tenant_describe,
                RequestName("control_v1_tenant_describe"),
            )
        })
        .get("/control/v1/tenant", |r| {
            tenant_service_handler(r, handle_tenant_list, RequestName("control_v1_tenant_list"))
        })
        .put("/control/v1/tenant/:tenant_id/policy", |r| {
            named_request_span(
                r,
                handle_tenant_update_policy,
                RequestName("control_v1_tenant_policy"),
            )
        })
        .put("/control/v1/preferred_azs", |r| {
            named_request_span(
                r,
                handle_update_preferred_azs,
                RequestName("control_v1_preferred_azs"),
            )
        })
        .put("/control/v1/step_down", |r| {
            named_request_span(r, handle_step_down, RequestName("control_v1_step_down"))
        })
        // Tenant operations
        // The ^/v1/ endpoints act as a "Virtual Pageserver", enabling shard-naive clients to call into
        // this service to manage tenants that actually consist of many tenant shards, as if they are a single entity.
        .post("/v1/tenant", |r| {
            tenant_service_handler(r, handle_tenant_create, RequestName("v1_tenant"))
        })
        .delete("/v1/tenant/:tenant_id", |r| {
            tenant_service_handler(r, handle_tenant_delete, RequestName("v1_tenant"))
        })
        .patch("/v1/tenant/config", |r| {
            tenant_service_handler(
                r,
                handle_tenant_config_patch,
                RequestName("v1_tenant_config"),
            )
        })
        .put("/v1/tenant/config", |r| {
            tenant_service_handler(r, handle_tenant_config_set, RequestName("v1_tenant_config"))
        })
        .get("/v1/tenant/:tenant_id/config", |r| {
            tenant_service_handler(r, handle_tenant_config_get, RequestName("v1_tenant_config"))
        })
        .put("/v1/tenant/:tenant_shard_id/location_config", |r| {
            tenant_service_handler(
                r,
                handle_tenant_location_config,
                RequestName("v1_tenant_location_config"),
            )
        })
        .put("/v1/tenant/:tenant_id/time_travel_remote_storage", |r| {
            tenant_service_handler(
                r,
                handle_tenant_time_travel_remote_storage,
                RequestName("v1_tenant_time_travel_remote_storage"),
            )
        })
        .post("/v1/tenant/:tenant_id/secondary/download", |r| {
            tenant_service_handler(
                r,
                handle_tenant_secondary_download,
                RequestName("v1_tenant_secondary_download"),
            )
        })
        // Timeline operations
        .get("/control/v1/tenant/:tenant_id/timeline/:timeline_id", |r| {
            tenant_service_handler(
                r,
                handle_tenant_timeline_describe,
                RequestName("v1_tenant_timeline_describe"),
            )
        })
        .delete("/v1/tenant/:tenant_id/timeline/:timeline_id", |r| {
            tenant_service_handler(
                r,
                handle_tenant_timeline_delete,
                RequestName("v1_tenant_timeline"),
            )
        })
        .post("/v1/tenant/:tenant_id/timeline", |r| {
            tenant_service_handler(
                r,
                handle_tenant_timeline_create,
                RequestName("v1_tenant_timeline"),
            )
        })
        .put(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/archival_config",
            |r| {
                tenant_service_handler(
                    r,
                    handle_tenant_timeline_archival_config,
                    RequestName("v1_tenant_timeline_archival_config"),
                )
            },
        )
        .put(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/detach_ancestor",
            |r| {
                tenant_service_handler(
                    r,
                    handle_tenant_timeline_detach_ancestor,
                    RequestName("v1_tenant_timeline_detach_ancestor"),
                )
            },
        )
        .post(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/block_gc",
            |r| {
                tenant_service_handler(
                    r,
                    |s, r| handle_tenant_timeline_block_unblock_gc(s, r, BlockUnblock::Block),
                    RequestName("v1_tenant_timeline_block_unblock_gc"),
                )
            },
        )
        .post(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/unblock_gc",
            |r| {
                tenant_service_handler(
                    r,
                    |s, r| handle_tenant_timeline_block_unblock_gc(s, r, BlockUnblock::Unblock),
                    RequestName("v1_tenant_timeline_block_unblock_gc"),
                )
            },
        )
        .post(
            "/v1/tenant/:tenant_shard_id/timeline/:timeline_id/download_heatmap_layers",
            |r| {
                tenant_service_handler(
                    r,
                    handle_tenant_timeline_download_heatmap_layers,
                    RequestName("v1_tenant_timeline_download_heatmap_layers"),
                )
            },
        )
        .post(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/safekeeper_migrate",
            |r| {
                tenant_service_handler(
                    r,
                    handle_tenant_timeline_safekeeper_migrate,
                    RequestName("v1_tenant_timeline_safekeeper_migrate"),
                )
            },
        )
        .post(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/safekeeper_migrate_abort",
            |r| {
                tenant_service_handler(
                    r,
                    handle_tenant_timeline_safekeeper_migrate_abort,
                    RequestName("v1_tenant_timeline_safekeeper_migrate_abort"),
                )
            },
        )
        // LSN lease passthrough to all shards
        .post(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/lsn_lease",
            |r| {
                tenant_service_handler(
                    r,
                    handle_tenant_timeline_lsn_lease,
                    RequestName("v1_tenant_timeline_lsn_lease"),
                )
            },
        )
        // Tenant timeline mark_invisible passthrough to shard zero
        .put(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/mark_invisible",
            |r| {
                tenant_service_handler(
                    r,
                    handle_tenant_timeline_passthrough,
                    RequestName("v1_tenant_timeline_mark_invisible_passthrough"),
                )
            },
        )
        // Tenant detail GET passthrough to shard zero:
        .get("/v1/tenant/:tenant_id", |r| {
            tenant_service_handler(
                r,
                handle_tenant_timeline_passthrough,
                RequestName("v1_tenant_passthrough"),
            )
        })
        // The `*` in the  URL is a wildcard: any tenant/timeline GET APIs on the pageserver
        // are implicitly exposed here.  This must be last in the list to avoid
        // taking precedence over other GET methods we might implement by hand.
        .get("/v1/tenant/:tenant_id/*", |r| {
            tenant_service_handler(
                r,
                handle_tenant_timeline_passthrough,
                RequestName("v1_tenant_passthrough"),
            )
        })
}

#[cfg(test)]
mod test {

    use super::path_without_ids;

    #[test]
    fn test_path_without_ids() {
        assert_eq!(
            path_without_ids(
                "/v1/tenant/1a2b3344556677881122334455667788/timeline/AA223344556677881122334455667788"
            ),
            "/v1/tenant//timeline/"
        );
        assert_eq!(
            path_without_ids(
                "/v1/tenant/1a2b3344556677881122334455667788-0108/timeline/AA223344556677881122334455667788"
            ),
            "/v1/tenant//timeline/"
        );
        assert_eq!(
            path_without_ids(
                "/v1/tenant/1a2b3344556677881122334455667788-0108/timeline/AA223344556677881122334455667788?parameter=foo"
            ),
            "/v1/tenant//timeline/"
        );
    }
}
