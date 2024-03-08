use crate::reconciler::ReconcileError;
use crate::service::{Service, STARTUP_RECONCILE_TIMEOUT};
use hyper::{Body, Request, Response};
use hyper::{StatusCode, Uri};
use pageserver_api::models::{
    TenantConfigRequest, TenantCreateRequest, TenantLocationConfigRequest, TenantShardSplitRequest,
    TenantTimeTravelRequest, TimelineCreateRequest,
};
use pageserver_api::shard::TenantShardId;
use pageserver_client::mgmt_api;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use utils::auth::{Scope, SwappableJwtAuth};
use utils::failpoint_support::failpoints_handler;
use utils::http::endpoint::{auth_middleware, check_permission_with, request_span};
use utils::http::request::{must_get_query_param, parse_request_param};
use utils::id::{TenantId, TimelineId};

use utils::{
    http::{
        endpoint::{self},
        error::ApiError,
        json::{json_request, json_response},
        RequestExt, RouterBuilder,
    },
    id::NodeId,
};

use pageserver_api::controller_api::{
    NodeConfigureRequest, NodeRegisterRequest, TenantShardMigrateRequest,
};
use pageserver_api::upcall_api::{ReAttachRequest, ValidateRequest};

use control_plane::attachment_service::{AttachHookRequest, InspectRequest};

/// State available to HTTP request handlers
#[derive(Clone)]
pub struct HttpState {
    service: Arc<crate::service::Service>,
    auth: Option<Arc<SwappableJwtAuth>>,
    allowlist_routes: Vec<Uri>,
}

impl HttpState {
    pub fn new(service: Arc<crate::service::Service>, auth: Option<Arc<SwappableJwtAuth>>) -> Self {
        let allowlist_routes = ["/status", "/ready", "/metrics"]
            .iter()
            .map(|v| v.parse().unwrap())
            .collect::<Vec<_>>();
        Self {
            service,
            auth,
            allowlist_routes,
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

/// Pageserver calls into this on startup, to learn which tenants it should attach
async fn handle_re_attach(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::GenerationsApi)?;

    let reattach_req = json_request::<ReAttachRequest>(&mut req).await?;
    let state = get_state(&req);
    json_response(StatusCode::OK, state.service.re_attach(reattach_req).await?)
}

/// Pageserver calls into this before doing deletions, to confirm that it still
/// holds the latest generation for the tenants with deletions enqueued
async fn handle_validate(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::GenerationsApi)?;

    let validate_req = json_request::<ValidateRequest>(&mut req).await?;
    let state = get_state(&req);
    json_response(StatusCode::OK, state.service.validate(validate_req))
}

/// Call into this before attaching a tenant to a pageserver, to acquire a generation number
/// (in the real control plane this is unnecessary, because the same program is managing
///  generation numbers and doing attachments).
async fn handle_attach_hook(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

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

async fn handle_inspect(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let inspect_req = json_request::<InspectRequest>(&mut req).await?;

    let state = get_state(&req);

    json_response(StatusCode::OK, state.service.inspect(inspect_req))
}

async fn handle_tenant_create(
    service: Arc<Service>,
    mut req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::PageServerApi)?;

    let create_req = json_request::<TenantCreateRequest>(&mut req).await?;

    json_response(
        StatusCode::CREATED,
        service.tenant_create(create_req).await?,
    )
}

// For tenant and timeline deletions, which both implement an "initially return 202, then 404 once
// we're done" semantic, we wrap with a retry loop to expose a simpler API upstream.  This avoids
// needing to track a "deleting" state for tenants.
async fn deletion_wrapper<R, F>(service: Arc<Service>, f: F) -> Result<Response<Body>, ApiError>
where
    R: std::future::Future<Output = Result<StatusCode, ApiError>> + Send + 'static,
    F: Fn(Arc<Service>) -> R + Send + Sync + 'static,
{
    let started_at = Instant::now();
    // To keep deletion reasonably snappy for small tenants, initially check after 1 second if deletion
    // completed.
    let mut retry_period = Duration::from_secs(1);
    // On subsequent retries, wait longer.
    let max_retry_period = Duration::from_secs(5);
    // Enable callers with a 30 second request timeout to reliably get a response
    let max_wait = Duration::from_secs(25);

    loop {
        let status = f(service.clone()).await?;
        match status {
            StatusCode::ACCEPTED => {
                tracing::info!("Deletion accepted, waiting to try again...");
                tokio::time::sleep(retry_period).await;
                retry_period = max_retry_period;
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
        if now + retry_period > started_at + max_wait {
            tracing::info!("Deletion timed out waiting for 404");
            // REQUEST_TIMEOUT would be more appropriate, but CONFLICT is already part of
            // the pageserver's swagger definition for this endpoint, and has the same desired
            // effect of causing the control plane to retry later.
            return json_response(StatusCode::CONFLICT, ());
        }
    }
}

async fn handle_tenant_location_config(
    service: Arc<Service>,
    mut req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;

    let config_req = json_request::<TenantLocationConfigRequest>(&mut req).await?;
    json_response(
        StatusCode::OK,
        service
            .tenant_location_config(tenant_id, config_req)
            .await?,
    )
}

async fn handle_tenant_config_set(
    service: Arc<Service>,
    mut req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::PageServerApi)?;

    let config_req = json_request::<TenantConfigRequest>(&mut req).await?;

    json_response(StatusCode::OK, service.tenant_config_set(config_req).await?)
}

async fn handle_tenant_config_get(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;

    json_response(StatusCode::OK, service.tenant_config_get(tenant_id)?)
}

async fn handle_tenant_time_travel_remote_storage(
    service: Arc<Service>,
    mut req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;

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

async fn handle_tenant_secondary_download(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    service.tenant_secondary_download(tenant_id).await?;
    json_response(StatusCode::OK, ())
}

async fn handle_tenant_delete(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;

    deletion_wrapper(service, move |service| async move {
        service.tenant_delete(tenant_id).await
    })
    .await
}

async fn handle_tenant_timeline_create(
    service: Arc<Service>,
    mut req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;

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
    check_permissions(&req, Scope::PageServerApi)?;

    let timeline_id: TimelineId = parse_request_param(&req, "timeline_id")?;

    deletion_wrapper(service, move |service| async move {
        service.tenant_timeline_delete(tenant_id, timeline_id).await
    })
    .await
}

async fn handle_tenant_timeline_passthrough(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;

    let Some(path) = req.uri().path_and_query() else {
        // This should never happen, our request router only calls us if there is a path
        return Err(ApiError::BadRequest(anyhow::anyhow!("Missing path")));
    };

    tracing::info!("Proxying request for tenant {} ({})", tenant_id, path);

    // Find the node that holds shard zero
    let (base_url, tenant_shard_id) = service.tenant_shard0_baseurl(tenant_id)?;

    // Callers will always pass an unsharded tenant ID.  Before proxying, we must
    // rewrite this to a shard-aware shard zero ID.
    let path = format!("{}", path);
    let tenant_str = tenant_id.to_string();
    let tenant_shard_str = format!("{}", tenant_shard_id);
    let path = path.replace(&tenant_str, &tenant_shard_str);

    let client = mgmt_api::Client::new(base_url, service.get_config().jwt_token.as_deref());
    let resp = client.get_raw(path).await.map_err(|_e|
        // FIXME: give APiError a proper Unavailable variant.  We return 503 here because
        // if we can't successfully send a request to the pageserver, we aren't available.
        ApiError::ShuttingDown)?;

    // We have a reqest::Response, would like a http::Response
    let mut builder = hyper::Response::builder()
        .status(resp.status())
        .version(resp.version());
    for (k, v) in resp.headers() {
        builder = builder.header(k, v);
    }

    let response = builder
        .body(Body::wrap_stream(resp.bytes_stream()))
        .map_err(|e| ApiError::InternalServerError(e.into()))?;

    Ok(response)
}

async fn handle_tenant_locate(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    json_response(StatusCode::OK, service.tenant_locate(tenant_id)?)
}

async fn handle_node_register(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let register_req = json_request::<NodeRegisterRequest>(&mut req).await?;
    let state = get_state(&req);
    state.service.node_register(register_req).await?;
    json_response(StatusCode::OK, ())
}

async fn handle_node_list(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let state = get_state(&req);
    json_response(StatusCode::OK, state.service.node_list().await?)
}

async fn handle_node_drop(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let state = get_state(&req);
    let node_id: NodeId = parse_request_param(&req, "node_id")?;
    json_response(StatusCode::OK, state.service.node_drop(node_id).await?)
}

async fn handle_node_configure(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

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
        state.service.node_configure(config_req).await?,
    )
}

async fn handle_tenant_shard_split(
    service: Arc<Service>,
    mut req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let split_req = json_request::<TenantShardSplitRequest>(&mut req).await?;

    json_response(
        StatusCode::OK,
        service.tenant_shard_split(tenant_id, split_req).await?,
    )
}

async fn handle_tenant_shard_migrate(
    service: Arc<Service>,
    mut req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let tenant_shard_id: TenantShardId = parse_request_param(&req, "tenant_shard_id")?;
    let migrate_req = json_request::<TenantShardMigrateRequest>(&mut req).await?;
    json_response(
        StatusCode::OK,
        service
            .tenant_shard_migrate(tenant_shard_id, migrate_req)
            .await?,
    )
}

async fn handle_tenant_drop(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    check_permissions(&req, Scope::PageServerApi)?;

    let state = get_state(&req);

    json_response(StatusCode::OK, state.service.tenant_drop(tenant_id).await?)
}

async fn handle_tenants_dump(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let state = get_state(&req);
    state.service.tenants_dump()
}

async fn handle_scheduler_dump(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let state = get_state(&req);
    state.service.scheduler_dump()
}

async fn handle_consistency_check(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    check_permissions(&req, Scope::Admin)?;

    let state = get_state(&req);

    json_response(StatusCode::OK, state.service.consistency_check().await?)
}

/// Status endpoint is just used for checking that our HTTP listener is up
async fn handle_status(_req: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, ())
}

/// Readiness endpoint indicates when we're done doing startup I/O (e.g. reconciling
/// with remote pageserver nodes).  This is intended for use as a kubernetes readiness probe.
async fn handle_ready(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let state = get_state(&req);
    if state.service.startup_complete.is_ready() {
        json_response(StatusCode::OK, ())
    } else {
        json_response(StatusCode::SERVICE_UNAVAILABLE, ())
    }
}

impl From<ReconcileError> for ApiError {
    fn from(value: ReconcileError) -> Self {
        ApiError::Conflict(format!("Reconciliation error: {}", value))
    }
}

/// Common wrapper for request handlers that call into Service and will operate on tenants: they must only
/// be allowed to run if Service has finished its initial reconciliation.
async fn tenant_service_handler<R, H>(request: Request<Body>, handler: H) -> R::Output
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

    request_span(
        request,
        |request| async move { handler(service, request).await },
    )
    .await
}

fn check_permissions(request: &Request<Body>, required_scope: Scope) -> Result<(), ApiError> {
    check_permission_with(request, |claims| {
        crate::auth::check_permission(claims, required_scope)
    })
}

pub fn make_router(
    service: Arc<Service>,
    auth: Option<Arc<SwappableJwtAuth>>,
) -> RouterBuilder<hyper::Body, ApiError> {
    let mut router = endpoint::make_router();
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
        .data(Arc::new(HttpState::new(service, auth)))
        // Non-prefixed generic endpoints (status, metrics)
        .get("/status", |r| request_span(r, handle_status))
        .get("/ready", |r| request_span(r, handle_ready))
        // Upcalls for the pageserver: point the pageserver's `control_plane_api` config to this prefix
        .post("/upcall/v1/re-attach", |r| {
            request_span(r, handle_re_attach)
        })
        .post("/upcall/v1/validate", |r| request_span(r, handle_validate))
        // Test/dev/debug endpoints
        .post("/debug/v1/attach-hook", |r| {
            request_span(r, handle_attach_hook)
        })
        .post("/debug/v1/inspect", |r| request_span(r, handle_inspect))
        .post("/debug/v1/tenant/:tenant_id/drop", |r| {
            request_span(r, handle_tenant_drop)
        })
        .post("/debug/v1/node/:node_id/drop", |r| {
            request_span(r, handle_node_drop)
        })
        .get("/debug/v1/tenant", |r| request_span(r, handle_tenants_dump))
        .get("/debug/v1/scheduler", |r| {
            request_span(r, handle_scheduler_dump)
        })
        .post("/debug/v1/consistency_check", |r| {
            request_span(r, handle_consistency_check)
        })
        .put("/debug/v1/failpoints", |r| {
            request_span(r, |r| failpoints_handler(r, CancellationToken::new()))
        })
        .get("/control/v1/tenant/:tenant_id/locate", |r| {
            tenant_service_handler(r, handle_tenant_locate)
        })
        // Node operations
        .post("/control/v1/node", |r| {
            request_span(r, handle_node_register)
        })
        .get("/control/v1/node", |r| request_span(r, handle_node_list))
        .put("/control/v1/node/:node_id/config", |r| {
            request_span(r, handle_node_configure)
        })
        // Tenant Shard operations
        .put("/control/v1/tenant/:tenant_shard_id/migrate", |r| {
            tenant_service_handler(r, handle_tenant_shard_migrate)
        })
        .put("/control/v1/tenant/:tenant_id/shard_split", |r| {
            tenant_service_handler(r, handle_tenant_shard_split)
        })
        // Tenant operations
        // The ^/v1/ endpoints act as a "Virtual Pageserver", enabling shard-naive clients to call into
        // this service to manage tenants that actually consist of many tenant shards, as if they are a single entity.
        .post("/v1/tenant", |r| {
            tenant_service_handler(r, handle_tenant_create)
        })
        .delete("/v1/tenant/:tenant_id", |r| {
            tenant_service_handler(r, handle_tenant_delete)
        })
        .put("/v1/tenant/config", |r| {
            tenant_service_handler(r, handle_tenant_config_set)
        })
        .get("/v1/tenant/:tenant_id/config", |r| {
            tenant_service_handler(r, handle_tenant_config_get)
        })
        .put("/v1/tenant/:tenant_id/location_config", |r| {
            tenant_service_handler(r, handle_tenant_location_config)
        })
        .put("/v1/tenant/:tenant_id/time_travel_remote_storage", |r| {
            tenant_service_handler(r, handle_tenant_time_travel_remote_storage)
        })
        .post("/v1/tenant/:tenant_id/secondary/download", |r| {
            tenant_service_handler(r, handle_tenant_secondary_download)
        })
        // Timeline operations
        .delete("/v1/tenant/:tenant_id/timeline/:timeline_id", |r| {
            tenant_service_handler(r, handle_tenant_timeline_delete)
        })
        .post("/v1/tenant/:tenant_id/timeline", |r| {
            tenant_service_handler(r, handle_tenant_timeline_create)
        })
        // Tenant detail GET passthrough to shard zero
        .get("/v1/tenant/:tenant_id", |r| {
            tenant_service_handler(r, handle_tenant_timeline_passthrough)
        })
        // Timeline GET passthrough to shard zero.  Note that the `*` in the URL is a wildcard: any future
        // timeline GET APIs will be implicitly included.
        .get("/v1/tenant/:tenant_id/timeline*", |r| {
            tenant_service_handler(r, handle_tenant_timeline_passthrough)
        })
}
