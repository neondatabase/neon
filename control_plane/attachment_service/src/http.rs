use crate::reconciler::ReconcileError;
use crate::service::{Service, STARTUP_RECONCILE_TIMEOUT};
use hyper::{Body, Request, Response};
use hyper::{StatusCode, Uri};
use pageserver_api::models::{TenantCreateRequest, TimelineCreateRequest};
use pageserver_api::shard::TenantShardId;
use std::sync::Arc;
use utils::auth::SwappableJwtAuth;
use utils::http::endpoint::{auth_middleware, request_span};
use utils::http::request::parse_request_param;
use utils::id::TenantId;

use utils::{
    http::{
        endpoint::{self},
        error::ApiError,
        json::{json_request, json_response},
        RequestExt, RouterBuilder,
    },
    id::NodeId,
};

use pageserver_api::control_api::{ReAttachRequest, ValidateRequest};

use control_plane::attachment_service::{
    AttachHookRequest, InspectRequest, NodeConfigureRequest, NodeRegisterRequest,
    TenantShardMigrateRequest,
};

/// State available to HTTP request handlers
#[derive(Clone)]
pub struct HttpState {
    service: Arc<crate::service::Service>,
    auth: Option<Arc<SwappableJwtAuth>>,
    allowlist_routes: Vec<Uri>,
}

impl HttpState {
    pub fn new(service: Arc<crate::service::Service>, auth: Option<Arc<SwappableJwtAuth>>) -> Self {
        let allowlist_routes = ["/status"]
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
    let reattach_req = json_request::<ReAttachRequest>(&mut req).await?;
    let state = get_state(&req);
    json_response(
        StatusCode::OK,
        state
            .service
            .re_attach(reattach_req)
            .await
            .map_err(ApiError::InternalServerError)?,
    )
}

/// Pageserver calls into this before doing deletions, to confirm that it still
/// holds the latest generation for the tenants with deletions enqueued
async fn handle_validate(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let validate_req = json_request::<ValidateRequest>(&mut req).await?;
    let state = get_state(&req);
    json_response(StatusCode::OK, state.service.validate(validate_req))
}

/// Call into this before attaching a tenant to a pageserver, to acquire a generation number
/// (in the real control plane this is unnecessary, because the same program is managing
///  generation numbers and doing attachments).
async fn handle_attach_hook(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
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
    let inspect_req = json_request::<InspectRequest>(&mut req).await?;

    let state = get_state(&req);

    json_response(StatusCode::OK, state.service.inspect(inspect_req))
}

async fn handle_tenant_create(
    service: Arc<Service>,
    mut req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let create_req = json_request::<TenantCreateRequest>(&mut req).await?;
    json_response(StatusCode::OK, service.tenant_create(create_req).await?)
}

async fn handle_tenant_timeline_create(
    service: Arc<Service>,
    mut req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    let create_req = json_request::<TimelineCreateRequest>(&mut req).await?;
    json_response(
        StatusCode::OK,
        service
            .tenant_timeline_create(tenant_id, create_req)
            .await?,
    )
}

async fn handle_tenant_locate(
    service: Arc<Service>,
    req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_id: TenantId = parse_request_param(&req, "tenant_id")?;
    json_response(StatusCode::OK, service.tenant_locate(tenant_id)?)
}

async fn handle_node_register(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let register_req = json_request::<NodeRegisterRequest>(&mut req).await?;
    let state = get_state(&req);
    state.service.node_register(register_req).await?;
    json_response(StatusCode::OK, ())
}

async fn handle_node_configure(mut req: Request<Body>) -> Result<Response<Body>, ApiError> {
    let node_id: NodeId = parse_request_param(&req, "node_id")?;
    let config_req = json_request::<NodeConfigureRequest>(&mut req).await?;
    if node_id != config_req.node_id {
        return Err(ApiError::BadRequest(anyhow::anyhow!(
            "Path and body node_id differ"
        )));
    }
    let state = get_state(&req);

    json_response(StatusCode::OK, state.service.node_configure(config_req)?)
}

async fn handle_tenant_shard_migrate(
    service: Arc<Service>,
    mut req: Request<Body>,
) -> Result<Response<Body>, ApiError> {
    let tenant_shard_id: TenantShardId = parse_request_param(&req, "tenant_shard_id")?;
    let migrate_req = json_request::<TenantShardMigrateRequest>(&mut req).await?;
    json_response(
        StatusCode::OK,
        service
            .tenant_shard_migrate(tenant_shard_id, migrate_req)
            .await?,
    )
}

/// Status endpoint is just used for checking that our HTTP listener is up
async fn handle_status(_req: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(StatusCode::OK, ())
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
        .get("/status", |r| request_span(r, handle_status))
        .post("/re-attach", |r| request_span(r, handle_re_attach))
        .post("/validate", |r| request_span(r, handle_validate))
        .post("/attach-hook", |r| request_span(r, handle_attach_hook))
        .post("/inspect", |r| request_span(r, handle_inspect))
        .post("/node", |r| request_span(r, handle_node_register))
        .put("/node/:node_id/config", |r| {
            request_span(r, handle_node_configure)
        })
        .post("/v1/tenant", |r| {
            tenant_service_handler(r, handle_tenant_create)
        })
        .post("/v1/tenant/:tenant_id/timeline", |r| {
            tenant_service_handler(r, handle_tenant_timeline_create)
        })
        .get("/tenant/:tenant_id/locate", |r| {
            tenant_service_handler(r, handle_tenant_locate)
        })
        .put("/tenant/:tenant_shard_id/migrate", |r| {
            tenant_service_handler(r, handle_tenant_shard_migrate)
        })
        // Path aliases for tests_forward_compatibility
        // TODO: remove these in future PR
        .post("/re-attach", |r| request_span(r, handle_re_attach))
        .post("/validate", |r| request_span(r, handle_validate))
}
