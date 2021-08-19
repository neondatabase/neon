use std::sync::Arc;

use anyhow::Result;
use hyper::header;
use hyper::StatusCode;
use hyper::{Body, Request, Response, Uri};
use routerify::{ext::RequestExt, RouterBuilder};
use zenith_utils::auth::JwtAuth;
use zenith_utils::http::endpoint::attach_openapi_ui;
use zenith_utils::http::endpoint::auth_middleware;
use zenith_utils::http::endpoint::check_permission;
use zenith_utils::http::error::ApiError;
use zenith_utils::http::{
    endpoint,
    error::HttpErrorBody,
    json::{json_request, json_response},
};

use super::models::BranchCreateRequest;
use super::models::TenantCreateRequest;
use crate::page_cache;
use crate::{
    branches::{self},
    PageServerConf, ZTenantId,
};

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
async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{}"))
        .map_err(ApiError::from_err)?)
}

async fn branch_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let request_data: BranchCreateRequest = json_request(&mut request).await?;

    check_permission(&request, Some(request_data.tenant_id))?;

    let response_data = tokio::task::spawn_blocking(move || {
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

async fn branch_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenantid: ZTenantId = match request.param("tenant_id") {
        Some(arg) => arg
            .parse()
            .map_err(|_| ApiError::BadRequest("failed to parse tenant id".to_string()))?,
        None => {
            return Err(ApiError::BadRequest(
                "no tenant id specified in path param".to_string(),
            ))
        }
    };

    check_permission(&request, Some(tenantid))?;

    let response_data = tokio::task::spawn_blocking(move || {
        crate::branches::get_branches(get_config(&request), &tenantid)
    })
    .await
    .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::OK, response_data)?)
}

async fn tenant_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // check for management permission
    check_permission(&request, None)?;

    let response_data =
        tokio::task::spawn_blocking(move || crate::branches::get_tenants(get_config(&request)))
            .await
            .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::OK, response_data)?)
}

async fn tenant_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // check for management permission
    check_permission(&request, None)?;

    let request_data: TenantCreateRequest = json_request(&mut request).await?;

    let response_data = tokio::task::spawn_blocking(move || {
        page_cache::create_repository_for_tenant(get_config(&request), request_data.tenant_id)
    })
    .await
    .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::CREATED, response_data)?)
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
        .get("/v1/branch/:tenant_id", branch_list_handler)
        .post("/v1/branch", branch_create_handler)
        .get("/v1/tenant", tenant_list_handler)
        .post("/v1/tenant", tenant_create_handler)
        .any(handler_404)
}
