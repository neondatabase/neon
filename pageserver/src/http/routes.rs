use std::sync::Arc;

use anyhow::Result;
use hyper::header;
use hyper::StatusCode;
use hyper::{Body, Request, Response, Uri};
use routerify::Middleware;
use routerify::{ext::RequestExt, RouterBuilder};
use zenith_utils::auth::JwtAuth;
use zenith_utils::http::endpoint::attach_openapi_ui;
use zenith_utils::http::endpoint::auth_middleware;
use zenith_utils::http::endpoint::check_permission;
use zenith_utils::http::endpoint::AuthProvider;
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
    auth: Arc<Option<JwtAuth>>,
    whitelist_routes: Vec<Uri>,
}

impl State {
    fn new(conf: &'static PageServerConf, auth: Arc<Option<JwtAuth>>) -> Self {
        let whitelist_routes = ["/v1/status", "/v1/doc", "/swagger.yml"]
            .iter()
            .map(|v| v.parse().unwrap())
            .collect::<Vec<_>>();
        Self {
            conf,
            auth,
            whitelist_routes,
        }
    }
}

impl AuthProvider for State {
    fn provide_auth(&self, req: &Request<Body>) -> Arc<Option<JwtAuth>> {
        if self.whitelist_routes.contains(req.uri()) {
            Arc::new(None)
        } else {
            self.auth.clone()
        }
    }
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
    let state = request.data::<Arc<State>>().unwrap().clone();
    let request_data: BranchCreateRequest = json_request(&mut request).await?;

    check_permission(&request, Some(request_data.tenant_id))?;

    let response_data = tokio::task::spawn_blocking(move || {
        branches::create_branch(
            state.conf,
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

    let state = request.data::<Arc<State>>().unwrap().clone();
    let response_data =
        tokio::task::spawn_blocking(move || crate::branches::get_branches(state.conf, &tenantid))
            .await
            .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::OK, response_data)?)
}

async fn tenant_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // check for management permission
    check_permission(&request, None)?;

    let state = request.data::<Arc<State>>().unwrap().clone();
    let response_data =
        tokio::task::spawn_blocking(move || crate::branches::get_tenants(state.conf))
            .await
            .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::OK, response_data)?)
}

async fn tenant_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // check for management permission
    check_permission(&request, None)?;

    let state = request.data::<Arc<State>>().unwrap().clone();
    let request_data: TenantCreateRequest = json_request(&mut request).await?;

    let response_data = tokio::task::spawn_blocking(move || {
        page_cache::create_repository_for_tenant(state.conf, request_data.tenant_id)
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

pub fn get_router(
    conf: &'static PageServerConf,
    auth: Arc<Option<JwtAuth>>,
) -> RouterBuilder<hyper::Body, ApiError> {
    let spec = include_bytes!("openapi_spec.yml");
    let mut router = attach_openapi_ui(endpoint::get_router(), spec, "/swagger.yml", "/v1/doc");
    if let Some(_) = &auth.as_ref() {
        // note that State is used as a type parameteer without an Arc
        // this is a simple solution because it is not possible to implement
        // AuthProvider for Arc<State> so middleware assumes that state is wrapped in Arc
        router = router.middleware(Middleware::pre(auth_middleware::<State>))
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
