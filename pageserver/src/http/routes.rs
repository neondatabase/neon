use std::str::FromStr;
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
use crate::branches::BranchInfo;
use crate::{branches, tenant_mgr, PageServerConf, ZTenantId};

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

fn get_request_param<'a>(
    request: &'a Request<Body>,
    param_name: &str,
) -> Result<&'a str, ApiError> {
    match request.param(param_name) {
        Some(arg) => Ok(arg),
        None => {
            return Err(ApiError::BadRequest(format!(
                "no {} specified in path param",
                param_name
            )))
        }
    }
}

fn parse_request_param<T: FromStr>(
    request: &Request<Body>,
    param_name: &str,
) -> Result<T, ApiError> {
    match get_request_param(request, param_name)?.parse() {
        Ok(v) => Ok(v),
        Err(_) => {
            return Err(ApiError::BadRequest(
                "failed to parse tenant id".to_string(),
            ))
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
    let tenantid: ZTenantId = parse_request_param(&request, "tenant_id")?;

    check_permission(&request, Some(tenantid))?;

    let response_data = tokio::task::spawn_blocking(move || {
        crate::branches::get_branches(get_config(&request), &tenantid)
    })
    .await
    .map_err(ApiError::from_err)??;
    Ok(json_response(StatusCode::OK, response_data)?)
}

// TODO add to swagger
async fn branch_detail_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenantid: ZTenantId = parse_request_param(&request, "tenant_id")?;
    let branch_name: &str = get_request_param(&request, "branch_name")?;
    let conf = get_state(&request).conf;
    let path = conf.branch_path(&branch_name, &tenantid);

    let response_data = tokio::task::spawn_blocking(move || {
        let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
        BranchInfo::from_path(path, conf, &tenantid, &repo)
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
        tenant_mgr::create_repository_for_tenant(get_config(&request), request_data.tenant_id)
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
        .get("/v1/branch/:tenant_id/:branch_name", branch_detail_handler)
        .post("/v1/branch", branch_create_handler)
        .get("/v1/tenant", tenant_list_handler)
        .post("/v1/tenant", tenant_create_handler)
        .any(handler_404)
}
