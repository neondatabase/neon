use hyper::{Body, Request, Response, StatusCode};
use zenith_utils::http::RouterBuilder;

use zenith_utils::http::endpoint;
use zenith_utils::http::error::ApiError;
use zenith_utils::http::json::json_response;

async fn status_handler(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    Ok(json_response(StatusCode::OK, "")?)
}

pub fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    let router = endpoint::make_router();
    router.get("/v1/status", status_handler)
}
