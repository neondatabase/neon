use routerify::RouterBuilder;

use zenith_utils::http::endpoint;
use zenith_utils::http::error::ApiError;
use zenith_utils::http::json::json_response;
use zenith_utils::http::request::parse_request_param;
use zenith_utils::zid::{ZTenantId, ZTimelineId};


pub fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    let router = endpoint::make_router();
    // TODO add routes
    router
}
