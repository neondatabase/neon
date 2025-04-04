use std::sync::Arc;

use axum::extract::State;
use axum::response::Response;
use http::StatusCode;

use crate::catalog::get_dbs_and_roles;
use crate::compute::ComputeNode;
use crate::http::JsonResponse;

/// Get the databases and roles from the compute.
pub(in crate::http) async fn get_catalog_objects(
    State(compute): State<Arc<ComputeNode>>,
) -> Response {
    match get_dbs_and_roles(&compute).await {
        Ok(catalog_objects) => JsonResponse::success(StatusCode::OK, catalog_objects),
        Err(e) => JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}
