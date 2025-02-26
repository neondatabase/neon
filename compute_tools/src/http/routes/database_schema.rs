use std::sync::Arc;

use axum::{body::Body, extract::State, response::Response};
use http::{StatusCode, header::CONTENT_TYPE};
use serde::Deserialize;

use crate::{
    catalog::{SchemaDumpError, get_database_schema},
    compute::ComputeNode,
    http::{JsonResponse, extract::Query},
};

#[derive(Debug, Clone, Deserialize)]
pub(in crate::http) struct DatabaseSchemaParams {
    database: String,
}

/// Get a schema dump of the requested database.
pub(in crate::http) async fn get_schema_dump(
    params: Query<DatabaseSchemaParams>,
    State(compute): State<Arc<ComputeNode>>,
) -> Response {
    match get_database_schema(&compute, &params.database).await {
        Ok(schema) => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE.as_str(), "application/json")
            .body(Body::from_stream(schema))
            .unwrap(),
        Err(SchemaDumpError::DatabaseDoesNotExist) => {
            JsonResponse::error(StatusCode::NOT_FOUND, SchemaDumpError::DatabaseDoesNotExist)
        }
        Err(e) => JsonResponse::error(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}
