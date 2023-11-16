use anyhow::Context;
use bytes::Buf;
use hyper::{header, Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use super::error::ApiError;

pub async fn json_request<T: for<'de> Deserialize<'de>>(
    request: &mut Request<Body>,
) -> Result<T, ApiError> {
    json_request_or_empty_body(request)
        .await?
        .context("missing request body")
        .map_err(ApiError::BadRequest)
}

/// Will be removed as part of <https://github.com/neondatabase/neon/issues/4282>
pub async fn json_request_or_empty_body<T: for<'de> Deserialize<'de>>(
    request: &mut Request<Body>,
) -> Result<Option<T>, ApiError> {
    let body = hyper::body::aggregate(request.body_mut())
        .await
        .context("Failed to read request body")
        .map_err(ApiError::BadRequest)?;
    if body.remaining() == 0 {
        return Ok(None);
    }
    serde_json::from_reader(body.reader())
        .context("Failed to parse json request")
        .map(Some)
        .map_err(ApiError::BadRequest)
}

pub fn json_response<T: Serialize>(
    status: StatusCode,
    data: T,
) -> Result<Response<Body>, ApiError> {
    let json = serde_json::to_string(&data)
        .context("Failed to serialize JSON response")
        .map_err(ApiError::InternalServerError)?;
    let response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .map_err(|e| ApiError::InternalServerError(e.into()))?;
    Ok(response)
}
