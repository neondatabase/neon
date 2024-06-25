use anyhow::Context;
use bytes::Buf;
use hyper::{header, Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use super::error::ApiError;

pub async fn json_request<T: for<'de> Deserialize<'de>>(
    request: &mut Request<Body>,
) -> Result<T, ApiError> {
    let body = hyper::body::aggregate(request.body_mut())
        .await
        .context("Failed to read request body")
        .map_err(ApiError::BadRequest)?;

    if body.remaining() == 0 {
        return Err(ApiError::BadRequest(anyhow::anyhow!(
            "missing request body"
        )));
    }

    let mut deser = serde_json::de::Deserializer::from_reader(body.reader());

    serde_path_to_error::deserialize(&mut deser)
        // intentionally stringify because the debug version is not helpful in python logs
        .map_err(|e| anyhow::anyhow!("Failed to parse json request: {e}"))
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
