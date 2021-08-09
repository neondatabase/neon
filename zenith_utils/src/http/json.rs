use bytes::Buf;
use hyper::{header, Body, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json;

use super::error::ApiError;

pub async fn json_request<T: for<'de> Deserialize<'de>>(
    request: &mut Request<Body>,
) -> Result<T, ApiError> {
    let whole_body = hyper::body::aggregate(request.body_mut())
        .await
        .map_err(ApiError::from_err)?;
    Ok(serde_json::from_reader(whole_body.reader())
        .map_err(|err| ApiError::BadRequest(format!("Failed to parse json request {}", err)))?)
}

pub fn json_response<T: Serialize>(
    status: StatusCode,
    data: T,
) -> Result<Response<Body>, ApiError> {
    let json = serde_json::to_string(&data).map_err(ApiError::from_err)?;
    let response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .map_err(ApiError::from_err)?;
    Ok(response)
}
