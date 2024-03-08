use bytes::Bytes;

use anyhow::Context;
use http::{Response, StatusCode};
use http_body_util::Full;

use serde::Serialize;
use utils::http::error::ApiError;

pub fn api_error_into_response(this: ApiError) -> Response<Full<Bytes>> {
    match this {
        ApiError::BadRequest(err) => HttpErrorBody::response_from_msg_and_status(
            format!("{err:#?}"), // use debug printing so that we give the cause
            StatusCode::BAD_REQUEST,
        ),
        ApiError::Forbidden(_) => {
            HttpErrorBody::response_from_msg_and_status(this.to_string(), StatusCode::FORBIDDEN)
        }
        ApiError::Unauthorized(_) => {
            HttpErrorBody::response_from_msg_and_status(this.to_string(), StatusCode::UNAUTHORIZED)
        }
        ApiError::NotFound(_) => {
            HttpErrorBody::response_from_msg_and_status(this.to_string(), StatusCode::NOT_FOUND)
        }
        ApiError::Conflict(_) => {
            HttpErrorBody::response_from_msg_and_status(this.to_string(), StatusCode::CONFLICT)
        }
        ApiError::PreconditionFailed(_) => HttpErrorBody::response_from_msg_and_status(
            this.to_string(),
            StatusCode::PRECONDITION_FAILED,
        ),
        ApiError::ShuttingDown => HttpErrorBody::response_from_msg_and_status(
            "Shutting down".to_string(),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        ApiError::ResourceUnavailable(err) => HttpErrorBody::response_from_msg_and_status(
            err.to_string(),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        ApiError::Timeout(err) => HttpErrorBody::response_from_msg_and_status(
            err.to_string(),
            StatusCode::REQUEST_TIMEOUT,
        ),
        ApiError::InternalServerError(err) => HttpErrorBody::response_from_msg_and_status(
            err.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

#[derive(Serialize)]
struct HttpErrorBody {
    pub msg: String,
}

impl HttpErrorBody {
    pub fn response_from_msg_and_status(msg: String, status: StatusCode) -> Response<Full<Bytes>> {
        HttpErrorBody { msg }.to_response(status)
    }

    pub fn to_response(&self, status: StatusCode) -> Response<Full<Bytes>> {
        Response::builder()
            .status(status)
            .header(http::header::CONTENT_TYPE, "application/json")
            // we do not have nested maps with non string keys so serialization shouldn't fail
            .body(Full::new(Bytes::from(serde_json::to_string(self).unwrap())))
            .unwrap()
    }
}

pub fn json_response<T: Serialize>(
    status: StatusCode,
    data: T,
) -> Result<Response<Full<Bytes>>, ApiError> {
    let json = serde_json::to_string(&data)
        .context("Failed to serialize JSON response")
        .map_err(ApiError::InternalServerError)?;
    let response = Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(json)))
        .map_err(|e| ApiError::InternalServerError(e.into()))?;
    Ok(response)
}
