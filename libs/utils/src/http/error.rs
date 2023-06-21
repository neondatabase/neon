use hyper::{header, Body, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use thiserror::Error;
use tracing::error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("Bad request: {0:#?}")]
    BadRequest(anyhow::Error),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("NotFound: {0}")]
    NotFound(Box<dyn StdError + Send + Sync + 'static>),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Precondition failed: {0}")]
    PreconditionFailed(Box<str>),

    #[error(transparent)]
    InternalServerError(anyhow::Error),
}

impl ApiError {
    pub fn into_response(self) -> Response<Body> {
        match self {
            ApiError::BadRequest(err) => HttpErrorBody::response_from_msg_and_status(
                format!("{err:#?}"), // use debug printing so that we give the cause
                StatusCode::BAD_REQUEST,
            ),
            ApiError::Forbidden(_) => {
                HttpErrorBody::response_from_msg_and_status(self.to_string(), StatusCode::FORBIDDEN)
            }
            ApiError::Unauthorized(_) => HttpErrorBody::response_from_msg_and_status(
                self.to_string(),
                StatusCode::UNAUTHORIZED,
            ),
            ApiError::NotFound(_) => {
                HttpErrorBody::response_from_msg_and_status(self.to_string(), StatusCode::NOT_FOUND)
            }
            ApiError::Conflict(_) => {
                HttpErrorBody::response_from_msg_and_status(self.to_string(), StatusCode::CONFLICT)
            }
            ApiError::PreconditionFailed(_) => HttpErrorBody::response_from_msg_and_status(
                self.to_string(),
                StatusCode::PRECONDITION_FAILED,
            ),
            ApiError::InternalServerError(err) => HttpErrorBody::response_from_msg_and_status(
                err.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct HttpErrorBody {
    pub msg: String,
}

impl HttpErrorBody {
    pub fn from_msg(msg: String) -> Self {
        HttpErrorBody { msg }
    }

    pub fn response_from_msg_and_status(msg: String, status: StatusCode) -> Response<Body> {
        HttpErrorBody { msg }.to_response(status)
    }

    pub fn to_response(&self, status: StatusCode) -> Response<Body> {
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            // we do not have nested maps with non string keys so serialization shouldn't fail
            .body(Body::from(serde_json::to_string(self).unwrap()))
            .unwrap()
    }
}

pub async fn route_error_handler(err: routerify::RouteError) -> Response<Body> {
    match err.downcast::<ApiError>() {
        Ok(api_error) => api_error_handler(*api_error),
        Err(other_error) => {
            // We expect all the request handlers to return an ApiError, so this should
            // not be reached. But just in case.
            error!("Error processing HTTP request: {other_error:?}");
            HttpErrorBody::response_from_msg_and_status(
                other_error.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        }
    }
}

pub fn api_error_handler(api_error: ApiError) -> Response<Body> {
    // Print a stack trace for Internal Server errors
    if let ApiError::InternalServerError(_) = api_error {
        error!("Error processing HTTP request: {api_error:?}");
    } else {
        error!("Error processing HTTP request: {api_error:#}");
    }

    api_error.into_response()
}
