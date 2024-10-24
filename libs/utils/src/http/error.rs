use hyper::{header, Body, Response, StatusCode};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::error::Error as StdError;
use thiserror::Error;
use tracing::{error, info, warn};

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

    #[error("Resource temporarily unavailable: {0}")]
    ResourceUnavailable(Cow<'static, str>),

    #[error("Too many requests: {0}")]
    TooManyRequests(Cow<'static, str>),

    #[error("Shutting down")]
    ShuttingDown,

    #[error("Timeout")]
    Timeout(Cow<'static, str>),

    #[error("Request cancelled")]
    Cancelled,

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
            ApiError::ShuttingDown => HttpErrorBody::response_from_msg_and_status(
                "Shutting down".to_string(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            ApiError::ResourceUnavailable(err) => HttpErrorBody::response_from_msg_and_status(
                err.to_string(),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            ApiError::TooManyRequests(err) => HttpErrorBody::response_from_msg_and_status(
                err.to_string(),
                StatusCode::TOO_MANY_REQUESTS,
            ),
            ApiError::Timeout(err) => HttpErrorBody::response_from_msg_and_status(
                err.to_string(),
                StatusCode::REQUEST_TIMEOUT,
            ),
            ApiError::Cancelled => HttpErrorBody::response_from_msg_and_status(
                self.to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            ApiError::InternalServerError(err) => HttpErrorBody::response_from_msg_and_status(
                format!("{err:#}"), // use alternative formatting so that we give the cause without backtrace
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

    match api_error {
        ApiError::Forbidden(_) | ApiError::Unauthorized(_) => {
            warn!("Error processing HTTP request: {api_error:#}")
        }
        ApiError::ResourceUnavailable(_) => info!("Error processing HTTP request: {api_error:#}"),
        ApiError::NotFound(_) => info!("Error processing HTTP request: {api_error:#}"),
        ApiError::InternalServerError(_) => error!("Error processing HTTP request: {api_error:?}"),
        ApiError::ShuttingDown => info!("Shut down while processing HTTP request"),
        ApiError::Timeout(_) => info!("Timeout while processing HTTP request: {api_error:#}"),
        ApiError::Cancelled => info!("Request cancelled while processing HTTP request"),
        _ => info!("Error processing HTTP request: {api_error:#}"),
    }

    api_error.into_response()
}
