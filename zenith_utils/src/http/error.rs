use anyhow::anyhow;
use hyper::{header, Body, Response, StatusCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error(transparent)]
    InternalServerError(#[from] anyhow::Error),
}

impl ApiError {
    pub fn from_err<E: Into<anyhow::Error>>(err: E) -> Self {
        Self::InternalServerError(anyhow!(err))
    }

    pub fn into_response(self) -> Response<Body> {
        match self {
            ApiError::BadRequest(_) => HttpErrorBody::response_from_msg_and_status(
                self.to_string(),
                StatusCode::BAD_REQUEST,
            ),
            ApiError::Forbidden(_) => {
                HttpErrorBody::response_from_msg_and_status(self.to_string(), StatusCode::FORBIDDEN)
            }
            ApiError::Unauthorized(_) => HttpErrorBody::response_from_msg_and_status(
                self.to_string(),
                StatusCode::UNAUTHORIZED,
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

pub async fn handler(err: routerify::RouteError) -> Response<Body> {
    tracing::error!("{}", err);
    err.downcast::<ApiError>()
        .expect("handler should always return api error")
        .into_response()
}
