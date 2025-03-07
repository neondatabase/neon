use std::{
    fmt::Display,
    ops::{Deref, DerefMut},
};

use axum::{extract::FromRequestParts, response::IntoResponse};
use http::{StatusCode, request::Parts};

use crate::http::{JsonResponse, headers::X_REQUEST_ID};

/// Extract the request ID from the `X-Request-Id` header.
#[derive(Debug, Clone, Default)]
pub(crate) struct RequestId(pub String);

#[derive(Debug)]
/// Rejection used for [`RequestId`].
///
/// Contains one variant for each way the [`RequestId`] extractor can
/// fail.
pub(crate) enum RequestIdRejection {
    /// The request is missing the header.
    MissingRequestId,

    /// The value of the header is invalid UTF-8.
    InvalidUtf8,
}

impl RequestIdRejection {
    pub fn status(&self) -> StatusCode {
        match self {
            RequestIdRejection::MissingRequestId => StatusCode::INTERNAL_SERVER_ERROR,
            RequestIdRejection::InvalidUtf8 => StatusCode::BAD_REQUEST,
        }
    }

    pub fn message(&self) -> String {
        match self {
            RequestIdRejection::MissingRequestId => "request ID is missing",
            RequestIdRejection::InvalidUtf8 => "request ID is invalid UTF-8",
        }
        .to_string()
    }
}

impl IntoResponse for RequestIdRejection {
    fn into_response(self) -> axum::response::Response {
        JsonResponse::error(self.status(), self.message())
    }
}

impl<S> FromRequestParts<S> for RequestId
where
    S: Send + Sync,
{
    type Rejection = RequestIdRejection;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match parts.headers.get(X_REQUEST_ID) {
            Some(value) => match value.to_str() {
                Ok(request_id) => Ok(Self(request_id.to_string())),
                Err(_) => Err(RequestIdRejection::InvalidUtf8),
            },
            None => Err(RequestIdRejection::MissingRequestId),
        }
    }
}

impl Deref for RequestId {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RequestId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
