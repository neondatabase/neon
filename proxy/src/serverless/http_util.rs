//! Things stolen from `libs/utils/src/http` to add hyper 1.0 compatibility
//! Will merge back in at some point in the future.

use anyhow::Context;
use bytes::Bytes;
use http::header::AUTHORIZATION;
use http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use http_utils::error::ApiError;
use serde::Serialize;
use url::Url;
use uuid::Uuid;

use super::conn_pool::AuthData;
use super::conn_pool::ConnInfoWithAuth;
use super::conn_pool_lib::ConnInfo;
use super::error::{ConnInfoError, Credentials};
use crate::auth::backend::ComputeUserInfo;
use crate::config::AuthenticationConfig;
use crate::context::RequestContext;
use crate::metrics::{Metrics, SniGroup, SniKind};
use crate::pqproto::StartupMessageParams;
use crate::proxy::NeonOptions;
use crate::types::{DbName, EndpointId, RoleName};

// Common header names used across serverless modules
pub(super) static NEON_REQUEST_ID: HeaderName = HeaderName::from_static("neon-request-id");
pub(super) static CONN_STRING: HeaderName = HeaderName::from_static("neon-connection-string");
pub(super) static RAW_TEXT_OUTPUT: HeaderName = HeaderName::from_static("neon-raw-text-output");
pub(super) static ARRAY_MODE: HeaderName = HeaderName::from_static("neon-array-mode");
pub(super) static ALLOW_POOL: HeaderName = HeaderName::from_static("neon-pool-opt-in");
pub(super) static TXN_ISOLATION_LEVEL: HeaderName =
    HeaderName::from_static("neon-batch-isolation-level");
pub(super) static TXN_READ_ONLY: HeaderName = HeaderName::from_static("neon-batch-read-only");
pub(super) static TXN_DEFERRABLE: HeaderName = HeaderName::from_static("neon-batch-deferrable");

pub(crate) fn uuid_to_header_value(id: Uuid) -> HeaderValue {
    let mut uuid = [0; uuid::fmt::Hyphenated::LENGTH];
    HeaderValue::from_str(id.as_hyphenated().encode_lower(&mut uuid[..]))
        .expect("uuid hyphenated format should be all valid header characters")
}

/// Like [`ApiError::into_response`]
pub(crate) fn api_error_into_response(this: ApiError) -> Response<BoxBody<Bytes, hyper::Error>> {
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
        ApiError::TooManyRequests(err) => HttpErrorBody::response_from_msg_and_status(
            err.to_string(),
            StatusCode::TOO_MANY_REQUESTS,
        ),
        ApiError::Timeout(err) => HttpErrorBody::response_from_msg_and_status(
            err.to_string(),
            StatusCode::REQUEST_TIMEOUT,
        ),
        ApiError::Cancelled => HttpErrorBody::response_from_msg_and_status(
            this.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        ApiError::InternalServerError(err) => HttpErrorBody::response_from_msg_and_status(
            err.to_string(),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

/// Same as [`http_utils::error::HttpErrorBody`]
#[derive(Serialize)]
struct HttpErrorBody {
    pub(crate) msg: String,
}

impl HttpErrorBody {
    /// Same as [`http_utils::error::HttpErrorBody::response_from_msg_and_status`]
    fn response_from_msg_and_status(
        msg: String,
        status: StatusCode,
    ) -> Response<BoxBody<Bytes, hyper::Error>> {
        HttpErrorBody { msg }.to_response(status)
    }

    /// Same as [`http_utils::error::HttpErrorBody::to_response`]
    fn to_response(&self, status: StatusCode) -> Response<BoxBody<Bytes, hyper::Error>> {
        Response::builder()
            .status(status)
            .header(http::header::CONTENT_TYPE, "application/json")
            // we do not have nested maps with non string keys so serialization shouldn't fail
            .body(
                Full::new(Bytes::from(
                    serde_json::to_string(self)
                        .expect("serialising HttpErrorBody should never fail"),
                ))
                .map_err(|x| match x {})
                .boxed(),
            )
            .expect("content-type header should be valid")
    }
}

/// Same as [`http_utils::json::json_response`]
pub(crate) fn json_response<T: Serialize>(
    status: StatusCode,
    data: T,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, ApiError> {
    let json = serde_json::to_string(&data)
        .context("Failed to serialize JSON response")
        .map_err(ApiError::InternalServerError)?;
    let response = Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(json)).map_err(|x| match x {}).boxed())
        .map_err(|e| ApiError::InternalServerError(e.into()))?;
    Ok(response)
}

pub(crate) fn get_conn_info(
    config: &'static AuthenticationConfig,
    ctx: &RequestContext,
    connection_string: Option<&str>,
    headers: &HeaderMap
) -> Result<ConnInfoWithAuth, ConnInfoError> {
    let connection_url = match connection_string {
        Some(connection_string) => Url::parse(connection_string)?,
        None => {
            let connection_string = headers
                .get(&CONN_STRING)
                .ok_or(ConnInfoError::InvalidHeader(&CONN_STRING))?
                .to_str()
                .map_err(|_| ConnInfoError::InvalidHeader(&CONN_STRING))?;
            Url::parse(connection_string)?
        }
    };

    let protocol = connection_url.scheme();
    if protocol != "postgres" && protocol != "postgresql" {
        return Err(ConnInfoError::IncorrectScheme);
    }

    let mut url_path = connection_url
        .path_segments()
        .ok_or(ConnInfoError::MissingDbName)?;

    let dbname: DbName =
        urlencoding::decode(url_path.next().ok_or(ConnInfoError::InvalidDbName)?)?.into();
    ctx.set_dbname(dbname.clone());

    let username = RoleName::from(urlencoding::decode(connection_url.username())?);
    if username.is_empty() {
        return Err(ConnInfoError::MissingUsername);
    }
    ctx.set_user(username.clone());
    // TODO: make sure this is right in the context of rest broker
    let auth = if let Some(auth) = headers.get(&AUTHORIZATION) {
        if !config.accept_jwts {
            return Err(ConnInfoError::MissingCredentials(Credentials::Password));
        }

        let auth = auth
            .to_str()
            .map_err(|_| ConnInfoError::InvalidHeader(&AUTHORIZATION))?;
        AuthData::Jwt(
            auth.strip_prefix("Bearer ")
                .ok_or(ConnInfoError::MissingCredentials(Credentials::BearerJwt))?
                .into(),
        )
    } else if let Some(pass) = connection_url.password() {
        // wrong credentials provided
        if config.accept_jwts {
            return Err(ConnInfoError::MissingCredentials(Credentials::BearerJwt));
        }

        AuthData::Password(match urlencoding::decode_binary(pass.as_bytes()) {
            std::borrow::Cow::Borrowed(b) => b.into(),
            std::borrow::Cow::Owned(b) => b.into(),
        })
    } else if config.accept_jwts {
        return Err(ConnInfoError::MissingCredentials(Credentials::BearerJwt));
    } else {
        return Err(ConnInfoError::MissingCredentials(Credentials::Password));
    };
    let endpoint: EndpointId = match connection_url.host() {
        Some(url::Host::Domain(hostname)) => hostname
            .split_once('.')
            .map_or(hostname, |(prefix, _)| prefix)
            .into(),
        Some(url::Host::Ipv4(_) | url::Host::Ipv6(_)) | None => {
            return Err(ConnInfoError::MissingHostname);
        }
    };
    ctx.set_endpoint_id(endpoint.clone());

    let pairs = connection_url.query_pairs();

    let mut options = Option::None;

    let mut params = StartupMessageParams::default();
    params.insert("user", &username);
    params.insert("database", &dbname);
    for (key, value) in pairs {
        params.insert(&key, &value);
        if key == "options" {
            options = Some(NeonOptions::parse_options_raw(&value));
        }
    }

    // check the URL that was used, for metrics
    {
        let host_endpoint = headers
            // get the host header
            .get("host")
            // extract the domain
            .and_then(|h| {
                let (host, _port) = h.to_str().ok()?.split_once(':')?;
                Some(host)
            })
            // get the endpoint prefix
            .map(|h| h.split_once('.').map_or(h, |(prefix, _)| prefix));

        let kind = if host_endpoint == Some(&*endpoint) {
            SniKind::Sni
        } else {
            SniKind::NoSni
        };

        let protocol = ctx.protocol();
        Metrics::get()
            .proxy
            .accepted_connections_by_sni
            .inc(SniGroup { protocol, kind });
    }

    ctx.set_user_agent(
        headers
            .get(hyper::header::USER_AGENT)
            .and_then(|h| h.to_str().ok())
            .map(Into::into),
    );

    let user_info = ComputeUserInfo {
        endpoint,
        user: username,
        options: options.unwrap_or_default(),
    };

    let conn_info = ConnInfo { user_info, dbname };
    Ok(ConnInfoWithAuth { conn_info, auth })
}
