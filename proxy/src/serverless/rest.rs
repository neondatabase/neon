use std::sync::Arc;

use bytes::Bytes;
use http::Method;
use http::header::AUTHORIZATION;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt};
use http_utils::error::ApiError;
use hyper::body::Incoming;
use hyper::http::{HeaderName, HeaderValue};
use hyper::{HeaderMap, Request, Response, StatusCode};
use indexmap::IndexMap;
use postgres_client::error::{DbError, ErrorPosition, SqlState};


use serde_json::value::RawValue;

use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use typed_json::json;
use url::Url;
use uuid::Uuid;

use super::backend::{LocalProxyConnError, PoolingBackend};
use super::conn_pool::{AuthData, ConnInfoWithAuth};
use super::conn_pool_lib::{ConnInfo};
use super::error::HttpCodeError;
use super::http_util::json_response;
use super::json::{JsonConversionError};
use crate::auth::backend::{ComputeUserInfo};
use crate::auth::{ComputeUserInfoParseError, endpoint_sni};
use crate::config::{AuthenticationConfig, ProxyConfig, TlsConfig};
use crate::context::RequestContext;
use crate::error::{ErrorKind, ReportableError, UserFacingError};
use crate::http::{ReadBodyError, read_body_with_limit};
use crate::metrics::{Metrics, SniGroup, SniKind};
use crate::pqproto::StartupMessageParams;
use crate::proxy::NeonOptions;
use crate::serverless::backend::HttpConnError;
use crate::types::{DbName, RoleName};




pub(super) static NEON_REQUEST_ID: HeaderName = HeaderName::from_static("neon-request-id");

static CONN_STRING: HeaderName = HeaderName::from_static("neon-connection-string");


#[derive(Debug, thiserror::Error)]
pub(crate) enum ConnInfoError {
    #[error("invalid header: {0}")]
    InvalidHeader(&'static HeaderName),
    #[error("invalid connection string: {0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("incorrect scheme")]
    IncorrectScheme,
    #[error("missing database name")]
    MissingDbName,
    #[error("invalid database name")]
    InvalidDbName,
    #[error("missing username")]
    MissingUsername,
    #[error("invalid username: {0}")]
    InvalidUsername(#[from] std::string::FromUtf8Error),
    #[error("missing authentication credentials: {0}")]
    MissingCredentials(Credentials),
    #[error("missing hostname")]
    MissingHostname,
    #[error("invalid hostname: {0}")]
    InvalidEndpoint(#[from] ComputeUserInfoParseError),
    #[error("malformed endpoint")]
    MalformedEndpoint,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Credentials {
    #[error("required password")]
    Password,
    #[error("required authorization bearer token in JWT format")]
    BearerJwt,
}

impl ReportableError for ConnInfoError {
    fn get_error_kind(&self) -> ErrorKind {
        ErrorKind::User
    }
}

impl UserFacingError for ConnInfoError {
    fn to_string_client(&self) -> String {
        self.to_string()
    }
}

fn get_conn_info(
    config: &'static AuthenticationConfig,
    ctx: &RequestContext,
    headers: &HeaderMap,
    tls: Option<&TlsConfig>,
) -> Result<ConnInfoWithAuth, ConnInfoError> {
    // let connection_string = headers
    //     .get(&CONN_STRING)
    //     .ok_or(ConnInfoError::InvalidHeader(&CONN_STRING))?
    //     .to_str()
    //     .map_err(|_| ConnInfoError::InvalidHeader(&CONN_STRING))?;

    let connection_string = "postgresql://authenticated@foo.local.neon.build/database";
    let connection_url = Url::parse(connection_string)?;

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
    info!("auth passed !!!!!!!!!!!: {auth:?}");
    let endpoint = match connection_url.host() {
        Some(url::Host::Domain(hostname)) => {
            if let Some(tls) = tls {
                endpoint_sni(hostname, &tls.common_names).ok_or(ConnInfoError::MalformedEndpoint)?
            } else {
                hostname
                    .split_once('.')
                    .map_or(hostname, |(prefix, _)| prefix)
                    .into()
            }
        }
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

pub(crate) async fn handle(
    config: &'static ProxyConfig,
    ctx: RequestContext,
    request: Request<Incoming>,
    backend: Arc<PoolingBackend>,
    cancel: CancellationToken,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, ApiError> {
    info!("entered rest:handle!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    let result = handle_inner(cancel, config, &ctx, request, backend).await;

    let mut response = match result {
        Ok(r) => {
            ctx.set_success();

            // Handling the error response from local proxy here
            if config.authentication_config.is_auth_broker && r.status().is_server_error() {
                let status = r.status();

                let body_bytes = r
                    .collect()
                    .await
                    .map_err(|e| {
                        ApiError::InternalServerError(anyhow::Error::msg(format!(
                            "could not collect http body: {e}"
                        )))
                    })?
                    .to_bytes();

                if let Ok(mut json_map) =
                    serde_json::from_slice::<IndexMap<&str, &RawValue>>(&body_bytes)
                {
                    let message = json_map.get("message");
                    if let Some(message) = message {
                        let msg: String = match serde_json::from_str(message.get()) {
                            Ok(msg) => msg,
                            Err(_) => {
                                "Unable to parse the response message from server".to_string()
                            }
                        };

                        error!("Error response from local_proxy: {status} {msg}");

                        json_map.retain(|key, _| !key.starts_with("neon:")); // remove all the neon-related keys

                        let resp_json = serde_json::to_string(&json_map)
                            .unwrap_or("failed to serialize the response message".to_string());

                        return json_response(status, resp_json);
                    }
                }

                error!("Unable to parse the response message from local_proxy");
                return json_response(
                    status,
                    json!({ "message": "Unable to parse the response message from server".to_string() }),
                );
            }
            r
        }
        Err(e @ RestError::Cancelled(_)) => {
            let error_kind = e.get_error_kind();
            ctx.set_error_kind(error_kind);

            let message = "Query cancelled, connection was terminated";

            tracing::info!(
                kind=error_kind.to_metric_label(),
                error=%e,
                msg=message,
                "forwarding error to user"
            );

            json_response(
                StatusCode::BAD_REQUEST,
                json!({ "message": message, "code": SqlState::PROTOCOL_VIOLATION.code() }),
            )?
        }
        Err(e) => {
            let error_kind = e.get_error_kind();
            ctx.set_error_kind(error_kind);

            let mut message = e.to_string_client();
            let db_error = match &e {
                RestError::ConnectCompute(HttpConnError::PostgresConnectionError(e))
                | RestError::Postgres(e) => e.as_db_error(),
                _ => None,
            };
            fn get<'a, T: Default>(db: Option<&'a DbError>, x: impl FnOnce(&'a DbError) -> T) -> T {
                db.map(x).unwrap_or_default()
            }

            if let Some(db_error) = db_error {
                db_error.message().clone_into(&mut message);
            }

            let position = db_error.and_then(|db| db.position());
            let (position, internal_position, internal_query) = match position {
                Some(ErrorPosition::Original(position)) => (Some(position.to_string()), None, None),
                Some(ErrorPosition::Internal { position, query }) => {
                    (None, Some(position.to_string()), Some(query.clone()))
                }
                None => (None, None, None),
            };

            let code = get(db_error, |db| db.code().code());
            let severity = get(db_error, |db| db.severity());
            let detail = get(db_error, |db| db.detail());
            let hint = get(db_error, |db| db.hint());
            let where_ = get(db_error, |db| db.where_());
            let table = get(db_error, |db| db.table());
            let column = get(db_error, |db| db.column());
            let schema = get(db_error, |db| db.schema());
            let datatype = get(db_error, |db| db.datatype());
            let constraint = get(db_error, |db| db.constraint());
            let file = get(db_error, |db| db.file());
            let line = get(db_error, |db| db.line().map(|l| l.to_string()));
            let routine = get(db_error, |db| db.routine());

            tracing::info!(
                kind=error_kind.to_metric_label(),
                error=%e,
                msg=message,
                "forwarding error to user"
            );

            json_response(
                e.get_http_status_code(),
                json!({
                    "message": message,
                    "code": code,
                    "detail": detail,
                    "hint": hint,
                    "position": position,
                    "internalPosition": internal_position,
                    "internalQuery": internal_query,
                    "severity": severity,
                    "where": where_,
                    "table": table,
                    "column": column,
                    "schema": schema,
                    "dataType": datatype,
                    "constraint": constraint,
                    "file": file,
                    "line": line,
                    "routine": routine,
                }),
            )?
        }
    };

    response
        .headers_mut()
        .insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
    Ok(response)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RestError {
    #[error("{0}")]
    ReadPayload(#[from] ReadPayloadError),
    #[error("{0}")]
    ConnectCompute(#[from] HttpConnError),
    #[error("{0}")]
    ConnInfo(#[from] ConnInfoError),
    #[error("response is too large (max is {0} bytes)")]
    ResponseTooLarge(usize),
    #[error("invalid isolation level")]
    InvalidIsolationLevel,
    /// for queries our customers choose to run
    #[error("{0}")]
    Postgres(#[source] postgres_client::Error),
    /// for queries we choose to run
    #[error("{0}")]
    InternalPostgres(#[source] postgres_client::Error),
    #[error("{0}")]
    JsonConversion(#[from] JsonConversionError),
    #[error("{0}")]
    Cancelled(SqlOverHttpCancel),
}

impl ReportableError for RestError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            RestError::ReadPayload(e) => e.get_error_kind(),
            RestError::ConnectCompute(e) => e.get_error_kind(),
            RestError::ConnInfo(e) => e.get_error_kind(),
            RestError::ResponseTooLarge(_) => ErrorKind::User,
            RestError::InvalidIsolationLevel => ErrorKind::User,
            RestError::Postgres(p) => p.get_error_kind(),
            RestError::InternalPostgres(p) => {
                if p.as_db_error().is_some() {
                    ErrorKind::Service
                } else {
                    ErrorKind::Compute
                }
            }
            RestError::JsonConversion(_) => ErrorKind::Postgres,
            RestError::Cancelled(c) => c.get_error_kind(),
        }
    }
}

impl UserFacingError for RestError {
    fn to_string_client(&self) -> String {
        match self {
            RestError::ReadPayload(p) => p.to_string(),
            RestError::ConnectCompute(c) => c.to_string_client(),
            RestError::ConnInfo(c) => c.to_string_client(),
            RestError::ResponseTooLarge(_) => self.to_string(),
            RestError::InvalidIsolationLevel => self.to_string(),
            RestError::Postgres(p) => p.to_string(),
            RestError::InternalPostgres(p) => p.to_string(),
            RestError::JsonConversion(_) => "could not parse postgres response".to_string(),
            RestError::Cancelled(_) => self.to_string(),
        }
    }
}

impl HttpCodeError for RestError {
    fn get_http_status_code(&self) -> StatusCode {
        match self {
            RestError::ReadPayload(e) => e.get_http_status_code(),
            RestError::ConnectCompute(h) => match h.get_error_kind() {
                ErrorKind::User => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            RestError::ConnInfo(_) => StatusCode::BAD_REQUEST,
            RestError::ResponseTooLarge(_) => StatusCode::INSUFFICIENT_STORAGE,
            RestError::InvalidIsolationLevel => StatusCode::BAD_REQUEST,
            RestError::Postgres(_) => StatusCode::BAD_REQUEST,
            RestError::InternalPostgres(_) => StatusCode::INTERNAL_SERVER_ERROR,
            RestError::JsonConversion(_) => StatusCode::INTERNAL_SERVER_ERROR,
            RestError::Cancelled(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReadPayloadError {
    #[error("could not read the HTTP request body: {0}")]
    Read(#[from] hyper::Error),
    #[error("request is too large (max is {limit} bytes)")]
    BodyTooLarge { limit: usize },
    #[error("could not parse the HTTP request body: {0}")]
    Parse(#[from] serde_json::Error),
}

impl From<ReadBodyError<hyper::Error>> for ReadPayloadError {
    fn from(value: ReadBodyError<hyper::Error>) -> Self {
        match value {
            ReadBodyError::BodyTooLarge { limit } => Self::BodyTooLarge { limit },
            ReadBodyError::Read(e) => Self::Read(e),
        }
    }
}

impl ReportableError for ReadPayloadError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            ReadPayloadError::Read(_) => ErrorKind::ClientDisconnect,
            ReadPayloadError::BodyTooLarge { .. } => ErrorKind::User,
            ReadPayloadError::Parse(_) => ErrorKind::User,
        }
    }
}

impl HttpCodeError for ReadPayloadError {
    fn get_http_status_code(&self) -> StatusCode {
        match self {
            ReadPayloadError::Read(_) => StatusCode::BAD_REQUEST,
            ReadPayloadError::BodyTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            ReadPayloadError::Parse(_) => StatusCode::BAD_REQUEST,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SqlOverHttpCancel {
    #[error("query was cancelled")]
    Postgres,
    #[error("query was cancelled while stuck trying to connect to the database")]
    Connect,
}

impl ReportableError for SqlOverHttpCancel {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            SqlOverHttpCancel::Postgres => ErrorKind::ClientDisconnect,
            SqlOverHttpCancel::Connect => ErrorKind::ClientDisconnect,
        }
    }
}


async fn handle_inner(
    _cancel: CancellationToken,
    config: &'static ProxyConfig,
    ctx: &RequestContext,
    request: Request<Incoming>,
    backend: Arc<PoolingBackend>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, RestError> {
    info!("entered rest:handle_inner!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    let _requeset_gauge = Metrics::get()
        .proxy
        .connection_requests
        .guard(ctx.protocol());
    info!(
        protocol = %ctx.protocol(),
        "handling interactive connection from client"
    );

    let conn_info = get_conn_info(
        &config.authentication_config,
        ctx,
        request.headers(),
        // todo: race condition?
        // we're unlikely to change the common names.
        config.tls_config.load().as_deref(),
    )?;
    info!(
        user = conn_info.conn_info.user_info.user.as_str(),
        "credentials"
    );

    match conn_info.auth {
        AuthData::Jwt(jwt) if config.authentication_config.is_auth_broker => {
            handle_rest_inner(ctx, request, conn_info.conn_info, jwt, backend).await
        }
        _ => {
            Err(RestError::ConnInfo(ConnInfoError::MissingCredentials(Credentials::Password)))
        }
    }
}


pub(crate) fn uuid_to_header_value(id: Uuid) -> HeaderValue {
    let mut uuid = [0; uuid::fmt::Hyphenated::LENGTH];
    HeaderValue::from_str(id.as_hyphenated().encode_lower(&mut uuid[..]))
        .expect("uuid hyphenated format should be all valid header characters")
}

async fn handle_rest_inner(
    ctx: &RequestContext,
    request: Request<Incoming>,
    conn_info: ConnInfo,
    jwt: String,
    backend: Arc<PoolingBackend>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, RestError> {
    backend
        .authenticate_with_jwt(ctx, &conn_info.user_info, jwt)
        .await
        .map_err(HttpConnError::from)?;

    let mut client = backend.connect_to_local_proxy(ctx, conn_info).await?;

    let local_proxy_uri = ::http::Uri::from_static("http://proxy.local/sql");

    let (mut parts, body) = request.into_parts();
    let mut req = Request::builder().method(Method::POST).uri(local_proxy_uri);

    // todo(conradludgate): maybe auth-broker should parse these and re-serialize
    // these instead just to ensure they remain normalised.
    // for &h in HEADERS_TO_FORWARD {
    //     if let Some(hv) = parts.headers.remove(h) {
    //         req = req.header(h, hv);
    //     }
    // }
    req = req.header(&NEON_REQUEST_ID, uuid_to_header_value(ctx.session_id()));

    let req = req
        .body(body)
        .expect("all headers and params received via hyper should be valid for request");

    // todo: map body to count egress
    let _metrics = client.metrics(ctx);

    info!("sending request to local proxy !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    Ok(client
        .inner
        .inner
        .send_request(req)
        .await
        .map_err(LocalProxyConnError::from)
        .map_err(HttpConnError::from)?
        .map(|b| b.boxed()))
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload() {
        
    }
}
