use std::borrow::Cow;
use std::sync::Arc;

use bytes::Bytes;
use http::Method;
use http::header::AUTHORIZATION;
use http_body_util::combinators::BoxBody;
use http_body_util::Full;
use http_body_util::{BodyExt};
use http_utils::error::ApiError;
use hyper::body::Incoming;
use hyper::http::{HeaderName, HeaderValue};
use hyper::{HeaderMap, Request, Response, StatusCode};
use indexmap::IndexMap;
use postgres_client::error::{DbError, ErrorPosition, SqlState};


use serde_json::{value::RawValue, Value as JsonValue};

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
use crate::auth::backend::{ComputeUserInfo, ComputeCredentialKeys};
use crate::auth::{ComputeUserInfoParseError, endpoint_sni, };
use crate::config::{AuthenticationConfig, ProxyConfig, TlsConfig};
use crate::context::RequestContext;
use crate::error::{ErrorKind, ReportableError, UserFacingError};
use crate::http::{ReadBodyError, read_body_with_limit};
use crate::metrics::{Metrics, SniGroup, SniKind};
use crate::pqproto::StartupMessageParams;
use crate::proxy::NeonOptions;
use crate::serverless::backend::HttpConnError;
use crate::types::{DbName, RoleName};

use subzero_core::{
    api::{ApiRequest, ApiResponse, ContentType::*, SingleVal, ListVal, Payload},
    error::Error::{self as SubzeroCoreError, SingularityError, PutMatchingPkError, PermissionDenied, JsonDeserialize, NotFound, JwtTokenInvalid, InternalError},
    schema::DbSchema,
    formatter::{
        Param,
        Param::*,
        postgresql::{fmt_main_query, generate},
        ToParam, Snippet, SqlParam,
    },
    error::JsonDeserializeSnafu,
    dynamic_statement::{param, sql, JoinIterator},
};
use subzero_core::{
    api::{ContentType, ContentType::*, Preferences, QueryNode::*, Representation, Resolution::*,},
    error::{*},
    parser::postgrest::parse,
    permissions::{check_safe_functions, check_privileges, insert_policy_conditions, replace_select_star},
    api::DEFAULT_SAFE_SELECT_FUNCTIONS,
};
use std::collections::HashMap;
use jsonpath_lib::select;
use url::form_urlencoded;




pub(super) static NEON_REQUEST_ID: HeaderName = HeaderName::from_static("neon-request-id");

static CONN_STRING: HeaderName = HeaderName::from_static("neon-connection-string");
static RAW_TEXT_OUTPUT: HeaderName = HeaderName::from_static("neon-raw-text-output");
static ARRAY_MODE: HeaderName = HeaderName::from_static("neon-array-mode");
static ALLOW_POOL: HeaderName = HeaderName::from_static("neon-pool-opt-in");
static TXN_ISOLATION_LEVEL: HeaderName = HeaderName::from_static("neon-batch-isolation-level");
static TXN_READ_ONLY: HeaderName = HeaderName::from_static("neon-batch-read-only");
static TXN_DEFERRABLE: HeaderName = HeaderName::from_static("neon-batch-deferrable");

static HEADER_VALUE_TRUE: HeaderValue = HeaderValue::from_static("true");

// FIXME: remove this header
static HACK_TRUST_ROLE_SWITCHING: HeaderName = HeaderName::from_static("neon-hack-trust-role-switching");



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
    connection_string: &str,
    headers: &HeaderMap,
    tls: Option<&TlsConfig>,
) -> Result<ConnInfoWithAuth, ConnInfoError> {
    // let connection_string = headers
    //     .get(&CONN_STRING)
    //     .ok_or(ConnInfoError::InvalidHeader(&CONN_STRING))?
    //     .to_str()
    //     .map_err(|_| ConnInfoError::InvalidHeader(&CONN_STRING))?;

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
    #[error("{0}")]
    SubzeroCore(#[source] SubzeroCoreError),
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
            RestError::SubzeroCore(s) => ErrorKind::User,
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
            RestError::SubzeroCore(s) => {
                // TODO: this is a hack to get the message from the json body
                let json = s.json_body();
                let default_message = "Unknown error".to_string();
                let message = json.get("message").map_or(default_message.clone(), |m| 
                    match m {
                        JsonValue::String(s) => s.clone(),
                        _ => default_message,
                    }
                );
                message
            }
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
            RestError::SubzeroCore(e) => {
                let status = e.status_code();
                StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            }
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
    let _requeset_gauge = Metrics::get()
        .proxy
        .connection_requests
        .guard(ctx.protocol());
    info!(
        protocol = %ctx.protocol(),
        "handling interactive connection from client"
    );


    let host = request.uri().host().unwrap_or("").split('.').next().unwrap_or("");

    // we always use the authenticator role to connect to the database
    let autheticator_role = "authenticator";
    let connection_string = format!("postgresql://{}@{}.local.neon.build/database", autheticator_role, host);

    let conn_info = get_conn_info(
        &config.authentication_config,
        ctx,
        &connection_string,
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
            handle_rest_inner(ctx, request, &connection_string, conn_info.conn_info, jwt, backend).await
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

static HEADERS_TO_STRIP: &[&HeaderName] = &[
    //AUTHORIZATION,
    &NEON_REQUEST_ID,
    &CONN_STRING,
    &RAW_TEXT_OUTPUT,
    &ARRAY_MODE,
    &TXN_ISOLATION_LEVEL,
    &TXN_READ_ONLY,
    &TXN_DEFERRABLE,
];

static JSON_SCHEMA: &str = r#"
    {
        "schemas":[
            {
                "name":"test",
                "objects":[
                    {
                        "kind":"table",
                        "name":"items",
                        "columns":[
                            {
                                "name":"id",
                                "data_type":"integer",
                                "primary_key":true
                            },
                            {
                                "name":"name",
                                "data_type":"text"
                            }
                        ],
                        "foreign_keys":[],
                        "permissions":[]
                    }
                ]
            }
        ]
    }
"#;

pub fn fmt_env_query<'a>(env: &'a HashMap<&'a str, &'a str>) -> Snippet<'a> {
    "select "
        + if env.is_empty() {
            sql("null")
        } else {
            env.iter()
                .map(|(k, v)| "set_config(" + param(k as &SqlParam) + ", " + param(v as &SqlParam) + ", true)")
                .join(",")
        }
}

fn current_schema(db_schemas: &Vec<String>, method: &Method, headers: &HeaderMap) -> Result<String, SubzeroCoreError> {
    match (db_schemas.len() > 1, method, headers.get("accept-profile"), headers.get("content-profile")) {
        (false, ..) => Ok(db_schemas.first().unwrap_or(&"_inexistent_".to_string()).clone()),
        (_, &Method::DELETE, _, Some(content_profile_header))
        | (_, &Method::POST, _, Some(content_profile_header))
        | (_, &Method::PATCH, _, Some(content_profile_header))
        | (_, &Method::PUT, _, Some(content_profile_header)) => {
            match content_profile_header.to_str() {
                Ok(content_profile_str) => {
                    let content_profile = String::from(content_profile_str);
                    if db_schemas.contains(&content_profile) {
                        Ok(content_profile)
                    } else {
                        Err(SubzeroCoreError::UnacceptableSchema {
                            schemas: db_schemas.clone(),
                        })
                    }
                }
                Err(_) => Err(SubzeroCoreError::UnacceptableSchema {
                    schemas: db_schemas.clone(),
                })
            }
        }
        (_, _, Some(accept_profile_header), _) => {
            match accept_profile_header.to_str() {
                Ok(accept_profile_str) => {
                    let accept_profile = String::from(accept_profile_str);
                    if db_schemas.contains(&accept_profile) {
                        Ok(accept_profile)
                    } else {
                        Err(SubzeroCoreError::UnacceptableSchema {
                            schemas: db_schemas.clone(),
                        })
                    }
                }
                Err(_) => Err(SubzeroCoreError::UnacceptableSchema {
                    schemas: db_schemas.clone(),
                })
            }
        }
        _ => Ok(db_schemas.first().unwrap_or(&"_inexistent_".to_string()).clone()),
    }
}
pub fn to_core_error(e: SubzeroCoreError) -> RestError {
    RestError::SubzeroCore(e)
}


fn to_sql_param(p: &Param) -> JsonValue {
    match p {
        SV(SingleVal(v, ..)) => {
            JsonValue::String(v.to_string())
        }
        Str(v) => {
            JsonValue::String(v.to_string())
        }
        StrOwned(v) => {
            JsonValue::String((*v).clone())
        }
        PL(Payload(v, ..)) => {
            JsonValue::String(v.clone().into_owned())
        }
        LV(ListVal(v, ..)) => {
            if !v.is_empty() {
                JsonValue::String(format!(
                    "{{\"{}\"}}",
                    v.iter()
                        .map(|e| e.replace('\\', "\\\\").replace('\"', "\\\""))
                        .collect::<Vec<_>>()
                        .join("\",\"")
                    ))
            } else {
                JsonValue::String(r#"{}"#.to_string())
            }
        }
    }
}

async fn handle_rest_inner(
    ctx: &RequestContext,
    request: Request<Incoming>,
    connection_string: &str,
    conn_info: ConnInfo,
    jwt: String,
    backend: Arc<PoolingBackend>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, RestError> {
    let mut response_headers = vec![];
    // hardcoded values for now
    let max_http_body_size = 10 * 1024 * 1024; // 10MB limit
    
    let db_schemas = Vec::from(["test".to_string()]); // list of schemas available for the api
    let mut db_schema_parsed = serde_json::from_str::<DbSchema>(JSON_SCHEMA) // database schema shape (will come from introspection)
        .map_err(|e| RestError::SubzeroCore(JsonDeserialize {source:e }))?;
    let disable_internal_permissions = true; // in the context of neon we emulate postgrest (so no internal permissions checks)
    db_schema_parsed.use_internal_permissions = false; // TODO: change the introspection query to auto set this to false depending on params
    let db_schema = &db_schema_parsed;
    let api_prefix = "/rest/v1/";
    let db_extra_search_path = "public, extensions".to_string();
    let role_claim_key = ".role".to_string();
    let role_claim_path = format!("${}", role_claim_key);
    println!("role_claim_path: {:?}", role_claim_path);
    let db_anon_role = Some("anon".to_string());
    //let max_rows = Some("1000".to_string());
    let max_rows = None;
    let db_allowed_select_functions = DEFAULT_SAFE_SELECT_FUNCTIONS.iter().map(|m| *m).collect::<Vec<_>>();

    let jwt_parsed = backend
        .authenticate_with_jwt(ctx, &conn_info.user_info, jwt.clone()) //TODO: do not clone jwt
        .await
        .map_err(HttpConnError::from)?;
    let jwt_claims = match jwt_parsed.keys {
        ComputeCredentialKeys::JwtPayload(payload_bytes) => {
            // `payload_bytes` contains the raw JWT payload as Vec<u8>
            // You can deserialize it back to JSON or parse specific claims
            let payload: serde_json::Value = serde_json::from_slice(&payload_bytes)
                .map_err(|e| RestError::SubzeroCore(JsonDeserialize {source:e }))?;
            Some(payload)
        },
        _ => {
            None
        }
    };
    println!("jwt_payload: {:?}", &jwt_claims);
    let (role, authenticated) = match &jwt_claims {
        Some(claims) => match select(claims, &role_claim_path) {
            Ok(v) => match &v[..] {
                [JsonValue::String(s)] => Ok((Some(s), true)),
                _ => Ok((db_anon_role.as_ref(), true)),
            },
            Err(e) => Err(RestError::SubzeroCore(JwtTokenInvalid { message: format!("{e}") })),
        },
        None => Ok((db_anon_role.as_ref(), false)),
    }?;
    println!("role: {:?}", role);
    println!("authenticated: {:?}", authenticated);

    // do not allow unauthenticated requests when there is no anonymous role setup
    if let (None, false) = (role, authenticated) {
        return Err(RestError::SubzeroCore(JwtTokenInvalid {
            message: "unauthenticated requests not allowed".to_string(),
        }));
    }
    // println!("jwt: {:?}", jwt.keys);

    let role = match role {
        Some(r) => r,
        None => "",
    };

    
    let (parts, originial_body) = request.into_parts();
    let method = parts.method.to_string();
    let path = parts.uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");

    // this is actually the table name (or rpc/function_name)
    // TODO: rename this to something more descriptive
    let root = match parts.uri.path().strip_prefix(api_prefix) {
        Some(p) => Ok(p),
        None => Err(RestError::SubzeroCore(NotFound {
            target: parts.uri.path().to_string(),
        })),
    }?;
    
    let schema_name = &current_schema(&db_schemas, &parts.method, &parts.headers).map_err(RestError::SubzeroCore)?;
    if db_schemas.len() > 1 {
        response_headers.push(("Content-Profile".to_string(), schema_name.clone()));
    }

    let body = Full::new(Bytes::new()).map_err(|never| match never {}).boxed();
    

    // print all the local variables
    println!("schema_name: {:?}", schema_name);
    println!("db_schemas: {:?}", db_schemas);
    println!("db_schema: {:?}", db_schema);
    println!("root: {:?}", root);
    println!("method: {:?}", method);
    println!("path: {:?}", path);
    println!("response_headers: {:?}", response_headers);
    println!("originial_body: {:?}", originial_body);
    //println!("parts: {:?}", parts);
   
    println!("conn_info: {:?}", conn_info);
    println!("jwt: {:?}", jwt);
    
    
    let query = match parts.uri.query() {
        Some(q) => form_urlencoded::parse(q.as_bytes()).collect(),
        None => vec![],
    };
    let get: Vec<(&str, &str)> = query.iter().map(|(k, v)| (&**k, &**v)).collect();

    let headers_map = parts.headers;
    let headers: HashMap<&str, &str> = headers_map.iter()
        .map(|(k, v)| (k.as_str(), v.to_str().unwrap_or("__BAD_HEADER__")))
        .collect();

    let cookies = HashMap::new(); // TODO: add cookies
    
    // Read the request body
    let body_bytes = read_body_with_limit(originial_body, max_http_body_size).await.map_err(ReadPayloadError::from)?; // 10MB limit
    let body_as_string: Option<String> = if body_bytes.is_empty() {
        None
    } else {
        Some(String::from_utf8_lossy(&body_bytes).into_owned())
    };
    
    println!("ready to parse!!!!!!!");
    let mut api_request = parse(schema_name, root, db_schema, method.as_str(), path, get, body_as_string.as_deref(), headers, cookies, max_rows).map_err(RestError::SubzeroCore)?;

    

    // in case when the role is not set (but authenticated through jwt) the query will be executed with the privileges
    // of the "authenticator" role unless the DbSchema has internal privileges set

    // replace "*" with the list of columns the user has access to
    // so that he does not encounter permission errors
    // replace_select_star(db_schema, schema_name, role, &mut api_request.query).map_err(to_core_error)?;
    println!("after replace_select_star !!!!!!!");

    if !disable_internal_permissions {
        // check_privileges(db_schema, schema_name, role, &api_request).map_err(to_core_error)?;
        println!("after check_privileges !!!!!!!");
    }
    println!("after check_privileges 2 !!!!!!!");
    check_safe_functions(&api_request, &db_allowed_select_functions).map_err(to_core_error)?;
    println!("after check_safe_functions !!!!!!!");
    if !disable_internal_permissions {
        insert_policy_conditions(db_schema, schema_name, role, &mut api_request.query).map_err(to_core_error)?;
        println!("after insert_policy_conditions !!!!!!!");
    }

    println!("api_request after checks: {:?}", api_request);

    // when using internal privileges not switch "current_role"
    // TODO: why do we need this?
    let env_role = if !disable_internal_permissions && db_schema.use_internal_permissions {
        None
    } else {
        Some(role)
    };

    let empty_json = "{}".to_string();
    let headers_env = serde_json::to_string(&api_request.headers).unwrap_or(empty_json.clone());
    let cookies_env = serde_json::to_string(&api_request.cookies).unwrap_or(empty_json.clone());
    let get_env = serde_json::to_string(&api_request.get).unwrap_or(empty_json.clone());
    let jwt_claims_env = jwt_claims.as_ref().map(|v| serde_json::to_string(v).unwrap_or(empty_json.clone()))
        .unwrap_or(
            if let Some(r) = env_role {
                let claims: HashMap<&str, &str> = HashMap::from([("role", r)]);
                serde_json::to_string(&claims).unwrap_or(empty_json.clone())
            } else {
                empty_json.clone()
            }
        );
    let mut env: HashMap<&str, &str> = HashMap::from([
        ("request.method", api_request.method),
        ("request.path", api_request.path),
        ("search_path", &db_extra_search_path),
        ("request.headers", &headers_env),
        ("request.cookies", &cookies_env),
        ("request.get", &get_env),
        ("request.jwt.claims", &jwt_claims_env),
    ]);
    if let Some(r) = env_role {
        env.insert("role", r.into());
    }
    let (env_statement, env_parameters, _) = generate(fmt_env_query(&env));
    let (main_statement, main_parameters, _) = generate(fmt_main_query(db_schema, api_request.schema_name, &api_request, &env).map_err(to_core_error)?);

    println!("env_statement: {:?} \n env_parameters: {:?}", env_statement, env_parameters);
    println!("main_statement: {:?} \n main_parameters: {:?}", main_statement, main_parameters);

    
    // now we are ready to send the request to the local proxy
    let mut client = backend.connect_to_local_proxy(ctx, conn_info).await?;

    let local_proxy_uri = ::http::Uri::from_static("http://proxy.local/sql");

    
    let mut req = Request::builder().method(Method::POST).uri(local_proxy_uri);

    // todo(conradludgate): maybe auth-broker should parse these and re-serialize
    // these instead just to ensure they remain normalised.
    // for &h in HEADERS_TO_FORWARD {
    //     if let Some(hv) = parts.headers.remove(h) {
    //         req = req.header(h, hv);
    //     }
    // }
    // forward all headers except the ones in HEADERS_TO_STRIP
    for (h, v) in headers_map.iter() {
        if !HEADERS_TO_STRIP.contains(&h) {
            req = req.header(h, v);
        }
    }
    req = req.header(&NEON_REQUEST_ID, uuid_to_header_value(ctx.session_id()));
    req = req.header(&CONN_STRING, HeaderValue::from_str(connection_string).unwrap());

    // FIXME: remove this header
    req = req.header(&HACK_TRUST_ROLE_SWITCHING, HeaderValue::from_static("true"));

    
    
    
    let env_parameters_json  = env_parameters.iter().map(|p| to_sql_param(&p.to_param())).collect::<Vec<_>>();
    let main_parameters_json = main_parameters.iter().map(|p| to_sql_param(&p.to_param())).collect::<Vec<_>>();
    let body: String = json!({
        "queries": [
            {
                "query": env_statement,
                "params": env_parameters_json,
            },
            {
                "query": main_statement,
                "params": main_parameters_json,
            }
        ]
    }).to_string();

    let body_boxed = Full::new(Bytes::from(body))
        .map_err(|never| match never {}) // Convert Infallible to hyper::Error
        .boxed();
    
    
    let req = req
        .body(body_boxed)
        .expect("all headers and params received via hyper should be valid for request");

    // todo: map body to count egress
    let _metrics = client.metrics(ctx);

    let response = client
        .inner
        .inner
        .send_request(req)
        .await
        .map_err(LocalProxyConnError::from)
        .map_err(HttpConnError::from)?;

    // Capture the response body
    let response_body = response
        .collect()
        .await
        .map_err(ReadPayloadError::from)?
        .to_bytes();

    // Parse the JSON response and extract the body content efficiently
    //let body_string = {
    let mut response_json: serde_json::Value = serde_json::from_slice(&response_body)
        .map_err(|e| RestError::SubzeroCore(JsonDeserialize { source: e }))?;

    println!("Response from local proxy: {:?}", response_json);

    // Extract the second query result (main query)
    let results = response_json["results"].as_array_mut()
        .ok_or_else(|| RestError::SubzeroCore(InternalError { 
            message: "Missing 'results' array".to_string() 
        }))?;

    if results.len() < 2 {
        return Err(RestError::SubzeroCore(InternalError { 
            message: "Expected at least 2 results".to_string() 
        }));
    }

    let second_result = &mut results[1];
    let rows = second_result["rows"].as_array_mut()
        .ok_or_else(|| RestError::SubzeroCore(InternalError { 
            message: "Missing 'rows' array in second result".to_string() 
        }))?;

    if rows.is_empty() {
        return Err(RestError::SubzeroCore(InternalError { 
            message: "No rows in second result".to_string() 
        }));
    }

    // Extract columns from the first (and only) row
    let row = &mut rows[0];
    

    // Extract the owned String directly from the JsonValue (without copying)
    let body_string = match row["body"].take() {
        JsonValue::String(s) => s,
        _ => {
            return Err(RestError::SubzeroCore(InternalError { 
                message: "Missing 'body' field".to_string() 
            }));
        }
    };
    let page_total = row["page_total"].as_str();
    let total_result_set = row["total_result_set"].as_str();
    let constraints_satisfied = row["constraints_satisfied"].as_bool()
        .unwrap_or(true);
    let response_headers = row["response_headers"].as_str();
    let response_status = row["response_status"].as_str();

    println!("Extracted columns:");
    println!("  page_total: {:?}", page_total);
    println!("  total_result_set: {:?}", total_result_set);
    println!("  constraints_satisfied: {:?}", constraints_satisfied);
    println!("  response_headers: {:?}", response_headers);
    println!("  response_status: {:?}", response_status);
    println!("  body: {:?}", &body_string);
        
        //body_string
    //};

    // For now, return the body content as the response - Bytes::from(String) consumes without copying
    let response_body = Full::new(Bytes::from(body_string))
        .map_err(|never| match never {})
        .boxed();

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(response_body)
        .unwrap())

}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload() {
        
    }
}
