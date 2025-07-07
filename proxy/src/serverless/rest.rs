use super::backend::HttpConnError;
use super::backend::{LocalProxyConnError, PoolingBackend};
use super::conn_pool::AuthData;
use super::conn_pool_lib::ConnInfo;
use super::error::{ConnInfoError, Credentials, HttpCodeError, ReadPayloadError};
use super::http_conn_pool::{self, Send};
use super::http_util::{
    ALLOW_POOL, CONN_STRING, NEON_REQUEST_ID, RAW_TEXT_OUTPUT, TXN_ISOLATION_LEVEL, TXN_READ_ONLY,
    get_conn_info, json_response, uuid_to_header_value,
};
use super::json::JsonConversionError;
use crate::auth::backend::ComputeCredentialKeys;
use crate::cache::TimedLru;
use crate::config::ProxyConfig;
use crate::context::RequestContext;
use crate::error::{ErrorKind, ReportableError, UserFacingError};
use crate::http::read_body_with_limit;
use crate::metrics::Metrics;
use crate::types::EndpointCacheKey;
use bytes::Bytes;
use http::Method;
use http::header::AUTHORIZATION;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use http_utils::error::ApiError;
use hyper::{
    Request, Response, StatusCode,
    body::Incoming,
    http::{HeaderName, HeaderValue},
};
use indexmap::IndexMap;
use jsonpath_lib::select;
use ouroboros::self_referencing;
use serde::{Deserialize, Deserializer};
use serde_json::{Value as JsonValue, value::RawValue};
use std::collections::HashMap;
use std::sync::Arc;
use subzero_core::{
    api::{
        ApiResponse, ContentType::*, ListVal, Payload, Preferences, QueryNode::*, Representation,
        Resolution::*, SingleVal,
    },
    config::{db_allowed_select_functions, db_schemas, role_claim_key /*to_tuple*/},
    content_range_header, content_range_status,
    dynamic_statement::{JoinIterator, param, sql},
    error::Error::{
        self as SubzeroCoreError, ContentTypeError, GucHeadersError, GucStatusError, InternalError,
        JsonDeserialize, JwtTokenInvalid, NotFound,
    },
    error::pg_error_to_status_code,
    formatter::{
        Param,
        Param::*,
        Snippet, SqlParam,
        postgresql::{fmt_main_query, generate},
    },
    parser::postgrest::parse,
    permissions::check_safe_functions,
    schema::{DbSchema, POSTGRESQL_INTROSPECTION_SQL, get_postgresql_configuration_query},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use typed_json::json;
use url::form_urlencoded;

static EMPTY_JSON_SCHEMA: &str = r#"{"schemas":[]}"#;
const INTROSPECTION_SQL: &str = POSTGRESQL_INTROSPECTION_SQL;

// A wrapper around the DbSchema that allows for self-referencing
#[self_referencing]
pub struct DbSchemaOwned {
    schema_string: String,
    #[covariant]
    #[borrows(schema_string)]
    schema: Result<DbSchema<'this>, SubzeroCoreError>,
}

fn split_comma_separated(s: &str) -> Vec<String> {
    s.split(',').map(|s| s.trim().to_string()).collect()
}

fn deserialize_comma_separated<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(split_comma_separated(&s))
}

fn deserialize_comma_separated_option<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    Ok(opt.map(|s| split_comma_separated(&s)))
}

// The ApiConfig is the configuration for the API per endpoint
// The configuration is read from the database and cached in the DbSchemaCache
#[derive(Deserialize, Debug)]
pub struct ApiConfig {
    #[serde(
        default = "db_schemas",
        deserialize_with = "deserialize_comma_separated"
    )]
    pub db_schemas: Vec<String>,
    pub db_anon_role: Option<String>,
    pub db_max_rows: Option<String>,
    #[serde(default = "db_allowed_select_functions")]
    pub db_allowed_select_functions: Vec<String>,
    //#[serde(deserialize_with = "to_tuple", default)]
    //pub db_pre_request: Option<(String, String)>,
    #[serde(default = "role_claim_key")]
    pub role_claim_key: String,
    #[serde(deserialize_with = "deserialize_comma_separated_option")]
    pub db_extra_search_path: Option<Vec<String>>,
}

// The DbSchemaCache is a cache of the ApiConfig and DbSchemaOwned for each endpoint
pub(crate) type DbSchemaCache = TimedLru<EndpointCacheKey, Arc<(ApiConfig, DbSchemaOwned)>>;
impl DbSchemaCache {
    pub async fn get_cached_or_remote(
        &self,
        endpoint_id: &EndpointCacheKey,
        auth_header: &HeaderValue,
        connection_string: &str,
        client: &mut http_conn_pool::Client<Send>,
        ctx: &RequestContext,
        config: &'static ProxyConfig,
    ) -> Result<Arc<(ApiConfig, DbSchemaOwned)>, RestError> {
        match self.get(endpoint_id) {
            Some(entry) => Ok(entry.value),
            None => {
                info!("db_schema cache miss for endpoint: {:?}", endpoint_id);
                let remote_value = self
                    .get_remote(auth_header, connection_string, client, ctx, config)
                    .await;
                let (api_config, schema_owned) = match remote_value {
                    Ok((api_config, schema_owned)) => (api_config, schema_owned),
                    Err(e @ RestError::SchemaTooLarge(_, _)) => {
                        // for the case where the schema is too large, we cache an empty dummy value
                        // all the other requests will fail without triggering the introspection query
                        let schema_owned = DbSchemaOwned::new(EMPTY_JSON_SCHEMA.to_string(), |s| {
                            serde_json::from_str::<DbSchema>(s.as_str())
                                .map_err(|e| JsonDeserialize { source: e })
                        });
                        let api_config = ApiConfig {
                            db_schemas: vec![],
                            db_anon_role: None,
                            db_max_rows: None,
                            db_allowed_select_functions: vec![],
                            role_claim_key: "".to_string(),
                            db_extra_search_path: None,
                        };
                        let value = Arc::new((api_config, schema_owned));
                        self.insert(endpoint_id.clone(), value);
                        return Err(e);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                };
                let value = Arc::new((api_config, schema_owned));
                self.insert(endpoint_id.clone(), value.clone());
                Ok(value)
            }
        }
    }
    pub async fn get_remote(
        &self,
        auth_header: &HeaderValue,
        connection_string: &str,
        client: &mut http_conn_pool::Client<Send>,
        ctx: &RequestContext,
        config: &'static ProxyConfig,
    ) -> Result<(ApiConfig, DbSchemaOwned), RestError> {
        let headers = vec![
            (&NEON_REQUEST_ID, uuid_to_header_value(ctx.session_id())),
            (
                &CONN_STRING,
                HeaderValue::from_str(connection_string).unwrap(),
            ),
            (
                &TXN_ISOLATION_LEVEL,
                HeaderValue::from_str("ReadCommitted").unwrap(),
            ),
            (&AUTHORIZATION, auth_header.clone()),
            (&RAW_TEXT_OUTPUT, HeaderValue::from_str("true").unwrap()),
        ];

        let query = get_postgresql_configuration_query(Some("pgrst.pre_config"));
        let body = serde_json::json!({"query": query});
        let (response_status, mut response_json) =
            make_local_proxy_request(client, headers, body).await?;

        if response_status != StatusCode::OK {
            return Err(RestError::SubzeroCore(InternalError {
                message: "Failed to get endpoint configuration".to_string(),
            }));
        }

        let rows = response_json["rows"].as_array_mut().ok_or_else(|| {
            RestError::SubzeroCore(InternalError {
                message: "Missing 'rows' array in second result".to_string(),
            })
        })?;

        if rows.is_empty() {
            return Err(RestError::SubzeroCore(InternalError {
                message: "No rows in second result".to_string(),
            }));
        }

        // Extract columns from the first (and only) row
        let mut row = &mut rows[0];
        let config_string = extract_string(&mut row, "config").unwrap_or_default();

        // Parse the configuration response
        let api_config: ApiConfig = serde_json::from_str(&config_string)
            .map_err(|e| RestError::SubzeroCore(JsonDeserialize { source: e }))?;

        // now that we have the api_config let's run the second INTROSPECTION_SQL query
        let headers = vec![
            (&NEON_REQUEST_ID, uuid_to_header_value(ctx.session_id())),
            (
                &CONN_STRING,
                HeaderValue::from_str(connection_string).unwrap(),
            ),
            (
                &TXN_ISOLATION_LEVEL,
                HeaderValue::from_str("ReadCommitted").unwrap(),
            ),
            (&AUTHORIZATION, auth_header.clone()),
            (&RAW_TEXT_OUTPUT, HeaderValue::from_str("true").unwrap()),
        ];

        let body = serde_json::json!({
            "query": INTROSPECTION_SQL,
            "params": [
                &api_config.db_schemas,
                false, // include_roles_with_login
                false, // use_internal_permissions
            ]
        });
        let (response_status, mut response_json) =
            make_local_proxy_request(client, headers, body).await?;

        if response_status != StatusCode::OK {
            return Err(RestError::SubzeroCore(InternalError {
                message: "Failed to get endpoint schema".to_string(),
            }));
        }

        let rows = response_json["rows"].as_array_mut().ok_or_else(|| {
            RestError::SubzeroCore(InternalError {
                message: "Missing 'rows' array in second result".to_string(),
            })
        })?;

        if rows.is_empty() {
            return Err(RestError::SubzeroCore(InternalError {
                message: "No rows in second result".to_string(),
            }));
        }

        // Extract columns from the first (and only) row
        let mut row = &mut rows[0];
        let json_schema = extract_string(&mut row, "json_schema").unwrap_or_default();
        let string_size = json_schema.len();

        if string_size > config.rest_config.max_schema_size {
            return Err(RestError::SchemaTooLarge(config.rest_config.max_schema_size, string_size));
        }

        let schema_owned = DbSchemaOwned::new(json_schema, |s| {
            serde_json::from_str::<DbSchema>(s.as_str()).map_err(|e| JsonDeserialize { source: e })
        });

        // check if schema is an ok result
        let schema = schema_owned.borrow_schema();
        if schema.is_ok() {
            Ok((api_config, schema_owned))
        } else {
            //
            Err(RestError::SubzeroCore(SubzeroCoreError::InternalError {
                message: "Failed to get schema".to_string(),
            }))
        }
    }
}

// A type to represent a postgresql errors
// we use our own type (instead of postgres_client::Error) because we get the error from the json response
#[derive(Debug, thiserror::Error)]
pub(crate) struct PostgresError {
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}
impl HttpCodeError for PostgresError {
    fn get_http_status_code(&self) -> StatusCode {
        let status = pg_error_to_status_code(&self.code, true);
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    }
}
impl ReportableError for PostgresError {
    fn get_error_kind(&self) -> ErrorKind {
        ErrorKind::User
    }
}
impl UserFacingError for PostgresError {
    fn to_string_client(&self) -> String {
        if self.code.starts_with("PT") {
            "Postgres error".to_string()
        } else {
            self.message.clone()
        }
    }
}
impl std::fmt::Display for PostgresError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

// A type to represent errors that can occur in the rest broker
#[derive(Debug, thiserror::Error)]
pub(crate) enum RestError {
    #[error("{0}")]
    ReadPayload(#[from] ReadPayloadError),
    #[error("{0}")]
    ConnectCompute(#[from] HttpConnError),
    #[error("{0}")]
    ConnInfo(#[from] ConnInfoError),
    #[error("{0}")]
    Postgres(#[source] PostgresError),
    #[error("{0}")]
    JsonConversion(#[from] JsonConversionError),
    #[error("{0}")]
    SubzeroCore(#[source] SubzeroCoreError),
    #[error("schema is too large (max is {0} bytes, current is {1} bytes)")]
    SchemaTooLarge(usize, usize),
}
impl ReportableError for RestError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            RestError::ReadPayload(e) => e.get_error_kind(),
            RestError::ConnectCompute(e) => e.get_error_kind(),
            RestError::ConnInfo(e) => e.get_error_kind(),
            RestError::Postgres(_) => ErrorKind::Postgres,
            RestError::JsonConversion(_) => ErrorKind::Postgres,
            RestError::SubzeroCore(_) => ErrorKind::User,
            RestError::SchemaTooLarge(_, _) => ErrorKind::User,
        }
    }
}
impl UserFacingError for RestError {
    fn to_string_client(&self) -> String {
        match self {
            RestError::ReadPayload(p) => p.to_string(),
            RestError::ConnectCompute(c) => c.to_string_client(),
            RestError::ConnInfo(c) => c.to_string_client(),
            RestError::SchemaTooLarge(_, _) => self.to_string(),
            RestError::Postgres(p) => p.to_string_client(),
            RestError::JsonConversion(_) => "could not parse postgres response".to_string(),
            RestError::SubzeroCore(s) => {
                // TODO: this is a hack to get the message from the json body
                let json = s.json_body();
                let default_message = "Unknown error".to_string();
                let message = json
                    .get("message")
                    .map_or(default_message.clone(), |m| match m {
                        JsonValue::String(s) => s.clone(),
                        _ => default_message,
                    });
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
            RestError::Postgres(e) => e.get_http_status_code(),
            RestError::JsonConversion(_) => StatusCode::INTERNAL_SERVER_ERROR,
            RestError::SchemaTooLarge(_, _) => StatusCode::INTERNAL_SERVER_ERROR,
            RestError::SubzeroCore(e) => {
                let status = e.status_code();
                StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }
}
impl From<SubzeroCoreError> for RestError {
    fn from(e: SubzeroCoreError) -> Self {
        RestError::SubzeroCore(e)
    }
}

// Helper functions for the rest broker

fn fmt_env_query<'a>(env: &'a HashMap<&'a str, &'a str>) -> Snippet<'a> {
    "select "
        + if env.is_empty() {
            sql("null")
        } else {
            env.iter()
                .map(|(k, v)| {
                    "set_config(" + param(k as &SqlParam) + ", " + param(v as &SqlParam) + ", true)"
                })
                .join(",")
        }
}

// TODO: see about removing the need for cloning the values (inner things are &Cow<str> already)
fn to_sql_param(p: &Param) -> JsonValue {
    match p {
        SV(SingleVal(v, ..)) => JsonValue::String(v.to_string()),
        Str(v) => JsonValue::String(v.to_string()),
        StrOwned(v) => JsonValue::String((*v).clone()),
        PL(Payload(v, ..)) => JsonValue::String(v.clone().into_owned()),
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

fn extract_string(json: &mut serde_json::Value, key: &str) -> Option<String> {
    match json[key].take() {
        JsonValue::String(s) => Some(s),
        _ => None,
    }
}

async fn make_local_proxy_request(
    client: &mut http_conn_pool::Client<Send>,
    headers: Vec<(&HeaderName, HeaderValue)>,
    body: JsonValue,
) -> Result<(StatusCode, JsonValue), RestError> {
    let local_proxy_uri = ::http::Uri::from_static("http://proxy.local/sql");
    let mut req = Request::builder().method(Method::POST).uri(local_proxy_uri);
    let req_headers = req.headers_mut().unwrap();
    // Add all provided headers to the request
    for (header_name, header_value) in headers.into_iter() {
        req_headers.insert(header_name, header_value);
    }

    let body_string = body.to_string();
    let body_boxed = Full::new(Bytes::from(body_string))
        .map_err(|never| match never {}) // Convert Infallible to hyper::Error
        .boxed();

    let req = req.body(body_boxed).map_err(|_| {
        RestError::SubzeroCore(InternalError {
            message: "Failed to build request".to_string(),
        })
    })?;

    // Send the request to the local proxy
    let response = client
        .inner
        .inner
        .send_request(req)
        .await
        .map_err(LocalProxyConnError::from)
        .map_err(HttpConnError::from)?;

    let response_status = response.status();

    // Capture the response body
    let response_body = response
        .collect()
        .await
        .map_err(ReadPayloadError::from)?
        .to_bytes();

    // Parse the JSON response
    let response_json: serde_json::Value = serde_json::from_slice(&response_body)
        .map_err(|e| RestError::SubzeroCore(JsonDeserialize { source: e }))?;

    Ok((response_status, response_json))
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
            if config.rest_config.is_rest_broker && r.status().is_server_error() {
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
        // Err(e @ RestError::Cancelled(_)) => {
        //     let error_kind = e.get_error_kind();
        //     ctx.set_error_kind(error_kind);

        //     let message = "Query cancelled, connection was terminated";

        //     tracing::info!(
        //         kind=error_kind.to_metric_label(),
        //         error=%e,
        //         msg=message,
        //         "forwarding error to user"
        //     );

        //     json_response(
        //         StatusCode::BAD_REQUEST,
        //         json!({ "message": message, "code": SqlState::PROTOCOL_VIOLATION.code() }),
        //     )?
        // }
        Err(e @ RestError::SubzeroCore(_)) => {
            let error_kind = e.get_error_kind();
            ctx.set_error_kind(error_kind);

            tracing::info!(
                kind=error_kind.to_metric_label(),
                error=%e,
                msg="subzero core error",
                "forwarding error to user"
            );
            let subzero_err = match e {
                RestError::SubzeroCore(e) => e,
                _ => panic!("expected subzero core error"),
            };

            let json_body = subzero_err.json_body();
            let status_code = StatusCode::from_u16(subzero_err.status_code())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

            json_response(status_code, json_body)?
        }
        Err(e) => {
            let error_kind = e.get_error_kind();
            ctx.set_error_kind(error_kind);

            let message = e.to_string_client();
            let status_code = e.get_http_status_code();

            tracing::info!(
                kind=error_kind.to_metric_label(),
                error=%e,
                msg=message,
                "forwarding error to user"
            );

            let (code, detail, hint) = match e {
                RestError::Postgres(e) => (
                    if e.code.starts_with("PT") {
                        None
                    } else {
                        Some(e.code)
                    },
                    e.detail,
                    e.hint,
                ),
                _ => (None, None, None),
            };

            json_response(
                status_code,
                json!({
                    "message": message,
                    "code": code,
                    "detail": detail,
                    "hint": hint,
                }),
            )?
        }
    };

    response
        .headers_mut()
        .insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
    Ok(response)
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

    let host = request.uri().host().unwrap_or("");
    let path_parts: Vec<&str> = request.uri().path().split('/').collect();
    // a valid path is /database/rest/v1/... so parts should be ["", "database", "rest", "v1", ...]
    let database_name = if path_parts.len() >= 3 && path_parts[1].len() > 0 {
        Ok(path_parts[1])
    } else {
        Err(RestError::SubzeroCore(NotFound {
            target: request.uri().path().to_string(),
        }))
    }?;

    // we always use the authenticator role to connect to the database
    let autheticator_role = "authenticator";
    let connection_string = format!(
        "postgresql://{}@{}/{}",
        autheticator_role, host, database_name
    );

    let conn_info = get_conn_info(
        &config.authentication_config,
        ctx,
        Some(&connection_string),
        request.headers(),
    )?;
    info!(
        user = conn_info.conn_info.user_info.user.as_str(),
        "credentials"
    );

    match conn_info.auth {
        AuthData::Jwt(jwt) if config.rest_config.is_rest_broker => {
            let api_prefix = format!("/{}/rest/v1/", database_name);
            handle_rest_inner(
                config,
                ctx,
                &api_prefix,
                request,
                &connection_string,
                conn_info.conn_info,
                jwt,
                backend,
            )
            .await
        }
        _ => Err(RestError::ConnInfo(ConnInfoError::MissingCredentials(
            Credentials::BearerJwt,
        ))),
    }
}

async fn handle_rest_inner(
    config: &'static ProxyConfig,
    ctx: &RequestContext,
    api_prefix: &str,
    request: Request<Incoming>,
    connection_string: &str,
    conn_info: ConnInfo,
    jwt: String,
    backend: Arc<PoolingBackend>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, RestError> {
    // validate the jwt token
    let jwt_parsed = backend
        .authenticate_with_jwt(ctx, &conn_info.user_info, jwt)
        .await
        .map_err(HttpConnError::from)?;

    let db_schema_cache = match config.rest_config.db_schema_cache.as_ref() {
        Some(cache) => cache,
        None => {
            return Err(RestError::SubzeroCore(InternalError {
                message: "DB schema cache is not configured".to_string(),
            }));
        }
    };

    let endpoint_cache_key = conn_info.endpoint_cache_key().ok_or_else(|| {
        RestError::SubzeroCore(InternalError {
            message: "Failed to get endpoint cache key".to_string(),
        })
    })?;
    let mut client = backend.connect_to_local_proxy(ctx, conn_info).await?;
    let (parts, originial_body) = request.into_parts();
    let headers_map = parts.headers;
    let auth_header = headers_map.get(AUTHORIZATION).unwrap();
    let entry = db_schema_cache
        .get_cached_or_remote(
            &endpoint_cache_key,
            auth_header,
            &connection_string,
            &mut client,
            &ctx,
            config,
        )
        .await?;
    let (api_config, db_schema_owned) = entry.as_ref();
    let db_schema = db_schema_owned.borrow_schema().as_ref().map_err(|_| {
        RestError::SubzeroCore(InternalError {
            message: "Failed to get schema".to_string(),
        })
    })?;

    let db_schemas = &api_config.db_schemas; // list of schemas available for the api
    let db_extra_search_path = &api_config.db_extra_search_path;
    let role_claim_key = &api_config.role_claim_key;
    let role_claim_path = format!("${}", role_claim_key);
    let db_anon_role = &api_config.db_anon_role;
    let max_rows = api_config.db_max_rows.as_ref().map(|s| s.as_str());
    let db_allowed_select_functions = api_config
        .db_allowed_select_functions
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>();

    // extract the jwt claims (we'll need them later to set the role and env)
    let jwt_claims = match jwt_parsed.keys {
        ComputeCredentialKeys::JwtPayload(payload_bytes) => {
            // `payload_bytes` contains the raw JWT payload as Vec<u8>
            // You can deserialize it back to JSON or parse specific claims
            let payload: serde_json::Value = serde_json::from_slice(&payload_bytes)
                .map_err(|e| RestError::SubzeroCore(JsonDeserialize { source: e }))?;
            Some(payload)
        }
        _ => None,
    };
    //TODO: check if the token is properly cached in the backend (should we cache the parsed claims?)
    // read the role from the jwt claims (and set it to the "anon" role if not present)
    let (role, authenticated) = match &jwt_claims {
        Some(claims) => match select(claims, &role_claim_path) {
            Ok(v) => match &v[..] {
                [JsonValue::String(s)] => Ok((Some(s), true)),
                _ => Ok((db_anon_role.as_ref(), true)),
            },
            Err(e) => Err(RestError::SubzeroCore(JwtTokenInvalid {
                message: format!("{e}"),
            })),
        },
        None => Ok((db_anon_role.as_ref(), false)),
    }?;

    // do not allow unauthenticated requests when there is no anonymous role setup
    if let (None, false) = (role, authenticated) {
        return Err(RestError::SubzeroCore(JwtTokenInvalid {
            message: "unauthenticated requests not allowed".to_string(),
        }));
    }

    // start deconstructing the request because subzero core mostly works with &str
    let method = parts.method;
    let method_str = method.to_string();
    let path = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");

    // this is actually the table name (or rpc/function_name)
    // TODO: rename this to something more descriptive
    let root = match parts.uri.path().strip_prefix(api_prefix) {
        Some(p) => Ok(p),
        None => Err(RestError::SubzeroCore(NotFound {
            target: parts.uri.path().to_string(),
        })),
    }?;

    // pick the current schema from the headers (or the first one from config)
    let schema_name = &DbSchema::pick_current_schema(db_schemas, &method_str, &headers_map)?;

    // add the content-profile header to the response
    let mut response_headers = vec![];
    if db_schemas.len() > 1 {
        response_headers.push(("Content-Profile".to_string(), schema_name.to_string()));
    }

    // parse the query string into a Vec<(&str, &str)>
    let query = match parts.uri.query() {
        Some(q) => form_urlencoded::parse(q.as_bytes()).collect(),
        None => vec![],
    };
    let get: Vec<(&str, &str)> = query.iter().map(|(k, v)| (&**k, &**v)).collect();

    // convert the headers map to a HashMap<&str, &str>
    let headers: HashMap<&str, &str> = headers_map
        .iter()
        .map(|(k, v)| (k.as_str(), v.to_str().unwrap_or("__BAD_HEADER__")))
        .collect();

    let cookies = HashMap::new(); // TODO: add cookies

    // Read the request body
    let body_bytes = read_body_with_limit(originial_body, config.http_config.max_request_size_bytes)
        .await
        .map_err(ReadPayloadError::from)?;
    let body_as_string: Option<String> = if body_bytes.is_empty() {
        None
    } else {
        Some(String::from_utf8_lossy(&body_bytes).into_owned())
    };

    // parse the request into an ApiRequest struct
    let api_request = parse(
        schema_name,
        root,
        db_schema,
        &method_str,
        path,
        get,
        body_as_string.as_deref(),
        headers,
        cookies,
        max_rows,
    )
    .map_err(RestError::SubzeroCore)?;

    // replace "*" with the list of columns the user has access to
    // so that he does not encounter permission errors
    // this only works if the schema intropsection includes the database permissions (which for now it does not)
    // replace_select_star(db_schema, schema_name, role, &mut api_request.query)?;

    let role_str = match role {
        Some(r) => r,
        None => "",
    };

    // this is not relevant when acting as PostgREST
    // if !disable_internal_permissions {
    //     check_privileges(db_schema, schema_name, role_str, &api_request)?;
    // }

    check_safe_functions(&api_request, &db_allowed_select_functions)?;

    // this is not relevant when acting as PostgREST
    // if !disable_internal_permissions {
    //     insert_policy_conditions(db_schema, schema_name, role_str, &mut api_request.query)?;
    // }

    let env_role = Some(role_str);

    // construct the env (passed in to the sql context as GUCs)
    let empty_json = "{}".to_string();
    let headers_env = serde_json::to_string(&api_request.headers).unwrap_or(empty_json.clone());
    let cookies_env = serde_json::to_string(&api_request.cookies).unwrap_or(empty_json.clone());
    let get_env = serde_json::to_string(&api_request.get).unwrap_or(empty_json.clone());
    let jwt_claims_env = jwt_claims
        .as_ref()
        .map(|v| serde_json::to_string(v).unwrap_or(empty_json.clone()))
        .unwrap_or(if let Some(r) = env_role {
            let claims: HashMap<&str, &str> = HashMap::from([("role", r)]);
            serde_json::to_string(&claims).unwrap_or(empty_json.clone())
        } else {
            empty_json.clone()
        });
    let mut search_path = vec![api_request.schema_name];
    if let Some(extra) = &db_extra_search_path {
        search_path.extend(extra.iter().map(|s| s.as_str()));
    }
    let search_path_str = search_path.join(",");
    let mut env: HashMap<&str, &str> = HashMap::from([
        ("request.method", api_request.method),
        ("request.path", api_request.path),
        ("request.headers", &headers_env),
        ("request.cookies", &cookies_env),
        ("request.get", &get_env),
        ("request.jwt.claims", &jwt_claims_env),
        ("search_path", &search_path_str),
    ]);
    if let Some(r) = env_role {
        env.insert("role", r.into());
    }

    // generate the sql statements
    let (env_statement, env_parameters, _) = generate(fmt_env_query(&env));
    let (main_statement, main_parameters, _) = generate(fmt_main_query(
        db_schema,
        api_request.schema_name,
        &api_request,
        &env,
    )?);

    let mut headers = vec![
        (&NEON_REQUEST_ID, uuid_to_header_value(ctx.session_id())),
        (
            &CONN_STRING,
            HeaderValue::from_str(connection_string).unwrap(),
        ),
        (&AUTHORIZATION, auth_header.clone()),
        (
            &TXN_ISOLATION_LEVEL,
            HeaderValue::from_str("ReadCommitted").unwrap(),
        ),
        (&ALLOW_POOL, HeaderValue::from_str("true").unwrap()),
    ];

    if api_request.read_only {
        headers.push((&TXN_READ_ONLY, HeaderValue::from_str("true").unwrap()));
    }

    // convert the parameters from subzero core representation to a Vec<JsonValue>
    let env_parameters_json = env_parameters
        .iter()
        .map(|p| to_sql_param(&p.to_param()))
        .collect::<Vec<_>>();
    let main_parameters_json = main_parameters
        .iter()
        .map(|p| to_sql_param(&p.to_param()))
        .collect::<Vec<_>>();
    let body = serde_json::json!({
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
    });

    // todo: map body to count egress
    let _metrics = client.metrics(ctx); // FIXME: is everything in the context set correctly?

    // send the request to the local proxy
    let (response_status, mut response_json) =
        make_local_proxy_request(&mut client, headers, body).await?;

    // if the response status is greater than 399, then it is an error
    // FIXME: check if there are other error codes or shapes of the response
    if response_status.as_u16() > 399 {
        // turn this postgres error from the json into PostgresError
        let postgres_error = PostgresError {
            message: extract_string(&mut response_json, "message").unwrap_or_default(),
            code: extract_string(&mut response_json, "code").unwrap_or_default(),
            detail: extract_string(&mut response_json, "detail"),
            hint: extract_string(&mut response_json, "hint"),
        };

        return Err(RestError::Postgres(postgres_error));
    }

    // Extract the second query result (main query)
    let results = response_json["results"].as_array_mut().ok_or_else(|| {
        RestError::SubzeroCore(InternalError {
            message: "Missing 'results' array".to_string(),
        })
    })?;

    if results.len() < 2 {
        return Err(RestError::SubzeroCore(InternalError {
            message: "Expected at least 2 results".to_string(),
        }));
    }

    let second_result = &mut results[1];
    let rows = second_result["rows"].as_array_mut().ok_or_else(|| {
        RestError::SubzeroCore(InternalError {
            message: "Missing 'rows' array in second result".to_string(),
        })
    })?;

    if rows.is_empty() {
        return Err(RestError::SubzeroCore(InternalError {
            message: "No rows in second result".to_string(),
        }));
    }

    // Extract columns from the first (and only) row
    let mut row = &mut rows[0];
    let body_string = extract_string(&mut row, "body").unwrap_or_default();
    let page_total = extract_string(&mut row, "page_total");
    let total_result_set = extract_string(&mut row, "total_result_set");
    // constraints_satisfied is relevant only when using internal permissions
    // let constraints_satisfied = extract_string(&mut row, "constraints_satisfied");
    let response_headers_json = extract_string(&mut row, "response_headers");
    let response_status = extract_string(&mut row, "response_status");

    // build the intermediate response object
    let api_response = ApiResponse {
        page_total: page_total
            .map(|v| v.parse::<u64>().unwrap_or(0))
            .unwrap_or(0),
        total_result_set: total_result_set.map(|v| v.parse::<u64>().unwrap_or(0)),
        top_level_offset: 0, // FIXME: check why this is 0
        response_headers: response_headers_json,
        response_status,
        body: body_string,
    };

    // rollback the transaction if the page_total is not 1 and the accept_content_type is SingularJSON
    // we can not do this in the context of proxy for now
    // if api_request.accept_content_type == SingularJSON && api_response.page_total != 1 {
    //     transaction.rollback().await.context(PgDbSnafu { authenticated })?;
    //     return Err(to_core_error(SingularityError {
    //         count: api_response.page_total,
    //         content_type: "application/vnd.pgrst.object+json".to_string(),
    //     }));
    // }

    // rollback the transaction if the page_total is not 1 and the method is PUT
    // we can not do this in the context of proxy for now
    // if api_request.method == Method::PUT && api_response.page_total != 1 {
    //     // Makes sure the querystring pk matches the payload pk
    //     // e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted,
    //     // PUT /items?id=eq.14 { "id" : 2, .. } is rejected.
    //     // If this condition is not satisfied then nothing is inserted,
    //     transaction.rollback().await.context(PgDbSnafu { authenticated })?;
    //     return Err(to_core_error(PutMatchingPkError));
    // }

    // create and return the response to the client
    // this section mostly deals with setting the right headers according to PostgREST specs
    let page_total = api_response.page_total;
    let total_result_set = api_response.total_result_set;
    let top_level_offset = api_response.top_level_offset;
    let response_content_type = match (&api_request.accept_content_type, &api_request.query.node) {
        (SingularJSON, _)
        | (
            _,
            FunctionCall {
                returns_single: true,
                is_scalar: false,
                ..
            },
        ) => SingularJSON,
        (TextCSV, _) => TextCSV,
        _ => ApplicationJSON,
    };

    // check if the SQL env set some response headers (happens when we called a rpc function)
    if let Some(response_headers_str) = api_response.response_headers {
        match serde_json::from_str(response_headers_str.as_str()) {
            Ok(JsonValue::Array(headers_json)) => {
                for h in headers_json {
                    match h {
                        JsonValue::Object(o) => {
                            for (k, v) in o.into_iter() {
                                match v {
                                    JsonValue::String(s) => {
                                        response_headers.push((k, s));
                                        Ok(())
                                    }
                                    _ => Err(RestError::SubzeroCore(GucHeadersError)),
                                }?
                            }
                            Ok(())
                        }
                        _ => Err(RestError::SubzeroCore(GucHeadersError)),
                    }?
                }
                Ok(())
            }
            _ => Err(RestError::SubzeroCore(GucHeadersError)),
        }?
    }

    // calculate and set the content range header
    let lower = top_level_offset as i64;
    let upper = top_level_offset as i64 + page_total as i64 - 1;
    let total = total_result_set.map(|t| t as i64);
    let content_range = match (&method, &api_request.query.node) {
        (&Method::POST, Insert { .. }) => content_range_header(1, 0, total),
        (&Method::DELETE, Delete { .. }) => content_range_header(1, upper, total),
        _ => content_range_header(lower, upper, total),
    };
    response_headers.push(("Content-Range".to_string(), content_range));

    // calculate the status code
    #[rustfmt::skip]
    let mut status = match (&method, &api_request.query.node, page_total, &api_request.preferences) {
        (&Method::POST,   Insert { .. }, ..) => 201,
        (&Method::DELETE, Delete { .. }, _, Some(Preferences {representation: Some(Representation::Full),..}),) => 200,
        (&Method::DELETE, Delete { .. }, ..) => 204,
        (&Method::PATCH,  Update { columns, .. }, 0, _) if !columns.is_empty() => 404,
        (&Method::PATCH,  Update { .. }, _,Some(Preferences {representation: Some(Representation::Full),..}),) => 200,
        (&Method::PATCH,  Update { .. }, ..) => 204,
        (&Method::PUT,    Insert { .. },_,Some(Preferences {representation: Some(Representation::Full),..}),) => 200,
        (&Method::PUT,    Insert { .. }, ..) => 204,
        _ => content_range_status(lower, upper, total),
    };

    // add the preference-applied header
    if let Some(Preferences {
        resolution: Some(r),
        ..
    }) = api_request.preferences
    {
        response_headers.push((
            "Preference-Applied".to_string(),
            match r {
                MergeDuplicates => "resolution=merge-duplicates".to_string(),
                IgnoreDuplicates => "resolution=ignore-duplicates".to_string(),
            },
        ));
    }

    // check if the SQL env set some response status (happens when we called a rpc function)
    if let Some(response_status_str) = api_response.response_status {
        status = response_status_str
            .parse::<u16>()
            .map_err(|_| RestError::SubzeroCore(GucStatusError))?;
    }

    // set the content type header
    let http_content_type = match response_content_type {
        SingularJSON => Ok("application/vnd.pgrst.object+json"),
        TextCSV => Ok("text/csv"),
        ApplicationJSON => Ok("application/json"),
        Other(t) => Err(RestError::SubzeroCore(ContentTypeError {
            message: format!("None of these Content-Types are available: {t}"),
        })),
    }?;

    // build the response body
    let response_body = Full::new(Bytes::from(api_response.body))
        .map_err(|never| match never {})
        .boxed();

    // build the response
    let mut response = Response::builder()
        .status(StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
        .header("content-type", http_content_type);

    // Add all headers from response_headers vector
    for (header_name, header_value) in response_headers {
        response = response.header(header_name, header_value);
    }

    // add the body and return the response
    Ok(response.body(response_body).map_err(|_| {
        RestError::SubzeroCore(InternalError {
            message: "Failed to build response".to_string(),
        })
    })?)
}

#[cfg(test)]
mod tests {
    //use super::*;

    #[test]
    fn test_payload() {}
}
