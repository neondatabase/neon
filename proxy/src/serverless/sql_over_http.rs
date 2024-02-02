use std::sync::Arc;

use anyhow::bail;
use anyhow::Context;
use futures::pin_mut;
use futures::StreamExt;
use hyper::body::HttpBody;
use hyper::header;
use hyper::http::HeaderName;
use hyper::http::HeaderValue;
use hyper::Response;
use hyper::StatusCode;
use hyper::{Body, HeaderMap, Request};
use serde_json::json;
use serde_json::Value;
use tokio_postgres::error::DbError;
use tokio_postgres::error::ErrorPosition;
use tokio_postgres::GenericClient;
use tokio_postgres::IsolationLevel;
use tokio_postgres::ReadyForQueryStatus;
use tokio_postgres::Transaction;
use tracing::error;
use tracing::instrument;
use url::Url;
use utils::http::error::ApiError;
use utils::http::json::json_response;

use crate::auth::backend::ComputeUserInfo;
use crate::auth::endpoint_sni;
use crate::config::HttpConfig;
use crate::config::TlsConfig;
use crate::context::RequestMonitoring;
use crate::metrics::NUM_CONNECTION_REQUESTS_GAUGE;
use crate::proxy::NeonOptions;
use crate::RoleName;

use super::conn_pool::ConnInfo;
use super::conn_pool::GlobalConnPool;
use super::json::{json_to_pg_text, pg_text_row_to_json};
use super::SERVERLESS_DRIVER_SNI;

#[derive(serde::Deserialize)]
struct QueryData {
    query: String,
    params: Vec<serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct BatchQueryData {
    queries: Vec<QueryData>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum Payload {
    Single(QueryData),
    Batch(BatchQueryData),
}

const MAX_RESPONSE_SIZE: usize = 10 * 1024 * 1024; // 10 MiB
const MAX_REQUEST_SIZE: u64 = 10 * 1024 * 1024; // 10 MiB

static RAW_TEXT_OUTPUT: HeaderName = HeaderName::from_static("neon-raw-text-output");
static ARRAY_MODE: HeaderName = HeaderName::from_static("neon-array-mode");
static ALLOW_POOL: HeaderName = HeaderName::from_static("neon-pool-opt-in");
static TXN_ISOLATION_LEVEL: HeaderName = HeaderName::from_static("neon-batch-isolation-level");
static TXN_READ_ONLY: HeaderName = HeaderName::from_static("neon-batch-read-only");
static TXN_DEFERRABLE: HeaderName = HeaderName::from_static("neon-batch-deferrable");

static HEADER_VALUE_TRUE: HeaderValue = HeaderValue::from_static("true");

fn get_conn_info(
    ctx: &mut RequestMonitoring,
    headers: &HeaderMap,
    sni_hostname: Option<String>,
    tls: &TlsConfig,
) -> Result<ConnInfo, anyhow::Error> {
    let connection_string = headers
        .get("Neon-Connection-String")
        .ok_or(anyhow::anyhow!("missing connection string"))?
        .to_str()?;

    let connection_url = Url::parse(connection_string)?;

    let protocol = connection_url.scheme();
    if protocol != "postgres" && protocol != "postgresql" {
        return Err(anyhow::anyhow!(
            "connection string must start with postgres: or postgresql:"
        ));
    }

    let mut url_path = connection_url
        .path_segments()
        .ok_or(anyhow::anyhow!("missing database name"))?;

    let dbname = url_path
        .next()
        .ok_or(anyhow::anyhow!("invalid database name"))?;

    let username = RoleName::from(connection_url.username());
    if username.is_empty() {
        return Err(anyhow::anyhow!("missing username"));
    }
    ctx.set_user(username.clone());

    let password = connection_url
        .password()
        .ok_or(anyhow::anyhow!("no password"))?;

    // TLS certificate selector now based on SNI hostname, so if we are running here
    // we are sure that SNI hostname is set to one of the configured domain names.
    let sni_hostname = sni_hostname.ok_or(anyhow::anyhow!("no SNI hostname set"))?;

    let hostname = connection_url
        .host_str()
        .ok_or(anyhow::anyhow!("no host"))?;

    let host_header = headers
        .get("host")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.split(':').next());

    // sni_hostname has to be either the same as hostname or the one used in serverless driver.
    if !check_matches(&sni_hostname, hostname)? {
        return Err(anyhow::anyhow!("mismatched SNI hostname and hostname"));
    } else if let Some(h) = host_header {
        if h != sni_hostname {
            return Err(anyhow::anyhow!("mismatched host header and hostname"));
        }
    }

    let endpoint = endpoint_sni(hostname, &tls.common_names)?.context("malformed endpoint")?;
    ctx.set_endpoint_id(endpoint.clone());

    let pairs = connection_url.query_pairs();

    let mut options = Option::None;

    for (key, value) in pairs {
        if key == "options" {
            options = Some(NeonOptions::parse_options_raw(&value));
            break;
        }
    }

    let user_info = ComputeUserInfo {
        endpoint,
        user: username,
        options: options.unwrap_or_default(),
    };

    Ok(ConnInfo {
        user_info,
        dbname: dbname.into(),
        password: password.into(),
    })
}

fn check_matches(sni_hostname: &str, hostname: &str) -> Result<bool, anyhow::Error> {
    if sni_hostname == hostname {
        return Ok(true);
    }
    let (sni_hostname_first, sni_hostname_rest) = sni_hostname
        .split_once('.')
        .ok_or_else(|| anyhow::anyhow!("Unexpected sni format."))?;
    let (_, hostname_rest) = hostname
        .split_once('.')
        .ok_or_else(|| anyhow::anyhow!("Unexpected hostname format."))?;
    Ok(sni_hostname_rest == hostname_rest && sni_hostname_first == SERVERLESS_DRIVER_SNI)
}

// TODO: return different http error codes
pub async fn handle(
    tls: &'static TlsConfig,
    config: &'static HttpConfig,
    ctx: &mut RequestMonitoring,
    request: Request<Body>,
    sni_hostname: Option<String>,
    conn_pool: Arc<GlobalConnPool>,
) -> Result<Response<Body>, ApiError> {
    let result = tokio::time::timeout(
        config.request_timeout,
        handle_inner(tls, config, ctx, request, sni_hostname, conn_pool),
    )
    .await;
    let mut response = match result {
        Ok(r) => match r {
            Ok(r) => r,
            Err(e) => {
                let mut message = format!("{:?}", e);
                let db_error = e
                    .downcast_ref::<tokio_postgres::Error>()
                    .and_then(|e| e.as_db_error());
                fn get<'a, T: serde::Serialize>(
                    db: Option<&'a DbError>,
                    x: impl FnOnce(&'a DbError) -> T,
                ) -> Value {
                    db.map(x)
                        .and_then(|t| serde_json::to_value(t).ok())
                        .unwrap_or_default()
                }

                if let Some(db_error) = db_error {
                    db_error.message().clone_into(&mut message);
                }

                let position = db_error.and_then(|db| db.position());
                let (position, internal_position, internal_query) = match position {
                    Some(ErrorPosition::Original(position)) => (
                        Value::String(position.to_string()),
                        Value::Null,
                        Value::Null,
                    ),
                    Some(ErrorPosition::Internal { position, query }) => (
                        Value::Null,
                        Value::String(position.to_string()),
                        Value::String(query.clone()),
                    ),
                    None => (Value::Null, Value::Null, Value::Null),
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

                error!(
                    ?code,
                    "sql-over-http per-client task finished with an error: {e:#}"
                );
                // TODO: this shouldn't always be bad request.
                json_response(
                    StatusCode::BAD_REQUEST,
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
        },
        Err(_) => {
            let message = format!(
                "HTTP-Connection timed out, execution time exeeded {} seconds",
                config.request_timeout.as_secs()
            );
            error!(message);
            json_response(
                StatusCode::GATEWAY_TIMEOUT,
                json!({ "message": message, "code": StatusCode::GATEWAY_TIMEOUT.as_u16() }),
            )?
        }
    };
    response.headers_mut().insert(
        "Access-Control-Allow-Origin",
        hyper::http::HeaderValue::from_static("*"),
    );
    Ok(response)
}

#[instrument(name = "sql-over-http", fields(pid = tracing::field::Empty), skip_all)]
async fn handle_inner(
    tls: &'static TlsConfig,
    config: &'static HttpConfig,
    ctx: &mut RequestMonitoring,
    request: Request<Body>,
    sni_hostname: Option<String>,
    conn_pool: Arc<GlobalConnPool>,
) -> anyhow::Result<Response<Body>> {
    let _request_gauge = NUM_CONNECTION_REQUESTS_GAUGE
        .with_label_values(&["http"])
        .guard();

    //
    // Determine the destination and connection params
    //
    let headers = request.headers();
    let conn_info = get_conn_info(ctx, headers, sni_hostname, tls)?;

    // Determine the output options. Default behaviour is 'false'. Anything that is not
    // strictly 'true' assumed to be false.
    let raw_output = headers.get(&RAW_TEXT_OUTPUT) == Some(&HEADER_VALUE_TRUE);
    let array_mode = headers.get(&ARRAY_MODE) == Some(&HEADER_VALUE_TRUE);

    // Allow connection pooling only if explicitly requested
    // or if we have decided that http pool is no longer opt-in
    let allow_pool =
        !config.pool_options.opt_in || headers.get(&ALLOW_POOL) == Some(&HEADER_VALUE_TRUE);

    // isolation level, read only and deferrable

    let txn_isolation_level_raw = headers.get(&TXN_ISOLATION_LEVEL).cloned();
    let txn_isolation_level = match txn_isolation_level_raw {
        Some(ref x) => Some(match x.as_bytes() {
            b"Serializable" => IsolationLevel::Serializable,
            b"ReadUncommitted" => IsolationLevel::ReadUncommitted,
            b"ReadCommitted" => IsolationLevel::ReadCommitted,
            b"RepeatableRead" => IsolationLevel::RepeatableRead,
            _ => bail!("invalid isolation level"),
        }),
        None => None,
    };

    let txn_read_only = headers.get(&TXN_READ_ONLY) == Some(&HEADER_VALUE_TRUE);
    let txn_deferrable = headers.get(&TXN_DEFERRABLE) == Some(&HEADER_VALUE_TRUE);

    let paused = ctx.latency_timer.pause();
    let request_content_length = match request.body().size_hint().upper() {
        Some(v) => v,
        None => MAX_REQUEST_SIZE + 1,
    };
    drop(paused);

    // we don't have a streaming request support yet so this is to prevent OOM
    // from a malicious user sending an extremely large request body
    if request_content_length > MAX_REQUEST_SIZE {
        return Err(anyhow::anyhow!(
            "request is too large (max is {MAX_REQUEST_SIZE} bytes)"
        ));
    }

    //
    // Read the query and query params from the request body
    //
    let body = hyper::body::to_bytes(request.into_body()).await?;
    let payload: Payload = serde_json::from_slice(&body)?;

    let mut client = conn_pool.get(ctx, conn_info, !allow_pool).await?;

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json");

    //
    // Now execute the query and return the result
    //
    let mut size = 0;
    let result =
        match payload {
            Payload::Single(stmt) => {
                let (status, results) =
                    query_to_json(&*client, stmt, &mut 0, raw_output, array_mode)
                        .await
                        .map_err(|e| {
                            client.discard();
                            e
                        })?;
                client.check_idle(status);
                results
            }
            Payload::Batch(statements) => {
                let (inner, mut discard) = client.inner();
                let mut builder = inner.build_transaction();
                if let Some(isolation_level) = txn_isolation_level {
                    builder = builder.isolation_level(isolation_level);
                }
                if txn_read_only {
                    builder = builder.read_only(true);
                }
                if txn_deferrable {
                    builder = builder.deferrable(true);
                }

                let transaction = builder.start().await.map_err(|e| {
                    // if we cannot start a transaction, we should return immediately
                    // and not return to the pool. connection is clearly broken
                    discard.discard();
                    e
                })?;

                let results =
                    match query_batch(&transaction, statements, &mut size, raw_output, array_mode)
                        .await
                    {
                        Ok(results) => {
                            let status = transaction.commit().await.map_err(|e| {
                                // if we cannot commit - for now don't return connection to pool
                                // TODO: get a query status from the error
                                discard.discard();
                                e
                            })?;
                            discard.check_idle(status);
                            results
                        }
                        Err(err) => {
                            let status = transaction.rollback().await.map_err(|e| {
                                // if we cannot rollback - for now don't return connection to pool
                                // TODO: get a query status from the error
                                discard.discard();
                                e
                            })?;
                            discard.check_idle(status);
                            return Err(err);
                        }
                    };

                if txn_read_only {
                    response = response.header(
                        TXN_READ_ONLY.clone(),
                        HeaderValue::try_from(txn_read_only.to_string())?,
                    );
                }
                if txn_deferrable {
                    response = response.header(
                        TXN_DEFERRABLE.clone(),
                        HeaderValue::try_from(txn_deferrable.to_string())?,
                    );
                }
                if let Some(txn_isolation_level) = txn_isolation_level_raw {
                    response = response.header(TXN_ISOLATION_LEVEL.clone(), txn_isolation_level);
                }
                json!({ "results": results })
            }
        };

    ctx.set_success();
    ctx.log();
    let metrics = client.metrics();

    // how could this possibly fail
    let body = serde_json::to_string(&result).expect("json serialization should not fail");
    let len = body.len();
    let response = response
        .body(Body::from(body))
        // only fails if invalid status code or invalid header/values are given.
        // these are not user configurable so it cannot fail dynamically
        .expect("building response payload should not fail");

    // count the egress bytes - we miss the TLS and header overhead but oh well...
    // moving this later in the stack is going to be a lot of effort and ehhhh
    metrics.record_egress(len as u64);

    Ok(response)
}

async fn query_batch(
    transaction: &Transaction<'_>,
    queries: BatchQueryData,
    total_size: &mut usize,
    raw_output: bool,
    array_mode: bool,
) -> anyhow::Result<Vec<Value>> {
    let mut results = Vec::with_capacity(queries.queries.len());
    let mut current_size = 0;
    for stmt in queries.queries {
        // TODO: maybe we should check that the transaction bit is set here
        let (_, values) =
            query_to_json(transaction, stmt, &mut current_size, raw_output, array_mode).await?;
        results.push(values);
    }
    *total_size += current_size;
    Ok(results)
}

async fn query_to_json<T: GenericClient>(
    client: &T,
    data: QueryData,
    current_size: &mut usize,
    raw_output: bool,
    array_mode: bool,
) -> anyhow::Result<(ReadyForQueryStatus, Value)> {
    let query_params = json_to_pg_text(data.params);
    let row_stream = client.query_raw_txt(&data.query, query_params).await?;

    // Manually drain the stream into a vector to leave row_stream hanging
    // around to get a command tag. Also check that the response is not too
    // big.
    pin_mut!(row_stream);
    let mut rows: Vec<tokio_postgres::Row> = Vec::new();
    while let Some(row) = row_stream.next().await {
        let row = row?;
        *current_size += row.body_len();
        rows.push(row);
        // we don't have a streaming response support yet so this is to prevent OOM
        // from a malicious query (eg a cross join)
        if *current_size > MAX_RESPONSE_SIZE {
            return Err(anyhow::anyhow!(
                "response is too large (max is {MAX_RESPONSE_SIZE} bytes)"
            ));
        }
    }

    let ready = row_stream.ready_status();

    // grab the command tag and number of rows affected
    let command_tag = row_stream.command_tag().unwrap_or_default();
    let mut command_tag_split = command_tag.split(' ');
    let command_tag_name = command_tag_split.next().unwrap_or_default();
    let command_tag_count = if command_tag_name == "INSERT" {
        // INSERT returns OID first and then number of rows
        command_tag_split.nth(1)
    } else {
        // other commands return number of rows (if any)
        command_tag_split.next()
    }
    .and_then(|s| s.parse::<i64>().ok());

    let mut fields = vec![];
    let mut columns = vec![];

    for c in row_stream.columns() {
        fields.push(json!({
            "name": Value::String(c.name().to_owned()),
            "dataTypeID": Value::Number(c.type_().oid().into()),
            "tableID": c.table_oid(),
            "columnID": c.column_id(),
            "dataTypeSize": c.type_size(),
            "dataTypeModifier": c.type_modifier(),
            "format": "text",
        }));
        columns.push(client.get_type(c.type_oid()).await?);
    }

    // convert rows to JSON
    let rows = rows
        .iter()
        .map(|row| pg_text_row_to_json(row, &columns, raw_output, array_mode))
        .collect::<Result<Vec<_>, _>>()?;

    // resulting JSON format is based on the format of node-postgres result
    Ok((
        ready,
        json!({
            "command": command_tag_name,
            "rowCount": command_tag_count,
            "rows": rows,
            "fields": fields,
            "rowAsArray": array_mode,
        }),
    ))
}
