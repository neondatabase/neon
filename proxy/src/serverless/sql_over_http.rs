use std::net::IpAddr;
use std::sync::Arc;

use anyhow::bail;
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
use serde_json::Map;
use serde_json::Value;
use tokio_postgres::error::DbError;
use tokio_postgres::types::Kind;
use tokio_postgres::types::Type;
use tokio_postgres::GenericClient;
use tokio_postgres::IsolationLevel;
use tokio_postgres::ReadyForQueryStatus;
use tokio_postgres::Row;
use tokio_postgres::Transaction;
use tracing::error;
use tracing::instrument;
use url::Url;
use utils::http::error::ApiError;
use utils::http::json::json_response;

use crate::config::HttpConfig;
use crate::metrics::NUM_CONNECTION_REQUESTS_GAUGE;

use super::conn_pool::ConnInfo;
use super::conn_pool::GlobalConnPool;

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

//
// Convert json non-string types to strings, so that they can be passed to Postgres
// as parameters.
//
fn json_to_pg_text(json: Vec<Value>) -> Vec<Option<String>> {
    json.iter()
        .map(|value| {
            match value {
                // special care for nulls
                Value::Null => None,

                // convert to text with escaping
                v @ (Value::Bool(_) | Value::Number(_) | Value::Object(_)) => Some(v.to_string()),

                // avoid escaping here, as we pass this as a parameter
                Value::String(s) => Some(s.to_string()),

                // special care for arrays
                Value::Array(_) => json_array_to_pg_array(value),
            }
        })
        .collect()
}

//
// Serialize a JSON array to a Postgres array. Contrary to the strings in the params
// in the array we need to escape the strings. Postgres is okay with arrays of form
// '{1,"2",3}'::int[], so we don't check that array holds values of the same type, leaving
// it for Postgres to check.
//
// Example of the same escaping in node-postgres: packages/pg/lib/utils.js
//
fn json_array_to_pg_array(value: &Value) -> Option<String> {
    match value {
        // special care for nulls
        Value::Null => None,

        // convert to text with escaping
        // here string needs to be escaped, as it is part of the array
        v @ (Value::Bool(_) | Value::Number(_) | Value::String(_)) => Some(v.to_string()),
        v @ Value::Object(_) => json_array_to_pg_array(&Value::String(v.to_string())),

        // recurse into array
        Value::Array(arr) => {
            let vals = arr
                .iter()
                .map(json_array_to_pg_array)
                .map(|v| v.unwrap_or_else(|| "NULL".to_string()))
                .collect::<Vec<_>>()
                .join(",");

            Some(format!("{{{}}}", vals))
        }
    }
}

fn get_conn_info(
    headers: &HeaderMap,
    sni_hostname: Option<String>,
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

    let username = connection_url.username();
    if username.is_empty() {
        return Err(anyhow::anyhow!("missing username"));
    }

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

    if hostname != sni_hostname {
        return Err(anyhow::anyhow!("mismatched SNI hostname and hostname"));
    } else if let Some(h) = host_header {
        if h != hostname {
            return Err(anyhow::anyhow!("mismatched host header and hostname"));
        }
    }

    let pairs = connection_url.query_pairs();

    let mut options = Option::None;

    for (key, value) in pairs {
        if key == "options" {
            options = Some(value.into());
            break;
        }
    }

    Ok(ConnInfo {
        username: username.into(),
        dbname: dbname.into(),
        hostname: hostname.into(),
        password: password.into(),
        options,
    })
}

// TODO: return different http error codes
pub async fn handle(
    request: Request<Body>,
    sni_hostname: Option<String>,
    conn_pool: Arc<GlobalConnPool>,
    session_id: uuid::Uuid,
    peer_addr: IpAddr,
    config: &'static HttpConfig,
) -> Result<Response<Body>, ApiError> {
    let result = tokio::time::timeout(
        config.timeout,
        handle_inner(
            config,
            request,
            sni_hostname,
            conn_pool,
            session_id,
            peer_addr,
        ),
    )
    .await;
    let mut response = match result {
        Ok(r) => match r {
            Ok(r) => r,
            Err(e) => {
                let message = format!("{:?}", e);
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

                // TODO(conrad): db_error.position()
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
                let line = get(db_error, |db| db.line());
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
                        "severity": severity,
                        "where": where_,
                        "table": table,
                        "column": column,
                        "schema": schema,
                        "datatype": datatype,
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
                config.timeout.as_secs()
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
    config: &'static HttpConfig,
    request: Request<Body>,
    sni_hostname: Option<String>,
    conn_pool: Arc<GlobalConnPool>,
    session_id: uuid::Uuid,
    peer_addr: IpAddr,
) -> anyhow::Result<Response<Body>> {
    let _request_gauge = NUM_CONNECTION_REQUESTS_GAUGE
        .with_label_values(&["http"])
        .guard();

    //
    // Determine the destination and connection params
    //
    let headers = request.headers();
    let conn_info = get_conn_info(headers, sni_hostname)?;

    // Determine the output options. Default behaviour is 'false'. Anything that is not
    // strictly 'true' assumed to be false.
    let raw_output = headers.get(&RAW_TEXT_OUTPUT) == Some(&HEADER_VALUE_TRUE);
    let array_mode = headers.get(&ARRAY_MODE) == Some(&HEADER_VALUE_TRUE);

    // Allow connection pooling only if explicitly requested
    // or if we have decided that http pool is no longer opt-in
    let allow_pool = !config.pool_opt_in || headers.get(&ALLOW_POOL) == Some(&HEADER_VALUE_TRUE);

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

    let request_content_length = match request.body().size_hint().upper() {
        Some(v) => v,
        None => MAX_REQUEST_SIZE + 1,
    };

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

    let mut client = conn_pool
        .get(&conn_info, !allow_pool, session_id, peer_addr)
        .await?;

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

//
// Convert postgres row with text-encoded values to JSON object
//
pub fn pg_text_row_to_json(
    row: &Row,
    columns: &[Type],
    raw_output: bool,
    array_mode: bool,
) -> Result<Value, anyhow::Error> {
    let iter = row
        .columns()
        .iter()
        .zip(columns)
        .enumerate()
        .map(|(i, (column, typ))| {
            let name = column.name();
            let pg_value = row.as_text(i)?;
            let json_value = if raw_output {
                match pg_value {
                    Some(v) => Value::String(v.to_string()),
                    None => Value::Null,
                }
            } else {
                pg_text_to_json(pg_value, typ)?
            };
            Ok((name.to_string(), json_value))
        });

    if array_mode {
        // drop keys and aggregate into array
        let arr = iter
            .map(|r| r.map(|(_key, val)| val))
            .collect::<Result<Vec<Value>, anyhow::Error>>()?;
        Ok(Value::Array(arr))
    } else {
        let obj = iter.collect::<Result<Map<String, Value>, anyhow::Error>>()?;
        Ok(Value::Object(obj))
    }
}

//
// Convert postgres text-encoded value to JSON value
//
pub fn pg_text_to_json(pg_value: Option<&str>, pg_type: &Type) -> Result<Value, anyhow::Error> {
    if let Some(val) = pg_value {
        if let Kind::Array(elem_type) = pg_type.kind() {
            return pg_array_parse(val, elem_type);
        }

        match *pg_type {
            Type::BOOL => Ok(Value::Bool(val == "t")),
            Type::INT2 | Type::INT4 => {
                let val = val.parse::<i32>()?;
                Ok(Value::Number(serde_json::Number::from(val)))
            }
            Type::FLOAT4 | Type::FLOAT8 => {
                let fval = val.parse::<f64>()?;
                let num = serde_json::Number::from_f64(fval);
                if let Some(num) = num {
                    Ok(Value::Number(num))
                } else {
                    // Pass Nan, Inf, -Inf as strings
                    // JS JSON.stringify() does converts them to null, but we
                    // want to preserve them, so we pass them as strings
                    Ok(Value::String(val.to_string()))
                }
            }
            Type::JSON | Type::JSONB => Ok(serde_json::from_str(val)?),
            _ => Ok(Value::String(val.to_string())),
        }
    } else {
        Ok(Value::Null)
    }
}

//
// Parse postgres array into JSON array.
//
// This is a bit involved because we need to handle nested arrays and quoted
// values. Unlike postgres we don't check that all nested arrays have the same
// dimensions, we just return them as is.
//
fn pg_array_parse(pg_array: &str, elem_type: &Type) -> Result<Value, anyhow::Error> {
    _pg_array_parse(pg_array, elem_type, false).map(|(v, _)| v)
}

fn _pg_array_parse(
    pg_array: &str,
    elem_type: &Type,
    nested: bool,
) -> Result<(Value, usize), anyhow::Error> {
    let mut pg_array_chr = pg_array.char_indices();
    let mut level = 0;
    let mut quote = false;
    let mut entries: Vec<Value> = Vec::new();
    let mut entry = String::new();

    // skip bounds decoration
    if let Some('[') = pg_array.chars().next() {
        for (_, c) in pg_array_chr.by_ref() {
            if c == '=' {
                break;
            }
        }
    }

    fn push_checked(
        entry: &mut String,
        entries: &mut Vec<Value>,
        elem_type: &Type,
    ) -> Result<(), anyhow::Error> {
        if !entry.is_empty() {
            // While in usual postgres response we get nulls as None and everything else
            // as Some(&str), in arrays we get NULL as unquoted 'NULL' string (while
            // string with value 'NULL' will be represented by '"NULL"'). So catch NULLs
            // here while we have quotation info and convert them to None.
            if entry == "NULL" {
                entries.push(pg_text_to_json(None, elem_type)?);
            } else {
                entries.push(pg_text_to_json(Some(entry), elem_type)?);
            }
            entry.clear();
        }

        Ok(())
    }

    while let Some((mut i, mut c)) = pg_array_chr.next() {
        let mut escaped = false;

        if c == '\\' {
            escaped = true;
            (i, c) = pg_array_chr.next().unwrap();
        }

        match c {
            '{' if !quote => {
                level += 1;
                if level > 1 {
                    let (res, off) = _pg_array_parse(&pg_array[i..], elem_type, true)?;
                    entries.push(res);
                    for _ in 0..off - 1 {
                        pg_array_chr.next();
                    }
                }
            }
            '}' if !quote => {
                level -= 1;
                if level == 0 {
                    push_checked(&mut entry, &mut entries, elem_type)?;
                    if nested {
                        return Ok((Value::Array(entries), i));
                    }
                }
            }
            '"' if !escaped => {
                if quote {
                    // end of quoted string, so push it manually without any checks
                    // for emptiness or nulls
                    entries.push(pg_text_to_json(Some(&entry), elem_type)?);
                    entry.clear();
                }
                quote = !quote;
            }
            ',' if !quote => {
                push_checked(&mut entry, &mut entries, elem_type)?;
            }
            _ => {
                entry.push(c);
            }
        }
    }

    if level != 0 {
        return Err(anyhow::anyhow!("unbalanced array"));
    }

    Ok((Value::Array(entries), 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_atomic_types_to_pg_params() {
        let json = vec![Value::Bool(true), Value::Bool(false)];
        let pg_params = json_to_pg_text(json);
        assert_eq!(
            pg_params,
            vec![Some("true".to_owned()), Some("false".to_owned())]
        );

        let json = vec![Value::Number(serde_json::Number::from(42))];
        let pg_params = json_to_pg_text(json);
        assert_eq!(pg_params, vec![Some("42".to_owned())]);

        let json = vec![Value::String("foo\"".to_string())];
        let pg_params = json_to_pg_text(json);
        assert_eq!(pg_params, vec![Some("foo\"".to_owned())]);

        let json = vec![Value::Null];
        let pg_params = json_to_pg_text(json);
        assert_eq!(pg_params, vec![None]);
    }

    #[test]
    fn test_json_array_to_pg_array() {
        // atoms and escaping
        let json = "[true, false, null, \"NULL\", 42, \"foo\", \"bar\\\"-\\\\\"]";
        let json: Value = serde_json::from_str(json).unwrap();
        let pg_params = json_to_pg_text(vec![json]);
        assert_eq!(
            pg_params,
            vec![Some(
                "{true,false,NULL,\"NULL\",42,\"foo\",\"bar\\\"-\\\\\"}".to_owned()
            )]
        );

        // nested arrays
        let json = "[[true, false], [null, 42], [\"foo\", \"bar\\\"-\\\\\"]]";
        let json: Value = serde_json::from_str(json).unwrap();
        let pg_params = json_to_pg_text(vec![json]);
        assert_eq!(
            pg_params,
            vec![Some(
                "{{true,false},{NULL,42},{\"foo\",\"bar\\\"-\\\\\"}}".to_owned()
            )]
        );
        // array of objects
        let json = r#"[{"foo": 1},{"bar": 2}]"#;
        let json: Value = serde_json::from_str(json).unwrap();
        let pg_params = json_to_pg_text(vec![json]);
        assert_eq!(
            pg_params,
            vec![Some(r#"{"{\"foo\":1}","{\"bar\":2}"}"#.to_owned())]
        );
    }

    #[test]
    fn test_atomic_types_parse() {
        assert_eq!(
            pg_text_to_json(Some("foo"), &Type::TEXT).unwrap(),
            json!("foo")
        );
        assert_eq!(pg_text_to_json(None, &Type::TEXT).unwrap(), json!(null));
        assert_eq!(pg_text_to_json(Some("42"), &Type::INT4).unwrap(), json!(42));
        assert_eq!(pg_text_to_json(Some("42"), &Type::INT2).unwrap(), json!(42));
        assert_eq!(
            pg_text_to_json(Some("42"), &Type::INT8).unwrap(),
            json!("42")
        );
        assert_eq!(
            pg_text_to_json(Some("42.42"), &Type::FLOAT8).unwrap(),
            json!(42.42)
        );
        assert_eq!(
            pg_text_to_json(Some("42.42"), &Type::FLOAT4).unwrap(),
            json!(42.42)
        );
        assert_eq!(
            pg_text_to_json(Some("NaN"), &Type::FLOAT4).unwrap(),
            json!("NaN")
        );
        assert_eq!(
            pg_text_to_json(Some("Infinity"), &Type::FLOAT4).unwrap(),
            json!("Infinity")
        );
        assert_eq!(
            pg_text_to_json(Some("-Infinity"), &Type::FLOAT4).unwrap(),
            json!("-Infinity")
        );

        let json: Value =
            serde_json::from_str("{\"s\":\"str\",\"n\":42,\"f\":4.2,\"a\":[null,3,\"a\"]}")
                .unwrap();
        assert_eq!(
            pg_text_to_json(
                Some(r#"{"s":"str","n":42,"f":4.2,"a":[null,3,"a"]}"#),
                &Type::JSONB
            )
            .unwrap(),
            json
        );
    }

    #[test]
    fn test_pg_array_parse_text() {
        fn pt(pg_arr: &str) -> Value {
            pg_array_parse(pg_arr, &Type::TEXT).unwrap()
        }
        assert_eq!(
            pt(r#"{"aa\"\\\,a",cha,"bbbb"}"#),
            json!(["aa\"\\,a", "cha", "bbbb"])
        );
        assert_eq!(
            pt(r#"{{"foo","bar"},{"bee","bop"}}"#),
            json!([["foo", "bar"], ["bee", "bop"]])
        );
        assert_eq!(
            pt(r#"{{{{"foo",NULL,"bop",bup}}}}"#),
            json!([[[["foo", null, "bop", "bup"]]]])
        );
        assert_eq!(
            pt(r#"{{"1",2,3},{4,NULL,6},{NULL,NULL,NULL}}"#),
            json!([["1", "2", "3"], ["4", null, "6"], [null, null, null]])
        );
    }

    #[test]
    fn test_pg_array_parse_bool() {
        fn pb(pg_arr: &str) -> Value {
            pg_array_parse(pg_arr, &Type::BOOL).unwrap()
        }
        assert_eq!(pb(r#"{t,f,t}"#), json!([true, false, true]));
        assert_eq!(pb(r#"{{t,f,t}}"#), json!([[true, false, true]]));
        assert_eq!(
            pb(r#"{{t,f},{f,t}}"#),
            json!([[true, false], [false, true]])
        );
        assert_eq!(
            pb(r#"{{t,NULL},{NULL,f}}"#),
            json!([[true, null], [null, false]])
        );
    }

    #[test]
    fn test_pg_array_parse_numbers() {
        fn pn(pg_arr: &str, ty: &Type) -> Value {
            pg_array_parse(pg_arr, ty).unwrap()
        }
        assert_eq!(pn(r#"{1,2,3}"#, &Type::INT4), json!([1, 2, 3]));
        assert_eq!(pn(r#"{1,2,3}"#, &Type::INT2), json!([1, 2, 3]));
        assert_eq!(pn(r#"{1,2,3}"#, &Type::INT8), json!(["1", "2", "3"]));
        assert_eq!(pn(r#"{1,2,3}"#, &Type::FLOAT4), json!([1.0, 2.0, 3.0]));
        assert_eq!(pn(r#"{1,2,3}"#, &Type::FLOAT8), json!([1.0, 2.0, 3.0]));
        assert_eq!(
            pn(r#"{1.1,2.2,3.3}"#, &Type::FLOAT4),
            json!([1.1, 2.2, 3.3])
        );
        assert_eq!(
            pn(r#"{1.1,2.2,3.3}"#, &Type::FLOAT8),
            json!([1.1, 2.2, 3.3])
        );
        assert_eq!(
            pn(r#"{NaN,Infinity,-Infinity}"#, &Type::FLOAT4),
            json!(["NaN", "Infinity", "-Infinity"])
        );
        assert_eq!(
            pn(r#"{NaN,Infinity,-Infinity}"#, &Type::FLOAT8),
            json!(["NaN", "Infinity", "-Infinity"])
        );
    }

    #[test]
    fn test_pg_array_with_decoration() {
        fn p(pg_arr: &str) -> Value {
            pg_array_parse(pg_arr, &Type::INT2).unwrap()
        }
        assert_eq!(
            p(r#"[1:1][-2:-1][3:5]={{{1,2,3},{4,5,6}}}"#),
            json!([[[1, 2, 3], [4, 5, 6]]])
        );
    }
    #[test]
    fn test_pg_array_parse_json() {
        fn pt(pg_arr: &str) -> Value {
            pg_array_parse(pg_arr, &Type::JSONB).unwrap()
        }
        assert_eq!(pt(r#"{"{}"}"#), json!([{}]));
        assert_eq!(
            pt(r#"{"{\"foo\": 1, \"bar\": 2}"}"#),
            json!([{"foo": 1, "bar": 2}])
        );
        assert_eq!(
            pt(r#"{"{\"foo\": 1}", "{\"bar\": 2}"}"#),
            json!([{"foo": 1}, {"bar": 2}])
        );
        assert_eq!(
            pt(r#"{{"{\"foo\": 1}", "{\"bar\": 2}"}}"#),
            json!([[{"foo": 1}, {"bar": 2}]])
        );
    }
}
