use std::sync::Arc;

use anyhow::bail;
use futures::pin_mut;
use futures::StreamExt;
use hashbrown::HashMap;
use hyper::body::HttpBody;
use hyper::http::HeaderName;
use hyper::http::HeaderValue;
use hyper::{Body, HeaderMap, Request};
use serde_json::json;
use serde_json::Map;
use serde_json::Value;
use tokio_postgres::types::Kind;
use tokio_postgres::types::Type;
use tokio_postgres::GenericClient;
use tokio_postgres::IsolationLevel;
use tokio_postgres::Row;
use url::Url;

use super::conn_pool::ConnInfo;
use super::conn_pool::GlobalConnPool;

#[derive(serde::Deserialize)]
struct QueryData {
    query: String,
    params: Vec<serde_json::Value>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum Payload {
    Single(QueryData),
    Batch(Vec<QueryData>),
}

pub const MAX_RESPONSE_SIZE: usize = 1024 * 1024; // 1 MB
const MAX_REQUEST_SIZE: u64 = 1024 * 1024; // 1 MB

static RAW_TEXT_OUTPUT: HeaderName = HeaderName::from_static("neon-raw-text-output");
static ARRAY_MODE: HeaderName = HeaderName::from_static("neon-array-mode");
static ALLOW_POOL: HeaderName = HeaderName::from_static("neon-pool-opt-in");
static TXN_ISOLATION_LEVEL: HeaderName = HeaderName::from_static("neon-batch-isolation-level");
static TXN_READ_ONLY: HeaderName = HeaderName::from_static("neon-batch-read-only");

static HEADER_VALUE_TRUE: HeaderValue = HeaderValue::from_static("true");

//
// Convert json non-string types to strings, so that they can be passed to Postgres
// as parameters.
//
fn json_to_pg_text(json: Vec<Value>) -> Result<Vec<Option<String>>, serde_json::Error> {
    json.iter()
        .map(|value| {
            match value {
                // special care for nulls
                Value::Null => Ok(None),

                // convert to text with escaping
                Value::Bool(_) => serde_json::to_string(value).map(Some),
                Value::Number(_) => serde_json::to_string(value).map(Some),
                Value::Object(_) => serde_json::to_string(value).map(Some),

                // avoid escaping here, as we pass this as a parameter
                Value::String(s) => Ok(Some(s.to_string())),

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
fn json_array_to_pg_array(value: &Value) -> Result<Option<String>, serde_json::Error> {
    match value {
        // special care for nulls
        Value::Null => Ok(None),

        // convert to text with escaping
        Value::Bool(_) => serde_json::to_string(value).map(Some),
        Value::Number(_) => serde_json::to_string(value).map(Some),
        Value::Object(_) => serde_json::to_string(value).map(Some),

        // here string needs to be escaped, as it is part of the array
        Value::String(_) => serde_json::to_string(value).map(Some),

        // recurse into array
        Value::Array(arr) => {
            let vals = arr
                .iter()
                .map(json_array_to_pg_array)
                .map(|r| r.map(|v| v.unwrap_or_else(|| "NULL".to_string())))
                .collect::<Result<Vec<_>, _>>()?
                .join(",");

            Ok(Some(format!("{{{}}}", vals)))
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

    Ok(ConnInfo {
        username: username.to_owned(),
        dbname: dbname.to_owned(),
        hostname: hostname.to_owned(),
        password: password.to_owned(),
    })
}

// TODO: return different http error codes
pub async fn handle(
    request: Request<Body>,
    sni_hostname: Option<String>,
    conn_pool: Arc<GlobalConnPool>,
) -> anyhow::Result<(Value, HashMap<HeaderName, HeaderValue>)> {
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
    let allow_pool = headers.get(&ALLOW_POOL) == Some(&HEADER_VALUE_TRUE);

    // isolation level and read only

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

    let txn_read_only_raw = headers.get(&TXN_READ_ONLY).cloned();
    let txn_read_only = txn_read_only_raw.as_ref() == Some(&HEADER_VALUE_TRUE);

    let request_content_length = match request.body().size_hint().upper() {
        Some(v) => v,
        None => MAX_REQUEST_SIZE + 1,
    };

    if request_content_length > MAX_REQUEST_SIZE {
        return Err(anyhow::anyhow!(
            "request is too large (max {MAX_REQUEST_SIZE} bytes)"
        ));
    }

    //
    // Read the query and query params from the request body
    //
    let body = hyper::body::to_bytes(request.into_body()).await?;
    let payload: Payload = serde_json::from_slice(&body)?;

    let mut client = conn_pool.get(&conn_info, !allow_pool).await?;

    //
    // Now execute the query and return the result
    //
    let result = match payload {
        Payload::Single(query) => query_to_json(&client, query, raw_output, array_mode)
            .await
            .map(|x| (x, HashMap::default())),
        Payload::Batch(queries) => {
            let mut results = Vec::new();
            let mut builder = client.build_transaction();
            if let Some(isolation_level) = txn_isolation_level {
                builder = builder.isolation_level(isolation_level);
            }
            if txn_read_only {
                builder = builder.read_only(true);
            }
            let transaction = builder.start().await?;
            for query in queries {
                let result = query_to_json(&transaction, query, raw_output, array_mode).await;
                match result {
                    Ok(r) => results.push(r),
                    Err(e) => {
                        transaction.rollback().await?;
                        return Err(e);
                    }
                }
            }
            transaction.commit().await?;
            let mut headers = HashMap::default();
            headers.insert(
                TXN_READ_ONLY.clone(),
                HeaderValue::try_from(txn_read_only.to_string())?,
            );
            if let Some(txn_isolation_level_raw) = txn_isolation_level_raw {
                headers.insert(TXN_ISOLATION_LEVEL.clone(), txn_isolation_level_raw);
            }
            Ok((json!({ "results": results }), headers))
        }
    };

    if allow_pool {
        // return connection to the pool
        tokio::task::spawn(async move {
            let _ = conn_pool.put(&conn_info, client).await;
        });
    }

    result
}

async fn query_to_json<T: GenericClient>(
    client: &T,
    data: QueryData,
    raw_output: bool,
    array_mode: bool,
) -> anyhow::Result<Value> {
    let query_params = json_to_pg_text(data.params)?;
    let row_stream = client
        .query_raw_txt::<String, _>(data.query, query_params)
        .await?;

    // Manually drain the stream into a vector to leave row_stream hanging
    // around to get a command tag. Also check that the response is not too
    // big.
    pin_mut!(row_stream);
    let mut rows: Vec<tokio_postgres::Row> = Vec::new();
    let mut curret_size = 0;
    while let Some(row) = row_stream.next().await {
        let row = row?;
        curret_size += row.body_len();
        rows.push(row);
        if curret_size > MAX_RESPONSE_SIZE {
            return Err(anyhow::anyhow!("response too large"));
        }
    }

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

    let fields = if !rows.is_empty() {
        rows[0]
            .columns()
            .iter()
            .map(|c| {
                json!({
                    "name": Value::String(c.name().to_owned()),
                    "dataTypeID": Value::Number(c.type_().oid().into()),
                    "tableID": c.table_oid(),
                    "columnID": c.column_id(),
                    "dataTypeSize": c.type_size(),
                    "dataTypeModifier": c.type_modifier(),
                    "format": "text",
                })
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    // convert rows to JSON
    let rows = rows
        .iter()
        .map(|row| pg_text_row_to_json(row, raw_output, array_mode))
        .collect::<Result<Vec<_>, _>>()?;

    // resulting JSON format is based on the format of node-postgres result
    Ok(json!({
        "command": command_tag_name,
        "rowCount": command_tag_count,
        "rows": rows,
        "fields": fields,
        "rowAsArray": array_mode,
    }))
}

//
// Convert postgres row with text-encoded values to JSON object
//
pub fn pg_text_row_to_json(
    row: &Row,
    raw_output: bool,
    array_mode: bool,
) -> Result<Value, anyhow::Error> {
    let iter = row.columns().iter().enumerate().map(|(i, column)| {
        let name = column.name();
        let pg_value = row.as_text(i)?;
        let json_value = if raw_output {
            match pg_value {
                Some(v) => Value::String(v.to_string()),
                None => Value::Null,
            }
        } else {
            pg_text_to_json(pg_value, column.type_())?
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
            '}' => {
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
        let pg_params = json_to_pg_text(json).unwrap();
        assert_eq!(
            pg_params,
            vec![Some("true".to_owned()), Some("false".to_owned())]
        );

        let json = vec![Value::Number(serde_json::Number::from(42))];
        let pg_params = json_to_pg_text(json).unwrap();
        assert_eq!(pg_params, vec![Some("42".to_owned())]);

        let json = vec![Value::String("foo\"".to_string())];
        let pg_params = json_to_pg_text(json).unwrap();
        assert_eq!(pg_params, vec![Some("foo\"".to_owned())]);

        let json = vec![Value::Null];
        let pg_params = json_to_pg_text(json).unwrap();
        assert_eq!(pg_params, vec![None]);
    }

    #[test]
    fn test_json_array_to_pg_array() {
        // atoms and escaping
        let json = "[true, false, null, \"NULL\", 42, \"foo\", \"bar\\\"-\\\\\"]";
        let json: Value = serde_json::from_str(json).unwrap();
        let pg_params = json_to_pg_text(vec![json]).unwrap();
        assert_eq!(
            pg_params,
            vec![Some(
                "{true,false,NULL,\"NULL\",42,\"foo\",\"bar\\\"-\\\\\"}".to_owned()
            )]
        );

        // nested arrays
        let json = "[[true, false], [null, 42], [\"foo\", \"bar\\\"-\\\\\"]]";
        let json: Value = serde_json::from_str(json).unwrap();
        let pg_params = json_to_pg_text(vec![json]).unwrap();
        assert_eq!(
            pg_params,
            vec![Some(
                "{{true,false},{NULL,42},{\"foo\",\"bar\\\"-\\\\\"}}".to_owned()
            )]
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
}
