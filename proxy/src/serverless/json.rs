use json::{ListSer, ObjectSer, RawValue, ValueSer};
use postgres_client::Row;
use postgres_client::types::{Kind, Type};
use serde_json::Value;

//
// Convert json non-string types to strings, so that they can be passed to Postgres
// as parameters.
//
pub(crate) fn json_to_pg_text(json: Vec<Value>) -> Vec<Option<String>> {
    json.iter().map(json_value_to_pg_text).collect()
}

fn json_value_to_pg_text(value: &Value) -> Option<String> {
    match value {
        // special care for nulls
        Value::Null => None,

        // convert to text with escaping
        v @ (Value::Bool(_) | Value::Number(_) | Value::Object(_)) => Some(v.to_string()),

        // avoid escaping here, as we pass this as a parameter
        Value::String(s) => Some(s.clone()),

        // special care for arrays
        Value::Array(_) => json_array_to_pg_array(value),
    }
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

            Some(format!("{{{vals}}}"))
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum JsonConversionError {
    #[error("internal error compute returned invalid data: {0}")]
    AsTextError(postgres_client::Error),
    #[error("parse int error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
    #[error("parse json error: {0}")]
    ParseJsonError(#[from] serde_json::Error),
    #[error("unbalanced array")]
    UnbalancedArray,
    #[error("unbalanced quoted string")]
    UnbalancedString,
}

enum OutputMode<'a> {
    Array(ListSer<'a>),
    Object(ObjectSer<'a>),
}

impl OutputMode<'_> {
    fn key(&mut self, key: &str) -> ValueSer<'_> {
        match self {
            OutputMode::Array(values) => values.entry(),
            OutputMode::Object(map) => map.key(key),
        }
    }

    fn finish(self) {
        match self {
            OutputMode::Array(values) => values.finish(),
            OutputMode::Object(map) => map.finish(),
        }
    }
}

//
// Convert postgres row with text-encoded values to JSON object
//
pub(crate) fn pg_text_row_to_json(
    output: ValueSer,
    row: &Row,
    raw_output: bool,
    array_mode: bool,
) -> Result<(), JsonConversionError> {
    let mut entries = if array_mode {
        OutputMode::Array(output.list())
    } else {
        OutputMode::Object(output.object())
    };

    for (i, column) in row.columns().iter().enumerate() {
        let pg_value = row.as_text(i).map_err(JsonConversionError::AsTextError)?;

        let value = entries.key(column.name());

        match pg_value {
            Some(v) if raw_output => value.value(v),
            Some(v) => pg_text_to_json(value, v, column.type_())?,
            None => value.value(json::RawValue::NULL),
        }
    }

    entries.finish();
    Ok(())
}

//
// Convert postgres text-encoded value to JSON value
//
fn pg_text_to_json(output: ValueSer, val: &str, pg_type: &Type) -> Result<(), JsonConversionError> {
    if let Kind::Array(elem_type) = pg_type.kind() {
        // todo: we should fetch this from postgres.
        let delimiter = ',';

        json::value_as_list!(|output| pg_array_parse(output, val, elem_type, delimiter)?);
        return Ok(());
    }

    match *pg_type {
        Type::BOOL => output.value(val == "t"),
        Type::INT2 | Type::INT4 => {
            let val = val.parse::<i32>()?;
            output.value(val);
        }
        Type::FLOAT4 | Type::FLOAT8 => {
            let fval = val.parse::<f64>()?;
            if fval.is_finite() {
                output.value(fval);
            } else {
                // Pass Nan, Inf, -Inf as strings
                // JS JSON.stringify() does converts them to null, but we
                // want to preserve them, so we pass them as strings
                output.value(val);
            }
        }
        // we assume that the string value is valid json.
        Type::JSON | Type::JSONB => output.value(RawValue::new_unchecked(val.as_bytes())),
        _ => output.value(val),
    }

    Ok(())
}

/// Parse postgres array into JSON array.
///
/// This is a bit involved because we need to handle nested arrays and quoted
/// values. Unlike postgres we don't check that all nested arrays have the same
/// dimensions, we just return them as is.
///
/// <https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO>
///
/// The external text representation of an array value consists of items that are interpreted
/// according to the I/O conversion rules for the array's element type, plus decoration that
/// indicates the array structure. The decoration consists of curly braces (`{` and `}`) around
/// the array value plus delimiter characters between adjacent items. The delimiter character
/// is usually a comma (,) but can be something else: it is determined by the typdelim setting
/// for the array's element type. Among the standard data types provided in the PostgreSQL
/// distribution, all use a comma, except for type box, which uses a semicolon (;).
///
/// In a multidimensional array, each dimension (row, plane, cube, etc.)
/// gets its own level of curly braces, and delimiters must be written between adjacent
/// curly-braced entities of the same level.
fn pg_array_parse(
    elements: &mut ListSer,
    mut pg_array: &str,
    elem: &Type,
    delim: char,
) -> Result<(), JsonConversionError> {
    // skip bounds decoration, eg:
    // `[1:1][-2:-1][3:5]={{{1,2,3},{4,5,6}}}`
    // technically these are significant, but we have no way to represent them in json.
    if let Some('[') = pg_array.chars().next() {
        let Some((_bounds, array)) = pg_array.split_once('=') else {
            return Err(JsonConversionError::UnbalancedArray);
        };
        pg_array = array;
    }

    // whitespace might preceed a `{`.
    let pg_array = pg_array.trim_start();

    let rest = pg_array_parse_inner(elements, pg_array, elem, delim)?;
    if !rest.is_empty() {
        return Err(JsonConversionError::UnbalancedArray);
    }

    Ok(())
}

/// reads a single array from the `pg_array` string and pushes each values to `elements`.
/// returns the rest of the `pg_array` string that was not read.
fn pg_array_parse_inner<'a>(
    elements: &mut ListSer,
    mut pg_array: &'a str,
    elem: &Type,
    delim: char,
) -> Result<&'a str, JsonConversionError> {
    // array should have a `{` prefix.
    pg_array = pg_array
        .strip_prefix('{')
        .ok_or(JsonConversionError::UnbalancedArray)?;

    let mut q = String::new();

    loop {
        let value = elements.entry();
        pg_array = pg_array_parse_item(value, &mut q, pg_array, elem, delim)?;

        // check for separator.
        if let Some(next) = pg_array.strip_prefix(delim) {
            // next item.
            pg_array = next;
        } else {
            break;
        }
    }

    let Some(next) = pg_array.strip_prefix('}') else {
        // missing `}` terminator.
        return Err(JsonConversionError::UnbalancedArray);
    };

    // whitespace might follow a `}`.
    Ok(next.trim_start())
}

/// reads a single item from the `pg_array` string.
/// returns the rest of the `pg_array` string that was not read.
///
/// `quoted` is a scratch allocation that has no defined output.
fn pg_array_parse_item<'a>(
    output: ValueSer,
    quoted: &mut String,
    mut pg_array: &'a str,
    elem: &Type,
    delim: char,
) -> Result<&'a str, JsonConversionError> {
    // We are trying to parse an array item.
    // This could be a new array, if this is a multi-dimentional array.
    // This could be a quoted string representing `elem`.
    // This could be an unquoted string representing `elem`.

    // whitespace might preceed an item.
    pg_array = pg_array.trim_start();

    if pg_array.strip_prefix('{').is_some() {
        // nested array.
        pg_array =
            json::value_as_list!(|output| pg_array_parse_inner(output, pg_array, elem, delim))?;
        return Ok(pg_array);
    }

    if let Some(mut pg_array) = pg_array.strip_prefix('"') {
        pg_array = pg_array_parse_quoted(quoted, pg_array)?;

        // we have unquoted an item string:
        pg_text_to_json(output, quoted, elem)?;

        quoted.clear();

        return Ok(pg_array);
    }

    // we need to parse an item. read until we find a delimiter or `}`.
    let index = pg_array
        .find([delim, '}'])
        .ok_or(JsonConversionError::UnbalancedArray)?;

    let item;
    (item, pg_array) = pg_array.split_at(index);

    // item might have trailing whitespace that we need to ignore.
    let item = item.trim_end();

    // we might have an item string:
    // check for null
    if item == "NULL" {
        output.value(RawValue::NULL);
    } else {
        pg_text_to_json(output, item, elem)?;
    }

    Ok(pg_array)
}

/// reads a single quoted item from the `pg_array` string.
///
/// Returns the rest of the `pg_array` string that was not read.
/// The output is written into `quoted`.
///
/// The pg_array string must have a `"` terminator, but the `"` initial value
/// must have already been removed from the input. The terminator is removed.
fn pg_array_parse_quoted<'a>(
    quoted: &mut String,
    mut pg_array: &'a str,
) -> Result<&'a str, JsonConversionError> {
    // The array output routine will put double quotes around element values if they are empty strings,
    // contain curly braces, delimiter characters, double quotes, backslashes, or white space,
    // or match the word `NULL`. Double quotes and backslashes embedded in element values will be backslash-escaped.
    // For numeric data types it is safe to assume that double quotes will never appear,
    // but for textual data types one should be prepared to cope with either the presence or absence of quotes.

    // We write to quoted in chunks terminated by an escape character.
    // Eg if we have the input `foo\"bar"`, then we write `foo`, then `"`, then finally `bar`.

    loop {
        // we need to parse an chunk. read until we find a '\\' or `"`.
        let i = pg_array
            .find(['\\', '"'])
            .ok_or(JsonConversionError::UnbalancedString)?;

        let chunk: &str;
        (chunk, pg_array) = pg_array
            .split_at_checked(i)
            .expect("i is guaranteed to be in-bounds of pg_array");

        // push the chunk.
        quoted.push_str(chunk);

        // consume the chunk_end character.
        let chunk_end: char;
        (chunk_end, pg_array) =
            split_first_char(pg_array).expect("pg_array should start with either '\\\\' or '\"'");

        // finished.
        if chunk_end == '"' {
            // whitespace might follow the '"'.
            pg_array = pg_array.trim_start();

            break Ok(pg_array);
        }

        // consume the escaped character.
        let escaped: char;
        (escaped, pg_array) =
            split_first_char(pg_array).ok_or(JsonConversionError::UnbalancedString)?;

        quoted.push(escaped);
    }
}

fn split_first_char(s: &str) -> Option<(char, &str)> {
    let mut chars = s.chars();
    let c = chars.next()?;
    Some((c, chars.as_str()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

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

    fn pg_text_to_json(val: &str, pg_type: &Type) -> Value {
        let output = json::value_to_string!(|v| super::pg_text_to_json(v, val, pg_type).unwrap());
        serde_json::from_str(&output).unwrap()
    }

    fn pg_array_parse(pg_array: &str, pg_type: &Type) -> Value {
        let output = json::value_to_string!(|v| json::value_as_list!(|v| {
            super::pg_array_parse(v, pg_array, pg_type, ',').unwrap();
        }));
        serde_json::from_str(&output).unwrap()
    }

    #[test]
    fn test_atomic_types_parse() {
        assert_eq!(pg_text_to_json("foo", &Type::TEXT), json!("foo"));
        assert_eq!(pg_text_to_json("42", &Type::INT4), json!(42));
        assert_eq!(pg_text_to_json("42", &Type::INT2), json!(42));
        assert_eq!(pg_text_to_json("42", &Type::INT8), json!("42"));
        assert_eq!(pg_text_to_json("42.42", &Type::FLOAT8), json!(42.42));
        assert_eq!(pg_text_to_json("42.42", &Type::FLOAT4), json!(42.42));
        assert_eq!(pg_text_to_json("NaN", &Type::FLOAT4), json!("NaN"));
        assert_eq!(
            pg_text_to_json("Infinity", &Type::FLOAT4),
            json!("Infinity")
        );
        assert_eq!(
            pg_text_to_json("-Infinity", &Type::FLOAT4),
            json!("-Infinity")
        );

        let json: Value =
            serde_json::from_str("{\"s\":\"str\",\"n\":42,\"f\":4.2,\"a\":[null,3,\"a\"]}")
                .unwrap();
        assert_eq!(
            pg_text_to_json(
                r#"{"s":"str","n":42,"f":4.2,"a":[null,3,"a"]}"#,
                &Type::JSONB
            ),
            json
        );
    }

    #[test]
    fn test_pg_array_parse_text() {
        fn pt(pg_arr: &str) -> Value {
            pg_array_parse(pg_arr, &Type::TEXT)
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
            pg_array_parse(pg_arr, &Type::BOOL)
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
            pg_array_parse(pg_arr, ty)
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
            pg_array_parse(pg_arr, &Type::INT2)
        }
        assert_eq!(
            p(r#"[1:1][-2:-1][3:5]={{{1,2,3},{4,5,6}}}"#),
            json!([[[1, 2, 3], [4, 5, 6]]])
        );
    }

    #[test]
    fn test_pg_array_parse_json() {
        fn pt(pg_arr: &str) -> Value {
            pg_array_parse(pg_arr, &Type::JSONB)
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
