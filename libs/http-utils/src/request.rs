use core::fmt;
use std::borrow::Cow;
use std::str::FromStr;

use anyhow::anyhow;
use hyper::body::HttpBody;
use hyper::{Body, Request};
use routerify::ext::RequestExt;

use super::error::ApiError;

pub fn get_request_param<'a>(
    request: &'a Request<Body>,
    param_name: &str,
) -> Result<&'a str, ApiError> {
    match request.param(param_name) {
        Some(arg) => Ok(arg),
        None => Err(ApiError::BadRequest(anyhow!(
            "no {param_name} specified in path param",
        ))),
    }
}

pub fn parse_request_param<T: FromStr>(
    request: &Request<Body>,
    param_name: &str,
) -> Result<T, ApiError> {
    match get_request_param(request, param_name)?.parse() {
        Ok(v) => Ok(v),
        Err(_) => Err(ApiError::BadRequest(anyhow!(
            "failed to parse {param_name}",
        ))),
    }
}

pub fn get_query_param<'a>(
    request: &'a Request<Body>,
    param_name: &str,
) -> Result<Option<Cow<'a, str>>, ApiError> {
    let query = match request.uri().query() {
        Some(q) => q,
        None => return Ok(None),
    };
    let values = url::form_urlencoded::parse(query.as_bytes())
        .filter_map(|(k, v)| if k == param_name { Some(v) } else { None })
        // we call .next() twice below. If it's None the first time, .fuse() ensures it's None afterwards
        .fuse();

    // Work around an issue with Alloy's pyroscope scrape where the "seconds"
    // parameter is added several times. https://github.com/grafana/alloy/issues/3026
    // TODO: revert after Alloy is fixed.
    let value1 = values
        .map(Ok)
        .reduce(|acc, i| {
            match acc {
                Err(_) => acc,

                // It's okay to have duplicates as along as they have the same value.
                Ok(ref a) if a == &i.unwrap() => acc,

                _ => Err(ApiError::BadRequest(anyhow!(
                    "param {param_name} specified more than once"
                ))),
            }
        })
        .transpose()?;
    // if values.next().is_some() {
    //     return Err(ApiError::BadRequest(anyhow!(
    //         "param {param_name} specified more than once"
    //     )));
    // }

    Ok(value1)
}

pub fn must_get_query_param<'a>(
    request: &'a Request<Body>,
    param_name: &str,
) -> Result<Cow<'a, str>, ApiError> {
    get_query_param(request, param_name)?.ok_or_else(|| {
        ApiError::BadRequest(anyhow!("no {param_name} specified in query parameters"))
    })
}

pub fn parse_query_param<E: fmt::Display, T: FromStr<Err = E>>(
    request: &Request<Body>,
    param_name: &str,
) -> Result<Option<T>, ApiError> {
    get_query_param(request, param_name)?
        .map(|v| {
            v.parse().map_err(|e| {
                ApiError::BadRequest(anyhow!("cannot parse query param {param_name}: {e}"))
            })
        })
        .transpose()
}

pub fn must_parse_query_param<E: fmt::Display, T: FromStr<Err = E>>(
    request: &Request<Body>,
    param_name: &str,
) -> Result<T, ApiError> {
    parse_query_param(request, param_name)?.ok_or_else(|| {
        ApiError::BadRequest(anyhow!("no {param_name} specified in query parameters"))
    })
}

pub async fn ensure_no_body(request: &mut Request<Body>) -> Result<(), ApiError> {
    match request.body_mut().data().await {
        Some(_) => Err(ApiError::BadRequest(anyhow!("Unexpected request body"))),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_query_param_duplicate() {
        let req = Request::builder()
            .uri("http://localhost:12345/testuri?testparam=1")
            .body(hyper::Body::empty())
            .unwrap();
        let value = get_query_param(&req, "testparam").unwrap();
        assert_eq!(value.unwrap(), "1");

        let req = Request::builder()
            .uri("http://localhost:12345/testuri?testparam=1&testparam=1")
            .body(hyper::Body::empty())
            .unwrap();
        let value = get_query_param(&req, "testparam").unwrap();
        assert_eq!(value.unwrap(), "1");

        let req = Request::builder()
            .uri("http://localhost:12345/testuri")
            .body(hyper::Body::empty())
            .unwrap();
        let value = get_query_param(&req, "testparam").unwrap();
        assert!(value.is_none());

        let req = Request::builder()
            .uri("http://localhost:12345/testuri?testparam=1&testparam=2&testparam=3")
            .body(hyper::Body::empty())
            .unwrap();
        let value = get_query_param(&req, "testparam");
        assert!(value.is_err());
    }
}
