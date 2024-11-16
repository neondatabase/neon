use core::fmt;
use std::{borrow::Cow, str::FromStr};

use super::error::ApiError;
use anyhow::anyhow;
use hyper::{body::HttpBody, Body, Request};
use routerify::ext::RequestExt;

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
    let mut values = url::form_urlencoded::parse(query.as_bytes())
        .filter_map(|(k, v)| if k == param_name { Some(v) } else { None })
        // we call .next() twice below. If it's None the first time, .fuse() ensures it's None afterwards
        .fuse();

    let value1 = values.next();
    if values.next().is_some() {
        return Err(ApiError::BadRequest(anyhow!(
            "param {param_name} specified more than once"
        )));
    }
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
