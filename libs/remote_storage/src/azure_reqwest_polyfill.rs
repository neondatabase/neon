use std::{collections::HashMap, str::FromStr};

use azure_core::{
    Body, HttpClient, Method, Request, Response, StatusCode,
    error::{Error, ErrorKind, ResultExt},
    headers::{HeaderName, HeaderValue, Headers},
};
use futures::TryStreamExt;
use tracing::{debug, warn};

#[derive(Debug)]
pub struct Client(pub reqwest::Client);

#[async_trait::async_trait]
impl HttpClient for Client {
    async fn execute_request(&self, request: &Request) -> azure_core::Result<Response> {
        let url = request.url().clone();
        let method = request.method();
        let mut req = self.0.request(try_from_method(*method)?, url.clone());

        for (name, value) in request.headers().iter() {
            req = req.header(name.as_str(), value.as_str());
        }

        let req = match request.body().clone() {
            Body::Bytes(bytes) => req.body(bytes),
            Body::SeekableStream(seekable_stream) => {
                req.body(reqwest::Body::wrap_stream(seekable_stream))
            }
        };

        let reqwest_request = req
            .build()
            .context(ErrorKind::Other, "failed to build `reqwest` request")?;

        debug!("performing request {method} '{url}' with `reqwest`");
        let rsp = self
            .0
            .execute(reqwest_request)
            .await
            .context(ErrorKind::Io, "failed to execute `reqwest` request")?;

        let status = rsp.status();
        let headers = to_headers(rsp.headers());

        let body = Box::pin(
            rsp.bytes_stream()
                .map_err(|error| Error::new(ErrorKind::Io, error)),
        );

        Ok(Response::new(try_from_status(status)?, headers, body))
    }
}

fn to_headers(map: &reqwest::header::HeaderMap) -> Headers {
    let map = map
        .iter()
        .filter_map(|(k, v)| {
            let key = k.as_str();
            if let Ok(value) = v.to_str() {
                Some((
                    HeaderName::from(key.to_owned()),
                    HeaderValue::from(value.to_owned()),
                ))
            } else {
                warn!("header value for `{key}` is not utf8");
                None
            }
        })
        .collect::<HashMap<_, _>>();
    Headers::from(map)
}

fn try_from_method(method: Method) -> azure_core::Result<reqwest::Method> {
    match method {
        Method::Connect => Ok(reqwest::Method::CONNECT),
        Method::Delete => Ok(reqwest::Method::DELETE),
        Method::Get => Ok(reqwest::Method::GET),
        Method::Head => Ok(reqwest::Method::HEAD),
        Method::Options => Ok(reqwest::Method::OPTIONS),
        Method::Patch => Ok(reqwest::Method::PATCH),
        Method::Post => Ok(reqwest::Method::POST),
        Method::Put => Ok(reqwest::Method::PUT),
        Method::Trace => Ok(reqwest::Method::TRACE),
        _ => reqwest::Method::from_str(method.as_ref()).map_kind(ErrorKind::DataConversion),
    }
}

fn try_from_status(status: reqwest::StatusCode) -> azure_core::Result<StatusCode> {
    let status = u16::from(status);
    StatusCode::try_from(status).map_err(|_| {
        Error::with_message(ErrorKind::DataConversion, || {
            format!("invalid status code {status}")
        })
    })
}
