//! HTTP client and server impls.
//! Other modules should use stuff from this module instead of
//! directly relying on deps like `reqwest` (think loose coupling).

pub mod health_server;

use std::time::Duration;

use bytes::Bytes;
use http::Method;
use http_body_util::BodyExt;
use hyper::body::Body;
pub(crate) use reqwest::{Request, Response};
use reqwest_middleware::RequestBuilder;
pub(crate) use reqwest_middleware::{ClientWithMiddleware, Error};
pub(crate) use reqwest_retry::policies::ExponentialBackoff;
pub(crate) use reqwest_retry::RetryTransientMiddleware;
use thiserror::Error;

use crate::metrics::{ConsoleRequest, Metrics};
use crate::url::ApiUrl;

/// This is the preferred way to create new http clients,
/// because it takes care of observability (OpenTelemetry).
/// We deliberately don't want to replace this with a public static.
pub fn new_client() -> ClientWithMiddleware {
    let client = reqwest::ClientBuilder::new()
        .build()
        .expect("Failed to create http client");

    reqwest_middleware::ClientBuilder::new(client)
        .with(reqwest_tracing::TracingMiddleware::default())
        .build()
}

pub(crate) fn new_client_with_timeout(
    request_timeout: Duration,
    total_retry_duration: Duration,
) -> ClientWithMiddleware {
    let timeout_client = reqwest::ClientBuilder::new()
        .timeout(request_timeout)
        .build()
        .expect("Failed to create http client with timeout");

    let retry_policy =
        ExponentialBackoff::builder().build_with_total_retry_duration(total_retry_duration);

    reqwest_middleware::ClientBuilder::new(timeout_client)
        .with(reqwest_tracing::TracingMiddleware::default())
        // As per docs, "This middleware always errors when given requests with streaming bodies".
        // That's all right because we only use this client to send `serde_json::RawValue`, which
        // is not a stream.
        //
        // ex-maintainer note:
        // this limitation can be fixed if streaming is necessary.
        // retries will still not be performed, but it wont error immediately
        .with(RetryTransientMiddleware::new_with_policy(retry_policy))
        .build()
}

/// Thin convenience wrapper for an API provided by an http endpoint.
#[derive(Debug, Clone)]
pub struct Endpoint {
    /// API's base URL.
    endpoint: ApiUrl,
    /// Connection manager with built-in pooling.
    client: ClientWithMiddleware,
}

impl Endpoint {
    /// Construct a new HTTP endpoint wrapper.
    /// Http client is not constructed under the hood so that it can be shared.
    pub fn new(endpoint: ApiUrl, client: impl Into<ClientWithMiddleware>) -> Self {
        Self {
            endpoint,
            client: client.into(),
        }
    }

    #[inline(always)]
    pub(crate) fn url(&self) -> &ApiUrl {
        &self.endpoint
    }

    /// Return a [builder](RequestBuilder) for a `GET` request,
    /// appending a single `path` segment to the base endpoint URL.
    pub(crate) fn get_path(&self, path: &str) -> RequestBuilder {
        self.get_with_url(|u| {
            u.path_segments_mut().push(path);
        })
    }

    /// Return a [builder](RequestBuilder) for a `GET` request,
    /// accepting a closure to modify the url path segments for more complex paths queries.
    pub(crate) fn get_with_url(&self, f: impl for<'a> FnOnce(&'a mut ApiUrl)) -> RequestBuilder {
        self.request_with_url(Method::GET, f)
    }

    /// Return a [builder](RequestBuilder) for a request,
    /// accepting a closure to modify the url path segments for more complex paths queries.
    pub(crate) fn request_with_url(
        &self,
        method: Method,
        f: impl for<'a> FnOnce(&'a mut ApiUrl),
    ) -> RequestBuilder {
        let mut url = self.endpoint.clone();
        f(&mut url);
        self.client.request(method, url.into_inner())
    }

    /// Execute a [request](reqwest::Request).
    pub(crate) async fn execute(&self, request: Request) -> Result<Response, Error> {
        let _timer = Metrics::get()
            .proxy
            .console_request_latency
            .start_timer(ConsoleRequest {
                request: request.url().path(),
            });

        self.client.execute(request).await
    }
}

#[derive(Error, Debug)]
pub(crate) enum ReadBodyError<E> {
    #[error("Content length exceeds limit of {limit} bytes")]
    BodyTooLarge { limit: usize },

    #[error(transparent)]
    Read(#[from] E),
}

pub(crate) async fn read_body_with_limit<E>(
    mut b: impl Body<Data = Bytes, Error = E> + Unpin,
    limit: usize,
) -> Result<Vec<u8>, ReadBodyError<E>> {
    // We could use `b.limited().collect().await.to_bytes()` here
    // but this ends up being slightly more efficient as far as I can tell.

    // check the lower bound of the size hint.
    // in reqwest, this value is influenced by the Content-Length header.
    let lower_bound = match usize::try_from(b.size_hint().lower()) {
        Ok(bound) if bound <= limit => bound,
        _ => return Err(ReadBodyError::BodyTooLarge { limit }),
    };
    let mut bytes = Vec::with_capacity(lower_bound);

    while let Some(frame) = b.frame().await.transpose()? {
        if let Ok(data) = frame.into_data() {
            if bytes.len() + data.len() > limit {
                return Err(ReadBodyError::BodyTooLarge { limit });
            }
            bytes.extend_from_slice(&data);
        }
    }

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use reqwest::Client;

    use super::*;

    #[test]
    fn optional_query_params() -> anyhow::Result<()> {
        let url = "http://example.com".parse()?;
        let endpoint = Endpoint::new(url, Client::new());

        // Validate that this pattern makes sense.
        let req = endpoint
            .get_path("frobnicate")
            .query(&[
                ("foo", Some("10")), // should be just `foo=10`
                ("bar", None),       // shouldn't be passed at all
            ])
            .build()?;

        assert_eq!(req.url().as_str(), "http://example.com/frobnicate?foo=10");

        Ok(())
    }

    #[test]
    fn uuid_params() -> anyhow::Result<()> {
        let url = "http://example.com".parse()?;
        let endpoint = Endpoint::new(url, Client::new());

        let req = endpoint
            .get_path("frobnicate")
            .query(&[("session_id", uuid::Uuid::nil())])
            .build()?;

        assert_eq!(
            req.url().as_str(),
            "http://example.com/frobnicate?session_id=00000000-0000-0000-0000-000000000000"
        );

        Ok(())
    }
}
