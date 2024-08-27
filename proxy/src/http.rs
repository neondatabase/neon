//! HTTP client and server impls.
//! Other modules should use stuff from this module instead of
//! directly relying on deps like `reqwest` (think loose coupling).

pub mod health_server;

use std::time::Duration;

use anyhow::bail;
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper1::body::Body;
use serde::de::DeserializeOwned;

pub(crate) use reqwest::{Request, Response};
pub(crate) use reqwest_middleware::{ClientWithMiddleware, Error};
pub(crate) use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};

use crate::{
    metrics::{ConsoleRequest, Metrics},
    url::ApiUrl,
};
use reqwest_middleware::RequestBuilder;

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

pub(crate) fn new_client_with_timeout(default_timout: Duration) -> ClientWithMiddleware {
    let timeout_client = reqwest::ClientBuilder::new()
        .timeout(default_timout)
        .build()
        .expect("Failed to create http client with timeout");

    let retry_policy =
        ExponentialBackoff::builder().build_with_total_retry_duration(default_timout);

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
    pub(crate) fn get(&self, path: &str) -> RequestBuilder {
        let mut url = self.endpoint.clone();
        url.path_segments_mut().push(path);
        self.client.get(url.into_inner())
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

pub(crate) async fn parse_json_body_with_limit<D: DeserializeOwned>(
    mut b: impl Body<Data = Bytes, Error = reqwest::Error> + Unpin,
    limit: usize,
) -> anyhow::Result<D> {
    // We could use `b.limited().collect().await.to_bytes()` here
    // but this ends up being slightly more efficient as far as I can tell.

    // check the lower bound of the size hint.
    // in reqwest, this value is influenced by the Content-Length header.
    let lower_bound = match usize::try_from(b.size_hint().lower()) {
        Ok(bound) if bound <= limit => bound,
        _ => bail!("Content length exceeds limit of {limit} bytes"),
    };
    let mut bytes = Vec::with_capacity(lower_bound);

    while let Some(frame) = b.frame().await.transpose()? {
        if let Ok(data) = frame.into_data() {
            if bytes.len() + data.len() > limit {
                bail!("Content length exceeds limit of {limit} bytes")
            }
            bytes.extend_from_slice(&data);
        }
    }

    Ok(serde_json::from_slice::<D>(&bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::Client;

    #[test]
    fn optional_query_params() -> anyhow::Result<()> {
        let url = "http://example.com".parse()?;
        let endpoint = Endpoint::new(url, Client::new());

        // Validate that this pattern makes sense.
        let req = endpoint
            .get("frobnicate")
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
            .get("frobnicate")
            .query(&[("session_id", uuid::Uuid::nil())])
            .build()?;

        assert_eq!(
            req.url().as_str(),
            "http://example.com/frobnicate?session_id=00000000-0000-0000-0000-000000000000"
        );

        Ok(())
    }
}
