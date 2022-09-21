pub mod server;

use crate::url::ApiUrl;

/// Thin convenience wrapper for an API provided by an http endpoint.
#[derive(Debug, Clone)]
pub struct Endpoint {
    /// API's base URL.
    endpoint: ApiUrl,
    /// Connection manager with built-in pooling.
    client: reqwest::Client,
}

impl Endpoint {
    /// Construct a new HTTP endpoint wrapper.
    pub fn new(endpoint: ApiUrl, client: reqwest::Client) -> Self {
        Self { endpoint, client }
    }

    pub fn url(&self) -> &ApiUrl {
        &self.endpoint
    }

    /// Return a [builder](reqwest::RequestBuilder) for a `GET` request,
    /// appending a single `path` segment to the base endpoint URL.
    pub fn get(&self, path: &str) -> reqwest::RequestBuilder {
        let mut url = self.endpoint.clone();
        url.path_segments_mut().push(path);
        self.client.get(url.into_inner())
    }

    /// Execute a [request](reqwest::Request).
    pub async fn execute(
        &self,
        request: reqwest::Request,
    ) -> Result<reqwest::Response, reqwest::Error> {
        self.client.execute(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_query_params() -> anyhow::Result<()> {
        let url = "http://example.com".parse()?;
        let endpoint = Endpoint::new(url, reqwest::Client::new());

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
        let endpoint = Endpoint::new(url, reqwest::Client::new());

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
