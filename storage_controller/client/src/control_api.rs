use pageserver_client::mgmt_api::{self, ResponseErrorMessageExt};
use reqwest::{Method, Url};
use serde::{de::DeserializeOwned, Serialize};
use std::str::FromStr;

pub struct Client {
    base_url: Url,
    jwt_token: Option<String>,
    client: reqwest::Client,
}

impl Client {
    pub fn new(base_url: Url, jwt_token: Option<String>) -> Self {
        Self {
            base_url,
            jwt_token,
            client: reqwest::ClientBuilder::new()
                .build()
                .expect("Failed to construct http client"),
        }
    }

    /// Simple HTTP request wrapper for calling into storage controller
    pub async fn dispatch<RQ, RS>(
        &self,
        method: Method,
        path: String,
        body: Option<RQ>,
    ) -> mgmt_api::Result<RS>
    where
        RQ: Serialize + Sized,
        RS: DeserializeOwned + Sized,
    {
        // The configured URL has the /upcall path prefix for pageservers to use: we will strip that out
        // for general purpose API access.
        let url = Url::from_str(&format!(
            "http://{}:{}/{path}",
            self.base_url.host_str().unwrap(),
            self.base_url.port().unwrap()
        ))
        .unwrap();

        let mut builder = self.client.request(method, url);
        if let Some(body) = body {
            builder = builder.json(&body)
        }
        if let Some(jwt_token) = &self.jwt_token {
            builder = builder.header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {jwt_token}"),
            );
        }

        let response = builder.send().await.map_err(mgmt_api::Error::ReceiveBody)?;
        let response = response.error_from_body().await?;

        response
            .json()
            .await
            .map_err(pageserver_client::mgmt_api::Error::ReceiveBody)
    }
}
