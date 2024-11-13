//! FIXME: most of this is copy-paste from mgmt_api.rs ; dedupe into a `reqwest_utils::Client` crate.
use pageserver_client::mgmt_api::{Error, ResponseErrorMessageExt};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::config::PageServerConf;
use reqwest::Method;

use super::importbucket_format::Spec;

pub struct Client {
    base_url: String,
    authorization_header: Option<String>,
    client: reqwest::Client,
    cancel: CancellationToken,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize, Debug)]
struct ImportProgressRequest {
    // no fields yet, not sure if there every will be any
}

#[derive(Serialize, Deserialize, Debug)]
struct ImportProgressResponse {
    // we don't care
}

impl Client {
    pub fn new(conf: &PageServerConf, cancel: CancellationToken) -> anyhow::Result<Self> {
        let Some(ref base_url) = conf.import_pgdata_upcall_api else {
            anyhow::bail!("import_pgdata_upcall_api is not configured")
        };
        Ok(Self {
            base_url: base_url.to_string(),
            client: reqwest::Client::new(),
            cancel,
            authorization_header: conf
                .import_pgdata_upcall_api_token
                .as_ref()
                .map(|secret_string| secret_string.get_contents())
                .map(|jwt| format!("Bearer {jwt}")),
        })
    }

    fn start_request<U: reqwest::IntoUrl>(
        &self,
        method: Method,
        uri: U,
    ) -> reqwest::RequestBuilder {
        let req = self.client.request(method, uri);
        if let Some(value) = &self.authorization_header {
            req.header(reqwest::header::AUTHORIZATION, value)
        } else {
            req
        }
    }

    async fn request_noerror<B: serde::Serialize, U: reqwest::IntoUrl>(
        &self,
        method: Method,
        uri: U,
        body: B,
    ) -> Result<reqwest::Response> {
        self.start_request(method, uri)
            .json(&body)
            .send()
            .await
            .map_err(Error::ReceiveBody)
    }

    async fn request<B: serde::Serialize, U: reqwest::IntoUrl>(
        &self,
        method: Method,
        uri: U,
        body: B,
    ) -> Result<reqwest::Response> {
        let res = self.request_noerror(method, uri, body).await?;
        let response = res.error_from_body().await?;
        Ok(response)
    }

    pub async fn send_progress_once(&self, spec: &Spec) -> Result<()> {
        let url = format!(
            "{}/projects/{}/branches/{}/import_progress",
            self.base_url, spec.project_id, spec.branch_id
        );
        let ImportProgressResponse {} = self
            .request(Method::POST, url, &ImportProgressRequest {})
            .await?
            .json()
            .await
            .map_err(Error::ReceiveBody)?;
        Ok(())
    }

    pub async fn send_progress_until_success(&self, spec: &Spec) -> anyhow::Result<()> {
        loop {
            match self.send_progress_once(spec).await {
                Ok(()) => return Ok(()),
                Err(Error::Cancelled) => return Err(anyhow::anyhow!("cancelled")),
                Err(err) => {
                    error!(?err, "error sending progress, retrying");
                    if tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        self.cancel.cancelled(),
                    )
                    .await
                    .is_ok()
                    {
                        anyhow::bail!("cancelled while sending early progress update");
                    }
                }
            }
        }
    }
}
