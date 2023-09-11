use std::collections::HashMap;

use pageserver_api::control_api::{
    ReAttachRequest, ReAttachResponse, ValidateRequest, ValidateRequestTenant, ValidateResponse,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio_util::sync::CancellationToken;
use url::Url;
use utils::{
    backoff,
    generation::Generation,
    id::{NodeId, TenantId},
};

use crate::config::PageServerConf;

/// The Pageserver's client for using the control plane API: this is a small subset
/// of the overall control plane API, for dealing with generations (see docs/rfcs/025-generation-numbers.md)
pub struct ControlPlaneClient {
    http_client: reqwest::Client,
    base_url: Url,
    node_id: NodeId,
    cancel: CancellationToken,
}

#[async_trait::async_trait]
pub trait ControlPlaneGenerationsApi {
    async fn re_attach(&self) -> anyhow::Result<HashMap<TenantId, Generation>>;
    async fn validate(
        &self,
        tenants: Vec<(TenantId, Generation)>,
    ) -> anyhow::Result<HashMap<TenantId, bool>>;
}

unsafe impl Send for ControlPlaneClient {}
unsafe impl Sync for ControlPlaneClient {}

impl ControlPlaneClient {
    /// A None return value indicates that the input `conf` object does not have control
    /// plane API enabled.
    pub fn new(conf: &'static PageServerConf, cancel: &CancellationToken) -> Option<Self> {
        let mut url = match conf.control_plane_api.as_ref() {
            Some(u) => u.clone(),
            None => return None,
        };

        if let Ok(mut segs) = url.path_segments_mut() {
            // This ensures that `url` ends with a slash if it doesn't already.
            // That way, we can subsequently use join() to safely attach extra path elements.
            segs.pop_if_empty().push("");
        }

        let client = reqwest::ClientBuilder::new()
            .build()
            .expect("Failed to construct http client");

        Some(Self {
            http_client: client,
            base_url: url,
            node_id: conf.id,
            cancel: cancel.clone(),
        })
    }

    async fn retry_http_forever<R, T>(&self, url: &url::Url, request: R) -> Result<T, anyhow::Error>
    where
        R: Serialize,
        T: DeserializeOwned,
    {
        #[derive(thiserror::Error, Debug)]
        enum RemoteAttemptError {
            #[error("shutdown")]
            Shutdown,
            #[error("remote: {0}")]
            Remote(reqwest::Error),
        }

        match backoff::retry(
            || async {
                let response = self
                    .http_client
                    .post(url.clone())
                    .json(&request)
                    .send()
                    .await
                    .map_err(|e| RemoteAttemptError::Remote(e))?;

                response
                    .error_for_status_ref()
                    .map_err(|e| RemoteAttemptError::Remote(e))?;
                response
                    .json::<T>()
                    .await
                    .map_err(|e| RemoteAttemptError::Remote(e))
            },
            |_| false,
            3,
            u32::MAX,
            "calling control plane generation validation API",
            backoff::Cancel::new(self.cancel.clone(), || RemoteAttemptError::Shutdown),
        )
        .await
        {
            Err(RemoteAttemptError::Shutdown) => Err(anyhow::anyhow!("Shutting down")),
            Err(RemoteAttemptError::Remote(_)) => {
                panic!("We retry forever, this should never be reached");
            }
            Ok(r) => Ok(r),
        }
    }
}

#[async_trait::async_trait]
impl ControlPlaneGenerationsApi for ControlPlaneClient {
    /// Block until we get a successful response
    async fn re_attach(&self) -> anyhow::Result<HashMap<TenantId, Generation>> {
        let re_attach_path = self
            .base_url
            .join("re-attach")
            .expect("Failed to build re-attach path");
        let request = ReAttachRequest {
            node_id: self.node_id,
        };

        let response: ReAttachResponse = self.retry_http_forever(&re_attach_path, request).await?;
        tracing::info!(
            "Received re-attach response with {} tenants",
            response.tenants.len()
        );

        return Ok(response
            .tenants
            .into_iter()
            .map(|t| (t.id, Generation::new(t.generation)))
            .collect::<HashMap<_, _>>());
    }

    async fn validate(
        &self,
        tenants: Vec<(TenantId, Generation)>,
    ) -> anyhow::Result<HashMap<TenantId, bool>> {
        let re_attach_path = self
            .base_url
            .join("validate")
            .expect("Failed to build validate path");

        let request = ValidateRequest {
            tenants: tenants
                .into_iter()
                .map(|(id, gen)| ValidateRequestTenant {
                    id,
                    gen: gen
                        .into()
                        .expect("Generation should always be valid for a Tenant doing deletions"),
                })
                .collect(),
        };

        let response: ValidateResponse = self.retry_http_forever(&re_attach_path, request).await?;

        Ok(response
            .tenants
            .into_iter()
            .map(|rt| (rt.id, rt.valid))
            .collect())
    }
}
