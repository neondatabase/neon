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

/// Represent operations which internally retry on all errors other than
/// cancellation token firing: the only way they can fail is ShuttingDown.
pub enum RetryForeverError {
    ShuttingDown,
}

#[async_trait::async_trait]
pub trait ControlPlaneGenerationsApi {
    async fn re_attach(&self) -> Result<HashMap<TenantId, Generation>, RetryForeverError>;
    async fn validate(
        &self,
        tenants: Vec<(TenantId, Generation)>,
    ) -> Result<HashMap<TenantId, bool>, RetryForeverError>;
}

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

        let mut client = reqwest::ClientBuilder::new();

        if let Some(jwt) = &conf.control_plane_api_token {
            let mut headers = hyper::HeaderMap::new();
            headers.insert(
                "Authorization",
                format!("Bearer {}", jwt.get_contents()).parse().unwrap(),
            );
            client = client.default_headers(headers);
        }

        Some(Self {
            http_client: client.build().expect("Failed to construct HTTP client"),
            base_url: url,
            node_id: conf.id,
            cancel: cancel.clone(),
        })
    }

    async fn retry_http_forever<R, T>(
        &self,
        url: &url::Url,
        request: R,
    ) -> Result<T, RetryForeverError>
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
                    .map_err(RemoteAttemptError::Remote)?;

                response
                    .error_for_status_ref()
                    .map_err(RemoteAttemptError::Remote)?;
                response
                    .json::<T>()
                    .await
                    .map_err(RemoteAttemptError::Remote)
            },
            |_| false,
            3,
            u32::MAX,
            "calling control plane generation validation API",
            backoff::Cancel::new(self.cancel.clone(), || RemoteAttemptError::Shutdown),
        )
        .await
        {
            Err(RemoteAttemptError::Shutdown) => Err(RetryForeverError::ShuttingDown),
            Err(RemoteAttemptError::Remote(_)) => {
                panic!("We retry forever, this should never be reached");
            }
            Ok(r) => Ok(r),
        }
    }
}

#[async_trait::async_trait]
impl ControlPlaneGenerationsApi for ControlPlaneClient {
    /// Block until we get a successful response, or error out if we are shut down
    async fn re_attach(&self) -> Result<HashMap<TenantId, Generation>, RetryForeverError> {
        let re_attach_path = self
            .base_url
            .join("re-attach")
            .expect("Failed to build re-attach path");
        let request = ReAttachRequest {
            node_id: self.node_id,
        };

        fail::fail_point!("control-plane-client-re-attach");

        let response: ReAttachResponse = self.retry_http_forever(&re_attach_path, request).await?;
        tracing::info!(
            "Received re-attach response with {} tenants",
            response.tenants.len()
        );

        Ok(response
            .tenants
            .into_iter()
            .map(|t| (t.id, Generation::new(t.gen)))
            .collect::<HashMap<_, _>>())
    }

    /// Block until we get a successful response, or error out if we are shut down
    async fn validate(
        &self,
        tenants: Vec<(TenantId, Generation)>,
    ) -> Result<HashMap<TenantId, bool>, RetryForeverError> {
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

        fail::fail_point!("control-plane-client-validate");

        let response: ValidateResponse = self.retry_http_forever(&re_attach_path, request).await?;

        Ok(response
            .tenants
            .into_iter()
            .map(|rt| (rt.id, rt.valid))
            .collect())
    }
}
