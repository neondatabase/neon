use std::collections::HashMap;

use hyper::StatusCode;
use pageserver_api::control_api::{ReAttachRequest, ReAttachResponse};
use tokio_util::sync::CancellationToken;
use url::Url;
use utils::{
    backoff,
    generation::Generation,
    id::{NodeId, TenantId},
};

use crate::config::PageServerConf;

// Backoffs when control plane requests do not succeed: compromise between reducing load
// on control plane, and retrying frequently when we are blocked on a control plane
// response to make progress.
const BACKOFF_INCREMENT: f64 = 0.1;
const BACKOFF_MAX: f64 = 10.0;

/// The Pageserver's client for using the control plane API: this is a small subset
/// of the overall control plane API, for dealing with generations (see docs/rfcs/025-generation-numbers.md)
pub(crate) struct ControlPlaneClient {
    http_client: reqwest::Client,
    base_url: Url,
    node_id: NodeId,
    cancel: CancellationToken,
}

impl ControlPlaneClient {
    /// A None return value indicates that the input `conf` object does not have control
    /// plane API enabled.
    pub(crate) fn new(conf: &'static PageServerConf, cancel: &CancellationToken) -> Option<Self> {
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

    async fn try_re_attach(
        &self,
        url: Url,
        request: &ReAttachRequest,
    ) -> anyhow::Result<ReAttachResponse> {
        match self.http_client.post(url).json(request).send().await {
            Err(e) => Err(anyhow::Error::from(e)),
            Ok(r) => {
                if r.status() == StatusCode::OK {
                    r.json::<ReAttachResponse>()
                        .await
                        .map_err(anyhow::Error::from)
                } else {
                    Err(anyhow::anyhow!("Unexpected status {}", r.status()))
                }
            }
        }
    }

    /// Block until we get a successful response
    pub(crate) async fn re_attach(&self) -> anyhow::Result<HashMap<TenantId, Generation>> {
        let re_attach_path = self
            .base_url
            .join("re-attach")
            .expect("Failed to build re-attach path");
        let request = ReAttachRequest {
            node_id: self.node_id,
        };

        let mut attempt = 0;
        loop {
            let result = self.try_re_attach(re_attach_path.clone(), &request).await;
            match result {
                Ok(res) => {
                    tracing::info!(
                        "Received re-attach response with {} tenants",
                        res.tenants.len()
                    );

                    return Ok(res
                        .tenants
                        .into_iter()
                        .map(|t| (t.id, Generation::new(t.generation)))
                        .collect::<HashMap<_, _>>());
                }
                Err(e) => {
                    tracing::error!("Error re-attaching tenants, retrying: {e:#}");
                    backoff::exponential_backoff(
                        attempt,
                        BACKOFF_INCREMENT,
                        BACKOFF_MAX,
                        &self.cancel,
                    )
                    .await;
                    if self.cancel.is_cancelled() {
                        return Err(anyhow::anyhow!("Shutting down"));
                    }
                    attempt += 1;
                }
            }
        }
    }
}
