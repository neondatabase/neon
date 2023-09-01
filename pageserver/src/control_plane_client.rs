use std::{collections::HashMap, time::Duration};

use hyper::StatusCode;
use pageserver_api::control_api::{ReAttachRequest, ReAttachResponse};
use url::Url;
use utils::{
    generation::Generation,
    id::{NodeId, TenantId},
};

use crate::config::PageServerConf;

/// The Pageserver's client for using the control plane API: this is a small subset
/// of the overall control plane API, for dealing with generations (see docs/rfcs/025-generation-numbers.md)
pub(crate) struct ControlPlaneClient {
    http_client: reqwest::Client,
    base_url: Url,
    node_id: NodeId,
}

impl ControlPlaneClient {
    /// A None return value indicates that the input `conf` object does not have control
    /// plane API enabled.
    pub(crate) fn new(conf: &'static PageServerConf) -> Option<Self> {
        let url = match conf.control_plane_api.as_ref() {
            Some(u) => u.clone(),
            None => return None,
        };

        // FIXME: it's awkward that join() requires the base to have a trailing slash, makes
        // it easy to get a config wrong
        assert!(
            url.as_str().ends_with('/'),
            "control plane API needs trailing slash"
        );

        let client = reqwest::ClientBuilder::new()
            .build()
            .expect("Failed to construct http client");

        Some(Self {
            http_client: client,
            base_url: url,
            node_id: conf.id,
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

        // TODO: we should have been passed a cancellation token, and use it to end
        // this loop gracefully
        loop {
            let result = self.try_re_attach(re_attach_path.clone(), &request).await;
            match result {
                Ok(res) => {
                    tracing::info!(
                        "Received re-attach response with {0} tenants",
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
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}
