use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use pageserver_api::config::PostHogConfig;
use pageserver_client::mgmt_api;
use posthog_client_lite::{PostHogClient, PostHogClientConfig};
use reqwest::StatusCode;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::{pageserver_client::PageserverClient, service::Service};

pub struct FeatureFlagService {
    service: Arc<Service>,
    config: PostHogConfig,
    client: PostHogClient,
    http_client: reqwest::Client,
}

const DEFAULT_POSTHOG_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

impl FeatureFlagService {
    pub fn new(service: Arc<Service>, config: PostHogConfig) -> Self {
        let client = PostHogClient::new(PostHogClientConfig {
            project_id: config.project_id.clone(),
            server_api_key: config.server_api_key.clone(),
            client_api_key: config.client_api_key.clone(),
            private_api_url: config.private_api_url.clone(),
            public_api_url: config.public_api_url.clone(),
        });
        Self {
            service,
            config,
            client,
            http_client: reqwest::Client::new(),
        }
    }

    async fn refresh(self: Arc<Self>, cancel: CancellationToken) -> Result<(), anyhow::Error> {
        let nodes = {
            let inner = self.service.inner.read().unwrap();
            inner.nodes.clone()
        };

        let feature_flag_spec = self.client.get_feature_flags_local_evaluation_raw().await?;
        let stream = futures::stream::iter(nodes.values().cloned()).map(|node| {
            let this = self.clone();
            let feature_flag_spec = feature_flag_spec.clone();
            async move {
                let res = async {
                    let client = PageserverClient::new(
                        node.get_id(),
                        this.http_client.clone(),
                        node.base_url(),
                        // TODO: what if we rotate the token during storcon lifetime?
                        this.service.config.pageserver_jwt_token.as_deref(),
                    );

                    client.update_feature_flag_spec(feature_flag_spec).await?;
                    tracing::info!(
                        "Updated {}({}) with feature flag spec",
                        node.get_id(),
                        node.base_url()
                    );
                    Ok::<_, mgmt_api::Error>(())
                };

                if let Err(e) = res.await {
                    if let mgmt_api::Error::ApiError(status, _) = e {
                        if status == StatusCode::NOT_FOUND {
                            // This is expected during deployments where the API is not available, so we can ignore it
                            return;
                        }
                    }
                    tracing::warn!(
                        "Failed to update feature flag spec for {}: {e}",
                        node.get_id()
                    );
                }
            }
        });
        let mut stream = stream.buffer_unordered(8);

        while stream.next().await.is_some() {
            if cancel.is_cancelled() {
                return Ok(());
            }
        }

        Ok(())
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let refresh_interval = self
            .config
            .refresh_interval
            .unwrap_or(DEFAULT_POSTHOG_REFRESH_INTERVAL);
        let mut interval = tokio::time::interval(refresh_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tracing::info!(
            "Starting feature flag service with refresh interval: {:?}",
            refresh_interval
        );
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                _ = cancel.cancelled() => {
                    break;
                }
            }
            let res = self.clone().refresh(cancel.clone()).await;
            if let Err(e) = res {
                tracing::error!("Failed to refresh feature flags: {e:#?}");
            }
        }
    }
}
