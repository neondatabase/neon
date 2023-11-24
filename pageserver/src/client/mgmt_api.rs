use anyhow::Context;

use hyper::{client::HttpConnector, Uri};
use utils::id::{TenantId, TimelineId};

pub struct Client {
    mgmt_api_endpoint: String,
    client: hyper::Client<HttpConnector, hyper::Body>,
}

impl Client {
    pub fn new(mgmt_api_endpoint: String) -> Self {
        Self {
            mgmt_api_endpoint,
            client: hyper::client::Client::new(),
        }
    }

    pub async fn list_tenants(&self) -> anyhow::Result<Vec<pageserver_api::models::TenantInfo>> {
        let uri = Uri::try_from(format!("{}/v1/tenant", self.mgmt_api_endpoint))?;
        let resp = self.client.get(uri).await?;
        if !resp.status().is_success() {
            anyhow::bail!("status error");
        }
        let body = hyper::body::to_bytes(resp).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn list_timelines(
        &self,
        tenant_id: TenantId,
    ) -> anyhow::Result<Vec<pageserver_api::models::TimelineInfo>> {
        let uri = Uri::try_from(format!(
            "{}/v1/tenant/{tenant_id}/timeline",
            self.mgmt_api_endpoint
        ))?;
        let resp = self.client.get(uri).await?;
        if !resp.status().is_success() {
            anyhow::bail!("status error");
        }
        let body = hyper::body::to_bytes(resp).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn keyspace(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> anyhow::Result<crate::http::models::partitioning::Partitioning> {
        let uri = Uri::try_from(format!(
            "{}/v1/tenant/{tenant_id}/timeline/{timeline_id}/keyspace?check_serialization_roundtrip=true",
            self.mgmt_api_endpoint
        ))?;
        let resp = self.client.get(uri).await?;
        if !resp.status().is_success() {
            anyhow::bail!("status error");
        }
        let body = hyper::body::to_bytes(resp).await?;
        Ok(serde_json::from_slice(&body).context("deserialize")?)
    }
}
