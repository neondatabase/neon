use anyhow::Context;

use hyper::{client::HttpConnector, Uri};
use utils::id::{TenantId, TimelineId};

pub struct Client {
    mgmt_api_endpoint: String,
    authorization_header: Option<String>,
    client: hyper::Client<HttpConnector, hyper::Body>,
}

impl Client {
    pub fn new(mgmt_api_endpoint: String, jwt: Option<&str>) -> Self {
        Self {
            mgmt_api_endpoint,
            authorization_header: jwt.map(|jwt| format!("Bearer {jwt}")),
            client: hyper::client::Client::new(),
        }
    }

    pub async fn list_tenants(&self) -> anyhow::Result<Vec<pageserver_api::models::TenantInfo>> {
        let uri = Uri::try_from(format!("{}/v1/tenant", self.mgmt_api_endpoint))?;
        let resp = self.get(uri).await?;
        if !resp.status().is_success() {
            anyhow::bail!("status error");
        }
        let body = hyper::body::to_bytes(resp).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn tenant_details(
        &self,
        tenant_id: TenantId,
    ) -> anyhow::Result<pageserver_api::models::TenantDetails> {
        let uri = Uri::try_from(format!("{}/v1/tenant/{tenant_id}", self.mgmt_api_endpoint))?;
        let resp = self.get(uri).await?;
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
        let resp = self.get(uri).await?;
        if !resp.status().is_success() {
            anyhow::bail!("status error");
        }
        let body = hyper::body::to_bytes(resp).await?;
        Ok(serde_json::from_slice(&body)?)
    }

    pub async fn timeline_info(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> anyhow::Result<pageserver_api::models::TimelineInfo> {
        let uri = Uri::try_from(format!(
            "{}/v1/tenant/{tenant_id}/timeline/{timeline_id}",
            self.mgmt_api_endpoint
        ))?;
        let resp = self.get(uri).await?;
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
        let resp = self.get(uri).await?;
        if !resp.status().is_success() {
            anyhow::bail!("status error");
        }
        let body = hyper::body::to_bytes(resp).await?;
        Ok(serde_json::from_slice(&body).context("deserialize")?)
    }

    async fn get(&self, uri: Uri) -> hyper::Result<hyper::Response<hyper::Body>> {
        let req = hyper::Request::builder().uri(uri).method("GET");
        let req = if let Some(value) = &self.authorization_header {
            req.header("Authorization", value)
        } else {
            req
        };
        let req = req.body(hyper::Body::default());
        self.client.request(req.unwrap()).await
    }
}
