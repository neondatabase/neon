//! Safekeeper http client.
//!
//! Partially copied from pageserver client; some parts might be better to be
//! united.

use std::error::Error as _;

use http_utils::error::HttpErrorBody;
use reqwest::{IntoUrl, Method, StatusCode};
use safekeeper_api::models::{
    self, PullTimelineRequest, PullTimelineResponse, SafekeeperUtilization, TimelineCreateRequest,
    TimelineStatus,
};
use utils::id::{NodeId, TenantId, TimelineId};
use utils::logging::SecretString;

#[derive(Debug, Clone)]
pub struct Client {
    mgmt_api_endpoint: String,
    authorization_header: Option<SecretString>,
    client: reqwest::Client,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Failed to receive body (reqwest error).
    #[error("receive body: {0}{}", .0.source().map(|e| format!(": {e}")).unwrap_or_default())]
    ReceiveBody(reqwest::Error),

    /// Status is not ok, but failed to parse body as `HttpErrorBody`.
    #[error("receive error body: {0}")]
    ReceiveErrorBody(String),

    /// Status is not ok; parsed error in body as `HttpErrorBody`.
    #[error("safekeeper API: {1}")]
    ApiError(StatusCode, String),

    #[error("Cancelled")]
    Cancelled,

    #[error("request timed out: {0}")]
    Timeout(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait ResponseErrorMessageExt: Sized {
    fn error_from_body(self) -> impl std::future::Future<Output = Result<Self>> + Send;
}

/// If status is not ok, try to extract error message from the body.
impl ResponseErrorMessageExt for reqwest::Response {
    async fn error_from_body(self) -> Result<Self> {
        let status = self.status();
        if !(status.is_client_error() || status.is_server_error()) {
            return Ok(self);
        }

        let url = self.url().to_owned();
        Err(match self.json::<HttpErrorBody>().await {
            Ok(HttpErrorBody { msg }) => Error::ApiError(status, msg),
            Err(_) => {
                Error::ReceiveErrorBody(format!("http error ({}) at {}.", status.as_u16(), url))
            }
        })
    }
}

impl Client {
    pub fn new(
        client: reqwest::Client,
        mgmt_api_endpoint: String,
        jwt: Option<SecretString>,
    ) -> Self {
        Self {
            mgmt_api_endpoint,
            authorization_header: jwt
                .map(|jwt| SecretString::from(format!("Bearer {}", jwt.get_contents()))),
            client,
        }
    }

    pub async fn create_timeline(&self, req: &TimelineCreateRequest) -> Result<reqwest::Response> {
        let uri = format!("{}/v1/tenant/timeline", self.mgmt_api_endpoint);
        let resp = self.post(&uri, req).await?;
        Ok(resp)
    }

    pub async fn pull_timeline(&self, req: &PullTimelineRequest) -> Result<PullTimelineResponse> {
        let uri = format!("{}/v1/pull_timeline", self.mgmt_api_endpoint);
        let resp = self.post(&uri, req).await?;
        resp.json().await.map_err(Error::ReceiveBody)
    }

    pub async fn exclude_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: &models::TimelineMembershipSwitchRequest,
    ) -> Result<models::TimelineDeleteResult> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline/{}/exclude",
            self.mgmt_api_endpoint, tenant_id, timeline_id
        );
        let resp = self.put(&uri, req).await?;
        resp.json().await.map_err(Error::ReceiveBody)
    }

    pub async fn delete_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<models::TimelineDeleteResult> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline/{}",
            self.mgmt_api_endpoint, tenant_id, timeline_id
        );
        let resp = self
            .request_maybe_body(Method::DELETE, &uri, None::<()>)
            .await?;
        resp.json().await.map_err(Error::ReceiveBody)
    }

    pub async fn switch_timeline_membership(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: &models::TimelineMembershipSwitchRequest,
    ) -> Result<models::TimelineMembershipSwitchResponse> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline/{}/membership",
            self.mgmt_api_endpoint, tenant_id, timeline_id
        );
        let resp = self.put(&uri, req).await?;
        resp.json().await.map_err(Error::ReceiveBody)
    }

    pub async fn delete_tenant(&self, tenant_id: TenantId) -> Result<models::TenantDeleteResult> {
        let uri = format!("{}/v1/tenant/{}", self.mgmt_api_endpoint, tenant_id);
        let resp = self
            .request_maybe_body(Method::DELETE, &uri, None::<()>)
            .await?;
        resp.json().await.map_err(Error::ReceiveBody)
    }

    pub async fn bump_timeline_term(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        req: &models::TimelineTermBumpRequest,
    ) -> Result<models::TimelineTermBumpResponse> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline/{}/term_bump",
            self.mgmt_api_endpoint, tenant_id, timeline_id
        );
        let resp = self.post(&uri, req).await?;
        resp.json().await.map_err(Error::ReceiveBody)
    }

    pub async fn timeline_status(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<TimelineStatus> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline/{}",
            self.mgmt_api_endpoint, tenant_id, timeline_id
        );
        let resp = self.get(&uri).await?;
        resp.json().await.map_err(Error::ReceiveBody)
    }

    pub async fn snapshot(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        stream_to: NodeId,
    ) -> Result<reqwest::Response> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline/{}/snapshot/{}",
            self.mgmt_api_endpoint, tenant_id, timeline_id, stream_to.0
        );
        self.get(&uri).await
    }

    pub async fn utilization(&self) -> Result<SafekeeperUtilization> {
        let uri = format!("{}/v1/utilization", self.mgmt_api_endpoint);
        let resp = self.get(&uri).await?;
        resp.json().await.map_err(Error::ReceiveBody)
    }

    async fn post<B: serde::Serialize, U: IntoUrl>(
        &self,
        uri: U,
        body: B,
    ) -> Result<reqwest::Response> {
        self.request(Method::POST, uri, body).await
    }

    async fn put<B: serde::Serialize, U: IntoUrl>(
        &self,
        uri: U,
        body: B,
    ) -> Result<reqwest::Response> {
        self.request(Method::PUT, uri, body).await
    }

    async fn get<U: IntoUrl>(&self, uri: U) -> Result<reqwest::Response> {
        self.request(Method::GET, uri, ()).await
    }

    /// Send the request and check that the status code is good.
    async fn request<B: serde::Serialize, U: reqwest::IntoUrl>(
        &self,
        method: Method,
        uri: U,
        body: B,
    ) -> Result<reqwest::Response> {
        self.request_maybe_body(method, uri, Some(body)).await
    }

    /// Send the request and check that the status code is good, with an optional body.
    async fn request_maybe_body<B: serde::Serialize, U: reqwest::IntoUrl>(
        &self,
        method: Method,
        uri: U,
        body: Option<B>,
    ) -> Result<reqwest::Response> {
        let res = self.request_noerror(method, uri, body).await?;
        let response = res.error_from_body().await?;
        Ok(response)
    }

    /// Just send the request.
    async fn request_noerror<B: serde::Serialize, U: reqwest::IntoUrl>(
        &self,
        method: Method,
        uri: U,
        body: Option<B>,
    ) -> Result<reqwest::Response> {
        let mut req = self.client.request(method, uri);
        if let Some(value) = &self.authorization_header {
            req = req.header(reqwest::header::AUTHORIZATION, value.get_contents())
        }
        if let Some(body) = body {
            req = req.json(&body);
        }
        req.send().await.map_err(Error::ReceiveBody)
    }
}
