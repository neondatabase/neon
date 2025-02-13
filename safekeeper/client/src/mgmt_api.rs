//! Safekeeper http client.
//!
//! Partially copied from pageserver client; some parts might be better to be
//! united.

use http_utils::error::HttpErrorBody;
use reqwest::{IntoUrl, Method, StatusCode};
use safekeeper_api::models::{SafekeeperUtilization, TimelineCreateRequest, TimelineStatus};
use std::error::Error as _;
use utils::{
    id::{NodeId, TenantId, TimelineId},
    logging::SecretString,
};

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
    pub fn new(mgmt_api_endpoint: String, jwt: Option<SecretString>) -> Self {
        Self::from_client(reqwest::Client::new(), mgmt_api_endpoint, jwt)
    }

    pub fn from_client(
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

    pub async fn create_timeline(&self, req: &TimelineCreateRequest) -> Result<TimelineStatus> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline/{}",
            self.mgmt_api_endpoint, req.tenant_id, req.timeline_id
        );
        let resp = self.post(&uri, req).await?;
        resp.json().await.map_err(Error::ReceiveBody)
    }

    pub async fn delete_timeline(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<TimelineStatus> {
        let uri = format!(
            "{}/v1/tenant/{}/timeline/{}",
            self.mgmt_api_endpoint, tenant_id, timeline_id
        );
        let resp = self.request(Method::DELETE, &uri, ()).await?;
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
        let uri = format!("{}/v1/utilization/", self.mgmt_api_endpoint);
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
        let res = self.request_noerror(method, uri, body).await?;
        let response = res.error_from_body().await?;
        Ok(response)
    }

    /// Just send the request.
    async fn request_noerror<B: serde::Serialize, U: reqwest::IntoUrl>(
        &self,
        method: Method,
        uri: U,
        body: B,
    ) -> Result<reqwest::Response> {
        let req = self.client.request(method, uri);
        let req = if let Some(value) = &self.authorization_header {
            req.header(reqwest::header::AUTHORIZATION, value.get_contents())
        } else {
            req
        };
        req.json(&body).send().await.map_err(Error::ReceiveBody)
    }
}
