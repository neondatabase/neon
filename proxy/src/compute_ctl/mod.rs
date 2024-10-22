use compute_api::responses::GenericAPIError;
use hyper::{Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::http;
use crate::types::{DbName, RoleName};
use crate::url::ApiUrl;

pub struct ComputeCtlApi {
    pub(crate) api: http::Endpoint,
}

#[derive(Serialize, Debug)]
pub struct ExtensionInstallRequest {
    pub extension: &'static str,
    pub database: DbName,
    pub version: &'static str,
}

#[derive(Serialize, Debug)]
pub struct SetRoleGrantsRequest {
    pub database: DbName,
    pub schema: &'static str,
    pub privileges: Vec<Privilege>,
    pub role: RoleName,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ExtensionInstallResponse {}

#[derive(Clone, Debug, Deserialize)]
pub struct SetRoleGrantsResponse {}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "UPPERCASE")]
pub enum Privilege {
    Usage,
}

#[derive(Error, Debug)]
pub enum ComputeCtlError {
    #[error("connection error: {0}")]
    ConnectionError(#[source] reqwest_middleware::Error),
    #[error("request error [{status}]: {body:?}")]
    RequestError {
        status: StatusCode,
        body: Option<GenericAPIError>,
    },
    #[error("response parsing error: {0}")]
    ResponseError(#[source] reqwest::Error),
}

impl ComputeCtlApi {
    pub async fn install_extension(
        &self,
        req: &ExtensionInstallRequest,
    ) -> Result<ExtensionInstallResponse, ComputeCtlError> {
        self.generic_request(req, Method::POST, |url| {
            url.path_segments_mut().push("extensions");
        })
        .await
    }

    pub async fn grant_role(
        &self,
        req: &SetRoleGrantsRequest,
    ) -> Result<SetRoleGrantsResponse, ComputeCtlError> {
        self.generic_request(req, Method::POST, |url| {
            url.path_segments_mut().push("grants");
        })
        .await
    }

    async fn generic_request<Req, Resp>(
        &self,
        req: &Req,
        method: Method,
        url: impl for<'a> FnOnce(&'a mut ApiUrl),
    ) -> Result<Resp, ComputeCtlError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let resp = self
            .api
            .request_with_url(method, url)
            .json(req)
            .send()
            .await
            .map_err(ComputeCtlError::ConnectionError)?;

        let status = resp.status();
        if status.is_client_error() || status.is_server_error() {
            let body = resp.json().await.ok();
            return Err(ComputeCtlError::RequestError { status, body });
        }

        resp.json().await.map_err(ComputeCtlError::ResponseError)
    }
}
