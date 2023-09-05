#![allow(unused)]

use chrono::{DateTime, Utc};
use reqwest::{header, Client, Url};
use tokio::sync::Semaphore;

use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;

#[derive(Debug)]
pub struct Error {
    context: String,
    kind: ErrorKind,
}

impl Error {
    fn new(context: String, kind: ErrorKind) -> Self {
        Self { context, kind }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            ErrorKind::RequestSend(e) => write!(
                f,
                "Failed to send a request. Context: {}, error: {}",
                self.context, e
            ),
            ErrorKind::BodyRead(e) => {
                write!(
                    f,
                    "Failed to read a request body. Context: {}, error: {}",
                    self.context, e
                )
            }
            ErrorKind::UnexpectedState => write!(f, "Unexpected state: {}", self.context),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, Hash, PartialEq, Eq)]
#[serde(transparent)]
pub struct ProjectId(pub String);

#[derive(Clone, Debug, serde::Deserialize, Hash, PartialEq, Eq)]
#[serde(transparent)]
pub struct BranchId(pub String);

impl std::error::Error for Error {}

#[derive(Debug)]
pub enum ErrorKind {
    RequestSend(reqwest::Error),
    BodyRead(reqwest::Error),
    UnexpectedState,
}

pub struct CloudAdminApiClient {
    request_limiter: Semaphore,
    token: String,
    base_url: Url,
    http_client: Client,
}

#[derive(Debug, serde::Deserialize)]
struct AdminApiResponse<T> {
    data: T,
    total: Option<usize>,
}

#[derive(Debug, serde::Deserialize)]
pub struct PageserverData {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub region_id: String,
    pub version: i64,
    pub instance_id: String,
    pub port: u16,
    pub http_host: String,
    pub http_port: u16,
    pub active: bool,
    pub projects_count: usize,
    pub availability_zone_id: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SafekeeperData {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub region_id: String,
    pub version: i64,
    pub instance_id: String,
    pub active: bool,
    pub host: String,
    pub port: u16,
    pub projects_count: usize,
    pub availability_zone_id: String,
}

#[serde_with::serde_as]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ProjectData {
    pub id: ProjectId,
    pub name: String,
    pub region_id: String,
    pub platform_id: String,
    pub user_id: String,
    pub pageserver_id: u64,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub tenant: TenantId,
    pub safekeepers: Vec<SafekeeperData>,
    pub deleted: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub pg_version: u32,
    pub max_project_size: u64,
    pub remote_storage_size: u64,
    pub resident_size: u64,
    pub synthetic_storage_size: u64,
    pub compute_time: u64,
    pub data_transfer: u64,
    pub data_storage: u64,
    pub maintenance_set: Option<String>,
}

#[serde_with::serde_as]
#[derive(Debug, serde::Deserialize)]
pub struct BranchData {
    pub id: BranchId,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub name: String,
    pub project_id: ProjectId,
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub timeline_id: TimelineId,
    #[serde(default)]
    pub parent_id: Option<BranchId>,
    #[serde(default)]
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    pub parent_lsn: Option<Lsn>,
    pub default: bool,
    pub deleted: bool,
    pub logical_size: Option<u64>,
    pub physical_size: Option<u64>,
    pub written_size: Option<u64>,
}

impl CloudAdminApiClient {
    pub fn new(token: String, base_url: Url) -> Self {
        Self {
            token,
            base_url,
            request_limiter: Semaphore::new(200),
            http_client: Client::new(), // TODO timeout configs at least
        }
    }

    pub async fn find_tenant_project(
        &self,
        tenant_id: TenantId,
    ) -> Result<Option<ProjectData>, Error> {
        let _permit = self
            .request_limiter
            .acquire()
            .await
            .expect("Semaphore is not closed");

        let response = self
            .http_client
            .get(self.append_url("/projects"))
            .query(&[
                ("tenant_id", tenant_id.to_string()),
                ("show_deleted", "true".to_string()),
            ])
            .header(header::ACCEPT, "application/json")
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| {
                Error::new(
                    "Find project for tenant".to_string(),
                    ErrorKind::RequestSend(e),
                )
            })?;

        let response: AdminApiResponse<Vec<ProjectData>> = response.json().await.map_err(|e| {
            Error::new(
                "Find project for tenant".to_string(),
                ErrorKind::BodyRead(e),
            )
        })?;
        match response.data.len() {
            0 => Ok(None),
            1 => Ok(Some(
                response
                    .data
                    .into_iter()
                    .next()
                    .expect("Should have exactly one element"),
            )),
            too_many => Err(Error::new(
                format!("Find project for tenant returned {too_many} projects instead of 0 or 1"),
                ErrorKind::UnexpectedState,
            )),
        }
    }

    pub async fn find_timeline_branch(
        &self,
        timeline_id: TimelineId,
    ) -> Result<Option<BranchData>, Error> {
        let _permit = self
            .request_limiter
            .acquire()
            .await
            .expect("Semaphore is not closed");

        let response = self
            .http_client
            .get(self.append_url("/branches"))
            .query(&[
                ("timeline_id", timeline_id.to_string()),
                ("show_deleted", "true".to_string()),
            ])
            .header(header::ACCEPT, "application/json")
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| {
                Error::new(
                    "Find branch for timeline".to_string(),
                    ErrorKind::RequestSend(e),
                )
            })?;

        let response: AdminApiResponse<Vec<BranchData>> = response.json().await.map_err(|e| {
            Error::new(
                "Find branch for timeline".to_string(),
                ErrorKind::BodyRead(e),
            )
        })?;
        match response.data.len() {
            0 => Ok(None),
            1 => Ok(Some(
                response
                    .data
                    .into_iter()
                    .next()
                    .expect("Should have exactly one element"),
            )),
            too_many => Err(Error::new(
                format!("Find branch for timeline returned {too_many} branches instead of 0 or 1"),
                ErrorKind::UnexpectedState,
            )),
        }
    }

    pub async fn list_pageservers(&self) -> Result<Vec<PageserverData>, Error> {
        let _permit = self
            .request_limiter
            .acquire()
            .await
            .expect("Semaphore is not closed");

        let response = self
            .http_client
            .get(self.append_url("/pageservers"))
            .header(header::ACCEPT, "application/json")
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| Error::new("List pageservers".to_string(), ErrorKind::RequestSend(e)))?;

        let response: AdminApiResponse<Vec<PageserverData>> = response
            .json()
            .await
            .map_err(|e| Error::new("List pageservers".to_string(), ErrorKind::BodyRead(e)))?;

        Ok(response.data)
    }

    pub async fn list_safekeepers(&self) -> Result<Vec<SafekeeperData>, Error> {
        let _permit = self
            .request_limiter
            .acquire()
            .await
            .expect("Semaphore is not closed");

        let response = self
            .http_client
            .get(self.append_url("/safekeepers"))
            .header(header::ACCEPT, "application/json")
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| Error::new("List safekeepers".to_string(), ErrorKind::RequestSend(e)))?;

        let response: AdminApiResponse<Vec<SafekeeperData>> = response
            .json()
            .await
            .map_err(|e| Error::new("List safekeepers".to_string(), ErrorKind::BodyRead(e)))?;

        Ok(response.data)
    }

    pub async fn projects_for_pageserver(
        &self,
        pageserver_id: u64,
        show_deleted: bool,
    ) -> Result<Vec<ProjectData>, Error> {
        let _permit = self
            .request_limiter
            .acquire()
            .await
            .expect("Semaphore is not closed");

        let response = self
            .http_client
            .get(self.append_url("/projects"))
            .query(&[
                ("pageserver_id", &pageserver_id.to_string()),
                ("show_deleted", &show_deleted.to_string()),
            ])
            .header(header::ACCEPT, "application/json")
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| Error::new("Project for tenant".to_string(), ErrorKind::RequestSend(e)))?;

        let response: AdminApiResponse<Vec<ProjectData>> = response
            .json()
            .await
            .map_err(|e| Error::new("Project for tenant".to_string(), ErrorKind::BodyRead(e)))?;

        Ok(response.data)
    }

    pub async fn project_for_tenant(
        &self,
        tenant_id: TenantId,
        show_deleted: bool,
    ) -> Result<Option<ProjectData>, Error> {
        let _permit = self
            .request_limiter
            .acquire()
            .await
            .expect("Semaphore is not closed");

        let response = self
            .http_client
            .get(self.append_url("/projects"))
            .query(&[
                ("search", &tenant_id.to_string()),
                ("show_deleted", &show_deleted.to_string()),
            ])
            .header(header::ACCEPT, "application/json")
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| Error::new("Project for tenant".to_string(), ErrorKind::RequestSend(e)))?;

        let response: AdminApiResponse<Vec<ProjectData>> = response
            .json()
            .await
            .map_err(|e| Error::new("Project for tenant".to_string(), ErrorKind::BodyRead(e)))?;

        match response.data.as_slice() {
            [] => Ok(None),
            [_single] => Ok(Some(response.data.into_iter().next().unwrap())),
            multiple => Err(Error::new(
                format!("Got more than one project for tenant {tenant_id} : {multiple:?}"),
                ErrorKind::UnexpectedState,
            )),
        }
    }

    pub async fn branches_for_project(
        &self,
        project_id: &ProjectId,
        show_deleted: bool,
    ) -> Result<Vec<BranchData>, Error> {
        let _permit = self
            .request_limiter
            .acquire()
            .await
            .expect("Semaphore is not closed");

        let response = self
            .http_client
            .get(self.append_url("/branches"))
            .query(&[
                ("project_id", &project_id.0),
                ("show_deleted", &show_deleted.to_string()),
            ])
            .header(header::ACCEPT, "application/json")
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| Error::new("Project for tenant".to_string(), ErrorKind::RequestSend(e)))?;

        let response: AdminApiResponse<Vec<BranchData>> = response
            .json()
            .await
            .map_err(|e| Error::new("Project for tenant".to_string(), ErrorKind::BodyRead(e)))?;

        Ok(response.data)
    }

    fn append_url(&self, subpath: &str) -> Url {
        // TODO fugly, but `.join` does not work when called
        (self.base_url.to_string() + subpath)
            .parse()
            .unwrap_or_else(|e| panic!("Could not append {subpath} to base url: {e}"))
    }
}
