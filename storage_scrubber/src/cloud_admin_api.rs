use std::error::Error as _;

use chrono::{DateTime, Utc};
use futures::Future;
use hex::FromHex;

use reqwest::{header, Client, StatusCode, Url};
use serde::Deserialize;
use tokio::sync::Semaphore;

use tokio_util::sync::CancellationToken;
use utils::backoff;
use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;

use crate::ConsoleConfig;

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
                "Failed to send a request. Context: {}, error: {}{}",
                self.context,
                e,
                e.source().map(|e| format!(": {e}")).unwrap_or_default()
            ),
            ErrorKind::BodyRead(e) => {
                write!(
                    f,
                    "Failed to read a request body. Context: {}, error: {}{}",
                    self.context,
                    e,
                    e.source().map(|e| format!(": {e}")).unwrap_or_default()
                )
            }
            ErrorKind::ResponseStatus(status) => {
                write!(f, "Bad response status {}: {}", status, self.context)
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
    ResponseStatus(StatusCode),
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

/// For ID fields, the Console API does not always return a value or null.  It will
/// sometimes return an empty string.  Our native Id type does not consider this acceptable
/// (nor should it), so we use a wrapper for talking to the Console API.
fn from_nullable_id<'de, D>(deserializer: D) -> Result<TenantId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    if deserializer.is_human_readable() {
        let id_str = String::deserialize(deserializer)?;
        if id_str.is_empty() {
            // This is a bogus value, but for the purposes of the scrubber all that
            // matters is that it doesn't collide with any real IDs.
            Ok(TenantId::from([0u8; 16]))
        } else {
            TenantId::from_hex(&id_str).map_err(|e| serde::de::Error::custom(format!("{e}")))
        }
    } else {
        let id_arr = <[u8; 16]>::deserialize(deserializer)?;
        Ok(TenantId::from(id_arr))
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ProjectData {
    pub id: ProjectId,
    pub name: String,
    pub region_id: String,
    pub platform_id: String,
    pub user_id: Option<String>,
    pub pageserver_id: Option<u64>,
    #[serde(deserialize_with = "from_nullable_id")]
    pub tenant: TenantId,
    pub safekeepers: Vec<SafekeeperData>,
    pub deleted: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub pg_version: u32,
    pub max_project_size: i64,
    pub remote_storage_size: u64,
    pub resident_size: u64,
    pub synthetic_storage_size: u64,
    pub compute_time: u64,
    pub data_transfer: u64,
    pub data_storage: u64,
    pub maintenance_set: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct BranchData {
    pub id: BranchId,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub name: String,
    pub project_id: ProjectId,
    pub timeline_id: TimelineId,
    #[serde(default)]
    pub parent_id: Option<BranchId>,
    #[serde(default)]
    pub parent_lsn: Option<Lsn>,
    pub default: bool,
    pub deleted: bool,
    pub logical_size: Option<u64>,
    pub physical_size: Option<u64>,
    pub written_size: Option<u64>,
}

pub trait MaybeDeleted {
    fn is_deleted(&self) -> bool;
}

impl MaybeDeleted for ProjectData {
    fn is_deleted(&self) -> bool {
        self.deleted
    }
}

impl MaybeDeleted for BranchData {
    fn is_deleted(&self) -> bool {
        self.deleted
    }
}

impl CloudAdminApiClient {
    pub fn new(config: ConsoleConfig) -> Self {
        Self {
            token: config.token,
            base_url: config.base_url,
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

        let response = CloudAdminApiClient::with_retries(
            || async {
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

                let response: AdminApiResponse<Vec<ProjectData>> =
                    response.json().await.map_err(|e| {
                        Error::new(
                            "Find project for tenant".to_string(),
                            ErrorKind::BodyRead(e),
                        )
                    })?;
                Ok(response)
            },
            "find_tenant_project",
        )
        .await?;

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

    pub async fn list_projects(&self) -> Result<Vec<ProjectData>, Error> {
        let _permit = self
            .request_limiter
            .acquire()
            .await
            .expect("Semaphore is not closed");

        let mut pagination_offset = 0;
        const PAGINATION_LIMIT: usize = 512;
        let mut result: Vec<ProjectData> = Vec::with_capacity(PAGINATION_LIMIT);
        loop {
            let response_bytes = CloudAdminApiClient::with_retries(
                || async {
                    let response = self
                        .http_client
                        .get(self.append_url("/projects"))
                        .query(&[
                            ("show_deleted", "false".to_string()),
                            ("limit", format!("{PAGINATION_LIMIT}")),
                            ("offset", format!("{pagination_offset}")),
                        ])
                        .header(header::ACCEPT, "application/json")
                        .bearer_auth(&self.token)
                        .send()
                        .await
                        .map_err(|e| {
                            Error::new(
                                "List active projects".to_string(),
                                ErrorKind::RequestSend(e),
                            )
                        })?;

                    response.bytes().await.map_err(|e| {
                        Error::new("List active projects".to_string(), ErrorKind::BodyRead(e))
                    })
                },
                "list_projects",
            )
            .await?;

            let decode_result =
                serde_json::from_slice::<AdminApiResponse<Vec<ProjectData>>>(&response_bytes);

            let mut response = match decode_result {
                Ok(r) => r,
                Err(decode) => {
                    tracing::error!(
                        "Failed to decode response body: {}\n{}",
                        decode,
                        String::from_utf8(response_bytes.to_vec()).unwrap()
                    );
                    panic!("we out");
                }
            };

            pagination_offset += response.data.len();

            result.append(&mut response.data);

            if pagination_offset >= response.total.unwrap_or(0) {
                break;
            }
        }

        Ok(result)
    }

    pub async fn find_timeline_branch(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> Result<Option<BranchData>, Error> {
        let _permit = self
            .request_limiter
            .acquire()
            .await
            .expect("Semaphore is not closed");

        let response = CloudAdminApiClient::with_retries(
            || async {
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

                let response: AdminApiResponse<Vec<BranchData>> =
                    response.json().await.map_err(|e| {
                        Error::new(
                            "Find branch for timeline".to_string(),
                            ErrorKind::BodyRead(e),
                        )
                    })?;
                Ok(response)
            },
            "find_timeline_branch",
        )
        .await?;

        let mut branches: Vec<BranchData> = response.data.into_iter().collect();
        // Normally timeline_id is unique. However, we do have at least one case
        // of the same timeline_id in two different projects, apparently after
        // manual recovery. So always recheck project_id (discovered through
        // tenant_id).
        let project_data = match self.find_tenant_project(tenant_id).await? {
            Some(pd) => pd,
            None => return Ok(None),
        };
        branches.retain(|b| b.project_id == project_data.id);
        if branches.len() < 2 {
            Ok(branches.first().cloned())
        } else {
            Err(Error::new(
                format!(
                    "Find branch for timeline {}/{} returned {} branches instead of 0 or 1",
                    tenant_id,
                    timeline_id,
                    branches.len()
                ),
                ErrorKind::UnexpectedState,
            ))
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

    async fn with_retries<T, O, F>(op: O, description: &str) -> Result<T, Error>
    where
        O: FnMut() -> F,
        F: Future<Output = Result<T, Error>>,
    {
        let cancel = CancellationToken::new(); // not really used
        backoff::retry(op, |_| false, 1, 20, description, &cancel)
            .await
            .expect("cancellations are disabled")
    }
}
