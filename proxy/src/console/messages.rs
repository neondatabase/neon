use measured::FixedCardinalityLabel;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

use crate::auth::IpPattern;

use crate::intern::{BranchIdInt, EndpointIdInt, ProjectIdInt};
use crate::proxy::retry::ShouldRetry;

/// Generic error response with human-readable description.
/// Note that we can't always present it to user as is.
#[derive(Debug, Deserialize)]
pub struct ConsoleError {
    pub error: Box<str>,
    #[serde(skip)]
    pub http_status_code: http::StatusCode,
    pub status: Option<Status>,
}

impl ConsoleError {
    pub fn get_reason(&self) -> Reason {
        self.status
            .as_ref()
            .and_then(|s| s.details.error_info.as_ref())
            .map(|e| e.reason)
            .unwrap_or(Reason::Unknown)
    }
    pub fn get_user_facing_message(&self) -> String {
        use super::provider::errors::REQUEST_FAILED;
        self.status
            .as_ref()
            .and_then(|s| s.details.user_facing_message.as_ref())
            .map(|m| m.message.clone().into())
            .unwrap_or_else(|| {
                // Ask @neondatabase/control-plane for review before adding more.
                match self.http_status_code {
                    http::StatusCode::NOT_FOUND => {
                        // Status 404: failed to get a project-related resource.
                        format!("{REQUEST_FAILED}: endpoint cannot be found")
                    }
                    http::StatusCode::NOT_ACCEPTABLE => {
                        // Status 406: endpoint is disabled (we don't allow connections).
                        format!("{REQUEST_FAILED}: endpoint is disabled")
                    }
                    http::StatusCode::LOCKED | http::StatusCode::UNPROCESSABLE_ENTITY => {
                        // Status 423: project might be in maintenance mode (or bad state), or quotas exceeded.
                        format!("{REQUEST_FAILED}: endpoint is temporarily unavailable. Check your quotas and/or contact our support.")
                    }
                    _ => REQUEST_FAILED.to_owned(),
                }
            })
    }
}

impl Display for ConsoleError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = self
            .status
            .as_ref()
            .and_then(|s| s.details.user_facing_message.as_ref())
            .map(|m| m.message.as_ref())
            .unwrap_or_else(|| &self.error);
        write!(f, "{}", msg)
    }
}

impl ShouldRetry for ConsoleError {
    fn could_retry(&self) -> bool {
        if self.status.is_none() || self.status.as_ref().unwrap().details.retry_info.is_none() {
            // retry some temporary failures because the compute was in a bad state
            // (bad request can be returned when the endpoint was in transition)
            return match &self {
                ConsoleError {
                    http_status_code: http::StatusCode::BAD_REQUEST,
                    ..
                } => true,
                // don't retry when quotas are exceeded
                ConsoleError {
                    http_status_code: http::StatusCode::UNPROCESSABLE_ENTITY,
                    ref error,
                    ..
                } => !error.contains("compute time quota of non-primary branches is exceeded"),
                // locked can be returned when the endpoint was in transition
                // or when quotas are exceeded. don't retry when quotas are exceeded
                ConsoleError {
                    http_status_code: http::StatusCode::LOCKED,
                    ref error,
                    ..
                } => {
                    !error.contains("quota exceeded")
                        && !error.contains("the limit for current plan reached")
                }
                _ => false,
            };
        }

        // retry if the response has a retry delay
        if let Some(retry_info) = self
            .status
            .as_ref()
            .and_then(|s| s.details.retry_info.as_ref())
        {
            retry_info.retry_delay_ms > 0
        } else {
            false
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Status {
    pub code: Box<str>,
    pub message: Box<str>,
    pub details: Details,
}

#[derive(Debug, Deserialize)]
pub struct Details {
    pub error_info: Option<ErrorInfo>,
    pub retry_info: Option<RetryInfo>,
    pub user_facing_message: Option<UserFacingMessage>,
}

#[derive(Debug, Deserialize)]
pub struct ErrorInfo {
    pub reason: Reason,
    // Schema could also have `metadata` field, but it's not structured. Skip it for now.
}

#[derive(Clone, Copy, Debug, Deserialize, Default)]
pub enum Reason {
    #[serde(rename = "ROLE_PROTECTED")]
    RoleProtected,
    #[serde(rename = "RESOURCE_NOT_FOUND")]
    ResourceNotFound,
    #[serde(rename = "PROJECT_NOT_FOUND")]
    ProjectNotFound,
    #[serde(rename = "ENDPOINT_NOT_FOUND")]
    EndpointNotFound,
    #[serde(rename = "BRANCH_NOT_FOUND")]
    BranchNotFound,
    #[serde(rename = "RATE_LIMIT_EXCEEDED")]
    RateLimitExceeded,
    #[serde(rename = "NON_PRIMARY_BRANCH_COMPUTE_TIME_EXCEEDED")]
    NonPrimaryBranchComputeTimeExceeded,
    #[serde(rename = "ACTIVE_TIME_QUOTA_EXCEEDED")]
    ActiveTimeQuotaExceeded,
    #[serde(rename = "COMPUTE_TIME_QUOTA_EXCEEDED")]
    ComputeTimeQuotaExceeded,
    #[serde(rename = "WRITTEN_DATA_QUOTA_EXCEEDED")]
    WrittenDataQuotaExceeded,
    #[serde(rename = "DATA_TRANSFER_QUOTA_EXCEEDED")]
    DataTransferQuotaExceeded,
    #[serde(rename = "LOGICAL_SIZE_QUOTA_EXCEEDED")]
    LogicalSizeQuotaExceeded,
    #[default]
    #[serde(other)]
    Unknown,
}

impl Reason {
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Reason::ResourceNotFound
                | Reason::ProjectNotFound
                | Reason::EndpointNotFound
                | Reason::BranchNotFound
        )
    }
}

#[derive(Debug, Deserialize)]
pub struct RetryInfo {
    pub retry_delay_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct UserFacingMessage {
    pub message: Box<str>,
}

/// Response which holds client's auth secret, e.g. [`crate::scram::ServerSecret`].
/// Returned by the `/proxy_get_role_secret` API method.
#[derive(Deserialize)]
pub struct GetRoleSecret {
    pub role_secret: Box<str>,
    pub allowed_ips: Option<Vec<IpPattern>>,
    pub project_id: Option<ProjectIdInt>,
}

// Manually implement debug to omit sensitive info.
impl fmt::Debug for GetRoleSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GetRoleSecret").finish_non_exhaustive()
    }
}

/// Response which holds compute node's `host:port` pair.
/// Returned by the `/proxy_wake_compute` API method.
#[derive(Debug, Deserialize)]
pub struct WakeCompute {
    pub address: Box<str>,
    pub aux: MetricsAuxInfo,
}

/// Async response which concludes the link auth flow.
/// Also known as `kickResponse` in the console.
#[derive(Debug, Deserialize)]
pub struct KickSession<'a> {
    /// Session ID is assigned by the proxy.
    pub session_id: &'a str,

    /// Compute node connection params.
    #[serde(deserialize_with = "KickSession::parse_db_info")]
    pub result: DatabaseInfo,
}

impl KickSession<'_> {
    fn parse_db_info<'de, D>(des: D) -> Result<DatabaseInfo, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        enum Wrapper {
            // Currently, console only reports `Success`.
            // `Failure(String)` used to be here... RIP.
            Success(DatabaseInfo),
        }

        Wrapper::deserialize(des).map(|x| match x {
            Wrapper::Success(info) => info,
        })
    }
}

/// Compute node connection params.
#[derive(Deserialize)]
pub struct DatabaseInfo {
    pub host: Box<str>,
    pub port: u16,
    pub dbname: Box<str>,
    pub user: Box<str>,
    /// Console always provides a password, but it might
    /// be inconvenient for debug with local PG instance.
    pub password: Option<Box<str>>,
    pub aux: MetricsAuxInfo,
}

// Manually implement debug to omit sensitive info.
impl fmt::Debug for DatabaseInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DatabaseInfo")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("dbname", &self.dbname)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

/// Various labels for prometheus metrics.
/// Also known as `ProxyMetricsAuxInfo` in the console.
#[derive(Debug, Deserialize, Clone)]
pub struct MetricsAuxInfo {
    pub endpoint_id: EndpointIdInt,
    pub project_id: ProjectIdInt,
    pub branch_id: BranchIdInt,
    #[serde(default)]
    pub cold_start_info: ColdStartInfo,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, FixedCardinalityLabel)]
#[serde(rename_all = "snake_case")]
pub enum ColdStartInfo {
    #[default]
    Unknown,
    /// Compute was already running
    Warm,
    #[serde(rename = "pool_hit")]
    #[label(rename = "pool_hit")]
    /// Compute was not running but there was an available VM
    VmPoolHit,
    #[serde(rename = "pool_miss")]
    #[label(rename = "pool_miss")]
    /// Compute was not running and there were no VMs available
    VmPoolMiss,

    // not provided by control plane
    /// Connection available from HTTP pool
    HttpPoolHit,
    /// Cached connection info
    WarmCached,
}

impl ColdStartInfo {
    pub fn as_str(&self) -> &'static str {
        match self {
            ColdStartInfo::Unknown => "unknown",
            ColdStartInfo::Warm => "warm",
            ColdStartInfo::VmPoolHit => "pool_hit",
            ColdStartInfo::VmPoolMiss => "pool_miss",
            ColdStartInfo::HttpPoolHit => "http_pool_hit",
            ColdStartInfo::WarmCached => "warm_cached",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dummy_aux() -> serde_json::Value {
        json!({
            "endpoint_id": "endpoint",
            "project_id": "project",
            "branch_id": "branch",
            "cold_start_info": "unknown",
        })
    }

    #[test]
    fn parse_kick_session() -> anyhow::Result<()> {
        // This is what the console's kickResponse looks like.
        let json = json!({
            "session_id": "deadbeef",
            "result": {
                "Success": {
                    "host": "localhost",
                    "port": 5432,
                    "dbname": "postgres",
                    "user": "john_doe",
                    "password": "password",
                    "aux": dummy_aux(),
                }
            }
        });
        let _: KickSession = serde_json::from_str(&json.to_string())?;

        Ok(())
    }

    #[test]
    fn parse_db_info() -> anyhow::Result<()> {
        // with password
        let _: DatabaseInfo = serde_json::from_value(json!({
            "host": "localhost",
            "port": 5432,
            "dbname": "postgres",
            "user": "john_doe",
            "password": "password",
            "aux": dummy_aux(),
        }))?;

        // without password
        let _: DatabaseInfo = serde_json::from_value(json!({
            "host": "localhost",
            "port": 5432,
            "dbname": "postgres",
            "user": "john_doe",
            "aux": dummy_aux(),
        }))?;

        // new field (forward compatibility)
        let _: DatabaseInfo = serde_json::from_value(json!({
            "host": "localhost",
            "port": 5432,
            "dbname": "postgres",
            "user": "john_doe",
            "project": "hello_world",
            "N.E.W": "forward compatibility check",
            "aux": dummy_aux(),
        }))?;

        Ok(())
    }

    #[test]
    fn parse_wake_compute() -> anyhow::Result<()> {
        let json = json!({
            "address": "0.0.0.0",
            "aux": dummy_aux(),
        });
        let _: WakeCompute = serde_json::from_str(&json.to_string())?;
        Ok(())
    }

    #[test]
    fn parse_get_role_secret() -> anyhow::Result<()> {
        // Empty `allowed_ips` field.
        let json = json!({
            "role_secret": "secret",
        });
        let _: GetRoleSecret = serde_json::from_str(&json.to_string())?;
        let json = json!({
            "role_secret": "secret",
            "allowed_ips": ["8.8.8.8"],
        });
        let _: GetRoleSecret = serde_json::from_str(&json.to_string())?;
        let json = json!({
            "role_secret": "secret",
            "allowed_ips": ["8.8.8.8"],
            "project_id": "project",
        });
        let _: GetRoleSecret = serde_json::from_str(&json.to_string())?;

        Ok(())
    }
}
