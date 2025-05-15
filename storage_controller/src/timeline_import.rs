use std::time::Duration;
use std::{collections::HashMap, str::FromStr};

use http_utils::error::ApiError;
use reqwest::Method;
use serde::{Deserialize, Serialize};

use pageserver_api::models::ShardImportStatus;
use tokio_util::sync::CancellationToken;
use utils::{
    id::{TenantId, TimelineId},
    shard::ShardIndex,
};

use crate::{persistence::TimelineImportPersistence, service::Config};

#[derive(Deserialize, Serialize, PartialEq, Eq)]
pub(crate) enum TimelineImportState {
    Importing,
    Idle,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct ShardImportStatuses(pub(crate) HashMap<ShardIndex, ShardImportStatus>);

impl ShardImportStatuses {
    pub(crate) fn new(shards: Vec<ShardIndex>) -> Self {
        ShardImportStatuses(
            shards
                .into_iter()
                .map(|ts_id| (ts_id, ShardImportStatus::InProgress))
                .collect(),
        )
    }
}

#[derive(Debug)]
pub(crate) struct TimelineImport {
    pub(crate) tenant_id: TenantId,
    pub(crate) timeline_id: TimelineId,
    pub(crate) shard_statuses: ShardImportStatuses,
}

pub(crate) enum TimelineImportUpdateFollowUp {
    Persist,
    None,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum TimelineImportFinalizeError {
    #[error("Shut down interrupted import finalize")]
    ShuttingDown,
    #[error("Mismatched shard detected during import finalize: {0}")]
    MismatchedShards(ShardIndex),
}

pub(crate) enum TimelineImportUpdateError {
    ImportNotFound {
        tenant_id: TenantId,
        timeline_id: TimelineId,
    },
    MismatchedShards,
    UnexpectedUpdate,
}

impl From<TimelineImportUpdateError> for ApiError {
    fn from(err: TimelineImportUpdateError) -> ApiError {
        match err {
            TimelineImportUpdateError::ImportNotFound {
                tenant_id,
                timeline_id,
            } => ApiError::NotFound(
                anyhow::anyhow!("Import for {tenant_id}/{timeline_id} not found").into(),
            ),
            TimelineImportUpdateError::MismatchedShards => {
                ApiError::InternalServerError(anyhow::anyhow!(
                    "Import shards do not match update request, likely a shard split happened during import, this is a bug"
                ))
            }
            TimelineImportUpdateError::UnexpectedUpdate => {
                ApiError::InternalServerError(anyhow::anyhow!("Update request is unexpected"))
            }
        }
    }
}

impl TimelineImport {
    pub(crate) fn from_persistent(persistent: TimelineImportPersistence) -> anyhow::Result<Self> {
        let tenant_id = TenantId::from_str(persistent.tenant_id.as_str())?;
        let timeline_id = TimelineId::from_str(persistent.timeline_id.as_str())?;
        let shard_statuses = serde_json::from_value(persistent.shard_statuses)?;

        Ok(TimelineImport {
            tenant_id,
            timeline_id,
            shard_statuses,
        })
    }

    pub(crate) fn to_persistent(&self) -> TimelineImportPersistence {
        TimelineImportPersistence {
            tenant_id: self.tenant_id.to_string(),
            timeline_id: self.timeline_id.to_string(),
            shard_statuses: serde_json::to_value(self.shard_statuses.clone()).unwrap(),
        }
    }

    pub(crate) fn update(
        &mut self,
        shard: ShardIndex,
        status: ShardImportStatus,
    ) -> Result<TimelineImportUpdateFollowUp, TimelineImportUpdateError> {
        use std::collections::hash_map::Entry::*;

        match self.shard_statuses.0.entry(shard) {
            Occupied(mut occ) => {
                let crnt = occ.get_mut();
                if *crnt == status {
                    Ok(TimelineImportUpdateFollowUp::None)
                } else if crnt.is_terminal() && *crnt != status {
                    Err(TimelineImportUpdateError::UnexpectedUpdate)
                } else {
                    *crnt = status;
                    Ok(TimelineImportUpdateFollowUp::Persist)
                }
            }
            Vacant(_) => Err(TimelineImportUpdateError::MismatchedShards),
        }
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.shard_statuses
            .0
            .values()
            .all(|status| status.is_terminal())
    }

    pub(crate) fn completion_error(&self) -> Option<String> {
        assert!(self.is_complete());

        let shard_errors: HashMap<_, _> = self
            .shard_statuses
            .0
            .iter()
            .filter_map(|(shard, status)| {
                if let ShardImportStatus::Error(err) = status {
                    Some((*shard, err.clone()))
                } else {
                    None
                }
            })
            .collect();

        if shard_errors.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&shard_errors).unwrap())
        }
    }
}

pub(crate) type ImportResult = Result<(), String>;

pub(crate) struct UpcallClient {
    authorization_header: Option<String>,
    client: reqwest::Client,
    cancel: CancellationToken,
    base_url: String,
}

const IMPORT_COMPLETE_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Serialize, Deserialize, Debug)]
struct ImportCompleteRequest {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    error: Option<String>,
}

impl UpcallClient {
    pub(crate) fn new(config: &Config, cancel: CancellationToken) -> Self {
        let authorization_header = config
            .control_plane_jwt_token
            .clone()
            .map(|jwt| format!("Bearer {}", jwt));

        let client = reqwest::ClientBuilder::new()
            .timeout(IMPORT_COMPLETE_REQUEST_TIMEOUT)
            .build()
            .expect("Failed to construct HTTP client");

        let base_url = config
            .control_plane_url
            .clone()
            .expect("must be configured");

        Self {
            authorization_header,
            client,
            cancel,
            base_url,
        }
    }

    /// Notify control plane of a completed import
    ///
    /// This method guarantees at least once delivery semantics assuming
    /// eventual cplane availability. The cplane API is idempotent.
    pub(crate) async fn notify_import_complete(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        import_result: ImportResult,
    ) -> anyhow::Result<()> {
        let endpoint = if self.base_url.ends_with('/') {
            format!("{}import_complete", self.base_url)
        } else {
            format!("{}/import_complete", self.base_url)
        };

        let request = self
            .client
            .request(Method::PUT, endpoint)
            .json(&ImportCompleteRequest {
                tenant_id,
                timeline_id,
                error: import_result.err(),
            })
            .timeout(IMPORT_COMPLETE_REQUEST_TIMEOUT);

        let request = if let Some(auth) = &self.authorization_header {
            request.header(reqwest::header::AUTHORIZATION, auth)
        } else {
            request
        };

        const RETRY_DELAY: Duration = Duration::from_secs(1);
        let mut attempt = 1;

        loop {
            if self.cancel.is_cancelled() {
                return Err(anyhow::anyhow!(
                    "Shutting down while notifying cplane of import completion"
                ));
            }

            match request.try_clone().unwrap().send().await {
                Ok(response) if response.status().is_success() => {
                    return Ok(());
                }
                Ok(response) => {
                    tracing::warn!(
                        "Import complete notification failed with status {}, attempt {}",
                        response.status(),
                        attempt
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "Import complete notification failed with error: {}, attempt {}",
                        e,
                        attempt
                    );
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(RETRY_DELAY) => {}
                _ = self.cancel.cancelled() => {
                    return Err(anyhow::anyhow!("Shutting down while notifying cplane of import completion"));
                }
            }
            attempt += 1;
        }
    }
}
