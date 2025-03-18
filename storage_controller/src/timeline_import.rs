use std::{collections::HashMap, str::FromStr};

use serde::{Deserialize, Serialize};

use pageserver_api::models::ShardImportStatus;
use utils::{
    id::{TenantId, TimelineId},
    shard::ShardIndex,
};

use crate::persistence::TimelineImportPersistence;

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ShardImportStatuses(HashMap<ShardIndex, ShardImportStatus>);

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

pub(crate) struct TimelineImport {
    pub(crate) tenant_id: TenantId,
    pub(crate) timeline_id: TimelineId,
    pub(crate) shard_statuses: ShardImportStatuses,
}

impl TimelineImport {
    pub(crate) fn to_persistent(&self) -> TimelineImportPersistence {
        TimelineImportPersistence {
            tenant_id: self.tenant_id.to_string(),
            timeline_id: self.timeline_id.to_string(),
            shard_statuses: serde_json::to_value(self.shard_statuses.clone()).unwrap(),
        }
    }
}
