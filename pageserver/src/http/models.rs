use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use zenith_utils::{
    lsn::Lsn,
    zid::{ZNodeId, ZTenantId, ZTimelineId},
};

use crate::timelines::{LocalTimelineInfo, TimelineInfo};

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub new_timeline_id: Option<ZTimelineId>,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_timeline_id: Option<ZTimelineId>,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ancestor_start_lsn: Option<Lsn>,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct TenantCreateRequest {
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub new_tenant_id: Option<ZTenantId>,
}

#[serde_as]
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantCreateResponse(#[serde_as(as = "DisplayFromStr")] pub ZTenantId);

#[derive(Clone)]
pub enum TimelineInfoV1 {
    Local {
        timeline_id: ZTimelineId,
        tenant_id: ZTenantId,
        last_record_lsn: Lsn,
        prev_record_lsn: Option<Lsn>,
        ancestor_timeline_id: Option<ZTimelineId>,
        ancestor_lsn: Option<Lsn>,
        disk_consistent_lsn: Lsn,
        current_logical_size: Option<usize>,
        current_logical_size_non_incremental: Option<usize>,
    },
    Remote {
        timeline_id: ZTimelineId,
        tenant_id: ZTenantId,
        disk_consistent_lsn: Lsn,
    },
}

#[serde_as]
#[derive(Serialize, Deserialize)]
pub struct TimelineInfoResponseV1 {
    pub kind: String,
    #[serde_as(as = "DisplayFromStr")]
    timeline_id: ZTimelineId,
    #[serde_as(as = "DisplayFromStr")]
    tenant_id: ZTenantId,
    #[serde_as(as = "DisplayFromStr")]
    disk_consistent_lsn: Lsn,
    #[serde_as(as = "Option<DisplayFromStr>")]
    last_record_lsn: Option<Lsn>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    prev_record_lsn: Option<Lsn>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    ancestor_timeline_id: Option<ZTimelineId>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    ancestor_lsn: Option<Lsn>,
    current_logical_size: Option<usize>,
    current_logical_size_non_incremental: Option<usize>,
}

impl From<TimelineInfoV1> for TimelineInfoResponseV1 {
    fn from(other: TimelineInfoV1) -> Self {
        match other {
            TimelineInfoV1::Local {
                timeline_id,
                tenant_id,
                last_record_lsn,
                prev_record_lsn,
                ancestor_timeline_id,
                ancestor_lsn,
                disk_consistent_lsn,
                current_logical_size,
                current_logical_size_non_incremental,
            } => TimelineInfoResponseV1 {
                kind: "Local".to_owned(),
                timeline_id,
                tenant_id,
                disk_consistent_lsn,
                last_record_lsn: Some(last_record_lsn),
                prev_record_lsn,
                ancestor_timeline_id,
                ancestor_lsn,
                current_logical_size,
                current_logical_size_non_incremental,
            },
            TimelineInfoV1::Remote {
                timeline_id,
                tenant_id,
                disk_consistent_lsn,
            } => TimelineInfoResponseV1 {
                kind: "Remote".to_owned(),
                timeline_id,
                tenant_id,
                disk_consistent_lsn,
                last_record_lsn: None,
                prev_record_lsn: None,
                ancestor_timeline_id: None,
                ancestor_lsn: None,
                current_logical_size: None,
                current_logical_size_non_incremental: None,
            },
        }
    }
}

impl TryFrom<TimelineInfoResponseV1> for TimelineInfoV1 {
    type Error = anyhow::Error;

    fn try_from(other: TimelineInfoResponseV1) -> anyhow::Result<Self> {
        Ok(match other.kind.as_str() {
            "Local" => TimelineInfoV1::Local {
                timeline_id: other.timeline_id,
                tenant_id: other.tenant_id,
                last_record_lsn: other.last_record_lsn.ok_or(anyhow::anyhow!(
                    "Local timeline should have last_record_lsn"
                ))?,
                prev_record_lsn: other.prev_record_lsn,
                ancestor_timeline_id: other.ancestor_timeline_id.map(ZTimelineId::from),
                ancestor_lsn: other.ancestor_lsn,
                disk_consistent_lsn: other.disk_consistent_lsn,
                current_logical_size: other.current_logical_size,
                current_logical_size_non_incremental: other.current_logical_size_non_incremental,
            },
            "Remote" => TimelineInfoV1::Remote {
                timeline_id: other.timeline_id,
                tenant_id: other.tenant_id,
                disk_consistent_lsn: other.disk_consistent_lsn,
            },
            unknown => anyhow::bail!("Unknown timeline kind: {}", unknown),
        })
    }
}

fn from_local(
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    local: &LocalTimelineInfo,
) -> TimelineInfoV1 {
    TimelineInfoV1::Local {
        timeline_id,
        tenant_id,
        last_record_lsn: local.last_record_lsn,
        prev_record_lsn: local.prev_record_lsn,
        ancestor_timeline_id: local.ancestor_timeline_id.map(ZTimelineId::from),
        ancestor_lsn: local.ancestor_lsn,
        disk_consistent_lsn: local.disk_consistent_lsn,
        current_logical_size: local.current_logical_size,
        current_logical_size_non_incremental: local.current_logical_size_non_incremental,
    }
}

impl From<TimelineInfo> for TimelineInfoV1 {
    fn from(t: TimelineInfo) -> Self {
        match (t.local.as_ref(), t.remote.as_ref()) {
            (None, None) => unreachable!(),
            (None, Some(remote)) => TimelineInfoV1::Remote {
                timeline_id: t.timeline_id,
                tenant_id: t.tenant_id,
                disk_consistent_lsn: remote.remote_consistent_lsn.unwrap_or(Lsn(0)),
            },
            (Some(local), None) => from_local(t.tenant_id, t.timeline_id, local),
            (Some(local), Some(_)) => from_local(t.tenant_id, t.timeline_id, local),
        }
    }
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub id: ZNodeId,
}
