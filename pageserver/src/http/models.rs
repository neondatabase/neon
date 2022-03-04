use crate::timelines::TimelineInfo;
use anyhow::{anyhow, bail, Context};
use serde::{Deserialize, Serialize};
use zenith_utils::{
    lsn::Lsn,
    zid::{HexZTenantId, HexZTimelineId, ZNodeId, ZTenantId, ZTimelineId},
};

#[derive(Serialize, Deserialize)]
pub struct TimelineCreateRequest {
    pub new_timeline_id: Option<HexZTimelineId>,
    pub ancestor_timeline_id: Option<HexZTimelineId>,
    pub ancestor_start_lsn: Option<Lsn>,
}

#[derive(Serialize, Deserialize)]
pub struct TenantCreateRequest {
    pub new_tenant_id: Option<HexZTenantId>,
}

#[derive(Serialize, Deserialize)]
pub struct TimelineInfoResponse {
    pub kind: String,
    #[serde(with = "hex")]
    timeline_id: ZTimelineId,
    #[serde(with = "hex")]
    tenant_id: ZTenantId,
    disk_consistent_lsn: String,
    last_record_lsn: Option<String>,
    prev_record_lsn: Option<String>,
    ancestor_timeline_id: Option<HexZTimelineId>,
    ancestor_lsn: Option<String>,
    current_logical_size: Option<usize>,
    current_logical_size_non_incremental: Option<usize>,
}

impl From<TimelineInfo> for TimelineInfoResponse {
    fn from(other: TimelineInfo) -> Self {
        match other {
            TimelineInfo::Local {
                timeline_id,
                tenant_id,
                last_record_lsn,
                prev_record_lsn,
                ancestor_timeline_id,
                ancestor_lsn,
                disk_consistent_lsn,
                current_logical_size,
                current_logical_size_non_incremental,
            } => TimelineInfoResponse {
                kind: "Local".to_owned(),
                timeline_id,
                tenant_id,
                disk_consistent_lsn: disk_consistent_lsn.to_string(),
                last_record_lsn: Some(last_record_lsn.to_string()),
                prev_record_lsn: Some(prev_record_lsn.to_string()),
                ancestor_timeline_id: ancestor_timeline_id.map(HexZTimelineId::from),
                ancestor_lsn: ancestor_lsn.map(|lsn| lsn.to_string()),
                current_logical_size: Some(current_logical_size),
                current_logical_size_non_incremental,
            },
            TimelineInfo::Remote {
                timeline_id,
                tenant_id,
                disk_consistent_lsn,
            } => TimelineInfoResponse {
                kind: "Remote".to_owned(),
                timeline_id,
                tenant_id,
                disk_consistent_lsn: disk_consistent_lsn.to_string(),
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

impl TryFrom<TimelineInfoResponse> for TimelineInfo {
    type Error = anyhow::Error;

    fn try_from(other: TimelineInfoResponse) -> anyhow::Result<Self> {
        let parse_lsn_hex_string = |lsn_string: String| {
            lsn_string
                .parse::<Lsn>()
                .with_context(|| format!("Failed to parse Lsn as hex string from '{}'", lsn_string))
        };

        let disk_consistent_lsn = parse_lsn_hex_string(other.disk_consistent_lsn)?;
        Ok(match other.kind.as_str() {
            "Local" => TimelineInfo::Local {
                timeline_id: other.timeline_id,
                tenant_id: other.tenant_id,
                last_record_lsn: other
                    .last_record_lsn
                    .ok_or(anyhow!("Local timeline should have last_record_lsn"))
                    .and_then(parse_lsn_hex_string)?,
                prev_record_lsn: other
                    .prev_record_lsn
                    .ok_or(anyhow!("Local timeline should have prev_record_lsn"))
                    .and_then(parse_lsn_hex_string)?,
                ancestor_timeline_id: other.ancestor_timeline_id.map(ZTimelineId::from),
                ancestor_lsn: other.ancestor_lsn.map(parse_lsn_hex_string).transpose()?,
                disk_consistent_lsn,
                current_logical_size: other.current_logical_size.ok_or(anyhow!("No "))?,
                current_logical_size_non_incremental: other.current_logical_size_non_incremental,
            },
            "Remote" => TimelineInfo::Remote {
                timeline_id: other.timeline_id,
                tenant_id: other.tenant_id,
                disk_consistent_lsn,
            },
            unknown => bail!("Unknown timeline kind: {}", unknown),
        })
    }
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub id: ZNodeId,
}
