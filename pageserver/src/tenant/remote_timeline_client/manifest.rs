use chrono::NaiveDateTime;
use pageserver_api::shard::ShardStripeSize;
use serde::{Deserialize, Serialize};
use utils::id::TimelineId;
use utils::lsn::Lsn;

/// Tenant-shard scoped manifest
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantManifest {
    /// Debugging aid describing the version of this manifest.
    /// Can also be used for distinguishing breaking changes later on.
    pub version: usize,

    /// This tenant's stripe size. This is only advisory, and used to recover tenant data from
    /// remote storage. The autoritative source is the storage controller. If None, assume the
    /// original default value of 32768 blocks (256 MB).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stripe_size: Option<ShardStripeSize>,

    /// The list of offloaded timelines together with enough information
    /// to not have to actually load them.
    ///
    /// Note: the timelines mentioned in this list might be deleted, i.e.
    /// we don't hold an invariant that the references aren't dangling.
    /// Existence of index-part.json is the actual indicator of timeline existence.
    #[serde(default)]
    pub offloaded_timelines: Vec<OffloadedTimelineManifest>,
}

/// The remote level representation of an offloaded timeline.
///
/// Very similar to [`pageserver_api::models::OffloadedTimelineInfo`],
/// but the two datastructures serve different needs, this is for a persistent disk format
/// that must be backwards compatible, while the other is only for informative purposes.
#[derive(Clone, Debug, Serialize, Deserialize, Copy, PartialEq, Eq)]
pub struct OffloadedTimelineManifest {
    pub timeline_id: TimelineId,
    /// Whether the timeline has a parent it has been branched off from or not
    pub ancestor_timeline_id: Option<TimelineId>,
    /// Whether to retain the branch lsn at the ancestor or not
    pub ancestor_retain_lsn: Option<Lsn>,
    /// The time point when the timeline was archived
    pub archived_at: NaiveDateTime,
}

/// The version number is incremented on changes, even non-breaking ones. We do not use
/// deny_unknown_fields, so new fields are not breaking.
///
/// When making changes, also add a compatibility test case below.
///
/// 2: +stripe_size
pub const LATEST_TENANT_MANIFEST_VERSION: usize = 2;

impl TenantManifest {
    pub(crate) fn empty() -> Self {
        Self {
            version: LATEST_TENANT_MANIFEST_VERSION,
            // NB: set stripe_size to None, not DEFAULT_STRIPE_SIZE. This will trigger a manifest
            // upload with the real stripe size during attach.
            stripe_size: None,
            offloaded_timelines: vec![],
        }
    }
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice::<Self>(bytes)
    }

    pub(crate) fn to_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use utils::id::TimelineId;

    use super::*;

    /// Empty manifests should be parsed. Version is required.
    #[test]
    fn parse_empty() -> anyhow::Result<()> {
        let json = r#"{
            "version": 0
        }"#;
        let expected = TenantManifest {
            version: 0,
            stripe_size: None,
            offloaded_timelines: Vec::new(),
        };
        assert_eq!(expected, TenantManifest::from_json_bytes(json.as_bytes())?);
        Ok(())
    }

    /// Unknown fields should be ignored, for forwards compatibility.
    #[test]
    fn parse_unknown_fields() -> anyhow::Result<()> {
        let json = r#"{
            "version": 1,
            "foo": "bar"
        }"#;
        let expected = TenantManifest {
            version: 1,
            stripe_size: None,
            offloaded_timelines: Vec::new(),
        };
        assert_eq!(expected, TenantManifest::from_json_bytes(json.as_bytes())?);
        Ok(())
    }

    /// v1 manifests should be parsed, for backwards compatibility.
    #[test]
    fn parse_v1() -> anyhow::Result<()> {
        let json = r#"{
            "version": 1,
            "offloaded_timelines": [
                {
                    "timeline_id": "5c4df612fd159e63c1b7853fe94d97da",
                    "archived_at": "2025-03-07T11:07:11.373105434"
                },
                {
                    "timeline_id": "f3def5823ad7080d2ea538d8e12163fa",
                    "ancestor_timeline_id": "5c4df612fd159e63c1b7853fe94d97da",
                    "ancestor_retain_lsn": "0/1F79038",
                    "archived_at": "2025-03-05T11:10:22.257901390"
                }
            ]
        }"#;
        let expected = TenantManifest {
            version: 1,
            stripe_size: None,
            offloaded_timelines: vec![
                OffloadedTimelineManifest {
                    timeline_id: TimelineId::from_str("5c4df612fd159e63c1b7853fe94d97da")?,
                    ancestor_timeline_id: None,
                    ancestor_retain_lsn: None,
                    archived_at: NaiveDateTime::from_str("2025-03-07T11:07:11.373105434")?,
                },
                OffloadedTimelineManifest {
                    timeline_id: TimelineId::from_str("f3def5823ad7080d2ea538d8e12163fa")?,
                    ancestor_timeline_id: Some(TimelineId::from_str(
                        "5c4df612fd159e63c1b7853fe94d97da",
                    )?),
                    ancestor_retain_lsn: Some(Lsn::from_str("0/1F79038")?),
                    archived_at: NaiveDateTime::from_str("2025-03-05T11:10:22.257901390")?,
                },
            ],
        };
        assert_eq!(expected, TenantManifest::from_json_bytes(json.as_bytes())?);
        Ok(())
    }

    /// v2 manifests should be parsed, for backwards compatibility.
    #[test]
    fn parse_v2() -> anyhow::Result<()> {
        let json = r#"{
            "version": 1,
            "stripe_size": 32768,
            "offloaded_timelines": [
                {
                    "timeline_id": "5c4df612fd159e63c1b7853fe94d97da",
                    "archived_at": "2025-03-07T11:07:11.373105434"
                },
                {
                    "timeline_id": "f3def5823ad7080d2ea538d8e12163fa",
                    "ancestor_timeline_id": "5c4df612fd159e63c1b7853fe94d97da",
                    "ancestor_retain_lsn": "0/1F79038",
                    "archived_at": "2025-03-05T11:10:22.257901390"
                }
            ]
        }"#;
        let expected = TenantManifest {
            version: 1,
            stripe_size: Some(ShardStripeSize(32768)),
            offloaded_timelines: vec![
                OffloadedTimelineManifest {
                    timeline_id: TimelineId::from_str("5c4df612fd159e63c1b7853fe94d97da")?,
                    ancestor_timeline_id: None,
                    ancestor_retain_lsn: None,
                    archived_at: NaiveDateTime::from_str("2025-03-07T11:07:11.373105434")?,
                },
                OffloadedTimelineManifest {
                    timeline_id: TimelineId::from_str("f3def5823ad7080d2ea538d8e12163fa")?,
                    ancestor_timeline_id: Some(TimelineId::from_str(
                        "5c4df612fd159e63c1b7853fe94d97da",
                    )?),
                    ancestor_retain_lsn: Some(Lsn::from_str("0/1F79038")?),
                    archived_at: NaiveDateTime::from_str("2025-03-05T11:10:22.257901390")?,
                },
            ],
        };
        assert_eq!(expected, TenantManifest::from_json_bytes(json.as_bytes())?);
        Ok(())
    }
}
