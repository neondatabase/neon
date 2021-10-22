//! Every image of a certain timeline from [`crate::layered_repository::LayeredRepository`]
//! has a metadata that needs to be stored persistently.
//!
//! Later, the file gets is used in [`crate::relish_storage::storage_sync`] as a part of
//! external storage import and export operations.
//!
//! The module contains all structs and related helper methods related to timeline metadata.

use std::{convert::TryInto, path::PathBuf};

use anyhow::ensure;
use zenith_utils::{
    bin_ser::BeSer,
    lsn::Lsn,
    zid::{ZTenantId, ZTimelineId},
};

use crate::{
    layered_repository::{METADATA_CHECKSUM_SIZE, METADATA_MAX_DATA_SIZE, METADATA_MAX_SAFE_SIZE},
    PageServerConf,
};

/// The name of the metadata file pageserver creates per timeline.
pub const METADATA_FILE_NAME: &str = "metadata";

/// Metadata stored on disk for each timeline
///
/// The fields correspond to the values we hold in memory, in LayeredTimeline.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimelineMetadata {
    disk_consistent_lsn: Lsn,
    // This is only set if we know it. We track it in memory when the page
    // server is running, but we only track the value corresponding to
    // 'last_record_lsn', not 'disk_consistent_lsn' which can lag behind by a
    // lot. We only store it in the metadata file when we flush *all* the
    // in-memory data so that 'last_record_lsn' is the same as
    // 'disk_consistent_lsn'.  That's OK, because after page server restart, as
    // soon as we reprocess at least one record, we will have a valid
    // 'prev_record_lsn' value in memory again. This is only really needed when
    // doing a clean shutdown, so that there is no more WAL beyond
    // 'disk_consistent_lsn'
    prev_record_lsn: Option<Lsn>,
    ancestor_timeline: Option<ZTimelineId>,
    ancestor_lsn: Lsn,
}

/// Points to a place in pageserver's local directory,
/// where certain timeline's metadata file should be located.
pub fn metadata_path(
    conf: &'static PageServerConf,
    timelineid: ZTimelineId,
    tenantid: ZTenantId,
) -> PathBuf {
    conf.timeline_path(&timelineid, &tenantid)
        .join(METADATA_FILE_NAME)
}

impl TimelineMetadata {
    pub fn new(
        disk_consistent_lsn: Lsn,
        prev_record_lsn: Option<Lsn>,
        ancestor_timeline: Option<ZTimelineId>,
        ancestor_lsn: Lsn,
    ) -> Self {
        Self {
            disk_consistent_lsn,
            prev_record_lsn,
            ancestor_timeline,
            ancestor_lsn,
        }
    }

    pub fn from_bytes(metadata_bytes: &[u8]) -> anyhow::Result<Self> {
        ensure!(
            metadata_bytes.len() == METADATA_MAX_SAFE_SIZE,
            "metadata bytes size is wrong"
        );

        let data = &metadata_bytes[..METADATA_MAX_DATA_SIZE];
        let calculated_checksum = crc32c::crc32c(data);

        let checksum_bytes: &[u8; METADATA_CHECKSUM_SIZE] =
            metadata_bytes[METADATA_MAX_DATA_SIZE..].try_into()?;
        let expected_checksum = u32::from_le_bytes(*checksum_bytes);
        ensure!(
            calculated_checksum == expected_checksum,
            "metadata checksum mismatch"
        );

        let data = TimelineMetadata::from(serialize::DeTimelineMetadata::des_prefix(data)?);
        assert!(data.disk_consistent_lsn.is_aligned());

        Ok(data)
    }

    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let serializeable_metadata = serialize::SeTimelineMetadata::from(self);
        let mut metadata_bytes = serialize::SeTimelineMetadata::ser(&serializeable_metadata)?;
        assert!(metadata_bytes.len() <= METADATA_MAX_DATA_SIZE);
        metadata_bytes.resize(METADATA_MAX_SAFE_SIZE, 0u8);

        let checksum = crc32c::crc32c(&metadata_bytes[..METADATA_MAX_DATA_SIZE]);
        metadata_bytes[METADATA_MAX_DATA_SIZE..].copy_from_slice(&u32::to_le_bytes(checksum));
        Ok(metadata_bytes)
    }

    /// [`Lsn`] that corresponds to the corresponding timeline directory
    /// contents, stored locally in the pageserver workdir.
    pub fn disk_consistent_lsn(&self) -> Lsn {
        self.disk_consistent_lsn
    }

    pub fn prev_record_lsn(&self) -> Option<Lsn> {
        self.prev_record_lsn
    }

    pub fn ancestor_timeline(&self) -> Option<ZTimelineId> {
        self.ancestor_timeline
    }

    pub fn ancestor_lsn(&self) -> Lsn {
        self.ancestor_lsn
    }
}

/// This module is for direct conversion of metadata to bytes and back.
/// For a certain metadata, besides the conversion a few verification steps has to
/// be done, so all serde derives are hidden from the user, to avoid accidental
/// verification-less metadata creation.
mod serialize {
    use serde::{Deserialize, Serialize};
    use zenith_utils::{lsn::Lsn, zid::ZTimelineId};

    use super::TimelineMetadata;

    #[derive(Serialize)]
    pub(super) struct SeTimelineMetadata<'a> {
        disk_consistent_lsn: &'a Lsn,
        prev_record_lsn: &'a Option<Lsn>,
        ancestor_timeline: &'a Option<ZTimelineId>,
        ancestor_lsn: &'a Lsn,
    }

    impl<'a> From<&'a TimelineMetadata> for SeTimelineMetadata<'a> {
        fn from(other: &'a TimelineMetadata) -> Self {
            Self {
                disk_consistent_lsn: &other.disk_consistent_lsn,
                prev_record_lsn: &other.prev_record_lsn,
                ancestor_timeline: &other.ancestor_timeline,
                ancestor_lsn: &other.ancestor_lsn,
            }
        }
    }

    #[derive(Deserialize)]
    pub(super) struct DeTimelineMetadata {
        disk_consistent_lsn: Lsn,
        prev_record_lsn: Option<Lsn>,
        ancestor_timeline: Option<ZTimelineId>,
        ancestor_lsn: Lsn,
    }

    impl From<DeTimelineMetadata> for TimelineMetadata {
        fn from(other: DeTimelineMetadata) -> Self {
            Self {
                disk_consistent_lsn: other.disk_consistent_lsn,
                prev_record_lsn: other.prev_record_lsn,
                ancestor_timeline: other.ancestor_timeline,
                ancestor_lsn: other.ancestor_lsn,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::repository::repo_harness::TIMELINE_ID;

    use super::*;

    #[test]
    fn metadata_serializes_correctly() {
        let original_metadata = TimelineMetadata {
            disk_consistent_lsn: Lsn(0x200),
            prev_record_lsn: Some(Lsn(0x100)),
            ancestor_timeline: Some(TIMELINE_ID),
            ancestor_lsn: Lsn(0),
        };

        let metadata_bytes = original_metadata
            .to_bytes()
            .expect("Should serialize correct metadata to bytes");

        let deserialized_metadata = TimelineMetadata::from_bytes(&metadata_bytes)
            .expect("Should deserialize its own bytes");

        assert_eq!(
            deserialized_metadata, original_metadata,
            "Metadata that was serialized to bytes and deserialized back should not change"
        );
    }
}
