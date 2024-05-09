//! Every image of a certain timeline from [`crate::tenant::Tenant`]
//! has a metadata that needs to be stored persistently.
//!
//! Later, the file gets used in [`remote_timeline_client`] as a part of
//! external storage import and export operations.
//!
//! The module contains all structs and related helper methods related to timeline metadata.
//!
//! [`remote_timeline_client`]: super::remote_timeline_client

use anyhow::ensure;
use serde::{de::Error, Deserialize, Serialize, Serializer};
use utils::bin_ser::SerializeError;
use utils::{bin_ser::BeSer, id::TimelineId, lsn::Lsn};

/// Use special format number to enable backward compatibility.
const METADATA_FORMAT_VERSION: u16 = 5;

/// Previous supported format versions.
const METADATA_OLD_FORMAT_VERSION_V2: u16 = 4;
const METADATA_OLD_FORMAT_VERSION_V1: u16 = 3;

/// Metadata stored on disk for each timeline
///
/// The fields correspond to the values we hold in memory, in Timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineMetadata {
    hdr: TimelineMetadataHeader,
    body: TimelineMetadataBodyV3,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimelineMetadataHeader {
    checksum: u32,       // CRC of serialized metadata body
    size: u16,           // size of serialized metadata
    format_version: u16, // metadata format version (used for compatibility checks)
}
const METADATA_HDR_SIZE: usize = std::mem::size_of::<TimelineMetadataHeader>();

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimelineMetadataBodyV3 {
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
    ancestor_timeline: Option<TimelineId>,
    ancestor_lsn: Lsn,
    latest_gc_cutoff_lsn: Lsn,
    initdb_lsn: Lsn,
    pg_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimelineMetadataBodyV2 {
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
    ancestor_timeline: Option<TimelineId>,
    ancestor_lsn: Lsn,
    latest_gc_cutoff_lsn: Lsn,
    initdb_lsn: Lsn,
    pg_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimelineMetadataBodyV1 {
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
    ancestor_timeline: Option<TimelineId>,
    ancestor_lsn: Lsn,
    latest_gc_cutoff_lsn: Lsn,
    initdb_lsn: Lsn,
}

impl TimelineMetadata {
    pub fn new(
        disk_consistent_lsn: Lsn,
        prev_record_lsn: Option<Lsn>,
        ancestor_timeline: Option<TimelineId>,
        ancestor_lsn: Lsn,
        latest_gc_cutoff_lsn: Lsn,
        initdb_lsn: Lsn,
        pg_version: u32,
    ) -> Self {
        Self {
            hdr: TimelineMetadataHeader {
                checksum: 0,
                size: 0,
                format_version: METADATA_FORMAT_VERSION,
            },
            body: TimelineMetadataBodyV3 {
                disk_consistent_lsn,
                prev_record_lsn,
                ancestor_timeline,
                ancestor_lsn,
                latest_gc_cutoff_lsn,
                initdb_lsn,
                pg_version,
            },
        }
    }

    fn upgrade_timeline_metadata(metadata_bytes: &[u8]) -> anyhow::Result<Self> {
        let mut hdr = TimelineMetadataHeader::des(&metadata_bytes[0..METADATA_HDR_SIZE])?;

        // backward compatible only up to this version
        let body = match hdr.format_version {
            METADATA_OLD_FORMAT_VERSION_V2 => {
                let metadata_size = hdr.size as usize;

                let body: TimelineMetadataBodyV2 =
                    TimelineMetadataBodyV2::des(&metadata_bytes[METADATA_HDR_SIZE..metadata_size])?;

                let body = TimelineMetadataBodyV3 {
                    disk_consistent_lsn: body.disk_consistent_lsn,
                    prev_record_lsn: body.prev_record_lsn,
                    ancestor_timeline: body.ancestor_timeline,
                    ancestor_lsn: body.ancestor_lsn,
                    latest_gc_cutoff_lsn: body.latest_gc_cutoff_lsn,
                    initdb_lsn: body.initdb_lsn,
                    pg_version: body.pg_version,
                };

                hdr.format_version = METADATA_FORMAT_VERSION;
                body
            }
            METADATA_OLD_FORMAT_VERSION_V1 => {
                let metadata_size = hdr.size as usize;

                let body: TimelineMetadataBodyV1 =
                    TimelineMetadataBodyV1::des(&metadata_bytes[METADATA_HDR_SIZE..metadata_size])?;

                let body = TimelineMetadataBodyV3 {
                    disk_consistent_lsn: body.disk_consistent_lsn,
                    prev_record_lsn: body.prev_record_lsn,
                    ancestor_timeline: body.ancestor_timeline,
                    ancestor_lsn: body.ancestor_lsn,
                    latest_gc_cutoff_lsn: body.latest_gc_cutoff_lsn,
                    initdb_lsn: body.initdb_lsn,
                    pg_version: 14, // All timelines created before this version had pg_version 14
                };

                hdr.format_version = METADATA_FORMAT_VERSION;
                body
            }
            _ => {
                anyhow::bail!("unsupported metadata format version {}", hdr.format_version);
            }
        };
        Ok(Self { hdr, body })
    }

    pub fn from_bytes(metadata_bytes: &[u8]) -> anyhow::Result<Self> {
        let hdr = TimelineMetadataHeader::des(&metadata_bytes[0..METADATA_HDR_SIZE])?;

        let metadata_size = hdr.size as usize;
        let calculated_checksum = crc32c::crc32c(&metadata_bytes[METADATA_HDR_SIZE..metadata_size]);
        ensure!(
            hdr.checksum == calculated_checksum,
            "metadata checksum mismatch"
        );

        if hdr.format_version != METADATA_FORMAT_VERSION {
            // If metadata has the old format,
            // upgrade it and return the result
            TimelineMetadata::upgrade_timeline_metadata(metadata_bytes)
        } else {
            let body: TimelineMetadataBodyV3 =
                serde_json::from_slice(&metadata_bytes[METADATA_HDR_SIZE..metadata_size])?;
            ensure!(
                body.disk_consistent_lsn.is_aligned(),
                "disk_consistent_lsn is not aligned"
            );
            Ok(TimelineMetadata { hdr, body })
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        let body_bytes =
            serde_json::to_vec(&self.body).map_err(|e| SerializeError::BadInput(e.into()))?;
        let metadata_size = METADATA_HDR_SIZE + body_bytes.len();
        let hdr = TimelineMetadataHeader {
            size: metadata_size as u16,
            format_version: METADATA_FORMAT_VERSION,
            checksum: crc32c::crc32c(&body_bytes),
        };
        let hdr_bytes = hdr.ser()?;
        let mut metadata_bytes = Vec::new();
        metadata_bytes.extend(hdr_bytes);
        metadata_bytes.extend(body_bytes);
        Ok(metadata_bytes)
    }

    /// [`Lsn`] that corresponds to the corresponding timeline directory
    /// contents, stored locally in the pageserver workdir.
    pub fn disk_consistent_lsn(&self) -> Lsn {
        self.body.disk_consistent_lsn
    }

    pub fn prev_record_lsn(&self) -> Option<Lsn> {
        self.body.prev_record_lsn
    }

    pub fn ancestor_timeline(&self) -> Option<TimelineId> {
        self.body.ancestor_timeline
    }

    pub fn ancestor_lsn(&self) -> Lsn {
        self.body.ancestor_lsn
    }

    /// When reparenting, the `ancestor_lsn` does not change.
    pub fn reparent(&mut self, timeline: &TimelineId) {
        assert!(self.body.ancestor_timeline.is_some());
        // no assertion for redoing this: it's fine, we may have to repeat this multiple times over
        self.body.ancestor_timeline = Some(*timeline);
    }

    pub fn detach_from_ancestor(&mut self, timeline: &TimelineId, ancestor_lsn: &Lsn) {
        if let Some(ancestor) = self.body.ancestor_timeline {
            assert_eq!(ancestor, *timeline);
        }
        if self.body.ancestor_lsn != Lsn(0) {
            assert_eq!(self.body.ancestor_lsn, *ancestor_lsn);
        }
        self.body.ancestor_timeline = None;
        self.body.ancestor_lsn = Lsn(0);
    }

    pub fn latest_gc_cutoff_lsn(&self) -> Lsn {
        self.body.latest_gc_cutoff_lsn
    }

    pub fn initdb_lsn(&self) -> Lsn {
        self.body.initdb_lsn
    }

    pub fn pg_version(&self) -> u32 {
        self.body.pg_version
    }

    // Checksums make it awkward to build a valid instance by hand.  This helper
    // provides a TimelineMetadata with a valid checksum in its header.
    #[cfg(test)]
    pub fn example() -> Self {
        let instance = Self::new(
            "0/16960E8".parse::<Lsn>().unwrap(),
            None,
            None,
            Lsn::from_hex("00000000").unwrap(),
            Lsn::from_hex("00000000").unwrap(),
            Lsn::from_hex("00000000").unwrap(),
            0,
        );
        let bytes = instance.to_bytes().unwrap();
        Self::from_bytes(&bytes).unwrap()
    }

    pub(crate) fn apply(&mut self, update: &MetadataUpdate) {
        self.body.disk_consistent_lsn = update.disk_consistent_lsn;
        self.body.prev_record_lsn = update.prev_record_lsn;
        self.body.latest_gc_cutoff_lsn = update.latest_gc_cutoff_lsn;
    }
}

impl<'de> Deserialize<'de> for TimelineMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Self::from_bytes(bytes.as_slice()).map_err(|e| D::Error::custom(format!("{e}")))
    }
}

impl Serialize for TimelineMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self
            .to_bytes()
            .map_err(|e| serde::ser::Error::custom(format!("{e}")))?;
        bytes.serialize(serializer)
    }
}

/// Parts of the metadata which are regularly modified.
pub(crate) struct MetadataUpdate {
    disk_consistent_lsn: Lsn,
    prev_record_lsn: Option<Lsn>,
    latest_gc_cutoff_lsn: Lsn,
}

impl MetadataUpdate {
    pub(crate) fn new(
        disk_consistent_lsn: Lsn,
        prev_record_lsn: Option<Lsn>,
        latest_gc_cutoff_lsn: Lsn,
    ) -> Self {
        Self {
            disk_consistent_lsn,
            prev_record_lsn,
            latest_gc_cutoff_lsn,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::tenant::harness::TIMELINE_ID;

    // Previous limit on maximum metadata size
    const METADATA_MAX_SIZE: usize = 512;

    #[test]
    fn metadata_serializes_correctly() {
        let original_metadata = TimelineMetadata::new(
            Lsn(0x200),
            Some(Lsn(0x100)),
            Some(TIMELINE_ID),
            Lsn(0),
            Lsn(0),
            Lsn(0),
            // Any version will do here, so use the default
            crate::DEFAULT_PG_VERSION,
        );

        let metadata_bytes = original_metadata
            .to_bytes()
            .expect("Should serialize correct metadata to bytes");

        let deserialized_metadata = TimelineMetadata::from_bytes(&metadata_bytes)
            .expect("Should deserialize its own bytes");

        assert_eq!(
            deserialized_metadata.body, original_metadata.body,
            "Metadata that was serialized to bytes and deserialized back should not change"
        );
    }

    // Generate old version metadata and read it with current code.
    // Ensure that it is upgraded correctly
    #[test]
    fn test_metadata_upgrade() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct TimelineMetadataV1 {
            hdr: TimelineMetadataHeader,
            body: TimelineMetadataBodyV1,
        }

        let metadata_v1 = TimelineMetadataV1 {
            hdr: TimelineMetadataHeader {
                checksum: 0,
                size: 0,
                format_version: METADATA_OLD_FORMAT_VERSION_V1,
            },
            body: TimelineMetadataBodyV1 {
                disk_consistent_lsn: Lsn(0x200),
                prev_record_lsn: Some(Lsn(0x100)),
                ancestor_timeline: Some(TIMELINE_ID),
                ancestor_lsn: Lsn(0),
                latest_gc_cutoff_lsn: Lsn(0),
                initdb_lsn: Lsn(0),
            },
        };

        impl TimelineMetadataV1 {
            pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
                let body_bytes = self.body.ser()?;
                let metadata_size = METADATA_HDR_SIZE + body_bytes.len();
                let hdr = TimelineMetadataHeader {
                    size: metadata_size as u16,
                    format_version: METADATA_OLD_FORMAT_VERSION_V1,
                    checksum: crc32c::crc32c(&body_bytes),
                };
                let hdr_bytes = hdr.ser()?;
                let mut metadata_bytes = vec![0u8; METADATA_MAX_SIZE];
                metadata_bytes[0..METADATA_HDR_SIZE].copy_from_slice(&hdr_bytes);
                metadata_bytes[METADATA_HDR_SIZE..metadata_size].copy_from_slice(&body_bytes);
                Ok(metadata_bytes)
            }
        }

        let metadata_bytes = metadata_v1
            .to_bytes()
            .expect("Should serialize correct metadata to bytes");

        // This should deserialize to the latest version format
        let deserialized_metadata = TimelineMetadata::from_bytes(&metadata_bytes)
            .expect("Should deserialize its own bytes");

        let expected_metadata = TimelineMetadata::new(
            Lsn(0x200),
            Some(Lsn(0x100)),
            Some(TIMELINE_ID),
            Lsn(0),
            Lsn(0),
            Lsn(0),
            14, // All timelines created before this version had pg_version 14
        );

        assert_eq!(
            deserialized_metadata.body, expected_metadata.body,
            "Metadata of the old version {} should be upgraded to the latest version {}",
            METADATA_OLD_FORMAT_VERSION_V1, METADATA_FORMAT_VERSION
        );
    }

    // Generate old version metadata and read it with current code.
    // Ensure that it is upgraded correctly
    #[test]
    fn test_metadata_upgrade_v2() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct TimelineMetadataV2 {
            hdr: TimelineMetadataHeader,
            body: TimelineMetadataBodyV2,
        }

        let metadata_v2 = TimelineMetadataV2 {
            hdr: TimelineMetadataHeader {
                checksum: 0,
                size: 0,
                format_version: METADATA_OLD_FORMAT_VERSION_V2,
            },
            body: TimelineMetadataBodyV2 {
                disk_consistent_lsn: Lsn(0x200),
                prev_record_lsn: Some(Lsn(0x100)),
                ancestor_timeline: Some(TIMELINE_ID),
                ancestor_lsn: Lsn(0),
                latest_gc_cutoff_lsn: Lsn(0),
                initdb_lsn: Lsn(0),
                pg_version: 16,
            },
        };

        impl TimelineMetadataV2 {
            pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
                let body_bytes = self.body.ser()?;
                let metadata_size = METADATA_HDR_SIZE + body_bytes.len();
                let hdr = TimelineMetadataHeader {
                    size: metadata_size as u16,
                    format_version: METADATA_OLD_FORMAT_VERSION_V2,
                    checksum: crc32c::crc32c(&body_bytes),
                };
                let hdr_bytes = hdr.ser()?;
                let mut metadata_bytes = vec![0u8; METADATA_MAX_SIZE];
                metadata_bytes[0..METADATA_HDR_SIZE].copy_from_slice(&hdr_bytes);
                metadata_bytes[METADATA_HDR_SIZE..metadata_size].copy_from_slice(&body_bytes);
                Ok(metadata_bytes)
            }
        }

        let metadata_bytes = metadata_v2
            .to_bytes()
            .expect("Should serialize correct metadata to bytes");

        // This should deserialize to the latest version format
        let deserialized_metadata = TimelineMetadata::from_bytes(&metadata_bytes)
            .expect("Should deserialize its own bytes");

        let expected_metadata = TimelineMetadata::new(
            Lsn(0x200),
            Some(Lsn(0x100)),
            Some(TIMELINE_ID),
            Lsn(0),
            Lsn(0),
            Lsn(0),
            16,
        );

        assert_eq!(
            deserialized_metadata.body, expected_metadata.body,
            "Metadata of the old version {} should be upgraded to the latest version {}",
            METADATA_OLD_FORMAT_VERSION_V2, METADATA_FORMAT_VERSION
        );
    }

    #[test]
    fn test_roundtrip_metadata_v3() {
        let metadata_v3 = TimelineMetadata {
            hdr: TimelineMetadataHeader {
                checksum: 0,
                size: 0,
                format_version: METADATA_FORMAT_VERSION,
            },
            body: TimelineMetadataBodyV3 {
                disk_consistent_lsn: Lsn(0x200),
                prev_record_lsn: Some(Lsn(0x100)),
                ancestor_timeline: Some(TIMELINE_ID),
                ancestor_lsn: Lsn(0),
                latest_gc_cutoff_lsn: Lsn(0),
                initdb_lsn: Lsn(0),
                pg_version: 16,
            },
        };

        let metadata_bytes = metadata_v3
            .to_bytes()
            .expect("Should serialize correct metadata to bytes");

        // This should deserialize to the latest version format
        let deserialized_metadata = TimelineMetadata::from_bytes(&metadata_bytes)
            .expect("Should deserialize its own bytes");

        let expected_metadata = TimelineMetadata::new(
            Lsn(0x200),
            Some(Lsn(0x100)),
            Some(TIMELINE_ID),
            Lsn(0),
            Lsn(0),
            Lsn(0),
            16,
        );

        assert_eq!(deserialized_metadata.body, expected_metadata.body);
    }

    #[test]
    fn test_encode_regression() {
        let metadata_v3 = TimelineMetadata {
            hdr: TimelineMetadataHeader {
                checksum: 0,
                size: 0,
                format_version: METADATA_FORMAT_VERSION,
            },
            body: TimelineMetadataBodyV3 {
                disk_consistent_lsn: Lsn(0x200),
                prev_record_lsn: Some(Lsn(0x100)),
                ancestor_timeline: Some(TIMELINE_ID),
                ancestor_lsn: Lsn(0),
                latest_gc_cutoff_lsn: Lsn(0),
                initdb_lsn: Lsn(0),
                pg_version: 16,
            },
        };

        let metadata_bytes = metadata_v3
            .to_bytes()
            .expect("Should serialize correct metadata to bytes");

        assert_eq!(
            &metadata_bytes[..METADATA_HDR_SIZE],
            &[202, 106, 183, 219, 0, 205, 0, 5]
        );
        let json_value: serde_json::Value =
            serde_json::from_slice(&metadata_bytes[METADATA_HDR_SIZE..]).unwrap();
        assert_eq!(
            json_value,
            json!({
                "ancestor_lsn": "0/0",
                "ancestor_timeline": "11223344556677881122334455667788",
                "disk_consistent_lsn": "0/200",
                "initdb_lsn": "0/0",
                "latest_gc_cutoff_lsn": "0/0",
                "pg_version": 16,
                "prev_record_lsn": "0/100"
            })
        );
    }
}
