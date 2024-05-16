//! In-memory index to track the tenant files on the remote storage.
//! Able to restore itself from the storage index parts, that are located in every timeline's remote directory and contain all data about
//! remote timeline layers and its metadata.

use std::collections::HashMap;

use chrono::NaiveDateTime;
use pageserver_api::models::AuxFilePolicy;
use serde::{Deserialize, Serialize};
use utils::id::TimelineId;

use crate::tenant::metadata::TimelineMetadata;
use crate::tenant::storage_layer::LayerName;
use crate::tenant::upload_queue::UploadQueueInitialized;
use crate::tenant::Generation;
use pageserver_api::shard::ShardIndex;

use utils::lsn::Lsn;

/// Metadata gathered for each of the layer files.
///
/// Fields have to be `Option`s because remote [`IndexPart`]'s can be from different version, which
/// might have less or more metadata depending if upgrading or rolling back an upgrade.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
//#[cfg_attr(test, derive(Default))]
pub struct LayerFileMetadata {
    file_size: u64,

    pub(crate) generation: Generation,

    pub(crate) shard: ShardIndex,
}

impl From<&'_ IndexLayerMetadata> for LayerFileMetadata {
    fn from(other: &IndexLayerMetadata) -> Self {
        LayerFileMetadata {
            file_size: other.file_size,
            generation: other.generation,
            shard: other.shard,
        }
    }
}

impl LayerFileMetadata {
    pub fn new(file_size: u64, generation: Generation, shard: ShardIndex) -> Self {
        LayerFileMetadata {
            file_size,
            generation,
            shard,
        }
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }
}

// TODO seems like another part of the remote storage file format
// compatibility issue, see https://github.com/neondatabase/neon/issues/3072
/// In-memory representation of an `index_part.json` file
///
/// Contains the data about all files in the timeline, present remotely and its metadata.
///
/// This type needs to be backwards and forwards compatible. When changing the fields,
/// remember to add a test case for the changed version.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct IndexPart {
    /// Debugging aid describing the version of this type.
    #[serde(default)]
    version: usize,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<NaiveDateTime>,

    /// Per layer file name metadata, which can be present for a present or missing layer file.
    ///
    /// Older versions of `IndexPart` will not have this property or have only a part of metadata
    /// that latest version stores.
    pub layer_metadata: HashMap<LayerName, IndexLayerMetadata>,

    // 'disk_consistent_lsn' is a copy of the 'disk_consistent_lsn' in the metadata.
    // It's duplicated for convenience when reading the serialized structure, but is
    // private because internally we would read from metadata instead.
    disk_consistent_lsn: Lsn,

    #[serde(rename = "metadata_bytes")]
    pub metadata: TimelineMetadata,

    #[serde(default)]
    pub(crate) lineage: Lineage,

    /// Describes the kind of aux files stored in the timeline.
    ///
    /// The value is modified during file ingestion when the latest wanted value communicated via tenant config is applied if it is acceptable.
    /// A V1 setting after V2 files have been committed is not accepted.
    ///
    /// None means no aux files have been written to the storage before the point
    /// when this flag is introduced.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(crate) last_aux_file_policy: Option<AuxFilePolicy>,
}

impl IndexPart {
    /// When adding or modifying any parts of `IndexPart`, increment the version so that it can be
    /// used to understand later versions.
    ///
    /// Version is currently informative only.
    /// Version history
    /// - 2: added `deleted_at`
    /// - 3: no longer deserialize `timeline_layers` (serialized format is the same, but timeline_layers
    ///      is always generated from the keys of `layer_metadata`)
    /// - 4: timeline_layers is fully removed.
    /// - 5: lineage was added
    /// - 6: last_aux_file_policy is added.
    const LATEST_VERSION: usize = 6;

    // Versions we may see when reading from a bucket.
    pub const KNOWN_VERSIONS: &'static [usize] = &[1, 2, 3, 4, 5, 6];

    pub const FILE_NAME: &'static str = "index_part.json";

    fn new(
        layers_and_metadata: &HashMap<LayerName, LayerFileMetadata>,
        disk_consistent_lsn: Lsn,
        metadata: TimelineMetadata,
        lineage: Lineage,
        last_aux_file_policy: Option<AuxFilePolicy>,
    ) -> Self {
        let layer_metadata = layers_and_metadata
            .iter()
            .map(|(k, v)| (k.to_owned(), IndexLayerMetadata::from(v)))
            .collect();

        Self {
            version: Self::LATEST_VERSION,
            layer_metadata,
            disk_consistent_lsn,
            metadata,
            deleted_at: None,
            lineage,
            last_aux_file_policy,
        }
    }

    pub fn get_version(&self) -> usize {
        self.version
    }

    /// If you want this under normal operations, read it from self.metadata:
    /// this method is just for the scrubber to use when validating an index.
    pub fn get_disk_consistent_lsn(&self) -> Lsn {
        self.disk_consistent_lsn
    }

    pub fn from_s3_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice::<IndexPart>(bytes)
    }

    pub fn to_s3_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    #[cfg(test)]
    pub(crate) fn example() -> Self {
        let example_metadata = TimelineMetadata::example();
        Self::new(
            &HashMap::new(),
            example_metadata.disk_consistent_lsn(),
            example_metadata,
            Default::default(),
            Some(AuxFilePolicy::V1),
        )
    }

    pub(crate) fn last_aux_file_policy(&self) -> Option<AuxFilePolicy> {
        self.last_aux_file_policy
    }
}

impl From<&UploadQueueInitialized> for IndexPart {
    fn from(uq: &UploadQueueInitialized) -> Self {
        let disk_consistent_lsn = uq.latest_metadata.disk_consistent_lsn();
        let metadata = uq.latest_metadata.clone();
        let lineage = uq.latest_lineage.clone();

        Self::new(
            &uq.latest_files,
            disk_consistent_lsn,
            metadata,
            lineage,
            uq.last_aux_file_policy,
        )
    }
}

/// Serialized form of [`LayerFileMetadata`].
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct IndexLayerMetadata {
    pub file_size: u64,

    #[serde(default = "Generation::none")]
    #[serde(skip_serializing_if = "Generation::is_none")]
    pub generation: Generation,

    #[serde(default = "ShardIndex::unsharded")]
    #[serde(skip_serializing_if = "ShardIndex::is_unsharded")]
    pub shard: ShardIndex,
}

impl From<&LayerFileMetadata> for IndexLayerMetadata {
    fn from(other: &LayerFileMetadata) -> Self {
        IndexLayerMetadata {
            file_size: other.file_size,
            generation: other.generation,
            shard: other.shard,
        }
    }
}

/// Limited history of earlier ancestors.
///
/// A timeline can have more than 1 earlier ancestor, in the rare case that it was repeatedly
/// reparented by having an later timeline be detached from it's ancestor.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Default)]
pub(crate) struct Lineage {
    /// Has the `reparenting_history` been truncated to [`Lineage::REMEMBER_AT_MOST`].
    #[serde(skip_serializing_if = "is_false", default)]
    reparenting_history_truncated: bool,

    /// Earlier ancestors, truncated when [`Self::reparenting_history_truncated`]
    ///
    /// These are stored in case we want to support WAL based DR on the timeline. There can be many
    /// of these and at most one [`Self::original_ancestor`]. There cannot be more reparentings
    /// after [`Self::original_ancestor`] has been set.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    reparenting_history: Vec<TimelineId>,

    /// The ancestor from which this timeline has been detached from and when.
    ///
    /// If you are adding support for detaching from a hierarchy, consider changing the ancestry
    /// into a `Vec<(TimelineId, Lsn)>` to be a path instead.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    original_ancestor: Option<(TimelineId, Lsn, NaiveDateTime)>,
}

fn is_false(b: &bool) -> bool {
    !b
}

impl Lineage {
    const REMEMBER_AT_MOST: usize = 100;

    pub(crate) fn record_previous_ancestor(&mut self, old_ancestor: &TimelineId) {
        if self.reparenting_history.last() == Some(old_ancestor) {
            // do not re-record it
            return;
        }

        let drop_oldest = self.reparenting_history.len() + 1 >= Self::REMEMBER_AT_MOST;

        self.reparenting_history_truncated |= drop_oldest;
        if drop_oldest {
            self.reparenting_history.remove(0);
        }
        self.reparenting_history.push(*old_ancestor);
    }

    pub(crate) fn record_detaching(&mut self, branchpoint: &(TimelineId, Lsn)) {
        assert!(self.original_ancestor.is_none());

        self.original_ancestor =
            Some((branchpoint.0, branchpoint.1, chrono::Utc::now().naive_utc()));
    }

    /// The queried lsn is most likely the basebackup lsn, and this answers question "is it allowed
    /// to start a read/write primary at this lsn".
    ///
    /// Returns true if the Lsn was previously a branch point.
    pub(crate) fn is_previous_ancestor_lsn(&self, lsn: Lsn) -> bool {
        self.original_ancestor
            .as_ref()
            .is_some_and(|(_, ancestor_lsn, _)| lsn == *ancestor_lsn)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn v1_indexpart_is_parsed() {
        let example = r#"{
            "version":1,
            "timeline_layers":["000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9"],
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        }"#;

        let expected = IndexPart {
            // note this is not verified, could be anything, but exists for humans debugging.. could be the git version instead?
            version: 1,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), IndexLayerMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), IndexLayerMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::from_bytes(&[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]).unwrap(),
            deleted_at: None,
            lineage: Lineage::default(),
            last_aux_file_policy: None,
        };

        let part = IndexPart::from_s3_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v1_indexpart_is_parsed_with_optional_missing_layers() {
        let example = r#"{
            "version":1,
            "timeline_layers":["000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9"],
            "missing_layers":["This shouldn't fail deserialization"],
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        }"#;

        let expected = IndexPart {
            // note this is not verified, could be anything, but exists for humans debugging.. could be the git version instead?
            version: 1,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), IndexLayerMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), IndexLayerMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::from_bytes(&[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]).unwrap(),
            deleted_at: None,
            lineage: Lineage::default(),
            last_aux_file_policy: None,
        };

        let part = IndexPart::from_s3_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v2_indexpart_is_parsed_with_deleted_at() {
        let example = r#"{
            "version":2,
            "timeline_layers":["000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9"],
            "missing_layers":["This shouldn't fail deserialization"],
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            "deleted_at": "2023-07-31T09:00:00.123"
        }"#;

        let expected = IndexPart {
            // note this is not verified, could be anything, but exists for humans debugging.. could be the git version instead?
            version: 2,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), IndexLayerMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), IndexLayerMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::from_bytes(&[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]).unwrap(),
            deleted_at: Some(chrono::NaiveDateTime::parse_from_str(
                "2023-07-31T09:00:00.123000000", "%Y-%m-%dT%H:%M:%S.%f").unwrap()),
            lineage: Lineage::default(),
            last_aux_file_policy: None,
        };

        let part = IndexPart::from_s3_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn empty_layers_are_parsed() {
        let empty_layers_json = r#"{
            "version":1,
            "timeline_layers":[],
            "layer_metadata":{},
            "disk_consistent_lsn":"0/2532648",
            "metadata_bytes":[136,151,49,208,0,70,0,4,0,0,0,0,2,83,38,72,1,0,0,0,0,2,83,38,32,1,87,198,240,135,97,119,45,125,38,29,155,161,140,141,255,210,0,0,0,0,2,83,38,72,0,0,0,0,1,73,240,192,0,0,0,0,1,73,240,192,0,0,0,15,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        }"#;

        let expected = IndexPart {
            version: 1,
            layer_metadata: HashMap::new(),
            disk_consistent_lsn: "0/2532648".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::from_bytes(&[
                136, 151, 49, 208, 0, 70, 0, 4, 0, 0, 0, 0, 2, 83, 38, 72, 1, 0, 0, 0, 0, 2, 83,
                38, 32, 1, 87, 198, 240, 135, 97, 119, 45, 125, 38, 29, 155, 161, 140, 141, 255,
                210, 0, 0, 0, 0, 2, 83, 38, 72, 0, 0, 0, 0, 1, 73, 240, 192, 0, 0, 0, 0, 1, 73,
                240, 192, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0,
            ])
            .unwrap(),
            deleted_at: None,
            lineage: Lineage::default(),
            last_aux_file_policy: None,
        };

        let empty_layers_parsed = IndexPart::from_s3_bytes(empty_layers_json.as_bytes()).unwrap();

        assert_eq!(empty_layers_parsed, expected);
    }

    #[test]
    fn v4_indexpart_is_parsed() {
        let example = r#"{
            "version":4,
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            "deleted_at": "2023-07-31T09:00:00.123"
        }"#;

        let expected = IndexPart {
            version: 4,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), IndexLayerMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), IndexLayerMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::from_bytes(&[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]).unwrap(),
            deleted_at: Some(parse_naive_datetime("2023-07-31T09:00:00.123000000")),
            lineage: Lineage::default(),
            last_aux_file_policy: None,
        };

        let part = IndexPart::from_s3_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v5_indexpart_is_parsed() {
        let example = r#"{
            "version":5,
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014EF420-00000000014EF499":{"file_size":23289856,"generation":1},
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014EF499-00000000015A7619":{"file_size":1015808,"generation":1}},
                "disk_consistent_lsn":"0/15A7618",
                "metadata_bytes":[226,88,25,241,0,46,0,4,0,0,0,0,1,90,118,24,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,78,244,32,0,0,0,0,1,78,244,32,0,0,0,16,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                "lineage":{
                    "original_ancestor":["e2bfd8c633d713d279e6fcd2bcc15b6d","0/15A7618","2024-05-07T18:52:36.322426563"],
                    "reparenting_history":["e1bfd8c633d713d279e6fcd2bcc15b6d"]
                }
        }"#;

        let expected = IndexPart {
            version: 5,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014EF420-00000000014EF499".parse().unwrap(), IndexLayerMetadata {
                    file_size: 23289856,
                    generation: Generation::new(1),
                    shard: ShardIndex::unsharded(),
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000014EF499-00000000015A7619".parse().unwrap(), IndexLayerMetadata {
                    file_size: 1015808,
                    generation: Generation::new(1),
                    shard: ShardIndex::unsharded(),
                })
            ]),
            disk_consistent_lsn: Lsn::from_str("0/15A7618").unwrap(),
            metadata: TimelineMetadata::from_bytes(&[226,88,25,241,0,46,0,4,0,0,0,0,1,90,118,24,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,78,244,32,0,0,0,0,1,78,244,32,0,0,0,16,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]).unwrap(),
            deleted_at: None,
            lineage: Lineage {
                reparenting_history_truncated: false,
                reparenting_history: vec![TimelineId::from_str("e1bfd8c633d713d279e6fcd2bcc15b6d").unwrap()],
                original_ancestor: Some((TimelineId::from_str("e2bfd8c633d713d279e6fcd2bcc15b6d").unwrap(), Lsn::from_str("0/15A7618").unwrap(), parse_naive_datetime("2024-05-07T18:52:36.322426563"))),
            },
            last_aux_file_policy: None,
        };

        let part = IndexPart::from_s3_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v6_indexpart_is_parsed() {
        let example = r#"{
            "version":6,
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            "deleted_at": "2023-07-31T09:00:00.123",
            "lineage":{
                "original_ancestor":["e2bfd8c633d713d279e6fcd2bcc15b6d","0/15A7618","2024-05-07T18:52:36.322426563"],
                "reparenting_history":["e1bfd8c633d713d279e6fcd2bcc15b6d"]
            },
            "last_aux_file_policy": "V2"
        }"#;

        let expected = IndexPart {
            version: 6,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), IndexLayerMetadata {
                    file_size: 25600000,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), IndexLayerMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: 9007199254741001,
                    generation: Generation::none(),
                    shard: ShardIndex::unsharded()
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata: TimelineMetadata::from_bytes(&[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]).unwrap(),
            deleted_at: Some(chrono::NaiveDateTime::parse_from_str(
                "2023-07-31T09:00:00.123000000", "%Y-%m-%dT%H:%M:%S.%f").unwrap()),
            lineage: Lineage {
                reparenting_history_truncated: false,
                reparenting_history: vec![TimelineId::from_str("e1bfd8c633d713d279e6fcd2bcc15b6d").unwrap()],
                original_ancestor: Some((TimelineId::from_str("e2bfd8c633d713d279e6fcd2bcc15b6d").unwrap(), Lsn::from_str("0/15A7618").unwrap(), parse_naive_datetime("2024-05-07T18:52:36.322426563"))),
            },
            last_aux_file_policy: Some(AuxFilePolicy::V2),
        };

        let part = IndexPart::from_s3_bytes(example.as_bytes()).unwrap();
        assert_eq!(part, expected);
    }

    fn parse_naive_datetime(s: &str) -> NaiveDateTime {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S.%f").unwrap()
    }
}
