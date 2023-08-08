//! In-memory index to track the tenant files on the remote storage.
//! Able to restore itself from the storage index parts, that are located in every timeline's remote directory and contain all data about
//! remote timeline layers and its metadata.

use std::collections::{HashMap, HashSet};

use chrono::NaiveDateTime;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use serde_with::serde_as;
use utils::bin_ser::SerializeError;

use crate::tenant::metadata::TimelineMetadata;
use crate::tenant::storage_layer::LayerFileName;
use crate::tenant::upload_queue::UploadQueueInitialized;

/// Metadata gathered for each of the layer files.
///
/// Fields have to be `Option`s because remote [`IndexPart`]'s can be from different version, which
/// might have less or more metadata depending if upgrading or rolling back an upgrade.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(test, derive(Default))]
pub struct LayerFileMetadata {
    file_size: u64,
}

impl From<&'_ IndexLayerMetadata> for LayerFileMetadata {
    fn from(other: &IndexLayerMetadata) -> Self {
        LayerFileMetadata {
            file_size: other.file_size,
        }
    }
}

impl LayerFileMetadata {
    pub fn new(file_size: u64) -> Self {
        LayerFileMetadata { file_size }
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
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct IndexPart {
    version: usize,

    pub deleted_at: Option<NaiveDateTime>,

    /// Per layer file name metadata, which can be present for a present or missing layer file.
    ///
    /// Older versions of `IndexPart` will not have this property or have only a part of metadata
    /// that latest version stores.
    pub layer_metadata: HashMap<LayerFileName, IndexLayerMetadata>,

    pub metadata: TimelineMetadata,
}

impl<'de> Deserialize<'de> for IndexPart {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Declaring a struct is simpler that implementing a Visitor to handle decoding
        // a JSON struct while ignoring fields we don't care about.
        #[serde_as]
        #[derive(Deserialize)]
        struct SerializedIndexPart {
            #[serde(default)]
            version: usize,
            #[serde(default)]
            deleted_at: Option<NaiveDateTime>,
            layer_metadata: HashMap<LayerFileName, IndexLayerMetadata>,
            metadata_bytes: TimelineMetadata,
        }

        let inner = SerializedIndexPart::deserialize(deserializer)?;

        Ok(IndexPart {
            version: inner.version,
            deleted_at: inner.deleted_at,
            layer_metadata: inner.layer_metadata,
            metadata: inner.metadata_bytes,
        })
    }
}

impl Serialize for IndexPart {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("IndexPart", 6)?;

        state.serialize_field("version", &(self.version as u32))?;

        // Forward compat: write out this field only so that v2 readers can read
        // the v3 structure.  This could be written more efficiently but this forward
        // compat code will go away in the near future.
        let timeline_layers: HashSet<LayerFileName> =
            self.layer_metadata.keys().map(|k| k.to_owned()).collect();
        state.serialize_field("timeline_layers", &timeline_layers)?;

        state.serialize_field("deleted_at", &self.deleted_at)?;
        state.serialize_field("layer_metadata", &self.layer_metadata)?;
        let metadata_bytes = self.metadata.to_bytes().map_err(|e| {
            serde::ser::Error::custom(format!("Unserializable IndexPart metadata: {e}"))
        })?;
        state.serialize_field("metadata_bytes", &metadata_bytes)?;

        // This field is written out for convenience of human readers, but is
        // not read back in deserialization
        state.serialize_field(
            "disk_consistent_lsn",
            &format!("{}", self.metadata.disk_consistent_lsn()),
        )?;

        state.end()
    }
}

impl IndexPart {
    /// When adding or modifying any parts of `IndexPart`, increment the version so that it can be
    /// used to understand later versions.
    ///
    /// Version is currently informative only.
    const LATEST_VERSION: usize = 2;
    pub const FILE_NAME: &'static str = "index_part.json";

    pub fn new(
        layers_and_metadata: HashMap<LayerFileName, LayerFileMetadata>,
        metadata: TimelineMetadata,
    ) -> Self {
        let mut layer_metadata = HashMap::with_capacity(layers_and_metadata.len());

        for (remote_name, metadata) in layers_and_metadata {
            layer_metadata.insert(remote_name.to_owned(), IndexLayerMetadata::from(metadata));
        }

        Self {
            version: Self::LATEST_VERSION,
            layer_metadata,
            metadata,
            deleted_at: None,
        }
    }
}

impl TryFrom<&UploadQueueInitialized> for IndexPart {
    type Error = SerializeError;

    fn try_from(upload_queue: &UploadQueueInitialized) -> Result<Self, Self::Error> {
        Ok(Self::new(
            upload_queue.latest_files.clone(),
            upload_queue.latest_metadata.clone(),
        ))
    }
}

/// Serialized form of [`LayerFileMetadata`].
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Default)]
pub struct IndexLayerMetadata {
    pub(super) file_size: u64,
}

impl From<LayerFileMetadata> for IndexLayerMetadata {
    fn from(other: LayerFileMetadata) -> Self {
        IndexLayerMetadata {
            file_size: other.file_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_indexpart_is_parsed() {
        let metadata_bytes: Vec<u8> = [
            113, 11, 159, 210, 0, 54, 0, 4, 0, 0, 0, 0, 1, 105, 96, 232, 1, 0, 0, 0, 0, 1, 105, 96,
            112, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 105, 96, 112, 0, 0, 0, 0, 1, 105, 96,
            112, 0, 0, 0, 14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        let metadata_bytes_str = serde_json::to_string(&metadata_bytes).unwrap();

        let example = format!(
            r#"{{
            "version":1,
            "timeline_layers":[
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9",
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51"
                ],
            "layer_metadata":{{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": {{ "file_size": 25600000 }},
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": {{ "file_size": 9007199254741001 }}
            }},
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":{metadata_bytes_str}
        }}"#
        );

        let expected = IndexPart {
            // note this is not verified, could be anything, but exists for humans debugging.. could be the git version instead?
            version: 1,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), IndexLayerMetadata {
                    file_size: 25600000,
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), IndexLayerMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: 9007199254741001,
                })
            ]),
            metadata: TimelineMetadata::from_bytes(&metadata_bytes).unwrap(),
            deleted_at: None,
        };

        let part = serde_json::from_str::<IndexPart>(&example).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v2_indexpart_is_parsed_with_deleted_at() {
        let metadata_bytes: Vec<u8> = [
            136, 151, 49, 208, 0, 70, 0, 4, 0, 0, 0, 0, 2, 83, 38, 72, 1, 0, 0, 0, 0, 2, 83, 38,
            32, 1, 87, 198, 240, 135, 97, 119, 45, 125, 38, 29, 155, 161, 140, 141, 255, 210, 0, 0,
            0, 0, 2, 83, 38, 72, 0, 0, 0, 0, 1, 73, 240, 192, 0, 0, 0, 0, 1, 73, 240, 192, 0, 0, 0,
            15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        let metadata_bytes_str = serde_json::to_string(&metadata_bytes).unwrap();

        let example = format!(
            r#"{{
            "version":2,
            "timeline_layers":[
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9",
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51"
                ],
            "missing_layers":["This shouldn't fail deserialization"],
            "layer_metadata":{{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": {{ "file_size": 25600000 }},
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": {{ "file_size": 9007199254741001 }}
            }},
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":{metadata_bytes_str},
            "deleted_at": "2023-07-31T09:00:00.123"
        }}"#
        );

        let expected = IndexPart {
            // note this is not verified, could be anything, but exists for humans debugging.. could be the git version instead?
            version: 2,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), IndexLayerMetadata {
                    file_size: 25600000,
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), IndexLayerMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: 9007199254741001,
                })
            ]),
            metadata: TimelineMetadata::from_bytes(&metadata_bytes).unwrap(),
            deleted_at: Some(chrono::NaiveDateTime::parse_from_str(
                "2023-07-31T09:00:00.123000000", "%Y-%m-%dT%H:%M:%S.%f").unwrap())
        };

        let part = serde_json::from_str::<IndexPart>(&example).unwrap();
        assert_eq!(part, expected);

        // Validate that when we write out, we are writing the same v2 format that older pageservers
        // will understand
        let reserialized = serde_json::to_string(&part).unwrap();

        // We do not expect exact symmetry, but the reserialized version should include the legacy fields that
        // v2 requires, and not be limited to just the fields that are in the runtime IndexPart
        assert!(reserialized.contains("layer_metadata"));
        assert!(reserialized.contains("disk_consistent_lsn"));
        // The missing_layers attribute is not required
        assert!(!reserialized.contains("missing_layers"));
    }

    #[test]
    fn v3_indexpart_is_parsed() {
        let metadata_bytes: Vec<u8> = [
            136, 151, 49, 208, 0, 70, 0, 4, 0, 0, 0, 0, 2, 83, 38, 72, 1, 0, 0, 0, 0, 2, 83, 38,
            32, 1, 87, 198, 240, 135, 97, 119, 45, 125, 38, 29, 155, 161, 140, 141, 255, 210, 0, 0,
            0, 0, 2, 83, 38, 72, 0, 0, 0, 0, 1, 73, 240, 192, 0, 0, 0, 0, 1, 73, 240, 192, 0, 0, 0,
            15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        let metadata_bytes_str = serde_json::to_string(&metadata_bytes).unwrap();

        let example = format!(
            r#"{{
            "version":3,
            "layer_metadata":{{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": {{ "file_size": 25600000 }},
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51": {{ "file_size": 9007199254741001 }}
            }},
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":{metadata_bytes_str},
            "deleted_at": "2023-07-31T09:00:00.123"
        }}"#
        );

        let expected = IndexPart {
            version: 3,
            layer_metadata: HashMap::from([
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".parse().unwrap(), IndexLayerMetadata {
                    file_size: 25600000,
                }),
                ("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap(), IndexLayerMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: 9007199254741001,
                })
            ]),
            metadata: TimelineMetadata::from_bytes(metadata_bytes.as_slice()).unwrap(),
            deleted_at: Some(chrono::NaiveDateTime::parse_from_str(
                "2023-07-31T09:00:00.123000000", "%Y-%m-%dT%H:%M:%S.%f").unwrap())
        };

        let part = serde_json::from_str::<IndexPart>(&example).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn empty_layers_are_parsed() {
        let metadata_bytes: Vec<u8> = [
            136, 151, 49, 208, 0, 70, 0, 4, 0, 0, 0, 0, 2, 83, 38, 72, 1, 0, 0, 0, 0, 2, 83, 38,
            32, 1, 87, 198, 240, 135, 97, 119, 45, 125, 38, 29, 155, 161, 140, 141, 255, 210, 0, 0,
            0, 0, 2, 83, 38, 72, 0, 0, 0, 0, 1, 73, 240, 192, 0, 0, 0, 0, 1, 73, 240, 192, 0, 0, 0,
            15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ]
        .to_vec();
        let metadata_bytes_str = serde_json::to_string(&metadata_bytes).unwrap();

        let empty_layers_json = format!(
            r#"{{
            "version":1,
            "timeline_layers":[],
            "layer_metadata":{{}},
            "disk_consistent_lsn":"0/2532648",
            "metadata_bytes":{metadata_bytes_str}
        }}"#
        );

        let expected = IndexPart {
            version: 1,
            layer_metadata: HashMap::new(),
            metadata: TimelineMetadata::from_bytes(&metadata_bytes).unwrap(),
            deleted_at: None,
        };

        let empty_layers_parsed =
            serde_json::from_str::<IndexPart>(empty_layers_json.as_str()).unwrap();

        assert_eq!(empty_layers_parsed, expected);
    }
}
