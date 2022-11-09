//! In-memory index to track the tenant files on the remote storage.
//! Able to restore itself from the storage index parts, that are located in every timeline's remote directory and contain all data about
//! remote timeline layers and its metadata.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::{Context, Ok};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

use crate::tenant::metadata::TimelineMetadata;

use utils::lsn::Lsn;

/// A part of the filesystem path, that needs a root to become a path again.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RelativePath(String);

impl RelativePath {
    /// Attempts to strip off the base from path, producing a relative path or an error.
    pub fn from_local_path(timeline_path: &Path, path: &Path) -> anyhow::Result<RelativePath> {
        let relative = path.strip_prefix(timeline_path).with_context(|| {
            format!(
                "path '{}' is not relative to base '{}'",
                path.display(),
                timeline_path.display()
            )
        })?;
        Ok(Self::from_filename(relative))
    }

    pub fn from_filename(path: &Path) -> RelativePath {
        RelativePath(path.to_string_lossy().to_string())
    }

    pub fn to_local_path(&self, timeline_path: &Path) -> PathBuf {
        timeline_path.join(&self.0)
    }
}

/// Metadata gathered for each of the layer files.
///
/// Fields have to be `Option`s because remote [`IndexPart`]'s can be from different version, which
/// might have less or more metadata depending if upgrading or rolling back an upgrade.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(test, derive(Default))]
pub struct LayerFileMetadata {
    file_size: Option<u64>,
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
        LayerFileMetadata {
            file_size: Some(file_size),
        }
    }

    /// This is used to initialize the metadata for remote layers, for which
    /// the metadata was missing from the index part file.
    pub const MISSING: Self = LayerFileMetadata { file_size: None };

    pub fn file_size(&self) -> Option<u64> {
        self.file_size
    }

    /// Metadata has holes due to version upgrades. This method is called to upgrade self with the
    /// other value.
    ///
    /// This is called on the possibly outdated version.
    pub fn merge(&mut self, other: &Self) {
        self.file_size = other.file_size.or(self.file_size);
    }
}

/// In-memory representation of an `index_part.json` file
///
/// Contains the data about all files in the timeline, present remotely and its metadata.
///
/// This type needs to be backwards and forwards compatible. When changing the fields,
/// remember to add a test case for the changed version.
#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct IndexPart {
    /// Debugging aid describing the version of this type.
    #[serde(default)]
    version: usize,

    /// Each of the layers present on remote storage.
    ///
    /// Additional metadata can might exist in `layer_metadata`.
    pub timeline_layers: HashSet<RelativePath>,

    /// FIXME: unused field. This should be removed, but that changes the on-disk format,
    /// so we need to make sure we're backwards- (and maybe forwards-) compatible
    missing_layers: HashSet<RelativePath>,

    /// Per layer file metadata, which can be present for a present or missing layer file.
    ///
    /// Older versions of `IndexPart` will not have this property or have only a part of metadata
    /// that latest version stores.
    #[serde(default)]
    pub layer_metadata: HashMap<RelativePath, IndexLayerMetadata>,

    // 'disk_consistent_lsn' is a copy of the 'disk_consistent_lsn' in the metadata.
    // It's duplicated here for convenience.
    #[serde_as(as = "DisplayFromStr")]
    pub disk_consistent_lsn: Lsn,
    metadata_bytes: Vec<u8>,
}

impl IndexPart {
    /// When adding or modifying any parts of `IndexPart`, increment the version so that it can be
    /// used to understand later versions.
    ///
    /// Version is currently informative only.
    const LATEST_VERSION: usize = 1;
    pub const FILE_NAME: &'static str = "index_part.json";

    pub fn new(
        layers_and_metadata: HashMap<RelativePath, LayerFileMetadata>,
        disk_consistent_lsn: Lsn,
        metadata_bytes: Vec<u8>,
    ) -> Self {
        let mut timeline_layers = HashSet::new();
        let mut layer_metadata = HashMap::new();

        separate_paths_and_metadata(
            &layers_and_metadata,
            &mut timeline_layers,
            &mut layer_metadata,
        );

        Self {
            version: Self::LATEST_VERSION,
            timeline_layers,
            missing_layers: HashSet::new(),
            layer_metadata,
            disk_consistent_lsn,
            metadata_bytes,
        }
    }

    pub fn parse_metadata(&self) -> anyhow::Result<TimelineMetadata> {
        TimelineMetadata::from_bytes(&self.metadata_bytes)
    }
}

/// Serialized form of [`LayerFileMetadata`].
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Default)]
pub struct IndexLayerMetadata {
    file_size: Option<u64>,
}

impl From<&'_ LayerFileMetadata> for IndexLayerMetadata {
    fn from(other: &'_ LayerFileMetadata) -> Self {
        IndexLayerMetadata {
            file_size: other.file_size,
        }
    }
}

fn separate_paths_and_metadata(
    input: &HashMap<RelativePath, LayerFileMetadata>,
    output: &mut HashSet<RelativePath>,
    layer_metadata: &mut HashMap<RelativePath, IndexLayerMetadata>,
) {
    for (path, metadata) in input {
        let metadata = IndexLayerMetadata::from(metadata);
        layer_metadata.insert(path.clone(), metadata);
        output.insert(path.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v0_indexpart_is_parsed() {
        let example = r#"{
            "timeline_layers":["000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9"],
            "missing_layers":["not_a_real_layer_but_adding_coverage"],
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        }"#;

        let expected = IndexPart {
            version: 0,
            timeline_layers: [RelativePath("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".to_owned())].into_iter().collect(),
            missing_layers: [RelativePath("not_a_real_layer_but_adding_coverage".to_owned())].into_iter().collect(),
            layer_metadata: HashMap::default(),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata_bytes: [113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0].to_vec(),
        };

        let part = serde_json::from_str::<IndexPart>(example).unwrap();
        assert_eq!(part, expected);
    }

    #[test]
    fn v1_indexpart_is_parsed() {
        let example = r#"{
            "version":1,
            "timeline_layers":["000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9"],
            "missing_layers":["not_a_real_layer_but_adding_coverage"],
            "layer_metadata":{
                "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9": { "file_size": 25600000 },
                "not_a_real_layer_but_adding_coverage": { "file_size": 9007199254741001 }
            },
            "disk_consistent_lsn":"0/16960E8",
            "metadata_bytes":[113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        }"#;

        let expected = IndexPart {
            // note this is not verified, could be anything, but exists for humans debugging.. could be the git version instead?
            version: 1,
            timeline_layers: [RelativePath("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".to_owned())].into_iter().collect(),
            missing_layers: [RelativePath("not_a_real_layer_but_adding_coverage".to_owned())].into_iter().collect(),
            layer_metadata: HashMap::from([
                (RelativePath("000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001696070-00000000016960E9".to_owned()), IndexLayerMetadata {
                    file_size: Some(25600000),
                }),
                (RelativePath("not_a_real_layer_but_adding_coverage".to_owned()), IndexLayerMetadata {
                    // serde_json should always parse this but this might be a double with jq for
                    // example.
                    file_size: Some(9007199254741001),
                })
            ]),
            disk_consistent_lsn: "0/16960E8".parse::<Lsn>().unwrap(),
            metadata_bytes: [113,11,159,210,0,54,0,4,0,0,0,0,1,105,96,232,1,0,0,0,0,1,105,96,112,0,0,0,0,0,0,0,0,0,0,0,0,0,1,105,96,112,0,0,0,0,1,105,96,112,0,0,0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0].to_vec(),
        };

        let part = serde_json::from_str::<IndexPart>(example).unwrap();
        assert_eq!(part, expected);
    }
}
