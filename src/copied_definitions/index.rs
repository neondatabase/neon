//! In-memory index to track the tenant files on the remote storage.
//! Able to restore itself from the storage index parts, that are located in every timeline's remote directory and contain all data about
//! remote timeline layers and its metadata.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use tracing::warn;

use super::filename::LayerFileName;
use super::metadata::TimelineMetadata;

use super::lsn::Lsn;

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
#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct IndexPart {
    /// Debugging aid describing the version of this type.
    #[serde(default)]
    pub version: usize,

    /// Layer names, which are stored on the remote storage.
    ///
    /// Additional metadata can might exist in `layer_metadata`.
    pub timeline_layers: HashSet<LayerFileName>,

    /// Per layer file name metadata, which can be present for a present or missing layer file.
    ///
    /// Older versions of `IndexPart` will not have this property or have only a part of metadata
    /// that latest version stores.
    pub layer_metadata: HashMap<LayerFileName, IndexLayerMetadata>,

    // 'disk_consistent_lsn' is a copy of the 'disk_consistent_lsn' in the metadata.
    // It's duplicated here for convenience.
    #[serde_as(as = "DisplayFromStr")]
    pub disk_consistent_lsn: Lsn,
    pub metadata_bytes: Vec<u8>,
}

impl IndexPart {
    /// When adding or modifying any parts of `IndexPart`, increment the version so that it can be
    /// used to understand later versions.
    ///
    /// Version is currently informative only.
    const LATEST_VERSION: usize = 1;
    pub const FILE_NAME: &'static str = "index_part.json";

    pub fn new(
        layers_and_metadata: HashMap<LayerFileName, LayerFileMetadata>,
        disk_consistent_lsn: Lsn,
        metadata_bytes: Vec<u8>,
    ) -> Self {
        let mut timeline_layers = HashSet::with_capacity(layers_and_metadata.len());
        let mut layer_metadata = HashMap::with_capacity(layers_and_metadata.len());

        for (remote_name, metadata) in &layers_and_metadata {
            timeline_layers.insert(remote_name.to_owned());
            let metadata = IndexLayerMetadata::from(metadata);
            layer_metadata.insert(remote_name.to_owned(), metadata);
        }

        Self {
            version: Self::LATEST_VERSION,
            timeline_layers,
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
    pub file_size: u64,
}

impl From<&'_ LayerFileMetadata> for IndexLayerMetadata {
    fn from(other: &'_ LayerFileMetadata) -> Self {
        IndexLayerMetadata {
            file_size: other.file_size,
        }
    }
}
