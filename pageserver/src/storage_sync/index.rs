//! In-memory index to track the tenant files on the remote storage.
//! Able to restore itself from the storage index parts, that are located in every timeline's remote directory and contain all data about
//! remote timeline layers and its metadata.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use anyhow::{Context, Ok};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

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
        Ok(RelativePath(relative.to_string_lossy().to_string()))
    }

    /// Joins the relative path with the base path.
    pub fn to_local_path(&self, timeline_path: &Path) -> PathBuf {
        timeline_path.join(&self.0)
    }
}

/// Part of the remote index, corresponding to a certain timeline.
/// Contains the data about all files in the timeline, present remotely and its metadata.
#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct IndexPart {
    pub timeline_layers: HashSet<RelativePath>,
    #[serde_as(as = "DisplayFromStr")]
    pub disk_consistent_lsn: Lsn, // FIXME: is this needed as a separate field? It's also in metadata
    pub metadata_bytes: Vec<u8>,
}

///
/// In-memory representation of an `index_part.json` file
///
impl IndexPart {
    pub const FILE_NAME: &'static str = "index_part.json";

    #[cfg(test)]
    pub fn new(
        timeline_layers: HashSet<RelativePath>,
        disk_consistent_lsn: Lsn,
        metadata_bytes: Vec<u8>,
    ) -> Self {
        Self {
            timeline_layers,
            disk_consistent_lsn,
            metadata_bytes,
        }
    }
}
