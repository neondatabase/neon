//! In-memory index to track the tenant files on the remote storage.
//! Able to restore itself from the storage index parts, that are located in every timeline's remote directory and contain all data about
//! remote timeline layers and its metadata.

use std::ops::{Deref, DerefMut};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Context, Ok};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use tokio::sync::RwLock;

use crate::{config::PageServerConf, layered_repository::metadata::TimelineMetadata};
use utils::{
    lsn::Lsn,
    zid::{ZTenantId, ZTenantTimelineId, ZTimelineId},
};

/// A part of the filesystem path, that needs a root to become a path again.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RelativePath(String);

impl RelativePath {
    /// Attempts to strip off the base from path, producing a relative path or an error.
    pub fn new<P: AsRef<Path>>(base: &Path, path: P) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let relative = path.strip_prefix(base).with_context(|| {
            format!(
                "path '{}' is not relative to base '{}'",
                path.display(),
                base.display()
            )
        })?;
        Ok(RelativePath(relative.to_string_lossy().to_string()))
    }

    /// Joins the relative path with the base path.
    fn as_path(&self, base: &Path) -> PathBuf {
        base.join(&self.0)
    }
}

#[derive(Debug, Clone, Default)]
pub struct TenantEntry(HashMap<ZTimelineId, RemoteTimeline>);

impl TenantEntry {
    pub fn has_in_progress_downloads(&self) -> bool {
        self.values()
            .any(|remote_timeline| remote_timeline.awaits_download)
    }
}

impl Deref for TenantEntry {
    type Target = HashMap<ZTimelineId, RemoteTimeline>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TenantEntry {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<HashMap<ZTimelineId, RemoteTimeline>> for TenantEntry {
    fn from(inner: HashMap<ZTimelineId, RemoteTimeline>) -> Self {
        Self(inner)
    }
}

/// An index to track tenant files that exist on the remote storage.
#[derive(Debug, Clone, Default)]
pub struct RemoteTimelineIndex {
    entries: HashMap<ZTenantId, TenantEntry>,
}

/// A wrapper to synchronize the access to the index, should be created and used before dealing with any [`RemoteTimelineIndex`].
#[derive(Default)]
pub struct RemoteIndex(Arc<RwLock<RemoteTimelineIndex>>);

impl RemoteIndex {
    pub fn from_parts(
        conf: &'static PageServerConf,
        index_parts: HashMap<ZTenantId, HashMap<ZTimelineId, IndexPart>>,
    ) -> anyhow::Result<Self> {
        let mut entries: HashMap<ZTenantId, TenantEntry> = HashMap::new();

        for (tenant_id, timelines) in index_parts {
            for (timeline_id, index_part) in timelines {
                let timeline_path = conf.timeline_path(&timeline_id, &tenant_id);
                let remote_timeline =
                    RemoteTimeline::from_index_part(&timeline_path, index_part)
                        .context("Failed to restore remote timeline data from index part")?;

                entries
                    .entry(tenant_id)
                    .or_default()
                    .insert(timeline_id, remote_timeline);
            }
        }

        Ok(Self(Arc::new(RwLock::new(RemoteTimelineIndex { entries }))))
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, RemoteTimelineIndex> {
        self.0.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, RemoteTimelineIndex> {
        self.0.write().await
    }
}

impl Clone for RemoteIndex {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl RemoteTimelineIndex {
    pub fn timeline_entry(
        &self,
        ZTenantTimelineId {
            tenant_id,
            timeline_id,
        }: &ZTenantTimelineId,
    ) -> Option<&RemoteTimeline> {
        self.entries.get(tenant_id)?.get(timeline_id)
    }

    pub fn timeline_entry_mut(
        &mut self,
        ZTenantTimelineId {
            tenant_id,
            timeline_id,
        }: &ZTenantTimelineId,
    ) -> Option<&mut RemoteTimeline> {
        self.entries.get_mut(tenant_id)?.get_mut(timeline_id)
    }

    pub fn add_timeline_entry(
        &mut self,
        ZTenantTimelineId {
            tenant_id,
            timeline_id,
        }: ZTenantTimelineId,
        entry: RemoteTimeline,
    ) {
        self.entries
            .entry(tenant_id)
            .or_default()
            .insert(timeline_id, entry);
    }

    pub fn tenant_entry(&self, tenant_id: &ZTenantId) -> Option<&TenantEntry> {
        self.entries.get(tenant_id)
    }

    pub fn tenant_entry_mut(&mut self, tenant_id: &ZTenantId) -> Option<&mut TenantEntry> {
        self.entries.get_mut(tenant_id)
    }

    pub fn add_tenant_entry(&mut self, tenant_id: ZTenantId) -> &mut TenantEntry {
        self.entries.entry(tenant_id).or_default()
    }

    pub fn set_awaits_download(
        &mut self,
        id: &ZTenantTimelineId,
        awaits_download: bool,
    ) -> anyhow::Result<()> {
        self.timeline_entry_mut(id)
            .ok_or_else(|| anyhow!("unknown timeline sync {id}"))?
            .awaits_download = awaits_download;
        Ok(())
    }
}

/// Restored index part data about the timeline, stored in the remote index.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RemoteTimeline {
    timeline_layers: HashSet<PathBuf>,
    missing_layers: HashSet<PathBuf>,

    pub metadata: TimelineMetadata,
    pub awaits_download: bool,
}

impl RemoteTimeline {
    pub fn new(metadata: TimelineMetadata) -> Self {
        Self {
            timeline_layers: HashSet::new(),
            missing_layers: HashSet::new(),
            metadata,
            awaits_download: false,
        }
    }

    pub fn add_timeline_layers(&mut self, new_layers: impl IntoIterator<Item = PathBuf>) {
        self.timeline_layers.extend(new_layers.into_iter());
    }

    pub fn add_upload_failures(&mut self, upload_failures: impl IntoIterator<Item = PathBuf>) {
        self.missing_layers.extend(upload_failures.into_iter());
    }

    pub fn remove_layers(&mut self, layers_to_remove: &HashSet<PathBuf>) {
        self.timeline_layers
            .retain(|layer| !layers_to_remove.contains(layer));
        self.missing_layers
            .retain(|layer| !layers_to_remove.contains(layer));
    }

    /// Lists all layer files in the given remote timeline. Omits the metadata file.
    pub fn stored_files(&self) -> &HashSet<PathBuf> {
        &self.timeline_layers
    }

    pub fn from_index_part(timeline_path: &Path, index_part: IndexPart) -> anyhow::Result<Self> {
        let metadata = TimelineMetadata::from_bytes(&index_part.metadata_bytes)?;
        Ok(Self {
            timeline_layers: to_local_paths(timeline_path, index_part.timeline_layers),
            missing_layers: to_local_paths(timeline_path, index_part.missing_layers),
            metadata,
            awaits_download: false,
        })
    }
}

/// Part of the remote index, corresponding to a certain timeline.
/// Contains the data about all files in the timeline, present remotely and its metadata.
#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct IndexPart {
    timeline_layers: HashSet<RelativePath>,
    /// Currently is not really used in pageserver,
    /// present to manually keep track of the layer files that pageserver might never retrieve.
    ///
    /// Such "holes" might appear if any upload task was evicted on an error threshold:
    /// the this layer will only be rescheduled for upload on pageserver restart.
    missing_layers: HashSet<RelativePath>,
    #[serde_as(as = "DisplayFromStr")]
    disk_consistent_lsn: Lsn,
    metadata_bytes: Vec<u8>,
}

impl IndexPart {
    pub const FILE_NAME: &'static str = "index_part";
    pub const FILE_EXTENSION: &'static str = "json";

    #[cfg(test)]
    pub fn new(
        timeline_layers: HashSet<RelativePath>,
        missing_layers: HashSet<RelativePath>,
        disk_consistent_lsn: Lsn,
        metadata_bytes: Vec<u8>,
    ) -> Self {
        Self {
            timeline_layers,
            missing_layers,
            disk_consistent_lsn,
            metadata_bytes,
        }
    }

    pub fn missing_files(&self) -> &HashSet<RelativePath> {
        &self.missing_layers
    }

    pub fn from_remote_timeline(
        timeline_path: &Path,
        remote_timeline: RemoteTimeline,
    ) -> anyhow::Result<Self> {
        let metadata_bytes = remote_timeline.metadata.to_bytes()?;
        Ok(Self {
            timeline_layers: to_relative_paths(timeline_path, remote_timeline.timeline_layers)
                .context("Failed to convert timeline layers' paths to relative ones")?,
            missing_layers: to_relative_paths(timeline_path, remote_timeline.missing_layers)
                .context("Failed to convert missing layers' paths to relative ones")?,
            disk_consistent_lsn: remote_timeline.metadata.disk_consistent_lsn(),
            metadata_bytes,
        })
    }
}

fn to_local_paths(
    timeline_path: &Path,
    paths: impl IntoIterator<Item = RelativePath>,
) -> HashSet<PathBuf> {
    paths
        .into_iter()
        .map(|path| path.as_path(timeline_path))
        .collect()
}

fn to_relative_paths(
    timeline_path: &Path,
    paths: impl IntoIterator<Item = PathBuf>,
) -> anyhow::Result<HashSet<RelativePath>> {
    paths
        .into_iter()
        .map(|path| RelativePath::new(timeline_path, path))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::repository::repo_harness::{RepoHarness, TIMELINE_ID};

    #[test]
    fn index_part_conversion() {
        let harness = RepoHarness::create("index_part_conversion").unwrap();
        let timeline_path = harness.timeline_path(&TIMELINE_ID);
        let metadata =
            TimelineMetadata::new(Lsn(5).align(), Some(Lsn(4)), None, Lsn(3), Lsn(2), Lsn(1));
        let remote_timeline = RemoteTimeline {
            timeline_layers: HashSet::from([
                timeline_path.join("layer_1"),
                timeline_path.join("layer_2"),
            ]),
            missing_layers: HashSet::from([
                timeline_path.join("missing_1"),
                timeline_path.join("missing_2"),
            ]),
            metadata: metadata.clone(),
            awaits_download: false,
        };

        let index_part = IndexPart::from_remote_timeline(&timeline_path, remote_timeline.clone())
            .expect("Correct remote timeline should be convertible to index part");

        assert_eq!(
            index_part.timeline_layers.iter().collect::<BTreeSet<_>>(),
            BTreeSet::from([
                &RelativePath("layer_1".to_string()),
                &RelativePath("layer_2".to_string())
            ]),
            "Index part should have all remote timeline layers after the conversion"
        );
        assert_eq!(
            index_part.missing_layers.iter().collect::<BTreeSet<_>>(),
            BTreeSet::from([
                &RelativePath("missing_1".to_string()),
                &RelativePath("missing_2".to_string())
            ]),
            "Index part should have all missing remote timeline layers after the conversion"
        );
        assert_eq!(
            index_part.disk_consistent_lsn,
            metadata.disk_consistent_lsn(),
            "Index part should have disk consistent lsn from the timeline"
        );
        assert_eq!(
            index_part.metadata_bytes,
            metadata
                .to_bytes()
                .expect("Failed to serialize correct metadata into bytes"),
            "Index part should have all missing remote timeline layers after the conversion"
        );

        let restored_timeline = RemoteTimeline::from_index_part(&timeline_path, index_part)
            .expect("Correct index part should be convertible to remote timeline");

        let original_metadata = &remote_timeline.metadata;
        let restored_metadata = &restored_timeline.metadata;
        // we have to compare the metadata this way, since its header is different after creation and restoration,
        // but that is now consireded ok.
        assert_eq!(
            original_metadata.disk_consistent_lsn(),
            restored_metadata.disk_consistent_lsn(),
            "remote timeline -> index part -> remote timeline conversion should not alter metadata"
        );
        assert_eq!(
            original_metadata.prev_record_lsn(),
            restored_metadata.prev_record_lsn(),
            "remote timeline -> index part -> remote timeline conversion should not alter metadata"
        );
        assert_eq!(
            original_metadata.ancestor_timeline(),
            restored_metadata.ancestor_timeline(),
            "remote timeline -> index part -> remote timeline conversion should not alter metadata"
        );
        assert_eq!(
            original_metadata.ancestor_lsn(),
            restored_metadata.ancestor_lsn(),
            "remote timeline -> index part -> remote timeline conversion should not alter metadata"
        );
        assert_eq!(
            original_metadata.latest_gc_cutoff_lsn(),
            restored_metadata.latest_gc_cutoff_lsn(),
            "remote timeline -> index part -> remote timeline conversion should not alter metadata"
        );
        assert_eq!(
            original_metadata.initdb_lsn(),
            restored_metadata.initdb_lsn(),
            "remote timeline -> index part -> remote timeline conversion should not alter metadata"
        );

        assert_eq!(
            remote_timeline.awaits_download, restored_timeline.awaits_download,
            "remote timeline -> index part -> remote timeline conversion should not loose download flag"
        );

        assert_eq!(
            remote_timeline
                .timeline_layers
                .into_iter()
                .collect::<BTreeSet<_>>(),
            restored_timeline
                .timeline_layers
                .into_iter()
                .collect::<BTreeSet<_>>(),
            "remote timeline -> index part -> remote timeline conversion should not loose layer data"
        );
        assert_eq!(
            remote_timeline
                .missing_layers
                .into_iter()
                .collect::<BTreeSet<_>>(),
            restored_timeline
                .missing_layers
                .into_iter()
                .collect::<BTreeSet<_>>(),
            "remote timeline -> index part -> remote timeline conversion should not loose missing file data"
        );
    }

    #[test]
    fn index_part_conversion_negatives() {
        let harness = RepoHarness::create("index_part_conversion_negatives").unwrap();
        let timeline_path = harness.timeline_path(&TIMELINE_ID);
        let metadata =
            TimelineMetadata::new(Lsn(5).align(), Some(Lsn(4)), None, Lsn(3), Lsn(2), Lsn(1));

        let conversion_result = IndexPart::from_remote_timeline(
            &timeline_path,
            RemoteTimeline {
                timeline_layers: HashSet::from([
                    PathBuf::from("bad_path"),
                    timeline_path.join("layer_2"),
                ]),
                missing_layers: HashSet::from([
                    timeline_path.join("missing_1"),
                    timeline_path.join("missing_2"),
                ]),
                metadata: metadata.clone(),
                awaits_download: false,
            },
        );
        assert!(conversion_result.is_err(), "Should not be able to convert metadata with layer paths that are not in the timeline directory");

        let conversion_result = IndexPart::from_remote_timeline(
            &timeline_path,
            RemoteTimeline {
                timeline_layers: HashSet::from([
                    timeline_path.join("layer_1"),
                    timeline_path.join("layer_2"),
                ]),
                missing_layers: HashSet::from([
                    PathBuf::from("bad_path"),
                    timeline_path.join("missing_2"),
                ]),
                metadata,
                awaits_download: false,
            },
        );
        assert!(conversion_result.is_err(), "Should not be able to convert metadata with missing layer paths that are not in the timeline directory");
    }
}
