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
use tracing::log::warn;

use crate::{config::PageServerConf, tenant::metadata::TimelineMetadata};
use utils::{
    id::{TenantId, TenantTimelineId, TimelineId},
    lsn::Lsn,
};

use super::download::TenantIndexParts;

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
pub struct TenantEntry(HashMap<TimelineId, RemoteTimeline>);

impl TenantEntry {
    pub fn has_in_progress_downloads(&self) -> bool {
        self.values()
            .any(|remote_timeline| remote_timeline.awaits_download)
    }
}

impl Deref for TenantEntry {
    type Target = HashMap<TimelineId, RemoteTimeline>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TenantEntry {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<HashMap<TimelineId, RemoteTimeline>> for TenantEntry {
    fn from(inner: HashMap<TimelineId, RemoteTimeline>) -> Self {
        Self(inner)
    }
}

/// An index to track tenant files that exist on the remote storage.
#[derive(Debug, Clone, Default)]
pub struct RemoteTimelineIndex {
    entries: HashMap<TenantId, TenantEntry>,
}

/// A wrapper to synchronize the access to the index, should be created and used before dealing with any [`RemoteTimelineIndex`].
#[derive(Default)]
pub struct RemoteIndex(Arc<RwLock<RemoteTimelineIndex>>);

impl RemoteIndex {
    pub fn from_parts(
        conf: &'static PageServerConf,
        index_parts: HashMap<TenantId, TenantIndexParts>,
    ) -> anyhow::Result<Self> {
        let mut entries: HashMap<TenantId, TenantEntry> = HashMap::new();

        for (tenant_id, index_parts) in index_parts {
            match index_parts {
                // TODO: should we schedule a retry so it can be recovered? otherwise we can revive it only through detach/attach or pageserver restart
                TenantIndexParts::Poisoned { missing, ..} => warn!("skipping tenant_id set up for remote index because the index download has failed for timeline(s): {missing:?}"),
                TenantIndexParts::Present(timelines) => {
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
                },
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
        TenantTimelineId {
            tenant_id,
            timeline_id,
        }: &TenantTimelineId,
    ) -> Option<&RemoteTimeline> {
        self.entries.get(tenant_id)?.get(timeline_id)
    }

    pub fn timeline_entry_mut(
        &mut self,
        TenantTimelineId {
            tenant_id,
            timeline_id,
        }: &TenantTimelineId,
    ) -> Option<&mut RemoteTimeline> {
        self.entries.get_mut(tenant_id)?.get_mut(timeline_id)
    }

    pub fn add_timeline_entry(
        &mut self,
        TenantTimelineId {
            tenant_id,
            timeline_id,
        }: TenantTimelineId,
        entry: RemoteTimeline,
    ) {
        self.entries
            .entry(tenant_id)
            .or_default()
            .insert(timeline_id, entry);
    }

    pub fn remove_timeline_entry(
        &mut self,
        TenantTimelineId {
            tenant_id,
            timeline_id,
        }: TenantTimelineId,
    ) -> Option<RemoteTimeline> {
        self.entries
            .entry(tenant_id)
            .or_default()
            .remove(&timeline_id)
    }

    pub fn tenant_entry(&self, tenant_id: &TenantId) -> Option<&TenantEntry> {
        self.entries.get(tenant_id)
    }

    pub fn tenant_entry_mut(&mut self, tenant_id: &TenantId) -> Option<&mut TenantEntry> {
        self.entries.get_mut(tenant_id)
    }

    pub fn add_tenant_entry(&mut self, tenant_id: TenantId) -> &mut TenantEntry {
        self.entries.entry(tenant_id).or_default()
    }

    pub fn remove_tenant_entry(&mut self, tenant_id: &TenantId) -> Option<TenantEntry> {
        self.entries.remove(tenant_id)
    }

    pub fn set_awaits_download(
        &mut self,
        id: &TenantTimelineId,
        awaits_download: bool,
    ) -> anyhow::Result<()> {
        self.timeline_entry_mut(id)
            .ok_or_else(|| anyhow!("unknown timeline sync {id}"))?
            .awaits_download = awaits_download;
        Ok(())
    }
}

/// Restored index part data about the timeline, stored in the remote index.
#[derive(Debug, Clone)]
pub struct RemoteTimeline {
    timeline_layers: HashMap<PathBuf, LayerFileMetadata>,
    missing_layers: HashMap<PathBuf, LayerFileMetadata>,

    pub metadata: TimelineMetadata,
    pub awaits_download: bool,
}

impl RemoteTimeline {
    pub fn new(metadata: TimelineMetadata) -> Self {
        Self {
            timeline_layers: HashMap::default(),
            missing_layers: HashMap::default(),
            metadata,
            awaits_download: false,
        }
    }

    pub fn add_timeline_layers(
        &mut self,
        new_layers: impl IntoIterator<Item = (PathBuf, LayerFileMetadata)>,
    ) {
        self.timeline_layers.extend(new_layers);
    }

    pub fn add_upload_failures(
        &mut self,
        upload_failures: impl IntoIterator<Item = (PathBuf, LayerFileMetadata)>,
    ) {
        self.missing_layers.extend(upload_failures);
    }

    pub fn remove_layers(&mut self, layers_to_remove: &HashSet<PathBuf>) {
        self.timeline_layers
            .retain(|layer, _| !layers_to_remove.contains(layer));
        self.missing_layers
            .retain(|layer, _| !layers_to_remove.contains(layer));
    }

    /// Lists all layer files in the given remote timeline. Omits the metadata file.
    pub fn stored_files(&self) -> &HashMap<PathBuf, LayerFileMetadata> {
        &self.timeline_layers
    }

    /// Combines metadata gathered or verified during downloading needed layer files to metadata on
    /// the [`RemoteIndex`], so it can be uploaded later.
    pub fn merge_metadata_from_downloaded(
        &mut self,
        downloaded: &HashMap<PathBuf, LayerFileMetadata>,
    ) {
        downloaded.iter().for_each(|(path, metadata)| {
            if let Some(upgraded) = self.timeline_layers.get_mut(path) {
                upgraded.merge(metadata);
            }
        });
    }

    pub fn from_index_part(timeline_path: &Path, index_part: IndexPart) -> anyhow::Result<Self> {
        let metadata = TimelineMetadata::from_bytes(&index_part.metadata_bytes)?;
        let default_metadata = &IndexLayerMetadata::default();

        let find_metadata = |key: &RelativePath| -> LayerFileMetadata {
            index_part
                .layer_metadata
                .get(key)
                .unwrap_or(default_metadata)
                .into()
        };

        Ok(Self {
            timeline_layers: index_part
                .timeline_layers
                .iter()
                .map(|layer_path| (layer_path.as_path(timeline_path), find_metadata(layer_path)))
                .collect(),
            missing_layers: index_part
                .missing_layers
                .iter()
                .map(|layer_path| (layer_path.as_path(timeline_path), find_metadata(layer_path)))
                .collect(),
            metadata,
            awaits_download: false,
        })
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

/// Part of the remote index, corresponding to a certain timeline.
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
    timeline_layers: HashSet<RelativePath>,

    /// Currently is not really used in pageserver,
    /// present to manually keep track of the layer files that pageserver might never retrieve.
    ///
    /// Such "holes" might appear if any upload task was evicted on an error threshold:
    /// the this layer will only be rescheduled for upload on pageserver restart.
    missing_layers: HashSet<RelativePath>,

    /// Per layer file metadata, which can be present for a present or missing layer file.
    ///
    /// Older versions of `IndexPart` will not have this property or have only a part of metadata
    /// that latest version stores.
    #[serde(default)]
    layer_metadata: HashMap<RelativePath, IndexLayerMetadata>,

    #[serde_as(as = "DisplayFromStr")]
    disk_consistent_lsn: Lsn,
    metadata_bytes: Vec<u8>,
}

impl IndexPart {
    /// When adding or modifying any parts of `IndexPart`, increment the version so that it can be
    /// used to understand later versions.
    ///
    /// Version is currently informative only.
    const LATEST_VERSION: usize = 1;
    pub const FILE_NAME: &'static str = "index_part.json";

    #[cfg(test)]
    pub fn new(
        timeline_layers: HashSet<RelativePath>,
        missing_layers: HashSet<RelativePath>,
        disk_consistent_lsn: Lsn,
        metadata_bytes: Vec<u8>,
    ) -> Self {
        Self {
            version: Self::LATEST_VERSION,
            timeline_layers,
            missing_layers,
            layer_metadata: HashMap::default(),
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

        let mut layer_metadata = HashMap::new();

        let mut missing_layers = HashSet::new();

        separate_paths_and_metadata(
            timeline_path,
            &remote_timeline.missing_layers,
            &mut missing_layers,
            &mut layer_metadata,
        )
        .context("Failed to convert missing layers' paths to relative ones")?;

        let mut timeline_layers = HashSet::new();

        separate_paths_and_metadata(
            timeline_path,
            &remote_timeline.timeline_layers,
            &mut timeline_layers,
            &mut layer_metadata,
        )
        .context("Failed to convert timeline layers' paths to relative ones")?;

        Ok(Self {
            version: Self::LATEST_VERSION,
            timeline_layers,
            missing_layers,
            layer_metadata,
            disk_consistent_lsn: remote_timeline.metadata.disk_consistent_lsn(),
            metadata_bytes,
        })
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
    timeline_path: &Path,
    input: &HashMap<PathBuf, LayerFileMetadata>,
    output: &mut HashSet<RelativePath>,
    layer_metadata: &mut HashMap<RelativePath, IndexLayerMetadata>,
) -> anyhow::Result<()> {
    for (path, metadata) in input {
        let rel_path = RelativePath::new(timeline_path, path)?;
        let metadata = IndexLayerMetadata::from(metadata);

        layer_metadata.insert(rel_path.clone(), metadata);
        output.insert(rel_path);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::tenant::harness::{TenantHarness, TIMELINE_ID};
    use crate::DEFAULT_PG_VERSION;

    #[test]
    fn index_part_conversion() {
        let harness = TenantHarness::create("index_part_conversion").unwrap();
        let timeline_path = harness.timeline_path(&TIMELINE_ID);
        let metadata = TimelineMetadata::new(
            Lsn(5).align(),
            Some(Lsn(4)),
            None,
            Lsn(3),
            Lsn(2),
            Lsn(1),
            DEFAULT_PG_VERSION,
        );
        let remote_timeline = RemoteTimeline {
            timeline_layers: HashMap::from([
                (timeline_path.join("layer_1"), LayerFileMetadata::new(1)),
                (timeline_path.join("layer_2"), LayerFileMetadata::new(2)),
            ]),
            missing_layers: HashMap::from([
                (timeline_path.join("missing_1"), LayerFileMetadata::new(3)),
                (timeline_path.join("missing_2"), LayerFileMetadata::new(4)),
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
        let harness = TenantHarness::create("index_part_conversion_negatives").unwrap();
        let timeline_path = harness.timeline_path(&TIMELINE_ID);
        let metadata = TimelineMetadata::new(
            Lsn(5).align(),
            Some(Lsn(4)),
            None,
            Lsn(3),
            Lsn(2),
            Lsn(1),
            DEFAULT_PG_VERSION,
        );

        let conversion_result = IndexPart::from_remote_timeline(
            &timeline_path,
            RemoteTimeline {
                timeline_layers: HashMap::from([
                    (PathBuf::from("bad_path"), LayerFileMetadata::new(1)),
                    (timeline_path.join("layer_2"), LayerFileMetadata::new(2)),
                ]),
                missing_layers: HashMap::from([
                    (timeline_path.join("missing_1"), LayerFileMetadata::new(3)),
                    (timeline_path.join("missing_2"), LayerFileMetadata::new(4)),
                ]),
                metadata: metadata.clone(),
                awaits_download: false,
            },
        );
        assert!(conversion_result.is_err(), "Should not be able to convert metadata with layer paths that are not in the timeline directory");

        let conversion_result = IndexPart::from_remote_timeline(
            &timeline_path,
            RemoteTimeline {
                timeline_layers: HashMap::from([
                    (timeline_path.join("layer_1"), LayerFileMetadata::new(1)),
                    (timeline_path.join("layer_2"), LayerFileMetadata::new(2)),
                ]),
                missing_layers: HashMap::from([
                    (PathBuf::from("bad_path"), LayerFileMetadata::new(3)),
                    (timeline_path.join("missing_2"), LayerFileMetadata::new(4)),
                ]),
                metadata,
                awaits_download: false,
            },
        );
        assert!(conversion_result.is_err(), "Should not be able to convert metadata with missing layer paths that are not in the timeline directory");
    }

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
