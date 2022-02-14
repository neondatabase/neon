//! In-memory index to track the tenant files on the remote strorage, mitigating the storage format differences between the local and remote files.
//! Able to restore itself from the storage archive data and reconstruct archive indices on demand.
//!
//! The index is intended to be portable, so deliberately does not store any local paths inside.
//! This way in the future, the index could be restored fast from its serialized stored form.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::{bail, ensure, Context};
use serde::{Deserialize, Serialize};
use tracing::debug;
use zenith_utils::{
    lsn::Lsn,
    zid::{ZTenantId, ZTimelineId},
};

use crate::{
    config::PageServerConf,
    layered_repository::TIMELINES_SEGMENT_NAME,
    remote_storage::{
        storage_sync::compression::{parse_archive_name, FileEntry},
        ZTenantTimelineId,
    },
};

use super::compression::ArchiveHeader;

/// A part of the filesystem path, that needs a root to become a path again.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RelativePath(String);

impl RelativePath {
    /// Attempts to strip off the base from path, producing a relative path or an error.
    pub fn new<P: AsRef<Path>>(base: &Path, path: P) -> anyhow::Result<Self> {
        let relative = path
            .as_ref()
            .strip_prefix(base)
            .context("path is not relative to base")?;
        Ok(RelativePath(relative.to_string_lossy().to_string()))
    }

    /// Joins the relative path with the base path.
    pub fn as_path(&self, base: &Path) -> PathBuf {
        base.join(&self.0)
    }
}

/// An index to track tenant files that exist on the remote storage.
/// Currently, timeline archives and branch files are tracked.
#[derive(Debug, Clone)]
pub struct RemoteTimelineIndex {
    branch_files: HashMap<ZTenantId, HashSet<RelativePath>>,
    timeline_files: HashMap<ZTenantTimelineId, TimelineIndexEntry>,
}

impl RemoteTimelineIndex {
    /// Attempts to parse file paths (not checking the file contents) and find files
    /// that can be tracked wiht the index.
    /// On parse falures, logs the error and continues, so empty index can be created from not suitable paths.
    pub fn try_parse_descriptions_from_paths<P: AsRef<Path>>(
        conf: &'static PageServerConf,
        paths: impl Iterator<Item = P>,
    ) -> Self {
        let mut index = Self {
            branch_files: HashMap::new(),
            timeline_files: HashMap::new(),
        };
        for path in paths {
            if let Err(e) = try_parse_index_entry(&mut index, conf, path.as_ref()) {
                debug!(
                    "Failed to parse path '{}' as index entry: {:#}",
                    path.as_ref().display(),
                    e
                );
            }
        }
        index
    }

    pub fn timeline_entry(&self, id: &ZTenantTimelineId) -> Option<&TimelineIndexEntry> {
        self.timeline_files.get(id)
    }

    pub fn timeline_entry_mut(
        &mut self,
        id: &ZTenantTimelineId,
    ) -> Option<&mut TimelineIndexEntry> {
        self.timeline_files.get_mut(id)
    }

    pub fn add_timeline_entry(&mut self, id: ZTenantTimelineId, entry: TimelineIndexEntry) {
        self.timeline_files.insert(id, entry);
    }

    pub fn all_sync_ids(&self) -> impl Iterator<Item = ZTenantTimelineId> + '_ {
        self.timeline_files.keys().copied()
    }

    pub fn add_branch_file(&mut self, tenant_id: ZTenantId, path: RelativePath) {
        self.branch_files
            .entry(tenant_id)
            .or_insert_with(HashSet::new)
            .insert(path);
    }

    pub fn branch_files(&self, tenant_id: ZTenantId) -> Option<&HashSet<RelativePath>> {
        self.branch_files.get(&tenant_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimelineIndexEntry {
    /// An archive found on the remote storage, but not yet downloaded, only a metadata from its storage path is available, without archive contents.
    Description(BTreeMap<ArchiveId, ArchiveDescription>),
    /// Full archive metadata, including the file list, parsed from the archive header.
    Full(RemoteTimeline),
}

impl TimelineIndexEntry {
    pub fn uploaded_checkpoints(&self) -> BTreeSet<Lsn> {
        match self {
            Self::Description(description) => {
                description.keys().map(|archive_id| archive_id.0).collect()
            }
            Self::Full(remote_timeline) => remote_timeline
                .checkpoint_archives
                .keys()
                .map(|archive_id| archive_id.0)
                .collect(),
        }
    }

    /// Gets latest uploaded checkpoint's disk consisten Lsn for the corresponding timeline.
    pub fn disk_consistent_lsn(&self) -> Option<Lsn> {
        match self {
            Self::Description(description) => {
                description.keys().map(|archive_id| archive_id.0).max()
            }
            Self::Full(remote_timeline) => remote_timeline
                .checkpoint_archives
                .keys()
                .map(|archive_id| archive_id.0)
                .max(),
        }
    }
}

/// Checkpoint archive's id, corresponding to the `disk_consistent_lsn` from the timeline's metadata file during checkpointing.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct ArchiveId(pub(super) Lsn);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct FileId(ArchiveId, ArchiveEntryNumber);

type ArchiveEntryNumber = usize;

/// All archives and files in them, representing a certain timeline.
/// Uses file and archive IDs to reference those without ownership issues.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RemoteTimeline {
    timeline_files: BTreeMap<FileId, FileEntry>,
    checkpoint_archives: BTreeMap<ArchiveId, CheckpointArchive>,
}

/// Archive metadata, enough to restore a header with the timeline data.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CheckpointArchive {
    disk_consistent_lsn: Lsn,
    metadata_file_size: u64,
    files: BTreeSet<FileId>,
    archive_header_size: u64,
}

impl CheckpointArchive {
    pub fn disk_consistent_lsn(&self) -> Lsn {
        self.disk_consistent_lsn
    }
}

impl RemoteTimeline {
    pub fn empty() -> Self {
        Self {
            timeline_files: BTreeMap::new(),
            checkpoint_archives: BTreeMap::new(),
        }
    }

    pub fn checkpoints(&self) -> impl Iterator<Item = Lsn> + '_ {
        self.checkpoint_archives
            .values()
            .map(CheckpointArchive::disk_consistent_lsn)
    }

    /// Lists all relish files in the given remote timeline. Omits the metadata file.
    pub fn stored_files(&self, timeline_dir: &Path) -> BTreeSet<PathBuf> {
        self.timeline_files
            .values()
            .map(|file_entry| file_entry.subpath.as_path(timeline_dir))
            .collect()
    }

    pub fn contains_checkpoint_at(&self, disk_consistent_lsn: Lsn) -> bool {
        self.checkpoint_archives
            .contains_key(&ArchiveId(disk_consistent_lsn))
    }

    pub fn archive_data(&self, archive_id: ArchiveId) -> Option<&CheckpointArchive> {
        self.checkpoint_archives.get(&archive_id)
    }

    /// Restores a header of a certain remote archive from the memory data.
    /// Returns the header and its compressed size in the archive, both can be used to uncompress that archive.
    pub fn restore_header(&self, archive_id: ArchiveId) -> anyhow::Result<(ArchiveHeader, u64)> {
        let archive = self
            .checkpoint_archives
            .get(&archive_id)
            .with_context(|| format!("Archive {:?} not found", archive_id))?;

        let mut header_files = Vec::with_capacity(archive.files.len());
        for (expected_archive_position, archive_file) in archive.files.iter().enumerate() {
            let &FileId(archive_id, archive_position) = archive_file;
            ensure!(
                expected_archive_position == archive_position,
                "Archive header is corrupt, file # {} from archive {:?} header is missing",
                expected_archive_position,
                archive_id,
            );

            let timeline_file = self.timeline_files.get(archive_file).with_context(|| {
                format!(
                    "File with id {:?} not found for archive {:?}",
                    archive_file, archive_id
                )
            })?;
            header_files.push(timeline_file.clone());
        }

        Ok((
            ArchiveHeader {
                files: header_files,
                metadata_file_size: archive.metadata_file_size,
            },
            archive.archive_header_size,
        ))
    }

    /// Updates (creates, if necessary) the data about certain archive contents.
    pub fn update_archive_contents(
        &mut self,
        disk_consistent_lsn: Lsn,
        header: ArchiveHeader,
        header_size: u64,
    ) {
        let archive_id = ArchiveId(disk_consistent_lsn);
        let mut common_archive_files = BTreeSet::new();
        for (file_index, file_entry) in header.files.into_iter().enumerate() {
            let file_id = FileId(archive_id, file_index);
            self.timeline_files.insert(file_id, file_entry);
            common_archive_files.insert(file_id);
        }

        let metadata_file_size = header.metadata_file_size;
        self.checkpoint_archives
            .entry(archive_id)
            .or_insert_with(|| CheckpointArchive {
                metadata_file_size,
                files: BTreeSet::new(),
                archive_header_size: header_size,
                disk_consistent_lsn,
            })
            .files
            .extend(common_archive_files.into_iter());
    }
}

/// Metadata abput timeline checkpoint archive, parsed from its remote storage path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveDescription {
    pub header_size: u64,
    pub disk_consistent_lsn: Lsn,
    pub archive_name: String,
}

fn try_parse_index_entry(
    index: &mut RemoteTimelineIndex,
    conf: &'static PageServerConf,
    path: &Path,
) -> anyhow::Result<()> {
    let tenants_dir = conf.tenants_path();
    let tenant_id = path
        .strip_prefix(&tenants_dir)
        .with_context(|| {
            format!(
                "Path '{}' does not belong to tenants directory '{}'",
                path.display(),
                tenants_dir.display(),
            )
        })?
        .iter()
        .next()
        .with_context(|| format!("Found no tenant id in path '{}'", path.display()))?
        .to_string_lossy()
        .parse::<ZTenantId>()
        .with_context(|| format!("Failed to parse tenant id from path '{}'", path.display()))?;

    let branches_path = conf.branches_path(&tenant_id);
    let timelines_path = conf.timelines_path(&tenant_id);
    match (
        RelativePath::new(&branches_path, &path),
        path.strip_prefix(&timelines_path),
    ) {
        (Ok(_), Ok(_)) => bail!(
            "Path '{}' cannot start with both branches '{}' and the timelines '{}' prefixes",
            path.display(),
            branches_path.display(),
            timelines_path.display()
        ),
        (Ok(branches_entry), Err(_)) => index.add_branch_file(tenant_id, branches_entry),
        (Err(_), Ok(timelines_subpath)) => {
            let mut segments = timelines_subpath.iter();
            let timeline_id = segments
                .next()
                .with_context(|| {
                    format!(
                        "{} directory of tenant {} (path '{}') is not an index entry",
                        TIMELINES_SEGMENT_NAME,
                        tenant_id,
                        path.display()
                    )
                })?
                .to_string_lossy()
                .parse::<ZTimelineId>()
                .with_context(|| {
                    format!("Failed to parse timeline id from path '{}'", path.display())
                })?;

            let (disk_consistent_lsn, header_size) =
                parse_archive_name(path).with_context(|| {
                    format!(
                        "Failed to parse archive name out in path '{}'",
                        path.display()
                    )
                })?;

            let archive_name = path
                .file_name()
                .with_context(|| format!("Archive '{}' has no file name", path.display()))?
                .to_string_lossy()
                .to_string();

            let sync_id = ZTenantTimelineId {
                tenant_id,
                timeline_id,
            };
            let timeline_index_entry = index
                .timeline_files
                .entry(sync_id)
                .or_insert_with(|| TimelineIndexEntry::Description(BTreeMap::new()));
            match timeline_index_entry {
                TimelineIndexEntry::Description(descriptions) => {
                    descriptions.insert(
                        ArchiveId(disk_consistent_lsn),
                        ArchiveDescription {
                            header_size,
                            disk_consistent_lsn,
                            archive_name,
                        },
                    );
                }
                TimelineIndexEntry::Full(_) => {
                    bail!("Cannot add parsed archive description to its full context in index with sync id {}", sync_id)
                }
            }
        }
        (Err(branches_error), Err(timelines_strip_error)) => {
            bail!(
                "Path '{}' is not an index entry: it's neither parsable as a branch entry '{:#}' nor as an archive entry '{}'",
                path.display(),
                branches_error,
                timelines_strip_error,
            )
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_restoration_preserves_file_order() {
        let header = ArchiveHeader {
            files: vec![
                FileEntry {
                    size: 5,
                    subpath: RelativePath("one".to_string()),
                },
                FileEntry {
                    size: 1,
                    subpath: RelativePath("two".to_string()),
                },
                FileEntry {
                    size: 222,
                    subpath: RelativePath("zero".to_string()),
                },
            ],
            metadata_file_size: 5,
        };

        let lsn = Lsn(1);
        let mut remote_timeline = RemoteTimeline::empty();
        remote_timeline.update_archive_contents(lsn, header.clone(), 15);

        let (restored_header, _) = remote_timeline
            .restore_header(ArchiveId(lsn))
            .expect("Should be able to restore header from a valid remote timeline");

        assert_eq!(
            header, restored_header,
            "Header restoration should preserve file order"
        );
    }
}
