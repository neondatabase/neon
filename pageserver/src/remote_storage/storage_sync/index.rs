//! In-memory index to track the timeline files in the remote strorage's archives.
//! Able to restore itself from the storage archive data and reconstruct archive indices on demand.
//!
//! The index is intended to be portable, so deliberately does not store any local paths inside.
//! This way in the future, the index could be restored fast from its serialized stored form.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, bail, ensure, Context};
use futures::stream::{FuturesUnordered, StreamExt};
use tracing::error;
use zenith_utils::{
    lsn::Lsn,
    zid::{ZTenantId, ZTimelineId},
};

use crate::{
    layered_repository::{TENANTS_SEGMENT_NAME, TIMELINES_SEGMENT_NAME},
    remote_storage::{
        storage_sync::compression::{parse_archive_name, FileEntry},
        RemoteStorage, TimelineSyncId,
    },
};

use super::compression::{read_archive_header, ArchiveHeader};

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

    /// Lists all relish files in the given remote timeline. Omits the metadata file.
    pub fn stored_files(&self, timeline_dir: &Path) -> BTreeSet<PathBuf> {
        self.timeline_files
            .values()
            .map(|file_entry| timeline_dir.join(&file_entry.subpath))
            .collect()
    }

    pub fn stored_archives(&self) -> Vec<ArchiveId> {
        self.checkpoint_archives.keys().copied().collect()
    }

    #[cfg(test)]
    pub fn latest_disk_consistent_lsn(&self) -> Option<Lsn> {
        self.checkpoint_archives
            .keys()
            .last()
            .map(|archive_id| archive_id.0)
    }

    pub fn contains_archive(&self, disk_consistent_lsn: Lsn) -> bool {
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
            .ok_or_else(|| anyhow!("Archive {:?} not found", archive_id))?;

        let mut header_files = Vec::with_capacity(archive.files.len());
        for (expected_archive_position, archive_file) in archive.files.iter().enumerate() {
            let &FileId(archive_id, archive_position) = archive_file;
            ensure!(
                expected_archive_position == archive_position,
                "Archive header is corrupt, file # {} from archive {:?} header is missing",
                expected_archive_position,
                archive_id,
            );

            let timeline_file = self.timeline_files.get(archive_file).ok_or_else(|| {
                anyhow!(
                    "File with id {:?} not found for archive {:?}",
                    archive_file,
                    archive_id
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

    /// Updates (creates, if necessary) the data about a certain archive contents.
    pub fn set_archive_contents(
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

/// Reads remote storage file list, parses the data from the file paths and uses it to read every archive's header for every timeline,
/// thus restoring the file list for every timeline.
/// Due to the way headers are stored, S3 api for accessing file byte range is used, so we don't have to download an entire archive for its listing.
pub(super) async fn reconstruct_from_storage<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    storage: &S,
) -> anyhow::Result<HashMap<TimelineSyncId, RemoteTimeline>> {
    let mut index = HashMap::<TimelineSyncId, RemoteTimeline>::new();
    for (sync_id, remote_archives) in collect_archives(storage).await? {
        let mut archive_header_downloads = remote_archives
            .into_iter()
            .map(|(archive_id, (archive, remote_path))| async move {
                let mut header_buf = std::io::Cursor::new(Vec::new());
                storage
                    .download_range(&remote_path, 0, Some(archive.header_size), &mut header_buf)
                    .await
                    .map_err(|e| (e, archive_id))?;
                let header_buf = header_buf.into_inner();
                let header = read_archive_header(&archive.archive_name, &mut header_buf.as_slice())
                    .await
                    .map_err(|e| (e, archive_id))?;
                Ok::<_, (anyhow::Error, ArchiveId)>((archive_id, archive.header_size, header))
            })
            .collect::<FuturesUnordered<_>>();

        while let Some(header_data) = archive_header_downloads.next().await {
            match header_data {
                Ok((archive_id, header_size, header)) => {
                    index
                        .entry(sync_id)
                        .or_insert_with(RemoteTimeline::empty)
                        .set_archive_contents(archive_id.0, header, header_size);
                }
                Err((e, archive_id)) => {
                    bail!(
                        "Failed to download archive header for tenant {}, timeline {}, archive for Lsn {}: {}",
                        sync_id.0, sync_id.1, archive_id.0,
                        e
                    );
                }
            }
        }
    }
    Ok(index)
}

async fn collect_archives<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    storage: &S,
) -> anyhow::Result<HashMap<TimelineSyncId, BTreeMap<ArchiveId, (ArchiveDescription, P)>>> {
    let mut remote_archives =
        HashMap::<TimelineSyncId, BTreeMap<ArchiveId, (ArchiveDescription, P)>>::new();
    for (local_path, remote_path) in storage
        .list()
        .await
        .context("Failed to list remote storage files")?
        .into_iter()
        .map(|remote_path| (storage.local_path(&remote_path), remote_path))
    {
        match local_path.and_then(|local_path| parse_archive_description(&local_path)) {
            Ok((sync_id, archive_description)) => {
                remote_archives.entry(sync_id).or_default().insert(
                    ArchiveId(archive_description.disk_consistent_lsn),
                    (archive_description, remote_path),
                );
            }
            Err(e) => error!(
                "Failed to parse archive description from path '{:?}', reason: {:#}",
                remote_path, e
            ),
        }
    }
    Ok(remote_archives)
}

struct ArchiveDescription {
    header_size: u64,
    disk_consistent_lsn: Lsn,
    archive_name: String,
}

fn parse_archive_description(
    archive_path: &Path,
) -> anyhow::Result<(TimelineSyncId, ArchiveDescription)> {
    let (disk_consistent_lsn, header_size) =
        parse_archive_name(archive_path).with_context(|| {
            format!(
                "Failed to parse timeline id from archive name '{}'",
                archive_path.display()
            )
        })?;

    let mut segments = archive_path
        .iter()
        .skip_while(|&segment| segment != TENANTS_SEGMENT_NAME);
    let tenants_segment = segments.next().ok_or_else(|| {
        anyhow!(
            "Found no '{}' segment in the archive path '{}'",
            TENANTS_SEGMENT_NAME,
            archive_path.display()
        )
    })?;
    ensure!(
        tenants_segment == TENANTS_SEGMENT_NAME,
        "Failed to extract '{}' segment from archive path '{}'",
        TENANTS_SEGMENT_NAME,
        archive_path.display()
    );
    let tenant_id = segments
        .next()
        .ok_or_else(|| {
            anyhow!(
                "Found no tenant id in the archive path '{}'",
                archive_path.display()
            )
        })?
        .to_string_lossy()
        .parse::<ZTenantId>()
        .with_context(|| {
            format!(
                "Failed to parse tenant id from archive path '{}'",
                archive_path.display()
            )
        })?;

    let timelines_segment = segments.next().ok_or_else(|| {
        anyhow!(
            "Found no '{}' segment in the archive path '{}'",
            TIMELINES_SEGMENT_NAME,
            archive_path.display()
        )
    })?;
    ensure!(
        timelines_segment == TIMELINES_SEGMENT_NAME,
        "Failed to extract '{}' segment from archive path '{}'",
        TIMELINES_SEGMENT_NAME,
        archive_path.display()
    );
    let timeline_id = segments
        .next()
        .ok_or_else(|| {
            anyhow!(
                "Found no timeline id in the archive path '{}'",
                archive_path.display()
            )
        })?
        .to_string_lossy()
        .parse::<ZTimelineId>()
        .with_context(|| {
            format!(
                "Failed to parse timeline id from archive path '{}'",
                archive_path.display()
            )
        })?;

    let archive_name = archive_path
        .file_name()
        .ok_or_else(|| anyhow!("Archive '{}' has no file name", archive_path.display()))?
        .to_string_lossy()
        .to_string();
    Ok((
        TimelineSyncId(tenant_id, timeline_id),
        ArchiveDescription {
            header_size,
            disk_consistent_lsn,
            archive_name,
        },
    ))
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
                    subpath: "one".to_string(),
                },
                FileEntry {
                    size: 1,
                    subpath: "two".to_string(),
                },
                FileEntry {
                    size: 222,
                    subpath: "zero".to_string(),
                },
            ],
            metadata_file_size: 5,
        };

        let lsn = Lsn(1);
        let mut remote_timeline = RemoteTimeline::empty();
        remote_timeline.set_archive_contents(lsn, header.clone(), 15);

        let (restored_header, _) = remote_timeline
            .restore_header(ArchiveId(lsn))
            .expect("Should be able to restore header from a valid remote timeline");

        assert_eq!(
            header, restored_header,
            "Header restoration should preserve file order"
        );
    }
}
