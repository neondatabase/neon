//! Timeline synchrnonization logic to put files from archives on remote storage into pageserver's local directory.

use std::{borrow::Cow, collections::BTreeSet, path::PathBuf, sync::Arc};

use anyhow::{ensure, Context};
use tokio::{fs, sync::RwLock};
use tracing::{debug, error, trace, warn};
use zenith_utils::zid::ZTenantId;

use crate::{
    config::PageServerConf,
    layered_repository::metadata::{metadata_path, TimelineMetadata},
    remote_storage::{
        storage_sync::{
            compression, fetch_full_index, index::TimelineIndexEntryInner, sync_queue, SyncKind,
            SyncTask,
        },
        RemoteStorage, ZTenantTimelineId,
    },
};

use super::{
    index::{ArchiveId, RemoteTimeline, RemoteTimelineIndex},
    TimelineDownload,
};

/// Timeline download result, with extra data, needed for downloading.
pub(super) enum DownloadedTimeline {
    /// Remote timeline data is either absent or corrupt, no download possible.
    Abort,
    /// Remote timeline data is found, its latest checkpoint's metadata contents (disk_consistent_lsn) is known.
    /// Initial download failed due to some error, the download task is rescheduled for another retry.
    FailedAndRescheduled,
    /// Remote timeline data is found, its latest checkpoint's metadata contents (disk_consistent_lsn) is known.
    /// Initial download successful.
    Successful,
}

/// Attempts to download and uncompress files from all remote archives for the timeline given.
/// Timeline files that already exist locally are skipped during the download, but the local metadata file is
/// updated in the end of every checkpoint archive extraction.
///
/// On an error, bumps the retries count and reschedules the download, with updated archive skip list
/// (for any new successful archive downloads and extractions).
pub(super) async fn download_timeline<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    conf: &'static PageServerConf,
    remote_assets: Arc<(S, Arc<RwLock<RemoteTimelineIndex>>)>,
    sync_id: ZTenantTimelineId,
    mut download: TimelineDownload,
    retries: u32,
) -> DownloadedTimeline {
    debug!("Downloading layers for sync id {}", sync_id);

    let ZTenantTimelineId {
        tenant_id,
        timeline_id,
    } = sync_id;
    let index = &remote_assets.1;

    let index_read = index.read().await;
    let remote_timeline = match index_read.timeline_entry(&sync_id) {
        None => {
            error!("Cannot download: no timeline is present in the index for given id");
            return DownloadedTimeline::Abort;
        }

        Some(index_entry) => match index_entry.inner() {
            TimelineIndexEntryInner::Full(remote_timeline) => Cow::Borrowed(remote_timeline),
            TimelineIndexEntryInner::Description(_) => {
                // we do not check here for awaits_download because it is ok
                // to call this function while the download is in progress
                // so it is not a concurrent download, it is the same one

                let remote_disk_consistent_lsn = index_entry.disk_consistent_lsn();
                drop(index_read);
                debug!("Found timeline description for the given ids, downloading the full index");
                match fetch_full_index(
                    remote_assets.as_ref(),
                    &conf.timeline_path(&timeline_id, &tenant_id),
                    sync_id,
                )
                .await
                {
                    Ok(remote_timeline) => Cow::Owned(remote_timeline),
                    Err(e) => {
                        error!("Failed to download full timeline index: {:?}", e);

                        return match remote_disk_consistent_lsn {
                            Some(_) => {
                                sync_queue::push(SyncTask::new(
                                    sync_id,
                                    retries,
                                    SyncKind::Download(download),
                                ));
                                DownloadedTimeline::FailedAndRescheduled
                            }
                            None => {
                                error!("Cannot download: no disk consistent Lsn is present for the index entry");
                                DownloadedTimeline::Abort
                            }
                        };
                    }
                }
            }
        },
    };
    if remote_timeline.checkpoints().max().is_none() {
        debug!("Cannot download: no disk consistent Lsn is present for the remote timeline");
        return DownloadedTimeline::Abort;
    };

    debug!("Downloading timeline archives");
    let archives_to_download = remote_timeline
        .checkpoints()
        .map(ArchiveId)
        .filter(|remote_archive| !download.archives_to_skip.contains(remote_archive))
        .collect::<Vec<_>>();

    let archives_total = archives_to_download.len();
    debug!("Downloading {} archives of a timeline", archives_total);
    trace!("Archives to download: {:?}", archives_to_download);

    for (archives_downloaded, archive_id) in archives_to_download.into_iter().enumerate() {
        match try_download_archive(
            conf,
            sync_id,
            Arc::clone(&remote_assets),
            &remote_timeline,
            archive_id,
            Arc::clone(&download.files_to_skip),
        )
        .await
        {
            Err(e) => {
                let archives_left = archives_total - archives_downloaded;
                error!(
                    "Failed to download archive {:?} (archives downloaded: {}; archives left: {}) for tenant {} timeline {}, requeueing the download: {:?}",
                    archive_id, archives_downloaded, archives_left, tenant_id, timeline_id, e
                );
                sync_queue::push(SyncTask::new(
                    sync_id,
                    retries,
                    SyncKind::Download(download),
                ));
                return DownloadedTimeline::FailedAndRescheduled;
            }
            Ok(()) => {
                debug!("Successfully downloaded archive {:?}", archive_id);
                download.archives_to_skip.insert(archive_id);
            }
        }
    }

    debug!("Finished downloading all timeline's archives");
    DownloadedTimeline::Successful
}

async fn try_download_archive<
    P: Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    conf: &'static PageServerConf,
    ZTenantTimelineId {
        tenant_id,
        timeline_id,
    }: ZTenantTimelineId,
    remote_assets: Arc<(S, Arc<RwLock<RemoteTimelineIndex>>)>,
    remote_timeline: &RemoteTimeline,
    archive_id: ArchiveId,
    files_to_skip: Arc<BTreeSet<PathBuf>>,
) -> anyhow::Result<()> {
    debug!("Downloading archive {:?}", archive_id);
    let archive_to_download = remote_timeline
        .archive_data(archive_id)
        .with_context(|| format!("Archive {:?} not found in remote storage", archive_id))?;
    let (archive_header, header_size) = remote_timeline
        .restore_header(archive_id)
        .context("Failed to restore header when downloading an archive")?;

    match read_local_metadata(conf, timeline_id, tenant_id).await {
        Ok(local_metadata) => ensure!(
            // need to allow `<=` instead of `<` due to cases when a failed archive can be redownloaded
            local_metadata.disk_consistent_lsn() <= archive_to_download.disk_consistent_lsn(),
            "Cannot download archive with Lsn {} since it's earlier than local Lsn {}",
            archive_to_download.disk_consistent_lsn(),
            local_metadata.disk_consistent_lsn()
        ),
        Err(e) => warn!("Failed to read local metadata file, assuming it's safe to override its with the download. Read: {:#}", e),
    }
    compression::uncompress_file_stream_with_index(
        conf.timeline_path(&timeline_id, &tenant_id),
        files_to_skip,
        archive_to_download.disk_consistent_lsn(),
        archive_header,
        header_size,
        move |mut archive_target, archive_name| async move {
            let archive_local_path = conf
                .timeline_path(&timeline_id, &tenant_id)
                .join(&archive_name);
            let remote_storage = &remote_assets.0;
            remote_storage
                .download_range(
                    &remote_storage.storage_path(&archive_local_path)?,
                    header_size,
                    None,
                    &mut archive_target,
                )
                .await
        },
    )
    .await?;

    Ok(())
}

async fn read_local_metadata(
    conf: &'static PageServerConf,
    timeline_id: zenith_utils::zid::ZTimelineId,
    tenant_id: ZTenantId,
) -> anyhow::Result<TimelineMetadata> {
    let local_metadata_path = metadata_path(conf, timeline_id, tenant_id);
    let local_metadata_bytes = fs::read(&local_metadata_path)
        .await
        .context("Failed to read local metadata file bytes")?;
    Ok(TimelineMetadata::from_bytes(&local_metadata_bytes)
        .context("Failed to read local metadata files bytes")?)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tempfile::tempdir;
    use tokio::fs;
    use zenith_utils::lsn::Lsn;

    use crate::{
        remote_storage::{
            local_fs::LocalFs,
            storage_sync::test_utils::{
                assert_index_descriptions, assert_timeline_files_match, create_local_timeline,
                dummy_metadata, ensure_correct_timeline_upload, expect_timeline,
            },
        },
        repository::repo_harness::{RepoHarness, TIMELINE_ID},
    };

    use super::*;

    #[tokio::test]
    async fn test_download_timeline() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("test_download_timeline")?;
        let sync_id = ZTenantTimelineId::new(repo_harness.tenant_id, TIMELINE_ID);
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let index = Arc::new(RwLock::new(
            RemoteTimelineIndex::try_parse_descriptions_from_paths(
                repo_harness.conf,
                storage
                    .list()
                    .await?
                    .into_iter()
                    .map(|storage_path| storage.local_path(&storage_path).unwrap()),
            ),
        ));
        let remote_assets = Arc::new((storage, index));
        let storage = &remote_assets.0;
        let index = &remote_assets.1;

        let regular_timeline_path = repo_harness.timeline_path(&TIMELINE_ID);
        let regular_timeline = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["a", "b"],
            dummy_metadata(Lsn(0x30)),
        )?;
        ensure_correct_timeline_upload(
            &repo_harness,
            Arc::clone(&remote_assets),
            TIMELINE_ID,
            regular_timeline,
        )
        .await;
        // upload multiple checkpoints for the same timeline
        let regular_timeline = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["c", "d"],
            dummy_metadata(Lsn(0x40)),
        )?;
        ensure_correct_timeline_upload(
            &repo_harness,
            Arc::clone(&remote_assets),
            TIMELINE_ID,
            regular_timeline,
        )
        .await;

        fs::remove_dir_all(&regular_timeline_path).await?;
        let remote_regular_timeline = expect_timeline(index, sync_id).await;

        download_timeline(
            repo_harness.conf,
            Arc::clone(&remote_assets),
            sync_id,
            TimelineDownload {
                files_to_skip: Arc::new(BTreeSet::new()),
                archives_to_skip: BTreeSet::new(),
            },
            0,
        )
        .await;
        assert_index_descriptions(
            index,
            RemoteTimelineIndex::try_parse_descriptions_from_paths(
                repo_harness.conf,
                remote_assets
                    .0
                    .list()
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|storage_path| storage.local_path(&storage_path).unwrap()),
            ),
        )
        .await;
        assert_timeline_files_match(&repo_harness, TIMELINE_ID, remote_regular_timeline);

        Ok(())
    }
}
