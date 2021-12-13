//! Timeline synchrnonization logic to put files from archives on remote storage into pageserver's local directory.
//! Currently, tenant branch files are also downloaded, but this does not appear final.

use std::{borrow::Cow, collections::BTreeSet, path::PathBuf, sync::Arc};

use anyhow::{anyhow, ensure, Context};
use futures::{stream::FuturesUnordered, StreamExt};
use tokio::{fs, sync::RwLock};
use tracing::{debug, error, trace, warn};
use zenith_utils::zid::ZTenantId;

use crate::{
    layered_repository::metadata::{metadata_path, TimelineMetadata},
    remote_storage::{
        storage_sync::{
            compression, index::TimelineIndexEntry, sync_queue, tenant_branch_files,
            update_index_description, SyncKind, SyncTask,
        },
        RemoteStorage, TimelineSyncId,
    },
    PageServerConf,
};

use super::{
    index::{ArchiveId, RemoteTimeline, RemoteTimelineIndex},
    TimelineDownload,
};

/// Attempts to download and uncompress files from all remote archives for the timeline given.
/// Timeline files that already exist locally are skipped during the download, but the local metadata file is
/// updated in the end of every checkpoint archive extraction.
///
/// Before any archives are considered, the branch files are checked locally and remotely, all remote-only files are downloaded.
///
/// On an error, bumps the retries count and reschedules the download, with updated archive skip list
/// (for any new successful archive downloads and extractions).
pub(super) async fn download_timeline<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    conf: &'static PageServerConf,
    remote_assets: Arc<(S, RwLock<RemoteTimelineIndex>)>,
    sync_id: TimelineSyncId,
    mut download: TimelineDownload,
    retries: u32,
) -> Option<bool> {
    debug!("Downloading layers for sync id {}", sync_id);
    if let Err(e) = download_missing_branches(conf, remote_assets.as_ref(), sync_id.0).await {
        error!(
            "Failed to download missing branches for sync id {}: {:#}",
            sync_id, e
        );
        sync_queue::push(SyncTask::new(
            sync_id,
            retries,
            SyncKind::Download(download),
        ));
        return Some(false);
    }

    let TimelineSyncId(tenant_id, timeline_id) = sync_id;

    let index_read = remote_assets.1.read().await;
    let remote_timeline = match index_read.timeline_entry(&sync_id) {
        None => {
            error!("Cannot download: no timeline is present in the index for given ids");
            return None;
        }
        Some(TimelineIndexEntry::Full(remote_timeline)) => Cow::Borrowed(remote_timeline),
        Some(TimelineIndexEntry::Description(_)) => {
            drop(index_read);
            debug!("Found timeline description for the given ids, downloading the full index");
            match update_index_description(
                remote_assets.as_ref(),
                &conf.timeline_path(&timeline_id, &tenant_id),
                sync_id,
            )
            .await
            {
                Ok(remote_timeline) => Cow::Owned(remote_timeline),
                Err(e) => {
                    error!("Failed to download full timeline index: {:#}", e);
                    sync_queue::push(SyncTask::new(
                        sync_id,
                        retries,
                        SyncKind::Download(download),
                    ));
                    return Some(false);
                }
            }
        }
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
            remote_timeline.as_ref(),
            archive_id,
            Arc::clone(&download.files_to_skip),
        )
        .await
        {
            Err(e) => {
                let archives_left = archives_total - archives_downloaded;
                error!(
                    "Failed to download archive {:?} (archives downloaded: {}; archives left: {}) for tenant {} timeline {}, requeueing the download: {:#}",
                    archive_id, archives_downloaded, archives_left, tenant_id, timeline_id, e
                );
                sync_queue::push(SyncTask::new(
                    sync_id,
                    retries,
                    SyncKind::Download(download),
                ));
                return Some(false);
            }
            Ok(()) => {
                debug!("Successfully downloaded archive {:?}", archive_id);
                download.archives_to_skip.insert(archive_id);
            }
        }
    }

    debug!("Finished downloading all timeline's archives");
    Some(true)
}

async fn try_download_archive<
    P: Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    conf: &'static PageServerConf,
    TimelineSyncId(tenant_id, timeline_id): TimelineSyncId,
    remote_assets: Arc<(S, RwLock<RemoteTimelineIndex>)>,
    remote_timeline: &RemoteTimeline,
    archive_id: ArchiveId,
    files_to_skip: Arc<BTreeSet<PathBuf>>,
) -> anyhow::Result<()> {
    debug!("Downloading archive {:?}", archive_id);
    let archive_to_download = remote_timeline
        .archive_data(archive_id)
        .ok_or_else(|| anyhow!("Archive {:?} not found in remote storage", archive_id))?;
    let (archive_header, header_size) = remote_timeline
        .restore_header(archive_id)
        .context("Failed to restore header when downloading an archive")?;

    match read_local_metadata(conf, timeline_id, tenant_id).await {
        Ok(local_metadata) => ensure!(
            // need to allow `<=` instead of `<` due to cases when a failed archive can be redownloaded
            local_metadata.disk_consistent_lsn() <= archive_to_download.disk_consistent_lsn(),
            "Cannot download archive with LSN {} since it's earlier than local LSN {}",
            archive_to_download.disk_consistent_lsn(),
            local_metadata.disk_consistent_lsn()
        ),
        Err(e) => warn!("Failed to read local metadata file, assuing it's safe to override its with the download. Read: {:#}", e),
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

async fn download_missing_branches<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    conf: &'static PageServerConf,
    (storage, index): &(S, RwLock<RemoteTimelineIndex>),
    tenant_id: ZTenantId,
) -> anyhow::Result<()> {
    let local_branches = tenant_branch_files(conf, tenant_id)
        .await
        .context("Failed to list local branch files for the tenant")?;
    let local_branches_dir = conf.branches_path(&tenant_id);
    if !local_branches_dir.exists() {
        fs::create_dir_all(&local_branches_dir)
            .await
            .with_context(|| {
                format!(
                    "Failed to create local branches directory at path '{}'",
                    local_branches_dir.display()
                )
            })?;
    }

    if let Some(remote_branches) = index.read().await.branch_files(tenant_id) {
        let mut remote_only_branches_downloads = remote_branches
            .difference(&local_branches)
            .map(|remote_only_branch| async move {
                let branches_dir = conf.branches_path(&tenant_id);
                let remote_branch_path = remote_only_branch.as_path(&branches_dir);
                let storage_path =
                    storage.storage_path(&remote_branch_path).with_context(|| {
                        format!(
                            "Failed to derive a storage path for branch with local path '{}'",
                            remote_branch_path.display()
                        )
                    })?;
                let mut target_file = fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&remote_branch_path)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to create local branch file at '{}'",
                            remote_branch_path.display()
                        )
                    })?;
                storage
                    .download(&storage_path, &mut target_file)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to download branch file from the remote path {:?}",
                            storage_path
                        )
                    })?;
                Ok::<_, anyhow::Error>(())
            })
            .collect::<FuturesUnordered<_>>();

        let mut branch_downloads_failed = false;
        while let Some(download_result) = remote_only_branches_downloads.next().await {
            if let Err(e) = download_result {
                branch_downloads_failed = true;
                error!("Failed to download a branch file: {:#}", e);
            }
        }
        ensure!(
            !branch_downloads_failed,
            "Failed to download all branch files"
        );
    }

    Ok(())
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
        let sync_id = TimelineSyncId(repo_harness.tenant_id, TIMELINE_ID);
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let index = RwLock::new(RemoteTimelineIndex::try_parse_descriptions_from_paths(
            repo_harness.conf,
            storage
                .list()
                .await?
                .into_iter()
                .map(|storage_path| storage.local_path(&storage_path).unwrap()),
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
