//! Timeline synchronization logic to compress and upload to the remote storage all new timeline files from the checkpoints.

use std::{borrow::Cow, collections::BTreeSet, path::PathBuf, sync::Arc};

use tracing::{debug, error, warn};

use crate::{
    config::PageServerConf,
    remote_storage::{
        storage_sync::{
            compression, fetch_full_index,
            index::{RemoteTimeline, TimelineIndexEntry, TimelineIndexEntryInner},
            sync_queue, SyncKind, SyncTask,
        },
        RemoteStorage, ZTenantTimelineId,
    },
};

use super::{compression::ArchiveHeader, NewCheckpoint, RemoteIndex};

/// Attempts to compress and upload given checkpoint files.
/// No extra checks for overlapping files is made: download takes care of that, ensuring no non-metadata local timeline files are overwritten.
///
/// On an error, bumps the retries count and reschedules the entire task.
/// On success, populates index data with new downloads.
pub(super) async fn upload_timeline_checkpoint<
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    remote_assets: Arc<(S, RemoteIndex)>,
    sync_id: ZTenantTimelineId,
    new_checkpoint: NewCheckpoint,
    retries: u32,
) -> Option<bool> {
    debug!("Uploading checkpoint for sync id {}", sync_id);
    let new_upload_lsn = new_checkpoint.metadata.disk_consistent_lsn();

    let index = &remote_assets.1;

    let ZTenantTimelineId {
        tenant_id,
        timeline_id,
    } = sync_id;
    let timeline_dir = config.timeline_path(&timeline_id, &tenant_id);

    let index_read = index.read().await;
    let remote_timeline = match index_read.timeline_entry(&sync_id) {
        None => None,
        Some(entry) => match entry.inner() {
            TimelineIndexEntryInner::Full(remote_timeline) => Some(Cow::Borrowed(remote_timeline)),
            TimelineIndexEntryInner::Description(_) => {
                debug!("Found timeline description for the given ids, downloading the full index");
                match fetch_full_index(remote_assets.as_ref(), &timeline_dir, sync_id).await {
                    Ok(remote_timeline) => Some(Cow::Owned(remote_timeline)),
                    Err(e) => {
                        error!("Failed to download full timeline index: {:?}", e);
                        sync_queue::push(SyncTask::new(
                            sync_id,
                            retries,
                            SyncKind::Upload(new_checkpoint),
                        ));
                        return Some(false);
                    }
                }
            }
        },
    };

    let already_contains_upload_lsn = remote_timeline
        .as_ref()
        .map(|remote_timeline| remote_timeline.contains_checkpoint_at(new_upload_lsn))
        .unwrap_or(false);
    if already_contains_upload_lsn {
        warn!(
            "Received a checkpoint with Lsn {} that's already been uploaded to remote storage, skipping the upload.",
            new_upload_lsn
        );
        return None;
    }

    let already_uploaded_files = remote_timeline
        .map(|timeline| timeline.stored_files(&timeline_dir))
        .unwrap_or_default();
    drop(index_read);

    match try_upload_checkpoint(
        config,
        Arc::clone(&remote_assets),
        sync_id,
        &new_checkpoint,
        already_uploaded_files,
    )
    .await
    {
        Some(Ok((archive_header, header_size))) => {
            let mut index_write = index.write().await;
            match index_write
                .timeline_entry_mut(&sync_id)
                .map(|e| e.inner_mut())
            {
                None => {
                    let mut new_timeline = RemoteTimeline::empty();
                    new_timeline.update_archive_contents(
                        new_checkpoint.metadata.disk_consistent_lsn(),
                        archive_header,
                        header_size,
                    );
                    index_write.add_timeline_entry(
                        sync_id,
                        TimelineIndexEntry::new(TimelineIndexEntryInner::Full(new_timeline), false),
                    )
                }
                Some(TimelineIndexEntryInner::Full(remote_timeline)) => {
                    remote_timeline.update_archive_contents(
                        new_checkpoint.metadata.disk_consistent_lsn(),
                        archive_header,
                        header_size,
                    );
                }
                Some(TimelineIndexEntryInner::Description(_)) => {
                    let mut new_timeline = RemoteTimeline::empty();
                    new_timeline.update_archive_contents(
                        new_checkpoint.metadata.disk_consistent_lsn(),
                        archive_header,
                        header_size,
                    );
                    index_write.add_timeline_entry(
                        sync_id,
                        TimelineIndexEntry::new(TimelineIndexEntryInner::Full(new_timeline), false),
                    )
                }
            }
            debug!("Checkpoint uploaded successfully");
            Some(true)
        }
        Some(Err(e)) => {
            error!(
                "Failed to upload checkpoint: {:?}, requeueing the upload",
                e
            );
            sync_queue::push(SyncTask::new(
                sync_id,
                retries,
                SyncKind::Upload(new_checkpoint),
            ));
            Some(false)
        }
        None => Some(true),
    }
}

async fn try_upload_checkpoint<
    P: Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
>(
    config: &'static PageServerConf,
    remote_assets: Arc<(S, RemoteIndex)>,
    sync_id: ZTenantTimelineId,
    new_checkpoint: &NewCheckpoint,
    files_to_skip: BTreeSet<PathBuf>,
) -> Option<anyhow::Result<(ArchiveHeader, u64)>> {
    let ZTenantTimelineId {
        tenant_id,
        timeline_id,
    } = sync_id;
    let timeline_dir = config.timeline_path(&timeline_id, &tenant_id);

    let files_to_upload = new_checkpoint
        .layers
        .iter()
        .filter(|&path_to_upload| {
            if files_to_skip.contains(path_to_upload) {
                warn!(
                    "Skipping file upload '{}', since it was already uploaded",
                    path_to_upload.display()
                );
                false
            } else {
                true
            }
        })
        .collect::<Vec<_>>();

    if files_to_upload.is_empty() {
        warn!(
            "No files to upload. Upload request was: {:?}, already uploaded files: {:?}",
            new_checkpoint.layers, files_to_skip
        );
        return None;
    }

    let upload_result = compression::archive_files_as_stream(
        &timeline_dir,
        files_to_upload.into_iter(),
        &new_checkpoint.metadata,
        move |archive_streamer, archive_name| async move {
            let timeline_dir = config.timeline_path(&timeline_id, &tenant_id);
            let remote_storage = &remote_assets.0;
            remote_storage
                .upload(
                    archive_streamer,
                    &remote_storage.storage_path(&timeline_dir.join(&archive_name))?,
                )
                .await
        },
    )
    .await
    .map(|(header, header_size, _)| (header, header_size));

    Some(upload_result)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use zenith_utils::lsn::Lsn;

    use crate::{
        remote_storage::{
            local_fs::LocalFs,
            storage_sync::{
                index::ArchiveId,
                test_utils::{
                    assert_index_descriptions, create_local_timeline, dummy_metadata,
                    ensure_correct_timeline_upload, expect_timeline,
                },
            },
        },
        repository::repo_harness::{RepoHarness, TIMELINE_ID},
    };

    use super::*;

    #[tokio::test]
    async fn reupload_timeline() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("reupload_timeline")?;
        let sync_id = ZTenantTimelineId::new(repo_harness.tenant_id, TIMELINE_ID);
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let index = RemoteIndex::try_parse_descriptions_from_paths(
            repo_harness.conf,
            storage
                .list()
                .await?
                .into_iter()
                .map(|storage_path| storage.local_path(&storage_path).unwrap()),
        );
        let remote_assets = Arc::new((storage, index));
        let index = &remote_assets.1;

        let first_upload_metadata = dummy_metadata(Lsn(0x10));
        let first_checkpoint = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["a", "b"],
            first_upload_metadata.clone(),
        )?;
        let local_timeline_path = repo_harness.timeline_path(&TIMELINE_ID);
        ensure_correct_timeline_upload(
            &repo_harness,
            Arc::clone(&remote_assets),
            TIMELINE_ID,
            first_checkpoint,
        )
        .await;

        let uploaded_timeline = expect_timeline(index, sync_id).await;
        let uploaded_archives = uploaded_timeline
            .checkpoints()
            .map(ArchiveId)
            .collect::<Vec<_>>();
        assert_eq!(
            uploaded_archives.len(),
            1,
            "Only one archive is expected after a first upload"
        );
        let first_uploaded_archive = uploaded_archives.first().copied().unwrap();
        assert_eq!(
            uploaded_timeline.checkpoints().last(),
            Some(first_upload_metadata.disk_consistent_lsn()),
            "Metadata that was uploaded, should have its Lsn stored"
        );
        assert_eq!(
            uploaded_timeline
                .archive_data(uploaded_archives.first().copied().unwrap())
                .unwrap()
                .disk_consistent_lsn(),
            first_upload_metadata.disk_consistent_lsn(),
            "Uploaded archive should have corresponding Lsn"
        );
        assert_eq!(
            uploaded_timeline.stored_files(&local_timeline_path),
            vec![local_timeline_path.join("a"), local_timeline_path.join("b")]
                .into_iter()
                .collect(),
            "Should have all files from the first checkpoint"
        );

        let second_upload_metadata = dummy_metadata(Lsn(0x40));
        let second_checkpoint = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["b", "c"],
            second_upload_metadata.clone(),
        )?;
        assert!(
            first_upload_metadata.disk_consistent_lsn()
                < second_upload_metadata.disk_consistent_lsn()
        );
        ensure_correct_timeline_upload(
            &repo_harness,
            Arc::clone(&remote_assets),
            TIMELINE_ID,
            second_checkpoint,
        )
        .await;

        let updated_timeline = expect_timeline(index, sync_id).await;
        let mut updated_archives = updated_timeline
            .checkpoints()
            .map(ArchiveId)
            .collect::<Vec<_>>();
        assert_eq!(
            updated_archives.len(),
            2,
            "Two archives are expected after a successful update of the upload"
        );
        updated_archives.retain(|archive_id| archive_id != &first_uploaded_archive);
        assert_eq!(
            updated_archives.len(),
            1,
            "Only one new archive is expected among the uploaded"
        );
        let second_uploaded_archive = updated_archives.last().copied().unwrap();
        assert_eq!(
            updated_timeline.checkpoints().max(),
            Some(second_upload_metadata.disk_consistent_lsn()),
            "Metadata that was uploaded, should have its Lsn stored"
        );
        assert_eq!(
            updated_timeline
                .archive_data(second_uploaded_archive)
                .unwrap()
                .disk_consistent_lsn(),
            second_upload_metadata.disk_consistent_lsn(),
            "Uploaded archive should have corresponding Lsn"
        );
        assert_eq!(
            updated_timeline.stored_files(&local_timeline_path),
            vec![
                local_timeline_path.join("a"),
                local_timeline_path.join("b"),
                local_timeline_path.join("c"),
            ]
            .into_iter()
            .collect(),
            "Should have all files from both checkpoints without duplicates"
        );

        let third_upload_metadata = dummy_metadata(Lsn(0x20));
        let third_checkpoint = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["d"],
            third_upload_metadata.clone(),
        )?;
        assert_ne!(
            third_upload_metadata.disk_consistent_lsn(),
            first_upload_metadata.disk_consistent_lsn()
        );
        assert!(
            third_upload_metadata.disk_consistent_lsn()
                < second_upload_metadata.disk_consistent_lsn()
        );
        ensure_correct_timeline_upload(
            &repo_harness,
            Arc::clone(&remote_assets),
            TIMELINE_ID,
            third_checkpoint,
        )
        .await;

        let updated_timeline = expect_timeline(index, sync_id).await;
        let mut updated_archives = updated_timeline
            .checkpoints()
            .map(ArchiveId)
            .collect::<Vec<_>>();
        assert_eq!(
            updated_archives.len(),
            3,
            "Three archives are expected after two successful updates of the upload"
        );
        updated_archives.retain(|archive_id| {
            archive_id != &first_uploaded_archive && archive_id != &second_uploaded_archive
        });
        assert_eq!(
            updated_archives.len(),
            1,
            "Only one new archive is expected among the uploaded"
        );
        let third_uploaded_archive = updated_archives.last().copied().unwrap();
        assert!(
            updated_timeline.checkpoints().max().unwrap()
                > third_upload_metadata.disk_consistent_lsn(),
            "Should not influence the last lsn by uploading an older checkpoint"
        );
        assert_eq!(
            updated_timeline
                .archive_data(third_uploaded_archive)
                .unwrap()
                .disk_consistent_lsn(),
            third_upload_metadata.disk_consistent_lsn(),
            "Uploaded archive should have corresponding Lsn"
        );
        assert_eq!(
            updated_timeline.stored_files(&local_timeline_path),
            vec![
                local_timeline_path.join("a"),
                local_timeline_path.join("b"),
                local_timeline_path.join("c"),
                local_timeline_path.join("d"),
            ]
            .into_iter()
            .collect(),
            "Should have all files from three checkpoints without duplicates"
        );

        Ok(())
    }

    #[tokio::test]
    async fn reupload_timeline_rejected() -> anyhow::Result<()> {
        let repo_harness = RepoHarness::create("reupload_timeline_rejected")?;
        let sync_id = ZTenantTimelineId::new(repo_harness.tenant_id, TIMELINE_ID);
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &repo_harness.conf.workdir)?;
        let index = RemoteIndex::try_parse_descriptions_from_paths(
            repo_harness.conf,
            storage
                .list()
                .await?
                .into_iter()
                .map(|storage_path| storage.local_path(&storage_path).unwrap()),
        );
        let remote_assets = Arc::new((storage, index));
        let storage = &remote_assets.0;
        let index = &remote_assets.1;

        let first_upload_metadata = dummy_metadata(Lsn(0x10));
        let first_checkpoint = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["a", "b"],
            first_upload_metadata.clone(),
        )?;
        ensure_correct_timeline_upload(
            &repo_harness,
            Arc::clone(&remote_assets),
            TIMELINE_ID,
            first_checkpoint,
        )
        .await;
        let after_first_uploads = RemoteIndex::try_parse_descriptions_from_paths(
            repo_harness.conf,
            remote_assets
                .0
                .list()
                .await
                .unwrap()
                .into_iter()
                .map(|storage_path| storage.local_path(&storage_path).unwrap()),
        );

        let normal_upload_metadata = dummy_metadata(Lsn(0x20));
        assert_ne!(
            normal_upload_metadata.disk_consistent_lsn(),
            first_upload_metadata.disk_consistent_lsn()
        );

        let checkpoint_with_no_files = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &[],
            normal_upload_metadata.clone(),
        )?;
        upload_timeline_checkpoint(
            repo_harness.conf,
            Arc::clone(&remote_assets),
            sync_id,
            checkpoint_with_no_files,
            0,
        )
        .await;
        assert_index_descriptions(index, &after_first_uploads).await;

        let checkpoint_with_uploaded_lsn = create_local_timeline(
            &repo_harness,
            TIMELINE_ID,
            &["something", "new"],
            first_upload_metadata.clone(),
        )?;
        upload_timeline_checkpoint(
            repo_harness.conf,
            Arc::clone(&remote_assets),
            sync_id,
            checkpoint_with_uploaded_lsn,
            0,
        )
        .await;
        assert_index_descriptions(index, &after_first_uploads).await;

        Ok(())
    }
}
