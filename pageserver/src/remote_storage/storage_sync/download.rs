//! Timeline synchrnonization logic to fetch the layer files from remote storage into pageserver's local directory.

use std::fmt::Debug;

use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::fs;
use tracing::{debug, error, info, warn};

use crate::{
    config::PageServerConf,
    layered_repository::metadata::metadata_path,
    remote_storage::{
        storage_sync::{sync_queue, SyncTask},
        RemoteStorage, ZTenantTimelineId,
    },
};

use super::{
    index::{IndexPart, RemoteTimeline},
    SyncData, TimelineDownload,
};

/// Retrieves index data from the remote storage for a given timeline.
pub async fn download_index_part<P, S>(
    conf: &'static PageServerConf,
    storage: &S,
    sync_id: ZTenantTimelineId,
) -> anyhow::Result<IndexPart>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
{
    let index_part_path = metadata_path(conf, sync_id.timeline_id, sync_id.tenant_id)
        .with_file_name(IndexPart::FILE_NAME)
        .with_extension(IndexPart::FILE_EXTENSION);
    let part_storage_path = storage.storage_path(&index_part_path).with_context(|| {
        format!(
            "Failed to get the index part storage path for local path '{}'",
            index_part_path.display()
        )
    })?;
    let mut index_part_bytes = Vec::new();
    storage
        .download(&part_storage_path, &mut index_part_bytes)
        .await
        .with_context(|| {
            format!("Failed to download an index part from storage path '{part_storage_path:?}'")
        })?;

    let index_part: IndexPart = serde_json::from_slice(&index_part_bytes).with_context(|| {
        format!("Failed to deserialize index part file from storage path '{part_storage_path:?}'")
    })?;

    let missing_files = index_part.missing_files();
    if !missing_files.is_empty() {
        warn!("Found missing layers in index part for timeline {sync_id}: {missing_files:?}");
    }

    Ok(index_part)
}

/// Timeline download result, with extra data, needed for downloading.
#[derive(Debug)]
pub(super) enum DownloadedTimeline {
    /// Remote timeline data is either absent or corrupt, no download possible.
    Abort,
    /// Remote timeline data is found, its latest checkpoint's metadata contents (disk_consistent_lsn) is known.
    /// Initial download failed due to some error, the download task is rescheduled for another retry.
    FailedAndRescheduled,
    /// Remote timeline data is found, its latest checkpoint's metadata contents (disk_consistent_lsn) is known.
    /// Initial download successful.
    Successful(SyncData<TimelineDownload>),
}

/// Attempts to download all given timeline's layers.
/// Timeline files that already exist locally are skipped during the download, but the local metadata file is
/// updated in the end, if the remote one contains a newer disk_consistent_lsn.
///
/// On an error, bumps the retries count and updates the files to skip with successful downloads, rescheduling the task.
pub(super) async fn download_timeline_layers<'a, P, S>(
    storage: &'a S,
    remote_timeline: Option<&'a RemoteTimeline>,
    sync_id: ZTenantTimelineId,
    mut download_data: SyncData<TimelineDownload>,
) -> DownloadedTimeline
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
{
    let remote_timeline = match remote_timeline {
        Some(remote_timeline) => {
            if !remote_timeline.awaits_download {
                error!("Timeline with sync id {sync_id} is not awaiting download");
                return DownloadedTimeline::Abort;
            }
            remote_timeline
        }
        None => {
            error!("Timeline with sync id {sync_id} is not present in the remote index");
            return DownloadedTimeline::Abort;
        }
    };

    let download = &mut download_data.data;

    let layers_to_download = remote_timeline
        .stored_files()
        .difference(&download.layers_to_skip)
        .cloned()
        .collect::<Vec<_>>();

    debug!("Layers to download: {layers_to_download:?}");
    info!("Downloading {} timeline layers", layers_to_download.len());

    let mut download_tasks = layers_to_download
        .into_iter()
        .map(|layer_desination_path| async move {
            if layer_desination_path.exists() {
                debug!(
                    "Layer already exists locally, skipping download: {}",
                    layer_desination_path.display()
                );
            } else {
                let layer_storage_path = storage
                    .storage_path(&layer_desination_path)
                    .with_context(|| {
                        format!(
                            "Failed to get the layer storage path for local path '{}'",
                            layer_desination_path.display()
                        )
                    })?;

                let mut destination_file = fs::File::create(&layer_desination_path)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to create a destination file for layer '{}'",
                            layer_desination_path.display()
                        )
                    })?;

                storage
                    .download(&layer_storage_path, &mut destination_file)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to download a layer from storage path '{layer_storage_path:?}'"
                        )
                    })?;
            }
            Ok::<_, anyhow::Error>(layer_desination_path)
        })
        .collect::<FuturesUnordered<_>>();

    let mut errors_happened = false;
    while let Some(download_result) = download_tasks.next().await {
        match download_result {
            Ok(downloaded_path) => {
                download.layers_to_skip.insert(downloaded_path);
            }
            Err(e) => {
                errors_happened = true;
                error!("Failed to download a layer for timeline {sync_id}: {e:?}");
            }
        }
    }

    if errors_happened {
        debug!("Reenqueuing failed download task for timeline {sync_id}");
        download_data.retries += 1;
        sync_queue::push(sync_id, SyncTask::Download(download_data));
        DownloadedTimeline::FailedAndRescheduled
    } else {
        info!("Successfully downloaded all layers");
        DownloadedTimeline::Successful(download_data)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use tempfile::tempdir;
    use zenith_utils::lsn::Lsn;

    use crate::{
        remote_storage::{
            storage_sync::{
                index::RelativePath,
                test_utils::{create_local_timeline, dummy_metadata},
            },
            LocalFs,
        },
        repository::repo_harness::{RepoHarness, TIMELINE_ID},
    };

    use super::*;

    #[tokio::test]
    async fn download_timeline() -> anyhow::Result<()> {
        let harness = RepoHarness::create("download_timeline")?;
        let sync_id = ZTenantTimelineId::new(harness.tenant_id, TIMELINE_ID);
        let layer_files = ["a", "b", "layer_to_skip", "layer_to_keep_locally"];
        let storage = LocalFs::new(tempdir()?.path().to_path_buf(), &harness.conf.workdir)?;
        let current_retries = 3;
        let metadata = dummy_metadata(Lsn(0x30));
        let local_timeline_path = harness.timeline_path(&TIMELINE_ID);
        let timeline_upload =
            create_local_timeline(&harness, TIMELINE_ID, &layer_files, metadata.clone()).await?;

        for local_path in timeline_upload.layers_to_upload {
            let remote_path = storage.storage_path(&local_path)?;
            let remote_parent_dir = remote_path.parent().unwrap();
            if !remote_parent_dir.exists() {
                fs::create_dir_all(&remote_parent_dir).await?;
            }
            fs::copy(&local_path, &remote_path).await?;
        }
        let mut read_dir = fs::read_dir(&local_timeline_path).await?;
        while let Some(dir_entry) = read_dir.next_entry().await? {
            if dir_entry.file_name().to_str() == Some("layer_to_keep_locally") {
                continue;
            } else {
                fs::remove_file(dir_entry.path()).await?;
            }
        }

        let mut remote_timeline = RemoteTimeline::new(metadata.clone());
        remote_timeline.awaits_download = true;
        remote_timeline.add_timeline_layers(
            layer_files
                .iter()
                .map(|layer| local_timeline_path.join(layer)),
        );

        let download_data = match download_timeline_layers(
            &storage,
            Some(&remote_timeline),
            sync_id,
            SyncData::new(
                current_retries,
                TimelineDownload {
                    layers_to_skip: HashSet::from([local_timeline_path.join("layer_to_skip")]),
                },
            ),
        )
        .await
        {
            DownloadedTimeline::Successful(data) => data,
            wrong_result => {
                panic!("Expected a successful download for timeline, but got: {wrong_result:?}")
            }
        };

        assert_eq!(
            current_retries, download_data.retries,
            "On successful download, retries are not expected to change"
        );
        assert_eq!(
            download_data
                .data
                .layers_to_skip
                .into_iter()
                .collect::<BTreeSet<_>>(),
            layer_files
                .iter()
                .map(|layer| local_timeline_path.join(layer))
                .collect(),
            "On successful download, layers to skip should contain all downloaded files and present layers that were skipped"
        );

        let mut downloaded_files = BTreeSet::new();
        let mut read_dir = fs::read_dir(&local_timeline_path).await?;
        while let Some(dir_entry) = read_dir.next_entry().await? {
            downloaded_files.insert(dir_entry.path());
        }

        assert_eq!(
            downloaded_files,
            layer_files
                .iter()
                .filter(|layer| layer != &&"layer_to_skip")
                .map(|layer| local_timeline_path.join(layer))
                .collect(),
            "On successful download, all layers that were not skipped, should be downloaded"
        );

        Ok(())
    }

    #[tokio::test]
    async fn download_timeline_negatives() -> anyhow::Result<()> {
        let harness = RepoHarness::create("download_timeline_negatives")?;
        let sync_id = ZTenantTimelineId::new(harness.tenant_id, TIMELINE_ID);
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &harness.conf.workdir)?;

        let empty_remote_timeline_download = download_timeline_layers(
            &storage,
            None,
            sync_id,
            SyncData::new(
                0,
                TimelineDownload {
                    layers_to_skip: HashSet::new(),
                },
            ),
        )
        .await;
        assert!(
            matches!(empty_remote_timeline_download, DownloadedTimeline::Abort),
            "Should not allow downloading for empty remote timeline"
        );

        let not_expecting_download_remote_timeline = RemoteTimeline::new(dummy_metadata(Lsn(5)));
        assert!(
            !not_expecting_download_remote_timeline.awaits_download,
            "Should not expect download for the timeline"
        );
        let already_downloading_remote_timeline_download = download_timeline_layers(
            &storage,
            Some(&not_expecting_download_remote_timeline),
            sync_id,
            SyncData::new(
                0,
                TimelineDownload {
                    layers_to_skip: HashSet::new(),
                },
            ),
        )
        .await;
        assert!(
            matches!(
                dbg!(already_downloading_remote_timeline_download),
                DownloadedTimeline::Abort,
            ),
            "Should not allow downloading for remote timeline that does not expect it"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_download_index_part() -> anyhow::Result<()> {
        let harness = RepoHarness::create("test_download_index_part")?;
        let sync_id = ZTenantTimelineId::new(harness.tenant_id, TIMELINE_ID);

        let storage = LocalFs::new(tempdir()?.path().to_path_buf(), &harness.conf.workdir)?;
        let metadata = dummy_metadata(Lsn(0x30));
        let local_timeline_path = harness.timeline_path(&TIMELINE_ID);

        let index_part = IndexPart::new(
            HashSet::from([
                RelativePath::new(&local_timeline_path, local_timeline_path.join("one"))?,
                RelativePath::new(&local_timeline_path, local_timeline_path.join("two"))?,
            ]),
            HashSet::from([RelativePath::new(
                &local_timeline_path,
                local_timeline_path.join("three"),
            )?]),
            metadata.disk_consistent_lsn(),
            metadata.to_bytes()?,
        );

        let local_index_part_path =
            metadata_path(harness.conf, sync_id.timeline_id, sync_id.tenant_id)
                .with_file_name(IndexPart::FILE_NAME)
                .with_extension(IndexPart::FILE_EXTENSION);
        let storage_path = storage.storage_path(&local_index_part_path)?;
        fs::create_dir_all(storage_path.parent().unwrap()).await?;
        fs::write(&storage_path, serde_json::to_vec(&index_part)?).await?;

        let downloaded_index_part = download_index_part(harness.conf, &storage, sync_id).await?;

        assert_eq!(
            downloaded_index_part, index_part,
            "Downloaded index part should be the same as the one in storage"
        );

        Ok(())
    }
}
