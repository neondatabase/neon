//! Timeline synchronization logic to fetch the layer files from remote storage into pageserver's local directory.

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    path::Path,
};

use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use remote_storage::{path_with_suffix_extension, RemoteObjectName, RemoteStorage};
use tokio::{
    fs,
    io::{self, AsyncWriteExt},
};
use tracing::{debug, error, info, warn};

use crate::{
    config::PageServerConf, layered_repository::metadata::metadata_path, storage_sync::SyncTask,
};
use utils::zid::{ZTenantId, ZTenantTimelineId, ZTimelineId};

use super::{
    index::{IndexPart, RemoteTimeline},
    LayersDownload, SyncData, SyncQueue,
};

pub const TEMP_DOWNLOAD_EXTENSION: &str = "temp_download";

/// FIXME: Needs cleanup. Currently it swallows errors. Here we need to ensure that
/// we successfully downloaded all metadata parts for one tenant.
/// And successful includes absence of index_part in the remote. Because it is valid situation
/// when timeline was just created and pageserver restarted before upload of index part was completed.
/// But currently RemoteStorage interface does not provide this knowledge because it uses
/// anyhow::Error as an error type. So this needs a refactoring.
///
/// In other words we need to yield only complete sets of tenant timelines.
/// Failure for one timeline of a tenant should exclude whole tenant from returned hashmap.
/// So there are two requirements: keep everything in one futures unordered
/// to allow higher concurrency. Mark tenants as failed independently.
/// That requires some bookeeping.
pub async fn download_index_parts<P, S>(
    conf: &'static PageServerConf,
    storage: &S,
    keys: HashSet<ZTenantTimelineId>,
) -> HashMap<ZTenantId, HashMap<ZTimelineId, IndexPart>>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let mut index_parts: HashMap<ZTenantId, HashMap<ZTimelineId, IndexPart>> = HashMap::new();

    let mut part_downloads = keys
        .into_iter()
        .map(|id| async move { (id, download_index_part(conf, storage, id).await) })
        .collect::<FuturesUnordered<_>>();

    while let Some((id, part_upload_result)) = part_downloads.next().await {
        match part_upload_result {
            Ok(index_part) => {
                debug!("Successfully fetched index part for {id}");
                index_parts
                    .entry(id.tenant_id)
                    .or_default()
                    .insert(id.timeline_id, index_part);
            }
            Err(e) => error!("Failed to fetch index part for {id}: {e}"),
        }
    }

    index_parts
}

/// Note: The function is rather expensive from s3 access point of view, it will execute ceil(N/1000) + N requests.
/// At least one request to obtain a list of tenant timelines (more requests is there are more than 1000 timelines).
/// And then will attempt to download all index files that belong to these timelines.
pub async fn gather_tenant_timelines_index_parts<P, S>(
    conf: &'static PageServerConf,
    storage: &S,
    tenant_id: ZTenantId,
) -> anyhow::Result<HashMap<ZTimelineId, IndexPart>>
where
    P: RemoteObjectName + Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let tenant_path = conf.timelines_path(&tenant_id);
    let tenant_storage_path = storage.remote_object_id(&tenant_path).with_context(|| {
        format!(
            "Failed to get tenant storage path for local path '{}'",
            tenant_path.display()
        )
    })?;
    let timelines = storage
        .list_prefixes(Some(tenant_storage_path))
        .await
        .with_context(|| {
            format!(
                "Failed to list tenant storage path to get remote timelines to download: {}",
                tenant_id
            )
        })?;

    let mut sync_ids = HashSet::new();

    for timeline_remote_storage_key in timelines {
        let object_name = timeline_remote_storage_key.object_name().ok_or_else(|| {
            anyhow::anyhow!("failed to get timeline id for remote tenant {tenant_id}")
        })?;

        let timeline_id: ZTimelineId = object_name
            .parse()
            .with_context(|| {
                format!("failed to parse object name into timeline id for tenant {tenant_id} '{object_name}'")
            })?;

        sync_ids.insert(ZTenantTimelineId {
            tenant_id,
            timeline_id,
        });
    }

    download_index_parts(conf, storage, sync_ids)
        .await
        .remove(&tenant_id)
        .ok_or(anyhow::anyhow!(
            "Missing tenant index parts. This is a bug."
        ))
}

/// Retrieves index data from the remote storage for a given timeline.
async fn download_index_part<P, S>(
    conf: &'static PageServerConf,
    storage: &S,
    sync_id: ZTenantTimelineId,
) -> anyhow::Result<IndexPart>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let index_part_path = metadata_path(conf, sync_id.timeline_id, sync_id.tenant_id)
        .with_file_name(IndexPart::FILE_NAME)
        .with_extension(IndexPart::FILE_EXTENSION);
    let part_storage_path = storage
        .remote_object_id(&index_part_path)
        .with_context(|| {
            format!(
                "Failed to get the index part storage path for local path '{}'",
                index_part_path.display()
            )
        })?;

    let mut index_part_download =
        storage
            .download(&part_storage_path)
            .await
            .with_context(|| {
                format!("Failed to open download stream for for storage path {part_storage_path:?}")
            })?;
    let mut index_part_bytes = Vec::new();
    io::copy(
        &mut index_part_download.download_stream,
        &mut index_part_bytes,
    )
    .await
    .with_context(|| {
        format!("Failed to download an index part from storage path {part_storage_path:?}")
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
    Successful(SyncData<LayersDownload>),
}

/// Attempts to download all given timeline's layers.
/// Timeline files that already exist locally are skipped during the download, but the local metadata file is
/// updated in the end, if the remote one contains a newer disk_consistent_lsn.
///
/// On an error, bumps the retries count and updates the files to skip with successful downloads, rescheduling the task.
pub(super) async fn download_timeline_layers<'a, P, S>(
    conf: &'static PageServerConf,
    storage: &'a S,
    sync_queue: &'a SyncQueue,
    remote_timeline: Option<&'a RemoteTimeline>,
    sync_id: ZTenantTimelineId,
    mut download_data: SyncData<LayersDownload>,
) -> DownloadedTimeline
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
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

    if layers_to_download.is_empty() {
        info!("No layers to download after filtering, skipping");
        return DownloadedTimeline::Successful(download_data);
    }

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
                    .remote_object_id(&layer_desination_path)
                    .with_context(|| {
                        format!(
                            "Failed to get the layer storage path for local path '{}'",
                            layer_desination_path.display()
                        )
                    })?;

                // Perform a rename inspired by durable_rename from file_utils.c.
                // The sequence:
                //     write(tmp)
                //     fsync(tmp)
                //     rename(tmp, new)
                //     fsync(new)
                //     fsync(parent)
                // For more context about durable_rename check this email from postgres mailing list:
                // https://www.postgresql.org/message-id/56583BDD.9060302@2ndquadrant.com
                // If pageserver crashes the temp file will be deleted on startup and re-downloaded.
                let temp_file_path =
                    path_with_suffix_extension(&layer_desination_path, TEMP_DOWNLOAD_EXTENSION);

                let mut destination_file =
                    fs::File::create(&temp_file_path).await.with_context(|| {
                        format!(
                            "Failed to create a destination file for layer '{}'",
                            temp_file_path.display()
                        )
                    })?;
                let mut download = storage
                    .download(&layer_storage_path)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to open a download stream for layer with remote storage path '{layer_storage_path:?}'"
                        )
                    })?;
                io::copy(&mut download.download_stream, &mut destination_file).await.with_context(|| {
                    format!(
                        "Failed to download layer with remote storage path '{layer_storage_path:?}' into file '{}'", temp_file_path.display()
                    )
                })?;

                // Tokio doc here: https://docs.rs/tokio/1.17.0/tokio/fs/struct.File.html states that:
                // A file will not be closed immediately when it goes out of scope if there are any IO operations
                // that have not yet completed. To ensure that a file is closed immediately when it is dropped,
                // you should call flush before dropping it.
                //
                // From the tokio code I see that it waits for pending operations to complete. There shouldt be any because
                // we assume that `destination_file` file is fully written. I e there is no pending .write(...).await operations.
                // But for additional safety lets check/wait for any pending operations.
                destination_file.flush().await.with_context(|| {
                    format!(
                        "failed to flush source file at {}",
                        temp_file_path.display()
                    )
                })?;

                // not using sync_data because it can lose file size update
                destination_file.sync_all().await.with_context(|| {
                    format!(
                        "failed to fsync source file at {}",
                        temp_file_path.display()
                    )
                })?;
                drop(destination_file);

                fail::fail_point!("remote-storage-download-pre-rename", |_| {
                    anyhow::bail!("remote-storage-download-pre-rename failpoint triggered")
                });

                fs::rename(&temp_file_path, &layer_desination_path).await?;

                fsync_path(&layer_desination_path).await.with_context(|| {
                    format!(
                        "Cannot fsync layer destination path {}",
                        layer_desination_path.display(),
                    )
                })?;
            }
            Ok::<_, anyhow::Error>(layer_desination_path)
        })
        .collect::<FuturesUnordered<_>>();

    let mut errors_happened = false;
    // keep files we've downloaded to remove them from layers_to_skip if directory fsync fails
    let mut undo = HashSet::new();
    while let Some(download_result) = download_tasks.next().await {
        match download_result {
            Ok(downloaded_path) => {
                undo.insert(downloaded_path.clone());
                download.layers_to_skip.insert(downloaded_path);
            }
            Err(e) => {
                errors_happened = true;
                error!("Failed to download a layer for timeline {sync_id}: {e:?}");
            }
        }
    }

    // fsync timeline directory which is a parent directory for downloaded files
    let ZTenantTimelineId {
        tenant_id,
        timeline_id,
    } = &sync_id;
    let timeline_dir = conf.timeline_path(timeline_id, tenant_id);
    if let Err(e) = fsync_path(&timeline_dir).await {
        error!(
            "Cannot fsync parent directory {} error {}",
            timeline_dir.display(),
            e
        );
        for item in undo {
            download.layers_to_skip.remove(&item);
        }
        errors_happened = true;
    }

    if errors_happened {
        debug!("Reenqueuing failed download task for timeline {sync_id}");
        download_data.retries += 1;
        sync_queue.push(sync_id, SyncTask::Download(download_data));
        DownloadedTimeline::FailedAndRescheduled
    } else {
        info!("Successfully downloaded all layers");
        DownloadedTimeline::Successful(download_data)
    }
}

async fn fsync_path(path: impl AsRef<Path>) -> Result<(), io::Error> {
    fs::File::open(path).await?.sync_all().await
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, HashSet},
        num::NonZeroUsize,
    };

    use remote_storage::{LocalFs, RemoteStorage};
    use tempfile::tempdir;
    use utils::lsn::Lsn;

    use crate::{
        repository::repo_harness::{RepoHarness, TIMELINE_ID},
        storage_sync::{
            index::RelativePath,
            test_utils::{create_local_timeline, dummy_metadata},
        },
    };

    use super::*;

    #[tokio::test]
    async fn download_timeline() -> anyhow::Result<()> {
        let harness = RepoHarness::create("download_timeline")?;
        let sync_queue = SyncQueue::new(NonZeroUsize::new(100).unwrap());

        let sync_id = ZTenantTimelineId::new(harness.tenant_id, TIMELINE_ID);
        let layer_files = ["a", "b", "layer_to_skip", "layer_to_keep_locally"];
        let storage = LocalFs::new(
            tempdir()?.path().to_path_buf(),
            harness.conf.workdir.clone(),
        )?;
        let current_retries = 3;
        let metadata = dummy_metadata(Lsn(0x30));
        let local_timeline_path = harness.timeline_path(&TIMELINE_ID);
        let timeline_upload =
            create_local_timeline(&harness, TIMELINE_ID, &layer_files, metadata.clone()).await?;

        for local_path in timeline_upload.layers_to_upload {
            let remote_path = storage.remote_object_id(&local_path)?;
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
            harness.conf,
            &storage,
            &sync_queue,
            Some(&remote_timeline),
            sync_id,
            SyncData::new(
                current_retries,
                LayersDownload {
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
        let sync_queue = SyncQueue::new(NonZeroUsize::new(100).unwrap());
        let sync_id = ZTenantTimelineId::new(harness.tenant_id, TIMELINE_ID);
        let storage = LocalFs::new(tempdir()?.path().to_owned(), harness.conf.workdir.clone())?;

        let empty_remote_timeline_download = download_timeline_layers(
            harness.conf,
            &storage,
            &sync_queue,
            None,
            sync_id,
            SyncData::new(
                0,
                LayersDownload {
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
            harness.conf,
            &storage,
            &sync_queue,
            Some(&not_expecting_download_remote_timeline),
            sync_id,
            SyncData::new(
                0,
                LayersDownload {
                    layers_to_skip: HashSet::new(),
                },
            ),
        )
        .await;
        assert!(
            matches!(
                already_downloading_remote_timeline_download,
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

        let storage = LocalFs::new(
            tempdir()?.path().to_path_buf(),
            harness.conf.workdir.clone(),
        )?;
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
        let storage_path = storage.remote_object_id(&local_index_part_path)?;
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
