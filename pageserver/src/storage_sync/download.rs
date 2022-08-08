//! Timeline synchronization logic to fetch the layer files from remote storage into pageserver's local directory.

use std::{collections::HashSet, fmt::Debug};

use anyhow::{bail, Context};
use futures::stream::{FuturesUnordered, StreamExt};
use remote_storage::path_with_suffix_extension;
use remote_storage::{DownloadError, GenericRemoteStorage, RemoteObjectName, RemoteStorage};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::debug;

use crate::{config::PageServerConf, layered_repository::metadata::metadata_path};
use utils::zid::{ZTenantId, ZTimelineId};

use super::index::IndexPart;
use super::RelativePath;

pub const TEMP_DOWNLOAD_EXTENSION: &str = "temp_download";

async fn fsync_path(path: impl AsRef<std::path::Path>) -> Result<(), std::io::Error> {
    fs::File::open(path).await?.sync_all().await
}

pub async fn download_index_file(
    conf: &'static PageServerConf,
    storage: &GenericRemoteStorage,
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
) -> Result<IndexPart, DownloadError> {
    match &storage {
        GenericRemoteStorage::Local(s) => {
            download_index_file_impl(conf, s, tenant_id, timeline_id).await
        }
        GenericRemoteStorage::S3(s) => {
            download_index_file_impl(conf, s, tenant_id, timeline_id).await
        }
    }
}

async fn download_index_file_impl<S, P>(
    conf: &'static PageServerConf,
    storage: &S,
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
) -> Result<IndexPart, DownloadError>
where
    P: RemoteObjectName + Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let index_part_path =
        metadata_path(conf, timeline_id, tenant_id).with_file_name(IndexPart::FILE_NAME);
    let part_storage_path = storage
        .remote_object_id(&index_part_path)
        .with_context(|| {
            format!(
                "Failed to get the index part storage path for local path '{}'",
                index_part_path.display()
            )
        })
        .map_err(DownloadError::BadInput)?;
    let mut index_part_download = storage.download(&part_storage_path).await?;

    let mut index_part_bytes = Vec::new();
    tokio::io::copy(
        &mut index_part_download.download_stream,
        &mut index_part_bytes,
    )
    .await
    .with_context(|| {
        format!("Failed to download an index part from storage path {part_storage_path:?}")
    })
    .map_err(DownloadError::Other)?;

    let index_part: IndexPart = serde_json::from_slice(&index_part_bytes)
        .with_context(|| {
            format!(
                "Failed to deserialize index part file from storage path '{part_storage_path:?}'"
            )
        })
        .map_err(DownloadError::Other)?;

    Ok(index_part)
}

pub async fn download_layer_file(
    conf: &'static PageServerConf,
    storage: &GenericRemoteStorage,
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    path: &RelativePath,
) -> anyhow::Result<()> {
    match &storage {
        GenericRemoteStorage::Local(s) => {
            download_layer_file_impl(conf, s, tenant_id, timeline_id, path).await
        }
        GenericRemoteStorage::S3(s) => {
            download_layer_file_impl(conf, s, tenant_id, timeline_id, path).await
        }
    }
}

async fn download_layer_file_impl<'a, S, P>(
    conf: &'static PageServerConf,
    storage: &'a S,
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    path: &'a RelativePath,
) -> anyhow::Result<()>
where
    P: RemoteObjectName + Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    const TEMP_DOWNLOAD_EXTENSION: &str = "temp_download";

    let timeline_path = conf.timeline_path(&timeline_id, &tenant_id);

    let local_path = path.to_local_path(&timeline_path);

    let layer_storage_path = storage.remote_object_id(&local_path).with_context(|| {
        format!(
            "Failed to get the layer storage path for local path '{}'",
            local_path.display()
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
    let temp_file_path = path_with_suffix_extension(&local_path, TEMP_DOWNLOAD_EXTENSION);

    let mut destination_file = fs::File::create(&temp_file_path).await.with_context(|| {
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
    tokio::io::copy(&mut download.download_stream, &mut destination_file).await.with_context(|| {
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
        bail!("remote-storage-download-pre-rename failpoint triggered")
    });

    fs::rename(&temp_file_path, &local_path).await?;

    fsync_path(&local_path)
        .await
        .with_context(|| format!("Could not fsync layer file {}", local_path.display(),))?;

    tracing::info!("download complete: {}", local_path.display());

    Ok(())
}

///
/// List timelines of given tenant in remote storage
///
pub async fn list_remote_timelines(
    conf: &'static PageServerConf,
    tenant_id: ZTenantId,
) -> anyhow::Result<Vec<(ZTimelineId, IndexPart)>> {
    let storage_config = match conf.remote_storage_config.as_ref() {
        Some(storage_config) => storage_config,
        None => {
            bail!("no remote storage configured");
        }
    };

    let storage_impl = GenericRemoteStorage::new(conf.workdir.clone(), storage_config)
        .map_err(DownloadError::Other)?;
    match storage_impl {
        GenericRemoteStorage::Local(s) => list_remote_timelines_impl(conf, tenant_id, &s).await,
        GenericRemoteStorage::S3(s) => list_remote_timelines_impl(conf, tenant_id, &s).await,
    }
}

async fn list_remote_timelines_impl<S, P>(
    conf: &'static PageServerConf,
    tenant_id: ZTenantId,
    storage_impl: &S,
) -> anyhow::Result<Vec<(ZTimelineId, IndexPart)>>
where
    P: RemoteObjectName + Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let tenant_path = conf.timelines_path(&tenant_id);
    let tenant_storage_path = storage_impl
        .remote_object_id(&tenant_path)
        .with_context(|| {
            format!(
                "Failed to get tenant storage path for local path '{}'",
                tenant_path.display()
            )
        })?;

    let timeline_object_ids = storage_impl
        .list_prefixes(Some(tenant_storage_path))
        .await
        .with_context(|| {
            format!(
                "Failed to list tenant storage path to get remote timelines to download: {}",
                tenant_id
            )
        })?;

    if timeline_object_ids.is_empty() {
        anyhow::bail!(
            "no timelines found on the remote storage for tenant {}",
            tenant_id
        )
    }

    let mut timeline_ids = HashSet::new();
    let mut part_downloads = FuturesUnordered::new();

    for timeline_remote_storage_key in timeline_object_ids {
        let object_name = timeline_remote_storage_key.object_name().ok_or_else(|| {
            anyhow::anyhow!("failed to get timeline id for remote tenant {tenant_id}")
        })?;

        let timeline_id: ZTimelineId = object_name
            .parse()
            .with_context(|| {
                format!("failed to parse object name into timeline id for tenant {tenant_id} '{object_name}'")
            })?;

        // list_prefixes returns all files with the prefix. If we haven't seen this timeline ID
        // yet, launch a download task for it.
        if !timeline_ids.contains(&timeline_id) {
            timeline_ids.insert(timeline_id);
            part_downloads.push(async move {
                (
                    timeline_id,
                    download_index_part(conf, storage_impl, tenant_id, timeline_id).await,
                )
            });
        }
    }

    // Wait for all the download tasks to complete.
    let mut timeline_parts = Vec::new();
    while let Some((timeline_id, part_upload_result)) = part_downloads.next().await {
        let index_part = part_upload_result
            .with_context(|| format!("Failed to fetch index part for timeline {timeline_id}"))?;

        debug!("Successfully fetched index part for timeline {timeline_id}");
        timeline_parts.push((timeline_id, index_part));
    }
    Ok(timeline_parts)
}

/// Retrieves index data from the remote storage for a given timeline.
async fn download_index_part<P, S>(
    conf: &'static PageServerConf,
    storage: &S,
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
) -> Result<IndexPart, DownloadError>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let index_part_path =
        metadata_path(conf, timeline_id, tenant_id).with_file_name(IndexPart::FILE_NAME);
    let part_storage_path = storage
        .remote_object_id(&index_part_path)
        .with_context(|| {
            format!(
                "Failed to get the index part storage path for local path '{}'",
                index_part_path.display()
            )
        })
        .map_err(DownloadError::BadInput)?;

    let mut index_part_download = storage.download(&part_storage_path).await?;

    let mut index_part_bytes = Vec::new();
    tokio::io::copy(
        &mut index_part_download.download_stream,
        &mut index_part_bytes,
    )
    .await
    .with_context(|| {
        format!("Failed to download an index part from storage path {part_storage_path:?}")
    })
    .map_err(DownloadError::Other)?;

    let index_part: IndexPart = serde_json::from_slice(&index_part_bytes)
        .with_context(|| {
            format!(
                "Failed to deserialize index part file from storage path '{part_storage_path:?}'"
            )
        })
        .map_err(DownloadError::Other)?;

    Ok(index_part)
}
