//! Helper functions to download files from remote storage with a RemoteStorage
//!
//! The functions in this module retry failed operations automatically, according
//! to the FAILED_DOWNLOAD_RETRIES constant.

use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, Context};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use tracing::{info, warn};

use crate::config::PageServerConf;
use crate::tenant::storage_layer::LayerFileName;
use crate::tenant::timeline::span::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::{exponential_backoff, DEFAULT_BASE_BACKOFF_SECONDS, DEFAULT_MAX_BACKOFF_SECONDS};
use remote_storage::{DownloadError, GenericRemoteStorage};
use utils::crashsafe::path_with_suffix_extension;
use utils::id::{TenantId, TimelineId};

use super::index::{IndexPart, LayerFileMetadata};
use super::{FAILED_DOWNLOAD_RETRIES, FAILED_DOWNLOAD_WARN_THRESHOLD};

async fn fsync_path(path: impl AsRef<std::path::Path>) -> Result<(), std::io::Error> {
    fs::File::open(path).await?.sync_all().await
}

static MAX_DOWNLOAD_DURATION: Duration = Duration::from_secs(120);

///
/// If 'metadata' is given, we will validate that the downloaded file's size matches that
/// in the metadata. (In the future, we might do more cross-checks, like CRC validation)
///
/// Returns the size of the downloaded file.
pub async fn download_layer_file<'a>(
    conf: &'static PageServerConf,
    storage: &'a GenericRemoteStorage,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    layer_file_name: &'a LayerFileName,
    layer_metadata: &'a LayerFileMetadata,
) -> Result<u64, DownloadError> {
    debug_assert_current_span_has_tenant_and_timeline_id();

    let timeline_path = conf.timeline_path(&tenant_id, &timeline_id);

    let local_path = timeline_path.join(layer_file_name.file_name());

    let remote_path = conf
        .remote_path(&local_path)
        .map_err(DownloadError::Other)?;

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

    let (mut destination_file, bytes_amount) = download_retry(
        || async {
            // TODO: this doesn't use the cached fd for some reason?
            let mut destination_file = fs::File::create(&temp_file_path).await.with_context(|| {
                format!(
                    "create a destination file for layer '{}'",
                    temp_file_path.display()
                )
            })
            .map_err(DownloadError::Other)?;
            let mut download = storage.download(&remote_path).await.with_context(|| {
                format!(
                    "open a download stream for layer with remote storage path '{remote_path:?}'"
                )
            })
            .map_err(DownloadError::Other)?;

            let bytes_amount = tokio::time::timeout(MAX_DOWNLOAD_DURATION, tokio::io::copy(&mut download.download_stream, &mut destination_file))
                .await
                .map_err(|e| DownloadError::Other(anyhow::anyhow!("Timed out  {:?}", e)))?
                .with_context(|| {
                    format!("Failed to download layer with remote storage path '{remote_path:?}' into file {temp_file_path:?}")
                })
                .map_err(DownloadError::Other)?;

            Ok((destination_file, bytes_amount))

        },
        &format!("download {remote_path:?}"),
    ).await?;

    // Tokio doc here: https://docs.rs/tokio/1.17.0/tokio/fs/struct.File.html states that:
    // A file will not be closed immediately when it goes out of scope if there are any IO operations
    // that have not yet completed. To ensure that a file is closed immediately when it is dropped,
    // you should call flush before dropping it.
    //
    // From the tokio code I see that it waits for pending operations to complete. There shouldt be any because
    // we assume that `destination_file` file is fully written. I e there is no pending .write(...).await operations.
    // But for additional safety lets check/wait for any pending operations.
    destination_file
        .flush()
        .await
        .with_context(|| {
            format!(
                "failed to flush source file at {}",
                temp_file_path.display()
            )
        })
        .map_err(DownloadError::Other)?;

    let expected = layer_metadata.file_size();
    if expected != bytes_amount {
        return Err(DownloadError::Other(anyhow!(
            "According to layer file metadata should have downloaded {expected} bytes but downloaded {bytes_amount} bytes into file {temp_file_path:?}",
        )));
    }

    // not using sync_data because it can lose file size update
    destination_file
        .sync_all()
        .await
        .with_context(|| {
            format!(
                "failed to fsync source file at {}",
                temp_file_path.display()
            )
        })
        .map_err(DownloadError::Other)?;
    drop(destination_file);

    fail::fail_point!("remote-storage-download-pre-rename", |_| {
        Err(DownloadError::Other(anyhow!(
            "remote-storage-download-pre-rename failpoint triggered"
        )))
    });

    fs::rename(&temp_file_path, &local_path)
        .await
        .with_context(|| {
            format!(
                "Could not rename download layer file to {}",
                local_path.display(),
            )
        })
        .map_err(DownloadError::Other)?;

    fsync_path(&local_path)
        .await
        .with_context(|| format!("Could not fsync layer file {}", local_path.display(),))
        .map_err(DownloadError::Other)?;

    tracing::debug!("download complete: {}", local_path.display());

    Ok(bytes_amount)
}

const TEMP_DOWNLOAD_EXTENSION: &str = "temp_download";

pub fn is_temp_download_file(path: &Path) -> bool {
    let extension = path.extension().map(|pname| {
        pname
            .to_str()
            .expect("paths passed to this function must be valid Rust strings")
    });
    match extension {
        Some(TEMP_DOWNLOAD_EXTENSION) => true,
        Some(_) => false,
        None => false,
    }
}

/// List timelines of given tenant in remote storage
pub async fn list_remote_timelines<'a>(
    storage: &'a GenericRemoteStorage,
    conf: &'static PageServerConf,
    tenant_id: TenantId,
) -> anyhow::Result<HashSet<TimelineId>> {
    let tenant_path = conf.timelines_path(&tenant_id);
    let tenant_storage_path = conf.remote_path(&tenant_path)?;

    fail::fail_point!("storage-sync-list-remote-timelines", |_| {
        anyhow::bail!("storage-sync-list-remote-timelines");
    });

    let timelines = download_retry(
        || storage.list_prefixes(Some(&tenant_storage_path)),
        &format!("list prefixes for {tenant_path:?}"),
    )
    .await?;

    if timelines.is_empty() {
        anyhow::bail!("no timelines found on the remote storage")
    }

    let mut timeline_ids = HashSet::new();

    for timeline_remote_storage_key in timelines {
        let object_name = timeline_remote_storage_key.object_name().ok_or_else(|| {
            anyhow::anyhow!("failed to get timeline id for remote tenant {tenant_id}")
        })?;

        let timeline_id: TimelineId = object_name.parse().with_context(|| {
            format!("failed to parse object name into timeline id '{object_name}'")
        })?;

        // list_prefixes is assumed to return unique names. Ensure this here.
        // NB: it's safer to bail out than warn-log this because the pageserver
        //     needs to absolutely know about _all_ timelines that exist, so that
        //     GC knows all the branchpoints. If we skipped over a timeline instead,
        //     GC could delete a layer that's still needed by that timeline.
        anyhow::ensure!(
            !timeline_ids.contains(&timeline_id),
            "list_prefixes contains duplicate timeline id {timeline_id}"
        );
        timeline_ids.insert(timeline_id);
    }

    Ok(timeline_ids)
}

pub(super) async fn download_index_part(
    conf: &'static PageServerConf,
    storage: &GenericRemoteStorage,
    tenant_id: &TenantId,
    timeline_id: &TimelineId,
) -> Result<IndexPart, DownloadError> {
    let index_part_path = conf
        .metadata_path(tenant_id, timeline_id)
        .with_file_name(IndexPart::FILE_NAME);
    let part_storage_path = conf
        .remote_path(&index_part_path)
        .map_err(DownloadError::BadInput)?;

    let index_part_bytes = download_retry(
        || async {
            let mut index_part_download = storage.download(&part_storage_path).await?;

            let mut index_part_bytes = Vec::new();
            tokio::io::copy(
                &mut index_part_download.download_stream,
                &mut index_part_bytes,
            )
            .await
            .with_context(|| {
                format!("Failed to download an index part into file {index_part_path:?}")
            })
            .map_err(DownloadError::Other)?;
            Ok(index_part_bytes)
        },
        &format!("download {part_storage_path:?}"),
    )
    .await?;

    let index_part: IndexPart = serde_json::from_slice(&index_part_bytes)
        .with_context(|| {
            format!("Failed to deserialize index part file into file {index_part_path:?}")
        })
        .map_err(DownloadError::Other)?;

    Ok(index_part)
}

///
/// Helper function to handle retries for a download operation.
///
/// Remote operations can fail due to rate limits (IAM, S3), spurious network
/// problems, or other external reasons. Retry FAILED_DOWNLOAD_RETRIES times,
/// with backoff.
///
/// (See similar logic for uploads in `perform_upload_task`)
async fn download_retry<T, O, F>(mut op: O, description: &str) -> Result<T, DownloadError>
where
    O: FnMut() -> F,
    F: Future<Output = Result<T, DownloadError>>,
{
    let mut attempts = 0;
    loop {
        let result = op().await;
        match result {
            Ok(_) => {
                if attempts > 0 {
                    info!("{description} succeeded after {attempts} retries");
                }
                return result;
            }

            // These are "permanent" errors that should not be retried.
            Err(DownloadError::BadInput(_)) | Err(DownloadError::NotFound) => {
                return result;
            }
            // Assume that any other failure might be transient, and the operation might
            // succeed if we just keep trying.
            Err(DownloadError::Other(err)) if attempts < FAILED_DOWNLOAD_WARN_THRESHOLD => {
                info!("{description} failed, will retry (attempt {attempts}): {err:#}");
            }
            Err(DownloadError::Other(err)) if attempts < FAILED_DOWNLOAD_RETRIES => {
                warn!("{description} failed, will retry (attempt {attempts}): {err:#}");
            }
            Err(DownloadError::Other(ref err)) => {
                // Operation failed FAILED_DOWNLOAD_RETRIES times. Time to give up.
                warn!("{description} still failed after {attempts} retries, giving up: {err:?}");
                return result;
            }
        }
        // sleep and retry
        exponential_backoff(
            attempts,
            DEFAULT_BASE_BACKOFF_SECONDS,
            DEFAULT_MAX_BACKOFF_SECONDS,
        )
        .await;
        attempts += 1;
    }
}
