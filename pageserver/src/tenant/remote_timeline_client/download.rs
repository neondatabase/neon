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
use tokio_util::sync::CancellationToken;
use utils::{backoff, crashsafe};

use crate::config::PageServerConf;
use crate::tenant::remote_timeline_client::{remote_layer_path, remote_timelines_path};
use crate::tenant::storage_layer::LayerFileName;
use crate::tenant::timeline::span::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::tenant::Generation;
use remote_storage::{DownloadError, GenericRemoteStorage, RemotePath};
use utils::crashsafe::path_with_suffix_extension;
use utils::id::{TenantId, TimelineId};

use super::index::{IndexPart, LayerFileMetadata};
use super::{remote_index_path, FAILED_DOWNLOAD_WARN_THRESHOLD, FAILED_REMOTE_OP_RETRIES};

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

    let local_path = conf
        .timeline_path(&tenant_id, &timeline_id)
        .join(layer_file_name.file_name());

    let remote_path = remote_layer_path(&tenant_id, &timeline_id, layer_file_name, layer_metadata);

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
            let mut destination_file = fs::File::create(&temp_file_path)
                .await
                .with_context(|| {
                    format!(
                        "create a destination file for layer '{}'",
                        temp_file_path.display()
                    )
                })
                .map_err(DownloadError::Other)?;
            let mut download = storage
                .download(&remote_path)
                .await
                .with_context(|| {
                    format!(
                    "open a download stream for layer with remote storage path '{remote_path:?}'"
                )
                })
                .map_err(DownloadError::Other)?;

            let bytes_amount = tokio::time::timeout(
                MAX_DOWNLOAD_DURATION,
                tokio::io::copy(&mut download.download_stream, &mut destination_file),
            )
            .await
            .map_err(|e| DownloadError::Other(anyhow::anyhow!("Timed out  {:?}", e)))?
            .with_context(|| {
                format!(
                    "download layer at remote path '{remote_path:?}' into file {temp_file_path:?}"
                )
            })
            .map_err(DownloadError::Other)?;

            Ok((destination_file, bytes_amount))
        },
        &format!("download {remote_path:?}"),
    )
    .await?;

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
        .with_context(|| format!("flush source file at {}", temp_file_path.display()))
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
        .with_context(|| format!("rename download layer file to {}", local_path.display(),))
        .map_err(DownloadError::Other)?;

    crashsafe::fsync_async(&local_path)
        .await
        .with_context(|| format!("fsync layer file {}", local_path.display(),))
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
pub async fn list_remote_timelines(
    storage: &GenericRemoteStorage,
    tenant_id: TenantId,
) -> anyhow::Result<HashSet<TimelineId>> {
    let remote_path = remote_timelines_path(&tenant_id);

    fail::fail_point!("storage-sync-list-remote-timelines", |_| {
        anyhow::bail!("storage-sync-list-remote-timelines");
    });

    let timelines = download_retry(
        || storage.list_prefixes(Some(&remote_path)),
        &format!("list prefixes for {tenant_id}"),
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

        let timeline_id: TimelineId = object_name
            .parse()
            .with_context(|| format!("parse object name into timeline id '{object_name}'"))?;

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

async fn do_download_index_part(
    storage: &GenericRemoteStorage,
    tenant_id: &TenantId,
    timeline_id: &TimelineId,
    index_generation: Generation,
) -> Result<IndexPart, DownloadError> {
    let remote_path = remote_index_path(tenant_id, timeline_id, index_generation);

    let index_part_bytes = download_retry(
        || async {
            let mut index_part_download = storage.download(&remote_path).await?;

            let mut index_part_bytes = Vec::new();
            tokio::io::copy(
                &mut index_part_download.download_stream,
                &mut index_part_bytes,
            )
            .await
            .with_context(|| format!("download index part at {remote_path:?}"))
            .map_err(DownloadError::Other)?;
            Ok(index_part_bytes)
        },
        &format!("download {remote_path:?}"),
    )
    .await?;

    let index_part: IndexPart = serde_json::from_slice(&index_part_bytes)
        .with_context(|| format!("download index part file at {remote_path:?}"))
        .map_err(DownloadError::Other)?;

    Ok(index_part)
}

pub(super) async fn download_index_part(
    storage: &GenericRemoteStorage,
    tenant_id: &TenantId,
    timeline_id: &TimelineId,
    my_generation: Generation,
) -> Result<IndexPart, DownloadError> {
    if my_generation.is_none() {
        // Operating without generations: just fetch the generation-less path
        return do_download_index_part(storage, tenant_id, timeline_id, my_generation).await;
    }

    let previous_gen = my_generation.previous();
    let r_previous = do_download_index_part(storage, tenant_id, timeline_id, previous_gen).await;

    match r_previous {
        Ok(index_part) => {
            tracing::debug!("Found index_part from previous generation {previous_gen:?}");
            return Ok(index_part);
        }
        Err(e) => {
            if matches!(e, DownloadError::NotFound) {
                tracing::debug!("No index_part found from previous generation {previous_gen:?}, falling back to listing");
            } else {
                return Err(e);
            }
        }
    };

    /// Given the key of an index, parse out the generation part of the name
    fn parse_generation(path: RemotePath) -> Option<Generation> {
        let file_name = match path.get_path().file_name() {
            Some(f) => f,
            None => {
                // Unexpected: we should be seeing index_part.json paths only
                tracing::warn!("Malformed index key {0}", path);
                return None;
            }
        };

        let file_name_str = match file_name.to_str() {
            Some(s) => s,
            None => {
                tracing::warn!("Malformed index key {0:?}", path);
                return None;
            }
        };

        match file_name_str.split_once('-') {
            Some((_, gen_suffix)) => u32::from_str_radix(gen_suffix, 16)
                .map(Generation::new)
                .ok(),
            None => None,
        }
    }

    // Fallback: we did not find an index_part.json from the previous generation, so
    // we will list all the index_part objects and pick the most recent.
    let index_prefix = remote_index_path(tenant_id, timeline_id, Generation::none());
    let indices = backoff::retry(
        || async { storage.list_files(Some(&index_prefix)).await },
        |_| false,
        FAILED_DOWNLOAD_WARN_THRESHOLD,
        FAILED_REMOTE_OP_RETRIES,
        "listing index_part files",
        // TODO: use a cancellation token (https://github.com/neondatabase/neon/issues/5066)
        backoff::Cancel::new(CancellationToken::new(), || -> anyhow::Error {
            unreachable!()
        }),
    )
    .await
    .map_err(DownloadError::Other)?;

    let mut generations: Vec<_> = indices
        .into_iter()
        .filter_map(parse_generation)
        .filter(|g| g <= &my_generation)
        .collect();

    generations.sort();
    match generations.last() {
        Some(g) => {
            tracing::debug!(
                "Found index_part in generation {g:?} (my generation {my_generation:?})"
            );
            do_download_index_part(storage, tenant_id, timeline_id, *g).await
        }
        None => {
            // This is not an error: the timeline may be newly created, or we may be
            // upgrading and have no historical index_part with a generation suffix.
            // Fall back to trying to load the un-suffixed index_part.json.
            tracing::info!(
                "No index_part.json-* found when loading {tenant_id}/{timeline_id} in generation {my_generation:?}"
            );
            do_download_index_part(storage, tenant_id, timeline_id, Generation::none()).await
        }
    }
}

/// Helper function to handle retries for a download operation.
///
/// Remote operations can fail due to rate limits (IAM, S3), spurious network
/// problems, or other external reasons. Retry FAILED_DOWNLOAD_RETRIES times,
/// with backoff.
///
/// (See similar logic for uploads in `perform_upload_task`)
async fn download_retry<T, O, F>(op: O, description: &str) -> Result<T, DownloadError>
where
    O: FnMut() -> F,
    F: Future<Output = Result<T, DownloadError>>,
{
    backoff::retry(
        op,
        |e| matches!(e, DownloadError::BadInput(_) | DownloadError::NotFound),
        FAILED_DOWNLOAD_WARN_THRESHOLD,
        FAILED_REMOTE_OP_RETRIES,
        description,
        // TODO: use a cancellation token (https://github.com/neondatabase/neon/issues/5066)
        backoff::Cancel::new(CancellationToken::new(), || -> DownloadError {
            unreachable!()
        }),
    )
    .await
}
