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
use remote_storage::{DownloadError, GenericRemoteStorage};
use utils::crashsafe::path_with_suffix_extension;
use utils::id::{TenantId, TimelineId};

use super::index::{IndexPart, LayerFileMetadata};
use super::{
    parse_remote_index_path, remote_index_path, FAILED_DOWNLOAD_WARN_THRESHOLD,
    FAILED_REMOTE_OP_RETRIES,
};

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

/// index_part.json objects are suffixed with a generation number, so we cannot
/// directly GET the latest index part without doing some probing.
///
/// In this function we probe for the most recent index in a generation <= our current generation.
/// See "Finding the remote indices for timelines" in docs/rfcs/025-generation-numbers.md
#[tracing::instrument(skip_all, fields(generation=?my_generation))]
pub(super) async fn download_index_part(
    storage: &GenericRemoteStorage,
    tenant_id: &TenantId,
    timeline_id: &TimelineId,
    my_generation: Generation,
) -> Result<IndexPart, DownloadError> {
    debug_assert_current_span_has_tenant_and_timeline_id();

    if my_generation.is_none() {
        // Operating without generations: just fetch the generation-less path
        return do_download_index_part(storage, tenant_id, timeline_id, my_generation).await;
    }

    // Stale case: If we were intentionally attached in a stale generation, there may already be a remote
    // index in our generation.
    //
    // This is an optimization to avoid doing the listing for the general case below.
    let res = do_download_index_part(storage, tenant_id, timeline_id, my_generation).await;
    match res {
        Ok(index_part) => {
            tracing::debug!(
                "Found index_part from current generation (this is a stale attachment)"
            );
            return Ok(index_part);
        }
        Err(DownloadError::NotFound) => {}
        Err(e) => return Err(e),
    };

    // Typical case: the previous generation of this tenant was running healthily, and had uploaded
    // and index part.  We may safely start from this index without doing a listing, because:
    //  - We checked for current generation case above
    //  - generations > my_generation are to be ignored
    //  - any other indices that exist would have an older generation than `previous_gen`, and
    //    we want to find the most recent index from a previous generation.
    //
    // This is an optimization to avoid doing the listing for the general case below.
    let res =
        do_download_index_part(storage, tenant_id, timeline_id, my_generation.previous()).await;
    match res {
        Ok(index_part) => {
            tracing::debug!("Found index_part from previous generation");
            return Ok(index_part);
        }
        Err(DownloadError::NotFound) => {
            tracing::debug!(
                "No index_part found from previous generation, falling back to listing"
            );
        }
        Err(e) => {
            return Err(e);
        }
    }

    // General case/fallback: if there is no index at my_generation or prev_generation, then list all index_part.json
    // objects, and select the highest one with a generation <= my_generation.
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

    // General case logic for which index to use: the latest index whose generation
    // is <= our own.  See "Finding the remote indices for timelines" in docs/rfcs/025-generation-numbers.md
    let max_previous_generation = indices
        .into_iter()
        .filter_map(parse_remote_index_path)
        .filter(|g| g <= &my_generation)
        .max();

    match max_previous_generation {
        Some(g) => {
            tracing::debug!("Found index_part in generation {g:?}");
            do_download_index_part(storage, tenant_id, timeline_id, g).await
        }
        None => {
            // Migration from legacy pre-generation state: we have a generation but no prior
            // attached pageservers did.  Try to load from a no-generation path.
            tracing::info!("No index_part.json* found");
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
