//! Helper functions to download files from remote storage with a RemoteStorage
//!
//! The functions in this module retry failed operations automatically, according
//! to the FAILED_DOWNLOAD_RETRIES constant.

use std::collections::HashSet;
use std::future::Future;

use anyhow::{anyhow, Context};
use camino::{Utf8Path, Utf8PathBuf};
use pageserver_api::shard::TenantShardId;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use utils::timeout::timeout_cancellable;
use utils::{backoff, crashsafe};

use crate::config::PageServerConf;
use crate::tenant::remote_timeline_client::{
    download_cancellable, remote_layer_path, remote_timelines_path, DOWNLOAD_TIMEOUT,
};
use crate::tenant::storage_layer::LayerFileName;
use crate::tenant::timeline::span::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::tenant::Generation;
use crate::virtual_file::on_fatal_io_error;
use crate::TEMP_FILE_SUFFIX;
use remote_storage::{DownloadError, GenericRemoteStorage, ListingMode};
use utils::crashsafe::path_with_suffix_extension;
use utils::id::TimelineId;

use super::index::{IndexPart, LayerFileMetadata};
use super::{
    parse_remote_index_path, remote_index_path, remote_initdb_archive_path,
    remote_initdb_preserved_archive_path, FAILED_DOWNLOAD_WARN_THRESHOLD, FAILED_REMOTE_OP_RETRIES,
    INITDB_PATH,
};

///
/// If 'metadata' is given, we will validate that the downloaded file's size matches that
/// in the metadata. (In the future, we might do more cross-checks, like CRC validation)
///
/// Returns the size of the downloaded file.
pub async fn download_layer_file<'a>(
    conf: &'static PageServerConf,
    storage: &'a GenericRemoteStorage,
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    layer_file_name: &'a LayerFileName,
    layer_metadata: &'a LayerFileMetadata,
    cancel: &CancellationToken,
) -> Result<u64, DownloadError> {
    debug_assert_current_span_has_tenant_and_timeline_id();

    let local_path = conf
        .timeline_path(&tenant_shard_id, &timeline_id)
        .join(layer_file_name.file_name());

    let remote_path = remote_layer_path(
        &tenant_shard_id.tenant_id,
        &timeline_id,
        layer_metadata.shard,
        layer_file_name,
        layer_metadata.generation,
    );

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

    let cancel_inner = cancel.clone();
    let (mut destination_file, bytes_amount) = download_retry(
        || async {
            let destination_file = tokio::fs::File::create(&temp_file_path)
                .await
                .with_context(|| format!("create a destination file for layer '{temp_file_path}'"))
                .map_err(DownloadError::Other)?;

            // Cancellation safety: it is safe to cancel this future, because it isn't writing to a local
            // file: the write to local file doesn't start until after the request header is returned
            // and we start draining the body stream below
            let download = download_cancellable(&cancel_inner, storage.download(&remote_path))
                .await
                .with_context(|| {
                    format!(
                    "open a download stream for layer with remote storage path '{remote_path:?}'"
                )
                })
                .map_err(DownloadError::Other)?;

            let mut destination_file =
                tokio::io::BufWriter::with_capacity(super::BUFFER_SIZE, destination_file);

            let mut reader = tokio_util::io::StreamReader::new(download.download_stream);

            // Cancellation safety: it is safe to cancel this future because it is writing into a temporary file,
            // and we will unlink the temporary file if there is an error.  This unlink is important because we
            // are in a retry loop, and we wouldn't want to leave behind a rogue write I/O to a file that
            // we will imminiently try and write to again.
            let bytes_amount: u64 = match timeout_cancellable(
                DOWNLOAD_TIMEOUT,
                &cancel_inner,
                tokio::io::copy_buf(&mut reader, &mut destination_file),
            )
            .await
            .with_context(|| {
                format!(
                    "download layer at remote path '{remote_path:?}' into file {temp_file_path:?}"
                )
            })
            .map_err(DownloadError::Other)?
            {
                Ok(b) => Ok(b),
                Err(e) => {
                    // Remove incomplete files: on restart Timeline would do this anyway, but we must
                    // do it here for the retry case.
                    if let Err(e) = tokio::fs::remove_file(&temp_file_path).await {
                        on_fatal_io_error(&e, &format!("Removing temporary file {temp_file_path}"));
                    }
                    Err(e)
                }
            }
            .with_context(|| {
                format!(
                    "download layer at remote path '{remote_path:?}' into file {temp_file_path:?}"
                )
            })
            .map_err(DownloadError::Other)?;

            let destination_file = destination_file.into_inner();

            Ok((destination_file, bytes_amount))
        },
        &format!("download {remote_path:?}"),
        cancel,
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
        .with_context(|| format!("flush source file at {temp_file_path}"))
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
        .with_context(|| format!("failed to fsync source file at {temp_file_path}"))
        .map_err(DownloadError::Other)?;
    drop(destination_file);

    fail::fail_point!("remote-storage-download-pre-rename", |_| {
        Err(DownloadError::Other(anyhow!(
            "remote-storage-download-pre-rename failpoint triggered"
        )))
    });

    fs::rename(&temp_file_path, &local_path)
        .await
        .with_context(|| format!("rename download layer file to {local_path}"))
        .map_err(DownloadError::Other)?;

    crashsafe::fsync_async(&local_path)
        .await
        .with_context(|| format!("fsync layer file {local_path}"))
        .map_err(DownloadError::Other)?;

    tracing::debug!("download complete: {local_path}");

    Ok(bytes_amount)
}

const TEMP_DOWNLOAD_EXTENSION: &str = "temp_download";

pub fn is_temp_download_file(path: &Utf8Path) -> bool {
    let extension = path.extension();
    match extension {
        Some(TEMP_DOWNLOAD_EXTENSION) => true,
        Some(_) => false,
        None => false,
    }
}

/// List timelines of given tenant in remote storage
pub async fn list_remote_timelines(
    storage: &GenericRemoteStorage,
    tenant_shard_id: TenantShardId,
    cancel: CancellationToken,
) -> anyhow::Result<(HashSet<TimelineId>, HashSet<String>)> {
    let remote_path = remote_timelines_path(&tenant_shard_id);

    fail::fail_point!("storage-sync-list-remote-timelines", |_| {
        anyhow::bail!("storage-sync-list-remote-timelines");
    });

    let cancel_inner = cancel.clone();
    let listing = download_retry_forever(
        || {
            download_cancellable(
                &cancel_inner,
                storage.list(Some(&remote_path), ListingMode::WithDelimiter),
            )
        },
        &format!("list timelines for {tenant_shard_id}"),
        cancel,
    )
    .await?;

    let mut timeline_ids = HashSet::new();
    let mut other_prefixes = HashSet::new();

    for timeline_remote_storage_key in listing.prefixes {
        let object_name = timeline_remote_storage_key.object_name().ok_or_else(|| {
            anyhow::anyhow!("failed to get timeline id for remote tenant {tenant_shard_id}")
        })?;

        match object_name.parse::<TimelineId>() {
            Ok(t) => timeline_ids.insert(t),
            Err(_) => other_prefixes.insert(object_name.to_string()),
        };
    }

    for key in listing.keys {
        let object_name = key
            .object_name()
            .ok_or_else(|| anyhow::anyhow!("object name for key {key}"))?;
        other_prefixes.insert(object_name.to_string());
    }

    Ok((timeline_ids, other_prefixes))
}

async fn do_download_index_part(
    storage: &GenericRemoteStorage,
    tenant_shard_id: &TenantShardId,
    timeline_id: &TimelineId,
    index_generation: Generation,
    cancel: CancellationToken,
) -> Result<IndexPart, DownloadError> {
    use futures::stream::StreamExt;

    let remote_path = remote_index_path(tenant_shard_id, timeline_id, index_generation);

    let cancel_inner = cancel.clone();
    let index_part_bytes = download_retry_forever(
        || async {
            // Cancellation: if is safe to cancel this future because we're just downloading into
            // a memory buffer, not touching local disk.
            let index_part_download =
                download_cancellable(&cancel_inner, storage.download(&remote_path)).await?;

            let mut index_part_bytes = Vec::new();
            let mut stream = std::pin::pin!(index_part_download.download_stream);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk
                    .with_context(|| format!("download index part at {remote_path:?}"))
                    .map_err(DownloadError::Other)?;
                index_part_bytes.extend_from_slice(&chunk[..]);
            }
            Ok(index_part_bytes)
        },
        &format!("download {remote_path:?}"),
        cancel,
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
    tenant_shard_id: &TenantShardId,
    timeline_id: &TimelineId,
    my_generation: Generation,
    cancel: CancellationToken,
) -> Result<IndexPart, DownloadError> {
    debug_assert_current_span_has_tenant_and_timeline_id();

    if my_generation.is_none() {
        // Operating without generations: just fetch the generation-less path
        return do_download_index_part(
            storage,
            tenant_shard_id,
            timeline_id,
            my_generation,
            cancel,
        )
        .await;
    }

    // Stale case: If we were intentionally attached in a stale generation, there may already be a remote
    // index in our generation.
    //
    // This is an optimization to avoid doing the listing for the general case below.
    let res = do_download_index_part(
        storage,
        tenant_shard_id,
        timeline_id,
        my_generation,
        cancel.clone(),
    )
    .await;
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
    let res = do_download_index_part(
        storage,
        tenant_shard_id,
        timeline_id,
        my_generation.previous(),
        cancel.clone(),
    )
    .await;
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
    // objects, and select the highest one with a generation <= my_generation.  Constructing the prefix is equivalent
    // to constructing a full index path with no generation, because the generation is a suffix.
    let index_prefix = remote_index_path(tenant_shard_id, timeline_id, Generation::none());
    let indices = backoff::retry(
        || async { storage.list_files(Some(&index_prefix)).await },
        |_| false,
        FAILED_DOWNLOAD_WARN_THRESHOLD,
        FAILED_REMOTE_OP_RETRIES,
        "listing index_part files",
        backoff::Cancel::new(cancel.clone(), || anyhow::anyhow!("Cancelled")),
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
            do_download_index_part(storage, tenant_shard_id, timeline_id, g, cancel).await
        }
        None => {
            // Migration from legacy pre-generation state: we have a generation but no prior
            // attached pageservers did.  Try to load from a no-generation path.
            tracing::debug!("No index_part.json* found");
            do_download_index_part(
                storage,
                tenant_shard_id,
                timeline_id,
                Generation::none(),
                cancel,
            )
            .await
        }
    }
}

pub(crate) async fn download_initdb_tar_zst(
    conf: &'static PageServerConf,
    storage: &GenericRemoteStorage,
    tenant_shard_id: &TenantShardId,
    timeline_id: &TimelineId,
    cancel: &CancellationToken,
) -> Result<(Utf8PathBuf, File), DownloadError> {
    debug_assert_current_span_has_tenant_and_timeline_id();

    let remote_path = remote_initdb_archive_path(&tenant_shard_id.tenant_id, timeline_id);

    let remote_preserved_path =
        remote_initdb_preserved_archive_path(&tenant_shard_id.tenant_id, timeline_id);

    let timeline_path = conf.timelines_path(tenant_shard_id);

    if !timeline_path.exists() {
        tokio::fs::create_dir_all(&timeline_path)
            .await
            .with_context(|| format!("timeline dir creation {timeline_path}"))
            .map_err(DownloadError::Other)?;
    }
    let temp_path = timeline_path.join(format!(
        "{INITDB_PATH}.download-{timeline_id}.{TEMP_FILE_SUFFIX}"
    ));

    let cancel_inner = cancel.clone();

    let file = download_retry(
        || async {
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&temp_path)
                .await
                .with_context(|| format!("tempfile creation {temp_path}"))
                .map_err(DownloadError::Other)?;

            let download = match download_cancellable(&cancel_inner, storage.download(&remote_path))
                .await
            {
                Ok(dl) => dl,
                Err(DownloadError::NotFound) => {
                    download_cancellable(&cancel_inner, storage.download(&remote_preserved_path))
                        .await?
                }
                Err(other) => Err(other)?,
            };
            let mut download = tokio_util::io::StreamReader::new(download.download_stream);
            let mut writer = tokio::io::BufWriter::with_capacity(super::BUFFER_SIZE, file);

            // TODO: this consumption of the response body should be subject to timeout + cancellation, but
            // not without thinking carefully about how to recover safely from cancelling a write to
            // local storage (e.g. by writing into a temp file as we do in download_layer)
            tokio::io::copy_buf(&mut download, &mut writer)
                .await
                .with_context(|| format!("download initdb.tar.zst at {remote_path:?}"))
                .map_err(DownloadError::Other)?;

            let mut file = writer.into_inner();

            file.seek(std::io::SeekFrom::Start(0))
                .await
                .with_context(|| format!("rewinding initdb.tar.zst at: {remote_path:?}"))
                .map_err(DownloadError::Other)?;

            Ok(file)
        },
        &format!("download {remote_path}"),
        cancel,
    )
    .await
    .map_err(|e| {
        // Do a best-effort attempt at deleting the temporary file upon encountering an error.
        // We don't have async here nor do we want to pile on any extra errors.
        if let Err(e) = std::fs::remove_file(&temp_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("error deleting temporary file {temp_path}: {e}");
            }
        }
        e
    })?;

    Ok((temp_path, file))
}

/// Helper function to handle retries for a download operation.
///
/// Remote operations can fail due to rate limits (IAM, S3), spurious network
/// problems, or other external reasons. Retry FAILED_DOWNLOAD_RETRIES times,
/// with backoff.
///
/// (See similar logic for uploads in `perform_upload_task`)
async fn download_retry<T, O, F>(
    op: O,
    description: &str,
    cancel: &CancellationToken,
) -> Result<T, DownloadError>
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
        backoff::Cancel::new(cancel.clone(), || DownloadError::Cancelled),
    )
    .await
}

async fn download_retry_forever<T, O, F>(
    op: O,
    description: &str,
    cancel: CancellationToken,
) -> Result<T, DownloadError>
where
    O: FnMut() -> F,
    F: Future<Output = Result<T, DownloadError>>,
{
    backoff::retry(
        op,
        |e| matches!(e, DownloadError::BadInput(_) | DownloadError::NotFound),
        FAILED_DOWNLOAD_WARN_THRESHOLD,
        u32::MAX,
        description,
        backoff::Cancel::new(cancel, || DownloadError::Cancelled),
    )
    .await
}
