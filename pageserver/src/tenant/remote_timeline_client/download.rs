//! Helper functions to download files from remote storage with a RemoteStorage
//!
//! The functions in this module retry failed operations automatically, according
//! to the FAILED_DOWNLOAD_RETRIES constant.

use std::collections::HashSet;
use std::future::Future;

use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};
use pageserver_api::shard::TenantShardId;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncSeekExt;
use tokio_util::io::StreamReader;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use utils::backoff;

use crate::config::PageServerConf;
use crate::span::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::tenant::remote_timeline_client::remote_timelines_path;

use crate::tenant::Generation;
use crate::TEMP_FILE_SUFFIX;
use remote_storage::{DownloadError, GenericRemoteStorage, ListingMode};

use utils::id::TimelineId;

use super::index::IndexPart;
use super::{
    parse_remote_index_path, remote_index_path, remote_initdb_archive_path,
    remote_initdb_preserved_archive_path, FAILED_DOWNLOAD_WARN_THRESHOLD, FAILED_REMOTE_OP_RETRIES,
    INITDB_PATH,
};

mod layer_file;
pub(crate) use layer_file::download_layer_file;

const TEMP_DOWNLOAD_EXTENSION: &str = "temp_download";

pub(crate) fn is_temp_download_file(path: &Utf8Path) -> bool {
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

    let listing = download_retry_forever(
        || {
            storage.list(
                Some(&remote_path),
                ListingMode::WithDelimiter,
                None,
                &cancel,
            )
        },
        &format!("list timelines for {tenant_shard_id}"),
        &cancel,
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
    cancel: &CancellationToken,
) -> Result<IndexPart, DownloadError> {
    let remote_path = remote_index_path(tenant_shard_id, timeline_id, index_generation);

    let index_part_bytes = download_retry_forever(
        || async {
            let download = storage.download(&remote_path, cancel).await?;

            let mut bytes = Vec::new();

            let stream = download.download_stream;
            let mut stream = StreamReader::new(stream);

            tokio::io::copy_buf(&mut stream, &mut bytes).await?;

            Ok(bytes)
        },
        &format!("download {remote_path:?}"),
        cancel,
    )
    .await?;

    let index_part: IndexPart = serde_json::from_slice(&index_part_bytes)
        .with_context(|| format!("deserialize index part file at {remote_path:?}"))
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
    cancel: &CancellationToken,
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
    let res =
        do_download_index_part(storage, tenant_shard_id, timeline_id, my_generation, cancel).await;
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
        cancel,
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

    let indices = download_retry(
        || async { storage.list_files(Some(&index_prefix), None, cancel).await },
        "list index_part files",
        cancel,
    )
    .await?;

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

            let download = match storage.download(&remote_path, cancel).await {
                Ok(dl) => dl,
                Err(DownloadError::NotFound) => {
                    storage.download(&remote_preserved_path, cancel).await?
                }
                Err(other) => Err(other)?,
            };
            let mut download = tokio_util::io::StreamReader::new(download.download_stream);
            let mut writer = tokio::io::BufWriter::with_capacity(*super::BUFFER_SIZE, file);

            tokio::io::copy_buf(&mut download, &mut writer).await?;

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
/// Remote operations can fail due to rate limits (S3), spurious network
/// problems, or other external reasons. Retry FAILED_DOWNLOAD_RETRIES times,
/// with backoff.
///
/// (See similar logic for uploads in `perform_upload_task`)
pub(super) async fn download_retry<T, O, F>(
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
        DownloadError::is_permanent,
        FAILED_DOWNLOAD_WARN_THRESHOLD,
        FAILED_REMOTE_OP_RETRIES,
        description,
        cancel,
    )
    .await
    .ok_or_else(|| DownloadError::Cancelled)
    .and_then(|x| x)
}

async fn download_retry_forever<T, O, F>(
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
        DownloadError::is_permanent,
        FAILED_DOWNLOAD_WARN_THRESHOLD,
        u32::MAX,
        description,
        cancel,
    )
    .await
    .ok_or_else(|| DownloadError::Cancelled)
    .and_then(|x| x)
}
