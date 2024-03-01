use super::super::super::index::LayerFileMetadata;
use super::super::download_retry;
use super::super::TEMP_DOWNLOAD_EXTENSION;
use crate::config::PageServerConf;
use crate::span::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::tenant::remote_timeline_client::download_cancellable;
use crate::tenant::remote_timeline_client::remote_layer_path;
use crate::tenant::remote_timeline_client::DOWNLOAD_TIMEOUT;
use crate::tenant::storage_layer::LayerFileName;
use crate::virtual_file::on_fatal_io_error;
use anyhow::Context;
use pageserver_api::shard::TenantShardId;
use remote_storage::DownloadError;
use remote_storage::GenericRemoteStorage;
use tokio::fs;

use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use anyhow::anyhow;
use utils::crashsafe;
use utils::crashsafe::path_with_suffix_extension;
use utils::id::TimelineId;
use utils::timeout::timeout_cancellable;

///
/// If 'metadata' is given, we will validate that the downloaded file's size matches that
/// in the metadata. (In the future, we might do more cross-checks, like CRC validation)
///
/// Returns the size of the downloaded file.
pub(crate) async fn download_layer_file_legacy<'a>(
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

    let (mut destination_file, bytes_amount) = download_retry(
        || async {
            let destination_file = tokio::fs::File::create(&temp_file_path)
                .await
                .with_context(|| format!("create a destination file for layer '{temp_file_path}'"))
                .map_err(DownloadError::Other)?;

            // Cancellation safety: it is safe to cancel this future, because it isn't writing to a local
            // file: the write to local file doesn't start until after the request header is returned
            // and we start draining the body stream below
            let download = download_cancellable(cancel, storage.download(&remote_path))
                .await
                .with_context(|| {
                    format!(
                    "open a download stream for layer with remote storage path '{remote_path:?}'"
                )
                })
                .map_err(DownloadError::Other)?;

            let mut destination_file = tokio::io::BufWriter::with_capacity(
                crate::tenant::remote_timeline_client::BUFFER_SIZE,
                destination_file,
            );

            let mut reader = tokio_util::io::StreamReader::new(download.download_stream);

            // Cancellation safety: it is safe to cancel this future because it is writing into a temporary file,
            // and we will unlink the temporary file if there is an error.  This unlink is important because we
            // are in a retry loop, and we wouldn't want to leave behind a rogue write I/O to a file that
            // we will imminiently try and write to again.
            let bytes_amount: u64 = match timeout_cancellable(
                DOWNLOAD_TIMEOUT,
                cancel,
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
