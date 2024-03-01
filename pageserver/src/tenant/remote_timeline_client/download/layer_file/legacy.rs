use super::super::super::index::LayerFileMetadata;
use super::super::download_retry;
use super::super::TEMP_DOWNLOAD_EXTENSION;
use crate::config::PageServerConf;
use crate::span::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::tenant::remote_timeline_client::remote_layer_path;
use crate::tenant::remote_timeline_client::BUFFER_SIZE;
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

///
/// If 'metadata' is given, we will validate that the downloaded file's size matches that
/// in the metadata. (In the future, we might do more cross-checks, like CRC validation)
///
/// Returns the size of the downloaded file.
pub(crate) async fn download_layer_file<'a>(
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

            let download = storage.download(&remote_path, cancel).await?;

            let mut destination_file =
                tokio::io::BufWriter::with_capacity(BUFFER_SIZE, destination_file);

            let mut reader = tokio_util::io::StreamReader::new(download.download_stream);

            let bytes_amount = tokio::io::copy_buf(&mut reader, &mut destination_file).await;

            match bytes_amount {
                Ok(bytes_amount) => {
                    let destination_file = destination_file.into_inner();
                    Ok((destination_file, bytes_amount))
                }
                Err(e) => {
                    if let Err(e) = tokio::fs::remove_file(&temp_file_path).await {
                        on_fatal_io_error(&e, &format!("Removing temporary file {temp_file_path}"));
                    }

                    Err(e.into())
                }
            }
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

    // FIXME: should fsync the directory, not the destination file
    crashsafe::fsync_async(&local_path)
        .await
        .with_context(|| format!("fsync layer file {local_path}"))
        .map_err(DownloadError::Other)?;

    tracing::debug!("download complete: {local_path}");

    Ok(bytes_amount)
}
