use super::super::super::index::LayerFileMetadata;
use super::super::download_retry;
use super::super::TEMP_DOWNLOAD_EXTENSION;
use crate::config::PageServerConf;
use crate::span::debug_assert_current_span_has_tenant_and_timeline_id;
use crate::tenant::remote_timeline_client::download_cancellable;
use crate::tenant::remote_timeline_client::remote_layer_path;
use crate::tenant::remote_timeline_client::BUFFER_SIZE;
use crate::tenant::remote_timeline_client::DOWNLOAD_TIMEOUT;
use crate::tenant::storage_layer::LayerFileName;
use crate::virtual_file::on_fatal_io_error;
use crate::virtual_file::VirtualFile;
use anyhow::Context;
use bytes::BytesMut;
use futures::StreamExt;
use pageserver_api::shard::TenantShardId;
use remote_storage::DownloadError;
use remote_storage::GenericRemoteStorage;
use tokio::fs;

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

    let (destination_file, bytes_amount) = download_retry(
        || async {
            let mut destination_file = VirtualFile::create(&temp_file_path)
                .await
                .with_context(|| format!("create a destination file for layer '{temp_file_path}'"))
                .map_err(DownloadError::Other)?;

            // Cancellation safety: it is safe to cancel this future, because it isn't writing to a local
            // file: the write to local file doesn't start until after the request header is returned
            // and we start draining the body stream below
            let mut download = download_cancellable(cancel, storage.download(&remote_path))
                .await
                .with_context(|| {
                    format!(
                    "open a download stream for layer with remote storage path '{remote_path:?}'"
                )
                })
                .map_err(DownloadError::Other)?;

            // Cancellation safety: it is safe to cancel this future because it is writing into a temporary file,
            // and we will unlink the temporary file if there is an error.  This unlink is important because we
            // are in a retry loop, and we wouldn't want to leave behind a rogue write I/O to a file that
            // we will imminiently try and write to again.
            match timeout_cancellable(DOWNLOAD_TIMEOUT, cancel, async move {
                // TODO: use vectored write (writev) once supported by tokio-epoll-uring.
                // There's chunks_vectored()
                let mut buf = BytesMut::with_capacity(BUFFER_SIZE);
                let mut bytes_amount: u64 = 0;
                while let Some(chunk) = download.download_stream.next().await {
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(e) => return Err(e),
                    };
                    let mut chunk = &chunk[..];
                    while !chunk.is_empty() {
                        let need = BUFFER_SIZE - buf.len();
                        let have = chunk.len();
                        let n = std::cmp::min(need, have);
                        buf.extend_from_slice(&chunk[..n]);
                        chunk = &chunk[n..];
                        let res;
                        (buf, res) = destination_file.write_all(buf).await;
                        let nwritten = res?;
                        assert_eq!(nwritten, n);
                        bytes_amount += u64::try_from(n).unwrap();
                    }
                }
                Ok((destination_file, bytes_amount))
            })
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
            .map_err(DownloadError::Other)
        },
        &format!("download {remote_path:?}"),
        cancel,
    )
    .await?;

    let expected = layer_metadata.file_size();
    if expected != bytes_amount {
        return Err(DownloadError::Other(anyhow!(
            "According to layer file metadata should have downloaded {expected} bytes but downloaded {bytes_amount} bytes into file {temp_file_path:?}",
        )));
    }

    todo!("implement tokio-epoll-uring version of crashsafe::durable_rename and use it here; also needed by follow-up of https://github.com/neondatabase/neon/pull/6731");

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
