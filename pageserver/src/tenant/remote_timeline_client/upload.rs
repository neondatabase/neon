//! Helper functions to upload files to remote storage with a RemoteStorage

use anyhow::{bail, Context};
use fail::fail_point;
use std::{io::ErrorKind, path::Path};
use tokio::fs;

use crate::{config::PageServerConf, tenant::remote_timeline_client::index::IndexPart};
use remote_storage::GenericRemoteStorage;
use utils::id::{TenantId, TimelineId};

use super::index::LayerFileMetadata;

use tracing::info;

/// Serializes and uploads the given index part data to the remote storage.
pub(super) async fn upload_index_part<'a>(
    conf: &'static PageServerConf,
    storage: &'a GenericRemoteStorage,
    tenant_id: &TenantId,
    timeline_id: &TimelineId,
    index_part: &'a IndexPart,
) -> anyhow::Result<()> {
    tracing::trace!("uploading new index part");

    fail_point!("before-upload-index", |_| {
        bail!("failpoint before-upload-index")
    });

    let index_part_bytes = serde_json::to_vec(&index_part)
        .context("Failed to serialize index part file into bytes")?;
    let index_part_size = index_part_bytes.len();
    let index_part_bytes = tokio::io::BufReader::new(std::io::Cursor::new(index_part_bytes));

    let index_part_path = conf
        .metadata_path(tenant_id, timeline_id)
        .with_file_name(IndexPart::FILE_NAME);
    let storage_path = conf.remote_path(&index_part_path)?;

    storage
        .upload_storage_object(Box::new(index_part_bytes), index_part_size, &storage_path)
        .await
        .with_context(|| format!("Failed to upload index part for '{tenant_id} / {timeline_id}'"))
}

/// Attempts to upload given layer files.
/// No extra checks for overlapping files is made and any files that are already present remotely will be overwritten, if submitted during the upload.
///
/// On an error, bumps the retries count and reschedules the entire task.
pub(super) async fn upload_timeline_layer<'a>(
    conf: &'static PageServerConf,
    storage: &'a GenericRemoteStorage,
    source_path: &'a Path,
    known_metadata: &'a LayerFileMetadata,
) -> anyhow::Result<()> {
    fail_point!("before-upload-layer", |_| {
        bail!("failpoint before-upload-layer")
    });
    let storage_path = conf.remote_path(source_path)?;

    let source_file_res = fs::File::open(&source_path).await;
    let source_file = match source_file_res {
        Ok(source_file) => source_file,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            // In some situations we might run into the underlying file being deleted by
            // e.g. compaction before the uploader gets to it. In that instance, we don't
            // want to retry the error: a deleted file won't come back. In theory, the
            // file might not have been written in the first place, which also indicates
            // a bug. Still log the situation so that we can keep an eye on it.
            // See https://github.com/neondatabase/neon/issues/4526
            info!(path = %source_path.display(), "File to upload doesn't exist. Likely the file has been deleted and an upload is not required any more.");
            return Ok(());
        }
        Err(e) => Err(e)
            .with_context(|| format!("Failed to open a source file for layer {source_path:?}"))?,
    };

    let fs_size = source_file
        .metadata()
        .await
        .with_context(|| {
            format!("Failed to get the source file metadata for layer {source_path:?}")
        })?
        .len();

    let metadata_size = known_metadata.file_size();
    if metadata_size != fs_size {
        bail!("File {source_path:?} has its current FS size {fs_size} diferent from initially determined {metadata_size}");
    }

    let fs_size = usize::try_from(fs_size).with_context(|| {
        format!("File {source_path:?} size {fs_size} could not be converted to usize")
    })?;

    storage
        .upload(source_file, fs_size, &storage_path, None)
        .await
        .with_context(|| {
            format!(
                "Failed to upload a layer from local path '{}'",
                source_path.display()
            )
        })?;

    Ok(())
}
