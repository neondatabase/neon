//! Timeline synchronization logic to compress and upload to the remote storage all new timeline files from the checkpoints.
//! FIXME update comment

use std::{fmt::Debug, path::Path};

use anyhow::{Context, Result};
use tokio::fs;

use remote_storage::GenericRemoteStorage;
use remote_storage::RemoteStorage;
use utils::zid::{ZTenantId, ZTimelineId};

use super::index::IndexPart;
use crate::{config::PageServerConf, layered_repository::metadata::metadata_path};

/// Serializes and uploads the given index part data to the remote storage.
pub(super) async fn upload_index_part(
    conf: &'static PageServerConf,
    storage: &GenericRemoteStorage,
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    index_part: &IndexPart,
) -> anyhow::Result<()> {
    match storage {
        GenericRemoteStorage::Local(s) => {
            upload_index_part_impl(conf, s, tenant_id, timeline_id, index_part).await
        }
        GenericRemoteStorage::S3(s) => {
            upload_index_part_impl(conf, s, tenant_id, timeline_id, index_part).await
        }
    }
}
async fn upload_index_part_impl<P, S>(
    conf: &'static PageServerConf,
    storage: &S,
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
    index_part: &IndexPart,
) -> anyhow::Result<()>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let index_part_bytes = serde_json::to_vec(&index_part)
        .context("Failed to serialize index part file into bytes")?;
    let index_part_size = index_part_bytes.len();
    let index_part_bytes = tokio::io::BufReader::new(std::io::Cursor::new(index_part_bytes));

    let index_part_path =
        metadata_path(conf, timeline_id, tenant_id).with_file_name(IndexPart::FILE_NAME);
    let index_part_storage_path =
        storage
            .remote_object_id(&index_part_path)
            .with_context(|| {
                format!(
                    "Failed to get the index part storage path for local path '{}'",
                    index_part_path.display()
                )
            })?;

    storage
        .upload(
            index_part_bytes,
            index_part_size,
            &index_part_storage_path,
            None,
        )
        .await
        .with_context(|| {
            format!("Failed to upload index part to the storage path '{index_part_storage_path:?}'")
        })?;

    Ok(())
}

/// Attempts to upload given layer files.
/// No extra checks for overlapping files is made and any files that are already present remotely will be overwritten, if submitted during the upload.
///
/// On an error, bumps the retries count and reschedules the entire task.
pub(super) async fn upload_timeline_layer(
    storage: &GenericRemoteStorage,
    source_path: &Path,
) -> Result<()> {
    match storage {
        GenericRemoteStorage::Local(s) => upload_timeline_layer_impl(s, source_path).await,
        GenericRemoteStorage::S3(s) => upload_timeline_layer_impl(s, source_path).await,
    }
}

async fn upload_timeline_layer_impl<'a, P, S>(storage: &'a S, source_path: &Path) -> Result<()>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    let storage_path = storage.remote_object_id(&source_path).with_context(|| {
        format!(
            "Failed to get the layer storage path for local path '{}'",
            source_path.display()
        )
    })?;
    println!("STORAGE_PATH: {:?}", storage_path);

    let source_file = fs::File::open(&source_path).await.with_context(|| {
        format!(
            "Failed to upen a source file for layer '{}'",
            source_path.display()
        )
    })?;

    let source_size = source_file
        .metadata()
        .await
        .with_context(|| {
            format!(
                "Failed to get the source file metadata for layer '{}'",
                source_path.display()
            )
        })?
        .len() as usize;

    storage
        .upload(source_file, source_size, &storage_path, None)
        .await
        .with_context(|| {
            format!(
                "Failed to upload a layer from local path '{}'",
                source_path.display()
            )
        })?;

    Ok(())
}
