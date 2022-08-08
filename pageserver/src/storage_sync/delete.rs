//! Timeline synchronization logic to delete a bulk of timeline's remote files from the remote storage.

use anyhow::{Context, Result};
use tracing::debug;

use std::path::Path;

use remote_storage::GenericRemoteStorage;
use remote_storage::RemoteStorage;

/// Attempts to remove the timleline layers from the remote storage.
/// If the task had not adjusted the metadata before, the deletion will fail.
pub(super) async fn delete_layer(
    storage: &GenericRemoteStorage,
    local_layer_path: &Path,
) -> Result<()> {
    match storage {
        GenericRemoteStorage::Local(s) => delete_layer_impl(s, local_layer_path).await,
        GenericRemoteStorage::S3(s) => delete_layer_impl(s, local_layer_path).await,
    }
}

async fn delete_layer_impl<'a, P, S>(storage: &'a S, local_layer_path: &Path) -> Result<()>
where
    P: std::fmt::Debug + Send + Sync + 'static,
    S: RemoteStorage<RemoteObjectId = P> + Send + Sync + 'static,
{
    debug!(
        "Deleting layer from remote storage: {:?}",
        local_layer_path.display()
    );

    let storage_path = storage
        .remote_object_id(local_layer_path)
        .with_context(|| {
            format!(
                "Failed to get the layer storage path for local path '{}'",
                local_layer_path.display()
            )
        })?;

    storage.delete(&storage_path).await.with_context(|| {
        format!(
            "Failed to delete remote layer from storage at '{:?}'",
            storage_path
        )
    })
}
