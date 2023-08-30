//! Helper functions to delete files from remote storage with a RemoteStorage
use anyhow::Context;
use std::path::Path;
use tracing::debug;

use remote_storage::GenericRemoteStorage;

use crate::{
    config::PageServerConf,
    tenant::{remote_timeline_client::remote_path, Generation},
};

pub(super) async fn delete_layer<'a>(
    conf: &'static PageServerConf,
    storage: &'a GenericRemoteStorage,
    local_layer_path: &'a Path,
    generation: Generation,
) -> anyhow::Result<()> {
    fail::fail_point!("before-delete-layer", |_| {
        anyhow::bail!("failpoint before-delete-layer")
    });
    debug!("Deleting layer from remote storage: {local_layer_path:?}",);

    let path_to_delete = remote_path(conf, local_layer_path, generation)?;

    // We don't want to print an error if the delete failed if the file has
    // already been deleted. Thankfully, in this situation S3 already
    // does not yield an error. While OS-provided local file system APIs do yield
    // errors, we avoid them in the `LocalFs` wrapper.
    storage
        .delete(&path_to_delete)
        .await
        .with_context(|| format!("delete remote layer from storage at {path_to_delete:?}"))
}
