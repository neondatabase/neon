//! Abstractions for the page server to store its relish layer data in the external storage.
//!
//! Main purpose of this module subtree is to provide a set of abstractions to manage the storage state
//! in a way, optimal for page server.
//!
//! The abstractions hide multiple custom external storage API implementations,
//! such as AWS S3, local filesystem, etc., located in the submodules.

mod local_fs;
mod rust_s3;
/// A queue-based storage with the background machinery behind it to synchronize
/// local page server layer files with external storage.
mod synced_storage;

use std::{path::Path, thread};

use anyhow::Context;

pub use self::synced_storage::schedule_timeline_upload;
use self::{local_fs::LocalFs, rust_s3::RustS3};
use crate::{PageServerConf, RelishStorageKind};

pub fn run_storage_sync_thread(
    config: &'static PageServerConf,
) -> anyhow::Result<Option<thread::JoinHandle<anyhow::Result<()>>>> {
    match &config.relish_storage_config {
        Some(relish_storage_config) => {
            let max_concurrent_sync = relish_storage_config.max_concurrent_sync;
            match &relish_storage_config.storage {
                RelishStorageKind::LocalFs(root) => synced_storage::run_storage_sync_thread(
                    config,
                    LocalFs::new(root.clone())?,
                    max_concurrent_sync,
                ),
                RelishStorageKind::AwsS3(s3_config) => synced_storage::run_storage_sync_thread(
                    config,
                    RustS3::new(s3_config)?,
                    max_concurrent_sync,
                ),
            }
        }
        None => Ok(None),
    }
}

/// Storage (potentially remote) API to manage its state.
#[async_trait::async_trait]
pub trait RelishStorage: Send + Sync {
    type RelishStoragePath;

    fn derive_destination(
        page_server_workdir: &Path,
        relish_local_path: &Path,
    ) -> anyhow::Result<Self::RelishStoragePath>;

    async fn list_relishes(&self) -> anyhow::Result<Vec<Self::RelishStoragePath>>;

    async fn download_relish(
        &self,
        from: &Self::RelishStoragePath,
        to: &Path,
    ) -> anyhow::Result<()>;

    async fn delete_relish(&self, path: &Self::RelishStoragePath) -> anyhow::Result<()>;

    async fn upload_relish(&self, from: &Path, to: &Self::RelishStoragePath) -> anyhow::Result<()>;
}

fn strip_workspace_prefix<'a>(
    page_server_workdir: &'a Path,
    relish_local_path: &'a Path,
) -> anyhow::Result<&'a Path> {
    relish_local_path
        .strip_prefix(page_server_workdir)
        .with_context(|| {
            format!(
                "Unexpected: relish local path '{}' is not relevant to server workdir",
                relish_local_path.display(),
            )
        })
}
