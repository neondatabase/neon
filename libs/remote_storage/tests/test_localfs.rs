use std::ops::ControlFlow;
use std::sync::Arc;
use std::{collections::HashSet, time::Duration};

use remote_storage::{GenericRemoteStorage, LocalFs, RemotePath};
use test_context::AsyncTestContext;
use tokio_util::sync::CancellationToken;
use tracing::info;

mod common;

#[path = "common/tests.rs"]
mod tests_localfs;

use common::{
    cleanup, ensure_logging_ready, upload_remote_data, upload_simple_remote_data, RemoteBlobInfo,
};

const BASE_PREFIX: &str = "test";

struct EnabledLocalFs {
    client: Arc<GenericRemoteStorage>,
    base_prefix: &'static str,
}

impl EnabledLocalFs {
    async fn setup() -> Self {
        let storage_root = camino_tempfile::tempdir()
            .expect("create tempdir")
            .path()
            .to_path_buf();
        let (local_fs, _) = LocalFs::new(storage_root, Duration::from_secs(120))
            .map(|s| (s, CancellationToken::new()))
            .expect("create LocalFs");
        let client = Arc::new(GenericRemoteStorage::LocalFs(local_fs));
        EnabledLocalFs {
            client,
            base_prefix: &BASE_PREFIX,
        }
    }
}

enum MaybeEnabledStorage {
    Enabled(EnabledLocalFs),
    Disabled,
}

impl AsyncTestContext for MaybeEnabledStorage {
    async fn setup() -> Self {
        ensure_logging_ready();
        Self::Enabled(EnabledLocalFs::setup().await)
    }
}

enum MaybeEnabledStorageWithTestBlobs {
    Enabled(LocalFsWithTestBlobs),
    Disabled,
    UploadsFailed(anyhow::Error, LocalFsWithTestBlobs),
}

struct LocalFsWithTestBlobs {
    enabled: EnabledLocalFs,
    remote_prefixes: HashSet<RemotePath>,
    remote_blobs: HashSet<RemoteBlobInfo>,
}

impl AsyncTestContext for MaybeEnabledStorageWithTestBlobs {
    async fn setup() -> Self {
        ensure_logging_ready();

        let enabled = EnabledLocalFs::setup().await;

        match upload_remote_data(&enabled.client, enabled.base_prefix, UPLOAD_TASKS_COUNT).await {
            ControlFlow::Continue(uploads) => {
                info!("Remote objects created successfully");

                Self::Enabled(LocalFsWithTestBlobs {
                    enabled,
                    remote_prefixes: uploads.prefixes,
                    remote_blobs: uploads.blobs,
                })
            }
            ControlFlow::Break(uploads) => Self::UploadsFailed(
                anyhow::anyhow!("One or multiple blobs failed to upload to Azure"),
                LocalFsWithTestBlobs {
                    enabled,
                    remote_prefixes: uploads.prefixes,
                    remote_blobs: uploads.blobs,
                },
            ),
        }
    }

    async fn teardown(self) {
        match self {
            Self::Disabled => {}
            Self::Enabled(ctx) | Self::UploadsFailed(_, ctx) => {
                cleanup(&ctx.enabled.client, ctx.remote_blobs).await;
            }
        }
    }
}

enum MaybeEnabledStorageWithSimpleTestBlobs {
    Enabled(LocalFsWithSimpleTestBlobs),
    Disabled,
    UploadsFailed(anyhow::Error, LocalFsWithSimpleTestBlobs),
}
struct LocalFsWithSimpleTestBlobs {
    enabled: EnabledLocalFs,
    remote_blobs: HashSet<RemoteBlobInfo>,
}

const UPLOAD_TASKS_COUNT: usize = 10;

impl AsyncTestContext for MaybeEnabledStorageWithSimpleTestBlobs {
    async fn setup() -> Self {
        ensure_logging_ready();
        let enabled = EnabledLocalFs::setup().await;

        match upload_simple_remote_data(&enabled.client, UPLOAD_TASKS_COUNT).await {
            ControlFlow::Continue(uploads) => {
                info!("Remote objects created successfully");

                Self::Enabled(LocalFsWithSimpleTestBlobs {
                    enabled,
                    remote_blobs: uploads,
                })
            }
            ControlFlow::Break(uploads) => Self::UploadsFailed(
                anyhow::anyhow!("One or multiple blobs failed to upload to Azure"),
                LocalFsWithSimpleTestBlobs {
                    enabled,
                    remote_blobs: uploads,
                },
            ),
        }
    }

    async fn teardown(self) {
        match self {
            Self::Disabled => {}
            Self::Enabled(ctx) | Self::UploadsFailed(_, ctx) => {
                cleanup(&ctx.enabled.client, ctx.remote_blobs).await;
            }
        }
    }
}
