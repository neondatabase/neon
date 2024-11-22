use std::num::NonZeroU32;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::{collections::HashSet, time::Duration};

use anyhow::Context;
use remote_storage::{
    GenericRemoteStorage, LocalFs, RemotePath, RemoteStorageConfig, RemoteStorageKind,
};
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
    async fn setup(max_keys_per_list_response: Option<u32>) -> Self {
        let storage_root = camino_tempfile::tempdir()
            .expect("create tempdir")
            .path()
            .to_path_buf();

        let remote_storage_config = RemoteStorageConfig {
            storage: RemoteStorageKind::LocalFs {
                local_path: storage_root,
                max_keys_per_list_response: max_keys_per_list_response.and_then(NonZeroU32::new),
            },
            timeout: Duration::from_secs(120),
        };
        let client = Arc::new(
            GenericRemoteStorage::from_config(&remote_storage_config)
                .await
                .context("remote storage init")
                .unwrap(),
        );

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
        Self::Enabled(EnabledLocalFs::setup(None).await)
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

        let max_keys_in_list_response = 10;
        let upload_tasks_count = 1 + (2 * usize::try_from(max_keys_in_list_response).unwrap());

        let enabled = EnabledLocalFs::setup(Some(max_keys_in_list_response)).await;

        match upload_remote_data(&enabled.client, enabled.base_prefix, upload_tasks_count).await {
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

impl AsyncTestContext for MaybeEnabledStorageWithSimpleTestBlobs {
    async fn setup() -> Self {
        ensure_logging_ready();
        let max_keys_in_list_response = 10;
        let upload_tasks_count = 1 + (2 * usize::try_from(max_keys_in_list_response).unwrap());

        let enabled = EnabledLocalFs::setup(Some(max_keys_in_list_response)).await;

        match upload_simple_remote_data(&enabled.client, upload_tasks_count).await {
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
