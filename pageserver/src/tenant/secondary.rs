pub mod heatmap;
mod heatmap_uploader;
mod scheduler;

use std::sync::Arc;

use crate::task_mgr::{self, TaskKind, BACKGROUND_RUNTIME};

use self::{heatmap_uploader::heatmap_uploader_task, scheduler::TenantScoped};

use super::mgr::TenantManager;

use pageserver_api::shard::TenantShardId;
use remote_storage::GenericRemoteStorage;

use tokio_util::sync::CancellationToken;
use utils::completion::Barrier;

enum UploadCommand {
    Upload(TenantShardId),
}

impl TenantScoped for UploadCommand {
    fn get_tenant_shard_id(&self) -> &TenantShardId {
        match self {
            Self::Upload(id) => &id,
        }
    }
}

struct CommandRequest<T> {
    payload: T,
    response_tx: tokio::sync::oneshot::Sender<CommandResponse>,
}

struct CommandResponse {
    result: anyhow::Result<()>,
}

/// The SecondaryController is a pseudo-rpc client for administrative control of secondary mode downloads,
/// and heatmap uploads.  This is not a hot data path: it's primarily a hook for tests,
/// where we want to immediately upload/download for a particular tenant.  In normal operation
/// uploads & downloads are autonomous and not driven by this interface.
pub struct SecondaryController {
    upload_req_tx: tokio::sync::mpsc::Sender<CommandRequest<UploadCommand>>,
}

impl SecondaryController {
    async fn dispatch<T>(
        &self,
        queue: &tokio::sync::mpsc::Sender<CommandRequest<T>>,
        payload: T,
    ) -> anyhow::Result<()> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        queue
            .send(CommandRequest {
                payload,
                response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("Receiver shut down"))?;

        let response = response_rx
            .await
            .map_err(|_| anyhow::anyhow!("Request dropped"))?;

        response.result
    }

    pub async fn upload_tenant(&self, tenant_shard_id: TenantShardId) -> anyhow::Result<()> {
        self.dispatch(&self.upload_req_tx, UploadCommand::Upload(tenant_shard_id))
            .await
    }
}

pub fn spawn_tasks(
    tenant_manager: Arc<TenantManager>,
    remote_storage: GenericRemoteStorage,
    background_jobs_can_start: Barrier,
    cancel: CancellationToken,
) -> SecondaryController {
    let (upload_req_tx, upload_req_rx) =
        tokio::sync::mpsc::channel::<CommandRequest<UploadCommand>>(16);

    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::SecondaryUploads,
        None,
        None,
        "heatmap uploads",
        false,
        async move {
            heatmap_uploader_task(
                tenant_manager,
                remote_storage,
                upload_req_rx,
                background_jobs_can_start,
                cancel,
            )
            .await;

            Ok(())
        },
    );

    SecondaryController { upload_req_tx }
}

/// For running with remote storage disabled: a SecondaryController that is connected to nothing.
pub fn null_controller() -> SecondaryController {
    let (upload_req_tx, _upload_req_rx) =
        tokio::sync::mpsc::channel::<CommandRequest<UploadCommand>>(16);
    SecondaryController { upload_req_tx }
}
