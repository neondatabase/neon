use std::sync::Arc;

use camino::Utf8PathBuf;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_util::sync::CancellationToken;
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

pub struct BasebackupPrepareRequest {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub lsn: Lsn,
}

pub type BasebackupPrepareSender = UnboundedSender<BasebackupPrepareRequest>;

pub struct BasebackupCache {
    datadir: Utf8PathBuf,

    checkpoint_sender: BasebackupPrepareSender,
    cancel: CancellationToken,
}

impl BasebackupCache {
    // Creates a BasebackupCache and spawns a background task.
    pub fn spawn(
        runtime_handle: &tokio::runtime::Handle,
        datadir: Utf8PathBuf,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        let (checkpoint_sender, checkpoint_receiver) = unbounded_channel();

        let cache = Arc::new(BasebackupCache {
            datadir,
            checkpoint_sender,
            cancel,
        });

        runtime_handle.spawn(cache.clone().background(checkpoint_receiver));

        cache
    }

    // Non-blocking. If an entry exists, opens the archive file and return reader.
    pub async fn get(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> Option<tokio::fs::File> {
        // TODO(diko): add a fast check to avoid syscall every on every call.

        let path = self.entry_path(tenant_id, timeline_id, lsn);

        match tokio::fs::File::open(path).await {
            Ok(file) => Some(file),
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!("Unexpected error opening basebackup cache file: {:?}", e);
                }
                None
            }
        }
    }

    pub fn get_checkpoint_shutdown_sender(&self) -> BasebackupPrepareSender {
        self.checkpoint_sender.clone()
    }

    fn entry_path(&self, tenant_id: TenantId, timeline_id: TimelineId, lsn: Lsn) -> Utf8PathBuf {
        // Placeholder for actual implementation
        self.datadir
            .join(format!("basebackup_{tenant_id}_{timeline_id}_{lsn}.tar",))
    }

    async fn background(
        self: Arc<Self>,
        mut cp_receiver: UnboundedReceiver<BasebackupPrepareRequest>,
    ) {
        loop {
            tokio::select! {
                // Wait for a checkpoint shutdown message
                Some(cp) = cp_receiver.recv() => {
                    // Handle the checkpoint shutdown message
                    tracing::info!("Received checkpoint shutdown for timeline: {:?} at {:?}", cp.timeline_id, cp.lsn);
                }
                // Check for cancellation
                _ = self.cancel.cancelled() => {
                    tracing::info!("BasebackupCache background task cancelled");
                    break;
                }
            }
        }
    }
}
