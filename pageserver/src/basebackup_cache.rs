use std::{path::PathBuf, sync::Arc};

use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio_util::sync::CancellationToken;
use utils::{id::TimelineId, lsn::Lsn};

use crate::tenant::{CheckpointShutdownEvent, CheckpointShutdownSender};

pub struct BasebackupCache {
    datadir: PathBuf,

    checkpoint_sender: CheckpointShutdownSender,
    cancel: CancellationToken,
}

impl BasebackupCache {
    // Creates a BasebackupCache and spawns a background task.
    pub fn spawn(
        datadir: PathBuf,
        handle: &tokio::runtime::Handle,
        cancel: CancellationToken,
    ) -> anyhow::Result<Arc<Self>> {
        let (checkpoint_sender, checkpoint_receiver) = unbounded_channel();

        let cache = Arc::new(BasebackupCache {
            datadir,
            checkpoint_sender,
            cancel,
        });

        handle.spawn(cache.clone().background(checkpoint_receiver));

        Ok(cache)
    }

    // Non-blocking. If an entry exists, opens the archive file and return reader.
    pub async fn get(
        &self,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> std::io::Result<Option<tokio::fs::File>> {
        let path = self.entry_path(timeline_id, lsn);

        let res = match tokio::fs::File::open(path).await {
            Ok(file) => Some(file),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };

        Ok(res)
    }

    pub fn get_checkpoint_shutdown_sender(&self) -> CheckpointShutdownSender {
        self.checkpoint_sender.clone()
    }

    fn entry_path(&self, timeline_id: TimelineId, lsn: Lsn) -> PathBuf {
        // Placeholder for actual implementation
        self.datadir
            .join(format!("basebackup_{}_{}.tar", timeline_id, lsn))
    }

    async fn background(
        self: Arc<Self>,
        mut cp_receiver: UnboundedReceiver<CheckpointShutdownEvent>,
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
