use remote_storage::GenericRemoteStorage;
use tokio;
use tracing::{self, debug, error, info};
use utils::id::{TenantId, TimelineId};

use crate::{config::PageServerConf, tenant::storage_layer::LayerFileName};

/// We aggregate object deletions from many tenants in one place, for several reasons:
/// - Coalesce deletions into fewer DeleteObjects calls
/// - Enable Tenant/Timeline lifetimes to be shorter than the time it takes
///   to flush any outstanding deletions.
/// - Globally control throughput of deletions, as these are a low priority task: do
///   not compete with the same S3 clients/connections used for higher priority uploads.
///
/// DeletionQueue is the frontend that the rest of the pageserver interacts with.
#[derive(Clone)]
pub struct DeletionQueue {
    tx: tokio::sync::mpsc::Sender<QueueMessage>,
}

#[derive(Debug)]
enum QueueMessage {
    Delete(DeletionOp),
    Flush(FlushOp),
}

#[derive(Debug)]
struct DeletionOp {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    layers: Vec<LayerFileName>,
}

#[derive(Debug)]
struct FlushOp {
    tx: tokio::sync::oneshot::Sender<()>,
}

#[derive(Clone)]
pub struct DeletionQueueClient {
    tx: tokio::sync::mpsc::Sender<QueueMessage>,
}

impl DeletionQueueClient {
    async fn do_push(&self, msg: QueueMessage) {
        match self.tx.send(msg).await {
            Ok(_) => {}
            Err(e) => {
                // This shouldn't happen, we should shut down all tenants before
                // we shut down the global delete queue.  If we encounter a bug like this,
                // we may leak objects as deletions won't be processed.
                error!("Deletion queue closed while pushing, shutting down? ({e})");
            }
        }
    }

    pub async fn push(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        layers: Vec<LayerFileName>,
    ) {
        self.do_push(QueueMessage::Delete(DeletionOp {
            tenant_id,
            timeline_id,
            layers,
        }))
        .await;
    }

    pub async fn flush(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.do_push(QueueMessage::Flush(FlushOp { tx })).await;
        if let Err(_) = rx.await {
            // This shouldn't happen if tenants are shut down before deletion queue.  If we
            // encounter a bug like this, then a flusher will incorrectly believe it has flushed
            // when it hasn't, possibly leading to leaking objects.
            error!("Deletion queue dropped flush op while client was still waiting");
        }
    }
}

// TODO: metrics for queue length, deletions executed, deletion errors

pub struct DeletionQueueWorker {
    remote_storage: Option<GenericRemoteStorage>,
    conf: &'static PageServerConf,
    rx: tokio::sync::mpsc::Receiver<QueueMessage>,
}

impl DeletionQueueWorker {
    pub async fn background(&mut self) {
        let remote_storage = match &self.remote_storage {
            Some(rs) => rs,
            None => {
                info!("No remote storage configured, deletion queue will not run");
                return;
            }
        };
        while let Some(msg) = self.rx.recv().await {
            match msg {
                QueueMessage::Delete(op) => {
                    let timeline_path = self.conf.timeline_path(&op.tenant_id, &op.timeline_id);

                    let _span = tracing::info_span!(
                        "execute_deletion",
                        tenant_id = %op.tenant_id,
                        timeline_id = %op.timeline_id,
                    );

                    for layer in op.layers {
                        // TODO go directly to remote path without composing local path
                        let local_path = timeline_path.join(layer.file_name());
                        let path = match self.conf.remote_path(&local_path) {
                            Ok(p) => p,
                            Err(e) => {
                                panic!("Can't make a timeline path! {e}");
                            }
                        };
                        match remote_storage.delete(&path).await {
                            Ok(_) => {
                                debug!("Deleted {path}");
                            }
                            Err(e) => {
                                // TODO: we should persist delete queue before attempting deletion, and then retry later.
                                error!("Failed to delete {path}, leaking object! ({e})");
                            }
                        }
                    }
                }
                QueueMessage::Flush(op) => {
                    if let Err(_) = op.tx.send(()) {
                        // oneshot channel closed. This is legal: a client could be destroyed while waiting for a flush.
                        debug!("deletion queue flush from dropped client");
                    };
                }
            }
        }
        info!("Deletion queue shut down.");
    }
}
impl DeletionQueue {
    pub fn new_client(&self) -> DeletionQueueClient {
        DeletionQueueClient {
            tx: self.tx.clone(),
        }
    }

    pub fn new(
        remote_storage: Option<GenericRemoteStorage>,
        conf: &'static PageServerConf,
    ) -> (Self, DeletionQueueWorker) {
        let (tx, rx) = tokio::sync::mpsc::channel(16384);

        (
            Self { tx },
            DeletionQueueWorker {
                remote_storage,
                conf,
                rx,
            },
        )
    }
}

/// A lightweight queue which can issue ordinary DeletionQueueClient objects, but doesn't do any persistence
/// or coalescing, and doesn't actually execute any deletions unless you call pump() to kick it.
#[cfg(test)]
pub mod mock {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    pub struct MockDeletionQueue {
        tx: tokio::sync::mpsc::Sender<QueueMessage>,
        tx_pump: tokio::sync::mpsc::Sender<FlushOp>,
        executed: Arc<AtomicUsize>,
    }

    impl MockDeletionQueue {
        pub fn new(
            remote_storage: Option<GenericRemoteStorage>,
            conf: &'static PageServerConf,
        ) -> Self {
            let (tx, mut rx) = tokio::sync::mpsc::channel(16384);
            let (tx_pump, mut rx_pump) = tokio::sync::mpsc::channel::<FlushOp>(1);

            let executed = Arc::new(AtomicUsize::new(0));
            let executed_bg = executed.clone();

            tokio::spawn(async move {
                let _span = tracing::info_span!("mock_deletion_queue");
                let remote_storage = match &remote_storage {
                    Some(rs) => rs,
                    None => {
                        info!("No remote storage configured, deletion queue will not run");
                        return;
                    }
                };
                info!("Running mock deletion queue");
                // Each time we are asked to pump, drain the queue of deletions
                while let Some(flush_op) = rx_pump.recv().await {
                    info!("Executing all pending deletions");
                    while let Ok(msg) = rx.try_recv() {
                        match msg {
                            QueueMessage::Delete(op) => {
                                let timeline_path =
                                    conf.timeline_path(&op.tenant_id, &op.timeline_id);

                                let _span = tracing::info_span!(
                                    "execute_deletion",
                                    tenant_id = %op.tenant_id,
                                    timeline_id = %op.timeline_id,
                                );

                                for layer in op.layers {
                                    let local_path = timeline_path.join(layer.file_name());
                                    let path = match conf.remote_path(&local_path) {
                                        Ok(p) => p,
                                        Err(e) => {
                                            panic!("Can't make a timeline path! {e}");
                                        }
                                    };
                                    info!("Executing deletion {path}");
                                    match remote_storage.delete(&path).await {
                                        Ok(_) => {
                                            debug!("Deleted {path}");
                                        }
                                        Err(e) => {
                                            error!(
                                                "Failed to delete {path}, leaking object! ({e})"
                                            );
                                        }
                                    }
                                    executed_bg.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            QueueMessage::Flush(op) => {
                                if let Err(_) = op.tx.send(()) {
                                    // oneshot channel closed. This is legal: a client could be destroyed while waiting for a flush.
                                    debug!("deletion queue flush from dropped client");
                                };
                            }
                        }
                        info!("All pending deletions have been executed");
                    }
                    flush_op
                        .tx
                        .send(())
                        .expect("Test called flush but dropped before finishing");
                }
            });

            Self {
                tx: tx,
                tx_pump,
                executed,
            }
        }

        pub fn get_executed(&self) -> usize {
            self.executed.load(Ordering::Relaxed)
        }

        pub async fn pump(&self) {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.tx_pump
                .send(FlushOp { tx })
                .await
                .expect("pump called after deletion queue loop stopped");
            rx.await
                .expect("Mock delete queue shutdown while waiting to pump");
        }

        pub fn new_client(&self) -> DeletionQueueClient {
            DeletionQueueClient {
                tx: self.tx.clone(),
            }
        }
    }
}
