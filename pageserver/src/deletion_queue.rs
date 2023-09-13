mod executor;

use crate::metrics::DELETION_QUEUE_SUBMITTED;
use crate::tenant::remote_timeline_client::remote_layer_path;
use remote_storage::{GenericRemoteStorage, RemotePath};
use thiserror::Error;
use tokio;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use tracing::{self, debug, error};
use utils::generation::Generation;
use utils::id::{TenantId, TimelineId};

use self::executor::ExecutorWorker;
use executor::ExecutorMessage;

use crate::{config::PageServerConf, tenant::storage_layer::LayerFileName};

// TODO: adminstrative "panic button" config property to disable all deletions
// TODO: configurable for how long to wait before executing deletions

/// We aggregate object deletions from many tenants in one place, for several reasons:
/// - Coalesce deletions into fewer DeleteObjects calls
/// - Enable Tenant/Timeline lifetimes to be shorter than the time it takes
///   to flush any outstanding deletions.
/// - Globally control throughput of deletions, as these are a low priority task: do
///   not compete with the same S3 clients/connections used for higher priority uploads.
/// - Enable gating deletions on validation of a tenant's generation number, to make
///   it safe to multi-attach tenants (see docs/rfcs/025-generation-numbers.md)
///
/// There are two kinds of deletion: deferred and immediate.  A deferred deletion
/// may be intentionally delayed to protect passive readers of S3 data, and is
/// subject to a generation number validation step.  An immediate deletion is
/// ready to execute immediately, and is only queued up so that it can be coalesced
/// with other deletions in flight.
///
/// Deferred deletions pass through three steps:
/// - Frontend: accumulate deletion requests from Timelines, and batch them up into
///   DeletionLists, which are persisted to disk.
/// - Backend: accumulate deletion lists, and validate them en-masse prior to passing
///   the keys in the list onward for actual deletion.  Also validate remote_consistent_lsn
///   updates for running timelines.
/// - Executor: accumulate object keys that the backend has validated, and execute them in
///   batches of 1000 keys via DeleteObjects.
///
/// Non-deferred deletions, such as during timeline deletion, bypass the first
/// two stages and are passed straight into the Executor.
///
/// Internally, each stage is joined by a channel to the next.  On disk, there is only
/// one queue (of DeletionLists), which is written by the frontend and consumed
/// by the backend.
#[derive(Clone)]
pub struct DeletionQueue {
    client: DeletionQueueClient,

    // Parent cancellation token for the tokens passed into background workers
    cancel: CancellationToken,
}

/// Opaque wrapper around individual worker tasks, to avoid making the
/// worker objects themselves public
pub struct DeletionQueueWorkers {
    executor: ExecutorWorker,
}

impl DeletionQueueWorkers {
    pub fn spawn_with(mut self, runtime: &tokio::runtime::Handle) -> tokio::task::JoinHandle<()> {
        let jh_executor = runtime.spawn(async move {
            self.executor
                .background()
                .instrument(tracing::info_span!(parent:None, "deletion executor"))
                .await
        });

        runtime.spawn({
            async move {
                drop(jh_executor.await.expect("error joining executor worker"));
            }
        })
    }
}

#[derive(Debug)]
struct FlushOp {
    tx: tokio::sync::oneshot::Sender<()>,
}

impl FlushOp {
    fn fire(self) {
        if self.tx.send(()).is_err() {
            // oneshot channel closed. This is legal: a client could be destroyed while waiting for a flush.
            debug!("deletion queue flush from dropped client");
        };
    }
}

#[derive(Clone, Debug)]
pub struct DeletionQueueClient {
    executor_tx: tokio::sync::mpsc::Sender<ExecutorMessage>,
}

#[derive(Error, Debug)]
pub enum DeletionQueueError {
    #[error("Deletion queue unavailable during shutdown")]
    ShuttingDown,
}

impl DeletionQueueClient {
    pub(crate) fn broken() -> Self {
        // Channels whose receivers are immediately dropped.
        let (executor_tx, _executor_rx) = tokio::sync::mpsc::channel(1);
        Self { executor_tx }
    }

    /// Submit a list of layers for deletion: this function will return before the deletion is
    /// persistent, but it may be executed at any time after this function enters: do not push
    /// layers until you're sure they can be deleted safely (i.e. remote metadata no longer
    /// references them).
    ///
    /// The `current_generation` is the generation of this pageserver's current attachment.  The
    /// generations in `layers` are the generations in which those layers were written.
    pub(crate) async fn push_layers(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        current_generation: Generation,
        layers: Vec<(LayerFileName, Generation)>,
    ) -> Result<(), DeletionQueueError> {
        if !current_generation.is_none() {
            unimplemented!("generation support not yet implemented");
        }
        debug!("Enqueuing deletions in legacy mode, skipping queue");
        let mut layer_paths = Vec::new();
        for (layer, generation) in layers {
            layer_paths.push(remote_layer_path(
                &tenant_id,
                &timeline_id,
                &layer,
                generation,
            ));
        }
        self.push_immediate(layer_paths).await?;
        return self.flush_immediate().await;
    }

    /// This interface bypasses the persistent deletion queue, and any validation
    /// that this pageserver is still elegible to execute the deletions.  It is for
    /// use in timeline deletions, where the control plane is telling us we may
    /// delete everything in the timeline.
    ///
    /// DO NOT USE THIS FROM GC OR COMPACTION CODE.  Use the regular `push_layers`.
    pub(crate) async fn push_immediate(
        &self,
        objects: Vec<RemotePath>,
    ) -> Result<(), DeletionQueueError> {
        DELETION_QUEUE_SUBMITTED.inc_by(objects.len() as u64);
        self.executor_tx
            .send(ExecutorMessage::Delete(objects))
            .await
            .map_err(|_| DeletionQueueError::ShuttingDown)
    }

    /// Companion to push_immediate.  When this returns Ok, all prior objects sent
    /// into push_immediate have been deleted from remote storage.
    pub(crate) async fn flush_immediate(&self) -> Result<(), DeletionQueueError> {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        self.executor_tx
            .send(ExecutorMessage::Flush(FlushOp { tx }))
            .await
            .map_err(|_| DeletionQueueError::ShuttingDown)?;

        rx.await.map_err(|_| DeletionQueueError::ShuttingDown)
    }
}

impl DeletionQueue {
    pub fn new_client(&self) -> DeletionQueueClient {
        self.client.clone()
    }

    /// Caller may use the returned object to construct clients with new_client.
    /// Caller should tokio::spawn the background() members of the two worker objects returned:
    /// we don't spawn those inside new() so that the caller can use their runtime/spans of choice.
    ///
    /// If remote_storage is None, then the returned workers will also be None.
    pub fn new(
        remote_storage: Option<GenericRemoteStorage>,
        _conf: &'static PageServerConf,
    ) -> (Self, Option<DeletionQueueWorkers>) {
        // Shallow channel: it carries lists of paths, and we expect the main queueing to
        // happen in the backend (persistent), not in this queue.
        let (executor_tx, executor_rx) = tokio::sync::mpsc::channel(16);

        // The deletion queue has an independent cancellation token to
        // the general pageserver shutdown token, because it stays alive a bit
        // longer to flush after Tenants have all been torn down.
        let cancel = CancellationToken::new();

        let remote_storage = match remote_storage {
            None => {
                return (
                    Self {
                        client: DeletionQueueClient { executor_tx },
                        cancel,
                    },
                    None,
                )
            }
            Some(r) => r,
        };

        (
            Self {
                client: DeletionQueueClient {
                    executor_tx: executor_tx.clone(),
                },
                cancel: cancel.clone(),
            },
            Some(DeletionQueueWorkers {
                executor: ExecutorWorker::new(remote_storage, executor_rx, cancel.clone()),
            }),
        )
    }
}

/// A lightweight queue which can issue ordinary DeletionQueueClient objects, but doesn't do any persistence
/// or coalescing, and doesn't actually execute any deletions unless you call pump() to kick it.
#[cfg(test)]
pub(crate) mod mock {
    use tracing::info;

    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    pub struct ConsumerState {
        executor_rx: tokio::sync::mpsc::Receiver<ExecutorMessage>,
    }

    impl ConsumerState {
        async fn consume(&mut self, remote_storage: &GenericRemoteStorage) -> usize {
            let mut executed = 0;

            info!("Executing all pending deletions");

            // Transform all executor messages to generic frontend messages
            while let Ok(msg) = self.executor_rx.try_recv() {
                match msg {
                    ExecutorMessage::Delete(objects) => {
                        for path in objects {
                            match remote_storage.delete(&path).await {
                                Ok(_) => {
                                    debug!("Deleted {path}");
                                }
                                Err(e) => {
                                    error!("Failed to delete {path}, leaking object! ({e})");
                                }
                            }
                            executed += 1;
                        }
                    }
                    ExecutorMessage::Flush(flush_op) => {
                        flush_op.fire();
                    }
                }
            }

            executed
        }
    }

    pub struct MockDeletionQueue {
        executor_tx: tokio::sync::mpsc::Sender<ExecutorMessage>,
        executed: Arc<AtomicUsize>,
        remote_storage: Option<GenericRemoteStorage>,
        consumer: std::sync::Mutex<ConsumerState>,
    }

    impl MockDeletionQueue {
        pub fn new(remote_storage: Option<GenericRemoteStorage>) -> Self {
            let (executor_tx, executor_rx) = tokio::sync::mpsc::channel(16384);

            let executed = Arc::new(AtomicUsize::new(0));

            Self {
                executor_tx,
                executed,
                remote_storage,
                consumer: std::sync::Mutex::new(ConsumerState { executor_rx }),
            }
        }

        pub fn get_executed(&self) -> usize {
            self.executed.load(Ordering::Relaxed)
        }

        #[allow(clippy::await_holding_lock)]
        pub async fn pump(&self) {
            if let Some(remote_storage) = &self.remote_storage {
                // Permit holding mutex across await, because this is only ever
                // called once at a time in tests.
                let mut locked = self.consumer.lock().unwrap();
                let count = locked.consume(remote_storage).await;
                self.executed.fetch_add(count, Ordering::Relaxed);
            }
        }

        pub(crate) fn new_client(&self) -> DeletionQueueClient {
            DeletionQueueClient {
                executor_tx: self.executor_tx.clone(),
            }
        }
    }
}
