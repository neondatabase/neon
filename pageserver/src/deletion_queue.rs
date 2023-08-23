mod backend;
mod executor;
mod frontend;

use crate::metrics::DELETION_QUEUE_SUBMITTED;
use remote_storage::{GenericRemoteStorage, RemotePath};
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use thiserror::Error;
use tokio;
use tokio_util::sync::CancellationToken;
use tracing::{self, debug, error};
use utils::id::{TenantId, TimelineId};

pub(crate) use self::backend::BackendQueueWorker;
use self::executor::ExecutorWorker;
use self::frontend::DeletionOp;
pub(crate) use self::frontend::FrontendQueueWorker;
use backend::BackendQueueMessage;
use executor::ExecutorMessage;
use frontend::FrontendQueueMessage;

use crate::{config::PageServerConf, tenant::storage_layer::LayerFileName};

// Arbitrary thresholds for retries: we do not depend on success
// within OP_RETRIES, as workers will just go around their consume loop:
// the purpose of the backoff::retries with these constants are to
// retry _sooner_ than we would if going around the whole loop.
const FAILED_REMOTE_OP_WARN_THRESHOLD: u32 = 3;

const FAILED_REMOTE_OP_RETRIES: u32 = 10;

// TODO: adminstrative "panic button" config property to disable all deletions
// TODO: configurable for how long to wait before executing deletions

/// We aggregate object deletions from many tenants in one place, for several reasons:
/// - Coalesce deletions into fewer DeleteObjects calls
/// - Enable Tenant/Timeline lifetimes to be shorter than the time it takes
///   to flush any outstanding deletions.
/// - Globally control throughput of deletions, as these are a low priority task: do
///   not compete with the same S3 clients/connections used for higher priority uploads.
/// - Future: enable validating that we may do deletions in a multi-attached scenario,
///   via generation numbers (see https://github.com/neondatabase/neon/pull/4919)
///
/// There are two kinds of deletion: deferred and immediate.  A deferred deletion
/// may be intentionally delayed to protect passive readers of S3 data, and may
/// be subject to a generation number validation step.  An immediate deletion is
/// ready to execute immediately, and is only queued up so that it can be coalesced
/// with other deletions in flight.
///
/// Deferred deletions pass through three steps:
/// - Frontend: accumulate deletion requests from Timelines, and batch them up into
///   DeletionLists, which are persisted to S3.
/// - Backend: accumulate deletion lists, and validate them en-masse prior to passing
///   the keys in the list onward for actual deletion
/// - Executor: accumulate object keys that the backend has validated for immediate
///   deletion, and execute them in batches of 1000 keys via DeleteObjects.
///
/// Non-deferred deletions, such as during timeline deletion, bypass the first
/// two stages and are passed straight into the Executor.
///
/// Internally, each stage is joined by a channel to the next.  In S3, there is only
/// one queue (of DeletionLists), which is written by the frontend and consumed
/// by the backend.
#[derive(Clone)]
pub struct DeletionQueue {
    client: DeletionQueueClient,
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

#[derive(Clone)]
pub struct DeletionQueueClient {
    tx: tokio::sync::mpsc::Sender<FrontendQueueMessage>,
    executor_tx: tokio::sync::mpsc::Sender<ExecutorMessage>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct DeletionList {
    /// Serialization version, for future use
    version: u8,

    /// Used for constructing a unique key for each deletion list we write out.
    sequence: u64,

    /// These objects are elegible for deletion: they are unlinked from timeline metadata, and
    /// we are free to delete them at any time from their presence in this data structure onwards.
    objects: Vec<RemotePath>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct DeletionHeader {
    /// Serialization version, for future use
    version: u8,

    /// Enable determining the next sequence number even if there are no deletion lists present.
    /// If there _are_ deletion lists present, then their sequence numbers take precedence over
    /// this.
    last_deleted_list_seq: u64,
    // TODO: this is where we will track a 'clean' sequence number that indicates all deletion
    // lists <= that sequence have had their generations validated with the control plane
    // and are OK to execute.
}

impl DeletionHeader {
    const VERSION_LATEST: u8 = 1;

    fn new(last_deleted_list_seq: u64) -> Self {
        Self {
            version: Self::VERSION_LATEST,
            last_deleted_list_seq,
        }
    }
}

impl DeletionList {
    const VERSION_LATEST: u8 = 1;
    fn new(sequence: u64) -> Self {
        Self {
            version: Self::VERSION_LATEST,
            sequence,
            objects: Vec::new(),
        }
    }
}

#[derive(Error, Debug)]
pub enum DeletionQueueError {
    #[error("Deletion queue unavailable during shutdown")]
    ShuttingDown,
}

impl DeletionQueueClient {
    async fn do_push(&self, msg: FrontendQueueMessage) -> Result<(), DeletionQueueError> {
        match self.tx.send(msg).await {
            Ok(_) => Ok(()),
            Err(e) => {
                // This shouldn't happen, we should shut down all tenants before
                // we shut down the global delete queue.  If we encounter a bug like this,
                // we may leak objects as deletions won't be processed.
                error!("Deletion queue closed while pushing, shutting down? ({e})");
                Err(DeletionQueueError::ShuttingDown)
            }
        }
    }

    /// Submit a list of layers for deletion: this function will return before the deletion is
    /// persistent, but it may be executed at any time after this function enters: do not push
    /// layers until you're sure they can be deleted safely (i.e. remote metadata no longer
    /// references them).
    pub(crate) async fn push_layers(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        layers: Vec<LayerFileName>,
    ) -> Result<(), DeletionQueueError> {
        DELETION_QUEUE_SUBMITTED.inc_by(layers.len() as u64);
        self.do_push(FrontendQueueMessage::Delete(DeletionOp {
            tenant_id,
            timeline_id,
            layers,
            objects: Vec::new(),
        }))
        .await
    }

    async fn do_flush(
        &self,
        msg: FrontendQueueMessage,
        rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), DeletionQueueError> {
        self.do_push(msg).await?;
        if rx.await.is_err() {
            // This shouldn't happen if tenants are shut down before deletion queue.  If we
            // encounter a bug like this, then a flusher will incorrectly believe it has flushed
            // when it hasn't, possibly leading to leaking objects.
            error!("Deletion queue dropped flush op while client was still waiting");
            Err(DeletionQueueError::ShuttingDown)
        } else {
            Ok(())
        }
    }

    /// Wait until all previous deletions are persistent (either executed, or written to a DeletionList)
    pub async fn flush(&self) -> Result<(), DeletionQueueError> {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        self.do_flush(FrontendQueueMessage::Flush(FlushOp { tx }), rx)
            .await
    }

    // Wait until all previous deletions are executed
    pub(crate) async fn flush_execute(&self) -> Result<(), DeletionQueueError> {
        debug!("flush_execute: flushing to deletion lists...");
        // Flush any buffered work to deletion lists
        self.flush().await?;

        // Flush execution of deletion lists
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        debug!("flush_execute: flushing execution...");
        self.do_flush(FrontendQueueMessage::FlushExecute(FlushOp { tx }), rx)
            .await?;
        debug!("flush_execute: finished flushing execution...");
        Ok(())
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
        conf: &'static PageServerConf,
        cancel: CancellationToken,
    ) -> (
        Self,
        Option<FrontendQueueWorker>,
        Option<BackendQueueWorker>,
        Option<ExecutorWorker>,
    ) {
        // Deep channel: it consumes deletions from all timelines and we do not want to block them
        let (tx, rx) = tokio::sync::mpsc::channel(16384);

        // Shallow channel: it carries DeletionLists which each contain up to thousands of deletions
        let (backend_tx, backend_rx) = tokio::sync::mpsc::channel(16);

        // Shallow channel: it carries lists of paths, and we expect the main queueing to
        // happen in the backend (persistent), not in this queue.
        let (executor_tx, executor_rx) = tokio::sync::mpsc::channel(16);

        let remote_storage = match remote_storage {
            None => {
                return (
                    Self {
                        client: DeletionQueueClient { tx, executor_tx },
                    },
                    None,
                    None,
                    None,
                )
            }
            Some(r) => r,
        };

        (
            Self {
                client: DeletionQueueClient {
                    tx,
                    executor_tx: executor_tx.clone(),
                },
            },
            Some(FrontendQueueWorker::new(
                remote_storage.clone(),
                conf,
                rx,
                backend_tx,
                cancel.clone(),
            )),
            Some(BackendQueueWorker::new(
                remote_storage.clone(),
                conf,
                backend_rx,
                executor_tx,
            )),
            Some(ExecutorWorker::new(
                remote_storage,
                executor_rx,
                cancel.clone(),
            )),
        )
    }
}

#[cfg(test)]
mod test {
    use hex_literal::hex;
    use std::{
        io::ErrorKind,
        path::{Path, PathBuf},
    };
    use tracing::info;

    use remote_storage::{RemoteStorageConfig, RemoteStorageKind};
    use tokio::{runtime::EnterGuard, task::JoinHandle};

    use crate::tenant::harness::TenantHarness;

    use super::*;
    pub const TIMELINE_ID: TimelineId =
        TimelineId::from_array(hex!("11223344556677881122334455667788"));

    struct TestSetup {
        runtime: &'static tokio::runtime::Runtime,
        _entered_runtime: EnterGuard<'static>,
        harness: TenantHarness,
        remote_fs_dir: PathBuf,
        storage: GenericRemoteStorage,
        deletion_queue: DeletionQueue,
        fe_worker: JoinHandle<()>,
        be_worker: JoinHandle<()>,
        ex_worker: JoinHandle<()>,
    }

    impl TestSetup {
        /// Simulate a pageserver restart by destroying and recreating the deletion queue
        fn restart(&mut self) {
            let (deletion_queue, fe_worker, be_worker, ex_worker) = DeletionQueue::new(
                Some(self.storage.clone()),
                self.harness.conf,
                CancellationToken::new(),
            );

            self.deletion_queue = deletion_queue;

            let mut fe_worker = fe_worker.unwrap();
            let mut be_worker = be_worker.unwrap();
            let mut ex_worker = ex_worker.unwrap();
            let mut fe_worker = self
                .runtime
                .spawn(async move { fe_worker.background().await });
            let mut be_worker = self
                .runtime
                .spawn(async move { be_worker.background().await });
            let mut ex_worker = self.runtime.spawn(async move {
                drop(ex_worker.background().await);
            });
            std::mem::swap(&mut self.fe_worker, &mut fe_worker);
            std::mem::swap(&mut self.be_worker, &mut be_worker);
            std::mem::swap(&mut self.ex_worker, &mut ex_worker);

            // Join the old workers
            self.runtime.block_on(fe_worker).unwrap();
            self.runtime.block_on(be_worker).unwrap();
            self.runtime.block_on(ex_worker).unwrap();
        }
    }

    fn setup(test_name: &str) -> anyhow::Result<TestSetup> {
        let test_name = Box::leak(Box::new(format!("deletion_queue__{test_name}")));
        let harness = TenantHarness::create(test_name)?;

        // We do not load() the harness: we only need its config and remote_storage

        // Set up a GenericRemoteStorage targetting a directory
        let remote_fs_dir = harness.conf.workdir.join("remote_fs");
        std::fs::create_dir_all(remote_fs_dir)?;
        let remote_fs_dir = std::fs::canonicalize(harness.conf.workdir.join("remote_fs"))?;
        let storage_config = RemoteStorageConfig {
            max_concurrent_syncs: std::num::NonZeroUsize::new(
                remote_storage::DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNCS,
            )
            .unwrap(),
            max_sync_errors: std::num::NonZeroU32::new(
                remote_storage::DEFAULT_REMOTE_STORAGE_MAX_SYNC_ERRORS,
            )
            .unwrap(),
            storage: RemoteStorageKind::LocalFs(remote_fs_dir.clone()),
        };
        let storage = GenericRemoteStorage::from_config(&storage_config).unwrap();

        let runtime = Box::leak(Box::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
        ));
        let entered_runtime = runtime.enter();

        let (deletion_queue, fe_worker, be_worker, ex_worker) = DeletionQueue::new(
            Some(storage.clone()),
            harness.conf,
            CancellationToken::new(),
        );

        let mut fe_worker = fe_worker.unwrap();
        let mut be_worker = be_worker.unwrap();
        let mut ex_worker = ex_worker.unwrap();
        let fe_worker_join = runtime.spawn(async move { fe_worker.background().await });
        let be_worker_join = runtime.spawn(async move { be_worker.background().await });
        let ex_worker_join = runtime.spawn(async move {
            drop(ex_worker.background().await);
        });

        Ok(TestSetup {
            runtime,
            _entered_runtime: entered_runtime,
            harness,
            remote_fs_dir,
            storage,
            deletion_queue,
            fe_worker: fe_worker_join,
            be_worker: be_worker_join,
            ex_worker: ex_worker_join,
        })
    }

    // TODO: put this in a common location so that we can share with remote_timeline_client's tests
    fn assert_remote_files(expected: &[&str], remote_path: &Path) {
        let mut expected: Vec<String> = expected.iter().map(|x| String::from(*x)).collect();
        expected.sort();

        let mut found: Vec<String> = Vec::new();
        let dir = match std::fs::read_dir(remote_path) {
            Ok(d) => d,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    if expected.is_empty() {
                        // We are asserting prefix is empty: it is expected that the dir is missing
                        return;
                    } else {
                        assert_eq!(expected, Vec::<String>::new());
                        unreachable!();
                    }
                } else {
                    panic!(
                        "Unexpected error listing {0}: {e}",
                        remote_path.to_string_lossy()
                    );
                }
            }
        };

        for entry in dir.flatten() {
            let entry_name = entry.file_name();
            let fname = entry_name.to_str().unwrap();
            found.push(String::from(fname));
        }
        found.sort();

        assert_eq!(expected, found);
    }

    #[test]
    fn deletion_queue_smoke() -> anyhow::Result<()> {
        // Basic test that the deletion queue processes the deletions we pass into it
        let ctx = setup("deletion_queue_smoke").expect("Failed test setup");
        let client = ctx.deletion_queue.new_client();

        let layer_file_name_1: LayerFileName = "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap();
        let tenant_id = ctx.harness.tenant_id;

        let content: Vec<u8> = "victim1 contents".into();
        let relative_remote_path = ctx
            .harness
            .conf
            .remote_path(&ctx.harness.timeline_path(&TIMELINE_ID))
            .expect("Failed to construct remote path");
        let remote_timeline_path = ctx.remote_fs_dir.join(relative_remote_path.get_path());
        let remote_deletion_prefix = ctx
            .remote_fs_dir
            .join(ctx.harness.conf.remote_deletion_node_prefix());

        // Inject a victim file to remote storage
        info!("Writing");
        std::fs::create_dir_all(&remote_timeline_path)?;
        std::fs::write(
            remote_timeline_path.join(layer_file_name_1.to_string()),
            content,
        )?;
        assert_remote_files(&[&layer_file_name_1.file_name()], &remote_timeline_path);

        // File should still be there after we push it to the queue (we haven't pushed enough to flush anything)
        info!("Pushing");
        ctx.runtime.block_on(client.push_layers(
            tenant_id,
            TIMELINE_ID,
            [layer_file_name_1.clone()].to_vec(),
        ))?;
        assert_remote_files(&[&layer_file_name_1.file_name()], &remote_timeline_path);
        assert_remote_files(&[], &remote_deletion_prefix);

        // File should still be there after we write a deletion list (we haven't pushed enough to execute anything)
        info!("Flushing");
        ctx.runtime.block_on(client.flush())?;
        assert_remote_files(&[&layer_file_name_1.file_name()], &remote_timeline_path);
        assert_remote_files(
            &["0000000000000001-00000000-01.list"],
            &remote_deletion_prefix,
        );

        // File should go away when we execute
        info!("Flush-executing");
        ctx.runtime.block_on(client.flush_execute())?;
        assert_remote_files(&[], &remote_timeline_path);
        assert_remote_files(&["header-00000000-01"], &remote_deletion_prefix);

        // Flushing on an empty queue should succeed immediately, and not write any lists
        info!("Flush-executing on empty");
        ctx.runtime.block_on(client.flush_execute())?;
        assert_remote_files(&["header-00000000-01"], &remote_deletion_prefix);

        Ok(())
    }

    #[test]
    fn deletion_queue_recovery() -> anyhow::Result<()> {
        // Basic test that the deletion queue processes the deletions we pass into it
        let mut ctx = setup("deletion_queue_recovery").expect("Failed test setup");
        let client = ctx.deletion_queue.new_client();

        let layer_file_name_1: LayerFileName = "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap();
        let tenant_id = ctx.harness.tenant_id;

        let content: Vec<u8> = "victim1 contents".into();
        let relative_remote_path = ctx
            .harness
            .conf
            .remote_path(&ctx.harness.timeline_path(&TIMELINE_ID))
            .expect("Failed to construct remote path");
        let remote_timeline_path = ctx.remote_fs_dir.join(relative_remote_path.get_path());
        let remote_deletion_prefix = ctx
            .remote_fs_dir
            .join(ctx.harness.conf.remote_deletion_node_prefix());

        // Inject a file, delete it, and flush to a deletion list
        std::fs::create_dir_all(&remote_timeline_path)?;
        std::fs::write(
            remote_timeline_path.join(layer_file_name_1.to_string()),
            content,
        )?;
        ctx.runtime.block_on(client.push_layers(
            tenant_id,
            TIMELINE_ID,
            [layer_file_name_1.clone()].to_vec(),
        ))?;
        ctx.runtime.block_on(client.flush())?;
        assert_remote_files(
            &["0000000000000001-00000000-01.list"],
            &remote_deletion_prefix,
        );

        // Restart the deletion queue
        drop(client);
        ctx.restart();
        let client = ctx.deletion_queue.new_client();

        // If we have recovered the deletion list properly, then executing after restart should purge it
        info!("Flush-executing");
        ctx.runtime.block_on(client.flush_execute())?;
        assert_remote_files(&[], &remote_timeline_path);
        assert_remote_files(&["header-00000000-01"], &remote_deletion_prefix);
        Ok(())
    }
}

/// A lightweight queue which can issue ordinary DeletionQueueClient objects, but doesn't do any persistence
/// or coalescing, and doesn't actually execute any deletions unless you call pump() to kick it.
#[cfg(test)]
pub mod mock {
    use tracing::info;

    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    pub struct MockDeletionQueue {
        tx: tokio::sync::mpsc::Sender<FrontendQueueMessage>,
        executor_tx: tokio::sync::mpsc::Sender<ExecutorMessage>,
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
            let (executor_tx, mut executor_rx) = tokio::sync::mpsc::channel(16384);

            let executed = Arc::new(AtomicUsize::new(0));
            let executed_bg = executed.clone();

            tokio::spawn(async move {
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

                    // Transform all executor messages to generic frontend messages
                    while let Ok(msg) = executor_rx.try_recv() {
                        match msg {
                            ExecutorMessage::Delete(objects) => {
                                for path in objects {
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
                            ExecutorMessage::Flush(flush_op) => {
                                flush_op.fire();
                            }
                        }
                    }

                    while let Ok(msg) = rx.try_recv() {
                        match msg {
                            FrontendQueueMessage::Delete(op) => {
                                let timeline_path =
                                    conf.timeline_path(&op.tenant_id, &op.timeline_id);

                                let mut objects = op.objects;
                                for layer in op.layers {
                                    let local_path = timeline_path.join(layer.file_name());
                                    let path = match conf.remote_path(&local_path) {
                                        Ok(p) => p,
                                        Err(e) => {
                                            panic!("Can't make a timeline path! {e}");
                                        }
                                    };
                                    objects.push(path);
                                }

                                for path in objects {
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
                            FrontendQueueMessage::Flush(op) => {
                                op.fire();
                            }
                            FrontendQueueMessage::FlushExecute(op) => {
                                // We have already executed all prior deletions because mock does them inline
                                op.fire();
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
                tx,
                tx_pump,
                executor_tx,
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

        pub(crate) fn new_client(&self) -> DeletionQueueClient {
            DeletionQueueClient {
                tx: self.tx.clone(),
                executor_tx: self.executor_tx.clone(),
            }
        }
    }
}
