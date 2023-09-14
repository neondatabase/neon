mod backend;
mod executor;
mod frontend;

use std::collections::HashMap;

use std::sync::Arc;
use std::time::Duration;

use crate::control_plane_client::ControlPlaneGenerationsApi;
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
use utils::lsn::AtomicLsn;
use utils::lsn::Lsn;

use self::backend::BackendQueueWorker;
use self::executor::ExecutorWorker;
use self::frontend::DeletionOp;
use self::frontend::FrontendQueueWorker;
use self::frontend::RecoverOp;
use backend::BackendQueueMessage;
use executor::ExecutorMessage;
use frontend::FrontendQueueMessage;

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
pub struct DeletionQueueWorkers<C>
where
    C: ControlPlaneGenerationsApi + Send + Sync,
{
    frontend: FrontendQueueWorker,
    backend: BackendQueueWorker<C>,
    executor: ExecutorWorker,
}

impl<C> DeletionQueueWorkers<C>
where
    C: ControlPlaneGenerationsApi + Send + Sync + 'static,
{
    pub fn spawn_with(mut self, runtime: &tokio::runtime::Handle) -> tokio::task::JoinHandle<()> {
        let jh_frontend = runtime.spawn(async move {
            self.frontend
                .background()
                .instrument(tracing::info_span!(parent:None, "deletion frontend"))
                .await
        });
        let jh_backend = runtime.spawn(async move {
            self.backend
                .background()
                .instrument(tracing::info_span!(parent:None, "deletion backend"))
                .await
        });
        let jh_executor = runtime.spawn(async move {
            self.executor
                .background()
                .instrument(tracing::info_span!(parent:None, "deletion executor"))
                .await
        });

        runtime.spawn({
            async move {
                jh_frontend.await.expect("error joining frontend worker");
                jh_backend.await.expect("error joining backend worker");
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
    tx: tokio::sync::mpsc::Sender<FrontendQueueMessage>,
    executor_tx: tokio::sync::mpsc::Sender<ExecutorMessage>,

    lsn_table: Arc<std::sync::RwLock<VisibleLsnUpdates>>,
}

mod deletion_list;
use deletion_list::*;

struct PendingLsn {
    projected: Lsn,
    result_slot: Arc<AtomicLsn>,
}

struct TenantLsnState {
    timelines: HashMap<TimelineId, PendingLsn>,

    // In what generation was the most recent update proposed?
    generation: Generation,
}

struct VisibleLsnUpdates {
    tenants: HashMap<TenantId, TenantLsnState>,
}

impl VisibleLsnUpdates {
    fn new() -> Self {
        Self {
            tenants: HashMap::new(),
        }
    }
}

impl std::fmt::Debug for VisibleLsnUpdates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VisibleLsnUpdates({} tenants)", self.tenants.len())
    }
}

#[derive(Error, Debug)]
pub enum DeletionQueueError {
    #[error("Deletion queue unavailable during shutdown")]
    ShuttingDown,
}

impl DeletionQueueClient {
    pub(crate) fn broken() -> Self {
        // Channels whose receivers are immediately dropped.
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let (executor_tx, _executor_rx) = tokio::sync::mpsc::channel(1);
        Self {
            tx,
            executor_tx,
            lsn_table: Arc::new(std::sync::RwLock::new(VisibleLsnUpdates::new())),
        }
    }

    async fn do_push<T>(
        &self,
        queue: &tokio::sync::mpsc::Sender<T>,
        msg: T,
    ) -> Result<(), DeletionQueueError> {
        match queue.send(msg).await {
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

    pub(crate) async fn recover(
        &self,
        attached_tenants: HashMap<TenantId, Generation>,
    ) -> Result<(), DeletionQueueError> {
        self.do_push(
            &self.tx,
            FrontendQueueMessage::Recover(RecoverOp { attached_tenants }),
        )
        .await
    }

    /// When a Timeline wishes to update the remote_consistent_lsn that it exposes to the outside
    /// world, it must validate its generation number before doing so.  Rather than do this synchronously,
    /// we allow the timeline to publish updates at will via this API, and then read back what LSN was most
    /// recently validated separately.
    ///
    /// In this function we publish the LSN to the `projected` field of the timeline's entry in the VisibleLsnUpdates.  The
    /// backend will later wake up and notice that the tenant's generation requires validation.
    pub(crate) async fn update_remote_consistent_lsn(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        current_generation: Generation,
        lsn: Lsn,
        result_slot: Arc<AtomicLsn>,
    ) {
        let mut locked = self
            .lsn_table
            .write()
            .expect("Lock should never be poisoned");

        let tenant_entry = locked.tenants.entry(tenant_id).or_insert(TenantLsnState {
            timelines: HashMap::new(),
            generation: current_generation,
        });

        if tenant_entry.generation != current_generation {
            // Generation might have changed if we were detached and then re-attached: in this case,
            // state from the previous generation cannot be trusted.
            tenant_entry.timelines.clear();
            tenant_entry.generation = current_generation;
        }

        tenant_entry.timelines.insert(
            timeline_id,
            PendingLsn {
                projected: lsn,
                result_slot,
            },
        );
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
        if current_generation.is_none() {
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

        DELETION_QUEUE_SUBMITTED.inc_by(layers.len() as u64);
        self.do_push(
            &self.tx,
            FrontendQueueMessage::Delete(DeletionOp {
                tenant_id,
                timeline_id,
                layers,
                generation: current_generation,
                objects: Vec::new(),
            }),
        )
        .await
    }

    async fn do_flush<T>(
        &self,
        queue: &tokio::sync::mpsc::Sender<T>,
        msg: T,
        rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), DeletionQueueError> {
        self.do_push(queue, msg).await?;
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
        self.do_flush(&self.tx, FrontendQueueMessage::Flush(FlushOp { tx }), rx)
            .await
    }

    // Wait until all previous deletions are executed
    pub(crate) async fn flush_execute(&self) -> Result<(), DeletionQueueError> {
        debug!("flush_execute: flushing to deletion lists...");
        // Flush any buffered work to deletion lists
        self.flush().await?;

        // Flush the backend into the executor of deletion lists
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        debug!("flush_execute: flushing backend...");
        self.do_flush(
            &self.tx,
            FrontendQueueMessage::FlushExecute(FlushOp { tx }),
            rx,
        )
        .await?;
        debug!("flush_execute: finished flushing backend...");

        // Flush any immediate-mode deletions (the above backend flush will only flush
        // the executor if deletions had flowed through the backend)
        debug!("flush_execute: flushing execution...");
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        self.do_flush(
            &self.executor_tx,
            ExecutorMessage::Flush(FlushOp { tx }),
            rx,
        )
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
    pub fn new<C>(
        remote_storage: Option<GenericRemoteStorage>,
        control_plane_client: Option<C>,
        conf: &'static PageServerConf,
    ) -> (Self, Option<DeletionQueueWorkers<C>>)
    where
        C: ControlPlaneGenerationsApi + Send + Sync,
    {
        // Deep channel: it consumes deletions from all timelines and we do not want to block them
        let (tx, rx) = tokio::sync::mpsc::channel(16384);

        // Shallow channel: it carries DeletionLists which each contain up to thousands of deletions
        let (backend_tx, backend_rx) = tokio::sync::mpsc::channel(16);

        // Shallow channel: it carries lists of paths, and we expect the main queueing to
        // happen in the backend (persistent), not in this queue.
        let (executor_tx, executor_rx) = tokio::sync::mpsc::channel(16);

        let lsn_table = Arc::new(std::sync::RwLock::new(VisibleLsnUpdates::new()));

        // The deletion queue has an independent cancellation token to
        // the general pageserver shutdown token, because it stays alive a bit
        // longer to flush after Tenants have all been torn down.
        let cancel = CancellationToken::new();

        let remote_storage = match remote_storage {
            None => {
                return (
                    Self {
                        client: DeletionQueueClient {
                            tx,
                            executor_tx,
                            lsn_table: lsn_table.clone(),
                        },
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
                    tx,
                    executor_tx: executor_tx.clone(),
                    lsn_table: lsn_table.clone(),
                },
                cancel: cancel.clone(),
            },
            Some(DeletionQueueWorkers {
                frontend: FrontendQueueWorker::new(conf, rx, backend_tx, cancel.clone()),
                backend: BackendQueueWorker::new(
                    conf,
                    backend_rx,
                    executor_tx,
                    control_plane_client,
                    lsn_table.clone(),
                    cancel.clone(),
                ),
                executor: ExecutorWorker::new(remote_storage, executor_rx, cancel.clone()),
            }),
        )
    }

    pub async fn shutdown(&mut self, timeout: Duration) {
        self.cancel.cancel();

        match tokio::time::timeout(timeout, self.client.flush()).await {
            Ok(flush_r) => {
                match flush_r {
                    Ok(()) => {
                        tracing::info!("Deletion queue flushed successfully on shutdown")
                    }
                    Err(e) => {
                        match e {
                            DeletionQueueError::ShuttingDown => {
                                // This is not harmful for correctness, but is unexpected: the deletion
                                // queue's workers should stay alive as long as there are any client handles instantiated.
                                tracing::warn!("Deletion queue stopped prematurely");
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Timed out flushing deletion queue on shutdown ({e})")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use hex_literal::hex;
    use std::{
        io::ErrorKind,
        path::{Path, PathBuf},
        time::Duration,
    };
    use tracing::info;

    use remote_storage::{RemoteStorageConfig, RemoteStorageKind};
    use tokio::task::JoinHandle;

    use crate::{
        repository::Key,
        tenant::{
            harness::TenantHarness, remote_timeline_client::remote_timeline_path,
            storage_layer::DeltaFileName,
        },
    };

    use super::*;
    pub const TIMELINE_ID: TimelineId =
        TimelineId::from_array(hex!("11223344556677881122334455667788"));

    pub const EXAMPLE_LAYER_NAME: LayerFileName = LayerFileName::Delta(DeltaFileName {
        key_range: Key::from_i128(0x0)..Key::from_i128(0xFFFFFFFFFFFFFFFF),
        lsn_range: Lsn(0x00000000016B59D8)..Lsn(0x00000000016B5A51),
    });

    // When you need a second layer in a test.
    pub const EXAMPLE_LAYER_NAME_ALT: LayerFileName = LayerFileName::Delta(DeltaFileName {
        key_range: Key::from_i128(0x0)..Key::from_i128(0xFFFFFFFFFFFFFFFF),
        lsn_range: Lsn(0x00000000016B5A51)..Lsn(0x00000000016B5A61),
    });

    struct TestSetup {
        harness: TenantHarness,
        remote_fs_dir: PathBuf,
        storage: GenericRemoteStorage,
        mock_control_plane: MockControlPlane,
        deletion_queue: DeletionQueue,
        worker_join: JoinHandle<()>,
    }

    impl TestSetup {
        /// Simulate a pageserver restart by destroying and recreating the deletion queue
        async fn restart(&mut self) {
            let (deletion_queue, workers) = DeletionQueue::new(
                Some(self.storage.clone()),
                Some(self.mock_control_plane.clone()),
                self.harness.conf,
            );

            tracing::debug!("Spawning worker for new queue queue");
            let worker_join = workers
                .unwrap()
                .spawn_with(&tokio::runtime::Handle::current());

            let old_worker_join = std::mem::replace(&mut self.worker_join, worker_join);
            let old_deletion_queue = std::mem::replace(&mut self.deletion_queue, deletion_queue);

            tracing::debug!("Joining worker from previous queue");
            old_deletion_queue.cancel.cancel();
            old_worker_join
                .await
                .expect("Failed to join workers for previous deletion queue");
        }

        fn set_latest_generation(&self, gen: Generation) {
            let tenant_id = self.harness.tenant_id;
            self.mock_control_plane
                .latest_generation
                .lock()
                .unwrap()
                .insert(tenant_id, gen);
        }

        /// Returns remote layer file name, suitable for use in assert_remote_files
        fn write_remote_layer(
            &self,
            file_name: LayerFileName,
            gen: Generation,
        ) -> anyhow::Result<String> {
            let tenant_id = self.harness.tenant_id;
            let relative_remote_path = remote_timeline_path(&tenant_id, &TIMELINE_ID);
            let remote_timeline_path = self.remote_fs_dir.join(relative_remote_path.get_path());
            std::fs::create_dir_all(&remote_timeline_path)?;
            let remote_layer_file_name = format!("{}{}", file_name, gen.get_suffix());

            let content: Vec<u8> = format!("placeholder contents of {file_name}").into();

            std::fs::write(
                remote_timeline_path.join(remote_layer_file_name.clone()),
                content,
            )?;

            Ok(remote_layer_file_name)
        }
    }

    #[derive(Debug, Clone)]
    struct MockControlPlane {
        pub latest_generation: std::sync::Arc<std::sync::Mutex<HashMap<TenantId, Generation>>>,
    }

    impl MockControlPlane {
        fn new() -> Self {
            Self {
                latest_generation: Arc::new(std::sync::Mutex::new(HashMap::new())),
            }
        }
    }

    unsafe impl Send for MockControlPlane {}
    unsafe impl Sync for MockControlPlane {}

    #[async_trait::async_trait]
    impl ControlPlaneGenerationsApi for MockControlPlane {
        #[allow(clippy::diverging_sub_expression)] // False positive via async_trait
        async fn re_attach(&self) -> anyhow::Result<HashMap<TenantId, Generation>> {
            unimplemented!()
        }
        async fn validate(
            &self,
            tenants: Vec<(TenantId, Generation)>,
        ) -> anyhow::Result<HashMap<TenantId, bool>> {
            let mut result = HashMap::new();

            let latest_generation = self.latest_generation.lock().unwrap();

            for (tenant_id, generation) in tenants {
                if let Some(latest) = latest_generation.get(&tenant_id) {
                    result.insert(tenant_id, *latest == generation);
                }
            }

            Ok(result)
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

        let mock_control_plane = MockControlPlane::new();

        let (deletion_queue, worker) = DeletionQueue::new(
            Some(storage.clone()),
            Some(mock_control_plane.clone()),
            harness.conf,
        );

        let worker = worker.unwrap();
        let worker_join = worker.spawn_with(&tokio::runtime::Handle::current());

        Ok(TestSetup {
            harness,
            remote_fs_dir,
            storage,
            mock_control_plane,
            deletion_queue,
            worker_join,
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
                        "Unexpected error listing {}: {e}",
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

    fn assert_local_files(expected: &[&str], directory: &Path) {
        let dir = match std::fs::read_dir(directory) {
            Ok(d) => d,
            Err(_) => {
                assert_eq!(expected, &Vec::<String>::new());
                return;
            }
        };
        let mut found = Vec::new();
        for dentry in dir {
            let dentry = dentry.unwrap();
            let file_name = dentry.file_name();
            let file_name_str = file_name.to_string_lossy();
            found.push(file_name_str.to_string());
        }
        found.sort();
        assert_eq!(expected, found);
    }

    #[tokio::test]
    async fn deletion_queue_smoke() -> anyhow::Result<()> {
        // Basic test that the deletion queue processes the deletions we pass into it
        let ctx = setup("deletion_queue_smoke").expect("Failed test setup");
        let client = ctx.deletion_queue.new_client();

        let layer_file_name_1: LayerFileName = "000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__00000000016B59D8-00000000016B5A51".parse().unwrap();
        let tenant_id = ctx.harness.tenant_id;

        let content: Vec<u8> = "victim1 contents".into();
        let relative_remote_path = remote_timeline_path(&tenant_id, &TIMELINE_ID);
        let remote_timeline_path = ctx.remote_fs_dir.join(relative_remote_path.get_path());
        let deletion_prefix = ctx.harness.conf.deletion_prefix();

        // Exercise the distinction between the generation of the layers
        // we delete, and the generation of the running Tenant.
        let layer_generation = Generation::new(0xdeadbeef);
        let now_generation = Generation::new(0xfeedbeef);

        let remote_layer_file_name_1 =
            format!("{}{}", layer_file_name_1, layer_generation.get_suffix());

        // Set mock control plane state to valid for our generation
        ctx.set_latest_generation(now_generation);

        // Inject a victim file to remote storage
        info!("Writing");
        std::fs::create_dir_all(&remote_timeline_path)?;
        std::fs::write(
            remote_timeline_path.join(remote_layer_file_name_1.clone()),
            content,
        )?;
        assert_remote_files(&[&remote_layer_file_name_1], &remote_timeline_path);

        // File should still be there after we push it to the queue (we haven't pushed enough to flush anything)
        info!("Pushing");
        client
            .push_layers(
                tenant_id,
                TIMELINE_ID,
                now_generation,
                [(layer_file_name_1.clone(), layer_generation)].to_vec(),
            )
            .await?;
        assert_remote_files(&[&remote_layer_file_name_1], &remote_timeline_path);

        assert_local_files(&[], &deletion_prefix);

        // File should still be there after we write a deletion list (we haven't pushed enough to execute anything)
        info!("Flushing");
        client.flush().await?;
        assert_remote_files(&[&remote_layer_file_name_1], &remote_timeline_path);
        assert_local_files(&["0000000000000001-01.list"], &deletion_prefix);

        // File should go away when we execute
        info!("Flush-executing");
        client.flush_execute().await?;
        assert_remote_files(&[], &remote_timeline_path);
        assert_local_files(&["header-01"], &deletion_prefix);

        // Flushing on an empty queue should succeed immediately, and not write any lists
        info!("Flush-executing on empty");
        client.flush_execute().await?;
        assert_local_files(&["header-01"], &deletion_prefix);

        Ok(())
    }

    #[tokio::test]
    async fn deletion_queue_validation() -> anyhow::Result<()> {
        let ctx = setup("deletion_queue_validation").expect("Failed test setup");
        let client = ctx.deletion_queue.new_client();

        // Generation that the control plane thinks is current
        let latest_generation = Generation::new(0xdeadbeef);
        // Generation that our DeletionQueue thinks the tenant is running with
        let stale_generation = latest_generation.previous();
        // Generation that our example layer file was written with
        let layer_generation = stale_generation.previous();

        ctx.set_latest_generation(latest_generation);

        let tenant_id = ctx.harness.tenant_id;
        let relative_remote_path = remote_timeline_path(&tenant_id, &TIMELINE_ID);
        let remote_timeline_path = ctx.remote_fs_dir.join(relative_remote_path.get_path());

        // Initial state: a remote layer exists
        let remote_layer_name = ctx.write_remote_layer(EXAMPLE_LAYER_NAME, layer_generation)?;
        assert_remote_files(&[&remote_layer_name], &remote_timeline_path);

        tracing::debug!("Pushing...");
        client
            .push_layers(
                tenant_id,
                TIMELINE_ID,
                stale_generation,
                [(EXAMPLE_LAYER_NAME.clone(), layer_generation)].to_vec(),
            )
            .await?;

        // We enqueued the operation in a stale generation: it should have failed validation
        tracing::debug!("Flushing...");
        tokio::time::timeout(Duration::from_secs(5), client.flush_execute()).await??;
        assert_remote_files(&[&remote_layer_name], &remote_timeline_path);

        tracing::debug!("Pushing...");
        client
            .push_layers(
                tenant_id,
                TIMELINE_ID,
                latest_generation,
                [(EXAMPLE_LAYER_NAME.clone(), layer_generation)].to_vec(),
            )
            .await?;

        // We enqueued the operation in a fresh generation: it should have passed validation
        tracing::debug!("Flushing...");
        tokio::time::timeout(Duration::from_secs(5), client.flush_execute()).await??;
        assert_remote_files(&[], &remote_timeline_path);

        Ok(())
    }

    #[tokio::test]
    async fn deletion_queue_recovery() -> anyhow::Result<()> {
        // Basic test that the deletion queue processes the deletions we pass into it
        let mut ctx = setup("deletion_queue_recovery").expect("Failed test setup");
        let client = ctx.deletion_queue.new_client();

        let tenant_id = ctx.harness.tenant_id;

        let relative_remote_path = remote_timeline_path(&tenant_id, &TIMELINE_ID);
        let remote_timeline_path = ctx.remote_fs_dir.join(relative_remote_path.get_path());
        let deletion_prefix = ctx.harness.conf.deletion_prefix();

        let layer_generation = Generation::new(0xdeadbeef);
        let now_generation = Generation::new(0xfeedbeef);

        // Inject a deletion in the generation before generation_now: after restart,
        // this deletion should _not_ get executed (only the immediately previous
        // generation gets that treatment)
        let remote_layer_file_name_historical =
            ctx.write_remote_layer(EXAMPLE_LAYER_NAME, layer_generation)?;
        client
            .push_layers(
                tenant_id,
                TIMELINE_ID,
                now_generation.previous(),
                [(EXAMPLE_LAYER_NAME.clone(), layer_generation)].to_vec(),
            )
            .await?;

        // Inject a deletion in the generation before generation_now: after restart,
        // this deletion should get executed, because we execute deletions in the
        // immediately previous generation on the same node.
        let remote_layer_file_name_previous =
            ctx.write_remote_layer(EXAMPLE_LAYER_NAME_ALT, layer_generation)?;
        client
            .push_layers(
                tenant_id,
                TIMELINE_ID,
                now_generation,
                [(EXAMPLE_LAYER_NAME_ALT.clone(), layer_generation)].to_vec(),
            )
            .await?;

        client.flush().await?;
        assert_remote_files(
            &[
                &remote_layer_file_name_historical,
                &remote_layer_file_name_previous,
            ],
            &remote_timeline_path,
        );

        // Different generatinos for the same tenant will cause two separate
        // deletion lists to be emitted.
        assert_local_files(
            &["0000000000000001-01.list", "0000000000000002-01.list"],
            &deletion_prefix,
        );

        // Simulate a node restart: the latest generation advances
        let now_generation = now_generation.next();
        ctx.set_latest_generation(now_generation);

        // Restart the deletion queue
        drop(client);
        ctx.restart().await;
        let client = ctx.deletion_queue.new_client();
        client
            .recover(HashMap::from([(tenant_id, now_generation)]))
            .await?;

        info!("Flush-executing");
        client.flush_execute().await?;
        // The deletion from immediately prior generation was executed, the one from
        // an older generation was not.
        assert_remote_files(&[&remote_layer_file_name_historical], &remote_timeline_path);
        Ok(())
    }
}

/// A lightweight queue which can issue ordinary DeletionQueueClient objects, but doesn't do any persistence
/// or coalescing, and doesn't actually execute any deletions unless you call pump() to kick it.
#[cfg(test)]
pub mod mock {
    use tracing::info;

    use crate::tenant::remote_timeline_client::remote_layer_path;

    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    pub struct ConsumerState {
        rx: tokio::sync::mpsc::Receiver<FrontendQueueMessage>,
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

            while let Ok(msg) = self.rx.try_recv() {
                match msg {
                    FrontendQueueMessage::Delete(op) => {
                        let mut objects = op.objects;
                        for (layer, generation) in op.layers {
                            objects.push(remote_layer_path(
                                &op.tenant_id,
                                &op.timeline_id,
                                &layer,
                                generation,
                            ));
                        }

                        for path in objects {
                            info!("Executing deletion {path}");
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
                    FrontendQueueMessage::Flush(op) => {
                        op.fire();
                    }
                    FrontendQueueMessage::FlushExecute(op) => {
                        // We have already executed all prior deletions because mock does them inline
                        op.fire();
                    }
                    FrontendQueueMessage::Recover(_) => {
                        // no-op in mock
                    }
                }
                info!("All pending deletions have been executed");
            }

            executed
        }
    }

    pub struct MockDeletionQueue {
        tx: tokio::sync::mpsc::Sender<FrontendQueueMessage>,
        executor_tx: tokio::sync::mpsc::Sender<ExecutorMessage>,
        executed: Arc<AtomicUsize>,
        remote_storage: Option<GenericRemoteStorage>,
        consumer: std::sync::Mutex<ConsumerState>,
        lsn_table: Arc<std::sync::RwLock<VisibleLsnUpdates>>,
    }

    impl MockDeletionQueue {
        pub fn new(remote_storage: Option<GenericRemoteStorage>) -> Self {
            let (tx, rx) = tokio::sync::mpsc::channel(16384);
            let (executor_tx, executor_rx) = tokio::sync::mpsc::channel(16384);

            let executed = Arc::new(AtomicUsize::new(0));

            Self {
                tx,
                executor_tx,
                executed,
                remote_storage,
                consumer: std::sync::Mutex::new(ConsumerState { rx, executor_rx }),
                lsn_table: Arc::new(std::sync::RwLock::new(VisibleLsnUpdates::new())),
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
                tx: self.tx.clone(),
                executor_tx: self.executor_tx.clone(),
                lsn_table: self.lsn_table.clone(),
            }
        }
    }
}
