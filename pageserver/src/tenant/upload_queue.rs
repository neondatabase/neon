use super::storage_layer::LayerFileName;
use super::storage_layer::{Layer, ResidentLayer};
use crate::tenant::metadata::TimelineMetadata;
use crate::tenant::remote_timeline_client::index::IndexPart;
use crate::tenant::remote_timeline_client::index::LayerFileMetadata;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;

use chrono::NaiveDateTime;
use std::sync::Arc;
use tracing::info;
use utils::lsn::AtomicLsn;

use std::sync::atomic::AtomicU32;
use utils::lsn::Lsn;

#[cfg(feature = "testing")]
use utils::generation::Generation;

// clippy warns that Uninitialized is much smaller than Initialized, which wastes
// memory for Uninitialized variants. Doesn't matter in practice, there are not
// that many upload queues in a running pageserver, and most of them are initialized
// anyway.
#[allow(clippy::large_enum_variant)]
pub(super) enum UploadQueue {
    Uninitialized,
    Initialized(UploadQueueInitialized),
    Stopped(UploadQueueStopped),
}

impl UploadQueue {
    pub fn as_str(&self) -> &'static str {
        match self {
            UploadQueue::Uninitialized => "Uninitialized",
            UploadQueue::Initialized(_) => "Initialized",
            UploadQueue::Stopped(_) => "Stopped",
        }
    }
}

/// This keeps track of queued and in-progress tasks.
pub(crate) struct UploadQueueInitialized {
    /// Counter to assign task IDs
    pub(crate) task_counter: u64,

    /// All layer files stored in the remote storage, taking into account all
    /// in-progress and queued operations
    pub(crate) latest_files: HashMap<LayerFileName, LayerFileMetadata>,

    /// How many file uploads or deletions been scheduled, since the
    /// last (scheduling of) metadata index upload?
    pub(crate) latest_files_changes_since_metadata_upload_scheduled: u64,

    /// Metadata stored in the remote storage, taking into account all
    /// in-progress and queued operations.
    /// DANGER: do not return to outside world, e.g., safekeepers.
    pub(crate) latest_metadata: TimelineMetadata,

    /// `disk_consistent_lsn` from the last metadata file that was successfully
    /// uploaded. `Lsn(0)` if nothing was uploaded yet.
    /// Unlike `latest_files` or `latest_metadata`, this value is never ahead.
    /// Safekeeper can rely on it to make decisions for WAL storage.
    ///
    /// visible_remote_consistent_lsn is only updated after our generation has been validated with
    /// the control plane (unlesss a timeline's generation is None, in which case
    /// we skip validation)
    pub(crate) projected_remote_consistent_lsn: Option<Lsn>,
    pub(crate) visible_remote_consistent_lsn: Arc<AtomicLsn>,

    // Breakdown of different kinds of tasks currently in-progress
    pub(crate) num_inprogress_layer_uploads: usize,
    pub(crate) num_inprogress_layer_copies: usize,
    pub(crate) num_inprogress_metadata_uploads: usize,
    pub(crate) num_inprogress_deletions: usize,

    /// Tasks that are currently in-progress. In-progress means that a tokio Task
    /// has been launched for it. An in-progress task can be busy uploading, but it can
    /// also be waiting on the `concurrency_limiter` Semaphore in S3Bucket, or it can
    /// be waiting for retry in `exponential_backoff`.
    pub(crate) inprogress_tasks: HashMap<u64, Arc<UploadTask>>,

    /// Queued operations that have not been launched yet. They might depend on previous
    /// tasks to finish. For example, metadata upload cannot be performed before all
    /// preceding layer file uploads have completed.
    pub(crate) queued_operations: VecDeque<UploadOp>,

    /// Files which have been unlinked but not yet had scheduled a deletion for. Only kept around
    /// for error logging.
    ///
    /// Putting this behind a testing feature to catch problems in tests, but assuming we could have a
    /// bug causing leaks, then it's better to not leave this enabled for production builds.
    #[cfg(feature = "testing")]
    pub(crate) dangling_files: HashMap<LayerFileName, Generation>,

    /// Set to true when we have inserted the `UploadOp::Shutdown` into the `inprogress_tasks`.
    pub(crate) shutting_down: bool,

    /// Permitless semaphore on which any number of `RemoteTimelineClient::shutdown` futures can
    /// wait on until one of them stops the queue. The semaphore is closed when
    /// `RemoteTimelineClient::launch_queued_tasks` encounters `UploadOp::Shutdown`.
    pub(crate) shutdown_ready: Arc<tokio::sync::Semaphore>,
}

impl UploadQueueInitialized {
    pub(super) fn no_pending_work(&self) -> bool {
        self.inprogress_tasks.is_empty() && self.queued_operations.is_empty()
    }

    pub(super) fn get_last_remote_consistent_lsn_visible(&self) -> Lsn {
        self.visible_remote_consistent_lsn.load()
    }

    pub(super) fn get_last_remote_consistent_lsn_projected(&self) -> Option<Lsn> {
        self.projected_remote_consistent_lsn
    }
}

#[derive(Clone, Copy)]
pub(super) enum SetDeletedFlagProgress {
    NotRunning,
    InProgress(NaiveDateTime),
    Successful(NaiveDateTime),
}

pub(super) struct UploadQueueStopped {
    pub(super) upload_queue_for_deletion: UploadQueueInitialized,
    pub(super) deleted_at: SetDeletedFlagProgress,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum NotInitialized {
    #[error("queue is in state Uninitialized")]
    Uninitialized,
    #[error("queue is in state Stopped")]
    Stopped,
    #[error("queue is shutting down")]
    ShuttingDown,
}

impl NotInitialized {
    pub(crate) fn is_stopping(&self) -> bool {
        use NotInitialized::*;
        match self {
            Uninitialized => false,
            Stopped => true,
            ShuttingDown => true,
        }
    }
}

impl UploadQueue {
    pub(crate) fn initialize_empty_remote(
        &mut self,
        metadata: &TimelineMetadata,
    ) -> anyhow::Result<&mut UploadQueueInitialized> {
        match self {
            UploadQueue::Uninitialized => (),
            UploadQueue::Initialized(_) | UploadQueue::Stopped(_) => {
                anyhow::bail!("already initialized, state {}", self.as_str())
            }
        }

        info!("initializing upload queue for empty remote");

        let state = UploadQueueInitialized {
            // As described in the doc comment, it's ok for `latest_files` and `latest_metadata` to be ahead.
            latest_files: HashMap::new(),
            latest_files_changes_since_metadata_upload_scheduled: 0,
            latest_metadata: metadata.clone(),
            projected_remote_consistent_lsn: None,
            visible_remote_consistent_lsn: Arc::new(AtomicLsn::new(0)),
            // what follows are boring default initializations
            task_counter: 0,
            num_inprogress_layer_uploads: 0,
            num_inprogress_layer_copies: 0,
            num_inprogress_metadata_uploads: 0,
            num_inprogress_deletions: 0,
            inprogress_tasks: HashMap::new(),
            queued_operations: VecDeque::new(),
            #[cfg(feature = "testing")]
            dangling_files: HashMap::new(),
            shutting_down: false,
            shutdown_ready: Arc::new(tokio::sync::Semaphore::new(0)),
        };

        *self = UploadQueue::Initialized(state);
        Ok(self.initialized_mut().expect("we just set it"))
    }

    pub(crate) fn initialize_with_current_remote_index_part(
        &mut self,
        index_part: &IndexPart,
    ) -> anyhow::Result<&mut UploadQueueInitialized> {
        match self {
            UploadQueue::Uninitialized => (),
            UploadQueue::Initialized(_) | UploadQueue::Stopped(_) => {
                anyhow::bail!("already initialized, state {}", self.as_str())
            }
        }

        let mut files = HashMap::with_capacity(index_part.layer_metadata.len());
        for (layer_name, layer_metadata) in &index_part.layer_metadata {
            files.insert(
                layer_name.to_owned(),
                LayerFileMetadata::from(layer_metadata),
            );
        }

        info!(
            "initializing upload queue with remote index_part.disk_consistent_lsn: {}",
            index_part.metadata.disk_consistent_lsn()
        );

        let state = UploadQueueInitialized {
            latest_files: files,
            latest_files_changes_since_metadata_upload_scheduled: 0,
            latest_metadata: index_part.metadata.clone(),
            projected_remote_consistent_lsn: Some(index_part.metadata.disk_consistent_lsn()),
            visible_remote_consistent_lsn: Arc::new(
                index_part.metadata.disk_consistent_lsn().into(),
            ),
            // what follows are boring default initializations
            task_counter: 0,
            num_inprogress_layer_uploads: 0,
            num_inprogress_layer_copies: 0,
            num_inprogress_metadata_uploads: 0,
            num_inprogress_deletions: 0,
            inprogress_tasks: HashMap::new(),
            queued_operations: VecDeque::new(),
            #[cfg(feature = "testing")]
            dangling_files: HashMap::new(),
            shutting_down: false,
            shutdown_ready: Arc::new(tokio::sync::Semaphore::new(0)),
        };

        *self = UploadQueue::Initialized(state);
        Ok(self.initialized_mut().expect("we just set it"))
    }

    pub(crate) fn initialized_mut(&mut self) -> anyhow::Result<&mut UploadQueueInitialized> {
        use UploadQueue::*;
        match self {
            Uninitialized => Err(NotInitialized::Uninitialized.into()),
            Initialized(x) => {
                if x.shutting_down {
                    Err(NotInitialized::ShuttingDown.into())
                } else {
                    Ok(x)
                }
            }
            Stopped(_) => Err(NotInitialized::Stopped.into()),
        }
    }

    pub(crate) fn stopped_mut(&mut self) -> anyhow::Result<&mut UploadQueueStopped> {
        match self {
            UploadQueue::Initialized(_) | UploadQueue::Uninitialized => {
                anyhow::bail!("queue is in state {}", self.as_str())
            }
            UploadQueue::Stopped(stopped) => Ok(stopped),
        }
    }
}

/// An in-progress upload or delete task.
#[derive(Debug)]
pub(crate) struct UploadTask {
    /// Unique ID of this task. Used as the key in `inprogress_tasks` above.
    pub(crate) task_id: u64,
    pub(crate) retries: AtomicU32,

    pub(crate) op: UploadOp,
}

/// A deletion of some layers within the lifetime of a timeline.  This is not used
/// for timeline deletion, which skips this queue and goes directly to DeletionQueue.
#[derive(Debug)]
pub(crate) struct Delete {
    pub(crate) layers: Vec<(LayerFileName, LayerFileMetadata)>,
}

#[derive(Debug)]
pub(crate) enum UploadOp {
    /// Upload a layer file
    UploadLayer(ResidentLayer, LayerFileMetadata),

    /// Upload the metadata file
    UploadMetadata(IndexPart, Lsn),

    /// Copy layer from another timeline
    AdoptLayer { adopted: Layer, owned: Layer },

    /// Delete layer files
    Delete(Delete),

    /// Barrier. When the barrier operation is reached,
    Barrier(tokio::sync::watch::Sender<()>),

    /// Shutdown; upon encountering this operation no new operations will be spawned, otherwise
    /// this is the same as a Barrier.
    Shutdown,
}

impl std::fmt::Display for UploadOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            UploadOp::UploadLayer(layer, metadata) => {
                write!(
                    f,
                    "UploadLayer({}, size={:?}, gen={:?})",
                    layer,
                    metadata.file_size(),
                    metadata.generation
                )
            }
            UploadOp::UploadMetadata(_, lsn) => {
                write!(f, "UploadMetadata(lsn: {})", lsn)
            }
            UploadOp::AdoptLayer { adopted, owned } => {
                let m = owned.metadata();
                write!(
                    f,
                    "AdoptLayer({adopted}, size={:?}, gen={:?}=>{:?})",
                    m.file_size(),
                    adopted.metadata().generation,
                    m.generation,
                )
            }
            UploadOp::Delete(delete) => {
                write!(f, "Delete({} layers)", delete.layers.len())
            }
            UploadOp::Barrier(_) => write!(f, "Barrier"),
            UploadOp::Shutdown => write!(f, "Shutdown"),
        }
    }
}
