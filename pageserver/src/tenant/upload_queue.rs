use super::storage_layer::LayerName;
use super::storage_layer::ResidentLayer;
use crate::tenant::metadata::TimelineMetadata;
use crate::tenant::remote_timeline_client::index::IndexPart;
use crate::tenant::remote_timeline_client::index::LayerFileMetadata;
use std::collections::HashSet;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;

use chrono::NaiveDateTime;
use std::sync::Arc;
use tracing::info;
use utils::lsn::AtomicLsn;

use std::sync::atomic::AtomicU32;
use utils::lsn::Lsn;

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

#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum OpType {
    MayReorder,
    FlushDeletion,
}

/// This keeps track of queued and in-progress tasks.
pub(crate) struct UploadQueueInitialized {
    /// Counter to assign task IDs
    pub(crate) task_counter: u64,

    /// The next uploaded index_part.json; assumed to be dirty.
    ///
    /// Should not be read, directly except for layer file updates. Instead you should add a
    /// projected field.
    pub(crate) dirty: IndexPart,

    /// The latest remote persisted IndexPart.
    ///
    /// Each completed metadata upload will update this. The second item is the task_id which last
    /// updated the value, used to ensure we never store an older value over a newer one.
    pub(crate) clean: (IndexPart, Option<u64>),

    /// How many file uploads or deletions been scheduled, since the
    /// last (scheduling of) metadata index upload?
    pub(crate) latest_files_changes_since_metadata_upload_scheduled: u64,

    /// The Lsn is only updated after our generation has been validated with
    /// the control plane (unlesss a timeline's generation is None, in which case
    /// we skip validation)
    pub(crate) visible_remote_consistent_lsn: Arc<AtomicLsn>,

    // Breakdown of different kinds of tasks currently in-progress
    pub(crate) num_inprogress_layer_uploads: usize,
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
    pub(crate) dangling_files: HashMap<LayerName, Generation>,

    /// Ensure we order file operations correctly.
    pub(crate) recently_deleted: HashSet<(LayerName, Generation)>,

    /// Deletions that are blocked by the tenant configuration
    pub(crate) blocked_deletions: Vec<Delete>,

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
        let lsn = self.clean.0.metadata.disk_consistent_lsn();
        self.clean.1.map(|_| lsn)
    }
}

#[derive(Clone, Copy)]
pub(super) enum SetDeletedFlagProgress {
    NotRunning,
    InProgress(NaiveDateTime),
    Successful(NaiveDateTime),
}

pub(super) struct UploadQueueStoppedDeletable {
    pub(super) upload_queue_for_deletion: UploadQueueInitialized,
    pub(super) deleted_at: SetDeletedFlagProgress,
}

pub(super) enum UploadQueueStopped {
    Deletable(UploadQueueStoppedDeletable),
    Uninitialized,
}

#[derive(thiserror::Error, Debug)]
pub enum NotInitialized {
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

        let index_part = IndexPart::empty(metadata.clone());

        let state = UploadQueueInitialized {
            dirty: index_part.clone(),
            clean: (index_part, None),
            latest_files_changes_since_metadata_upload_scheduled: 0,
            visible_remote_consistent_lsn: Arc::new(AtomicLsn::new(0)),
            // what follows are boring default initializations
            task_counter: 0,
            num_inprogress_layer_uploads: 0,
            num_inprogress_metadata_uploads: 0,
            num_inprogress_deletions: 0,
            inprogress_tasks: HashMap::new(),
            queued_operations: VecDeque::new(),
            #[cfg(feature = "testing")]
            dangling_files: HashMap::new(),
            recently_deleted: HashSet::new(),
            blocked_deletions: Vec::new(),
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

        info!(
            "initializing upload queue with remote index_part.disk_consistent_lsn: {}",
            index_part.metadata.disk_consistent_lsn()
        );

        let state = UploadQueueInitialized {
            dirty: index_part.clone(),
            clean: (index_part.clone(), None),
            latest_files_changes_since_metadata_upload_scheduled: 0,
            visible_remote_consistent_lsn: Arc::new(
                index_part.metadata.disk_consistent_lsn().into(),
            ),
            // what follows are boring default initializations
            task_counter: 0,
            num_inprogress_layer_uploads: 0,
            num_inprogress_metadata_uploads: 0,
            num_inprogress_deletions: 0,
            inprogress_tasks: HashMap::new(),
            queued_operations: VecDeque::new(),
            #[cfg(feature = "testing")]
            dangling_files: HashMap::new(),
            recently_deleted: HashSet::new(),
            blocked_deletions: Vec::new(),
            shutting_down: false,
            shutdown_ready: Arc::new(tokio::sync::Semaphore::new(0)),
        };

        *self = UploadQueue::Initialized(state);
        Ok(self.initialized_mut().expect("we just set it"))
    }

    pub(crate) fn initialized_mut(
        &mut self,
    ) -> Result<&mut UploadQueueInitialized, NotInitialized> {
        use UploadQueue::*;
        match self {
            Uninitialized => Err(NotInitialized::Uninitialized),
            Initialized(x) => {
                if x.shutting_down {
                    Err(NotInitialized::ShuttingDown)
                } else {
                    Ok(x)
                }
            }
            Stopped(_) => Err(NotInitialized::Stopped),
        }
    }

    pub(crate) fn stopped_mut(&mut self) -> anyhow::Result<&mut UploadQueueStoppedDeletable> {
        match self {
            UploadQueue::Initialized(_) | UploadQueue::Uninitialized => {
                anyhow::bail!("queue is in state {}", self.as_str())
            }
            UploadQueue::Stopped(UploadQueueStopped::Uninitialized) => {
                anyhow::bail!("queue is in state Stopped(Uninitialized)")
            }
            UploadQueue::Stopped(UploadQueueStopped::Deletable(deletable)) => Ok(deletable),
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
#[derive(Debug, Clone)]
pub(crate) struct Delete {
    pub(crate) layers: Vec<(LayerName, LayerFileMetadata)>,
}

#[derive(Debug)]
pub(crate) enum UploadOp {
    /// Upload a layer file. The last field indicates the last operation for thie file.
    UploadLayer(ResidentLayer, LayerFileMetadata, Option<OpType>),

    /// Upload a index_part.json file
    UploadMetadata {
        /// The next [`UploadQueueInitialized::clean`] after this upload succeeds.
        uploaded: Box<IndexPart>,
    },

    /// Delete layer files
    Delete(Delete),

    /// Barrier. When the barrier operation is reached, the channel is closed.
    Barrier(tokio::sync::watch::Sender<()>),

    /// Shutdown; upon encountering this operation no new operations will be spawned, otherwise
    /// this is the same as a Barrier.
    Shutdown,
}

impl std::fmt::Display for UploadOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            UploadOp::UploadLayer(layer, metadata, mode) => {
                write!(
                    f,
                    "UploadLayer({}, size={:?}, gen={:?}, mode={:?})",
                    layer, metadata.file_size, metadata.generation, mode
                )
            }
            UploadOp::UploadMetadata { uploaded, .. } => {
                write!(
                    f,
                    "UploadMetadata(lsn: {})",
                    uploaded.metadata.disk_consistent_lsn()
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
