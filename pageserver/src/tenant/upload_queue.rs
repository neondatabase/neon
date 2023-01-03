use crate::metrics::RemoteOpFileKind;

use super::storage_layer::LayerFileName;
use crate::tenant::metadata::TimelineMetadata;
use crate::tenant::remote_timeline_client::index::IndexPart;
use crate::tenant::remote_timeline_client::index::LayerFileMetadata;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;

use std::sync::Arc;
use tracing::info;

use std::sync::atomic::AtomicU32;
use utils::lsn::Lsn;

// clippy warns that Uninitialized is much smaller than Initialized, which wastes
// memory for Uninitialized variants. Doesn't matter in practice, there are not
// that many upload queues in a running pageserver, and most of them are initialized
// anyway.
#[allow(clippy::large_enum_variant)]
pub(crate) enum UploadQueue {
    Uninitialized,
    Initialized(UploadQueueInitialized),
    Stopped(UploadQueueStopped),
}

impl UploadQueue {
    fn as_str(&self) -> &'static str {
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
    pub(crate) last_uploaded_consistent_lsn: Lsn,

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
}

pub(crate) struct UploadQueueStopped {
    pub(crate) last_uploaded_consistent_lsn: Lsn,
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
            // We haven't uploaded anything yet, so, `last_uploaded_consistent_lsn` must be 0 to prevent
            // safekeepers from garbage-collecting anything.
            last_uploaded_consistent_lsn: Lsn(0),
            // what follows are boring default initializations
            task_counter: 0,
            num_inprogress_layer_uploads: 0,
            num_inprogress_metadata_uploads: 0,
            num_inprogress_deletions: 0,
            inprogress_tasks: HashMap::new(),
            queued_operations: VecDeque::new(),
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

        let mut files = HashMap::with_capacity(index_part.timeline_layers.len());
        for layer_name in &index_part.timeline_layers {
            let layer_metadata = index_part
                .layer_metadata
                .get(layer_name)
                .map(LayerFileMetadata::from)
                .unwrap_or(LayerFileMetadata::MISSING);
            files.insert(layer_name.to_owned(), layer_metadata);
        }

        let index_part_metadata = index_part.parse_metadata()?;
        info!(
            "initializing upload queue with remote index_part.disk_consistent_lsn: {}",
            index_part_metadata.disk_consistent_lsn()
        );

        let state = UploadQueueInitialized {
            latest_files: files,
            latest_files_changes_since_metadata_upload_scheduled: 0,
            latest_metadata: index_part_metadata.clone(),
            last_uploaded_consistent_lsn: index_part_metadata.disk_consistent_lsn(),
            // what follows are boring default initializations
            task_counter: 0,
            num_inprogress_layer_uploads: 0,
            num_inprogress_metadata_uploads: 0,
            num_inprogress_deletions: 0,
            inprogress_tasks: HashMap::new(),
            queued_operations: VecDeque::new(),
        };

        *self = UploadQueue::Initialized(state);
        Ok(self.initialized_mut().expect("we just set it"))
    }

    pub(crate) fn initialized_mut(&mut self) -> anyhow::Result<&mut UploadQueueInitialized> {
        match self {
            UploadQueue::Uninitialized | UploadQueue::Stopped(_) => {
                anyhow::bail!("queue is in state {}", self.as_str())
            }
            UploadQueue::Initialized(x) => Ok(x),
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

#[derive(Debug)]
pub(crate) enum UploadOp {
    /// Upload a layer file
    UploadLayer(LayerFileName, LayerFileMetadata),

    /// Upload the metadata file
    UploadMetadata(IndexPart, Lsn),

    /// Delete a file.
    Delete(RemoteOpFileKind, LayerFileName),

    /// Barrier. When the barrier operation is reached,
    Barrier(tokio::sync::watch::Sender<()>),
}

impl std::fmt::Display for UploadOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            UploadOp::UploadLayer(path, metadata) => {
                write!(
                    f,
                    "UploadLayer({}, size={:?})",
                    path.file_name(),
                    metadata.file_size()
                )
            }
            UploadOp::UploadMetadata(_, lsn) => write!(f, "UploadMetadata(lsn: {})", lsn),
            UploadOp::Delete(_, path) => write!(f, "Delete({})", path.file_name()),
            UploadOp::Barrier(_) => write!(f, "Barrier"),
        }
    }
}
