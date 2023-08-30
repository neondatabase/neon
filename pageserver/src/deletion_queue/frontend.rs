use super::BackendQueueMessage;
use super::DeletionHeader;
use super::DeletionList;
use super::FlushOp;

use std::fs::create_dir_all;
use std::time::Duration;

use regex::Regex;
use remote_storage::RemotePath;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::info;
use tracing::warn;
use utils::generation::Generation;
use utils::id::TenantId;
use utils::id::TimelineId;

use crate::config::PageServerConf;
use crate::metrics::DELETION_QUEUE_ERRORS;
use crate::metrics::DELETION_QUEUE_SUBMITTED;
use crate::tenant::remote_timeline_client::remote_layer_path;
use crate::tenant::storage_layer::LayerFileName;

// The number of keys in a DeletionList before we will proactively persist it
// (without reaching a flush deadline).  This aims to deliver objects of the order
// of magnitude 1MB when we are under heavy delete load.
const DELETION_LIST_TARGET_SIZE: usize = 16384;

// Ordinarily, we only flush to DeletionList periodically, to bound the window during
// which we might leak objects from not flushing a DeletionList after
// the objects are already unlinked from timeline metadata.
const FRONTEND_DEFAULT_TIMEOUT: Duration = Duration::from_millis(10000);

// If someone is waiting for a flush to DeletionList, only delay a little to accumulate
// more objects before doing the flush.
const FRONTEND_FLUSHING_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub(super) struct DeletionOp {
    pub(super) tenant_id: TenantId,
    pub(super) timeline_id: TimelineId,
    // `layers` and `objects` are both just lists of objects.  `layers` is used if you do not
    // have a config object handy to project it to a remote key, and need the consuming worker
    // to do it for you.
    pub(super) layers: Vec<(LayerFileName, Generation)>,
    pub(super) objects: Vec<RemotePath>,
}

#[derive(Debug)]
pub(super) enum FrontendQueueMessage {
    Delete(DeletionOp),
    // Wait until all prior deletions make it into a persistent DeletionList
    Flush(FlushOp),
    // Wait until all prior deletions have been executed (i.e. objects are actually deleted)
    FlushExecute(FlushOp),
}

pub struct FrontendQueueWorker {
    conf: &'static PageServerConf,

    // Incoming frontend requests to delete some keys
    rx: tokio::sync::mpsc::Receiver<FrontendQueueMessage>,

    // Outbound requests to the backend to execute deletion lists we have composed.
    tx: tokio::sync::mpsc::Sender<BackendQueueMessage>,

    // The list we are currently building, contains a buffer of keys to delete
    // and our next sequence number
    pending: DeletionList,

    // These FlushOps should fire the next time we flush
    pending_flushes: Vec<FlushOp>,

    // Worker loop is torn down when this fires.
    cancel: CancellationToken,
}

impl FrontendQueueWorker {
    pub(super) fn new(
        conf: &'static PageServerConf,
        rx: tokio::sync::mpsc::Receiver<FrontendQueueMessage>,
        tx: tokio::sync::mpsc::Sender<BackendQueueMessage>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            pending: DeletionList::new(1),
            conf,
            rx,
            tx,
            pending_flushes: Vec::new(),
            cancel,
        }
    }
    async fn upload_pending_list(&mut self) -> anyhow::Result<()> {
        let path = self.conf.deletion_list_path(self.pending.sequence);

        let bytes = serde_json::to_vec(&self.pending).expect("Failed to serialize deletion list");
        tokio::fs::write(&path, &bytes).await?;
        tokio::fs::File::open(&path).await?.sync_all().await?;
        Ok(())
    }

    /// Try to flush `list` to persistent storage
    ///
    /// This does not return errors, because on failure to flush we do not lose
    /// any state: flushing will be retried implicitly on the next deadline
    async fn flush(&mut self) {
        if self.pending.is_empty() {
            for f in self.pending_flushes.drain(..) {
                f.fire();
            }
            return;
        }

        match self.upload_pending_list().await {
            Ok(_) => {
                info!(sequence = self.pending.sequence, "Stored deletion list");

                for f in self.pending_flushes.drain(..) {
                    f.fire();
                }

                let mut onward_list = DeletionList::new(self.pending.sequence);
                std::mem::swap(&mut onward_list.objects, &mut self.pending.objects);

                // We have consumed out of pending: reset it for the next incoming deletions to accumulate there
                self.pending = DeletionList::new(self.pending.sequence + 1);

                if let Err(e) = self.tx.send(BackendQueueMessage::Delete(onward_list)).await {
                    // This is allowed to fail: it will only happen if the backend worker is shut down,
                    // so we can just drop this on the floor.
                    info!("Deletion list dropped, this is normal during shutdown ({e:#})");
                }
            }
            Err(e) => {
                DELETION_QUEUE_ERRORS.with_label_values(&["put_list"]).inc();
                warn!(
                    sequence = self.pending.sequence,
                    "Failed to write deletion list to remote storage, will retry later ({e:#})"
                );
            }
        }
    }

    async fn recover(&mut self) -> Result<(), anyhow::Error> {
        // Load header: this is not required to be present, e.g. when a pageserver first runs
        let header_path = self.conf.deletion_header_path();

        // Synchronous, but we only do it once per process lifetime so it's tolerable
        create_dir_all(&self.conf.deletion_prefix())?;

        let header_bytes = match tokio::fs::read(&header_path).await {
            Ok(h) => Ok(Some(h)),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    debug!(
                        "Deletion header {0} not found, first start?",
                        header_path.display()
                    );
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }?;

        if let Some(header_bytes) = header_bytes {
            if let Some(header) = match serde_json::from_slice::<DeletionHeader>(&header_bytes) {
                Ok(h) => Some(h),
                Err(e) => {
                    warn!(
                        "Failed to deserialize deletion header, ignoring {0}: {e:#}",
                        header_path.display()
                    );
                    // This should never happen unless we make a mistake with our serialization.
                    // Ignoring a deletion header is not consequential for correctnes because all deletions
                    // are ultimately allowed to fail: worst case we leak some objects for the scrubber to clean up.
                    None
                }
            } {
                self.pending.sequence =
                    std::cmp::max(self.pending.sequence, header.last_deleted_list_seq + 1);
            };
        };

        let mut dir = match tokio::fs::read_dir(&self.conf.deletion_prefix()).await {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    "Failed to open deletion list directory {0}: {e:#}",
                    header_path.display()
                );

                // Give up: if we can't read the deletion list directory, we probably can't
                // write lists into it later, so the queue won't work.
                return Err(e.into());
            }
        };

        let list_name_pattern = Regex::new("([a-zA-Z0-9]{16})-([a-zA-Z0-9]{2}).list").unwrap();

        let mut seqs: Vec<u64> = Vec::new();
        while let Some(dentry) = dir.next_entry().await? {
            let file_name = dentry.file_name().to_owned();
            let basename = file_name.to_string_lossy();
            let seq_part = if let Some(m) = list_name_pattern.captures(&basename) {
                m.get(1)
                    .expect("Non optional group should be present")
                    .as_str()
            } else {
                warn!("Unexpected key in deletion queue: {basename}");
                continue;
            };

            let seq: u64 = match u64::from_str_radix(seq_part, 16) {
                Ok(s) => s,
                Err(e) => {
                    warn!("Malformed key '{basename}': {e}");
                    continue;
                }
            };
            seqs.push(seq);
        }
        seqs.sort();

        // Initialize the next sequence number in the frontend based on the maximum of the highest list we see,
        // and the last list that was deleted according to the header.  Combined with writing out the header
        // prior to deletions, this guarnatees no re-use of sequence numbers.
        if let Some(max_list_seq) = seqs.last() {
            self.pending.sequence = std::cmp::max(self.pending.sequence, max_list_seq + 1);
        }

        for s in seqs {
            let list_path = self.conf.deletion_list_path(s);
            let list_bytes = tokio::fs::read(&list_path).await?;

            let deletion_list = match serde_json::from_slice::<DeletionList>(&list_bytes) {
                Ok(l) => l,
                Err(e) => {
                    // Drop the list on the floor: any objects it referenced will be left behind
                    // for scrubbing to clean up.  This should never happen unless we have a serialization bug.
                    warn!(sequence = s, "Failed to deserialize deletion list: {e}");
                    continue;
                }
            };

            // We will drop out of recovery if this fails: it indicates that we are shutting down
            // or the backend has panicked
            DELETION_QUEUE_SUBMITTED.inc_by(deletion_list.len() as u64);
            self.tx
                .send(BackendQueueMessage::Delete(deletion_list))
                .await?;
        }

        info!(next_sequence = self.pending.sequence, "Replay complete");

        Ok(())
    }

    /// This is the front-end ingest, where we bundle up deletion requests into DeletionList
    /// and write them out, for later
    pub async fn background(&mut self) {
        info!("Started deletion frontend worker");

        let mut recovered: bool = false;

        while !self.cancel.is_cancelled() {
            let timeout = if self.pending_flushes.is_empty() {
                FRONTEND_DEFAULT_TIMEOUT
            } else {
                FRONTEND_FLUSHING_TIMEOUT
            };

            let msg = match tokio::time::timeout(timeout, self.rx.recv()).await {
                Ok(Some(msg)) => msg,
                Ok(None) => {
                    // Queue sender destroyed, shutting down
                    break;
                }
                Err(_) => {
                    // Hit deadline, flush.
                    self.flush().await;
                    continue;
                }
            };

            // On first message, do recovery.  This avoids unnecessary recovery very
            // early in startup, and simplifies testing by avoiding a 404 reading the
            // header on every first pageserver startup.
            if !recovered {
                // Before accepting any input from this pageserver lifetime, recover all deletion lists that are in S3
                if let Err(e) = self.recover().await {
                    // This should only happen in truly unrecoverable cases, like the recovery finding that the backend
                    // queue receiver has been dropped.
                    info!("Deletion queue recover aborted, deletion queue will not proceed ({e})");
                    return;
                } else {
                    recovered = true;
                }
            }

            match msg {
                FrontendQueueMessage::Delete(op) => {
                    debug!(
                        "Delete: ingesting {0} layers, {1} other objects",
                        op.layers.len(),
                        op.objects.len()
                    );

                    let mut layer_paths = Vec::new();
                    for (layer, generation) in op.layers {
                        layer_paths.push(remote_layer_path(
                            &op.tenant_id,
                            &op.timeline_id,
                            &layer,
                            generation,
                        ));
                    }

                    self.pending
                        .push(&op.tenant_id, &op.timeline_id, layer_paths);
                    self.pending
                        .push(&op.tenant_id, &op.timeline_id, op.objects);
                }
                FrontendQueueMessage::Flush(op) => {
                    if self.pending.objects.is_empty() {
                        // Execute immediately
                        debug!("Flush: No pending objects, flushing immediately");
                        op.fire()
                    } else {
                        // Execute next time we flush
                        debug!("Flush: adding to pending flush list for next deadline flush");
                        self.pending_flushes.push(op);
                    }
                }
                FrontendQueueMessage::FlushExecute(op) => {
                    debug!("FlushExecute: passing through to backend");
                    // We do not flush to a deletion list here: the client sends a Flush before the FlushExecute
                    if let Err(e) = self.tx.send(BackendQueueMessage::Flush(op)).await {
                        info!("Can't flush, shutting down ({e})");
                        // Caller will get error when their oneshot sender was dropped.
                    }
                }
            }

            if self.pending.objects.len() > DELETION_LIST_TARGET_SIZE
                || !self.pending_flushes.is_empty()
            {
                self.flush().await;
            }
        }
        info!("Deletion queue shut down.");
    }
}
