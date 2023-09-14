//! The frontend batches deletion requests into DeletionLists and once batched,
//! passes them to the backend for validation & execution.
//!
//! Durability: the frontend persists the DeletionLists to disk, and the backend persists
//! the header file that keeps track of which deletion lists have been validated yet.
//! The split responsiblity is a bit headache-inducing, but, it allows us to persist
//! intention to delete in the frontend, even if the backend is down/full/slow.

use super::BackendQueueMessage;
use super::DeletionHeader;
use super::DeletionList;
use super::FlushOp;

use std::collections::HashMap;
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

    /// The _current_ generation of the Tenant attachment in which we are enqueuing
    /// this deletion.
    pub(super) generation: Generation,
}

#[derive(Debug)]
pub(super) struct RecoverOp {
    pub(super) attached_tenants: HashMap<TenantId, Generation>,
}

#[derive(Debug)]
pub(super) enum FrontendQueueMessage {
    Delete(DeletionOp),
    // Wait until all prior deletions make it into a persistent DeletionList
    Flush(FlushOp),
    // Wait until all prior deletions have been executed (i.e. objects are actually deleted)
    FlushExecute(FlushOp),
    // Call once after re-attaching to control plane, to notify the deletion queue about
    // latest attached generations & load any saved deletion lists from disk.
    Recover(RecoverOp),
}

pub(super) struct FrontendQueueWorker {
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
    // Initially DeletionHeader.validated_sequence is zero.  The place we start our
    // sequence numbers must be higher than that.
    const BASE_SEQUENCE: u64 = 1;

    pub(super) fn new(
        conf: &'static PageServerConf,
        rx: tokio::sync::mpsc::Receiver<FrontendQueueMessage>,
        tx: tokio::sync::mpsc::Sender<BackendQueueMessage>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            pending: DeletionList::new(Self::BASE_SEQUENCE),
            conf,
            rx,
            tx,
            pending_flushes: Vec::new(),
            cancel,
        }
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

        match self.pending.save(self.conf).await {
            Ok(_) => {
                info!(sequence = self.pending.sequence, "Stored deletion list");

                for f in self.pending_flushes.drain(..) {
                    f.fire();
                }

                // the .drain() is like a C++ move constructor; unidiomatics.
                // Use std::mem::replace (together with the line below) instead.
                let onward_list = self.pending.drain();

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
                    "Failed to write deletion list, will retry later ({e:#})"
                );
            }
        }
    }

    /// Load the header, to learn the sequence number up to which deletions
    /// have been validated.  We will apply validated=true to DeletionLists
    /// <= this sequence when loading them.
    ///
    /// It is not an error for the header to not exist: we return None, and
    /// the caller should act as if validated_sequence is 0
    async fn load_validated_sequence(&self) -> Result<Option<u64>, anyhow::Error> {
        let header_path = self.conf.deletion_header_path();
        match tokio::fs::read(&header_path).await {
            Ok(header_bytes) => {
                match serde_json::from_slice::<DeletionHeader>(&header_bytes) {
                    Ok(h) => Ok(Some(h.validated_sequence)),
                    Err(e) => {
                        warn!(
                            "Failed to deserialize deletion header, ignoring {}: {e:#}",
                            header_path.display()
                        );
                        // This should never happen unless we make a mistake with our serialization.
                        // Ignoring a deletion header is not consequential for correctnes because all deletions
                        // are ultimately allowed to fail: worst case we leak some objects for the scrubber to clean up.
                        Ok(None)
                    }
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    debug!(
                        "Deletion header {} not found, first start?",
                        header_path.display()
                    );
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!(e))
                }
            }
        }
    }

    /// 1. There are no safeguards that this function isn't called more than once.
    /// 2. Why isn't this part of the DeletionQueue::new() function?
    async fn recover(
        &mut self,
        attached_tenants: HashMap<TenantId, Generation>,
    ) -> Result<(), anyhow::Error> {
        debug!(
            "recovering with {} attached tenants",
            attached_tenants.len()
        );

        // Load the header
        let validated_sequence = self.load_validated_sequence().await?.unwrap_or(0);

        // Start our next deletion list from after the last location validated by
        // previous process lifetime, or after the last location found (it is updated
        // below after enumerating the deletion lists)
        // isn't self.pending.sequence always 0 at this point?
        self.pending.sequence = std::cmp::max(self.pending.sequence, validated_sequence + 1);

        let deletion_directory = self.conf.deletion_prefix();
        let mut dir = match tokio::fs::read_dir(&deletion_directory).await {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    "Failed to open deletion list directory {}: {e:#}",
                    deletion_directory.display(),
                );

                // Give up: if we can't read the deletion list directory, we probably can't
                // write lists into it later, so the queue won't work.
                return Err(e.into());
            }
        };

        let list_name_pattern = Regex::new("([a-zA-Z0-9]{16})-([a-zA-Z0-9]{2}).list").unwrap();

        let header_path = self.conf.deletion_header_path();
        let mut seqs: Vec<u64> = Vec::new();
        while let Some(dentry) = dir.next_entry().await? {
            if Some(dentry.file_name().as_os_str()) == header_path.file_name() {
                // Don't try and parse the header's name like a list
                continue;
            }

            let file_name = dentry.file_name().to_owned();
            let basename = file_name.to_string_lossy();
            let seq_part = if let Some(m) = list_name_pattern.captures(&basename) {
                // named capture group would help readability here.
                // Also, I see two capture groups, what is the second one for? version number, right?
                // Would have been self-documenting through named capture groups.
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
            // why doesn't the header simply store the set of DeletionList names / sequence numbers?
            // Then we could save ourselves the directory enumeration above, along with the
            // regex parsing and cmp::max stuff.
            let list_path = self.conf.deletion_list_path(s);

            let list_bytes = tokio::fs::read(&list_path).await?;

            let mut deletion_list = match serde_json::from_slice::<DeletionList>(&list_bytes) {
                Ok(l) => l,
                Err(e) => {
                    // Drop the list on the floor: any objects it referenced will be left behind
                    // for scrubbing to clean up.  This should never happen unless we have a serialization bug.
                    warn!(sequence = s, "Failed to deserialize deletion list: {e}");
                    continue;
                }
            };

            if deletion_list.sequence <= validated_sequence {
                // If the deletion list falls below valid_seq, we may assume that it was
                // already validated the last time this pageserver ran.  Otherwise, we still
                // load it, as it may still contain content valid in this generation.
                deletion_list.validated = true;
            } else {
                // Special case optimization: if a tenant is still attached, and no other
                // generation was issued to another node in the interval while we restarted,
                // then we may treat deletion lists from the previous generation as if they
                // belong to our currently attached generation, and proceed to validate & execute.
                for (tenant_id, tenant_list) in &mut deletion_list.tenants {
                    if let Some(attached_gen) = attached_tenants.get(tenant_id) {
                        if attached_gen.previous() == tenant_list.generation {
                            tenant_list.generation = *attached_gen;
                        }
                    }
                }
            }

            info!(
                validated = deletion_list.validated,
                sequence = deletion_list.sequence,
                "Recovered deletion list"
            );

            // We will drop out of recovery if this fails: it indicates that we are shutting down
            // or the backend has panicked
            // we already counted these before we crashed, mustn't count them again
            DELETION_QUEUE_SUBMITTED.inc_by(deletion_list.len() as u64);
            self.tx
                .send(BackendQueueMessage::Delete(deletion_list))
                .await?;
        }

        info!(next_sequence = self.pending.sequence, "Recovery complete");

        Ok(())
    }

    /// This is the front-end ingest, where we bundle up deletion requests into DeletionList
    /// and write them out, for later validation by the backend and execution by the executor.
    pub(super) async fn background(&mut self) {
        info!("Started deletion frontend worker");

        // Synchronous, but we only do it once per process lifetime so it's tolerable
        if let Err(e) = create_dir_all(&self.conf.deletion_prefix()) {
            tracing::error!(
                "Failed to create deletion list directory {}, deletions will not be executed ({e})",
                self.conf.deletion_prefix().display()
            );
            return;
        }

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

            match msg {
                FrontendQueueMessage::Delete(op) => {
                    debug!(
                        "Delete: ingesting {} layers, {} other objects",
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
                    layer_paths.extend(op.objects);

                    if !self.pending.push(
                        &op.tenant_id,
                        &op.timeline_id,
                        op.generation,
                        &mut layer_paths,
                    ) {
                        self.flush().await;
                        let retry_succeeded = self.pending.push(
                            &op.tenant_id,
                            &op.timeline_id,
                            op.generation,
                            &mut layer_paths,
                        );
                        if !retry_succeeded {
                            // Unexpected: after we flush, we should have
                            // drained self.pending, so a conflict on
                            // generation numbers should be impossible.
                            // Wondering whether we should have a counter metric that we bump each time we know we might be leaking objects.
                            // Better / easier / cheaper to monitor than scraping logs.
                            tracing::error!(
                                "Failed to enqueue deletions, leaking objects.  This is a bug."
                            );
                        }
                    }
                }
                FrontendQueueMessage::Flush(op) => {
                    if self.pending.is_empty() {
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
                FrontendQueueMessage::Recover(op) => {
                    if let Err(e) = self.recover(op.attached_tenants).await {
                        // This should only happen in truly unrecoverable cases, like the recovery finding that the backend
                        // queue receiver has been dropped, or something is critically broken with
                        // the local filesystem holding deletion lists.
                        //
                        // Hmm, so, if we fail to recover, we return from the frontend queue worker.
                        // This means subsequent submissions will fail, right?
                        // Which means each tenant's compaction loop affected by failed submission will enter the "retry in 2" seconds loop.
                        // => IMO we should move recovery to the DeletionQueue::new() constructor function,
                        //    and if it fails, refuse to start the pageserver. Avoids a whole bunch of pain.
                        //    If recovery fails because the on-disk state is garbage, can always `rm -rf` it and then
                        //    restart the PS.
                        info!(
                            "Deletion queue recover aborted, deletion queue will not proceed ({e})"
                        );
                        return;
                    }
                }
            }

            if self.pending.len() > DELETION_LIST_TARGET_SIZE || !self.pending_flushes.is_empty() {
                self.flush().await;
            }
        }
        info!("Deletion queue shut down.");
    }
}
