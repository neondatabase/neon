use crate::metrics::{DELETION_QUEUE_ERRORS, DELETION_QUEUE_EXECUTED, DELETION_QUEUE_SUBMITTED};
use regex::Regex;
use remote_storage::DownloadError;
use remote_storage::{GenericRemoteStorage, RemotePath};
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use tokio;
use tokio::time::Duration;
use tracing::{self, debug, error, info, warn};
use utils::backoff;
use utils::id::{TenantId, TimelineId};

use crate::{config::PageServerConf, tenant::storage_layer::LayerFileName};

// The number of keys in a DeletionList before we will proactively persist it
// (without reaching a flush deadline).  This aims to deliver objects of the order
// of magnitude 1MB when we are under heavy delete load.
const DELETION_LIST_TARGET_SIZE: usize = 16384;

// Ordinarily, we only flush to DeletionList periodically, to bound the window during
// which we might leak objects from not flushing a DeletionList after
// the objects are already unlinked from timeline metadata.
const FLUSH_DEFAULT_DEADLINE: Duration = Duration::from_millis(10000);

// If someone is waiting for a flush to DeletionList, only delay a little to accumulate
// more objects before doing the flush.
const FLUSH_EXPLICIT_DEADLINE: Duration = Duration::from_millis(100);

// After this length of time, execute deletions which are elegible to run,
// even if we haven't accumulated enough for a full-sized DeleteObjects
const EXECUTE_IDLE_DEADLINE: Duration = Duration::from_secs(60);

// If the last attempt to execute failed, wait only this long before
// trying again.
const EXECUTE_RETRY_DEADLINE: Duration = Duration::from_secs(1);

// From the S3 spec
const MAX_KEYS_PER_DELETE: usize = 1000;

// TODO: adminstrative "panic button" config property to disable all deletions
// TODO: configurable for how long to wait before executing deletions

/// We aggregate object deletions from many tenants in one place, for several reasons:
/// - Coalesce deletions into fewer DeleteObjects calls
/// - Enable Tenant/Timeline lifetimes to be shorter than the time it takes
///   to flush any outstanding deletions.
/// - Globally control throughput of deletions, as these are a low priority task: do
///   not compete with the same S3 clients/connections used for higher priority uploads.
///
/// There are two parts to this, frontend and backend, joined by channels:
/// - DeletionQueueWorker consumes the frontend queue: the "DeletionQueue" that makes up
///   the public interface and accepts deletion requests.
/// - BackendQueueWorker consumes the backend queue: a queue of DeletionList that have
///   already been written to S3 and are now eligible for final deletion.
///
/// There are three queues internally:
/// - Incoming deletes (the DeletionQueue that the outside world sees)
/// - Persistent deletion blocks: these represent deletion lists that have already been written to S3 and
///   are pending execution.
/// - Deletions read back frorm the persistent deletion blocks, which are batched up into groups
///   of 1000 for execution via a DeleteObjects call.
///
/// In S3, there is just one queue, made up of a series of DeletionList objects and
/// a DeletionHeader
#[derive(Clone)]
pub struct DeletionQueue {
    tx: tokio::sync::mpsc::Sender<FrontendQueueMessage>,
}

#[derive(Debug)]
enum FrontendQueueMessage {
    Delete(DeletionOp),
    // Wait until all prior deletions make it into a persistent DeletionList
    Flush(FlushOp),
    // Wait until all prior deletions have been executed (i.e. objects are actually deleted)
    FlushExecute(FlushOp),
}

#[derive(Debug)]
struct DeletionOp {
    tenant_id: TenantId,
    timeline_id: TimelineId,
    // `layers` and `objects` are both just lists of objects.  `layers` is used if you do not
    // have a config object handy to project it to a remote key, and need the consuming worker
    // to do it for you.
    layers: Vec<LayerFileName>,
    objects: Vec<RemotePath>,
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
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct DeletionList {
    /// Used for constructing a unique key for each deletion list we write out.
    sequence: u64,

    /// These objects are elegible for deletion: they are unlinked from timeline metadata, and
    /// we are free to delete them at any time from their presence in this data structure onwards.
    objects: Vec<RemotePath>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
struct DeletionHeader {
    /// Enable determining the next sequence number even if there are no deletion lists present.
    /// If there _are_ deletion lists present, then their sequence numbers take precedence over
    /// this.
    last_deleted_list_seq: u64,
    // TODO: this is where we will track a 'clean' sequence number that indicates all deletion
    // lists <= that sequence have had their generations validated with the control plane
    // and are OK to execute.
}

impl DeletionList {
    fn new(sequence: u64) -> Self {
        Self {
            sequence,
            objects: Vec::new(),
        }
    }
}

impl DeletionQueueClient {
    async fn do_push(&self, msg: FrontendQueueMessage) {
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

    /// Submit a list of layers for deletion: this function will return before the deletion is
    /// persistent, but it may be executed at any time after this function enters: do not push
    /// layers until you're sure they can be deleted safely (i.e. remote metadata no longer
    /// references them).
    pub async fn push_layers(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        layers: Vec<LayerFileName>,
    ) {
        DELETION_QUEUE_SUBMITTED.inc_by(layers.len() as u64);
        self.do_push(FrontendQueueMessage::Delete(DeletionOp {
            tenant_id,
            timeline_id,
            layers,
            objects: Vec::new(),
        }))
        .await;
    }

    /// Just like push_layers, but using some already-known remote paths, instead of abstract layer names
    pub async fn push_objects(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        objects: Vec<RemotePath>,
    ) {
        DELETION_QUEUE_SUBMITTED.inc_by(objects.len() as u64);
        self.do_push(FrontendQueueMessage::Delete(DeletionOp {
            tenant_id,
            timeline_id,
            layers: Vec::new(),
            objects: objects,
        }))
        .await;
    }

    async fn do_flush(&self, msg: FrontendQueueMessage, rx: tokio::sync::oneshot::Receiver<()>) {
        self.do_push(msg).await;
        if rx.await.is_err() {
            // This shouldn't happen if tenants are shut down before deletion queue.  If we
            // encounter a bug like this, then a flusher will incorrectly believe it has flushed
            // when it hasn't, possibly leading to leaking objects.
            error!("Deletion queue dropped flush op while client was still waiting");
        }
    }

    /// Wait until all previous deletions are persistent (either executed, or written to a DeletionList)
    pub async fn flush(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        self.do_flush(FrontendQueueMessage::Flush(FlushOp { tx }), rx)
            .await
    }

    // Wait until all previous deletions are executed
    pub async fn flush_execute(&self) {
        // Flush any buffered work to deletion lists
        self.flush().await;

        // Flush execution of deletion lists
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        self.do_flush(FrontendQueueMessage::FlushExecute(FlushOp { tx }), rx)
            .await
    }
}

pub struct BackendQueueWorker {
    remote_storage: GenericRemoteStorage,
    conf: &'static PageServerConf,
    rx: tokio::sync::mpsc::Receiver<BackendQueueMessage>,

    // Accumulate up to 1000 keys for the next deletion operation
    accumulator: Vec<RemotePath>,

    // DeletionLists we have fully ingested but might still have
    // some keys in accumulator.
    pending_lists: Vec<DeletionList>,

    // DeletionLists we have fully executed, which may be deleted
    // from remote storage.
    executed_lists: Vec<DeletionList>,

    // How long to wait for a message before executing anyway
    timeout: Duration,
}

impl BackendQueueWorker {
    async fn maybe_execute(&mut self) -> bool {
        fail::fail_point!("deletion-queue-before-execute", |_| {
            info!("Skipping execution, failpoint set");
            DELETION_QUEUE_ERRORS
                .with_label_values(&["failpoint"])
                .inc();
            return false;
        });

        if self.accumulator.is_empty() {
            return true;
        }

        match self.remote_storage.delete_objects(&self.accumulator).await {
            Ok(()) => {
                DELETION_QUEUE_EXECUTED.inc_by(self.accumulator.len() as u64);
                info!(
                    "Executed deletion batch {}..{}",
                    self.accumulator
                        .first()
                        .expect("accumulator should be non-empty"),
                    self.accumulator
                        .last()
                        .expect("accumulator should be non-empty"),
                );
                self.accumulator.clear();
                self.executed_lists.append(&mut self.pending_lists);
                self.timeout = EXECUTE_IDLE_DEADLINE;
                true
            }
            Err(e) => {
                warn!("DeleteObjects request failed: {e:#}, will retry");
                DELETION_QUEUE_ERRORS.with_label_values(&["execute"]).inc();
                self.timeout = EXECUTE_RETRY_DEADLINE;
                false
            }
        }
    }

    async fn cleanup_lists(&mut self) {
        debug!(
            "cleanup_lists: {0} executed lists, {1} pending lists",
            self.executed_lists.len(),
            self.pending_lists.len()
        );

        // Lists are always pushed into the queues + executed list in sequence order, so
        // no sort is required: can find the highest sequence number by peeking at last element
        let max_executed_seq = match self.executed_lists.last() {
            Some(v) => v.sequence,
            None => {
                // No executed lists, nothing to clean up.
                return;
            }
        };

        // In case this is the last list, write a header out first so that
        // we don't risk losing our knowledge of the sequence number (on replay, our
        // next sequence number is the highest list seen + 1, or read from the header
        // if there are no lists)
        let header = DeletionHeader::new(max_executed_seq);
        debug!("Writing header {:?}", header);
        let bytes = serde_json::to_vec(&header).expect("Failed to serialize deletion header");
        let size = bytes.len();
        let source = tokio::io::BufReader::new(std::io::Cursor::new(bytes));
        let header_key = self.conf.remote_deletion_header_path();

        if let Err(e) = self
            .remote_storage
            .upload(source, size, &header_key, None)
            .await
        {
            warn!("Failed to upload deletion queue header: {e:#}");
            DELETION_QUEUE_ERRORS
                .with_label_values(&["put_headerr"])
                .inc();
            return;
        }

        let executed_keys: Vec<RemotePath> = self
            .executed_lists
            .iter()
            .rev()
            .take(MAX_KEYS_PER_DELETE)
            .map(|l| self.conf.remote_deletion_list_path(l.sequence))
            .collect();

        match self.remote_storage.delete_objects(&executed_keys).await {
            Ok(()) => {
                // Retain any lists that couldn't be deleted in that request
                self.executed_lists
                    .truncate(self.executed_lists.len() - executed_keys.len());
            }
            Err(e) => {
                warn!("Failed to delete deletion list(s): {e:#}");
                // Do nothing: the elements remain in executed_lists, and purge will be retried
                // next time we process some deletions and go around the loop.
                DELETION_QUEUE_ERRORS
                    .with_label_values(&["delete_list"])
                    .inc();
            }
        }
    }

    pub async fn background(&mut self) {
        // TODO: if we would like to be able to defer deletions while a Layer still has
        // refs (but it will be elegible for deletion after process ends), then we may
        // add an ephemeral part to BackendQueueMessage::Delete that tracks which keys
        // in the deletion list may not be deleted yet, with guards to block on while
        // we wait to proceed.

        self.accumulator.reserve(MAX_KEYS_PER_DELETE);

        loop {
            let msg = match tokio::time::timeout(self.timeout, self.rx.recv()).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    // All queue senders closed
                    info!("Shutting down");
                    break;
                }
                Err(_) => {
                    // Timeout, we hit deadline to execute whatever we have in hand.  These functions will
                    // return immediately if no work is pending
                    self.maybe_execute().await;
                    self.cleanup_lists().await;

                    continue;
                }
            };

            match msg {
                BackendQueueMessage::Delete(mut list) => {
                    if list.objects.is_empty() {
                        // This shouldn't happen, but is harmless.  warn so that
                        // tests will fail if we have such a bug, but proceed with
                        // processing subsequent messages.
                        warn!("Empty DeletionList passed to deletion backend");
                        self.executed_lists.push(list);
                        continue;
                    }

                    // This loop handles deletion lists that require multiple DeleteObjects requests,
                    // and also handles retries if a deletion fails: we will keep going around until
                    // we have either deleted everything, or we have a remainder in accumulator.
                    while !list.objects.is_empty() || self.accumulator.len() == MAX_KEYS_PER_DELETE
                    {
                        let take_count = if self.accumulator.len() == MAX_KEYS_PER_DELETE {
                            0
                        } else {
                            let available_slots = MAX_KEYS_PER_DELETE - self.accumulator.len();
                            std::cmp::min(available_slots, list.objects.len())
                        };

                        for object in list.objects.drain(list.objects.len() - take_count..) {
                            self.accumulator.push(object);
                        }

                        if self.accumulator.len() == MAX_KEYS_PER_DELETE {
                            // Great, we got a full request: issue it.
                            if self.maybe_execute().await == false {
                                // Failed to execute: retry delay
                                tokio::time::sleep(EXECUTE_RETRY_DEADLINE).await;
                            };
                        }
                    }

                    if !self.accumulator.is_empty() {
                        // We have a remainder, deletion list is not fully processed yet
                        self.pending_lists.push(list);
                    } else {
                        // We fully processed this list, it is ready for purge
                        self.executed_lists.push(list);
                    }

                    self.cleanup_lists().await;
                }
                BackendQueueMessage::Flush(op) => {
                    self.maybe_execute().await;

                    self.cleanup_lists().await;

                    op.fire();
                }
            }
        }
    }
}

#[derive(Debug)]
enum BackendQueueMessage {
    Delete(DeletionList),
    Flush(FlushOp),
}

pub struct FrontendQueueWorker {
    remote_storage: GenericRemoteStorage,
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

    // After how long will we flush a DeletionList without reaching the target size:
    // this is lazy usually, but after a failed flush it is set to a smaller time
    // period to drive retries
    timeout: Duration,
}

impl FrontendQueueWorker {
    /// Try to flush `list` to persistent storage
    ///
    /// This does not return errors, because on failure to flush we do not lose
    /// any state: flushing will be retried implicitly on the next deadline
    async fn flush(&mut self) {
        let key = &self.conf.remote_deletion_list_path(self.pending.sequence);

        let bytes = serde_json::to_vec(&self.pending).expect("Failed to serialize deletion list");
        let size = bytes.len();
        let source = tokio::io::BufReader::new(std::io::Cursor::new(bytes));

        if self.pending.objects.is_empty() {
            // We do not expect to be called in this state, but handle it so that later
            // logging code can be assured that therre is always a first+last key to print
            for f in self.pending_flushes.drain(..) {
                f.fire();
            }
            return;
        }

        match self.remote_storage.upload(source, size, &key, None).await {
            Ok(_) => {
                info!(
                    "Stored deletion list {key} ({0}..{1})",
                    self.pending
                        .objects
                        .first()
                        .expect("list should be non-empty"),
                    self.pending
                        .objects
                        .last()
                        .expect("list should be non-empty"),
                );

                for f in self.pending_flushes.drain(..) {
                    f.fire();
                }

                let mut onward_list = DeletionList {
                    sequence: self.pending.sequence,
                    objects: Vec::new(),
                };
                std::mem::swap(&mut onward_list.objects, &mut self.pending.objects);
                self.pending.sequence += 1;

                if let Err(e) = self.tx.send(BackendQueueMessage::Delete(onward_list)).await {
                    // This is allowed to fail: it will only happen if the backend worker is shut down,
                    // so we can just drop this on the floor.
                    info!("Deletion list dropped, this is normal during shutdown ({e:#})");
                }

                self.timeout = FLUSH_DEFAULT_DEADLINE;
            }
            Err(e) => {
                DELETION_QUEUE_ERRORS.with_label_values(&["put_list"]).inc();
                warn!(
                    sequence = self.pending.sequence,
                    "Failed to write deletion list to remote storage, will retry later ({e:#})"
                );
                self.timeout = FLUSH_EXPLICIT_DEADLINE;
            }
        }
    }

    async fn recover(&mut self) -> Result<(), anyhow::Error> {
        // Load header: this is not required to be present, e.g. when a pageserver first runs
        let header_path = self.conf.remote_deletion_header_path();
        let header_bytes = match backoff::retry(
            || self.remote_storage.download_all(&header_path),
            |e| {
                if let DownloadError::NotFound = e {
                    true
                } else {
                    false
                }
            },
            3,
            u32::MAX,
            "Reading deletion queue header",
        )
        .await
        {
            Ok(h) => Ok(Some(h)),
            Err(e) => {
                if let DownloadError::NotFound = e {
                    debug!("Deletion header {header_path} not found, first start?");
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
                    warn!("Failed to deserialize deletion header, ignoring {header_path}: {e:#}");
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

        // TODO: this needs a CancellationToken or equivalent: usual worker teardown happens via the channel
        let prefix = RemotePath::new(&self.conf.remote_deletion_node_prefix())
            .expect("Failed to compose path");
        let lists = backoff::retry(
            || async { self.remote_storage.list_prefixes(Some(&prefix)).await },
            |_| false, // TODO impl is_permanent
            3,
            u32::MAX, // There's no point giving up, since once we do that the deletion queue is stuck
            "Recovering deletion lists",
        )
        .await?;

        debug!("Loaded {} keys in deletion prefix {}", lists.len(), prefix);

        let list_name_pattern =
            Regex::new("([a-zA-Z0-9]{16})-([a-zA-Z0-9]{8})-([a-zA-Z0-9]{2}).list").unwrap();

        let mut seqs: Vec<u64> = Vec::new();
        for l in &lists {
            if l == &header_path {
                // Don't try and parse the header key as a list key
                continue;
            }

            let basename = l
                .strip_prefix(&prefix)
                .expect("Stripping prefix frrom a prefix listobjects should always work");
            let basename = match basename.to_str() {
                Some(s) => s,
                None => {
                    // Should never happen, we are the only ones writing objects here
                    warn!("Unexpected key encoding in deletion queue object");
                    continue;
                }
            };

            let seq_part = if let Some(m) = list_name_pattern.captures(basename) {
                m.get(1)
                    .expect("Non optional group should be present")
                    .as_str()
            } else {
                warn!("Unexpected key in deletion queue: {basename}");
                continue;
            };

            info!("seq_part {seq_part}");

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
            let list_path = self.conf.remote_deletion_list_path(s);
            let lists_body = backoff::retry(
                || self.remote_storage.download_all(&list_path),
                |_| false,
                3,
                u32::MAX,
                "Reading a deletion list",
            )
            .await?;

            let deletion_list = match serde_json::from_slice::<DeletionList>(lists_body.as_slice())
            {
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
            DELETION_QUEUE_SUBMITTED.inc_by(deletion_list.objects.len() as u64);
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

        // Before accepting any input from this pageserver lifetime, recover all deletion lists that are in S3
        if let Err(e) = self.recover().await {
            // This should only happen in truly unrecoverable cases, like the recovery finding that the backend
            // queue receiver has been dropped.
            info!("Deletion queue recover aborted, deletion queue will not proceed ({e:#})");
            return;
        }

        loop {
            let msg = match tokio::time::timeout(self.timeout, self.rx.recv()).await {
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
                    let timeline_path = self.conf.timeline_path(&op.tenant_id, &op.timeline_id);

                    let _span = tracing::info_span!(
                        "deletion_frontend_enqueue",
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
                        self.pending.objects.push(path);
                    }

                    self.pending.objects.extend(op.objects.into_iter())
                }
                FrontendQueueMessage::Flush(op) => {
                    if self.pending.objects.is_empty() {
                        // Execute immediately
                        op.fire()
                    } else {
                        // Execute next time we flush
                        self.pending_flushes.push(op);

                        // Move up the deadline since we have been explicitly asked to flush
                        self.timeout = FLUSH_EXPLICIT_DEADLINE;
                    }
                }
                FrontendQueueMessage::FlushExecute(op) => {
                    // We do not flush to a deletion list here: the client sends a Flush before the FlushExecute
                    if let Err(e) = self.tx.send(BackendQueueMessage::Flush(op)).await {
                        info!("Can't flush, shutting down ({e})");
                        // Caller will get error when their oneshot sender was dropped.
                    }
                }
            }

            if self.pending.objects.len() > DELETION_LIST_TARGET_SIZE {
                debug!(sequence = self.pending.sequence, "Flushing for deadline");
                self.flush().await;
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

    /// Caller may use the returned object to construct clients with new_client.
    /// Caller should tokio::spawn the background() members of the two worker objects returned:
    /// we don't spawn those inside new() so that the caller can use their runtime/spans of choice.
    ///
    /// If remote_storage is None, then the returned workers will also be None.
    pub fn new(
        remote_storage: Option<GenericRemoteStorage>,
        conf: &'static PageServerConf,
    ) -> (
        Self,
        Option<FrontendQueueWorker>,
        Option<BackendQueueWorker>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel(16384);

        let remote_storage = match remote_storage {
            None => return (Self { tx }, None, None),
            Some(r) => r,
        };

        let (backend_tx, backend_rx) = tokio::sync::mpsc::channel(16384);

        (
            Self { tx },
            Some(FrontendQueueWorker {
                pending: DeletionList::new(1),
                remote_storage: remote_storage.clone(),
                conf,
                rx,
                tx: backend_tx,
                timeout: FLUSH_DEFAULT_DEADLINE,
                pending_flushes: Vec::new(),
            }),
            Some(BackendQueueWorker {
                remote_storage,
                conf,
                rx: backend_rx,
                accumulator: Vec::new(),
                pending_lists: Vec::new(),
                executed_lists: Vec::new(),
                timeout: EXECUTE_IDLE_DEADLINE,
            }),
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
    }

    impl TestSetup {
        /// Simulate a pageserver restart by destroying and recreating the deletion queue
        fn restart(&mut self) {
            let (deletion_queue, fe_worker, be_worker) =
                DeletionQueue::new(Some(self.storage.clone()), self.harness.conf);

            self.deletion_queue = deletion_queue;

            let mut fe_worker = fe_worker.unwrap();
            let mut be_worker = be_worker.unwrap();
            let mut fe_worker = self
                .runtime
                .spawn(async move { fe_worker.background().await });
            let mut be_worker = self
                .runtime
                .spawn(async move { be_worker.background().await });
            std::mem::swap(&mut self.fe_worker, &mut fe_worker);
            std::mem::swap(&mut self.be_worker, &mut be_worker);

            // Join the old workers
            self.runtime.block_on(fe_worker).unwrap();
            self.runtime.block_on(be_worker).unwrap();
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

        let (deletion_queue, fe_worker, be_worker) =
            DeletionQueue::new(Some(storage.clone()), harness.conf);

        let mut fe_worker = fe_worker.unwrap();
        let mut be_worker = be_worker.unwrap();
        let fe_worker_join = runtime.spawn(async move { fe_worker.background().await });
        let be_worker_join = runtime.spawn(async move { be_worker.background().await });

        Ok(TestSetup {
            runtime,
            _entered_runtime: entered_runtime,
            harness,
            remote_fs_dir,
            storage,
            deletion_queue,
            fe_worker: fe_worker_join,
            be_worker: be_worker_join,
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
            &content,
        )?;
        assert_remote_files(&[&layer_file_name_1.file_name()], &remote_timeline_path);

        // File should still be there after we push it to the queue (we haven't pushed enough to flush anything)
        info!("Pushing");
        ctx.runtime.block_on(client.push_layers(
            tenant_id,
            TIMELINE_ID,
            [layer_file_name_1.clone()].to_vec(),
        ));
        assert_remote_files(&[&layer_file_name_1.file_name()], &remote_timeline_path);
        assert_remote_files(&[], &remote_deletion_prefix);

        // File should still be there after we write a deletion list (we haven't pushed enough to execute anything)
        info!("Flushing");
        ctx.runtime.block_on(client.flush());
        assert_remote_files(&[&layer_file_name_1.file_name()], &remote_timeline_path);
        assert_remote_files(
            &["0000000000000001-00000000-01.list"],
            &remote_deletion_prefix,
        );

        // File should go away when we execute
        info!("Flush-executing");
        ctx.runtime.block_on(client.flush_execute());
        assert_remote_files(&[], &remote_timeline_path);
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
            &content,
        )?;
        ctx.runtime.block_on(client.push_layers(
            tenant_id,
            TIMELINE_ID,
            [layer_file_name_1.clone()].to_vec(),
        ));
        ctx.runtime.block_on(client.flush());
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
        ctx.runtime.block_on(client.flush_execute());
        assert_remote_files(&[], &remote_timeline_path);
        assert_remote_files(&["header-00000000-01"], &remote_deletion_prefix);
        Ok(())
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
        tx: tokio::sync::mpsc::Sender<FrontendQueueMessage>,
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
                            FrontendQueueMessage::Delete(op) => {
                                let timeline_path =
                                    conf.timeline_path(&op.tenant_id, &op.timeline_id);

                                let _span = tracing::info_span!(
                                    "execute_deletion",
                                    tenant_id = %op.tenant_id,
                                    timeline_id = %op.timeline_id,
                                );

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
