use std::time::Duration;

use remote_storage::GenericRemoteStorage;
use remote_storage::RemotePath;
use remote_storage::MAX_KEYS_PER_DELETE;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::config::PageServerConf;
use crate::metrics::DELETION_QUEUE_ERRORS;

use super::executor::ExecutorMessage;
use super::DeletionHeader;
use super::DeletionList;
use super::FlushOp;

// After this length of time, execute deletions which are elegible to run,
// even if we haven't accumulated enough for a full-sized DeleteObjects
const EXECUTE_IDLE_DEADLINE: Duration = Duration::from_secs(60);

// If we have received this number of keys, proceed with attempting to execute
const AUTOFLUSH_KEY_COUNT: usize = 16384;

#[derive(Debug)]
pub(super) enum BackendQueueMessage {
    Delete(DeletionList),
    Flush(FlushOp),
}
pub struct BackendQueueWorker {
    remote_storage: GenericRemoteStorage,
    conf: &'static PageServerConf,
    rx: tokio::sync::mpsc::Receiver<BackendQueueMessage>,
    tx: tokio::sync::mpsc::Sender<ExecutorMessage>,

    // Accumulate some lists to execute in a batch.
    // The purpose of this accumulation is to implement batched validation of
    // attachment generations, when split-brain protection is implemented.
    // (see https://github.com/neondatabase/neon/pull/4919)
    pending_lists: Vec<DeletionList>,

    // Sum of all the lengths of lists in pending_lists
    pending_key_count: usize,

    // DeletionLists we have fully executed, which may be deleted
    // from remote storage.
    executed_lists: Vec<DeletionList>,
}

impl BackendQueueWorker {
    pub(super) fn new(
        remote_storage: GenericRemoteStorage,
        conf: &'static PageServerConf,
        rx: tokio::sync::mpsc::Receiver<BackendQueueMessage>,
        tx: tokio::sync::mpsc::Sender<ExecutorMessage>,
    ) -> Self {
        Self {
            remote_storage,
            conf,
            rx,
            tx,
            pending_lists: Vec::new(),
            pending_key_count: 0,
            executed_lists: Vec::new(),
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
                .with_label_values(&["put_header"])
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

    pub async fn flush(&mut self) {
        let mut onward_lists: Vec<DeletionList> = Vec::new();
        std::mem::swap(&mut onward_lists, &mut self.pending_lists);
        for list in onward_lists {
            let objects = list.objects.clone();
            // TODO: a take_objects method
            self.executed_lists.push(list);
            if let Err(_e) = self.tx.send(ExecutorMessage::Delete(objects)).await {
                warn!("Shutting down");
                return;
            };
        }

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let flush_op = FlushOp { tx };
        if let Err(_e) = self.tx.send(ExecutorMessage::Flush(flush_op)).await {
            warn!("Shutting down");
            return;
        };

        if rx.await.is_err() {
            warn!("Shutting down");
            return;
        }

        self.cleanup_lists().await;
    }

    pub async fn background(&mut self) {
        // TODO: if we would like to be able to defer deletions while a Layer still has
        // refs (but it will be elegible for deletion after process ends), then we may
        // add an ephemeral part to BackendQueueMessage::Delete that tracks which keys
        // in the deletion list may not be deleted yet, with guards to block on while
        // we wait to proceed.

        loop {
            let msg = match tokio::time::timeout(EXECUTE_IDLE_DEADLINE, self.rx.recv()).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    // All queue senders closed
                    info!("Shutting down");
                    break;
                }
                Err(_) => {
                    // Timeout, we hit deadline to execute whatever we have in hand.  These functions will
                    // return immediately if no work is pending
                    self.flush().await;

                    continue;
                }
            };

            match msg {
                BackendQueueMessage::Delete(list) => {
                    if list.objects.is_empty() {
                        // This shouldn't happen, but is harmless.  warn so that
                        // tests will fail if we have such a bug, but proceed with
                        // processing subsequent messages.
                        warn!("Empty DeletionList passed to deletion backend");
                        self.executed_lists.push(list);
                        continue;
                    }

                    self.pending_key_count += list.objects.len();
                    self.pending_lists.push(list);

                    if self.pending_key_count > AUTOFLUSH_KEY_COUNT {
                        self.flush().await;
                    }
                }
                BackendQueueMessage::Flush(op) => {
                    self.flush().await;
                    op.fire();
                }
            }
        }
    }
}
