use std::time::Duration;

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
        conf: &'static PageServerConf,
        rx: tokio::sync::mpsc::Receiver<BackendQueueMessage>,
        tx: tokio::sync::mpsc::Sender<ExecutorMessage>,
    ) -> Self {
        Self {
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
        let header_bytes =
            serde_json::to_vec(&header).expect("Failed to serialize deletion header");
        let header_path = self.conf.deletion_header_path();

        if let Err(e) = tokio::fs::write(&header_path, header_bytes).await {
            warn!("Failed to upload deletion queue header: {e:#}");
            DELETION_QUEUE_ERRORS
                .with_label_values(&["put_header"])
                .inc();
            return;
        }

        while let Some(list) = self.executed_lists.pop() {
            let list_path = self.conf.deletion_list_path(list.sequence);
            if let Err(e) = tokio::fs::remove_file(&list_path).await {
                // Unexpected: we should have permissions and nothing else should
                // be touching these files
                tracing::error!("Failed to delete {0}: {e:#}", list_path.display());
                self.executed_lists.push(list);
                break;
            }
        }
    }

    pub async fn flush(&mut self) {
        self.pending_key_count = 0;

        // Submit all keys from pending DeletionLists into the executor
        for list in &mut self.pending_lists {
            let objects = list.take_paths();
            if let Err(_e) = self.tx.send(ExecutorMessage::Delete(objects)).await {
                warn!("Shutting down");
                return;
            };
        }

        // Flush the executor to ensure all the operations we just submitted have been executed
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

        // After flush, we are assured that all contents of the pending lists
        // are executed
        self.executed_lists.append(&mut self.pending_lists);

        // Erase the lists we executed
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
