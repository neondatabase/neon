use std::time::Duration;

use remote_storage::GenericRemoteStorage;
use remote_storage::RemotePath;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::config::PageServerConf;
use crate::metrics::DELETION_QUEUE_ERRORS;
use crate::metrics::DELETION_QUEUE_EXECUTED;

use super::DeletionHeader;
use super::DeletionList;
use super::FlushOp;

// After this length of time, execute deletions which are elegible to run,
// even if we haven't accumulated enough for a full-sized DeleteObjects
const EXECUTE_IDLE_DEADLINE: Duration = Duration::from_secs(60);

// If the last attempt to execute failed, wait only this long before
// trying again.
const EXECUTE_RETRY_DEADLINE: Duration = Duration::from_millis(100);

// From the S3 spec
const MAX_KEYS_PER_DELETE: usize = 1000;

#[derive(Debug)]
pub(super) enum BackendQueueMessage {
    Delete(DeletionList),
    Flush(FlushOp),
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

    // These FlushOps should fire the next time we flush
    pending_flushes: Vec<FlushOp>,

    // How long to wait for a message before executing anyway
    timeout: Duration,
}

impl BackendQueueWorker {
    pub(super) fn new(
        remote_storage: GenericRemoteStorage,
        conf: &'static PageServerConf,
        rx: tokio::sync::mpsc::Receiver<BackendQueueMessage>,
    ) -> Self {
        Self {
            remote_storage,
            conf,
            rx,
            accumulator: Vec::new(),
            pending_lists: Vec::new(),
            executed_lists: Vec::new(),
            timeout: EXECUTE_IDLE_DEADLINE,
            pending_flushes: Vec::new(),
        }
    }

    async fn maybe_execute(&mut self) -> bool {
        fail::fail_point!("deletion-queue-before-execute", |_| {
            info!("Skipping execution, failpoint set");
            DELETION_QUEUE_ERRORS
                .with_label_values(&["failpoint"])
                .inc();

            // Retry fast when failpoint is active, so that when it is disabled we resume promptly
            self.timeout = EXECUTE_RETRY_DEADLINE;
            false
        });

        if self.accumulator.is_empty() {
            for f in self.pending_flushes.drain(..) {
                f.fire();
            }
            return true;
        }

        match self.remote_storage.delete_objects(&self.accumulator).await {
            Ok(()) => {
                // Note: we assume that the remote storage layer returns Ok(()) if some
                // or all of the deleted objects were already gone.
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

                for f in self.pending_flushes.drain(..) {
                    f.fire();
                }
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
                            if !self.maybe_execute().await {
                                // Failed to execute: retry delay
                                tokio::time::sleep(EXECUTE_RETRY_DEADLINE).await;
                            };
                        }
                    }

                    if !self.accumulator.is_empty() {
                        // We have a remainder, `list` not fully executed yet
                        self.pending_lists.push(list);
                    } else {
                        // We fully processed this list, it is ready for purge
                        self.executed_lists.push(list);
                    }

                    self.cleanup_lists().await;
                }
                BackendQueueMessage::Flush(op) => {
                    if self.accumulator.is_empty() {
                        op.fire();
                        continue;
                    }

                    self.maybe_execute().await;

                    if self.accumulator.is_empty() {
                        // Successful flush.  Clean up lists before firing, for the benefit of tests that would
                        // like to have a deterministic state post-flush.
                        self.cleanup_lists().await;
                        op.fire();
                    } else {
                        // We didn't flush inline: defer until next time we successfully drain accumulatorr
                        self.pending_flushes.push(op);
                    }
                }
            }
        }
    }
}
