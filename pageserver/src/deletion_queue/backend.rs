use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::config::PageServerConf;
use crate::control_plane_client::ControlPlaneGenerationsApi;
use crate::metrics::DELETION_QUEUE_DROPPED;
use crate::metrics::DELETION_QUEUE_ERRORS;

use super::executor::ExecutorMessage;
use super::lsn_visibility;
use super::DeletionHeader;
use super::DeletionList;
use super::DeletionQueueError;
use super::FlushOp;

// After this length of time, do any validation work that is pending,
// even if we haven't accumulated many keys to delete.
//
// This also causes updates to remote_consistent_lsn to be validated, even
// if there were no deletions enqueued.
const AUTOFLUSH_INTERVAL: Duration = Duration::from_secs(10);

// If we have received this number of keys, proceed with attempting to execute
const AUTOFLUSH_KEY_COUNT: usize = 16384;

#[derive(Debug)]
pub(super) enum BackendQueueMessage {
    Delete(DeletionList),
    Flush(FlushOp),
}
pub(super) struct BackendQueueWorker<C>
where
    C: ControlPlaneGenerationsApi,
{
    conf: &'static PageServerConf,
    rx: tokio::sync::mpsc::Receiver<BackendQueueMessage>,
    tx: tokio::sync::mpsc::Sender<ExecutorMessage>,

    // Client for calling into control plane API for validation of deletes
    control_plane_client: Option<C>,

    // DeletionLists which are waiting generation validation.  Not safe to
    // execute until [`validate`] has processed them.
    pending_lists: Vec<DeletionList>,

    // DeletionLists which have passed validation and are ready to execute.
    validated_lists: Vec<DeletionList>,

    // Sum of all the lengths of lists in pending_lists
    pending_key_count: usize,

    // Lsn validation state: we read projected LSNs and write back visible LSNs
    // after validation.  This is the LSN equivalent of `pending_validation_lists`:
    // it is drained in [`validate`]
    lsn_table: Arc<std::sync::RwLock<lsn_visibility::VisibleLsnUpdates>>,

    cancel: CancellationToken,
}

impl<C> BackendQueueWorker<C>
where
    C: ControlPlaneGenerationsApi,
{
    pub(super) fn new(
        conf: &'static PageServerConf,
        rx: tokio::sync::mpsc::Receiver<BackendQueueMessage>,
        tx: tokio::sync::mpsc::Sender<ExecutorMessage>,
        control_plane_client: Option<C>,
        lsn_table: Arc<std::sync::RwLock<lsn_visibility::VisibleLsnUpdates>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            conf,
            rx,
            tx,
            control_plane_client,
            lsn_table,
            pending_lists: Vec::new(),
            validated_lists: Vec::new(),
            pending_key_count: 0,
            cancel,
        }
    }

    // these lists have been drained, all we need from them is the list path.
    // See my comments in the caller caller.
    async fn cleanup_lists(&mut self, lists: Vec<DeletionList>) {
        for list in lists {
            let list_path = self.conf.deletion_list_path(list.sequence);
            debug!("Removing deletion list {list} at {}", list_path.display());

            if let Err(e) = tokio::fs::remove_file(&list_path).await {
                // Unexpected: we should have permissions and nothing else should
                // be touching these files.  We will leave the file behind.  Subsequent
                // pageservers will try and load it again: hopefully whatever storage
                // issue (probably permissions) has been fixed by then.
                //
                // Hm, the recover() function would load these again as `validate=true`, right?
                // So, we'd retry the deletions, even though we know that by the time this
                // function gets called, the deletions were successful.
                // That seems wasteful. Can't the header keep an additional pointer/sequence number
                // that tracks the deletions lists that were already fully executed?
                // (... I guess I need to read the code that handles failure to write out the header)
                tracing::error!("Failed to delete {}: {e:#}", list_path.display());
                break;
            }
        }
    }

    /// Process any outstanding validations of generations of pending LSN updates or pending
    /// DeletionLists.
    ///
    /// Valid LSN updates propagate back to their result channel immediately, valid DeletionLists
    /// go into the queue of ready-to-execute lists.
    async fn validate(&mut self) -> Result<(), DeletionQueueError> {
        let mut tenant_generations = HashMap::new();
        for list in &self.pending_lists {
            for (tenant_id, tenant_list) in &list.tenants {
                // Note: DeletionLists are in logical time order, so generation always
                // goes up.  By doing a simple insert() we will always end up with
                // the latest generation seen for a tenant.
                tenant_generations.insert(*tenant_id, tenant_list.generation);
            }
        }

        let pending_lsn_updates = {
            let mut lsn_table = self.lsn_table.write().expect("Lock should not be poisoned");
            let mut pending_updates = lsn_visibility::VisibleLsnUpdates::new();
            std::mem::swap(&mut pending_updates, &mut lsn_table);
            pending_updates
        };
        for (tenant_id, update) in &pending_lsn_updates.tenants {
            let entry = tenant_generations
                .entry(*tenant_id)
                .or_insert(update.generation);
            if update.generation > *entry {
                *entry = update.generation;
            }
        }

        if tenant_generations.is_empty() {
            // No work to do
            return Ok(());
        }

        let tenants_valid = if let Some(control_plane_client) = &self.control_plane_client {
            control_plane_client
                .validate(tenant_generations.iter().map(|(k, v)| (*k, *v)).collect())
                .await
                // The only wait a validation call returns an error is when the cancellation token fires
                // Can't it also return an error if there is a networking issue?
                .map_err(|_| DeletionQueueError::ShuttingDown)?
        } else {
            // Control plane API disabled.  In legacy mode we consider everything valid.
            tenant_generations.keys().map(|k| (*k, true)).collect()
        };

        let mut validated_sequence: Option<u64> = None;

        // Apply the validation results to the pending LSN updates
        for (tenant_id, tenant_lsn_state) in pending_lsn_updates.tenants {
            let validated_generation = tenant_generations
                .get(&tenant_id)
                .expect("Map was built from the same keys we're reading");

            // If the tenant was missing from the validation response, it has been deleted.  We may treat
            // deletions as valid as the tenant's remote storage is all to be wiped anyway.
            // What's the downside of being conservative here and treating as invalid?
            // That would guard us against console bugs (e.g., someone putting an accidental LIMIT where there should be none)
            let valid = tenants_valid.get(&tenant_id).copied().unwrap_or(true);

            if valid && *validated_generation == tenant_lsn_state.generation {
                for (_timeline_id, pending_lsn) in tenant_lsn_state.timelines {
                    // Drop result of send: it is legal for the Timeline to have been dropped along
                    // with its queue receiver while we were doing validation.
                    // This comment seems outdated.
                    pending_lsn.result_slot.store(pending_lsn.projected);
                }
            } else {
                // If we failed validation, then do not apply any of the projected updates
                // Add a global counter metric (no tenant_id label) for this so we can have a distinguished alert on it? Should be initialzed to 0 in preinitialize_metrics.
                warn!("Dropped remote consistent LSN updates for tenant {tenant_id} in stale generation {0:?}", tenant_lsn_state.generation);
            }
        }

        // Apply the validation results to the pending deletion lists
        for list in &mut self.pending_lists {
            // Filter the list based on whether the server responded valid: true.
            // If a tenant is omitted in the response, it has been deleted, and we should
            // proceed with deletion.
            // Again, what's the downside of being conservative here and not doing anything on omissions?
            let mut mutated = false;
            list.tenants.retain(|tenant_id, tenant| {
                let validated_generation = tenant_generations
                    .get(tenant_id)
                    .expect("Map was built from the same keys we're reading");

                // If the tenant was missing from the validation response, it has been deleted.  We may treat
                // deletions as valid as the tenant's remote storage is all to be wiped anyway.
                let valid = tenants_valid.get(tenant_id).copied().unwrap_or(true);

                // A list is valid if it comes from the current _or previous_ generation.
                // The previous generation case is due to how we store deletion lists locally:
                // if we see the immediately previous generation in a locally stored deletion list,
                // it proves that this node's disk was used for both current & previous generations,
                // and therefore no other node was involved in between: the two generations may be
                // logically treated as the same.
                //
                // 1. I don't see the check for previous generatio nhere? at a minimum variable names are not obvious?
                // 2. Wondering what would have been if we had made this part of /re-attach, i.e.,
                //    option to present previous_generation in re-attach and if that previous generation
                //    is still the most recent, return the same generation number.
                //    Would avoid special cases like this one here.
                //    Basically, what we have now is we persist the generation number across restart in the DeletionList,
                //    but don't really trust it unless it passes this special-case check here.
                //    Why not move the special-case check into the /re-attach endpoint?
                //    Not a blocker for this PR, but would appreciate a response in #team-storage on this.
                let this_list_valid = valid
                    && (tenant.generation == *validated_generation);

                if !this_list_valid {
                    warn!("Dropping stale deletions for tenant {tenant_id} in generation {:?}, objects may be leaked", tenant.generation);
                    DELETION_QUEUE_DROPPED.inc_by(tenant.len() as u64);
                    mutated = true;
                }
                this_list_valid
            });
            list.validated = true;

            if mutated {
                // Save the deletion list if we had to make changes due to stale generations.  The
                // saved list is valid for execution.
                // This save is overwriting and it's neither atomic nor crashsafe; use VirtualFile::crashsafe_overwrite or similar.
                if let Err(e) = list.save(self.conf).await {
                    // Highly unexpected.  Could happen if e.g. disk full.
                    // If we didn't save the trimmed list, it is _not_ valid to execute.
                    // log message should mention leaking of objects. also I requested a counter metric for such events somehwere else, should increment it here.
                    warn!("Failed to save modified deletion list {list}: {e:#}");

                    // Rather than have a complex retry process, just drop it and leak the objects,
                    // scrubber will clean up eventually.
                    list.tenants.clear(); // Result is a valid-but-empty list, which is a no-op for execution.

                    // if we hit a save error here, i.e., leave the untrimmed list on disk, and later successfully save the header.
                    // wouldn't recovery treat the untrimmed list as validated?
                }
            }

            validated_sequence = Some(list.sequence);
        }

        if let Some(validated_sequence) = validated_sequence {
            // Write the queue header to record how far validation progressed.  This avoids having
            // to rewrite each DeletionList to set validated=true in it.
            let header = DeletionHeader::new(validated_sequence);

            // Drop result because the validated_sequence is an optimization.  If we fail to save it,
            // then restart, we will drop some deletion lists, creating work for scrubber.
            // The save() function logs a warning on error.
            // 1. Again, use VirtualFile::crashsafe_overwrite here for atomic replace.
            // 2. Have you thought through whether it's safe to continue with the existing queue?
            //    Would feel more comfortable if I didn't have to think about what it means for recovery.
            //    How about just starting a new queue if this here happens? Or: panic the pageserver? I'd be ok with that in this very unlikely case.
            if let Err(e) = header.save(self.conf).await {
                warn!("Failed to write deletion queue header: {e:#}");
                DELETION_QUEUE_ERRORS
                    .with_label_values(&["put_header"])
                    .inc();
            }
        }

        // Transfer the validated lists to the validated queue, for eventual execution
        self.validated_lists.append(&mut self.pending_lists);

        Ok(())
    }

    async fn flush(&mut self) -> Result<(), DeletionQueueError> {
        tracing::debug!("Flushing with {} pending lists", self.pending_lists.len());

        // Issue any required generation validation calls to the control plane
        self.validate().await?;

        // After successful validation, nothing is pending: any lists that
        // made it through validation will be in validated_lists.
        assert!(self.pending_lists.is_empty());
        self.pending_key_count = 0; // why can't we just pending_lists.len() in all places we use pending_key_count

        tracing::debug!(
            "Validation complete, have {} validated lists",
            self.validated_lists.len()
        );

        // Return quickly if we have no validated lists to execute.  This avoids flushing the
        // executor when an idle backend hits its autoflush interval
        if self.validated_lists.is_empty() {
            return Ok(());
        }

        // Drain `validated_lists` into the executor
        let mut executing_lists = Vec::new();
        for mut list in self.validated_lists.drain(..) {
            // again, this weird C++-esque &mut self drain function.
            // I think a consuming `into_drain_paths()` would be more idiomatic.
            //
            // I see you need the drained list for the `cleanup_lists` call below.
            // Make an `into_...()` function that returns a tuple, then.
            // Less mutable state, more values = better.
            let objects = list.drain_paths();
            self.tx
                .send(ExecutorMessage::Delete(objects))
                .await
                .map_err(|_| DeletionQueueError::ShuttingDown)?;
            executing_lists.push(list);
        }

        self.flush_executor().await?;

        // Erase the deletion lists whose keys have all be deleted from remote storage
        self.cleanup_lists(executing_lists).await;

        Ok(())
    }

    // better name: flush_to_executor_and_wait
    async fn flush_executor(&mut self) -> Result<(), DeletionQueueError> {
        // Flush the executor, so that all the keys referenced by these deletion lists
        // are actually removed from remote storage.  This is a precondition to deleting
        // the deletion lists themselves.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let flush_op = FlushOp { tx };
        self.tx
            .send(ExecutorMessage::Flush(flush_op))
            .await
            .map_err(|_| DeletionQueueError::ShuttingDown)?;

        rx.await.map_err(|_| DeletionQueueError::ShuttingDown)
    }

    pub(super) async fn background(&mut self) {
        tracing::info!("Started deletion backend worker");

        while !self.cancel.is_cancelled() {
            let msg = match tokio::time::timeout(AUTOFLUSH_INTERVAL, self.rx.recv()).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    // All queue senders closed
                    info!("Shutting down");
                    break;
                }
                Err(_) => {
                    // Timeout, we hit deadline to execute whatever we have in hand.  These functions will
                    // return immediately if no work is pending.
                    // Drop result, because it' a background flush and we don't care whether it really worked.
                    drop(self.flush().await);

                    continue;
                }
            };

            match msg {
                BackendQueueMessage::Delete(list) => {
                    if list.validated {
                        // this only happens during recovery
                        self.validated_lists.push(list)
                    } else {
                        self.pending_key_count += list.len();
                        self.pending_lists.push(list);
                    }

                    if self.pending_key_count > AUTOFLUSH_KEY_COUNT {
                        // Drop possible shutdown error, because we will just fall out of loop if that happens
                        // TODO: log the error at least?
                        // Alternatively, put a match here to ensure that it's really the shutdown error,
                        // and not some other error variant we might add in the future.
                        drop(self.flush().await);
                    }
                }
                BackendQueueMessage::Flush(op) => {
                    // would prefer an exhaustive match of the Ok() and each Err(...) variant here.
                    if let Ok(()) = self.flush().await {
                        // If we fail due to shutting down, we will just drop `op` to propagate that status.
                        op.fire();
                    }
                }
            }
        }
    }
}
