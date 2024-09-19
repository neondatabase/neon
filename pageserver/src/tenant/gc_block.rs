use std::{collections::HashMap, time::Duration};

use super::remote_timeline_client::index::GcBlockingReason;
use tokio::time::Instant;
use utils::id::TimelineId;

type TimelinesBlocked = HashMap<TimelineId, enumset::EnumSet<GcBlockingReason>>;

#[derive(Default)]
struct Storage {
    timelines_blocked: TimelinesBlocked,
    /// The deadline before which we are blocked from GC so that
    /// leases have a chance to be renewed.
    lsn_lease_deadline: Option<Instant>,
}

impl Storage {
    fn is_blocked_by_lsn_lease_deadline(&self) -> bool {
        self.lsn_lease_deadline
            .map(|d| Instant::now() < d)
            .unwrap_or(false)
    }
}

/// GcBlock provides persistent (per-timeline) gc blocking and facilitates transient time based gc
/// blocking.
#[derive(Default)]
pub(crate) struct GcBlock {
    /// The timelines which have current reasons to block gc.
    ///
    /// LOCK ORDER: this is held locked while scheduling the next index_part update. This is done
    /// to keep the this field up to date with RemoteTimelineClient `upload_queue.dirty`.
    reasons: std::sync::Mutex<Storage>,

    /// GC background task or manually run `Tenant::gc_iteration` holds a lock on this.
    ///
    /// Do not add any more features taking and forbidding taking this lock. It should be
    /// `tokio::sync::Notify`, but that is rarely used. On the other side, [`GcBlock::insert`]
    /// synchronizes with gc attempts by locking and unlocking this mutex.
    blocking: tokio::sync::Mutex<()>,
}

impl GcBlock {
    /// Start another gc iteration.
    ///
    /// Returns a guard to be held for the duration of gc iteration to allow synchronizing with
    /// it's ending, or if not currently possible, a value describing the reasons why not.
    ///
    /// Cancellation safe.
    pub(super) async fn start(&self) -> Result<Guard<'_>, BlockingReasons> {
        let reasons = {
            let g = self.reasons.lock().unwrap();

            // TODO: the assumption is that this method gets called periodically. in prod, we use 1h, in
            // tests, we use everything. we should warn if the gc has been consecutively blocked
            // for more than 1h (within single tenant session?).
            BlockingReasons::clean_and_summarize(g)
        };

        if let Some(reasons) = reasons {
            Err(reasons)
        } else {
            Ok(Guard {
                _inner: self.blocking.lock().await,
            })
        }
    }

    /// Sets a deadline before which we cannot proceed to GC due to lsn lease.
    ///
    /// We do this as the leases mapping are not persisted to disk. By delaying GC by lease
    /// length, we guarantee that all the leases we granted before will have a chance to renew
    /// when we run GC for the first time after restart / transition from AttachedMulti to AttachedSingle.
    pub(super) fn set_lsn_lease_deadline(&self, lsn_lease_length: Duration) {
        let deadline = Instant::now() + lsn_lease_length;
        let mut g = self.reasons.lock().unwrap();
        g.lsn_lease_deadline = Some(deadline);
    }

    /// Describe the current gc blocking reasons.
    ///
    /// TODO: make this json serializable.
    pub(crate) fn summary(&self) -> Option<BlockingReasons> {
        let g = self.reasons.lock().unwrap();

        BlockingReasons::summarize(&g)
    }

    /// Start blocking gc for this one timeline for the given reason.
    ///
    /// This is not a guard based API but instead it mimics set API. The returned future will not
    /// resolve until an existing gc round has completed.
    ///
    /// Returns true if this block was new, false if gc was already blocked for this reason.
    ///
    /// Cancellation safe: cancelling after first poll will keep the reason to block gc, but will
    /// keep the gc blocking reason.
    pub(crate) async fn insert(
        &self,
        timeline: &super::Timeline,
        reason: GcBlockingReason,
    ) -> anyhow::Result<bool> {
        let (added, uploaded) = {
            let mut g = self.reasons.lock().unwrap();
            let set = g.timelines_blocked.entry(timeline.timeline_id).or_default();
            let added = set.insert(reason);

            // LOCK ORDER: intentionally hold the lock, see self.reasons.
            let uploaded = timeline
                .remote_client
                .schedule_insert_gc_block_reason(reason)?;

            (added, uploaded)
        };

        uploaded.await?;

        // ensure that any ongoing gc iteration has completed
        drop(self.blocking.lock().await);

        Ok(added)
    }

    /// Remove blocking gc for this one timeline and the given reason.
    pub(crate) async fn remove(
        &self,
        timeline: &super::Timeline,
        reason: GcBlockingReason,
    ) -> anyhow::Result<()> {
        use std::collections::hash_map::Entry;

        super::span::debug_assert_current_span_has_tenant_and_timeline_id();

        let (remaining_blocks, uploaded) = {
            let mut g = self.reasons.lock().unwrap();
            match g.timelines_blocked.entry(timeline.timeline_id) {
                Entry::Occupied(mut oe) => {
                    let set = oe.get_mut();
                    set.remove(reason);
                    if set.is_empty() {
                        oe.remove();
                    }
                }
                Entry::Vacant(_) => {
                    // we must still do the index_part.json update regardless, in case we had earlier
                    // been cancelled
                }
            }

            let remaining_blocks = g.timelines_blocked.len();

            // LOCK ORDER: intentionally hold the lock while scheduling; see self.reasons
            let uploaded = timeline
                .remote_client
                .schedule_remove_gc_block_reason(reason)?;

            (remaining_blocks, uploaded)
        };
        uploaded.await?;

        // no need to synchronize with gc iteration again

        if remaining_blocks > 0 {
            tracing::info!(remaining_blocks, removed=?reason, "gc blocking removed, but gc remains blocked");
        } else {
            tracing::info!("gc is now unblocked for the tenant");
        }

        Ok(())
    }

    pub(crate) fn before_delete(&self, timeline: &super::Timeline) {
        let unblocked = {
            let mut g = self.reasons.lock().unwrap();
            if g.timelines_blocked.is_empty() {
                return;
            }

            g.timelines_blocked.remove(&timeline.timeline_id);

            BlockingReasons::clean_and_summarize(g).is_none()
        };

        if unblocked {
            tracing::info!("gc is now unblocked following deletion");
        }
    }

    /// Initialize with the non-deleted timelines of this tenant.
    pub(crate) fn set_scanned(&self, scanned: TimelinesBlocked) {
        let mut g = self.reasons.lock().unwrap();
        assert!(g.timelines_blocked.is_empty());
        g.timelines_blocked
            .extend(scanned.into_iter().filter(|(_, v)| !v.is_empty()));

        if let Some(reasons) = BlockingReasons::clean_and_summarize(g) {
            tracing::info!(summary=?reasons, "initialized with gc blocked");
        }
    }
}

pub(super) struct Guard<'a> {
    _inner: tokio::sync::MutexGuard<'a, ()>,
}

#[derive(Debug)]
pub(crate) struct BlockingReasons {
    tenant_blocked_by_lsn_lease_deadline: bool,
    timelines: usize,
    reasons: enumset::EnumSet<GcBlockingReason>,
}

impl std::fmt::Display for BlockingReasons {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tenant_blocked_by_lsn_lease_deadline: {}, {} timelines block for {:?}",
            self.tenant_blocked_by_lsn_lease_deadline, self.timelines, self.reasons
        )
    }
}

impl BlockingReasons {
    fn clean_and_summarize(mut g: std::sync::MutexGuard<'_, Storage>) -> Option<Self> {
        let mut reasons = enumset::EnumSet::empty();
        g.timelines_blocked.retain(|_key, value| {
            reasons = reasons.union(*value);
            !value.is_empty()
        });
        let blocked_by_lsn_lease_deadline = g.is_blocked_by_lsn_lease_deadline();
        if !g.timelines_blocked.is_empty() || blocked_by_lsn_lease_deadline {
            Some(BlockingReasons {
                tenant_blocked_by_lsn_lease_deadline: blocked_by_lsn_lease_deadline,
                timelines: g.timelines_blocked.len(),
                reasons,
            })
        } else {
            None
        }
    }

    fn summarize(g: &std::sync::MutexGuard<'_, Storage>) -> Option<Self> {
        let blocked_by_lsn_lease_deadline = g.is_blocked_by_lsn_lease_deadline();
        if g.timelines_blocked.is_empty() && !blocked_by_lsn_lease_deadline {
            None
        } else {
            let reasons = g
                .timelines_blocked
                .values()
                .fold(enumset::EnumSet::empty(), |acc, next| acc.union(*next));
            Some(BlockingReasons {
                tenant_blocked_by_lsn_lease_deadline: blocked_by_lsn_lease_deadline,
                timelines: g.timelines_blocked.len(),
                reasons,
            })
        }
    }
}
