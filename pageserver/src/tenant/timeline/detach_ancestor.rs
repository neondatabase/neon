use std::{collections::HashSet, sync::Arc};

use super::{layer_manager::LayerManager, FlushLayerError, Timeline};
use crate::{
    context::{DownloadBehavior, RequestContext},
    task_mgr::TaskKind,
    tenant::{
        mgr::GetActiveTenantError,
        storage_layer::{AsLayerDesc as _, DeltaLayerWriter, Layer, ResidentLayer},
        Tenant,
    },
    virtual_file::{MaybeFatalIo, VirtualFile},
};
use anyhow::Context;
use pageserver_api::models::detach_ancestor::AncestorDetached;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use utils::{completion, generation::Generation, http::error::ApiError, id::TimelineId, lsn::Lsn};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("no ancestors")]
    NoAncestor,
    #[error("too many ancestors")]
    TooManyAncestors,
    #[error("shutting down, please retry later")]
    ShuttingDown,
    #[error("flushing failed")]
    FlushAncestor(#[source] FlushLayerError),
    #[error("layer download failed")]
    RewrittenDeltaDownloadFailed(#[source] crate::tenant::storage_layer::layer::DownloadError),
    #[error("copying LSN prefix locally failed")]
    CopyDeltaPrefix(#[source] anyhow::Error),
    #[error("upload rewritten layer")]
    UploadRewritten(#[source] anyhow::Error),

    #[error("ancestor is already being detached by: {}", .0)]
    OtherTimelineDetachOngoing(TimelineId),

    #[error("remote copying layer failed")]
    CopyFailed(#[source] anyhow::Error),

    #[error("wait for tenant to activate after restarting")]
    WaitToActivate(#[source] GetActiveTenantError),

    #[error("unexpected error")]
    Unexpected(#[source] anyhow::Error),

    #[error("failpoint: {}", .0)]
    Failpoint(&'static str),
}

impl From<Error> for ApiError {
    fn from(value: Error) -> Self {
        match value {
            e @ Error::NoAncestor => ApiError::Conflict(e.to_string()),
            // TODO: ApiError converts the anyhow using debug formatting ... just stop using ApiError?
            e @ Error::TooManyAncestors => ApiError::BadRequest(anyhow::anyhow!("{}", e)),
            Error::ShuttingDown => ApiError::ShuttingDown,
            Error::OtherTimelineDetachOngoing(_) => {
                ApiError::ResourceUnavailable("other timeline detach is already ongoing".into())
            }
            e @ Error::WaitToActivate(_) => {
                let s = utils::error::report_compact_sources(&e).to_string();
                ApiError::ResourceUnavailable(s.into())
            }
            // All of these contain shutdown errors, in fact, it's the most common
            e @ Error::FlushAncestor(_)
            | e @ Error::RewrittenDeltaDownloadFailed(_)
            | e @ Error::CopyDeltaPrefix(_)
            | e @ Error::UploadRewritten(_)
            | e @ Error::CopyFailed(_)
            | e @ Error::Unexpected(_)
            | e @ Error::Failpoint(_) => ApiError::InternalServerError(e.into()),
        }
    }
}

impl From<crate::tenant::upload_queue::NotInitialized> for Error {
    fn from(_: crate::tenant::upload_queue::NotInitialized) -> Self {
        // treat all as shutting down signals, even though that is not entirely correct
        // (uninitialized state)
        Error::ShuttingDown
    }
}
impl From<super::layer_manager::Shutdown> for Error {
    fn from(_: super::layer_manager::Shutdown) -> Self {
        Error::ShuttingDown
    }
}

impl From<FlushLayerError> for Error {
    fn from(value: FlushLayerError) -> Self {
        match value {
            FlushLayerError::Cancelled => Error::ShuttingDown,
            FlushLayerError::NotRunning(_) => {
                // FIXME(#6424): technically statically unreachable right now, given how we never
                // drop the sender
                Error::ShuttingDown
            }
            FlushLayerError::CreateImageLayersError(_) | FlushLayerError::Other(_) => {
                Error::FlushAncestor(value)
            }
        }
    }
}

pub(crate) enum Progress {
    Prepared(Attempt, PreparedTimelineDetach),
    Done(AncestorDetached),
}

pub(crate) struct PreparedTimelineDetach {
    layers: Vec<Layer>,
}

/// TODO: this should be part of PageserverConf because we cannot easily modify cplane arguments.
#[derive(Debug)]
pub(crate) struct Options {
    pub(crate) rewrite_concurrency: std::num::NonZeroUsize,
    pub(crate) copy_concurrency: std::num::NonZeroUsize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            rewrite_concurrency: std::num::NonZeroUsize::new(2).unwrap(),
            copy_concurrency: std::num::NonZeroUsize::new(100).unwrap(),
        }
    }
}

/// SharedState manages the pausing of background tasks (GC) for the duration of timeline detach
/// ancestor.
///
/// Currently this is tracked at tenant level, but it could be moved to be on the roots
/// of each timeline tree.
#[derive(Default)]
pub(crate) struct SharedState {
    inner: std::sync::Mutex<SharedStateInner>,
    gc_waiting: tokio::sync::Notify,
    attempt_waiting: tokio::sync::Notify,
}

impl SharedState {
    /// Notify an uninitialized shared state that an attempt to detach timeline ancestor continues
    /// from previous instance.
    pub(crate) fn continue_existing_attempt(&self, attempt: &Attempt) {
        self.inner
            .lock()
            .unwrap()
            .continue_existing_attempt(attempt);
        self.gc_waiting.notify_one();
    }

    /// Only GC must be paused while a detach ancestor is ongoing. Compaction can happen, to aid
    /// with any ongoing ingestion. Compaction even after restart is ok because layers will not be
    /// removed until the detach has been persistently completed.
    pub(crate) fn attempt_blocks_gc(&self) -> bool {
        // if we have any started and not finished ancestor detaches, we must remain paused
        // and also let any trying to start operation know that we've paused.
        self.mark_witnessed_and_notify()
    }

    /// Sleep for the duration, while letting any ongoing ancestor_detach attempt know that gc has
    /// been paused.
    pub(crate) async fn gc_sleeping_while<T, F: std::future::Future<Output = T>>(
        &self,
        fut: F,
    ) -> T {
        // this needs to wrap the sleeping so that we can quickly let ancestor_detach continue
        let mut fut = std::pin::pin!(fut);

        loop {
            tokio::select! {
                x = &mut fut => { return x; },
                _ = self.gc_waiting.notified() => {
                    self.mark_witnessed_and_notify();
                }
            }
        }
    }

    fn mark_witnessed_and_notify(&self) -> bool {
        let mut g = self.inner.lock().unwrap();
        if let Some((_, witnessed)) = g.latest.as_mut() {
            if !*witnessed {
                *witnessed = true;
                self.attempt_waiting.notify_one();
            }

            true
        } else {
            false
        }
    }

    /// Acquire the exclusive lock for a new detach ancestor attempt and ensure that GC task has
    /// been persistently paused via [`crate::tenant::IndexPart`], awaiting for completion.
    ///
    /// Cancellation safe.
    async fn start_new_attempt(&self, detached: &Arc<Timeline>) -> Result<Attempt, Error> {
        if detached.cancel.is_cancelled() {
            return Err(Error::ShuttingDown);
        }

        let completion = {
            let mut guard = self.inner.lock().unwrap();
            let completion = guard.start_new(&detached.timeline_id)?;
            // now that we changed the contents, notify any long-sleeping gc
            self.gc_waiting.notify_one();
            completion
        };

        let started_at = std::time::Instant::now();
        let mut cancelled = std::pin::pin!(detached.cancel.cancelled());

        loop {
            tokio::select! {
                _ = &mut cancelled => { return Err(Error::ShuttingDown); },
                _ = self.attempt_waiting.notified() => {},
            };

            // reading a notification which was not intended for us is not a problem,
            // because we check if *our* progress has been witnessed by gc.
            let g = self.inner.lock().unwrap();
            if g.is_gc_paused(&detached.timeline_id) {
                break;
            }
        }

        // finally
        let gate_entered = detached.gate.enter().map_err(|_| Error::ShuttingDown)?;
        let synced_in = started_at.elapsed();

        detached
            .remote_client
            .schedule_started_detach_ancestor_mark_and_wait()
            .await
            // FIXME: aaaargh
            .map_err(|_| Error::ShuttingDown)?;

        let uploaded_in = started_at.elapsed() - synced_in;

        // FIXME: get rid of this logging or make it a metric or two
        tracing::info!(
            sync_ms = synced_in.as_millis(),
            upload_ms = uploaded_in.as_millis(),
            "gc paused, gate entered, and uploaded"
        );

        Ok(Attempt {
            timeline_id: detached.timeline_id,
            _guard: completion,
            gate_entered: Some(gate_entered),
        })
    }

    /// Completes a previously started detach ancestor attempt. To be called *after* the operation
    /// including the tenant has been restarted. The completion is persistent, and no reparentings
    /// can be done afterwards.
    ///
    /// Cancellation safe.
    pub(crate) async fn complete(
        &self,
        attempt: Attempt,
        tenant: &Arc<Tenant>,
    ) -> Result<(), Error> {
        // do we need the tenant to actually activate...? yes we do, but that can happen via
        // background task starting, because we will similarly want to confirm that the gc has
        // paused, before we unpause it?

        // assert that such and such state has been collected
        // find the timeline the attempt represents
        // using the timelines remote client, upload an index part with completion information

        {
            let g = self.inner.lock().unwrap();

            // TODO: cover the case where retry completes?
            g.validate(&attempt);
        }

        let mut attempt = scopeguard::guard(attempt, |attempt| {
            // our attempt will no longer be valid, so release it
            self.inner.lock().unwrap().cancel(attempt);
        });

        tenant
            .wait_to_become_active(std::time::Duration::from_secs(9999))
            .await
            .map_err(Error::WaitToActivate)?;

        // TODO: pause failpoint here to catch the situation where detached timeline is deleted...?
        // we are not yet holding the gate so it could advance to the point of removing from
        // timelines.

        let Some(timeline) = tenant
            .timelines
            .lock()
            .unwrap()
            .get(&attempt.timeline_id)
            .cloned()
        else {
            // FIXME: this needs a test case ... basically deletion right after activation?
            unreachable!("unsure if there is an ordering, but perhaps this is possible?");
        };

        // the gate being antered does not matter much, but lets be strict
        if attempt.gate_entered.is_none() {
            let entered = timeline.gate.enter().map_err(|_| Error::ShuttingDown)?;
            attempt.gate_entered = Some(entered);
        } else {
            // Some(gate_entered) means the tenant was not restarted, as is not required
        }

        // this should be an 503 at least...?
        fail::fail_point!(
            "timeline-detach-ancestor::complete_before_uploading",
            |_| Err(Error::Failpoint(
                "timeline-detach-ancestor::complete_before_uploading"
            ))
        );

        timeline
            .remote_client
            .schedule_completed_detach_ancestor_mark_and_wait()
            .await
            .map_err(|_| Error::ShuttingDown)?;

        // now that the upload has gone through, we must remove this timeline from inprogress
        let attempt = scopeguard::ScopeGuard::into_inner(attempt);

        self.inner.lock().unwrap().complete(attempt);

        Ok(())
    }

    pub(crate) fn cancel(&self, attempt: Attempt) {
        let mut g = self.inner.lock().unwrap();
        g.cancel(attempt);
        tracing::info!("keeping the gc blocking for retried detach_ancestor");
    }

    pub(crate) fn on_delete(&self, timeline: &Arc<Timeline>) {
        let mut g = self.inner.lock().unwrap();
        g.on_delete(&timeline.timeline_id);
    }
}

#[derive(Default)]
struct SharedStateInner {
    known_ongoing: std::collections::HashSet<TimelineId>,
    latest: Option<(ExistingAttempt, bool)>,
}

impl SharedStateInner {
    fn continue_existing_attempt(&mut self, attempt: &Attempt) {
        assert!(self.known_ongoing.is_empty());
        assert!(self.latest.is_none());
        self.known_ongoing.insert(attempt.timeline_id);
        self.latest = Some((
            ExistingAttempt::ContinuedOverRestart(attempt.timeline_id),
            false,
        ));
    }

    fn start_new(&mut self, detached: &TimelineId) -> Result<completion::Completion, Error> {
        let completion = if let Some((existing, witnessed)) = self.latest.as_mut() {
            let completion = existing.start_new(detached)?;
            *witnessed = false;
            completion
        } else {
            let (completion, attempt) = ExistingAttempt::new(detached);
            self.latest = Some((attempt, false));
            completion
        };

        self.known_ongoing.insert(*detached);

        Ok(completion)
    }

    fn is_gc_paused(&self, timeline_id: &TimelineId) -> bool {
        match &self.latest {
            Some((ExistingAttempt::Actual(x, _), paused)) => {
                assert_eq!(x, timeline_id);
                *paused
            }
            other => {
                unreachable!("unexpected state {other:?}")
            }
        }
    }

    fn validate(&self, attempt: &Attempt) {
        let (latest, _) = self
            .latest
            .as_ref()
            .expect("to validate there has to be an attempt");

        assert_eq!(latest, attempt);

        assert!(self.known_ongoing.contains(&attempt.timeline_id));
    }

    fn complete(&mut self, attempt: Attempt) {
        let (latest, witnessed) = self
            .latest
            .as_ref()
            .expect("there has to be a latest attempt to complete");
        assert_eq!(latest, attempt);
        let witnessed = *witnessed;

        assert!(self.known_ongoing.remove(&attempt.timeline_id));

        if self.known_ongoing.is_empty() {
            self.latest = None;
            tracing::info!("gc is now unblocked following completion");
        } else {
            self.latest = Some((ExistingAttempt::ReadFromIndexPart, witnessed));
            tracing::info!(
                n = self.known_ongoing.len(),
                "gc is still blocked for remaining ongoing detaches"
            );
        }
    }

    fn cancel(&mut self, attempt: Attempt) {
        let (latest, witnessed) = self
            .latest
            .as_ref()
            .expect("there has to be a latest to cancel");
        assert_eq!(latest, attempt);
        let witnessed = *witnessed;

        assert!(!self.known_ongoing.is_empty());
        self.latest = Some((ExistingAttempt::ReadFromIndexPart, witnessed));
    }

    fn on_delete(&mut self, timeline_id: &TimelineId) {
        let Some((attempt, witnessed)) = self.latest.as_ref() else {
            return;
        };

        if !self.known_ongoing.contains(timeline_id) {
            assert_ne!(attempt, timeline_id);
            return;
        }

        if let ExistingAttempt::ReadFromIndexPart = attempt {
            // ReadFromIndexPart is not equal to any attempt
        } else {
            assert_eq!(attempt, timeline_id);

            if let ExistingAttempt::Actual(_, barrier) = attempt {
                assert!(
                    barrier.is_ready(),
                    "the attempt is still ongoing; is this call happening before closing the gate?"
                );
            }
        }

        let witnessed = *witnessed;

        assert!(self.known_ongoing.remove(timeline_id));

        if self.known_ongoing.is_empty() {
            self.latest = None;
            tracing::info!("gc is now unblocked following timeline deletion");
        } else {
            self.latest = Some((ExistingAttempt::ReadFromIndexPart, witnessed));
            tracing::info!(
                n = self.known_ongoing.len(),
                "gc is still blocked for remaining ongoing detaches"
            );
        }
    }
}

#[derive(Debug)]
enum ExistingAttempt {
    /// Informative; there are still non-zero known ongoing timeline ancestor detaches
    ReadFromIndexPart,

    /// Exclusive lock carried over tenant reset
    ContinuedOverRestart(TimelineId),

    /// Exclusive while barrier shows the task running
    Actual(TimelineId, completion::Barrier),
}

impl PartialEq<Attempt> for &'_ ExistingAttempt {
    fn eq(&self, other: &Attempt) -> bool {
        use ExistingAttempt::*;
        match self {
            ReadFromIndexPart => false,
            ContinuedOverRestart(x) if x == &other.timeline_id => true,
            Actual(x, barrier) if x == &other.timeline_id && other._guard.blocks(barrier) => true,
            _ => false,
        }
    }
}

impl PartialEq<&'_ Attempt> for &'_ ExistingAttempt {
    fn eq(&self, other: &&Attempt) -> bool {
        self == *other
    }
}

impl PartialEq<&'_ TimelineId> for &'_ ExistingAttempt {
    fn eq(&self, other: &&TimelineId) -> bool {
        use ExistingAttempt::*;
        match self {
            ReadFromIndexPart => false,
            ContinuedOverRestart(x) | Actual(x, _) if x == *other => true,
            _ => false,
        }
    }
}

impl ExistingAttempt {
    fn start_new(&mut self, detached: &TimelineId) -> Result<completion::Completion, Error> {
        use ExistingAttempt::*;
        match self {
            ReadFromIndexPart => {}
            Actual(other, barrier) if barrier.is_ready() => {
                if other != detached {
                    tracing::warn!(prev=%other, next=%detached, "switching ongoing detach; this is not expected to happen normally, but doesn't necessarily mean anything catastrophic");
                }
            }
            Actual(other, _) | ContinuedOverRestart(other) => {
                return Err(Error::OtherTimelineDetachOngoing(*other))
            }
        }

        let (guard, attempt) = Self::new(detached);
        *self = attempt;
        Ok(guard)
    }

    fn new(detached: &TimelineId) -> (completion::Completion, Self) {
        let (guard, barrier) = completion::channel();
        (guard, ExistingAttempt::Actual(*detached, barrier))
    }
}

/// Represents an across tenant reset exclusive single attempt to detach ancestor.
#[derive(Debug)]
pub(crate) struct Attempt {
    timeline_id: TimelineId,

    _guard: completion::Completion,
    gate_entered: Option<utils::sync::gate::GateGuard>,
}

impl Attempt {
    pub(crate) fn before_shutdown(&mut self) {
        let taken = self.gate_entered.take();
        assert!(taken.is_some());
    }
}

#[derive(Default)]
pub(crate) struct SharedStateBuilder {
    inprogress: std::collections::HashSet<TimelineId>,
}

impl SharedStateBuilder {
    /// While loading, visit a timelines persistent [`crate::tenant::IndexPart`] and record if it is being
    /// detached.
    pub(crate) fn record_loading_timeline(
        &mut self,
        timeline_id: &TimelineId,
        index_part: &crate::tenant::IndexPart,
    ) {
        if index_part.ongoing_detach_ancestor.is_some() {
            // if the loading a timeline fails, tenant loading must fail as it does right now, or
            // something more elaborate needs to be done with this tracking
            self.inprogress.insert(*timeline_id);
        }
    }

    /// Merge the loaded not yet deleting in-progress to the existing datastructure.
    pub(crate) fn build(self, target: &SharedState) {
        let mut g = target.inner.lock().unwrap();

        assert_eq!(g.latest.is_none(), g.known_ongoing.is_empty());

        g.known_ongoing.extend(self.inprogress);
        if g.latest.is_none() && !g.known_ongoing.is_empty() {
            g.latest = Some((ExistingAttempt::ReadFromIndexPart, false));
            target.gc_waiting.notify_one();
        }
    }
}

/// See [`Timeline::prepare_to_detach_from_ancestor`]
pub(super) async fn prepare(
    detached: &Arc<Timeline>,
    tenant: &Tenant,
    options: Options,
    ctx: &RequestContext,
) -> Result<Progress, Error> {
    use Error::*;

    let Some((ancestor, ancestor_lsn)) = detached
        .ancestor_timeline
        .as_ref()
        .map(|tl| (tl.clone(), detached.ancestor_lsn))
    else {
        let still_in_progress = {
            let accessor = detached.remote_client.initialized_upload_queue()?;

            // we are safe to inspect the latest uploaded, because we can only witness this after
            // restart is complete and ancestor is no more.
            let latest = accessor.latest_uploaded_index_part();
            if latest.lineage.detached_previous_ancestor().is_none() {
                return Err(NoAncestor);
            };

            latest.gc_blocking.as_ref().is_some_and(|b| {
                b.blocked_by(
                    crate::tenant::remote_timeline_client::index::GcBlockingReason::DetachAncestor,
                )
            })
        };

        if still_in_progress {
            // gc is still blocked, we can still reparent and complete.
            //
            // this of course represents a challenge: how to *not* reparent branches which were not
            // there when we started? cannot, unfortunately, if not recorded to the ongoing_detach_ancestor.
            //
            // FIXME: if a new timeline had been created on ancestor which was reparentable between
            // the attempts, we could end up with it having different ancestry across shards. Fix
            // this by locking the parentable timelines before the operation starts, and storing
            // them in index_part.json.
            //
            // because the ancestor of detached is already set to none, we have published all
            // of the layers.
            let attempt = tenant
                .ongoing_timeline_detach
                .start_new_attempt(detached)
                .await?;
            return Ok(Progress::Prepared(
                attempt,
                PreparedTimelineDetach { layers: Vec::new() },
            ));
        }

        // `detached` has previously been detached; let's inspect each of the current timelines and
        // report back the timelines which have been reparented by our detach, or which are still
        // reparentable

        let reparented_timelines = reparented_direct_children(detached, tenant)?;
        return Ok(Progress::Done(AncestorDetached {
            reparented_timelines,
        }));
    };

    if !ancestor_lsn.is_valid() {
        // rare case, probably wouldn't even load
        tracing::error!("ancestor is set, but ancestor_lsn is invalid, this timeline needs fixing");
        return Err(NoAncestor);
    }

    if ancestor.ancestor_timeline.is_some() {
        // non-technical requirement; we could flatten N ancestors just as easily but we chose
        // not to, at least initially
        return Err(TooManyAncestors);
    }

    let attempt = tenant
        .ongoing_timeline_detach
        .start_new_attempt(detached)
        .await?;

    utils::pausable_failpoint!("timeline-detach-ancestor::before_starting_after_locking_pausable");

    fail::fail_point!(
        "timeline-detach-ancestor::before_starting_after_locking",
        |_| Err(Error::Failpoint(
            "timeline-detach-ancestor::before_starting_after_locking"
        ))
    );

    if ancestor_lsn >= ancestor.get_disk_consistent_lsn() {
        let span =
            tracing::info_span!("freeze_and_flush", ancestor_timeline_id=%ancestor.timeline_id);
        async {
            let started_at = std::time::Instant::now();
            let freeze_and_flush = ancestor.freeze_and_flush0();
            let mut freeze_and_flush = std::pin::pin!(freeze_and_flush);

            let res =
                tokio::time::timeout(std::time::Duration::from_secs(1), &mut freeze_and_flush)
                    .await;

            let res = match res {
                Ok(res) => res,
                Err(_elapsed) => {
                    tracing::info!("freezing and flushing ancestor is still ongoing");
                    freeze_and_flush.await
                }
            };

            res?;

            // we do not need to wait for uploads to complete but we do need `struct Layer`,
            // copying delta prefix is unsupported currently for `InMemoryLayer`.
            tracing::info!(
                elapsed_ms = started_at.elapsed().as_millis(),
                "froze and flushed the ancestor"
            );
            Ok::<_, Error>(())
        }
        .instrument(span)
        .await?;
    }

    let end_lsn = ancestor_lsn + 1;

    let (filtered_layers, straddling_branchpoint, rest_of_historic) = {
        // we do not need to start from our layers, because they can only be layers that come
        // *after* ancestor_lsn
        let layers = tokio::select! {
            guard = ancestor.layers.read() => guard,
            _ = detached.cancel.cancelled() => {
                return Err(ShuttingDown);
            }
            _ = ancestor.cancel.cancelled() => {
                return Err(ShuttingDown);
            }
        };

        // between retries, these can change if compaction or gc ran in between. this will mean
        // we have to redo work.
        partition_work(ancestor_lsn, &layers)?
    };

    // TODO: layers are already sorted by something: use that to determine how much of remote
    // copies are already done.
    tracing::info!(filtered=%filtered_layers, to_rewrite = straddling_branchpoint.len(), historic=%rest_of_historic.len(), "collected layers");

    // TODO: copying and lsn prefix copying could be done at the same time with a single fsync after
    let mut new_layers: Vec<Layer> =
        Vec::with_capacity(straddling_branchpoint.len() + rest_of_historic.len());

    {
        tracing::debug!(to_rewrite = %straddling_branchpoint.len(), "copying prefix of delta layers");

        let mut tasks = tokio::task::JoinSet::new();

        let mut wrote_any = false;

        let limiter = Arc::new(Semaphore::new(options.rewrite_concurrency.get()));

        for layer in straddling_branchpoint {
            let limiter = limiter.clone();
            let timeline = detached.clone();
            let ctx = ctx.detached_child(TaskKind::DetachAncestor, DownloadBehavior::Download);

            let span = tracing::info_span!("upload_rewritten_layer", %layer);
            tasks.spawn(
                async move {
                    let _permit = limiter.acquire().await;
                    let copied =
                        upload_rewritten_layer(end_lsn, &layer, &timeline, &timeline.cancel, &ctx)
                            .await?;
                    if let Some(copied) = copied.as_ref() {
                        tracing::info!(%copied, "rewrote and uploaded");
                    }
                    Ok(copied)
                }
                .instrument(span),
            );
        }

        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(Ok(Some(copied))) => {
                    wrote_any = true;
                    new_layers.push(copied);
                }
                Ok(Ok(None)) => {}
                Ok(Err(e)) => return Err(e),
                Err(je) => return Err(Unexpected(je.into())),
            }
        }

        // FIXME: the fsync should be mandatory, after both rewrites and copies
        if wrote_any {
            let timeline_dir = VirtualFile::open(
                &detached
                    .conf
                    .timeline_path(&detached.tenant_shard_id, &detached.timeline_id),
                ctx,
            )
            .await
            .fatal_err("VirtualFile::open for timeline dir fsync");
            timeline_dir
                .sync_all()
                .await
                .fatal_err("VirtualFile::sync_all timeline dir");
        }
    }

    let mut tasks = tokio::task::JoinSet::new();
    let limiter = Arc::new(Semaphore::new(options.copy_concurrency.get()));

    for adopted in rest_of_historic {
        let limiter = limiter.clone();
        let timeline = detached.clone();

        tasks.spawn(
            async move {
                let _permit = limiter.acquire().await;
                let owned =
                    remote_copy(&adopted, &timeline, timeline.generation, &timeline.cancel).await?;
                tracing::info!(layer=%owned, "remote copied");
                Ok(owned)
            }
            .in_current_span(),
        );
    }

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(owned)) => {
                new_layers.push(owned);
            }
            Ok(Err(failed)) => {
                return Err(failed);
            }
            Err(je) => return Err(Unexpected(je.into())),
        }
    }

    // TODO: fsync directory again if we hardlinked something

    let prepared = PreparedTimelineDetach { layers: new_layers };

    Ok(Progress::Prepared(attempt, prepared))
}

fn reparented_direct_children(
    detached: &Arc<Timeline>,
    tenant: &Tenant,
) -> Result<HashSet<TimelineId>, Error> {
    let mut all_direct_children = tenant
        .timelines
        .lock()
        .unwrap()
        .values()
        .filter_map(|tl| {
            let is_direct_child = matches!(tl.ancestor_timeline.as_ref(), Some(ancestor) if Arc::ptr_eq(ancestor, detached));

            if is_direct_child {
                Some(tl.clone())
            } else {
                if let Some(timeline) = tl.ancestor_timeline.as_ref() {
                    assert_ne!(timeline.timeline_id, detached.timeline_id, "we cannot have two timelines with the same timeline_id live");
                }
                None
            }
        })
        // Collect to avoid lock taking order problem with Tenant::timelines and
        // Timeline::remote_client
        .collect::<Vec<_>>();

    let mut any_shutdown = false;

    all_direct_children.retain(|tl| match tl.remote_client.initialized_upload_queue() {
        Ok(accessor) => accessor
            .latest_uploaded_index_part()
            .lineage
            .is_reparented(),
        Err(_shutdownalike) => {
            // not 100% a shutdown, but let's bail early not to give inconsistent results in
            // sharded enviroment.
            any_shutdown = true;
            true
        }
    });

    if any_shutdown {
        // it could be one or many being deleted; have client retry
        return Err(Error::ShuttingDown);
    }

    Ok(all_direct_children
        .into_iter()
        .map(|tl| tl.timeline_id)
        .collect())
}

fn partition_work(
    ancestor_lsn: Lsn,
    source: &LayerManager,
) -> Result<(usize, Vec<Layer>, Vec<Layer>), Error> {
    let mut straddling_branchpoint = vec![];
    let mut rest_of_historic = vec![];

    let mut later_by_lsn = 0;

    for desc in source.layer_map()?.iter_historic_layers() {
        // off by one chances here:
        // - start is inclusive
        // - end is exclusive
        if desc.lsn_range.start > ancestor_lsn {
            later_by_lsn += 1;
            continue;
        }

        let target = if desc.lsn_range.start <= ancestor_lsn
            && desc.lsn_range.end > ancestor_lsn
            && desc.is_delta
        {
            // TODO: image layer at Lsn optimization
            &mut straddling_branchpoint
        } else {
            &mut rest_of_historic
        };

        target.push(source.get_from_desc(&desc));
    }

    Ok((later_by_lsn, straddling_branchpoint, rest_of_historic))
}

async fn upload_rewritten_layer(
    end_lsn: Lsn,
    layer: &Layer,
    target: &Arc<Timeline>,
    cancel: &CancellationToken,
    ctx: &RequestContext,
) -> Result<Option<Layer>, Error> {
    use Error::UploadRewritten;
    let copied = copy_lsn_prefix(end_lsn, layer, target, ctx).await?;

    let Some(copied) = copied else {
        return Ok(None);
    };

    // FIXME: better shuttingdown error
    target
        .remote_client
        .upload_layer_file(&copied, cancel)
        .await
        .map_err(UploadRewritten)?;

    Ok(Some(copied.into()))
}

async fn copy_lsn_prefix(
    end_lsn: Lsn,
    layer: &Layer,
    target_timeline: &Arc<Timeline>,
    ctx: &RequestContext,
) -> Result<Option<ResidentLayer>, Error> {
    use Error::{CopyDeltaPrefix, RewrittenDeltaDownloadFailed, ShuttingDown};

    if target_timeline.cancel.is_cancelled() {
        return Err(ShuttingDown);
    }

    tracing::debug!(%layer, %end_lsn, "copying lsn prefix");

    let mut writer = DeltaLayerWriter::new(
        target_timeline.conf,
        target_timeline.timeline_id,
        target_timeline.tenant_shard_id,
        layer.layer_desc().key_range.start,
        layer.layer_desc().lsn_range.start..end_lsn,
        ctx,
    )
    .await
    .map_err(CopyDeltaPrefix)?;

    let resident = layer
        .download_and_keep_resident()
        .await
        // likely shutdown
        .map_err(RewrittenDeltaDownloadFailed)?;

    let records = resident
        .copy_delta_prefix(&mut writer, end_lsn, ctx)
        .await
        .map_err(CopyDeltaPrefix)?;

    drop(resident);

    tracing::debug!(%layer, records, "copied records");

    if records == 0 {
        drop(writer);
        // TODO: we might want to store an empty marker in remote storage for this
        // layer so that we will not needlessly walk `layer` on repeated attempts.
        Ok(None)
    } else {
        // reuse the key instead of adding more holes between layers by using the real
        // highest key in the layer.
        let reused_highest_key = layer.layer_desc().key_range.end;
        let (desc, path) = writer
            .finish(reused_highest_key, ctx)
            .await
            .map_err(CopyDeltaPrefix)?;
        let copied = Layer::finish_creating(target_timeline.conf, target_timeline, desc, &path)
            .map_err(CopyDeltaPrefix)?;

        tracing::debug!(%layer, %copied, "new layer produced");

        Ok(Some(copied))
    }
}

/// Creates a new Layer instance for the adopted layer, and ensures it is found from the remote
/// storage on successful return without the adopted layer being added to `index_part.json`.
async fn remote_copy(
    adopted: &Layer,
    adoptee: &Arc<Timeline>,
    generation: Generation,
    cancel: &CancellationToken,
) -> Result<Layer, Error> {
    use Error::CopyFailed;

    // depending if Layer::keep_resident we could hardlink

    let mut metadata = adopted.metadata();
    debug_assert!(metadata.generation <= generation);
    metadata.generation = generation;

    let owned = crate::tenant::storage_layer::Layer::for_evicted(
        adoptee.conf,
        adoptee,
        adopted.layer_desc().layer_name(),
        metadata,
    );

    // FIXME: better shuttingdown error
    adoptee
        .remote_client
        .copy_timeline_layer(adopted, &owned, cancel)
        .await
        .map(move |()| owned)
        .map_err(CopyFailed)
}

pub(crate) enum DetachingAndReparenting {
    /// All of the following timeline ids were reparented and the timeline ancestor detach must be
    /// marked as completed.
    Reparented(HashSet<TimelineId>),

    /// Some of the reparentings failed. The timeline ancestor detach must **not** be marked as
    /// completed.
    ///
    /// Nested `must_restart` is set to true when any restart requiring changes were made.
    SomeReparentingFailed { must_restart: bool },

    /// Detaching and reparentings were completed in a previous attempt. Timeline ancestor detach
    /// must be marked as completed.
    AlreadyDone(HashSet<TimelineId>),
}

impl DetachingAndReparenting {
    pub(crate) fn reset_tenant_required(&self) -> bool {
        use DetachingAndReparenting::*;
        match self {
            Reparented(_) => true,
            SomeReparentingFailed { must_restart } => *must_restart,
            AlreadyDone(_) => false,
        }
    }

    pub(crate) fn completed(self) -> Option<Vec<TimelineId>> {
        use DetachingAndReparenting::*;
        match self {
            Reparented(x) | AlreadyDone(x) => Some(x),
            SomeReparentingFailed { .. } => None,
        }
    }
}

/// See [`Timeline::detach_from_ancestor_and_reparent`].
pub(super) async fn detach_and_reparent(
    detached: &Arc<Timeline>,
    tenant: &Tenant,
    prepared: PreparedTimelineDetach,
    _ctx: &RequestContext,
) -> Result<DetachingAndReparenting, anyhow::Error> {
    let PreparedTimelineDetach { layers } = prepared;

    #[derive(Debug)]
    enum Ancestor {
        NotDetached(Arc<Timeline>, Lsn),
        Detached(Arc<Timeline>, Lsn),
    }

    let (recorded_branchpoint, detach_is_ongoing) = {
        let access = detached.remote_client.initialized_upload_queue()?;
        let latest = access.latest_uploaded_index_part();

        (
            latest.lineage.detached_previous_ancestor(),
            latest.gc_blocking.as_ref().is_some_and(|b| {
                b.blocked_by(
                    crate::tenant::remote_timeline_client::index::GcBlockingReason::DetachAncestor,
                )
            }),
        )
    };

    let ancestor = if let Some(ancestor) = detached.ancestor_timeline.as_ref() {
        assert!(
            recorded_branchpoint.is_none(),
            "it should be impossible to get to here without having gone through the tenant reset"
        );
        Ancestor::NotDetached(ancestor.clone(), detached.ancestor_lsn)
    } else if let Some((ancestor_id, ancestor_lsn)) = recorded_branchpoint {
        // it has been either:
        // - detached but still exists => we can try reparenting
        // - detached and deleted
        //
        // either way, we must complete
        assert!(
            layers.is_empty(),
            "no layers should had been copied as detach is done"
        );
        let existing = tenant.timelines.lock().unwrap().get(&ancestor_id).cloned();

        if let Some(ancestor) = existing {
            Ancestor::Detached(ancestor, ancestor_lsn)
        } else {
            let direct_children = reparented_direct_children(detached, tenant)?;
            return Ok(DetachingAndReparenting::AlreadyDone(direct_children));
        }
    } else {
        // TODO: make sure there are no `?` before tenant_reset from after a questionmark from
        // here.
        panic!("bug: complete called on a timeline which has not been detached or which has no live ancestor");
    };

    // publish the prepared layers before we reparent any of the timelines, so that on restart
    // reparented timelines find layers. also do the actual detaching.
    //
    // if we crash after this operation, a retry will allow reparenting the remaining timelines as
    // gc is blocked.
    assert!(
        detach_is_ongoing,
        "to detach and reparent, gc must still be blocked"
    );

    let (ancestor, ancestor_lsn, was_detached) = match ancestor {
        Ancestor::NotDetached(ancestor, ancestor_lsn) => {
            // this has to complete before any reparentings because otherwise they would not have
            // layers on the new parent.
            detached
                .remote_client
                .schedule_adding_existing_layers_to_index_detach_and_wait(
                    &layers,
                    (ancestor.timeline_id, ancestor_lsn),
                )
                .await
                .context("publish layers and detach ancestor")?;

            (ancestor, ancestor_lsn, true)
        }
        Ancestor::Detached(ancestor, ancestor_lsn) => (ancestor, ancestor_lsn, false),
    };

    let mut tasks = tokio::task::JoinSet::new();

    // Returns a single permit semaphore which will be used to make one reparenting succeed,
    // others will fail as if those timelines had been stopped for whatever reason.
    #[cfg(feature = "testing")]
    let failpoint_sem = || -> Option<Arc<Semaphore>> {
        fail::fail_point!("timeline-detach-ancestor::allow_one_reparented", |_| Some(
            Arc::new(Semaphore::new(1))
        ));
        None
    }();

    // because we are now keeping the slot in progress, it is unlikely that there will be any
    // timeline deletions during this time. if we raced one, then we'll just ignore it.
    tenant
        .timelines
        .lock()
        .unwrap()
        .values()
        .filter_map(|tl| {
            if Arc::ptr_eq(tl, detached) {
                return None;
            }

            let tl_ancestor = tl.ancestor_timeline.as_ref()?;
            let is_same = Arc::ptr_eq(&ancestor, tl_ancestor);
            let is_earlier = tl.get_ancestor_lsn() <= ancestor_lsn;

            let is_deleting = tl
                .delete_progress
                .try_lock()
                .map(|flow| !flow.is_not_started())
                .unwrap_or(true);

            if is_same && is_earlier && !is_deleting {
                Some(tl.clone())
            } else {
                None
            }
        })
        .for_each(|timeline| {
            // important in this scope: we are holding the Tenant::timelines lock
            let span = tracing::info_span!("reparent", reparented=%timeline.timeline_id);
            let new_parent = detached.timeline_id;
            #[cfg(feature = "testing")]
            let failpoint_sem = failpoint_sem.clone();

            tasks.spawn(
                async move {
                    let res = async {
                        #[cfg(feature = "testing")]
                        if let Some(failpoint_sem) = failpoint_sem {
                            let _permit = failpoint_sem.acquire().await.map_err(|_| {
                                anyhow::anyhow!(
                                    "failpoint: timeline-detach-ancestor::allow_one_reparented",
                                )
                            })?;
                            failpoint_sem.close();
                        }

                        timeline
                            .remote_client
                            .schedule_reparenting_and_wait(&new_parent)
                            .await
                    }
                    .await;

                    match res {
                        Ok(()) => {
                            tracing::info!("reparented");
                            Some(timeline)
                        }
                        Err(e) => {
                            // with the use of tenant slot, raced timeline deletion is the most
                            // likely reason.
                            tracing::warn!("reparenting failed: {e:#}");
                            None
                        }
                    }
                }
                .instrument(span),
            );
        });

    let reparenting_candidates = tasks.len();
    let mut reparented = HashSet::with_capacity(tasks.len());

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Some(timeline)) => {
                assert!(
                    reparented.insert(timeline.timeline_id),
                    "duplicate reparenting? timeline_id={}",
                    timeline.timeline_id
                );
            }
            Ok(None) => {
                // lets just ignore this for now. one or all reparented timelines could had
                // started deletion, and that is fine. deleting a timeline is the most likely
                // reason for this.
            }
            Err(je) if je.is_cancelled() => unreachable!("not used"),
            Err(je) if je.is_panic() => {
                // ignore; it's better to continue with a single reparenting failing (or even
                // all of them) in order to get to the goal state. we will retry this after
                // restart.
            }
            Err(je) => tracing::error!("unexpected join error: {je:?}"),
        }
    }

    let reparented_all = reparenting_candidates == reparented.len();

    if reparented_all {
        Ok(DetachingAndReparenting::Reparented(reparented))
    } else {
        // FIXME: we must return 503 kind of response in the end and do the restart anyways because
        // otherwise the runtime state remains diverged
        tracing::info!(
            reparented = reparented.len(),
            candidates = reparenting_candidates,
            "failed to reparent all candidates; they will be retried after the restart",
        );

        Ok(DetachingAndReparenting::SomeReparentingFailed {
            must_restart: !reparented.is_empty() || was_detached,
        })
    }
}
