use std::{collections::HashSet, sync::Arc};

use super::{layer_manager::LayerManager, FlushLayerError, Timeline};
use crate::{
    context::{DownloadBehavior, RequestContext},
    task_mgr::TaskKind,
    tenant::{
        storage_layer::{AsLayerDesc as _, DeltaLayerWriter, Layer, ResidentLayer},
        Tenant,
    },
    virtual_file::{MaybeFatalIo, VirtualFile},
};
use pageserver_api::models::detach_ancestor::AncestorDetached;
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
pub(crate) struct SharedState {
    inner: std::sync::Mutex<Option<(TimelineId, completion::Barrier)>>,
}

impl Default for SharedState {
    fn default() -> Self {
        SharedState {
            inner: Default::default(),
        }
    }
}

impl SharedState {
    /// Notify an uninitialized shared state that an attempt to detach timeline ancestor continues
    /// from previous instance.
    pub(crate) fn notify(&self, attempt: &Attempt) {}

    /// Only GC must be paused while a detach ancestor is ongoing. Compaction can happen, to aid
    /// with any ongoing ingestion. Compaction even after restart is ok because layers will not be
    /// removed until the detach has been persistently completed.
    pub(crate) fn attempt_blocks_gc(&self) -> bool {
        // if we have any started and not finished ancestor detaches, we must remain paused
        // and also let any trying to start operation know that we've paused.

        // Two cases:
        // - there is an actual attempt started
        // - we have learned from indexparts that an attempt will be retried in near future
        self.inner.lock().unwrap().is_some()
    }

    /// Sleep for the duration, while letting any ongoing ancestor_detach attempt know that gc has
    /// been paused.
    ///
    /// Cancellation safe.
    pub(crate) async fn gc_sleeping_while<T, F: std::future::Future<Output = T>>(
        &self,
        fut: F,
    ) -> T {
        // this needs to wrap the sleeping so that we can quickly let ancestor_detach continue
        //
        // FIXME: with the on_gc_task_start this might be unnecessary? no, how would we otherwise
        // know about gc attempt ending in a failure and sleeping 300s. perhaps the
        // on_gc_task_start is unnecessary? no, it is needed to know if we ever need to wait for
        // to start?
        fut.await
    }

    /// Acquire the exclusive lock for a new detach ancestor attempt and ensure that GC task has
    /// been persistently paused via [`crate::tenant::IndexPart`], awaiting for completion.
    ///
    /// Cancellation safe.
    async fn start_new_attempt(&self, detached: &Arc<Timeline>) -> Result<Attempt, Error> {
        if detached.cancel.is_cancelled() {
            return Err(Error::ShuttingDown);
        }

        let (guard, barrier) = completion::channel();

        {
            let mut guard = self.inner.lock().unwrap();
            if let Some((tl, other)) = guard.as_ref() {
                if !other.is_ready() {
                    return Err(Error::OtherTimelineDetachOngoing(*tl));
                }
            }
            *guard = Some((detached.timeline_id, barrier));
        }

        // FIXME: modify the index part to have a "detach-ancestor: inprogress { started_at }"
        // unsure if it should be awaited to upload yet...

        // finally
        let gate_entered = detached.gate.enter().map_err(|_| Error::ShuttingDown)?;

        Ok(Attempt {
            _guard: guard,
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
        _attempt: Attempt,
        _tenant: &Arc<Tenant>,
    ) -> Result<(), Error> {
        // do we need the tenant to actually activate...? yes we do, but that can happen via
        // background task starting, because we will similarly want to confirm that the gc has
        // paused, before we unpause it?
        //
        // assert that such and such state has been collected
        // find the timeline the attempt represents
        // using the timelines remote client, upload an index part with completion information
        Ok(())
    }
}

/// Token which represents a persistent, exclusive, awaitable single attempt.
pub(crate) struct Attempt {
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
pub(crate) struct SharedStateBuilder {}

impl SharedStateBuilder {
    /// While loading, visit a timelines persistent [`crate::tenant::IndexPart`] and record if it is being
    /// detached.
    pub(crate) fn record_loading_timeline(
        &mut self,
        _timeline_id: &TimelineId,
        _index_part: &crate::tenant::IndexPart,
    ) {
    }

    /// Merge the loaded not yet deleting in-progress to the existing datastructure.
    pub(crate) fn build(self, _target: &SharedState) {}
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
        {
            let accessor = detached.remote_client.initialized_upload_queue()?;

            // we are safe to inspect the latest uploaded, because we can only witness this after
            // restart is complete and ancestor is no more.
            let latest = accessor.latest_uploaded_index_part();
            if !latest.lineage.is_detached_from_original_ancestor() {
                return Err(NoAncestor);
            }
        }

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

        let limiter = Arc::new(tokio::sync::Semaphore::new(
            options.rewrite_concurrency.get(),
        ));

        for layer in straddling_branchpoint {
            let limiter = limiter.clone();
            let timeline = detached.clone();
            let ctx = ctx.detached_child(TaskKind::DetachAncestor, DownloadBehavior::Download);

            tasks.spawn(async move {
                let _permit = limiter.acquire().await;
                let copied =
                    upload_rewritten_layer(end_lsn, &layer, &timeline, &timeline.cancel, &ctx)
                        .await?;
                Ok(copied)
            });
        }

        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(Ok(Some(copied))) => {
                    wrote_any = true;
                    tracing::info!(layer=%copied, "rewrote and uploaded");
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
    let limiter = Arc::new(tokio::sync::Semaphore::new(options.copy_concurrency.get()));

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

/// See [`Timeline::complete_detaching_timeline_ancestor`].
pub(super) async fn complete(
    detached: &Arc<Timeline>,
    tenant: &Tenant,
    prepared: PreparedTimelineDetach,
    _ctx: &RequestContext,
) -> Result<HashSet<TimelineId>, anyhow::Error> {
    let PreparedTimelineDetach { layers } = prepared;

    let ancestor = detached
        .ancestor_timeline
        .as_ref()
        .expect("must still have a ancestor");
    let ancestor_lsn = detached.get_ancestor_lsn();

    // publish the prepared layers before we reparent any of the timelines, so that on restart
    // reparented timelines find layers. also do the actual detaching.
    //
    // if we crash after this operation, we will at least come up having detached a timeline, but
    // we cannot go back and reparent the timelines which would had been reparented in normal
    // execution.
    //
    // this is not perfect, but it avoids us a retry happening after a compaction or gc on restart
    // which could give us a completely wrong layer combination.
    detached
        .remote_client
        .schedule_adding_existing_layers_to_index_detach_and_wait(
            &layers,
            (ancestor.timeline_id, ancestor_lsn),
        )
        .await?;

    // FIXME: assert that the persistent record of inprogress detach exists
    // FIXME: assert that gc is still blocked

    let mut tasks = tokio::task::JoinSet::new();

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

            if !tl.is_active() {
                return None;
            }

            let tl_ancestor = tl.ancestor_timeline.as_ref()?;
            let is_same = Arc::ptr_eq(ancestor, tl_ancestor);
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

            tasks.spawn(
                async move {
                    let res = timeline
                        .remote_client
                        .schedule_reparenting_and_wait(&new_parent)
                        .await;

                    match res {
                        Ok(()) => Some(timeline),
                        Err(e) => {
                            // with the use of tenant slot, we no longer expect these.
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
                tracing::info!(reparented=%timeline.timeline_id, "reparenting done");

                assert!(
                    reparented.insert(timeline.timeline_id),
                    "duplicate reparenting? timeline_id={}",
                    timeline.timeline_id
                );
            }
            Ok(None) => {
                // lets just ignore this for now. one or all reparented timelines could had
                // started deletion, and that is fine.
            }
            Err(je) if je.is_cancelled() => unreachable!("not used"),
            Err(je) if je.is_panic() => {
                // ignore; it's better to continue with a single reparenting failing (or even
                // all of them) in order to get to the goal state.
                //
                // these timelines will never be reparentable, but they can be always detached as
                // separate tree roots.
            }
            Err(je) => tracing::error!("unexpected join error: {je:?}"),
        }
    }

    if reparenting_candidates != reparented.len() {
        // FIXME: we must return 503 kind of response
        tracing::info!("failed to reparent some candidates");
    }

    // FIXME: here everything has gone peachy, the tenant will be restarted next.
    // after restart and before returning the response, the gc blocking must be undone
    Ok(reparented)
}
