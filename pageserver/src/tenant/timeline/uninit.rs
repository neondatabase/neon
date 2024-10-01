use std::{collections::hash_map::Entry, fs, sync::Arc};

use anyhow::Context;
use camino::Utf8PathBuf;
use tracing::{error, info, info_span};
use utils::{fs_ext, id::TimelineId, lsn::Lsn};

use crate::{
    context::RequestContext,
    import_datadir,
    tenant::{
        metadata::TimelineMetadata,
        remote_timeline_client::MaybeDeletedIndexPart,
        timeline::{import_pgdata, ShutdownMode},
        Tenant,
    },
};

use super::Timeline;

/// A timeline with some of its files on disk, being initialized.
/// This struct ensures the atomicity of the timeline init: it's either properly created and inserted into pageserver's memory, or
/// its local files are removed.  If we crash while this class exists, then the timeline's local
/// state is cleaned up during [`Tenant::clean_up_timelines`], because the timeline's content isn't in remote storage.
///
/// The caller is responsible for proper timeline data filling before the final init.
#[must_use]
pub struct UninitializedTimeline<'t> {
    pub(crate) owning_tenant: &'t Tenant,
    timeline_id: TimelineId,
    raw_timeline: Option<(Arc<Timeline>, TimelineCreateGuard<'t>)>,
}

impl<'t> UninitializedTimeline<'t> {
    pub(crate) fn new(
        owning_tenant: &'t Tenant,
        timeline_id: TimelineId,
        raw_timeline: Option<(Arc<Timeline>, TimelineCreateGuard<'t>)>,
    ) -> Self {
        Self {
            owning_tenant,
            timeline_id,
            raw_timeline,
        }
    }

    /// Finish timeline creation: insert it into the Tenant's timelines map
    ///
    /// This function launches the flush loop if not already done.
    ///
    /// The caller is responsible for activating the timeline (function `.activate()`).
    pub(crate) fn finish_creation(mut self) -> anyhow::Result<Arc<Timeline>> {
        let timeline_id = self.timeline_id;
        let tenant_shard_id = self.owning_tenant.tenant_shard_id;

        if self.raw_timeline.is_none() {
            return Err(anyhow::anyhow!(
                "No timeline for initialization found for {tenant_shard_id}/{timeline_id}"
            ));
        }

        // Check that the caller initialized disk_consistent_lsn
        let new_disk_consistent_lsn = self
            .raw_timeline
            .as_ref()
            .expect("checked above")
            .0
            .get_disk_consistent_lsn();

        anyhow::ensure!(
            new_disk_consistent_lsn.is_valid(),
            "new timeline {tenant_shard_id}/{timeline_id} has invalid disk_consistent_lsn"
        );

        let mut timelines = self.owning_tenant.timelines.lock().unwrap();
        match timelines.entry(timeline_id) {
            Entry::Occupied(_) => anyhow::bail!(
                "Found freshly initialized timeline {tenant_shard_id}/{timeline_id} in the tenant map"
            ),
            Entry::Vacant(v) => {
                // after taking here should be no fallible operations, because the drop guard will not
                // cleanup after and would block for example the tenant deletion
                let (new_timeline, _create_guard) =
                    self.raw_timeline.take().expect("already checked");

                v.insert(Arc::clone(&new_timeline));

                new_timeline.maybe_spawn_flush_loop();

                Ok(new_timeline)
            }
        }
    }

    /// Finish creation by dropping the in-memory struct and reloading the timeline from object storage.
    ///
    /// XXX factor this out into a method on tenant and
    /// share code with Tenant::attach
    /// Like, Tenant::reload_timeline() or whatever.
    ///
    /// Returns a not-activated timeline that's already in the tenant's timelines map.
    ///
    // Not recoverable.
    pub(crate) async fn finish_by_reloading_timeline_from_remote(
        mut self,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<Timeline>> {
        let tenant = self.owning_tenant;
        let timeline_id = self.timeline_id;
        let tenant_shard_id = tenant.tenant_shard_id;

        let Some((raw_timeline, _create_guard)) = self.raw_timeline.take() else {
            return Err(anyhow::anyhow!(
                "No timeline for initialization found for {tenant_shard_id}/{timeline_id}"
            ));
        };

        raw_timeline.shutdown(ShutdownMode::Hard).await; // in theory this shouldn't even .await anything except for coop yield
        let Some(raw_timeline) = Arc::into_inner(raw_timeline) else {
            anyhow::bail!("implementation error: timeline that we shut down was still referenced from somewhere");
        };
        drop(raw_timeline);

        // load from object storage like Tenant::attach does
        let resources = tenant.make_timeline_resources(timeline_id);
        let index_part = resources
            .remote_client
            .download_index_file(&tenant.cancel)
            .await?;
        let index_part = match index_part {
            MaybeDeletedIndexPart::Deleted(_) => {
                // likely concurrent delete call, cplane should prevent this
                anyhow::bail!("index part says deleted but we are not done creating yet, this should not happen but")
            }
            MaybeDeletedIndexPart::IndexPart(p) => p,
        };
        let metadata = index_part.metadata.clone();
        let timeline = self
            .owning_tenant
            .load_remote_timeline(timeline_id, index_part, metadata, resources, ctx)
            .await?;
        Ok(timeline)
    }

    /// Prepares timeline data by loading it from the basebackup archive.
    pub(crate) async fn import_basebackup_from_tar(
        self,
        tenant: Arc<Tenant>,
        copyin_read: &mut (impl tokio::io::AsyncRead + Send + Sync + Unpin),
        base_lsn: Lsn,
        broker_client: storage_broker::BrokerClientChannel,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<Timeline>> {
        let raw_timeline = self.raw_timeline()?;

        import_datadir::import_basebackup_from_tar(raw_timeline, copyin_read, base_lsn, ctx)
            .await
            .context("Failed to import basebackup")?;

        // Flush the new layer files to disk, before we make the timeline as available to
        // the outside world.
        //
        // Flush loop needs to be spawned in order to be able to flush.
        raw_timeline.maybe_spawn_flush_loop();

        fail::fail_point!("before-checkpoint-new-timeline", |_| {
            anyhow::bail!("failpoint before-checkpoint-new-timeline");
        });

        raw_timeline
            .freeze_and_flush()
            .await
            .context("Failed to flush after basebackup import")?;

        // All the data has been imported. Insert the Timeline into the tenant's timelines map
        let tl = self.finish_creation()?;
        tl.activate(tenant, broker_client, None, ctx);
        Ok(tl)
    }

    /// Prepares timeline data by loading it from the basebackup archive.
    pub(crate) async fn import_pgdata(
        self,
        tenant: Arc<Tenant>,
        prepared: import_pgdata::Prepared,
        broker_client: storage_broker::BrokerClientChannel,
        ctx: &RequestContext,
    ) -> anyhow::Result<Arc<Timeline>> {
        // Do the import while everything is at lsn 0.
        // The index parts that get uploaded during the import will look invalid.

        let base_lsn = prepared.base_lsn();

        // This fills the layer map with layers.
        import_pgdata::doit(&self, prepared, ctx)
            .await
            .context("import")?;

        let raw_timeline = self.raw_timeline()?;

        // FIXME: The 'disk_consistent_lsn' should be the LSN at the *end* of the
        // checkpoint record, and prev_record_lsn should point to its beginning.
        // We should read the real end of the record from the WAL, but here we
        // just fake it.
        let disk_consistent_lsn = Lsn(base_lsn.0 + 8);
        let prev_record_lsn = Some(base_lsn);
        let metadata = TimelineMetadata::new(
            disk_consistent_lsn,
            prev_record_lsn,
            None,     // no ancestor
            Lsn(0),   // no ancestor lsn
            base_lsn, // latest_gc_cutoff_lsn
            base_lsn, // initdb_lsn
            raw_timeline.pg_version,
        );
        raw_timeline
            .remote_client
            .schedule_index_upload_for_full_metadata_update(&metadata)?;
        raw_timeline.remote_client.wait_completion().await?;
        // TODO: what guarantees _today_ and _in the future_ that no more uploads
        // will happen after? Maybe just shutdown the timeline
        // => for now, we'll put in a bunch of assertions after reloading

        let tl = self.finish_by_reloading_timeline_from_remote(ctx).await?;

        assert_eq!(tl.disk_consistent_lsn.load(), disk_consistent_lsn);
        assert_eq!(tl.get_prev_record_lsn(), prev_record_lsn.unwrap());
        assert_eq!(tl.initdb_lsn, base_lsn);
        assert_eq!(*tl.latest_gc_cutoff_lsn.read(), base_lsn);
        // TODO: assert remote timeline client's metadata eq exactly our `metadata` variable

        tl.activate(tenant, broker_client, None, ctx);
        Ok(tl)
    }

    pub(crate) fn raw_timeline(&self) -> anyhow::Result<&Arc<Timeline>> {
        Ok(&self
            .raw_timeline
            .as_ref()
            .with_context(|| {
                format!(
                    "No raw timeline {}/{} found",
                    self.owning_tenant.tenant_shard_id, self.timeline_id
                )
            })?
            .0)
    }
}

impl Drop for UninitializedTimeline<'_> {
    fn drop(&mut self) {
        if let Some((_, create_guard)) = self.raw_timeline.take() {
            let _entered = info_span!("drop_uninitialized_timeline", tenant_id = %self.owning_tenant.tenant_shard_id.tenant_id, shard_id = %self.owning_tenant.tenant_shard_id.shard_slug(), timeline_id = %self.timeline_id).entered();
            error!("Timeline got dropped without initializing, cleaning its files");
            cleanup_timeline_directory(create_guard);
        }
    }
}

pub(crate) fn cleanup_timeline_directory(create_guard: TimelineCreateGuard) {
    let timeline_path = &create_guard.timeline_path;
    match fs_ext::ignore_absent_files(|| fs::remove_dir_all(timeline_path)) {
        Ok(()) => {
            info!("Timeline dir {timeline_path:?} removed successfully")
        }
        Err(e) => {
            error!("Failed to clean up uninitialized timeline directory {timeline_path:?}: {e:?}")
        }
    }
    // Having cleaned up, we can release this TimelineId in `[Tenant::timelines_creating]` to allow other
    // timeline creation attempts under this TimelineId to proceed
    drop(create_guard);
}

/// A guard for timeline creations in process: as long as this object exists, the timeline ID
/// is kept in `[Tenant::timelines_creating]` to exclude concurrent attempts to create the same timeline.
#[must_use]
pub(crate) struct TimelineCreateGuard<'t> {
    owning_tenant: &'t Tenant,
    timeline_id: TimelineId,
    pub(crate) timeline_path: Utf8PathBuf,
}

/// Errors when acquiring exclusive access to a timeline ID for creation
#[derive(thiserror::Error, Debug)]
pub(crate) enum TimelineExclusionError {
    #[error("Already exists")]
    AlreadyExists(Arc<Timeline>),
    #[error("Already creating")]
    AlreadyCreating,

    // e.g. I/O errors, or some failure deep in postgres initdb
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl<'t> TimelineCreateGuard<'t> {
    pub(crate) fn new(
        owning_tenant: &'t Tenant,
        timeline_id: TimelineId,
        timeline_path: Utf8PathBuf,
    ) -> Result<Self, TimelineExclusionError> {
        // Lock order: this is the only place we take both locks.  During drop() we only
        // lock creating_timelines
        let timelines = owning_tenant.timelines.lock().unwrap();
        let mut creating_timelines: std::sync::MutexGuard<
            '_,
            std::collections::HashSet<TimelineId>,
        > = owning_tenant.timelines_creating.lock().unwrap();

        if let Some(existing) = timelines.get(&timeline_id) {
            Err(TimelineExclusionError::AlreadyExists(existing.clone()))
        } else if creating_timelines.contains(&timeline_id) {
            Err(TimelineExclusionError::AlreadyCreating)
        } else {
            creating_timelines.insert(timeline_id);
            Ok(Self {
                owning_tenant,
                timeline_id,
                timeline_path,
            })
        }
    }
}

impl Drop for TimelineCreateGuard<'_> {
    fn drop(&mut self) {
        self.owning_tenant
            .timelines_creating
            .lock()
            .unwrap()
            .remove(&self.timeline_id);
    }
}
