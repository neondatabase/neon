use std::{collections::hash_map::Entry, fs, sync::Arc};

use anyhow::Context;
use camino::Utf8PathBuf;
use tracing::{info, info_span, warn};
use utils::{
    crashsafe,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use crate::{
    config::PageServerConf,
    context::RequestContext,
    import_datadir,
    tenant::Tenant,
    virtual_file::{on_fatal_io_error, MaybeFatalIo},
};

use super::Timeline;

/// A timeline with some of its files on disk, being initialized.
/// This struct ensures the atomicity of the timeline init: it's either properly created and inserted into pageserver's memory, or
/// its local files are removed. In the worst case of a crash, an uninit mark file is left behind, which causes the directory
/// to be removed on next restart.
///
/// The caller is responsible for proper timeline data filling before the final init.
#[must_use]
pub struct UninitializedTimeline<'t> {
    pub(crate) owning_tenant: &'t Tenant,
    timeline_id: TimelineId,
    raw_timeline: Option<(Arc<Timeline>, TimelineUninitMark)>,
}

impl<'t> UninitializedTimeline<'t> {
    pub(crate) fn new(
        owning_tenant: &'t Tenant,
        timeline_id: TimelineId,
        raw_timeline: Option<(Arc<Timeline>, TimelineUninitMark)>,
    ) -> Self {
        Self {
            owning_tenant,
            timeline_id,
            raw_timeline,
        }
    }

    /// Finish timeline creation: insert it into the Tenant's timelines map and remove the
    /// uninit mark file.
    ///
    /// This function launches the flush loop if not already done.
    ///
    /// The caller is responsible for activating the timeline (function `.activate()`).
    pub(crate) fn finish_creation(mut self) -> anyhow::Result<Arc<Timeline>> {
        let timeline_id = self.timeline_id;
        let tenant_id = self.owning_tenant.tenant_id;

        let (new_timeline, uninit_mark) = self.raw_timeline.take().with_context(|| {
            format!("No timeline for initalization found for {tenant_id}/{timeline_id}")
        })?;

        // Check that the caller initialized disk_consistent_lsn
        let new_disk_consistent_lsn = new_timeline.get_disk_consistent_lsn();
        anyhow::ensure!(
            new_disk_consistent_lsn.is_valid(),
            "new timeline {tenant_id}/{timeline_id} has invalid disk_consistent_lsn"
        );

        let mut timelines = self.owning_tenant.timelines.lock().unwrap();
        match timelines.entry(timeline_id) {
            Entry::Occupied(_) => anyhow::bail!(
                "Found freshly initialized timeline {tenant_id}/{timeline_id} in the tenant map"
            ),
            Entry::Vacant(v) => {
                uninit_mark.remove_uninit_mark();
                v.insert(Arc::clone(&new_timeline));
                new_timeline.maybe_spawn_flush_loop();
            }
        }

        Ok(new_timeline)
    }

    /// Prepares timeline data by loading it from the basebackup archive.
    pub(crate) async fn import_basebackup_from_tar(
        self,
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

        // All the data has been imported. Insert the Timeline into the tenant's timelines
        // map and remove the uninit mark file.
        let tl = self.finish_creation()?;
        tl.activate(broker_client, None, ctx);
        Ok(tl)
    }

    pub(crate) fn raw_timeline(&self) -> anyhow::Result<&Arc<Timeline>> {
        Ok(&self
            .raw_timeline
            .as_ref()
            .with_context(|| {
                format!(
                    "No raw timeline {}/{} found",
                    self.owning_tenant.tenant_id, self.timeline_id
                )
            })?
            .0)
    }
}

/// An uninit mark file, created along the timeline dir to ensure the timeline either gets fully initialized and loaded into pageserver's memory,
/// or gets removed eventually.
///
/// XXX: it's important to create it near the timeline dir, not inside it to ensure timeline dir gets removed first.
#[must_use]
pub(crate) struct TimelineUninitMark {
    uninit_mark_deleted: bool,
    uninit_mark_path: Utf8PathBuf,
    pub(crate) timeline_path: Utf8PathBuf,
    common_parent: Utf8PathBuf,
}

impl TimelineUninitMark {
    pub(crate) fn new(
        conf: &'static PageServerConf,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> anyhow::Result<Self> {
        let timeline_path = conf.timeline_path(&tenant_id, &timeline_id);
        let uninit_mark_path = conf.timeline_uninit_mark_file_path(tenant_id, timeline_id);

        // assert they share the same parent
        let timeline_parent_path = timeline_path
            .parent()
            .expect("timeline_path must have a parent");
        let uninit_mark_parent_path = uninit_mark_path
            .parent()
            .expect("uninit mark path must have a parent");
        assert_eq!(timeline_parent_path, uninit_mark_parent_path);
        let common_parent = uninit_mark_parent_path;

        // crate the uninit file
        let _ = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&uninit_mark_path)
            .context("create uninit mark file")?;
        crashsafe::fsync_file_and_parent(&common_parent).context("fsync uninit mark file")?;

        Ok(Self {
            uninit_mark_deleted: false,
            common_parent: common_parent.to_owned(),
            uninit_mark_path,
            timeline_path,
        })
    }

    fn remove_uninit_mark(mut self) {
        // remove the uninit mark
        fs::remove_file(&self.uninit_mark_path).fatal_err(&format!(
            "TimelineUninitMark::drop: remove_file uninit mark: {}",
            self.uninit_mark_path
        ));

        // fsync to persist the removal
        crashsafe::fsync(&self.common_parent).fatal_err(&format!(
            "TimelineUninitMark::drop: fsync common parent dir: {}",
            self.common_parent
        ));

        self.uninit_mark_deleted = true;
    }
}

impl Drop for TimelineUninitMark {
    fn drop(&mut self) {
        if !self.uninit_mark_deleted {
            // unblock later timeline creation attempts
            let _entered =
                info_span!("TimelineUninitMark_drop", timeline_path=%self.timeline_path).entered();
            warn!("removing timeline dir and uninit mark file");

            // sanity-check: ensure the uninit mark file still exists on disk
            let uninit_mark_file_exists = self.uninit_mark_path.try_exists().fatal_err(&format!(
                "TimelineUninitMark::drop: stat() uninit mark file: {}",
                self.uninit_mark_path
            ));
            if !uninit_mark_file_exists {
                panic!(
                    "uninit mark file assumed to exists but doesn't: {}",
                    self.uninit_mark_path
                );
            }

            // recursively delete `timeline_path`, ignoring NotFound errors and aborting the process on all others.
            match fs::remove_dir_all(&self.timeline_path) {
                Ok(()) => {
                    info!("timeline dir removed successfully");
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // this can happen both if the timeline_path does not exist
                    // and if the timeline_path exists and there's another thread
                    // still operating on that directory and our remove_dir_all call
                    // effectively got hit by time-of-check vs time-of-use.
                    // Disambiguate by calling remove_dir against the timeline_path
                    match std::fs::remove_dir(&self.timeline_path) {
                        Ok(()) => {
                            warn!("retrying timeline dir removal succeeded after NotFound, this is indicative of a race condition");
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            // this is the good case: the first NotFound was because the dir didn't exist
                            info!("timeline dir does not exist");
                        }
                        Err(e) => {
                            on_fatal_io_error(&e, &format!("TimelineUninitMark::drop: remove_dir_all failed with NotFound, then remove_dir failed: {}", self.timeline_path));
                        }
                    }
                }
                Err(e) => {
                    on_fatal_io_error(
                        &e,
                        &format!(
                            "TimelineUninitMark::drop: delete timeline directory: {:?}",
                            self.timeline_path
                        ),
                    );
                }
            }

            // fsync to order timelines_dir removal before unint mark removal
            crashsafe::fsync(&self.common_parent).fatal_err(&format!(
                "TimelineUninitMark::drop: fsync after timeline dir removal: {}",
                self.common_parent,
            ));

            // remove the uninit mark
            fs::remove_file(&self.common_parent).fatal_err(&format!(
                "TimelineUninitMark::drop: remove_file uninit mark: {}",
                self.common_parent,
            ));

            // fsync to persist the removal
            crashsafe::fsync(&self.common_parent).fatal_err(&format!(
                "TimelineUninitMark::drop: fsync common parent dir: {}",
                self.common_parent,
            ));
        }
    }
}
