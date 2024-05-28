use std::sync::Arc;

use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};
use pageserver_api::{models::TenantState, shard::TenantShardId};
use remote_storage::{GenericRemoteStorage, RemotePath, TimeoutOrCancel};
use tokio::sync::OwnedMutexGuard;
use tokio_util::sync::CancellationToken;
use tracing::{error, instrument, Instrument};

use utils::{backoff, completion, crashsafe, fs_ext, id::TimelineId, pausable_failpoint};

use crate::{
    config::PageServerConf,
    context::RequestContext,
    task_mgr::{self, TaskKind},
    tenant::{
        mgr::{TenantSlot, TenantsMapRemoveResult},
        timeline::ShutdownMode,
    },
};

use super::{
    mgr::{GetTenantError, TenantSlotError, TenantSlotUpsertError, TenantsMap},
    remote_timeline_client::{FAILED_REMOTE_OP_RETRIES, FAILED_UPLOAD_WARN_THRESHOLD},
    span,
    timeline::delete::DeleteTimelineFlow,
    tree_sort_timelines, DeleteTimelineError, Tenant, TenantPreload,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum DeleteTenantError {
    #[error("GetTenant {0}")]
    Get(#[from] GetTenantError),

    #[error("Tenant not attached")]
    NotAttached,

    #[error("Invalid state {0}. Expected Active or Broken")]
    InvalidState(TenantState),

    #[error("Tenant deletion is already in progress")]
    AlreadyInProgress,

    #[error("Tenant map slot error {0}")]
    SlotError(#[from] TenantSlotError),

    #[error("Tenant map slot upsert error {0}")]
    SlotUpsertError(#[from] TenantSlotUpsertError),

    #[error("Timeline {0}")]
    Timeline(#[from] DeleteTimelineError),

    #[error("Cancelled")]
    Cancelled,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

type DeletionGuard = tokio::sync::OwnedMutexGuard<DeleteTenantFlow>;

fn remote_tenant_delete_mark_path(
    conf: &PageServerConf,
    tenant_shard_id: &TenantShardId,
) -> anyhow::Result<RemotePath> {
    let tenant_remote_path = conf
        .tenant_path(tenant_shard_id)
        .strip_prefix(&conf.workdir)
        .context("Failed to strip workdir prefix")
        .and_then(RemotePath::new)
        .context("tenant path")?;
    Ok(tenant_remote_path.join(Utf8Path::new("timelines/deleted")))
}

async fn create_remote_delete_mark(
    conf: &PageServerConf,
    remote_storage: &GenericRemoteStorage,
    tenant_shard_id: &TenantShardId,
    cancel: &CancellationToken,
) -> Result<(), DeleteTenantError> {
    let remote_mark_path = remote_tenant_delete_mark_path(conf, tenant_shard_id)?;

    let data: &[u8] = &[];
    backoff::retry(
        || async {
            let data = bytes::Bytes::from_static(data);
            let stream = futures::stream::once(futures::future::ready(Ok(data)));
            remote_storage
                .upload(stream, 0, &remote_mark_path, None, cancel)
                .await
        },
        TimeoutOrCancel::caused_by_cancel,
        FAILED_UPLOAD_WARN_THRESHOLD,
        FAILED_REMOTE_OP_RETRIES,
        "mark_upload",
        cancel,
    )
    .await
    .ok_or_else(|| anyhow::Error::new(TimeoutOrCancel::Cancel))
    .and_then(|x| x)
    .context("mark_upload")?;

    Ok(())
}

async fn create_local_delete_mark(
    conf: &PageServerConf,
    tenant_shard_id: &TenantShardId,
) -> Result<(), DeleteTenantError> {
    let marker_path = conf.tenant_deleted_mark_file_path(tenant_shard_id);

    // Note: we're ok to replace existing file.
    let _ = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&marker_path)
        .with_context(|| format!("could not create delete marker file {marker_path:?}"))?;

    crashsafe::fsync_file_and_parent(&marker_path).context("sync_mark")?;

    Ok(())
}

async fn schedule_ordered_timeline_deletions(
    tenant: &Arc<Tenant>,
) -> Result<Vec<(Arc<tokio::sync::Mutex<DeleteTimelineFlow>>, TimelineId)>, DeleteTenantError> {
    // Tenant is stopping at this point. We know it will be deleted.
    // No new timelines should be created.
    // Tree sort timelines to delete from leafs to the root.
    // NOTE: by calling clone we release the mutex which creates a possibility for a race: pending deletion
    // can complete and remove timeline from the map in between our call to clone
    // and `DeleteTimelineFlow::run`, so `run` wont find timeline in `timelines` map.
    // timelines.lock is currently synchronous so we cant hold it across await point.
    // So just ignore NotFound error if we get it from `run`.
    // Beware: in case it becomes async and we try to hold it here, `run` also locks it, which can create a deadlock.
    let timelines = tenant.timelines.lock().unwrap().clone();
    let sorted =
        tree_sort_timelines(timelines, |t| t.get_ancestor_timeline_id()).context("tree sort")?;

    let mut already_running_deletions = vec![];

    for (timeline_id, _) in sorted.into_iter().rev() {
        let span = tracing::info_span!("timeline_delete", %timeline_id);
        let res = DeleteTimelineFlow::run(tenant, timeline_id, true)
            .instrument(span)
            .await;
        if let Err(e) = res {
            match e {
                DeleteTimelineError::NotFound => {
                    // Timeline deletion finished after call to clone above but before call
                    // to `DeleteTimelineFlow::run` and removed timeline from the map.
                    continue;
                }
                DeleteTimelineError::AlreadyInProgress(guard) => {
                    already_running_deletions.push((guard, timeline_id));
                    continue;
                }
                e => return Err(DeleteTenantError::Timeline(e)),
            }
        }
    }

    Ok(already_running_deletions)
}

async fn ensure_timelines_dir_empty(timelines_path: &Utf8Path) -> Result<(), DeleteTenantError> {
    // Assert timelines dir is empty.
    if !fs_ext::is_directory_empty(timelines_path).await? {
        // Display first 10 items in directory
        let list = fs_ext::list_dir(timelines_path).await.context("list_dir")?;
        let list = &list.into_iter().take(10).collect::<Vec<_>>();
        return Err(DeleteTenantError::Other(anyhow::anyhow!(
            "Timelines directory is not empty after all timelines deletion: {list:?}"
        )));
    }

    Ok(())
}

async fn remove_tenant_remote_delete_mark(
    conf: &PageServerConf,
    remote_storage: &GenericRemoteStorage,
    tenant_shard_id: &TenantShardId,
    cancel: &CancellationToken,
) -> Result<(), DeleteTenantError> {
    let path = remote_tenant_delete_mark_path(conf, tenant_shard_id)?;
    backoff::retry(
        || async { remote_storage.delete(&path, cancel).await },
        TimeoutOrCancel::caused_by_cancel,
        FAILED_UPLOAD_WARN_THRESHOLD,
        FAILED_REMOTE_OP_RETRIES,
        "remove_tenant_remote_delete_mark",
        cancel,
    )
    .await
    .ok_or_else(|| anyhow::Error::new(TimeoutOrCancel::Cancel))
    .and_then(|x| x)
    .context("remove_tenant_remote_delete_mark")?;
    Ok(())
}

// Cleanup fs traces: tenant config, timelines dir local delete mark, tenant dir
async fn cleanup_remaining_fs_traces(
    conf: &PageServerConf,
    tenant_shard_id: &TenantShardId,
) -> Result<(), DeleteTenantError> {
    let rm = |p: Utf8PathBuf, is_dir: bool| async move {
        if is_dir {
            tokio::fs::remove_dir(&p).await
        } else {
            tokio::fs::remove_file(&p).await
        }
        .or_else(fs_ext::ignore_not_found)
        .with_context(|| format!("failed to delete {p}"))
    };

    rm(conf.tenant_config_path(tenant_shard_id), false).await?;
    rm(conf.tenant_location_config_path(tenant_shard_id), false).await?;

    fail::fail_point!("tenant-delete-before-remove-timelines-dir", |_| {
        Err(anyhow::anyhow!(
            "failpoint: tenant-delete-before-remove-timelines-dir"
        ))?
    });

    rm(conf.timelines_path(tenant_shard_id), true).await?;

    fail::fail_point!("tenant-delete-before-remove-deleted-mark", |_| {
        Err(anyhow::anyhow!(
            "failpoint: tenant-delete-before-remove-deleted-mark"
        ))?
    });

    // Make sure previous deletions are ordered before mark removal.
    // Otherwise there is no guarantee that they reach the disk before mark deletion.
    // So its possible for mark to reach disk first and for other deletions
    // to be reordered later and thus missed if a crash occurs.
    // Note that we dont need to sync after mark file is removed
    // because we can tolerate the case when mark file reappears on startup.
    let tenant_path = &conf.tenant_path(tenant_shard_id);
    if tenant_path.exists() {
        crashsafe::fsync_async(&conf.tenant_path(tenant_shard_id))
            .await
            .context("fsync_pre_mark_remove")?;
    }

    rm(conf.tenant_deleted_mark_file_path(tenant_shard_id), false).await?;

    rm(conf.tenant_heatmap_path(tenant_shard_id), false).await?;

    fail::fail_point!("tenant-delete-before-remove-tenant-dir", |_| {
        Err(anyhow::anyhow!(
            "failpoint: tenant-delete-before-remove-tenant-dir"
        ))?
    });

    rm(conf.tenant_path(tenant_shard_id), true).await?;

    Ok(())
}

/// Orchestrates tenant shut down of all tasks, removes its in-memory structures,
/// and deletes its data from both disk and s3.
/// The sequence of steps:
/// 1. Upload remote deletion mark.
/// 2. Create local mark file.
/// 3. Shutdown tasks
/// 4. Run ordered timeline deletions
/// 5. Wait for timeline deletion operations that were scheduled before tenant deletion was requested
/// 6. Remove remote mark
/// 7. Cleanup remaining fs traces, tenant dir, config, timelines dir, local delete mark
/// It is resumable from any step in case a crash/restart occurs.
/// There are two entrypoints to the process:
/// 1. [`DeleteTenantFlow::run`] this is the main one called by a management api handler.
/// 2. [`DeleteTenantFlow::resume_from_attach`] is called when deletion is resumed tenant is found to be deleted during attach process.
///  Note the only other place that messes around timeline delete mark is the `Tenant::spawn_load` function.
#[derive(Default)]
pub enum DeleteTenantFlow {
    #[default]
    NotStarted,
    InProgress,
    Finished,
}

impl DeleteTenantFlow {
    // These steps are run in the context of management api request handler.
    // Long running steps are continued to run in the background.
    // NB: If this fails half-way through, and is retried, the retry will go through
    // all the same steps again. Make sure the code here is idempotent, and don't
    // error out if some of the shutdown tasks have already been completed!
    // NOTE: static needed for background part.
    // We assume that calling code sets up the span with tenant_id.
    #[instrument(skip_all)]
    pub(crate) async fn run(
        conf: &'static PageServerConf,
        remote_storage: GenericRemoteStorage,
        tenants: &'static std::sync::RwLock<TenantsMap>,
        tenant: Arc<Tenant>,
        cancel: &CancellationToken,
    ) -> Result<(), DeleteTenantError> {
        span::debug_assert_current_span_has_tenant_id();

        pausable_failpoint!("tenant-delete-before-run");

        let mut guard = Self::prepare(&tenant).await?;

        if let Err(e) = Self::run_inner(&mut guard, conf, &remote_storage, &tenant, cancel).await {
            tenant.set_broken(format!("{e:#}")).await;
            return Err(e);
        }

        Self::schedule_background(guard, conf, remote_storage, tenants, tenant);

        Ok(())
    }

    // Helper function needed to be able to match once on returned error and transition tenant into broken state.
    // This is needed because tenant.shutwodn is not idempotent. If tenant state is set to stopping another call to tenant.shutdown
    // will result in an error, but here we need to be able to retry shutdown when tenant deletion is retried.
    // So the solution is to set tenant state to broken.
    async fn run_inner(
        guard: &mut OwnedMutexGuard<Self>,
        conf: &'static PageServerConf,
        remote_storage: &GenericRemoteStorage,
        tenant: &Tenant,
        cancel: &CancellationToken,
    ) -> Result<(), DeleteTenantError> {
        guard.mark_in_progress()?;

        fail::fail_point!("tenant-delete-before-create-remote-mark", |_| {
            Err(anyhow::anyhow!(
                "failpoint: tenant-delete-before-create-remote-mark"
            ))?
        });

        create_remote_delete_mark(conf, remote_storage, &tenant.tenant_shard_id, cancel)
            .await
            .context("remote_mark")?;

        fail::fail_point!("tenant-delete-before-create-local-mark", |_| {
            Err(anyhow::anyhow!(
                "failpoint: tenant-delete-before-create-local-mark"
            ))?
        });

        create_local_delete_mark(conf, &tenant.tenant_shard_id)
            .await
            .context("local delete mark")?;

        fail::fail_point!("tenant-delete-before-background", |_| {
            Err(anyhow::anyhow!(
                "failpoint: tenant-delete-before-background"
            ))?
        });

        Ok(())
    }

    fn mark_in_progress(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Finished => anyhow::bail!("Bug. Is in finished state"),
            Self::InProgress { .. } => { /* We're in a retry */ }
            Self::NotStarted => { /* Fresh start */ }
        }

        *self = Self::InProgress;

        Ok(())
    }

    pub(crate) async fn should_resume_deletion(
        conf: &'static PageServerConf,
        remote_mark_exists: bool,
        tenant: &Tenant,
    ) -> Result<Option<DeletionGuard>, DeleteTenantError> {
        let acquire = |t: &Tenant| {
            Some(
                Arc::clone(&t.delete_progress)
                    .try_lock_owned()
                    .expect("we're the only owner during init"),
            )
        };

        if remote_mark_exists {
            return Ok(acquire(tenant));
        }

        // Check local mark first, if its there there is no need to go to s3 to check whether remote one exists.
        if conf
            .tenant_deleted_mark_file_path(&tenant.tenant_shard_id)
            .exists()
        {
            Ok(acquire(tenant))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn resume_from_attach(
        guard: DeletionGuard,
        tenant: &Arc<Tenant>,
        preload: Option<TenantPreload>,
        tenants: &'static std::sync::RwLock<TenantsMap>,
        ctx: &RequestContext,
    ) -> Result<(), DeleteTenantError> {
        let (_, progress) = completion::channel();

        tenant
            .set_stopping(progress, false, true)
            .await
            .expect("cant be stopping or broken");

        tenant
            .attach(preload, super::SpawnMode::Eager, ctx)
            .await
            .context("attach")?;

        Self::background(
            guard,
            tenant.conf,
            tenant.remote_storage.clone(),
            tenants,
            tenant,
        )
        .await
    }

    /// Check whether background deletion of this tenant is currently in progress
    pub(crate) fn is_in_progress(tenant: &Tenant) -> bool {
        tenant.delete_progress.try_lock().is_err()
    }

    async fn prepare(
        tenant: &Arc<Tenant>,
    ) -> Result<tokio::sync::OwnedMutexGuard<Self>, DeleteTenantError> {
        // FIXME: unsure about active only. Our init jobs may not be cancellable properly,
        // so at least for now allow deletions only for active tenants. TODO recheck
        // Broken and Stopping is needed for retries.
        if !matches!(
            tenant.current_state(),
            TenantState::Active | TenantState::Broken { .. }
        ) {
            return Err(DeleteTenantError::InvalidState(tenant.current_state()));
        }

        let guard = Arc::clone(&tenant.delete_progress)
            .try_lock_owned()
            .map_err(|_| DeleteTenantError::AlreadyInProgress)?;

        fail::fail_point!("tenant-delete-before-shutdown", |_| {
            Err(anyhow::anyhow!("failpoint: tenant-delete-before-shutdown"))?
        });

        // make pageserver shutdown not to wait for our completion
        let (_, progress) = completion::channel();

        // It would be good to only set stopping here and continue shutdown in the background, but shutdown is not idempotent.
        // i e it is an error to do:
        // tenant.set_stopping
        // tenant.shutdown
        // Its also bad that we're holding tenants.read here.
        // TODO relax set_stopping to be idempotent?
        if tenant.shutdown(progress, ShutdownMode::Hard).await.is_err() {
            return Err(DeleteTenantError::Other(anyhow::anyhow!(
                "tenant shutdown is already in progress"
            )));
        }

        Ok(guard)
    }

    fn schedule_background(
        guard: OwnedMutexGuard<Self>,
        conf: &'static PageServerConf,
        remote_storage: GenericRemoteStorage,
        tenants: &'static std::sync::RwLock<TenantsMap>,
        tenant: Arc<Tenant>,
    ) {
        let tenant_shard_id = tenant.tenant_shard_id;

        task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            TaskKind::TimelineDeletionWorker,
            Some(tenant_shard_id),
            None,
            "tenant_delete",
            false,
            async move {
                if let Err(err) =
                    Self::background(guard, conf, remote_storage, tenants, &tenant).await
                {
                    error!("Error: {err:#}");
                    tenant.set_broken(format!("{err:#}")).await;
                };
                Ok(())
            }
            .instrument(tracing::info_span!(parent: None, "delete_tenant", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug())),
        );
    }

    async fn background(
        mut guard: OwnedMutexGuard<Self>,
        conf: &PageServerConf,
        remote_storage: GenericRemoteStorage,
        tenants: &'static std::sync::RwLock<TenantsMap>,
        tenant: &Arc<Tenant>,
    ) -> Result<(), DeleteTenantError> {
        // Tree sort timelines, schedule delete for them. Mention retries from the console side.
        // Note that if deletion fails we dont mark timelines as broken,
        // the whole tenant will become broken as by `Self::schedule_background` logic
        let already_running_timeline_deletions = schedule_ordered_timeline_deletions(tenant)
            .await
            .context("schedule_ordered_timeline_deletions")?;

        fail::fail_point!("tenant-delete-before-polling-ongoing-deletions", |_| {
            Err(anyhow::anyhow!(
                "failpoint: tenant-delete-before-polling-ongoing-deletions"
            ))?
        });

        // Wait for deletions that were already running at the moment when tenant deletion was requested.
        // When we can lock deletion guard it means that corresponding timeline deletion finished.
        for (guard, timeline_id) in already_running_timeline_deletions {
            let flow = guard.lock().await;
            if !flow.is_finished() {
                return Err(DeleteTenantError::Other(anyhow::anyhow!(
                    "already running timeline deletion failed: {timeline_id}"
                )));
            }
        }

        let timelines_path = conf.timelines_path(&tenant.tenant_shard_id);
        // May not exist if we fail in cleanup_remaining_fs_traces after removing it
        if timelines_path.exists() {
            // sanity check to guard against layout changes
            ensure_timelines_dir_empty(&timelines_path)
                .await
                .context("timelines dir not empty")?;
        }

        remove_tenant_remote_delete_mark(
            conf,
            &remote_storage,
            &tenant.tenant_shard_id,
            &task_mgr::shutdown_token(),
        )
        .await?;

        pausable_failpoint!("tenant-delete-before-cleanup-remaining-fs-traces-pausable");
        fail::fail_point!("tenant-delete-before-cleanup-remaining-fs-traces", |_| {
            Err(anyhow::anyhow!(
                "failpoint: tenant-delete-before-cleanup-remaining-fs-traces"
            ))?
        });

        cleanup_remaining_fs_traces(conf, &tenant.tenant_shard_id)
            .await
            .context("cleanup_remaining_fs_traces")?;

        {
            pausable_failpoint!("tenant-delete-before-map-remove");

            // This block is simply removing the TenantSlot for this tenant.  It requires a loop because
            // we might conflict with a TenantSlot::InProgress marker and need to wait for it.
            //
            // This complexity will go away when we simplify how deletion works:
            // https://github.com/neondatabase/neon/issues/5080
            loop {
                // Under the TenantMap lock, try to remove the tenant.  We usually succeed, but if
                // we encounter an InProgress marker, yield the barrier it contains and wait on it.
                let barrier = {
                    let mut locked = tenants.write().unwrap();
                    let removed = locked.remove(tenant.tenant_shard_id);

                    // FIXME: we should not be modifying this from outside of mgr.rs.
                    // This will go away when we simplify deletion (https://github.com/neondatabase/neon/issues/5080)

                    // Update stats
                    match &removed {
                        TenantsMapRemoveResult::Occupied(slot) => {
                            crate::metrics::TENANT_MANAGER.slot_removed(slot);
                        }
                        TenantsMapRemoveResult::InProgress(barrier) => {
                            crate::metrics::TENANT_MANAGER
                                .slot_removed(&TenantSlot::InProgress(barrier.clone()));
                        }
                        TenantsMapRemoveResult::Vacant => {
                            // Nothing changed in map, no metric update
                        }
                    }

                    match removed {
                        TenantsMapRemoveResult::Occupied(TenantSlot::Attached(tenant)) => {
                            match tenant.current_state() {
                                TenantState::Stopping { .. } | TenantState::Broken { .. } => {
                                    // Expected: we put the tenant into stopping state before we start deleting it
                                }
                                state => {
                                    // Unexpected state
                                    tracing::warn!(
                                        "Tenant in unexpected state {state} after deletion"
                                    );
                                }
                            }
                            break;
                        }
                        TenantsMapRemoveResult::Occupied(TenantSlot::Secondary(_)) => {
                            // This is unexpected: this secondary tenants should not have been created, and we
                            // are not in a position to shut it down from here.
                            tracing::warn!("Tenant transitioned to secondary mode while deleting!");
                            break;
                        }
                        TenantsMapRemoveResult::Occupied(TenantSlot::InProgress(_)) => {
                            unreachable!("TenantsMap::remove handles InProgress separately, should never return it here");
                        }
                        TenantsMapRemoveResult::Vacant => {
                            tracing::warn!(
                                "Tenant removed from TenantsMap before deletion completed"
                            );
                            break;
                        }
                        TenantsMapRemoveResult::InProgress(barrier) => {
                            // An InProgress entry was found, we must wait on its barrier
                            barrier
                        }
                    }
                };

                tracing::info!(
                    "Waiting for competing operation to complete before deleting state for tenant"
                );
                barrier.wait().await;
            }
        }

        *guard = Self::Finished;

        Ok(())
    }
}
