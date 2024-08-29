use std::sync::Arc;

use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};
use pageserver_api::{models::TenantState, shard::TenantShardId};
use remote_storage::{GenericRemoteStorage, RemotePath, TimeoutOrCancel};
use tokio::sync::OwnedMutexGuard;
use tokio_util::sync::CancellationToken;
use tracing::{error, Instrument};

use utils::{backoff, completion, crashsafe, fs_ext, id::TimelineId, pausable_failpoint};

use crate::{
    config::PageServerConf,
    context::RequestContext,
    task_mgr::{self},
    tenant::{
        mgr::{TenantSlot, TenantsMapRemoveResult},
        remote_timeline_client::remote_heatmap_path,
    },
};

use super::{
    mgr::{GetTenantError, TenantSlotError, TenantSlotUpsertError, TenantsMap},
    remote_timeline_client::{FAILED_REMOTE_OP_RETRIES, FAILED_UPLOAD_WARN_THRESHOLD},
    timeline::delete::DeleteTimelineFlow,
    tree_sort_timelines, DeleteTimelineError, Tenant, TenantPreload,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum DeleteTenantError {
    #[error("GetTenant {0}")]
    Get(#[from] GetTenantError),

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

#[derive(Default)]
pub enum DeleteTenantFlow {
    #[default]
    NotStarted,
    InProgress,
    Finished,
}

impl DeleteTenantFlow {
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

        // Remove top-level tenant objects that don't belong to a timeline, such as heatmap
        let heatmap_path = remote_heatmap_path(&tenant.tenant_shard_id());
        if let Some(Err(e)) = backoff::retry(
            || async {
                remote_storage
                    .delete(&heatmap_path, &task_mgr::shutdown_token())
                    .await
            },
            TimeoutOrCancel::caused_by_cancel,
            FAILED_UPLOAD_WARN_THRESHOLD,
            FAILED_REMOTE_OP_RETRIES,
            "remove_remote_tenant_heatmap",
            &task_mgr::shutdown_token(),
        )
        .await
        {
            tracing::warn!("Failed to delete heatmap at {heatmap_path}: {e}");
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
