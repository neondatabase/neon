use std::sync::Arc;

use anyhow::Context;
use camino::Utf8Path;
use pageserver_api::models::TenantState;
use remote_storage::{GenericRemoteStorage, RemotePath};
use tokio_util::sync::CancellationToken;
use tracing::error;

use utils::{
    backoff, crashsafe,
    id::{TenantId, TimelineId},
};

use crate::{
    config::PageServerConf,
    tenant::{mgr::safe_rename_tenant_dir, ShutdownError},
};

use super::{
    mgr::{GetTenantError, SlotGuard, TenantSlotError, TenantSlotUpsertError},
    remote_timeline_client::{FAILED_REMOTE_OP_RETRIES, FAILED_UPLOAD_WARN_THRESHOLD},
    timeline::delete::DeleteTimelineFlow,
    tree_sort_timelines, DeleteTimelineError, Tenant,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum DeleteTenantError {
    #[error("GetTenant {0}")]
    Get(#[from] GetTenantError),

    #[error("Tenant not attached")]
    NotAttached,

    #[error("Invalid state {0}. Expected Active or Broken")]
    InvalidState(TenantState),

    #[error("Tenant map slot error {0}")]
    SlotError(#[from] TenantSlotError),

    #[error("Tenant map slot upsert error {0}")]
    SlotUpsertError(#[from] TenantSlotUpsertError),

    #[error("Timeline {0}")]
    Timeline(#[from] DeleteTimelineError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// The part of tenant deletion that must happen before acknowledging the deletion request
///
/// Usually deletion requires that the tenant is not already in Stopping state: set
/// `resume` to true to skip this check if resuming deletion at startup, since we
/// would have loaded the tenant into Stopping mode already
pub(crate) async fn delete_tenant_foreground(
    conf: &'static PageServerConf,
    remote_storage: Option<GenericRemoteStorage>,
    tenant: &Arc<Tenant>,
    resume: bool,
    // Witness that we are doing this work within a TenantSlot::InProgress state.
    _slot_guard: &SlotGuard,
) -> Result<(), DeleteTenantError> {
    // In resume mode, we must be stopping.  Else, we must _not_ be Stopping.
    if !(matches!(tenant.current_state(), TenantState::Stopping) ^ !resume) {
        return Err(DeleteTenantError::InvalidState(tenant.current_state()));
    }

    fail::fail_point!("tenant-delete-before-create-remote-mark", |_| {
        Err(anyhow::anyhow!(
            "failpoint: tenant-delete-before-create-remote-mark"
        ))?
    });

    // Write persistent tombstone
    if let Some(remote_storage) = &remote_storage {
        create_remote_delete_mark(conf, remote_storage, &tenant.get_tenant_id()).await?;
    }

    fail::fail_point!("tenant-delete-before-create-local-mark", |_| {
        Err(anyhow::anyhow!(
            "failpoint: tenant-delete-before-create-local-mark"
        ))?
    });

    create_local_delete_mark(conf, &tenant.tenant_id)
        .await
        .context("local create mark")?;

    fail::fail_point!("tenant-delete-before-background", |_| {
        Err(anyhow::anyhow!(
            "failpoint: tenant-delete-before-background"
        ))?
    });

    Ok(())
}

/// The part of tenant deletion that happens after we have already acknowledged the request.
///
/// This part may retried after pageserver restart if we see a tenant that has a deletion marker
/// but has not been completely deleted.
pub(crate) async fn delete_tenant_background(
    conf: &'static PageServerConf,
    remote_storage: Option<GenericRemoteStorage>,
    tenant_id: TenantId,
    tenant: &Arc<Tenant>,
    resume: bool,
    // Witness that we are doing this work within a TenantSlot::InProgress state.
    _slot_guard: &SlotGuard,
) -> anyhow::Result<()> {
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
            ))
            .into());
        }
    }

    // For convenience, use the Tenant's deletion queue reference so that we don't have
    // to take it as an  argument.
    let deletion_queue_client = tenant.deletion_queue_client.clone();

    fail::fail_point!("tenant-delete-before-shutdown", |_| {
        Err(anyhow::anyhow!("failpoint: tenant-delete-before-shutdown"))?
    });

    // Tear down local runtime state
    if !resume {
        tenant.shutdown(false).await.map_err(|e| match e {
            ShutdownError::AlreadyStopping => {
                DeleteTenantError::InvalidState(TenantState::Stopping)
            }
        })?;
    }

    // Not necessary for correctness, but executing deletions before erasing local contents
    // means that we will have a better chance to resume the delete if we crash.
    deletion_queue_client.flush_execute().await?;

    fail::fail_point!("tenant-delete-before-remove-deleted-mark", |_| {
        Err(anyhow::anyhow!(
            "failpoint: tenant-delete-before-remove-deleted-mark"
        ))?
    });

    // Remove the deletion marker
    remove_tenant_remote_delete_mark(conf, remote_storage.as_ref(), &tenant_id).await?;

    // Not necessary for correctness, but helps make it true that when we log that deletion is
    // done, it really is.
    deletion_queue_client.flush_execute().await?;

    fail::fail_point!("tenant-delete-before-remove-tenant-dir", |_| {
        Err(anyhow::anyhow!(
            "failpoint: tenant-delete-before-remove-tenant-dir"
        ))?
    });

    // Remove local storage contents.  We do this last, so that if we crash during delete, on
    // restart we will attempt to re-attach the tenant and resume the deletion.
    let local_tenant_directory = conf.tenant_path(&tenant_id);
    let tmp_path = safe_rename_tenant_dir(&local_tenant_directory)
        .await
        .with_context(|| format!("local tenant directory {local_tenant_directory:?} rename"))?;

    tokio::fs::remove_dir_all(tmp_path.as_path())
        .await
        .with_context(|| format!("tenant directory {:?} deletion", tmp_path))?;

    Ok(())
}

fn remote_tenant_delete_mark_path(
    conf: &PageServerConf,
    tenant_id: &TenantId,
) -> anyhow::Result<RemotePath> {
    let tenant_remote_path = conf
        .tenant_path(tenant_id)
        .strip_prefix(&conf.workdir)
        .context("Failed to strip workdir prefix")
        .and_then(RemotePath::new)
        .context("tenant path")?;
    Ok(tenant_remote_path.join(Utf8Path::new("timelines/deleted")))
}

pub(crate) async fn create_remote_delete_mark(
    conf: &PageServerConf,
    remote_storage: &GenericRemoteStorage,
    tenant_id: &TenantId,
) -> Result<(), DeleteTenantError> {
    let remote_mark_path = remote_tenant_delete_mark_path(conf, tenant_id)?;

    let data: &[u8] = &[];
    backoff::retry(
        || async {
            remote_storage
                .upload(data, 0, &remote_mark_path, None)
                .await
        },
        |_e| false,
        FAILED_UPLOAD_WARN_THRESHOLD,
        FAILED_REMOTE_OP_RETRIES,
        "mark_upload",
        // TODO: use a cancellation token (https://github.com/neondatabase/neon/issues/5066)
        backoff::Cancel::new(CancellationToken::new(), || unreachable!()),
    )
    .await
    .context("mark_upload")?;

    Ok(())
}

async fn create_local_delete_mark(
    conf: &PageServerConf,
    tenant_id: &TenantId,
) -> Result<(), DeleteTenantError> {
    let marker_path = conf.tenant_deleted_mark_file_path(tenant_id);

    // Note: we're ok to replace existing file.
    let _ = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&marker_path)
        .with_context(|| format!("could not create delete marker file {marker_path:?}"))?;

    crashsafe::fsync_file_and_parent(&marker_path).context("sync_mark")?;

    Ok(())
}

pub(crate) async fn should_resume_deletion(
    conf: &'static PageServerConf,
    remote_mark_exists: bool,
    tenant_id: &TenantId,
) -> Result<bool, DeleteTenantError> {
    if remote_mark_exists {
        return Ok(true);
    }

    // In the very last stage of deletion, we might have already removed the remote
    // marker, but be attaching the tenant anyway on the basis of its local directory
    // existing: to resume deletion in this case we need the local deletion marker.
    if conf.tenant_deleted_mark_file_path(tenant_id).exists() {
        Ok(true)
    } else {
        Ok(false)
    }
}

pub(crate) async fn schedule_ordered_timeline_deletions(
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
        if let Err(e) = DeleteTimelineFlow::run(tenant, timeline_id, true).await {
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

pub(crate) async fn remove_tenant_remote_delete_mark(
    conf: &PageServerConf,
    remote_storage: Option<&GenericRemoteStorage>,
    tenant_id: &TenantId,
) -> Result<(), DeleteTenantError> {
    if let Some(remote_storage) = remote_storage {
        let path = remote_tenant_delete_mark_path(conf, tenant_id)?;
        backoff::retry(
            || async { remote_storage.delete(&path).await },
            |_e| false,
            FAILED_UPLOAD_WARN_THRESHOLD,
            FAILED_REMOTE_OP_RETRIES,
            "remove_tenant_remote_delete_mark",
            // TODO: use a cancellation token (https://github.com/neondatabase/neon/issues/5066)
            backoff::Cancel::new(CancellationToken::new(), || unreachable!()),
        )
        .await
        .context("remove_tenant_remote_delete_mark")?;
    }
    Ok(())
}
