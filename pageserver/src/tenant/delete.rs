use std::{path::Path, sync::Arc};

use anyhow::Context;
use remote_storage::{DownloadError, GenericRemoteStorage, RemotePath};
use tokio::sync::OwnedMutexGuard;
use tracing::{error, instrument, warn, Instrument, Span};

use utils::{
    completion, crashsafe, fs_ext,
    id::{TenantId, TimelineId},
};

use crate::{
    config::PageServerConf,
    context::RequestContext,
    task_mgr::{self, TaskKind},
    InitializationOrder,
};

use super::{
    mgr::{GetTenantError, TenantsMap},
    timeline::delete::DeleteTimelineFlow,
    tree_sort_timelines, DeleteTimelineError, Tenant,
};

#[derive(Debug, thiserror::Error)]
pub enum DeleteTenantError {
    #[error("GetTenant {0}")]
    Get(#[from] GetTenantError),

    #[error("Tenant deletion is already in progress")]
    AlreadyInProgress,

    #[error("Timeline {0}")]
    Timeline(#[from] DeleteTimelineError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

type DeletionGuard = tokio::sync::OwnedMutexGuard<DeleteTenantFlow>;

fn remote_tenant_delete_mark_path(
    conf: &PageServerConf,
    tenant_id: &TenantId,
) -> anyhow::Result<RemotePath> {
    let tenant_remote_path =
        RemotePath::new(&conf.tenant_path(tenant_id)).context("tenant path")?;
    Ok(tenant_remote_path.join(Path::new("deleted")))
}

async fn create_remote_delete_mark(
    conf: &PageServerConf,
    remote_storage: &GenericRemoteStorage,
    tenant_id: TenantId,
) -> Result<(), DeleteTenantError> {
    let remote_mark_path = remote_tenant_delete_mark_path(conf, &tenant_id)?;

    // TODO check if that works, if not put a timestamp here
    let data: &[u8] = &[];
    remote_storage
        .upload(data, 0, &remote_mark_path, None)
        .await
        .context("mark upload")?;

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

async fn schedule_ordered_timeline_deletions(
    tenant: &Arc<Tenant>,
) -> Result<Vec<(Arc<tokio::sync::Mutex<DeleteTimelineFlow>>, TimelineId)>, DeleteTenantError> {
    // Tenant is stopping at this point. We know it will be deleted. No new timelines should be created.
    // Tree sort timelines to delete from leafs to the root.
    let timelines = tenant.timelines.lock().unwrap().clone();
    let sorted =
        tree_sort_timelines(timelines, |t| t.get_ancestor_timeline_id()).context("tree sort")?;

    let mut already_running_deletions = vec![];

    for (timeline_id, _) in sorted.into_iter().rev() {
        if let Err(e) = DeleteTimelineFlow::run(tenant, timeline_id, true).await {
            if let DeleteTimelineError::AlreadyInProgress(guard) = e {
                already_running_deletions.push((guard, timeline_id));
                continue;
            }

            return Err(DeleteTenantError::Timeline(e));
        }
    }

    Ok(already_running_deletions)
}

async fn assert_timeline_dir_empty(
    conf: &PageServerConf,
    tenant: &Tenant,
) -> Result<(), DeleteTenantError> {
    // Assert timelines dir is empty.
    let timelines_path = conf.timelines_path(&tenant.tenant_id);
    if !fs_ext::is_directory_empty(&timelines_path).await? {
        // Display first 10 items in directory
        let list = &fs_ext::list_dir(&timelines_path)
            .await
            .context("list_dir")?[..10];
        return Err(DeleteTenantError::Other(anyhow::anyhow!(
            "Timelines directory is not empty after all timelines deletion: {list:?}"
        )));
    }

    Ok(())
}

async fn remove_tenant_remote_delete_mark(
    conf: &PageServerConf,
    remote_storage: Option<&GenericRemoteStorage>,
    tenant_id: &TenantId,
) -> Result<(), DeleteTenantError> {
    if let Some(remote_storage) = remote_storage {
        remote_storage
            .delete(&remote_tenant_delete_mark_path(conf, tenant_id)?)
            .await?;
    }
    Ok(())
}

async fn cleanup_remaining_fs_traces(
    conf: &PageServerConf,
    tenant_id: &TenantId,
) -> Result<(), DeleteTenantError> {
    // Cleanup fs traces: tenant config, timelines dir local delete mark, tenant dir
    let remains = [
        (conf.tenant_config_path(tenant_id), false),
        (conf.timelines_path(tenant_id), true),
        (conf.tenant_deleted_mark_file_path(tenant_id), false),
        (conf.tenant_path(tenant_id), true),
    ];

    for (remain, is_dir) in remains {
        if is_dir {
            tokio::fs::remove_dir(&remain).await
        } else {
            tokio::fs::remove_file(&remain).await
        }
        .or_else(fs_ext::ignore_not_found)
        .with_context(|| {
            let to_display = remain.display();
            format!("failed to delete {to_display}")
        })?;
    }

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
/// There are three entrypoints to the process:
/// 1. [`DeleteTimelineFlow::run`] this is the main one called by a management api handler.
/// 2. [`DeleteTimelineFlow::resume`] is called during restarts when local or remote deletion marks are still there.
/// 3. [`DeleteTimelineFlow::cleanup_remaining_fs_traces_after_timeline_deletion`] is used when we deleted remote
/// index but still have local metadata, timeline directory and delete mark.
/// Note the only other place that messes around timeline delete mark is the `Tenant::spawn_load` function.
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
    // NOTE: static Lazy is only needed for background part.
    #[instrument(skip_all, fields(%tenant_id))]
    pub(crate) async fn run(
        conf: &'static PageServerConf,
        remote_storage: Option<GenericRemoteStorage>,
        tenants: &'static tokio::sync::RwLock<TenantsMap>,
        tenant_id: TenantId,
    ) -> Result<(), DeleteTenantError> {
        let (tenant, mut guard) = Self::prepare(tenants, tenant_id).await?;

        guard.mark_in_progress()?;
        // IDEA: implement detach as delete without remote storage. Then they would use the same lock (deletion_progress) so wont contend.
        // Though sounds scary, different mark name?
        // Detach currently uses remove_dir_all so in case of a crash we can end up in a weird state.
        if let Some(remote_storage) = &remote_storage {
            create_remote_delete_mark(conf, remote_storage, tenant_id)
                .await
                .context("create delete mark")?
        }

        create_local_delete_mark(conf, &tenant_id)
            .await
            .context("local delete mark")?;

        Self::schedule_background(guard, conf, remote_storage, tenants, tenant);

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

    pub async fn should_resume_deletion(
        conf: &'static PageServerConf,
        remote_storage: Option<&GenericRemoteStorage>,
        tenant: &Tenant,
    ) -> Result<Option<DeletionGuard>, DeleteTenantError> {
        let acquire = |t: &Tenant| {
            Some(
                Arc::clone(&t.delete_progress)
                    .try_lock_owned()
                    .expect("we're the only owner during init"),
            )
        };

        let tenant_id = tenant.tenant_id;
        // Check local mark first, if its there there is no need to go to s3 to check whether remote one exists.
        if conf.tenant_deleted_mark_file_path(&tenant_id).exists() {
            return Ok(acquire(tenant));
        }

        // If remote storage is there we rely on it
        if let Some(remote_storage) = remote_storage {
            let remote_mark_path = remote_tenant_delete_mark_path(conf, &tenant_id)?;

            match remote_storage.download(&remote_mark_path).await {
                Ok(_) => return Ok(acquire(tenant)),
                Err(e) => {
                    if matches!(e, DownloadError::NotFound) {
                        return Ok(None);
                    }
                    return Err(anyhow::anyhow!(e))?;
                }
            }
        }

        Ok(None)
    }

    pub(crate) async fn resume(
        guard: DeletionGuard,
        tenant: &Arc<Tenant>,
        init_order: Option<&InitializationOrder>,
        tenants: &'static tokio::sync::RwLock<TenantsMap>,
        ctx: &RequestContext,
    ) -> Result<(), DeleteTenantError> {
        let (_, progress) = completion::channel();
        tenant
            .set_stopping(progress)
            .await
            .expect("cant be stopping or broken");

        // Do not consume valuable resources during the load phase, continue deletion once init phase is complete.
        let background_jobs_can_start = init_order.as_ref().map(|x| &x.background_jobs_can_start);
        if let Some(background) = background_jobs_can_start {
            background.clone().wait().await
        }

        tenant.load(init_order, ctx).await.context("load")?;

        Self::background(
            guard,
            tenant.conf,
            tenant.remote_storage.clone(),
            tenants,
            tenant,
        )
        .await
    }

    async fn prepare(
        tenants: &tokio::sync::RwLock<TenantsMap>,
        tenant_id: TenantId,
    ) -> Result<(Arc<Tenant>, tokio::sync::OwnedMutexGuard<Self>), DeleteTenantError> {
        let m = tenants.read().await;

        let tenant = m
            .get(&tenant_id)
            .ok_or(GetTenantError::NotFound(tenant_id))?;

        // FIXME: unsure about active only. Our init jobs may not be cancellable properly,
        // so at least for now allow deletions only for active tenants. TODO recheck
        // Broken is needed for retries.
        if !(tenant.is_active() || tenant.is_broken()) {
            return Err(GetTenantError::NotActive(tenant_id).into());
        }

        let guard = Arc::clone(&tenant.delete_progress)
            .try_lock_owned()
            .map_err(|_| DeleteTenantError::AlreadyInProgress)?;

        // make pageserver shutdown not to wait for our completion
        let (_, progress) = completion::channel();

        // It would be good to only set stopping here and continue shutdown in the background, but shutdown is not idempotent.
        // i e it is an error to do:
        // tenant.set_stopping
        // tenant.shutdown
        // Its also bad that we're holding tenants.read here.
        // TODO relax set_stopping to be idempotent.
        if tenant.shutdown(progress, false).await.is_err() {
            return Err(DeleteTenantError::Other(anyhow::anyhow!(
                "tenant shutdown is already in progress"
            )));
        }

        Ok((Arc::clone(tenant), guard))
    }

    fn schedule_background(
        guard: OwnedMutexGuard<Self>,
        conf: &'static PageServerConf,
        remote_storage: Option<GenericRemoteStorage>,
        tenants: &'static tokio::sync::RwLock<TenantsMap>,
        tenant: Arc<Tenant>,
    ) {
        let tenant_id = tenant.tenant_id;

        task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            TaskKind::TimelineDeletionWorker,
            Some(tenant_id),
            None,
            "tenant_delete",
            false,
            async move {
                if let Err(err) =
                    Self::background(guard, conf, remote_storage, tenants, &tenant).await
                {
                    error!("Error: {err:#}");
                    tenant.set_broken(err.to_string()).await;
                };
                Ok(())
            }
            .instrument({
                let span = tracing::info_span!(parent: None, "delete_tenant", tenant_id=%tenant_id);
                span.follows_from(Span::current());
                span
            }),
        );
    }

    async fn background(
        mut guard: OwnedMutexGuard<Self>,
        conf: &PageServerConf,
        remote_storage: Option<GenericRemoteStorage>,
        tenants: &'static tokio::sync::RwLock<TenantsMap>,
        tenant: &Arc<Tenant>,
    ) -> Result<(), DeleteTenantError> {
        // Tree sort timelines, schedule delete for them. Mention retries from the console side.
        // Note that if deletion fails we dont mark timelines as broken,
        // the whole tenant will become broken as by `Self::schedule_background` logic
        let already_running_timeline_deletions = schedule_ordered_timeline_deletions(tenant)
            .await
            .context("run_ordered_timeline_deletions")?;

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

        assert_timeline_dir_empty(conf, tenant).await?;

        remove_tenant_remote_delete_mark(conf, remote_storage.as_ref(), &tenant.tenant_id).await?;

        cleanup_remaining_fs_traces(conf, &tenant.tenant_id)
            .await
            .context("cleanup_remaining_fs_traces")?;

        let mut locked = tenants.write().await;
        // NOTE:
        // race with shutdown? -- not a problem because of completion::Barrier in TenantState::Stopping
        // race with detach?
        // race with ignore?
        // can be fixed if migrated to this flow (it can be generalized to support other tenant scoped destructive operations)
        if locked.remove(&tenant.tenant_id).is_none() {
            warn!("Tenant got removed from tenants map during deletion");
        };

        *guard = Self::Finished;

        Ok(())
    }
}
