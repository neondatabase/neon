//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use std::collections::hash_map;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use tracing::*;

use remote_storage::GenericRemoteStorage;

use crate::config::PageServerConf;
use crate::task_mgr::{self, TaskKind};
use crate::tenant::{Tenant, TenantState};
use crate::tenant_config::TenantConfOpt;
use crate::walredo::PostgresRedoManager;
use crate::TEMP_FILE_SUFFIX;

use utils::crashsafe::{self, path_with_suffix_extension};
use utils::fs_ext::PathExt;
use utils::id::{TenantId, TimelineId};

mod tenants_state {
    use once_cell::sync::Lazy;
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    };
    use utils::id::TenantId;

    use crate::tenant::Tenant;

    static TENANTS: Lazy<RwLock<HashMap<TenantId, Arc<Tenant>>>> =
        Lazy::new(|| RwLock::new(HashMap::new()));

    pub(super) fn read_tenants() -> RwLockReadGuard<'static, HashMap<TenantId, Arc<Tenant>>> {
        TENANTS
            .read()
            .expect("Failed to read() tenants lock, it got poisoned")
    }

    pub(super) fn write_tenants() -> RwLockWriteGuard<'static, HashMap<TenantId, Arc<Tenant>>> {
        TENANTS
            .write()
            .expect("Failed to write() tenants lock, it got poisoned")
    }
}

/// Initialize repositories with locally available timelines.
/// Timelines that are only partially available locally (remote storage has more data than this pageserver)
/// are scheduled for download and added to the tenant once download is completed.
pub fn init_tenant_mgr(
    conf: &'static PageServerConf,
    remote_storage: Option<GenericRemoteStorage>,
) -> anyhow::Result<()> {
    let _entered = info_span!("init_tenant_mgr").entered();

    // Scan local filesystem for attached tenants
    let mut number_of_tenants = 0;
    let tenants_dir = conf.tenants_path();
    for dir_entry in std::fs::read_dir(&tenants_dir)
        .with_context(|| format!("Failed to list tenants dir {}", tenants_dir.display()))?
    {
        match &dir_entry {
            Ok(dir_entry) => {
                let tenant_dir_path = dir_entry.path();
                if crate::is_temporary(&tenant_dir_path) {
                    info!(
                        "Found temporary tenant directory, removing: {}",
                        tenant_dir_path.display()
                    );
                    if let Err(e) = std::fs::remove_dir_all(&tenant_dir_path) {
                        error!(
                            "Failed to remove temporary directory '{}': {:?}",
                            tenant_dir_path.display(),
                            e
                        );
                    }
                } else {
                    match load_local_tenant(conf, &tenant_dir_path, remote_storage.clone()) {
                        Ok(Some(_)) => number_of_tenants += 1,
                        Ok(None) => {
                            // This case happens if we crash during attach before creating the attach marker file
                            if let Err(e) = std::fs::remove_dir(&tenant_dir_path) {
                                error!(
                                    "Failed to remove empty tenant directory '{}': {e:#}",
                                    tenant_dir_path.display()
                                )
                            }
                        }
                        Err(e) => {
                            error!(
                            "Failed to collect tenant files from dir '{}' for entry {:?}, reason: {:#}",
                            tenants_dir.display(),
                            dir_entry,
                            e
                        );
                        }
                    }
                }
            }
            Err(e) => {
                // On error, print it, but continue with the other tenants. If we error out
                // here, the pageserver startup fails altogether, causing outage for *all*
                // tenants. That seems worse.
                error!(
                    "Failed to list tenants dir entry {:?} in directory {}, reason: {:?}",
                    dir_entry,
                    tenants_dir.display(),
                    e,
                );
            }
        }
    }

    info!("Processed {number_of_tenants} local tenants at startup");
    Ok(())
}

fn load_local_tenant(
    conf: &'static PageServerConf,
    tenant_path: &Path,
    remote_storage: Option<GenericRemoteStorage>,
) -> anyhow::Result<Option<()>> {
    if !tenant_path.is_dir() {
        anyhow::bail!("tenant_path is not a directory: {tenant_path:?}")
    }

    let is_empty = tenant_path
        .is_empty_dir()
        .context("check whether tenant_path is an empty dir")?;
    if is_empty {
        info!("skipping empty tenant directory {tenant_path:?}");
        return Ok(None);
    }

    let tenant_id = tenant_path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .parse::<TenantId>()
        .context("Could not parse tenant id out of the tenant dir name")?;

    let tenant = if conf.tenant_attaching_mark_file_path(&tenant_id).exists() {
        info!("tenant {tenant_id} has attaching mark file, resuming its attach operation");
        if let Some(remote_storage) = remote_storage {
            Tenant::spawn_attach(conf, tenant_id, &remote_storage)?
        } else {
            warn!("tenant {tenant_id} has attaching mark file, but pageserver has no remote storage configured");
            // XXX there should be a constructor to make a broken tenant
            // TODO should we use Tenant::load_tenant_config() here?
            let tenant_conf = TenantConfOpt::default();
            let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
            let tenant = Tenant::new(
                TenantState::Broken,
                conf,
                tenant_conf,
                wal_redo_manager,
                tenant_id,
                remote_storage,
            );
            Arc::new(tenant)
        }
    } else {
        info!("tenant {tenant_id} is assumed to be loadable, starting load operation");
        // Start loading the tenant into memory. It will initially be in Loading state.
        Tenant::spawn_load(conf, tenant_id, remote_storage)?
    };
    tenants_state::write_tenants().insert(tenant_id, tenant);
    Ok(Some(()))
}

///
/// Shut down all tenants. This runs as part of pageserver shutdown.
///
pub async fn shutdown_all_tenants() {
    let tenants_to_shut_down = {
        let mut m = tenants_state::write_tenants();
        let mut tenants_to_shut_down = Vec::with_capacity(m.len());
        for (_, tenant) in m.drain() {
            if tenant.is_active() {
                // updates tenant state, forbidding new GC and compaction iterations from starting
                tenant.set_state(TenantState::Paused);
                tenants_to_shut_down.push(tenant)
            }
        }
        drop(m);
        tenants_to_shut_down
    };

    // Shut down all existing walreceiver connections and stop accepting the new ones.
    task_mgr::shutdown_tasks(Some(TaskKind::WalReceiverManager), None, None).await;

    // Ok, no background tasks running anymore. Flush any remaining data in
    // memory to disk.
    //
    // We assume that any incoming connections that might request pages from
    // the tenant have already been terminated by the caller, so there
    // should be no more activity in any of the repositories.
    //
    // On error, log it but continue with the shutdown for other tenants.
    for tenant in tenants_to_shut_down {
        let tenant_id = tenant.tenant_id();
        debug!("shutdown tenant {tenant_id}");

        if let Err(err) = tenant.checkpoint().await {
            error!("Could not checkpoint tenant {tenant_id} during shutdown: {err:?}");
        }
    }
}

fn create_tenant_files(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
) -> anyhow::Result<()> {
    let target_tenant_directory = conf.tenant_path(&tenant_id);
    anyhow::ensure!(
        !target_tenant_directory.exists(),
        "cannot create new tenant repo: '{tenant_id}' directory already exists",
    );

    let temporary_tenant_dir =
        path_with_suffix_extension(&target_tenant_directory, TEMP_FILE_SUFFIX);
    debug!(
        "Creating temporary directory structure in {}",
        temporary_tenant_dir.display()
    );

    // top-level dir may exist if we are creating it through CLI
    crashsafe::create_dir_all(&temporary_tenant_dir).with_context(|| {
        format!(
            "could not create temporary tenant directory {}",
            temporary_tenant_dir.display()
        )
    })?;

    let creation_result = try_create_target_tenant_dir(
        conf,
        tenant_conf,
        tenant_id,
        &temporary_tenant_dir,
        &target_tenant_directory,
    );

    if creation_result.is_err() {
        error!("Failed to create directory structure for tenant {tenant_id}, cleaning tmp data");
        if let Err(e) = fs::remove_dir_all(&temporary_tenant_dir) {
            error!("Failed to remove temporary tenant directory {temporary_tenant_dir:?}: {e}")
        } else if let Err(e) = crashsafe::fsync(&temporary_tenant_dir) {
            error!(
                "Failed to fsync removed temporary tenant directory {temporary_tenant_dir:?}: {e}"
            )
        }
    }

    creation_result
}

fn try_create_target_tenant_dir(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
    temporary_tenant_dir: &Path,
    target_tenant_directory: &Path,
) -> Result<(), anyhow::Error> {
    let temporary_tenant_timelines_dir = rebase_directory(
        &conf.timelines_path(&tenant_id),
        target_tenant_directory,
        temporary_tenant_dir,
    )
    .with_context(|| format!("Failed to resolve tenant {tenant_id} temporary timelines dir"))?;
    let temporary_tenant_config_path = rebase_directory(
        &conf.tenant_config_path(tenant_id),
        target_tenant_directory,
        temporary_tenant_dir,
    )
    .with_context(|| format!("Failed to resolve tenant {tenant_id} temporary config path"))?;

    Tenant::persist_tenant_config(&temporary_tenant_config_path, tenant_conf, true).with_context(
        || {
            format!(
                "Failed to write tenant {} config to {}",
                tenant_id,
                temporary_tenant_config_path.display()
            )
        },
    )?;
    crashsafe::create_dir(&temporary_tenant_timelines_dir).with_context(|| {
        format!(
            "could not create tenant {} temporary timelines directory {}",
            tenant_id,
            temporary_tenant_timelines_dir.display()
        )
    })?;
    fail::fail_point!("tenant-creation-before-tmp-rename", |_| {
        anyhow::bail!("failpoint tenant-creation-before-tmp-rename");
    });

    fs::rename(&temporary_tenant_dir, target_tenant_directory).with_context(|| {
        format!(
            "failed to move tenant {} temporary directory {} into the permanent one {}",
            tenant_id,
            temporary_tenant_dir.display(),
            target_tenant_directory.display()
        )
    })?;
    let target_dir_parent = target_tenant_directory.parent().with_context(|| {
        format!(
            "Failed to get tenant {} dir parent for {}",
            tenant_id,
            target_tenant_directory.display()
        )
    })?;
    crashsafe::fsync(target_dir_parent).with_context(|| {
        format!(
            "Failed to fsync renamed directory's parent {} for tenant {}",
            target_dir_parent.display(),
            tenant_id,
        )
    })?;

    Ok(())
}

fn rebase_directory(original_path: &Path, base: &Path, new_base: &Path) -> anyhow::Result<PathBuf> {
    let relative_path = original_path.strip_prefix(base).with_context(|| {
        format!(
            "Failed to strip base prefix '{}' off path '{}'",
            base.display(),
            original_path.display()
        )
    })?;
    Ok(new_base.join(relative_path))
}

pub fn create_tenant(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
    remote_storage: Option<&GenericRemoteStorage>,
) -> anyhow::Result<Option<TenantId>> {
    match tenants_state::write_tenants().entry(tenant_id) {
        hash_map::Entry::Occupied(_) => {
            debug!("tenant {tenant_id} already exists");
            Ok(None)
        }
        hash_map::Entry::Vacant(v) => {
            let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
            create_tenant_files(conf, tenant_conf, tenant_id)?;
            // create tenant in Active state so it is possible to issue create_timeline
            // request. Which on timeline activation will trigger tenant activation
            let tenant = Arc::new(Tenant::new(
                TenantState::Active {
                    background_jobs_running: false,
                },
                conf,
                tenant_conf,
                wal_redo_manager,
                tenant_id,
                remote_storage.cloned(),
            ));
            v.insert(tenant);
            Ok(Some(tenant_id))
        }
    }
}

pub fn update_tenant_config(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
) -> anyhow::Result<()> {
    info!("configuring tenant {tenant_id}");
    get_tenant(tenant_id, true)?.update_tenant_config(tenant_conf);
    Tenant::persist_tenant_config(&conf.tenant_config_path(tenant_id), tenant_conf, false)?;
    Ok(())
}

/// Gets the tenant from the in-memory data, erroring if it's absent or is not fitting to the query.
/// `active_only = true` allows to query only tenants that are ready for operations, erroring on other kinds of tenants.
pub fn get_tenant(tenant_id: TenantId, active_only: bool) -> anyhow::Result<Arc<Tenant>> {
    let m = tenants_state::read_tenants();
    let tenant = m
        .get(&tenant_id)
        .with_context(|| format!("Tenant {tenant_id} not found in the local state"))?;
    if active_only && !tenant.is_active() {
        anyhow::bail!(
            "Tenant {tenant_id} is not active. Current state: {:?}",
            tenant.current_state()
        )
    } else {
        Ok(Arc::clone(tenant))
    }
}

pub async fn delete_timeline(tenant_id: TenantId, timeline_id: TimelineId) -> anyhow::Result<()> {
    // Start with the shutdown of timeline tasks (this shuts down the walreceiver)
    // It is important that we do not take locks here, and do not check whether the timeline exists
    // because if we hold tenants_state::write_tenants() while awaiting for the tasks to join
    // we cannot create new timelines and tenants, and that can take quite some time,
    // it can even become stuck due to a bug making whole pageserver unavailable for some operations
    // so this is the way how we deal with concurrent delete requests: shutdown everythig, wait for confirmation
    // and then try to actually remove timeline from inmemory state and this is the point when concurrent requests
    // will synchronize and either fail with the not found error or succeed

    debug!("waiting for wal receiver to shutdown");
    task_mgr::shutdown_tasks(
        Some(TaskKind::WalReceiverManager),
        Some(tenant_id),
        Some(timeline_id),
    )
    .await;
    debug!("wal receiver shutdown confirmed");

    info!("waiting for timeline tasks to shutdown");
    task_mgr::shutdown_tasks(None, Some(tenant_id), Some(timeline_id)).await;
    info!("timeline task shutdown completed");
    match get_tenant(tenant_id, true) {
        Ok(tenant) => {
            tenant.delete_timeline(timeline_id)?;
            if tenant.list_timelines().is_empty() {
                tenant.activate(false);
            }
        }
        Err(e) => anyhow::bail!("Cannot access tenant {tenant_id} in local tenant state: {e:?}"),
    }

    Ok(())
}

pub async fn detach_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
) -> anyhow::Result<()> {
    let tenant = match {
        let mut tenants_accessor = tenants_state::write_tenants();
        tenants_accessor.remove(&tenant_id)
    } {
        Some(tenant) => tenant,
        None => anyhow::bail!("Tenant not found for id {tenant_id}"),
    };

    tenant.set_state(TenantState::Paused);
    // shutdown all tenant and timeline tasks: gc, compaction, page service)
    task_mgr::shutdown_tasks(None, Some(tenant_id), None).await;

    // If removal fails there will be no way to successfully retry detach,
    // because the tenant no longer exists in the in-memory map. And it needs to be removed from it
    // before we remove files, because it contains references to tenant
    // which references ephemeral files which are deleted on drop. So if we keep these references,
    // we will attempt to remove files which no longer exist. This can be fixed by having shutdown
    // mechanism for tenant that will clean temporary data to avoid any references to ephemeral files
    let local_tenant_directory = conf.tenant_path(&tenant_id);
    fs::remove_dir_all(&local_tenant_directory).with_context(|| {
        format!(
            "Failed to remove local tenant directory '{}'",
            local_tenant_directory.display()
        )
    })?;

    Ok(())
}

///
/// Get list of tenants, for the mgmt API
///
pub fn list_tenants() -> Vec<(TenantId, TenantState)> {
    tenants_state::read_tenants()
        .iter()
        .map(|(id, tenant)| (*id, tenant.current_state()))
        .collect()
}

/// Execute Attach mgmt API command.
///
/// Downloading all the tenant data is performed in the background, this merely
/// spawns the background task and returns quickly.
pub async fn attach_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    remote_storage: &GenericRemoteStorage,
) -> anyhow::Result<()> {
    match tenants_state::write_tenants().entry(tenant_id) {
        hash_map::Entry::Occupied(e) => {
            // Cannot attach a tenant that already exists. The error message depends on
            // the state it's in.
            match e.get().current_state() {
                current_state => {
                    anyhow::bail!("tenant already exists, current state: {current_state:?}")
                }
            }
        }
        hash_map::Entry::Vacant(v) => {
            let tenant = Tenant::spawn_attach(conf, tenant_id, remote_storage)?;
            v.insert(tenant);
            Ok(())
        }
    }
}

#[cfg(feature = "testing")]
use {
    crate::repository::GcResult, pageserver_api::models::TimelineGcRequest,
    utils::http::error::ApiError,
};

#[cfg(feature = "testing")]
pub fn immediate_gc(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    gc_req: TimelineGcRequest,
) -> Result<tokio::sync::oneshot::Receiver<Result<GcResult, anyhow::Error>>, ApiError> {
    let guard = tenants_state::read_tenants();

    let tenant = guard
        .get(&tenant_id)
        .map(Arc::clone)
        .with_context(|| format!("Tenant {tenant_id} not found"))
        .map_err(ApiError::NotFound)?;

    let gc_horizon = gc_req.gc_horizon.unwrap_or_else(|| tenant.get_gc_horizon());
    // Use tenant's pitr setting
    let pitr = tenant.get_pitr_interval();

    // Run in task_mgr to avoid race with detach operation
    let (task_done, wait_task_done) = tokio::sync::oneshot::channel();
    task_mgr::spawn(
        &tokio::runtime::Handle::current(),
        TaskKind::GarbageCollector,
        Some(tenant_id),
        Some(timeline_id),
        &format!("timeline_gc_handler garbage collection run for tenant {tenant_id} timeline {timeline_id}"),
        false,
        async move {
            fail::fail_point!("immediate_gc_task_pre");
            let result = tenant
                .gc_iteration(Some(timeline_id), gc_horizon, pitr, true)
                .instrument(info_span!("manual_gc", tenant = %tenant_id, timeline = %timeline_id))
                .await;
                // FIXME: `gc_iteration` can return an error for multiple reasons; we should handle it
                // better once the types support it.
            match task_done.send(result) {
                Ok(_) => (),
                Err(result) => error!("failed to send gc result: {result:?}"),
            }
            Ok(())
        }
    );

    // drop the guard until after we've spawned the task so that timeline shutdown will wait for the task
    drop(guard);

    Ok(wait_task_done)
}
