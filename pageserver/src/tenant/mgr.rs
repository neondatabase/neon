//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use std::collections::{hash_map, HashMap};
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;

use anyhow::Context;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use tracing::*;

use remote_storage::GenericRemoteStorage;
use utils::crashsafe;

use crate::config::PageServerConf;
use crate::context::RequestContext;
use crate::tenant::config::TenantConfOpt;
use crate::tenant::{Tenant, TenantState};
use crate::IGNORED_TENANT_FILE_NAME;

use utils::fs_ext::PathExt;
use utils::id::{TenantId, TimelineId};

static TENANTS: Lazy<RwLock<HashMap<TenantId, Arc<Tenant>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Initialize repositories with locally available timelines.
/// Timelines that are only partially available locally (remote storage has more data than this pageserver)
/// are scheduled for download and added to the tenant once download is completed.
#[instrument(skip(conf, remote_storage))]
pub async fn init_tenant_mgr(
    conf: &'static PageServerConf,
    remote_storage: Option<GenericRemoteStorage>,
) -> anyhow::Result<()> {
    // Scan local filesystem for attached tenants
    let mut number_of_tenants = 0;
    let tenants_dir = conf.tenants_path();

    let mut dir_entries = fs::read_dir(&tenants_dir)
        .await
        .with_context(|| format!("Failed to list tenants dir {tenants_dir:?}"))?;

    loop {
        match dir_entries.next_entry().await {
            Ok(None) => break,
            Ok(Some(dir_entry)) => {
                let tenant_dir_path = dir_entry.path();
                if crate::is_temporary(&tenant_dir_path) {
                    info!(
                        "Found temporary tenant directory, removing: {}",
                        tenant_dir_path.display()
                    );
                    if let Err(e) = fs::remove_dir_all(&tenant_dir_path).await {
                        error!(
                            "Failed to remove temporary directory '{}': {:?}",
                            tenant_dir_path.display(),
                            e
                        );
                    }
                } else {
                    // This case happens if we crash during attach before creating the attach marker file
                    let is_empty = tenant_dir_path.is_empty_dir().with_context(|| {
                        format!("Failed to check whether {tenant_dir_path:?} is an empty dir")
                    })?;
                    if is_empty {
                        info!("removing empty tenant directory {tenant_dir_path:?}");
                        if let Err(e) = fs::remove_dir(&tenant_dir_path).await {
                            error!(
                                "Failed to remove empty tenant directory '{}': {e:#}",
                                tenant_dir_path.display()
                            )
                        }
                        continue;
                    }

                    let tenant_ignore_mark_file = tenant_dir_path.join(IGNORED_TENANT_FILE_NAME);
                    if tenant_ignore_mark_file.exists() {
                        info!("Found an ignore mark file {tenant_ignore_mark_file:?}, skipping the tenant");
                        continue;
                    }

                    match schedule_local_tenant_processing(
                        conf,
                        &tenant_dir_path,
                        remote_storage.clone(),
                    ) {
                        Ok(tenant) => {
                            TENANTS.write().await.insert(tenant.tenant_id(), tenant);
                            number_of_tenants += 1;
                        }
                        Err(e) => {
                            error!("Failed to collect tenant files from dir {tenants_dir:?} for entry {dir_entry:?}, reason: {e:#}");
                        }
                    }
                }
            }
            Err(e) => {
                // On error, print it, but continue with the other tenants. If we error out
                // here, the pageserver startup fails altogether, causing outage for *all*
                // tenants. That seems worse.
                error!(
                    "Failed to list tenants dir entry in directory {tenants_dir:?}, reason: {e:?}"
                );
            }
        }
    }

    info!("Processed {number_of_tenants} local tenants at startup");
    Ok(())
}

pub fn schedule_local_tenant_processing(
    conf: &'static PageServerConf,
    tenant_path: &Path,
    remote_storage: Option<GenericRemoteStorage>,
) -> anyhow::Result<Arc<Tenant>> {
    anyhow::ensure!(
        tenant_path.is_dir(),
        "Cannot load tenant from path {tenant_path:?}, it either does not exist or not a directory"
    );
    anyhow::ensure!(
        !crate::is_temporary(tenant_path),
        "Cannot load tenant from temporary path {tenant_path:?}"
    );
    anyhow::ensure!(
        !tenant_path.is_empty_dir().with_context(|| {
            format!("Failed to check whether {tenant_path:?} is an empty dir")
        })?,
        "Cannot load tenant from empty directory {tenant_path:?}"
    );

    let tenant_id = tenant_path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .parse::<TenantId>()
        .with_context(|| {
            format!("Could not parse tenant id out of the tenant dir name in path {tenant_path:?}")
        })?;

    let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(tenant_id);
    anyhow::ensure!(
        !conf.tenant_ignore_mark_file_path(tenant_id).exists(),
        "Cannot load tenant, ignore mark found at {tenant_ignore_mark:?}"
    );

    let tenant = if conf.tenant_attaching_mark_file_path(&tenant_id).exists() {
        info!("tenant {tenant_id} has attaching mark file, resuming its attach operation");
        if let Some(remote_storage) = remote_storage {
            Tenant::spawn_attach(conf, tenant_id, remote_storage)
        } else {
            warn!("tenant {tenant_id} has attaching mark file, but pageserver has no remote storage configured");
            Tenant::create_broken_tenant(conf, tenant_id)
        }
    } else {
        info!("tenant {tenant_id} is assumed to be loadable, starting load operation");
        // Start loading the tenant into memory. It will initially be in Loading state.
        Tenant::spawn_load(conf, tenant_id, remote_storage)
    };
    Ok(tenant)
}

///
/// Shut down all tenants. This runs as part of pageserver shutdown.
///
pub async fn shutdown_all_tenants() {
    let tenants_to_shut_down = {
        let mut m = TENANTS.write().await;
        let mut tenants_to_shut_down = Vec::with_capacity(m.len());
        for (_, tenant) in m.drain() {
            if tenant.is_active() {
                // updates tenant state, forbidding new GC and compaction iterations from starting
                tenant.set_stopping();
                tenants_to_shut_down.push(tenant)
            }
        }
        drop(m);
        tenants_to_shut_down
    };

    let mut shutdown_futures: FuturesUnordered<_> = FuturesUnordered::new();
    for tenant in tenants_to_shut_down.iter() {
        shutdown_futures.push(tenant.graceful_shutdown(true));
    }
    while let Some(_result) = shutdown_futures.next().await {}
}

pub async fn create_tenant(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
    remote_storage: Option<GenericRemoteStorage>,
) -> anyhow::Result<Option<Arc<Tenant>>> {
    match TENANTS.write().await.entry(tenant_id) {
        hash_map::Entry::Occupied(_) => {
            debug!("tenant {tenant_id} already exists");
            Ok(None)
        }
        hash_map::Entry::Vacant(v) => {
            // Hold the write_tenants() lock, since all of this is local IO.
            // If this section ever becomes contentious, introduce a new `TenantState::Creating`.
            let tenant_directory = super::create_tenant_files(conf, tenant_conf, tenant_id)?;
            let created_tenant =
                schedule_local_tenant_processing(conf, &tenant_directory, remote_storage)?;
            let crated_tenant_id = created_tenant.tenant_id();
            anyhow::ensure!(
                tenant_id == crated_tenant_id,
                "loaded created tenant has unexpected tenant id (expect {tenant_id} != actual {crated_tenant_id})",
            );
            v.insert(Arc::clone(&created_tenant));
            Ok(Some(created_tenant))
        }
    }
}

pub async fn update_tenant_config(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: TenantId,
    cxt: &RequestContext,
) -> anyhow::Result<()> {
    info!("configuring tenant {tenant_id}");
    let (tenant, _cxt) = get_active_tenant(tenant_id, cxt).await?;

    tenant.update_tenant_config(tenant_conf);
    Tenant::persist_tenant_config(&conf.tenant_config_path(tenant_id), tenant_conf, false)?;
    Ok(())
}

/// Gets the tenant from the in-memory data, erroring if it's absent or is not fitting to the query.
/// `active_only = true` allows to query only tenants that are ready for operations, erroring on other kinds of tenants.
pub async fn get_active_tenant(
    tenant_id: TenantId,
    parent_cxt: &RequestContext,
) -> anyhow::Result<(Arc<Tenant>, RequestContext)> {
    let tenant = get_tenant(tenant_id).await?;
    let tenant_cxt = match tenant.get_context(parent_cxt) {
        Ok(cxt) => cxt,
        Err(state) => anyhow::bail!("Tenant {} is not active, state: {:?}", tenant_id, state,),
    };
    Ok((tenant, tenant_cxt))
}

pub async fn get_tenant(tenant_id: TenantId) -> anyhow::Result<Arc<Tenant>> {
    let m = TENANTS.read().await;
    let tenant = m
        .get(&tenant_id)
        .with_context(|| format!("Tenant {tenant_id} not found in the local state"))?;

    Ok(Arc::clone(tenant))
}

pub async fn delete_timeline(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    cxt: &RequestContext,
) -> anyhow::Result<()> {
    match get_active_tenant(tenant_id, cxt).await {
        Ok((tenant, cxt)) => {
            tenant.delete_timeline(timeline_id, &cxt).await?;
        }
        Err(e) => anyhow::bail!("Cannot access tenant {tenant_id} in local tenant state: {e:?}"),
    }

    Ok(())
}

pub async fn detach_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
) -> anyhow::Result<()> {
    remove_tenant_from_memory(tenant_id, async {
        let local_tenant_directory = conf.tenant_path(&tenant_id);
        fs::remove_dir_all(&local_tenant_directory)
            .await
            .with_context(|| {
                format!("Failed to remove local tenant directory {local_tenant_directory:?}")
            })?;
        Ok(())
    })
    .await
}

pub async fn load_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
    remote_storage: Option<GenericRemoteStorage>,
) -> anyhow::Result<()> {
    run_if_no_tenant_in_memory(tenant_id, |vacant_entry| {
        let tenant_path = conf.tenant_path(&tenant_id);
        let tenant_ignore_mark = conf.tenant_ignore_mark_file_path(tenant_id);
        if tenant_ignore_mark.exists() {
            std::fs::remove_file(&tenant_ignore_mark)
                .with_context(|| format!("Failed to remove tenant ignore mark {tenant_ignore_mark:?} during tenant loading"))?;
        }

        let new_tenant = schedule_local_tenant_processing(conf, &tenant_path, remote_storage)
            .with_context(|| {
                format!("Failed to schedule tenant processing in path {tenant_path:?}")
            })?;

        vacant_entry.insert(new_tenant);
        Ok(())
    }).await
}

pub async fn ignore_tenant(
    conf: &'static PageServerConf,
    tenant_id: TenantId,
) -> anyhow::Result<()> {
    remove_tenant_from_memory(tenant_id, async {
        let ignore_mark_file = conf.tenant_ignore_mark_file_path(tenant_id);
        fs::File::create(&ignore_mark_file)
            .await
            .context("Failed to create ignore mark file")
            .and_then(|_| {
                crashsafe::fsync_file_and_parent(&ignore_mark_file)
                    .context("Failed to fsync ignore mark file")
            })
            .with_context(|| format!("Failed to crate ignore mark for tenant {tenant_id}"))?;
        Ok(())
    })
    .await
}

///
/// Get list of tenants, for the mgmt API
///
pub async fn list_tenants() -> Vec<(TenantId, TenantState)> {
    TENANTS
        .read()
        .await
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
    remote_storage: GenericRemoteStorage,
) -> anyhow::Result<()> {
    run_if_no_tenant_in_memory(tenant_id, |vacant_entry| {
        let tenant_path = conf.tenant_path(&tenant_id);
        anyhow::ensure!(
            !tenant_path.exists(),
            "Cannot attach tenant {tenant_id}, local tenant directory already exists"
        );

        let tenant = Tenant::spawn_attach(conf, tenant_id, remote_storage);
        vacant_entry.insert(tenant);

        Ok(())
    })
    .await
}

async fn run_if_no_tenant_in_memory<F, V>(tenant_id: TenantId, run: F) -> anyhow::Result<V>
where
    F: FnOnce(hash_map::VacantEntry<TenantId, Arc<Tenant>>) -> anyhow::Result<V>,
{
    match TENANTS.write().await.entry(tenant_id) {
        hash_map::Entry::Occupied(e) => {
            anyhow::bail!(
                "tenant {tenant_id} already exists, state: {:?}",
                e.get().current_state()
            )
        }
        hash_map::Entry::Vacant(v) => run(v),
    }
}

/// Stops and removes the tenant from memory, if it's not [`TenantState::Stopping`] already, bails otherwise.
/// Allows to remove other tenant resources manually, via `tenant_cleanup`.
/// If the cleanup fails, tenant will stay in memory in [`TenantState::Broken`] state, and another removal
/// operation would be needed to remove it.
async fn remove_tenant_from_memory<V, F>(
    tenant_id: TenantId,
    tenant_cleanup: F,
) -> anyhow::Result<V>
where
    F: std::future::Future<Output = anyhow::Result<V>>,
{
    // It's important to keep the tenant in memory after the final cleanup, to avoid cleanup races.
    // The exclusive lock here ensures we don't miss the tenant state updates before trying another removal.
    // tenant-wde cleanup operations may take some time (removing the entire tenant directory), we want to
    // avoid holding the lock for the entire process.
    let tenant = {
        let tenants_accessor = TENANTS.write().await;
        match tenants_accessor.get(&tenant_id) {
            Some(tenant) => match tenant.current_state() {
                TenantState::Attaching
                | TenantState::Loading
                | TenantState::Broken
                | TenantState::Active => {
                    tenant.set_stopping();
                    Arc::clone(tenant)
                }
                TenantState::Stopping => {
                    anyhow::bail!("Tenant {tenant_id} is stopping already")
                }
            },
            None => anyhow::bail!("Tenant not found for id {tenant_id}"),
        }
    };

    // Shut down all tenant and timeline tasks.
    tenant.graceful_shutdown(true).await;

    // All tasks that operated on the tenant or any of its timelines have no finished,
    // and they are in Stopped state so that new ones cannot appear anymore. Proceed
    // with the cleanup.
    match tenant_cleanup
        .await
        .with_context(|| format!("Failed to run cleanup for tenant {tenant_id}"))
    {
        Ok(hook_value) => {
            let mut tenants_accessor = TENANTS.write().await;
            if tenants_accessor.remove(&tenant_id).is_none() {
                warn!("Tenant {tenant_id} got removed from memory before operation finished");
            }
            Ok(hook_value)
        }
        Err(e) => {
            let tenants_accessor = TENANTS.read().await;
            match tenants_accessor.get(&tenant_id) {
                Some(tenant) => tenant.set_broken(),
                None => warn!("Tenant {tenant_id} got removed from memory"),
            }
            Err(e)
        }
    }
}
