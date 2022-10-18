//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use std::collections::{hash_map, HashMap};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use tracing::*;

use remote_storage::GenericRemoteStorage;

use crate::config::{PageServerConf, METADATA_FILE_NAME};
use crate::http::models::TenantInfo;
use crate::storage_sync::index::{LayerFileMetadata, RemoteIndex, RemoteTimelineIndex};
use crate::storage_sync::{self, LocalTimelineInitStatus, SyncStartupData, TimelineLocalFiles};
use crate::task_mgr::{self, TaskKind};
use crate::tenant::{
    ephemeral_file::is_ephemeral_file, metadata::TimelineMetadata, Tenant, TenantState,
};
use crate::tenant_config::TenantConfOpt;
use crate::walredo::PostgresRedoManager;
use crate::TEMP_FILE_SUFFIX;

use utils::crashsafe_dir::{self, path_with_suffix_extension};
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
) -> anyhow::Result<RemoteIndex> {
    let _entered = info_span!("init_tenant_mgr").entered();

    let local_tenant_files = local_tenant_timeline_files(conf)
        .context("Failed to collect local tenant timeline files")?;

    let (remote_index, tenants_to_attach) = if let Some(storage) = remote_storage {
        let storage_config = conf
            .remote_storage_config
            .as_ref()
            .expect("remote storage without config");
        let mut broken_tenants = HashMap::new();
        let mut ready_tenants = HashMap::new();
        for (tenant_id, tenant_attach_data) in local_tenant_files.into_iter() {
            match tenant_attach_data {
                TenantAttachData::Ready(t) => {
                    ready_tenants.insert(tenant_id, t);
                }
                TenantAttachData::Broken(e) => {
                    broken_tenants.insert(tenant_id, TenantAttachData::Broken(e));
                }
            }
        }
        let SyncStartupData {
            remote_index,
            local_timeline_init_statuses,
        } = storage_sync::spawn_storage_sync_task(
            conf,
            ready_tenants,
            storage,
            storage_config.max_concurrent_syncs,
            storage_config.max_sync_errors,
        )
        .context("Failed to spawn the storage sync thread")?;

        let n = local_timeline_init_statuses.0.len();
        let mut synced_timelines = local_timeline_init_statuses.0.into_iter().fold(
            HashMap::<TenantId, TenantAttachData>::with_capacity(n),
            |mut new_values, (tenant_id, old_values)| {
                let new_timeline_values = new_values
                    .entry(tenant_id)
                    .or_insert_with(|| TenantAttachData::Ready(HashMap::new()));
                if let TenantAttachData::Ready(t) = new_timeline_values {
                    for (timeline_id, old_value) in old_values {
                        if let LocalTimelineInitStatus::LocallyComplete(metadata) = old_value {
                            t.insert(timeline_id, TimelineLocalFiles::ready(metadata));
                        }
                    }
                }
                new_values
            },
        );
        synced_timelines.extend(broken_tenants);

        (remote_index, synced_timelines)
    } else {
        info!("No remote storage configured, skipping storage sync, considering all local timelines with correct metadata files enabled");
        (RemoteIndex::default(), local_tenant_files)
    };
    attach_local_tenants(conf, &remote_index, tenants_to_attach);

    Ok(remote_index)
}

/// Reads local files to load tenants and their timelines given into pageserver's memory.
/// Ignores other timelines that might be present for tenant, but were not passed as a parameter.
/// Attempts to load as many entites as possible: if a certain timeline fails during the load, the tenant is marked as "Broken",
/// and the load continues.
///
/// For successful tenant attach, it first has to have a `timelines/` subdirectory and a tenant config file that's loaded into memory successfully.
/// If either of the conditions fails, the tenant will be added to memory with [`TenantState::Broken`] state, otherwise we start to load its timelines.
/// Alternatively, tenant is considered loaded successfully, if it's already in pageserver's memory (i.e. was loaded already before).
///
/// Attach happens on startup and sucessful timeline downloads
/// (some subset of timeline files, always including its metadata, after which the new one needs to be registered).
pub fn attach_local_tenants(
    conf: &'static PageServerConf,
    remote_index: &RemoteIndex,
    tenants_to_attach: HashMap<TenantId, TenantAttachData>,
) {
    let _entered = info_span!("attach_local_tenants").entered();
    let number_of_tenants = tenants_to_attach.len();

    for (tenant_id, local_timelines) in tenants_to_attach {
        let mut tenants_accessor = tenants_state::write_tenants();
        let tenant = match tenants_accessor.entry(tenant_id) {
            hash_map::Entry::Occupied(o) => {
                info!("Tenant {tenant_id} was found in pageserver's memory");
                Arc::clone(o.get())
            }
            hash_map::Entry::Vacant(v) => {
                info!("Tenant {tenant_id} was not found in pageserver's memory, loading it");
                let tenant = Arc::new(Tenant::new(
                    conf,
                    TenantConfOpt::default(),
                    Arc::new(PostgresRedoManager::new(conf, tenant_id)),
                    tenant_id,
                    remote_index.clone(),
                    conf.remote_storage_config.is_some(),
                ));
                match local_timelines {
                    TenantAttachData::Broken(_) => {
                        tenant.set_state(TenantState::Broken);
                    }
                    TenantAttachData::Ready(_) => {
                        match Tenant::load_tenant_config(conf, tenant_id) {
                            Ok(tenant_conf) => {
                                tenant.update_tenant_config(tenant_conf);
                                tenant.activate(false);
                            }
                            Err(e) => {
                                error!("Failed to read config for tenant {tenant_id}, disabling tenant: {e:?}");
                                tenant.set_state(TenantState::Broken);
                            }
                        };
                    }
                }
                v.insert(Arc::clone(&tenant));
                tenant
            }
        };
        drop(tenants_accessor);
        match local_timelines {
            TenantAttachData::Broken(e) => warn!("{}", e),
            TenantAttachData::Ready(ref timelines) => {
                info!("Attaching {} timelines for {tenant_id}", timelines.len());
                debug!("Timelines to attach: {local_timelines:?}");
                let has_timelines = !timelines.is_empty();
                let timelines_to_attach = timelines
                    .iter()
                    .map(|(&k, v)| (k, v.metadata().to_owned()))
                    .collect();
                match tenant.init_attach_timelines(timelines_to_attach) {
                    Ok(()) => {
                        info!("successfully loaded local timelines for tenant {tenant_id}");
                        tenant.activate(has_timelines);
                    }
                    Err(e) => {
                        error!("Failed to attach tenant timelines: {e:?}");
                        tenant.set_state(TenantState::Broken);
                    }
                }
            }
        }
    }

    info!("Processed {number_of_tenants} local tenants during attach")
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

        if let Err(err) = tenant.checkpoint() {
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

    let temporary_tenant_timelines_dir = rebase_directory(
        &conf.timelines_path(&tenant_id),
        &target_tenant_directory,
        &temporary_tenant_dir,
    )?;
    let temporary_tenant_config_path = rebase_directory(
        &conf.tenant_config_path(tenant_id),
        &target_tenant_directory,
        &temporary_tenant_dir,
    )?;

    // top-level dir may exist if we are creating it through CLI
    crashsafe_dir::create_dir_all(&temporary_tenant_dir).with_context(|| {
        format!(
            "could not create temporary tenant directory {}",
            temporary_tenant_dir.display()
        )
    })?;
    // first, create a config in the top-level temp directory, fsync the file
    Tenant::persist_tenant_config(&temporary_tenant_config_path, tenant_conf, true)?;
    // then, create a subdirectory in the top-level temp directory, fsynced
    crashsafe_dir::create_dir(&temporary_tenant_timelines_dir).with_context(|| {
        format!(
            "could not create temporary tenant timelines directory {}",
            temporary_tenant_timelines_dir.display()
        )
    })?;

    fail::fail_point!("tenant-creation-before-tmp-rename", |_| {
        anyhow::bail!("failpoint tenant-creation-before-tmp-rename");
    });

    // move-rename tmp directory with all files synced into a permanent directory, fsync its parent
    fs::rename(&temporary_tenant_dir, &target_tenant_directory).with_context(|| {
        format!(
            "failed to move temporary tenant directory {} into the permanent one {}",
            temporary_tenant_dir.display(),
            target_tenant_directory.display()
        )
    })?;
    let target_dir_parent = target_tenant_directory.parent().with_context(|| {
        format!(
            "Failed to get tenant dir parent for {}",
            target_tenant_directory.display()
        )
    })?;
    fs::File::open(target_dir_parent)?.sync_all()?;

    info!(
        "created tenant directory structure in {}",
        target_tenant_directory.display()
    );

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
    remote_index: RemoteIndex,
) -> anyhow::Result<Option<TenantId>> {
    match tenants_state::write_tenants().entry(tenant_id) {
        hash_map::Entry::Occupied(_) => {
            debug!("tenant {tenant_id} already exists");
            Ok(None)
        }
        hash_map::Entry::Vacant(v) => {
            let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
            create_tenant_files(conf, tenant_conf, tenant_id)?;
            let tenant = Arc::new(Tenant::new(
                conf,
                tenant_conf,
                wal_redo_manager,
                tenant_id,
                remote_index,
                conf.remote_storage_config.is_some(),
            ));
            tenant.activate(false);
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
        anyhow::bail!("Tenant {tenant_id} is not active")
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
pub fn list_tenant_info(remote_index: &RemoteTimelineIndex) -> Vec<TenantInfo> {
    tenants_state::read_tenants()
        .iter()
        .map(|(id, tenant)| {
            let has_in_progress_downloads = remote_index
            .tenant_entry(id)
            .map(|entry| entry.has_in_progress_downloads());

            // TODO this is not correct when we might have remote storage sync disabled:
            // we keep `RemoteTimelineIndex` in memory anyway for simplicity and this error message is printed still
            if has_in_progress_downloads.is_none() {
                error!("timeline is not found in remote index while it is present in the tenants registry")
            }

            TenantInfo {
                id: *id,
                state: tenant.current_state(),
                current_physical_size: None,
                has_in_progress_downloads,
            }
        })
        .collect()
}

#[derive(Debug)]
pub enum TenantAttachData {
    Ready(HashMap<TimelineId, TimelineLocalFiles>),
    Broken(anyhow::Error),
}
/// Attempts to collect information about all tenant and timelines, existing on the local FS.
/// If finds any, deletes all temporary files and directories, created before. Also removes empty directories,
/// that may appear due to such removals.
/// Does not fail on particular timeline or tenant collection errors, rather logging them and ignoring the entities.
fn local_tenant_timeline_files(
    config: &'static PageServerConf,
) -> anyhow::Result<HashMap<TenantId, TenantAttachData>> {
    let _entered = info_span!("local_tenant_timeline_files").entered();

    let mut local_tenant_timeline_files = HashMap::new();
    let tenants_dir = config.tenants_path();
    for tenants_dir_entry in fs::read_dir(&tenants_dir)
        .with_context(|| format!("Failed to list tenants dir {}", tenants_dir.display()))?
    {
        match &tenants_dir_entry {
            Ok(tenants_dir_entry) => {
                let tenant_dir_path = tenants_dir_entry.path();
                if is_temporary(&tenant_dir_path) {
                    info!(
                        "Found temporary tenant directory, removing: {}",
                        tenant_dir_path.display()
                    );
                    if let Err(e) = fs::remove_dir_all(&tenant_dir_path) {
                        error!(
                            "Failed to remove temporary directory '{}': {:?}",
                            tenant_dir_path.display(),
                            e
                        );
                    }
                } else {
                    match collect_timelines_for_tenant(config, &tenant_dir_path) {
                        Ok((tenant_id, TenantAttachData::Broken(e))) => {
                            local_tenant_timeline_files.entry(tenant_id).or_insert(TenantAttachData::Broken(e));
                        },
                        Ok((tenant_id, TenantAttachData::Ready(collected_files))) => {
                            if collected_files.is_empty() {
                                match remove_if_empty(&tenant_dir_path) {
                                    Ok(true) => info!("Removed empty tenant directory {}", tenant_dir_path.display()),
                                    Ok(false) => {
                                        // insert empty timeline entry: it has some non-temporary files inside that we cannot remove
                                        // so make obvious for HTTP API callers, that something exists there and try to load the tenant
                                        let _ = local_tenant_timeline_files.entry(tenant_id).or_insert_with(|| TenantAttachData::Ready(HashMap::new()));
                                    },
                                    Err(e) => error!("Failed to remove empty tenant directory: {e:?}"),
                                }
                            } else {
                                match local_tenant_timeline_files.entry(tenant_id) {
                                    hash_map::Entry::Vacant(entry) => {
                                        entry.insert(TenantAttachData::Ready(collected_files));
                                    }
                                    hash_map::Entry::Occupied(entry) =>{
                                        if let TenantAttachData::Ready(old_timelines) = entry.into_mut() {
                                            old_timelines.extend(collected_files);
                                        }
                                    },
                                }
                            }
                        },
                        Err(e) => error!(
                            "Failed to collect tenant files from dir '{}' for entry {:?}, reason: {:#}",
                            tenants_dir.display(),
                            tenants_dir_entry,
                            e
                        ),
                    }
                }
            }
            Err(e) => error!(
                "Failed to list tenants dir entry {:?} in directory {}, reason: {:?}",
                tenants_dir_entry,
                tenants_dir.display(),
                e
            ),
        }
    }

    info!(
        "Collected files for {} tenants",
        local_tenant_timeline_files.len(),
    );
    Ok(local_tenant_timeline_files)
}

fn remove_if_empty(tenant_dir_path: &Path) -> anyhow::Result<bool> {
    let directory_is_empty = tenant_dir_path
        .read_dir()
        .with_context(|| {
            format!(
                "Failed to read directory '{}' contents",
                tenant_dir_path.display()
            )
        })?
        .next()
        .is_none();

    if directory_is_empty {
        fs::remove_dir_all(&tenant_dir_path).with_context(|| {
            format!(
                "Failed to remove empty directory '{}'",
                tenant_dir_path.display(),
            )
        })?;

        Ok(true)
    } else {
        Ok(false)
    }
}

fn is_temporary(path: &Path) -> bool {
    match path.file_name() {
        Some(name) => name.to_string_lossy().ends_with(TEMP_FILE_SUFFIX),
        None => false,
    }
}

fn collect_timelines_for_tenant(
    config: &'static PageServerConf,
    tenant_path: &Path,
) -> anyhow::Result<(TenantId, TenantAttachData)> {
    let tenant_id = tenant_path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .parse::<TenantId>()
        .context("Could not parse tenant id out of the tenant dir name")?;
    let timelines_dir = config.timelines_path(&tenant_id);

    if !timelines_dir.as_path().is_dir() {
        return Ok((
            tenant_id,
            TenantAttachData::Broken(anyhow::anyhow!(
                "Tenant {} has no timelines directory at {}",
                tenant_id,
                timelines_dir.display()
            )),
        ));
    }

    let mut tenant_timelines = HashMap::new();
    for timelines_dir_entry in fs::read_dir(&timelines_dir)
        .with_context(|| format!("Failed to list timelines dir entry for tenant {tenant_id}"))?
    {
        match timelines_dir_entry {
            Ok(timelines_dir_entry) => {
                let timeline_dir = timelines_dir_entry.path();
                if is_temporary(&timeline_dir) {
                    info!(
                        "Found temporary timeline directory, removing: {}",
                        timeline_dir.display()
                    );
                    if let Err(e) = fs::remove_dir_all(&timeline_dir) {
                        error!(
                            "Failed to remove temporary directory '{}': {:?}",
                            timeline_dir.display(),
                            e
                        );
                    }
                } else {
                    match collect_timeline_files(&timeline_dir) {
                        Ok((timeline_id, metadata, timeline_files)) => {
                            tenant_timelines.insert(
                                timeline_id,
                                TimelineLocalFiles::collected(metadata, timeline_files),
                            );
                        }
                        Err(e) => {
                            error!(
                                "Failed to process timeline dir contents at '{}', reason: {:?}",
                                timeline_dir.display(),
                                e
                            );
                            match remove_if_empty(&timeline_dir) {
                                Ok(true) => info!(
                                    "Removed empty timeline directory {}",
                                    timeline_dir.display()
                                ),
                                Ok(false) => (),
                                Err(e) => {
                                    error!("Failed to remove empty timeline directory: {e:?}")
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to list timelines for entry tenant {tenant_id}, reason: {e:?}")
            }
        }
    }

    if tenant_timelines.is_empty() {
        // this is normal, we've removed all broken, empty and temporary timeline dirs
        // but should allow the tenant to stay functional and allow creating new timelines
        // on a restart, we require tenants to have the timelines dir, so leave it on disk
        debug!("Tenant {tenant_id} has no timelines loaded");
    }

    Ok((tenant_id, TenantAttachData::Ready(tenant_timelines)))
}

// discover timeline files and extract timeline metadata
//  NOTE: ephemeral files are excluded from the list
fn collect_timeline_files(
    timeline_dir: &Path,
) -> anyhow::Result<(
    TimelineId,
    TimelineMetadata,
    HashMap<PathBuf, LayerFileMetadata>,
)> {
    let mut timeline_files = HashMap::new();
    let mut timeline_metadata_path = None;

    let timeline_id = timeline_dir
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .parse::<TimelineId>()
        .context("Could not parse timeline id out of the timeline dir name")?;
    let timeline_dir_entries =
        fs::read_dir(&timeline_dir).context("Failed to list timeline dir contents")?;
    for entry in timeline_dir_entries {
        let entry_path = entry.context("Failed to list timeline dir entry")?.path();
        let metadata = entry_path.metadata()?;

        if metadata.is_file() {
            if entry_path.file_name().and_then(OsStr::to_str) == Some(METADATA_FILE_NAME) {
                timeline_metadata_path = Some(entry_path);
            } else if is_ephemeral_file(&entry_path.file_name().unwrap().to_string_lossy()) {
                debug!("skipping ephemeral file {}", entry_path.display());
                continue;
            } else if is_temporary(&entry_path) {
                info!("removing temp timeline file at {}", entry_path.display());
                fs::remove_file(&entry_path).with_context(|| {
                    format!(
                        "failed to remove temp download file at {}",
                        entry_path.display()
                    )
                })?;
            } else {
                let layer_metadata = LayerFileMetadata::new(metadata.len());
                timeline_files.insert(entry_path, layer_metadata);
            }
        }
    }

    // FIXME (rodionov) if attach call succeeded, and then pageserver is restarted before download is completed
    //   then attach is lost. There would be no retries for that,
    //   initial collect will fail because there is no metadata.
    //   We either need to start download if we see empty dir after restart or attach caller should
    //   be aware of that and retry attach if awaits_download for timeline switched from true to false
    //   but timelinne didn't appear locally.
    //   Check what happens with remote index in that case.
    let timeline_metadata_path = match timeline_metadata_path {
        Some(path) => path,
        None => anyhow::bail!("No metadata file found in the timeline directory"),
    };
    let metadata = TimelineMetadata::from_bytes(
        &fs::read(&timeline_metadata_path).context("Failed to read timeline metadata file")?,
    )
    .context("Failed to parse timeline metadata file bytes")?;

    anyhow::ensure!(
        metadata.ancestor_timeline().is_some() || !timeline_files.is_empty(),
        "Timeline has no ancestor and no layer files"
    );

    Ok((timeline_id, metadata, timeline_files))
}
