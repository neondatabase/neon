//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use crate::config::PageServerConf;
use crate::http::models::TenantInfo;
use crate::layered_repository::ephemeral_file::is_ephemeral_file;
use crate::layered_repository::metadata::{TimelineMetadata, METADATA_FILE_NAME};
use crate::layered_repository::{Repository, Timeline};
use crate::storage_sync::index::{RemoteIndex, RemoteTimelineIndex};
use crate::storage_sync::{self, LocalTimelineInitStatus, SyncStartupData};
use crate::tenant_config::TenantConfOpt;
use crate::thread_mgr::ThreadKind;
use crate::walredo::PostgresRedoManager;
use crate::{thread_mgr, timelines, walreceiver, TenantTimelineValues, TEMP_FILE_SUFFIX};
use anyhow::Context;
use remote_storage::GenericRemoteStorage;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::{self, Entry};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::*;

pub use tenants_state::try_send_timeline_update;
use utils::zid::{ZTenantId, ZTenantTimelineId, ZTimelineId};

mod tenants_state {
    use anyhow::ensure;
    use once_cell::sync::Lazy;
    use std::{
        collections::HashMap,
        sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    };
    use tokio::sync::mpsc;
    use tracing::{debug, error};
    use utils::zid::ZTenantId;

    use crate::tenant_mgr::{LocalTimelineUpdate, Tenant};

    static TENANTS: Lazy<RwLock<HashMap<ZTenantId, Tenant>>> =
        Lazy::new(|| RwLock::new(HashMap::new()));

    /// Sends updates to the local timelines (creation and deletion) to the WAL receiver,
    /// so that it can enable/disable corresponding processes.
    static TIMELINE_UPDATE_SENDER: Lazy<
        RwLock<Option<mpsc::UnboundedSender<LocalTimelineUpdate>>>,
    > = Lazy::new(|| RwLock::new(None));

    pub(super) fn read_tenants() -> RwLockReadGuard<'static, HashMap<ZTenantId, Tenant>> {
        TENANTS
            .read()
            .expect("Failed to read() tenants lock, it got poisoned")
    }

    pub(super) fn write_tenants() -> RwLockWriteGuard<'static, HashMap<ZTenantId, Tenant>> {
        TENANTS
            .write()
            .expect("Failed to write() tenants lock, it got poisoned")
    }

    pub(super) fn set_timeline_update_sender(
        timeline_updates_sender: mpsc::UnboundedSender<LocalTimelineUpdate>,
    ) -> anyhow::Result<()> {
        let mut sender_guard = TIMELINE_UPDATE_SENDER
            .write()
            .expect("Failed to write() timeline_update_sender lock, it got poisoned");
        ensure!(sender_guard.is_none(), "Timeline update sender already set");
        *sender_guard = Some(timeline_updates_sender);
        Ok(())
    }

    pub fn try_send_timeline_update(update: LocalTimelineUpdate) {
        match TIMELINE_UPDATE_SENDER
            .read()
            .expect("Failed to read() timeline_update_sender lock, it got poisoned")
            .as_ref()
        {
            Some(sender) => {
                if let Err(e) = sender.send(update) {
                    error!("Failed to send timeline update: {}", e);
                }
            }
            None => debug!("Timeline update sender is not enabled, cannot send update {update:?}"),
        }
    }

    pub(super) fn stop_timeline_update_sender() {
        TIMELINE_UPDATE_SENDER
            .write()
            .expect("Failed to write() timeline_update_sender lock, it got poisoned")
            .take();
    }
}

struct Tenant {
    state: TenantState,
    /// Contains in-memory state, including the timeline that might not yet flushed on disk or loaded form disk.
    repo: Arc<Repository>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum TenantState {
    // All data for this tenant is complete on local disk, but we haven't loaded the Repository,
    // Timeline and Layer structs into memory yet, so it cannot be accessed yet.
    //Ready,
    // This tenant exists on local disk, and the layer map has been loaded into memory.
    // The local disk might have some newer files that don't exist in cloud storage yet.
    Active,
    // Tenant is active, but there is no walreceiver connection.
    Idle,
    // This tenant exists on local disk, and the layer map has been loaded into memory.
    // The local disk might have some newer files that don't exist in cloud storage yet.
    // The tenant cannot be accessed anymore for any reason, but graceful shutdown.
    Stopping,

    // Something went wrong loading the tenant state
    Broken,
}

impl fmt::Display for TenantState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => f.write_str("Active"),
            Self::Idle => f.write_str("Idle"),
            Self::Stopping => f.write_str("Stopping"),
            Self::Broken => f.write_str("Broken"),
        }
    }
}

/// Initialize repositories with locally available timelines.
/// Timelines that are only partially available locally (remote storage has more data than this pageserver)
/// are scheduled for download and added to the repository once download is completed.
pub fn init_tenant_mgr(
    conf: &'static PageServerConf,
    remote_storage: Option<GenericRemoteStorage>,
) -> anyhow::Result<RemoteIndex> {
    let _entered = info_span!("init_tenant_mgr").entered();
    let (timeline_updates_sender, timeline_updates_receiver) =
        mpsc::unbounded_channel::<LocalTimelineUpdate>();
    tenants_state::set_timeline_update_sender(timeline_updates_sender)?;
    walreceiver::init_wal_receiver_main_thread(conf, timeline_updates_receiver)?;

    let local_tenant_files = local_tenant_timeline_files(conf)
        .context("Failed to collect local tenant timeline files")?;

    let (remote_index, tenants_to_attach) = if let Some(storage) = remote_storage {
        let storage_config = conf
            .remote_storage_config
            .as_ref()
            .expect("remote storage without config");

        let SyncStartupData {
            remote_index,
            local_timeline_init_statuses,
        } = storage_sync::spawn_storage_sync_thread(
            conf,
            local_tenant_files,
            storage,
            storage_config.max_concurrent_syncs,
            storage_config.max_sync_errors,
        )
        .context("Failed to spawn the storage sync thread")?;

        (
            remote_index,
            local_timeline_init_statuses.filter_map(|init_status| match init_status {
                LocalTimelineInitStatus::LocallyComplete(metadata) => Some(metadata),
                LocalTimelineInitStatus::NeedsSync => None,
            }),
        )
    } else {
        info!("No remote storage configured, skipping storage sync, considering all local timelines with correct metadata files enabled");
        (
            RemoteIndex::default(),
            local_tenant_files.filter_map(|(metadata, _)| Some(metadata)),
        )
    };

    attach_local_tenants(conf, &remote_index, tenants_to_attach)?;

    Ok(remote_index)
}

pub enum LocalTimelineUpdate {
    Detach {
        id: ZTenantTimelineId,
        // used to signal to the detach caller that walreceiver successfully terminated for specified id
        join_confirmation_sender: std::sync::mpsc::Sender<()>,
    },
    Attach {
        id: ZTenantTimelineId,
        timeline: Arc<Timeline>,
    },
}

impl std::fmt::Debug for LocalTimelineUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Detach { id, .. } => f.debug_tuple("Detach").field(id).finish(),
            Self::Attach { id, .. } => f.debug_tuple("Attach").field(id).finish(),
        }
    }
}

/// Reads local files to load tenants and their timelines given into pageserver's memory.
/// Ignores other timelines that might be present for tenant, but were not passed as a parameter.
/// Attempts to load as many entites as possible: if a certain timeline fails during the load, the tenant is marked as "Broken",
/// and the load continues.
pub fn attach_local_tenants(
    conf: &'static PageServerConf,
    remote_index: &RemoteIndex,
    tenants_to_attach: TenantTimelineValues<TimelineMetadata>,
) -> anyhow::Result<()> {
    let _entered = info_span!("attach_local_tenants").entered();
    let number_of_tenants = tenants_to_attach.0.len();

    for (tenant_id, local_timelines) in tenants_to_attach.0 {
        info!(
            "Attaching {} timelines for {tenant_id}",
            local_timelines.len()
        );
        debug!("Timelines to attach: {local_timelines:?}");

        let repository = load_local_repo(conf, tenant_id, remote_index)
            .context("Failed to load repository for tenant")?;

        let repo = Arc::clone(&repository);
        {
            match tenants_state::write_tenants().entry(tenant_id) {
                hash_map::Entry::Occupied(_) => {
                    anyhow::bail!("Cannot attach tenant {tenant_id}: there's already an entry in the tenant state");
                }
                hash_map::Entry::Vacant(v) => {
                    v.insert(Tenant {
                        state: TenantState::Idle,
                        repo,
                    });
                }
            }
        }
        // XXX: current timeline init enables walreceiver that looks for tenant in the state, so insert the tenant entry before
        repository
            .init_attach_timelines(local_timelines)
            .context("Failed to attach timelines for tenant")?;
    }

    info!("Processed {number_of_tenants} local tenants during attach");
    Ok(())
}

fn load_local_repo(
    conf: &'static PageServerConf,
    tenant_id: ZTenantId,
    remote_index: &RemoteIndex,
) -> anyhow::Result<Arc<Repository>> {
    let repository = Repository::new(
        conf,
        TenantConfOpt::default(),
        Arc::new(PostgresRedoManager::new(conf, tenant_id)),
        tenant_id,
        remote_index.clone(),
        conf.remote_storage_config.is_some(),
    );
    let tenant_conf = Repository::load_tenant_config(conf, tenant_id)?;
    repository.update_tenant_config(tenant_conf);

    Ok(Arc::new(repository))
}

///
/// Shut down all tenants. This runs as part of pageserver shutdown.
///
pub fn shutdown_all_tenants() {
    tenants_state::stop_timeline_update_sender();
    let mut m = tenants_state::write_tenants();
    let mut tenantids = Vec::new();
    for (tenantid, tenant) in m.iter_mut() {
        match tenant.state {
            TenantState::Active | TenantState::Idle | TenantState::Stopping => {
                tenant.state = TenantState::Stopping;
                tenantids.push(*tenantid)
            }
            TenantState::Broken => {}
        }
    }
    drop(m);

    thread_mgr::shutdown_threads(Some(ThreadKind::WalReceiverManager), None, None);

    // Ok, no background threads running anymore. Flush any remaining data in
    // memory to disk.
    //
    // We assume that any incoming connections that might request pages from
    // the repository have already been terminated by the caller, so there
    // should be no more activity in any of the repositories.
    //
    // On error, log it but continue with the shutdown for other tenants.
    for tenant_id in tenantids {
        debug!("shutdown tenant {tenant_id}");
        match get_repository_for_tenant(tenant_id) {
            Ok(repo) => {
                if let Err(err) = repo.checkpoint() {
                    error!("Could not checkpoint tenant {tenant_id} during shutdown: {err:?}");
                }
            }
            Err(err) => {
                error!("Could not get repository for tenant {tenant_id} during shutdown: {err:?}");
            }
        }
    }
}

pub fn create_tenant_repository(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: ZTenantId,
    remote_index: RemoteIndex,
) -> anyhow::Result<Option<ZTenantId>> {
    match tenants_state::write_tenants().entry(tenant_id) {
        Entry::Occupied(_) => {
            debug!("tenant {tenant_id} already exists");
            Ok(None)
        }
        Entry::Vacant(v) => {
            let wal_redo_manager = Arc::new(PostgresRedoManager::new(conf, tenant_id));
            let repo = timelines::create_repo(
                conf,
                tenant_conf,
                tenant_id,
                wal_redo_manager,
                remote_index,
            )?;
            v.insert(Tenant {
                state: TenantState::Idle,
                repo,
            });
            Ok(Some(tenant_id))
        }
    }
}

pub fn update_tenant_config(
    conf: &'static PageServerConf,
    tenant_conf: TenantConfOpt,
    tenant_id: ZTenantId,
) -> anyhow::Result<()> {
    info!("configuring tenant {tenant_id}");
    get_repository_for_tenant(tenant_id)?.update_tenant_config(tenant_conf);

    Repository::persist_tenant_config(conf, tenant_id, tenant_conf)?;
    Ok(())
}

pub fn get_tenant_state(tenantid: ZTenantId) -> Option<TenantState> {
    Some(tenants_state::read_tenants().get(&tenantid)?.state)
}

pub fn set_tenant_state(tenant_id: ZTenantId, new_state: TenantState) -> anyhow::Result<()> {
    let mut m = tenants_state::write_tenants();
    let tenant = m
        .get_mut(&tenant_id)
        .with_context(|| format!("Tenant not found for id {tenant_id}"))?;
    let old_state = tenant.state;
    tenant.state = new_state;
    drop(m);

    match (old_state, new_state) {
        (TenantState::Broken, TenantState::Broken)
        | (TenantState::Active, TenantState::Active)
        | (TenantState::Idle, TenantState::Idle)
        | (TenantState::Stopping, TenantState::Stopping) => {
            debug!("tenant {tenant_id} already in state {new_state}");
        }
        (TenantState::Broken, ignored) => {
            debug!("Ignoring {ignored} since tenant {tenant_id} is in broken state");
        }
        (_, TenantState::Broken) => {
            debug!("Setting tenant {tenant_id} status to broken");
        }
        (TenantState::Stopping, ignored) => {
            debug!("Ignoring {ignored} since tenant {tenant_id} is in stopping state");
        }
        (TenantState::Idle, TenantState::Active) => {
            info!("activating tenant {tenant_id}");

            // Spawn gc and compaction loops. The loops will shut themselves
            // down when they notice that the tenant is inactive.
            // TODO maybe use tokio::sync::watch instead?
            crate::tenant_tasks::start_compaction_loop(tenant_id)?;
            crate::tenant_tasks::start_gc_loop(tenant_id)?;
        }
        (TenantState::Idle, TenantState::Stopping) => {
            info!("stopping idle tenant {tenant_id}");
        }
        (TenantState::Active, TenantState::Stopping | TenantState::Idle) => {
            info!("stopping tenant {tenant_id} threads due to new state {new_state}");
            thread_mgr::shutdown_threads(
                Some(ThreadKind::WalReceiverManager),
                Some(tenant_id),
                None,
            );

            // Wait until all gc/compaction tasks finish
            let repo = get_repository_for_tenant(tenant_id)?;
            let _guard = repo.file_lock.write().unwrap();
        }
    }

    Ok(())
}

pub fn get_repository_for_tenant(tenant_id: ZTenantId) -> anyhow::Result<Arc<Repository>> {
    let m = tenants_state::read_tenants();
    let tenant = m
        .get(&tenant_id)
        .with_context(|| format!("Tenant {tenant_id} not found"))?;

    Ok(Arc::clone(&tenant.repo))
}

pub fn delete_timeline(tenant_id: ZTenantId, timeline_id: ZTimelineId) -> anyhow::Result<()> {
    // Start with the shutdown of timeline tasks (this shuts down the walreceiver)
    // It is important that we do not take locks here, and do not check whether the timeline exists
    // because if we hold tenants_state::write_tenants() while awaiting for the threads to join
    // we cannot create new timelines and tenants, and that can take quite some time,
    // it can even become stuck due to a bug making whole pageserver unavailable for some operations
    // so this is the way how we deal with concurrent delete requests: shutdown everythig, wait for confirmation
    // and then try to actually remove timeline from inmemory state and this is the point when concurrent requests
    // will synchronize and either fail with the not found error or succeed

    let (sender, receiver) = std::sync::mpsc::channel::<()>();
    tenants_state::try_send_timeline_update(LocalTimelineUpdate::Detach {
        id: ZTenantTimelineId::new(tenant_id, timeline_id),
        join_confirmation_sender: sender,
    });

    debug!("waiting for wal receiver to shutdown");
    let _ = receiver.recv();
    debug!("wal receiver shutdown confirmed");
    debug!("waiting for threads to shutdown");
    thread_mgr::shutdown_threads(None, None, Some(timeline_id));
    debug!("thread shutdown completed");
    match tenants_state::read_tenants().get(&tenant_id) {
        Some(tenant) => tenant.repo.delete_timeline(timeline_id)?,
        None => anyhow::bail!("Tenant {tenant_id} not found in local tenant state"),
    }

    Ok(())
}

pub fn detach_tenant(conf: &'static PageServerConf, tenant_id: ZTenantId) -> anyhow::Result<()> {
    set_tenant_state(tenant_id, TenantState::Stopping)?;
    // shutdown the tenant and timeline threads: gc, compaction, page service threads)
    thread_mgr::shutdown_threads(None, Some(tenant_id), None);

    let mut walreceiver_join_handles = Vec::new();
    let removed_tenant = {
        let mut tenants_accessor = tenants_state::write_tenants();
        tenants_accessor.remove(&tenant_id)
    };
    if let Some(tenant) = removed_tenant {
        for (timeline_id, _) in tenant.repo.list_timelines() {
            let (sender, receiver) = std::sync::mpsc::channel::<()>();
            tenants_state::try_send_timeline_update(LocalTimelineUpdate::Detach {
                id: ZTenantTimelineId::new(tenant_id, timeline_id),
                join_confirmation_sender: sender,
            });
            walreceiver_join_handles.push((timeline_id, receiver));
        }
    }

    // wait for wal receivers to stop without holding the lock, because walreceiver
    // will attempt to change tenant state which is protected by the same global tenants lock.
    // TODO do we need a timeout here? how to handle it?
    // recv_timeout is broken: https://github.com/rust-lang/rust/issues/94518#issuecomment-1057440631
    // need to use crossbeam-channel
    for (timeline_id, join_handle) in walreceiver_join_handles {
        info!("waiting for wal receiver to shutdown timeline_id {timeline_id}");
        join_handle.recv().ok();
        info!("wal receiver shutdown confirmed timeline_id {timeline_id}");
    }

    // If removal fails there will be no way to successfully retry detach,
    // because the tenant no longer exists in the in-memory map. And it needs to be removed from it
    // before we remove files, because it contains references to repository
    // which references ephemeral files which are deleted on drop. So if we keep these references,
    // we will attempt to remove files which no longer exist. This can be fixed by having shutdown
    // mechanism for repository that will clean temporary data to avoid any references to ephemeral files
    let local_tenant_directory = conf.tenant_path(&tenant_id);
    std::fs::remove_dir_all(&local_tenant_directory).with_context(|| {
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
                state: Some(tenant.state),
                current_physical_size: None,
                has_in_progress_downloads,
            }
        })
        .collect()
}

/// Attempts to collect information about all tenant and timelines, existing on the local FS.
/// If finds any, deletes all temporary files and directories, created before. Also removes empty directories,
/// that may appear due to such removals.
/// Does not fail on particular timeline or tenant collection errors, rather logging them and ignoring the entities.
fn local_tenant_timeline_files(
    config: &'static PageServerConf,
) -> anyhow::Result<TenantTimelineValues<(TimelineMetadata, HashSet<PathBuf>)>> {
    let _entered = info_span!("local_tenant_timeline_files").entered();

    let mut local_tenant_timeline_files = TenantTimelineValues::new();
    let tenants_dir = config.tenants_path();
    for tenants_dir_entry in std::fs::read_dir(&tenants_dir)
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
                    if let Err(e) = std::fs::remove_dir_all(&tenant_dir_path) {
                        error!(
                            "Failed to remove temporary directory '{}': {:?}",
                            tenant_dir_path.display(),
                            e
                        );
                    }
                } else {
                    match collect_timelines_for_tenant(config, &tenant_dir_path) {
                        Ok((tenant_id, collected_files)) => {
                            if collected_files.is_empty() {
                                match remove_if_empty(&tenant_dir_path) {
                                    Ok(true) => info!("Removed empty tenant directory {}", tenant_dir_path.display()),
                                    Ok(false) => {
                                        // insert empty timeline entry: it has some non-temporary files inside that we cannot remove
                                        // so make obvious for HTTP API callers, that something exists there and try to load the tenant
                                        let _ = local_tenant_timeline_files.0.entry(tenant_id).or_default();
                                    },
                                    Err(e) => error!("Failed to remove empty tenant directory: {e:?}"),
                                }
                            } else {
                                local_tenant_timeline_files.0.entry(tenant_id).or_default().extend(collected_files.into_iter())
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
        local_tenant_timeline_files.0.len()
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
        std::fs::remove_dir_all(&tenant_dir_path).with_context(|| {
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

#[allow(clippy::type_complexity)]
fn collect_timelines_for_tenant(
    config: &'static PageServerConf,
    tenant_path: &Path,
) -> anyhow::Result<(
    ZTenantId,
    HashMap<ZTimelineId, (TimelineMetadata, HashSet<PathBuf>)>,
)> {
    let tenant_id = tenant_path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .parse::<ZTenantId>()
        .context("Could not parse tenant id out of the tenant dir name")?;
    let timelines_dir = config.timelines_path(&tenant_id);

    let mut tenant_timelines = HashMap::new();
    for timelines_dir_entry in std::fs::read_dir(&timelines_dir)
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
                    if let Err(e) = std::fs::remove_dir_all(&timeline_dir) {
                        error!(
                            "Failed to remove temporary directory '{}': {:?}",
                            timeline_dir.display(),
                            e
                        );
                    }
                } else {
                    match collect_timeline_files(&timeline_dir) {
                        Ok((timeline_id, metadata, timeline_files)) => {
                            tenant_timelines.insert(timeline_id, (metadata, timeline_files));
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
        match remove_if_empty(&timelines_dir) {
            Ok(true) => info!(
                "Removed empty tenant timelines directory {}",
                timelines_dir.display()
            ),
            Ok(false) => (),
            Err(e) => error!("Failed to remove empty tenant timelines directory: {e:?}"),
        }
    }

    Ok((tenant_id, tenant_timelines))
}

// discover timeline files and extract timeline metadata
//  NOTE: ephemeral files are excluded from the list
fn collect_timeline_files(
    timeline_dir: &Path,
) -> anyhow::Result<(ZTimelineId, TimelineMetadata, HashSet<PathBuf>)> {
    let mut timeline_files = HashSet::new();
    let mut timeline_metadata_path = None;

    let timeline_id = timeline_dir
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .parse::<ZTimelineId>()
        .context("Could not parse timeline id out of the timeline dir name")?;
    let timeline_dir_entries =
        std::fs::read_dir(&timeline_dir).context("Failed to list timeline dir contents")?;
    for entry in timeline_dir_entries {
        let entry_path = entry.context("Failed to list timeline dir entry")?.path();
        if entry_path.is_file() {
            if entry_path.file_name().and_then(OsStr::to_str) == Some(METADATA_FILE_NAME) {
                timeline_metadata_path = Some(entry_path);
            } else if is_ephemeral_file(&entry_path.file_name().unwrap().to_string_lossy()) {
                debug!("skipping ephemeral file {}", entry_path.display());
                continue;
            } else if is_temporary(&entry_path) {
                info!("removing temp timeline file at {}", entry_path.display());
                std::fs::remove_file(&entry_path).with_context(|| {
                    format!(
                        "failed to remove temp download file at {}",
                        entry_path.display()
                    )
                })?;
            } else {
                timeline_files.insert(entry_path);
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
        &std::fs::read(&timeline_metadata_path).context("Failed to read timeline metadata file")?,
    )
    .context("Failed to parse timeline metadata file bytes")?;

    anyhow::ensure!(
        metadata.ancestor_timeline().is_some() || !timeline_files.is_empty(),
        "Timeline has no ancestor and no layer files"
    );

    Ok((timeline_id, metadata, timeline_files))
}
