//! This module acts as a switchboard to access different repositories managed by this
//! page server.

use crate::config::PageServerConf;
use crate::layered_repository::{load_metadata, LayeredRepository};
use crate::pgdatadir_mapping::DatadirTimeline;
use crate::repository::Repository;
use crate::storage_sync::index::{RemoteIndex, RemoteTimelineIndex};
use crate::storage_sync::{self, LocalTimelineInitStatus, SyncStartupData};
use crate::tenant_config::TenantConfOpt;
use crate::thread_mgr::ThreadKind;
use crate::timelines::CreateRepo;
use crate::walredo::PostgresRedoManager;
use crate::{thread_mgr, timelines, walreceiver};
use crate::{DatadirTimelineImpl, RepositoryImpl};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::*;
use utils::lsn::Lsn;

use utils::zid::{ZTenantId, ZTenantTimelineId, ZTimelineId};

mod tenants_state {
    use anyhow::ensure;
    use std::{
        collections::HashMap,
        sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
    };
    use tokio::sync::mpsc;
    use tracing::{debug, error};

    use utils::zid::ZTenantId;

    use crate::tenant_mgr::{LocalTimelineUpdate, Tenant};

    lazy_static::lazy_static! {
        static ref TENANTS: RwLock<HashMap<ZTenantId, Tenant>> = RwLock::new(HashMap::new());
        /// Sends updates to the local timelines (creation and deletion) to the WAL receiver,
        /// so that it can enable/disable corresponding processes.
        static ref TIMELINE_UPDATE_SENDER: RwLock<Option<mpsc::UnboundedSender<LocalTimelineUpdate>>> = RwLock::new(None);
    }

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

    pub(super) fn try_send_timeline_update(update: LocalTimelineUpdate) {
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
    repo: Arc<RepositoryImpl>,
    /// Timelines, located locally in the pageserver's datadir.
    /// Timelines can entirely be removed entirely by the `detach` operation only.
    ///
    /// Local timelines have more metadata that's loaded into memory,
    /// that is located in the `repo.timelines` field, [`crate::layered_repository::LayeredTimelineEntry`].
    local_timelines: HashMap<ZTimelineId, Arc<DatadirTimelineImpl>>,
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
pub fn init_tenant_mgr(conf: &'static PageServerConf) -> anyhow::Result<RemoteIndex> {
    let (timeline_updates_sender, timeline_updates_receiver) =
        mpsc::unbounded_channel::<LocalTimelineUpdate>();
    tenants_state::set_timeline_update_sender(timeline_updates_sender)?;
    walreceiver::init_wal_receiver_main_thread(conf, timeline_updates_receiver)?;

    let SyncStartupData {
        remote_index,
        local_timeline_init_statuses,
    } = storage_sync::start_local_timeline_sync(conf)
        .context("Failed to set up local files sync with external storage")?;

    for (tenant_id, local_timeline_init_statuses) in local_timeline_init_statuses {
        if let Err(err) =
            init_local_repository(conf, tenant_id, local_timeline_init_statuses, &remote_index)
        {
            // Report the error, but continue with the startup for other tenants. An error
            // loading a tenant is serious, but it's better to complete the startup and
            // serve other tenants, than fail completely.
            error!("Failed to initialize local tenant {tenant_id}: {:?}", err);

            if let Err(err) = set_tenant_state(tenant_id, TenantState::Broken) {
                error!(
                    "Failed to set tenant state to broken {tenant_id}: {:?}",
                    err
                );
            }
        }
    }

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
        datadir: Arc<DatadirTimelineImpl>,
    },
}

impl std::fmt::Debug for LocalTimelineUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Detach { id, .. } => f.debug_tuple("Remove").field(id).finish(),
            Self::Attach { id, .. } => f.debug_tuple("Add").field(id).finish(),
        }
    }
}

/// Updates tenants' repositories, changing their timelines state in memory.
pub fn attach_downloaded_tenants(
    conf: &'static PageServerConf,
    remote_index: &RemoteIndex,
    sync_status_updates: HashMap<ZTenantId, HashSet<ZTimelineId>>,
) {
    if sync_status_updates.is_empty() {
        debug!("No sync status updates to apply");
        return;
    }
    for (tenant_id, downloaded_timelines) in sync_status_updates {
        info!(
            "Registering downlloaded timelines for {tenant_id} {} timelines",
            downloaded_timelines.len()
        );
        debug!("Downloaded timelines: {downloaded_timelines:?}");

        let repo = match load_local_repo(conf, tenant_id, remote_index) {
            Ok(repo) => repo,
            Err(e) => {
                error!("Failed to load repo for tenant {tenant_id} Error: {e:?}");
                continue;
            }
        };
        match attach_downloaded_tenant(&repo, downloaded_timelines) {
            Ok(()) => info!("successfully applied sync status updates for tenant {tenant_id}"),
            Err(e) => error!(
                "Failed to apply timeline sync timeline status updates for tenant {tenant_id}: {e:?}"
            ),
        }
    }
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
                CreateRepo::Real {
                    wal_redo_manager,
                    remote_index,
                },
            )?;
            v.insert(Tenant {
                state: TenantState::Idle,
                repo,
                local_timelines: HashMap::new(),
            });
            Ok(Some(tenant_id))
        }
    }
}

pub fn update_tenant_config(
    tenant_conf: TenantConfOpt,
    tenant_id: ZTenantId,
) -> anyhow::Result<()> {
    info!("configuring tenant {tenant_id}");
    let repo = get_repository_for_tenant(tenant_id)?;

    repo.update_tenant_config(tenant_conf)?;
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

pub fn get_repository_for_tenant(tenant_id: ZTenantId) -> anyhow::Result<Arc<RepositoryImpl>> {
    let m = tenants_state::read_tenants();
    let tenant = m
        .get(&tenant_id)
        .with_context(|| format!("Tenant {tenant_id} not found"))?;

    Ok(Arc::clone(&tenant.repo))
}

/// Retrieves local timeline for tenant.
/// Loads it into memory if it is not already loaded.
pub fn get_local_timeline_with_load(
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
) -> anyhow::Result<Arc<DatadirTimelineImpl>> {
    let mut m = tenants_state::write_tenants();
    let tenant = m
        .get_mut(&tenant_id)
        .with_context(|| format!("Tenant {tenant_id} not found"))?;

    if let Some(page_tline) = tenant.local_timelines.get(&timeline_id) {
        Ok(Arc::clone(page_tline))
    } else {
        let page_tline = load_local_timeline(&tenant.repo, timeline_id)
            .with_context(|| format!("Failed to load local timeline for tenant {tenant_id}"))?;
        tenant
            .local_timelines
            .insert(timeline_id, Arc::clone(&page_tline));
        Ok(page_tline)
    }
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
    match tenants_state::write_tenants().get_mut(&tenant_id) {
        Some(tenant) => {
            tenant.repo.delete_timeline(timeline_id)?;
            tenant.local_timelines.remove(&timeline_id);
        }
        None => anyhow::bail!("Tenant {tenant_id} not found in local tenant state"),
    }

    Ok(())
}

pub fn detach_tenant(conf: &'static PageServerConf, tenant_id: ZTenantId) -> anyhow::Result<()> {
    set_tenant_state(tenant_id, TenantState::Stopping)?;
    // shutdown the tenant and timeline threads: gc, compaction, page service threads)
    thread_mgr::shutdown_threads(None, Some(tenant_id), None);

    // FIXME should we protect somehow from starting new threads/walreceivers when tenant is in stopping state?
    // send stop signal to wal receiver and collect join handles while holding the lock
    let walreceiver_join_handles = {
        let tenants = tenants_state::write_tenants();
        let tenant = tenants.get(&tenant_id).context("tenant not found")?;
        let mut walreceiver_join_handles = Vec::with_capacity(tenant.local_timelines.len());
        for timeline_id in tenant.local_timelines.keys() {
            let (sender, receiver) = std::sync::mpsc::channel::<()>();
            tenants_state::try_send_timeline_update(LocalTimelineUpdate::Detach {
                id: ZTenantTimelineId::new(tenant_id, *timeline_id),
                join_confirmation_sender: sender,
            });
            walreceiver_join_handles.push((*timeline_id, receiver));
        }
        // drop the tenants lock
        walreceiver_join_handles
    };

    // wait for wal receivers to stop without holding the lock, because walreceiver
    // will attempt to change tenant state which is protected by the same global tenants lock.
    // TODO do we need a timeout here? how to handle it?
    // recv_timeout is broken: https://github.com/rust-lang/rust/issues/94518#issuecomment-1057440631
    // need to use crossbeam-channel
    for (timeline_id, join_handle) in walreceiver_join_handles {
        info!("waiting for wal receiver to shutdown timeline_id {timeline_id}");
        join_handle.recv().context("failed to join walreceiver")?;
        info!("wal receiver shutdown confirmed timeline_id {timeline_id}");
    }

    tenants_state::write_tenants().remove(&tenant_id);

    // If removal fails there will be no way to successfully retry detach,
    // because tenant no longer exists in in memory map. And it needs to be removed from it
    // before we remove files because it contains references to repository
    // which references ephemeral files which are deleted on drop. So if we keep these references
    // code will attempt to remove files which no longer exist. This can be fixed by having shutdown
    // mechanism for repository that will clean temporary data to avoid any references to ephemeral files
    let local_tenant_directory = conf.tenant_path(&tenant_id);
    std::fs::remove_dir_all(&local_tenant_directory).with_context(|| {
        format!(
            "Failed to remove local timeline directory '{}'",
            local_tenant_directory.display()
        )
    })?;

    Ok(())
}

fn load_local_timeline(
    repo: &RepositoryImpl,
    timeline_id: ZTimelineId,
) -> anyhow::Result<Arc<DatadirTimeline<LayeredRepository>>> {
    let inmem_timeline = repo.get_timeline_load(timeline_id).with_context(|| {
        format!("Inmem timeline {timeline_id} not found in tenant's repository")
    })?;
    let repartition_distance = repo.get_checkpoint_distance() / 10;
    let page_tline = Arc::new(DatadirTimelineImpl::new(
        inmem_timeline,
        repartition_distance,
    ));
    page_tline.init_logical_size()?;

    tenants_state::try_send_timeline_update(LocalTimelineUpdate::Attach {
        id: ZTenantTimelineId::new(repo.tenant_id(), timeline_id),
        datadir: Arc::clone(&page_tline),
    });

    Ok(page_tline)
}

#[serde_as]
#[derive(Serialize, Deserialize, Clone)]
pub struct TenantInfo {
    #[serde_as(as = "DisplayFromStr")]
    pub id: ZTenantId,
    pub state: Option<TenantState>,
    pub has_in_progress_downloads: Option<bool>,
}

pub fn list_tenants(remote_index: &RemoteTimelineIndex) -> Vec<TenantInfo> {
    tenants_state::read_tenants()
        .iter()
        .map(|(id, tenant)| {
            let has_in_progress_downloads = remote_index
                .tenant_entry(id)
                .map(|entry| entry.has_in_progress_downloads());

            if has_in_progress_downloads.is_none() {
                error!("timeline is not found in remote index while it is present in the tenants registry")
            }

            TenantInfo {
                id: *id,
                state: Some(tenant.state),
                has_in_progress_downloads,
            }
        })
        .collect()
}

/// Check if a given timeline is "broken" \[1\].
/// The function returns an error if the timeline is "broken".
///
/// \[1\]: it's not clear now how should we classify a timeline as broken.
/// A timeline is categorized as broken when any of following conditions is true:
/// - failed to load the timeline's metadata
/// - the timeline's disk consistent LSN is zero
fn check_broken_timeline(
    conf: &'static PageServerConf,
    tenant_id: ZTenantId,
    timeline_id: ZTimelineId,
) -> anyhow::Result<()> {
    let metadata =
        load_metadata(conf, timeline_id, tenant_id).context("failed to load metadata")?;

    // A timeline with zero disk consistent LSN can happen when the page server
    // failed to checkpoint the timeline import data when creating that timeline.
    if metadata.disk_consistent_lsn() == Lsn::INVALID {
        anyhow::bail!("Timeline {timeline_id} has a zero disk consistent LSN.");
    }

    Ok(())
}

/// Note: all timelines are attached at once if and only if all of them are locally complete
fn init_local_repository(
    conf: &'static PageServerConf,
    tenant_id: ZTenantId,
    local_timeline_init_statuses: HashMap<ZTimelineId, LocalTimelineInitStatus>,
    remote_index: &RemoteIndex,
) -> anyhow::Result<(), anyhow::Error> {
    let mut timelines_to_attach = HashSet::new();
    for (timeline_id, init_status) in local_timeline_init_statuses {
        match init_status {
            LocalTimelineInitStatus::LocallyComplete => {
                debug!("timeline {timeline_id} for tenant {tenant_id} is locally complete, registering it in repository");
                check_broken_timeline(conf, tenant_id, timeline_id)
                    .context("found broken timeline")?;
                timelines_to_attach.insert(timeline_id);
            }
            LocalTimelineInitStatus::NeedsSync => {
                debug!(
                    "timeline {tenant_id} for tenant {timeline_id} needs sync, \
                     so skipped for adding into repository until sync is finished"
                );
                return Ok(());
            }
        }
    }

    // initialize local tenant
    let repo = load_local_repo(conf, tenant_id, remote_index)
        .with_context(|| format!("Failed to load repo for tenant {tenant_id}"))?;

    // Lets fail here loudly to be on the safe side.
    // XXX: It may be a better api to actually distinguish between repository startup
    //   and processing of newly downloaded timelines.
    attach_downloaded_tenant(&repo, timelines_to_attach)
        .with_context(|| format!("Failed to bootstrap timelines for tenant {tenant_id}"))?;
    Ok(())
}

fn attach_downloaded_tenant(
    repo: &LayeredRepository,
    downloaded_timelines: HashSet<ZTimelineId>,
) -> anyhow::Result<()> {
    let mut registration_queue = Vec::with_capacity(downloaded_timelines.len());

    // first need to register the in-mem representations, to avoid missing ancestors during the local disk data registration
    for timeline_id in downloaded_timelines {
        repo.attach_timeline(timeline_id).with_context(|| {
            format!("Failed to load timeline {timeline_id} into in-memory repository")
        })?;
        registration_queue.push(timeline_id);
    }

    for timeline_id in registration_queue {
        let tenant_id = repo.tenant_id();
        match tenants_state::write_tenants().get_mut(&tenant_id) {
            Some(tenant) => match tenant.local_timelines.entry(timeline_id) {
                Entry::Occupied(_) => {
                    anyhow::bail!("Local timeline {timeline_id} already registered")
                }
                Entry::Vacant(v) => {
                    v.insert(load_local_timeline(repo, timeline_id).with_context(|| {
                        format!("Failed to register add local timeline for tenant {tenant_id}")
                    })?);
                }
            },
            None => anyhow::bail!(
                "Tenant {} not found in local tenant state",
                repo.tenant_id()
            ),
        }
    }

    Ok(())
}

// Sets up wal redo manager and repository for tenant. Reduces code duplication.
// Used during pageserver startup, or when new tenant is attached to pageserver.
fn load_local_repo(
    conf: &'static PageServerConf,
    tenant_id: ZTenantId,
    remote_index: &RemoteIndex,
) -> anyhow::Result<Arc<RepositoryImpl>> {
    let mut m = tenants_state::write_tenants();
    let tenant = m.entry(tenant_id).or_insert_with(|| {
        // Set up a WAL redo manager, for applying WAL records.
        let walredo_mgr = PostgresRedoManager::new(conf, tenant_id);

        // Set up an object repository, for actual data storage.
        let repo: Arc<LayeredRepository> = Arc::new(LayeredRepository::new(
            conf,
            TenantConfOpt::default(),
            Arc::new(walredo_mgr),
            tenant_id,
            remote_index.clone(),
            conf.remote_storage_config.is_some(),
        ));
        Tenant {
            state: TenantState::Idle,
            repo,
            local_timelines: HashMap::new(),
        }
    });

    // Restore tenant config
    let tenant_conf = LayeredRepository::load_tenant_config(conf, tenant_id)?;
    tenant.repo.update_tenant_config(tenant_conf)?;

    Ok(Arc::clone(&tenant.repo))
}
