//! This module contains global (tenant_id, timeline_id) -> Arc<Timeline> mapping.
//! All timelines should always be present in this map, this is done by loading them
//! all from the disk on startup and keeping them in memory.

use crate::safekeeper::ServerInfo;
use crate::timeline::{Timeline, TimelineError};
use crate::SafeKeeperConf;
use anyhow::{anyhow, bail, Context, Result};
use once_cell::sync::Lazy;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::mpsc::Sender;
use tracing::*;
use utils::zid::{ZTenantId, ZTenantTimelineId, ZTimelineId};

struct GlobalTimelinesState {
    timelines: HashMap<ZTenantTimelineId, Arc<Timeline>>,
    wal_backup_launcher_tx: Option<Sender<ZTenantTimelineId>>,
    conf: SafeKeeperConf,
}

impl GlobalTimelinesState {
    /// Get dependencies for a timeline constructor.
    fn get_dependencies(&self) -> (SafeKeeperConf, Sender<ZTenantTimelineId>) {
        (
            self.conf.clone(),
            self.wal_backup_launcher_tx.as_ref().unwrap().clone(),
        )
    }

    /// Insert timeline into the map. Returns error if timeline with the same id already exists.
    fn try_insert(&mut self, timeline: Arc<Timeline>) -> Result<()> {
        let zttid = timeline.zttid;
        if self.timelines.contains_key(&zttid) {
            bail!(TimelineError::AlreadyExists(zttid));
        }
        self.timelines.insert(zttid, timeline);
        Ok(())
    }

    /// Get timeline from the map. Returns error if timeline doesn't exist.
    fn get(&self, zttid: &ZTenantTimelineId) -> Result<Arc<Timeline>> {
        self.timelines
            .get(zttid)
            .cloned()
            .ok_or_else(|| anyhow!(TimelineError::NotFound(*zttid)))
    }
}

static TIMELINES_STATE: Lazy<Mutex<GlobalTimelinesState>> = Lazy::new(|| {
    Mutex::new(GlobalTimelinesState {
        timelines: HashMap::new(),
        wal_backup_launcher_tx: None,
        conf: SafeKeeperConf::default(),
    })
});

/// A zero-sized struct used to manage access to the global timelines map.
pub struct GlobalTimelines;

impl GlobalTimelines {
    /// Inject dependencies needed for the timeline constructors and load all timelines to memory.
    pub fn init(
        conf: SafeKeeperConf,
        wal_backup_launcher_tx: Sender<ZTenantTimelineId>,
    ) -> Result<()> {
        let mut state = TIMELINES_STATE.lock().unwrap();
        assert!(state.wal_backup_launcher_tx.is_none());
        state.wal_backup_launcher_tx = Some(wal_backup_launcher_tx);
        state.conf = conf;

        // Iterate through all directories and load tenants for all directories
        // named as a valid tenant_id.
        let mut tenant_count = 0;
        let tenants_dir = state.conf.workdir.clone();
        for tenants_dir_entry in std::fs::read_dir(&tenants_dir)
            .with_context(|| format!("failed to list tenants dir {}", tenants_dir.display()))?
        {
            match &tenants_dir_entry {
                Ok(tenants_dir_entry) => {
                    if let Ok(tenant_id) =
                        ZTenantId::from_str(tenants_dir_entry.file_name().to_str().unwrap_or(""))
                    {
                        tenant_count += 1;
                        GlobalTimelines::load_tenant_timelines(&mut state, tenant_id)?;
                    }
                }
                Err(e) => error!(
                    "failed to list tenants dir entry {:?} in directory {}, reason: {:?}",
                    tenants_dir_entry,
                    tenants_dir.display(),
                    e
                ),
            }
        }

        info!(
            "found {} tenants directories, successfully loaded {} timelines",
            tenant_count,
            state.timelines.len()
        );
        Ok(())
    }

    /// Loads all timelines for the given tenant to memory. Returns fs::read_dir errors if any.
    fn load_tenant_timelines(
        state: &mut MutexGuard<GlobalTimelinesState>,
        tenant_id: ZTenantId,
    ) -> Result<()> {
        let timelines_dir = state.conf.tenant_dir(&tenant_id);
        for timelines_dir_entry in std::fs::read_dir(&timelines_dir)
            .with_context(|| format!("failed to list timelines dir {}", timelines_dir.display()))?
        {
            match &timelines_dir_entry {
                Ok(timeline_dir_entry) => {
                    if let Ok(timeline_id) =
                        ZTimelineId::from_str(timeline_dir_entry.file_name().to_str().unwrap_or(""))
                    {
                        let zttid = ZTenantTimelineId::new(tenant_id, timeline_id);
                        match Timeline::load_timeline(
                            state.conf.clone(),
                            zttid,
                            state.wal_backup_launcher_tx.as_ref().unwrap().clone(),
                        ) {
                            Ok(timeline) => {
                                state.timelines.insert(zttid, Arc::new(timeline));
                            }
                            // If we can't load a timeline, it's most likely because of a corrupted
                            // directory. We will log an error and won't allow to delete/recreate
                            // this timeline. The only way to fix this timeline is to repair manually
                            // and restart the safekeeper.
                            Err(e) => error!(
                                "failed to load timeline {} for tenant {}, reason: {:?}",
                                timeline_id, tenant_id, e
                            ),
                        }
                    }
                }
                Err(e) => error!(
                    "failed to list timelines dir entry {:?} in directory {}, reason: {:?}",
                    timelines_dir_entry,
                    timelines_dir.display(),
                    e
                ),
            }
        }

        Ok(())
    }

    /// Create a new timeline with the given id. If the timeline already exists, returns
    /// an existing timeline.
    pub fn create(zttid: ZTenantTimelineId, server_info: ServerInfo) -> Result<Arc<Timeline>> {
        let (conf, wal_backup_launcher_tx) = {
            let state = TIMELINES_STATE.lock().unwrap();
            if let Ok(timeline) = state.get(&zttid) {
                // Timeline already exists, return it.
                return Ok(timeline);
            }
            state.get_dependencies()
        };

        info!("creating new timeline {}", zttid);

        let timeline = Arc::new(Timeline::create_empty(
            conf,
            zttid,
            wal_backup_launcher_tx,
            server_info,
        )?);

        // Take a lock and finish the initialization holding this mutex. No other threads
        // can interfere with creation after we will insert timeline into the map.
        let mut shared_state = timeline.write_shared_state();

        // We can get a race condition here in case of concurrent create calls, but only
        // in theory. create() will return valid timeline on the next try.
        TIMELINES_STATE
            .lock()
            .unwrap()
            .try_insert(timeline.clone())?;

        // Write the new timeline to the disk and start background workers.
        // Bootstrap is transactional, so if it fails, the timeline will be deleted,
        // and the state on disk should remain unchanged.
        match timeline.bootstrap(&mut shared_state) {
            Ok(_) => {
                // We are done with bootstrap, release the lock, return the timeline.
                drop(shared_state);
                Ok(timeline)
            }
            Err(e) => {
                // Note: the most likely reason for bootstrap failure is that the timeline
                // directory already exists on disk. This happens when timeline is corrupted
                // and wasn't loaded from disk on startup because of that. We want to preserve
                // the timeline directory in this case, for further inspection.

                // TODO: this is an unusual error, perhaps we should send it to sentry
                // TODO: compute will try to create timeline every second, we should add backoff
                error!("failed to bootstrap timeline {}: {}", zttid, e);

                // Timeline failed to bootstrap, it cannot be used. Remove it from the map.
                TIMELINES_STATE.lock().unwrap().timelines.remove(&zttid);
                Err(e)
            }
        }
    }

    /// Get a timeline from the global map. If it's not present, it doesn't exist on disk,
    /// or was corrupted and couldn't be loaded on startup. Returned timeline is always valid,
    /// i.e. loaded in memory and not cancelled.
    pub fn get(zttid: ZTenantTimelineId) -> Result<Arc<Timeline>> {
        let global_lock = TIMELINES_STATE.lock().unwrap();

        match global_lock.timelines.get(&zttid) {
            Some(result) => {
                if result.is_cancelled() {
                    anyhow::bail!(TimelineError::Cancelled(zttid));
                }
                Ok(Arc::clone(result))
            }
            None => anyhow::bail!(TimelineError::NotFound(zttid)),
        }
    }

    /// Returns all timelines. This is used for background timeline proccesses.
    pub fn get_all() -> Vec<Arc<Timeline>> {
        let global_lock = TIMELINES_STATE.lock().unwrap();
        global_lock
            .timelines
            .values()
            .cloned()
            .filter(|t| !t.is_cancelled())
            .collect()
    }

    /// Returns all timelines belonging to a given tenant. Used for deleting all timelines of a tenant,
    /// and that's why it can return cancelled timelines, to retry deleting them.
    fn get_all_for_tenant(tenant_id: ZTenantId) -> Vec<Arc<Timeline>> {
        let global_lock = TIMELINES_STATE.lock().unwrap();
        global_lock
            .timelines
            .values()
            .filter(|t| t.zttid.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Cancels timeline, then deletes the corresponding data directory.
    pub fn delete_force(zttid: &ZTenantTimelineId) -> Result<TimelineDeleteForceResult> {
        let timeline = TIMELINES_STATE.lock().unwrap().get(zttid)?;

        // Take a lock and finish the deletion holding this mutex.
        let mut shared_state = timeline.write_shared_state();

        info!("deleting timeline {}", zttid);
        let (dir_existed, was_active) = timeline.delete_from_disk(&mut shared_state)?;

        // Remove timeline from the map.
        TIMELINES_STATE.lock().unwrap().timelines.remove(zttid);

        Ok(TimelineDeleteForceResult {
            dir_existed,
            was_active,
        })
    }

    /// Deactivates and deletes all timelines for the tenant. Returns map of all timelines which
    /// the tenant had, `true` if a timeline was active. There may be a race if new timelines are
    /// created simultaneously. In that case the function will return error and the caller should
    /// retry tenant deletion again later.
    pub fn delete_force_all_for_tenant(
        tenant_id: &ZTenantId,
    ) -> Result<HashMap<ZTenantTimelineId, TimelineDeleteForceResult>> {
        info!("deleting all timelines for tenant {}", tenant_id);
        let to_delete = Self::get_all_for_tenant(*tenant_id);

        let mut err = None;

        let mut deleted = HashMap::new();
        for tli in &to_delete {
            match Self::delete_force(&tli.zttid) {
                Ok(result) => {
                    deleted.insert(tli.zttid, result);
                }
                Err(e) => {
                    error!("failed to delete timeline {}: {}", tli.zttid, e);
                    // Save error to return later.
                    err = Some(e);
                }
            }
        }

        // If there was an error, return it.
        if let Some(e) = err {
            return Err(e);
        }

        // There may be broken timelines on disk, so delete the whole tenant dir as well.
        // Note that we could concurrently create new timelines while we were deleting them,
        // so the directory may be not empty. In this case timelines will have bad state
        // and timeline background jobs can panic.
        delete_dir(TIMELINES_STATE.lock().unwrap().conf.tenant_dir(tenant_id))?;

        let tlis_after_delete = Self::get_all_for_tenant(*tenant_id);
        if !tlis_after_delete.is_empty() {
            // Some timelines were created while we were deleting them, returning error
            // to the caller, so it can retry later.
            bail!(
                "failed to delete all timelines for tenant {}: some timelines were created while we were deleting them",
                tenant_id
            );
        }

        Ok(deleted)
    }
}

#[derive(Clone, Copy, Serialize)]
pub struct TimelineDeleteForceResult {
    pub dir_existed: bool,
    pub was_active: bool,
}

/// Deletes directory and it's contents. Returns false if directory does not exist.
fn delete_dir(path: PathBuf) -> Result<bool> {
    match std::fs::remove_dir_all(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}
