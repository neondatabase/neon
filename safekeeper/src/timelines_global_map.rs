//! This module contains global `(tenant_id, timeline_id)` -> `Arc<Timeline>` mapping.
//! All timelines should always be present in this map, this is done by loading them
//! all from the disk on startup and keeping them in memory.

use crate::safekeeper::ServerInfo;
use crate::timeline::{Timeline, TimelineError};
use crate::SafeKeeperConf;
use anyhow::{bail, Context, Result};
use once_cell::sync::Lazy;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::mpsc::Sender;
use tracing::*;
use utils::id::{TenantId, TenantTimelineId, TimelineId};
use utils::lsn::Lsn;

struct GlobalTimelinesState {
    timelines: HashMap<TenantTimelineId, Arc<Timeline>>,
    wal_backup_launcher_tx: Option<Sender<TenantTimelineId>>,
    conf: Option<SafeKeeperConf>,
}

impl GlobalTimelinesState {
    /// Get configuration, which must be set once during init.
    fn get_conf(&self) -> &SafeKeeperConf {
        self.conf
            .as_ref()
            .expect("GlobalTimelinesState conf is not initialized")
    }

    /// Get dependencies for a timeline constructor.
    fn get_dependencies(&self) -> (SafeKeeperConf, Sender<TenantTimelineId>) {
        (
            self.get_conf().clone(),
            self.wal_backup_launcher_tx.as_ref().unwrap().clone(),
        )
    }

    /// Insert timeline into the map. Returns error if timeline with the same id already exists.
    fn try_insert(&mut self, timeline: Arc<Timeline>) -> Result<()> {
        let ttid = timeline.ttid;
        if self.timelines.contains_key(&ttid) {
            bail!(TimelineError::AlreadyExists(ttid));
        }
        self.timelines.insert(ttid, timeline);
        Ok(())
    }

    /// Get timeline from the map. Returns error if timeline doesn't exist.
    fn get(&self, ttid: &TenantTimelineId) -> Result<Arc<Timeline>, TimelineError> {
        self.timelines
            .get(ttid)
            .cloned()
            .ok_or(TimelineError::NotFound(*ttid))
    }
}

static TIMELINES_STATE: Lazy<Mutex<GlobalTimelinesState>> = Lazy::new(|| {
    Mutex::new(GlobalTimelinesState {
        timelines: HashMap::new(),
        wal_backup_launcher_tx: None,
        conf: None,
    })
});

/// A zero-sized struct used to manage access to the global timelines map.
pub struct GlobalTimelines;

impl GlobalTimelines {
    /// Inject dependencies needed for the timeline constructors and load all timelines to memory.
    pub fn init(
        conf: SafeKeeperConf,
        wal_backup_launcher_tx: Sender<TenantTimelineId>,
    ) -> Result<()> {
        let mut state = TIMELINES_STATE.lock().unwrap();
        assert!(state.wal_backup_launcher_tx.is_none());
        state.wal_backup_launcher_tx = Some(wal_backup_launcher_tx);
        state.conf = Some(conf);

        // Iterate through all directories and load tenants for all directories
        // named as a valid tenant_id.
        let mut tenant_count = 0;
        let tenants_dir = state.get_conf().workdir.clone();
        for tenants_dir_entry in std::fs::read_dir(&tenants_dir)
            .with_context(|| format!("failed to list tenants dir {}", tenants_dir.display()))?
        {
            match &tenants_dir_entry {
                Ok(tenants_dir_entry) => {
                    if let Ok(tenant_id) =
                        TenantId::from_str(tenants_dir_entry.file_name().to_str().unwrap_or(""))
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

    /// Loads all timelines for the given tenant to memory. Returns fs::read_dir
    /// errors if any.
    ///
    /// Note: This function (and all reading/loading below) is sync because
    /// timelines are loaded while holding GlobalTimelinesState lock. Which is
    /// fine as this is called only from single threaded main runtime on boot,
    /// but clippy complains anyway, and suppressing that isn't trivial as async
    /// is the keyword, ha. That only other user is pull_timeline.rs for which
    /// being blocked is not that bad, and we can do spawn_blocking.
    fn load_tenant_timelines(
        state: &mut MutexGuard<'_, GlobalTimelinesState>,
        tenant_id: TenantId,
    ) -> Result<()> {
        let timelines_dir = state.get_conf().tenant_dir(&tenant_id);
        for timelines_dir_entry in std::fs::read_dir(&timelines_dir)
            .with_context(|| format!("failed to list timelines dir {}", timelines_dir.display()))?
        {
            match &timelines_dir_entry {
                Ok(timeline_dir_entry) => {
                    if let Ok(timeline_id) =
                        TimelineId::from_str(timeline_dir_entry.file_name().to_str().unwrap_or(""))
                    {
                        let ttid = TenantTimelineId::new(tenant_id, timeline_id);
                        match Timeline::load_timeline(
                            state.get_conf().clone(),
                            ttid,
                            state.wal_backup_launcher_tx.as_ref().unwrap().clone(),
                        ) {
                            Ok(timeline) => {
                                state.timelines.insert(ttid, Arc::new(timeline));
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

    /// Load timeline from disk to the memory.
    pub fn load_timeline(ttid: TenantTimelineId) -> Result<Arc<Timeline>> {
        let (conf, wal_backup_launcher_tx) = TIMELINES_STATE.lock().unwrap().get_dependencies();

        match Timeline::load_timeline(conf, ttid, wal_backup_launcher_tx) {
            Ok(timeline) => {
                let tli = Arc::new(timeline);
                // TODO: prevent concurrent timeline creation/loading
                TIMELINES_STATE
                    .lock()
                    .unwrap()
                    .timelines
                    .insert(ttid, tli.clone());
                Ok(tli)
            }
            // If we can't load a timeline, it's bad. Caller will figure it out.
            Err(e) => bail!("failed to load timeline {}, reason: {:?}", ttid, e),
        }
    }

    /// Get the number of timelines in the map.
    pub fn timelines_count() -> usize {
        TIMELINES_STATE.lock().unwrap().timelines.len()
    }

    /// Get the global safekeeper config.
    pub fn get_global_config() -> SafeKeeperConf {
        TIMELINES_STATE.lock().unwrap().get_conf().clone()
    }

    /// Create a new timeline with the given id. If the timeline already exists, returns
    /// an existing timeline.
    pub async fn create(
        ttid: TenantTimelineId,
        server_info: ServerInfo,
        commit_lsn: Lsn,
        local_start_lsn: Lsn,
    ) -> Result<Arc<Timeline>> {
        let (conf, wal_backup_launcher_tx) = {
            let state = TIMELINES_STATE.lock().unwrap();
            if let Ok(timeline) = state.get(&ttid) {
                // Timeline already exists, return it.
                return Ok(timeline);
            }
            state.get_dependencies()
        };

        info!("creating new timeline {}", ttid);

        let timeline = Arc::new(Timeline::create_empty(
            conf,
            ttid,
            wal_backup_launcher_tx,
            server_info,
            commit_lsn,
            local_start_lsn,
        )?);

        // Take a lock and finish the initialization holding this mutex. No other threads
        // can interfere with creation after we will insert timeline into the map.
        {
            let mut shared_state = timeline.write_shared_state().await;

            // We can get a race condition here in case of concurrent create calls, but only
            // in theory. create() will return valid timeline on the next try.
            TIMELINES_STATE
                .lock()
                .unwrap()
                .try_insert(timeline.clone())?;

            // Write the new timeline to the disk and start background workers.
            // Bootstrap is transactional, so if it fails, the timeline will be deleted,
            // and the state on disk should remain unchanged.
            if let Err(e) = timeline.bootstrap(&mut shared_state).await {
                // Note: the most likely reason for bootstrap failure is that the timeline
                // directory already exists on disk. This happens when timeline is corrupted
                // and wasn't loaded from disk on startup because of that. We want to preserve
                // the timeline directory in this case, for further inspection.

                // TODO: this is an unusual error, perhaps we should send it to sentry
                // TODO: compute will try to create timeline every second, we should add backoff
                error!("failed to bootstrap timeline {}: {}", ttid, e);

                // Timeline failed to bootstrap, it cannot be used. Remove it from the map.
                TIMELINES_STATE.lock().unwrap().timelines.remove(&ttid);
                return Err(e);
            }
            // We are done with bootstrap, release the lock, return the timeline.
            // {} block forces release before .await
        }
        timeline.wal_backup_launcher_tx.send(timeline.ttid).await?;
        Ok(timeline)
    }

    /// Get a timeline from the global map. If it's not present, it doesn't exist on disk,
    /// or was corrupted and couldn't be loaded on startup. Returned timeline is always valid,
    /// i.e. loaded in memory and not cancelled.
    pub fn get(ttid: TenantTimelineId) -> Result<Arc<Timeline>, TimelineError> {
        let res = TIMELINES_STATE.lock().unwrap().get(&ttid);

        match res {
            Ok(tli) => {
                if tli.is_cancelled() {
                    return Err(TimelineError::Cancelled(ttid));
                }
                Ok(tli)
            }
            _ => res,
        }
    }

    /// Returns all timelines. This is used for background timeline processes.
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
    fn get_all_for_tenant(tenant_id: TenantId) -> Vec<Arc<Timeline>> {
        let global_lock = TIMELINES_STATE.lock().unwrap();
        global_lock
            .timelines
            .values()
            .filter(|t| t.ttid.tenant_id == tenant_id)
            .cloned()
            .collect()
    }

    /// Cancels timeline, then deletes the corresponding data directory.
    pub async fn delete_force(ttid: &TenantTimelineId) -> Result<TimelineDeleteForceResult> {
        let tli_res = TIMELINES_STATE.lock().unwrap().get(ttid);
        match tli_res {
            Ok(timeline) => {
                // Take a lock and finish the deletion holding this mutex.
                let mut shared_state = timeline.write_shared_state().await;

                info!("deleting timeline {}", ttid);
                let (dir_existed, was_active) =
                    timeline.delete_from_disk(&mut shared_state).await?;

                // Remove timeline from the map.
                // FIXME: re-enable it once we fix the issue with recreation of deleted timelines
                // https://github.com/neondatabase/neon/issues/3146
                // TIMELINES_STATE.lock().unwrap().timelines.remove(ttid);

                Ok(TimelineDeleteForceResult {
                    dir_existed,
                    was_active,
                })
            }
            Err(_) => {
                // Timeline is not memory, but it may still exist on disk in broken state.
                let dir_path = TIMELINES_STATE
                    .lock()
                    .unwrap()
                    .get_conf()
                    .timeline_dir(ttid);
                let dir_existed = delete_dir(dir_path)?;

                Ok(TimelineDeleteForceResult {
                    dir_existed,
                    was_active: false,
                })
            }
        }
    }

    /// Deactivates and deletes all timelines for the tenant. Returns map of all timelines which
    /// the tenant had, `true` if a timeline was active. There may be a race if new timelines are
    /// created simultaneously. In that case the function will return error and the caller should
    /// retry tenant deletion again later.
    pub async fn delete_force_all_for_tenant(
        tenant_id: &TenantId,
    ) -> Result<HashMap<TenantTimelineId, TimelineDeleteForceResult>> {
        info!("deleting all timelines for tenant {}", tenant_id);
        let to_delete = Self::get_all_for_tenant(*tenant_id);

        let mut err = None;

        let mut deleted = HashMap::new();
        for tli in &to_delete {
            match Self::delete_force(&tli.ttid).await {
                Ok(result) => {
                    deleted.insert(tli.ttid, result);
                }
                Err(e) => {
                    error!("failed to delete timeline {}: {}", tli.ttid, e);
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
        delete_dir(
            TIMELINES_STATE
                .lock()
                .unwrap()
                .get_conf()
                .tenant_dir(tenant_id),
        )?;

        // FIXME: we temporarily disabled removing timelines from the map, see `delete_force`
        // let tlis_after_delete = Self::get_all_for_tenant(*tenant_id);
        // if !tlis_after_delete.is_empty() {
        //     // Some timelines were created while we were deleting them, returning error
        //     // to the caller, so it can retry later.
        //     bail!(
        //         "failed to delete all timelines for tenant {}: some timelines were created while we were deleting them",
        //         tenant_id
        //     );
        // }

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
