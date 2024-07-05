//! This module contains global `(tenant_id, timeline_id)` -> `Arc<Timeline>` mapping.
//! All timelines should always be present in this map, this is done by loading them
//! all from the disk on startup and keeping them in memory.

use crate::safekeeper::ServerInfo;
use crate::timeline::{get_tenant_dir, get_timeline_dir, Timeline, TimelineError};
use crate::timelines_set::TimelinesSet;
use crate::wal_backup_partial::RateLimiter;
use crate::SafeKeeperConf;
use anyhow::{bail, Context, Result};
use camino::Utf8PathBuf;
use once_cell::sync::Lazy;
use serde::Serialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::*;
use utils::id::{TenantId, TenantTimelineId, TimelineId};
use utils::lsn::Lsn;

struct GlobalTimelinesState {
    timelines: HashMap<TenantTimelineId, Arc<Timeline>>,

    // A tombstone indicates this timeline used to exist has been deleted.  These are used to prevent
    // on-demand timeline creation from recreating deleted timelines.  This is only soft-enforced, as
    // this map is dropped on restart.
    tombstones: HashMap<TenantTimelineId, Instant>,

    conf: Option<SafeKeeperConf>,
    broker_active_set: Arc<TimelinesSet>,
    load_lock: Arc<tokio::sync::Mutex<TimelineLoadLock>>,
    partial_backup_rate_limiter: RateLimiter,
}

// Used to prevent concurrent timeline loading.
pub struct TimelineLoadLock;

impl GlobalTimelinesState {
    /// Get configuration, which must be set once during init.
    fn get_conf(&self) -> &SafeKeeperConf {
        self.conf
            .as_ref()
            .expect("GlobalTimelinesState conf is not initialized")
    }

    /// Get dependencies for a timeline constructor.
    fn get_dependencies(&self) -> (SafeKeeperConf, Arc<TimelinesSet>, RateLimiter) {
        (
            self.get_conf().clone(),
            self.broker_active_set.clone(),
            self.partial_backup_rate_limiter.clone(),
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

    fn delete(&mut self, ttid: TenantTimelineId) {
        self.timelines.remove(&ttid);
        self.tombstones.insert(ttid, Instant::now());
    }
}

static TIMELINES_STATE: Lazy<Mutex<GlobalTimelinesState>> = Lazy::new(|| {
    Mutex::new(GlobalTimelinesState {
        timelines: HashMap::new(),
        tombstones: HashMap::new(),
        conf: None,
        broker_active_set: Arc::new(TimelinesSet::default()),
        load_lock: Arc::new(tokio::sync::Mutex::new(TimelineLoadLock)),
        partial_backup_rate_limiter: RateLimiter::new(1),
    })
});

/// A zero-sized struct used to manage access to the global timelines map.
pub struct GlobalTimelines;

impl GlobalTimelines {
    /// Inject dependencies needed for the timeline constructors and load all timelines to memory.
    pub async fn init(conf: SafeKeeperConf) -> Result<()> {
        // clippy isn't smart enough to understand that drop(state) releases the
        // lock, so use explicit block
        let tenants_dir = {
            let mut state = TIMELINES_STATE.lock().unwrap();
            state.partial_backup_rate_limiter = RateLimiter::new(conf.partial_backup_concurrency);
            state.conf = Some(conf);

            // Iterate through all directories and load tenants for all directories
            // named as a valid tenant_id.
            state.get_conf().workdir.clone()
        };
        let mut tenant_count = 0;
        for tenants_dir_entry in std::fs::read_dir(&tenants_dir)
            .with_context(|| format!("failed to list tenants dir {}", tenants_dir))?
        {
            match &tenants_dir_entry {
                Ok(tenants_dir_entry) => {
                    if let Ok(tenant_id) =
                        TenantId::from_str(tenants_dir_entry.file_name().to_str().unwrap_or(""))
                    {
                        tenant_count += 1;
                        GlobalTimelines::load_tenant_timelines(tenant_id).await?;
                    }
                }
                Err(e) => error!(
                    "failed to list tenants dir entry {:?} in directory {}, reason: {:?}",
                    tenants_dir_entry, tenants_dir, e
                ),
            }
        }

        info!(
            "found {} tenants directories, successfully loaded {} timelines",
            tenant_count,
            TIMELINES_STATE.lock().unwrap().timelines.len()
        );
        Ok(())
    }

    /// Loads all timelines for the given tenant to memory. Returns fs::read_dir
    /// errors if any.
    ///
    /// It is async for update_status_notify sake. Since TIMELINES_STATE lock is
    /// sync and there is no important reason to make it async (it is always
    /// held for a short while) we just lock and unlock it for each timeline --
    /// this function is called during init when nothing else is running, so
    /// this is fine.
    async fn load_tenant_timelines(tenant_id: TenantId) -> Result<()> {
        let (conf, broker_active_set, partial_backup_rate_limiter) = {
            let state = TIMELINES_STATE.lock().unwrap();
            state.get_dependencies()
        };

        let timelines_dir = get_tenant_dir(&conf, &tenant_id);
        for timelines_dir_entry in std::fs::read_dir(&timelines_dir)
            .with_context(|| format!("failed to list timelines dir {}", timelines_dir))?
        {
            match &timelines_dir_entry {
                Ok(timeline_dir_entry) => {
                    if let Ok(timeline_id) =
                        TimelineId::from_str(timeline_dir_entry.file_name().to_str().unwrap_or(""))
                    {
                        let ttid = TenantTimelineId::new(tenant_id, timeline_id);
                        match Timeline::load_timeline(&conf, ttid) {
                            Ok(timeline) => {
                                let tli = Arc::new(timeline);
                                TIMELINES_STATE
                                    .lock()
                                    .unwrap()
                                    .timelines
                                    .insert(ttid, tli.clone());
                                tli.bootstrap(
                                    &conf,
                                    broker_active_set.clone(),
                                    partial_backup_rate_limiter.clone(),
                                );
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
                    timelines_dir_entry, timelines_dir, e
                ),
            }
        }

        Ok(())
    }

    /// Take a lock for timeline loading.
    pub async fn loading_lock() -> Arc<tokio::sync::Mutex<TimelineLoadLock>> {
        TIMELINES_STATE.lock().unwrap().load_lock.clone()
    }

    /// Load timeline from disk to the memory.
    pub async fn load_timeline<'a>(
        _guard: &tokio::sync::MutexGuard<'a, TimelineLoadLock>,
        ttid: TenantTimelineId,
    ) -> Result<Arc<Timeline>> {
        let (conf, broker_active_set, partial_backup_rate_limiter) =
            TIMELINES_STATE.lock().unwrap().get_dependencies();

        match Timeline::load_timeline(&conf, ttid) {
            Ok(timeline) => {
                let tli = Arc::new(timeline);

                // TODO: prevent concurrent timeline creation/loading
                {
                    let mut state = TIMELINES_STATE.lock().unwrap();

                    // We may be have been asked to load a timeline that was previously deleted (e.g. from `pull_timeline.rs`).  We trust
                    // that the human doing this manual intervention knows what they are doing, and remove its tombstone.
                    if state.tombstones.remove(&ttid).is_some() {
                        warn!("Un-deleted timeline {ttid}");
                    }

                    state.timelines.insert(ttid, tli.clone());
                }

                tli.bootstrap(&conf, broker_active_set, partial_backup_rate_limiter);

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

    pub fn get_global_broker_active_set() -> Arc<TimelinesSet> {
        TIMELINES_STATE.lock().unwrap().broker_active_set.clone()
    }

    /// Create a new timeline with the given id. If the timeline already exists, returns
    /// an existing timeline.
    pub(crate) async fn create(
        ttid: TenantTimelineId,
        server_info: ServerInfo,
        commit_lsn: Lsn,
        local_start_lsn: Lsn,
    ) -> Result<Arc<Timeline>> {
        let (conf, broker_active_set, partial_backup_rate_limiter) = {
            let state = TIMELINES_STATE.lock().unwrap();
            if let Ok(timeline) = state.get(&ttid) {
                // Timeline already exists, return it.
                return Ok(timeline);
            }

            if state.tombstones.contains_key(&ttid) {
                anyhow::bail!("Timeline {ttid} is deleted, refusing to recreate");
            }

            state.get_dependencies()
        };

        info!("creating new timeline {}", ttid);

        let timeline = Arc::new(Timeline::create_empty(
            &conf,
            ttid,
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
            if let Err(e) = timeline
                .init_new(
                    &mut shared_state,
                    &conf,
                    broker_active_set,
                    partial_backup_rate_limiter,
                )
                .await
            {
                // Note: the most likely reason for init failure is that the timeline
                // directory already exists on disk. This happens when timeline is corrupted
                // and wasn't loaded from disk on startup because of that. We want to preserve
                // the timeline directory in this case, for further inspection.

                // TODO: this is an unusual error, perhaps we should send it to sentry
                // TODO: compute will try to create timeline every second, we should add backoff
                error!("failed to init new timeline {}: {}", ttid, e);

                // Timeline failed to init, it cannot be used. Remove it from the map.
                TIMELINES_STATE.lock().unwrap().timelines.remove(&ttid);
                return Err(e);
            }
            // We are done with bootstrap, release the lock, return the timeline.
            // {} block forces release before .await
        }
        Ok(timeline)
    }

    /// Get a timeline from the global map. If it's not present, it doesn't exist on disk,
    /// or was corrupted and couldn't be loaded on startup. Returned timeline is always valid,
    /// i.e. loaded in memory and not cancelled.
    pub(crate) fn get(ttid: TenantTimelineId) -> Result<Arc<Timeline>, TimelineError> {
        let tli_res = {
            let state = TIMELINES_STATE.lock().unwrap();
            state.get(&ttid)
        };
        match tli_res {
            Ok(tli) => {
                if tli.is_cancelled() {
                    return Err(TimelineError::Cancelled(ttid));
                }
                Ok(tli)
            }
            _ => tli_res,
        }
    }

    /// Returns all timelines. This is used for background timeline processes.
    pub fn get_all() -> Vec<Arc<Timeline>> {
        let global_lock = TIMELINES_STATE.lock().unwrap();
        global_lock
            .timelines
            .values()
            .filter(|t| !t.is_cancelled())
            .cloned()
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
    /// If only_local, doesn't remove WAL segments in remote storage.
    pub(crate) async fn delete(
        ttid: &TenantTimelineId,
        only_local: bool,
    ) -> Result<TimelineDeleteForceResult> {
        let tli_res = {
            let state = TIMELINES_STATE.lock().unwrap();

            if state.tombstones.contains_key(ttid) {
                // Presence of a tombstone guarantees that a previous deletion has completed and there is no work to do.
                info!("Timeline {ttid} was already deleted");
                return Ok(TimelineDeleteForceResult {
                    dir_existed: false,
                    was_active: false,
                });
            }

            state.get(ttid)
        };

        let result = match tli_res {
            Ok(timeline) => {
                let was_active = timeline.broker_active.load(Ordering::Relaxed);

                // Take a lock and finish the deletion holding this mutex.
                let mut shared_state = timeline.write_shared_state().await;

                info!("deleting timeline {}, only_local={}", ttid, only_local);
                let dir_existed = timeline.delete(&mut shared_state, only_local).await?;

                Ok(TimelineDeleteForceResult {
                    dir_existed,
                    was_active, // TODO: we probably should remove this field
                })
            }
            Err(_) => {
                // Timeline is not memory, but it may still exist on disk in broken state.
                let dir_path = get_timeline_dir(TIMELINES_STATE.lock().unwrap().get_conf(), ttid);
                let dir_existed = delete_dir(dir_path)?;

                Ok(TimelineDeleteForceResult {
                    dir_existed,
                    was_active: false,
                })
            }
        };

        // Finalize deletion, by dropping Timeline objects and storing smaller tombstones.  The tombstones
        // are used to prevent still-running computes from re-creating the same timeline when they send data,
        // and to speed up repeated deletion calls by avoiding re-listing objects.
        TIMELINES_STATE.lock().unwrap().delete(*ttid);

        result
    }

    /// Deactivates and deletes all timelines for the tenant. Returns map of all timelines which
    /// the tenant had, `true` if a timeline was active. There may be a race if new timelines are
    /// created simultaneously. In that case the function will return error and the caller should
    /// retry tenant deletion again later.
    ///
    /// If only_local, doesn't remove WAL segments in remote storage.
    pub async fn delete_force_all_for_tenant(
        tenant_id: &TenantId,
        only_local: bool,
    ) -> Result<HashMap<TenantTimelineId, TimelineDeleteForceResult>> {
        info!("deleting all timelines for tenant {}", tenant_id);
        let to_delete = Self::get_all_for_tenant(*tenant_id);

        let mut err = None;

        let mut deleted = HashMap::new();
        for tli in &to_delete {
            match Self::delete(&tli.ttid, only_local).await {
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
        delete_dir(get_tenant_dir(
            TIMELINES_STATE.lock().unwrap().get_conf(),
            tenant_id,
        ))?;

        Ok(deleted)
    }

    pub fn housekeeping(tombstone_ttl: &Duration) {
        let mut state = TIMELINES_STATE.lock().unwrap();

        // We keep tombstones long enough to have a good chance of preventing rogue computes from re-creating deleted
        // timelines.  If a compute kept running for longer than this TTL (or across a safekeeper restart) then they
        // may recreate a deleted timeline.
        let now = Instant::now();
        state
            .tombstones
            .retain(|_, v| now.duration_since(*v) < *tombstone_ttl);
    }
}

#[derive(Clone, Copy, Serialize)]
pub struct TimelineDeleteForceResult {
    pub dir_existed: bool,
    pub was_active: bool,
}

/// Deletes directory and it's contents. Returns false if directory does not exist.
fn delete_dir(path: Utf8PathBuf) -> Result<bool> {
    match std::fs::remove_dir_all(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}
