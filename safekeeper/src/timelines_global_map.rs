//! This module contains global (tenant_id, timeline_id) -> Arc<Timeline> mapping.

use crate::timeline::Timeline;
use crate::SafeKeeperConf;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Sender;
use utils::zid::{ZTenantId, ZTenantTimelineId};

struct GlobalTimelinesState {
    timelines: HashMap<ZTenantTimelineId, Arc<Timeline>>,
    wal_backup_launcher_tx: Option<Sender<ZTenantTimelineId>>,
    conf: SafeKeeperConf,
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
    // Inject dependencies needed for the timeline constructors.
    pub fn init(conf: SafeKeeperConf, wal_backup_launcher_tx: Sender<ZTenantTimelineId>) {
        let mut state = TIMELINES_STATE.lock().unwrap();
        assert!(state.wal_backup_launcher_tx.is_none());
        state.wal_backup_launcher_tx = Some(wal_backup_launcher_tx);
        state.conf = conf;
    }

    /// Get a timeline from the global map. If it doesn't exist in the map, new uninitialised timeline
    /// will be added to the map.
    pub fn get(zttid: ZTenantTimelineId) -> Arc<Timeline> {
        let mut global_lock = TIMELINES_STATE.lock().unwrap();

        match global_lock.timelines.get(&zttid) {
            Some(result) => Arc::clone(result),
            None => {
                let tli = Arc::new(Timeline::new(
                    global_lock.conf.clone(),
                    zttid,
                    global_lock.wal_backup_launcher_tx.as_ref().unwrap().clone(),
                ));
                global_lock.timelines.insert(zttid, tli.clone());
                tli
            }
        }
    }

    /// Returns all timelines. This is used for background timeline proccesses.
    pub fn get_all() -> Vec<Arc<Timeline>> {
        let global_lock = TIMELINES_STATE.lock().unwrap();
        global_lock.timelines.values().cloned().collect()
    }

    /// Returns all timelines belonging to a given tenant. Used for deleting all timelines of a tenant.
    pub fn get_all_for_tenant(tenant_id: ZTenantId) -> Vec<Arc<Timeline>> {
        let global_lock = TIMELINES_STATE.lock().unwrap();
        global_lock
            .timelines
            .values()
            .filter(|t| t.zttid.tenant_id == tenant_id)
            .cloned()
            .collect()
    }
}
