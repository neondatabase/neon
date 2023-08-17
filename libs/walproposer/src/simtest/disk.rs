use std::collections::HashMap;
use std::sync::Arc;

use safekeeper::safekeeper::SafeKeeperState;
use safekeeper::simlib::sync::Mutex;
use utils::id::TenantTimelineId;

pub struct Disk {
    pub timelines: Mutex<HashMap<TenantTimelineId, Arc<TimelineDisk>>>,
}

impl Disk {
    pub fn new() -> Self {
        Disk {
            timelines: Mutex::new(HashMap::new()),
        }
    }

    pub fn put(&self, ttid: &TenantTimelineId, state: SafeKeeperState) -> Arc<TimelineDisk> {
        self.timelines
            .lock()
            .entry(ttid.clone())
            .or_insert_with(|| {
                Arc::new(TimelineDisk {
                    state: Mutex::new(state),
                })
            })
            .clone()
    }
}

pub struct TimelineDisk {
    pub state: Mutex<SafeKeeperState>,
}
