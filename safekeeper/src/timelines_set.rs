use std::{collections::HashMap, sync::Arc};

use utils::id::TenantTimelineId;

use crate::timeline::Timeline;

/// Set of timelines, used to keep subset of timelines used for some tasks (like WAL removal).
pub struct TimelinesSet {
    timelines: std::sync::Mutex<HashMap<TenantTimelineId, Arc<Timeline>>>,
}

impl Default for TimelinesSet {
    fn default() -> Self {
        Self {
            timelines: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

impl TimelinesSet {
    pub fn insert(&self, tli: Arc<Timeline>) {
        self.timelines.lock().unwrap().insert(tli.ttid, tli);
    }

    pub fn delete(&self, ttid: &TenantTimelineId) {
        self.timelines.lock().unwrap().remove(ttid);
    }

    pub fn set_present(&self, tli: Arc<Timeline>, present: bool) {
        if present {
            self.insert(tli);
        } else {
            self.delete(&tli.ttid);
        }
    }

    pub fn is_present(&self, ttid: &TenantTimelineId) -> bool {
        self.timelines.lock().unwrap().contains_key(ttid)
    }

    pub fn get_all(&self) -> Vec<Arc<Timeline>> {
        self.timelines.lock().unwrap().values().cloned().collect()
    }

    pub fn guard(self: &Arc<Self>, tli: Arc<Timeline>) -> TimelineSetGuard {
        let is_present = self.is_present(&tli.ttid);
        TimelineSetGuard {
            timelines_set: self.clone(),
            tli,
            is_present,
        }
    }
}

pub struct TimelineSetGuard {
    timelines_set: Arc<TimelinesSet>,
    tli: Arc<Timeline>,
    is_present: bool,
}

impl TimelineSetGuard {
    /// Returns true if the state was changed.
    pub fn set(&mut self, present: bool) -> bool {
        if present == self.is_present {
            return false;
        }
        self.is_present = present;
        self.timelines_set.set_present(self.tli.clone(), present);
        true
    }
}

impl Drop for TimelineSetGuard {
    fn drop(&mut self) {
        // remove timeline from the map on drop
        self.timelines_set.delete(&self.tli.ttid);
    }
}
