use std::{collections::HashMap, sync::Arc};

use utils::id::TenantTimelineId;

use crate::timeline::Timeline;

/// Set of timelines, supports operations:
/// - add timeline
/// - remove timeline
/// - clone the set
///
/// Usually used for keeping subset of timelines. For example active timelines that require broker push.
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

    /// If present is true, adds timeline to the set, otherwise removes it.
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

    /// Returns all timelines in the set.
    pub fn get_all(&self) -> Vec<Arc<Timeline>> {
        self.timelines.lock().unwrap().values().cloned().collect()
    }

    /// Returns a timeline guard for easy presence control.
    pub fn guard(self: &Arc<Self>, tli: Arc<Timeline>) -> TimelineSetGuard {
        let is_present = self.is_present(&tli.ttid);
        TimelineSetGuard {
            timelines_set: self.clone(),
            tli,
            is_present,
        }
    }
}

/// Guard is used to add or remove timelines from the set.
///
/// If the timeline present in set, it will be removed from it on drop.
/// Note: do not use more than one guard for the same timeline, it caches the presence state.
/// It is designed to be used in the manager task only.
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

    pub fn get(&self) -> bool {
        self.is_present
    }
}

impl Drop for TimelineSetGuard {
    fn drop(&mut self) {
        // remove timeline from the map on drop
        self.timelines_set.delete(&self.tli.ttid);
    }
}
