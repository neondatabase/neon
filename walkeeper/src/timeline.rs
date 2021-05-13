//! This module contains tools for managing timelines.
//!

use crate::wal_service::{HotStandbyFeedback, SafeKeeperInfo, SharedState, END_REPLICATION_MARKER};
use crate::WalAcceptorConf;
use anyhow::Result;
use lazy_static::lazy_static;
use log::*;
use pageserver::ZTimelineId;
use postgres_ffi::xlog_utils::{find_end_of_wal, TimeLineID};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use zenith_utils::lsn::Lsn;

/// Database instance (tenant)
#[derive(Debug)]
pub struct Timeline {
    pub timelineid: ZTimelineId,
    mutex: Mutex<SharedState>,
    /// conditional variable used to notify wal senders
    cond: Condvar,
}

impl Timeline {
    pub fn new(timelineid: ZTimelineId, shared_state: SharedState) -> Timeline {
        Timeline {
            timelineid,
            mutex: Mutex::new(shared_state),
            cond: Condvar::new(),
        }
    }

    /// Wait for an LSN to be committed.
    ///
    /// Returns the last committed LSN, which will be at least
    /// as high as the LSN waited for.
    ///
    pub fn wait_for_lsn(&self, lsn: Lsn) -> Lsn {
        let mut shared_state = self.mutex.lock().unwrap();
        loop {
            let commit_lsn = shared_state.commit_lsn;
            // This must be `>`, not `>=`.
            if commit_lsn > lsn {
                return commit_lsn;
            }
            shared_state = self.cond.wait(shared_state).unwrap();
        }
    }

    // Notify caught-up WAL senders about new WAL data received
    pub fn notify_wal_senders(&self, commit_lsn: Lsn) {
        let mut shared_state = self.mutex.lock().unwrap();
        if shared_state.commit_lsn < commit_lsn {
            shared_state.commit_lsn = commit_lsn;
            self.cond.notify_all();
        }
    }

    fn _stop_wal_senders(&self) {
        self.notify_wal_senders(END_REPLICATION_MARKER);
    }

    pub fn get_info(&self) -> SafeKeeperInfo {
        return self.mutex.lock().unwrap().info.clone();
    }

    pub fn set_info(&self, info: &SafeKeeperInfo) {
        self.mutex.lock().unwrap().info = info.clone();
    }

    // Accumulate hot standby feedbacks from replicas
    pub fn add_hs_feedback(&self, feedback: HotStandbyFeedback) {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.add_hs_feedback(feedback);
    }

    pub fn get_hs_feedback(&self) -> HotStandbyFeedback {
        let shared_state = self.mutex.lock().unwrap();
        shared_state.hs_feedback.clone()
    }

    pub fn load_control_file(&self, conf: &WalAcceptorConf) -> Result<()> {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.load_control_file(conf, self.timelineid)
    }

    pub fn save_control_file(&self, sync: bool) -> Result<()> {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.save_control_file(sync)
    }
}

// Utilities needed by various Connection-like objects
pub trait TimelineTools {
    fn set(&mut self, timeline_id: ZTimelineId) -> Result<()>;
    fn get(&self) -> &Arc<Timeline>;
    fn find_end_of_wal(&self, data_dir: &Path, precise: bool) -> (Lsn, TimeLineID);
}

impl TimelineTools for Option<Arc<Timeline>> {
    fn set(&mut self, timeline_id: ZTimelineId) -> Result<()> {
        // We will only set the timeline once. If it were to ever change,
        // anyone who cloned the Arc would be out of date.
        assert!(self.is_none());
        *self = Some(GlobalTimelines::store(timeline_id)?);
        Ok(())
    }

    fn get(&self) -> &Arc<Timeline> {
        self.as_ref().unwrap()
    }

    /// Find last WAL record. If "precise" is false then just locate last partial segment
    fn find_end_of_wal(&self, data_dir: &Path, precise: bool) -> (Lsn, TimeLineID) {
        let seg_size = self.get().get_info().server.wal_seg_size as usize;
        let (lsn, timeline) = find_end_of_wal(data_dir, seg_size, precise);
        (Lsn(lsn), timeline)
    }
}

lazy_static! {
    pub static ref TIMELINES: Mutex<HashMap<ZTimelineId, Arc<Timeline>>> =
        Mutex::new(HashMap::new());
}

/// A zero-sized struct used to manage access to the global timelines map.
struct GlobalTimelines;

impl GlobalTimelines {
    /// Store a new timeline into the global TIMELINES map.
    fn store(timeline_id: ZTimelineId) -> Result<Arc<Timeline>> {
        let mut timelines = TIMELINES.lock().unwrap();

        match timelines.get(&timeline_id) {
            Some(result) => Ok(Arc::clone(result)),
            None => {
                info!("creating timeline dir {}", timeline_id);
                fs::create_dir_all(timeline_id.to_string())?;

                let shared_state = SharedState::new();

                let new_tid = Arc::new(Timeline::new(timeline_id, shared_state));
                timelines.insert(timeline_id, Arc::clone(&new_tid));
                Ok(new_tid)
            }
        }
    }
}
