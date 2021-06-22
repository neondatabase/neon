//! This module contains tools for managing timelines.
//!

use anyhow::{bail, Result};
use fs2::FileExt;
use lazy_static::lazy_static;
use log::*;
use pageserver::ZTimelineId;
use postgres_ffi::xlog_utils::{find_end_of_wal, TimeLineID};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use zenith_utils::bin_ser::LeSer;
use zenith_utils::lsn::Lsn;

use crate::receive_wal::{SafeKeeperInfo, CONTROL_FILE_NAME, SK_FORMAT_VERSION, SK_MAGIC};
use crate::replication::{HotStandbyFeedback, END_REPLICATION_MARKER};
use crate::WalAcceptorConf;

/// Shared state associated with database instance (tenant)
#[derive(Debug)]
struct SharedState {
    /// quorum commit LSN
    commit_lsn: Lsn,
    /// information about this safekeeper
    info: SafeKeeperInfo,
    /// opened file control file handle (needed to hold exlusive file lock
    control_file: Option<File>,
    /// combined hot standby feedback from all replicas
    hs_feedback: HotStandbyFeedback,
}

impl SharedState {
    fn new() -> Self {
        Self {
            commit_lsn: Lsn(0),
            info: SafeKeeperInfo::new(),
            control_file: None,
            hs_feedback: HotStandbyFeedback {
                ts: 0,
                xmin: u64::MAX,
                catalog_xmin: u64::MAX,
            },
        }
    }

    /// Accumulate hot standby feedbacks from replicas
    pub fn add_hs_feedback(&mut self, feedback: HotStandbyFeedback) {
        self.hs_feedback.xmin = min(self.hs_feedback.xmin, feedback.xmin);
        self.hs_feedback.catalog_xmin = min(self.hs_feedback.catalog_xmin, feedback.catalog_xmin);
        self.hs_feedback.ts = max(self.hs_feedback.ts, feedback.ts);
    }

    /// Load and lock control file (prevent running more than one instance of safekeeper)
    /// If create=false and file doesn't exist, bails out.
    pub fn load_control_file(
        &mut self,
        conf: &WalAcceptorConf,
        timelineid: ZTimelineId,
        create: bool,
    ) -> Result<()> {
        if self.control_file.is_some() {
            info!("control file for timeline {} is already open", timelineid);
            return Ok(());
        }

        let control_file_path = conf
            .data_dir
            .join(timelineid.to_string())
            .join(CONTROL_FILE_NAME);
        info!(
            "loading control file {}, create={}",
            control_file_path.display(),
            create
        );
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);
        if create {
            opts.create(true);
        }
        match opts.open(&control_file_path) {
            Ok(file) => {
                // Lock file to prevent two or more active wal_acceptors
                match file.try_lock_exclusive() {
                    Ok(()) => {}
                    Err(e) => {
                        bail!(
                            "Control file {:?} is locked by some other process: {}",
                            &control_file_path,
                            e
                        );
                    }
                }
                self.control_file = Some(file);

                let cfile_ref = self.control_file.as_mut().unwrap();
                if cfile_ref.metadata().unwrap().len() == 0 {
                    if !create {
                        bail!("control file is empty");
                    }
                } else {
                    match SafeKeeperInfo::des_from(cfile_ref) {
                        Err(e) => {
                            bail!("failed to read control file {:?}: {}", control_file_path, e);
                        }
                        Ok(info) => {
                            if info.magic != SK_MAGIC {
                                bail!("Invalid control file magic: {}", info.magic);
                            }
                            if info.format_version != SK_FORMAT_VERSION {
                                bail!(
                                    "Incompatible format version: {} vs. {}",
                                    info.format_version,
                                    SK_FORMAT_VERSION
                                );
                            }
                            self.info = info;
                        }
                    }
                }
            }
            Err(e) => {
                bail!(
                    "Failed to open control file {:?}: {}",
                    &control_file_path,
                    e
                );
            }
        }
        Ok(())
    }

    pub fn save_control_file(&mut self, sync: bool) -> Result<()> {
        let file = self.control_file.as_mut().unwrap();
        file.seek(SeekFrom::Start(0))?;
        self.info.ser_into(file)?;
        if sync {
            file.sync_all()?;
        }
        Ok(())
    }
}

/// Database instance (tenant)
#[derive(Debug)]
pub struct Timeline {
    pub timelineid: ZTimelineId,
    mutex: Mutex<SharedState>,
    /// conditional variable used to notify wal senders
    cond: Condvar,
}

impl Timeline {
    fn new(timelineid: ZTimelineId, shared_state: SharedState) -> Timeline {
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

    pub fn load_control_file(&self, conf: &WalAcceptorConf, create: bool) -> Result<()> {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.load_control_file(conf, self.timelineid, create)
    }

    pub fn save_control_file(&self, sync: bool) -> Result<()> {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.save_control_file(sync)
    }
}

// Utilities needed by various Connection-like objects
pub trait TimelineTools {
    fn set(&mut self, conf: &WalAcceptorConf, timeline_id: ZTimelineId, create: bool)
        -> Result<()>;
    fn get(&self) -> &Arc<Timeline>;
    fn find_end_of_wal(&self, data_dir: &Path, precise: bool) -> (Lsn, TimeLineID);
}

impl TimelineTools for Option<Arc<Timeline>> {
    fn set(
        &mut self,
        conf: &WalAcceptorConf,
        timeline_id: ZTimelineId,
        create: bool,
    ) -> Result<()> {
        // We will only set the timeline once. If it were to ever change,
        // anyone who cloned the Arc would be out of date.
        assert!(self.is_none());
        *self = Some(GlobalTimelines::get(conf, timeline_id, create)?);
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
pub struct GlobalTimelines;

impl GlobalTimelines {
    /// Get a timeline with control file loaded from the global TIMELINES map.
    /// If control file doesn't exist and create=false, bails out.
    pub fn get(
        conf: &WalAcceptorConf,
        timeline_id: ZTimelineId,
        create: bool,
    ) -> Result<Arc<Timeline>> {
        let mut timelines = TIMELINES.lock().unwrap();

        match timelines.get(&timeline_id) {
            Some(result) => Ok(Arc::clone(result)),
            None => {
                info!("creating timeline dir {}", timeline_id);
                fs::create_dir_all(timeline_id.to_string())?;

                let shared_state = SharedState::new();

                let new_tli = Arc::new(Timeline::new(timeline_id, shared_state));
                new_tli.load_control_file(conf, create)?;
                timelines.insert(timeline_id, Arc::clone(&new_tli));
                Ok(new_tli)
            }
        }
    }
}
