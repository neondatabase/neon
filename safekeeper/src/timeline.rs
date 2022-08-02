//! This module implements Timeline lifecycle management and has all neccessary code
//! to glue together SafeKeeper and all other background services.

use anyhow::{anyhow, bail, Context, Result};

use etcd_broker::subscription_value::SkTimelineInfo;

use postgres_ffi::v14::xlog_utils::XLogSegNo;

use serde::Serialize;
use tokio::sync::watch;

use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs;

use parking_lot::{MappedMutexGuard, Mutex, MutexGuard};
use std::io;
use std::path::PathBuf;
use std::time::Instant;

use tokio::sync::mpsc::Sender;
use tracing::*;

use utils::{
    lsn::Lsn,
    pq_proto::ReplicationFeedback,
    zid::{NodeId, ZTenantId, ZTenantTimelineId},
};

use crate::safekeeper::{
    AcceptorProposerMessage, ProposerAcceptorMessage, SafeKeeper, SafeKeeperState,
    SafekeeperMemState, ServerInfo,
};
use crate::send_wal::HotStandbyFeedback;
use crate::{control_file, GlobalTimelines};

use crate::metrics::FullTimelineInfo;
use crate::wal_storage;
use crate::wal_storage::Storage as wal_storage_iface;
use crate::SafeKeeperConf;

/// Replica status update + hot standby feedback
#[derive(Debug, Clone, Copy)]
pub struct ReplicaState {
    /// last known lsn received by replica
    pub last_received_lsn: Lsn, // None means we don't know
    /// combined remote consistent lsn of pageservers
    pub remote_consistent_lsn: Lsn,
    /// combined hot standby feedback from all replicas
    pub hs_feedback: HotStandbyFeedback,
    /// Replication specific feedback received from pageserver, if any
    pub pageserver_feedback: Option<ReplicationFeedback>,
}

impl Default for ReplicaState {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicaState {
    pub fn new() -> ReplicaState {
        ReplicaState {
            last_received_lsn: Lsn::MAX,
            remote_consistent_lsn: Lsn(0),
            hs_feedback: HotStandbyFeedback {
                ts: 0,
                xmin: u64::MAX,
                catalog_xmin: u64::MAX,
            },
            pageserver_feedback: None,
        }
    }
}

/// Shared state associated with database instance
struct SharedState {
    /// Safekeeper object
    sk: SafeKeeper<control_file::FileStorage, wal_storage::PhysicalStorage>,
    /// State of replicas
    replicas: Vec<Option<ReplicaState>>,
    /// True when WAL backup launcher oversees the timeline, making sure WAL is
    /// offloaded, allows to bother launcher less.
    wal_backup_active: bool,
    /// True whenever there is at least some pending activity on timeline: live
    /// compute connection, pageserver is not caughtup (it must have latest WAL
    /// for new compute start) or WAL backuping is not finished. Practically it
    /// means safekeepers broadcast info to peers about the timeline, old WAL is
    /// trimmed.
    ///
    /// TODO: it might be better to remove tli completely from GlobalTimelines
    /// when tli is inactive instead of having this flag.
    active: bool,
    num_computes: u32,
    last_removed_segno: XLogSegNo,
}

impl SharedState {
    /// Initialize timeline state, create a control file on disk.
    fn create(
        conf: &SafeKeeperConf,
        zttid: &ZTenantTimelineId,
        state: SafeKeeperState,
    ) -> Result<Self> {
        if state.server.wal_seg_size == 0 {
            bail!(TimelineError::UninitializedWalSegSize(*zttid));
        }

        let control_store = control_file::FileStorage::create_new(zttid, conf, state)?;
        let wal_store = wal_storage::PhysicalStorage::new(zttid, conf, &control_store)?;
        let sk = SafeKeeper::new(control_store, wal_store, conf.my_id)?;

        Ok(Self {
            sk,
            replicas: Vec::new(),
            wal_backup_active: false,
            active: false,
            num_computes: 0,
            last_removed_segno: 0,
        })
    }

    /// Restore SharedState from control file. If file doesn't exist, bails out.
    fn restore(conf: &SafeKeeperConf, zttid: &ZTenantTimelineId) -> Result<Self> {
        let control_store = control_file::FileStorage::restore_new(zttid, conf).map_err(|e| {
            if let Some(e) = e.downcast_ref::<io::Error>() {
                if e.kind() == io::ErrorKind::NotFound {
                    return anyhow!(TimelineError::NotFound(*zttid));
                }
            }
            e
        })?;

        if control_store.server.wal_seg_size == 0 {
            bail!(TimelineError::UninitializedWalSegSize(*zttid));
        }

        let wal_store = wal_storage::PhysicalStorage::new(zttid, conf, &control_store)?;

        Ok(Self {
            sk: SafeKeeper::new(control_store, wal_store, conf.my_id)?,
            replicas: Vec::new(),
            wal_backup_active: false,
            active: false,
            num_computes: 0,
            last_removed_segno: 0,
        })
    }


    fn is_active(&self) -> bool {
        self.is_wal_backup_required()
            // FIXME: add tracking of relevant pageservers and check them here individually,
            // otherwise migration won't work (we suspend too early).
            || self.sk.inmem.remote_consistent_lsn < self.sk.inmem.commit_lsn
    }

    /// Mark timeline active/inactive and return whether s3 offloading requires
    /// start/stop action.
    fn update_status(&mut self, ttid: ZTenantTimelineId) -> bool {
        let is_active = self.is_active();
        if self.active != is_active {
            info!("timeline {} active={} now", ttid, is_active);
        }
        self.active = is_active;
        self.is_wal_backup_action_pending()
    }

    /// Should we run s3 offloading in current state?
    fn is_wal_backup_required(&self) -> bool {
        let seg_size = self.get_wal_seg_size();
        self.num_computes > 0 ||
        // Currently only the whole segment is offloaded, so compare segment numbers.
               (self.sk.inmem.commit_lsn.segment_number(seg_size) >
                self.sk.inmem.backup_lsn.segment_number(seg_size))
    }

    /// Is current state of s3 offloading is not what it ought to be?
    fn is_wal_backup_action_pending(&self) -> bool {
        let res = self.wal_backup_active != self.is_wal_backup_required();
        if res {
            let action_pending = if self.is_wal_backup_required() {
                "start"
            } else {
                "stop"
            };
            trace!(
                "timeline {} s3 offloading action {} pending: num_computes={}, commit_lsn={}, backup_lsn={}",
                self.sk.state.timeline_id, action_pending, self.num_computes, self.sk.inmem.commit_lsn, self.sk.inmem.backup_lsn
            );
        }
        res
    }

    /// Returns whether s3 offloading is required and sets current status as
    /// matching.
    fn wal_backup_attend(&mut self) -> bool {
        self.wal_backup_active = self.is_wal_backup_required();
        self.wal_backup_active
    }

    // Can this safekeeper offload to s3? Recently joined safekeepers might not
    // have necessary WAL.
    fn can_wal_backup(&self) -> bool {
        self.sk.state.local_start_lsn <= self.sk.inmem.backup_lsn
    }

    fn get_wal_seg_size(&self) -> usize {
        self.sk.state.server.wal_seg_size as usize
    }

    /// Get combined state of all alive replicas
    pub fn get_replicas_state(&self) -> ReplicaState {
        let mut acc = ReplicaState::new();
        for state in self.replicas.iter().flatten() {
            acc.hs_feedback.ts = max(acc.hs_feedback.ts, state.hs_feedback.ts);
            acc.hs_feedback.xmin = min(acc.hs_feedback.xmin, state.hs_feedback.xmin);
            acc.hs_feedback.catalog_xmin =
                min(acc.hs_feedback.catalog_xmin, state.hs_feedback.catalog_xmin);

            // FIXME
            // If multiple pageservers are streaming WAL and send feedback for the same timeline simultaneously,
            // this code is not correct.
            // Now the most advanced feedback is used.
            // If one pageserver lags when another doesn't, the backpressure won't be activated on compute and lagging
            // pageserver is prone to timeout errors.
            //
            // To choose what feedback to use and resend to compute node,
            // we need to know which pageserver compute node considers to be main.
            // See https://github.com/zenithdb/zenith/issues/1171
            //
            if let Some(pageserver_feedback) = state.pageserver_feedback {
                if let Some(acc_feedback) = acc.pageserver_feedback {
                    if acc_feedback.ps_writelsn < pageserver_feedback.ps_writelsn {
                        warn!("More than one pageserver is streaming WAL for the timeline. Feedback resolving is not fully supported yet.");
                        acc.pageserver_feedback = Some(pageserver_feedback);
                    }
                } else {
                    acc.pageserver_feedback = Some(pageserver_feedback);
                }

                // last lsn received by pageserver
                // FIXME if multiple pageservers are streaming WAL, last_received_lsn must be tracked per pageserver.
                // See https://github.com/zenithdb/zenith/issues/1171
                acc.last_received_lsn = Lsn::from(pageserver_feedback.ps_writelsn);

                // When at least one pageserver has preserved data up to remote_consistent_lsn,
                // safekeeper is free to delete it, so choose max of all pageservers.
                acc.remote_consistent_lsn = max(
                    Lsn::from(pageserver_feedback.ps_applylsn),
                    acc.remote_consistent_lsn,
                );
            }
        }
        acc
    }

    /// Assign new replica ID. We choose first empty cell in the replicas vector
    /// or extend the vector if there are no free slots.
    pub fn add_replica(&mut self, state: ReplicaState) -> usize {
        if let Some(pos) = self.replicas.iter().position(|r| r.is_none()) {
            self.replicas[pos] = Some(state);
            return pos;
        }
        let pos = self.replicas.len();
        self.replicas.push(Some(state));
        pos
    }
}

enum TimelineState {
    /// Timeline on-disk state is unknown. We either haven't tried to restore
    /// timeline state from disk or got an error.
    Uninitialized(UninitializedState),
    /// Timeline exists on disk and loaded in memory.
    Loaded(Box<SharedState>),
    /// Timeline was deleted and cannot be used anymore.
    Deleted,
}

impl TimelineState {
    fn inner_mut(&mut self) -> &mut SharedState {
        match self {
            TimelineState::Loaded(state) => state,
            _ => panic!("timeline state is not initialized"),
        }
    }
}

#[derive(Debug)]
struct UninitializedState {
    /// Safekeeper config.
    conf: Box<SafeKeeperConf>,
    /// Error that occurred on last attempt to restore timeline state from disk.
    restore_error: Option<String>,
    /// Timestamp of the last restore attempt.
    last_restore_attempt: Option<Instant>,
}

#[derive(Debug, thiserror::Error)]
pub enum TimelineError {
    #[error("Timeline {0} was deleted and cannot be used anymore")]
    Deleted(ZTenantTimelineId),
    #[error("Timeline {0} is not initialized, wal_seg_size is zero")]
    UninitializedWalSegSize(ZTenantTimelineId),
    #[error("Timeline {0} was not found on disk")]
    NotFound(ZTenantTimelineId),
}

/// Timeline struct manages lifecycle (creation, deletion, restore) of a safekeeper timeline.
/// It also holds SharedState and provides mutually exclusive access to it.
pub struct Timeline {
    pub zttid: ZTenantTimelineId,
    /// Sending here asks for wal backup launcher attention (start/stop
    /// offloading). Sending zttid instead of concrete command allows to do
    /// sending without timeline lock.
    wal_backup_launcher_tx: Sender<ZTenantTimelineId>,
    commit_lsn_watch_tx: watch::Sender<Lsn>,
    /// For breeding receivers.
    commit_lsn_watch_rx: watch::Receiver<Lsn>,
    mutex: Mutex<TimelineState>,
}

impl Timeline {
    /// Create a new uninitialized timeline. Timeline will be tried to restore from disk
    /// automatically on most function calls.
    pub fn new(
        conf: SafeKeeperConf,
        zttid: ZTenantTimelineId,
        wal_backup_launcher_tx: Sender<ZTenantTimelineId>,
    ) -> Timeline {
        let (commit_lsn_watch_tx, commit_lsn_watch_rx) = watch::channel(Lsn::INVALID);
        Timeline {
            zttid,
            wal_backup_launcher_tx,
            commit_lsn_watch_tx,
            commit_lsn_watch_rx,
            mutex: Mutex::new(TimelineState::Uninitialized(UninitializedState {
                conf: Box::new(conf),
                restore_error: None,
                last_restore_attempt: None,
            })),
        }
    }

    /// Try to restore timeline state from disk.
    fn load_from_disk(
        zttid: &ZTenantTimelineId,
        uninit: &mut UninitializedState,
    ) -> Result<SharedState> {
        info!("Restoring timeline {} from disk", zttid);

        let res = SharedState::restore(&uninit.conf, zttid);
        if let Err(e) = &res {
            uninit.restore_error = Some(format!("{}", e));
            uninit.last_restore_attempt = Some(Instant::now());
            error!("Failed to restore timeline {} from disk: {}", zttid, e);
        }
        res
    }

    /// Try to create a new timeline on disk.
    fn create_on_disk(
        zttid: &ZTenantTimelineId,
        uninit: &mut UninitializedState,
        state: SafeKeeperState,
    ) -> Result<SharedState> {
        let conf = &uninit.conf;
        info!("Creating timeline {} on disk", zttid);

        // TODO: check directory existence
        let dir = conf.timeline_dir(zttid);
        fs::create_dir_all(dir)?;

        SharedState::create(conf, zttid, state).context("failed to create shared state")
    }

    /// Initialize timeline with shared_state.
    fn set_shared_state(
        &self,
        state: SharedState,
        state_lock: &mut MutexGuard<TimelineState>,
    ) -> Result<()> {
        assert!(matches!(&**state_lock, TimelineState::Uninitialized(_)));
        self.commit_lsn_watch_tx.send(state.sk.inmem.commit_lsn)?;
        **state_lock = TimelineState::Loaded(Box::new(state));
        Ok(())
    }

    /// Require timeline state to be loaded. If it's not loaded, try to restore it from disk.
    fn require_loaded(&self) -> Result<MappedMutexGuard<SharedState>> {
        let mut state = self.mutex.lock();
        match &mut *state {
            TimelineState::Loaded(_) => {}
            TimelineState::Uninitialized(uninit) => {
                if let Some(err) = &uninit.restore_error {
                    // We have an error from last restore attempt, next attempt will not help
                    // unless something was changed on disk manually. If we will try to restore
                    // every time, we will have a lot of spam in the logs.
                    bail!(err.clone())
                }
                // TODO: we can allow restoring once a minute, to automatically recover from
                // restore failure when something was changed on disk.

                let shared_state = Self::load_from_disk(&self.zttid, uninit)?;
                self.set_shared_state(shared_state, &mut state)?;
            }
            TimelineState::Deleted => bail!(TimelineError::Deleted(self.zttid)),
        }

        Ok(MutexGuard::map(state, TimelineState::inner_mut))
    }

    /// Try to load timeline state from disk. If timeline control file is not found,
    /// create and initialize a state for the new timeline.
    pub fn init_create_or_load(&self, server_info: ServerInfo) -> Result<()> {
        let mut state_lock = self.mutex.lock();
        match &mut *state_lock {
            TimelineState::Uninitialized(uninit) => {
                let shared_state = Self::load_from_disk(&self.zttid, uninit);
                match shared_state {
                    Ok(shared_state) => {
                        // restored successfully
                        self.set_shared_state(shared_state, &mut state_lock)?;
                        Ok(())
                    }
                    Err(e) => {
                        if let Some(e) = e.downcast_ref::<TimelineError>() {
                            if matches!(e, TimelineError::NotFound(_)) {
                                // timeline does not exist on disk, create new one
                                let state = SafeKeeperState::new(&self.zttid, server_info, vec![]);
                                let shared_state =
                                    Self::create_on_disk(&self.zttid, uninit, state)?;
                                self.set_shared_state(shared_state, &mut state_lock)?;
                                return Ok(());
                            }
                        }
                        Err(e)
                    }
                }
            }
            TimelineState::Loaded(_) => Ok(()),
            TimelineState::Deleted => bail!(TimelineError::Deleted(self.zttid)),
        }
    }

    /// Deactivates and marks timeline as deleted. Returns whether the timeline was
    /// already active. The timeline can no longer be used after this call, almost
    /// all functions will return `TimelineError::Deleted`.
    ///
    /// We assume all threads will stop by themselves eventually (possibly with errors,
    /// but no panics). There should be no compute threads (as we're deleting the timeline),
    /// actually. Some WAL may be left unsent, but we're deleting the timeline anyway.
    async fn delete(&self) -> bool {
        let was_active = {
            let mut state_lock = self.mutex.lock();
            let was_active = match &*state_lock {
                TimelineState::Loaded(state) => state.active,
                TimelineState::Uninitialized(_) => false,

                // already deleted
                TimelineState::Deleted => return false,
            };

            *state_lock = TimelineState::Deleted;
            was_active
        };

        // XXX: we can have a cancellation channel to notify all tasks that they should stop

        let res = self.wal_backup_launcher_tx.send(self.zttid).await;
        if let Err(e) = res {
            error!("Failed to send stop signal to wal_backup_launcher: {}", e);
        }

        was_active
    }

    /// Register compute connection, starting timeline-related activity if it is
    /// not running yet.
    pub fn on_compute_connect(&self) -> Result<()> {
        let is_wal_backup_action_pending: bool;
        {
            let mut shared_state = self.require_loaded()?;
            shared_state.num_computes += 1;
            is_wal_backup_action_pending = shared_state.update_status(self.zttid);
        }
        // Wake up wal backup launcher, if offloading not started yet.
        if is_wal_backup_action_pending {
            self.wal_backup_launcher_tx.blocking_send(self.zttid)?;
        }
        Ok(())
    }

    /// De-register compute connection, shutting down timeline activity if
    /// pageserver doesn't need catchup.
    pub fn on_compute_disconnect(&self) -> Result<()> {
        let is_wal_backup_action_pending: bool;
        {
            let mut shared_state = self.require_loaded()?;
            shared_state.num_computes -= 1;
            is_wal_backup_action_pending = shared_state.update_status(self.zttid);
        }
        // Wake up wal backup launcher, if it is time to stop the offloading.
        if is_wal_backup_action_pending {
            self.wal_backup_launcher_tx.blocking_send(self.zttid)?;
        }
        Ok(())
    }

    /// Returns true if walsender should stop sending WAL to pageserver.
    /// TODO: check this pageserver is actually interested in this timeline.
    pub fn should_walsender_stop(&self, replica_id: usize) -> Result<bool> {
        let mut shared_state = self.require_loaded()?;
        if shared_state.num_computes == 0 {
            let replica_state = shared_state.replicas[replica_id].unwrap();
            let stop = shared_state.sk.inmem.commit_lsn == Lsn(0) || // no data at all yet
            (replica_state.remote_consistent_lsn != Lsn::MAX && // Lsn::MAX means that we don't know the latest LSN yet.
             replica_state.remote_consistent_lsn >= shared_state.sk.inmem.commit_lsn);
            if stop {
                shared_state.update_status(self.zttid);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Returns whether s3 offloading is required and sets current status as
    /// matching it.
    pub fn wal_backup_attend(&self) -> bool {
        let shared_state = self.require_loaded();
        if shared_state.is_err() {
            return false;
        }
        let mut shared_state = shared_state.unwrap();
        shared_state.wal_backup_attend()
    }

    /// Can this safekeeper offload to s3? Recently joined safekeepers might not
    /// have necessary WAL.
    pub fn can_wal_backup(&self) -> bool {
        self.require_loaded()
            .map(|state| state.can_wal_backup())
            .unwrap_or(false)
    }

    /// Returns full timeline info, required for the metrics. If the timeline is
    /// not active, returns None instead.
    pub fn info_for_metrics(&self) -> Option<FullTimelineInfo> {
        self.require_loaded()
            .map(|state| {
                if state.active {
                    Some(FullTimelineInfo {
                        zttid: self.zttid,
                        replicas: state
                            .replicas
                            .iter()
                            .filter_map(|r| r.as_ref())
                            .copied()
                            .collect(),
                        wal_backup_active: state.wal_backup_active,
                        timeline_is_active: state.active,
                        num_computes: state.num_computes,
                        last_removed_segno: state.last_removed_segno,
                        epoch_start_lsn: state.sk.epoch_start_lsn,
                        mem_state: state.sk.inmem.clone(),
                        persisted_state: state.sk.state.clone(),
                        flush_lsn: state.sk.wal_store.flush_lsn(),
                    })
                } else {
                    None
                }
            })
            .unwrap_or(None)
    }

    /// Returns commit_lsn watch channel. Channel should be obtained only after
    /// the timeline is loaded. Otherwise, the channel will have invalid LSN value.
    pub fn get_commit_lsn_watch_rx(&self) -> watch::Receiver<Lsn> {
        self.commit_lsn_watch_rx.clone()
    }

    /// Pass arrived message to the safekeeper.
    pub fn process_msg(
        &self,
        msg: &ProposerAcceptorMessage,
    ) -> Result<Option<AcceptorProposerMessage>> {
        let mut rmsg: Option<AcceptorProposerMessage>;
        let commit_lsn: Lsn;
        {
            let mut shared_state = self.require_loaded()?;
            rmsg = shared_state.sk.process_msg(msg)?;

            // if this is AppendResponse, fill in proper hot standby feedback and disk consistent lsn
            if let Some(AcceptorProposerMessage::AppendResponse(ref mut resp)) = rmsg {
                let state = shared_state.get_replicas_state();
                resp.hs_feedback = state.hs_feedback;
                if let Some(pageserver_feedback) = state.pageserver_feedback {
                    resp.pageserver_feedback = pageserver_feedback;
                }
            }

            commit_lsn = shared_state.sk.inmem.commit_lsn;
        }
        self.commit_lsn_watch_tx.send(commit_lsn)?;
        Ok(rmsg)
    }

    /// Returns wal_seg_size or None if timeline is not loaded.
    pub fn get_wal_seg_size(&self) -> Result<usize> {
        self.require_loaded().map(|state| state.get_wal_seg_size())
    }

    /// Returns true only if the timeline is loaded and active.
    pub fn is_active(&self) -> bool {
        self.require_loaded()
            .map(|state| state.active)
            .unwrap_or(false)
    }

    /// Returns state of the timeline or None if timeline is not loaded.
    pub fn get_state(&self) -> Result<(SafekeeperMemState, SafeKeeperState)> {
        self.require_loaded()
            .map(|state| (state.sk.inmem.clone(), state.sk.state.clone()))
    }

    /// Returns backup_lsn or None if timeline is not loaded.
    pub fn get_wal_backup_lsn(&self) -> Result<Lsn> {
        self.require_loaded().map(|state| state.sk.inmem.backup_lsn)
    }

    /// Sets backup_lsn to the given value.
    pub fn set_wal_backup_lsn(&self, backup_lsn: Lsn) -> Result<()> {
        let mut shared_state = self.require_loaded()?;
        shared_state.sk.inmem.backup_lsn = backup_lsn;
        // we should check whether to shut down offloader, but this will be done
        // soon by peer communication anyway.
        Ok(())
    }

    /// Return public safekeeper info for broadcasting to broker and other peers.
    pub fn get_public_info(&self, conf: &SafeKeeperConf) -> Result<SkTimelineInfo> {
        let shared_state = self.require_loaded()?;
        Ok(SkTimelineInfo {
            last_log_term: Some(shared_state.sk.get_epoch()),
            flush_lsn: Some(shared_state.sk.wal_store.flush_lsn()),
            // note: this value is not flushed to control file yet and can be lost
            commit_lsn: Some(shared_state.sk.inmem.commit_lsn),
            // TODO: rework feedbacks to avoid max here
            remote_consistent_lsn: Some(max(
                shared_state.get_replicas_state().remote_consistent_lsn,
                shared_state.sk.inmem.remote_consistent_lsn,
            )),
            peer_horizon_lsn: Some(shared_state.sk.inmem.peer_horizon_lsn),
            safekeeper_connstr: Some(conf.listen_pg_addr.clone()),
            backup_lsn: Some(shared_state.sk.inmem.backup_lsn),
        })
    }

    /// Update timeline state with peer safekeeper data.
    pub async fn record_safekeeper_info(
        &self,
        sk_info: &SkTimelineInfo,
        _sk_id: NodeId,
    ) -> Result<()> {
        let is_wal_backup_action_pending: bool;
        let commit_lsn: Lsn;
        {
            let mut shared_state = self.require_loaded()?;
            shared_state.sk.record_safekeeper_info(sk_info)?;
            is_wal_backup_action_pending = shared_state.update_status(self.zttid);
            commit_lsn = shared_state.sk.inmem.commit_lsn;
        }
        self.commit_lsn_watch_tx.send(commit_lsn)?;
        // Wake up wal backup launcher, if it is time to stop the offloading.
        if is_wal_backup_action_pending {
            self.wal_backup_launcher_tx.send(self.zttid).await?;
        }
        Ok(())
    }

    /// Add send_wal replica to the in-memory vector of replicas.
    pub fn add_replica(&self, state: ReplicaState) -> Result<usize> {
        let mut shared_state = self.require_loaded()?;
        Ok(shared_state.add_replica(state))
    }

    /// Update replication replica state.
    pub fn update_replica_state(&self, id: usize, state: ReplicaState) -> Result<()> {
        let mut shared_state = self.require_loaded()?;
        shared_state.replicas[id] = Some(state);
        Ok(())
    }

    /// Remove send_wal replica from the in-memory vector of replicas.
    pub fn remove_replica(&self, id: usize) -> Result<()> {
        let mut shared_state = self.require_loaded()?;
        assert!(shared_state.replicas[id].is_some());
        shared_state.replicas[id] = None;
        Ok(())
    }

    /// Returns flush_lsn.
    pub fn get_flush_lsn(&self) -> Result<Lsn> {
        let shared_state = self.require_loaded()?;
        Ok(shared_state.sk.wal_store.flush_lsn())
    }

    /// Delete WAL segments from disk that are no longer needed. This is determined
    /// based on pageserver's remote_consistent_lsn and local backup_lsn/peer_lsn.
    pub fn remove_old_wal(&self, wal_backup_enabled: bool) -> Result<()> {
        let horizon_segno: XLogSegNo;
        let remover: Box<dyn Fn(u64) -> Result<(), anyhow::Error>>;
        {
            let shared_state = self.require_loaded()?;
            horizon_segno = shared_state.sk.get_horizon_segno(wal_backup_enabled);
            remover = shared_state.sk.wal_store.remove_up_to();
            if horizon_segno <= 1 || horizon_segno <= shared_state.last_removed_segno {
                return Ok(());
            }
            // release the lock before removing
        }

        // delete old WAL files
        remover(horizon_segno - 1)?;

        // update last_removed_segno
        let mut shared_state = self.require_loaded()?;
        shared_state.last_removed_segno = horizon_segno;
        Ok(())
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

/// Deactivates and sets TimelineState to Deleted, see `Timeline::delete()`, then deletes the
/// corresponding data directory. We assume all timeline threads will eventually check that
/// timeline is Deleted and terminate without panics.
///
/// Timeline cannot be recreated because we keep all deleted timelines in memory. It can be
/// accidentally created again if safekeeper will restart and compute will connect to it, but this
/// is very unlikely.
pub async fn delete_force(
    conf: &SafeKeeperConf,
    zttid: &ZTenantTimelineId,
) -> Result<TimelineDeleteForceResult> {
    info!("deleting timeline {}", zttid);
    let timeline = GlobalTimelines::get(*zttid);

    let was_active = timeline.delete().await;
    Ok(TimelineDeleteForceResult {
        dir_existed: delete_dir(conf.timeline_dir(zttid))?,
        was_active,
    })
}

/// Deactivates and deletes all timelines for the tenant. Returns map of all timelines which
/// the tenant had, `true` if a timeline was active. There may be a race if new timelines are
/// created simultaneously. In that case the function will return error and the caller should
/// try to delete tenant again later.
pub async fn delete_force_all_for_tenant(
    conf: &SafeKeeperConf,
    tenant_id: &ZTenantId,
) -> Result<HashMap<ZTenantTimelineId, TimelineDeleteForceResult>> {
    info!("deleting all timelines for tenant {}", tenant_id);
    let to_delete = GlobalTimelines::get_all_for_tenant(*tenant_id);

    let mut err = None;

    let mut deleted = HashMap::new();
    for tli in &to_delete {
        let was_active = tli.delete().await;
        let res = delete_dir(conf.timeline_dir(&tli.zttid));

        match res {
            Ok(dir_existed) => {
                deleted.insert(
                    tli.zttid,
                    TimelineDeleteForceResult {
                        dir_existed,
                        was_active,
                    },
                );
            }
            Err(e) => {
                error!("failed to delete timeline {}: {}", tli.zttid, e);
                // Save error to return later.
                err = Some(e);
            }
        }
    }

    // There may be inactive timelines, so delete the whole tenant dir as well.
    delete_dir(conf.tenant_dir(tenant_id))?;

    let tlis_after_delete = GlobalTimelines::get_all_for_tenant(*tenant_id);
    if tlis_after_delete.len() != to_delete.len() {
        // Some timelines were created while we were deleting them, returning error
        // to the caller, so it can retry later.
        bail!(
            "failed to delete all timelines for tenant {}: some timelines were created while we were deleting them",
            tenant_id
        );
    }

    if let Some(e) = err {
        Err(e)
    } else {
        Ok(deleted)
    }
}
