//! This module contains timeline id -> safekeeper state map with file-backed
//! persistence and support for interaction between sending and receiving wal.

use anyhow::{bail, Context, Result};

use etcd_broker::subscription_value::SkTimelineInfo;
use lazy_static::lazy_static;
use postgres_ffi::xlog_utils::XLogSegNo;

use serde::Serialize;
use tokio::sync::watch;

use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs::{self};

use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tracing::*;

use utils::{
    lsn::Lsn,
    pq_proto::ReplicationFeedback,
    zid::{NodeId, ZTenantId, ZTenantTimelineId},
};

use crate::control_file;
use crate::safekeeper::{
    AcceptorProposerMessage, ProposerAcceptorMessage, SafeKeeper, SafeKeeperState,
    SafekeeperMemState,
};
use crate::send_wal::HotStandbyFeedback;

use crate::metrics::FullTimelineInfo;
use crate::wal_storage;
use crate::wal_storage::Storage as wal_storage_iface;
use crate::SafeKeeperConf;

const POLL_STATE_TIMEOUT: Duration = Duration::from_secs(1);

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
    /// For receiving-sending wal cooperation
    /// quorum commit LSN we've notified walsenders about
    notified_commit_lsn: Lsn,
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
    /// Initialize timeline state, creating control file
    fn create(
        conf: &SafeKeeperConf,
        zttid: &ZTenantTimelineId,
        peer_ids: Vec<NodeId>,
    ) -> Result<Self> {
        let state = SafeKeeperState::new(zttid, peer_ids);
        let control_store = control_file::FileStorage::create_new(zttid, conf, state)?;

        let wal_store = wal_storage::PhysicalStorage::new(zttid, conf);
        let sk = SafeKeeper::new(zttid.timeline_id, control_store, wal_store, conf.my_id)?;

        Ok(Self {
            notified_commit_lsn: Lsn(0),
            sk,
            replicas: Vec::new(),
            wal_backup_active: false,
            active: false,
            num_computes: 0,
            last_removed_segno: 0,
        })
    }

    /// Restore SharedState from control file.
    /// If file doesn't exist, bails out.
    fn restore(conf: &SafeKeeperConf, zttid: &ZTenantTimelineId) -> Result<Self> {
        let control_store = control_file::FileStorage::restore_new(zttid, conf)?;
        let wal_store = wal_storage::PhysicalStorage::new(zttid, conf);

        info!("timeline {} restored", zttid.timeline_id);

        Ok(Self {
            notified_commit_lsn: Lsn(0),
            sk: SafeKeeper::new(zttid.timeline_id, control_store, wal_store, conf.my_id)?,
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
            || self.sk.inmem.remote_consistent_lsn <= self.sk.inmem.commit_lsn
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

/// Database instance (tenant)
pub struct Timeline {
    pub zttid: ZTenantTimelineId,
    /// Sending here asks for wal backup launcher attention (start/stop
    /// offloading). Sending zttid instead of concrete command allows to do
    /// sending without timeline lock.
    wal_backup_launcher_tx: Sender<ZTenantTimelineId>,
    commit_lsn_watch_tx: watch::Sender<Lsn>,
    /// For breeding receivers.
    commit_lsn_watch_rx: watch::Receiver<Lsn>,
    mutex: Mutex<SharedState>,
    /// conditional variable used to notify wal senders
    cond: Condvar,
}

impl Timeline {
    fn new(
        zttid: ZTenantTimelineId,
        wal_backup_launcher_tx: Sender<ZTenantTimelineId>,
        shared_state: SharedState,
    ) -> Timeline {
        let (commit_lsn_watch_tx, commit_lsn_watch_rx) =
            watch::channel(shared_state.sk.inmem.commit_lsn);
        Timeline {
            zttid,
            wal_backup_launcher_tx,
            commit_lsn_watch_tx,
            commit_lsn_watch_rx,
            mutex: Mutex::new(shared_state),
            cond: Condvar::new(),
        }
    }

    /// Register compute connection, starting timeline-related activity if it is
    /// not running yet.
    /// Can fail only if channel to a static thread got closed, which is not normal at all.
    pub fn on_compute_connect(&self) -> Result<()> {
        let is_wal_backup_action_pending: bool;
        {
            let mut shared_state = self.mutex.lock().unwrap();
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
    /// Can fail only if channel to a static thread got closed, which is not normal at all.
    pub fn on_compute_disconnect(&self) -> Result<()> {
        let is_wal_backup_action_pending: bool;
        {
            let mut shared_state = self.mutex.lock().unwrap();
            shared_state.num_computes -= 1;
            is_wal_backup_action_pending = shared_state.update_status(self.zttid);
        }
        // Wake up wal backup launcher, if it is time to stop the offloading.
        if is_wal_backup_action_pending {
            self.wal_backup_launcher_tx.blocking_send(self.zttid)?;
        }
        Ok(())
    }

    /// Whether we still need this walsender running?
    /// TODO: check this pageserver is actually interested in this timeline.
    pub fn stop_walsender(&self, replica_id: usize) -> Result<bool> {
        let mut shared_state = self.mutex.lock().unwrap();
        if shared_state.num_computes == 0 {
            let replica_state = shared_state.replicas[replica_id].unwrap();
            let stop = shared_state.notified_commit_lsn == Lsn(0) || // no data at all yet
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
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.wal_backup_attend()
    }

    // Can this safekeeper offload to s3? Recently joined safekeepers might not
    // have necessary WAL.
    pub fn can_wal_backup(&self) -> bool {
        self.mutex.lock().unwrap().can_wal_backup()
    }

    /// Deactivates the timeline, assuming it is being deleted.
    /// Returns whether the timeline was already active.
    ///
    /// We assume all threads will stop by themselves eventually (possibly with errors, but no panics).
    /// There should be no compute threads (as we're deleting the timeline), actually. Some WAL may be left unsent, but
    /// we're deleting the timeline anyway.
    pub async fn deactivate_for_delete(&self) -> Result<bool> {
        let was_active: bool;
        {
            let shared_state = self.mutex.lock().unwrap();
            was_active = shared_state.active;
        }
        self.wal_backup_launcher_tx.send(self.zttid).await?;
        Ok(was_active)
    }

    fn is_active(&self) -> bool {
        let shared_state = self.mutex.lock().unwrap();
        shared_state.active
    }

    /// Returns full timeline info, required for the metrics.
    /// If the timeline is not active, returns None instead.
    pub fn info_for_metrics(&self) -> Option<FullTimelineInfo> {
        let shared_state = self.mutex.lock().unwrap();
        if !shared_state.active {
            return None;
        }

        Some(FullTimelineInfo {
            zttid: self.zttid,
            replicas: shared_state
                .replicas
                .iter()
                .filter_map(|r| r.as_ref())
                .copied()
                .collect(),
            wal_backup_active: shared_state.wal_backup_active,
            timeline_is_active: shared_state.active,
            num_computes: shared_state.num_computes,
            last_removed_segno: shared_state.last_removed_segno,
            epoch_start_lsn: shared_state.sk.epoch_start_lsn,
            mem_state: shared_state.sk.inmem.clone(),
            persisted_state: shared_state.sk.state.clone(),
            flush_lsn: shared_state.sk.wal_store.flush_lsn(),
        })
    }

    /// Timed wait for an LSN to be committed.
    ///
    /// Returns the last committed LSN, which will be at least
    /// as high as the LSN waited for, or None if timeout expired.
    ///
    pub fn wait_for_lsn(&self, lsn: Lsn) -> Option<Lsn> {
        let mut shared_state = self.mutex.lock().unwrap();
        loop {
            let commit_lsn = shared_state.notified_commit_lsn;
            // This must be `>`, not `>=`.
            if commit_lsn > lsn {
                return Some(commit_lsn);
            }
            let result = self
                .cond
                .wait_timeout(shared_state, POLL_STATE_TIMEOUT)
                .unwrap();
            if result.1.timed_out() {
                return None;
            }
            shared_state = result.0
        }
    }

    // Notify caught-up WAL senders about new WAL data received
    // TODO: replace-unify it with commit_lsn_watch.
    fn notify_wal_senders(&self, shared_state: &mut MutexGuard<SharedState>) {
        if shared_state.notified_commit_lsn < shared_state.sk.inmem.commit_lsn {
            shared_state.notified_commit_lsn = shared_state.sk.inmem.commit_lsn;
            self.cond.notify_all();
        }
    }

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
            let mut shared_state = self.mutex.lock().unwrap();
            rmsg = shared_state.sk.process_msg(msg)?;

            // if this is AppendResponse, fill in proper hot standby feedback and disk consistent lsn
            if let Some(AcceptorProposerMessage::AppendResponse(ref mut resp)) = rmsg {
                let state = shared_state.get_replicas_state();
                resp.hs_feedback = state.hs_feedback;
                if let Some(pageserver_feedback) = state.pageserver_feedback {
                    resp.pageserver_feedback = pageserver_feedback;
                }
            }

            // Ping wal sender that new data might be available.
            self.notify_wal_senders(&mut shared_state);
            commit_lsn = shared_state.sk.inmem.commit_lsn;
        }
        self.commit_lsn_watch_tx.send(commit_lsn)?;
        Ok(rmsg)
    }

    pub fn get_wal_seg_size(&self) -> usize {
        self.mutex.lock().unwrap().get_wal_seg_size()
    }

    pub fn get_state(&self) -> (SafekeeperMemState, SafeKeeperState) {
        let shared_state = self.mutex.lock().unwrap();
        (shared_state.sk.inmem.clone(), shared_state.sk.state.clone())
    }

    pub fn get_wal_backup_lsn(&self) -> Lsn {
        self.mutex.lock().unwrap().sk.inmem.backup_lsn
    }

    pub fn set_wal_backup_lsn(&self, backup_lsn: Lsn) {
        self.mutex.lock().unwrap().sk.inmem.backup_lsn = backup_lsn;
        // we should check whether to shut down offloader, but this will be done
        // soon by peer communication anyway.
    }

    /// Prepare public safekeeper info for reporting.
    pub fn get_public_info(&self, conf: &SafeKeeperConf) -> anyhow::Result<SkTimelineInfo> {
        let shared_state = self.mutex.lock().unwrap();
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
            let mut shared_state = self.mutex.lock().unwrap();
            // WAL seg size not initialized yet (no message from compute ever
            // received), can't do much without it.
            if shared_state.get_wal_seg_size() == 0 {
                return Ok(());
            }
            shared_state.sk.record_safekeeper_info(sk_info)?;
            self.notify_wal_senders(&mut shared_state);
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

    pub fn add_replica(&self, state: ReplicaState) -> usize {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.add_replica(state)
    }

    pub fn update_replica_state(&self, id: usize, state: ReplicaState) {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.replicas[id] = Some(state);
    }

    pub fn remove_replica(&self, id: usize) {
        let mut shared_state = self.mutex.lock().unwrap();
        assert!(shared_state.replicas[id].is_some());
        shared_state.replicas[id] = None;
    }

    pub fn get_end_of_wal(&self) -> Lsn {
        let shared_state = self.mutex.lock().unwrap();
        shared_state.sk.wal_store.flush_lsn()
    }

    pub fn remove_old_wal(&self, wal_backup_enabled: bool) -> Result<()> {
        let horizon_segno: XLogSegNo;
        let remover: Box<dyn Fn(u64) -> Result<(), anyhow::Error>>;
        {
            let shared_state = self.mutex.lock().unwrap();
            // WAL seg size not initialized yet, no WAL exists.
            if shared_state.get_wal_seg_size() == 0 {
                return Ok(());
            }
            horizon_segno = shared_state.sk.get_horizon_segno(wal_backup_enabled);
            remover = shared_state.sk.wal_store.remove_up_to();
            if horizon_segno <= 1 || horizon_segno <= shared_state.last_removed_segno {
                return Ok(());
            }
            // release the lock before removing
        }
        let _enter =
            info_span!("", timeline = %self.zttid.tenant_id, tenant = %self.zttid.timeline_id)
                .entered();
        remover(horizon_segno - 1)?;
        self.mutex.lock().unwrap().last_removed_segno = horizon_segno;
        Ok(())
    }
}

// Utilities needed by various Connection-like objects
pub trait TimelineTools {
    fn set(&mut self, conf: &SafeKeeperConf, zttid: ZTenantTimelineId, create: bool) -> Result<()>;

    fn get(&self) -> &Arc<Timeline>;
}

impl TimelineTools for Option<Arc<Timeline>> {
    fn set(&mut self, conf: &SafeKeeperConf, zttid: ZTenantTimelineId, create: bool) -> Result<()> {
        *self = Some(GlobalTimelines::get(conf, zttid, create)?);
        Ok(())
    }

    fn get(&self) -> &Arc<Timeline> {
        self.as_ref().unwrap()
    }
}

struct GlobalTimelinesState {
    timelines: HashMap<ZTenantTimelineId, Arc<Timeline>>,
    wal_backup_launcher_tx: Option<Sender<ZTenantTimelineId>>,
}

lazy_static! {
    static ref TIMELINES_STATE: Mutex<GlobalTimelinesState> = Mutex::new(GlobalTimelinesState {
        timelines: HashMap::new(),
        wal_backup_launcher_tx: None,
    });
}

#[derive(Clone, Copy, Serialize)]
pub struct TimelineDeleteForceResult {
    pub dir_existed: bool,
    pub was_active: bool,
}

/// A zero-sized struct used to manage access to the global timelines map.
pub struct GlobalTimelines;

impl GlobalTimelines {
    pub fn init(wal_backup_launcher_tx: Sender<ZTenantTimelineId>) {
        let mut state = TIMELINES_STATE.lock().unwrap();
        assert!(state.wal_backup_launcher_tx.is_none());
        state.wal_backup_launcher_tx = Some(wal_backup_launcher_tx);
    }

    fn create_internal(
        mut state: MutexGuard<GlobalTimelinesState>,
        conf: &SafeKeeperConf,
        zttid: ZTenantTimelineId,
        peer_ids: Vec<NodeId>,
    ) -> Result<Arc<Timeline>> {
        match state.timelines.get(&zttid) {
            Some(_) => bail!("timeline {} already exists", zttid),
            None => {
                // TODO: check directory existence
                let dir = conf.timeline_dir(&zttid);
                fs::create_dir_all(dir)?;

                let shared_state = SharedState::create(conf, &zttid, peer_ids)
                    .context("failed to create shared state")?;

                let new_tli = Arc::new(Timeline::new(
                    zttid,
                    state.wal_backup_launcher_tx.as_ref().unwrap().clone(),
                    shared_state,
                ));
                state.timelines.insert(zttid, Arc::clone(&new_tli));
                Ok(new_tli)
            }
        }
    }

    pub fn create(
        conf: &SafeKeeperConf,
        zttid: ZTenantTimelineId,
        peer_ids: Vec<NodeId>,
    ) -> Result<Arc<Timeline>> {
        let state = TIMELINES_STATE.lock().unwrap();
        GlobalTimelines::create_internal(state, conf, zttid, peer_ids)
    }

    /// Get a timeline with control file loaded from the global TIMELINES_STATE.timelines map.
    /// If control file doesn't exist and create=false, bails out.
    pub fn get(
        conf: &SafeKeeperConf,
        zttid: ZTenantTimelineId,
        create: bool,
    ) -> Result<Arc<Timeline>> {
        let mut state = TIMELINES_STATE.lock().unwrap();

        match state.timelines.get(&zttid) {
            Some(result) => Ok(Arc::clone(result)),
            None => {
                let shared_state = SharedState::restore(conf, &zttid);

                let shared_state = match shared_state {
                    Ok(shared_state) => shared_state,
                    Err(error) => {
                        // TODO: always create timeline explicitly
                        if error
                            .root_cause()
                            .to_string()
                            .contains("No such file or directory")
                            && create
                        {
                            return GlobalTimelines::create_internal(state, conf, zttid, vec![]);
                        } else {
                            return Err(error);
                        }
                    }
                };

                let new_tli = Arc::new(Timeline::new(
                    zttid,
                    state.wal_backup_launcher_tx.as_ref().unwrap().clone(),
                    shared_state,
                ));
                state.timelines.insert(zttid, Arc::clone(&new_tli));
                Ok(new_tli)
            }
        }
    }

    /// Get loaded timeline, if it exists.
    pub fn get_loaded(zttid: ZTenantTimelineId) -> Option<Arc<Timeline>> {
        let state = TIMELINES_STATE.lock().unwrap();
        state.timelines.get(&zttid).map(Arc::clone)
    }

    /// Get ZTenantTimelineIDs of all active timelines.
    pub fn get_active_timelines() -> Vec<ZTenantTimelineId> {
        let state = TIMELINES_STATE.lock().unwrap();
        state
            .timelines
            .iter()
            .filter(|&(_, tli)| tli.is_active())
            .map(|(zttid, _)| *zttid)
            .collect()
    }

    /// Return FullTimelineInfo for all active timelines.
    pub fn active_timelines_metrics() -> Vec<FullTimelineInfo> {
        let state = TIMELINES_STATE.lock().unwrap();
        state
            .timelines
            .iter()
            .filter_map(|(_, tli)| tli.info_for_metrics())
            .collect()
    }

    fn delete_force_internal(
        conf: &SafeKeeperConf,
        zttid: &ZTenantTimelineId,
        was_active: bool,
    ) -> Result<TimelineDeleteForceResult> {
        match std::fs::remove_dir_all(conf.timeline_dir(zttid)) {
            Ok(_) => Ok(TimelineDeleteForceResult {
                dir_existed: true,
                was_active,
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(TimelineDeleteForceResult {
                dir_existed: false,
                was_active,
            }),
            Err(e) => Err(e.into()),
        }
    }

    /// Deactivates and deletes the timeline, see `Timeline::deactivate_for_delete()`, the deletes
    /// the corresponding data directory.
    /// We assume all timeline threads do not care about `GlobalTimelines` not containing the timeline
    /// anymore, and they will eventually terminate without panics.
    ///
    /// There are multiple ways the timeline may be accidentally "re-created" (so we end up with two
    /// `Timeline` objects in memory):
    /// a) a compute node connects after this method is called, or
    /// b) an HTTP GET request about the timeline is made and it's able to restore the current state, or
    /// c) an HTTP POST request for timeline creation is made after the timeline is already deleted.
    /// TODO: ensure all of the above never happens.
    pub async fn delete_force(
        conf: &SafeKeeperConf,
        zttid: &ZTenantTimelineId,
    ) -> Result<TimelineDeleteForceResult> {
        info!("deleting timeline {}", zttid);
        let timeline = TIMELINES_STATE.lock().unwrap().timelines.remove(zttid);
        let mut was_active = false;
        if let Some(tli) = timeline {
            was_active = tli.deactivate_for_delete().await?;
        }
        GlobalTimelines::delete_force_internal(conf, zttid, was_active)
    }

    /// Deactivates and deletes all timelines for the tenant, see `delete()`.
    /// Returns map of all timelines which the tenant had, `true` if a timeline was active.
    /// There may be a race if new timelines are created simultaneously.
    pub async fn delete_force_all_for_tenant(
        conf: &SafeKeeperConf,
        tenant_id: &ZTenantId,
    ) -> Result<HashMap<ZTenantTimelineId, TimelineDeleteForceResult>> {
        info!("deleting all timelines for tenant {}", tenant_id);
        let mut to_delete = HashMap::new();
        {
            // Keep mutex in this scope.
            let timelines = &mut TIMELINES_STATE.lock().unwrap().timelines;
            for (&zttid, tli) in timelines.iter() {
                if zttid.tenant_id == *tenant_id {
                    to_delete.insert(zttid, tli.clone());
                }
            }
            // TODO: test that the correct subset of timelines is removed. It's complicated because they are implicitly created currently.
            timelines.retain(|zttid, _| !to_delete.contains_key(zttid));
        }
        let mut deleted = HashMap::new();
        for (zttid, timeline) in to_delete {
            let was_active = timeline.deactivate_for_delete().await?;
            deleted.insert(
                zttid,
                GlobalTimelines::delete_force_internal(conf, &zttid, was_active)?,
            );
        }
        // There may be inactive timelines, so delete the whole tenant dir as well.
        match std::fs::remove_dir_all(conf.tenant_dir(tenant_id)) {
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            e => e?,
        };
        Ok(deleted)
    }
}
