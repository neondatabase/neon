//! This module implements Timeline lifecycle management and has all necessary code
//! to glue together SafeKeeper and all other background services.

use anyhow::{bail, Result};
use parking_lot::{Mutex, MutexGuard};
use postgres_ffi::XLogSegNo;
use pq_proto::ReplicationFeedback;
use std::cmp::{max, min};
use std::path::PathBuf;
use tokio::{
    sync::{mpsc::Sender, watch},
    time::Instant,
};
use tracing::*;
use utils::{
    id::{NodeId, TenantTimelineId},
    lsn::Lsn,
};

use storage_broker::proto::SafekeeperTimelineInfo;
use storage_broker::proto::TenantTimelineId as ProtoTenantTimelineId;

use crate::safekeeper::{
    AcceptorProposerMessage, ProposerAcceptorMessage, SafeKeeper, SafeKeeperState,
    SafekeeperMemState, ServerInfo, Term,
};
use crate::send_wal::HotStandbyFeedback;
use crate::{control_file, safekeeper::UNKNOWN_SERVER_VERSION};

use crate::metrics::FullTimelineInfo;
use crate::wal_storage;
use crate::wal_storage::Storage as wal_storage_iface;
use crate::SafeKeeperConf;

/// Things safekeeper should know about timeline state on peers.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub sk_id: NodeId,
    /// Term of the last entry.
    _last_log_term: Term,
    /// LSN of the last record.
    _flush_lsn: Lsn,
    pub commit_lsn: Lsn,
    /// Since which LSN safekeeper has WAL. TODO: remove this once we fill new
    /// sk since backup_lsn.
    pub local_start_lsn: Lsn,
    /// When info was received.
    ts: Instant,
}

impl PeerInfo {
    fn from_sk_info(sk_info: &SafekeeperTimelineInfo, ts: Instant) -> PeerInfo {
        PeerInfo {
            sk_id: NodeId(sk_info.safekeeper_id),
            _last_log_term: sk_info.last_log_term,
            _flush_lsn: Lsn(sk_info.flush_lsn),
            commit_lsn: Lsn(sk_info.commit_lsn),
            local_start_lsn: Lsn(sk_info.local_start_lsn),
            ts,
        }
    }
}

// vector-based node id -> peer state map with very limited functionality we
// need.
#[derive(Debug, Clone, Default)]
pub struct PeersInfo(pub Vec<PeerInfo>);

impl PeersInfo {
    fn get(&mut self, id: NodeId) -> Option<&mut PeerInfo> {
        self.0.iter_mut().find(|p| p.sk_id == id)
    }

    fn upsert(&mut self, p: &PeerInfo) {
        match self.get(p.sk_id) {
            Some(rp) => *rp = p.clone(),
            None => self.0.push(p.clone()),
        }
    }
}

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
pub struct SharedState {
    /// Safekeeper object
    sk: SafeKeeper<control_file::FileStorage, wal_storage::PhysicalStorage>,
    /// In memory list containing state of peers sent in latest messages from them.
    peers_info: PeersInfo,
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
    /// Initialize fresh timeline state without persisting anything to disk.
    fn create_new(
        conf: &SafeKeeperConf,
        ttid: &TenantTimelineId,
        state: SafeKeeperState,
    ) -> Result<Self> {
        if state.server.wal_seg_size == 0 {
            bail!(TimelineError::UninitializedWalSegSize(*ttid));
        }

        if state.server.pg_version == UNKNOWN_SERVER_VERSION {
            bail!(TimelineError::UninitialinzedPgVersion(*ttid));
        }

        if state.commit_lsn < state.local_start_lsn {
            bail!(
                "commit_lsn {} is higher than local_start_lsn {}",
                state.commit_lsn,
                state.local_start_lsn
            );
        }

        // We don't want to write anything to disk, because we may have existing timeline there.
        // These functions should not change anything on disk.
        let control_store = control_file::FileStorage::create_new(ttid, conf, state)?;
        let wal_store = wal_storage::PhysicalStorage::new(ttid, conf, &control_store)?;
        let sk = SafeKeeper::new(control_store, wal_store, conf.my_id)?;

        Ok(Self {
            sk,
            peers_info: PeersInfo(vec![]),
            replicas: vec![],
            wal_backup_active: false,
            active: false,
            num_computes: 0,
            last_removed_segno: 0,
        })
    }

    /// Restore SharedState from control file. If file doesn't exist, bails out.
    fn restore(conf: &SafeKeeperConf, ttid: &TenantTimelineId) -> Result<Self> {
        let control_store = control_file::FileStorage::restore_new(ttid, conf)?;
        if control_store.server.wal_seg_size == 0 {
            bail!(TimelineError::UninitializedWalSegSize(*ttid));
        }

        let wal_store = wal_storage::PhysicalStorage::new(ttid, conf, &control_store)?;

        Ok(Self {
            sk: SafeKeeper::new(control_store, wal_store, conf.my_id)?,
            peers_info: PeersInfo(vec![]),
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
    fn update_status(&mut self, ttid: TenantTimelineId) -> bool {
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
            // See https://github.com/neondatabase/neon/issues/1171
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
                // See https://github.com/neondatabase/neon/issues/1171
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

    fn get_safekeeper_info(
        &self,
        ttid: &TenantTimelineId,
        conf: &SafeKeeperConf,
    ) -> SafekeeperTimelineInfo {
        SafekeeperTimelineInfo {
            safekeeper_id: conf.my_id.0,
            tenant_timeline_id: Some(ProtoTenantTimelineId {
                tenant_id: ttid.tenant_id.as_ref().to_owned(),
                timeline_id: ttid.timeline_id.as_ref().to_owned(),
            }),
            last_log_term: self.sk.get_epoch(),
            flush_lsn: self.sk.wal_store.flush_lsn().0,
            // note: this value is not flushed to control file yet and can be lost
            commit_lsn: self.sk.inmem.commit_lsn.0,
            // TODO: rework feedbacks to avoid max here
            remote_consistent_lsn: max(
                self.get_replicas_state().remote_consistent_lsn,
                self.sk.inmem.remote_consistent_lsn,
            )
            .0,
            peer_horizon_lsn: self.sk.inmem.peer_horizon_lsn.0,
            safekeeper_connstr: conf.listen_pg_addr.clone(),
            backup_lsn: self.sk.inmem.backup_lsn.0,
            local_start_lsn: self.sk.state.local_start_lsn.0,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TimelineError {
    #[error("Timeline {0} was cancelled and cannot be used anymore")]
    Cancelled(TenantTimelineId),
    #[error("Timeline {0} was not found in global map")]
    NotFound(TenantTimelineId),
    #[error("Timeline {0} exists on disk, but wasn't loaded on startup")]
    Invalid(TenantTimelineId),
    #[error("Timeline {0} is already exists")]
    AlreadyExists(TenantTimelineId),
    #[error("Timeline {0} is not initialized, wal_seg_size is zero")]
    UninitializedWalSegSize(TenantTimelineId),
    #[error("Timeline {0} is not initialized, pg_version is unknown")]
    UninitialinzedPgVersion(TenantTimelineId),
}

/// Timeline struct manages lifecycle (creation, deletion, restore) of a safekeeper timeline.
/// It also holds SharedState and provides mutually exclusive access to it.
pub struct Timeline {
    pub ttid: TenantTimelineId,

    /// Sending here asks for wal backup launcher attention (start/stop
    /// offloading). Sending ttid instead of concrete command allows to do
    /// sending without timeline lock.
    pub wal_backup_launcher_tx: Sender<TenantTimelineId>,

    /// Used to broadcast commit_lsn updates to all background jobs.
    commit_lsn_watch_tx: watch::Sender<Lsn>,
    commit_lsn_watch_rx: watch::Receiver<Lsn>,

    /// Safekeeper and other state, that should remain consistent and synchronized
    /// with the disk.
    mutex: Mutex<SharedState>,

    /// Cancellation channel. Delete/cancel will send `true` here as a cancellation signal.
    cancellation_tx: watch::Sender<bool>,

    /// Timeline should not be used after cancellation. Background tasks should
    /// monitor this channel and stop eventually after receiving `true` from this channel.
    cancellation_rx: watch::Receiver<bool>,

    /// Directory where timeline state is stored.
    timeline_dir: PathBuf,
}

impl Timeline {
    /// Load existing timeline from disk.
    pub fn load_timeline(
        conf: SafeKeeperConf,
        ttid: TenantTimelineId,
        wal_backup_launcher_tx: Sender<TenantTimelineId>,
    ) -> Result<Timeline> {
        let _enter = info_span!("load_timeline", timeline = %ttid.timeline_id).entered();

        let shared_state = SharedState::restore(&conf, &ttid)?;
        let (commit_lsn_watch_tx, commit_lsn_watch_rx) =
            watch::channel(shared_state.sk.state.commit_lsn);
        let (cancellation_tx, cancellation_rx) = watch::channel(false);

        Ok(Timeline {
            ttid,
            wal_backup_launcher_tx,
            commit_lsn_watch_tx,
            commit_lsn_watch_rx,
            mutex: Mutex::new(shared_state),
            cancellation_rx,
            cancellation_tx,
            timeline_dir: conf.timeline_dir(&ttid),
        })
    }

    /// Create a new timeline, which is not yet persisted to disk.
    pub fn create_empty(
        conf: SafeKeeperConf,
        ttid: TenantTimelineId,
        wal_backup_launcher_tx: Sender<TenantTimelineId>,
        server_info: ServerInfo,
        commit_lsn: Lsn,
        local_start_lsn: Lsn,
    ) -> Result<Timeline> {
        let (commit_lsn_watch_tx, commit_lsn_watch_rx) = watch::channel(Lsn::INVALID);
        let (cancellation_tx, cancellation_rx) = watch::channel(false);
        let state = SafeKeeperState::new(&ttid, server_info, vec![], commit_lsn, local_start_lsn);

        Ok(Timeline {
            ttid,
            wal_backup_launcher_tx,
            commit_lsn_watch_tx,
            commit_lsn_watch_rx,
            mutex: Mutex::new(SharedState::create_new(&conf, &ttid, state)?),
            cancellation_rx,
            cancellation_tx,
            timeline_dir: conf.timeline_dir(&ttid),
        })
    }

    /// Initialize fresh timeline on disk and start background tasks. If bootstrap
    /// fails, timeline is cancelled and cannot be used anymore.
    ///
    /// Bootstrap is transactional, so if it fails, created files will be deleted,
    /// and state on disk should remain unchanged.
    pub fn bootstrap(&self, shared_state: &mut MutexGuard<SharedState>) -> Result<()> {
        match std::fs::metadata(&self.timeline_dir) {
            Ok(_) => {
                // Timeline directory exists on disk, we should leave state unchanged
                // and return error.
                bail!(TimelineError::Invalid(self.ttid));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e.into());
            }
        }

        // Create timeline directory.
        std::fs::create_dir_all(&self.timeline_dir)?;

        // Write timeline to disk and TODO: start background tasks.
        match || -> Result<()> {
            shared_state.sk.persist()?;
            // TODO: add more initialization steps here
            shared_state.update_status(self.ttid);
            Ok(())
        }() {
            Ok(_) => Ok(()),
            Err(e) => {
                // Bootstrap failed, cancel timeline and remove timeline directory.
                self.cancel(shared_state);

                if let Err(fs_err) = std::fs::remove_dir_all(&self.timeline_dir) {
                    warn!(
                        "failed to remove timeline {} directory after bootstrap failure: {}",
                        self.ttid, fs_err
                    );
                }

                Err(e)
            }
        }
    }

    /// Delete timeline from disk completely, by removing timeline directory. Background
    /// timeline activities will stop eventually.
    pub fn delete_from_disk(
        &self,
        shared_state: &mut MutexGuard<SharedState>,
    ) -> Result<(bool, bool)> {
        let was_active = shared_state.active;
        self.cancel(shared_state);
        let dir_existed = delete_dir(&self.timeline_dir)?;
        Ok((dir_existed, was_active))
    }

    /// Cancel timeline to prevent further usage. Background tasks will stop
    /// eventually after receiving cancellation signal.
    fn cancel(&self, shared_state: &mut MutexGuard<SharedState>) {
        info!("timeline {} is cancelled", self.ttid);
        let _ = self.cancellation_tx.send(true);
        let res = self.wal_backup_launcher_tx.blocking_send(self.ttid);
        if let Err(e) = res {
            error!("Failed to send stop signal to wal_backup_launcher: {}", e);
        }
        // Close associated FDs. Nobody will be able to touch timeline data once
        // it is cancelled, so WAL storage won't be opened again.
        shared_state.sk.wal_store.close();
    }

    /// Returns if timeline is cancelled.
    pub fn is_cancelled(&self) -> bool {
        *self.cancellation_rx.borrow()
    }

    /// Take a writing mutual exclusive lock on timeline shared_state.
    pub fn write_shared_state(&self) -> MutexGuard<SharedState> {
        self.mutex.lock()
    }

    /// Register compute connection, starting timeline-related activity if it is
    /// not running yet.
    pub async fn on_compute_connect(&self) -> Result<()> {
        if self.is_cancelled() {
            bail!(TimelineError::Cancelled(self.ttid));
        }

        let is_wal_backup_action_pending: bool;
        {
            let mut shared_state = self.write_shared_state();
            shared_state.num_computes += 1;
            is_wal_backup_action_pending = shared_state.update_status(self.ttid);
        }
        // Wake up wal backup launcher, if offloading not started yet.
        if is_wal_backup_action_pending {
            // Can fail only if channel to a static thread got closed, which is not normal at all.
            self.wal_backup_launcher_tx.send(self.ttid).await?;
        }
        Ok(())
    }

    /// De-register compute connection, shutting down timeline activity if
    /// pageserver doesn't need catchup.
    pub fn on_compute_disconnect(&self) -> Result<()> {
        let is_wal_backup_action_pending: bool;
        {
            let mut shared_state = self.write_shared_state();
            shared_state.num_computes -= 1;
            is_wal_backup_action_pending = shared_state.update_status(self.ttid);
        }
        // Wake up wal backup launcher, if it is time to stop the offloading.
        if is_wal_backup_action_pending {
            // Can fail only if channel to a static thread got closed, which is not normal at all.
            //
            // Note: this is blocking_send because on_compute_disconnect is called in Drop, there is
            // no async Drop and we use current thread runtimes. With current thread rt spawning
            // task in drop impl is racy, as thread along with runtime might finish before the task.
            // This should be switched send.await when/if we go to full async.
            self.wal_backup_launcher_tx.blocking_send(self.ttid)?;
        }
        Ok(())
    }

    /// Returns true if walsender should stop sending WAL to pageserver.
    /// TODO: check this pageserver is actually interested in this timeline.
    pub fn should_walsender_stop(&self, replica_id: usize) -> bool {
        if self.is_cancelled() {
            return true;
        }
        let mut shared_state = self.write_shared_state();
        if shared_state.num_computes == 0 {
            let replica_state = shared_state.replicas[replica_id].unwrap();
            let reported_remote_consistent_lsn = replica_state
                .pageserver_feedback
                .map(|f| Lsn(f.ps_applylsn))
                .unwrap_or(Lsn::INVALID);
            let stop = shared_state.sk.inmem.commit_lsn == Lsn(0) || // no data at all yet
            (reported_remote_consistent_lsn!= Lsn::MAX && // Lsn::MAX means that we don't know the latest LSN yet.
            reported_remote_consistent_lsn >= shared_state.sk.inmem.commit_lsn);
            if stop {
                shared_state.update_status(self.ttid);
                return true;
            }
        }
        false
    }

    /// Returns whether s3 offloading is required and sets current status as
    /// matching it.
    pub fn wal_backup_attend(&self) -> bool {
        if self.is_cancelled() {
            return false;
        }

        self.write_shared_state().wal_backup_attend()
    }

    /// Returns full timeline info, required for the metrics. If the timeline is
    /// not active, returns None instead.
    pub fn info_for_metrics(&self) -> Option<FullTimelineInfo> {
        if self.is_cancelled() {
            return None;
        }

        let state = self.write_shared_state();
        if state.active {
            Some(FullTimelineInfo {
                ttid: self.ttid,
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
                wal_storage: state.sk.wal_store.get_metrics(),
            })
        } else {
            None
        }
    }

    /// Returns commit_lsn watch channel.
    pub fn get_commit_lsn_watch_rx(&self) -> watch::Receiver<Lsn> {
        self.commit_lsn_watch_rx.clone()
    }

    /// Pass arrived message to the safekeeper.
    pub fn process_msg(
        &self,
        msg: &ProposerAcceptorMessage,
    ) -> Result<Option<AcceptorProposerMessage>> {
        if self.is_cancelled() {
            bail!(TimelineError::Cancelled(self.ttid));
        }

        let mut rmsg: Option<AcceptorProposerMessage>;
        let commit_lsn: Lsn;
        {
            let mut shared_state = self.write_shared_state();
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

    /// Returns wal_seg_size.
    pub fn get_wal_seg_size(&self) -> usize {
        self.write_shared_state().get_wal_seg_size()
    }

    /// Returns true only if the timeline is loaded and active.
    pub fn is_active(&self) -> bool {
        if self.is_cancelled() {
            return false;
        }

        self.write_shared_state().active
    }

    /// Returns state of the timeline.
    pub fn get_state(&self) -> (SafekeeperMemState, SafeKeeperState) {
        let state = self.write_shared_state();
        (state.sk.inmem.clone(), state.sk.state.clone())
    }

    /// Returns latest backup_lsn.
    pub fn get_wal_backup_lsn(&self) -> Lsn {
        self.write_shared_state().sk.inmem.backup_lsn
    }

    /// Sets backup_lsn to the given value.
    pub fn set_wal_backup_lsn(&self, backup_lsn: Lsn) -> Result<()> {
        if self.is_cancelled() {
            bail!(TimelineError::Cancelled(self.ttid));
        }

        self.write_shared_state().sk.inmem.backup_lsn = backup_lsn;
        // we should check whether to shut down offloader, but this will be done
        // soon by peer communication anyway.
        Ok(())
    }

    /// Get safekeeper info for broadcasting to broker and other peers.
    pub fn get_safekeeper_info(&self, conf: &SafeKeeperConf) -> SafekeeperTimelineInfo {
        let shared_state = self.write_shared_state();
        shared_state.get_safekeeper_info(&self.ttid, conf)
    }

    /// Update timeline state with peer safekeeper data.
    pub async fn record_safekeeper_info(&self, sk_info: &SafekeeperTimelineInfo) -> Result<()> {
        let is_wal_backup_action_pending: bool;
        let commit_lsn: Lsn;
        {
            let mut shared_state = self.write_shared_state();
            shared_state.sk.record_safekeeper_info(sk_info)?;
            let peer_info = PeerInfo::from_sk_info(sk_info, Instant::now());
            shared_state.peers_info.upsert(&peer_info);
            is_wal_backup_action_pending = shared_state.update_status(self.ttid);
            commit_lsn = shared_state.sk.inmem.commit_lsn;
        }
        self.commit_lsn_watch_tx.send(commit_lsn)?;
        // Wake up wal backup launcher, if it is time to stop the offloading.
        if is_wal_backup_action_pending {
            self.wal_backup_launcher_tx.send(self.ttid).await?;
        }
        Ok(())
    }

    /// Get our latest view of alive peers status on the timeline.
    /// We pass our own info through the broker as well, so when we don't have connection
    /// to the broker returned vec is empty.
    pub fn get_peers(&self, conf: &SafeKeeperConf) -> Vec<PeerInfo> {
        let shared_state = self.write_shared_state();
        let now = Instant::now();
        shared_state
            .peers_info
            .0
            .iter()
            // Regard peer as absent if we haven't heard from it within heartbeat_timeout.
            .filter(|p| now.duration_since(p.ts) <= conf.heartbeat_timeout)
            .cloned()
            .collect()
    }

    /// Add send_wal replica to the in-memory vector of replicas.
    pub fn add_replica(&self, state: ReplicaState) -> usize {
        self.write_shared_state().add_replica(state)
    }

    /// Update replication replica state.
    pub fn update_replica_state(&self, id: usize, state: ReplicaState) {
        let mut shared_state = self.write_shared_state();
        shared_state.replicas[id] = Some(state);
    }

    /// Remove send_wal replica from the in-memory vector of replicas.
    pub fn remove_replica(&self, id: usize) {
        let mut shared_state = self.write_shared_state();
        assert!(shared_state.replicas[id].is_some());
        shared_state.replicas[id] = None;
    }

    /// Returns flush_lsn.
    pub fn get_flush_lsn(&self) -> Lsn {
        self.write_shared_state().sk.wal_store.flush_lsn()
    }

    /// Delete WAL segments from disk that are no longer needed. This is determined
    /// based on pageserver's remote_consistent_lsn and local backup_lsn/peer_lsn.
    pub fn remove_old_wal(&self, wal_backup_enabled: bool) -> Result<()> {
        if self.is_cancelled() {
            bail!(TimelineError::Cancelled(self.ttid));
        }

        let horizon_segno: XLogSegNo;
        let remover: Box<dyn Fn(u64) -> Result<(), anyhow::Error>>;
        {
            let shared_state = self.write_shared_state();
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
        let mut shared_state = self.write_shared_state();
        shared_state.last_removed_segno = horizon_segno;
        Ok(())
    }
}

/// Deletes directory and it's contents. Returns false if directory does not exist.
fn delete_dir(path: &PathBuf) -> Result<bool> {
    match std::fs::remove_dir_all(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}
