//! This module implements Timeline lifecycle management and has all necessary code
//! to glue together SafeKeeper and all other background services.

use anyhow::{anyhow, bail, Result};
use camino::Utf8PathBuf;
use postgres_ffi::XLogSegNo;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio_util::sync::CancellationToken;

use std::cmp::max;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::{sync::watch, time::Instant};
use tracing::*;
use utils::http::error::ApiError;
use utils::{
    id::{NodeId, TenantTimelineId},
    lsn::Lsn,
};

use storage_broker::proto::SafekeeperTimelineInfo;
use storage_broker::proto::TenantTimelineId as ProtoTenantTimelineId;

use crate::receive_wal::WalReceivers;
use crate::recovery::{recovery_main, Donor, RecoveryNeededInfo};
use crate::safekeeper::{
    AcceptorProposerMessage, ProposerAcceptorMessage, SafeKeeper, ServerInfo, Term, TermLsn,
    INVALID_TERM,
};
use crate::send_wal::WalSenders;
use crate::state::{TimelineMemState, TimelinePersistentState};
use crate::timelines_set::TimelinesSet;
use crate::wal_backup::{self};
use crate::{control_file, safekeeper::UNKNOWN_SERVER_VERSION};

use crate::metrics::FullTimelineInfo;
use crate::wal_storage::Storage as wal_storage_iface;
use crate::{debug_dump, timeline_manager, wal_backup_partial, wal_storage};
use crate::{GlobalTimelines, SafeKeeperConf};

/// Things safekeeper should know about timeline state on peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub sk_id: NodeId,
    pub term: Term,
    /// Term of the last entry.
    pub last_log_term: Term,
    /// LSN of the last record.
    pub flush_lsn: Lsn,
    pub commit_lsn: Lsn,
    /// Since which LSN safekeeper has WAL.
    pub local_start_lsn: Lsn,
    /// When info was received. Serde annotations are not very useful but make
    /// the code compile -- we don't rely on this field externally.
    #[serde(skip)]
    #[serde(default = "Instant::now")]
    ts: Instant,
    pub pg_connstr: String,
    pub http_connstr: String,
}

impl PeerInfo {
    fn from_sk_info(sk_info: &SafekeeperTimelineInfo, ts: Instant) -> PeerInfo {
        PeerInfo {
            sk_id: NodeId(sk_info.safekeeper_id),
            term: sk_info.term,
            last_log_term: sk_info.last_log_term,
            flush_lsn: Lsn(sk_info.flush_lsn),
            commit_lsn: Lsn(sk_info.commit_lsn),
            local_start_lsn: Lsn(sk_info.local_start_lsn),
            pg_connstr: sk_info.safekeeper_connstr.clone(),
            http_connstr: sk_info.http_connstr.clone(),
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

pub type ReadGuardSharedState<'a> = RwLockReadGuard<'a, SharedState>;

/// WriteGuardSharedState is a wrapper around `RwLockWriteGuard<SharedState>` that
/// automatically updates `watch::Sender` channels with state on drop.
pub struct WriteGuardSharedState<'a> {
    tli: Arc<Timeline>,
    guard: RwLockWriteGuard<'a, SharedState>,
}

impl<'a> WriteGuardSharedState<'a> {
    fn new(tli: Arc<Timeline>, guard: RwLockWriteGuard<'a, SharedState>) -> Self {
        WriteGuardSharedState { tli, guard }
    }
}

impl<'a> Deref for WriteGuardSharedState<'a> {
    type Target = SharedState;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<'a> DerefMut for WriteGuardSharedState<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<'a> Drop for WriteGuardSharedState<'a> {
    fn drop(&mut self) {
        let term_flush_lsn = TermLsn::from((self.guard.sk.get_term(), self.guard.sk.flush_lsn()));
        let commit_lsn = self.guard.sk.state.inmem.commit_lsn;

        let _ = self.tli.term_flush_lsn_watch_tx.send_if_modified(|old| {
            if *old != term_flush_lsn {
                *old = term_flush_lsn;
                true
            } else {
                false
            }
        });

        let _ = self.tli.commit_lsn_watch_tx.send_if_modified(|old| {
            if *old != commit_lsn {
                *old = commit_lsn;
                true
            } else {
                false
            }
        });

        // send notification about shared state update
        self.tli.shared_state_version_tx.send_modify(|old| {
            *old += 1;
        });
    }
}

/// Shared state associated with database instance
pub struct SharedState {
    /// Safekeeper object
    pub(crate) sk: SafeKeeper<control_file::FileStorage, wal_storage::PhysicalStorage>,
    /// In memory list containing state of peers sent in latest messages from them.
    pub(crate) peers_info: PeersInfo,
    pub(crate) last_removed_segno: XLogSegNo,
}

impl SharedState {
    /// Initialize fresh timeline state without persisting anything to disk.
    fn create_new(
        conf: &SafeKeeperConf,
        ttid: &TenantTimelineId,
        state: TimelinePersistentState,
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
        let timeline_dir = conf.timeline_dir(ttid);
        let control_store = control_file::FileStorage::create_new(timeline_dir, conf, state)?;
        let wal_store =
            wal_storage::PhysicalStorage::new(ttid, conf.timeline_dir(ttid), conf, &control_store)?;
        let sk = SafeKeeper::new(control_store, wal_store, conf.my_id)?;

        Ok(Self {
            sk,
            peers_info: PeersInfo(vec![]),
            last_removed_segno: 0,
        })
    }

    /// Restore SharedState from control file. If file doesn't exist, bails out.
    fn restore(conf: &SafeKeeperConf, ttid: &TenantTimelineId) -> Result<Self> {
        let control_store = control_file::FileStorage::restore_new(ttid, conf)?;
        if control_store.server.wal_seg_size == 0 {
            bail!(TimelineError::UninitializedWalSegSize(*ttid));
        }

        let wal_store =
            wal_storage::PhysicalStorage::new(ttid, conf.timeline_dir(ttid), conf, &control_store)?;

        Ok(Self {
            sk: SafeKeeper::new(control_store, wal_store, conf.my_id)?,
            peers_info: PeersInfo(vec![]),
            last_removed_segno: 0,
        })
    }

    fn get_wal_seg_size(&self) -> usize {
        self.sk.state.server.wal_seg_size as usize
    }

    fn get_safekeeper_info(
        &self,
        ttid: &TenantTimelineId,
        conf: &SafeKeeperConf,
        standby_apply_lsn: Lsn,
    ) -> SafekeeperTimelineInfo {
        SafekeeperTimelineInfo {
            safekeeper_id: conf.my_id.0,
            tenant_timeline_id: Some(ProtoTenantTimelineId {
                tenant_id: ttid.tenant_id.as_ref().to_owned(),
                timeline_id: ttid.timeline_id.as_ref().to_owned(),
            }),
            term: self.sk.state.acceptor_state.term,
            last_log_term: self.sk.get_epoch(),
            flush_lsn: self.sk.flush_lsn().0,
            // note: this value is not flushed to control file yet and can be lost
            commit_lsn: self.sk.state.inmem.commit_lsn.0,
            remote_consistent_lsn: self.sk.state.inmem.remote_consistent_lsn.0,
            peer_horizon_lsn: self.sk.state.inmem.peer_horizon_lsn.0,
            safekeeper_connstr: conf
                .advertise_pg_addr
                .to_owned()
                .unwrap_or(conf.listen_pg_addr.clone()),
            http_connstr: conf.listen_http_addr.to_owned(),
            backup_lsn: self.sk.state.inmem.backup_lsn.0,
            local_start_lsn: self.sk.state.local_start_lsn.0,
            availability_zone: conf.availability_zone.clone(),
            standby_horizon: standby_apply_lsn.0,
        }
    }

    /// Get our latest view of alive peers status on the timeline.
    /// We pass our own info through the broker as well, so when we don't have connection
    /// to the broker returned vec is empty.
    pub(crate) fn get_peers(&self, heartbeat_timeout: Duration) -> Vec<PeerInfo> {
        let now = Instant::now();
        self.peers_info
            .0
            .iter()
            // Regard peer as absent if we haven't heard from it within heartbeat_timeout.
            .filter(|p| now.duration_since(p.ts) <= heartbeat_timeout)
            .cloned()
            .collect()
    }

    /// Get oldest segno we still need to keep. We hold WAL till it is consumed
    /// by all of 1) pageserver (remote_consistent_lsn) 2) peers 3) s3
    /// offloading.
    /// While it is safe to use inmem values for determining horizon,
    /// we use persistent to make possible normal states less surprising.
    fn get_horizon_segno(&self, extra_horizon_lsn: Option<Lsn>) -> XLogSegNo {
        let state = &self.sk.state;

        use std::cmp::min;
        let mut horizon_lsn = min(state.remote_consistent_lsn, state.peer_horizon_lsn);
        // we don't want to remove WAL that is not yet offloaded to s3
        horizon_lsn = min(horizon_lsn, state.backup_lsn);
        if let Some(extra_horizon_lsn) = extra_horizon_lsn {
            horizon_lsn = min(horizon_lsn, extra_horizon_lsn);
        }
        horizon_lsn.segment_number(state.server.wal_seg_size as usize)
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

// Convert to HTTP API error.
impl From<TimelineError> for ApiError {
    fn from(te: TimelineError) -> ApiError {
        match te {
            TimelineError::NotFound(ttid) => {
                ApiError::NotFound(anyhow!("timeline {} not found", ttid).into())
            }
            _ => ApiError::InternalServerError(anyhow!("{}", te)),
        }
    }
}

/// Timeline struct manages lifecycle (creation, deletion, restore) of a safekeeper timeline.
/// It also holds SharedState and provides mutually exclusive access to it.
pub struct Timeline {
    pub ttid: TenantTimelineId,

    /// Used to broadcast commit_lsn updates to all background jobs.
    commit_lsn_watch_tx: watch::Sender<Lsn>,
    commit_lsn_watch_rx: watch::Receiver<Lsn>,

    /// Broadcasts (current term, flush_lsn) updates, walsender is interested in
    /// them when sending in recovery mode (to walproposer or peers). Note: this
    /// is just a notification, WAL reading should always done with lock held as
    /// term can change otherwise.
    term_flush_lsn_watch_tx: watch::Sender<TermLsn>,
    term_flush_lsn_watch_rx: watch::Receiver<TermLsn>,

    /// Broadcasts shared state updates.
    shared_state_version_tx: watch::Sender<usize>,
    shared_state_version_rx: watch::Receiver<usize>,

    /// Safekeeper and other state, that should remain consistent and
    /// synchronized with the disk. This is tokio mutex as we write WAL to disk
    /// while holding it, ensuring that consensus checks are in order.
    mutex: RwLock<SharedState>,
    walsenders: Arc<WalSenders>,
    walreceivers: Arc<WalReceivers>,

    /// Delete/cancel will trigger this, background tasks should drop out as soon as it fires
    pub(crate) cancel: CancellationToken,

    /// Directory where timeline state is stored.
    pub timeline_dir: Utf8PathBuf,

    /// Should we keep WAL on disk for active replication connections.
    /// Especially useful for sharding, when different shards process WAL
    /// with different speed.
    // TODO: add `Arc<SafeKeeperConf>` here instead of adding each field separately.
    walsenders_keep_horizon: bool,

    // timeline_manager controlled state
    pub(crate) broker_active: AtomicBool,
    pub(crate) wal_backup_active: AtomicBool,
}

impl Timeline {
    /// Load existing timeline from disk.
    pub fn load_timeline(conf: &SafeKeeperConf, ttid: TenantTimelineId) -> Result<Timeline> {
        let _enter = info_span!("load_timeline", timeline = %ttid.timeline_id).entered();

        let shared_state = SharedState::restore(conf, &ttid)?;
        let (commit_lsn_watch_tx, commit_lsn_watch_rx) =
            watch::channel(shared_state.sk.state.commit_lsn);
        let (term_flush_lsn_watch_tx, term_flush_lsn_watch_rx) = watch::channel(TermLsn::from((
            shared_state.sk.get_term(),
            shared_state.sk.flush_lsn(),
        )));
        let (shared_state_version_tx, shared_state_version_rx) = watch::channel(0);

        let walreceivers = WalReceivers::new();
        Ok(Timeline {
            ttid,
            commit_lsn_watch_tx,
            commit_lsn_watch_rx,
            term_flush_lsn_watch_tx,
            term_flush_lsn_watch_rx,
            shared_state_version_tx,
            shared_state_version_rx,
            mutex: RwLock::new(shared_state),
            walsenders: WalSenders::new(walreceivers.clone()),
            walreceivers,
            cancel: CancellationToken::default(),
            timeline_dir: conf.timeline_dir(&ttid),
            walsenders_keep_horizon: conf.walsenders_keep_horizon,
            broker_active: AtomicBool::new(false),
            wal_backup_active: AtomicBool::new(false),
        })
    }

    /// Create a new timeline, which is not yet persisted to disk.
    pub fn create_empty(
        conf: &SafeKeeperConf,
        ttid: TenantTimelineId,
        server_info: ServerInfo,
        commit_lsn: Lsn,
        local_start_lsn: Lsn,
    ) -> Result<Timeline> {
        let (commit_lsn_watch_tx, commit_lsn_watch_rx) = watch::channel(Lsn::INVALID);
        let (term_flush_lsn_watch_tx, term_flush_lsn_watch_rx) =
            watch::channel(TermLsn::from((INVALID_TERM, Lsn::INVALID)));
        let (shared_state_version_tx, shared_state_version_rx) = watch::channel(0);

        let state =
            TimelinePersistentState::new(&ttid, server_info, vec![], commit_lsn, local_start_lsn);

        let walreceivers = WalReceivers::new();
        Ok(Timeline {
            ttid,
            commit_lsn_watch_tx,
            commit_lsn_watch_rx,
            term_flush_lsn_watch_tx,
            term_flush_lsn_watch_rx,
            shared_state_version_tx,
            shared_state_version_rx,
            mutex: RwLock::new(SharedState::create_new(conf, &ttid, state)?),
            walsenders: WalSenders::new(walreceivers.clone()),
            walreceivers,
            cancel: CancellationToken::default(),
            timeline_dir: conf.timeline_dir(&ttid),
            walsenders_keep_horizon: conf.walsenders_keep_horizon,
            broker_active: AtomicBool::new(false),
            wal_backup_active: AtomicBool::new(false),
        })
    }

    /// Initialize fresh timeline on disk and start background tasks. If init
    /// fails, timeline is cancelled and cannot be used anymore.
    ///
    /// Init is transactional, so if it fails, created files will be deleted,
    /// and state on disk should remain unchanged.
    pub async fn init_new(
        self: &Arc<Timeline>,
        shared_state: &mut WriteGuardSharedState<'_>,
        conf: &SafeKeeperConf,
        broker_active_set: Arc<TimelinesSet>,
    ) -> Result<()> {
        match fs::metadata(&self.timeline_dir).await {
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
        fs::create_dir_all(&self.timeline_dir).await?;

        // Write timeline to disk and start background tasks.
        if let Err(e) = shared_state.sk.state.flush().await {
            // Bootstrap failed, cancel timeline and remove timeline directory.
            self.cancel(shared_state);

            if let Err(fs_err) = fs::remove_dir_all(&self.timeline_dir).await {
                warn!(
                    "failed to remove timeline {} directory after bootstrap failure: {}",
                    self.ttid, fs_err
                );
            }

            return Err(e);
        }
        self.bootstrap(conf, broker_active_set);
        Ok(())
    }

    /// Bootstrap new or existing timeline starting background tasks.
    pub fn bootstrap(
        self: &Arc<Timeline>,
        conf: &SafeKeeperConf,
        broker_active_set: Arc<TimelinesSet>,
    ) {
        // Start manager task which will monitor timeline state and update
        // background tasks.
        tokio::spawn(timeline_manager::main_task(
            self.clone(),
            conf.clone(),
            broker_active_set,
        ));

        // Start recovery task which always runs on the timeline.
        if conf.peer_recovery_enabled {
            tokio::spawn(recovery_main(self.clone(), conf.clone()));
        }
        // TODO: migrate to timeline_manager
        if conf.is_wal_backup_enabled() && conf.partial_backup_enabled {
            tokio::spawn(wal_backup_partial::main_task(self.clone(), conf.clone()));
        }
    }

    /// Delete timeline from disk completely, by removing timeline directory.
    /// Background timeline activities will stop eventually.
    ///
    /// Also deletes WAL in s3. Might fail if e.g. s3 is unavailable, but
    /// deletion API endpoint is retriable.
    pub async fn delete(
        &self,
        shared_state: &mut WriteGuardSharedState<'_>,
        only_local: bool,
    ) -> Result<bool> {
        self.cancel(shared_state);

        // TODO: It's better to wait for s3 offloader termination before
        // removing data from s3. Though since s3 doesn't have transactions it
        // still wouldn't guarantee absense of data after removal.
        let conf = GlobalTimelines::get_global_config();
        if !only_local && conf.is_wal_backup_enabled() {
            // Note: we concurrently delete remote storage data from multiple
            // safekeepers. That's ok, s3 replies 200 if object doesn't exist and we
            // do some retries anyway.
            wal_backup::delete_timeline(&self.ttid).await?;
        }
        let dir_existed = delete_dir(&self.timeline_dir).await?;
        Ok(dir_existed)
    }

    /// Cancel timeline to prevent further usage. Background tasks will stop
    /// eventually after receiving cancellation signal.
    fn cancel(&self, shared_state: &mut WriteGuardSharedState<'_>) {
        info!("timeline {} is cancelled", self.ttid);
        self.cancel.cancel();
        // Close associated FDs. Nobody will be able to touch timeline data once
        // it is cancelled, so WAL storage won't be opened again.
        shared_state.sk.wal_store.close();
    }

    /// Returns if timeline is cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Take a writing mutual exclusive lock on timeline shared_state.
    pub async fn write_shared_state<'a>(self: &'a Arc<Self>) -> WriteGuardSharedState<'a> {
        WriteGuardSharedState::new(self.clone(), self.mutex.write().await)
    }

    pub async fn read_shared_state(&self) -> ReadGuardSharedState {
        self.mutex.read().await
    }

    /// Returns true if walsender should stop sending WAL to pageserver. We
    /// terminate it if remote_consistent_lsn reached commit_lsn and there is no
    /// computes. While there might be nothing to stream already, we learn about
    /// remote_consistent_lsn update through replication feedback, and we want
    /// to stop pushing to the broker if pageserver is fully caughtup.
    pub async fn should_walsender_stop(&self, reported_remote_consistent_lsn: Lsn) -> bool {
        if self.is_cancelled() {
            return true;
        }
        let shared_state = self.read_shared_state().await;
        if self.walreceivers.get_num() == 0 {
            return shared_state.sk.state.inmem.commit_lsn == Lsn(0) || // no data at all yet
            reported_remote_consistent_lsn >= shared_state.sk.state.inmem.commit_lsn;
        }
        false
    }

    /// Ensure that current term is t, erroring otherwise, and lock the state.
    pub async fn acquire_term(&self, t: Term) -> Result<ReadGuardSharedState> {
        let ss = self.read_shared_state().await;
        if ss.sk.state.acceptor_state.term != t {
            bail!(
                "failed to acquire term {}, current term {}",
                t,
                ss.sk.state.acceptor_state.term
            );
        }
        Ok(ss)
    }

    /// Returns commit_lsn watch channel.
    pub fn get_commit_lsn_watch_rx(&self) -> watch::Receiver<Lsn> {
        self.commit_lsn_watch_rx.clone()
    }

    /// Returns term_flush_lsn watch channel.
    pub fn get_term_flush_lsn_watch_rx(&self) -> watch::Receiver<TermLsn> {
        self.term_flush_lsn_watch_rx.clone()
    }

    /// Returns watch channel for SharedState update version.
    pub fn get_state_version_rx(&self) -> watch::Receiver<usize> {
        self.shared_state_version_rx.clone()
    }

    /// Pass arrived message to the safekeeper.
    pub async fn process_msg(
        self: &Arc<Self>,
        msg: &ProposerAcceptorMessage,
    ) -> Result<Option<AcceptorProposerMessage>> {
        if self.is_cancelled() {
            bail!(TimelineError::Cancelled(self.ttid));
        }

        let mut rmsg: Option<AcceptorProposerMessage>;
        {
            let mut shared_state = self.write_shared_state().await;
            rmsg = shared_state.sk.process_msg(msg).await?;

            // if this is AppendResponse, fill in proper hot standby feedback.
            if let Some(AcceptorProposerMessage::AppendResponse(ref mut resp)) = rmsg {
                resp.hs_feedback = self.walsenders.get_hotstandby().hs_feedback;
            }
        }
        Ok(rmsg)
    }

    /// Returns wal_seg_size.
    pub async fn get_wal_seg_size(&self) -> usize {
        self.read_shared_state().await.get_wal_seg_size()
    }

    /// Returns state of the timeline.
    pub async fn get_state(&self) -> (TimelineMemState, TimelinePersistentState) {
        let state = self.read_shared_state().await;
        (state.sk.state.inmem.clone(), state.sk.state.clone())
    }

    /// Returns latest backup_lsn.
    pub async fn get_wal_backup_lsn(&self) -> Lsn {
        self.read_shared_state().await.sk.state.inmem.backup_lsn
    }

    /// Sets backup_lsn to the given value.
    pub async fn set_wal_backup_lsn(self: &Arc<Self>, backup_lsn: Lsn) -> Result<()> {
        if self.is_cancelled() {
            bail!(TimelineError::Cancelled(self.ttid));
        }

        let mut state = self.write_shared_state().await;
        state.sk.state.inmem.backup_lsn = max(state.sk.state.inmem.backup_lsn, backup_lsn);
        // we should check whether to shut down offloader, but this will be done
        // soon by peer communication anyway.
        Ok(())
    }

    /// Get safekeeper info for broadcasting to broker and other peers.
    pub async fn get_safekeeper_info(&self, conf: &SafeKeeperConf) -> SafekeeperTimelineInfo {
        let standby_apply_lsn = self.walsenders.get_hotstandby().reply.apply_lsn;
        let shared_state = self.read_shared_state().await;
        shared_state.get_safekeeper_info(&self.ttid, conf, standby_apply_lsn)
    }

    /// Update timeline state with peer safekeeper data.
    pub async fn record_safekeeper_info(
        self: &Arc<Self>,
        sk_info: SafekeeperTimelineInfo,
    ) -> Result<()> {
        {
            let mut shared_state = self.write_shared_state().await;
            shared_state.sk.record_safekeeper_info(&sk_info).await?;
            let peer_info = PeerInfo::from_sk_info(&sk_info, Instant::now());
            shared_state.peers_info.upsert(&peer_info);
        }
        Ok(())
    }

    /// Update in memory remote consistent lsn.
    pub async fn update_remote_consistent_lsn(self: &Arc<Self>, candidate: Lsn) {
        let mut shared_state = self.write_shared_state().await;
        shared_state.sk.state.inmem.remote_consistent_lsn =
            max(shared_state.sk.state.inmem.remote_consistent_lsn, candidate);
    }

    pub async fn get_peers(&self, conf: &SafeKeeperConf) -> Vec<PeerInfo> {
        let shared_state = self.read_shared_state().await;
        shared_state.get_peers(conf.heartbeat_timeout)
    }

    /// Should we start fetching WAL from a peer safekeeper, and if yes, from
    /// which? Answer is yes, i.e. .donors is not empty if 1) there is something
    /// to fetch, and we can do that without running elections; 2) there is no
    /// actively streaming compute, as we don't want to compete with it.
    ///
    /// If donor(s) are choosen, theirs last_log_term is guaranteed to be equal
    /// to its last_log_term so we are sure such a leader ever had been elected.
    ///
    /// All possible donors are returned so that we could keep connection to the
    /// current one if it is good even if it slightly lags behind.
    ///
    /// Note that term conditions above might be not met, but safekeepers are
    /// still not aligned on last flush_lsn. Generally in this case until
    /// elections are run it is not possible to say which safekeeper should
    /// recover from which one -- history which would be committed is different
    /// depending on assembled quorum (e.g. classic picture 8 from Raft paper).
    /// Thus we don't try to predict it here.
    pub async fn recovery_needed(&self, heartbeat_timeout: Duration) -> RecoveryNeededInfo {
        let ss = self.read_shared_state().await;
        let term = ss.sk.state.acceptor_state.term;
        let last_log_term = ss.sk.get_epoch();
        let flush_lsn = ss.sk.flush_lsn();
        // note that peers contain myself, but that's ok -- we are interested only in peers which are strictly ahead of us.
        let mut peers = ss.get_peers(heartbeat_timeout);
        // Sort by <last log term, lsn> pairs.
        peers.sort_by(|p1, p2| {
            let tl1 = TermLsn {
                term: p1.last_log_term,
                lsn: p1.flush_lsn,
            };
            let tl2 = TermLsn {
                term: p2.last_log_term,
                lsn: p2.flush_lsn,
            };
            tl2.cmp(&tl1) // desc
        });
        let num_streaming_computes = self.walreceivers.get_num_streaming();
        let donors = if num_streaming_computes > 0 {
            vec![] // If there is a streaming compute, don't try to recover to not intervene.
        } else {
            peers
                .iter()
                .filter_map(|candidate| {
                    // Are we interested in this candidate?
                    let candidate_tl = TermLsn {
                        term: candidate.last_log_term,
                        lsn: candidate.flush_lsn,
                    };
                    let my_tl = TermLsn {
                        term: last_log_term,
                        lsn: flush_lsn,
                    };
                    if my_tl < candidate_tl {
                        // Yes, we are interested. Can we pull from it without
                        // (re)running elections? It is possible if 1) his term
                        // is equal to his last_log_term so we could act on
                        // behalf of leader of this term (we must be sure he was
                        // ever elected) and 2) our term is not higher, or we'll refuse data.
                        if candidate.term == candidate.last_log_term && candidate.term >= term {
                            Some(Donor::from(candidate))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect()
        };
        RecoveryNeededInfo {
            term,
            last_log_term,
            flush_lsn,
            peers,
            num_streaming_computes,
            donors,
        }
    }

    pub fn get_walsenders(&self) -> &Arc<WalSenders> {
        &self.walsenders
    }

    pub fn get_walreceivers(&self) -> &Arc<WalReceivers> {
        &self.walreceivers
    }

    /// Returns flush_lsn.
    pub async fn get_flush_lsn(&self) -> Lsn {
        self.read_shared_state().await.sk.wal_store.flush_lsn()
    }

    /// Delete WAL segments from disk that are no longer needed. This is determined
    /// based on pageserver's remote_consistent_lsn and local backup_lsn/peer_lsn.
    pub async fn remove_old_wal(self: &Arc<Self>) -> Result<()> {
        if self.is_cancelled() {
            bail!(TimelineError::Cancelled(self.ttid));
        }

        // If enabled, we use LSN of the most lagging walsender as a WAL removal horizon.
        // This allows to get better read speed for pageservers that are lagging behind,
        // at the cost of keeping more WAL on disk.
        let replication_horizon_lsn = if self.walsenders_keep_horizon {
            self.walsenders.laggard_lsn()
        } else {
            None
        };

        let horizon_segno: XLogSegNo;
        let remover = {
            let shared_state = self.read_shared_state().await;
            horizon_segno = shared_state.get_horizon_segno(replication_horizon_lsn);
            if horizon_segno <= 1 || horizon_segno <= shared_state.last_removed_segno {
                return Ok(()); // nothing to do
            }

            // release the lock before removing
            shared_state.sk.wal_store.remove_up_to(horizon_segno - 1)
        };

        // delete old WAL files
        remover.await?;

        // update last_removed_segno
        let mut shared_state = self.write_shared_state().await;
        shared_state.last_removed_segno = horizon_segno;
        Ok(())
    }

    /// Persist control file if there is something to save and enough time
    /// passed after the last save. This helps to keep remote_consistent_lsn up
    /// to date so that storage nodes restart doesn't cause many pageserver ->
    /// safekeeper reconnections.
    pub async fn maybe_persist_control_file(self: &Arc<Self>) -> Result<()> {
        self.write_shared_state()
            .await
            .sk
            .maybe_persist_inmem_control_file()
            .await
    }

    /// Gather timeline data for metrics.
    pub async fn info_for_metrics(&self) -> Option<FullTimelineInfo> {
        if self.is_cancelled() {
            return None;
        }

        let (ps_feedback_count, last_ps_feedback) = self.walsenders.get_ps_feedback_stats();
        let state = self.read_shared_state().await;
        Some(FullTimelineInfo {
            ttid: self.ttid,
            ps_feedback_count,
            last_ps_feedback,
            wal_backup_active: self.wal_backup_active.load(Ordering::Relaxed),
            timeline_is_active: self.broker_active.load(Ordering::Relaxed),
            num_computes: self.walreceivers.get_num() as u32,
            last_removed_segno: state.last_removed_segno,
            epoch_start_lsn: state.sk.epoch_start_lsn,
            mem_state: state.sk.state.inmem.clone(),
            persisted_state: state.sk.state.clone(),
            flush_lsn: state.sk.wal_store.flush_lsn(),
            wal_storage: state.sk.wal_store.get_metrics(),
        })
    }

    /// Returns in-memory timeline state to build a full debug dump.
    pub async fn memory_dump(&self) -> debug_dump::Memory {
        let state = self.read_shared_state().await;

        let (write_lsn, write_record_lsn, flush_lsn, file_open) =
            state.sk.wal_store.internal_state();

        debug_dump::Memory {
            is_cancelled: self.is_cancelled(),
            peers_info_len: state.peers_info.0.len(),
            walsenders: self.walsenders.get_all(),
            wal_backup_active: self.wal_backup_active.load(Ordering::Relaxed),
            active: self.broker_active.load(Ordering::Relaxed),
            num_computes: self.walreceivers.get_num() as u32,
            last_removed_segno: state.last_removed_segno,
            epoch_start_lsn: state.sk.epoch_start_lsn,
            mem_state: state.sk.state.inmem.clone(),
            write_lsn,
            write_record_lsn,
            flush_lsn,
            file_open,
        }
    }

    /// Apply a function to the control file state and persist it.
    pub async fn map_control_file<T>(
        self: &Arc<Self>,
        f: impl FnOnce(&mut TimelinePersistentState) -> Result<T>,
    ) -> Result<T> {
        let mut state = self.write_shared_state().await;
        let mut persistent_state = state.sk.state.start_change();
        // If f returns error, we abort the change and don't persist anything.
        let res = f(&mut persistent_state)?;
        // If persisting fails, we abort the change and return error.
        state.sk.state.finish_change(&persistent_state).await?;
        Ok(res)
    }
}

/// Deletes directory and it's contents. Returns false if directory does not exist.
async fn delete_dir(path: &Utf8PathBuf) -> Result<bool> {
    match fs::remove_dir_all(path).await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}
