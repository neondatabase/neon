//! This module contains timeline id -> safekeeper state map with file-backed
//! persistence and support for interaction between sending and receiving wal.

use anyhow::{bail, Context, Result};

use etcd_broker::SkTimelineInfo;
use lazy_static::lazy_static;
use postgres_ffi::xlog_utils::XLogSegNo;

use serde::Serialize;

use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs::{self};

use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tracing::*;

use utils::{
    lsn::Lsn,
    pq_proto::ZenithFeedback,
    zid::{ZNodeId, ZTenantId, ZTenantTimelineId},
};

use crate::callmemaybe::{CallmeEvent, SubscriptionStateKey};

use crate::control_file;
use crate::safekeeper::{
    AcceptorProposerMessage, ProposerAcceptorMessage, SafeKeeper, SafeKeeperState,
    SafekeeperMemState,
};
use crate::send_wal::HotStandbyFeedback;
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
    /// Zenith specific feedback received from pageserver, if any
    pub zenith_feedback: Option<ZenithFeedback>,
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
            zenith_feedback: None,
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
    /// Inactive clusters shouldn't occupy any resources, so timeline is
    /// activated whenever there is a compute connection or pageserver is not
    /// caughtup (it must have latest WAL for new compute start) and suspended
    /// otherwise.
    ///
    /// TODO: it might be better to remove tli completely from GlobalTimelines
    /// when tli is inactive instead of having this flag.
    active: bool,
    num_computes: u32,
    pageserver_connstr: Option<String>,
    last_removed_segno: XLogSegNo,
}

impl SharedState {
    /// Initialize timeline state, creating control file
    fn create(
        conf: &SafeKeeperConf,
        zttid: &ZTenantTimelineId,
        peer_ids: Vec<ZNodeId>,
    ) -> Result<Self> {
        let state = SafeKeeperState::new(zttid, peer_ids);
        let control_store = control_file::FileStorage::create_new(zttid, conf, state)?;
        let wal_store = wal_storage::PhysicalStorage::new(zttid, conf);
        let sk = SafeKeeper::new(zttid.timeline_id, control_store, wal_store, conf.my_id)?;

        Ok(Self {
            notified_commit_lsn: Lsn(0),
            sk,
            replicas: Vec::new(),
            active: false,
            num_computes: 0,
            pageserver_connstr: None,
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
            active: false,
            num_computes: 0,
            pageserver_connstr: None,
            last_removed_segno: 0,
        })
    }

    /// Activate the timeline: start/change walsender (via callmemaybe).
    fn activate(
        &mut self,
        zttid: &ZTenantTimelineId,
        pageserver_connstr: Option<&String>,
        callmemaybe_tx: &UnboundedSender<CallmeEvent>,
    ) -> Result<()> {
        if let Some(ref pageserver_connstr) = self.pageserver_connstr {
            // unsub old sub. xxx: callmemaybe is going out
            let old_subscription_key = SubscriptionStateKey::new(
                zttid.tenant_id,
                zttid.timeline_id,
                pageserver_connstr.to_owned(),
            );
            callmemaybe_tx
                .send(CallmeEvent::Unsubscribe(old_subscription_key))
                .unwrap_or_else(|e| {
                    error!("failed to send Pause request to callmemaybe thread {}", e);
                });
        }
        if let Some(pageserver_connstr) = pageserver_connstr {
            let subscription_key = SubscriptionStateKey::new(
                zttid.tenant_id,
                zttid.timeline_id,
                pageserver_connstr.to_owned(),
            );
            // xx: sending to channel under lock is not very cool, but
            // shouldn't be a problem here. If it is, we can grab a counter
            // here and later augment channel messages with it.
            callmemaybe_tx
                .send(CallmeEvent::Subscribe(subscription_key))
                .unwrap_or_else(|e| {
                    error!(
                        "failed to send Subscribe request to callmemaybe thread {}",
                        e
                    );
                });
            info!(
                "timeline {} is subscribed to callmemaybe to {}",
                zttid.timeline_id, pageserver_connstr
            );
        }
        self.pageserver_connstr = pageserver_connstr.map(|c| c.to_owned());
        self.active = true;
        Ok(())
    }

    /// Deactivate the timeline: stop callmemaybe.
    fn deactivate(
        &mut self,
        zttid: &ZTenantTimelineId,
        callmemaybe_tx: &UnboundedSender<CallmeEvent>,
    ) -> Result<()> {
        if self.active {
            if let Some(ref pageserver_connstr) = self.pageserver_connstr {
                let subscription_key = SubscriptionStateKey::new(
                    zttid.tenant_id,
                    zttid.timeline_id,
                    pageserver_connstr.to_owned(),
                );
                callmemaybe_tx
                    .send(CallmeEvent::Unsubscribe(subscription_key))
                    .unwrap_or_else(|e| {
                        error!(
                            "failed to send Unsubscribe request to callmemaybe thread {}",
                            e
                        );
                    });
                info!(
                    "timeline {} is unsubscribed from callmemaybe to {}",
                    zttid.timeline_id,
                    self.pageserver_connstr.as_ref().unwrap()
                );
            }
            self.active = false;
        }
        Ok(())
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
            if let Some(zenith_feedback) = state.zenith_feedback {
                if let Some(acc_feedback) = acc.zenith_feedback {
                    if acc_feedback.ps_writelsn < zenith_feedback.ps_writelsn {
                        warn!("More than one pageserver is streaming WAL for the timeline. Feedback resolving is not fully supported yet.");
                        acc.zenith_feedback = Some(zenith_feedback);
                    }
                } else {
                    acc.zenith_feedback = Some(zenith_feedback);
                }

                // last lsn received by pageserver
                // FIXME if multiple pageservers are streaming WAL, last_received_lsn must be tracked per pageserver.
                // See https://github.com/zenithdb/zenith/issues/1171
                acc.last_received_lsn = Lsn::from(zenith_feedback.ps_writelsn);

                // When at least one pageserver has preserved data up to remote_consistent_lsn,
                // safekeeper is free to delete it, so choose max of all pageservers.
                acc.remote_consistent_lsn = max(
                    Lsn::from(zenith_feedback.ps_applylsn),
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
    pub callmemaybe_tx: UnboundedSender<CallmeEvent>,
    mutex: Mutex<SharedState>,
    /// conditional variable used to notify wal senders
    cond: Condvar,
}

impl Timeline {
    fn new(
        zttid: ZTenantTimelineId,
        callmemaybe_tx: UnboundedSender<CallmeEvent>,
        shared_state: SharedState,
    ) -> Timeline {
        Timeline {
            zttid,
            callmemaybe_tx,
            mutex: Mutex::new(shared_state),
            cond: Condvar::new(),
        }
    }

    /// Register compute connection, starting timeline-related activity if it is
    /// not running yet.
    /// Can fail only if channel to a static thread got closed, which is not normal at all.
    pub fn on_compute_connect(&self, pageserver_connstr: Option<&String>) -> Result<()> {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.num_computes += 1;
        // FIXME: currently we always adopt latest pageserver connstr, but we
        // should have kind of generations assigned by compute to distinguish
        // the latest one or even pass it through consensus to reliably deliver
        // to all safekeepers.
        shared_state.activate(&self.zttid, pageserver_connstr, &self.callmemaybe_tx)?;
        Ok(())
    }

    /// De-register compute connection, shutting down timeline activity if
    /// pageserver doesn't need catchup.
    /// Can fail only if channel to a static thread got closed, which is not normal at all.
    pub fn on_compute_disconnect(&self) -> Result<()> {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.num_computes -= 1;
        // If there is no pageserver, can suspend right away; otherwise let
        // walsender do that.
        if shared_state.num_computes == 0 && shared_state.pageserver_connstr.is_none() {
            shared_state.deactivate(&self.zttid, &self.callmemaybe_tx)?;
        }
        Ok(())
    }

    /// Deactivate tenant if there is no computes and pageserver is caughtup,
    /// assuming the pageserver status is in replica_id.
    /// Returns true if deactivated.
    pub fn check_deactivate(&self, replica_id: usize) -> Result<bool> {
        let mut shared_state = self.mutex.lock().unwrap();
        if !shared_state.active {
            // already suspended
            return Ok(true);
        }
        if shared_state.num_computes == 0 {
            let replica_state = shared_state.replicas[replica_id].unwrap();
            let deactivate = shared_state.notified_commit_lsn == Lsn(0) || // no data at all yet
            (replica_state.last_received_lsn != Lsn::MAX && // Lsn::MAX means that we don't know the latest LSN yet.
             replica_state.last_received_lsn >= shared_state.sk.inmem.commit_lsn);
            if deactivate {
                shared_state.deactivate(&self.zttid, &self.callmemaybe_tx)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Deactivates the timeline, assuming it is being deleted.
    /// Returns whether the timeline was already active.
    ///
    /// The callmemaybe thread is stopped by the deactivation message. We assume all other threads
    /// will stop by themselves eventually (possibly with errors, but no panics). There should be no
    /// compute threads (as we're deleting the timeline), actually. Some WAL may be left unsent, but
    /// we're deleting the timeline anyway.
    pub fn deactivate_for_delete(&self) -> Result<bool> {
        let mut shared_state = self.mutex.lock().unwrap();
        let was_active = shared_state.active;
        shared_state.deactivate(&self.zttid, &self.callmemaybe_tx)?;
        Ok(was_active)
    }

    fn is_active(&self) -> bool {
        let shared_state = self.mutex.lock().unwrap();
        shared_state.active
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
    fn notify_wal_senders(&self, shared_state: &mut MutexGuard<SharedState>) {
        if shared_state.notified_commit_lsn < shared_state.sk.inmem.commit_lsn {
            shared_state.notified_commit_lsn = shared_state.sk.inmem.commit_lsn;
            self.cond.notify_all();
        }
    }

    /// Pass arrived message to the safekeeper.
    pub fn process_msg(
        &self,
        msg: &ProposerAcceptorMessage,
    ) -> Result<Option<AcceptorProposerMessage>> {
        let mut rmsg: Option<AcceptorProposerMessage>;
        {
            let mut shared_state = self.mutex.lock().unwrap();
            rmsg = shared_state.sk.process_msg(msg)?;

            // if this is AppendResponse, fill in proper hot standby feedback and disk consistent lsn
            if let Some(AcceptorProposerMessage::AppendResponse(ref mut resp)) = rmsg {
                let state = shared_state.get_replicas_state();
                resp.hs_feedback = state.hs_feedback;
                if let Some(zenith_feedback) = state.zenith_feedback {
                    resp.zenith_feedback = zenith_feedback;
                }
            }

            // Ping wal sender that new data might be available.
            self.notify_wal_senders(&mut shared_state);
        }
        Ok(rmsg)
    }

    pub fn get_state(&self) -> (SafekeeperMemState, SafeKeeperState) {
        let shared_state = self.mutex.lock().unwrap();
        (shared_state.sk.inmem.clone(), shared_state.sk.state.clone())
    }

    /// Prepare public safekeeper info for reporting.
    pub fn get_public_info(&self, conf: &SafeKeeperConf) -> anyhow::Result<SkTimelineInfo> {
        let shared_state = self.mutex.lock().unwrap();
        Ok(SkTimelineInfo {
            last_log_term: Some(shared_state.sk.get_epoch()),
            flush_lsn: Some(shared_state.sk.wal_store.flush_lsn()),
            // note: this value is not flushed to control file yet and can be lost
            commit_lsn: Some(shared_state.sk.inmem.commit_lsn),
            s3_wal_lsn: Some(shared_state.sk.inmem.s3_wal_lsn),
            // TODO: rework feedbacks to avoid max here
            remote_consistent_lsn: Some(max(
                shared_state.get_replicas_state().remote_consistent_lsn,
                shared_state.sk.inmem.remote_consistent_lsn,
            )),
            peer_horizon_lsn: Some(shared_state.sk.inmem.peer_horizon_lsn),
            safekeeper_connection_string: Some(conf.listen_pg_addr.clone()),
        })
    }

    /// Update timeline state with peer safekeeper data.
    pub fn record_safekeeper_info(&self, sk_info: &SkTimelineInfo, _sk_id: ZNodeId) -> Result<()> {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.sk.record_safekeeper_info(sk_info)?;
        self.notify_wal_senders(&mut shared_state);
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

    pub fn remove_old_wal(&self, s3_offload_enabled: bool) -> Result<()> {
        let horizon_segno: XLogSegNo;
        let remover: Box<dyn Fn(u64) -> Result<(), anyhow::Error>>;
        {
            let shared_state = self.mutex.lock().unwrap();
            // WAL seg size not initialized yet, no WAL exists.
            if shared_state.sk.state.server.wal_seg_size == 0 {
                return Ok(());
            }
            horizon_segno = shared_state.sk.get_horizon_segno(s3_offload_enabled);
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
    callmemaybe_tx: Option<UnboundedSender<CallmeEvent>>,
}

lazy_static! {
    static ref TIMELINES_STATE: Mutex<GlobalTimelinesState> = Mutex::new(GlobalTimelinesState {
        timelines: HashMap::new(),
        callmemaybe_tx: None
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
    pub fn set_callmemaybe_tx(callmemaybe_tx: UnboundedSender<CallmeEvent>) {
        let mut state = TIMELINES_STATE.lock().unwrap();
        assert!(state.callmemaybe_tx.is_none());
        state.callmemaybe_tx = Some(callmemaybe_tx);
    }

    fn create_internal(
        mut state: MutexGuard<GlobalTimelinesState>,
        conf: &SafeKeeperConf,
        zttid: ZTenantTimelineId,
        peer_ids: Vec<ZNodeId>,
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
                    state.callmemaybe_tx.as_ref().unwrap().clone(),
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
        peer_ids: Vec<ZNodeId>,
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
                let shared_state =
                    SharedState::restore(conf, &zttid).context("failed to restore shared state");

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
                    state.callmemaybe_tx.as_ref().unwrap().clone(),
                    shared_state,
                ));
                state.timelines.insert(zttid, Arc::clone(&new_tli));
                Ok(new_tli)
            }
        }
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
    pub fn delete_force(
        conf: &SafeKeeperConf,
        zttid: &ZTenantTimelineId,
    ) -> Result<TimelineDeleteForceResult> {
        info!("deleting timeline {}", zttid);
        let was_active = match TIMELINES_STATE.lock().unwrap().timelines.remove(zttid) {
            None => false,
            Some(tli) => tli.deactivate_for_delete()?,
        };
        GlobalTimelines::delete_force_internal(conf, zttid, was_active)
    }

    /// Deactivates and deletes all timelines for the tenant, see `delete()`.
    /// Returns map of all timelines which the tenant had, `true` if a timeline was active.
    /// There may be a race if new timelines are created simultaneously.
    pub fn delete_force_all_for_tenant(
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
                    to_delete.insert(zttid, tli.deactivate_for_delete()?);
                }
            }
            // TODO: test that the correct subset of timelines is removed. It's complicated because they are implicitly created currently.
            timelines.retain(|zttid, _| !to_delete.contains_key(zttid));
        }
        let mut deleted = HashMap::new();
        for (zttid, was_active) in to_delete {
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
