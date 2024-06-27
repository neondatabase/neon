//! The timeline manager task is responsible for managing the timeline's background tasks.
//! It is spawned alongside each timeline and exits when the timeline is deleted.
//! It watches for changes in the timeline state and decides when to spawn or kill background tasks.
//! It also can manage some reactive state, like should the timeline be active for broker pushes or not.
//!
//! Be aware that you need to be extra careful with manager code, because it is not respawned on panic.
//! Also, if it will stuck in some branch, it will prevent any further progress in the timeline.

use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use postgres_ffi::XLogSegNo;
use serde::{Deserialize, Serialize};
use tokio::{
    task::{JoinError, JoinHandle},
    time::Instant,
};
use tracing::{debug, info, info_span, instrument, warn, Instrument};
use utils::lsn::Lsn;

use crate::{
    control_file::{FileStorage, Storage},
    metrics::{MANAGER_ACTIVE_CHANGES, MANAGER_ITERATIONS_TOTAL},
    recovery::recovery_main,
    remove_wal::calc_horizon_lsn,
    safekeeper::Term,
    send_wal::WalSenders,
    state::TimelineState,
    timeline::{ManagerTimeline, PeerInfo, ReadGuardSharedState, StateSK, WalResidentTimeline},
    timeline_guard::{AccessService, GuardId, ResidenceGuard},
    timelines_set::{TimelineSetGuard, TimelinesSet},
    wal_backup::{self, WalBackupTaskHandle},
    wal_backup_partial::{self, PartialRemoteSegment},
    SafeKeeperConf,
};

pub(crate) struct StateSnapshot {
    // inmem values
    pub(crate) commit_lsn: Lsn,
    pub(crate) backup_lsn: Lsn,
    pub(crate) remote_consistent_lsn: Lsn,

    // persistent control file values
    pub(crate) cfile_peer_horizon_lsn: Lsn,
    pub(crate) cfile_remote_consistent_lsn: Lsn,
    pub(crate) cfile_backup_lsn: Lsn,

    // latest state
    pub(crate) flush_lsn: Lsn,
    pub(crate) last_log_term: Term,

    // misc
    pub(crate) cfile_last_persist_at: std::time::Instant,
    pub(crate) inmem_flush_pending: bool,
    pub(crate) wal_removal_on_hold: bool,
    pub(crate) peers: Vec<PeerInfo>,
}

impl StateSnapshot {
    /// Create a new snapshot of the timeline state.
    fn new(read_guard: ReadGuardSharedState, heartbeat_timeout: Duration) -> Self {
        let state = read_guard.sk.state();
        Self {
            commit_lsn: state.inmem.commit_lsn,
            backup_lsn: state.inmem.backup_lsn,
            remote_consistent_lsn: state.inmem.remote_consistent_lsn,
            cfile_peer_horizon_lsn: state.peer_horizon_lsn,
            cfile_remote_consistent_lsn: state.remote_consistent_lsn,
            cfile_backup_lsn: state.backup_lsn,
            flush_lsn: read_guard.sk.flush_lsn(),
            last_log_term: read_guard.sk.last_log_term(),
            cfile_last_persist_at: state.pers.last_persist_at(),
            inmem_flush_pending: Self::has_unflushed_inmem_state(state),
            wal_removal_on_hold: read_guard.wal_removal_on_hold,
            peers: read_guard.get_peers(heartbeat_timeout),
        }
    }

    fn has_unflushed_inmem_state(state: &TimelineState<FileStorage>) -> bool {
        state.inmem.commit_lsn > state.commit_lsn
            || state.inmem.backup_lsn > state.backup_lsn
            || state.inmem.peer_horizon_lsn > state.peer_horizon_lsn
            || state.inmem.remote_consistent_lsn > state.remote_consistent_lsn
    }
}

/// Control how often the manager task should wake up to check updates.
/// There is no need to check for updates more often than this.
const REFRESH_INTERVAL: Duration = Duration::from_millis(300);

pub enum ManagerCtlMessage {
    /// Request to get a guard for WalResidentTimeline, with WAL files available locally.
    GuardRequest(tokio::sync::oneshot::Sender<anyhow::Result<ResidenceGuard>>),
    /// Request to drop the guard.
    GuardDrop(GuardId),
}

impl std::fmt::Debug for ManagerCtlMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManagerCtlMessage::GuardRequest(_) => write!(f, "GuardRequest"),
            ManagerCtlMessage::GuardDrop(id) => write!(f, "GuardDrop({:?})", id),
        }
    }
}

pub struct ManagerCtl {
    manager_tx: tokio::sync::mpsc::UnboundedSender<ManagerCtlMessage>,

    // this is used to initialize manager, it will be moved out in bootstrap().
    init_manager_rx:
        std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<ManagerCtlMessage>>>,
}

impl Default for ManagerCtl {
    fn default() -> Self {
        Self::new()
    }
}

impl ManagerCtl {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            manager_tx: tx,
            init_manager_rx: std::sync::Mutex::new(Some(rx)),
        }
    }

    /// Issue a new guard and wait for manager to prepare the timeline.
    /// Sends a message to the manager and waits for the response.
    /// Can be blocked indefinitely if the manager is stuck.
    pub async fn wal_residence_guard(&self) -> anyhow::Result<ResidenceGuard> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.manager_tx.send(ManagerCtlMessage::GuardRequest(tx))?;

        // wait for the manager to respond with the guard
        rx.await
            .map_err(|e| anyhow::anyhow!("response read fail: {:?}", e))
            .and_then(std::convert::identity)
    }

    /// Must be called exactly once to bootstrap the manager.
    pub fn bootstrap_manager(
        &self,
    ) -> (
        tokio::sync::mpsc::UnboundedSender<ManagerCtlMessage>,
        tokio::sync::mpsc::UnboundedReceiver<ManagerCtlMessage>,
    ) {
        let rx = self
            .init_manager_rx
            .lock()
            .expect("mutex init_manager_rx poisoned")
            .take()
            .expect("manager already bootstrapped");

        (self.manager_tx.clone(), rx)
    }
}

pub(crate) struct Manager {
    // configuration & dependencies
    pub(crate) tli: ManagerTimeline,
    pub(crate) conf: SafeKeeperConf,
    pub(crate) wal_seg_size: usize,
    pub(crate) walsenders: Arc<WalSenders>,

    // current state
    pub(crate) state_version_rx: tokio::sync::watch::Receiver<usize>,
    pub(crate) num_computes_rx: tokio::sync::watch::Receiver<usize>,
    pub(crate) tli_broker_active: TimelineSetGuard,
    pub(crate) last_removed_segno: XLogSegNo,
    pub(crate) is_offloaded: bool,

    // background tasks
    pub(crate) backup_task: Option<WalBackupTaskHandle>,
    pub(crate) recovery_task: Option<JoinHandle<()>>,
    pub(crate) wal_removal_task: Option<JoinHandle<anyhow::Result<u64>>>,

    // partial backup
    pub(crate) partial_backup_task: Option<JoinHandle<Option<PartialRemoteSegment>>>,
    pub(crate) partial_backup_uploaded: Option<PartialRemoteSegment>,

    // misc
    pub(crate) access_service: AccessService,
}

/// This task gets spawned alongside each timeline and is responsible for managing the timeline's
/// background tasks.
/// Be careful, this task is not respawned on panic, so it should not panic.
#[instrument(name = "manager", skip_all, fields(ttid = %tli.ttid))]
pub async fn main_task(
    tli: ManagerTimeline,
    conf: SafeKeeperConf,
    broker_active_set: Arc<TimelinesSet>,
    manager_tx: tokio::sync::mpsc::UnboundedSender<ManagerCtlMessage>,
    mut manager_rx: tokio::sync::mpsc::UnboundedReceiver<ManagerCtlMessage>,
) {
    tli.set_status(Status::Started);

    let defer_tli = tli.tli.clone();
    scopeguard::defer! {
        if defer_tli.is_cancelled() {
            info!("manager task finished");
        } else {
            warn!("manager task finished prematurely");
        }
    };

    let mut mgr = Manager::new(tli, conf, broker_active_set, manager_tx).await;

    // Start recovery task which always runs on the timeline.
    if !mgr.is_offloaded && mgr.conf.peer_recovery_enabled {
        let tli = mgr.wal_resident_timeline();
        mgr.recovery_task = Some(tokio::spawn(recovery_main(tli, mgr.conf.clone())));
    }

    let last_state = 'outer: loop {
        MANAGER_ITERATIONS_TOTAL.inc();

        mgr.set_status(Status::StateSnapshot);
        let state_snapshot = mgr.state_snapshot().await;

        let mut next_event: Option<Instant> = None;
        if !mgr.is_offloaded {
            let num_computes = *mgr.num_computes_rx.borrow();

            mgr.set_status(Status::UpdateBackup);
            let is_wal_backup_required = mgr.update_backup(num_computes, &state_snapshot).await;
            mgr.update_is_active(is_wal_backup_required, num_computes, &state_snapshot);

            mgr.set_status(Status::UpdateControlFile);
            mgr.update_control_file_save(&state_snapshot, &mut next_event)
                .await;

            mgr.set_status(Status::UpdateWalRemoval);
            mgr.update_wal_removal(&state_snapshot).await;

            mgr.set_status(Status::UpdatePartialBackup);
            mgr.update_partial_backup(&state_snapshot).await;

            if mgr.conf.enable_offload && mgr.ready_for_eviction(&next_event, &state_snapshot) {
                mgr.set_status(Status::EvictTimeline);
                mgr.evict_timeline().await;
            }
        }

        mgr.set_status(Status::Wait);
        // wait until something changes. tx channels are stored under Arc, so they will not be
        // dropped until the manager task is finished.
        tokio::select! {
            _ = mgr.tli.cancel.cancelled() => {
                // timeline was deleted
                break 'outer state_snapshot;
            }
            _ = async {
                // don't wake up on every state change, but at most every REFRESH_INTERVAL
                tokio::time::sleep(REFRESH_INTERVAL).await;
                let _ = mgr.state_version_rx.changed().await;
            } => {
                // state was updated
            }
            _ = mgr.num_computes_rx.changed() => {
                // number of connected computes was updated
            }
            _ = sleep_until(&next_event) => {
                // we were waiting for some event (e.g. cfile save)
            }
            res = await_task_finish(&mut mgr.wal_removal_task) => {
                // WAL removal task finished
                mgr.wal_removal_task = None;
                mgr.update_wal_removal_end(res);
            }
            res = await_task_finish(&mut mgr.partial_backup_task) => {
                // partial backup task finished
                mgr.partial_backup_task = None;
                mgr.update_partial_backup_end(res);
            }

            msg = manager_rx.recv() => {
                mgr.set_status(Status::HandleMessage);
                mgr.handle_message(msg).await;
            }
        }
    };
    mgr.set_status(Status::Exiting);

    // remove timeline from the broker active set sooner, before waiting for background tasks
    mgr.tli_broker_active.set(false);

    // shutdown background tasks
    if mgr.conf.is_wal_backup_enabled() {
        wal_backup::update_task(&mut mgr, false, &last_state).await;
    }

    if let Some(recovery_task) = &mut mgr.recovery_task {
        if let Err(e) = recovery_task.await {
            warn!("recovery task failed: {:?}", e);
        }
    }

    if let Some(partial_backup_task) = &mut mgr.partial_backup_task {
        if let Err(e) = partial_backup_task.await {
            warn!("partial backup task failed: {:?}", e);
        }
    }

    if let Some(wal_removal_task) = &mut mgr.wal_removal_task {
        let res = wal_removal_task.await;
        mgr.update_wal_removal_end(res);
    }

    mgr.set_status(Status::Finished);
}

impl Manager {
    async fn new(
        tli: ManagerTimeline,
        conf: SafeKeeperConf,
        broker_active_set: Arc<TimelinesSet>,
        manager_tx: tokio::sync::mpsc::UnboundedSender<ManagerCtlMessage>,
    ) -> Manager {
        let (is_offloaded, partial_backup_uploaded) = tli.bootstrap_mgr().await;
        Manager {
            conf,
            wal_seg_size: tli.get_wal_seg_size().await,
            walsenders: tli.get_walsenders().clone(),
            state_version_rx: tli.get_state_version_rx(),
            num_computes_rx: tli.get_walreceivers().get_num_rx(),
            tli_broker_active: broker_active_set.guard(tli.clone()),
            last_removed_segno: 0,
            is_offloaded,
            backup_task: None,
            recovery_task: None,
            wal_removal_task: None,
            partial_backup_task: None,
            partial_backup_uploaded,
            access_service: AccessService::new(manager_tx),
            tli,
        }
    }

    fn set_status(&self, status: Status) {
        self.tli.set_status(status);
    }

    /// Get a WalResidentTimeline.
    /// Manager code must use this function instead of one from `Timeline`
    /// directly, because it will deadlock.
    pub(crate) fn wal_resident_timeline(&mut self) -> WalResidentTimeline {
        assert!(!self.is_offloaded);
        let guard = self.access_service.create_guard();
        WalResidentTimeline::new(self.tli.clone(), guard)
    }

    /// Get a snapshot of the timeline state.
    async fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot::new(
            self.tli.read_shared_state().await,
            self.conf.heartbeat_timeout,
        )
    }

    /// Spawns/kills backup task and returns true if backup is required.
    async fn update_backup(&mut self, num_computes: usize, state: &StateSnapshot) -> bool {
        let is_wal_backup_required =
            wal_backup::is_wal_backup_required(self.wal_seg_size, num_computes, state);

        if self.conf.is_wal_backup_enabled() {
            wal_backup::update_task(self, is_wal_backup_required, state).await;
        }

        // update the state in Arc<Timeline>
        self.tli.wal_backup_active.store(
            self.backup_task.is_some(),
            std::sync::atomic::Ordering::Relaxed,
        );
        is_wal_backup_required
    }

    /// Update is_active flag and returns its value.
    fn update_is_active(
        &mut self,
        is_wal_backup_required: bool,
        num_computes: usize,
        state: &StateSnapshot,
    ) {
        let is_active = is_wal_backup_required
            || num_computes > 0
            || state.remote_consistent_lsn < state.commit_lsn;

        // update the broker timeline set
        if self.tli_broker_active.set(is_active) {
            // write log if state has changed
            info!(
                "timeline active={} now, remote_consistent_lsn={}, commit_lsn={}",
                is_active, state.remote_consistent_lsn, state.commit_lsn,
            );

            MANAGER_ACTIVE_CHANGES.inc();
        }

        // update the state in Arc<Timeline>
        self.tli
            .broker_active
            .store(is_active, std::sync::atomic::Ordering::Relaxed);
    }

    /// Save control file if needed. Returns Instant if we should persist the control file in the future.
    async fn update_control_file_save(
        &self,
        state: &StateSnapshot,
        next_event: &mut Option<Instant>,
    ) {
        if !state.inmem_flush_pending {
            return;
        }

        if state.cfile_last_persist_at.elapsed() > self.conf.control_file_save_interval {
            let mut write_guard = self.tli.write_shared_state().await;
            // it should be done in the background because it blocks manager task, but flush() should
            // be fast enough not to be a problem now
            if let Err(e) = write_guard.sk.state_mut().flush().await {
                warn!("failed to save control file: {:?}", e);
            }
        } else {
            // we should wait until some time passed until the next save
            update_next_event(
                next_event,
                (state.cfile_last_persist_at + self.conf.control_file_save_interval).into(),
            );
        }
    }

    /// Spawns WAL removal task if needed.
    async fn update_wal_removal(&mut self, state: &StateSnapshot) {
        if self.wal_removal_task.is_some() || state.wal_removal_on_hold {
            // WAL removal is already in progress or hold off
            return;
        }

        // If enabled, we use LSN of the most lagging walsender as a WAL removal horizon.
        // This allows to get better read speed for pageservers that are lagging behind,
        // at the cost of keeping more WAL on disk.
        let replication_horizon_lsn = if self.conf.walsenders_keep_horizon {
            self.walsenders.laggard_lsn()
        } else {
            None
        };

        let removal_horizon_lsn = calc_horizon_lsn(state, replication_horizon_lsn);
        let removal_horizon_segno = removal_horizon_lsn
            .segment_number(self.wal_seg_size)
            .saturating_sub(1);

        if removal_horizon_segno > self.last_removed_segno {
            // we need to remove WAL
            let remover = match self.tli.read_shared_state().await.sk {
                StateSK::Loaded(ref sk) => {
                    crate::wal_storage::Storage::remove_up_to(&sk.wal_store, removal_horizon_segno)
                }
                StateSK::Offloaded(_) => {
                    // we can't remove WAL if it's not loaded
                    warn!("unexpectedly trying to run WAL removal on offloaded timeline");
                    return;
                }
                StateSK::Empty => unreachable!(),
            };

            self.wal_removal_task = Some(tokio::spawn(
                async move {
                    remover.await?;
                    Ok(removal_horizon_segno)
                }
                .instrument(info_span!("WAL removal", ttid=%self.tli.ttid)),
            ));
        }
    }

    /// Update the state after WAL removal task finished.
    fn update_wal_removal_end(&mut self, res: Result<anyhow::Result<u64>, JoinError>) {
        let new_last_removed_segno = match res {
            Ok(Ok(segno)) => segno,
            Err(e) => {
                warn!("WAL removal task failed: {:?}", e);
                return;
            }
            Ok(Err(e)) => {
                warn!("WAL removal task failed: {:?}", e);
                return;
            }
        };

        self.last_removed_segno = new_last_removed_segno;
        // update the state in Arc<Timeline>
        self.tli
            .last_removed_segno
            .store(new_last_removed_segno, std::sync::atomic::Ordering::Relaxed);
    }

    /// Spawns partial WAL backup task if needed.
    async fn update_partial_backup(&mut self, state: &StateSnapshot) {
        // check if partial backup is enabled and should be started
        if !self.conf.is_wal_backup_enabled() || !self.conf.partial_backup_enabled {
            return;
        }

        if self.partial_backup_task.is_some() {
            // partial backup is already running
            return;
        }

        if !wal_backup_partial::needs_uploading(state, &self.partial_backup_uploaded) {
            // nothing to upload
            return;
        }

        // Get WalResidentTimeline and start partial backup task.
        self.partial_backup_task = Some(tokio::spawn(wal_backup_partial::main_task(
            self.wal_resident_timeline(),
            self.conf.clone(),
        )));
    }

    /// Update the state after partial WAL backup task finished.
    fn update_partial_backup_end(&mut self, res: Result<Option<PartialRemoteSegment>, JoinError>) {
        match res {
            Ok(new_upload_state) => {
                self.partial_backup_uploaded = new_upload_state;
            }
            Err(e) => {
                warn!("partial backup task panicked: {:?}", e);
            }
        }
    }

    /// Handle message arrived from ManagerCtl.
    async fn handle_message(&mut self, msg: Option<ManagerCtlMessage>) {
        debug!("received manager message: {:?}", msg);
        match msg {
            Some(ManagerCtlMessage::GuardRequest(tx)) => {
                if self.is_offloaded {
                    // trying to unevict timeline, but without gurarantee that it will be successful
                    self.unevict_timeline().await;
                }

                let guard = if self.is_offloaded {
                    Err(anyhow::anyhow!("timeline is offloaded, can't get a guard"))
                } else {
                    Ok(self.access_service.create_guard())
                };

                if tx.send(guard).is_err() {
                    warn!("failed to reply with a guard, receiver dropped");
                }
            }
            Some(ManagerCtlMessage::GuardDrop(guard_id)) => {
                self.access_service.drop_guard(guard_id);
            }
            None => {
                // can't happen, we're holding the sender
                unreachable!();
            }
        }
    }
}

// utility functions
async fn sleep_until(option: &Option<tokio::time::Instant>) {
    if let Some(timeout) = option {
        tokio::time::sleep_until(*timeout).await;
    } else {
        futures::future::pending::<()>().await;
    }
}

async fn await_task_finish<T>(option: &mut Option<JoinHandle<T>>) -> Result<T, JoinError> {
    if let Some(task) = option {
        task.await
    } else {
        futures::future::pending().await
    }
}

/// Update next_event if candidate is earlier.
fn update_next_event(next_event: &mut Option<Instant>, candidate: Instant) {
    if let Some(next) = next_event {
        if candidate < *next {
            *next = candidate;
        }
    } else {
        *next_event = Some(candidate);
    }
}

#[repr(usize)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    NotStarted,
    Started,
    StateSnapshot,
    UpdateBackup,
    UpdateControlFile,
    UpdateWalRemoval,
    UpdatePartialBackup,
    EvictTimeline,
    Wait,
    HandleMessage,
    Exiting,
    Finished,
}

/// AtomicStatus is a wrapper around AtomicUsize adapted for the Status enum.
pub struct AtomicStatus {
    inner: AtomicUsize,
}

impl Default for AtomicStatus {
    fn default() -> Self {
        Self::new()
    }
}

impl AtomicStatus {
    pub fn new() -> Self {
        AtomicStatus {
            inner: AtomicUsize::new(Status::NotStarted as usize),
        }
    }

    pub fn load(&self, order: std::sync::atomic::Ordering) -> Status {
        // Safety: This line of code uses `std::mem::transmute` to reinterpret the loaded value as `Status`.
        // It is safe to use `transmute` in this context because `Status` is a repr(usize) enum,
        // which means it has the same memory layout as usize.
        // However, it is important to ensure that the loaded value is a valid variant of `Status`,
        // otherwise, the behavior will be undefined.
        unsafe { std::mem::transmute(self.inner.load(order)) }
    }

    pub fn get(&self) -> Status {
        self.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn store(&self, val: Status, order: std::sync::atomic::Ordering) {
        self.inner.store(val as usize, order);
    }
}
