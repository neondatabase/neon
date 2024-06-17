//! The timeline manager task is responsible for managing the timeline's background tasks.
//! It is spawned alongside each timeline and exits when the timeline is deleted.
//! It watches for changes in the timeline state and decides when to spawn or kill background tasks.
//! It also can manage some reactive state, like should the timeline be active for broker pushes or not.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use postgres_ffi::XLogSegNo;
use tokio::task::{JoinError, JoinHandle};
use tracing::{info, info_span, instrument, warn, Instrument};
use utils::lsn::Lsn;

use crate::{
    control_file::{FileStorage, Storage},
    metrics::{MANAGER_ACTIVE_CHANGES, MANAGER_ITERATIONS_TOTAL},
    recovery::recovery_main,
    remove_wal::calc_horizon_lsn,
    safekeeper::Term,
    send_wal::WalSenders,
    state::{EvictionState, TimelineState},
    timeline::{FullAccessTimeline, ManagerTimeline, PeerInfo, ReadGuardSharedState, StateSK},
    timelines_set::{TimelineSetGuard, TimelinesSet},
    wal_backup::{self, WalBackupTaskHandle},
    wal_backup_partial::{self, PartialRemoteSegment},
    wal_storage::wal_file_paths,
    SafeKeeperConf,
};

pub struct StateSnapshot {
    // inmem values
    pub commit_lsn: Lsn,
    pub backup_lsn: Lsn,
    pub remote_consistent_lsn: Lsn,

    // persistent control file values
    pub cfile_peer_horizon_lsn: Lsn,
    pub cfile_remote_consistent_lsn: Lsn,
    pub cfile_backup_lsn: Lsn,

    // latest state
    pub flush_lsn: Lsn,
    pub term: Term,

    // misc
    pub cfile_last_persist_at: Instant,
    pub inmem_flush_pending: bool,
    pub wal_removal_on_hold: bool,
    pub peers: Vec<PeerInfo>,
    pub eviction: EvictionState,
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
            term: state.acceptor_state.term,
            cfile_last_persist_at: state.pers.last_persist_at(),
            inmem_flush_pending: Self::has_unflushed_inmem_state(state),
            wal_removal_on_hold: read_guard.wal_removal_on_hold,
            peers: read_guard.get_peers(heartbeat_timeout),
            eviction: state.eviction_state,
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

/// How often to save the control file if the is no other activity.
const CF_SAVE_INTERVAL: Duration = Duration::from_secs(1);

pub enum ManagerCtlMessage {
    /// Request to get a guard for FullAccessTimeline, with WAL files available locally.
    GuardRequest(tokio::sync::oneshot::Sender<anyhow::Result<AccessGuard>>),
    /// Request to drop the guard.
    GuardDrop(u64),
}

impl std::fmt::Debug for ManagerCtlMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManagerCtlMessage::GuardRequest(_) => write!(f, "GuardRequest"),
            ManagerCtlMessage::GuardDrop(id) => write!(f, "GuardDrop({})", id),
        }
    }
}

pub struct ManagerCtl {
    manager_ch: tokio::sync::mpsc::UnboundedSender<ManagerCtlMessage>,

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
            manager_ch: tx,
            init_manager_rx: std::sync::Mutex::new(Some(rx)),
        }
    }

    /// Issue a new guard and wait for manager to prepare the timeline.
    pub async fn full_access_guard(&self) -> anyhow::Result<AccessGuard> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.manager_ch.send(ManagerCtlMessage::GuardRequest(tx))?;

        // wait for the manager to respond with the guard
        rx.await
            .map_err(|e| anyhow::anyhow!("failed to wait for manager guard: {:?}", e))
            .and_then(std::convert::identity)
    }

    /// Must be called exactly once to bootstrap the manager.
    pub fn bootstrap_manager(
        &self,
    ) -> (
        tokio::sync::mpsc::UnboundedReceiver<ManagerCtlMessage>,
        tokio::sync::mpsc::UnboundedSender<ManagerCtlMessage>,
    ) {
        let rx = self
            .init_manager_rx
            .lock()
            .expect("mutex init_manager_rx poisoned")
            .take()
            .expect("manager already bootstrapped");

        (rx, self.manager_ch.clone())
    }
}

pub struct AccessGuard {
    manager_ch: tokio::sync::mpsc::UnboundedSender<ManagerCtlMessage>,
    guard_id: u64,
}

impl Drop for AccessGuard {
    fn drop(&mut self) {
        // notify the manager that the guard is dropped
        let res = self
            .manager_ch
            .send(ManagerCtlMessage::GuardDrop(self.guard_id));
        if let Err(e) = res {
            warn!("failed to send GuardDrop message: {:?}", e);
        }
    }
}

/// This task gets spawned alongside each timeline and is responsible for managing the timeline's
/// background tasks.
/// Be careful, this task is not respawned on panic, so it should not panic.
#[instrument(name = "manager", skip_all, fields(ttid = %tli.ttid))]
pub async fn main_task(
    tli: ManagerTimeline,
    conf: SafeKeeperConf,
    broker_active_set: Arc<TimelinesSet>,
    mut manager_rx: tokio::sync::mpsc::UnboundedReceiver<ManagerCtlMessage>,
    manager_tx: tokio::sync::mpsc::UnboundedSender<ManagerCtlMessage>,
) {
    scopeguard::defer! {
        if tli.is_cancelled() {
            info!("manager task finished");
        } else {
            warn!("manager task finished prematurely");
        }
    };

    // configuration & dependencies
    let wal_seg_size = tli.get_wal_seg_size().await;
    let heartbeat_timeout = conf.heartbeat_timeout;
    let walsenders = tli.get_walsenders();
    let walreceivers = tli.get_walreceivers();

    // current state
    let mut state_version_rx = tli.get_state_version_rx();
    let mut num_computes_rx = walreceivers.get_num_rx();
    let mut tli_broker_active = broker_active_set.guard(tli.clone());
    let mut last_removed_segno = 0 as XLogSegNo;

    // list of background tasks
    let mut backup_task: Option<WalBackupTaskHandle> = None;
    let mut recovery_task: Option<JoinHandle<()>> = None;
    let mut wal_removal_task: Option<JoinHandle<anyhow::Result<u64>>> = None;

    // partial backup task
    let mut partial_backup_task: Option<JoinHandle<Option<PartialRemoteSegment>>> = None;
    // TODO: it should be initialized if timeline is evicted
    let mut partial_backup_uploaded: Option<PartialRemoteSegment> = None;

    // active FullAccessTimeline guards
    let mut next_guard_id: u64 = 0;
    let mut guard_ids: HashSet<u64> = HashSet::new();

    let mut is_offloaded = tli.is_offloaded().await;

    // Start recovery task which always runs on the timeline.
    // TODO: don't start it for evicted timelines
    if !is_offloaded && conf.peer_recovery_enabled {
        let guard = create_guard(&mut next_guard_id, &mut guard_ids, &manager_tx);
        let tli = FullAccessTimeline::new(tli.clone(), guard);
        recovery_task = Some(tokio::spawn(recovery_main(tli, conf.clone())));
    }

    let last_state = 'outer: loop {
        MANAGER_ITERATIONS_TOTAL.inc();

        let state_snapshot = StateSnapshot::new(tli.read_shared_state().await, heartbeat_timeout);
        let next_cfile_save = if !is_offloaded {
            let num_computes = *num_computes_rx.borrow();
            let is_wal_backup_required = update_backup(
                &conf,
                &tli,
                wal_seg_size,
                num_computes,
                &state_snapshot,
                &mut backup_task,
            )
            .await;

            let is_active = update_is_active(
                is_wal_backup_required,
                num_computes,
                &state_snapshot,
                &mut tli_broker_active,
                &tli,
            );

            let next_cfile_save = update_control_file_save(&state_snapshot, &tli).await;

            update_wal_removal(
                &conf,
                walsenders,
                &tli,
                wal_seg_size,
                &state_snapshot,
                last_removed_segno,
                &mut wal_removal_task,
            )
            .await;

            update_partial_backup(
                &conf,
                &tli,
                &state_snapshot,
                &mut partial_backup_task,
                &mut partial_backup_uploaded,
                &mut next_guard_id,
                &mut guard_ids,
                &manager_tx,
            )
            .await;

            let ready_for_eviction = backup_task.is_none()
                && recovery_task.is_none()
                && wal_removal_task.is_none()
                && partial_backup_task.is_none()
                && partial_backup_uploaded.is_some()
                && next_cfile_save.is_none()
                && guard_ids.is_empty()
                && !is_active
                && !wal_backup_partial::needs_uploading(&state_snapshot, &partial_backup_uploaded)
                && partial_backup_uploaded
                    .as_ref()
                    .unwrap()
                    .flush_lsn
                    .segment_number(wal_seg_size)
                    == last_removed_segno + 1;

            if ready_for_eviction {
                let _ = offload_timeline(
                    &tli,
                    partial_backup_uploaded.as_ref().unwrap(),
                    wal_seg_size,
                    &mut is_offloaded,
                )
                .await;
            }

            next_cfile_save
        } else {
            None
        };

        // wait until something changes. tx channels are stored under Arc, so they will not be
        // dropped until the manager task is finished.
        tokio::select! {
            _ = tli.cancel.cancelled() => {
                // timeline was deleted
                break 'outer state_snapshot;
            }
            _ = async {
                // don't wake up on every state change, but at most every REFRESH_INTERVAL
                tokio::time::sleep(REFRESH_INTERVAL).await;
                let _ = state_version_rx.changed().await;
            } => {
                // state was updated
            }
            _ = num_computes_rx.changed() => {
                // number of connected computes was updated
            }
            _ = async {
                if let Some(timeout) = next_cfile_save {
                    tokio::time::sleep_until(timeout).await
                } else {
                    futures::future::pending().await
                }
            } => {
                // it's time to save the control file
            }
            res = async {
                if let Some(task) = &mut wal_removal_task {
                    task.await
                } else {
                    futures::future::pending().await
                }
            } => {
                // WAL removal task finished
                wal_removal_task = None;
                update_wal_removal_end(res, &tli, &mut last_removed_segno);
            }
            res = async {
                if let Some(task) = &mut partial_backup_task {
                    task.await
                } else {
                    futures::future::pending().await
                }
            } => {
                // partial backup task finished
                partial_backup_task = None;

                match res {
                    Ok(new_upload_state) => {
                        partial_backup_uploaded = new_upload_state;
                    }
                    Err(e) => {
                        warn!("partial backup task panicked: {:?}", e);
                    }
                }
            }

            res = manager_rx.recv() => {
                info!("received manager message: {:?}", res);
                match res {
                    Some(ManagerCtlMessage::GuardRequest(tx)) => {
                        if is_offloaded {
                            // trying to unevict timeline
                            if !unoffload_timeline(
                                &tli,
                                &mut is_offloaded,
                                partial_backup_uploaded.as_ref().expect("partial backup should exist"),
                                wal_seg_size,
                            ).await {
                                warn!("failed to unoffload timeline");
                                let guard = Err(anyhow::anyhow!("failed to unoffload timeline"));
                                if tx.send(guard).is_err() {
                                    warn!("failed to reply with a guard");
                                }
                                continue 'outer;
                            }
                        }
                        assert!(!is_offloaded);

                        let guard = create_guard(&mut next_guard_id, &mut guard_ids, &manager_tx);
                        let guard_id = guard.guard_id;
                        if tx.send(Ok(guard)).is_err() {
                            warn!("failed to reply with a guard {}", guard_id);
                        }
                    }
                    Some(ManagerCtlMessage::GuardDrop(guard_id)) => {
                        info!("dropping guard {}", guard_id);
                        assert!(guard_ids.remove(&guard_id));
                    }
                    None => {
                        // can't happen, we're holding the sender
                        unreachable!();
                    }
                }
            }
        }
    };

    // remove timeline from the broker active set sooner, before waiting for background tasks
    tli_broker_active.set(false);

    // shutdown background tasks
    if conf.is_wal_backup_enabled() {
        wal_backup::update_task(&conf, &tli, false, &last_state, &mut backup_task).await;
    }

    if let Some(recovery_task) = recovery_task {
        if let Err(e) = recovery_task.await {
            warn!("recovery task failed: {:?}", e);
        }
    }

    if let Some(partial_backup_task) = partial_backup_task {
        if let Err(e) = partial_backup_task.await {
            warn!("partial backup task failed: {:?}", e);
        }
    }

    if let Some(wal_removal_task) = wal_removal_task {
        let res = wal_removal_task.await;
        update_wal_removal_end(res, &tli, &mut last_removed_segno);
    }
}

#[instrument(name = "offload_timeline", skip_all)]
async fn offload_timeline(
    tli: &ManagerTimeline,
    partial_backup_uploaded: &PartialRemoteSegment,
    wal_seg_size: usize,
    is_offloaded: &mut bool,
) -> bool {
    info!("timeline is ready for eviction");
    assert!(!(*is_offloaded));

    let flush_lsn = partial_backup_uploaded.flush_lsn;
    let segno = flush_lsn.segment_number(wal_seg_size);
    let (_, partial_segfile) = wal_file_paths(tli.timeline_dir(), segno, wal_seg_size).unwrap();

    info!("TODO: delete WAL file here: {}", partial_segfile);
    info!("offloading timeline at flush_lsn={}", flush_lsn);
    if let Err(e) = tli.switch_to_offloaded(&flush_lsn).await {
        warn!("failed to offload timeline: {:?}", e);
        return false;
    }
    info!("successfully offloaded timeline");
    *is_offloaded = true;

    true
}

#[instrument(name = "unoffload_timeline", skip_all)]
async fn unoffload_timeline(
    tli: &ManagerTimeline,
    is_offloaded: &mut bool,
    partial_backup_uploaded: &PartialRemoteSegment,
    wal_seg_size: usize,
) -> bool {
    info!("timeline is ready for uneviction");
    assert!(*is_offloaded);

    let flush_lsn = partial_backup_uploaded.flush_lsn;
    let segno = flush_lsn.segment_number(wal_seg_size);
    let (_, partial_segfile) = wal_file_paths(tli.timeline_dir(), segno, wal_seg_size).unwrap();

    info!("TODO: validate local WAL file: {}", partial_segfile);
    if let Err(e) = tli.switch_to_present().await {
        warn!("failed to unoffload timeline: {:?}", e);
        return false;
    }
    info!("successfully unoffloaded timeline");
    *is_offloaded = false;

    true
}

// WARN: can be used only if timeline is not evicted
fn create_guard(
    next_guard_id: &mut u64,
    guard_ids: &mut HashSet<u64>,
    manager_tx: &tokio::sync::mpsc::UnboundedSender<ManagerCtlMessage>,
) -> AccessGuard {
    let guard_id = *next_guard_id;
    *next_guard_id += 1;
    guard_ids.insert(guard_id);

    info!("issued a new guard {}", guard_id);

    AccessGuard {
        manager_ch: manager_tx.clone(),
        guard_id,
    }
}

/// Spawns/kills backup task and returns true if backup is required.
async fn update_backup(
    conf: &SafeKeeperConf,
    tli: &ManagerTimeline,
    wal_seg_size: usize,
    num_computes: usize,
    state: &StateSnapshot,
    backup_task: &mut Option<WalBackupTaskHandle>,
) -> bool {
    let is_wal_backup_required =
        wal_backup::is_wal_backup_required(wal_seg_size, num_computes, state);

    if conf.is_wal_backup_enabled() {
        wal_backup::update_task(conf, tli, is_wal_backup_required, state, backup_task).await;
    }

    // update the state in Arc<Timeline>
    tli.wal_backup_active
        .store(backup_task.is_some(), std::sync::atomic::Ordering::Relaxed);
    is_wal_backup_required
}

/// Update is_active flag and returns its value.
fn update_is_active(
    is_wal_backup_required: bool,
    num_computes: usize,
    state: &StateSnapshot,
    tli_broker_active: &mut TimelineSetGuard,
    tli: &ManagerTimeline,
) -> bool {
    let is_active = is_wal_backup_required
        || num_computes > 0
        || state.remote_consistent_lsn < state.commit_lsn;

    // update the broker timeline set
    if tli_broker_active.set(is_active) {
        // write log if state has changed
        info!(
            "timeline active={} now, remote_consistent_lsn={}, commit_lsn={}",
            is_active, state.remote_consistent_lsn, state.commit_lsn,
        );

        MANAGER_ACTIVE_CHANGES.inc();
    }

    // update the state in Arc<Timeline>
    tli.broker_active
        .store(is_active, std::sync::atomic::Ordering::Relaxed);
    is_active
}

/// Save control file if needed. Returns Instant if we should persist the control file in the future.
async fn update_control_file_save(
    state: &StateSnapshot,
    tli: &ManagerTimeline,
) -> Option<tokio::time::Instant> {
    if !state.inmem_flush_pending {
        return None;
    }

    if state.cfile_last_persist_at.elapsed() > CF_SAVE_INTERVAL {
        let mut write_guard = tli.write_shared_state().await;
        // this can be done in the background because it blocks manager task, but flush() should
        // be fast enough not to be a problem now
        if let Err(e) = write_guard.sk.state_mut().flush().await {
            warn!("failed to save control file: {:?}", e);
        }

        None
    } else {
        // we should wait until next CF_SAVE_INTERVAL
        Some((state.cfile_last_persist_at + CF_SAVE_INTERVAL).into())
    }
}

/// Spawns WAL removal task if needed.
async fn update_wal_removal(
    conf: &SafeKeeperConf,
    walsenders: &Arc<WalSenders>,
    tli: &ManagerTimeline,
    wal_seg_size: usize,
    state: &StateSnapshot,
    last_removed_segno: u64,
    wal_removal_task: &mut Option<JoinHandle<anyhow::Result<u64>>>,
) {
    if wal_removal_task.is_some() || state.wal_removal_on_hold {
        // WAL removal is already in progress or hold off
        return;
    }

    // If enabled, we use LSN of the most lagging walsender as a WAL removal horizon.
    // This allows to get better read speed for pageservers that are lagging behind,
    // at the cost of keeping more WAL on disk.
    let replication_horizon_lsn = if conf.walsenders_keep_horizon {
        walsenders.laggard_lsn()
    } else {
        None
    };

    let removal_horizon_lsn = calc_horizon_lsn(state, replication_horizon_lsn);
    let removal_horizon_segno = removal_horizon_lsn
        .segment_number(wal_seg_size)
        .saturating_sub(1);

    if removal_horizon_segno > last_removed_segno {
        // we need to remove WAL
        let remover = match tli.read_shared_state().await.sk {
            StateSK::Loaded(ref sk) => {
                crate::wal_storage::Storage::remove_up_to(&sk.wal_store, removal_horizon_segno)
            }
            StateSK::Offloaded(_) => {
                // we can't remove WAL if it's not loaded
                // TODO: log warning?
                return;
            }
            StateSK::Empty => unreachable!(),
        };

        *wal_removal_task = Some(tokio::spawn(
            async move {
                remover.await?;
                Ok(removal_horizon_segno)
            }
            .instrument(info_span!("WAL removal", ttid=%tli.ttid)),
        ));
    }
}

/// Update the state after WAL removal task finished.
fn update_wal_removal_end(
    res: Result<anyhow::Result<u64>, JoinError>,
    tli: &ManagerTimeline,
    last_removed_segno: &mut u64,
) {
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

    *last_removed_segno = new_last_removed_segno;
    // update the state in Arc<Timeline>
    tli.last_removed_segno
        .store(new_last_removed_segno, std::sync::atomic::Ordering::Relaxed);
}

async fn update_partial_backup(
    conf: &SafeKeeperConf,
    tli: &ManagerTimeline,
    state: &StateSnapshot,
    partial_backup_task: &mut Option<JoinHandle<Option<PartialRemoteSegment>>>,
    partial_backup_uploaded: &mut Option<PartialRemoteSegment>,
    next_guard_id: &mut u64,
    guard_ids: &mut HashSet<u64>,
    manager_tx: &tokio::sync::mpsc::UnboundedSender<ManagerCtlMessage>,
) {
    // check if partial backup is enabled and should be started
    if !conf.is_wal_backup_enabled() || !conf.partial_backup_enabled {
        return;
    }

    if partial_backup_task.is_some() {
        // partial backup is already running
        return;
    }

    if !wal_backup_partial::needs_uploading(state, partial_backup_uploaded) {
        // nothing to upload
        return;
    }

    // Get FullAccessTimeline and start partial backup task.
    let guard = create_guard(next_guard_id, guard_ids, manager_tx);
    let tli = FullAccessTimeline::new(tli.tli.clone(), guard);
    *partial_backup_task = Some(tokio::spawn(wal_backup_partial::main_task(
        tli,
        conf.clone(),
    )));
}
