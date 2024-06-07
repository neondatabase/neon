//! The timeline manager task is responsible for managing the timeline's background tasks.
//! It is spawned alongside each timeline and exits when the timeline is deleted.
//! It watches for changes in the timeline state and decides when to spawn or kill background tasks.
//! It also can manage some reactive state, like should the timeline be active for broker pushes or not.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use postgres_ffi::XLogSegNo;
use tokio::task::{JoinError, JoinHandle};
use tracing::{info, info_span, instrument, warn, Instrument};
use utils::lsn::Lsn;

use crate::{
    control_file::Storage,
    metrics::{MANAGER_ACTIVE_CHANGES, MANAGER_ITERATIONS_TOTAL},
    recovery::recovery_main,
    remove_wal::calc_horizon_lsn,
    send_wal::WalSenders,
    timeline::{PeerInfo, ReadGuardSharedState, Timeline},
    timelines_set::{TimelineSetGuard, TimelinesSet},
    wal_backup::{self, WalBackupTaskHandle},
    wal_backup_partial, SafeKeeperConf,
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

    // misc
    pub cfile_last_persist_at: Instant,
    pub inmem_flush_pending: bool,
    pub peers: Vec<PeerInfo>,
}

impl StateSnapshot {
    /// Create a new snapshot of the timeline state.
    fn new(read_guard: ReadGuardSharedState, heartbeat_timeout: Duration) -> Self {
        Self {
            commit_lsn: read_guard.sk.state.inmem.commit_lsn,
            backup_lsn: read_guard.sk.state.inmem.backup_lsn,
            remote_consistent_lsn: read_guard.sk.state.inmem.remote_consistent_lsn,
            cfile_peer_horizon_lsn: read_guard.sk.state.peer_horizon_lsn,
            cfile_remote_consistent_lsn: read_guard.sk.state.remote_consistent_lsn,
            cfile_backup_lsn: read_guard.sk.state.backup_lsn,
            cfile_last_persist_at: read_guard.sk.state.pers.last_persist_at(),
            inmem_flush_pending: Self::has_unflushed_inmem_state(&read_guard),
            peers: read_guard.get_peers(heartbeat_timeout),
        }
    }

    fn has_unflushed_inmem_state(read_guard: &ReadGuardSharedState) -> bool {
        let state = &read_guard.sk.state;
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
const CF_SAVE_INTERVAL: Duration = Duration::from_secs(300);

/// This task gets spawned alongside each timeline and is responsible for managing the timeline's
/// background tasks.
/// Be careful, this task is not respawned on panic, so it should not panic.
#[instrument(name = "manager", skip_all, fields(ttid = %tli.ttid))]
pub async fn main_task(
    tli: Arc<Timeline>,
    conf: SafeKeeperConf,
    broker_active_set: Arc<TimelinesSet>,
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
    let mut partial_backup_task: Option<JoinHandle<()>> = None;
    let mut wal_removal_task: Option<JoinHandle<anyhow::Result<u64>>> = None;

    // Start recovery task which always runs on the timeline.
    if conf.peer_recovery_enabled {
        match tli.full_access_guard().await {
            Ok(tli) => {
                recovery_task = Some(tokio::spawn(recovery_main(tli, conf.clone())));
            }
            Err(e) => {
                warn!("failed to start recovery task: {:?}", e);
            }
        }
    }

    // Start partial backup task which always runs on the timeline.
    if conf.is_wal_backup_enabled() && conf.partial_backup_enabled {
        match tli.full_access_guard().await {
            Ok(tli) => {
                partial_backup_task = Some(tokio::spawn(wal_backup_partial::main_task(
                    tli,
                    conf.clone(),
                )));
            }
            Err(e) => {
                warn!("failed to start partial backup task: {:?}", e);
            }
        }
    }

    let last_state = 'outer: loop {
        MANAGER_ITERATIONS_TOTAL.inc();

        let state_snapshot = StateSnapshot::new(tli.read_shared_state().await, heartbeat_timeout);
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

        let _is_active = update_is_active(
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

/// Spawns/kills backup task and returns true if backup is required.
async fn update_backup(
    conf: &SafeKeeperConf,
    tli: &Arc<Timeline>,
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
    tli: &Arc<Timeline>,
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
    tli: &Arc<Timeline>,
) -> Option<tokio::time::Instant> {
    if !state.inmem_flush_pending {
        return None;
    }

    if state.cfile_last_persist_at.elapsed() > CF_SAVE_INTERVAL {
        let mut write_guard = tli.write_shared_state().await;
        // this can be done in the background because it blocks manager task, but flush() should
        // be fast enough not to be a problem now
        if let Err(e) = write_guard.sk.state.flush().await {
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
    tli: &Arc<Timeline>,
    wal_seg_size: usize,
    state: &StateSnapshot,
    last_removed_segno: u64,
    wal_removal_task: &mut Option<JoinHandle<anyhow::Result<u64>>>,
) {
    if wal_removal_task.is_some() {
        // WAL removal is already in progress
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
        let remover = crate::wal_storage::Storage::remove_up_to(
            &tli.read_shared_state().await.sk.wal_store,
            removal_horizon_segno,
        );
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
    tli: &Arc<Timeline>,
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
