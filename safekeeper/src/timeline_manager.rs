//! The timeline manager task is responsible for managing the timeline's background tasks.
//! It is spawned alongside each timeline and exits when the timeline is deleted.
//! It watches for changes in the timeline state and decides when to spawn or kill background tasks.
//! It also can manage some reactive state, like should the timeline be active for broker pushes or not.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use postgres_ffi::XLogSegNo;
use tokio::task::JoinHandle;
use tracing::{info, instrument, warn};
use utils::lsn::Lsn;

use crate::{
    control_file::Storage,
    metrics::{MANAGER_ACTIVE_CHANGES, MANAGER_ITERATIONS_TOTAL},
    recovery::recovery_main,
    remove_wal::calc_horizon_lsn,
    timeline::{PeerInfo, ReadGuardSharedState, Timeline},
    timelines_set::TimelinesSet,
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
    pub last_persist_at: Instant,
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
            last_persist_at: read_guard.sk.state.pers.last_persist_at(),
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
            // TODO: add this to assert_no_errors() in python tests
            warn!("manager task finished prematurely");
        }
    };

    // configuration & dependencies
    let ttid = tli.ttid;
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

        let is_wal_backup_required =
            wal_backup::is_wal_backup_required(wal_seg_size, num_computes, &state_snapshot);

        if conf.is_wal_backup_enabled() {
            wal_backup::update_task(
                &conf,
                ttid,
                is_wal_backup_required,
                &state_snapshot,
                &mut backup_task,
            )
            .await;
        }

        let is_active = is_wal_backup_required
            || num_computes > 0
            || state_snapshot.remote_consistent_lsn < state_snapshot.commit_lsn;

        // update the broker timeline set
        if tli_broker_active.set(is_active) {
            // write log if state has changed
            info!(
                "timeline active={} now, remote_consistent_lsn={}, commit_lsn={}",
                is_active, state_snapshot.remote_consistent_lsn, state_snapshot.commit_lsn,
            );

            MANAGER_ACTIVE_CHANGES.inc();
        }

        // If enabled, we use LSN of the most lagging walsender as a WAL removal horizon.
        // This allows to get better read speed for pageservers that are lagging behind,
        // at the cost of keeping more WAL on disk.
        let replication_horizon_lsn = if conf.walsenders_keep_horizon {
            walsenders.laggard_lsn()
        } else {
            None
        };

        // update the state in Arc<Timeline>
        tli.wal_backup_active
            .store(backup_task.is_some(), std::sync::atomic::Ordering::Relaxed);
        tli.broker_active
            .store(is_active, std::sync::atomic::Ordering::Relaxed);

        // WAL removal
        let removal_horizon_lsn = calc_horizon_lsn(&state_snapshot, replication_horizon_lsn);
        let removal_horizon_segno = removal_horizon_lsn
            .segment_number(wal_seg_size)
            .saturating_sub(1);

        if removal_horizon_segno > last_removed_segno {
            // we need to remove WAL
            // TODO: do it in a tokio::spawn'ed task
            let remover = crate::wal_storage::Storage::remove_up_to(
                &tli.read_shared_state().await.sk.wal_store,
                removal_horizon_segno,
            );
            if let Err(e) = remover.await {
                warn!("failed to remove WAL: {}", e);
            } else {
                last_removed_segno = removal_horizon_segno;
                tli.last_removed_segno
                    .store(last_removed_segno, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // save control file if needed
        if state_snapshot.inmem_flush_pending
            && state_snapshot.last_persist_at.elapsed() > CF_SAVE_INTERVAL
        {
            let mut write_guard = tli.write_shared_state().await;
            if let Err(e) = write_guard.sk.state.flush().await {
                warn!("failed to save control file: {:?}", e);
            }

            // TODO: sleep until next CF_SAVE_INTERVAL
        }

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
        }
    };

    // shutdown background tasks
    if conf.is_wal_backup_enabled() {
        wal_backup::update_task(&conf, ttid, false, &last_state, &mut backup_task).await;
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
}
