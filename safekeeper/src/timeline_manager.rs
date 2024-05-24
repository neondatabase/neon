//! The timeline manager task is responsible for managing the timeline's background tasks.
//! It is spawned alongside each timeline and exits when the timeline is deleted.
//! It watches for changes in the timeline state and decides when to spawn or kill background tasks.
//! It also can manage some reactive state, like should the timeline be active for broker pushes or not.

use std::{sync::Arc, time::Duration};

use tracing::{info, instrument, warn};
use utils::lsn::Lsn;

use crate::{
    metrics::{MANAGER_ACTIVE_CHANGES, MANAGER_ITERATIONS_TOTAL},
    timeline::{PeerInfo, ReadGuardSharedState, Timeline},
    timelines_set::TimelinesSet,
    wal_backup::{self, WalBackupTaskHandle},
    SafeKeeperConf,
};

pub struct StateSnapshot {
    pub commit_lsn: Lsn,
    pub backup_lsn: Lsn,
    pub remote_consistent_lsn: Lsn,
    pub peers: Vec<PeerInfo>,
}

impl StateSnapshot {
    /// Create a new snapshot of the timeline state.
    fn new(read_guard: ReadGuardSharedState, heartbeat_timeout: Duration) -> Self {
        Self {
            commit_lsn: read_guard.sk.state.inmem.commit_lsn,
            backup_lsn: read_guard.sk.state.inmem.backup_lsn,
            remote_consistent_lsn: read_guard.sk.state.inmem.remote_consistent_lsn,
            peers: read_guard.get_peers(heartbeat_timeout),
        }
    }
}

/// Control how often the manager task should wake up to check updates.
/// There is no need to check for updates more often than this.
const REFRESH_INTERVAL: Duration = Duration::from_millis(300);

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
            warn!("manager task finished prematurely");
        }
    };

    // sets whether timeline is active for broker pushes or not
    let mut tli_broker_active = broker_active_set.guard(tli.clone());

    let ttid = tli.ttid;
    let wal_seg_size = tli.get_wal_seg_size().await;
    let heartbeat_timeout = conf.heartbeat_timeout;

    let mut state_version_rx = tli.get_state_version_rx();

    let walreceivers = tli.get_walreceivers();
    let mut num_computes_rx = walreceivers.get_num_rx();

    // list of background tasks
    let mut backup_task: Option<WalBackupTaskHandle> = None;

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

            if !is_active {
                // TODO: maybe use tokio::spawn?
                if let Err(e) = tli.maybe_persist_control_file().await {
                    warn!("control file save in update_status failed: {:?}", e);
                }
            }
        }

        // update the state in Arc<Timeline>
        tli.wal_backup_active
            .store(backup_task.is_some(), std::sync::atomic::Ordering::Relaxed);
        tli.broker_active
            .store(is_active, std::sync::atomic::Ordering::Relaxed);

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
}
