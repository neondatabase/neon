use std::{sync::Arc, time::Duration};

use tracing::{info, instrument, warn};
use utils::lsn::Lsn;

use crate::{
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
    let mut cancellation_rx = match tli.get_cancellation_rx() {
        Ok(rx) => rx,
        Err(_) => {
            info!("timeline canceled during task start");
            return;
        }
    };

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

    loop {
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

        // FIXME: add tracking of relevant pageservers and check them here individually,
        // otherwise migration won't work (we suspend too early).
        let is_active = is_wal_backup_required
            || state_snapshot.remote_consistent_lsn < state_snapshot.commit_lsn;

        // update the broker map
        if tli_broker_active.set(is_active) {
            // write log if state has changed
            info!(
                "timeline {} active={} now, remote_consistent_lsn={}, commit_lsn={}",
                ttid, is_active, state_snapshot.remote_consistent_lsn, state_snapshot.commit_lsn,
            );

            if !is_active {
                // TODO: maybe use tokio::spawn?
                if let Err(e) = tli.maybe_persist_control_file().await {
                    warn!("control file save in update_status failed: {:?}", e);
                }
            }
        }

        // update the state in Arc<Timeline>
        tli.wal_backup_active
            .store(is_wal_backup_required, std::sync::atomic::Ordering::SeqCst);
        tli.broker_active
            .store(is_active, std::sync::atomic::Ordering::SeqCst);

        // sleep to make the loop less busy
        tokio::time::sleep(REFRESH_INTERVAL).await;

        // wait until something changes
        tokio::select! {
            _ = cancellation_rx.changed() => {
                // timeline was deleted
                return;
            }
            _ = state_version_rx.changed() => {
                // state was updated
            }
            _ = num_computes_rx.changed() => {
                // number of connected computes was updated
            }
        }
    }
}
