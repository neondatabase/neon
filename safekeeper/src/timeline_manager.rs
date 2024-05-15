use std::{sync::Arc, time::Duration};

use tracing::{info, instrument, warn};
use utils::lsn::Lsn;

use crate::{
    timeline::{PeerInfo, ReadGuardSharedState, Timeline},
    wal_backup::{self, WalBackupTaskHandle},
    SafeKeeperConf,
};

pub struct StateSnapshot {
    pub commit_lsn: Lsn,
    pub backup_lsn: Lsn,
    pub peers: Vec<PeerInfo>,
}

impl StateSnapshot {
    fn new(read_guard: ReadGuardSharedState, heartbeat_timeout: Duration) -> Self {
        Self {
            commit_lsn: read_guard.sk.state.inmem.commit_lsn,
            backup_lsn: read_guard.sk.state.inmem.backup_lsn,
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
pub async fn main_task(tli: Arc<Timeline>, conf: SafeKeeperConf) {
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

        if conf.is_wal_backup_enabled() {
            wal_backup::update_task(
                &conf,
                ttid,
                wal_seg_size,
                num_computes,
                &state_snapshot,
                &mut backup_task,
            )
            .await;
        }

        // sleeping to make the loop less busy
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
