//! This module implements pulling WAL from peer safekeepers if compute can't
//! provide it, i.e. safekeeper lags too much.

use std::sync::Arc;

use tokio::{select, time::sleep, time::Duration};
use tracing::{info, instrument};

use crate::{timeline::Timeline, SafeKeeperConf};

/// Entrypoint for per timeline task which always runs, checking whether
/// recovery for this safekeeper is needed and starting it if so.
#[instrument(name = "recovery task", skip_all, fields(ttid = %tli.ttid))]
pub async fn recovery_main(tli: Arc<Timeline>, _conf: SafeKeeperConf) {
    info!("started");
    let mut cancellation_rx = match tli.get_cancellation_rx() {
        Ok(rx) => rx,
        Err(_) => {
            info!("timeline canceled during task start");
            return;
        }
    };

    select! {
        _ = recovery_main_loop(tli) => { unreachable!() }
        _ = cancellation_rx.changed() => {
            info!("stopped");
        }
    }
}

const CHECK_INTERVAL_MS: u64 = 2000;

/// Check regularly whether we need to start recovery.
async fn recovery_main_loop(_tli: Arc<Timeline>) {
    let check_duration = Duration::from_millis(CHECK_INTERVAL_MS);
    loop {
        sleep(check_duration).await;
    }
}
