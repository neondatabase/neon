//! Thread removing old WAL.

use std::time::Duration;

use tokio::time::sleep;
use tracing::*;

use crate::{GlobalTimelines, SafeKeeperConf};

const ALLOW_INACTIVE_TIMELINES: bool = true;

pub async fn task_main(_conf: SafeKeeperConf) -> anyhow::Result<()> {
    let wal_removal_interval = Duration::from_millis(5000);
    loop {
        let now = tokio::time::Instant::now();
        let mut active_timelines = 0;

        let tlis = GlobalTimelines::get_all();
        for tli in &tlis {
            let is_active = tli.is_active().await;
            if is_active {
                active_timelines += 1;
            }
            if !ALLOW_INACTIVE_TIMELINES && !is_active {
                continue;
            }
            let ttid = tli.ttid;
            async {
                if let Err(e) = tli.maybe_persist_control_file().await {
                    warn!("failed to persist control file: {e}");
                }
                if let Err(e) = tli.remove_old_wal().await {
                    error!("failed to remove WAL: {}", e);
                }
            }
            .instrument(info_span!("WAL removal", ttid = %ttid))
            .await;
        }

        let elapsed = now.elapsed();
        let total_timelines = tlis.len();

        if elapsed > wal_removal_interval {
            info!(
                "WAL removal is too long, processed {} active timelines ({} total) in {:?}",
                active_timelines, total_timelines, elapsed
            );
        }

        sleep(wal_removal_interval).await;
    }
}
