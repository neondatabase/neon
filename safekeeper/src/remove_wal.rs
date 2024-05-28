//! Thread removing old WAL.

use std::time::Duration;

use tokio::time::sleep;
use tracing::*;

use crate::{GlobalTimelines, SafeKeeperConf};

pub async fn task_main(_conf: SafeKeeperConf) -> anyhow::Result<()> {
    let wal_removal_interval = Duration::from_millis(5000);
    loop {
        let now = tokio::time::Instant::now();
        let tlis = GlobalTimelines::get_all();
        for tli in &tlis {
            let ttid = tli.ttid;
            async {
                if let Err(e) = tli.maybe_persist_control_file(false).await {
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
                "WAL removal is too long, processed {} timelines in {:?}",
                total_timelines, elapsed
            );
        }

        sleep(wal_removal_interval).await;
    }
}
