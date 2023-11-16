//! Thread removing old WAL.

use std::time::Duration;

use tokio::time::sleep;
use tracing::*;

use crate::{GlobalTimelines, SafeKeeperConf};

pub async fn task_main(conf: SafeKeeperConf) -> anyhow::Result<()> {
    let wal_removal_interval = Duration::from_millis(5000);
    loop {
        let tlis = GlobalTimelines::get_all();
        for tli in &tlis {
            if !tli.is_active().await {
                continue;
            }
            let ttid = tli.ttid;
            async {
                if let Err(e) = tli.maybe_persist_control_file().await {
                    warn!("failed to persist control file: {e}");
                }
                if let Err(e) = tli.remove_old_wal(conf.wal_backup_enabled).await {
                    error!("failed to remove WAL: {}", e);
                }
            }
            .instrument(info_span!("WAL removal", ttid = %ttid))
            .await;
        }
        sleep(wal_removal_interval).await;
    }
}
