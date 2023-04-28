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
            if let Err(e) = tli
                .remove_old_wal(conf.wal_backup_enabled)
                .instrument(info_span!("", tenant = %ttid.tenant_id, timeline = %ttid.timeline_id))
                .await
            {
                error!("failed to remove WAL: {}", e);
            }
        }
        sleep(wal_removal_interval).await;
    }
}
