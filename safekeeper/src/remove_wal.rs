//! Thread removing old WAL.

use std::{thread, time::Duration};

use tracing::*;

use crate::{timeline::GlobalTimelines, SafeKeeperConf};

pub fn thread_main(conf: SafeKeeperConf) {
    let wal_removal_interval = Duration::from_millis(5000);
    loop {
        let active_tlis = GlobalTimelines::get_active_timelines();
        for zttid in &active_tlis {
            if let Ok(tli) = GlobalTimelines::get(&conf, *zttid, false) {
                if let Err(e) = tli.remove_old_wal(conf.s3_offload_enabled) {
                    warn!(
                        "failed to remove WAL for tenant {} timeline {}: {}",
                        tli.zttid.tenant_id, tli.zttid.timeline_id, e
                    );
                }
            }
        }
        thread::sleep(wal_removal_interval)
    }
}
