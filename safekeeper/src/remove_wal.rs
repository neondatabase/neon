//! Thread removing old WAL.

use std::{thread, time::Duration};

use tracing::*;

use crate::{GlobalTimelines, SafeKeeperConf};

pub fn thread_main(conf: SafeKeeperConf) {
    let wal_removal_interval = Duration::from_millis(5000);
    loop {
        let tlis = GlobalTimelines::get_all();
        for tli in &tlis {
            if !tli.is_active() {
                continue;
            }
            let zttid = tli.zttid;
            let _enter =
                info_span!("", tenant = %zttid.tenant_id, timeline = %zttid.timeline_id).entered();
            if let Err(e) = tli.remove_old_wal(conf.wal_backup_enabled) {
                warn!("failed to remove WAL: {}", e);
            }
        }
        thread::sleep(wal_removal_interval)
    }
}
