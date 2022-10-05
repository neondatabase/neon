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
            let ttid = tli.ttid;
            let _enter =
                info_span!("", tenant = %ttid.tenant_id, timeline = %ttid.timeline_id).entered();
            if let Err(e) = tli.remove_old_wal(conf.wal_backup_enabled) {
                warn!("failed to remove WAL: {}", e);
            }
        }
        thread::sleep(wal_removal_interval)
    }
}
