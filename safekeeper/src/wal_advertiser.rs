//! Advertise pending WAL to all pageservers that might be interested in it.

use std::{collections::HashSet, sync::Arc, time::Duration};

use crate::{GlobalTimelines, SafeKeeperConf};

pub(crate) mod advmap;

pub(crate) async fn wal_advertiser_loop(
    conf: Arc<SafeKeeperConf>,
    global_timelines: Arc<GlobalTimelines>,
) -> anyhow::Result<()> {
    todo!();
    node_loop().await;
    Ok(())
}

pub(crate) async fn node_loop() {
    loop {}
}
