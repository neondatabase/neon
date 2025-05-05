//! Advertise pending WAL to all pageservers that might be interested in it.

use std::{collections::HashSet, sync::Arc, time::Duration};

use desim::world::Node;

use crate::{GlobalTimelines, SafeKeeperConf};

mod advmap;

pub struct GlobalState {
    pub resolver: NodeIdToHostportMap,
}

#[derive(Default)]
pub struct NodeIdToHostportMap {
    inner: Arc<Mutex<HashMap<NodeId, String>>>,
}

impl NodeIdToHostportMap {
    pub fn learn(&self, ) {
        let mut inner = self.inner.lock().unwrap();
        inner.insert(node_id, hostport);
    }
}

pub(crate) async fn wal_advertiser_loop(
    conf: Arc<SafeKeeperConf>,
    global_timelines: Arc<GlobalTimelines>,
) -> anyhow::Result<()> {
    let active_timelines_set = global_timelines.get_global_broker_active_set();

    let push_interval = Duration::from_millis(PUSH_INTERVAL_MSEC);

    let outbound = async_stream::stream! {
        loop {
            // Note: we lock runtime here and in timeline methods as GlobalTimelines
            // is under plain mutex. That's ok, all this code is not performance
            // sensitive and there is no risk of deadlock as we don't await while
            // lock is held.
            let now = Instant::now();
            let all_tlis = active_timelines_set.get_all();
            let mut n_pushed_tlis = 0;
            for tli in &all_tlis {
                let sk_info = tli.get_safekeeper_info(&conf).await;
                yield sk_info;
                BROKER_PUSHED_UPDATES.inc();
                n_pushed_tlis += 1;
            }
            let elapsed = now.elapsed();

            BROKER_PUSH_ALL_UPDATES_SECONDS.observe(elapsed.as_secs_f64());
            BROKER_ITERATION_TIMELINES.observe(n_pushed_tlis as f64);

            if elapsed > push_interval / 2 {
                info!("broker push is too long, pushed {} timeline updates to broker in {:?}", n_pushed_tlis, elapsed);
            }

            sleep(push_interval).await;
        }
    };
    client
        .publish_safekeeper_info(Request::new(outbound))
        .await?;
    Ok(())
}

pub(crate) async fn node_loop() {


    loop {

    }


}
