use std::collections::HashMap;

use utils::{
    id::TenantTimelineId,
    sync::{spsc_fold, spsc_watch},
};

use crate::{GlobalTimelines, SafeKeeperConf};

pub async fn task_main(
    conf: Arc<SafeKeeperConf>,
    global_timelines: Arc<GlobalTimelines>,
) -> anyhow::Result<()> {
    let mut world = sk_ps_discovery::World::default();

    let mut senders: HashMap<NodeId> = HashMap::new();
    loop {
        let advertisements = world.get_commit_lsn_advertisements();
        for (node_id, advs) in advertisements {
            loop {
                let tx = senders.entry(node_id).or_insert_with(|| {
                    let (tx, rx) = spsc_watch::channel(advs);
                    tokio::spawn(
                        NodeTask {
                            ps_id: node_id,
                            advs: rx,
                        }
                        .run()
                        .instrument(info_span!("wal_advertiser", ps_id=%node_id)),
                    );
                    tx
                });
                if let Err(err) = tx.send_modify(advs) {
                    senders.remove(&node_id);
                }
            }
        }
    }
}

struct PageserverTask {
    ps_id: NodeId,
    advs: spsc_watch::Receiver<HashMap<TenantTimelineId, Lsn>>,
}

impl PageserverTask {
    /// Cancellation: happens through last PageserverHandle being dropped.
    async fn run(mut self) {
        let mut current;
        loop {
            let res = self.run0(advs).await;
            match res {
                Ok(()) => {}
                Err(err) => {
                    error!(?err, "error sending advertisements");
                    // TODO: proper backoff?
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
    async fn run0(&mut self, advs: HashMap<TenantTimelineId, Lsn>) -> anyhow::Result<()> {
        use storage_broker::wal_advertisement as proto;
        use storage_broker::wal_advertisement::pageserver_client::PageserverClient;
        let stream = async_stream::stream! { loop {
            for (tenant_timeline_id, commit_lsn) in advs {
                yield proto::CommitLsnAdvertisement {tenant_timeline_id: Some(tenant_timeline_id), commit_lsn: Some(commit_lsn) };
            }
        }};
        let client: PageserverClient<_> =
            PageserverClient::connect(todo!("how do we learn pageserver hostnames?"))
                .await
                .context("connect")?;
        let publish_stream = client
            .publish_commit_lsn_advertisements(stream)
            .await
            .context("publish stream")?;
    }
}

struct GlobalState {}

pub mod advmap {
    use std::sync::Arc;

    use utils::id::TenantId;

    use crate::timeline::Timeline;

    pub struct World {}
    pub struct SafekeeperTimelineHandle {}

    impl World {
        pub fn update_pageserver_attachments(
            &self,
            tenant_id: TenantId,
            update: safekeeper_api::models::TenantShardPageserverAttachmentChange,
        ) -> anyhow::Result<()> {
            todo!()
        }
        pub fn register_timeline(
            &self,
            tli: Arc<Timeline>,
        ) -> anyhow::Result<SafekeeperTimelineHandle> {
            todo!()
        }
    }
    impl SafekeeperTimelineHandle {
        pub fn ready_for_eviction(&self) -> bool {
            todo!()
        }
    }
    impl Default for World {
        fn default() -> Self {
            todo!()
        }
    }
}
