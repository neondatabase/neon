use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context;
use tracing::{Instrument, error, info, info_span};
use utils::{
    id::{NodeId, TenantTimelineId},
    lsn::Lsn,
    sync::{spsc_fold, spsc_watch},
};

use crate::{GlobalTimelines, SafeKeeperConf};

type Advs = HashMap<TenantTimelineId, Lsn>;

pub async fn task_main(
    conf: Arc<SafeKeeperConf>,
    global_timelines: Arc<GlobalTimelines>,
) -> anyhow::Result<()> {
    let mut world = sk_ps_discovery::World::default();

    let mut senders: HashMap<utils::id::NodeId, spsc_watch::Sender<Advs>> = HashMap::new();
    let mut endpoints: HashMap<utils::id::NodeId, tonic::transport::Endpoint> = HashMap::new();
    loop {
        let advertisements = world.get_commit_lsn_advertisements();
        for (node_id, mut advs) in advertisements {
            'inner: loop {
                let tx = senders.entry(node_id).or_insert_with(|| {
                    let (tx, rx) = spsc_watch::channel();
                    tokio::spawn(
                        PageserverTask {
                            ps_id: node_id,
                            advs: rx,
                        }
                        .run()
                        .instrument(info_span!("wal_advertiser", ps_id=%node_id)),
                    );
                    tx
                });
                if let Err((failed, err)) = tx.send_replace(advs) {
                    senders.remove(&node_id);
                    advs = failed;
                } else {
                    break 'inner;
                }
            }
        }
    }
}

struct PageserverTask {
    ps_id: NodeId,
    endpoint: tonic::transport::Endpoint,
    advs: spsc_watch::Receiver<Advs>,
}

impl PageserverTask {
    /// Cancellation: happens through last PageserverHandle being dropped.
    async fn run(mut self) {
        loop {
            let Ok(advs) = self.advs.recv().await else {
                info!("main task gone, exiting");
                return;
            };
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
                yield proto::CommitLsnAdvertisement {tenant_timeline_id: Some(proto::TenantTimelineId {
                    tenant_id: tenant_timeline_id.tenant_id.as_ref().to_owned(),
                    timeline_id: tenant_timeline_id.timeline_id.as_ref().to_owned(),
                }), commit_lsn: commit_lsn.0 };
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

#[derive(Default)]
pub struct GlobalState {}

use utils::id::TenantId;

use crate::timeline::Timeline;

pub struct World {}
pub struct SafekeeperTimelineHandle {}

impl GlobalState {
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
