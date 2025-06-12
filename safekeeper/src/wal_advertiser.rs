mod persistence;
mod pageserver_connectivity;

use utils::id::TenantId;

use crate::timeline::Timeline;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Context;
use tracing::{Instrument, error, info, info_span, warn};
use utils::{
    id::{NodeId, TenantTimelineId},
    lsn::Lsn,
    sync::{spsc_fold, spsc_watch},
};

use crate::{GlobalTimelines, SafeKeeperConf};

type Advs = HashMap<TenantTimelineId, Lsn>;

#[derive(Default)]
pub struct GlobalState {
    inner: once_cell::sync::OnceCell<tokio::sync::mpsc::Sender<Message>>,
}

pub struct SafekeeperTimelineHandle {
    tx: tokio::sync::mpsc::Sender<Message>,
}

enum Message {
    NewTimeline {
        reply: tokio::sync::oneshot::Sender<Result<(), Error>>,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("cancelled")]
    Cancelled,
}

impl GlobalState {
    pub fn task_main(&self) -> impl 'static + Future<Output = anyhow::Result<()>> + Send {
        let mut ret = None;
        self.inner.get_or_init(|| {
            let (tx, task_fut) = MainTask::prepare_run();
            ret = Some(task_fut);
            tx
        });
        ret.expect("must only call this method once")
    }

    pub async fn new_timeline(
        &self,
        tli: Arc<Timeline>,
    ) -> Result<SafekeeperTimelineHandle, Error> {
        let tx = self.inner.get().unwrap().clone();
        let handle = SafekeeperTimelineHandle { tx };
        let (reply, rx) = tokio::sync::oneshot::channel();
        let Ok(()) = handle.tx.send(Message::NewTimeline { reply }).await else {
            return Err(Error::Cancelled);
        };
        let Ok(res) = rx.await else {
            return Err(Error::Cancelled);
        };
        Ok(handle)
    }
    pub fn update_pageserver_attachments(
        &self,
        tenant_id: TenantId,
        update: safekeeper_api::models::TenantShardPageserverAttachmentChange,
    ) -> anyhow::Result<()> {
        todo!()
    }
}
impl SafekeeperTimelineHandle {
    pub fn ready_for_eviction(&self) -> bool {
        todo!()
    }
}

struct MainTask {
    rx: tokio::sync::mpsc::Receiver<Message>,
    world: sk_ps_discovery::World,
    senders: HashMap<utils::id::NodeId, spsc_watch::Sender<Advs>>,
}

impl MainTask {
    fn prepare_run() -> (
        tokio::sync::mpsc::Sender<Message>,
        impl Future<Output = anyhow::Result<()>> + Send,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel(100 /* TODO think */);
        let task = MainTask {
            rx,
            world: sk_ps_discovery::World::default(),
            senders: Default::default(),
        };
        (tx, task.task())
    }
    async fn task(mut self) -> anyhow::Result<()> {
        let mut adv_frequency = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = adv_frequency.tick() => {
                    let start = Instant::now();
                    self.advertisements_iteration();
                    let elapsed = start.elapsed();
                    if elapsed > Duration::from_millis(10) {
                        warn!(?elapsed, "advertisements iteration is slow");
                    }
                },
                message = self.rx.recv() => {
                    match message {
                        None => anyhow::bail!("last main task sender dropped, shouldn't happen, exiting"),
                        Some(_) => todo!(),
                    }
                },
            }
        }
    }

    fn advertisements_iteration(&mut self) {
        loop {
            let advertisements = self.world.get_commit_lsn_advertisements();
            for (node_id, mut advs) in advertisements {
                'inner: loop {
                    let tx = self.senders.entry(node_id).or_insert_with(|| {
                        let (tx, rx) = spsc_watch::channel();
                        tokio::spawn(
                            PageserverTask {
                                ps_id: node_id,
                                endpoint: todo!(),
                                advs: rx,
                            }
                            .run()
                            .instrument(info_span!("wal_advertiser", ps_id=%node_id)),
                        );
                        tx
                    });
                    if let Err((failed, err)) = tx.send_replace(advs) {
                        self.senders.remove(&node_id);
                        advs = failed;
                    } else {
                        break 'inner;
                    }
                }
            }
        }
    }
}
struct PageserverTask {
    ps_id: NodeId,
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
        let stream = async_stream::stream! {
            for (tenant_timeline_id, commit_lsn) in advs {
                yield proto::CommitLsnAdvertisement {tenant_timeline_id: Some(proto::TenantTimelineId {
                    tenant_id: tenant_timeline_id.tenant_id.as_ref().to_owned(),
                    timeline_id: tenant_timeline_id.timeline_id.as_ref().to_owned(),
                }), commit_lsn: commit_lsn.0 };
            }
        };
        let mut client: PageserverClient<_> = PageserverClient::connect(self.endpoint.clone())
            .await
            .context("connect")?;
        let publish_stream = client
            .publish_commit_lsn_advertisements(stream)
            .await
            .context("publish stream")?;
        Ok(())
    }
}
