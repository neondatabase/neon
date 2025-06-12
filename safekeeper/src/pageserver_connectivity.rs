use desim::world::Node;
use hyper::Uri;
use pageserver_api::controller_api;
use utils::id::TenantId;

use crate::timeline::Timeline;

use std::{
    collections::{HashMap, hash_map},
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

enum Message {
    Resolve {
        ps_id: NodeId,
        reply: tokio::sync::oneshot::Sender<tokio::sync::watch::Receiver<hyper::Uri>>,
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
}

struct MainTask {
    rx: tokio::sync::mpsc::Receiver<Message>,
}

impl MainTask {
    fn prepare_run() -> (
        tokio::sync::mpsc::Sender<Message>,
        impl Future<Output = anyhow::Result<()>> + Send,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel(100 /* TODO think */);
        let task = MainTask { rx };
        (tx, task.task())
    }
    async fn task(mut self) -> anyhow::Result<()> {
        // TODO: persistence

        let storcon_client = todo!();

        let mut resolution: HashMap<NodeId, tokio::sync::watch::Sender<hyper::Uri>> =
            HashMap::new();

        while let Some(rx) = self.rx.recv().await {
            match rx {
                Message::Resolve { ps_id, reply } => match resolution.entry(ps_id) {
                    hash_map::Entry::Occupied(e) => {}
                    hash_map::Entry::Vacant(e) => {
                        tokio::spawn(
                            ResolutionTask { ps_id, storcon_client }.run()
                        )
                    },
                },
            }
        }
    }
}

struct ResolutionTask {
    ps_id: NodeId,
    storcon_client: storage_controller_client::control_api::Client,
}

impl ResolutionTask {
    pub async fn run(self) -> Result<Uri, Error> {
        loop {
            // XXX: well-defined upcall API?
            let res = self
                .storcon_client
                .dispatch(
                    reqwest::Method::GET,
                    format!("control/v1/node/{}", self.node_id),
                    None,
                )
                .await;
            let node: NodeDescribeResponse = match res {
                Ok(res) => res,
                Err(err) => {
                    warn!("storcon upcall failed")
                }
            };
        }
    }
}
