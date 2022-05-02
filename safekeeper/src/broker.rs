//! Communication with etcd, providing safekeeper peers and pageserver coordination.

use anyhow::anyhow;
use anyhow::Context;
use anyhow::Error;
use anyhow::Result;
use etcd_broker::subscription_value::SkTimelineInfo;
use std::time::Duration;
use tokio::spawn;
use tokio::task::JoinHandle;
use tokio::{runtime, time::sleep};
use tracing::*;
use url::Url;

use crate::{timeline::GlobalTimelines, SafeKeeperConf};
use etcd_broker::{
    subscription_key::{OperationKind, SkOperationKind, SubscriptionKey},
    Client, PutOptions,
};
use utils::zid::{NodeId, ZTenantTimelineId};

const RETRY_INTERVAL_MSEC: u64 = 1000;
const PUSH_INTERVAL_MSEC: u64 = 1000;
const LEASE_TTL_SEC: i64 = 5;

pub fn thread_main(conf: SafeKeeperConf) {
    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let _enter = info_span!("broker").entered();
    info!("started, broker endpoints {:?}", conf.broker_endpoints);

    runtime.block_on(async {
        main_loop(conf).await;
    });
}

/// Key to per timeline per safekeeper data.
fn timeline_safekeeper_path(
    broker_etcd_prefix: String,
    zttid: ZTenantTimelineId,
    sk_id: NodeId,
) -> String {
    format!(
        "{}/{sk_id}",
        SubscriptionKey::sk_timeline_info(broker_etcd_prefix, zttid).watch_key()
    )
}

pub struct Election {
    pub election_name: String,
    pub candidate_name: String,
    pub broker_endpoints: Vec<Url>,
}

impl Election {
    pub fn new(election_name: String, candidate_name: String, broker_endpoints: Vec<Url>) -> Self {
        Self {
            election_name,
            candidate_name,
            broker_endpoints,
        }
    }
}

pub struct ElectionLeader {
    client: Client,
    keep_alive: JoinHandle<Result<()>>,
}

impl ElectionLeader {
    pub async fn check_am_i(
        &mut self,
        election_name: String,
        candidate_name: String,
    ) -> Result<bool> {
        let resp = self.client.leader(election_name).await?;

        let kv = resp.kv().ok_or(anyhow!("failed to get leader response"))?;
        let leader = kv.value_str()?;

        Ok(leader == candidate_name)
    }

    pub async fn give_up(self) {
        self.keep_alive.abort();
        // TODO: it'll be wise to resign here but it'll happen after lease expiration anyway
        // should we await for keep alive termination?
        let _ = self.keep_alive.await;
    }
}

pub async fn get_leader(req: &Election, leader: &mut Option<ElectionLeader>) -> Result<()> {
    let mut client = Client::connect(req.broker_endpoints.clone(), None)
        .await
        .context("Could not connect to etcd")?;

    let lease = client
        .lease_grant(LEASE_TTL_SEC, None)
        .await
        .context("Could not acquire a lease");

    let lease_id = lease.map(|l| l.id()).unwrap();

    // kill previous keepalive, if any
    if let Some(l) = leader.take() {
        l.give_up().await;
    }

    let keep_alive = spawn::<_>(lease_keep_alive(client.clone(), lease_id));
    // immediately save handle to kill task if we get canceled below
    *leader = Some(ElectionLeader {
        client: client.clone(),
        keep_alive,
    });

    client
        .campaign(
            req.election_name.clone(),
            req.candidate_name.clone(),
            lease_id,
        )
        .await?;

    Ok(())
}

async fn lease_keep_alive(mut client: Client, lease_id: i64) -> Result<()> {
    let (mut keeper, mut ka_stream) = client
        .lease_keep_alive(lease_id)
        .await
        .context("failed to create keepalive stream")?;

    loop {
        let push_interval = Duration::from_millis(PUSH_INTERVAL_MSEC);

        keeper
            .keep_alive()
            .await
            .context("failed to send LeaseKeepAliveRequest")?;

        ka_stream
            .message()
            .await
            .context("failed to receive LeaseKeepAliveResponse")?;

        sleep(push_interval).await;
    }
}

pub fn get_candiate_name(system_id: NodeId) -> String {
    format!("id_{system_id}")
}

/// Push once in a while data about all active timelines to the broker.
async fn push_loop(conf: SafeKeeperConf) -> anyhow::Result<()> {
    let mut client = Client::connect(&conf.broker_endpoints, None).await?;

    // Get and maintain lease to automatically delete obsolete data
    let lease = client.lease_grant(LEASE_TTL_SEC, None).await?;
    let (mut keeper, mut ka_stream) = client.lease_keep_alive(lease.id()).await?;

    let push_interval = Duration::from_millis(PUSH_INTERVAL_MSEC);
    loop {
        // Note: we lock runtime here and in timeline methods as GlobalTimelines
        // is under plain mutex. That's ok, all this code is not performance
        // sensitive and there is no risk of deadlock as we don't await while
        // lock is held.
        for zttid in GlobalTimelines::get_active_timelines() {
            if let Some(tli) = GlobalTimelines::get_loaded(zttid) {
                let sk_info = tli.get_public_info(&conf)?;
                let put_opts = PutOptions::new().with_lease(lease.id());
                client
                    .put(
                        timeline_safekeeper_path(
                            conf.broker_etcd_prefix.clone(),
                            zttid,
                            conf.my_id,
                        ),
                        serde_json::to_string(&sk_info)?,
                        Some(put_opts),
                    )
                    .await
                    .context("failed to push safekeeper info")?;
            }
        }
        // revive the lease
        keeper
            .keep_alive()
            .await
            .context("failed to send LeaseKeepAliveRequest")?;
        ka_stream
            .message()
            .await
            .context("failed to receive LeaseKeepAliveResponse")?;
        sleep(push_interval).await;
    }
}

/// Subscribe and fetch all the interesting data from the broker.
async fn pull_loop(conf: SafeKeeperConf) -> Result<()> {
    let mut client = Client::connect(&conf.broker_endpoints, None).await?;

    let mut subscription = etcd_broker::subscribe_for_values(
        &mut client,
        SubscriptionKey::all(conf.broker_etcd_prefix.clone()),
        |full_key, value_str| {
            if full_key.operation == OperationKind::Safekeeper(SkOperationKind::TimelineInfo) {
                match serde_json::from_str::<SkTimelineInfo>(value_str) {
                    Ok(new_info) => return Some(new_info),
                    Err(e) => {
                        error!("Failed to parse timeline info from value str '{value_str}': {e}")
                    }
                }
            }
            None
        },
    )
    .await
    .context("failed to subscribe for safekeeper info")?;
    loop {
        match subscription.value_updates.recv().await {
            Some(new_info) => {
                // note: there are blocking operations below, but it's considered fine for now
                if let Ok(tli) = GlobalTimelines::get(&conf, new_info.key.id, false) {
                    tli.record_safekeeper_info(&new_info.value, new_info.key.node_id)
                        .await?
                }
            }
            None => {
                // XXX it means we lost connection with etcd, error is consumed inside sub object
                debug!("timeline updates sender closed, aborting the pull loop");
                return Ok(());
            }
        }
    }
}

async fn main_loop(conf: SafeKeeperConf) {
    let mut ticker = tokio::time::interval(Duration::from_millis(RETRY_INTERVAL_MSEC));
    let mut push_handle: Option<JoinHandle<Result<(), Error>>> = None;
    let mut pull_handle: Option<JoinHandle<Result<(), Error>>> = None;
    // Selecting on JoinHandles requires some squats; is there a better way to
    // reap tasks individually?

    // Handling failures in task itself won't catch panic and in Tokio, task's
    // panic doesn't kill the whole executor, so it is better to do reaping
    // here.
    loop {
        tokio::select! {
                res = async { push_handle.as_mut().unwrap().await }, if push_handle.is_some() => {
                    // was it panic or normal error?
                    let err = match res {
                        Ok(res_internal) => res_internal.unwrap_err(),
                        Err(err_outer) => err_outer.into(),
                    };
                    warn!("push task failed: {:?}", err);
                    push_handle = None;
                },
                res = async { pull_handle.as_mut().unwrap().await }, if pull_handle.is_some() => {
                    // was it panic or normal error?
                    match res {
                        Ok(res_internal) => if let Err(err_inner) = res_internal {
                            warn!("pull task failed: {:?}", err_inner);
                        }
                        Err(err_outer) => { warn!("pull task panicked: {:?}", err_outer) }
                    };
                    pull_handle = None;
                },
                _ = ticker.tick() => {
                    if push_handle.is_none() {
                        push_handle = Some(tokio::spawn(push_loop(conf.clone())));
                    }
                    if pull_handle.is_none() {
                        pull_handle = Some(tokio::spawn(pull_loop(conf.clone())));
                    }
            }
        }
    }
}
