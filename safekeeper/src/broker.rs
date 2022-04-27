//! Communication with etcd, providing safekeeper peers and pageserver coordination.

use anyhow::Context;
use anyhow::Error;
use anyhow::Result;
use etcd_broker::Client;
use etcd_broker::PutOptions;
use etcd_broker::SkTimelineSubscriptionKind;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::{runtime, time::sleep};
use tracing::*;

use crate::{timeline::GlobalTimelines, SafeKeeperConf};
use utils::zid::{ZNodeId, ZTenantTimelineId};

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
    broker_prefix: String,
    zttid: ZTenantTimelineId,
    sk_id: ZNodeId,
) -> String {
    format!(
        "{}/{sk_id}",
        SkTimelineSubscriptionKind::timeline(broker_prefix, zttid).watch_key()
    )
}

/// get a lease with default ttl
pub async fn get_lease(conf: &SafeKeeperConf) -> Result<i64> {
    info!("Getting a etcd lease {:?}", conf);

    let mut client = Client::connect(conf.broker_endpoints.as_ref().unwrap(), None)
        .await
        .context("Could not connect to etcd")?;

    let lease = client
        .lease_grant(LEASE_TTL_SEC, None)
        .await
        .context("Could not acquire a lease");

    lease.map(|l| l.id())
}

// create keepalive task with default heatbeat interval
pub async fn lease_keep_alive(lease_id: i64, conf: SafeKeeperConf) -> Result<()> {
    let mut client = Client::connect(conf.broker_endpoints.as_ref().unwrap(), None)
        .await
        .context("failed to get etcd client")?;

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

pub fn get_campaign_name(
    election_name: String,
    broker_prefix: String,
    timeline_id: &ZTenantTimelineId,
) -> String {
    return format!(
        "{}/{}",
        SkTimelineSubscriptionKind::timeline(broker_prefix, *timeline_id).watch_key(),
        election_name
    );
}

pub fn get_candiate_name(conf: &SafeKeeperConf) -> String {
    format!("id_{}", conf.my_id)
}

pub async fn is_leader(
    election_name: String,
    timeline_id: &ZTenantTimelineId,
    conf: &SafeKeeperConf,
) -> Result<bool> {
    let mut client = Client::connect(conf.broker_endpoints.as_ref().unwrap(), None)
        .await
        .context("failed to get etcd client")?;

    let campaign_name =
        get_campaign_name(election_name, conf.broker_etcd_prefix.clone(), timeline_id);
    let resp = client.leader(campaign_name).await?;

    let kv = resp.kv().expect("failed to get leader response");
    let leader = kv.value_str().expect("failed to get campaign leader value");

    let my_candidate_name = get_candiate_name(conf);
    Ok(leader == my_candidate_name)
}

/// create an election for a given timeline and wait to become a leader
pub async fn become_leader(
    lease_id: i64,
    election_name: String,
    timeline_id: &ZTenantTimelineId,
    conf: &SafeKeeperConf,
) -> Result<()> {
    let mut client = Client::connect(conf.broker_endpoints.as_ref().unwrap(), None).await?;

    info!(
        "Electing a leader for {} timeline {}",
        election_name, timeline_id
    );

    let campaign_name = get_campaign_name(
        election_name.clone(),
        conf.broker_etcd_prefix.clone(),
        timeline_id,
    );
    let candidate_name = get_candiate_name(conf);

    let resp = client
        .campaign(campaign_name, candidate_name, lease_id)
        .await?;
    let leader = resp.leader().unwrap();

    info!(
        "Leader elected for {} on timeline {}. leader id {:?}",
        election_name,
        timeline_id,
        leader.key_str()
    );

    Ok(())
}

/// Push once in a while data about all active timelines to the broker.
async fn push_loop(conf: SafeKeeperConf) -> anyhow::Result<()> {
    let mut client = Client::connect(&conf.broker_endpoints.as_ref().unwrap(), None).await?;

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
            if let Ok(tli) = GlobalTimelines::get(&conf, zttid, false) {
                let sk_info = tli.get_public_info()?;
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
    let mut client = Client::connect(&conf.broker_endpoints.as_ref().unwrap(), None).await?;

    let mut subscription = etcd_broker::subscribe_to_safekeeper_timeline_updates(
        &mut client,
        SkTimelineSubscriptionKind::all(conf.broker_etcd_prefix.clone()),
    )
    .await
    .context("failed to subscribe for safekeeper info")?;

    loop {
        match subscription.fetch_data().await {
            Some(new_info) => {
                for (zttid, sk_info) in new_info {
                    // note: there are blocking operations below, but it's considered fine for now
                    if let Ok(tli) = GlobalTimelines::get(&conf, zttid, false) {
                        for (safekeeper_id, info) in sk_info {
                            tli.record_safekeeper_info(&info, safekeeper_id)?
                        }
                    }
                }
            }
            None => {
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
                    let err = match res {
                        Ok(res_internal) => res_internal.unwrap_err(),
                        Err(err_outer) => err_outer.into(),
                    };
                    warn!("pull task failed: {:?}", err);
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
