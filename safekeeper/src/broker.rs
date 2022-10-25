//! Communication with etcd, providing safekeeper peers and pageserver coordination.

use anyhow::Context;
use anyhow::Error;
use anyhow::Result;
use etcd_broker::subscription_value::SkTimelineInfo;
use etcd_broker::LeaseKeepAliveStream;
use etcd_broker::LeaseKeeper;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::{runtime, time::sleep};
use tracing::*;

use crate::GlobalTimelines;
use crate::SafeKeeperConf;
use etcd_broker::{
    subscription_key::{OperationKind, SkOperationKind, SubscriptionKey},
    Client, PutOptions,
};
use utils::id::{NodeId, TenantTimelineId};

const RETRY_INTERVAL_MSEC: u64 = 1000;
const PUSH_INTERVAL_MSEC: u64 = 1000;
const LEASE_TTL_SEC: i64 = 10;

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
    ttid: TenantTimelineId,
    sk_id: NodeId,
) -> String {
    format!(
        "{}/{sk_id}",
        SubscriptionKey::sk_timeline_info(broker_etcd_prefix, ttid).watch_key()
    )
}

async fn push_sk_info(
    ttid: TenantTimelineId,
    mut client: Client,
    key: String,
    sk_info: SkTimelineInfo,
    mut lease: Lease,
) -> anyhow::Result<(TenantTimelineId, Lease)> {
    let put_opts = PutOptions::new().with_lease(lease.id);
    client
        .put(
            key.clone(),
            serde_json::to_string(&sk_info)?,
            Some(put_opts),
        )
        .await
        .with_context(|| format!("failed to push safekeeper info to {}", key))?;

    // revive the lease
    lease
        .keeper
        .keep_alive()
        .await
        .context("failed to send LeaseKeepAliveRequest")?;
    lease
        .ka_stream
        .message()
        .await
        .context("failed to receive LeaseKeepAliveResponse")?;

    Ok((ttid, lease))
}

struct Lease {
    id: i64,
    keeper: LeaseKeeper,
    ka_stream: LeaseKeepAliveStream,
}

/// Push once in a while data about all active timelines to the broker.
async fn push_loop(conf: SafeKeeperConf) -> anyhow::Result<()> {
    let mut client = Client::connect(&conf.broker_endpoints, None).await?;
    let mut leases: HashMap<TenantTimelineId, Lease> = HashMap::new();

    let push_interval = Duration::from_millis(PUSH_INTERVAL_MSEC);
    loop {
        // Note: we lock runtime here and in timeline methods as GlobalTimelines
        // is under plain mutex. That's ok, all this code is not performance
        // sensitive and there is no risk of deadlock as we don't await while
        // lock is held.
        let mut active_tlis = GlobalTimelines::get_all();
        active_tlis.retain(|tli| tli.is_active());

        let active_tlis_set: HashSet<TenantTimelineId> =
            active_tlis.iter().map(|tli| tli.ttid).collect();

        // // Get and maintain (if not yet) per timeline lease to automatically delete obsolete data.
        for tli in &active_tlis {
            if let Entry::Vacant(v) = leases.entry(tli.ttid) {
                let lease = client.lease_grant(LEASE_TTL_SEC, None).await?;
                let (keeper, ka_stream) = client.lease_keep_alive(lease.id()).await?;
                v.insert(Lease {
                    id: lease.id(),
                    keeper,
                    ka_stream,
                });
            }
        }
        leases.retain(|ttid, _| active_tlis_set.contains(ttid));

        // Push data concurrently to not suffer from latency, with many timelines it can be slow.
        let handles = active_tlis
            .iter()
            .map(|tli| {
                let sk_info = tli.get_safekeeper_info(&conf);
                let key =
                    timeline_safekeeper_path(conf.broker_etcd_prefix.clone(), tli.ttid, conf.my_id);
                let lease = leases.remove(&tli.ttid).unwrap();
                tokio::spawn(push_sk_info(tli.ttid, client.clone(), key, sk_info, lease))
            })
            .collect::<Vec<_>>();
        for h in handles {
            let (ttid, lease) = h.await??;
            // It is ugly to pull leases from hash and then put it back, but
            // otherwise we have to resort to long living per tli tasks (which
            // would generate a lot of errors when etcd is down) as task wants to
            // have 'static objects, we can't borrow to it.
            leases.insert(ttid, lease);
        }

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
                if let Ok(tli) = GlobalTimelines::get(new_info.key.id) {
                    // Note that we also receive *our own* info. That's
                    // important, as it is used as an indication of live
                    // connection to the broker.
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
