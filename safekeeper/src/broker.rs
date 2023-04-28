//! Communication with the broker, providing safekeeper peers and pageserver coordination.

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;

use anyhow::Error;
use anyhow::Result;

use storage_broker::parse_proto_ttid;
use storage_broker::proto::broker_service_client::BrokerServiceClient;
use storage_broker::proto::subscribe_safekeeper_info_request::SubscriptionKey as ProtoSubscriptionKey;
use storage_broker::proto::SubscribeSafekeeperInfoRequest;
use storage_broker::Request;

use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::*;

use crate::GlobalTimelines;
use crate::SafeKeeperConf;

const RETRY_INTERVAL_MSEC: u64 = 1000;
const PUSH_INTERVAL_MSEC: u64 = 1000;

/// Push once in a while data about all active timelines to the broker.
async fn push_loop(conf: SafeKeeperConf) -> anyhow::Result<()> {
    let mut client = BrokerServiceClient::connect(conf.broker_endpoint.clone()).await?;
    let push_interval = Duration::from_millis(PUSH_INTERVAL_MSEC);

    let outbound = async_stream::stream! {
        loop {
            // Note: we lock runtime here and in timeline methods as GlobalTimelines
            // is under plain mutex. That's ok, all this code is not performance
            // sensitive and there is no risk of deadlock as we don't await while
            // lock is held.
            let all_tlis = GlobalTimelines::get_all();
            for tli in &all_tlis {
                // filtering alternative futures::stream::iter(all_tlis)
                //   .filter(|tli| {let tli = tli.clone(); async move { tli.is_active().await}}).collect::<Vec<_>>().await;
                // doesn't look better, and I'm not sure how to do that without collect.
                if !tli.is_active().await {
                    continue;
                }
                let sk_info = tli.get_safekeeper_info(&conf).await;
                yield sk_info;
            }
            sleep(push_interval).await;
        }
    };
    client
        .publish_safekeeper_info(Request::new(outbound))
        .await?;
    Ok(())
}

/// Subscribe and fetch all the interesting data from the broker.
async fn pull_loop(conf: SafeKeeperConf) -> Result<()> {
    let mut client = storage_broker::connect(conf.broker_endpoint, conf.broker_keepalive_interval)?;

    // TODO: subscribe only to local timelines instead of all
    let request = SubscribeSafekeeperInfoRequest {
        subscription_key: Some(ProtoSubscriptionKey::All(())),
    };

    let mut stream = client
        .subscribe_safekeeper_info(request)
        .await
        .context("subscribe_safekeper_info request failed")?
        .into_inner();

    while let Some(msg) = stream.message().await? {
        let proto_ttid = msg
            .tenant_timeline_id
            .as_ref()
            .ok_or_else(|| anyhow!("missing tenant_timeline_id"))?;
        let ttid = parse_proto_ttid(proto_ttid)?;
        if let Ok(tli) = GlobalTimelines::get(ttid) {
            // Note that we also receive *our own* info. That's
            // important, as it is used as an indication of live
            // connection to the broker.

            // note: there are blocking operations below, but it's considered fine for now
            tli.record_safekeeper_info(msg).await?
        }
    }
    bail!("end of stream");
}

pub async fn task_main(conf: SafeKeeperConf) -> anyhow::Result<()> {
    info!("started, broker endpoint {:?}", conf.broker_endpoint);

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
