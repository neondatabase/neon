//! Communication with etcd, providing safekeeper peers and pageserver coordination.

use anyhow::bail;
use anyhow::Context;
use anyhow::Error;
use anyhow::Result;
use etcd_client::Client;
use etcd_client::EventType;
use etcd_client::PutOptions;
use etcd_client::WatchOptions;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::str::FromStr;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::{runtime, time::sleep};
use tracing::*;

use crate::{safekeeper::Term, timeline::GlobalTimelines, SafeKeeperConf};
use utils::{
    lsn::Lsn,
    zid::{ZNodeId, ZTenantId, ZTenantTimelineId, ZTimelineId},
};

const RETRY_INTERVAL_MSEC: u64 = 1000;
const PUSH_INTERVAL_MSEC: u64 = 1000;
const LEASE_TTL_SEC: i64 = 5;
// TODO: add global zenith installation ID.
const ZENITH_PREFIX: &str = "zenith";

/// Published data about safekeeper. Fields made optional for easy migrations.
#[serde_as]
#[derive(Deserialize, Serialize)]
pub struct SafekeeperInfo {
    /// Term of the last entry.
    pub last_log_term: Option<Term>,
    /// LSN of the last record.
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub flush_lsn: Option<Lsn>,
    /// Up to which LSN safekeeper regards its WAL as committed.
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub commit_lsn: Option<Lsn>,
    /// LSN up to which safekeeper offloaded WAL to s3.
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub s3_wal_lsn: Option<Lsn>,
    /// LSN of last checkpoint uploaded by pageserver.
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub remote_consistent_lsn: Option<Lsn>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub peer_horizon_lsn: Option<Lsn>,
}

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

/// Prefix to timeline related data.
fn timeline_path(zttid: &ZTenantTimelineId) -> String {
    format!(
        "{}/{}/{}",
        ZENITH_PREFIX, zttid.tenant_id, zttid.timeline_id
    )
}

/// Key to per timeline per safekeeper data.
fn timeline_safekeeper_path(zttid: &ZTenantTimelineId, sk_id: ZNodeId) -> String {
    format!("{}/safekeeper/{}", timeline_path(zttid), sk_id)
}

/// Push once in a while data about all active timelines to the broker.
async fn push_loop(conf: SafeKeeperConf) -> Result<()> {
    let mut client = Client::connect(conf.broker_endpoints.as_ref().unwrap(), None).await?;

    // Get and maintain lease to automatically delete obsolete data
    let lease = client.lease_grant(LEASE_TTL_SEC, None).await?;
    let (mut keeper, mut ka_stream) = client.lease_keep_alive(lease.id()).await?;

    let push_interval = Duration::from_millis(PUSH_INTERVAL_MSEC);
    loop {
        // Note: we lock runtime here and in timeline methods as GlobalTimelines
        // is under plain mutex. That's ok, all this code is not performance
        // sensitive and there is no risk of deadlock as we don't await while
        // lock is held.
        let active_tlis = GlobalTimelines::get_active_timelines();
        for zttid in &active_tlis {
            if let Ok(tli) = GlobalTimelines::get(&conf, *zttid, false) {
                let sk_info = tli.get_public_info();
                let put_opts = PutOptions::new().with_lease(lease.id());
                client
                    .put(
                        timeline_safekeeper_path(zttid, conf.my_id),
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
    lazy_static! {
        static ref TIMELINE_SAFEKEEPER_RE: Regex =
            Regex::new(r"^zenith/([[:xdigit:]]+)/([[:xdigit:]]+)/safekeeper/([[:digit:]])$")
                .unwrap();
    }
    let mut client = Client::connect(conf.broker_endpoints.as_ref().unwrap(), None).await?;
    loop {
        let wo = WatchOptions::new().with_prefix();
        // TODO: subscribe only to my timelines
        let (_, mut stream) = client.watch(ZENITH_PREFIX, Some(wo)).await?;
        while let Some(resp) = stream.message().await? {
            if resp.canceled() {
                bail!("watch canceled");
            }

            for event in resp.events() {
                if EventType::Put == event.event_type() {
                    if let Some(kv) = event.kv() {
                        if let Some(caps) = TIMELINE_SAFEKEEPER_RE.captures(kv.key_str()?) {
                            let tenant_id = ZTenantId::from_str(caps.get(1).unwrap().as_str())?;
                            let timeline_id = ZTimelineId::from_str(caps.get(2).unwrap().as_str())?;
                            let zttid = ZTenantTimelineId::new(tenant_id, timeline_id);
                            let safekeeper_id = ZNodeId(caps.get(3).unwrap().as_str().parse()?);
                            let value_str = kv.value_str()?;
                            match serde_json::from_str::<SafekeeperInfo>(value_str) {
                                Ok(safekeeper_info) => {
                                    if let Ok(tli) = GlobalTimelines::get(&conf, zttid, false) {
                                        tli.record_safekeeper_info(&safekeeper_info, safekeeper_id)?
                                    }
                                }
                                Err(err) => warn!(
                                    "failed to deserialize safekeeper info {}: {}",
                                    value_str, err
                                ),
                            }
                        }
                    }
                }
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
