//!
//!  Callmemaybe module is responsible for periodically requesting
//!  pageserver to initiate wal streaming.
//!
//!  Other threads can use CallmeEvent messages to subscribe or unsubscribe
//!  from the call list.
//!
use crate::SafeKeeperConf;
use anyhow::anyhow;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::runtime;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task;
use tokio_postgres::NoTls;
use tracing::*;
use zenith_utils::connstring::connection_host_port;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

async fn request_callback(
    pageserver_connstr: String,
    listen_pg_addr_str: String,
    timelineid: ZTimelineId,
    tenantid: ZTenantId,
) -> Result<()> {
    debug!(
        "callmemaybe request_callback Connecting to pageserver {}",
        &pageserver_connstr
    );
    let (client, connection) = tokio_postgres::connect(&pageserver_connstr, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // use Config parsing because SockAddr parsing doesnt allow to use host names instead of ip addresses
    let me_connstr = format!("postgresql://no_user@{}/no_db", listen_pg_addr_str);
    let me_conf: postgres::config::Config = me_connstr.parse().unwrap();
    let (host, port) = connection_host_port(&me_conf);

    let callme = format!(
        "callmemaybe {} {} host={} port={} options='-c ztimelineid={} ztenantid={}'",
        tenantid, timelineid, host, port, timelineid, tenantid
    );

    let _ = client.simple_query(&callme).await?;

    Ok(())
}

pub fn thread_main(conf: SafeKeeperConf, rx: UnboundedReceiver<CallmeEvent>) -> Result<()> {
    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(main_loop(conf, rx))
}

/// Messages to the callmemaybe thread
#[derive(Debug)]
pub enum CallmeEvent {
    // add new subscription to the list
    Subscribe(ZTenantId, ZTimelineId, String),
    // remove the subscription from the list
    Unsubscribe(ZTenantId, ZTimelineId),
    // don't serve this subscription, but keep it in the list
    Pause(ZTenantId, ZTimelineId),
    // resume this subscription, if it exists,
    // but don't create a new one if it is gone
    Resume(ZTenantId, ZTimelineId),
}

struct SubscriptionState {
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    pageserver_connstr: String,
    handle: Option<task::JoinHandle<()>>,
    last_call_time: Instant,
    paused: bool,
}

impl SubscriptionState {
    fn new(
        tenantid: ZTenantId,
        timelineid: ZTimelineId,
        pageserver_connstr: String,
    ) -> SubscriptionState {
        SubscriptionState {
            tenantid,
            timelineid,
            pageserver_connstr,
            handle: None,
            last_call_time: Instant::now(),
            paused: false,
        }
    }

    fn pause(&mut self) {
        self.paused = true;
        self.abort_handle();
    }

    fn resume(&mut self) {
        self.paused = false;
    }

    // Most likely, the task have already successfully completed
    // and abort() won't have any effect.
    fn abort_handle(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();

            let timelineid = self.timelineid;
            let tenantid = self.tenantid;
            tokio::spawn(async move {
                if let Err(err) = handle.await {
                    if err.is_cancelled() {
                        warn!("callback task for timelineid={} tenantid={} was cancelled before spawning a new one",
                            timelineid, tenantid);
                    } else {
                        error!(
                            "callback task for timelineid={} tenantid={} failed: {}",
                            timelineid, tenantid, err
                        );
                    }
                }
            });
        }
    }

    fn call(&mut self, recall_period: Duration, listen_pg_addr: String) {
        // Ignore call request if this subscription is paused
        if self.paused {
            debug!(
                "ignore call request for paused subscription
                tenantid: {}, timelineid: {}",
                self.tenantid, self.timelineid
            );
            return;
        }

        // Check if it too early to recall
        if self.handle.is_some() && self.last_call_time.elapsed() < recall_period {
            debug!(
                "too early to recall. self.last_call_time.elapsed: {:?}, recall_period: {:?}
                tenantid: {}, timelineid: {}",
                self.last_call_time, recall_period, self.tenantid, self.timelineid
            );
            return;
        }

        // If previous task didn't complete in recall_period, it must be hanging,
        // so don't wait for it forever, just abort it and try again.
        self.abort_handle();

        let timelineid = self.timelineid;
        let tenantid = self.tenantid;
        let pageserver_connstr = self.pageserver_connstr.clone();
        self.handle = Some(tokio::spawn(async move {
            request_callback(pageserver_connstr, listen_pg_addr, timelineid, tenantid)
                .await
                .unwrap_or_else(|e| {
                    error!(
                        "callback task for timelineid={} tenantid={} failed: {}",
                        timelineid, tenantid, e
                    )
                });
        }));

        // Update last_call_time
        self.last_call_time = Instant::now();
        debug!(
            "new call spawned. time {:?}
            tenantid: {}, timelineid: {}",
            self.last_call_time, self.tenantid, self.timelineid
        );
    }
}

impl Drop for SubscriptionState {
    fn drop(&mut self) {
        self.abort_handle();
    }
}

pub async fn main_loop(conf: SafeKeeperConf, mut rx: UnboundedReceiver<CallmeEvent>) -> Result<()> {
    let subscriptions: Mutex<HashMap<(ZTenantId, ZTimelineId), SubscriptionState>> =
        Mutex::new(HashMap::new());

    loop {
        tokio::select! {
            request = rx.recv() =>
            {
                match request.ok_or_else(|| anyhow!("done"))?
                {
                    CallmeEvent::Subscribe(tenantid, timelineid, pageserver_connstr) =>
                    {
                        let mut subscriptions = subscriptions.lock().unwrap();
                        if let Some(mut sub) = subscriptions.insert((tenantid, timelineid),
                            SubscriptionState::new(tenantid, timelineid, pageserver_connstr))
                        {
                            sub.call(conf.recall_period, conf.listen_pg_addr.clone());
                        }
                        debug!("callmemaybe. thread_main. subscribe callback request for timelineid={} tenantid={}",
                        timelineid, tenantid);
                    },
                    CallmeEvent::Unsubscribe(tenantid, timelineid) => {
                        let mut subscriptions = subscriptions.lock().unwrap();
                        subscriptions.remove(&(tenantid, timelineid));
                        debug!("callmemaybe. thread_main. unsubscribe callback request for timelineid={} tenantid={}",
                        timelineid, tenantid);
                    },
                    CallmeEvent::Pause(tenantid, timelineid) => {
                        let mut subscriptions = subscriptions.lock().unwrap();
                        if let Some(sub) = subscriptions.get_mut(&(tenantid, timelineid))
                        {
                            sub.pause();
                        };
                        debug!("callmemaybe. thread_main. pause callback request for timelineid={} tenantid={}",
                        timelineid, tenantid);
                    },
                    CallmeEvent::Resume(tenantid, timelineid) => {
                        let mut subscriptions = subscriptions.lock().unwrap();
                        if let Some(sub) = subscriptions.get_mut(&(tenantid, timelineid))
                        {
                            sub.resume();
                            sub.call(conf.recall_period, conf.listen_pg_addr.clone());
                        };

                        debug!("callmemaybe. thread_main. resume callback request for timelineid={} tenantid={}",
                        timelineid, tenantid);
                    },
                }
            },
            _ = tokio::time::sleep(conf.recall_period) => {
                let mut subscriptions = subscriptions.lock().unwrap();

                for (&(_tenantid, _timelineid), state) in subscriptions.iter_mut() {
                    state.call(conf.recall_period, conf.listen_pg_addr.clone());
                }
             },
        };
    }
}
