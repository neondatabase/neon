//!
//!  Callmemaybe module is responsible for periodically requesting
//!  pageserver to initiate wal streaming.
//!
//!  Other threads can use CallmeEvent messages to subscribe or unsubscribe
//!  from the call list.
//!
use crate::SafeKeeperConf;
use anyhow::{Context, Result};
use std::collections::hash_map::Entry;
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
    info!(
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

    // pageserver connstr is needed to be able to distinguish between different pageservers
    // it is required to correctly manage callmemaybe subscriptions when more than one pageserver is involved
    // TODO it is better to use some sort of a unique id instead of connection string, see https://github.com/zenithdb/zenith/issues/1105
    let callme = format!(
        "callmemaybe {} {} host={} port={} options='-c ztimelineid={} ztenantid={} pageserver_connstr={}'",
        tenantid, timelineid, host, port, timelineid, tenantid, pageserver_connstr,
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
    Unsubscribe(ZTenantId, ZTimelineId, String),
    // don't serve this subscription, but keep it in the list
    Pause(ZTenantId, ZTimelineId, String),
    // resume this subscription, if it exists,
    // but don't create a new one if it is gone
    Resume(ZTenantId, ZTimelineId, String),
    // TODO how do we delete from subscriptions?
}

#[derive(Debug)]
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
            let pageserver_connstr = self.pageserver_connstr.clone();
            tokio::spawn(async move {
                if let Err(err) = handle.await {
                    if err.is_cancelled() {
                        warn!("callback task for timelineid={} tenantid={} was cancelled before spawning a new one",
                            timelineid, tenantid);
                    } else {
                        error!(
                            "callback task for timelineid={} tenantid={} pageserver_connstr={} failed: {}",
                            timelineid, tenantid, pageserver_connstr, err
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
                "ignore call request for paused subscription \
                tenantid: {}, timelineid: {}",
                self.tenantid, self.timelineid
            );
            return;
        }

        // Check if it too early to recall
        if self.handle.is_some() && self.last_call_time.elapsed() < recall_period {
            debug!(
                "too early to recall. self.last_call_time.elapsed: {:?}, recall_period: {:?} \
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
        info!(
            "new call spawned. last call time {:?} tenantid: {}, timelineid: {}",
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
    let subscriptions: Mutex<HashMap<(ZTenantId, ZTimelineId, String), SubscriptionState>> =
        Mutex::new(HashMap::new());

    let mut ticker = tokio::time::interval(conf.recall_period);
    loop {
        tokio::select! {
            request = rx.recv() =>
            {
                match request.context("done")?
                {
                    CallmeEvent::Subscribe(tenantid, timelineid, pageserver_connstr) =>
                    {
                        let _enter = info_span!("callmemaybe: subscribe", timelineid = %timelineid, tenantid = %tenantid, pageserver_connstr=%pageserver_connstr.clone()).entered();
                        let mut subscriptions = subscriptions.lock().unwrap();
                        match subscriptions.entry((tenantid, timelineid, pageserver_connstr.clone())) {
                            Entry::Occupied(_) => {
                                // Do nothing if subscription already exists
                                // If it is paused it means that there is already established replication connection.
                                // If it is not paused it will be polled with other subscriptions when timeout expires.
                                // This can occur when replication channel is established before subscription is added.
                                info!(
                                    "subscription already exists",
                                );
                            }
                            Entry::Vacant(entry) => {
                                let subscription = entry.insert(SubscriptionState::new(
                                    tenantid,
                                    timelineid,
                                    pageserver_connstr,
                                ));
                                subscription.call(conf.recall_period, conf.listen_pg_addr.clone());
                            }
                        }
                    },
                    CallmeEvent::Unsubscribe(tenantid, timelineid, pageserver_connstr) => {
                        let _enter = debug_span!("callmemaybe: unsubscribe", timelineid = %timelineid, tenantid = %tenantid, pageserver_connstr=%pageserver_connstr.clone()).entered();
                        debug!("unsubscribe");
                        let mut subscriptions = subscriptions.lock().unwrap();
                        subscriptions.remove(&(tenantid, timelineid, pageserver_connstr));

                    },
                    CallmeEvent::Pause(tenantid, timelineid, pageserver_connstr) => {
                        let _enter = debug_span!("callmemaybe: pause", timelineid = %timelineid, tenantid = %tenantid, pageserver_connstr=%pageserver_connstr.clone()).entered();
                        let mut subscriptions = subscriptions.lock().unwrap();
                        // If pause received when no corresponding subscription exists it means that someone started replication
                        // without using callmemaybe. So we create subscription and pause it.
                        // In tenant relocation scenario subscribe call will be executed after pause when compute is restarted.
                        // In that case there is no need to create new/unpause existing subscription.
                        match subscriptions.entry((tenantid, timelineid, pageserver_connstr.clone())) {
                            Entry::Occupied(mut sub) => {
                                debug!("pause existing");
                                sub.get_mut().pause();
                            }
                            Entry::Vacant(entry) => {
                                debug!("create paused");
                                let subscription = entry.insert(SubscriptionState::new(
                                    tenantid,
                                    timelineid,
                                    pageserver_connstr,
                                ));
                                subscription.pause();
                            }
                        }
                    },
                    CallmeEvent::Resume(tenantid, timelineid, pageserver_connstr) => {
                        debug!("callmemaybe. thread_main. resume callback request for timelineid={} tenantid={} pageserver_connstr={}",
                        timelineid, tenantid, pageserver_connstr);
                        let mut subscriptions = subscriptions.lock().unwrap();
                        if let Some(sub) = subscriptions.get_mut(&(tenantid, timelineid, pageserver_connstr))
                        {
                            sub.resume();
                        };
                    },
                }
            },
            _ = ticker.tick() => {
                let mut subscriptions = subscriptions.lock().unwrap();

                for (&(_tenantid, _timelineid, _), state) in subscriptions.iter_mut() {
                    state.call(conf.recall_period, conf.listen_pg_addr.clone());
                }
             },
        };
    }
}
