//!
//!  Callmemaybe thread is responsible for periodically requesting
//!  pageserver to initiate wal streaming.
//!
use crate::SafeKeeperConf;
use anyhow::Result;
use log::*;
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::runtime;
use tokio::sync::mpsc::Receiver;
use tokio::task;
use tokio_postgres::NoTls;
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

    // Send callmemaybe request
    let listen_pg_addr = listen_pg_addr_str
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();

    let callme = format!(
        "callmemaybe {} {} host={} port={} options='-c ztimelineid={} ztenantid={}'",
        tenantid,
        timelineid,
        &listen_pg_addr.ip().to_string(),
        listen_pg_addr.port(),
        timelineid,
        tenantid
    );

    let _ = client.simple_query(&callme).await?;

    Ok(())
}

pub fn thread_main(conf: SafeKeeperConf, rx: Receiver<CallmeEvent>) -> Result<()> {
    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(main_loop(conf, rx))
}

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

struct SubscriptionStateInner {
    handle: Option<task::JoinHandle<()>>,
    last_call_time: Instant,
    paused: bool,
}

struct SubscriptionState {
    tenantid: ZTenantId,
    timelineid: ZTimelineId,
    pageserver_connstr: String,
    inner: Mutex<SubscriptionStateInner>,
}

impl SubscriptionState {
    fn new(
        tenantid: ZTenantId,
        timelineid: ZTimelineId,
        pageserver_connstr: String,
    ) -> SubscriptionState {
        let state_inner = SubscriptionStateInner {
            handle: None,
            last_call_time: Instant::now(),
            paused: false,
        };

        SubscriptionState {
            tenantid,
            timelineid,
            pageserver_connstr,
            inner: Mutex::new(state_inner),
        }
    }

    fn pause(&self) {
        let mut state_inner = self.inner.lock().unwrap();
        state_inner.paused = true;

        if let Some(handle) = state_inner.handle.take() {
            handle.abort();
        }
    }

    fn resume(&self) {
        let mut state_inner = self.inner.lock().unwrap();
        state_inner.paused = false;
    }

    fn call(&self, recall_period: Duration, listen_pg_addr: String) {
        let mut state_inner = self.inner.lock().unwrap();

        // Ignore call request if this subscription is paused
        if state_inner.paused {
            debug!(
                "ignore call request for paused subscription
                tenantid: {}, timelineid: {}",
                self.tenantid, self.timelineid
            );
            return;
        }

        // Check if it too early to recall
        if state_inner.handle.is_some() && state_inner.last_call_time.elapsed() < recall_period {
            debug!(
                "too early to recall. state_inner.last_call_time.elapsed: {:?}, recall_period: {:?}
                tenantid: {}, timelineid: {}",
                state_inner.last_call_time, recall_period, self.tenantid, self.timelineid
            );
            return;
        }

        // If previous task didn't complete in recall_period, it must be hanging,
        // so don't wait for it forever, just abort it and try again.
        //
        // Most likely, the task have already successfully completed
        // and abort() won't have any effect.
        if let Some(handle) = state_inner.handle.take() {
            handle.abort();

            let timelineid = self.timelineid;
            let tenantid = self.tenantid;
            tokio::spawn(async move {
                if let Err(err) = handle.await {
                    if err.is_cancelled() {
                        warn!("callback task for timelineid={} tenantid={} was cancelled before spawning a new one",
                            timelineid, tenantid);
                    }
                }
            });
        }

        let timelineid = self.timelineid;
        let tenantid = self.tenantid;
        let pageserver_connstr = self.pageserver_connstr.clone();
        state_inner.handle = Some(tokio::spawn(async move {
            request_callback(pageserver_connstr, listen_pg_addr, timelineid, tenantid)
                .await
                .unwrap_or_else(|e| {
                    error!(
                        "callmemaybe. request_callback for timelineid={} tenantid={} failed: {}",
                        timelineid, tenantid, e
                    );
                })
        }));

        // Update last_call_time
        state_inner.last_call_time = Instant::now();
        debug!(
            "new call spawned. time {:?}
            tenantid: {}, timelineid: {}",
            state_inner.last_call_time, self.tenantid, self.timelineid
        );
    }
}

impl Drop for SubscriptionStateInner {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

pub async fn main_loop(conf: SafeKeeperConf, mut rx: Receiver<CallmeEvent>) -> Result<()> {
    let default_timeout = Duration::from_secs(5);
    let recall_period = conf.recall_period.unwrap_or(default_timeout);

    let subscriptions: Mutex<HashMap<(ZTenantId, ZTimelineId), SubscriptionState>> =
        Mutex::new(HashMap::new());

    loop {
        let call_iteration = tokio::select! {
            request = rx.recv() => {
                match request {
                    Some(request) =>
                    {
                        match request
                        {
                            CallmeEvent::Subscribe(tenantid, timelineid, pageserver_connstr) =>
                            {
                                let mut subscriptions = subscriptions.lock().unwrap();
                                subscriptions.insert((tenantid, timelineid), SubscriptionState::new(tenantid, timelineid, pageserver_connstr)) ;
                                debug!("callmemaybe. thread_main. subscribe callback request for timelineid={} tenantid={}",
                                timelineid, tenantid);
                                true
                            },
                            CallmeEvent::Unsubscribe(tenantid, timelineid) => {
                                let mut subscriptions = subscriptions.lock().unwrap();
                                subscriptions.remove(&(tenantid, timelineid));
                                debug!("callmemaybe. thread_main. unsubscribe callback request for timelineid={} tenantid={}",
                                timelineid, tenantid);
                                false
                            },
                            CallmeEvent::Pause(tenantid, timelineid) => {
                                let subscriptions = subscriptions.lock().unwrap();
                                if let Some(sub) = subscriptions.get(&(tenantid, timelineid))
                                {
                                    sub.pause();
                                }
                                debug!("callmemaybe. thread_main. pause callback request for timelineid={} tenantid={}",
                                timelineid, tenantid);
                                false
                            },
                            CallmeEvent::Resume(tenantid, timelineid) => {
                                let mut call_iteration = false;
                                let subscriptions = subscriptions.lock().unwrap();
                                if let Some(sub) = subscriptions.get(&(tenantid, timelineid))
                                {
                                    sub.resume();
                                    debug!("callmemaybe. thread_main. resume callback request for timelineid={} tenantid={}",
                                    timelineid, tenantid);
                                    call_iteration = true;
                                }
                                call_iteration
                            },
                        }
                    },
                    // all senders disconnected
                    None =>  {
                        return Ok(());
                    },
                }
            },
            _ = tokio::time::sleep(recall_period) => { true },
        };

        if call_iteration {
            let subscriptions = subscriptions.lock().unwrap();

            for (&(_tenantid, _timelineid), state) in subscriptions.iter() {
                let listen_pg_addr = conf.listen_pg_addr.clone();

                state.call(recall_period, listen_pg_addr);
            }
        }
    }
}
