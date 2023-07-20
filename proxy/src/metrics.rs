//! Periodically collect proxy consumption metrics
//! and push them to a HTTP endpoint.
use crate::{config::MetricCollectionConfig, http};
use chrono::{DateTime, TimeZone, Utc};
use consumption_metrics::{idempotency_key_into, Event, EventChunk, EventType, CHUNK_SIZE};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    convert::Infallible,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tracing::{error, info, instrument, trace};

const PROXY_IO_BYTES_PER_CLIENT: &str = "proxy_io_bytes_per_client";

const DEFAULT_HTTP_REPORTING_TIMEOUT: Duration = Duration::from_secs(60);

///
/// Key that uniquely identifies the object, this metric describes.
/// Currently, endpoint_id is enough, but this may change later,
/// so keep it in a named struct.
///
/// Both the proxy and the ingestion endpoint will live in the same region (or cell)
/// so while the project-id is unique across regions the whole pipeline will work correctly
/// because we enrich the event with project_id in the control-plane endpoint.
///
#[derive(Eq, Hash, PartialEq, Serialize, Deserialize, Debug)]
pub struct Ids {
    pub endpoint_id: String,
    pub branch_id: String,
}

impl Clone for Ids {
    fn clone(&self) -> Self {
        Self {
            endpoint_id: self.endpoint_id.clone(),
            branch_id: self.branch_id.clone(),
        }
    }
    fn clone_from(&mut self, source: &Self) {
        self.branch_id.clone_from(&source.branch_id);
        self.endpoint_id.clone_from(&source.endpoint_id);
    }
}

static STARTED_PROXY_COUNTERS: OnceLock<tokio::sync::mpsc::Sender<CounterState>> = OnceLock::new();

pub async fn task_main(config: &MetricCollectionConfig) -> anyhow::Result<Infallible> {
    info!("metrics collector config: {config:?}");
    scopeguard::defer! {
        info!("metrics collector has shut down");
    }

    let (proxy_counters_tx, mut proxy_counters_rx) = mpsc::channel(256);
    STARTED_PROXY_COUNTERS
        .set(proxy_counters_tx)
        .map_err(|_| anyhow::anyhow!("invalid proxy metrics state"))?;

    let http_client = http::new_client_with_timeout(DEFAULT_HTTP_REPORTING_TIMEOUT);
    let mut proxy_counter_queue: VecDeque<CounterState> = VecDeque::new();

    // fill with dummy data.
    let mut events: Vec<Event<Ids>> = vec![
        Event {
            kind: EventType::Incremental {
                start_time: Utc.timestamp_nanos(0),
                stop_time: Utc.timestamp_nanos(0),
            },
            metric: "",
            idempotency_key: String::new(),
            value: 0,
            extra: Ids {
                endpoint_id: String::new(),
                branch_id: String::new(),
            },
        };
        CHUNK_SIZE
    ];

    let hostname = hostname::get()?;
    let hostname = hostname.as_os_str().to_string_lossy();

    let mut ticker = tokio::time::interval(config.interval);
    loop {
        // while waiting for the timer to tick, try acquire some new counters
        loop {
            tokio::select! {
                // time to do metrics
                _ = ticker.tick() => { break }
                // new counter registration
                Some(counter) = proxy_counters_rx.recv() => {
                    proxy_counter_queue.push_back(counter);
                }
            }
        }

        // currently we only report the metrics deltas.
        // in future, we should occasionally report absolute metrics to account
        // for missed metrics.
        let report_delta = true;
        let res = collect_metrics_iteration(
            &http_client,
            &mut proxy_counter_queue,
            &mut events,
            &config.endpoint,
            &hostname,
            report_delta,
        )
        .await;

        match res {
            Err(e) => error!("failed to send consumption metrics: {e} "),
            Ok(_) => {
                trace!("periodic metrics collection completed successfully")
            }
        }
    }
}

/// Counter for proxy bytes transferred
pub struct ProxyCounter {
    ids: Ids,
    /// Outbound bytes from neon compute to client
    pub tx: AtomicU64,
}

pub struct CounterState {
    counter: Arc<ProxyCounter>,
    last_seen: DateTime<Utc>,
    total_tx: u64,
}

impl CounterState {
    fn is_closed(&self) -> bool {
        // we are the only holders of this counter
        Arc::strong_count(&self.counter) == 1
    }

    fn get_tx_delta(&mut self, now: DateTime<Utc>) -> (DateTime<Utc>, u64) {
        let old = std::mem::replace(&mut self.last_seen, now);

        let tx_delta = self.counter.tx.swap(0, Ordering::Relaxed);
        self.total_tx += tx_delta;

        (old, tx_delta)
    }
}

impl ProxyCounter {
    /// Create a new `ProxyCounter` which counts how many bytes are transfered to be reported
    pub async fn new(endpoint_id: String, branch_id: String) -> Arc<Self> {
        let this = Arc::new(Self {
            ids: Ids {
                endpoint_id,
                branch_id,
            },
            tx: AtomicU64::new(0),
        });
        this.clone().submit().await;
        this
    }

    async fn submit(self: Arc<ProxyCounter>) {
        if let Some(counters) = STARTED_PROXY_COUNTERS.get() {
            let state = CounterState {
                counter: self,
                last_seen: Utc::now(),
                total_tx: 0,
            };
            if counters.send(state).await.is_err() {
                error!("new proxy job started but metrics task has shut down");
            }
        } else {
            error!("metrics state not configured but proxy has begun");
        }
    }
}

#[instrument(skip_all, fields(metrics_collection_endpoints))]
async fn collect_metrics_iteration(
    client: &http::ClientWithMiddleware,
    counter_queue: &mut VecDeque<CounterState>,
    events: &mut [Event<Ids>],
    metric_collection_endpoint: &reqwest::Url,
    hostname: &str,
    report_delta: bool,
) -> anyhow::Result<()> {
    // each iteration, we want to process every entry once and only once.
    // we re-insert back into the queue during this loop, so we keep a counter
    // for how many we want to process
    let mut counters_remaining = counter_queue.len();

    info!(counters_remaining, "metrics sweep");

    while counters_remaining > 0 {
        // index of the currently unused event slot
        let mut event_slot = 0;
        while event_slot < events.len() && counters_remaining > 0 {
            let Some(mut counter) = counter_queue.pop_front() else { break };
            counters_remaining -= 1;

            let now = Utc::now();
            let (last_seen, delta_tx) = counter.get_tx_delta(now);

            let event = &mut events[event_slot];
            event.idempotency_key.clear();

            // only report this metric event if we are reporting absolute values
            // or we have a delta_tx greater than 0
            let should_report = delta_tx > 0 || !report_delta;
            if should_report {
                // try to use minimal allocations, re-using buffers
                event.extra.clone_from(&counter.counter.ids);
                idempotency_key_into(&hostname, &mut event.idempotency_key);
                event.metric = PROXY_IO_BYTES_PER_CLIENT;

                if report_delta {
                    event.value = delta_tx;
                    event.kind = EventType::Incremental {
                        start_time: last_seen,
                        stop_time: now,
                    };
                } else {
                    event.value = counter.total_tx;
                    event.kind = EventType::Absolute { time: now };
                }

                event_slot += 1;
            }

            // if the counter is closed, don't push back to queue
            if !counter.is_closed() {
                counter_queue.push_back(counter);
            }
        }

        let chunk = &events[..event_slot];

        if chunk.is_empty() {
            trace!("no new metrics to send");
            return Ok(());
        }

        info!(n = chunk.len(), "uploading metrics");

        let res = client
            .post(metric_collection_endpoint.clone())
            .json(&EventChunk { events: chunk })
            .send()
            .await;

        let res = match res {
            Ok(x) => x,
            Err(err) => {
                error!("failed to send metrics: {:?}", err);
                continue;
            }
        };

        if !res.status().is_success() {
            error!("metrics endpoint refused the sent metrics: {:?}", res);
            for metric in chunk.iter().filter(|metric| metric.value > (1u64 << 40)) {
                // Report if the metric value is suspiciously large
                error!("potentially abnormal metric value: {:?}", metric);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::atomic;
    use std::time::Duration;

    use super::{task_main, Ids, ProxyCounter};
    use crate::config::MetricCollectionConfig;
    use crate::metrics::PROXY_IO_BYTES_PER_CLIENT;

    use consumption_metrics::EventType;
    use hyper::body::to_bytes;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio::task::yield_now;

    // Just a wrapper around a slice of events
    // to deserialize it as `{"events" : [ ] }
    #[derive(serde::Deserialize)]
    pub struct EventChunkOwned {
        pub events: Vec<EventOwned>,
    }

    #[derive(serde::Deserialize, Debug)]
    pub struct EventOwned {
        #[serde(flatten)]
        #[serde(rename = "type")]
        pub kind: EventType,

        pub metric: String,
        pub idempotency_key: String,
        pub value: u64,

        #[serde(flatten)]
        pub extra: Ids,
    }

    #[tokio::test]
    async fn metrics_test() {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let make_svc = make_service_fn(move |_| {
            let tx = tx.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                    let tx = tx.clone();
                    async move {
                        let body = to_bytes(req.into_body()).await.unwrap();
                        let events: EventChunkOwned = serde_json::from_slice(&body).unwrap();
                        for event in events.events {
                            tx.send(event).unwrap();
                        }

                        Ok::<_, Infallible>(Response::new(Body::empty()))
                    }
                }))
            }
        });

        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();

        tokio::spawn(async move {
            hyper::Server::from_tcp(tcp.into_std().unwrap())
                .unwrap()
                .serve(make_svc)
                .await
        });

        let config = MetricCollectionConfig {
            endpoint: format!("http://{addr}/").parse().unwrap(),
            interval: Duration::from_secs(1),
        };
        tokio::spawn(async move { task_main(&config).await });

        // let the main task do it's setup
        yield_now().await;

        let mut counters = vec![];
        for i in 0..20 {
            let i = i + 1;
            let counter = ProxyCounter::new(format!("endpoint{i}"), format!("branch{i}")).await;
            counter.tx.fetch_add(20 * i, atomic::Ordering::Relaxed);
            counters.push(counter);
        }

        // check all events arrived
        for _ in 0..20 {
            let event = rx.recv().await.unwrap();
            assert_eq!(event.metric, PROXY_IO_BYTES_PER_CLIENT);
            assert!(event.value > 0);
        }

        for counter in counters {
            assert_eq!(counter.tx.load(atomic::Ordering::Relaxed), 0);
        }
    }
}
