//! Periodically collect proxy consumption metrics
//! and push them to a HTTP endpoint.
use crate::{config::MetricCollectionConfig, http};
use chrono::{TimeZone, Utc};
use consumption_metrics::{idempotency_key_into, Event, EventChunk, EventType, CHUNK_SIZE};
use serde::Serialize;
use std::{
    collections::VecDeque,
    convert::Infallible,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
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
#[derive(Eq, Hash, PartialEq, Serialize, Debug)]
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

static STARTED_PROXY_COUNTERS: OnceLock<tokio::sync::mpsc::Sender<Arc<ProxyCounter>>> =
    OnceLock::new();

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
    let mut proxy_counter_queue: VecDeque<Arc<ProxyCounter>> = VecDeque::new();

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
        ticker.tick().await;

        while let Ok(counter) = proxy_counters_rx.try_recv() {
            proxy_counter_queue.push_back(counter);
        }

        let res = collect_metrics_iteration(
            &http_client,
            &mut proxy_counter_queue,
            &mut events,
            &config.endpoint,
            &hostname,
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
    /// Inbound bytes from client to neon compute
    pub rx: AtomicU64,
    // unix epoch nanoseconds. only valid up to year 2262-04-11.
    // maybe we can use microseconds, or maybe we don't care.
    last_seen: AtomicI64,
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
            rx: AtomicU64::new(0),
            last_seen: AtomicI64::new(Utc::now().timestamp_nanos()),
        });
        this.clone().submit().await;
        this
    }

    async fn submit(self: Arc<ProxyCounter>) {
        if let Some(counters) = STARTED_PROXY_COUNTERS.get() {
            if counters.send(self).await.is_err() {
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
    counter_queue: &mut VecDeque<Arc<ProxyCounter>>,
    events: &mut [Event<Ids>],
    metric_collection_endpoint: &reqwest::Url,
    hostname: &str,
) -> anyhow::Result<()> {
    // only call counter_queue once.
    // if we call this repeatedly, we might never exit the while loop below
    // since we re-insert into this queue.
    let mut counters = counter_queue.len();

    info!(counters, "metrics sweep");

    while counters > 0 {
        let mut i = 0;
        while i < events.len() && i < counters {
            let Some(counter) = counter_queue.pop_front() else { break };

            // update metrics eagerly. If there's a failure, there is a chance that this metric won't get recorded.
            // We don't want to send metrics twice as it might cause double billing which is worse than not billing enough.
            // see the relevant discussion: https://github.com/neondatabase/neon/pull/4563#discussion_r1246710956
            let tx = counter.tx.swap(0, Ordering::Relaxed);
            let _rx = counter.rx.swap(0, Ordering::Relaxed);

            // only record the metric event if it's not 0
            if tx > 0 {
                let now = Utc::now();
                let last_seen = Utc.timestamp_nanos(
                    counter
                        .last_seen
                        .swap(now.timestamp_nanos(), Ordering::Relaxed),
                );

                // try to use minimal allocations
                let event = &mut events[i];
                event.extra.clone_from(&counter.ids);
                idempotency_key_into(&hostname, &mut event.idempotency_key);

                // Only collect metric for outbound traffic
                event.value = tx;
                event.metric = PROXY_IO_BYTES_PER_CLIENT;
                event.kind = EventType::Incremental {
                    start_time: last_seen,
                    stop_time: now,
                };

                i += 1;
            } else {
                counters -= 1;
            }

            // if there is another owner of this counter, then the proxy task is still running.
            // add it back to the queue
            if Arc::strong_count(&counter) > 1 {
                counter_queue.push_back(counter);
            }
        }
        counters -= i;

        if i == 0 {
            trace!("no new metrics to send");
            return Ok(());
        }

        info!(n = i, "uploading metrics");

        let chunk = &events[..i];

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
