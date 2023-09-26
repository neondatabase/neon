//! Periodically collect proxy consumption metrics
//! and push them to a HTTP endpoint.
use crate::{config::MetricCollectionConfig, http};
use chrono::{DateTime, Utc};
use consumption_metrics::{idempotency_key, Event, EventChunk, EventType, CHUNK_SIZE};
use dashmap::{mapref::entry::Entry, DashMap};
use once_cell::sync::Lazy;
use serde::Serialize;
use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tracing::{error, info, instrument, trace};

const PROXY_IO_BYTES_PER_CLIENT: &str = "proxy_io_bytes_per_client";

const DEFAULT_HTTP_REPORTING_TIMEOUT: Duration = Duration::from_secs(60);

/// Key that uniquely identifies the object, this metric describes.
/// Currently, endpoint_id is enough, but this may change later,
/// so keep it in a named struct.
///
/// Both the proxy and the ingestion endpoint will live in the same region (or cell)
/// so while the project-id is unique across regions the whole pipeline will work correctly
/// because we enrich the event with project_id in the control-plane endpoint.
#[derive(Eq, Hash, PartialEq, Serialize, Debug, Clone)]
pub struct Ids {
    pub endpoint_id: String,
    pub branch_id: String,
}

pub struct MetricCounter {
    transmitted: AtomicU64,
    opened_connections: AtomicUsize,
}

impl MetricCounter {
    /// Record that some bytes were sent from the proxy to the client
    pub fn record_egress(&self, bytes: u64) {
        self.transmitted.fetch_add(bytes, Ordering::AcqRel);
    }

    /// extract the value that should be reported
    fn should_report(self: &Arc<Self>) -> Option<u64> {
        // heuristic to see if the branch is still open
        // if a clone happens while we are observing, the heurstic will be incorrect.
        //
        // Worst case is that we won't report an event for this endpoint.
        // However, for the strong count to be 1 it must have occured that at one instant
        // all the endpoints were closed, so missing a report because the endpoints are closed is valid.
        let is_open = Arc::strong_count(self) > 1;
        let opened = self.opened_connections.swap(0, Ordering::AcqRel);

        // update cached metrics eagerly, even if they can't get sent
        // (to avoid sending the same metrics twice)
        // see the relevant discussion on why to do so even if the status is not success:
        // https://github.com/neondatabase/neon/pull/4563#discussion_r1246710956
        let value = self.transmitted.swap(0, Ordering::AcqRel);

        // Our only requirement is that we report in every interval if there was an open connection
        // if there were no opened connections since, then we don't need to report
        if value == 0 && !is_open && opened == 0 {
            None
        } else {
            Some(value)
        }
    }

    //. determine whether the counter should be cleared from the global map.
    fn should_clear(self: &mut Arc<Self>) -> bool {
        // we can't clear this entry if it's acquired elsewhere
        let Some(counter) = Arc::get_mut(self) else {
            return false;
        };
        let opened = *counter.opened_connections.get_mut();
        let value = *counter.transmitted.get_mut();
        // clear if there's no data to report
        value == 0 && opened == 0
    }
}

// endpoint and branch IDs are not user generated so we don't run the risk of hash-dos
type FastHasher = std::hash::BuildHasherDefault<rustc_hash::FxHasher>;

pub struct Metrics {
    endpoints: DashMap<Ids, Arc<MetricCounter>, FastHasher>,
}

impl Metrics {
    /// Register a new byte metrics counter for this endpoint
    pub fn register(&self, ids: Ids) -> Arc<MetricCounter> {
        let entry = if let Some(entry) = self.endpoints.get(&ids) {
            entry.clone()
        } else {
            self.endpoints
                .entry(ids)
                .or_insert_with(|| {
                    Arc::new(MetricCounter {
                        transmitted: AtomicU64::new(0),
                        opened_connections: AtomicUsize::new(0),
                    })
                })
                .clone()
        };

        entry.opened_connections.fetch_add(1, Ordering::AcqRel);
        entry
    }
}

pub static USAGE_METRICS: Lazy<Metrics> = Lazy::new(|| Metrics {
    endpoints: DashMap::default(),
});

pub async fn task_main(config: &MetricCollectionConfig) -> anyhow::Result<Infallible> {
    info!("metrics collector config: {config:?}");
    scopeguard::defer! {
        info!("metrics collector has shut down");
    }

    let http_client = http::new_client_with_timeout(DEFAULT_HTTP_REPORTING_TIMEOUT);
    let hostname = hostname::get()?.as_os_str().to_string_lossy().into_owned();

    let mut prev = Utc::now();
    let mut ticker = tokio::time::interval(config.interval);
    loop {
        ticker.tick().await;

        let now = Utc::now();
        collect_metrics_iteration(&http_client, &config.endpoint, &hostname, prev, now).await;
        prev = now;
    }
}

#[instrument(skip_all)]
async fn collect_metrics_iteration(
    client: &http::ClientWithMiddleware,
    metric_collection_endpoint: &reqwest::Url,
    hostname: &str,
    prev: DateTime<Utc>,
    now: DateTime<Utc>,
) {
    info!(
        "starting collect_metrics_iteration. metric_collection_endpoint: {}",
        metric_collection_endpoint
    );

    let mut metrics_to_clear = Vec::new();

    let metrics_to_send: Vec<(Ids, u64)> = USAGE_METRICS
        .endpoints
        .iter()
        .filter_map(|counter| {
            let key = counter.key().clone();
            let Some(value) = counter.should_report() else {
                metrics_to_clear.push(key);
                return None;
            };
            Some((key, value))
        })
        .collect();

    if metrics_to_send.is_empty() {
        trace!("no new metrics to send");
        return;
    }

    // Send metrics.
    // Split into chunks of 1000 metrics to avoid exceeding the max request size
    for chunk in metrics_to_send.chunks(CHUNK_SIZE) {
        let events = chunk
            .iter()
            .map(|(ids, value)| Event {
                kind: EventType::Incremental {
                    start_time: prev,
                    stop_time: now,
                },
                metric: PROXY_IO_BYTES_PER_CLIENT,
                idempotency_key: idempotency_key(hostname),
                value: *value,
                extra: Ids {
                    endpoint_id: ids.endpoint_id.clone(),
                    branch_id: ids.branch_id.clone(),
                },
            })
            .collect();

        let res = client
            .post(metric_collection_endpoint.clone())
            .json(&EventChunk { events })
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
            for metric in chunk.iter().filter(|(_, value)| *value > (1u64 << 40)) {
                // Report if the metric value is suspiciously large
                error!("potentially abnormal metric value: {:?}", metric);
            }
        }
    }

    for metric in metrics_to_clear {
        match USAGE_METRICS.endpoints.entry(metric) {
            Entry::Occupied(mut counter) => {
                if counter.get_mut().should_clear() {
                    counter.remove_entry();
                }
            }
            Entry::Vacant(_) => {}
        }
    }
}
