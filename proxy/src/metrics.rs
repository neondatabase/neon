//! Periodically collect proxy consumption metrics
//! and push them to a HTTP endpoint.
use crate::http;
use chrono::{DateTime, Utc};
use consumption_metrics::{idempotency_key, Event, EventChunk, EventType, CHUNK_SIZE};
use serde::Serialize;
use std::{collections::HashMap, time::Duration};
use tracing::{debug, error, info, instrument, trace};

const PROXY_IO_BYTES_PER_CLIENT: &str = "proxy_io_bytes_per_client";

///
/// Key that uniquely identifies the object, this metric describes.
/// Currently, endpoint_id is enough, but this may change later,
/// so keep it in a named struct.
///
/// Both the proxy and the ingestion endpoint will live in the same region (or cell)
/// so while the project-id is unique across regions the whole pipeline will work correctly
/// because we enrich the event with project_id in the control-plane endpoint.
///
#[derive(Eq, Hash, PartialEq, Serialize)]
pub struct Ids {
    pub endpoint_id: String,
}

pub async fn collect_metrics(
    metric_collection_endpoint: &reqwest::Url,
    metric_collection_interval: Duration,
    hostname: String,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("collect_metrics has shut down");
    }

    let mut ticker = tokio::time::interval(metric_collection_interval);

    info!(
        "starting collect_metrics. metric_collection_endpoint: {}",
        metric_collection_endpoint
    );

    let http_client = http::new_client();
    let mut cached_metrics: HashMap<Ids, (u64, DateTime<Utc>)> = HashMap::new();

    loop {
        tokio::select! {
            _ = ticker.tick() => {

                match collect_metrics_iteration(&http_client, &mut cached_metrics, metric_collection_endpoint, hostname.clone()).await
                {
                    Err(e) => {
                        error!("Failed to send consumption metrics: {} ", e);
                    },
                    Ok(_) => { trace!("collect_metrics_iteration completed successfully") },
                }
            }
        }
    }
}

fn gather_proxy_io_bytes_per_client() -> Vec<(Ids, (u64, DateTime<Utc>))> {
    let mut current_metrics: Vec<(Ids, (u64, DateTime<Utc>))> = Vec::new();
    let metrics = prometheus::default_registry().gather();

    for m in metrics {
        if m.get_name() == "proxy_io_bytes_per_client" {
            for ms in m.get_metric() {
                let direction = ms
                    .get_label()
                    .iter()
                    .find(|l| l.get_name() == "direction")
                    .unwrap()
                    .get_value();

                // Only collect metric for outbound traffic
                if direction == "tx" {
                    let endpoint_id = ms
                        .get_label()
                        .iter()
                        .find(|l| l.get_name() == "endpoint_id")
                        .unwrap()
                        .get_value();
                    let value = ms.get_counter().get_value() as u64;

                    debug!("endpoint_id:val - {}: {}", endpoint_id, value);
                    current_metrics.push((
                        Ids {
                            endpoint_id: endpoint_id.to_string(),
                        },
                        (value, Utc::now()),
                    ));
                }
            }
        }
    }

    current_metrics
}

#[instrument(skip_all)]
async fn collect_metrics_iteration(
    client: &http::ClientWithMiddleware,
    cached_metrics: &mut HashMap<Ids, (u64, DateTime<Utc>)>,
    metric_collection_endpoint: &reqwest::Url,
    hostname: String,
) -> anyhow::Result<()> {
    info!(
        "starting collect_metrics_iteration. metric_collection_endpoint: {}",
        metric_collection_endpoint
    );

    let current_metrics = gather_proxy_io_bytes_per_client();

    let metrics_to_send: Vec<Event<Ids>> = current_metrics
        .iter()
        .filter_map(|(curr_key, (curr_val, curr_time))| {
            let mut start_time = *curr_time;
            let mut value = *curr_val;

            if let Some((prev_val, prev_time)) = cached_metrics.get(curr_key) {
                // Only send metrics updates if the metric has changed
                if curr_val - prev_val > 0 {
                    value = curr_val - prev_val;
                    start_time = *prev_time;
                } else {
                    return None;
                }
            };

            Some(Event {
                kind: EventType::Incremental {
                    start_time,
                    stop_time: *curr_time,
                },
                metric: PROXY_IO_BYTES_PER_CLIENT,
                idempotency_key: idempotency_key(hostname.clone()),
                value,
                extra: Ids {
                    endpoint_id: curr_key.endpoint_id.clone(),
                },
            })
        })
        .collect();

    if metrics_to_send.is_empty() {
        trace!("no new metrics to send");
        return Ok(());
    }

    // Send metrics.
    // Split into chunks of 1000 metrics to avoid exceeding the max request size
    for chunk in metrics_to_send.chunks(CHUNK_SIZE) {
        let chunk_json = serde_json::value::to_raw_value(&EventChunk { events: chunk })
            .expect("ProxyConsumptionMetric should not fail serialization");

        let res = client
            .post(metric_collection_endpoint.clone())
            .json(&chunk_json)
            .send()
            .await;

        let res = match res {
            Ok(x) => x,
            Err(err) => {
                error!("failed to send metrics: {:?}", err);
                continue;
            }
        };

        if res.status().is_success() {
            // update cached metrics after they were sent successfully
            for send_metric in chunk {
                let stop_time = match send_metric.kind {
                    EventType::Incremental { stop_time, .. } => stop_time,
                    _ => unreachable!(),
                };

                cached_metrics
                    .entry(Ids {
                        endpoint_id: send_metric.extra.endpoint_id.clone(),
                    })
                    // update cached value (add delta) and time
                    .and_modify(|e| {
                        e.0 += send_metric.value;
                        e.1 = stop_time
                    })
                    // cache new metric
                    .or_insert((send_metric.value, stop_time));
            }
        } else {
            error!("metrics endpoint refused the sent metrics: {:?}", res);
        }
    }
    Ok(())
}
