//! Periodically collect proxy consumption metrics
//! and push them to a HTTP endpoint.
use crate::{config::MetricCollectionConfig, http};
use chrono::{DateTime, Utc};
use consumption_metrics::{idempotency_key, Event, EventChunk, EventType, CHUNK_SIZE};
use serde::Serialize;
use std::{collections::HashMap, convert::Infallible, time::Duration};
use tracing::{error, info, instrument, trace, warn};

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

pub async fn task_main(config: &MetricCollectionConfig) -> anyhow::Result<Infallible> {
    info!("metrics collector config: {config:?}");
    scopeguard::defer! {
        info!("metrics collector has shut down");
    }

    let http_client = http::new_client_with_timeout(DEFAULT_HTTP_REPORTING_TIMEOUT);
    let mut cached_metrics: HashMap<Ids, (u64, DateTime<Utc>)> = HashMap::new();
    let hostname = hostname::get()?.as_os_str().to_string_lossy().into_owned();

    let mut ticker = tokio::time::interval(config.interval);
    loop {
        ticker.tick().await;

        let res = collect_metrics_iteration(
            &http_client,
            &mut cached_metrics,
            &config.endpoint,
            &hostname,
        )
        .await;

        match res {
            Err(e) => error!("failed to send consumption metrics: {e} "),
            Ok(_) => trace!("periodic metrics collection completed successfully"),
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
                    let branch_id = ms
                        .get_label()
                        .iter()
                        .find(|l| l.get_name() == "branch_id")
                        .unwrap()
                        .get_value();

                    let value = ms.get_counter().get_value() as u64;

                    // Report if the metric value is suspiciously large
                    if value > (1u64 << 40) {
                        warn!(
                            "potentially abnormal counter value: branch_id {} endpoint_id {} val: {}",
                            branch_id, endpoint_id, value
                        );
                    }

                    current_metrics.push((
                        Ids {
                            endpoint_id: endpoint_id.to_string(),
                            branch_id: branch_id.to_string(),
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
    hostname: &str,
) -> anyhow::Result<()> {
    info!(
        "starting collect_metrics_iteration. metric_collection_endpoint: {}",
        metric_collection_endpoint
    );

    let current_metrics = gather_proxy_io_bytes_per_client();

    let metrics_to_send: Vec<Event<Ids, &'static str>> = current_metrics
        .iter()
        .filter_map(|(curr_key, (curr_val, curr_time))| {
            let mut start_time = *curr_time;
            let mut value = *curr_val;

            if let Some((prev_val, prev_time)) = cached_metrics.get(curr_key) {
                // Only send metrics updates if the metric has increased
                if curr_val > prev_val {
                    value = curr_val - prev_val;
                    start_time = *prev_time;
                } else {
                    if curr_val < prev_val {
                        error!("proxy_io_bytes_per_client metric value decreased from {} to {} for key {:?}",
                        prev_val, curr_val, curr_key);
                    }
                    return None;
                }
            };

            Some(Event {
                kind: EventType::Incremental {
                    start_time,
                    stop_time: *curr_time,
                },
                metric: PROXY_IO_BYTES_PER_CLIENT,
                idempotency_key: idempotency_key(hostname),
                value,
                extra: Ids {
                    endpoint_id: curr_key.endpoint_id.clone(),
                    branch_id: curr_key.branch_id.clone(),
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
        let res = client
            .post(metric_collection_endpoint.clone())
            .json(&EventChunk {
                events: chunk.into(),
            })
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
        // update cached metrics after they were sent
        // (to avoid sending the same metrics twice)
        // see the relevant discussion on why to do so even if the status is not success:
        // https://github.com/neondatabase/neon/pull/4563#discussion_r1246710956
        for send_metric in chunk {
            let stop_time = match send_metric.kind {
                EventType::Incremental { stop_time, .. } => stop_time,
                _ => unreachable!(),
            };

            cached_metrics
                .entry(Ids {
                    endpoint_id: send_metric.extra.endpoint_id.clone(),
                    branch_id: send_metric.extra.branch_id.clone(),
                })
                // update cached value (add delta) and time
                .and_modify(|e| {
                    e.0 = e.0.saturating_add(send_metric.value);
                    e.1 = stop_time
                })
                // cache new metric
                .or_insert((send_metric.value, stop_time));
        }
    }
    Ok(())
}
