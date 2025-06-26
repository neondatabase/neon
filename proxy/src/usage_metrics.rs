//! Periodically collect proxy consumption metrics
//! and push them to a HTTP endpoint.
use std::borrow::Cow;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, bail};
use async_compression::tokio::write::GzipEncoder;
use bytes::Bytes;
use chrono::{DateTime, Datelike, Timelike, Utc};
use clashmap::ClashMap;
use clashmap::mapref::entry::Entry;
use consumption_metrics::{CHUNK_SIZE, Event, EventChunk, EventType, idempotency_key};
use once_cell::sync::Lazy;
use remote_storage::{GenericRemoteStorage, RemotePath, TimeoutOrCancel};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument, trace, warn};
use utils::backoff;
use uuid::{NoContext, Timestamp};

use crate::config::MetricCollectionConfig;
use crate::context::parquet::{FAILED_UPLOAD_MAX_RETRIES, FAILED_UPLOAD_WARN_THRESHOLD};
use crate::http;
use crate::intern::{BranchIdInt, EndpointIdInt};

const PROXY_IO_BYTES_PER_CLIENT: &str = "proxy_io_bytes_per_client";

const HTTP_REPORTING_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_REPORTING_RETRY_DURATION: Duration = Duration::from_secs(60);

/// Key that uniquely identifies the object, this metric describes.
/// Currently, endpoint_id is enough, but this may change later,
/// so keep it in a named struct.
///
/// Both the proxy and the ingestion endpoint will live in the same region (or cell)
/// so while the project-id is unique across regions the whole pipeline will work correctly
/// because we enrich the event with project_id in the control-plane endpoint.
#[derive(Eq, Hash, PartialEq, Serialize, Deserialize, Debug, Clone)]
pub(crate) struct Ids {
    pub(crate) endpoint_id: EndpointIdInt,
    pub(crate) branch_id: BranchIdInt,
    #[serde(with = "none_as_empty_string")]
    pub(crate) private_link_id: Option<SmolStr>,
}

#[derive(Eq, Hash, PartialEq, Serialize, Deserialize, Debug, Clone)]
struct Extra {
    #[serde(flatten)]
    ids: Ids,
    direction: TrafficDirection,
}

mod none_as_empty_string {
    use serde::Deserialize;
    use smol_str::SmolStr;

    #[allow(clippy::ref_option)]
    pub fn serialize<S: serde::Serializer>(t: &Option<SmolStr>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(t.as_deref().unwrap_or(""))
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> Result<Option<SmolStr>, D::Error> {
        let s = SmolStr::deserialize(d)?;
        if s.is_empty() { Ok(None) } else { Ok(Some(s)) }
    }
}

#[derive(Eq, Hash, PartialEq, Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TrafficDirection {
    Ingress,
    Egress,
}

pub(crate) trait MetricCounterRecorder {
    /// Record that some bytes were sent from the proxy to the client
    fn record_egress(&self, bytes: u64);

    /// Record that some bytes were sent from the client to the proxy
    fn record_ingress(&self, bytes: u64);

    /// Record that some connections were opened
    fn record_connection(&self, count: usize);
}

trait MetricCounterReporter {
    fn get_metrics(&mut self) -> MetricsData;
    fn move_metrics(&self) -> MetricsData;
}

#[derive(Debug)]
pub(crate) struct MetricCounter {
    transmitted: AtomicU64,
    received: AtomicU64,
    opened_connections: AtomicUsize,
}

impl MetricCounterRecorder for MetricCounter {
    /// Record that some bytes were sent from the proxy to the client
    fn record_egress(&self, bytes: u64) {
        self.transmitted.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record that some bytes were sent from the proxy to the client
    fn record_ingress(&self, bytes: u64) {
        self.received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record that some connections were opened
    fn record_connection(&self, count: usize) {
        self.opened_connections.fetch_add(count, Ordering::Relaxed);
    }
}

impl MetricCounterReporter for MetricCounter {
    fn get_metrics(&mut self) -> MetricsData {
        MetricsData {
            received: *self.received.get_mut(),
            transmitted: *self.transmitted.get_mut(),
            connections: *self.opened_connections.get_mut(),
        }
    }

    fn move_metrics(&self) -> MetricsData {
        MetricsData {
            received: self.received.swap(0, Ordering::Relaxed),
            transmitted: self.transmitted.swap(0, Ordering::Relaxed),
            connections: self.opened_connections.swap(0, Ordering::Relaxed),
        }
    }
}

struct MetricsData {
    transmitted: u64,
    received: u64,
    connections: usize,
}

struct BytesSent {
    transmitted: u64,
    received: u64,
}

trait Clearable {
    /// extract the value that should be reported
    fn should_report(self: &Arc<Self>) -> Option<BytesSent>;
    /// Determine whether the counter should be cleared from the global map.
    fn should_clear(self: &mut Arc<Self>) -> bool;
}

impl<C: MetricCounterReporter> Clearable for C {
    fn should_report(self: &Arc<Self>) -> Option<BytesSent> {
        // heuristic to see if the branch is still open
        // if a clone happens while we are observing, the heuristic will be incorrect.
        //
        // Worst case is that we won't report an event for this endpoint.
        // However, for the strong count to be 1 it must have occured that at one instant
        // all the endpoints were closed, so missing a report because the endpoints are closed is valid.
        let is_open = Arc::strong_count(self) > 1;

        // update cached metrics eagerly, even if they can't get sent
        // (to avoid sending the same metrics twice)
        // see the relevant discussion on why to do so even if the status is not success:
        // https://github.com/neondatabase/neon/pull/4563#discussion_r1246710956
        let MetricsData {
            transmitted,
            received,
            connections,
        } = self.move_metrics();

        // Our only requirement is that we report in every interval if there was an open connection
        // if there were no opened connections since, then we don't need to report
        if transmitted == 0 && received == 0 && !is_open && connections == 0 {
            None
        } else {
            Some(BytesSent {
                transmitted,
                received,
            })
        }
    }
    fn should_clear(self: &mut Arc<Self>) -> bool {
        // we can't clear this entry if it's acquired elsewhere
        let Some(counter) = Arc::get_mut(self) else {
            return false;
        };
        let MetricsData {
            transmitted,
            received,
            connections,
        } = counter.get_metrics();
        // clear if there's no data to report
        transmitted == 0 && received == 0 && connections == 0
    }
}

// endpoint and branch IDs are not user generated so we don't run the risk of hash-dos
type FastHasher = std::hash::BuildHasherDefault<rustc_hash::FxHasher>;

#[derive(Default)]
pub(crate) struct Metrics {
    endpoints: ClashMap<Ids, Arc<MetricCounter>, FastHasher>,
}

impl Metrics {
    /// Register a new byte metrics counter for this endpoint
    pub(crate) fn register(&self, ids: Ids) -> Arc<MetricCounter> {
        let entry = if let Some(entry) = self.endpoints.get(&ids) {
            entry.clone()
        } else {
            self.endpoints
                .entry(ids)
                .or_insert_with(|| {
                    Arc::new(MetricCounter {
                        received: AtomicU64::new(0),
                        transmitted: AtomicU64::new(0),
                        opened_connections: AtomicUsize::new(0),
                    })
                })
                .clone()
        };

        entry.record_connection(1);
        entry
    }
}

pub(crate) static USAGE_METRICS: Lazy<Metrics> = Lazy::new(Metrics::default);

pub async fn task_main(config: &MetricCollectionConfig) -> anyhow::Result<Infallible> {
    info!("metrics collector config: {config:?}");
    scopeguard::defer! {
        info!("metrics collector has shut down");
    }

    let http_client = http::new_client_with_timeout(
        HTTP_REPORTING_REQUEST_TIMEOUT,
        HTTP_REPORTING_RETRY_DURATION,
    );
    let hostname = hostname::get()?.as_os_str().to_string_lossy().into_owned();

    // Even if the remote storage is not configured, we still want to clear the metrics.
    let storage = if let Some(config) = config
        .backup_metric_collection_config
        .remote_storage_config
        .as_ref()
    {
        Some(
            GenericRemoteStorage::from_config(config)
                .await
                .context("remote storage init")?,
        )
    } else {
        None
    };

    let mut prev = Utc::now();
    let mut ticker = tokio::time::interval(config.interval);
    loop {
        ticker.tick().await;

        let now = Utc::now();
        collect_metrics_iteration(
            &USAGE_METRICS.endpoints,
            &http_client,
            &config.endpoint,
            storage.as_ref(),
            config.backup_metric_collection_config.chunk_size,
            &hostname,
            prev,
            now,
        )
        .await;
        prev = now;
    }
}

fn collect_and_clear_metrics<C: Clearable>(
    endpoints: &ClashMap<Ids, Arc<C>, FastHasher>,
) -> Vec<(Ids, BytesSent)> {
    let mut metrics_to_clear = Vec::new();

    let metrics_to_send: Vec<(Ids, BytesSent)> = endpoints
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

    for metric in metrics_to_clear {
        match endpoints.entry(metric) {
            Entry::Occupied(mut counter) => {
                if counter.get_mut().should_clear() {
                    counter.remove_entry();
                }
            }
            Entry::Vacant(_) => {}
        }
    }
    metrics_to_send
}

fn create_event_chunks<'a>(
    metrics_to_send: &'a [(Ids, BytesSent)],
    hostname: &'a str,
    prev: DateTime<Utc>,
    now: DateTime<Utc>,
    chunk_size: usize,
) -> impl Iterator<Item = EventChunk<'a, Event<Extra, &'static str>>> + 'a {
    metrics_to_send
        .chunks(chunk_size)
        .map(move |chunk| EventChunk {
            events: chunk
                .iter()
                .flat_map(|(ids, bytes)| {
                    [
                        Event {
                            kind: EventType::Incremental {
                                start_time: prev,
                                stop_time: now,
                            },
                            metric: PROXY_IO_BYTES_PER_CLIENT,
                            idempotency_key: idempotency_key(hostname),
                            value: bytes.transmitted,
                            extra: Extra {
                                ids: ids.clone(),
                                direction: TrafficDirection::Egress,
                            },
                        },
                        Event {
                            kind: EventType::Incremental {
                                start_time: prev,
                                stop_time: now,
                            },
                            metric: PROXY_IO_BYTES_PER_CLIENT,
                            idempotency_key: idempotency_key(hostname),
                            value: bytes.received,
                            extra: Extra {
                                ids: ids.clone(),
                                direction: TrafficDirection::Ingress,
                            },
                        },
                    ]
                })
                .collect(),
        })
}

#[expect(clippy::too_many_arguments)]
#[instrument(skip_all)]
async fn collect_metrics_iteration(
    endpoints: &ClashMap<Ids, Arc<MetricCounter>, FastHasher>,
    client: &http::ClientWithMiddleware,
    metric_collection_endpoint: &reqwest::Url,
    storage: Option<&GenericRemoteStorage>,
    outer_chunk_size: usize,
    hostname: &str,
    prev: DateTime<Utc>,
    now: DateTime<Utc>,
) {
    info!(
        "starting collect_metrics_iteration. metric_collection_endpoint: {}",
        metric_collection_endpoint
    );

    let metrics_to_send = collect_and_clear_metrics(endpoints);

    if metrics_to_send.is_empty() {
        trace!("no new metrics to send");
    }

    let cancel = CancellationToken::new();
    let path_prefix = create_remote_path_prefix(now);

    // Send metrics.
    for chunk in create_event_chunks(&metrics_to_send, hostname, prev, now, outer_chunk_size) {
        tokio::join!(
            upload_main_events_chunked(client, metric_collection_endpoint, &chunk, CHUNK_SIZE),
            async {
                if let Err(e) = upload_backup_events(storage, &chunk, &path_prefix, &cancel).await {
                    error!("failed to upload consumption events to remote storage: {e:?}");
                }
            }
        );
    }
}

fn create_remote_path_prefix(now: DateTime<Utc>) -> String {
    format!(
        "year={year:04}/month={month:02}/day={day:02}/hour={hour:02}/{hour:02}:{minute:02}:{second:02}Z",
        year = now.year(),
        month = now.month(),
        day = now.day(),
        hour = now.hour(),
        minute = now.minute(),
        second = now.second(),
    )
}

async fn upload_main_events_chunked(
    client: &http::ClientWithMiddleware,
    metric_collection_endpoint: &reqwest::Url,
    chunk: &EventChunk<'_, Event<Extra, &str>>,
    subchunk_size: usize,
) {
    // Split into smaller chunks to avoid exceeding the max request size
    for subchunk in chunk.events.chunks(subchunk_size).map(|c| EventChunk {
        events: Cow::Borrowed(c),
    }) {
        let res = client
            .post(metric_collection_endpoint.clone())
            .json(&subchunk)
            .send()
            .await;

        let res = match res {
            Ok(x) => x,
            Err(err) => {
                // TODO: retry?
                error!("failed to send metrics: {:?}", err);
                continue;
            }
        };

        if !res.status().is_success() {
            error!("metrics endpoint refused the sent metrics: {:?}", res);
            for metric in subchunk.events.iter().filter(|e| e.value > (1u64 << 40)) {
                // Report if the metric value is suspiciously large
                warn!("potentially abnormal metric value: {:?}", metric);
            }
        }
    }
}

async fn upload_backup_events(
    storage: Option<&GenericRemoteStorage>,
    chunk: &EventChunk<'_, Event<Extra, &'static str>>,
    path_prefix: &str,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let Some(storage) = storage else {
        warn!("no remote storage configured");
        return Ok(());
    };

    let real_now = Utc::now();
    let id = uuid::Uuid::new_v7(Timestamp::from_unix(
        NoContext,
        real_now.second().into(),
        real_now.nanosecond(),
    ));
    let path = format!("{path_prefix}_{id}.ndjson.gz");
    let remote_path = match RemotePath::from_string(&path) {
        Ok(remote_path) => remote_path,
        Err(e) => {
            bail!("failed to create remote path from str {path}: {:?}", e);
        }
    };

    // TODO: This is async compression from Vec to Vec. Rewrite as byte stream.
    //       Use sync compression in blocking threadpool.
    let mut encoder = GzipEncoder::new(Vec::new());
    for event in chunk.events.iter() {
        let data = serde_json::to_vec(event).context("serialize metrics")?;
        encoder.write_all(&data).await.context("compress metrics")?;
        encoder.write_all(b"\n").await.context("compress metrics")?;
    }
    encoder.shutdown().await.context("compress metrics")?;
    let compressed_data: Bytes = encoder.get_ref().clone().into();
    backoff::retry(
        || async {
            let stream = futures::stream::once(futures::future::ready(Ok(compressed_data.clone())));
            storage
                .upload(stream, compressed_data.len(), &remote_path, None, cancel)
                .await
        },
        TimeoutOrCancel::caused_by_cancel,
        FAILED_UPLOAD_WARN_THRESHOLD,
        FAILED_UPLOAD_MAX_RETRIES,
        "usage_metrics_upload",
        cancel,
    )
    .await
    .ok_or_else(|| anyhow::Error::new(TimeoutOrCancel::Cancel))
    .and_then(|x| x)
    .with_context(|| format!("usage_metrics_upload: path={remote_path}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{BufRead, BufReader};
    use std::sync::{Arc, Mutex};

    use anyhow::Error;
    use camino_tempfile::tempdir;
    use chrono::Utc;
    use consumption_metrics::{Event, EventChunk};
    use http_body_util::BodyExt;
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response};
    use hyper_util::rt::TokioIo;
    use remote_storage::{RemoteStorageConfig, RemoteStorageKind};
    use tokio::net::TcpListener;
    use url::Url;

    use super::*;
    use crate::http;
    use crate::types::{BranchId, EndpointId};

    #[tokio::test]
    async fn metrics() {
        type Report = EventChunk<'static, Event<Extra, String>>;
        let reports: Arc<Mutex<Vec<Report>>> = Arc::default();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn({
            let reports = reports.clone();
            async move {
                loop {
                    if let Ok((stream, _addr)) = listener.accept().await {
                        let reports = reports.clone();
                        http1::Builder::new()
                            .serve_connection(
                                TokioIo::new(stream),
                                service_fn(move |req: Request<Incoming>| {
                                    let reports = reports.clone();
                                    async move {
                                        let bytes = req.into_body().collect().await?.to_bytes();
                                        let events = serde_json::from_slice(&bytes)?;
                                        reports.lock().unwrap().push(events);
                                        Ok::<_, Error>(Response::new(String::new()))
                                    }
                                }),
                            )
                            .await
                            .unwrap();
                    }
                }
            }
        });

        let metrics = Metrics::default();
        let client = http::new_client();
        let endpoint = Url::parse(&format!("http://{addr}")).unwrap();
        let now = Utc::now();

        let storage_test_dir = tempdir().unwrap();
        let local_fs_path = storage_test_dir.path().join("usage_metrics");
        fs::create_dir_all(&local_fs_path).unwrap();
        let storage = GenericRemoteStorage::from_config(&RemoteStorageConfig {
            storage: RemoteStorageKind::LocalFs {
                local_path: local_fs_path.clone(),
            },
            timeout: Duration::from_secs(10),
            small_timeout: Duration::from_secs(1),
        })
        .await
        .unwrap();

        let mut pushed_chunks: Vec<Report> = Vec::new();
        let mut stored_chunks: Vec<Report> = Vec::new();

        // no counters have been registered
        collect_metrics_iteration(
            &metrics.endpoints,
            &client,
            &endpoint,
            Some(&storage),
            1000,
            "foo",
            now,
            now,
        )
        .await;
        let r = std::mem::take(&mut *reports.lock().unwrap());
        assert!(r.is_empty());

        // register a new counter

        let counter = metrics.register(Ids {
            endpoint_id: (&EndpointId::from("e1")).into(),
            branch_id: (&BranchId::from("b1")).into(),
            private_link_id: None,
        });

        // the counter should be observed despite 0 egress
        collect_metrics_iteration(
            &metrics.endpoints,
            &client,
            &endpoint,
            Some(&storage),
            1000,
            "foo",
            now,
            now,
        )
        .await;
        let r = std::mem::take(&mut *reports.lock().unwrap());
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].events.len(), 2);
        assert_eq!(r[0].events[0].value, 0);
        assert_eq!(r[0].events[0].extra.direction, TrafficDirection::Egress);
        assert_eq!(r[0].events[1].value, 0);
        assert_eq!(r[0].events[1].extra.direction, TrafficDirection::Ingress);
        pushed_chunks.extend(r);

        // record egress
        counter.record_egress(1);

        // record ingress
        counter.record_ingress(2);

        // egress should be observered
        collect_metrics_iteration(
            &metrics.endpoints,
            &client,
            &endpoint,
            Some(&storage),
            1000,
            "foo",
            now,
            now,
        )
        .await;
        let r = std::mem::take(&mut *reports.lock().unwrap());
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].events.len(), 2);
        assert_eq!(r[0].events[0].value, 1);
        assert_eq!(r[0].events[0].extra.direction, TrafficDirection::Egress);
        assert_eq!(r[0].events[1].value, 2);
        assert_eq!(r[0].events[1].extra.direction, TrafficDirection::Ingress);
        pushed_chunks.extend(r);

        // release counter
        drop(counter);

        // we do not observe the counter
        collect_metrics_iteration(
            &metrics.endpoints,
            &client,
            &endpoint,
            Some(&storage),
            1000,
            "foo",
            now,
            now,
        )
        .await;
        let r = std::mem::take(&mut *reports.lock().unwrap());
        assert!(r.is_empty());

        // counter is unregistered
        assert!(metrics.endpoints.is_empty());

        let path_prefix = create_remote_path_prefix(now);
        for entry in walkdir::WalkDir::new(&local_fs_path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = local_fs_path.join(&path_prefix).to_string();
            if entry.path().to_str().unwrap().starts_with(&path) {
                let file = fs::File::open(entry.into_path()).unwrap();
                let decoder = flate2::bufread::GzDecoder::new(BufReader::new(file));
                let reader = BufReader::new(decoder);

                let mut events: Vec<Event<Extra, String>> = Vec::new();
                for line in reader.lines() {
                    let line = line.unwrap();
                    let event: Event<Extra, String> = serde_json::from_str(&line).unwrap();
                    events.push(event);
                }

                let report = Report {
                    events: Cow::Owned(events),
                };

                stored_chunks.push(report);
            }
        }
        storage_test_dir.close().ok();

        // sort by first event's idempotency key because the order of files is nondeterministic
        pushed_chunks.sort_by_cached_key(|c| c.events[0].idempotency_key.clone());
        stored_chunks.sort_by_cached_key(|c| c.events[0].idempotency_key.clone());
        assert_eq!(pushed_chunks, stored_chunks);
    }
}
