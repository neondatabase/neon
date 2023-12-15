use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::checks::{
    branch_cleanup_and_check_errors, list_timeline_blobs, BlobDataParseResult, S3TimelineBlobData,
    TimelineAnalysis,
};
use crate::metadata_stream::stream_tenants;
use crate::{init_remote, BucketConfig, NodeKind, TenantShardTimelineId};

use futures_util::StreamExt;
use histogram::Histogram;
use pageserver::tenant::IndexPart;

use pageserver_api::shard::TenantShardId;
use serde::Serialize;
use tracing::Instrument;

#[derive(Serialize)]
pub struct MetadataSummary {
    count: usize,
    with_errors: HashSet<TenantShardTimelineId>,
    with_warnings: HashSet<TenantShardTimelineId>,
    with_garbage: HashSet<TenantShardTimelineId>,
    indices_by_version: HashMap<usize, usize>,

    layer_count: MinMaxHisto,
    timeline_size_bytes: MinMaxHisto,
    layer_size_bytes: MinMaxHisto,
}

/// A histogram plus minimum and maximum tracking
#[derive(Serialize)]
struct MinMaxHisto {
    #[serde(skip)]
    histo: Histogram,
    min: u64,
    max: u64,
}

impl MinMaxHisto {
    fn new() -> Self {
        Self {
            histo: histogram::Histogram::builder()
                .build()
                .expect("Bad histogram params"),
            min: u64::MAX,
            max: 0,
        }
    }

    fn sample(&mut self, v: u64) -> Result<(), histogram::Error> {
        self.min = std::cmp::min(self.min, v);
        self.max = std::cmp::max(self.max, v);
        let r = self.histo.increment(v, 1);

        if r.is_err() {
            tracing::warn!("Bad histogram sample: {v}");
        }

        r
    }

    fn oneline(&self) -> String {
        let percentiles = match self.histo.percentiles(&[1.0, 10.0, 50.0, 90.0, 99.0]) {
            Ok(p) => p,
            Err(e) => return format!("No data: {}", e),
        };

        let percentiles: Vec<u64> = percentiles
            .iter()
            .map(|p| p.bucket().low() + p.bucket().high() / 2)
            .collect();

        format!(
            "min {}, 1% {}, 10% {}, 50% {}, 90% {}, 99% {}, max {}",
            self.min,
            percentiles[0],
            percentiles[1],
            percentiles[2],
            percentiles[3],
            percentiles[4],
            self.max,
        )
    }
}

impl MetadataSummary {
    fn new() -> Self {
        Self {
            count: 0,
            with_errors: HashSet::new(),
            with_warnings: HashSet::new(),
            with_garbage: HashSet::new(),
            indices_by_version: HashMap::new(),
            layer_count: MinMaxHisto::new(),
            timeline_size_bytes: MinMaxHisto::new(),
            layer_size_bytes: MinMaxHisto::new(),
        }
    }

    fn update_histograms(&mut self, index_part: &IndexPart) -> Result<(), histogram::Error> {
        self.layer_count
            .sample(index_part.layer_metadata.len() as u64)?;
        let mut total_size: u64 = 0;
        for meta in index_part.layer_metadata.values() {
            total_size += meta.file_size;
            self.layer_size_bytes.sample(meta.file_size)?;
        }
        self.timeline_size_bytes.sample(total_size / 1024)?;

        Ok(())
    }

    fn update_data(&mut self, data: &S3TimelineBlobData) {
        self.count += 1;
        if let BlobDataParseResult::Parsed {
            index_part,
            index_part_generation: _,
            s3_layers: _,
        } = &data.blob_data
        {
            *self
                .indices_by_version
                .entry(index_part.get_version())
                .or_insert(0) += 1;

            if let Err(e) = self.update_histograms(index_part) {
                // Value out of range?  Warn that the results are untrustworthy
                tracing::warn!(
                    "Error updating histograms, summary stats may be wrong: {}",
                    e
                );
            }
        }
    }

    fn update_analysis(&mut self, id: &TenantShardTimelineId, analysis: &TimelineAnalysis) {
        if !analysis.errors.is_empty() {
            self.with_errors.insert(*id);
        }

        if !analysis.warnings.is_empty() {
            self.with_warnings.insert(*id);
        }
    }

    /// Long-form output for printing at end of a scan
    pub fn summary_string(&self) -> String {
        let version_summary: String = itertools::join(
            self.indices_by_version
                .iter()
                .map(|(k, v)| format!("{k}: {v}")),
            ", ",
        );

        format!(
            "Timelines: {0}
With errors: {1}
With warnings: {2}
With garbage: {3}
Index versions: {version_summary}
Timeline size KiB: {4}
Layer size bytes: {5}
Timeline layer count: {6}
",
            self.count,
            self.with_errors.len(),
            self.with_warnings.len(),
            self.with_garbage.len(),
            self.timeline_size_bytes.oneline(),
            self.layer_size_bytes.oneline(),
            self.layer_count.oneline(),
        )
    }

    pub fn is_fatal(&self) -> bool {
        !self.with_errors.is_empty()
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

#[derive(Debug)]
enum Either<A, B> {
    Left(A),
    Right(B),
}

/// Scan the pageserver metadata in an S3 bucket, reporting errors and statistics.
pub async fn scan_metadata(
    bucket_config: BucketConfig,
    tenant_ids: Vec<TenantShardId>,
) -> anyhow::Result<MetadataSummary> {
    let (s3_client, target) = init_remote(bucket_config, NodeKind::Pageserver)?;
    let target = Arc::new(target);

    let tenants = if tenant_ids.is_empty() {
        futures::future::Either::Left(stream_tenants(&s3_client, &target))
    } else {
        futures::future::Either::Right(futures::stream::iter(tenant_ids.into_iter().map(Ok)))
    };

    let tenants = tenants.fuse();

    let mut tenants = std::pin::pin!(tenants);

    let mut js = tokio::task::JoinSet::new();
    let mut consumed_all = false;

    let summary = MetadataSummary::new();

    // have timeline and timeline blob listings fight over the same semaphore
    let timeline_listings = Arc::new(tokio::sync::Semaphore::new(50));
    let blob_listings = timeline_listings.clone();

    let spawned_tenants = AtomicUsize::new(0);
    let spawned_timelines = Arc::new(AtomicUsize::new(0));
    let completed_tenants = AtomicUsize::new(0);
    let completed_timelines = AtomicUsize::new(0);

    let s3_client = s3_client.clone();
    let target = target.clone();

    let summary = std::sync::Mutex::new(summary);

    let scan_tenants = async {
        let timeline_listings = timeline_listings;
        let blob_listings = blob_listings;

        // used to control whether to receive more tenants
        let mut more_tenants = true;

        loop {
            let next_start = tokio::select! {
                next_tenant = tenants.next(), if !consumed_all && more_tenants => {
                    match next_tenant {
                        Some(Ok(tenant_id)) => Either::Left(tenant_id),
                        Some(Err(e)) => {
                            consumed_all = true;
                            tracing::error!("tenant streaming failed with: {e:?}");
                            continue;
                        }
                        None => {
                            consumed_all = true;
                            continue;
                        }
                    }
                },

                next = js.join_next(), if !js.is_empty() => {
                    more_tenants = js.len() < 10;

                    match next.unwrap() {
                        Ok(Either::Left((tenant_id, timelines))) => {
                            completed_tenants.fetch_add(1, Ordering::Relaxed);

                            Either::Right((tenant_id, timelines))
                        }
                        Ok(Either::Right(Some((ttid, data)))) => {
                            completed_timelines.fetch_add(1, Ordering::Relaxed);

                            let ttid: TenantShardTimelineId = ttid;
                            {
                                let _e = tracing::info_span!("analysis", tenant_shard_id=%ttid.tenant_shard_id, timeline_id=%ttid.timeline_id).entered();
                                let summary = &mut summary.lock().unwrap();
                                summary.update_data(&data);
                                let analysis = branch_cleanup_and_check_errors(&ttid, &target, None, None, Some(data));
                                summary.update_analysis(&ttid, &analysis);
                            }
                            continue;
                        }
                        Ok(Either::Right(None)) => {
                            completed_timelines.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        Err(je) if je.is_cancelled() => unreachable!("not used"),
                        Err(je) if je.is_panic() => {
                            continue;
                        },
                        Err(je) => {
                            tracing::error!("unknown join error: {je:?}");
                            continue;
                        }
                    }
                },

                else => break,
            };

            let s3_client = s3_client.clone();
            let target = target.clone();
            let timeline_listings = timeline_listings.clone();
            let blob_listings = blob_listings.clone();

            match next_start {
                Either::Left(tenant_shard_id) => {
                    let span = tracing::info_span!("get_timelines", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug());
                    js.spawn(
                        async move {
                            let _permit = timeline_listings.acquire().await;
                            let timelines = crate::metadata_stream::get_tenant_timelines(
                                &s3_client,
                                &target,
                                tenant_shard_id,
                            )
                            .await;

                            Either::Left((tenant_shard_id, timelines))
                        }
                        .instrument(span),
                    );

                    more_tenants = js.len() < 1000;

                    spawned_tenants.fetch_add(1, Ordering::Relaxed);
                }
                Either::Right((tenant_shard_id, timelines)) => {
                    for timeline_id in timelines {
                        let timeline_id = match timeline_id {
                            Ok(timeline_id) => timeline_id,
                            Err(e) => {
                                tracing::error!("failed to fetch a timeline: {e:?}");
                                continue;
                            }
                        };
                        let s3_client = s3_client.clone();
                        let target = target.clone();
                        let blob_listings = blob_listings.clone();
                        let span = tracing::info_span!("list_timelines_blobs", tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(), %timeline_id);
                        js.spawn(
                            async move {
                                let _permit = blob_listings.acquire().await;
                                let ttid = TenantShardTimelineId::new(tenant_shard_id, timeline_id);
                                match list_timeline_blobs(&s3_client, ttid, &target).await {
                                    Ok(data) => Either::Right(Some((ttid, data))),
                                    Err(e) => {
                                        tracing::error!("listing failed {e:?}");
                                        Either::Right(None)
                                    }
                                }
                            }
                            .instrument(span),
                        );

                        spawned_timelines.fetch_add(1, Ordering::Relaxed);
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
    };

    let started_at = std::time::Instant::now();

    {
        let mut scan_tenants = std::pin::pin!(scan_tenants);

        loop {
            let res =
                tokio::time::timeout(std::time::Duration::from_secs(1), &mut scan_tenants).await;

            let spawned_tenants = spawned_tenants.load(Ordering::Relaxed);
            let completed_tenants = completed_tenants.load(Ordering::Relaxed);
            let spawned_timelines = spawned_timelines.load(Ordering::Relaxed);
            let completed_timelines = completed_timelines.load(Ordering::Relaxed);

            match res {
                Ok(()) => {
                    tracing::info!("progress tenants: {completed_tenants} / {spawned_tenants}, timelines: {completed_timelines} / {spawned_timelines} after {:?}", started_at.elapsed());
                    break;
                }
                Err(_timeout) => {
                    tracing::info!("progress tenants: {completed_tenants} / {spawned_tenants}, timelines: {completed_timelines} / {spawned_timelines}");
                }
            }
        }
    }

    Ok(summary.into_inner().unwrap())
}
