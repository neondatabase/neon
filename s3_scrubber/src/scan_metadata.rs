use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::checks::{
    branch_cleanup_and_check_errors, list_timeline_blobs, BlobDataParseResult, S3TimelineBlobData,
    TimelineAnalysis,
};
use crate::metadata_stream::{stream_tenant_timelines, stream_tenants};
use crate::{init_logging, init_s3_client, BucketConfig, RootTarget, S3Target, CLI_NAME};
use aws_sdk_s3::Client;
use aws_types::region::Region;
use futures_util::{pin_mut, StreamExt, TryStreamExt};
use histogram::Histogram;
use pageserver::tenant::{IndexPart, TENANTS_SEGMENT_NAME};
use utils::id::TenantTimelineId;

pub struct MetadataSummary {
    count: usize,
    with_errors: HashSet<TenantTimelineId>,
    with_warnings: HashSet<TenantTimelineId>,
    with_garbage: HashSet<TenantTimelineId>,
    indices_by_version: HashMap<usize, usize>,

    layer_count: MinMaxHisto,
    timeline_size_bytes: MinMaxHisto,
    layer_size_bytes: MinMaxHisto,
}

/// A histogram plus minimum and maximum tracking
struct MinMaxHisto {
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
        self.timeline_size_bytes.sample(total_size)?;

        Ok(())
    }

    fn update_data(&mut self, data: &S3TimelineBlobData) {
        self.count += 1;
        if let BlobDataParseResult::Parsed {
            index_part,
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

    fn update_analysis(&mut self, id: &TenantTimelineId, analysis: &TimelineAnalysis) {
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
Timeline size bytes: {4}
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
}

/// Scan the pageserver metadata in an S3 bucket, reporting errors and statistics.
pub async fn scan_metadata(bucket_config: BucketConfig) -> anyhow::Result<MetadataSummary> {
    let file_name = format!(
        "{}_scan_metadata_{}_{}.log",
        CLI_NAME,
        bucket_config.bucket,
        chrono::Utc::now().format("%Y_%m_%d__%H_%M_%S")
    );

    let _guard = init_logging(&file_name);

    let s3_client = Arc::new(init_s3_client(
        bucket_config.sso_account_id,
        Region::new(bucket_config.region),
    ));
    let delimiter = "/";
    let target = RootTarget::Pageserver(S3Target {
        bucket_name: bucket_config.bucket.to_string(),
        prefix_in_bucket: ["pageserver", "v1", TENANTS_SEGMENT_NAME, ""].join(delimiter),
        delimiter: delimiter.to_string(),
    });

    let tenants = stream_tenants(&s3_client, &target);

    // How many tenants to process in parallel.  We need to be mindful of pageservers
    // accessing the same per tenant prefixes, so use a lower setting than pageservers.
    const CONCURRENCY: usize = 32;

    // Generate a stream of TenantTimelineId
    let timelines = tenants.map_ok(|t| stream_tenant_timelines(&s3_client, &target, t));
    let timelines = timelines.try_buffer_unordered(CONCURRENCY);
    let timelines = timelines.try_flatten();

    // Generate a stream of S3TimelineBlobData
    async fn report_on_timeline(
        s3_client: &Client,
        target: &RootTarget,
        ttid: TenantTimelineId,
    ) -> anyhow::Result<(TenantTimelineId, S3TimelineBlobData)> {
        let data = list_timeline_blobs(s3_client, ttid, target).await?;
        Ok((ttid, data))
    }
    let timelines = timelines.map_ok(|ttid| report_on_timeline(&s3_client, &target, ttid));
    let timelines = timelines.try_buffer_unordered(CONCURRENCY);

    let mut summary = MetadataSummary::new();
    pin_mut!(timelines);
    while let Some(i) = timelines.next().await {
        let (ttid, data) = i?;
        summary.update_data(&data);

        let analysis =
            branch_cleanup_and_check_errors(&ttid, &target, None, None, Some(data)).await;

        summary.update_analysis(&ttid, &analysis);
    }

    Ok(summary)
}
