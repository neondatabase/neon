use std::time::{Duration, UNIX_EPOCH};

use crate::checks::{list_timeline_blobs, BlobDataParseResult};
use crate::metadata_stream::{stream_tenant_timelines, stream_tenants};
use crate::{init_remote, BucketConfig, NodeKind, RootTarget, TenantShardTimelineId};
use aws_sdk_s3::Client;
use futures_util::{StreamExt, TryStreamExt};
use pageserver::tenant::remote_timeline_client::parse_remote_index_path;
use pageserver::tenant::IndexPart;
use pageserver_api::shard::TenantShardId;
use remote_storage::RemotePath;
use serde::Serialize;
use tracing::{info_span, Instrument};
use utils::generation::Generation;

#[derive(Serialize, Default)]
pub struct GcSummary {
    indices_deleted: usize,
    remote_storage_errors: usize,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
pub enum GcMode {
    // Delete nothing
    DryRun,

    // Enable only removing old-generation indices
    IndicesOnly,
    // Enable all forms of GC
    // TODO: this will be used when shard split ancestor layer deletion is added
    // All,
}

impl std::fmt::Display for GcMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GcMode::DryRun => write!(f, "dry-run"),
            GcMode::IndicesOnly => write!(f, "indices-only"),
        }
    }
}

async fn maybe_delete_index(
    s3_client: &Client,
    bucket_config: &BucketConfig,
    min_age: &Duration,
    latest_gen: Generation,
    key: &str,
    mode: GcMode,
    summary: &mut GcSummary,
) {
    // Validation: we will only delete things that parse cleanly
    let basename = key.rsplit_once('/').unwrap().1;
    let candidate_generation =
        match parse_remote_index_path(RemotePath::from_string(basename).unwrap()) {
            Some(g) => g,
            None => {
                if basename == IndexPart::FILE_NAME {
                    // A legacy pre-generation index
                    Generation::none()
                } else {
                    // A strange key: we will not delete this because we don't understand it.
                    tracing::warn!("Bad index key");
                    return;
                }
            }
        };

    // Validation: we will only delete indices more than one generation old, to avoid interfering
    // in typical migrations, even if they are very long running.
    if candidate_generation >= latest_gen {
        // This shouldn't happen: when we loaded metadata, it should have selected the latest
        // generation already, and only populated [`S3TimelineBlobData::unused_index_keys`]
        // with older generations.
        tracing::warn!("Deletion candidate is >= latest generation, this is a bug!");
        return;
    } else if candidate_generation.next() == latest_gen {
        // Skip deleting the latest-1th generation's index.
        return;
    }

    // Validation: we will only delete indices after one week, so that during incidents we will have
    // easy access to recent indices.
    let age: Duration = match s3_client
        .head_object()
        .bucket(&bucket_config.bucket)
        .key(key)
        .send()
        .await
    {
        Ok(response) => match response.last_modified {
            None => {
                tracing::warn!("Missing last_modified");
                summary.remote_storage_errors += 1;
                return;
            }
            Some(last_modified) => {
                let last_modified =
                    UNIX_EPOCH + Duration::from_secs_f64(last_modified.as_secs_f64());
                match last_modified.elapsed() {
                    Ok(e) => e,
                    Err(_) => {
                        tracing::warn!("Bad last_modified time: {last_modified:?}");
                        return;
                    }
                }
            }
        },
        Err(e) => {
            tracing::warn!("Failed to HEAD {key}: {e}");
            summary.remote_storage_errors += 1;
            return;
        }
    };
    if &age < min_age {
        tracing::info!(
            "Skipping young object {} < {}",
            age.as_secs_f64(),
            min_age.as_secs_f64()
        );
        return;
    }

    if matches!(mode, GcMode::DryRun) {
        tracing::info!("Dry run: would delete this key");
        return;
    }

    // All validations passed: erase the object
    match s3_client
        .delete_object()
        .bucket(&bucket_config.bucket)
        .key(key)
        .send()
        .await
    {
        Ok(_) => {
            tracing::info!("Successfully deleted index");
            summary.indices_deleted += 1;
        }
        Err(e) => {
            tracing::warn!("Failed to delete index: {e}");
            summary.remote_storage_errors += 1;
        }
    }
}

/// Physical garbage collection: removing unused S3 objects.  This is distinct from the garbage collection
/// done inside the pageserver, which operates at a higher level (keys, layers).  This type of garbage collection
/// is about removing:
/// - Objects that were uploaded but never referenced in the remote index (e.g. because of a shutdown between
///   uploading a layer and uploading an index)
/// - Index objects from historic generations
///
/// This type of GC is not necessary for correctness: rather it serves to reduce wasted storage capacity, and
/// make sure that object listings don't get slowed down by large numbers of garbage objects.
pub async fn pageserver_physical_gc(
    bucket_config: BucketConfig,
    tenant_ids: Vec<TenantShardId>,
    min_age: Duration,
    mode: GcMode,
) -> anyhow::Result<GcSummary> {
    let (s3_client, target) = init_remote(bucket_config.clone(), NodeKind::Pageserver)?;

    let tenants = if tenant_ids.is_empty() {
        futures::future::Either::Left(stream_tenants(&s3_client, &target))
    } else {
        futures::future::Either::Right(futures::stream::iter(tenant_ids.into_iter().map(Ok)))
    };

    // How many tenants to process in parallel.  We need to be mindful of pageservers
    // accessing the same per tenant prefixes, so use a lower setting than pageservers.
    const CONCURRENCY: usize = 32;

    // Generate a stream of TenantTimelineId
    let timelines = tenants.map_ok(|t| stream_tenant_timelines(&s3_client, &target, t));
    let timelines = timelines.try_buffered(CONCURRENCY);
    let timelines = timelines.try_flatten();

    // Generate a stream of S3TimelineBlobData
    async fn gc_timeline(
        s3_client: &Client,
        bucket_config: &BucketConfig,
        min_age: &Duration,
        target: &RootTarget,
        mode: GcMode,
        ttid: TenantShardTimelineId,
    ) -> anyhow::Result<GcSummary> {
        let mut summary = GcSummary::default();
        let data = list_timeline_blobs(s3_client, ttid, target).await?;

        let (latest_gen, candidates) = match &data.blob_data {
            BlobDataParseResult::Parsed {
                index_part: _index_part,
                index_part_generation,
                s3_layers: _s3_layers,
            } => (*index_part_generation, data.unused_index_keys),
            BlobDataParseResult::Relic => {
                // Post-deletion tenant location: don't try and GC it.
                return Ok(summary);
            }
            BlobDataParseResult::Incorrect(reasons) => {
                // Our primary purpose isn't to report on bad data, but log this rather than skipping silently
                tracing::warn!("Skipping timeline {ttid}, bad metadata: {reasons:?}");
                return Ok(summary);
            }
        };

        for key in candidates {
            maybe_delete_index(
                s3_client,
                bucket_config,
                min_age,
                latest_gen,
                &key,
                mode,
                &mut summary,
            )
            .instrument(info_span!("maybe_delete_index", %ttid, ?latest_gen, key))
            .await;
        }

        Ok(summary)
    }
    let timelines = timelines
        .map_ok(|ttid| gc_timeline(&s3_client, &bucket_config, &min_age, &target, mode, ttid));
    let mut timelines = std::pin::pin!(timelines.try_buffered(CONCURRENCY));

    let mut summary = GcSummary::default();

    while let Some(i) = timelines.next().await {
        let tl_summary = i?;

        summary.indices_deleted += tl_summary.indices_deleted;
        summary.remote_storage_errors += tl_summary.remote_storage_errors;
    }

    Ok(summary)
}
