use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::SinkExt;
use futures_util::{StreamExt, TryStreamExt};
use pageserver::tenant::remote_timeline_client::remote_layer_path;
use pageserver_api::controller_api::MetadataHealthUpdateRequest;
use pageserver_api::shard::TenantShardId;
use remote_storage::GenericRemoteStorage;
use serde::Serialize;
use tracing::{Instrument, info_span};
use utils::generation::Generation;
use utils::id::TenantId;
use utils::shard::ShardCount;

use crate::checks::{
    BlobDataParseResult, RemoteTimelineBlobData, TenantObjectListing, TimelineAnalysis,
    branch_cleanup_and_check_errors, list_timeline_blobs,
};
use crate::metadata_stream::{
    stream_tenant_timelines, stream_tenants, stream_tenants_maybe_prefix,
};
use crate::{BucketConfig, NodeKind, RootTarget, TenantShardTimelineId, init_remote};

#[derive(Serialize, Default, Clone)]
pub struct MetadataSummary {
    tenant_count: usize,
    timeline_count: usize,
    timeline_shard_count: usize,
    /// Tenant-shard timeline (key) mapping to errors. The key has to be a string because it will be serialized to a JSON.
    /// The key is generated using `TenantShardTimelineId::to_string()`.
    with_errors: HashMap<String, Vec<String>>,
    /// Tenant-shard timeline (key) mapping to warnings. The key has to be a string because it will be serialized to a JSON.
    /// The key is generated using `TenantShardTimelineId::to_string()`.
    with_warnings: HashMap<String, Vec<String>>,
    with_orphans: HashSet<TenantShardTimelineId>,
    indices_by_version: HashMap<usize, usize>,

    #[serde(skip)]
    pub(crate) healthy_tenant_shards: HashSet<TenantShardId>,
    #[serde(skip)]
    pub(crate) unhealthy_tenant_shards: HashSet<TenantShardId>,
}

impl MetadataSummary {
    fn new() -> Self {
        Self::default()
    }

    fn update_data(&mut self, data: &RemoteTimelineBlobData) {
        self.timeline_shard_count += 1;
        if let BlobDataParseResult::Parsed {
            index_part,
            index_part_generation: _,
            s3_layers: _,
            index_part_last_modified_time: _,
            index_part_snapshot_time: _,
        } = &data.blob_data
        {
            *self
                .indices_by_version
                .entry(index_part.version())
                .or_insert(0) += 1;
        }
    }

    fn update_analysis(
        &mut self,
        id: &TenantShardTimelineId,
        analysis: &TimelineAnalysis,
        verbose: bool,
    ) {
        if analysis.is_healthy() {
            self.healthy_tenant_shards.insert(id.tenant_shard_id);
        } else {
            self.healthy_tenant_shards.remove(&id.tenant_shard_id);
            self.unhealthy_tenant_shards.insert(id.tenant_shard_id);
        }

        if !analysis.errors.is_empty() {
            let entry = self.with_errors.entry(id.to_string()).or_default();
            if verbose {
                entry.extend(analysis.errors.iter().cloned());
            }
        }

        if !analysis.warnings.is_empty() {
            let entry = self.with_warnings.entry(id.to_string()).or_default();
            if verbose {
                entry.extend(analysis.warnings.iter().cloned());
            }
        }
    }

    fn notify_timeline_orphan(&mut self, ttid: &TenantShardTimelineId) {
        self.with_orphans.insert(*ttid);
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
            "Tenants: {}
    Timelines: {}
    Timeline-shards: {}
    With errors: {}
    With warnings: {}
    With orphan layers: {}
    Index versions: {version_summary}
    ",
            self.tenant_count,
            self.timeline_count,
            self.timeline_shard_count,
            self.with_errors.len(),
            self.with_warnings.len(),
            self.with_orphans.len(),
        )
    }

    pub fn is_fatal(&self) -> bool {
        !self.with_errors.is_empty()
    }

    pub fn is_empty(&self) -> bool {
        self.timeline_shard_count == 0
    }

    pub fn build_health_update_request(&self) -> MetadataHealthUpdateRequest {
        MetadataHealthUpdateRequest {
            healthy_tenant_shards: self.healthy_tenant_shards.clone(),
            unhealthy_tenant_shards: self.unhealthy_tenant_shards.clone(),
        }
    }
}

/// Scan the pageserver metadata in an S3 bucket, reporting errors and statistics.
pub async fn scan_pageserver_metadata(
    bucket_config: BucketConfig,
    tenant_ids: Vec<TenantShardId>,
    tenant_id_prefix: Option<TenantId>,
    verbose: bool,
) -> anyhow::Result<MetadataSummary> {
    let (remote_client, target) = init_remote(bucket_config, NodeKind::Pageserver).await?;

    if !tenant_ids.is_empty() && tenant_id_prefix.is_some() {
        anyhow::bail!("`tenant_id_prefix` is not supported when `tenant_ids` is provided");
    }

    let (mut list_tenants_tx, list_tenants_rx) = futures::channel::mpsc::channel(1);
    let remote_client_inner = remote_client.clone();
    let target_inner = target.clone();
    let list_tenants = tokio::spawn(async move {
        let mut cnt = 0;
        if tenant_ids.is_empty() {
            if let Some(tenant_id_prefix) = tenant_id_prefix {
                let stream = stream_tenants_maybe_prefix(
                    &remote_client_inner,
                    &target_inner,
                    Some(tenant_id_prefix.to_string()),
                );
                let mut stream = Box::pin(stream);
                while let Some(tenant) = stream.next().await {
                    let tenant = tenant?;
                    list_tenants_tx.send(tenant).await?;
                    cnt += 1;
                }
            } else {
                let stream = stream_tenants(&remote_client_inner, &target_inner);
                let mut stream = Box::pin(stream);
                while let Some(tenant) = stream.next().await {
                    let tenant = tenant?;
                    list_tenants_tx.send(tenant).await?;
                    cnt += 1;
                }
            }
        } else {
            for tenant_id in tenant_ids {
                list_tenants_tx.send(tenant_id).await?;
                cnt += 1;
            }
        }
        tracing::info!("list_tenants: collected {} tenants", cnt);
        Ok::<_, anyhow::Error>(())
    });

    let (mut list_timelines_tx, list_timelines_rx) = futures::channel::mpsc::channel(1);
    let remote_client_inner = remote_client.clone();
    let target_inner = target.clone();
    let list_timelines = tokio::spawn(async move {
        let stream = list_tenants_rx
            .map(|tenant_id| {
                stream_tenant_timelines(&remote_client_inner, &target_inner, tenant_id)
            })
            .buffered(8)
            .try_flatten();
        let mut stream = Box::pin(stream);
        while let Some(item) = stream.next().await {
            let item = item?;
            list_timelines_tx.send(item).await?;
        }
        Ok::<_, anyhow::Error>(())
    });

    let (mut read_timelines_tx, read_timelines_rx) = futures::channel::mpsc::channel(1);
    let remote_client_inner = remote_client.clone();
    let target_inner = target.clone();
    let read_timelines = tokio::spawn(async move {
        let stream = list_timelines_rx
            .map(|ttid| report_on_timeline(&remote_client_inner, &target_inner, ttid))
            .buffered(32);
        let mut stream = Box::pin(stream);
        while let Some(item) = stream.next().await {
            let item = item?;
            read_timelines_tx.send(item).await?;
        }
        Ok::<_, anyhow::Error>(())
    });

    let summary = Arc::new(tokio::sync::Mutex::new(MetadataSummary::new()));
    let summary_inner = summary.clone();

    let (mut consolidate_tenants_tx, consolidate_tenants_rx) = futures::channel::mpsc::channel(32);
    let consolidate_tenants = tokio::spawn(async move {
        // We must gather all the TenantShardTimelineId->S3TimelineBlobData for each tenant, because different
        // shards in the same tenant might refer to one anothers' keys if a shard split has happened.

        let mut tenant_id = None;
        let mut tenant_objects = TenantObjectListing::default();
        let mut tenant_timeline_results = Vec::new();

        // Iterate through  all the timeline results.  These are in key-order, so
        // all results for the same tenant will be adjacent.  We accumulate these,
        // and then call `analyze_tenant` to flush, when we see the next tenant ID.
        let mut highest_shard_count = ShardCount::MIN;
        let mut read_timelines_rx = read_timelines_rx;
        while let Some(i) = read_timelines_rx.next().await {
            let (ttid, data) = i;
            {
                let mut guard = summary_inner.lock().await;
                guard.update_data(&data);
            }

            match tenant_id {
                Some(prev_tenant_id) => {
                    if prev_tenant_id != ttid.tenant_shard_id.tenant_id {
                        // New tenant: analyze this tenant's timelines, clear accumulated tenant_timeline_results
                        let tenant_objects = std::mem::take(&mut tenant_objects);
                        let timelines = std::mem::take(&mut tenant_timeline_results);
                        analyze_tenant(
                            summary_inner.clone(),
                            Arc::new(tokio::sync::Mutex::new(tenant_objects)),
                            timelines,
                            highest_shard_count,
                            &mut consolidate_tenants_tx,
                        )
                        .await?;
                        tenant_id = Some(ttid.tenant_shard_id.tenant_id);
                        highest_shard_count = ttid.tenant_shard_id.shard_count;
                    } else {
                        highest_shard_count =
                            highest_shard_count.max(ttid.tenant_shard_id.shard_count);
                    }
                }
                None => {
                    tenant_id = Some(ttid.tenant_shard_id.tenant_id);
                    highest_shard_count = highest_shard_count.max(ttid.tenant_shard_id.shard_count);
                }
            }

            match &data.blob_data {
                BlobDataParseResult::Parsed {
                    index_part: _,
                    index_part_generation: _index_part_generation,
                    s3_layers,
                    index_part_last_modified_time: _,
                    index_part_snapshot_time: _,
                } => {
                    tenant_objects.push(ttid, s3_layers.clone());
                }
                BlobDataParseResult::Relic => (),
                BlobDataParseResult::Incorrect {
                    errors: _,
                    s3_layers,
                } => {
                    tenant_objects.push(ttid, s3_layers.clone());
                }
            }
            tenant_timeline_results.push((ttid, data));
        }

        if !tenant_timeline_results.is_empty() {
            analyze_tenant(
                summary_inner.clone(),
                Arc::new(tokio::sync::Mutex::new(tenant_objects)),
                tenant_timeline_results,
                highest_shard_count,
                &mut consolidate_tenants_tx,
            )
            .await?;
        }
        Ok::<_, anyhow::Error>(())
    });

    let remote_client_inner = remote_client.clone();
    let summary_inner = summary.clone();
    let analyze_tenants = tokio::spawn(async move {
        let stream = consolidate_tenants_rx
            .map(|(ttid, tenant_objects, data)| {
                let remote_client_inner = remote_client_inner.clone();
                async move {
                    let generation = if let BlobDataParseResult::Parsed {
                        index_part: _,
                        index_part_generation,
                        s3_layers: _,
                        index_part_last_modified_time: _,
                        index_part_snapshot_time: _,
                    } = &data.blob_data
                    {
                        Some(*index_part_generation)
                    } else {
                        None
                    };

                    let res = branch_cleanup_and_check_errors(
                        &remote_client_inner,
                        &ttid,
                        tenant_objects.clone(),
                        None,
                        None,
                        Some(data),
                    )
                    .await;
                    (ttid, tenant_objects.clone(), generation, res)
                }
            })
            .buffered(32);
        let mut last_tenant = None;
        let mut last_tenant_objects = None;
        let mut timeline_generations = HashMap::new();
        let mut stream = Box::pin(stream);
        while let Some((ttid, tenant_objects, generation, res)) = stream.next().await {
            if last_tenant != Some(ttid) {
                if let Some(tenant_id) = last_tenant {
                    let timeline_generations = std::mem::take(&mut timeline_generations);
                    identify_orphans(
                        tenant_id.tenant_shard_id.tenant_id,
                        last_tenant_objects.take().unwrap(),
                        summary_inner.clone(),
                        &timeline_generations,
                    )
                    .await;
                }
                last_tenant = Some(ttid);
                last_tenant_objects = Some(tenant_objects);
            }
            if let Some(generation) = generation {
                timeline_generations.insert(ttid, generation);
            }
            {
                let mut guard = summary_inner.lock().await;
                guard.update_analysis(&ttid, &res, verbose);
            }
        }

        if let Some(tenant_id) = last_tenant {
            identify_orphans(
                tenant_id.tenant_shard_id.tenant_id,
                last_tenant_objects.take().unwrap(),
                summary_inner.clone(),
                &timeline_generations,
            )
            .await;
        }

        Ok::<_, anyhow::Error>(())
    });

    // Generate a stream of S3TimelineBlobData
    async fn report_on_timeline(
        remote_client: &GenericRemoteStorage,
        target: &RootTarget,
        ttid: TenantShardTimelineId,
    ) -> anyhow::Result<(TenantShardTimelineId, RemoteTimelineBlobData)> {
        tracing::info!("listing blobs for timeline: {}", ttid);
        let data = list_timeline_blobs(remote_client, ttid, target).await?;
        Ok((ttid, data))
    }

    // DO NOT call any long-running tasks in this function; always route them through the channel and let
    // other tokio tasks handle them.
    async fn analyze_tenant(
        summary: Arc<tokio::sync::Mutex<MetadataSummary>>,
        tenant_objects: Arc<tokio::sync::Mutex<TenantObjectListing>>,
        timelines: Vec<(TenantShardTimelineId, RemoteTimelineBlobData)>,
        highest_shard_count: ShardCount,
        output_tx: &mut futures::channel::mpsc::Sender<(
            TenantShardTimelineId,
            Arc<tokio::sync::Mutex<TenantObjectListing>>,
            RemoteTimelineBlobData,
        )>,
    ) -> anyhow::Result<()> {
        {
            let mut guard = summary.lock().await;
            guard.tenant_count += 1;
        }

        let mut timeline_ids = HashSet::new();
        for (ttid, data) in timelines {
            async {
            if ttid.tenant_shard_id.shard_count == highest_shard_count {
                // Only analyze `TenantShardId`s with highest shard count.

                // Stash the generation of each timeline, for later use identifying orphan layers

                if let BlobDataParseResult::Parsed {
                    index_part,
                    index_part_generation: _,
                    s3_layers: _,
                    index_part_last_modified_time: _,
                    index_part_snapshot_time: _,
                } = &data.blob_data
                {
                    if index_part.deleted_at.is_some() {
                        // skip deleted timeline.
                        tracing::info!("Skip analysis of {} b/c timeline is already deleted", ttid);
                        return Ok(());
                    }
                }

                // Apply checks to this timeline shard's metadata, and in the process update `tenant_objects`
                // reference counts for layers across the tenant.

                output_tx.send((ttid, tenant_objects.clone(), data)).await?;

                timeline_ids.insert(ttid.timeline_id);
            } else {
                tracing::info!(
                    "Skip analysis of {} b/c a lower shard count than {}",
                    ttid,
                    highest_shard_count.0,
                );
            }
            Ok::<_, anyhow::Error>(())
        }.instrument(
            info_span!("analyze-timeline", shard = %ttid.tenant_shard_id.shard_slug(), timeline = %ttid.timeline_id),
        )
        .await?;
        }

        {
            let mut guard = summary.lock().await;
            guard.timeline_count += timeline_ids.len();
        }

        Ok(())
    }

    async fn identify_orphans(
        tenant_id: TenantId,
        tenant_objects: Arc<tokio::sync::Mutex<TenantObjectListing>>,
        summary: Arc<tokio::sync::Mutex<MetadataSummary>>,
        timeline_generations: &HashMap<TenantShardTimelineId, Generation>,
    ) {
        // Identifying orphan layers must be done on a tenant-wide basis, because individual
        // shards' layers may be referenced by other shards.
        //
        // Orphan layers are not a corruption, and not an indication of a problem.  They are just
        // consuming some space in remote storage, and may be cleaned up at leisure.

        let orphans = { tenant_objects.lock().await.get_orphans() };
        for (shard_index, timeline_id, layer_file, generation) in orphans {
            let ttid = TenantShardTimelineId {
                tenant_shard_id: TenantShardId {
                    tenant_id,
                    shard_count: shard_index.shard_count,
                    shard_number: shard_index.shard_number,
                },
                timeline_id,
            };

            if let Some(timeline_generation) = timeline_generations.get(&ttid) {
                if &generation >= timeline_generation {
                    // Candidate orphan layer is in the current or future generation relative
                    // to the index we read for this timeline shard, so its absence from the index
                    // doesn't make it an orphan: more likely, it is a case where the layer was
                    // uploaded, but the index referencing the layer wasn't written yet.
                    continue;
                }
            }

            let orphan_path = remote_layer_path(
                &tenant_id,
                &timeline_id,
                shard_index,
                &layer_file,
                generation,
            );

            tracing::info!("Orphan layer detected: {orphan_path}");

            {
                let mut guard = summary.lock().await;
                guard.notify_timeline_orphan(&ttid);
            }
        }
    }

    // TODO: bail out early if any of the tasks fail
    list_tenants.await??;
    list_timelines.await??;
    read_timelines.await??;
    consolidate_tenants.await??;
    analyze_tenants.await??;

    let summary = summary.lock().await;
    Ok(summary.clone())
}
