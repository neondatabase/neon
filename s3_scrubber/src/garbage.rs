//! Functionality for finding and purging garbage, as in "garbage collection".  Garbage means
//! S3 objects which are either not referenced by any metadata, or are referenced by a
//! control plane tenant/timeline in a deleted state.

use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use aws_sdk_s3::{
    types::{Delete, ObjectIdentifier},
    Client,
};
use futures_util::{pin_mut, TryStreamExt};
use pageserver_api::shard::TenantShardId;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;
use utils::id::TenantId;

use crate::{
    cloud_admin_api::{CloudAdminApiClient, MaybeDeleted, ProjectData},
    init_remote,
    metadata_stream::{stream_listing, stream_tenant_timelines, stream_tenants},
    BucketConfig, ConsoleConfig, NodeKind, RootTarget, TenantShardTimelineId, TraversingDepth,
};

#[derive(Serialize, Deserialize, Debug)]
enum GarbageReason {
    DeletedInConsole,
    MissingInConsole,
}

#[derive(Serialize, Deserialize, Debug)]
enum GarbageEntity {
    Tenant(TenantShardId),
    Timeline(TenantShardTimelineId),
}

#[derive(Serialize, Deserialize, Debug)]
struct GarbageItem {
    entity: GarbageEntity,
    reason: GarbageReason,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GarbageList {
    /// Remember what NodeKind we were finding garbage for, so that we can
    /// purge the list without re-stating it.
    node_kind: NodeKind,

    /// Embed the identity of the bucket, so that we do not risk executing
    /// the wrong list against the wrong bucket, and so that the user does not have
    /// to re-state the bucket details when purging.
    bucket_config: BucketConfig,

    items: Vec<GarbageItem>,

    /// Advisory information to enable consumers to do a validation that if we
    /// see garbage, we saw some active tenants too.  This protects against classes of bugs
    /// in the scrubber that might otherwise generate a "deleted all" result.
    active_tenant_count: usize,
}

impl GarbageList {
    fn new(node_kind: NodeKind, bucket_config: BucketConfig) -> Self {
        Self {
            items: Vec::new(),
            active_tenant_count: 0,
            node_kind,
            bucket_config,
        }
    }

    /// Return true if appended, false if not.  False means the result was not garbage.
    fn maybe_append<T>(&mut self, entity: GarbageEntity, result: Option<T>) -> bool
    where
        T: MaybeDeleted,
    {
        match result {
            Some(result_item) if result_item.is_deleted() => {
                self.items.push(GarbageItem {
                    entity,
                    reason: GarbageReason::DeletedInConsole,
                });
                true
            }
            Some(_) => false,
            None => {
                self.items.push(GarbageItem {
                    entity,
                    reason: GarbageReason::MissingInConsole,
                });
                true
            }
        }
    }
}

pub async fn find_garbage(
    bucket_config: BucketConfig,
    console_config: ConsoleConfig,
    depth: TraversingDepth,
    node_kind: NodeKind,
    output_path: String,
) -> anyhow::Result<()> {
    let garbage = find_garbage_inner(bucket_config, console_config, depth, node_kind).await?;
    let serialized = serde_json::to_vec_pretty(&garbage)?;

    tokio::fs::write(&output_path, &serialized).await?;

    tracing::info!("Wrote garbage report to {output_path}");

    Ok(())
}

// How many concurrent S3 operations to issue (approximately): this is the concurrency
// for things like listing the timelines within tenant prefixes.
const S3_CONCURRENCY: usize = 32;

// How many concurrent API requests to make to the console API.
const CONSOLE_CONCURRENCY: usize = 128;

async fn find_garbage_inner(
    bucket_config: BucketConfig,
    console_config: ConsoleConfig,
    depth: TraversingDepth,
    node_kind: NodeKind,
) -> anyhow::Result<GarbageList> {
    // Construct clients for S3 and for Console API
    let (s3_client, target) = init_remote(bucket_config.clone(), node_kind)?;
    let cloud_admin_api_client = Arc::new(CloudAdminApiClient::new(console_config));

    // Build a set of console-known tenants, for quickly eliminating known-active tenants without having
    // to issue O(N) console API requests.
    let console_projects: HashMap<TenantId, ProjectData> = cloud_admin_api_client
        // FIXME: we can't just assume that all console's region ids are aws-<something>.  This hack
        // will go away when we are talking to Control Plane APIs, which are per-region.
        .list_projects(format!("aws-{}", bucket_config.region))
        .await?
        .into_iter()
        .map(|t| (t.tenant, t))
        .collect();
    tracing::info!(
        "Loaded {} console projects tenant IDs",
        console_projects.len()
    );

    // TODO(sharding): batch calls into Console so that we only call once for each TenantId,
    // rather than checking the same TenantId for multiple TenantShardId

    // Enumerate Tenants in S3, and check if each one exists in Console
    tracing::info!("Finding all tenants in bucket {}...", bucket_config.bucket);
    let tenants = stream_tenants(&s3_client, &target);
    let tenants_checked = tenants.map_ok(|t| {
        let api_client = cloud_admin_api_client.clone();
        let console_projects = &console_projects;
        async move {
            match console_projects.get(&t.tenant_id) {
                Some(project_data) => Ok((t, Some(project_data.clone()))),
                None => api_client
                    .find_tenant_project(t.tenant_id)
                    .await
                    .map_err(|e| anyhow::anyhow!(e))
                    .map(|r| (t, r)),
            }
        }
    });
    let tenants_checked = tenants_checked.try_buffer_unordered(CONSOLE_CONCURRENCY);

    // Process the results of Tenant checks.  If a Tenant is garbage, it goes into
    // the `GarbageList`.  Else it goes into `active_tenants` for more detailed timeline
    // checks if they are enabled by the `depth` parameter.
    pin_mut!(tenants_checked);
    let mut garbage = GarbageList::new(node_kind, bucket_config);
    let mut active_tenants: Vec<TenantShardId> = vec![];
    let mut counter = 0;
    while let Some(result) = tenants_checked.next().await {
        let (tenant_shard_id, console_result) = result?;

        // Paranoia check
        if let Some(project) = &console_result {
            assert!(project.tenant == tenant_shard_id.tenant_id);
        }

        if garbage.maybe_append(GarbageEntity::Tenant(tenant_shard_id), console_result) {
            tracing::debug!("Tenant {tenant_shard_id} is garbage");
        } else {
            tracing::debug!("Tenant {tenant_shard_id} is active");
            active_tenants.push(tenant_shard_id);
        }

        counter += 1;
        if counter % 1000 == 0 {
            tracing::info!(
                "Progress: {counter} tenants checked, {} active, {} garbage",
                active_tenants.len(),
                garbage.items.len()
            );
        }
    }

    tracing::info!(
        "Found {}/{} garbage tenants",
        garbage.items.len(),
        garbage.items.len() + active_tenants.len()
    );

    // If we are only checking tenant-deep, we are done.  Otherwise we must
    // proceed to check the individual timelines of the active tenants.
    if depth == TraversingDepth::Tenant {
        return Ok(garbage);
    }

    tracing::info!(
        "Checking timelines for {} active tenants",
        active_tenants.len(),
    );

    // Construct a stream of all timelines within active tenants
    let active_tenants = tokio_stream::iter(active_tenants.iter().map(Ok));
    let timelines = active_tenants.map_ok(|t| stream_tenant_timelines(&s3_client, &target, *t));
    let timelines = timelines.try_buffer_unordered(S3_CONCURRENCY);
    let timelines = timelines.try_flatten();

    // For all timelines within active tenants, call into console API to check their existence
    let timelines_checked = timelines.map_ok(|ttid| {
        let api_client = cloud_admin_api_client.clone();
        async move {
            api_client
                .find_timeline_branch(ttid.timeline_id)
                .await
                .map_err(|e| anyhow::anyhow!(e))
                .map(|r| (ttid, r))
        }
    });
    let timelines_checked = timelines_checked.try_buffer_unordered(CONSOLE_CONCURRENCY);

    // Update the GarbageList with any timelines which appear not to exist.
    pin_mut!(timelines_checked);
    while let Some(result) = timelines_checked.next().await {
        let (ttid, console_result) = result?;
        if garbage.maybe_append(GarbageEntity::Timeline(ttid), console_result) {
            tracing::debug!("Timeline {ttid} is garbage");
        } else {
            tracing::debug!("Timeline {ttid} is active");
        }
    }

    Ok(garbage)
}

#[derive(clap::ValueEnum, Debug, Clone)]
pub enum PurgeMode {
    /// The safest mode: only delete tenants that were explicitly reported as deleted
    /// by Console API.
    DeletedOnly,

    /// Delete all garbage tenants, including those which are only presumed to be deleted,
    /// because the Console API could not find them.
    DeletedAndMissing,
}

impl std::fmt::Display for PurgeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PurgeMode::DeletedOnly => write!(f, "deleted-only"),
            PurgeMode::DeletedAndMissing => write!(f, "deleted-and-missing"),
        }
    }
}

pub async fn get_tenant_objects(
    s3_client: &Arc<Client>,
    target: RootTarget,
    tenant_shard_id: TenantShardId,
) -> anyhow::Result<Vec<ObjectIdentifier>> {
    tracing::debug!("Listing objects in tenant {tenant_shard_id}");
    // TODO: apply extra validation based on object modification time.  Don't purge
    // tenants where any timeline's index_part.json has been touched recently.

    let mut tenant_root = target.tenant_root(&tenant_shard_id);

    // Remove delimiter, so that object listing lists all keys in the prefix and not just
    // common prefixes.
    tenant_root.delimiter = String::new();

    let key_stream = stream_listing(s3_client, &tenant_root);
    key_stream.try_collect().await
}

pub async fn get_timeline_objects(
    s3_client: &Arc<Client>,
    target: RootTarget,
    ttid: TenantShardTimelineId,
) -> anyhow::Result<Vec<ObjectIdentifier>> {
    tracing::debug!("Listing objects in timeline {ttid}");
    let mut timeline_root = target.timeline_root(&ttid);

    // TODO: apply extra validation based on object modification time.  Don't purge
    // timelines whose index_part.json has been touched recently.

    // Remove delimiter, so that object listing lists all keys in the prefix and not just
    // common prefixes.
    timeline_root.delimiter = String::new();
    let key_stream = stream_listing(s3_client, &timeline_root);

    key_stream.try_collect().await
}

const MAX_KEYS_PER_DELETE: usize = 1000;

/// Drain a buffer of keys into DeleteObjects requests
async fn do_delete(
    s3_client: &Arc<Client>,
    bucket_name: &str,
    keys: &mut Vec<ObjectIdentifier>,
    dry_run: bool,
    drain: bool,
) -> anyhow::Result<()> {
    while (!keys.is_empty() && drain) || (keys.len() >= MAX_KEYS_PER_DELETE) {
        let request_keys =
            keys.split_off(keys.len() - (std::cmp::min(MAX_KEYS_PER_DELETE, keys.len())));
        if dry_run {
            tracing::info!("Dry-run deletion of objects: ");
            for k in request_keys {
                tracing::info!("  {k:?}");
            }
        } else {
            let delete_request = s3_client
                .delete_objects()
                .bucket(bucket_name)
                .delete(Delete::builder().set_objects(Some(request_keys)).build()?);
            delete_request
                .send()
                .await
                .context("DeleteObjects request")?;
        }
    }

    Ok(())
}

pub async fn purge_garbage(
    input_path: String,
    mode: PurgeMode,
    dry_run: bool,
) -> anyhow::Result<()> {
    let list_bytes = tokio::fs::read(&input_path).await?;
    let garbage_list = serde_json::from_slice::<GarbageList>(&list_bytes)?;
    tracing::info!(
        "Loaded {} items in garbage list from {}",
        garbage_list.items.len(),
        input_path
    );

    let (s3_client, target) =
        init_remote(garbage_list.bucket_config.clone(), garbage_list.node_kind)?;

    // Sanity checks on the incoming list
    if garbage_list.active_tenant_count == 0 {
        anyhow::bail!("Refusing to purge a garbage list that reports 0 active tenants");
    }

    let filtered_items = garbage_list
        .items
        .iter()
        .filter(|i| match (&mode, &i.reason) {
            (PurgeMode::DeletedAndMissing, _) => true,
            (PurgeMode::DeletedOnly, GarbageReason::DeletedInConsole) => true,
            (PurgeMode::DeletedOnly, GarbageReason::MissingInConsole) => false,
        });

    tracing::info!(
        "Filtered down to {} garbage items based on mode {}",
        garbage_list.items.len(),
        mode
    );

    let items = tokio_stream::iter(filtered_items.map(Ok));
    let get_objects_results = items.map_ok(|i| {
        let s3_client = s3_client.clone();
        let target = target.clone();
        async move {
            match i.entity {
                GarbageEntity::Tenant(tenant_id) => {
                    get_tenant_objects(&s3_client, target, tenant_id).await
                }
                GarbageEntity::Timeline(ttid) => {
                    get_timeline_objects(&s3_client, target, ttid).await
                }
            }
        }
    });
    let get_objects_results = get_objects_results.try_buffer_unordered(S3_CONCURRENCY);

    pin_mut!(get_objects_results);
    let mut objects_to_delete = Vec::new();
    while let Some(result) = get_objects_results.next().await {
        let mut object_list = result?;
        objects_to_delete.append(&mut object_list);
        if objects_to_delete.len() >= MAX_KEYS_PER_DELETE {
            do_delete(
                &s3_client,
                &garbage_list.bucket_config.bucket,
                &mut objects_to_delete,
                dry_run,
                false,
            )
            .await?;
        }
    }

    do_delete(
        &s3_client,
        &garbage_list.bucket_config.bucket,
        &mut objects_to_delete,
        dry_run,
        true,
    )
    .await?;

    tracing::info!("Fell through");

    Ok(())
}
