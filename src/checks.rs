use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use aws_sdk_s3::Client;
use tokio::io::AsyncReadExt;
use tokio::task::JoinSet;
use tracing::{error, info, info_span, warn, Instrument};

use crate::cloud_admin_api::{BranchData, CloudAdminApiClient, ProjectId};
use crate::copied_definitions::filename::LayerFileName;
use crate::copied_definitions::id::TenantTimelineId;
use crate::copied_definitions::index::IndexPart;
use crate::delete_batch_producer::DeleteProducerStats;
use crate::{list_objects_with_retries, RootTarget, MAX_RETRIES};

pub async fn validate_pageserver_active_tenant_and_timelines(
    s3_client: Arc<Client>,
    s3_root: RootTarget,
    admin_client: Arc<CloudAdminApiClient>,
    batch_producer_stats: DeleteProducerStats,
) -> anyhow::Result<BranchCheckStats> {
    let Some(timeline_stats) = batch_producer_stats.timeline_stats else {
        info!("No tenant-only checks, exiting");
        return Ok(BranchCheckStats);
    };

    let s3_active_projects = batch_producer_stats
        .tenant_stats
        .active_entries
        .into_iter()
        .map(|project| (project.id.clone(), project))
        .collect::<HashMap<_, _>>();

    let mut s3_active_branches_per_project = HashMap::<ProjectId, Vec<BranchData>>::new();
    let mut s3_blob_data = HashMap::<TenantTimelineId, S3TimelineBlobData>::new();
    for active_branch in timeline_stats.active_entries {
        let active_project_id = active_branch.project_id.clone();
        let active_branch_id = active_branch.id.clone();
        let active_timeline_id = active_branch.timeline_id;

        s3_active_branches_per_project
            .entry(active_project_id.clone())
            .or_default()
            .push(active_branch);

        let Some(active_project) = s3_active_projects.get(&active_project_id) else {
            error!("Branch {:?} for project {:?} has no such project in the active projects", active_branch_id, active_project_id);
            continue;
        };

        let id = TenantTimelineId::new(active_project.tenant, active_timeline_id);
        s3_blob_data.insert(
            id,
            match list_timeline_blobs(&s3_client, id, &s3_root).await {
                Ok(data) => data,
                Err(e) => S3TimelineBlobData::Incorrect(e),
            },
        );
    }

    let mut branch_checks = JoinSet::new();
    for (_, s3_active_project) in s3_active_projects {
        let project_id = &s3_active_project.id;
        let tenant_id = s3_active_project.tenant;

        let mut console_active_branches =
            branches_for_project_with_retries(&admin_client, project_id)
                .await
                .with_context(|| {
                    format!("Client API branches for project {project_id:?} retrieval")
                })?
                .into_iter()
                .map(|branch| (branch.id.clone(), branch))
                .collect::<HashMap<_, _>>();

        for s3_active_branch in s3_active_branches_per_project
            .remove(project_id)
            .unwrap_or_default()
        {
            let console_branch = console_active_branches.remove(&s3_active_branch.id);
            let timeline_id = s3_active_branch.timeline_id;
            let id = TenantTimelineId::new(tenant_id, timeline_id);
            let s3_data = s3_blob_data.remove(&id);
            branch_checks.spawn(
                async move { check_branch(&s3_active_branch, console_branch, s3_data).await }
                    .instrument(info_span!("check_branch", id = %id)),
            );
        }
    }

    let mut total_stats = BranchCheckStats;
    while let Some(branch_check_stats) = branch_checks
        .join_next()
        .await
        .transpose()
        .context("branch check task join")?
    {
        total_stats.merge(branch_check_stats);
    }
    Ok(total_stats)
}

async fn branches_for_project_with_retries(
    admin_client: &CloudAdminApiClient,
    project_id: &ProjectId,
) -> anyhow::Result<Vec<BranchData>> {
    for _ in 0..MAX_RETRIES {
        match admin_client.branches_for_project(project_id, false).await {
            Ok(branches) => return Ok(branches),
            Err(e) => {
                error!("admin list branches for project {project_id:?} query failed: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    anyhow::bail!("Failed to list branches for project {project_id:?} {MAX_RETRIES} times")
}

// TODO kb naming, has to cover tenant -> active project transition?
#[derive(Debug)]
pub struct BranchCheckStats;

impl BranchCheckStats {
    pub fn merge(&mut self, other: Self) {
        // TODO kb
    }
}

async fn check_branch(
    s3_active_branch: &BranchData,
    console_branch: Option<BranchData>,
    s3_data: Option<S3TimelineBlobData>,
) -> BranchCheckStats {
    match console_branch {
        Some(console_active_branch) => {
            if console_active_branch.deleted {
                warn!("Timeline has deleted branch data in the console, recheck whether if it got removed during the check")
            }
        },
        None => warn!("Timeline has no branch data in the console, recheck whether if it got removed during the check"),
    }

    match s3_data {
        Some(S3TimelineBlobData::Parsed { index_part, layers }) => {
            // TODO kb check layers — need match the really listed, not to be empty
        }
        Some(S3TimelineBlobData::Incorrect(e)) => {
            error!("Timeline has incorrect S3 data: {e}");
        }
        None => error!("Timeline has no data on S3 at all"),
    }

    BranchCheckStats
}

enum S3TimelineBlobData {
    Parsed {
        index_part: IndexPart,
        layers: Vec<LayerFileName>,
    },
    Incorrect(anyhow::Error),
}

async fn list_timeline_blobs(
    s3_client: &Client,
    id: TenantTimelineId,
    s3_root: &RootTarget,
) -> anyhow::Result<S3TimelineBlobData> {
    let mut layers = Vec::new();
    let mut index_part_object = None;

    let timeline_dir_target = s3_root.timeline_root(id);
    let mut continuation_token = None;
    let mut errors = HashMap::new();
    loop {
        let fetch_response =
            list_objects_with_retries(s3_client, &timeline_dir_target, continuation_token.clone())
                .await?;

        let subdirectories = fetch_response.common_prefixes().unwrap_or_default();
        if !subdirectories.is_empty() {
            errors.entry(id).or_insert_with(Vec::new).push(format!(
                "S3 list response should not contain any subdirectories, but got {subdirectories:?}"
            ));
        }

        for (object, key) in fetch_response
            .contents()
            .unwrap_or_default()
            .iter()
            .filter_map(|object| Some((object, object.key()?)))
        {
            let blob_name = key
                .strip_prefix(&timeline_dir_target.prefix_in_bucket)
                .and_then(|local_name| local_name.strip_suffix('/'));
            match blob_name {
                Some("index_part.json") => index_part_object = Some(object.clone()),
                Some(maybe_layer_name) => match maybe_layer_name.parse::<LayerFileName>() {
                    Ok(new_layer) => layers.push(new_layer),
                    Err(e) => errors.entry(id).or_default().push(
                        format!("S3 list response got an object with key {key} that is not a layer name: {e}"),
                    ),
                },
                None => errors
                    .entry(id)
                    .or_default()
                    .push(format!("S3 list response got an object with odd key {key}")),
            }
        }

        match fetch_response.next_continuation_token {
            Some(new_token) => continuation_token = Some(new_token),
            None => break,
        }
    }

    if index_part_object.is_none() {
        errors
            .entry(id)
            .or_default()
            .push("S3 list response got no index_part.json file".to_string());
    }
    anyhow::ensure!(
        errors.is_empty(),
        "Failed to list timeline S3 without errors: {errors:?}"
    );
    let Some(index_part_object_key) = index_part_object.as_ref().and_then(|object| object.key()) else { anyhow::bail!("Index part object {index_part_object:?} has no key") };

    let index_part_bytes = download_object_with_retries(
        s3_client,
        &timeline_dir_target.bucket_name,
        index_part_object_key,
    )
    .await
    .context("index_part.json download")?;
    let index_part =
        serde_json::from_slice(&index_part_bytes).context("index_part.json body parsing")?;

    Ok(S3TimelineBlobData::Parsed { index_part, layers })
}

async fn download_object_with_retries(
    s3_client: &Client,
    bucket_name: &str,
    key: &str,
) -> anyhow::Result<Vec<u8>> {
    for _ in 0..MAX_RETRIES {
        let mut body_buf = Vec::new();
        let response_stream = match s3_client
            .get_object()
            .bucket(bucket_name)
            .key(key)
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                error!("Failed to download object for key {key}: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        match response_stream
            .body
            .into_async_read()
            .read_to_end(&mut body_buf)
            .await
        {
            Ok(bytes_read) => {
                info!("Downloaded {bytes_read} for object object with key {key}");
                return Ok(body_buf);
            }
            Err(e) => {
                error!("Failed to stream object body for key {key}: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    anyhow::bail!("Failed to download objects with key {key} {MAX_RETRIES} times")
}
