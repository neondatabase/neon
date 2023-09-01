use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3::Client;
use either::Either;
use tokio::sync::mpsc::UnboundedSender;
use tracing::info;

use crate::cloud_admin_api::{CloudAdminApiClient, ProjectData};
use crate::delete_batch_producer::FetchResult;
use crate::{RootTarget, TraversingDepth};
use utils::id::{TenantId, TenantTimelineId};

use super::ProcessedS3List;

pub async fn schedule_cleanup_deleted_tenants(
    s3_root_target: &RootTarget,
    s3_client: &Arc<Client>,
    admin_client: &Arc<CloudAdminApiClient>,
    projects_to_check_sender: UnboundedSender<ProjectData>,
    delete_sender: Arc<UnboundedSender<Either<TenantId, TenantTimelineId>>>,
    traversing_depth: TraversingDepth,
) -> anyhow::Result<ProcessedS3List<TenantId, ProjectData>> {
    info!(
        "Starting to list the bucket from root {}",
        s3_root_target.bucket_name()
    );
    s3_client
        .head_bucket()
        .bucket(s3_root_target.bucket_name())
        .send()
        .await
        .with_context(|| format!("bucket {} was not found", s3_root_target.bucket_name()))?;

    let check_client = Arc::clone(admin_client);
    let tenant_stats = super::process_s3_target_recursively(
        s3_client,
        s3_root_target.tenants_root(),
        |s3_tenants| async move {
            let another_client = Arc::clone(&check_client);
            super::split_to_active_and_deleted_entries(s3_tenants, move |tenant_id| async move {
                let project_data = another_client
                    .find_tenant_project(tenant_id)
                    .await
                    .with_context(|| format!("Tenant {tenant_id} project admin check"))?;

                Ok(if let Some(console_project) = project_data {
                    if console_project.deleted {
                        delete_sender.send(Either::Left(tenant_id)).ok();
                        FetchResult::Deleted
                    } else {
                        if traversing_depth == TraversingDepth::Timeline {
                            projects_to_check_sender.send(console_project.clone()).ok();
                        }
                        FetchResult::Found(console_project)
                    }
                } else {
                    delete_sender.send(Either::Left(tenant_id)).ok();
                    FetchResult::Absent
                })
            })
            .await
        },
    )
    .await
    .context("tenant batch processing")?;

    info!(
        "Among {} tenants, found {} tenants to delete and {} active ones",
        tenant_stats.entries_total,
        tenant_stats.entries_to_delete.len(),
        tenant_stats.active_entries.len(),
    );

    let tenant_stats = match traversing_depth {
        TraversingDepth::Tenant => {
            info!("Finished listing the bucket for tenants only");
            tenant_stats
        }
        TraversingDepth::Timeline => {
            info!("Finished listing the bucket for tenants and sent {} active tenants to check for timelines", tenant_stats.active_entries.len());
            tenant_stats
        }
    };

    Ok(tenant_stats)
}
