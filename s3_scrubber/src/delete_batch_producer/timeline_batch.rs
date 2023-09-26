use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3::Client;
use either::Either;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{info, info_span, Instrument};

use crate::cloud_admin_api::{BranchData, CloudAdminApiClient, ProjectData};
use crate::delete_batch_producer::{FetchResult, ProcessedS3List};
use crate::RootTarget;
use utils::id::{TenantId, TenantTimelineId};

pub async fn schedule_cleanup_deleted_timelines(
    s3_root_target: &RootTarget,
    s3_client: &Arc<Client>,
    admin_client: &Arc<CloudAdminApiClient>,
    projects_to_check_receiver: &mut UnboundedReceiver<ProjectData>,
    delete_elements_sender: Arc<UnboundedSender<Either<TenantId, TenantTimelineId>>>,
) -> anyhow::Result<ProcessedS3List<TenantTimelineId, BranchData>> {
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

    let mut timeline_stats = ProcessedS3List::default();
    while let Some(project_to_check) = projects_to_check_receiver.recv().await {
        let check_client = Arc::clone(admin_client);

        let check_s3_client = Arc::clone(s3_client);

        let check_delete_sender = Arc::clone(&delete_elements_sender);

        let check_root = s3_root_target.clone();

        let new_stats = async move {
            let tenant_id_to_check = project_to_check.tenant;
            let check_target = check_root.timelines_root(&tenant_id_to_check);
            let stats = super::process_s3_target_recursively(
                &check_s3_client,
                &check_target,
                |s3_timelines| async move {
                    let another_client = check_client.clone();
                    super::split_to_active_and_deleted_entries(
                        s3_timelines,
                        move |timeline_id| async move {
                            let console_branch = another_client
                                .find_timeline_branch(timeline_id)
                                .await
                                .map_err(|e| {
                                    anyhow::anyhow!(
                                        "Timeline {timeline_id} branch admin check: {e}"
                                    )
                                })?;

                            let id = TenantTimelineId::new(tenant_id_to_check, timeline_id);
                            Ok(match console_branch {
                                Some(console_branch) => {
                                    if console_branch.deleted {
                                        check_delete_sender.send(Either::Right(id)).ok();
                                        FetchResult::Deleted
                                    } else {
                                        FetchResult::Found(console_branch)
                                    }
                                }
                                None => {
                                    check_delete_sender.send(Either::Right(id)).ok();
                                    FetchResult::Absent
                                }
                            })
                        },
                    )
                    .await
                },
            )
            .await
            .with_context(|| format!("tenant {tenant_id_to_check} timeline batch processing"))?
            .change_ids(|timeline_id| TenantTimelineId::new(tenant_id_to_check, timeline_id));

            Ok::<_, anyhow::Error>(stats)
        }
        .instrument(info_span!("delete_timelines_sender", tenant = %project_to_check.tenant))
        .await?;

        timeline_stats.merge(new_stats);
    }

    info!(
        "Among {} timelines, found {} timelines to delete and {} active ones",
        timeline_stats.entries_total,
        timeline_stats.entries_to_delete.len(),
        timeline_stats.active_entries.len(),
    );

    Ok(timeline_stats)
}
