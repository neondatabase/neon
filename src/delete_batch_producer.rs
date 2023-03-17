use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use aws_sdk_s3::Client;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{error, info, info_span, warn, Instrument};

use crate::cloud_admin_api::CloudAdminApiClient;
use crate::{list_objects_with_retries, S3Target, TenantId, MAX_RETRIES};

/// Typical tenant to remove contains 1 layer and 1 index_part.json blobs
/// Also, there are some non-standard tenants to remove, having more layers.
/// delete_objects request allows up to 1000 keys, so be on a safe side and allow most
/// batch processing tasks to do 1 delete objects request only.
///
/// Every batch item will be additionally S3 LS'ed later, so keep the batch size
/// even lower to allow multiple concurrent tasks do the LS requests.
const BATCH_SIZE: usize = 100;

pub struct DeleteBatchProducer {
    batch_sender_task: JoinHandle<anyhow::Result<usize>>,
    batch_receiver: Arc<Mutex<UnboundedReceiver<Vec<TenantId>>>>,
}

impl DeleteBatchProducer {
    pub fn start(
        admin_client: CloudAdminApiClient,
        s3_client: Arc<Client>,
        s3_target: S3Target,
        tenant_limit: Option<usize>,
    ) -> Self {
        let (batch_sender, batch_receiver) = tokio::sync::mpsc::unbounded_channel();
        let list_span = info_span!("bucket_list", name = %s3_target.bucket_name);
        let admin_client = Arc::new(admin_client);

        let batch_sender_task = tokio::spawn(
            async move {
                info!("Starting to list the bucket {}", s3_target.bucket_name);
                s3_client
                    .head_bucket()
                    .bucket(s3_target.bucket_name.clone())
                    .send()
                    .await
                    .with_context(|| format!("bucket {} was not found", s3_target.bucket_name))?;

                let mut continuation_token = None;
                let mut next_batch = Vec::with_capacity(BATCH_SIZE);
                let mut total_tenants_listed = 0_usize;

                loop {
                    let fetch_response = list_objects_with_retries(
                        &s3_client,
                        &s3_target,
                        continuation_token.clone(),
                    )
                    .await?;

                    let new_tenant_ids = fetch_response
                        .common_prefixes()
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|prefix| prefix.prefix())
                        .filter_map(|prefix| -> Option<&str> {
                            prefix
                                .strip_prefix(&s3_target.prefix_in_bucket)?
                                .strip_suffix('/')
                        })
                        .take(tenant_limit.unwrap_or(usize::MAX))
                        .map(|tenant_id_str| {
                            let tenant_id = tenant_id_str.parse().with_context(|| {
                                format!("Incorrect tenant id str: {tenant_id_str}")
                            });
                            if tenant_id.is_ok() {
                                total_tenants_listed += 1;
                            }
                            tenant_id
                        })
                        .collect::<anyhow::Result<Vec<TenantId>>>()
                        .context("list and parse bucket's tenant ids")?;

                    for tenant_id_to_delete in filter_active_tenants(&admin_client, new_tenant_ids)
                        .await
                        .context("filter active tenant ids")?
                    {
                        if next_batch.len() >= BATCH_SIZE {
                            let batch_to_send =
                                std::mem::replace(&mut next_batch, Vec::with_capacity(BATCH_SIZE));
                            info!("Produced new batch, size: {}", batch_to_send.len());
                            batch_sender
                                .send(batch_to_send)
                                .expect("Receiver is not held in the same struct");
                        }
                        next_batch.push(tenant_id_to_delete);
                    }

                    match fetch_response.next_continuation_token {
                        Some(new_token) => continuation_token = Some(new_token),
                        None => break,
                    }
                }

                if !next_batch.is_empty() {
                    info!("Produced final batch, size: {}", next_batch.len());
                    batch_sender
                        .send(next_batch)
                        .expect("Receiver is not held in the same struct");
                }

                info!("Finished listing the bucket");
                Ok(total_tenants_listed)
            }
            .instrument(list_span),
        );

        Self {
            batch_sender_task,
            batch_receiver: Arc::new(Mutex::new(batch_receiver)),
        }
    }

    pub fn subscribe(&self) -> Arc<Mutex<UnboundedReceiver<Vec<TenantId>>>> {
        self.batch_receiver.clone()
    }

    pub async fn join(self) -> anyhow::Result<usize> {
        match self.batch_sender_task.await {
            Ok(task_result) => task_result,
            Err(join_error) => anyhow::bail!("Failed to join the task: {join_error}"),
        }
    }
}

async fn filter_active_tenants(
    admin_client: &Arc<CloudAdminApiClient>,
    new_tenant_ids: Vec<TenantId>,
) -> anyhow::Result<Vec<TenantId>> {
    let api_request_limiter = Arc::new(Semaphore::new(200));
    let mut check_tasks = JoinSet::new();
    let mut filtered_ids = Vec::with_capacity(new_tenant_ids.len());

    for new_tenant in new_tenant_ids {
        let check_client = Arc::clone(admin_client);
        let check_limit = Arc::clone(&api_request_limiter);
        check_tasks.spawn(
            async move {
                (
                    new_tenant,
                    async move {
                        let _permit = check_limit
                            .acquire()
                            .await
                            .expect("Semaphore is not closed");
                        for _ in 0..MAX_RETRIES {
                            match check_client.find_tenant_project(new_tenant).await {
                                Ok(project_data) => return Ok(project_data),
                                Err(e) => {
                                    error!("find_tenant_project admin API call failed: {e}");
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }
                            }
                        }

                        anyhow::bail!("Failed to check tenant {new_tenant} {MAX_RETRIES} times")
                    }
                    .await,
                )
            }
            .instrument(info_span!("filter_active_tenants")),
        );
    }

    while let Some(task_result) = check_tasks.join_next().await {
        let (tenant_id, tenant_data_fetch_result) = task_result.context("task join")?;
        match tenant_data_fetch_result.context("tenant data fetch")? {
            Some(project) => {
                if project.deleted {
                    info!("Found deleted project for tenant {tenant_id} in admin data, can safely delete");
                    filtered_ids.push(tenant_id);
                } else {
                    info!("Found alive project for tenant {tenant_id}, cannot delete");
                }
            }
            None => {
                warn!("Found no project for tenant {tenant_id} in admin data, deleting");
                filtered_ids.push(tenant_id);
            }
        }
    }
    Ok(filtered_ids)
}
