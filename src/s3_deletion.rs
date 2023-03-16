use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3::client::fluent_builders::DeleteObjects;
use aws_sdk_s3::model::{Delete, ObjectIdentifier};
use aws_sdk_s3::{Client, Region};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{debug, error, info, info_span, warn, Instrument};

use crate::cloud_admin_api::CloudAdminApiClient;
use crate::{init_s3_client, TenantId, TenantsToClean};

pub struct S3Deleter {
    concurrent_tasks_count: NonZeroUsize,
    deletions_per_batch: usize,
    tenants_to_clean: Arc<Mutex<TenantsToClean>>,
    s3_client: Arc<Client>,
    admin_api_client: Arc<CloudAdminApiClient>,
}

impl S3Deleter {
    pub fn new(
        bucket_region: Region,
        concurrent_tasks_count: NonZeroUsize,
        tenants_to_clean: TenantsToClean,
        admin_api_client: CloudAdminApiClient,
    ) -> Self {
        Self {
            concurrent_tasks_count,
            // max is 1000, but we can accidentally get over that, so keep it less
            deletions_per_batch: 900,
            tenants_to_clean: Arc::new(Mutex::new(tenants_to_clean)),
            s3_client: Arc::new(init_s3_client(bucket_region)),
            admin_api_client: Arc::new(admin_api_client),
        }
    }

    pub async fn remove_all(self) -> anyhow::Result<()> {
        let concurrent_tasks_count = self.concurrent_tasks_count.get();

        let mut deletion_tasks = JoinSet::new();
        for id in 0..concurrent_tasks_count {
            let closure_admin_client = Arc::clone(&self.admin_api_client);
            let closure_client = Arc::clone(&self.s3_client);
            let closure_tenants_to_clean = Arc::clone(&self.tenants_to_clean);
            let limit = self.deletions_per_batch;
            deletion_tasks.spawn(
                async move {
                    (
                        id,
                        async move {
                            loop {
                                match delete_batch(
                                    &closure_admin_client,
                                    &closure_client,
                                    limit,
                                    &closure_tenants_to_clean,
                                )
                                .await
                                {
                                    Ok(ControlFlow::Break(())) => return,
                                    Ok(ControlFlow::Continue(())) => (),
                                    Err(e) => error!("Batch processing failed: {e:#}"),
                                }
                            }
                        }
                        .await,
                    )
                }
                .instrument(info_span!("deletion_task", %id)),
            );
        }

        while let Some(task_result) = deletion_tasks.join_next().await {
            match task_result {
                Ok((id, ())) => info!("Task {id} completed"),
                Err(join_error) => anyhow::bail!("Failed to join on a task: {join_error:?}"),
            }
        }

        Ok(())
    }
}

// TODO kb reschedule tenants after the failure
async fn delete_batch(
    admin_client: &CloudAdminApiClient,
    closure_client: &Client,
    limit: usize,
    tenant_provider: &Mutex<TenantsToClean>,
) -> anyhow::Result<ControlFlow<(), ()>> {
    match construct_delete_request(admin_client, closure_client, limit, tenant_provider)
        .await
        .context("delete request construction")
    {
        Ok(Some(delete_request)) => {
            let original_request = delete_request.delete_request.clone();

            match delete_request
                .delete_request
                .send()
                .await
                .context("delete request processing")
            {
                Ok(delete_response) => match delete_response.errors() {
                    Some(delete_errors) => {
                        error!("Delete request returned errors: {delete_errors:?}");
                        anyhow::bail!(
                            "Failed to delete all elements from the S3: {delete_errors:?}"
                        );
                    }
                    None => {
                        info!("Successfully removed an object batch from S3, start_tenant: {}, end_tenant: {}", delete_request.start_tenant, delete_request.end_tenant);
                        Ok(ControlFlow::Continue(()))
                    }
                },
                Err(e) => {
                    error!("Failed to send a delete request: {e:#}");
                    error!("Original request: {original_request:?}");
                    Err(e)
                }
            }
        }
        Ok(None) => {
            info!("No more delete requests to create, finishing the task");
            Ok(ControlFlow::Break(()))
        }
        Err(e) => {
            error!("Failed to construct a delete request: {e:#}");
            Err(e)
        }
    }
}

// s3://neon-staging-storage-us-east-2/pageserver/v1/tenants/000036a7f7ea960517c8a74790ee2ad6/;
const TENANT_PATH_IN_S3_BUCKET: &str = "pageserver/v1/tenants/";

struct DeleteRequest {
    delete_request: DeleteObjects,
    start_tenant: TenantId,
    end_tenant: TenantId,
}

async fn construct_delete_request(
    admin_client: &CloudAdminApiClient,
    s3_client: &Client,
    objects_limit: usize,
    tenants_to_clean: &Mutex<TenantsToClean>,
) -> anyhow::Result<Option<DeleteRequest>> {
    let mut request_bucket = None;
    let mut start_tenant = None;
    let mut end_tenant = None;
    let mut request_keys = Vec::with_capacity(objects_limit);

    while request_keys.len() < objects_limit {
        let mut tenants_accessor = tenants_to_clean.lock().await;
        let next_tenant = tenants_accessor.next_tenant();

        match next_tenant {
            Some((bucket_name, tenant_id)) => {
                let admin_project = admin_client
                    .find_tenant_project(tenant_id)
                    .await
                    .with_context(|| format!("fetching project data for tenant {tenant_id}"))?;
                match admin_project {
                    Some(project) => {
                        if project.deleted {
                            info!("Found deleted project for tenant {tenant_id} in admin data, can safely delete");
                        } else {
                            error!("Found alive project for tenant {tenant_id}, cannot delete");
                            continue;
                        }
                    }
                    None => {
                        warn!("Found no project for tenant {tenant_id} in admin data, deleting")
                    }
                }

                if request_bucket.is_none() {
                    s3_client
                        .head_bucket()
                        .bucket(bucket_name.0.clone())
                        .send()
                        .await
                        .with_context(|| format!("bucket {bucket_name:?} was not found"))?;
                    request_bucket = Some(bucket_name);
                } else if Some(&bucket_name) != request_bucket.as_ref() {
                    tenants_accessor.reschedule(bucket_name, tenant_id);
                    break;
                }

                if start_tenant.is_none() {
                    start_tenant = Some(tenant_id);
                } else {
                    start_tenant = start_tenant.min(Some(tenant_id));
                }

                if end_tenant.is_none() {
                    end_tenant = Some(tenant_id);
                } else {
                    end_tenant = end_tenant.max(Some(tenant_id));
                }

                drop(tenants_accessor);

                let mut should_reschedule = false;

                debug!("Listing S3 objects for tenant {tenant_id}");
                let list_response = s3_client
                    .list_objects_v2()
                    .bucket(
                        request_bucket
                            .as_ref()
                            .map(|bucket_name| &bucket_name.0)
                            .expect("Should have set a bucket on a first next_tenant call"),
                    )
                    .prefix(format!("{TENANT_PATH_IN_S3_BUCKET}{tenant_id}"))
                    .send()
                    .await
                    .context("Tenant directory list failure")?;

                if list_response.is_truncated() {
                    warn!("Tenant {tenant_id} has too many objects listed, rescheduling it for later removal");
                    should_reschedule = true;
                }

                match list_response.contents() {
                    Some(objects) => {
                        for object in objects {
                            let key = object.key().with_context(|| {
                                format!("No key for object {object:?}, tenant {tenant_id}")
                            })?;
                            request_keys.push(ObjectIdentifier::builder().key(key).build());
                            if request_keys.len() >= objects_limit {
                                should_reschedule = true;
                                warn!("Tenant {tenant_id} has too many objects returned, rescheduling it for later removal");
                                break;
                            }
                        }
                    }
                    None => debug!("No keys returned for tenant {tenant_id}, skipping it"),
                }

                if should_reschedule {
                    tenants_to_clean.lock().await.reschedule(
                        request_bucket
                            .clone()
                            .expect("With request keys present, we should have the bucket too"),
                        tenant_id,
                    );
                    break;
                }
            }
            None => break,
        }
    }

    if request_keys.is_empty() {
        Ok(None)
    } else {
        let delete_request = s3_client
            .delete_objects()
            .bucket(
                request_bucket
                    .expect("With request keys present, we should have the bucket too")
                    .0,
            )
            .delete(Delete::builder().set_objects(Some(request_keys)).build());
        Ok(Some(DeleteRequest {
            delete_request,
            start_tenant: start_tenant.expect("start_tenant should be present after collection"),
            end_tenant: end_tenant.expect("end_tenant should be present after collection"),
        }))
    }
}
