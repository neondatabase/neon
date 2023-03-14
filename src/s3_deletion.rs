use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3::client::fluent_builders::DeleteObjects;
use aws_sdk_s3::model::{Delete, ObjectIdentifier};
use aws_sdk_s3::{Client, Region};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{debug, error, info, info_span, warn, Instrument};

use crate::{init_s3_client, TenantsToClean};

pub struct S3Deleter {
    concurrent_tasks_count: NonZeroUsize,
    deletions_per_batch: usize,
    tenants_to_clean: Arc<Mutex<TenantsToClean>>,
    s3_client: Arc<Client>,
}

impl S3Deleter {
    pub fn new(
        bucket_region: Region,
        concurrent_tasks_count: NonZeroUsize,
        tenants_to_clean: TenantsToClean,
    ) -> Self {
        Self {
            concurrent_tasks_count,
            // max is 1000, but we can accidentally get over that, so keep it less
            deletions_per_batch: 900,
            tenants_to_clean: Arc::new(Mutex::new(tenants_to_clean)),
            s3_client: Arc::new(init_s3_client(bucket_region)),
        }
    }

    pub async fn remove_all(self) -> anyhow::Result<()> {
        let concurrent_tasks_count = self.concurrent_tasks_count.get();

        let mut deletion_tasks = JoinSet::new();
        for id in 0..concurrent_tasks_count {
            let closure_client = Arc::clone(&self.s3_client);
            let closure_tenants_to_clean = Arc::clone(&self.tenants_to_clean);
            let limit = self.deletions_per_batch;
            deletion_tasks.spawn(
                async move {
                    (
                        id,
                        async move {
                            loop {
                                match construct_delete_request(
                                    &closure_client,
                                    limit,
                                    &closure_tenants_to_clean,
                                )
                                .await
                                .context("delete request construction")
                                {
                                    Ok(Some(delete_request)) => {
                                        match delete_request
                                            .send()
                                            .await
                                            .context("delete request processing")
                                        {
                                            Ok(delete_response) => {
                                                match delete_response.errors() {
                                                    Some(delete_errors) => {
                                                        error!("Delete request returned errors: {delete_errors:?}");
                                                        anyhow::bail!("Failed to delete all elements from the S3: {delete_errors:?}");
                                                    }
                                                    None => info!("Successfully removed an object batch from S3"),
                                                }
                                            }
                                            Err(e) => {
                                                error!("Failed to send a delete request: {e:#}");
                                                return Err(e);
                                            }
                                        }
                                    }
                                    Ok(None) => {
                                        info!("No more delete requests to create, finishing the task");
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        error!("Failed to construct a delete request: {e:#}");
                                        return Err(e);
                                    }
                                }
                            }
                        }.await
                    )
                }
                .instrument(info_span!("deletion_task", %id)),
            );
        }

        let mut task_failed = false;
        while let Some(task_result) = deletion_tasks.join_next().await {
            match task_result {
                Ok((id, Ok(()))) => info!("Task {id} finished successfully"),
                Ok((id, Err(task_error))) => {
                    error!("Task {id} failed: {task_error:#}");
                    task_failed = true;
                }
                Err(join_error) => anyhow::bail!("Failed to join on a task: {join_error:?}"),
            }
        }

        anyhow::ensure!(!task_failed, "Some tasks failed");
        Ok(())
    }
}

// s3://neon-staging-storage-us-east-2/pageserver/v1/tenants/000036a7f7ea960517c8a74790ee2ad6/;
const TENANT_PATH_IN_S3_BUCKET: &str = "pageserver/v1/tenants/";

async fn construct_delete_request(
    s3_client: &Client,
    objects_limit: usize,
    tenants_to_clean: &Mutex<TenantsToClean>,
) -> anyhow::Result<Option<DeleteObjects>> {
    let mut request_bucket = None;
    let mut request_keys = Vec::with_capacity(objects_limit);

    while request_keys.len() < objects_limit {
        let mut tenants_accessor = tenants_to_clean.lock().await;
        let next_tenant = tenants_accessor.next_tenant();

        match next_tenant {
            Some((bucket_name, tenant_id)) => {
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

                drop(tenants_accessor);

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

                warn!("Tenant {tenant_id} has too many objects returned, rescheduling it for later removal");


                match list_response.contents() {
                    Some(objects) => {
                        for object in objects {
                            let key = object.key().with_context(|| {
                                format!("No key for object {object:?}, tenant {tenant_id}")
                            })?;
                            request_keys.push(ObjectIdentifier::builder().key(key).build());
                        }
                    }
                    None => debug!("No keys returned for tenant {tenant_id}, skipping it"),
                }
            }
            None => break,
        }
    }

    if request_keys.is_empty() {
        Ok(None)
    } else {
        Ok(Some(
            s3_client
                .delete_objects()
                .bucket(
                    request_bucket
                        .expect("With request keys present, we should have the bucket too")
                        .0,
                )
                .delete(Delete::builder().set_objects(Some(request_keys)).build()),
        ))
    }
}
