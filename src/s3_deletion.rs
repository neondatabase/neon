use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3::model::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use crossbeam::channel::Receiver;
use tokio::task::JoinSet;
use tracing::{error, info, info_span, Instrument};

use crate::{list_objects_with_retries, S3Target, TenantId};

pub struct S3Deleter {
    dry_run: bool,
    concurrent_tasks_count: NonZeroUsize,
    delete_batch_receiver: Receiver<Vec<TenantId>>,
    s3_client: Arc<Client>,
    s3_target: S3Target,
}

impl S3Deleter {
    pub fn new(
        dry_run: bool,
        concurrent_tasks_count: NonZeroUsize,
        s3_client: Arc<Client>,
        delete_batch_receiver: Receiver<Vec<TenantId>>,
        s3_target: S3Target,
    ) -> Self {
        Self {
            dry_run,
            concurrent_tasks_count,
            delete_batch_receiver,
            s3_client,
            s3_target,
        }
    }

    pub async fn remove_all(self) -> anyhow::Result<()> {
        let concurrent_tasks_count = self.concurrent_tasks_count.get();

        let mut deletion_tasks = JoinSet::new();
        for id in 0..concurrent_tasks_count {
            let closure_client = Arc::clone(&self.s3_client);
            let closure_s3_target = self.s3_target.clone();
            let closure_batch_receiver = self.delete_batch_receiver.clone();
            let dry_run = self.dry_run;
            deletion_tasks.spawn(
                async move {
                    (
                        id,
                        async move {
                            loop {
                                info!("Task started");
                                match closure_batch_receiver.recv() {
                                    Ok(batch) => {
                                        info!("Deleting batch of size {}", batch.len());
                                        info!("Tenant ids to remove: {batch:?}");
                                        delete_batch(
                                            &closure_client,
                                            &closure_s3_target,
                                            batch,
                                            dry_run,
                                        )
                                        .await
                                        .context("batch deletion")?;
                                    }
                                    Err(_sender_dropped) => {
                                        info!("Task finished: sender dropped");
                                        return Ok::<_, anyhow::Error>(());
                                    }
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
                Ok((id, Ok(()))) => info!("Task {id} completed"),
                Ok((id, Err(e))) => {
                    error!("Task {id} failed: {e:#}");
                    return Err(e);
                }
                Err(join_error) => anyhow::bail!("Failed to join on a task: {join_error:?}"),
            }
        }

        Ok(())
    }
}

/// S3 delete_objects allows up to 1000 keys to be passed in a single request.
const MAX_ITEMS_TO_DELETE: usize = 900;

async fn delete_batch(
    s3_client: &Client,
    s3_target: &S3Target,
    batch: Vec<TenantId>,
    dry_run: bool,
) -> anyhow::Result<()> {
    let mut object_ids_to_delete = Vec::with_capacity(MAX_ITEMS_TO_DELETE);

    for tenant_to_delete in batch {
        let mut new_target = s3_target.clone();
        new_target.add_segment_to_prefix(&tenant_to_delete.to_string());

        let mut continuation_token = None;
        loop {
            let fetch_response =
                list_objects_with_retries(s3_client, &new_target, continuation_token.clone())
                    .await?;

            for object_id in fetch_response
                .contents()
                .unwrap_or_default()
                .iter()
                .filter_map(|object| object.key())
                .map(|key| ObjectIdentifier::builder().key(key).build())
            {
                if object_ids_to_delete.len() >= MAX_ITEMS_TO_DELETE {
                    let object_ids_for_request = std::mem::replace(
                        &mut object_ids_to_delete,
                        Vec::with_capacity(MAX_ITEMS_TO_DELETE),
                    );
                    send_delete_request(
                        s3_client,
                        &s3_target.bucket_name,
                        object_ids_for_request,
                        dry_run,
                    )
                    .await
                    .context("object ids deletion")?;
                }
                object_ids_to_delete.push(object_id);
            }

            match fetch_response.continuation_token {
                Some(new_token) => continuation_token = Some(new_token),
                None => break,
            }
        }
    }

    if !object_ids_to_delete.is_empty() {
        info!("Removing last objects of the batch");
        send_delete_request(
            s3_client,
            &s3_target.bucket_name,
            object_ids_to_delete,
            dry_run,
        )
        .await
        .context("Last object ids deletion")?;
    }

    Ok(())
}

async fn send_delete_request(
    s3_client: &Client,
    bucket_name: &str,
    ids: Vec<ObjectIdentifier>,
    dry_run: bool,
) -> anyhow::Result<()> {
    info!("Removing {} object ids from S3", ids.len());
    info!("Object ids to remove: {ids:?}");
    let delete_request = s3_client
        .delete_objects()
        .bucket(bucket_name)
        .delete(Delete::builder().set_objects(Some(ids)).build());
    if dry_run {
        info!("Dry run, skipping the actual removal");
        Ok(())
    } else {
        let original_request = delete_request.clone();

        match delete_request
            .send()
            .await
            .context("delete request processing")
        {
            Ok(delete_response) => match delete_response.errors() {
                Some(delete_errors) => {
                    error!("Delete request returned errors: {delete_errors:?}");
                    anyhow::bail!("Failed to delete all elements from the S3: {delete_errors:?}");
                }
                None => {
                    info!("Successfully removed an object batch from S3");
                    Ok(())
                }
            },
            Err(e) => {
                error!("Failed to send a delete request: {e:#}");
                error!("Original request: {original_request:?}");
                Err(e)
            }
        }
    }
}
