use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use aws_sdk_s3::Client;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{error, info, info_span, Instrument};

use crate::cloud_admin_api::{BranchData, CloudAdminApiClient, ProjectData};
use crate::copied_definitions::id::TenantTimelineId;
use crate::{list_objects_with_retries, S3Target, TenantId, TraversingDepth, MAX_RETRIES};

/// Typical tenant to remove contains 1 layer and 1 index_part.json blobs
/// Also, there are some non-standard tenants to remove, having more layers.
/// delete_objects request allows up to 1000 keys, so be on a safe side and allow most
/// batch processing tasks to do 1 delete objects request only.
///
/// Every batch item will be additionally S3 LS'ed later, so keep the batch size
/// even lower to allow multiple concurrent tasks do the LS requests.
const BATCH_SIZE: usize = 100;

pub struct DeleteBatchProducer {
    batch_sender_task: JoinHandle<anyhow::Result<BatchProducerStats>>,
    batch_receiver: Arc<Mutex<UnboundedReceiver<DeleteBatch>>>,
}

pub struct BatchProducerStats {
    pub tenant_stats: ProcessedS3List<TenantId, ProjectData>,
    pub timeline_stats: Option<ProcessedS3List<TenantTimelineId, BranchData>>,
}

impl BatchProducerStats {
    pub fn tenants_checked(&self) -> usize {
        self.tenant_stats.entries_total
    }

    pub fn timelines_checked(&self) -> usize {
        self.timeline_stats
            .as_ref()
            .map(|stats| stats.entries_total)
            .unwrap_or(0)
    }
}

#[derive(Debug, Default)]
pub struct DeleteBatch {
    pub tenants: Vec<TenantId>,
    pub timelines: Vec<TenantTimelineId>,
}

impl DeleteBatch {
    pub fn merge(&mut self, other: Self) {
        self.tenants.extend(other.tenants);
        self.timelines.extend(other.timelines);
    }
}

impl DeleteBatchProducer {
    pub fn start(
        admin_client: CloudAdminApiClient,
        s3_client: Arc<Client>,
        s3_target: S3Target,
        traversing_depth: TraversingDepth,
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
                let check_client = Arc::clone(&admin_client);

                let tenant_stats =
                    process_s3_target_recursively(&s3_client, &s3_target, |s3_tenants| async {
                        let another_client = Arc::clone(&check_client);
                        split_to_active_and_deleted_entries(s3_tenants, |tenant_id| async move {
                            another_client
                                .find_tenant_project(tenant_id)
                                .await
                                .with_context(|| format!("Tenant {tenant_id} project admin check"))
                        })
                        .await
                    })
                    .await
                    .context("tenant batch processing")?;

                for tenant_batch in tenant_stats.entries_to_delete.chunks(BATCH_SIZE) {
                    batch_sender
                        .send(DeleteBatch {
                            tenants: tenant_batch.to_vec(),
                            timelines: Vec::new(),
                        })
                        .ok();
                }

                let batch_stats = match traversing_depth {
                    TraversingDepth::Tenant => {
                        info!("Finished listing the bucket for tenants only");
                        BatchProducerStats {
                            tenant_stats,
                            timeline_stats: None,
                        }
                    }
                    TraversingDepth::Timeline => {
                        info!(
                            "Processing timelines for {} active tenants",
                            tenant_stats.active_entries.len()
                        );

                        // TODO kb that should also be done in async fashion + batch on the receiver side
                        let mut common_timeline_stats = ProcessedS3List::default();
                        for active_tenant in &tenant_stats.active_entries {
                            let tenant_timelines_target = s3_target
                                .with_sub_segment(&active_tenant.tenant.to_string())
                                .with_sub_segment("timelines");
                            let check_client = Arc::clone(&admin_client);
                            let tenant_timelines_stats = process_s3_target_recursively(
                                &s3_client,
                                &tenant_timelines_target,
                                |s3_timelines| async {
                                    let another_client = check_client.clone();
                                    split_to_active_and_deleted_entries(
                                        s3_timelines,
                                        |timeline_id| async move {
                                            another_client
                                                .find_timeline_branch(timeline_id)
                                                .await
                                                .with_context(|| {
                                                    format!(
                                                        "Timeline {timeline_id} branch admin check"
                                                    )
                                                })
                                        },
                                    )
                                    .await
                                },
                            )
                            .await
                            .with_context(|| {
                                format!("tenant {} timeline batch processing", active_tenant.tenant)
                            })?
                            .change_ids(|timeline_id| {
                                TenantTimelineId::new(active_tenant.tenant, timeline_id)
                            });

                            common_timeline_stats.merge(tenant_timelines_stats);
                        }

                        for timeline_batch in
                            common_timeline_stats.entries_to_delete.chunks(BATCH_SIZE)
                        {
                            batch_sender
                                .send(DeleteBatch {
                                    tenants: Vec::new(),
                                    timelines: timeline_batch.to_vec(),
                                })
                                .ok();
                        }

                        info!("Finished listing the bucket for tenants and timelines");
                        BatchProducerStats {
                            tenant_stats,
                            timeline_stats: Some(common_timeline_stats),
                        }
                    }
                };

                Ok(batch_stats)
            }
            .instrument(list_span),
        );

        Self {
            batch_sender_task,
            batch_receiver: Arc::new(Mutex::new(batch_receiver)),
        }
    }

    pub fn subscribe(&self) -> Arc<Mutex<UnboundedReceiver<DeleteBatch>>> {
        self.batch_receiver.clone()
    }

    pub async fn join(self) -> anyhow::Result<BatchProducerStats> {
        match self.batch_sender_task.await {
            Ok(task_result) => task_result,
            Err(join_error) => anyhow::bail!("Failed to join the task: {join_error}"),
        }
    }
}

pub struct ProcessedS3List<I, A> {
    pub entries_total: usize,
    pub entries_to_delete: Vec<I>,
    pub active_entries: Vec<A>,
}

impl<I, A> Default for ProcessedS3List<I, A> {
    fn default() -> Self {
        Self {
            entries_total: 0,
            entries_to_delete: Vec::new(),
            active_entries: Vec::new(),
        }
    }
}

impl<I, A> ProcessedS3List<I, A> {
    fn merge(&mut self, other: Self) {
        self.entries_total = other.entries_total;
        self.entries_to_delete = other.entries_to_delete;
        self.active_entries = other.active_entries;
    }

    fn change_ids<NewI>(self, transform: impl Fn(I) -> NewI) -> ProcessedS3List<NewI, A> {
        ProcessedS3List {
            entries_total: self.entries_total,
            entries_to_delete: self
                .entries_to_delete
                .into_iter()
                .map(|entry| transform(entry))
                .collect(),
            active_entries: self.active_entries,
        }
    }
}

async fn process_s3_target_recursively<F, Fut, I, E, A>(
    s3_client: &Client,
    target: &S3Target,
    mut find_active_and_deleted_entries: F,
) -> anyhow::Result<ProcessedS3List<I, A>>
where
    I: FromStr<Err = E> + Send + Sync,
    E: Send + Sync + std::error::Error + 'static,
    F: FnMut(Vec<I>) -> Fut,
    Fut: Future<Output = anyhow::Result<ProcessedS3List<I, A>>>,
{
    let mut continuation_token = None;
    let mut total_entries = ProcessedS3List::default();

    loop {
        let fetch_response =
            list_objects_with_retries(s3_client, target, continuation_token.clone()).await?;

        let new_entry_ids = fetch_response
            .common_prefixes()
            .unwrap_or_default()
            .iter()
            .filter_map(|prefix| prefix.prefix())
            .filter_map(|prefix| -> Option<&str> {
                prefix
                    .strip_prefix(&target.prefix_in_bucket)?
                    .strip_suffix('/')
            })
            .map(|entry_id_str| {
                entry_id_str
                    .parse()
                    .with_context(|| format!("Incorrect entry id str: {entry_id_str}"))
            })
            .collect::<anyhow::Result<Vec<I>>>()
            .context("list and parse bucket's entry ids")?;

        total_entries.merge(
            find_active_and_deleted_entries(new_entry_ids)
                .await
                .context("filter active and deleted entry ids")?,
        );

        match fetch_response.next_continuation_token {
            Some(new_token) => continuation_token = Some(new_token),
            None => break,
        }
    }

    info!(
        "Among {}, found {} tenants to delete and {} active ones",
        total_entries.entries_total,
        total_entries.entries_to_delete.len(),
        total_entries.active_entries.len(),
    );

    Ok(total_entries)
}

async fn split_to_active_and_deleted_entries<I, A, F, Fut>(
    new_entry_ids: Vec<I>,
    find_active_entry: F,
) -> anyhow::Result<ProcessedS3List<I, A>>
where
    I: std::fmt::Display + Send + Sync + 'static + Copy,
    A: Send + 'static,
    F: FnOnce(I) -> Fut + Send + Sync + 'static + Clone,
    Fut: Future<Output = anyhow::Result<Option<A>>> + Send,
{
    let entries_total = new_entry_ids.len();
    let api_request_limiter = Arc::new(Semaphore::new(200));
    let mut check_tasks = JoinSet::new();
    let mut active_entries = Vec::with_capacity(entries_total);
    let mut entries_to_delete = Vec::with_capacity(entries_total);

    for new_entry_id in new_entry_ids {
        let check_limit = Arc::clone(&api_request_limiter);
        let check_closure = find_active_entry.clone();
        check_tasks.spawn(
            async move {
                (
                    new_entry_id,
                    async {
                        let _permit = check_limit
                            .acquire()
                            .await
                            .expect("Semaphore is not closed");
                        for _ in 0..MAX_RETRIES {
                            let closure_clone = check_closure.clone();
                            match closure_clone(new_entry_id).await {
                                Ok(active_entry) => return Ok(active_entry),
                                Err(e) => {
                                    error!("find active entry admin API call failed: {e}");
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                }
                            }
                        }

                        anyhow::bail!("Failed to check entry {new_entry_id} {MAX_RETRIES} times")
                    }
                    .await,
                )
            }
            .instrument(info_span!("filter_active_entries")),
        );
    }

    while let Some(task_result) = check_tasks.join_next().await {
        let (entry_id, entry_data_fetch_result) = task_result.context("task join")?;
        match entry_data_fetch_result.context("entry data fetch")? {
            Some(active_entry) => {
                info!("Found alive project for entry {entry_id}, cannot delete");
                active_entries.push(active_entry);
            }
            None => {
                info!("Found deleted or no project for entry {entry_id} in admin data, can safely delete");
                entries_to_delete.push(entry_id);
            }
        }
    }
    Ok(ProcessedS3List {
        entries_total,
        entries_to_delete,
        active_entries,
    })
}
