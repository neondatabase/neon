mod tenant_batch;
mod timeline_batch;

use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use aws_sdk_s3::Client;
use either::Either;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{error, info, info_span, Instrument};

use crate::cloud_admin_api::{BranchData, CloudAdminApiClient, ProjectData};
use crate::{list_objects_with_retries, RootTarget, S3Target, TraversingDepth, MAX_RETRIES};
use utils::id::{TenantId, TenantTimelineId};

/// Typical tenant to remove contains 1 layer and 1 index_part.json blobs
/// Also, there are some non-standard tenants to remove, having more layers.
/// delete_objects request allows up to 1000 keys, so be on a safe side and allow most
/// batch processing tasks to do 1 delete objects request only.
///
/// Every batch item will be additionally S3 LS'ed later, so keep the batch size
/// even lower to allow multiple concurrent tasks do the LS requests.
const BATCH_SIZE: usize = 100;

pub struct DeleteBatchProducer {
    delete_tenants_sender_task: JoinHandle<anyhow::Result<ProcessedS3List<TenantId, ProjectData>>>,
    delete_timelines_sender_task:
        JoinHandle<anyhow::Result<ProcessedS3List<TenantTimelineId, BranchData>>>,
    delete_batch_creator_task: JoinHandle<()>,
    delete_batch_receiver: Arc<Mutex<UnboundedReceiver<DeleteBatch>>>,
}

pub struct DeleteProducerStats {
    pub tenant_stats: ProcessedS3List<TenantId, ProjectData>,
    pub timeline_stats: Option<ProcessedS3List<TenantTimelineId, BranchData>>,
}

impl DeleteProducerStats {
    pub fn tenants_checked(&self) -> usize {
        self.tenant_stats.entries_total
    }

    pub fn active_tenants(&self) -> usize {
        self.tenant_stats.active_entries.len()
    }

    pub fn timelines_checked(&self) -> usize {
        self.timeline_stats
            .as_ref()
            .map(|stats| stats.entries_total)
            .unwrap_or(0)
    }
}

#[derive(Debug, Default, Clone)]
pub struct DeleteBatch {
    pub tenants: Vec<TenantId>,
    pub timelines: Vec<TenantTimelineId>,
}

impl DeleteBatch {
    pub fn merge(&mut self, other: Self) {
        self.tenants.extend(other.tenants);
        self.timelines.extend(other.timelines);
    }

    pub fn len(&self) -> usize {
        self.tenants.len() + self.timelines.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl DeleteBatchProducer {
    pub fn start(
        admin_client: Arc<CloudAdminApiClient>,
        s3_client: Arc<Client>,
        s3_root_target: RootTarget,
        traversing_depth: TraversingDepth,
    ) -> Self {
        let (delete_elements_sender, mut delete_elements_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let delete_elements_sender = Arc::new(delete_elements_sender);
        let admin_client = Arc::new(admin_client);

        let (projects_to_check_sender, mut projects_to_check_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let delete_tenants_root_target = s3_root_target.clone();
        let delete_tenants_client = Arc::clone(&s3_client);
        let delete_tenants_admin_client = Arc::clone(&admin_client);
        let delete_sender = Arc::clone(&delete_elements_sender);
        let delete_tenants_sender_task = tokio::spawn(
            async move {
                tenant_batch::schedule_cleanup_deleted_tenants(
                    &delete_tenants_root_target,
                    &delete_tenants_client,
                    &delete_tenants_admin_client,
                    projects_to_check_sender,
                    delete_sender,
                    traversing_depth,
                )
                .await
            }
            .instrument(info_span!("delete_tenants_sender")),
        );
        let delete_timelines_sender_task = tokio::spawn(async move {
            timeline_batch::schedule_cleanup_deleted_timelines(
                &s3_root_target,
                &s3_client,
                &admin_client,
                &mut projects_to_check_receiver,
                delete_elements_sender,
            )
            .in_current_span()
            .await
        });

        let (delete_batch_sender, delete_batch_receiver) = tokio::sync::mpsc::unbounded_channel();
        let delete_batch_creator_task = tokio::spawn(
            async move {
                'outer: loop {
                    let mut delete_batch = DeleteBatch::default();
                    while delete_batch.len() < BATCH_SIZE {
                        match delete_elements_receiver.recv().await {
                            Some(new_task) => match new_task {
                                Either::Left(tenant_id) => delete_batch.tenants.push(tenant_id),
                                Either::Right(timeline_id) => {
                                    delete_batch.timelines.push(timeline_id)
                                }
                            },
                            None => {
                                info!("Task finished: sender dropped");
                                delete_batch_sender.send(delete_batch).ok();
                                break 'outer;
                            }
                        }
                    }

                    if !delete_batch.is_empty() {
                        delete_batch_sender.send(delete_batch).ok();
                    }
                }
            }
            .instrument(info_span!("delete batch creator")),
        );

        Self {
            delete_tenants_sender_task,
            delete_timelines_sender_task,
            delete_batch_creator_task,
            delete_batch_receiver: Arc::new(Mutex::new(delete_batch_receiver)),
        }
    }

    pub fn subscribe(&self) -> Arc<Mutex<UnboundedReceiver<DeleteBatch>>> {
        self.delete_batch_receiver.clone()
    }

    pub async fn join(self) -> anyhow::Result<DeleteProducerStats> {
        let (delete_tenants_task_result, delete_timelines_task_result, batch_task_result) = tokio::join!(
            self.delete_tenants_sender_task,
            self.delete_timelines_sender_task,
            self.delete_batch_creator_task,
        );

        let tenant_stats = match delete_tenants_task_result {
            Ok(Ok(stats)) => stats,
            Ok(Err(tenant_deletion_error)) => return Err(tenant_deletion_error),
            Err(join_error) => {
                anyhow::bail!("Failed to join the delete tenant producing task: {join_error}")
            }
        };

        let timeline_stats = match delete_timelines_task_result {
            Ok(Ok(stats)) => Some(stats),
            Ok(Err(timeline_deletion_error)) => return Err(timeline_deletion_error),
            Err(join_error) => {
                anyhow::bail!("Failed to join the delete timeline producing task: {join_error}")
            }
        };

        match batch_task_result {
            Ok(()) => (),
            Err(join_error) => anyhow::bail!("Failed to join the batch forming task: {join_error}"),
        };

        Ok(DeleteProducerStats {
            tenant_stats,
            timeline_stats,
        })
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
        self.entries_total += other.entries_total;
        self.entries_to_delete.extend(other.entries_to_delete);
        self.active_entries.extend(other.active_entries);
    }

    fn change_ids<NewI>(self, transform: impl Fn(I) -> NewI) -> ProcessedS3List<NewI, A> {
        ProcessedS3List {
            entries_total: self.entries_total,
            entries_to_delete: self.entries_to_delete.into_iter().map(transform).collect(),
            active_entries: self.active_entries,
        }
    }
}

async fn process_s3_target_recursively<F, Fut, I, E, A>(
    s3_client: &Client,
    target: &S3Target,
    find_active_and_deleted_entries: F,
) -> anyhow::Result<ProcessedS3List<I, A>>
where
    I: FromStr<Err = E> + Send + Sync,
    E: Send + Sync + std::error::Error + 'static,
    F: FnOnce(Vec<I>) -> Fut + Clone,
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
            (find_active_and_deleted_entries.clone())(new_entry_ids)
                .await
                .context("filter active and deleted entry ids")?,
        );

        match fetch_response.next_continuation_token {
            Some(new_token) => continuation_token = Some(new_token),
            None => break,
        }
    }

    Ok(total_entries)
}

enum FetchResult<A> {
    Found(A),
    Deleted,
    Absent,
}

async fn split_to_active_and_deleted_entries<I, A, F, Fut>(
    new_entry_ids: Vec<I>,
    find_active_entry: F,
) -> anyhow::Result<ProcessedS3List<I, A>>
where
    I: std::fmt::Display + Send + Sync + 'static + Copy,
    A: Send + 'static,
    F: FnOnce(I) -> Fut + Send + Sync + 'static + Clone,
    Fut: Future<Output = anyhow::Result<FetchResult<A>>> + Send,
{
    let entries_total = new_entry_ids.len();
    let mut check_tasks = JoinSet::new();
    let mut active_entries = Vec::with_capacity(entries_total);
    let mut entries_to_delete = Vec::with_capacity(entries_total);

    for new_entry_id in new_entry_ids {
        let check_closure = find_active_entry.clone();
        check_tasks.spawn(
            async move {
                (
                    new_entry_id,
                    async {
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
            FetchResult::Found(active_entry) => {
                info!("Entry {entry_id} is alive, cannot delete");
                active_entries.push(active_entry);
            }
            FetchResult::Deleted => {
                info!("Entry {entry_id} deleted in the admin data, can safely delete");
                entries_to_delete.push(entry_id);
            }
            FetchResult::Absent => {
                info!("Entry {entry_id} absent in the admin data, can safely delete");
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
