use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use crate::{
    metrics::SECONDARY_MODE,
    tenant::{
        config::AttachmentMode, mgr::TenantManager, remote_timeline_client::remote_heatmap_path,
        secondary::CommandResponse, span::debug_assert_current_span_has_tenant_id, Tenant,
    },
};

use md5;
use pageserver_api::shard::TenantShardId;
use remote_storage::GenericRemoteStorage;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use utils::{backoff, completion::Barrier};

use super::{heatmap::HeatMapTenant, CommandRequest, UploadCommand};

/// Period between heatmap uploader walking Tenants to look for work to do.
/// If any tenants have a heatmap upload period lower than this, it will be adjusted
/// downward to match.
const DEFAULT_SCHEDULING_INTERVAL: Duration = Duration::from_millis(60000);
const MIN_SCHEDULING_INTERVAL: Duration = Duration::from_millis(1000);

struct WriteInProgress {
    barrier: Barrier,
}

struct UploadPending {
    tenant: Arc<Tenant>,
    last_digest: Option<md5::Digest>,
}

struct WriteComplete {
    tenant_shard_id: TenantShardId,
    completed_at: Instant,
    digest: Option<md5::Digest>,
    next_upload: Option<Instant>,
}

/// The heatmap uploader keeps a little bit of per-tenant state, mainly to remember
/// when we last did a write.  We only populate this after doing at least one
/// write for a tenant -- this avoids holding state for tenants that have
/// uploads disabled.

struct UploaderTenantState {
    // This Weak only exists to enable culling idle instances of this type
    // when the Tenant has been deallocated.
    tenant: Weak<Tenant>,

    /// Digest of the serialized heatmap that we last successfully uploaded
    ///
    /// md5 is generally a bad hash.  We use it because it's convenient for interop with AWS S3's ETag,
    /// which is also an md5sum.
    last_digest: Option<md5::Digest>,

    /// When the last upload attempt completed (may have been successful or failed)
    last_upload: Option<Instant>,

    /// When should we next do an upload?  None means never.
    next_upload: Option<Instant>,
}

/// This type is owned by a single task ([`heatmap_uploader_task`]) which runs an event
/// handling loop and mutates it as needed: there are no locks here, because that event loop
/// can hold &mut references to this type throughout.
struct HeatmapUploader {
    tenant_manager: Arc<TenantManager>,
    remote_storage: GenericRemoteStorage,
    cancel: CancellationToken,

    tenants: HashMap<TenantShardId, UploaderTenantState>,

    /// Tenants with work to do, for which tasks should be spawned as soon as concurrency
    /// limits permit it.
    tenants_pending: std::collections::VecDeque<UploadPending>,

    /// Tenants for which a task in `tasks` has been spawned.
    tenants_uploading: HashMap<TenantShardId, WriteInProgress>,

    tasks: JoinSet<()>,

    /// Channel for our child tasks to send results to: we use a channel for results rather than
    /// just getting task results via JoinSet because we need the channel's recv() "sleep until something
    /// is available" semantic, rather than JoinSet::join_next()'s "sleep until next thing is available _or_ I'm empty"
    /// behavior.
    task_result_tx: tokio::sync::mpsc::UnboundedSender<WriteComplete>,
    task_result_rx: tokio::sync::mpsc::UnboundedReceiver<WriteComplete>,

    concurrent_uploads: usize,

    scheduling_interval: Duration,
}

/// The uploader task runs a loop that periodically wakes up and schedules tasks for
/// tenants that require an upload, or handles any commands that have been sent into
/// `command_queue`.  No I/O is done in this loop: that all happens in the tasks we
/// spawn.
///
/// Scheduling iterations are somewhat infrequent.  However, each one will enqueue
/// all tenants that require an upload, and in between scheduling iterations we will
/// continue to spawn new tasks for pending tenants, as our concurrency limit permits.
///
/// While we take a CancellationToken here, it is subordinate to the CancellationTokens
/// of tenants: i.e. we expect all Tenants to have been shut down before we are shut down, otherwise
/// we might block waiting on a Tenant.
pub(super) async fn heatmap_uploader_task(
    tenant_manager: Arc<TenantManager>,
    remote_storage: GenericRemoteStorage,
    mut command_queue: tokio::sync::mpsc::Receiver<CommandRequest<UploadCommand>>,
    background_jobs_can_start: Barrier,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let concurrent_uploads = tenant_manager.get_conf().heatmap_upload_concurrency;

    let (result_tx, result_rx) = tokio::sync::mpsc::unbounded_channel();

    let mut uploader = HeatmapUploader {
        tenant_manager,
        remote_storage,
        cancel: cancel.clone(),
        tasks: JoinSet::new(),
        tenants: HashMap::new(),
        tenants_pending: std::collections::VecDeque::new(),
        tenants_uploading: HashMap::new(),
        task_result_tx: result_tx,
        task_result_rx: result_rx,
        concurrent_uploads,
        scheduling_interval: DEFAULT_SCHEDULING_INTERVAL,
    };

    tracing::info!("Waiting for background_jobs_can start...");
    background_jobs_can_start.wait().await;
    tracing::info!("background_jobs_can is ready, proceeding.");

    while !cancel.is_cancelled() {
        // Look for new work: this is relatively expensive because we have to go acquire the lock on
        // the tenant manager to retrieve tenants, and then iterate over them to figure out which ones
        // require an upload.
        uploader.schedule_iteration().await?;

        // Between scheduling iterations, we will:
        //  - Drain any complete tasks and spawn pending tasks
        //  - Handle incoming administrative commands
        //  - Check our cancellation token
        let next_scheduling_iteration = Instant::now()
            .checked_add(uploader.scheduling_interval)
            .unwrap_or_else(|| {
                tracing::warn!(
                    "Scheduling interval invalid ({}s), running immediately!",
                    uploader.scheduling_interval.as_secs_f64()
                );
                Instant::now()
            });
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    // We do not simply drop the JoinSet, in order to have an orderly shutdown without cancellation.
                    tracing::info!("Heatmap uploader joining tasks");
                    while let Some(_r) = uploader.tasks.join_next().await {};
                    tracing::info!("Heatmap uploader terminating");

                    break;
                },
                _ = tokio::time::sleep(next_scheduling_iteration.duration_since(Instant::now())) => {
                    tracing::debug!("heatmap_uploader_task: woke for scheduling interval");
                    break;},
                cmd = command_queue.recv() => {
                    tracing::debug!("heatmap_uploader_task: woke for command queue");
                    let cmd = match cmd {
                        Some(c) =>c,
                        None => {
                            // SecondaryController was destroyed, and this has raced with
                            // our CancellationToken
                            tracing::info!("Heatmap uploader terminating");
                            cancel.cancel();
                            break;
                        }
                    };

                    let CommandRequest{
                        response_tx,
                        payload
                    } = cmd;
                    uploader.handle_command(payload, response_tx);
                },
                _ = uploader.process_next_completion() => {
                    if !cancel.is_cancelled() {
                        uploader.spawn_pending();
                    }
                }
            }
        }
    }

    Ok(())
}

impl HeatmapUploader {
    /// Periodic execution phase: inspect all attached tenants and schedule any work they require.
    async fn schedule_iteration(&mut self) -> anyhow::Result<()> {
        // Cull any entries in self.tenants whose Arc<Tenant> is gone
        self.tenants
            .retain(|_k, v| v.tenant.upgrade().is_some() && v.next_upload.is_some());

        // The priority order of previously scheduled work may be invalidated by current state: drop
        // all pending work (it will be re-scheduled if still needed)
        self.tenants_pending.clear();

        // Used a fixed 'now' through the following loop, for efficiency and fairness.
        let now = Instant::now();

        // While iterating over the potentially-long list of tenants, we will periodically yield
        // to avoid blocking executor.
        const YIELD_ITERATIONS: usize = 1000;

        // Iterate over tenants looking for work to do.
        let tenants = self.tenant_manager.get_attached_active_tenant_shards();
        for (i, tenant) in tenants.into_iter().enumerate() {
            // Process is shutting down, drop out
            if self.cancel.is_cancelled() {
                return Ok(());
            }

            // Skip tenants that already have a write in flight
            if self
                .tenants_uploading
                .contains_key(tenant.get_tenant_shard_id())
            {
                continue;
            }

            self.maybe_schedule_upload(&now, tenant);

            if i + 1 % YIELD_ITERATIONS == 0 {
                tokio::task::yield_now().await;
            }
        }

        // Spawn tasks for as many of our pending tenants as we can.
        self.spawn_pending();

        Ok(())
    }

    ///
    /// Cancellation: this method is cancel-safe.
    async fn process_next_completion(&mut self) {
        match self.task_result_rx.recv().await {
            Some(r) => {
                self.on_completion(r);
            }
            None => {
                unreachable!("Result sender is stored on Self");
            }
        }
    }

    /// The 'maybe' refers to the tenant's state: whether it is configured
    /// for heatmap uploads at all, and whether sufficient time has passed
    /// since the last upload.
    fn maybe_schedule_upload(&mut self, now: &Instant, tenant: Arc<Tenant>) {
        match tenant.get_heatmap_period() {
            None => {
                // Heatmaps are disabled for this tenant
                return;
            }
            Some(period) => {
                // If any tenant has asked for uploads more frequent than our scheduling interval,
                // reduce it to match so that we can keep up.  This is mainly useful in testing, where
                // we may set rather short intervals.
                if period < self.scheduling_interval {
                    self.scheduling_interval = std::cmp::max(period, MIN_SCHEDULING_INTERVAL);
                }
            }
        }

        // Stale attachments do not upload anything: if we are in this state, there is probably some
        // other attachment in mode Single or Multi running on another pageserver, and we don't
        // want to thrash and overwrite their heatmap uploads.
        if tenant.get_attach_mode() == AttachmentMode::Stale {
            return;
        }

        // Create an entry in self.tenants if one doesn't already exist: this will later be updated
        // with the completion time in on_completion.
        let state = self
            .tenants
            .entry(*tenant.get_tenant_shard_id())
            .or_insert_with(|| UploaderTenantState {
                tenant: Arc::downgrade(&tenant),
                last_upload: None,
                next_upload: Some(Instant::now()),
                last_digest: None,
            });

        // Decline to do the upload if insufficient time has passed
        if state.next_upload.map(|nu| &nu > now).unwrap_or(false) {
            return;
        }

        let last_digest = state.last_digest;
        self.tenants_pending.push_back(UploadPending {
            tenant,
            last_digest,
        })
    }

    fn spawn_pending(&mut self) {
        while !self.tenants_pending.is_empty()
            && self.tenants_uploading.len() < self.concurrent_uploads
        {
            // unwrap: loop condition includes !is_empty()
            let pending = self.tenants_pending.pop_front().unwrap();
            self.spawn_upload(pending.tenant, pending.last_digest);
        }
    }

    fn spawn_upload(&mut self, tenant: Arc<Tenant>, last_digest: Option<md5::Digest>) {
        let remote_storage = self.remote_storage.clone();
        let tenant_shard_id = *tenant.get_tenant_shard_id();
        let (completion, barrier) = utils::completion::channel();
        let result_tx = self.task_result_tx.clone();
        self.tasks.spawn(async move {
            // Guard for the barrier in [`WriteInProgress`]
            let _completion = completion;

            let started_at = Instant::now();
            let digest = match upload_tenant_heatmap(remote_storage, &tenant, last_digest).await {
                Ok(UploadHeatmapOutcome::Uploaded(digest)) => {
                    let duration = Instant::now().duration_since(started_at);
                    SECONDARY_MODE
                        .upload_heatmap_duration
                        .observe(duration.as_secs_f64());
                    SECONDARY_MODE.upload_heatmap.inc();
                    Some(digest)
                }
                Ok(UploadHeatmapOutcome::NoChange | UploadHeatmapOutcome::Skipped) => last_digest,
                Err(UploadHeatmapError::Upload(e)) => {
                    tracing::warn!(
                        "Failed to upload heatmap for tenant {}: {e:#}",
                        tenant.get_tenant_shard_id(),
                    );
                    let duration = Instant::now().duration_since(started_at);
                    SECONDARY_MODE
                        .upload_heatmap_duration
                        .observe(duration.as_secs_f64());
                    SECONDARY_MODE.upload_heatmap_errors.inc();
                    last_digest
                }
                Err(UploadHeatmapError::Cancelled) => {
                    tracing::info!("Cancelled heatmap upload, shutting down");
                    last_digest
                }
            };

            let now = Instant::now();
            let next_upload = tenant
                .get_heatmap_period()
                .and_then(|period| now.checked_add(period));

            result_tx
                .send(WriteComplete {
                    tenant_shard_id: *tenant.get_tenant_shard_id(),
                    completed_at: now,
                    digest,
                    next_upload,
                })
                .ok();
        });

        self.tenants_uploading
            .insert(tenant_shard_id, WriteInProgress { barrier });
    }

    #[instrument(skip_all, fields(tenant_id=%completion.tenant_shard_id.tenant_id, shard_id=%completion.tenant_shard_id.shard_slug()))]
    fn on_completion(&mut self, completion: WriteComplete) {
        tracing::debug!("Heatmap upload completed");
        let WriteComplete {
            tenant_shard_id,
            completed_at,
            digest,
            next_upload,
        } = completion;
        self.tenants_uploading.remove(&tenant_shard_id);
        use std::collections::hash_map::Entry;
        match self.tenants.entry(tenant_shard_id) {
            Entry::Vacant(_) => {
                // Tenant state was dropped, nothing to update.
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().last_upload = Some(completed_at);
                entry.get_mut().last_digest = digest;
                entry.get_mut().next_upload = next_upload
            }
        }
    }

    fn handle_command(
        &mut self,
        command: UploadCommand,
        response_tx: tokio::sync::oneshot::Sender<CommandResponse>,
    ) {
        match command {
            UploadCommand::Upload(tenant_shard_id) => {
                // If an upload was ongoing for this tenant, let it finish first.
                let barrier = if let Some(writing_state) =
                    self.tenants_uploading.get(&tenant_shard_id)
                {
                    tracing::info!(
                        tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(),
                        "Waiting for heatmap write to complete");
                    writing_state.barrier.clone()
                } else {
                    // Spawn the upload then immediately wait for it.  This will block processing of other commands and
                    // starting of other background work.
                    tracing::info!(
                        tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(),
                        "Starting heatmap write on command");
                    let tenant = match self
                        .tenant_manager
                        .get_attached_tenant_shard(tenant_shard_id, true)
                    {
                        Ok(t) => t,
                        Err(e) => {
                            // Drop result of send: we don't care if caller dropped their receiver
                            drop(response_tx.send(CommandResponse {
                                result: Err(e.into()),
                            }));
                            return;
                        }
                    };
                    self.spawn_upload(tenant, None);
                    let writing_state = self
                        .tenants_uploading
                        .get(&tenant_shard_id)
                        .expect("We just inserted this");
                    tracing::info!(
                        tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(),
                        "Waiting for heatmap upload to complete");

                    writing_state.barrier.clone()
                };

                // This task does no I/O: it only listens for a barrier's completion and then
                // sends to the command response channel.  It is therefore safe to spawn this without
                // any gates/task_mgr hooks.
                tokio::task::spawn(async move {
                    barrier.wait().await;

                    tracing::info!(
                        tenant_id=%tenant_shard_id.tenant_id, shard_id=%tenant_shard_id.shard_slug(),
                        "Heatmap upload complete");

                    // Drop result of send: we don't care if caller dropped their receiver
                    drop(response_tx.send(CommandResponse { result: Ok(()) }))
                });
            }
        }
    }
}

enum UploadHeatmapOutcome {
    /// We successfully wrote to remote storage, with this digest.
    Uploaded(md5::Digest),
    /// We did not upload because the heatmap digest was unchanged since the last upload
    NoChange,
    /// We skipped the upload for some reason, such as tenant/timeline not ready
    Skipped,
}

#[derive(thiserror::Error, Debug)]
enum UploadHeatmapError {
    #[error("Cancelled")]
    Cancelled,

    #[error(transparent)]
    Upload(#[from] anyhow::Error),
}

/// The inner upload operation.  This will skip if `last_digest` is Some and matches the digest
/// of the object we would have uploaded.
#[instrument(skip_all, fields(tenant_id = %tenant.get_tenant_shard_id().tenant_id, shard_id = %tenant.get_tenant_shard_id().shard_slug()))]
async fn upload_tenant_heatmap(
    remote_storage: GenericRemoteStorage,
    tenant: &Arc<Tenant>,
    last_digest: Option<md5::Digest>,
) -> Result<UploadHeatmapOutcome, UploadHeatmapError> {
    debug_assert_current_span_has_tenant_id();

    let generation = tenant.get_generation();
    if generation.is_none() {
        // We do not expect this: generations were implemented before heatmap uploads.  However,
        // handle it so that we don't have to make the generation in the heatmap an Option<>
        // (Generation::none is not serializable)
        tracing::warn!("Skipping heatmap upload for tenant with generation==None");
        return Ok(UploadHeatmapOutcome::Skipped);
    }

    let mut heatmap = HeatMapTenant {
        timelines: Vec::new(),
        generation,
    };
    let timelines = tenant.timelines.lock().unwrap().clone();

    let tenant_cancel = tenant.cancel.clone();

    // Ensure that Tenant::shutdown waits for any upload in flight: this is needed because otherwise
    // when we delete a tenant, we might race with an upload in flight and end up leaving a heatmap behind
    // in remote storage.
    let _guard = match tenant.gate.enter() {
        Ok(g) => g,
        Err(_) => {
            tracing::info!("Skipping heatmap upload for tenant which is shutting down");
            return Err(UploadHeatmapError::Cancelled);
        }
    };

    for (timeline_id, timeline) in timelines {
        let heatmap_timeline = timeline.generate_heatmap().await;
        match heatmap_timeline {
            None => {
                tracing::debug!(
                    "Skipping heatmap upload because timeline {timeline_id} is not ready"
                );
                return Ok(UploadHeatmapOutcome::Skipped);
            }
            Some(heatmap_timeline) => {
                heatmap.timelines.push(heatmap_timeline);
            }
        }
    }

    // Serialize the heatmap
    let bytes = serde_json::to_vec(&heatmap).map_err(|e| anyhow::anyhow!(e))?;
    let size = bytes.len();

    // Drop out early if nothing changed since our last upload
    let digest = md5::compute(&bytes);
    if Some(digest) == last_digest {
        return Ok(UploadHeatmapOutcome::NoChange);
    }

    let path = remote_heatmap_path(tenant.get_tenant_shard_id());

    // Write the heatmap.
    tracing::debug!("Uploading {size} byte heatmap to {path}");
    if let Err(e) = backoff::retry(
        || async {
            let bytes = futures::stream::once(futures::future::ready(Ok(bytes::Bytes::from(
                bytes.clone(),
            ))));
            remote_storage
                .upload_storage_object(bytes, size, &path)
                .await
        },
        |_| false,
        3,
        u32::MAX,
        "Uploading heatmap",
        backoff::Cancel::new(tenant_cancel.clone(), || anyhow::anyhow!("Shutting down")),
    )
    .await
    {
        if tenant_cancel.is_cancelled() {
            return Err(UploadHeatmapError::Cancelled);
        } else {
            return Err(e.into());
        }
    }

    tracing::info!("Successfully uploaded {size} byte heatmap to {path}");

    Ok(UploadHeatmapOutcome::Uploaded(digest))
}
