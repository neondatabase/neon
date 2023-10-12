use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use crate::{
    metrics::SECONDARY_MODE,
    tenant::{
        mgr::{self, TenantManager},
        remote_timeline_client::remote_heatmap_path,
        secondary::CommandResponse,
        Tenant,
    },
};

use pageserver_api::models::TenantState;
use remote_storage::GenericRemoteStorage;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use utils::{backoff, completion::Barrier, id::TenantId};

use super::{heatmap::HeatMapTenant, CommandRequest, UploadCommand};

/// Period between heatmap writer walking Tenants to look for work to do
const HEATMAP_WAKE_INTERVAL: Duration = Duration::from_millis(1000);

/// Periodic between heatmap writes for each Tenant
const HEATMAP_UPLOAD_INTERVAL: Duration = Duration::from_millis(60000);

/// While we take a CancellationToken here, it is subordinate to the CancellationTokens
/// of tenants: i.e. we expect all Tenants to have been shut down before we are shut down, otherwise
/// we might block waiting on a Tenant.
pub(super) async fn heatmap_writer_task(
    tenant_manager: Arc<TenantManager>,
    remote_storage: GenericRemoteStorage,
    mut command_queue: tokio::sync::mpsc::Receiver<CommandRequest<UploadCommand>>,
    background_jobs_can_start: Barrier,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut writer = HeatmapWriter {
        tenant_manager,
        remote_storage,
        cancel: cancel.clone(),
        tasks: JoinSet::new(),
        tenants: HashMap::new(),
        tenants_writing: HashMap::new(),
        concurrent_writes: 8,
    };

    tracing::info!("Waiting for background_jobs_can start...");
    background_jobs_can_start.wait().await;
    tracing::info!("background_jobs_can is ready, proceeding.");

    while !cancel.is_cancelled() {
        writer.iteration().await?;

        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("Heatmap writer joining tasks");

                tracing::info!("Heatmap writer terminating");

                break;
            },
            _ = tokio::time::sleep(HEATMAP_WAKE_INTERVAL) => {},
            cmd = command_queue.recv() => {
                let cmd = match cmd {
                    Some(c) =>c,
                    None => {
                        // SecondaryController was destroyed, and this has raced with
                        // our CancellationToken
                        tracing::info!("Heatmap writer terminating");
                        break;
                    }
                };

                let CommandRequest{
                    response_tx,
                    payload
                } = cmd;
                let result = writer.handle_command(payload).await;
                if response_tx.send(CommandResponse{result}).is_err() {
                    // Caller went away, e.g. because an HTTP request timed out
                    tracing::info!("Dropping response to administrative command")
                }
            }
        }
    }

    Ok(())
}

struct WriteInProgress {
    barrier: Barrier,
}

struct WriteComplete {
    tenant_id: TenantId,
    completed_at: Instant,
}

/// The heatmap writer keeps a little bit of per-tenant state, mainly to remember
/// when we last did a write.  We only populate this after doing at least one
/// write for a tenant -- this avoids holding state for tenants that have
/// uploads disabled.

struct WriterTenantState {
    // This Weak only exists to enable culling IdleTenant instances
    // when the Tenant has been deallocated.
    tenant: Weak<Tenant>,

    last_write: Option<Instant>,
}

struct HeatmapWriter {
    tenant_manager: Arc<TenantManager>,
    remote_storage: GenericRemoteStorage,
    cancel: CancellationToken,

    tenants: HashMap<TenantId, WriterTenantState>,

    tenants_writing: HashMap<TenantId, WriteInProgress>,
    tasks: JoinSet<WriteComplete>,
    concurrent_writes: usize,
}

impl HeatmapWriter {
    /// Periodic execution phase: check for new work to do, and run it with `spawn_write`
    async fn iteration(&mut self) -> anyhow::Result<()> {
        self.drain().await;

        // Cull any entries in self.tenants whose Arc<Tenant> is gone
        self.tenants.retain(|_k, v| v.tenant.upgrade().is_some());

        // Cannot spawn more work right now
        if self.tenants_writing.len() >= self.concurrent_writes {
            return Ok(());
        }

        // Iterate over tenants looking for work to do.
        let tenants = self.tenant_manager.get_attached_tenants();
        for tenant in tenants {
            // Can't spawn any more work, drop out
            if self.tenants_writing.len() >= self.concurrent_writes {
                return Ok(());
            }

            // Process is shutting down, drop out
            if self.cancel.is_cancelled() {
                return Ok(());
            }

            // Skip tenants that don't have heatmaps enabled
            if !tenant.get_enable_heatmap() {
                continue;
            }

            // Skip tenants that aren't in a stable active state
            if tenant.current_state() != TenantState::Active {
                continue;
            }

            // Skip tenants that already have a write in flight
            if self.tenants_writing.contains_key(&tenant.get_tenant_id()) {
                continue;
            }

            // TODO: add a TenantConf for whether to upload at all.  This is useful for
            // a single-location mode for cheap tenants that don't require HA.

            // TODO: add a mechanism to check whether the active layer set has
            // changed since our last write

            self.maybe_spawn_write(tenant);
        }

        Ok(())
    }

    async fn drain(&mut self) {
        // Drain any complete background operations
        loop {
            tokio::select!(
                biased;
                Some(r) = self.tasks.join_next() => {
                    match r {
                        Ok(r) => {
                            self.on_completion(r);
                        },
                        Err(e) => {
                            // This should not happen, but needn't be fatal.
                            tracing::error!("Join error on heatmap writer JoinSet! {e}");
                        }
                    }
                }
                else => {
                    break;
                }
            )
        }
    }

    fn maybe_spawn_write(&mut self, tenant: Arc<Tenant>) {
        // Create an entry in self.tenants if one doesn't already exist: this will later be updated
        // with the completion time in on_completion.
        let state = self
            .tenants
            .entry(tenant.get_tenant_id())
            .or_insert_with(|| WriterTenantState {
                tenant: Arc::downgrade(&tenant),
                last_write: None,
            });

        // Decline to do the upload if insufficient time has passed
        if let Some(last_write) = state.last_write {
            if Instant::now().duration_since(last_write) < HEATMAP_UPLOAD_INTERVAL {
                return;
            }
        }

        self.spawn_write(tenant)
    }

    fn spawn_write(&mut self, tenant: Arc<Tenant>) {
        let remote_storage = self.remote_storage.clone();
        let tenant_id = tenant.get_tenant_id();
        let (completion, barrier) = utils::completion::channel();
        self.tasks.spawn(async move {
            // Guard for the barrier in [`WriteInProgress`]
            let _completion = completion;

            match write_tenant(remote_storage, &tenant)
                .instrument(tracing::info_span!(
                    "write_tenant",
                    tenant_id = %tenant.get_tenant_id()
                ))
                .await
            {
                Ok(()) => {}
                Err(e) => {
                    tracing::warn!(
                        "Failed to upload heatmap for tenant {}: {e:#}",
                        tenant.get_tenant_id(),
                    )
                }
            }

            WriteComplete {
                tenant_id: tenant.get_tenant_id(),
                completed_at: Instant::now(),
            }
        });

        self.tenants_writing
            .insert(tenant_id, WriteInProgress { barrier });
    }

    fn on_completion(&mut self, completion: WriteComplete) {
        tracing::debug!(tenant_id=%completion.tenant_id, "Heatmap write task complete");
        self.tenants_writing.remove(&completion.tenant_id);
        tracing::debug!("Task completed for tenant {}", completion.tenant_id);
        use std::collections::hash_map::Entry;
        match self.tenants.entry(completion.tenant_id) {
            Entry::Vacant(_) => {
                // Tenant state was dropped, nothing to update.
            }
            Entry::Occupied(mut entry) => {
                entry.get_mut().last_write = Some(completion.completed_at)
            }
        }
    }

    async fn handle_command(&mut self, command: UploadCommand) -> anyhow::Result<()> {
        match command {
            UploadCommand::Upload(tenant_id) => {
                // If an upload was ongoing for this tenant, let it finish first.
                if let Some(writing_state) = self.tenants_writing.get(&tenant_id) {
                    tracing::info!(%tenant_id, "Waiting for heatmap write to complete");
                    writing_state.barrier.clone().wait().await;
                }

                // Spawn the upload then immediately wait for it.  This will block processing of other commands and
                // starting of other background work.
                tracing::info!(%tenant_id, "Starting heatmap write on command");
                let tenant = mgr::get_tenant(tenant_id, true)?;
                self.spawn_write(tenant);
                let writing_state = self
                    .tenants_writing
                    .get(&tenant_id)
                    .expect("We just inserted this");
                tracing::info!(%tenant_id, "Waiting for heatmap write to complete");
                writing_state.barrier.clone().wait().await;
                tracing::info!(%tenant_id, "Heatmap write complete");

                // This drain is not necessary for correctness, but it is polite to avoid intentionally leaving
                // our complete task in self.tenants_writing.
                self.drain().await;

                Ok(())
            }
        }
    }
}

async fn write_tenant(
    remote_storage: GenericRemoteStorage,
    tenant: &Arc<Tenant>,
) -> anyhow::Result<()> {
    let mut heatmap = HeatMapTenant {
        timelines: Vec::new(),
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
            return Ok(());
        }
    };

    for (timeline_id, timeline) in timelines {
        let heatmap_timeline = timeline.generate_heatmap().await;
        match heatmap_timeline {
            None => {
                tracing::debug!(
                    "Skipping heatmap upload because timeline {timeline_id} is not ready"
                );
                return Ok(());
            }
            Some(heatmap_timeline) => {
                heatmap.timelines.push(heatmap_timeline);
            }
        }
    }

    // Serialize the heatmap
    let bytes = serde_json::to_vec(&heatmap)?;
    let size = bytes.len();

    let path = remote_heatmap_path(&tenant.get_tenant_id());

    // Write the heatmap.
    tracing::debug!("Uploading {size} byte heatmap to {path}");
    if let Err(e) = backoff::retry(
        || async {
            let bytes = tokio::io::BufReader::new(std::io::Cursor::new(bytes.clone()));
            let bytes = Box::new(bytes);
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
            return Ok(());
        } else {
            return Err(e);
        }
    }

    SECONDARY_MODE.upload_heatmap.inc();
    tracing::info!("Successfully uploaded {size} byte heatmap to {path}");

    Ok(())
}
