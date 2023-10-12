use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::tenant::{
    mgr::TenantManager, remote_timeline_client::remote_heatmap_path, secondary::CommandResponse,
    Tenant,
};

use pageserver_api::models::TenantState;
use remote_storage::GenericRemoteStorage;

use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use utils::{backoff, completion::Barrier};

use super::{heatmap::HeatMapTenant, CommandRequest, UploadCommand};

const HEATMAP_UPLOAD_INTERVAL: Duration = Duration::from_millis(60000);

pub(super) async fn heatmap_writer_task(
    tenant_manager: Arc<TenantManager>,
    remote_storage: GenericRemoteStorage,
    mut command_queue: tokio::sync::mpsc::Receiver<CommandRequest<UploadCommand>>,
    background_jobs_can_start: Barrier,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let writer = HeatmapWriter {
        tenant_manager,
        remote_storage,
        cancel: cancel.clone(),
    };

    tracing::info!("Waiting for background_jobs_can start...");
    background_jobs_can_start.wait().await;
    tracing::info!("background_jobs_can is ready, proceeding.");

    while !cancel.is_cancelled() {
        writer.iteration().await?;

        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("Heatmap writer terminating");
                break;
            },
            _ = tokio::time::sleep(HEATMAP_UPLOAD_INTERVAL) => {},
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

struct HeatmapWriter {
    tenant_manager: Arc<TenantManager>,
    remote_storage: GenericRemoteStorage,
    cancel: CancellationToken,
}

impl HeatmapWriter {
    async fn iteration(&self) -> anyhow::Result<()> {
        let tenants = self.tenant_manager.get_attached_tenants();

        for tenant in tenants {
            if self.cancel.is_cancelled() {
                return Ok(());
            }

            if tenant.current_state() != TenantState::Active {
                continue;
            }

            // TODO: add a mechanism to check whether the active layer set has
            // changed since our last write

            // TODO: add a minimum time between uploads

            match self
                .write_tenant(&tenant)
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
        }

        Ok(())
    }

    async fn handle_command(&self, command: UploadCommand) -> anyhow::Result<()> {
        match command {
            UploadCommand::Upload(tenant_id) => {
                let tenants = self.tenant_manager.get_attached_tenants();

                let map = tenants
                    .iter()
                    .map(|t| (t.get_tenant_id(), t))
                    .collect::<HashMap<_, _>>();
                match map.get(&tenant_id) {
                    Some(tenant) => self.write_tenant(tenant).await,
                    None => {
                        anyhow::bail!("Tenant is not attached");
                    }
                }
            }
        }
    }

    async fn write_tenant(&self, tenant: &Arc<Tenant>) -> anyhow::Result<()> {
        let mut heatmap = HeatMapTenant {
            timelines: Vec::new(),
        };
        let timelines = tenant.timelines.lock().unwrap().clone();

        let tenant_cancel = tenant.cancel.clone();

        // Ensure that Tenant::shutdown waits for any upload in flight
        let _guard = {
            let hook = tenant.heatmap_hook.lock().unwrap();
            match hook.enter() {
                Some(g) => g,
                None => {
                    // Tenant is shutting down
                    tracing::info!("Skipping, tenant is shutting down");
                    return Ok(());
                }
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
                self.remote_storage
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

        tracing::info!("Successfully uploading {size} byte heatmap to {path}");

        Ok(())
    }
}
