use std::sync::Arc;

use anyhow::{Context, bail};
use importbucket_client::{ControlFile, RemoteStorageWrapper};
use pageserver_api::models::ShardImportStatus;
use remote_storage::RemotePath;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;
use utils::lsn::Lsn;
use utils::pausable_failpoint;
use utils::sync::gate::Gate;

use super::{Timeline, TimelineDeleteProgress};
use crate::context::RequestContext;
use crate::controller_upcall_client::{StorageControllerUpcallApi, StorageControllerUpcallClient};
use crate::tenant::metadata::TimelineMetadata;
use crate::tenant::timeline::layer_manager::LayerManagerLockHolder;

mod flow;
mod importbucket_client;
mod importbucket_format;
pub(crate) mod index_part_format;

pub struct ImportingTimeline {
    pub import_task_handle: JoinHandle<()>,
    pub import_task_gate: Gate,
    pub timeline: Arc<Timeline>,
    pub delete_progress: TimelineDeleteProgress,
}

impl std::fmt::Debug for ImportingTimeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ImportingTimeline<{}>", self.timeline.timeline_id)
    }
}

impl ImportingTimeline {
    pub async fn shutdown(&self) {
        self.import_task_handle.abort();
        self.import_task_gate.close().await;

        self.timeline.remote_client.shutdown().await;
    }
}

pub async fn doit(
    timeline: &Arc<Timeline>,
    index_part: index_part_format::Root,
    ctx: &RequestContext,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let index_part_format::Root::V1(v1) = index_part;
    let index_part_format::InProgress {
        location,
        idempotency_key: _,
        started_at: _,
    } = match v1 {
        index_part_format::V1::Done(_) => return Ok(()),
        index_part_format::V1::InProgress(in_progress) => in_progress,
    };

    let storcon_client = StorageControllerUpcallClient::new(timeline.conf, &cancel);

    let shard_status = storcon_client
        .get_timeline_import_status(
            timeline.tenant_shard_id,
            timeline.timeline_id,
            timeline.generation,
        )
        .await
        .map_err(|_err| anyhow::anyhow!("Shut down while getting timeline import status"))?;

    info!(?shard_status, "peeking shard status");
    match shard_status {
        ShardImportStatus::InProgress(maybe_progress) => {
            let storage =
                importbucket_client::new(timeline.conf, &location, cancel.clone()).await?;

            let control_file_res = if maybe_progress.is_none() {
                // Only prepare the import once when there's no progress.
                prepare_import(timeline, storage.clone(), &cancel).await
            } else {
                storage.get_control_file().await
            };

            let control_file = match control_file_res {
                Ok(cf) => cf,
                Err(err) => {
                    return Err(
                        terminate_flow_with_error(timeline, err, &storcon_client, &cancel).await,
                    );
                }
            };

            let res = flow::run(
                timeline.clone(),
                control_file,
                storage.clone(),
                maybe_progress,
                ctx,
            )
            .await;
            if let Err(err) = res {
                return Err(
                    terminate_flow_with_error(timeline, err, &storcon_client, &cancel).await,
                );
            }

            tracing::info!("Import plan executed. Flushing remote changes and notifying storcon");

            timeline
                .remote_client
                .schedule_index_upload_for_file_changes()?;
            timeline.remote_client.wait_completion().await?;

            pausable_failpoint!("import-timeline-pre-success-notify-pausable");

            // Communicate that shard is done.
            // Ensure at-least-once delivery of the upcall to storage controller
            // before we mark the task as done and never come here again.
            //
            // Note that we do not mark the import complete in the index part now.
            // This happens in [`Tenant::finalize_importing_timeline`] in response
            // to the storage controller calling
            // `/v1/tenant/:tenant_id/timeline/:timeline_id/activate_post_import`.
            storcon_client
                .put_timeline_import_status(
                    timeline.tenant_shard_id,
                    timeline.timeline_id,
                    timeline.generation,
                    ShardImportStatus::Done,
                )
                .await
                .map_err(|_err| {
                    anyhow::anyhow!("Shut down while putting timeline import status")
                })?;
        }
        ShardImportStatus::Error(err) => {
            info!(
                "shard status indicates that the shard is done (error), skipping import {}",
                err
            );
        }
        ShardImportStatus::Done => {
            info!("shard status indicates that the shard is done (success), skipping import");
        }
    }

    Ok(())
}

async fn prepare_import(
    timeline: &Arc<Timeline>,
    storage: RemoteStorageWrapper,
    cancel: &CancellationToken,
) -> anyhow::Result<ControlFile> {
    // Wipe the slate clean before starting the import as a precaution.
    // This method is only called when there's no recorded checkpoint for the import
    // in the storage controller.
    //
    // Note that this is split-brain safe (two imports for same timeline shards running in
    // different generations) because we go through the usual deletion path, including deletion queue.
    info!("wipe the slate clean");
    {
        // TODO: do we need to hold GC lock for this?
        let mut guard = timeline
            .layers
            .write(LayerManagerLockHolder::ImportPgData)
            .await;
        assert!(
            guard.layer_map()?.open_layer.is_none(),
            "while importing, there should be no in-memory layer" // this just seems like a good place to assert it
        );
        let all_layers_keys = guard.all_persistent_layers();
        let all_layers: Vec<_> = all_layers_keys
            .iter()
            .map(|key| guard.get_from_key(key))
            .collect();
        let open = guard.open_mut().context("open_mut")?;

        timeline.remote_client.schedule_gc_update(&all_layers)?;
        open.finish_gc_timeline(&all_layers);
    }

    //
    // Wait for pgdata to finish uploading
    //
    info!("wait for pgdata to reach status 'done'");
    let status_prefix = RemotePath::from_string("status").unwrap();
    let pgdata_status_key = status_prefix.join("pgdata");
    loop {
        let res = async {
            let pgdata_status: Option<importbucket_format::PgdataStatus> = storage
                .get_json(&pgdata_status_key)
                .await
                .context("get pgdata status")?;
            info!(?pgdata_status, "peeking pgdata status");
            if pgdata_status.map(|st| st.done).unwrap_or(false) {
                Ok(())
            } else {
                Err(anyhow::anyhow!("pgdata not done yet"))
            }
        }
        .await;
        match res {
            Ok(_) => break,
            Err(_err) => {
                info!("indefinitely waiting for pgdata to finish");
                if tokio::time::timeout(std::time::Duration::from_secs(10), cancel.cancelled())
                    .await
                    .is_ok()
                {
                    bail!("cancelled while waiting for pgdata");
                }
            }
        }
    }

    let control_file = storage.get_control_file().await?;
    let base_lsn = control_file.base_lsn();

    info!("update TimelineMetadata based on LSNs from control file");
    {
        let pg_version = control_file.pg_version();
        async move {
            // FIXME: The 'disk_consistent_lsn' should be the LSN at the *end* of the
            // checkpoint record, and prev_record_lsn should point to its beginning.
            // We should read the real end of the record from the WAL, but here we
            // just fake it.
            let disk_consistent_lsn = Lsn(base_lsn.0 + 8);
            let prev_record_lsn = base_lsn;
            let metadata = TimelineMetadata::new(
                disk_consistent_lsn,
                Some(prev_record_lsn),
                None,     // no ancestor
                Lsn(0),   // no ancestor lsn
                base_lsn, // latest_gc_cutoff_lsn
                base_lsn, // initdb_lsn
                pg_version,
            );

            let _start_lsn = disk_consistent_lsn + 1;

            timeline
                .remote_client
                .schedule_index_upload_for_full_metadata_update(&metadata)?;

            timeline.remote_client.wait_completion().await?;

            anyhow::Ok(())
        }
    }
    .await?;

    Ok(control_file)
}

async fn terminate_flow_with_error(
    timeline: &Arc<Timeline>,
    error: anyhow::Error,
    storcon_client: &StorageControllerUpcallClient,
    cancel: &CancellationToken,
) -> anyhow::Error {
    // The import task is a aborted on tenant shutdown, so in principle, it should
    // never be cancelled. To be on the safe side, check the cancellation tokens
    // before marking the import as failed.
    if !(cancel.is_cancelled() || timeline.cancel.is_cancelled()) {
        let notify_res = storcon_client
            .put_timeline_import_status(
                timeline.tenant_shard_id,
                timeline.timeline_id,
                timeline.generation,
                ShardImportStatus::Error(format!("{error:#}")),
            )
            .await;

        if let Err(_notify_error) = notify_res {
            // The [`StorageControllerUpcallClient::put_timeline_import_status`] retries
            // forever internally, so errors returned by it can only be due to cancellation.
            info!("failed to notify storcon about permanent import error");
        }

        // Will be logged by [`Tenant::create_timeline_import_pgdata_task`]
        error
    } else {
        anyhow::anyhow!("Import task cancelled")
    }
}
