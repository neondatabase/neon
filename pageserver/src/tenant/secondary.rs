pub mod downloader;
pub mod heatmap;
pub mod heatmap_writer;

use std::{sync::Arc, time::SystemTime};

use crate::{
    config::PageServerConf,
    task_mgr::{self, TaskKind, BACKGROUND_RUNTIME},
};

use self::{
    downloader::{downloader_task, SecondaryDetail},
    heatmap_writer::heatmap_writer_task,
};

use super::{
    mgr::TenantManager,
    storage_layer::{AsLayerDesc, Layer},
    timeline::DiskUsageEvictionInfo,
};

use remote_storage::GenericRemoteStorage;

use tokio_util::sync::CancellationToken;
use utils::{
    completion::Barrier,
    fs_ext,
    id::{TenantId, TimelineId},
};

enum DownloadCommand {
    Download(TenantId),
}
enum UploadCommand {
    Upload(TenantId),
}

struct CommandRequest<T> {
    payload: T,
    response_tx: tokio::sync::oneshot::Sender<CommandResponse>,
}

struct CommandResponse {
    result: anyhow::Result<()>,
}

// Whereas [`Tenant`] represents an attached tenant, this type represents the work
// we do for secondary tenant locations: where we are not serving clients or
// ingesting WAL, but we are maintaining a warm cache of layer files.
//
// This type is all about the _download_ path for secondary mode.  The upload path
// runs while a regular attached `Tenant` exists.
//
// This structure coordinates TenantManager and SecondaryDownloader,
// so that the downloader can indicate which tenants it is currently
// operating on, and the manager can indicate when a particular
// secondary tenant should cancel any work in flight.
#[derive(Debug)]
pub(crate) struct SecondaryTenant {
    /// Cancellation token indicates to SecondaryDownloader that it should stop doing
    /// any work for this tenant at the next opportunity.
    pub(crate) cancel: CancellationToken,

    /// Lock must be held by SecondaryDownloader at any time that it might be operating
    /// on the local filesystem directory for this tenant ID.
    // Ordering: the TenantManager must set the cancellation token _before_
    // taking the lock.  The SecondaryDownloader must always check the cancellation
    // token immediately _after_ taking the lock (and at appropriate intervals
    // while holding it).
    pub(crate) busy: Arc<tokio::sync::Mutex<()>>,

    detail: std::sync::Mutex<SecondaryDetail>,
    // TODO: propagate the `warm` from LocationConf into here, and respect it when doing downloads
}

impl SecondaryTenant {
    pub(crate) fn new() -> Arc<Self> {
        // TODO; consider whether we really need to Arc this
        Arc::new(Self {
            busy: Arc::new(tokio::sync::Mutex::new(())),
            // todo: shall we make this a descendent of the
            // main cancellation token, or is it sufficient that
            // on shutdown we walk the tenants and fire their
            // individual cancellations?
            cancel: CancellationToken::new(),

            detail: std::sync::Mutex::default(),
        })
    }

    pub(crate) async fn shutdown(&self) {
        self.cancel.cancel();

        // Wait for any secondary downloader work to complete: once we
        // acquire this lock, we are guaranteed that the secondary downloader
        // won't touch the local filesystem again for this instance: it is safe
        // to e.g. construct a `Tenant` for the same TenantId
        drop(self.busy.lock().await);
    }

    pub(crate) fn get_layers_for_eviction(&self) -> Vec<(TimelineId, DiskUsageEvictionInfo)> {
        self.detail.lock().unwrap().get_layers_for_eviction()
    }

    pub(crate) async fn evict_layers(
        &self,
        _guard: tokio::sync::OwnedMutexGuard<()>,
        conf: &PageServerConf,
        tenant_id: &TenantId,
        layers: Vec<(TimelineId, Layer)>,
    ) {
        crate::tenant::debug_assert_current_span_has_tenant_and_timeline_id();

        if self.cancel.is_cancelled() {
            // Eviction is a no-op if shutdown() was already called.
            tracing::info!(
                "Dropping {} layer evictions, secondary tenant shutting down",
                layers.len()
            );
            return;
        }

        let now = SystemTime::now();

        for (timeline_id, layer) in layers {
            let layer_name = layer.layer_desc().filename();
            let path = conf
                .timeline_path(tenant_id, &timeline_id)
                .join(&layer_name.file_name());

            // We tolerate ENOENT, because between planning eviction and executing
            // it, the secondary downloader could have seen an updated heatmap that
            // resulted in a layer being deleted.
            tokio::fs::remove_file(path)
                .await
                .or_else(fs_ext::ignore_not_found)
                .expect("TODO: terminate process on local I/O errors");

            // TODO: batch up updates instead of acquiring lock in inner loop
            let mut detail = self.detail.lock().unwrap();
            // If there is no timeline detail for what we just deleted, that indicates that
            // the secondary downloader did some work (perhaps removing all)
            if let Some(timeline_detail) = detail.timelines.get_mut(&timeline_id) {
                timeline_detail.on_disk_layers.remove(&layer_name);
                timeline_detail.evicted_at.insert(layer_name, now);
            }
        }
    }
}

/// The SecondaryController is a pseudo-rpc client for administrative control of secondary mode downloads,
/// and heatmap uploads.  This is not a hot data path: it's primarily a hook for tests,
/// where we want to immediately upload/download for a particular tenant.  In normal operation
/// uploads & downloads are autonomous and not driven by this interface.
pub struct SecondaryController {
    upload_req_tx: tokio::sync::mpsc::Sender<CommandRequest<UploadCommand>>,

    download_req_tx: tokio::sync::mpsc::Sender<CommandRequest<DownloadCommand>>,
}

impl SecondaryController {
    async fn dispatch<T>(
        &self,
        queue: &tokio::sync::mpsc::Sender<CommandRequest<T>>,
        payload: T,
    ) -> anyhow::Result<()> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        queue
            .send(CommandRequest {
                payload,
                response_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("Receiver shut down"))?;

        let response = response_rx
            .await
            .map_err(|_| anyhow::anyhow!("Request dropped"))?;

        response.result
    }

    pub async fn download_tenant(&self, tenant_id: TenantId) -> anyhow::Result<()> {
        self.dispatch(&self.download_req_tx, DownloadCommand::Download(tenant_id))
            .await
    }

    pub async fn upload_tenant(&self, tenant_id: TenantId) -> anyhow::Result<()> {
        self.dispatch(&self.upload_req_tx, UploadCommand::Upload(tenant_id))
            .await
    }
}

pub fn spawn_tasks(
    conf: &'static PageServerConf,
    tenant_manager: Arc<TenantManager>,
    remote_storage: GenericRemoteStorage,
    background_jobs_can_start: Barrier,
    cancel: CancellationToken,
) -> SecondaryController {
    let mgr_clone = tenant_manager.clone();
    let storage_clone = remote_storage.clone();
    let cancel_clone = cancel.clone();
    let bg_jobs_clone = background_jobs_can_start.clone();

    let (download_req_tx, download_req_rx) =
        tokio::sync::mpsc::channel::<CommandRequest<DownloadCommand>>(16);
    let (upload_req_tx, upload_req_rx) =
        tokio::sync::mpsc::channel::<CommandRequest<UploadCommand>>(16);

    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::SecondaryDownloads,
        None,
        None,
        "secondary tenant downloads",
        false,
        async move {
            downloader_task(
                conf,
                mgr_clone,
                storage_clone,
                download_req_rx,
                bg_jobs_clone,
                cancel_clone,
            )
            .await
        },
    );

    task_mgr::spawn(
        BACKGROUND_RUNTIME.handle(),
        TaskKind::SecondaryDownloads,
        None,
        None,
        "heatmap uploads",
        false,
        async move {
            heatmap_writer_task(
                tenant_manager,
                remote_storage,
                upload_req_rx,
                background_jobs_can_start,
                cancel,
            )
            .await
        },
    );

    SecondaryController {
        download_req_tx,
        upload_req_tx,
    }
}

/// For running with remote storage disabled: a SecondaryController that is connected to nothing.
pub fn null_controller() -> SecondaryController {
    let (download_req_tx, _download_req_rx) =
        tokio::sync::mpsc::channel::<CommandRequest<DownloadCommand>>(16);
    let (upload_req_tx, _upload_req_rx) =
        tokio::sync::mpsc::channel::<CommandRequest<UploadCommand>>(16);
    SecondaryController {
        upload_req_tx,
        download_req_tx,
    }
}
