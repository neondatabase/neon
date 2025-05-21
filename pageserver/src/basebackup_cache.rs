use std::sync::Arc;

use async_compression::tokio::write::GzipEncoder;
use camino::Utf8PathBuf;
use pageserver_api::{config::BasebackupCacheConfig, models::TenantState};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tokio_util::sync::CancellationToken;
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
    shard::TenantShardId,
};

use crate::{
    basebackup::send_basebackup_tarball,
    context::{DownloadBehavior, RequestContext},
    task_mgr::TaskKind,
    tenant::mgr::TenantManager,
};

pub struct BasebackupPrepareRequest {
    pub tenant_shard_id: TenantShardId,
    pub timeline_id: TimelineId,
    pub lsn: Lsn,
}

pub type BasebackupPrepareSender = UnboundedSender<BasebackupPrepareRequest>;
pub type BasebackupPrepareReceiver = UnboundedReceiver<BasebackupPrepareRequest>;

pub struct BasebackupCache {
    datadir: Utf8PathBuf,
    #[allow(dead_code)]
    config: BasebackupCacheConfig,

    tenant_manager: Arc<TenantManager>,

    cancel: CancellationToken,
}

impl BasebackupCache {
    // Creates a BasebackupCache and spawns a background task.
    pub fn spawn(
        runtime_handle: &tokio::runtime::Handle,
        datadir: Utf8PathBuf,
        config: BasebackupCacheConfig,
        prepare_receiver: BasebackupPrepareReceiver,
        tenant_manager: Arc<TenantManager>,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        let cache = Arc::new(BasebackupCache {
            datadir,
            config,
            tenant_manager,
            cancel,
        });

        runtime_handle.spawn(cache.clone().background(prepare_receiver));

        cache
    }

    // Non-blocking. If an entry exists, opens the archive file and return reader.
    pub async fn get(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> Option<tokio::fs::File> {
        // TODO(diko): add a fast check to avoid syscall every on every call.

        let path = self.entry_path(tenant_id, timeline_id, lsn);

        match tokio::fs::File::open(path).await {
            Ok(file) => Some(file),
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!("Unexpected error opening basebackup cache file: {:?}", e);
                }
                None
            }
        }
    }

    fn entry_path(&self, tenant_id: TenantId, timeline_id: TimelineId, lsn: Lsn) -> Utf8PathBuf {
        // Placeholder for actual implementation
        self.datadir.join(format!(
            "basebackup_{tenant_id}_{timeline_id}_{:X}.tar.gz",
            lsn.0
        ))
    }

    async fn background(self: Arc<Self>, mut prepare_receiver: BasebackupPrepareReceiver) {
        // Ensure the datadir exists before processing requests
        if let Err(e) = tokio::fs::create_dir_all(&self.datadir).await {
            tracing::error!(
                "Failed to create basebackup cache datadir {:?}: {:?}",
                self.datadir,
                e
            );
            return;
        }

        loop {
            tokio::select! {
                // Wait for a checkpoint shutdown message
                Some(req) = prepare_receiver.recv() => {
                    // Handle the checkpoint shutdown message
                    self.prepare_basebackup(req.tenant_shard_id, req.timeline_id, req.lsn).await.unwrap_or_else(|e| {
                        tracing::info!("Failed to prepare basebackup: {:?}", e);
                    });
                }
                // Check for cancellation
                _ = self.cancel.cancelled() => {
                    tracing::info!("BasebackupCache background task cancelled");
                    break;
                }
            }
        }
    }

    async fn prepare_basebackup(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> anyhow::Result<()> {
        // Placeholder for actual implementation
        tracing::info!(
            "Preparing basebackup for tenant: {:?}, timeline: {:?}, lsn: {:?}",
            tenant_shard_id.tenant_id,
            timeline_id,
            lsn
        );

        let tenant = self
            .tenant_manager
            .get_attached_tenant_shard(tenant_shard_id)?;

        let tenant_state = tenant.current_state();
        if tenant_state != TenantState::Active {
            anyhow::bail!(
                "Tenant {} is not active, current state: {:?}",
                tenant_shard_id.tenant_id,
                tenant_state
            );
        }

        let timeline = tenant.get_timeline(timeline_id, true)?;

        // TODO(diko): make it via RequstContextBuilder.
        let ctx = RequestContext::new(TaskKind::BasebackupCache, DownloadBehavior::Download);
        let ctx = &ctx.with_scope_timeline(&timeline);

        let path = self.entry_path(tenant_shard_id.tenant_id, timeline_id, lsn);
        let file = tokio::fs::File::create(&path).await?;
        let mut writer = BufWriter::new(file);

        let mut encoder = GzipEncoder::with_quality(
            &mut writer,
            // NOTE using fast compression because it's on the critical path
            //      for compute startup. For an empty database, we get
            //      <100KB with this method. The Level::Best compression method
            //      gives us <20KB, but maybe we should add basebackup caching
            //      on compute shutdown first.
            async_compression::Level::Best,
        );

        send_basebackup_tarball(&mut encoder, &timeline, Some(lsn), None, false, false, ctx)
            .await?;

        encoder.shutdown().await?;

        Ok(())
    }
}
