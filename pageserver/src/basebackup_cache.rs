use std::{collections::HashMap, sync::Arc, time::Duration};

use async_compression::tokio::write::GzipEncoder;
use camino::Utf8PathBuf;
use pageserver_api::{key, models::TenantState};
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
    tenant::mgr::{TenantManager, TenantSlot},
};

pub struct BasebackupPrepareRequest {
    pub tenant_shard_id: TenantShardId,
    pub timeline_id: TimelineId,
    pub lsn: Lsn,
}

pub type BasebackupPrepareSender = UnboundedSender<BasebackupPrepareRequest>;
pub type BasebackupPrepareReceiver = UnboundedReceiver<BasebackupPrepareRequest>;

pub struct BasebackupCache {
    data_dir: Utf8PathBuf,

    data_cache: Arc<tokio::sync::Mutex<HashMap<(TenantId, TimelineId), Lsn>>>,

    tenant_manager: Arc<TenantManager>,

    cleanup_interval: Duration,

    cancel: CancellationToken,
}

impl BasebackupCache {
    // Creates a BasebackupCache and spawns a background task.
    pub fn spawn(
        runtime_handle: &tokio::runtime::Handle,
        data_dir: Utf8PathBuf,
        prepare_receiver: BasebackupPrepareReceiver,
        tenant_manager: Arc<TenantManager>,
        cleanup_interval: Duration,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        let cache = Arc::new(BasebackupCache {
            data_dir,
            data_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            tenant_manager,
            cleanup_interval,
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
        let data_guard = self.data_cache.lock().await;

        if data_guard.get(&(tenant_id, timeline_id)) != Some(&lsn) {
            return None;
        }

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
        self.data_dir.join(format!(
            "basebackup_{tenant_id}_{timeline_id}_{:X}.tar.gz",
            lsn.0
        ))
    }

    async fn cleanup(&self) -> anyhow::Result<()> {
        let mut new_data_cache = HashMap::new();
        let mut data_guard = self.data_cache.lock().await;

        for (tenant_shard_id, tenant_slot) in self.tenant_manager.list() {
            let tenant_id = tenant_shard_id.tenant_id;
            if let TenantSlot::Attached(tenant) = tenant_slot {
                for timeline in tenant.list_timelines() {
                    if let Some(lsn) = data_guard.get(&(tenant_id, timeline.timeline_id)) {
                        // TODO(ephemeralsad) Check if LSN is still valid -> if not than remvoe it
                        new_data_cache.insert((tenant_id, timeline.timeline_id), *lsn);
                    }
                }
            }
        }

        for (&(tenant_id, timeline_id), &lsn) in data_guard.iter() {
            if !new_data_cache.contains_key(&(tenant_id, timeline_id)) {
                let path = self.entry_path(tenant_id, timeline_id, lsn);
                tokio::fs::remove_file(path).await?;
            }
        }

        std::mem::swap(&mut *data_guard, &mut new_data_cache);

        Ok(())
    }

    async fn background(self: Arc<Self>, mut prepare_receiver: BasebackupPrepareReceiver) {
        // Ensure the data_dir exists before processing requests
        if let Err(e) = tokio::fs::create_dir_all(&self.data_dir).await {
            tracing::error!(
                "Failed to create basebackup cache data_dir {:?}: {:?}",
                self.data_dir,
                e
            );
            return;
        }

        let mut ticker = tokio::time::interval(self.cleanup_interval);

        loop {
            tokio::select! {
                // Wait for a checkpoint shutdown message
                Some(req) = prepare_receiver.recv() => {
                    // Handle the checkpoint shutdown message
                    self.prepare_basebackup(req.tenant_shard_id, req.timeline_id, req.lsn).await.unwrap_or_else(|e| {
                        tracing::info!("Failed to prepare basebackup: {:?}", e);
                    });
                }
                // Wait for the ticker to trigger
                _ = ticker.tick() => {
                    // TODO(ephemeralsad) Run it in a separate thread
                    // Clean up irrelevant entries
                    self.cleanup().await.unwrap_or_else(|e| {
                        tracing::warn!("Failed to clean up basebackup cache: {:?}", e);
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
            )
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

        self.data_cache
            .lock()
            .await
            .insert((tenant_shard_id.tenant_id, timeline_id), lsn);

        Ok(())
    }
}
