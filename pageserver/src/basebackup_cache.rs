use std::{collections::HashMap, sync::Arc};

use async_compression::tokio::write::GzipEncoder;
use camino::Utf8PathBuf;
use pageserver_api::{config::BasebackupCacheConfig, models::TenantState};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tokio_util::sync::CancellationToken;
use utils::{
    id::{TenantId, TenantTimelineId, TimelineId},
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

type BasebackupRemoveEntrySender = UnboundedSender<Utf8PathBuf>;
type BasebackupRemoveEntryReceiver = UnboundedReceiver<Utf8PathBuf>;

pub struct BasebackupCache {
    data_dir: Utf8PathBuf,

    entries: tokio::sync::Mutex<HashMap<TenantTimelineId, Lsn>>,

    #[allow(dead_code)]
    config: BasebackupCacheConfig,

    tenant_manager: Arc<TenantManager>,

    remove_entry_sender: BasebackupRemoveEntrySender,

    cancel: CancellationToken,
}

impl BasebackupCache {
    // Creates a BasebackupCache and spawns a background task.
    pub fn spawn(
        runtime_handle: &tokio::runtime::Handle,
        data_dir: Utf8PathBuf,
        config: BasebackupCacheConfig,
        prepare_receiver: BasebackupPrepareReceiver,
        tenant_manager: Arc<TenantManager>,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        let (remove_entry_sender, remove_entry_receiver) = tokio::sync::mpsc::unbounded_channel();

        let cache = Arc::new(BasebackupCache {
            data_dir,
            entries: tokio::sync::Mutex::new(HashMap::new()),
            config,
            tenant_manager,
            remove_entry_sender,
            cancel,
        });

        runtime_handle.spawn(
            cache
                .clone()
                .background(prepare_receiver, remove_entry_receiver),
        );

        cache
    }

    // Non-blocking. If an entry exists, opens the archive file and return reader.
    pub async fn get(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> Option<tokio::fs::File> {
        let tti = TenantTimelineId::new(tenant_id, timeline_id);
        if self.entries.lock().await.get(&tti) != Some(&lsn) {
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
        self.data_dir
            .join(format!("basebackup_{tenant_id}_{timeline_id}_{:X}", lsn.0))
    }

    fn parse_entry_path(filename: &str) -> Option<(TenantId, TimelineId, Lsn)> {
        let parts: Vec<&str> = filename
            .strip_prefix("basebackup_")?
            .strip_suffix(".tar.gz")?
            .split('_')
            .collect();
        if parts.len() != 3 {
            return None;
        }
        let tenant_id = parts[0].parse::<TenantId>().ok()?;
        let timeline_id = parts[1].parse::<TimelineId>().ok()?;
        let lsn = Lsn(u64::from_str_radix(parts[2], 16).ok()?);

        Some((tenant_id, timeline_id, lsn))
    }

    async fn cleanup(&self) -> anyhow::Result<()> {
        let mut new_data_cache = HashMap::new();
        let mut data_guard = self.entries.lock().await;
        for (tenant_shard_id, tenant_slot) in self.tenant_manager.list() {
            let tenant_id = tenant_shard_id.tenant_id;
            if let TenantSlot::Attached(tenant) = tenant_slot {
                for timeline in tenant.list_timelines() {
                    let tti = TenantTimelineId::new(tenant_id, timeline.timeline_id);
                    if let Some(lsn) = data_guard.get(&tti) {
                        if timeline.get_last_record_lsn() == *lsn {
                            new_data_cache.insert(tti, *lsn);
                        }
                    }
                }
            }
        }

        for (&tti, &lsn) in data_guard.iter() {
            if !new_data_cache.contains_key(&tti) {
                self.remove_entry_sender
                    .send(self.entry_path(tti.tenant_id, tti.timeline_id, lsn))
                    .unwrap();
            }
        }

        std::mem::swap(&mut *data_guard, &mut new_data_cache);

        Ok(())
    }

    async fn on_startup(&self) -> anyhow::Result<()> {
        // Ensure the data_dir exists before processing requests
        tokio::fs::create_dir_all(&self.data_dir)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to create basebackup cache data_dir {:?}: {:?}",
                    self.data_dir,
                    e
                )
            })?;

        // Read existing entries from the data_dir and add them to the cache
        let mut dir_entries = tokio::fs::read_dir(&self.data_dir).await?;
        while let Some(entry) = dir_entries.next_entry().await? {
            let parsed = Self::parse_entry_path(entry.file_name().to_string_lossy().as_ref());
            if let Some((tenant_id, timeline_id, lsn)) = parsed {
                let tti = TenantTimelineId::new(tenant_id, timeline_id);
                self.entries.lock().await.insert(tti, lsn);
            } else {
                tracing::warn!(
                    "Invalid basebackup cache file name: {:?}",
                    entry.file_name()
                );
            }
        }

        Ok(())
    }

    async fn background(
        self: Arc<Self>,
        mut prepare_receiver: BasebackupPrepareReceiver,
        mut remove_entry_receiver: BasebackupRemoveEntryReceiver,
    ) {
        self.on_startup().await.unwrap_or_else(|e| {
            tracing::error!("Failed to start basebackup cache: {:?}", e);
        });

        let mut ticker = tokio::time::interval(self.config.background_cleanup_period);

        loop {
            tokio::select! {
                // Wait for a checkpoint shutdown message
                Some(req) = prepare_receiver.recv() => {
                    // Handle the checkpoint shutdown message
                    self.prepare_basebackup(req.tenant_shard_id, req.timeline_id, req.lsn).await.unwrap_or_else(|e| {
                        tracing::info!("Failed to prepare basebackup: {:?}", e);
                    });
                }
                Some(req) = remove_entry_receiver.recv() => {
                    // Handle the remove entry message
                    if let Err(e) = tokio::fs::remove_file(req).await {
                        tracing::warn!("Failed to remove basebackup cache file: {:?}", e);
                    }
                }
                // Wait for the ticker to trigger
                _ = ticker.tick() => {
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

        let tti = TenantTimelineId::new(tenant_shard_id.tenant_id, timeline_id);
        if let Some(old_value) = self.entries.lock().await.insert(tti, lsn) {
            self.remove_entry_sender
                .send(self.entry_path(tenant_shard_id.tenant_id, timeline_id, old_value))
                .unwrap();
        }

        Ok(())
    }
}
