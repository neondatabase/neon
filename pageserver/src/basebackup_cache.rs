use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use async_compression::tokio::write::GzipEncoder;
use camino::{Utf8Path, Utf8PathBuf};
use metrics::core::{AtomicU64, GenericCounter};
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
    metrics::{BASEBACKUP_CACHE_ENTRIES, BASEBACKUP_CACHE_PREPARE, BASEBACKUP_CACHE_READ},
    task_mgr::TaskKind,
    tenant::{
        Timeline,
        mgr::{TenantManager, TenantSlot},
    },
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

/// BasebackupCache stores cached basebackup archives for timelines on local disk.
///
/// The main purpose of this cache is to speed up the startup process of compute nodes
/// after scaling to zero.
/// Thus, the basebackup is stored only for the latest LSN of the timeline and with
/// fixed set of parameters (gzip=true, full_backup=false, replica=false, prev_lsn=none).
///
/// The cache receives prepare requests through the `BasebackupPrepareSender` channel,
/// generates a basebackup from the timeline in the background, and stores it on disk.
///
/// Basebackup requests are pretty rare. We expect ~thousands of entries in the cache
/// and ~1 RPS for get requests.
pub struct BasebackupCache {
    data_dir: Utf8PathBuf,
    config: BasebackupCacheConfig,
    tenant_manager: Arc<TenantManager>,
    remove_entry_sender: BasebackupRemoveEntrySender,

    entries: std::sync::Mutex<HashMap<TenantTimelineId, Lsn>>,

    cancel: CancellationToken,

    read_hit_count: GenericCounter<AtomicU64>,
    read_miss_count: GenericCounter<AtomicU64>,
    read_err_count: GenericCounter<AtomicU64>,

    prepare_ok_count: GenericCounter<AtomicU64>,
    prepare_skip_count: GenericCounter<AtomicU64>,
    prepare_err_count: GenericCounter<AtomicU64>,
}

impl BasebackupCache {
    /// Creates a BasebackupCache and spawns the background task.
    /// The initialization of the cache is performed in the background and does not
    /// block the caller. The cache will return `None` for any get requests until
    /// initialization is complete.
    pub fn spawn(
        runtime_handle: &tokio::runtime::Handle,
        data_dir: Utf8PathBuf,
        config: Option<BasebackupCacheConfig>,
        prepare_receiver: BasebackupPrepareReceiver,
        tenant_manager: Arc<TenantManager>,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        let (remove_entry_sender, remove_entry_receiver) = tokio::sync::mpsc::unbounded_channel();

        let enabled = config.is_some();

        let cache = Arc::new(BasebackupCache {
            data_dir,
            config: config.unwrap_or_default(),
            tenant_manager,
            remove_entry_sender,

            entries: std::sync::Mutex::new(HashMap::new()),

            cancel,

            read_hit_count: BASEBACKUP_CACHE_READ.with_label_values(&["hit"]),
            read_miss_count: BASEBACKUP_CACHE_READ.with_label_values(&["miss"]),
            read_err_count: BASEBACKUP_CACHE_READ.with_label_values(&["error"]),

            prepare_ok_count: BASEBACKUP_CACHE_PREPARE.with_label_values(&["ok"]),
            prepare_skip_count: BASEBACKUP_CACHE_PREPARE.with_label_values(&["skip"]),
            prepare_err_count: BASEBACKUP_CACHE_PREPARE.with_label_values(&["error"]),
        });

        if enabled {
            runtime_handle.spawn(
                cache
                    .clone()
                    .background(prepare_receiver, remove_entry_receiver),
            );
        }

        cache
    }

    /// Gets a basebackup entry from the cache.
    /// If the entry is found, opens a file with the basebackup archive and returns it.
    /// The open file descriptor will prevent the file system from deleting the file
    /// even if the entry is removed from the cache in the background.
    pub async fn get(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> Option<tokio::fs::File> {
        // Fast path. Check if the entry exists using the in-memory state.
        let tti = TenantTimelineId::new(tenant_id, timeline_id);
        if self.entries.lock().unwrap().get(&tti) != Some(&lsn) {
            self.read_miss_count.inc();
            return None;
        }

        let path = self.entry_path(tenant_id, timeline_id, lsn);

        match tokio::fs::File::open(path).await {
            Ok(file) => {
                self.read_hit_count.inc();
                Some(file)
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    // We may end up here if the basebackup was concurrently removed by the cleanup task.
                    self.read_miss_count.inc();
                } else {
                    self.read_err_count.inc();
                    tracing::warn!("Unexpected error opening basebackup cache file: {:?}", e);
                }
                None
            }
        }
    }

    // Private methods.

    fn entry_filename(tenant_id: TenantId, timeline_id: TimelineId, lsn: Lsn) -> String {
        // The default format for LSN is 0/ABCDEF.
        // The backslash is not filename friendly, so serialize it as plain hex.
        let lsn = lsn.0;
        format!("basebackup_{tenant_id}_{timeline_id}_{lsn:016X}.tar.gz")
    }

    fn entry_path(&self, tenant_id: TenantId, timeline_id: TimelineId, lsn: Lsn) -> Utf8PathBuf {
        self.data_dir
            .join(Self::entry_filename(tenant_id, timeline_id, lsn))
    }

    fn tmp_dir(&self) -> Utf8PathBuf {
        self.data_dir.join("tmp")
    }

    fn entry_tmp_path(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> Utf8PathBuf {
        self.tmp_dir()
            .join(Self::entry_filename(tenant_id, timeline_id, lsn))
    }

    fn parse_entry_filename(filename: &str) -> Option<(TenantId, TimelineId, Lsn)> {
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

    // Recreate the tmp directory to clear all files in it.
    async fn recreate_tmp_dir(&self) -> anyhow::Result<()> {
        let tmp_dir = self.tmp_dir();
        if tmp_dir.exists() {
            tokio::fs::remove_dir_all(&tmp_dir).await?;
        }
        tokio::fs::create_dir_all(&tmp_dir).await?;
        Ok(())
    }

    async fn cleanup(&self) -> anyhow::Result<()> {
        self.recreate_tmp_dir().await?;

        // Remove outdated entries.
        let entries_old = self.entries.lock().unwrap().clone();
        let mut entries_new = HashMap::new();
        for (tenant_shard_id, tenant_slot) in self.tenant_manager.list() {
            if !tenant_shard_id.is_shard_zero() {
                continue;
            }
            let TenantSlot::Attached(tenant) = tenant_slot else {
                continue;
            };
            let tenant_id = tenant_shard_id.tenant_id;

            for timeline in tenant.list_timelines() {
                let tti = TenantTimelineId::new(tenant_id, timeline.timeline_id);
                if let Some(&entry_lsn) = entries_old.get(&tti) {
                    if timeline.get_last_record_lsn() <= entry_lsn {
                        entries_new.insert(tti, entry_lsn);
                    }
                }
            }
        }

        for (&tti, &lsn) in entries_old.iter() {
            if !entries_new.contains_key(&tti) {
                self.remove_entry_sender
                    .send(self.entry_path(tti.tenant_id, tti.timeline_id, lsn))
                    .unwrap();
            }
        }

        BASEBACKUP_CACHE_ENTRIES.set(entries_new.len() as i64);
        *self.entries.lock().unwrap() = entries_new;

        Ok(())
    }

    async fn on_startup(&self) -> anyhow::Result<()> {
        self.recreate_tmp_dir()
            .await
            .context("Failed to recreate tmp directory")?;

        // Read existing entries from the data_dir and add them to in-memory state.
        let mut entries = HashMap::new();
        let mut dir = tokio::fs::read_dir(&self.data_dir).await?;
        while let Some(dir_entry) = dir.next_entry().await? {
            let filename = dir_entry.file_name();

            if filename == "tmp" {
                // Skip the tmp directory.
                continue;
            }

            let parsed = Self::parse_entry_filename(filename.to_string_lossy().as_ref());
            let Some((tenant_id, timeline_id, lsn)) = parsed else {
                tracing::warn!("Invalid basebackup cache file name: {:?}", filename);
                continue;
            };

            let tti = TenantTimelineId::new(tenant_id, timeline_id);

            use std::collections::hash_map::Entry::*;

            match entries.entry(tti) {
                Occupied(mut entry) => {
                    let entry_lsn = *entry.get();
                    // Leave only the latest entry, remove the old one.
                    if lsn < entry_lsn {
                        self.remove_entry_sender.send(self.entry_path(
                            tenant_id,
                            timeline_id,
                            lsn,
                        ))?;
                    } else if lsn > entry_lsn {
                        self.remove_entry_sender.send(self.entry_path(
                            tenant_id,
                            timeline_id,
                            entry_lsn,
                        ))?;
                        entry.insert(lsn);
                    } else {
                        // Two different filenames parsed to the same timline_id and LSN.
                        // Should never happen.
                        return Err(anyhow::anyhow!(
                            "Duplicate basebackup cache entry with the same LSN: {:?}",
                            filename
                        ));
                    }
                }
                Vacant(entry) => {
                    entry.insert(lsn);
                }
            }
        }

        BASEBACKUP_CACHE_ENTRIES.set(entries.len() as i64);
        *self.entries.lock().unwrap() = entries;

        Ok(())
    }

    async fn background(
        self: Arc<Self>,
        mut prepare_receiver: BasebackupPrepareReceiver,
        mut remove_entry_receiver: BasebackupRemoveEntryReceiver,
    ) {
        // Panic in the background is a safe fallback.
        // It will drop receivers and the cache will be effectively disabled.
        self.on_startup()
            .await
            .expect("Failed to initialize basebackup cache");

        let mut cleanup_ticker = tokio::time::interval(self.config.cleanup_period);
        cleanup_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                Some(req) = prepare_receiver.recv() => {
                    if let Err(err) = self.prepare_basebackup(
                        req.tenant_shard_id,
                        req.timeline_id,
                        req.lsn,
                    ).await {
                        tracing::info!("Failed to prepare basebackup: {:#}", err);
                        self.prepare_err_count.inc();
                        continue;
                    }
                }
                Some(req) = remove_entry_receiver.recv() => {
                    if let Err(e) = tokio::fs::remove_file(req).await {
                        tracing::warn!("Failed to remove basebackup cache file: {:#}", e);
                    }
                }
                _ = cleanup_ticker.tick() => {
                    self.cleanup().await.unwrap_or_else(|e| {
                        tracing::warn!("Failed to clean up basebackup cache: {:#}", e);
                    });
                }
                _ = self.cancel.cancelled() => {
                    tracing::info!("BasebackupCache background task cancelled");
                    break;
                }
            }
        }
    }

    /// Prepare a basebackup for the given timeline.
    ///
    /// If the basebackup already exists with a higher LSN or the timeline already
    /// has a higher last_record_lsn, skip the preparation.
    ///
    /// The basebackup is prepared in a temporary directory and then moved to the final
    /// location to make the operation atomic.
    async fn prepare_basebackup(
        &self,
        tenant_shard_id: TenantShardId,
        timeline_id: TimelineId,
        req_lsn: Lsn,
    ) -> anyhow::Result<()> {
        tracing::info!(
            tenant_id = %tenant_shard_id.tenant_id,
            %timeline_id,
            %req_lsn,
            "Preparing basebackup for timeline",
        );

        let tti = TenantTimelineId::new(tenant_shard_id.tenant_id, timeline_id);

        {
            let entries = self.entries.lock().unwrap();
            if let Some(&entry_lsn) = entries.get(&tti) {
                if entry_lsn >= req_lsn {
                    tracing::info!(
                        %timeline_id,
                        %req_lsn,
                        %entry_lsn,
                        "Basebackup entry already exists for timeline with higher LSN, skipping basebackup",
                    );
                    self.prepare_skip_count.inc();
                    return Ok(());
                }
            }

            if entries.len() as i64 >= self.config.max_size_entries {
                tracing::info!(
                    %timeline_id,
                    %req_lsn,
                    "Basebackup cache is full, skipping basebackup",
                );
                self.prepare_skip_count.inc();
                return Ok(());
            }
        }

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

        let last_record_lsn = timeline.get_last_record_lsn();
        if last_record_lsn > req_lsn {
            tracing::info!(
                %timeline_id,
                %req_lsn,
                %last_record_lsn,
                "Timeline has a higher LSN than the requested one, skipping basebackup",
            );
            self.prepare_skip_count.inc();
            return Ok(());
        }

        let entry_tmp_path = self.entry_tmp_path(tenant_shard_id.tenant_id, timeline_id, req_lsn);

        let res = self
            .prepare_basebackup_tmp(&entry_tmp_path, &timeline, req_lsn)
            .await;

        if let Err(err) = res {
            tracing::info!("Failed to prepare basebackup tmp file: {:#}", err);
            // Try to clean up tmp file. If we fail, the background clean up task will take care of it.
            match tokio::fs::remove_file(&entry_tmp_path).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::info!("Failed to remove basebackup tmp file: {:?}", e);
                }
            }
            return Err(err);
        }

        // Move the tmp file to the final location atomically.
        // The tmp file is fsynced, so it's guaranteed that we will not have a partial file
        // in the main directory.
        // It's not necessary to fsync the inode after renaming, because the worst case is that
        // the rename operation will be rolled back on the disk failure, the entry will disappear
        // from the main directory, and the entry access will cause a cache miss.
        let entry_path = self.entry_path(tenant_shard_id.tenant_id, timeline_id, req_lsn);
        tokio::fs::rename(&entry_tmp_path, &entry_path).await?;

        let mut entries = self.entries.lock().unwrap();
        if let Some(old_lsn) = entries.insert(tti, req_lsn) {
            // Remove the old entry if it exists.
            self.remove_entry_sender
                .send(self.entry_path(tenant_shard_id.tenant_id, timeline_id, old_lsn))
                .unwrap();
        }
        BASEBACKUP_CACHE_ENTRIES.set(entries.len() as i64);

        self.prepare_ok_count.inc();
        Ok(())
    }

    /// Prepares a basebackup in a temporary file.
    /// Guarantees that the tmp file is fsynced before returning.
    async fn prepare_basebackup_tmp(
        &self,
        entry_tmp_path: &Utf8Path,
        timeline: &Arc<Timeline>,
        req_lsn: Lsn,
    ) -> anyhow::Result<()> {
        let ctx = RequestContext::new(TaskKind::BasebackupCache, DownloadBehavior::Download);
        let ctx = ctx.with_scope_timeline(timeline);

        let file = tokio::fs::File::create(entry_tmp_path).await?;
        let mut writer = BufWriter::new(file);

        let mut encoder = GzipEncoder::with_quality(
            &mut writer,
            // Level::Best because compression is not on the hot path of basebackup requests.
            // The decompression is almost not affected by the compression level.
            async_compression::Level::Best,
        );

        // We may receive a request before the WAL record is applied to the timeline.
        // Wait for the requested LSN to be applied.
        timeline
            .wait_lsn(
                req_lsn,
                crate::tenant::timeline::WaitLsnWaiter::BaseBackupCache,
                crate::tenant::timeline::WaitLsnTimeout::Default,
                &ctx,
            )
            .await?;

        send_basebackup_tarball(
            &mut encoder,
            timeline,
            Some(req_lsn),
            None,
            false,
            false,
            &ctx,
        )
        .await?;

        encoder.shutdown().await?;
        writer.flush().await?;
        writer.into_inner().sync_all().await?;

        Ok(())
    }
}
