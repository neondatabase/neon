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
    metrics::{
        BASEBACKUP_CACHE_ENTRIES, BASEBACKUP_CACHE_PREPARE, BASEBACKUP_CACHE_READ,
        BASEBACKUP_CACHE_SIZE,
    },
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

#[derive(Clone)]
struct CacheEntry {
    /// LSN at which the basebackup was taken.
    lsn: Lsn,
    /// Size of the basebackup archive in bytes.
    size_bytes: u64,
}

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

    entries: std::sync::Mutex<HashMap<TenantTimelineId, CacheEntry>>,

    read_hit_count: GenericCounter<AtomicU64>,
    read_miss_count: GenericCounter<AtomicU64>,
    read_err_count: GenericCounter<AtomicU64>,
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
        let cache = Arc::new(BasebackupCache {
            data_dir,

            entries: std::sync::Mutex::new(HashMap::new()),

            read_hit_count: BASEBACKUP_CACHE_READ.with_label_values(&["hit"]),
            read_miss_count: BASEBACKUP_CACHE_READ.with_label_values(&["miss"]),
            read_err_count: BASEBACKUP_CACHE_READ.with_label_values(&["error"]),
        });

        if let Some(config) = config {
            let background = BackgroundTask {
                c: cache.clone(),

                config,
                tenant_manager,
                cancel,

                entry_count: 0,
                total_size_bytes: 0,

                prepare_ok_count: BASEBACKUP_CACHE_PREPARE.with_label_values(&["ok"]),
                prepare_skip_count: BASEBACKUP_CACHE_PREPARE.with_label_values(&["skip"]),
                prepare_err_count: BASEBACKUP_CACHE_PREPARE.with_label_values(&["error"]),
            };
            runtime_handle.spawn(background.run(prepare_receiver));
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
        if self.entries.lock().unwrap().get(&tti).map(|e| e.lsn) != Some(lsn) {
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
}

/// The background task that does the job to prepare basebackups
/// and manage the cache entries on disk.
/// It is a separate struct from BasebackupCache to allow holding
/// a mutable reference to this state without a mutex lock,
/// while BasebackupCache is referenced by the clients.
struct BackgroundTask {
    c: Arc<BasebackupCache>,

    config: BasebackupCacheConfig,
    tenant_manager: Arc<TenantManager>,
    cancel: CancellationToken,

    /// Number of the entries in the cache.
    /// This counter is used for metrics and applying cache limits.
    /// It generally should be equal to c.entries.len(), but it's calculated
    /// pessimistically for abnormal situations: if we encountered some errors
    /// during removing the entry from disk, we won't decrement this counter to
    /// make sure that we don't exceed the limit with "trashed" files on the disk.
    /// It will also count files in the data_dir that are not valid cache entries.
    entry_count: usize,
    /// Total size of all the entries on the disk.
    /// This counter is used for metrics and applying cache limits.
    /// Similar to entry_count, it is calculated pessimistically for abnormal situations.
    total_size_bytes: u64,

    prepare_ok_count: GenericCounter<AtomicU64>,
    prepare_skip_count: GenericCounter<AtomicU64>,
    prepare_err_count: GenericCounter<AtomicU64>,
}

impl BackgroundTask {
    fn tmp_dir(&self) -> Utf8PathBuf {
        self.c.data_dir.join("tmp")
    }

    fn entry_tmp_path(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        lsn: Lsn,
    ) -> Utf8PathBuf {
        self.tmp_dir()
            .join(BasebackupCache::entry_filename(tenant_id, timeline_id, lsn))
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
    async fn clean_tmp_dir(&self) -> anyhow::Result<()> {
        let tmp_dir = self.tmp_dir();
        if tmp_dir.exists() {
            tokio::fs::remove_dir_all(&tmp_dir).await?;
        }
        tokio::fs::create_dir_all(&tmp_dir).await?;
        Ok(())
    }

    async fn cleanup(&mut self) -> anyhow::Result<()> {
        self.clean_tmp_dir().await?;

        // Leave only up-to-date entries.
        let entries_old = self.c.entries.lock().unwrap().clone();
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
                if let Some(entry) = entries_old.get(&tti) {
                    if timeline.get_last_record_lsn() <= entry.lsn {
                        entries_new.insert(tti, entry.clone());
                    }
                }
            }
        }

        // Try to remove all entries that are not up-to-date.
        for (&tti, entry) in entries_old.iter() {
            if !entries_new.contains_key(&tti) {
                self.try_remove_entry(tti.tenant_id, tti.timeline_id, entry)
                    .await;
            }
        }

        // Note: BackgroundTask is the only writer for self.c.entries,
        // so it couldn't have been modified concurrently.
        *self.c.entries.lock().unwrap() = entries_new;

        Ok(())
    }

    async fn on_startup(&mut self) -> anyhow::Result<()> {
        // Create data_dir if it does not exist.
        tokio::fs::create_dir_all(&self.c.data_dir)
            .await
            .context("Failed to create basebackup cache data directory")?;

        self.clean_tmp_dir()
            .await
            .context("Failed to clean tmp directory")?;

        // Read existing entries from the data_dir and add them to in-memory state.
        let mut entries = HashMap::<TenantTimelineId, CacheEntry>::new();
        let mut dir = tokio::fs::read_dir(&self.c.data_dir).await?;
        while let Some(dir_entry) = dir.next_entry().await? {
            let filename = dir_entry.file_name();

            if filename == "tmp" {
                // Skip the tmp directory.
                continue;
            }

            let size_bytes = dir_entry
                .metadata()
                .await
                .map_err(|e| {
                    anyhow::anyhow!("Failed to read metadata for file {:?}: {:?}", filename, e)
                })?
                .len();

            self.entry_count += 1;
            BASEBACKUP_CACHE_ENTRIES.set(self.entry_count as u64);

            self.total_size_bytes += size_bytes;
            BASEBACKUP_CACHE_SIZE.set(self.total_size_bytes);

            let parsed = Self::parse_entry_filename(filename.to_string_lossy().as_ref());
            let Some((tenant_id, timeline_id, lsn)) = parsed else {
                tracing::warn!("Invalid basebackup cache file name: {:?}", filename);
                continue;
            };

            let cur_entry = CacheEntry { lsn, size_bytes };

            let tti = TenantTimelineId::new(tenant_id, timeline_id);

            use std::collections::hash_map::Entry::*;

            match entries.entry(tti) {
                Occupied(mut entry) => {
                    let found_entry = entry.get();
                    // Leave only the latest entry, remove the old one.
                    if cur_entry.lsn < found_entry.lsn {
                        self.try_remove_entry(tenant_id, timeline_id, &cur_entry)
                            .await;
                    } else if cur_entry.lsn > found_entry.lsn {
                        self.try_remove_entry(tenant_id, timeline_id, found_entry)
                            .await;
                        entry.insert(cur_entry);
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
                    entry.insert(cur_entry);
                }
            }
        }

        *self.c.entries.lock().unwrap() = entries;

        Ok(())
    }

    async fn run(mut self, mut prepare_receiver: BasebackupPrepareReceiver) {
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

    /// Try to remove an entry from disk.
    /// The caller is responsible for removing the entry from the in-memory state.
    /// Updates size counters and corresponding metrics.
    /// Ignores the filesystem errors as not-so-important, but the size counters
    /// are not decremented in this case, so the file will continue to be counted
    /// towards the size limits.
    async fn try_remove_entry(
        &mut self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        entry: &CacheEntry,
    ) {
        let entry_path = self.c.entry_path(tenant_id, timeline_id, entry.lsn);

        match tokio::fs::remove_file(&entry_path).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                tracing::warn!(
                    "Failed to remove basebackup cache file for tenant {} timeline {} LSN {}: {:#}",
                    tenant_id,
                    timeline_id,
                    entry.lsn,
                    e
                );
                return;
            }
        }

        self.entry_count -= 1;
        BASEBACKUP_CACHE_ENTRIES.set(self.entry_count as u64);

        self.total_size_bytes -= entry.size_bytes;
        BASEBACKUP_CACHE_SIZE.set(self.total_size_bytes);
    }

    /// Insert the cache entry into in-memory state and update the size counters.
    /// Assumes that the file for the entry already exists on disk.
    /// If the entry already exists with previous LSN, it will be removed.
    async fn upsert_entry(
        &mut self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        entry: CacheEntry,
    ) {
        let tti = TenantTimelineId::new(tenant_id, timeline_id);

        self.entry_count += 1;
        BASEBACKUP_CACHE_ENTRIES.set(self.entry_count as u64);

        self.total_size_bytes += entry.size_bytes;
        BASEBACKUP_CACHE_SIZE.set(self.total_size_bytes);

        let old_entry = self.c.entries.lock().unwrap().insert(tti, entry);

        if let Some(old_entry) = old_entry {
            self.try_remove_entry(tenant_id, timeline_id, &old_entry)
                .await;
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
        &mut self,
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

        // TODO(diko): I don't think we will hit the limit,
        // but if we do, it makes sense to try to evict oldest entries. here
        if self.entry_count >= self.config.max_size_entries {
            tracing::info!(
                %tenant_shard_id,
                %timeline_id,
                %req_lsn,
                "Basebackup cache is full (max_size_entries), skipping basebackup",
            );
            self.prepare_skip_count.inc();
            return Ok(());
        }

        if self.total_size_bytes >= self.config.max_total_size_bytes {
            tracing::info!(
                %tenant_shard_id,
                %timeline_id,
                %req_lsn,
                "Basebackup cache is full (max_total_size_bytes), skipping basebackup",
            );
            self.prepare_skip_count.inc();
            return Ok(());
        }

        {
            let entries = self.c.entries.lock().unwrap();
            if let Some(entry) = entries.get(&tti) {
                if entry.lsn >= req_lsn {
                    tracing::info!(
                        %timeline_id,
                        %req_lsn,
                        %entry.lsn,
                        "Basebackup entry already exists for timeline with higher LSN, skipping basebackup",
                    );
                    self.prepare_skip_count.inc();
                    return Ok(());
                }
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

        let entry = match res {
            Ok(entry) => entry,
            Err(err) => {
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
        };

        // Move the tmp file to the final location atomically.
        // The tmp file is fsynced, so it's guaranteed that we will not have a partial file
        // in the main directory.
        // It's not necessary to fsync the inode after renaming, because the worst case is that
        // the rename operation will be rolled back on the disk failure, the entry will disappear
        // from the main directory, and the entry access will cause a cache miss.
        let entry_path = self
            .c
            .entry_path(tenant_shard_id.tenant_id, timeline_id, req_lsn);
        tokio::fs::rename(&entry_tmp_path, &entry_path).await?;

        self.upsert_entry(tenant_shard_id.tenant_id, timeline_id, entry)
            .await;

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
    ) -> anyhow::Result<CacheEntry> {
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

        // TODO(diko): we can count it via Writer wrapper instead of a syscall.
        let size_bytes = tokio::fs::metadata(entry_tmp_path).await?.len();

        Ok(CacheEntry {
            lsn: req_lsn,
            size_bytes,
        })
    }
}
