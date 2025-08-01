use std::cmp::min;
use std::collections::HashSet;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use futures::StreamExt;
use futures::stream::{self, FuturesOrdered};
use postgres_ffi::v14::xlog_utils::XLogSegNoOffsetToRecPtr;
use postgres_ffi::{PG_TLI, XLogFileName, XLogSegNo};
use remote_storage::{
    DownloadError, DownloadOpts, GenericRemoteStorage, ListingMode, RemotePath, StorageMetadata,
};
use safekeeper_api::models::PeerInfo;
use tokio::fs::File;
use tokio::select;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::*;
use utils::id::{NodeId, TenantTimelineId};
use utils::lsn::Lsn;
use utils::{backoff, pausable_failpoint};

use crate::metrics::{
    BACKED_UP_SEGMENTS, BACKUP_ERRORS, BACKUP_REELECT_LEADER_COUNT, WAL_BACKUP_TASKS,
};
use crate::timeline::WalResidentTimeline;
use crate::timeline_manager::{Manager, StateSnapshot};
use crate::{SafeKeeperConf, WAL_BACKUP_RUNTIME};

const UPLOAD_FAILURE_RETRY_MIN_MS: u64 = 10;
const UPLOAD_FAILURE_RETRY_MAX_MS: u64 = 5000;

/// Default buffer size when interfacing with [`tokio::fs::File`].
const BUFFER_SIZE: usize = 32 * 1024;

pub struct WalBackupTaskHandle {
    shutdown_tx: Sender<()>,
    handle: JoinHandle<()>,
}

impl WalBackupTaskHandle {
    pub(crate) async fn join(self) {
        if let Err(e) = self.handle.await {
            error!("WAL backup task panicked: {}", e);
        }
    }
}

/// Do we have anything to upload to S3, i.e. should safekeepers run backup activity?
pub(crate) fn is_wal_backup_required(
    wal_seg_size: usize,
    num_computes: usize,
    state: &StateSnapshot,
) -> bool {
    num_computes > 0 ||
    // Currently only the whole segment is offloaded, so compare segment numbers.
    (state.commit_lsn.segment_number(wal_seg_size) > state.backup_lsn.segment_number(wal_seg_size))
}

/// Based on peer information determine which safekeeper should offload; if it
/// is me, run (per timeline) task, if not yet. OTOH, if it is not me and task
/// is running, kill it.
pub(crate) async fn update_task(
    mgr: &mut Manager,
    storage: Arc<GenericRemoteStorage>,
    need_backup: bool,
    state: &StateSnapshot,
) {
    /* BEGIN_HADRON */
    let (offloader, election_dbg_str) = hadron_determine_offloader(mgr, state);
    /* END_HADRON */
    let elected_me = Some(mgr.conf.my_id) == offloader;

    let should_task_run = need_backup && elected_me;

    // start or stop the task
    if should_task_run != (mgr.backup_task.is_some()) {
        if should_task_run {
            info!("elected for backup: {}", election_dbg_str);

            let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

            let Ok(resident) = mgr.wal_resident_timeline() else {
                info!("Timeline shut down");
                return;
            };

            let async_task = backup_task_main(
                resident,
                storage,
                mgr.conf.backup_parallel_jobs,
                shutdown_rx,
            );

            let handle = if mgr.conf.current_thread_runtime {
                tokio::spawn(async_task)
            } else {
                WAL_BACKUP_RUNTIME.spawn(async_task)
            };

            mgr.backup_task = Some(WalBackupTaskHandle {
                shutdown_tx,
                handle,
            });
        } else {
            if !need_backup {
                // don't need backup at all
                info!("stepping down from backup, need_backup={}", need_backup);
            } else {
                // someone else has been elected
                info!("stepping down from backup: {}", election_dbg_str);
            }
            shut_down_task(&mut mgr.backup_task).await;
        }
    }
}

async fn shut_down_task(entry: &mut Option<WalBackupTaskHandle>) {
    if let Some(wb_handle) = entry.take() {
        // Tell the task to shutdown. Error means task exited earlier, that's ok.
        let _ = wb_handle.shutdown_tx.send(()).await;
        // Await the task itself. TODO: restart panicked tasks earlier.
        wb_handle.join().await;
    }
}

/* BEGIN_HADRON */
// On top of the neon determine_offloader, we also check if the current offloader is lagging behind too much.
// If it is, we re-elect a new offloader. This mitigates the below issue. It also helps distribute the load across SKs.
//
// We observe that the offloader fails to upload a segment due to race conditions on XLOG SWITCH and PG start streaming WALs.
// wal_backup task continously failing to upload a full segment while the segment remains partial on the disk.
// The consequence is that commit_lsn for all SKs move forward but backup_lsn stays the same. Then, all SKs run out of disk space.
// See go/sk-ood-xlog-switch for more details.
//
// To mitigate this issue, we will re-elect a new offloader if the current offloader is lagging behind too much.
// Each SK makes the decision locally but they are aware of each other's commit and backup lsns.
//
// determine_offloader will pick a SK. say SK-1.
// Each SK checks
// -- if commit_lsn - back_lsn > threshold,
// -- -- remove SK-1 from the candidate and call determine_offloader again.
// SK-1 will step down and all SKs will elect the same leader again.
// After the backup is caught up, the leader will become SK-1 again.
fn hadron_determine_offloader(mgr: &Manager, state: &StateSnapshot) -> (Option<NodeId>, String) {
    let mut offloader: Option<NodeId>;
    let mut election_dbg_str: String;
    let caughtup_peers_count: usize;
    (offloader, election_dbg_str, caughtup_peers_count) =
        determine_offloader(&state.peers, state.backup_lsn, mgr.tli.ttid, &mgr.conf);

    if offloader.is_none()
        || caughtup_peers_count <= 1
        || mgr.conf.max_reelect_offloader_lag_bytes == 0
    {
        return (offloader, election_dbg_str);
    }

    let offloader_sk_id = offloader.unwrap();

    let backup_lag = state.commit_lsn.checked_sub(state.backup_lsn);
    if backup_lag.is_none() {
        debug!("Backup lag is None. Skipping re-election.");
        return (offloader, election_dbg_str);
    }

    let backup_lag = backup_lag.unwrap().0;

    if backup_lag < mgr.conf.max_reelect_offloader_lag_bytes {
        return (offloader, election_dbg_str);
    }

    info!(
        "Electing a new leader: Backup lag is too high backup lsn lag {} threshold {}: {}",
        backup_lag, mgr.conf.max_reelect_offloader_lag_bytes, election_dbg_str
    );
    BACKUP_REELECT_LEADER_COUNT.inc();
    // Remove the current offloader if lag is too high.
    let new_peers: Vec<_> = state
        .peers
        .iter()
        .filter(|p| p.sk_id != offloader_sk_id)
        .cloned()
        .collect();
    (offloader, election_dbg_str, _) =
        determine_offloader(&new_peers, state.backup_lsn, mgr.tli.ttid, &mgr.conf);
    (offloader, election_dbg_str)
}
/* END_HADRON */

/// The goal is to ensure that normally only one safekeepers offloads. However,
/// it is fine (and inevitable, as s3 doesn't provide CAS) that for some short
/// time we have several ones as they PUT the same files. Also,
/// - frequently changing the offloader would be bad;
/// - electing seriously lagging safekeeper is undesirable;
///
/// So we deterministically choose among the reasonably caught up candidates.
/// TODO: take into account failed attempts to deal with hypothetical situation
/// where s3 is unreachable only for some sks.
fn determine_offloader(
    alive_peers: &[PeerInfo],
    wal_backup_lsn: Lsn,
    ttid: TenantTimelineId,
    conf: &SafeKeeperConf,
) -> (Option<NodeId>, String, usize) {
    // TODO: remove this once we fill newly joined safekeepers since backup_lsn.
    let capable_peers = alive_peers
        .iter()
        .filter(|p| p.local_start_lsn <= wal_backup_lsn);
    match capable_peers.clone().map(|p| p.commit_lsn).max() {
        None => (None, "no connected peers to elect from".to_string(), 0),
        Some(max_commit_lsn) => {
            let threshold = max_commit_lsn
                .checked_sub(conf.max_offloader_lag_bytes)
                .unwrap_or(Lsn(0));
            let mut caughtup_peers = capable_peers
                .clone()
                .filter(|p| p.commit_lsn >= threshold)
                .collect::<Vec<_>>();
            caughtup_peers.sort_by(|p1, p2| p1.sk_id.cmp(&p2.sk_id));

            // To distribute the load, shift by timeline_id.
            let offloader = caughtup_peers
                [(u128::from(ttid.timeline_id) % caughtup_peers.len() as u128) as usize]
                .sk_id;

            let mut capable_peers_dbg = capable_peers
                .map(|p| (p.sk_id, p.commit_lsn))
                .collect::<Vec<_>>();
            capable_peers_dbg.sort_by(|p1, p2| p1.0.cmp(&p2.0));
            (
                Some(offloader),
                format!(
                    "elected {} among {:?} peers, with {} of them being caughtup",
                    offloader,
                    capable_peers_dbg,
                    caughtup_peers.len()
                ),
                caughtup_peers.len(),
            )
        }
    }
}

pub struct WalBackup {
    storage: Option<Arc<GenericRemoteStorage>>,
}

impl WalBackup {
    /// Create a new WalBackup instance.
    pub async fn new(conf: &SafeKeeperConf) -> Result<Self> {
        if !conf.wal_backup_enabled {
            return Ok(Self { storage: None });
        }

        match conf.remote_storage.as_ref() {
            Some(config) => {
                let storage = GenericRemoteStorage::from_config(config).await?;
                Ok(Self {
                    storage: Some(Arc::new(storage)),
                })
            }
            None => Ok(Self { storage: None }),
        }
    }

    pub fn get_storage(&self) -> Option<Arc<GenericRemoteStorage>> {
        self.storage.clone()
    }
}

struct WalBackupTask {
    timeline: WalResidentTimeline,
    timeline_dir: Utf8PathBuf,
    wal_seg_size: usize,
    parallel_jobs: usize,
    commit_lsn_watch_rx: watch::Receiver<Lsn>,
    storage: Arc<GenericRemoteStorage>,
}

/// Offload single timeline.
#[instrument(name = "wal_backup", skip_all, fields(ttid = %tli.ttid))]
async fn backup_task_main(
    tli: WalResidentTimeline,
    storage: Arc<GenericRemoteStorage>,
    parallel_jobs: usize,
    mut shutdown_rx: Receiver<()>,
) {
    let _guard = WAL_BACKUP_TASKS.guard();
    info!("started");

    let cancel = tli.tli.cancel.clone();
    let mut wb = WalBackupTask {
        wal_seg_size: tli.get_wal_seg_size().await,
        commit_lsn_watch_rx: tli.get_commit_lsn_watch_rx(),
        timeline_dir: tli.get_timeline_dir(),
        timeline: tli,
        parallel_jobs,
        storage,
    };

    // task is spinned up only when wal_seg_size already initialized
    assert!(wb.wal_seg_size > 0);

    let mut canceled = false;
    select! {
        _ = wb.run() => {}
        _ = shutdown_rx.recv() => {
            canceled = true;
        },
        _ = cancel.cancelled() => {
            canceled = true;
        }
    }
    info!("task {}", if canceled { "canceled" } else { "terminated" });
}

impl WalBackupTask {
    /// This function must be called from a select! that also respects self.timeline's
    /// cancellation token.  This is done in [`backup_task_main`].
    ///
    /// The future returned by this function is safe to drop at any time because it
    /// does not write to local disk.
    async fn run(&mut self) {
        let mut backup_lsn = Lsn(0);

        let mut retry_attempt = 0u32;
        // offload loop
        while !self.timeline.cancel.is_cancelled() {
            if retry_attempt == 0 {
                // wait for new WAL to arrive
                if let Err(e) = self.commit_lsn_watch_rx.changed().await {
                    // should never happen, as we hold Arc to timeline and transmitter's lifetime
                    // is within Timeline's
                    error!("commit_lsn watch shut down: {:?}", e);
                    return;
                };
            } else {
                // or just sleep if we errored previously
                let mut retry_delay = UPLOAD_FAILURE_RETRY_MAX_MS;
                if let Some(backoff_delay) = UPLOAD_FAILURE_RETRY_MIN_MS.checked_shl(retry_attempt)
                {
                    retry_delay = min(retry_delay, backoff_delay);
                }
                tokio::time::sleep(Duration::from_millis(retry_delay)).await;
            }

            let commit_lsn = *self.commit_lsn_watch_rx.borrow();

            // Note that backup_lsn can be higher than commit_lsn if we
            // don't have much local WAL and others already uploaded
            // segments we don't even have.
            if backup_lsn.segment_number(self.wal_seg_size)
                >= commit_lsn.segment_number(self.wal_seg_size)
            {
                retry_attempt = 0;
                continue; /* nothing to do, common case as we wake up on every commit_lsn bump */
            }
            // Perhaps peers advanced the position, check shmem value.
            backup_lsn = self.timeline.get_wal_backup_lsn().await;
            if backup_lsn.segment_number(self.wal_seg_size)
                >= commit_lsn.segment_number(self.wal_seg_size)
            {
                retry_attempt = 0;
                continue;
            }

            match backup_lsn_range(
                &self.timeline,
                self.storage.clone(),
                &mut backup_lsn,
                commit_lsn,
                self.wal_seg_size,
                &self.timeline_dir,
                self.parallel_jobs,
            )
            .await
            {
                Ok(()) => {
                    retry_attempt = 0;
                }
                Err(e) => {
                    // We might have managed to upload some segment even though
                    // some later in the range failed, so log backup_lsn
                    // separately.
                    error!(
                        "failed while offloading range {}-{}, backup_lsn {}: {:?}",
                        backup_lsn, commit_lsn, backup_lsn, e
                    );

                    retry_attempt = retry_attempt.saturating_add(1);
                }
            }
        }
    }
}

async fn backup_lsn_range(
    timeline: &WalResidentTimeline,
    storage: Arc<GenericRemoteStorage>,
    backup_lsn: &mut Lsn,
    end_lsn: Lsn,
    wal_seg_size: usize,
    timeline_dir: &Utf8Path,
    parallel_jobs: usize,
) -> Result<()> {
    if parallel_jobs < 1 {
        anyhow::bail!("parallel_jobs must be >= 1");
    }

    pausable_failpoint!("backup-lsn-range-pausable");

    let remote_timeline_path = &timeline.remote_path;
    let start_lsn = *backup_lsn;
    let segments = get_segments(start_lsn, end_lsn, wal_seg_size);

    info!(
        "offloading segnos {:?} of range [{}-{})",
        segments.iter().map(|&s| s.seg_no).collect::<Vec<_>>(),
        start_lsn,
        end_lsn,
    );

    // Pool of concurrent upload tasks. We use `FuturesOrdered` to
    // preserve order of uploads, and update `backup_lsn` only after
    // all previous uploads are finished.
    let mut uploads = FuturesOrdered::new();
    let mut iter = segments.iter();

    loop {
        let added_task = match iter.next() {
            Some(s) => {
                uploads.push_back(backup_single_segment(
                    &storage,
                    s,
                    timeline_dir,
                    remote_timeline_path,
                ));
                true
            }
            None => false,
        };

        // Wait for the next segment to upload if we don't have any more segments,
        // or if we have too many concurrent uploads.
        if !added_task || uploads.len() >= parallel_jobs {
            let next = uploads.next().await;
            if let Some(res) = next {
                // next segment uploaded
                let segment = res?;
                let new_backup_lsn = segment.end_lsn;
                timeline
                    .set_wal_backup_lsn(new_backup_lsn)
                    .await
                    .context("setting wal_backup_lsn")?;
                *backup_lsn = new_backup_lsn;
            } else {
                // no more segments to upload
                break;
            }
        }
    }

    info!(
        "offloaded segnos {:?} of range [{}-{})",
        segments.iter().map(|&s| s.seg_no).collect::<Vec<_>>(),
        start_lsn,
        end_lsn,
    );
    Ok(())
}

async fn backup_single_segment(
    storage: &GenericRemoteStorage,
    seg: &Segment,
    timeline_dir: &Utf8Path,
    remote_timeline_path: &RemotePath,
) -> Result<Segment> {
    let segment_file_path = seg.file_path(timeline_dir)?;
    let remote_segment_path = seg.remote_path(remote_timeline_path);

    let res = backup_object(
        storage,
        &segment_file_path,
        &remote_segment_path,
        seg.size(),
    )
    .await;
    if res.is_ok() {
        BACKED_UP_SEGMENTS.inc();
    } else {
        BACKUP_ERRORS.inc();
    }
    res?;
    debug!("Backup of {} done", segment_file_path);

    Ok(*seg)
}

#[derive(Debug, Copy, Clone)]
pub struct Segment {
    seg_no: XLogSegNo,
    start_lsn: Lsn,
    end_lsn: Lsn,
}

impl Segment {
    pub fn new(seg_no: u64, start_lsn: Lsn, end_lsn: Lsn) -> Self {
        Self {
            seg_no,
            start_lsn,
            end_lsn,
        }
    }

    pub fn object_name(self) -> String {
        XLogFileName(PG_TLI, self.seg_no, self.size())
    }

    pub fn file_path(self, timeline_dir: &Utf8Path) -> Result<Utf8PathBuf> {
        Ok(timeline_dir.join(self.object_name()))
    }

    pub fn remote_path(self, remote_timeline_path: &RemotePath) -> RemotePath {
        remote_timeline_path.join(self.object_name())
    }

    pub fn size(self) -> usize {
        (u64::from(self.end_lsn) - u64::from(self.start_lsn)) as usize
    }
}

fn get_segments(start: Lsn, end: Lsn, seg_size: usize) -> Vec<Segment> {
    let first_seg = start.segment_number(seg_size);
    let last_seg = end.segment_number(seg_size);

    let res: Vec<Segment> = (first_seg..last_seg)
        .map(|s| {
            let start_lsn = XLogSegNoOffsetToRecPtr(s, 0, seg_size);
            let end_lsn = XLogSegNoOffsetToRecPtr(s + 1, 0, seg_size);
            Segment::new(s, Lsn::from(start_lsn), Lsn::from(end_lsn))
        })
        .collect();
    res
}

async fn backup_object(
    storage: &GenericRemoteStorage,
    source_file: &Utf8Path,
    target_file: &RemotePath,
    size: usize,
) -> Result<()> {
    let file = File::open(&source_file)
        .await
        .with_context(|| format!("Failed to open file {source_file:?} for wal backup"))?;

    let file = tokio_util::io::ReaderStream::with_capacity(file, BUFFER_SIZE);

    let cancel = CancellationToken::new();

    storage
        .upload_storage_object(file, size, target_file, &cancel)
        .await
}

pub(crate) async fn backup_partial_segment(
    storage: &GenericRemoteStorage,
    source_file: &Utf8Path,
    target_file: &RemotePath,
    size: usize,
) -> Result<()> {
    let file = File::open(&source_file)
        .await
        .with_context(|| format!("Failed to open file {source_file:?} for wal backup"))?;

    // limiting the file to read only the first `size` bytes
    let limited_file = tokio::io::AsyncReadExt::take(file, size as u64);

    let file = tokio_util::io::ReaderStream::with_capacity(limited_file, BUFFER_SIZE);

    let cancel = CancellationToken::new();

    storage
        .upload(
            file,
            size,
            target_file,
            Some(StorageMetadata::from([("sk_type", "partial_segment")])),
            &cancel,
        )
        .await
}

pub(crate) async fn copy_partial_segment(
    storage: &GenericRemoteStorage,
    source: &RemotePath,
    destination: &RemotePath,
) -> Result<()> {
    let cancel = CancellationToken::new();

    storage.copy_object(source, destination, &cancel).await
}

const WAL_READ_WARN_THRESHOLD: u32 = 2;
const WAL_READ_MAX_RETRIES: u32 = 3;

pub async fn read_object(
    storage: &GenericRemoteStorage,
    file_path: &RemotePath,
    offset: u64,
) -> anyhow::Result<Pin<Box<dyn tokio::io::AsyncRead + Send + Sync>>> {
    info!("segment download about to start from remote path {file_path:?} at offset {offset}");

    let cancel = CancellationToken::new();

    let opts = DownloadOpts {
        byte_start: std::ops::Bound::Included(offset),
        ..Default::default()
    };

    // This retry only solves the connect errors: subsequent reads can still fail as this function returns
    // a stream.
    let download = backoff::retry(
        || async { storage.download(file_path, &opts, &cancel).await },
        DownloadError::is_permanent,
        WAL_READ_WARN_THRESHOLD,
        WAL_READ_MAX_RETRIES,
        "download WAL segment",
        &cancel,
    )
    .await
    .ok_or_else(|| DownloadError::Cancelled)
    .and_then(|x| x)
    .with_context(|| {
        format!("Failed to open WAL segment download stream for remote path {file_path:?}")
    })?;

    let reader = tokio_util::io::StreamReader::new(download.download_stream);

    let reader = tokio::io::BufReader::with_capacity(BUFFER_SIZE, reader);

    Ok(Box::pin(reader))
}

/// Delete WAL files for the given timeline. Remote storage must be configured
/// when called.
pub async fn delete_timeline(
    storage: &GenericRemoteStorage,
    ttid: &TenantTimelineId,
) -> Result<()> {
    let remote_path = remote_timeline_path(ttid)?;

    // see DEFAULT_MAX_KEYS_PER_LIST_RESPONSE
    // const Option unwrap is not stable, otherwise it would be const.
    let batch_size: NonZeroU32 = NonZeroU32::new(1000).unwrap();

    // A backoff::retry is used here for two reasons:
    // - To provide a backoff rather than busy-polling the API on errors
    // - To absorb transient 429/503 conditions without hitting our error
    //   logging path for issues deleting objects.
    //
    // Note: listing segments might take a long time if there are many of them.
    // We don't currently have http requests timeout cancellation, but if/once
    // we have listing should get streaming interface to make progress.

    pausable_failpoint!("sk-delete-timeline-remote-pause");

    fail::fail_point!("sk-delete-timeline-remote", |_| {
        Err(anyhow::anyhow!("failpoint: sk-delete-timeline-remote"))
    });

    let cancel = CancellationToken::new(); // not really used
    backoff::retry(
        || async {
            // Do list-delete in batch_size batches to make progress even if there a lot of files.
            // Alternatively we could make remote storage list return iterator, but it is more complicated and
            // I'm not sure deleting while iterating is expected in s3.
            loop {
                let files = storage
                    .list(
                        Some(&remote_path),
                        ListingMode::NoDelimiter,
                        Some(batch_size),
                        &cancel,
                    )
                    .await?
                    .keys
                    .into_iter()
                    .map(|o| o.key)
                    .collect::<Vec<_>>();
                if files.is_empty() {
                    return Ok(()); // done
                }
                // (at least) s3 results are sorted, so can log min/max:
                // "List results are always returned in UTF-8 binary order."
                info!(
                    "deleting batch of {} WAL segments [{}-{}]",
                    files.len(),
                    files.first().unwrap().object_name().unwrap_or(""),
                    files.last().unwrap().object_name().unwrap_or("")
                );
                storage.delete_objects(&files, &cancel).await?;
            }
        },
        // consider TimeoutOrCancel::caused_by_cancel when using cancellation
        |_| false,
        3,
        10,
        "executing WAL segments deletion batch",
        &cancel,
    )
    .await
    .ok_or_else(|| anyhow::anyhow!("canceled"))
    .and_then(|x| x)?;

    Ok(())
}

/// Used by wal_backup_partial.
pub async fn delete_objects(storage: &GenericRemoteStorage, paths: &[RemotePath]) -> Result<()> {
    let cancel = CancellationToken::new(); // not really used
    storage.delete_objects(paths, &cancel).await
}

/// Copy segments from one timeline to another. Used in copy_timeline.
pub async fn copy_s3_segments(
    storage: &GenericRemoteStorage,
    wal_seg_size: usize,
    src_ttid: &TenantTimelineId,
    dst_ttid: &TenantTimelineId,
    from_segment: XLogSegNo,
    to_segment: XLogSegNo,
) -> Result<()> {
    let remote_dst_path = remote_timeline_path(dst_ttid)?;

    let cancel = CancellationToken::new();

    let files = storage
        .list(
            Some(&remote_dst_path),
            ListingMode::NoDelimiter,
            None,
            &cancel,
        )
        .await?
        .keys;

    let uploaded_segments = &files
        .iter()
        .filter_map(|o| o.key.object_name().map(ToOwned::to_owned))
        .collect::<HashSet<_>>();

    info!(
        "these segments have already been uploaded: {:?}",
        uploaded_segments
    );

    /* BEGIN_HADRON */
    // Copying multiple segments async.
    let mut copy_stream = stream::iter(from_segment..to_segment)
        .map(|segno| {
            let segment_name = XLogFileName(PG_TLI, segno, wal_seg_size);
            let remote_dst_path = remote_dst_path.clone();
            let cancel = cancel.clone();

            async move {
                if uploaded_segments.contains(&segment_name) {
                    return Ok(());
                }

                if segno % 1000 == 0 {
                    info!("copying segment {} {}", segno, segment_name);
                }

                let from = remote_timeline_path(src_ttid)?.join(&segment_name);
                let to = remote_dst_path.join(&segment_name);

                // Retry logic: retry up to 10 times with 1 second delay
                let mut retry_count = 0;
                const MAX_RETRIES: u32 = 10;

                loop {
                    match storage.copy_object(&from, &to, &cancel).await {
                        Ok(()) => return Ok(()),
                        Err(e) => {
                            if cancel.is_cancelled() {
                                // Don't retry if cancellation was requested
                                return Err(e);
                            }

                            retry_count += 1;
                            if retry_count >= MAX_RETRIES {
                                error!(
                                    "Failed to copy segment {} after {} retries: {}",
                                    segment_name, MAX_RETRIES, e
                                );
                                return Err(e);
                            }
                            warn!(
                                "Failed to copy segment {} (attempt {}/{}): {}, retrying...",
                                segment_name, retry_count, MAX_RETRIES, e
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        })
        .buffer_unordered(32); // Limit to 32 concurrent uploads

    // Process results, stopping on first error
    while let Some(result) = copy_stream.next().await {
        result?;
    }
    /* END_HADRON */

    info!(
        "finished copying segments from {} until {}",
        from_segment, to_segment
    );
    Ok(())
}

/// Get S3 (remote_storage) prefix path used for timeline files.
pub fn remote_timeline_path(ttid: &TenantTimelineId) -> Result<RemotePath> {
    RemotePath::new(&Utf8Path::new(&ttid.tenant_id.to_string()).join(ttid.timeline_id.to_string()))
}
