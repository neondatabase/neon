use anyhow::{Context, Result};

use tokio::task::JoinHandle;
use utils::id::NodeId;

use std::cmp::min;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use postgres_ffi::v14::xlog_utils::XLogSegNoOffsetToRecPtr;
use postgres_ffi::XLogFileName;
use postgres_ffi::{XLogSegNo, PG_TLI};
use remote_storage::{GenericRemoteStorage, RemotePath};
use tokio::fs::File;

use tokio::select;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::*;

use utils::{id::TenantTimelineId, lsn::Lsn};

use crate::timeline::{PeerInfo, Timeline};
use crate::{GlobalTimelines, SafeKeeperConf};

use once_cell::sync::OnceCell;

const UPLOAD_FAILURE_RETRY_MIN_MS: u64 = 10;
const UPLOAD_FAILURE_RETRY_MAX_MS: u64 = 5000;

/// Check whether wal backup is required for timeline. If yes, mark that launcher is
/// aware of current status and return the timeline.
async fn is_wal_backup_required(ttid: TenantTimelineId) -> Option<Arc<Timeline>> {
    match GlobalTimelines::get(ttid).ok() {
        Some(tli) => {
            tli.wal_backup_attend().await;
            Some(tli)
        }
        None => None,
    }
}

struct WalBackupTaskHandle {
    shutdown_tx: Sender<()>,
    handle: JoinHandle<()>,
}

struct WalBackupTimelineEntry {
    timeline: Arc<Timeline>,
    handle: Option<WalBackupTaskHandle>,
}

async fn shut_down_task(ttid: TenantTimelineId, entry: &mut WalBackupTimelineEntry) {
    if let Some(wb_handle) = entry.handle.take() {
        // Tell the task to shutdown. Error means task exited earlier, that's ok.
        let _ = wb_handle.shutdown_tx.send(()).await;
        // Await the task itself. TODO: restart panicked tasks earlier.
        if let Err(e) = wb_handle.handle.await {
            warn!("WAL backup task for {} panicked: {}", ttid, e);
        }
    }
}

/// The goal is to ensure that normally only one safekeepers offloads. However,
/// it is fine (and inevitable, as s3 doesn't provide CAS) that for some short
/// time we have several ones as they PUT the same files. Also,
/// - frequently changing the offloader would be bad;
/// - electing seriously lagging safekeeper is undesirable;
/// So we deterministically choose among the reasonably caught up candidates.
/// TODO: take into account failed attempts to deal with hypothetical situation
/// where s3 is unreachable only for some sks.
fn determine_offloader(
    alive_peers: &[PeerInfo],
    wal_backup_lsn: Lsn,
    ttid: TenantTimelineId,
    conf: &SafeKeeperConf,
) -> (Option<NodeId>, String) {
    // TODO: remove this once we fill newly joined safekeepers since backup_lsn.
    let capable_peers = alive_peers
        .iter()
        .filter(|p| p.local_start_lsn <= wal_backup_lsn);
    match capable_peers.clone().map(|p| p.commit_lsn).max() {
        None => (None, "no connected peers to elect from".to_string()),
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
            )
        }
    }
}

/// Based on peer information determine which safekeeper should offload; if it
/// is me, run (per timeline) task, if not yet. OTOH, if it is not me and task
/// is running, kill it.
async fn update_task(
    conf: &SafeKeeperConf,
    ttid: TenantTimelineId,
    entry: &mut WalBackupTimelineEntry,
) {
    let alive_peers = entry.timeline.get_peers(conf).await;
    let wal_backup_lsn = entry.timeline.get_wal_backup_lsn().await;
    let (offloader, election_dbg_str) =
        determine_offloader(&alive_peers, wal_backup_lsn, ttid, conf);
    let elected_me = Some(conf.my_id) == offloader;

    if elected_me != (entry.handle.is_some()) {
        if elected_me {
            info!("elected for backup {}: {}", ttid, election_dbg_str);

            let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
            let timeline_dir = conf.timeline_dir(&ttid);

            let handle = tokio::spawn(
                backup_task_main(ttid, timeline_dir, conf.workdir.clone(), shutdown_rx)
                    .instrument(info_span!("WAL backup task", ttid = %ttid)),
            );

            entry.handle = Some(WalBackupTaskHandle {
                shutdown_tx,
                handle,
            });
        } else {
            info!("stepping down from backup {}: {}", ttid, election_dbg_str);
            shut_down_task(ttid, entry).await;
        }
    }
}

const CHECK_TASKS_INTERVAL_MSEC: u64 = 1000;

/// Sits on wal_backup_launcher_rx and starts/stops per timeline wal backup
/// tasks. Having this in separate task simplifies locking, allows to reap
/// panics and separate elections from offloading itself.
pub async fn wal_backup_launcher_task_main(
    conf: SafeKeeperConf,
    mut wal_backup_launcher_rx: Receiver<TenantTimelineId>,
) -> anyhow::Result<()> {
    info!(
        "WAL backup launcher started, remote config {:?}",
        conf.remote_storage
    );

    let conf_ = conf.clone();
    REMOTE_STORAGE.get_or_init(|| {
        conf_
            .remote_storage
            .as_ref()
            .map(|c| GenericRemoteStorage::from_config(c).expect("failed to create remote storage"))
    });

    // Presence in this map means launcher is aware s3 offloading is needed for
    // the timeline, but task is started only if it makes sense for to offload
    // from this safekeeper.
    let mut tasks: HashMap<TenantTimelineId, WalBackupTimelineEntry> = HashMap::new();

    let mut ticker = tokio::time::interval(Duration::from_millis(CHECK_TASKS_INTERVAL_MSEC));
    loop {
        tokio::select! {
            ttid = wal_backup_launcher_rx.recv() => {
                // channel is never expected to get closed
                let ttid = ttid.unwrap();
                if conf.remote_storage.is_none() || !conf.wal_backup_enabled {
                    continue; /* just drain the channel and do nothing */
                }
                let timeline = is_wal_backup_required(ttid).await;
                // do we need to do anything at all?
                if timeline.is_some() != tasks.contains_key(&ttid) {
                    if let Some(timeline) = timeline {
                        // need to start the task
                        let entry = tasks.entry(ttid).or_insert(WalBackupTimelineEntry {
                            timeline,
                            handle: None,
                        });
                        update_task(&conf, ttid, entry).await;
                    } else {
                        // need to stop the task
                        info!("stopping WAL backup task for {}", ttid);
                        let mut entry = tasks.remove(&ttid).unwrap();
                        shut_down_task(ttid, &mut entry).await;
                    }
                }
            }
            // For each timeline needing offloading, check if this safekeeper
            // should do the job and start/stop the task accordingly.
            _ = ticker.tick() => {
                for (ttid, entry) in tasks.iter_mut() {
                    update_task(&conf, *ttid, entry).await;
                }
            }
        }
    }
}

struct WalBackupTask {
    timeline: Arc<Timeline>,
    timeline_dir: PathBuf,
    workspace_dir: PathBuf,
    wal_seg_size: usize,
    commit_lsn_watch_rx: watch::Receiver<Lsn>,
}

/// Offload single timeline.
async fn backup_task_main(
    ttid: TenantTimelineId,
    timeline_dir: PathBuf,
    workspace_dir: PathBuf,
    mut shutdown_rx: Receiver<()>,
) {
    info!("started");
    let res = GlobalTimelines::get(ttid);
    if let Err(e) = res {
        error!("backup error for timeline {}: {}", ttid, e);
        return;
    }
    let tli = res.unwrap();

    let mut wb = WalBackupTask {
        wal_seg_size: tli.get_wal_seg_size().await,
        commit_lsn_watch_rx: tli.get_commit_lsn_watch_rx(),
        timeline: tli,
        timeline_dir,
        workspace_dir,
    };

    // task is spinned up only when wal_seg_size already initialized
    assert!(wb.wal_seg_size > 0);

    let mut canceled = false;
    select! {
        _ = wb.run() => {}
        _ = shutdown_rx.recv() => {
            canceled = true;
        }
    }
    info!("task {}", if canceled { "canceled" } else { "terminated" });
}

impl WalBackupTask {
    async fn run(&mut self) {
        let mut backup_lsn = Lsn(0);

        let mut retry_attempt = 0u32;
        // offload loop
        loop {
            if retry_attempt == 0 {
                // wait for new WAL to arrive
                if let Err(e) = self.commit_lsn_watch_rx.changed().await {
                    // should never happen, as we hold Arc to timeline.
                    error!("commit_lsn watch shut down: {:?}", e);
                    return;
                }
            } else {
                // or just sleep if we errored previously
                let mut retry_delay = UPLOAD_FAILURE_RETRY_MAX_MS;
                if let Some(backoff_delay) = UPLOAD_FAILURE_RETRY_MIN_MS.checked_shl(retry_attempt)
                {
                    retry_delay = min(retry_delay, backoff_delay);
                }
                sleep(Duration::from_millis(retry_delay)).await;
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
                &mut backup_lsn,
                commit_lsn,
                self.wal_seg_size,
                &self.timeline_dir,
                &self.workspace_dir,
            )
            .await
            {
                Ok(()) => {
                    retry_attempt = 0;
                }
                Err(e) => {
                    error!(
                        "failed while offloading range {}-{}: {:?}",
                        backup_lsn, commit_lsn, e
                    );

                    retry_attempt = retry_attempt.saturating_add(1);
                }
            }
        }
    }
}

pub async fn backup_lsn_range(
    timeline: &Arc<Timeline>,
    backup_lsn: &mut Lsn,
    end_lsn: Lsn,
    wal_seg_size: usize,
    timeline_dir: &Path,
    workspace_dir: &Path,
) -> Result<()> {
    let start_lsn = *backup_lsn;
    let segments = get_segments(start_lsn, end_lsn, wal_seg_size);
    for s in &segments {
        backup_single_segment(s, timeline_dir, workspace_dir)
            .await
            .with_context(|| format!("offloading segno {}", s.seg_no))?;

        let new_backup_lsn = s.end_lsn;
        timeline
            .set_wal_backup_lsn(new_backup_lsn)
            .await
            .context("setting wal_backup_lsn")?;
        *backup_lsn = new_backup_lsn;
    }
    info!(
        "offloaded segnos {:?} up to {}, previous backup_lsn {}",
        segments.iter().map(|&s| s.seg_no).collect::<Vec<_>>(),
        end_lsn,
        start_lsn,
    );
    Ok(())
}

async fn backup_single_segment(
    seg: &Segment,
    timeline_dir: &Path,
    workspace_dir: &Path,
) -> Result<()> {
    let segment_file_path = seg.file_path(timeline_dir)?;
    let remote_segment_path = segment_file_path
        .strip_prefix(workspace_dir)
        .context("Failed to strip workspace dir prefix")
        .and_then(RemotePath::new)
        .with_context(|| {
            format!(
                "Failed to resolve remote part of path {segment_file_path:?} for base {workspace_dir:?}",
            )
        })?;

    backup_object(&segment_file_path, &remote_segment_path, seg.size()).await?;
    debug!("Backup of {} done", segment_file_path.display());

    Ok(())
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

    pub fn file_path(self, timeline_dir: &Path) -> Result<PathBuf> {
        Ok(timeline_dir.join(self.object_name()))
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

static REMOTE_STORAGE: OnceCell<Option<GenericRemoteStorage>> = OnceCell::new();

async fn backup_object(source_file: &Path, target_file: &RemotePath, size: usize) -> Result<()> {
    let storage = REMOTE_STORAGE
        .get()
        .expect("failed to get remote storage")
        .as_ref()
        .unwrap();

    let file = tokio::io::BufReader::new(File::open(&source_file).await.with_context(|| {
        format!(
            "Failed to open file {} for wal backup",
            source_file.display()
        )
    })?);

    storage
        .upload_storage_object(Box::new(file), size, target_file)
        .await
}

pub async fn read_object(
    file_path: &RemotePath,
    offset: u64,
) -> anyhow::Result<Pin<Box<dyn tokio::io::AsyncRead + Send + Sync>>> {
    let storage = REMOTE_STORAGE
        .get()
        .context("Failed to get remote storage")?
        .as_ref()
        .context("No remote storage configured")?;

    info!("segment download about to start from remote path {file_path:?} at offset {offset}");

    let download = storage
        .download_storage_object(Some((offset, None)), file_path)
        .await
        .with_context(|| {
            format!("Failed to open WAL segment download stream for remote path {file_path:?}")
        })?;

    Ok(download.download_stream)
}
