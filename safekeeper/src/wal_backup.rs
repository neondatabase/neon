use anyhow::{Context, Result};
use etcd_broker::subscription_key::{
    NodeKind, OperationKind, SkOperationKind, SubscriptionKey, SubscriptionKind,
};
use tokio::task::JoinHandle;

use std::cmp::min;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use postgres_ffi::v14::xlog_utils::XLogSegNoOffsetToRecPtr;
use postgres_ffi::XLogFileName;
use postgres_ffi::{XLogSegNo, PG_TLI};
use remote_storage::GenericRemoteStorage;
use tokio::fs::File;
use tokio::runtime::Builder;

use tokio::select;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::*;

use utils::{id::TenantTimelineId, lsn::Lsn};

use crate::broker::{Election, ElectionLeader};
use crate::timeline::Timeline;
use crate::{broker, GlobalTimelines, SafeKeeperConf};

use once_cell::sync::OnceCell;

const BROKER_CONNECTION_RETRY_DELAY_MS: u64 = 1000;

const UPLOAD_FAILURE_RETRY_MIN_MS: u64 = 10;
const UPLOAD_FAILURE_RETRY_MAX_MS: u64 = 5000;

pub fn wal_backup_launcher_thread_main(
    conf: SafeKeeperConf,
    wal_backup_launcher_rx: Receiver<TenantTimelineId>,
) {
    let rt = Builder::new_multi_thread()
        .worker_threads(conf.backup_runtime_threads)
        .enable_all()
        .build()
        .expect("failed to create wal backup runtime");

    rt.block_on(async {
        wal_backup_launcher_main_loop(conf, wal_backup_launcher_rx).await;
    });
}

/// Check whether wal backup is required for timeline. If yes, mark that launcher is
/// aware of current status and return the timeline.
fn is_wal_backup_required(ttid: TenantTimelineId) -> Option<Arc<Timeline>> {
    GlobalTimelines::get(ttid)
        .ok()
        .filter(|tli| tli.wal_backup_attend())
}

struct WalBackupTaskHandle {
    shutdown_tx: Sender<()>,
    handle: JoinHandle<()>,
}

struct WalBackupTimelineEntry {
    timeline: Arc<Timeline>,
    handle: Option<WalBackupTaskHandle>,
}

/// Start per timeline task, if it makes sense for this safekeeper to offload.
fn consider_start_task(
    conf: &SafeKeeperConf,
    ttid: TenantTimelineId,
    task: &mut WalBackupTimelineEntry,
) {
    if !task.timeline.can_wal_backup() {
        return;
    }
    info!("starting WAL backup task for {}", ttid);

    // TODO: decide who should offload right here by simply checking current
    // state instead of running elections in offloading task.
    let election_name = SubscriptionKey {
        cluster_prefix: conf.broker_etcd_prefix.clone(),
        kind: SubscriptionKind::Operation(
            ttid,
            NodeKind::Safekeeper,
            OperationKind::Safekeeper(SkOperationKind::WalBackup),
        ),
    }
    .watch_key();
    let my_candidate_name = broker::get_candiate_name(conf.my_id);
    let election = broker::Election::new(
        election_name,
        my_candidate_name,
        conf.broker_endpoints.clone(),
    );

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    let timeline_dir = conf.timeline_dir(&ttid);

    let handle = tokio::spawn(
        backup_task_main(ttid, timeline_dir, shutdown_rx, election)
            .instrument(info_span!("WAL backup task", ttid = %ttid)),
    );

    task.handle = Some(WalBackupTaskHandle {
        shutdown_tx,
        handle,
    });
}

const CHECK_TASKS_INTERVAL_MSEC: u64 = 1000;

/// Sits on wal_backup_launcher_rx and starts/stops per timeline wal backup
/// tasks. Having this in separate task simplifies locking, allows to reap
/// panics and separate elections from offloading itself.
async fn wal_backup_launcher_main_loop(
    conf: SafeKeeperConf,
    mut wal_backup_launcher_rx: Receiver<TenantTimelineId>,
) {
    info!(
        "WAL backup launcher started, remote config {:?}",
        conf.remote_storage
    );

    let conf_ = conf.clone();
    REMOTE_STORAGE.get_or_init(|| {
        conf_.remote_storage.as_ref().map(|c| {
            GenericRemoteStorage::from_config(conf_.workdir, c)
                .expect("failed to create remote storage")
        })
    });

    // Presense in this map means launcher is aware s3 offloading is needed for
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
                let timeline = is_wal_backup_required(ttid);
                // do we need to do anything at all?
                if timeline.is_some() != tasks.contains_key(&ttid) {
                    if let Some(timeline) = timeline {
                        // need to start the task
                        let entry = tasks.entry(ttid).or_insert(WalBackupTimelineEntry {
                            timeline,
                            handle: None,
                        });
                        consider_start_task(&conf, ttid, entry);
                    } else {
                        // need to stop the task
                        info!("stopping WAL backup task for {}", ttid);

                        let entry = tasks.remove(&ttid).unwrap();
                        if let Some(wb_handle) = entry.handle {
                            // Tell the task to shutdown. Error means task exited earlier, that's ok.
                            let _ = wb_handle.shutdown_tx.send(()).await;
                            // Await the task itself. TODO: restart panicked tasks earlier.
                            if let Err(e) = wb_handle.handle.await {
                                warn!("WAL backup task for {} panicked: {}", ttid, e);
                            }
                        }
                    }
                }
            }
            // Start known tasks, if needed and possible.
            _ = ticker.tick() => {
                for (ttid, entry) in tasks.iter_mut().filter(|(_, entry)| entry.handle.is_none()) {
                    consider_start_task(&conf, *ttid, entry);
                }
            }
        }
    }
}

struct WalBackupTask {
    timeline: Arc<Timeline>,
    timeline_dir: PathBuf,
    wal_seg_size: usize,
    commit_lsn_watch_rx: watch::Receiver<Lsn>,
    leader: Option<ElectionLeader>,
    election: Election,
}

/// Offload single timeline. Called only after we checked that backup
/// is required (wal_backup_attend) and possible (can_wal_backup).
async fn backup_task_main(
    ttid: TenantTimelineId,
    timeline_dir: PathBuf,
    mut shutdown_rx: Receiver<()>,
    election: Election,
) {
    info!("started");
    let res = GlobalTimelines::get(ttid);
    if let Err(e) = res {
        error!("backup error for timeline {}: {}", ttid, e);
        return;
    }
    let tli = res.unwrap();

    let mut wb = WalBackupTask {
        wal_seg_size: tli.get_wal_seg_size(),
        commit_lsn_watch_rx: tli.get_commit_lsn_watch_rx(),
        timeline: tli,
        timeline_dir,
        leader: None,
        election,
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
    if let Some(l) = wb.leader {
        l.give_up().await;
    }
    info!("task {}", if canceled { "canceled" } else { "terminated" });
}

impl WalBackupTask {
    async fn run(&mut self) {
        let mut backup_lsn = Lsn(0);

        // election loop
        loop {
            let mut retry_attempt = 0u32;

            info!("acquiring leadership");
            if let Err(e) = broker::get_leader(&self.election, &mut self.leader).await {
                error!("error during leader election {:?}", e);
                sleep(Duration::from_millis(BROKER_CONNECTION_RETRY_DELAY_MS)).await;
                continue;
            }
            info!("acquired leadership");

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
                    if let Some(backoff_delay) =
                        UPLOAD_FAILURE_RETRY_MIN_MS.checked_shl(retry_attempt)
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
                    continue; /* nothing to do, common case as we wake up on every commit_lsn bump */
                }
                // Perhaps peers advanced the position, check shmem value.
                backup_lsn = self.timeline.get_wal_backup_lsn();
                if backup_lsn.segment_number(self.wal_seg_size)
                    >= commit_lsn.segment_number(self.wal_seg_size)
                {
                    continue;
                }

                if let Some(l) = self.leader.as_mut() {
                    // Optimization idea for later:
                    //  Avoid checking election leader every time by returning current lease grant expiration time
                    //  Re-check leadership only after expiration time,
                    //  such approach would reduce overhead on write-intensive workloads

                    match l
                        .check_am_i(
                            self.election.election_name.clone(),
                            self.election.candidate_name.clone(),
                        )
                        .await
                    {
                        Ok(leader) => {
                            if !leader {
                                info!("lost leadership");
                                break;
                            }
                        }
                        Err(e) => {
                            warn!("error validating leader, {:?}", e);
                            break;
                        }
                    }
                }

                match backup_lsn_range(
                    backup_lsn,
                    commit_lsn,
                    self.wal_seg_size,
                    &self.timeline_dir,
                )
                .await
                {
                    Ok(backup_lsn_result) => {
                        backup_lsn = backup_lsn_result;
                        let res = self.timeline.set_wal_backup_lsn(backup_lsn_result);
                        if let Err(e) = res {
                            error!("backup error: {}", e);
                            return;
                        }
                        retry_attempt = 0;
                    }
                    Err(e) => {
                        error!(
                            "failed while offloading range {}-{}: {:?}",
                            backup_lsn, commit_lsn, e
                        );

                        retry_attempt = min(retry_attempt + 1, u32::MAX);
                    }
                }
            }
        }
    }
}

pub async fn backup_lsn_range(
    start_lsn: Lsn,
    end_lsn: Lsn,
    wal_seg_size: usize,
    timeline_dir: &Path,
) -> Result<Lsn> {
    let mut res = start_lsn;
    let segments = get_segments(start_lsn, end_lsn, wal_seg_size);
    for s in &segments {
        backup_single_segment(s, timeline_dir)
            .await
            .with_context(|| format!("offloading segno {}", s.seg_no))?;

        res = s.end_lsn;
    }
    info!(
        "offloaded segnos {:?} up to {}, previous backup_lsn {}",
        segments.iter().map(|&s| s.seg_no).collect::<Vec<_>>(),
        end_lsn,
        start_lsn,
    );
    Ok(res)
}

async fn backup_single_segment(seg: &Segment, timeline_dir: &Path) -> Result<()> {
    let segment_file_name = seg.file_path(timeline_dir)?;

    backup_object(&segment_file_name, seg.size()).await?;
    debug!("Backup of {} done", segment_file_name.display());

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

async fn backup_object(source_file: &Path, size: usize) -> Result<()> {
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
        .upload_storage_object(Box::new(file), size, source_file)
        .await
}

pub async fn read_object(
    file_path: PathBuf,
    offset: u64,
) -> anyhow::Result<Pin<Box<dyn tokio::io::AsyncRead>>> {
    let storage = REMOTE_STORAGE
        .get()
        .context("Failed to get remote storage")?
        .as_ref()
        .context("No remote storage configured")?;

    info!(
        "segment download about to start for local path {} at offset {}",
        file_path.display(),
        offset
    );
    let download = storage
        .download_storage_object(Some((offset, None)), &file_path)
        .await
        .with_context(|| {
            format!(
                "Failed to open WAL segment download stream for local path {}",
                file_path.display()
            )
        })?;

    Ok(download.download_stream)
}
