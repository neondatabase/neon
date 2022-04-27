use anyhow::{Result, ensure, bail, Context};

use std::path::PathBuf;
use std::time::Duration;

use postgres_ffi::xlog_utils::{XLogSegNo, PG_TLI, XLogFileName, XLogSegNoOffsetToRecPtr};
use remote_storage::{RemoteStorage, GenericRemoteStorage, RemoteStorageConfig};
use tokio::fs::File;
use tokio::runtime::{Builder, Runtime};

use tokio::spawn;
use tokio::sync::watch::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::*;

use utils::{
    lsn::Lsn,
    zid::{ZTenantTimelineId},
};


use crate::{broker, SafeKeeperConf, wal_storage};


use once_cell::sync::OnceCell;

static BACKUP_RUNTIME: OnceCell<Runtime> = OnceCell::new();
const DEFAULT_BACKUP_RUNTIME_SIZE: u32 = 16;

const MIN_WAL_SEGMENT_SIZE: usize = 1 << 20; // 1MB
const MAX_WAL_SEGMENT_SIZE: usize = 1 << 30; // 1GB

const BACKUP_ELECTION_PATH: &str = "WAL_BACKUP";

const LEASE_ACQUISITION_RETRY_DELAY_MS :u64 = 1000;

async fn detect_task(
    conf: SafeKeeperConf,
    timeline_id: ZTenantTimelineId,
    backup_start: Lsn,
    mut segment_size_set: Receiver<u32>,
    mut lsn_durable: Receiver<Lsn>, 
    lsn_backed_up: Sender<Lsn>,
    ) -> Result<()> {

    segment_size_set.changed().await.expect("Failed to recieve wal segment size");
    let wal_seg_size = *segment_size_set.borrow() as usize;
    ensure!((MIN_WAL_SEGMENT_SIZE..=MAX_WAL_SEGMENT_SIZE).contains(&wal_seg_size), "Invalid wal seg size provided, should be between 1MiB and 1GiB per postgres");

    let mut backup_lsn = backup_start;

    let mut lease: Option<i64> = None;

    loop {
        let mut keepalive =  None::<JoinHandle<Result<()>>>;
        let mut cancel = false;

        let c = conf.clone();
        if c.broker_endpoints.is_some() {
            let lease_id = match broker::get_lease(&c).await {
                Ok(l) => l,
                Err(e) => {
                    error!("Could not acqire lease {}", e);
                    sleep(Duration::from_millis(LEASE_ACQUISITION_RETRY_DELAY_MS)).await;
                    continue;
                },
            };

            keepalive = Some(spawn::<_>(broker::lease_keep_alive(lease_id, c.clone())));
            
            lease = Some(lease_id);
        }

        'leader: loop {
            if let Some(lease_id) = lease {
                if let Err(e) = broker::become_leader(lease_id, BACKUP_ELECTION_PATH.to_string(), &timeline_id, &conf).await {
                    warn!("Error retreiving leader election details, restarting. details: {:?}", e);
                    break;
                }
            }

            loop {
                if let Err(e) = lsn_durable.changed().await {
                        warn!("Channel closed shutting down wal backup {:?}", e);
                        cancel = true;
                        break 'leader;
                }

                let commit_lsn = *lsn_durable.borrow();

                ensure!(commit_lsn >= backup_lsn, "backup lsn should never pass commit lsn");

                if backup_lsn.segment_number(wal_seg_size) == commit_lsn.segment_number(wal_seg_size) {
                    continue;
                }

                if lease.is_some() {
                    // Optimization idea for later:
                    //  Avoid checking election leader every time by returning current lease grant expiration time
                    //  Re-check leadership only after expiration time, 
                    //  such approach woud reduce overhead on write-intensive workloads

                    match broker::is_leader(BACKUP_ELECTION_PATH.to_string(), &timeline_id, &conf).await {
                        Ok(is_leader) => if !is_leader {
                            info!("Leader has changed for for the timeline {}", timeline_id);
                            break;
                        },
                        Err(e) => { 
                            warn!("Error validating leader for the timeline {}, {:?}", timeline_id, e); 
                            break; 
                        },
                    }
                }

                info!("Woken up for lsn {} committed, will back it up. backup lsn {}", commit_lsn, backup_lsn);

                if let Ok(backup_lsn_result) = backup_lsn_range(backup_lsn, commit_lsn, wal_seg_size, &conf, timeline_id).await {
                    backup_lsn = backup_lsn_result;
                    lsn_backed_up.send(backup_lsn)?;    
                } else {
                    error!("Something went wrong with backup commit_lsn {} backup lsn {}", commit_lsn, backup_lsn);
                }
            }
        }

        if let Some(ka) = keepalive {
            ka.abort();
        }

        if cancel {
            break;
        }
    }

    Ok(())
}

pub async fn backup_lsn_range(start_lsn: Lsn, end_lsn: Lsn, wal_seg_size: usize, conf: &SafeKeeperConf, timeline_id: ZTenantTimelineId) -> Result<Lsn> {

    let mut res = start_lsn;
    for s in get_segments(start_lsn, end_lsn, wal_seg_size) {

        let seg_backup = backup_single_segment(s, wal_seg_size, conf, timeline_id).await;

        // TODO: antons limit this only to Not Found errors
        if seg_backup.is_err() && start_lsn.is_valid() {
            error!("Segment {} not found in timeline {}", s.seg_no, timeline_id)
        }

        ensure!(start_lsn >= s.start_lsn || start_lsn.is_valid(), "Out of order segment upload detected");

        if res == s.start_lsn {
            res = s.end_lsn;
        } else {
            warn!("Out of order Segment {} upload had been detected for timeline {}. Backup Lsn {}, Segment Start Lsn {}", s.seg_no, timeline_id, res, s.start_lsn)
        }
    }

    Ok(res)
}

async fn backup_single_segment(seg: Segment, wal_seg_size: usize, conf: &SafeKeeperConf, timeline_id: ZTenantTimelineId) -> Result<()> {

    let segment_file_name = seg.file_path(&timeline_id, conf)?;
    let dest_name = PathBuf::from(format!("{}/{}", timeline_id.tenant_id, timeline_id.timeline_id)).with_file_name(seg.file_name());

    info!("Backup of {} requested", segment_file_name.display());
    ensure!(seg.size() == wal_seg_size);

    if !segment_file_name.exists() {
        // TODO: antons return a specific error
        bail!("Segment file is Missing");
    } 

    backup_object(conf.remote_storage_config.as_ref(), conf.timeline_dir(&timeline_id), &segment_file_name, seg.size(), &dest_name).await?;

    info!("Backup of {} done", segment_file_name.display());

    Ok(())
}

pub fn create(
        conf: &SafeKeeperConf,
        timeline_id: &ZTenantTimelineId,
        initial_lsn: Lsn,
        wal_seg_size: Receiver<u32>,
        lsn_committed: Receiver<Lsn>,
        lsn_backed_up: Sender<Lsn>) -> Result<()> {
    info!("Creating wal backup task for timeline {}, starting with Lsn {}", timeline_id.timeline_id, initial_lsn);

    let runtime = BACKUP_RUNTIME.get_or_init(|| {
        let rt_size =  usize::try_from(conf.backup_runtime_threads.unwrap_or(DEFAULT_BACKUP_RUNTIME_SIZE))
            .expect("Could not get configuration value for backup_runtime_threads");

        info!("initialize backup async runtime with {} threads", rt_size);

        Builder::new_multi_thread()
            .worker_threads(rt_size)
            .enable_all()
            .build()
            .expect("Couldn't create wal backup runtime")
    });

    // TODO: antons add timeline id to the info span
    runtime.spawn(
        detect_task(conf.clone(), *timeline_id, initial_lsn, wal_seg_size, lsn_committed, lsn_backed_up)
        .instrument(info_span!("wal backup")));

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
        Self { seg_no, start_lsn, end_lsn }
    }

    pub fn file_name(self) -> String {
        XLogFileName(PG_TLI, self.seg_no, self.size())
    }

    pub fn file_path(self, timeline_id: &ZTenantTimelineId, conf: &SafeKeeperConf) -> Result<PathBuf> {
        let (wal_file_path, _wal_file_partial_path) =
        wal_storage::wal_file_paths(&conf.timeline_dir(timeline_id), self.seg_no, self.size())?;
        Ok(wal_file_path)
    }

    pub fn size(self) -> usize {
        (u64::from(self.end_lsn) - u64::from(self.start_lsn)) as usize
    }
}

fn get_segments(start: Lsn, end: Lsn, seg_size: usize) -> Vec<Segment> {
    let first_seg = start.segment_number(seg_size);
    let last_seg = end.segment_number(seg_size);
    
    let res:Vec<Segment> = (first_seg .. last_seg).map(|s| {
            let start_lsn = XLogSegNoOffsetToRecPtr(s, 0, seg_size);
            let end_lsn = XLogSegNoOffsetToRecPtr(s + 1, 0, seg_size);
            Segment::new(s, Lsn::from(start_lsn), Lsn::from(end_lsn))
    }).collect();

    res
}

// Ugly stuff, probably needs a new home
static REMOTE_STORAGE: OnceCell<Box<Option<GenericRemoteStorage>>> = OnceCell::new();

async fn backup_object(remote_storage_config: Option<&RemoteStorageConfig>, source_directory: PathBuf, source_file: &PathBuf, size: usize, destination: &PathBuf) -> Result<()> {

    let storage = REMOTE_STORAGE.get_or_init(|| {
        // TODO: antons clean the next line
        let rs = remote_storage_config.map(|c|  GenericRemoteStorage::new(source_directory, c).expect("Could not create remote storage"));

        Box::new(rs)
    });

    let file = File::open(&source_file).await?;

    match storage.as_ref() {
        Some(GenericRemoteStorage::Local(local_storage)) => 
        {
            info!("local upload about to start from {} to {}", source_file.display(), destination.display());
            local_storage.upload(file, size, destination, None).await
        }
        Some(GenericRemoteStorage::S3(s3_storage)) => 
        {
            let s3path = s3_storage
                .remote_object_id(destination.as_path())
                .with_context(|| format!("Could not format remote path for {}", destination.display()))?;

            info!("S3 upload about to start from {} to {}", source_file.display(), destination.display());
            s3_storage.upload(file, size, &s3path, None).await
        }
        None => {
            info!("no backup storage configured, skipping backup {}", destination.display());
            Ok(())
        },
    }.with_context(|| format!("Failed to backup {}", destination.display()))?;

    Ok(())
}
