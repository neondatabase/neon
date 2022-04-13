
use std::{path::PathBuf};
use std::{thread, time};

use tokio::runtime::{Builder};


use anyhow::Result;

use tokio::sync::watch::Receiver;
use zenith_utils::lsn::Lsn;
use tracing::*;


/// 
/// High level abstraction could be an async task service with tasks executed on a thread pool
/// task service doesn't have any state (other than in-memory), requests to process are equeued 
/// 
/// Wal Backup will have 2 interfaces:
/// 1. discover local state and segments in S3, enqueue each missing segment for upload
/// 2. provide enqueue interface for on-demand requests
/// 3. When queue is full we shuould introduce a spill interface
/// 4. uploads should be in order of creation
/// 5. service should have a knob for parallelism (as single threaded upload is slow)


#[allow(dead_code)]
pub struct Seg {
    lsn: Lsn,
    timeline: u32,
    name: PathBuf,
}

#[derive(Debug, Copy, Clone)]
pub struct WalBackup {
    
    _remote_storage : u32,
    // _async_service : Runtime,
    // have a per-timeline status of what segments are uploaded
    // Given segments have very specific naming we store it as a bitmap (we probably don't care about postgres timeline in this case, all we care is the start LSN)
    // we check the S3 state only when we're a leader and the timeline is initialized.
}

#[allow(unreachable_code)]
async fn detect_task(mut event: Receiver<Lsn>, backup: WalBackup) -> Result<()> {

    while event.changed().await.is_ok() {
        warn!("Woken Up for segment backup");
        backup.backup_stuff(*event.borrow())?;
    }

    Ok(())
}

async fn upload_task(s: String, lsn: Lsn) -> Result<()> {
    warn!("waiting for lsn {} ", lsn);
    thread::sleep(time::Duration::from_millis(1000));

    warn!("uploading {} ", s);
    thread::sleep(time::Duration::from_millis(1000));
    warn!("uploaded {}", s);
    Ok(())
}


// #[allow(dead_code)]
impl WalBackup {


    pub fn create(event: Receiver<Lsn>) -> Self {
        warn!("Backup service is created");

        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();


        let x = Self {_remote_storage : 0 };// , _async_service : runtime};
        runtime.spawn(detect_task(event, x));

        return x;
    }

    pub fn restore(event: Receiver<Lsn>) -> Self {
        warn!("Backup service is restored");


        let x = WalBackup::create(event);

        // schedule a task to enqueue missing segmets

        return x;
    }

    // Enqueue segment for upload
    // Input should have file id, and timeline information
    // TODO antons: in addition to the segment that can be identified by name or by ID, we should pass ZTenantTimelineId
    pub fn backup_segment(&self, seg: PathBuf, seg_end_lsn: Lsn) -> Result<()> {
        let tag = seg.as_path().display().to_string();
        warn!("Backup of {} requested for timeline {}", tag, "unknown");


        // TODO: figure out how to make two runtimes into one
        let _async_service = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();


        let _foo =
        _async_service.spawn(upload_task(tag, seg_end_lsn));

        // TODO: how should this be done?
        // Should we wait on timer to wait on each task or proactively when queue is "FULL", can framework handle this?
        // ERROR handling is not clear, maybe when we can't finish the upload (we should try forever) 
        // self._async_service.block_on(foo).unwrap()?;

        Ok(())
    }

    pub fn backup_stuff(&self, lsn: Lsn) -> Result<()> {
        // Discover what needs to be done
        // Get safekeeper's LSNs

        // TODO: get lower boundary from control file
        for s in self.get_segments(Lsn(0), lsn) {
            // sk.await_for_lsn(s.0);
            self.backup_segment(s.1, s.0)?;
            // TODO - post results into a Vector of ranges that would allow advancement of LSNs in large jumps
        } 

        Ok(())
    }

    // Returns a list of WAL segments that fall into [start lsn, end lsn).
    fn get_segments(&self, _start: Lsn, _end: Lsn) -> Vec<(Lsn, PathBuf)> {
        warn!("NYI");
        // TODO: this code should check what is current write lsn
        // TODO: this whould find out what was the last_backup_lsn
        // if current_write_lsn >= last_backup_lsn + wal_seg_size then return vector of such LSNs
        let res : Vec<(Lsn, PathBuf)> = Vec::new();

        return res;
    }
}
