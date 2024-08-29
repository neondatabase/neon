//! Safekeeper timeline has a background task which is subscribed to `commit_lsn`
//! and `flush_lsn` updates. After the partial segment was updated (`flush_lsn`
//! was changed), the segment will be uploaded to S3 in about 15 minutes.
//!
//! The filename format for partial segments is
//! `Segment_Term_Flush_Commit_skNN.partial`, where:
//! - `Segment` – the segment name, like `000000010000000000000001`
//! - `Term` – current term
//! - `Flush` – flush_lsn in hex format `{:016X}`, e.g. `00000000346BC568`
//! - `Commit` – commit_lsn in the same hex format
//! - `NN` – safekeeper_id, like `1`
//!
//! The full object name example:
//! `000000010000000000000002_2_0000000002534868_0000000002534410_sk1.partial`
//!
//! Each safekeeper will keep info about remote partial segments in its control
//! file. Code updates state in the control file before doing any S3 operations.
//! This way control file stores information about all potentially existing
//! remote partial segments and can clean them up after uploading a newer version.

use std::sync::Arc;

use camino::Utf8PathBuf;
use postgres_ffi::{XLogFileName, XLogSegNo, PG_TLI};
use rand::Rng;
use remote_storage::RemotePath;
use serde::{Deserialize, Serialize};

use tracing::{debug, error, info, instrument};
use utils::lsn::Lsn;

use crate::{
    metrics::{PARTIAL_BACKUP_UPLOADED_BYTES, PARTIAL_BACKUP_UPLOADS},
    safekeeper::Term,
    timeline::Timeline,
    wal_backup, SafeKeeperConf,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UploadStatus {
    /// Upload is in progress
    InProgress,
    /// Upload is finished
    Uploaded,
    /// Deletion is in progress
    Deleting,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartialRemoteSegment {
    pub status: UploadStatus,
    pub name: String,
    pub commit_lsn: Lsn,
    pub flush_lsn: Lsn,
    pub term: Term,
}

impl PartialRemoteSegment {
    fn eq_without_status(&self, other: &Self) -> bool {
        self.name == other.name
            && self.commit_lsn == other.commit_lsn
            && self.flush_lsn == other.flush_lsn
            && self.term == other.term
    }
}

// NB: these structures are a part of a control_file, you can't change them without
// changing the control file format version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct State {
    pub segments: Vec<PartialRemoteSegment>,
}

impl State {
    /// Find an Uploaded segment. There should be only one Uploaded segment at a time.
    fn uploaded_segment(&self) -> Option<PartialRemoteSegment> {
        self.segments
            .iter()
            .find(|seg| seg.status == UploadStatus::Uploaded)
            .cloned()
    }
}

struct PartialBackup {
    wal_seg_size: usize,
    tli: Arc<Timeline>,
    conf: SafeKeeperConf,
    local_prefix: Utf8PathBuf,
    remote_prefix: Utf8PathBuf,

    state: State,
}

// Read-only methods for getting segment names
impl PartialBackup {
    fn segno(&self, lsn: Lsn) -> XLogSegNo {
        lsn.segment_number(self.wal_seg_size)
    }

    fn segment_name(&self, segno: u64) -> String {
        XLogFileName(PG_TLI, segno, self.wal_seg_size)
    }

    fn remote_segment_name(
        &self,
        segno: u64,
        term: u64,
        commit_lsn: Lsn,
        flush_lsn: Lsn,
    ) -> String {
        format!(
            "{}_{}_{:016X}_{:016X}_sk{}.partial",
            self.segment_name(segno),
            term,
            flush_lsn.0,
            commit_lsn.0,
            self.conf.my_id.0,
        )
    }

    fn local_segment_name(&self, segno: u64) -> String {
        format!("{}.partial", self.segment_name(segno))
    }
}

impl PartialBackup {
    /// Takes a lock to read actual safekeeper state and returns a segment that should be uploaded.
    async fn prepare_upload(&self) -> PartialRemoteSegment {
        // this operation takes a lock to get the actual state
        let sk_info = self.tli.get_safekeeper_info(&self.conf).await;
        let flush_lsn = Lsn(sk_info.flush_lsn);
        let commit_lsn = Lsn(sk_info.commit_lsn);
        let term = sk_info.term;
        let segno = self.segno(flush_lsn);

        let name = self.remote_segment_name(segno, term, commit_lsn, flush_lsn);

        PartialRemoteSegment {
            status: UploadStatus::InProgress,
            name,
            commit_lsn,
            flush_lsn,
            term,
        }
    }

    /// Reads segment from disk and uploads it to the remote storage.
    async fn upload_segment(&mut self, prepared: PartialRemoteSegment) -> anyhow::Result<()> {
        let flush_lsn = prepared.flush_lsn;
        let segno = self.segno(flush_lsn);

        // We're going to backup bytes from the start of the segment up to flush_lsn.
        let backup_bytes = flush_lsn.segment_offset(self.wal_seg_size);

        let local_path = self.local_prefix.join(self.local_segment_name(segno));
        let remote_path = RemotePath::new(self.remote_prefix.join(&prepared.name).as_ref())?;

        // Upload first `backup_bytes` bytes of the segment to the remote storage.
        wal_backup::backup_partial_segment(&local_path, &remote_path, backup_bytes).await?;
        PARTIAL_BACKUP_UPLOADED_BYTES.inc_by(backup_bytes as u64);

        // We uploaded the segment, now let's verify that the data is still actual.
        // If the term changed, we cannot guarantee the validity of the uploaded data.
        // If the term is the same, we know the data is not corrupted.
        let sk_info = self.tli.get_safekeeper_info(&self.conf).await;
        if sk_info.term != prepared.term {
            anyhow::bail!("term changed during upload");
        }
        assert!(prepared.commit_lsn <= Lsn(sk_info.commit_lsn));
        assert!(prepared.flush_lsn <= Lsn(sk_info.flush_lsn));

        Ok(())
    }

    /// Write new state to disk. If in-memory and on-disk states diverged, returns an error.
    async fn commit_state(&mut self, new_state: State) -> anyhow::Result<()> {
        self.tli
            .map_control_file(|cf| {
                if cf.partial_backup != self.state {
                    let memory = self.state.clone();
                    self.state = cf.partial_backup.clone();
                    anyhow::bail!(
                        "partial backup state diverged, memory={:?}, disk={:?}",
                        memory,
                        cf.partial_backup
                    );
                }

                cf.partial_backup = new_state.clone();
                Ok(())
            })
            .await?;
        // update in-memory state
        self.state = new_state;
        Ok(())
    }

    /// Upload the latest version of the partial segment and garbage collect older versions.
    #[instrument(name = "upload", skip_all, fields(name = %prepared.name))]
    async fn do_upload(&mut self, prepared: &PartialRemoteSegment) -> anyhow::Result<()> {
        info!("starting upload {:?}", prepared);

        let state_0 = self.state.clone();
        let state_1 = {
            let mut state = state_0.clone();
            state.segments.push(prepared.clone());
            state
        };

        // we're going to upload a new segment, let's write it to disk to make GC later
        self.commit_state(state_1).await?;

        self.upload_segment(prepared.clone()).await?;

        let state_2 = {
            let mut state = state_0.clone();
            for seg in state.segments.iter_mut() {
                seg.status = UploadStatus::Deleting;
            }
            let mut actual_remote_segment = prepared.clone();
            actual_remote_segment.status = UploadStatus::Uploaded;
            state.segments.push(actual_remote_segment);
            state
        };

        // we've uploaded new segment, it's actual, all other segments should be GCed
        self.commit_state(state_2).await?;
        self.gc().await?;

        Ok(())
    }

    /// Delete all non-Uploaded segments from the remote storage. There should be only one
    /// Uploaded segment at a time.
    #[instrument(name = "gc", skip_all)]
    async fn gc(&mut self) -> anyhow::Result<()> {
        let mut segments_to_delete = vec![];

        let new_segments: Vec<PartialRemoteSegment> = self
            .state
            .segments
            .iter()
            .filter_map(|seg| {
                if seg.status == UploadStatus::Uploaded {
                    Some(seg.clone())
                } else {
                    segments_to_delete.push(seg.name.clone());
                    None
                }
            })
            .collect();

        info!("deleting objects: {:?}", segments_to_delete);
        let mut objects_to_delete = vec![];
        for seg in segments_to_delete.iter() {
            let remote_path = RemotePath::new(self.remote_prefix.join(seg).as_ref())?;
            objects_to_delete.push(remote_path);
        }

        // removing segments from remote storage
        wal_backup::delete_objects(&objects_to_delete).await?;

        // now we can update the state on disk
        let new_state = {
            let mut state = self.state.clone();
            state.segments = new_segments;
            state
        };
        self.commit_state(new_state).await?;

        Ok(())
    }
}

#[instrument(name = "Partial backup", skip_all, fields(ttid = %tli.ttid))]
pub async fn main_task(tli: Arc<Timeline>, conf: SafeKeeperConf) {
    debug!("started");
    let await_duration = conf.partial_backup_timeout;

    // sleep for random time to avoid thundering herd
    {
        let randf64 = rand::thread_rng().gen_range(0.0..1.0);
        let sleep_duration = await_duration.mul_f64(randf64);
        tokio::time::sleep(sleep_duration).await;
    }

    let (_, persistent_state) = tli.get_state().await;
    let mut commit_lsn_rx = tli.get_commit_lsn_watch_rx();
    let mut flush_lsn_rx = tli.get_term_flush_lsn_watch_rx();
    let wal_seg_size = tli.get_wal_seg_size().await;

    let local_prefix = tli.timeline_dir.clone();
    let remote_prefix = match tli.timeline_dir.strip_prefix(&conf.workdir) {
        Ok(path) => path.to_owned(),
        Err(e) => {
            error!("failed to strip workspace dir prefix: {:?}", e);
            return;
        }
    };

    let mut backup = PartialBackup {
        wal_seg_size,
        tli,
        state: persistent_state.partial_backup,
        conf,
        local_prefix,
        remote_prefix,
    };

    debug!("state: {:?}", backup.state);

    'outer: loop {
        // wait until we have something to upload
        let uploaded_segment = backup.state.uploaded_segment();
        if let Some(seg) = &uploaded_segment {
            // if we already uploaded something, wait until we have something new
            while flush_lsn_rx.borrow().lsn == seg.flush_lsn
                && *commit_lsn_rx.borrow() == seg.commit_lsn
                && flush_lsn_rx.borrow().term == seg.term
            {
                tokio::select! {
                    _ = backup.tli.cancel.cancelled() => {
                        info!("timeline canceled");
                        return;
                    }
                    _ = commit_lsn_rx.changed() => {}
                    _ = flush_lsn_rx.changed() => {}
                }
            }
        }

        // if we don't have any data and zero LSNs, wait for something
        while flush_lsn_rx.borrow().lsn == Lsn(0) {
            tokio::select! {
                _ = backup.tli.cancel.cancelled() => {
                    info!("timeline canceled");
                    return;
                }
                _ = flush_lsn_rx.changed() => {}
            }
        }

        // fixing the segno and waiting some time to prevent reuploading the same segment too often
        let pending_segno = backup.segno(flush_lsn_rx.borrow().lsn);
        let timeout = tokio::time::sleep(await_duration);
        tokio::pin!(timeout);
        let mut timeout_expired = false;

        // waiting until timeout expires OR segno changes
        'inner: loop {
            tokio::select! {
                _ = backup.tli.cancel.cancelled() => {
                    info!("timeline canceled");
                    return;
                }
                _ = commit_lsn_rx.changed() => {}
                _ = flush_lsn_rx.changed() => {
                    let segno = backup.segno(flush_lsn_rx.borrow().lsn);
                    if segno != pending_segno {
                        // previous segment is no longer partial, aborting the wait
                        break 'inner;
                    }
                }
                _ = &mut timeout => {
                    // timeout expired, now we are ready for upload
                    timeout_expired = true;
                    break 'inner;
                }
            }
        }

        if !timeout_expired {
            // likely segno has changed, let's try again in the next iteration
            continue 'outer;
        }

        let prepared = backup.prepare_upload().await;
        if let Some(seg) = &uploaded_segment {
            if seg.eq_without_status(&prepared) {
                // we already uploaded this segment, nothing to do
                continue 'outer;
            }
        }

        match backup.do_upload(&prepared).await {
            Ok(()) => {
                debug!(
                    "uploaded {} up to flush_lsn {}",
                    prepared.name, prepared.flush_lsn
                );
                PARTIAL_BACKUP_UPLOADS.with_label_values(&["ok"]).inc();
            }
            Err(e) => {
                info!("failed to upload {}: {:#}", prepared.name, e);
                PARTIAL_BACKUP_UPLOADS.with_label_values(&["error"]).inc();
            }
        }
    }
}
