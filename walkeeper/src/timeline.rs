//! This module contains timeline id -> safekeeper state map with file-backed
//! persistence and support for interaction between sending and receiving wal.

use anyhow::{bail, ensure, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use lazy_static::lazy_static;
use postgres_ffi::xlog_utils::{find_end_of_wal, PG_TLI};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tracing::*;
use zenith_metrics::{register_histogram_vec, Histogram, HistogramVec, DISK_WRITE_SECONDS_BUCKETS};
use zenith_utils::bin_ser::LeSer;
use zenith_utils::lsn::Lsn;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use crate::replication::HotStandbyFeedback;
use crate::safekeeper::{
    AcceptorProposerMessage, ProposerAcceptorMessage, SafeKeeper, SafeKeeperState, ServerInfo,
    Storage, SK_FORMAT_VERSION, SK_MAGIC,
};
use crate::upgrade::upgrade_control_file;
use crate::SafeKeeperConf;
use postgres_ffi::xlog_utils::{XLogFileName, XLOG_BLCKSZ};
use std::convert::TryInto;

// contains persistent metadata for safekeeper
const CONTROL_FILE_NAME: &str = "safekeeper.control";
// needed to atomically update the state using `rename`
const CONTROL_FILE_NAME_PARTIAL: &str = "safekeeper.control.partial";
const POLL_STATE_TIMEOUT: Duration = Duration::from_secs(1);
pub const CHECKSUM_SIZE: usize = std::mem::size_of::<u32>();

/// Replica status: host standby feedback + disk consistent lsn
#[derive(Debug, Clone, Copy)]
pub struct ReplicaState {
    /// last known lsn received by replica
    pub last_received_lsn: Lsn, // None means we don't know
    /// combined disk_consistent_lsn of pageservers
    pub disk_consistent_lsn: Lsn,
    /// combined hot standby feedback from all replicas
    pub hs_feedback: HotStandbyFeedback,
}

impl Default for ReplicaState {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicaState {
    pub fn new() -> ReplicaState {
        ReplicaState {
            last_received_lsn: Lsn::MAX,
            disk_consistent_lsn: Lsn(u64::MAX),
            hs_feedback: HotStandbyFeedback {
                ts: 0,
                xmin: u64::MAX,
                catalog_xmin: u64::MAX,
            },
        }
    }
}

/// Shared state associated with database instance (tenant)
struct SharedState {
    /// Safekeeper object
    sk: SafeKeeper<FileStorage>,
    /// For receiving-sending wal cooperation
    /// quorum commit LSN we've notified walsenders about
    notified_commit_lsn: Lsn,
    // Set stop_lsn to inform WAL senders that it's time to stop sending WAL,
    // so that it send all wal up stop_lsn and can safely exit streaming connections.
    stop_lsn: Option<Lsn>,
    /// State of replicas
    replicas: Vec<Option<ReplicaState>>,
}

// A named boolean.
#[derive(Debug)]
pub enum CreateControlFile {
    True,
    False,
}

lazy_static! {
    static ref PERSIST_CONTROL_FILE_SECONDS: HistogramVec = register_histogram_vec!(
        "safekeeper_persist_control_file_seconds",
        "Seconds to persist and sync control file, grouped by timeline",
        &["timeline_id"],
        DISK_WRITE_SECONDS_BUCKETS.to_vec()
    )
    .expect("Failed to register safekeeper_persist_control_file_seconds histogram vec");
}

impl SharedState {
    /// Get combined state of all alive replicas
    pub fn get_replicas_state(&self) -> ReplicaState {
        let mut acc = ReplicaState::new();
        for state in self.replicas.iter().flatten() {
            acc.hs_feedback.ts = max(acc.hs_feedback.ts, state.hs_feedback.ts);
            acc.hs_feedback.xmin = min(acc.hs_feedback.xmin, state.hs_feedback.xmin);
            acc.hs_feedback.catalog_xmin =
                min(acc.hs_feedback.catalog_xmin, state.hs_feedback.catalog_xmin);
            acc.disk_consistent_lsn = Lsn::min(acc.disk_consistent_lsn, state.disk_consistent_lsn);
            // currently not used, but update it to be consistent
            acc.last_received_lsn = Lsn::min(acc.last_received_lsn, state.last_received_lsn);
        }
        acc
    }

    /// Assign new replica ID. We choose first empty cell in the replicas vector
    /// or extend the vector if there are not free items.
    pub fn add_replica(&mut self, state: ReplicaState) -> usize {
        if let Some(pos) = self.replicas.iter().position(|r| r.is_none()) {
            self.replicas[pos] = Some(state);
            return pos;
        }
        let pos = self.replicas.len();
        self.replicas.push(Some(state));
        pos
    }

    /// Restore SharedState from control file.
    /// If create=false and file doesn't exist, bails out.
    fn create_restore(
        conf: &SafeKeeperConf,
        timelineid: ZTimelineId,
        create: CreateControlFile,
    ) -> Result<Self> {
        let (file_storage, state) = FileStorage::load_from_control_file(conf, timelineid, create)
            .with_context(|| "failed to load from control file")?;
        let flush_lsn = if state.server.wal_seg_size != 0 {
            let wal_dir = conf.timeline_dir(&timelineid);
            find_end_of_wal(
                &wal_dir,
                state.server.wal_seg_size as usize,
                true,
                state.wal_start_lsn,
            )?
            .0
        } else {
            0
        };

        Ok(Self {
            notified_commit_lsn: Lsn(0),
            stop_lsn: None,
            sk: SafeKeeper::new(Lsn(flush_lsn), file_storage, state),
            replicas: Vec::new(),
        })
    }
}

/// Database instance (tenant)
pub struct Timeline {
    pub timelineid: ZTimelineId,
    mutex: Mutex<SharedState>,
    /// conditional variable used to notify wal senders
    cond: Condvar,
}

impl Timeline {
    fn new(timelineid: ZTimelineId, shared_state: SharedState) -> Timeline {
        Timeline {
            timelineid,
            mutex: Mutex::new(shared_state),
            cond: Condvar::new(),
        }
    }

    /// Timed wait for an LSN to be committed.
    ///
    /// Returns the last committed LSN, which will be at least
    /// as high as the LSN waited for, or None if timeout expired.
    ///
    pub fn wait_for_lsn(&self, lsn: Lsn) -> Option<Lsn> {
        let mut shared_state = self.mutex.lock().unwrap();
        loop {
            let commit_lsn = shared_state.notified_commit_lsn;
            // This must be `>`, not `>=`.
            if commit_lsn > lsn {
                return Some(commit_lsn);
            }
            let result = self
                .cond
                .wait_timeout(shared_state, POLL_STATE_TIMEOUT)
                .unwrap();
            if result.1.timed_out() {
                return None;
            }
            shared_state = result.0
        }
    }

    // Notify caught-up WAL senders about new WAL data received
    pub fn notify_wal_senders(&self, commit_lsn: Lsn) {
        let mut shared_state = self.mutex.lock().unwrap();
        if shared_state.notified_commit_lsn < commit_lsn {
            shared_state.notified_commit_lsn = commit_lsn;
            self.cond.notify_all();
        }
    }

    // Notify WAL senders that it's time to stop sending WAL
    pub fn stop_streaming(&self) {
        let mut shared_state = self.mutex.lock().unwrap();
        // Ensure that safekeeper sends WAL up to the last known committed LSN.
        // It guarantees that pageserver will receive all the latest data
        // before walservice disconnects.
        shared_state.stop_lsn = Some(shared_state.notified_commit_lsn);
        trace!(
            "Stopping WAL senders. stop_lsn: {}",
            shared_state.notified_commit_lsn
        );
    }

    // Reset stop_lsn notification,
    // so that WAL senders will continue sending WAL
    pub fn continue_streaming(&self) {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.stop_lsn = None;
    }

    // Check if it's time to stop streaming to the given replica.
    //
    // Do not stop streaming until replica is caught up with the stop_lsn.
    // This is not necessary for correctness, just an optimization to
    // be able to remove WAL from safekeeper and decrease amount of work
    // on the next start.
    pub fn check_stop_streaming(&self, replica_id: usize) -> bool {
        let shared_state = self.mutex.lock().unwrap();

        // If stop_lsn is set, it's time to shutdown streaming.
        if let Some(stop_lsn_request) = shared_state.stop_lsn {
            let replica_state = shared_state.replicas[replica_id].unwrap();
            // There is no data to stream, so other clauses don't matter.
            if shared_state.notified_commit_lsn == Lsn(0) {
                return true;
            }
            // Lsn::MAX means that we don't know the latest LSN yet.
            // That may be a new replica, give it a chance to catch up.
            if replica_state.last_received_lsn != Lsn::MAX
            // If replica is fully caught up, disconnect it.
            && stop_lsn_request <= replica_state.last_received_lsn
            {
                return true;
            }
        }
        false
    }

    /// Pass arrived message to the safekeeper.
    pub fn process_msg(
        &self,
        msg: &ProposerAcceptorMessage,
    ) -> Result<Option<AcceptorProposerMessage>> {
        let mut rmsg: Option<AcceptorProposerMessage>;
        let commit_lsn: Lsn;
        {
            let mut shared_state = self.mutex.lock().unwrap();
            rmsg = shared_state.sk.process_msg(msg)?;
            // locally available commit lsn. flush_lsn can be smaller than
            // commit_lsn if we are catching up safekeeper.
            commit_lsn = shared_state.sk.commit_lsn;

            // if this is AppendResponse, fill in proper hot standby feedback and disk consistent lsn
            if let Some(AcceptorProposerMessage::AppendResponse(ref mut resp)) = rmsg {
                let state = shared_state.get_replicas_state();
                resp.hs_feedback = state.hs_feedback;
                resp.disk_consistent_lsn = state.disk_consistent_lsn;
                // XXX Do we need to add state.last_received_lsn to resp?
            }
        }
        // Ping wal sender that new data might be available.
        self.notify_wal_senders(commit_lsn);
        Ok(rmsg)
    }

    pub fn get_info(&self) -> SafeKeeperState {
        self.mutex.lock().unwrap().sk.s.clone()
    }

    pub fn add_replica(&self, state: ReplicaState) -> usize {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.add_replica(state)
    }

    pub fn update_replica_state(&self, id: usize, state: Option<ReplicaState>) {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.replicas[id] = state;
    }

    pub fn get_end_of_wal(&self) -> Lsn {
        let shared_state = self.mutex.lock().unwrap();
        shared_state.sk.flush_lsn
    }
}

// Utilities needed by various Connection-like objects
pub trait TimelineTools {
    fn set(
        &mut self,
        conf: &SafeKeeperConf,
        tenant_id: ZTenantId,
        timeline_id: ZTimelineId,
        create: CreateControlFile,
    ) -> Result<()>;

    fn get(&self) -> &Arc<Timeline>;
}

impl TimelineTools for Option<Arc<Timeline>> {
    fn set(
        &mut self,
        conf: &SafeKeeperConf,
        tenant_id: ZTenantId,
        timeline_id: ZTimelineId,
        create: CreateControlFile,
    ) -> Result<()> {
        // We will only set the timeline once. If it were to ever change,
        // anyone who cloned the Arc would be out of date.
        assert!(self.is_none());
        *self = Some(GlobalTimelines::get(conf, tenant_id, timeline_id, create)?);
        Ok(())
    }

    fn get(&self) -> &Arc<Timeline> {
        self.as_ref().unwrap()
    }
}

lazy_static! {
    pub static ref TIMELINES: Mutex<HashMap<(ZTenantId, ZTimelineId), Arc<Timeline>>> =
        Mutex::new(HashMap::new());
}

/// A zero-sized struct used to manage access to the global timelines map.
pub struct GlobalTimelines;

impl GlobalTimelines {
    /// Get a timeline with control file loaded from the global TIMELINES map.
    /// If control file doesn't exist and create=false, bails out.
    pub fn get(
        conf: &SafeKeeperConf,
        tenant_id: ZTenantId,
        timeline_id: ZTimelineId,
        create: CreateControlFile,
    ) -> Result<Arc<Timeline>> {
        let mut timelines = TIMELINES.lock().unwrap();

        match timelines.get(&(tenant_id, timeline_id)) {
            Some(result) => Ok(Arc::clone(result)),
            None => {
                info!(
                    "creating timeline dir {}, create is {:?}",
                    timeline_id, create
                );
                fs::create_dir_all(timeline_id.to_string())?;

                let shared_state = SharedState::create_restore(conf, timeline_id, create)
                    .with_context(|| "failed to restore shared state")?;

                let new_tli = Arc::new(Timeline::new(timeline_id, shared_state));
                timelines.insert((tenant_id, timeline_id), Arc::clone(&new_tli));
                Ok(new_tli)
            }
        }
    }
}

#[derive(Debug)]
struct FileStorage {
    // save timeline dir to avoid reconstructing it every time
    timeline_dir: PathBuf,
    conf: SafeKeeperConf,
    persist_control_file_seconds: Histogram,
}

impl FileStorage {
    // Check the magic/version in the on-disk data and deserialize it, if possible.
    fn deser_sk_state(buf: &mut &[u8]) -> Result<SafeKeeperState> {
        // Read the version independent part
        let magic = buf.read_u32::<LittleEndian>()?;
        if magic != SK_MAGIC {
            bail!(
                "bad control file magic: {:X}, expected {:X}",
                magic,
                SK_MAGIC
            );
        }
        let version = buf.read_u32::<LittleEndian>()?;
        if version == SK_FORMAT_VERSION {
            let res = SafeKeeperState::des(buf)?;
            return Ok(res);
        }
        // try to upgrade
        upgrade_control_file(buf, version)
    }

    /// Fetch and lock control file (prevent running more than one instance of safekeeper)
    /// If create=false and file doesn't exist, bails out.
    fn load_from_control_file(
        conf: &SafeKeeperConf,
        timelineid: ZTimelineId,
        create: CreateControlFile,
    ) -> Result<(FileStorage, SafeKeeperState)> {
        let timeline_dir = conf.timeline_dir(&timelineid);

        let control_file_path = timeline_dir.join(CONTROL_FILE_NAME);

        info!(
            "loading control file {}, create={:?}",
            control_file_path.display(),
            create,
        );

        let mut control_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(matches!(create, CreateControlFile::True))
            .open(&control_file_path)
            .with_context(|| {
                format!(
                    "failed to open control file at {}",
                    control_file_path.display(),
                )
            })?;

        // Empty file is legit on 'create', don't try to deser from it.
        let state = if control_file.metadata().unwrap().len() == 0 {
            if let CreateControlFile::False = create {
                bail!("control file is empty");
            }
            SafeKeeperState::new()
        } else {
            let mut buf = Vec::new();
            control_file
                .read_to_end(&mut buf)
                .with_context(|| "failed to read control file")?;

            let calculated_checksum = crc32c::crc32c(&buf[..buf.len() - CHECKSUM_SIZE]);

            let expected_checksum_bytes: &[u8; CHECKSUM_SIZE] =
                buf[buf.len() - CHECKSUM_SIZE..].try_into()?;
            let expected_checksum = u32::from_le_bytes(*expected_checksum_bytes);

            ensure!(
                calculated_checksum == expected_checksum,
                format!(
                    "safekeeper control file checksum mismatch: expected {} got {}",
                    expected_checksum, calculated_checksum
                )
            );

            FileStorage::deser_sk_state(&mut &buf[..buf.len() - CHECKSUM_SIZE]).with_context(
                || format!("while reading control file {}", control_file_path.display(),),
            )?
        };

        let timelineid_str = format!("{}", timelineid);

        Ok((
            FileStorage {
                timeline_dir,
                conf: conf.clone(),
                persist_control_file_seconds: PERSIST_CONTROL_FILE_SECONDS
                    .with_label_values(&[&timelineid_str]),
            },
            state,
        ))
    }
}

impl Storage for FileStorage {
    // persists state durably to underlying storage
    // for description see https://lwn.net/Articles/457667/
    fn persist(&mut self, s: &SafeKeeperState) -> Result<()> {
        let _timer = &self.persist_control_file_seconds.start_timer();

        // write data to safekeeper.control.partial
        let control_partial_path = self.timeline_dir.join(CONTROL_FILE_NAME_PARTIAL);
        let mut control_partial = File::create(&control_partial_path).with_context(|| {
            format!(
                "failed to create partial control file at: {}",
                &control_partial_path.display()
            )
        })?;
        let mut buf: Vec<u8> = Vec::new();
        buf.write_u32::<LittleEndian>(SK_MAGIC)?;
        buf.write_u32::<LittleEndian>(SK_FORMAT_VERSION)?;
        s.ser_into(&mut buf)?;

        // calculate checksum before resize
        let checksum = crc32c::crc32c(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());

        control_partial.write_all(&buf).with_context(|| {
            format!(
                "failed to write safekeeper state into control file at: {}",
                control_partial_path.display()
            )
        })?;

        // fsync the file
        control_partial.sync_all().with_context(|| {
            format!(
                "failed to sync partial control file at {}",
                control_partial_path.display()
            )
        })?;

        let control_path = self.timeline_dir.join(CONTROL_FILE_NAME);

        // rename should be atomic
        fs::rename(&control_partial_path, &control_path)?;
        // this sync is not required by any standard but postgres does this (see durable_rename)
        File::open(&control_path)
            .and_then(|f| f.sync_all())
            .with_context(|| {
                format!(
                    "failed to sync control file at: {}",
                    &control_path.display()
                )
            })?;

        // fsync the directory (linux specific)
        File::open(&self.timeline_dir)
            .and_then(|f| f.sync_all())
            .with_context(|| "failed to sync control file directory")?;
        Ok(())
    }

    fn write_wal(&mut self, server: &ServerInfo, startpos: Lsn, buf: &[u8]) -> Result<()> {
        let mut bytes_left: usize = buf.len();
        let mut bytes_written: usize = 0;
        let mut partial;
        let mut start_pos = startpos;
        const ZERO_BLOCK: &[u8] = &[0u8; XLOG_BLCKSZ];
        let wal_seg_size = server.wal_seg_size as usize;
        let ztli = server.ztli;

        /* Extract WAL location for this block */
        let mut xlogoff = start_pos.segment_offset(wal_seg_size) as usize;

        while bytes_left != 0 {
            let bytes_to_write;

            /*
             * If crossing a WAL boundary, only write up until we reach wal
             * segment size.
             */
            if xlogoff + bytes_left > wal_seg_size {
                bytes_to_write = wal_seg_size - xlogoff;
            } else {
                bytes_to_write = bytes_left;
            }

            /* Open file */
            let segno = start_pos.segment_number(wal_seg_size);
            // note: we basically don't support changing pg timeline
            let wal_file_name = XLogFileName(PG_TLI, segno, wal_seg_size);
            let wal_file_path = self.conf.timeline_dir(&ztli).join(wal_file_name.clone());
            let wal_file_partial_path = self
                .conf
                .timeline_dir(&ztli)
                .join(wal_file_name.clone() + ".partial");

            {
                let mut wal_file: File;
                /* Try to open already completed segment */
                if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_path) {
                    wal_file = file;
                    partial = false;
                } else if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_partial_path)
                {
                    /* Try to open existed partial file */
                    wal_file = file;
                    partial = true;
                } else {
                    /* Create and fill new partial file */
                    partial = true;
                    match OpenOptions::new()
                        .create(true)
                        .write(true)
                        .open(&wal_file_partial_path)
                    {
                        Ok(mut file) => {
                            for _ in 0..(wal_seg_size / XLOG_BLCKSZ) {
                                file.write_all(ZERO_BLOCK)?;
                            }
                            wal_file = file;
                        }
                        Err(e) => {
                            error!("Failed to open log file {:?}: {}", &wal_file_path, e);
                            return Err(e.into());
                        }
                    }
                }
                wal_file.seek(SeekFrom::Start(xlogoff as u64))?;
                wal_file.write_all(&buf[bytes_written..(bytes_written + bytes_to_write)])?;

                // Flush file, if not said otherwise
                if !self.conf.no_sync {
                    wal_file.sync_all()?;
                }
            }
            /* Write was successful, advance our position */
            bytes_written += bytes_to_write;
            bytes_left -= bytes_to_write;
            start_pos += bytes_to_write as u64;
            xlogoff += bytes_to_write;

            /* Did we reach the end of a WAL segment? */
            if start_pos.segment_offset(wal_seg_size) == 0 {
                xlogoff = 0;
                if partial {
                    fs::rename(&wal_file_partial_path, &wal_file_path)?;
                }
            }
        }
        Ok(())
    }

    fn truncate_wal(&mut self, server: &ServerInfo, end_pos: Lsn) -> Result<()> {
        let partial;
        const ZERO_BLOCK: &[u8] = &[0u8; XLOG_BLCKSZ];
        let wal_seg_size = server.wal_seg_size as usize;
        let ztli = server.ztli;

        /* Extract WAL location for this block */
        let mut xlogoff = end_pos.segment_offset(wal_seg_size) as usize;

        /* Open file */
        let mut segno = end_pos.segment_number(wal_seg_size);
        // note: we basically don't support changing pg timeline
        let wal_file_name = XLogFileName(PG_TLI, segno, wal_seg_size);
        let wal_file_path = self
            .conf
            .workdir
            .join(ztli.to_string())
            .join(wal_file_name.clone());
        let wal_file_partial_path = self
            .conf
            .workdir
            .join(ztli.to_string())
            .join(wal_file_name + ".partial");
        {
            let mut wal_file: File;
            /* Try to open already completed segment */
            if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_path) {
                wal_file = file;
                partial = false;
            } else {
                wal_file = OpenOptions::new()
                    .write(true)
                    .open(&wal_file_partial_path)?;
                partial = true;
            }
            wal_file.seek(SeekFrom::Start(xlogoff as u64))?;
            while xlogoff < wal_seg_size {
                let bytes_to_write = min(XLOG_BLCKSZ, wal_seg_size - xlogoff);
                wal_file.write_all(&ZERO_BLOCK[0..bytes_to_write])?;
                xlogoff += bytes_to_write;
            }
            // Flush file, if not said otherwise
            if !self.conf.no_sync {
                wal_file.sync_all()?;
            }
        }
        if !partial {
            // Make segment partial once again
            fs::rename(&wal_file_path, &wal_file_partial_path)?;
        }
        // Remove all subsequent segments
        loop {
            segno += 1;
            let wal_file_name = XLogFileName(PG_TLI, segno, wal_seg_size);
            let wal_file_path = self
                .conf
                .workdir
                .join(ztli.to_string())
                .join(wal_file_name.clone());
            let wal_file_partial_path = self
                .conf
                .workdir
                .join(ztli.to_string())
                .join(wal_file_name.clone() + ".partial");
            // TODO: better use fs::try_exists which is currenty avaialble only in nightly build
            if wal_file_path.exists() {
                fs::remove_file(&wal_file_path)?;
            } else if wal_file_partial_path.exists() {
                fs::remove_file(&wal_file_partial_path)?;
            } else {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::FileStorage;
    use crate::{
        safekeeper::{SafeKeeperState, Storage},
        timeline::{CreateControlFile, CONTROL_FILE_NAME},
        SafeKeeperConf,
    };
    use anyhow::Result;
    use std::fs;
    use zenith_utils::{lsn::Lsn, zid::ZTimelineId};

    fn stub_conf() -> SafeKeeperConf {
        let workdir = tempfile::tempdir().unwrap().into_path();
        SafeKeeperConf {
            workdir,
            ..Default::default()
        }
    }

    fn load_from_control_file(
        conf: &SafeKeeperConf,
        timeline_id: ZTimelineId,
        create: CreateControlFile,
    ) -> Result<(FileStorage, SafeKeeperState)> {
        fs::create_dir_all(&conf.timeline_dir(&timeline_id))
            .expect("failed to create timeline dir");
        FileStorage::load_from_control_file(conf, timeline_id, create)
    }

    #[test]
    fn test_read_write_safekeeper_state() {
        let conf = stub_conf();
        let timeline_id = ZTimelineId::generate();
        {
            let (mut storage, mut state) =
                load_from_control_file(&conf, timeline_id, CreateControlFile::True)
                    .expect("failed to read state");
            // change something
            state.wal_start_lsn = Lsn(42);
            storage.persist(&state).expect("failed to persist state");
        }

        let (_, state) = load_from_control_file(&conf, timeline_id, CreateControlFile::False)
            .expect("failed to read state");
        assert_eq!(state.wal_start_lsn, Lsn(42));
    }

    #[test]
    fn test_safekeeper_state_checksum_mismatch() {
        let conf = stub_conf();
        let timeline_id = ZTimelineId::generate();
        {
            let (mut storage, mut state) =
                load_from_control_file(&conf, timeline_id, CreateControlFile::True)
                    .expect("failed to read state");
            // change something
            state.wal_start_lsn = Lsn(42);
            storage.persist(&state).expect("failed to persist state");
        }
        let control_path = conf.timeline_dir(&timeline_id).join(CONTROL_FILE_NAME);
        let mut data = fs::read(&control_path).unwrap();
        data[0] += 1; // change the first byte of the file to fail checksum validation
        fs::write(&control_path, &data).expect("failed to write control file");

        match load_from_control_file(&conf, timeline_id, CreateControlFile::False) {
            Err(err) => assert!(err
                .to_string()
                .contains("safekeeper control file checksum mismatch")),
            Ok(_) => panic!("expected error"),
        }
    }
}
