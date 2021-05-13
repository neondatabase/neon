//!
//!   WAL service listens for client connections and
//!   receive WAL from wal_proposer and send it to WAL receivers
//!
use anyhow::{bail, Result};
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use fs2::FileExt;
use lazy_static::lazy_static;
use log::*;
use postgres::{Client, NoTls};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::mem;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::str;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use zenith_utils::bin_ser::LeSer;
use zenith_utils::lsn::Lsn;

use crate::pq_protocol::*;
use crate::WalAcceptorConf;
use pageserver::ZTimelineId;
use postgres_ffi::xlog_utils::{
    find_end_of_wal, TimeLineID, TimestampTz, XLogFileName, XLOG_BLCKSZ,
};

type FullTransactionId = u64;

const SK_MAGIC: u32 = 0xCafeCeefu32;
const SK_FORMAT_VERSION: u32 = 1;
const SK_PROTOCOL_VERSION: u32 = 1;
const UNKNOWN_SERVER_VERSION: u32 = 0;
const END_REPLICATION_MARKER: Lsn = Lsn::MAX;
const MAX_SEND_SIZE: usize = XLOG_BLCKSZ * 16;
const XLOG_HDR_SIZE: usize = 1 + 8 * 3; /* 'w' + startPos + walEnd + timestamp */
const LIBPQ_HDR_SIZE: usize = 5; /* 1 byte with message type + 4 bytes length */
const LIBPQ_MSG_SIZE_OFFS: usize = 1;
const CONTROL_FILE_NAME: &str = "safekeeper.control";
const END_OF_STREAM: Lsn = Lsn(0);

/// Unique node identifier used by Paxos
#[derive(Debug, Clone, Copy, Ord, PartialOrd, PartialEq, Eq, Serialize, Deserialize)]
struct NodeId {
    term: u64,
    uuid: [u8; 16],
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
struct ServerInfo {
    /// proxy-safekeeper protocol version
    protocol_version: u32,
    /// Postgres server version
    pg_version: u32,
    node_id: NodeId,
    system_id: SystemId,
    /// Zenith timelineid
    timeline_id: ZTimelineId,
    wal_end: Lsn,
    timeline: TimeLineID,
    wal_seg_size: u32,
}

/// Vote request sent from proxy to safekeepers
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct RequestVote {
    node_id: NodeId,
    /// volume commit LSN
    vcl: Lsn,
    /// new epoch when safekeeper reaches vcl
    epoch: u64,
}

/// Information of about storage node
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
struct SafeKeeperInfo {
    /// magic for verifying content the control file
    magic: u32,
    /// safekeeper format version
    format_version: u32,
    /// safekeeper's epoch
    epoch: u64,
    /// information about server
    server: ServerInfo,
    /// part of WAL acknowledged by quorum
    commit_lsn: Lsn,
    /// locally flushed part of WAL
    flush_lsn: Lsn,
    /// minimal LSN which may be needed for recovery of some safekeeper: min(commit_lsn) for all safekeepers
    restart_lsn: Lsn,
}

/// Hot standby feedback received from replica
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
struct HotStandbyFeedback {
    ts: TimestampTz,
    xmin: FullTransactionId,
    catalog_xmin: FullTransactionId,
}

/// Request with WAL message sent from proxy to safekeeper.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct SafeKeeperRequest {
    /// Sender's node identifier (looks like we do not need it for TCP streaming connection)
    sender_id: NodeId,
    /// start position of message in WAL
    begin_lsn: Lsn,
    /// end position of message in WAL
    end_lsn: Lsn,
    /// restart LSN position  (minimal LSN which may be needed by proxy to perform recovery)
    restart_lsn: Lsn,
    /// LSN committed by quorum of safekeepers
    commit_lsn: Lsn,
}

/// Report safekeeper state to proxy
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct SafeKeeperResponse {
    epoch: u64,
    flush_lsn: Lsn,
    hs_feedback: HotStandbyFeedback,
}

/// Shared state associated with database instance (tenant)
#[derive(Debug)]
struct SharedState {
    /// quorum commit LSN
    commit_lsn: Lsn,
    /// information about this safekeeper
    info: SafeKeeperInfo,
    /// opened file control file handle (needed to hold exlusive file lock
    control_file: Option<File>,
    /// combined hot standby feedback from all replicas
    hs_feedback: HotStandbyFeedback,
}

/// Database instance (tenant)
#[derive(Debug)]
pub struct Timeline {
    timelineid: ZTimelineId,
    mutex: Mutex<SharedState>,
    /// conditional variable used to notify wal senders
    cond: Condvar,
}

// Useful utilities needed by various Connection-like objects
trait TimelineTools {
    fn set(&mut self, timeline_id: ZTimelineId) -> Result<()>;
    fn get(&self) -> &Arc<Timeline>;
    fn find_end_of_wal(&self, data_dir: &Path, precise: bool) -> (Lsn, TimeLineID);
}

impl TimelineTools for Option<Arc<Timeline>> {
    fn set(&mut self, timeline_id: ZTimelineId) -> Result<()> {
        // We will only set the timeline once. If it were to ever change,
        // anyone who cloned the Arc would be out of date.
        assert!(self.is_none());
        *self = Some(GlobalTimelines::store(timeline_id)?);
        Ok(())
    }

    fn get(&self) -> &Arc<Timeline> {
        self.as_ref().unwrap()
    }

    /// Find last WAL record. If "precise" is false then just locate last partial segment
    fn find_end_of_wal(&self, data_dir: &Path, precise: bool) -> (Lsn, TimeLineID) {
        let seg_size = self.get().get_info().server.wal_seg_size as usize;
        let (lsn, timeline) = find_end_of_wal(data_dir, seg_size, precise);
        (Lsn(lsn), timeline)
    }
}

/// Private data
#[derive(Debug)]
pub struct Connection {
    timeline: Option<Arc<Timeline>>,
    /// Postgres connection, buffered input
    stream_in: BufReader<TcpStream>,
    /// Postgres connection, output   FIXME: To buffer, or not to buffer? flush() is a pain.
    stream_out: TcpStream,
    /// The cached result of socket.peer_addr()
    peer_addr: SocketAddr,
    /// input buffer
    //inbuf: BytesMut,
    /// output buffer
    outbuf: BytesMut,
    /// wal acceptor configuration
    conf: WalAcceptorConf,
}

/// Serde adapter for BytesMut
///
// It's not clear whether this will be needed in the long term.
// If so, it should probably move to `zenith_utils::bin_ser`
trait NewSerializer: Serialize + DeserializeOwned {
    fn pack(&self, buf: &mut BytesMut) {
        let mut buf_w = buf.writer();
        self.ser_into(&mut buf_w).unwrap();
    }

    fn unpack(buf: &mut BytesMut) -> Self {
        let mut buf_r = buf.reader();
        Self::des_from(&mut buf_r).unwrap()
    }
}

impl<T> NewSerializer for T where T: Serialize + DeserializeOwned {}

impl SafeKeeperInfo {
    fn new() -> SafeKeeperInfo {
        SafeKeeperInfo {
            magic: SK_MAGIC,
            format_version: SK_FORMAT_VERSION,
            epoch: 0,
            server: ServerInfo {
                protocol_version: SK_PROTOCOL_VERSION, /* proxy-safekeeper protocol version */
                pg_version: UNKNOWN_SERVER_VERSION,    /* Postgres server version */
                node_id: NodeId {
                    term: 0,
                    uuid: [0; 16],
                },
                system_id: 0, /* Postgres system identifier */
                timeline_id: ZTimelineId::from([0u8; 16]),
                wal_end: Lsn(0),
                timeline: 0,
                wal_seg_size: 0,
            },
            commit_lsn: Lsn(0),  /* part of WAL acknowledged by quorum */
            flush_lsn: Lsn(0),   /* locally flushed part of WAL */
            restart_lsn: Lsn(0), /* minimal LSN which may be needed for recovery of some safekeeper */
        }
    }
}

impl HotStandbyFeedback {
    fn parse(body: &Bytes) -> HotStandbyFeedback {
        HotStandbyFeedback {
            ts: BigEndian::read_u64(&body[0..8]),
            xmin: BigEndian::read_u64(&body[8..16]),
            catalog_xmin: BigEndian::read_u64(&body[16..24]),
        }
    }
}

lazy_static! {
    pub static ref TIMELINES: Mutex<HashMap<ZTimelineId, Arc<Timeline>>> =
        Mutex::new(HashMap::new());
}

/// A zero-sized struct used to manage access to the global timelines map.
struct GlobalTimelines;

impl GlobalTimelines {
    /// Store a new timeline into the global TIMELINES map.
    fn store(timeline_id: ZTimelineId) -> Result<Arc<Timeline>> {
        let mut timelines = TIMELINES.lock().unwrap();

        match timelines.get(&timeline_id) {
            Some(result) => Ok(Arc::clone(result)),
            None => {
                info!("creating timeline dir {}", timeline_id);
                fs::create_dir_all(timeline_id.to_string())?;
                let new_tid = Arc::new(Timeline::new(timeline_id));
                timelines.insert(timeline_id, Arc::clone(&new_tid));
                Ok(new_tid)
            }
        }
    }
}

/// Accept incoming TCP connections and spawn them into a background thread.
pub fn thread_main(conf: WalAcceptorConf) -> Result<()> {
    info!("Starting wal acceptor on {}", conf.listen_addr);
    let listener = TcpListener::bind(conf.listen_addr).map_err(|e| {
        error!("failed to bind to address {}: {}", conf.listen_addr, e);
        e
    })?;

    loop {
        match listener.accept() {
            Ok((socket, peer_addr)) => {
                debug!("accepted connection from {}", peer_addr);
                let conf = conf.clone();
                thread::spawn(move || {
                    if let Err(err) = handle_socket(socket, conf) {
                        error!("socket error: {}", err);
                    }
                });
            }
            Err(e) => error!("Failed to accept connection: {}", e),
        }
    }
}

/// This is run by main_loop, inside a background thread.
///
/// This is only a separate function to make a convenient place to collect
/// all errors for logging. Our caller can log errors in a single place.
fn handle_socket(socket: TcpStream, conf: WalAcceptorConf) -> Result<()> {
    socket.set_nodelay(true)?;
    let conn = Connection::new(socket, conf)?;
    conn.run()?;
    Ok(())
}

impl Timeline {
    pub fn new(timelineid: ZTimelineId) -> Timeline {
        let shared_state = SharedState {
            commit_lsn: Lsn(0),
            info: SafeKeeperInfo::new(),
            control_file: None,
            hs_feedback: HotStandbyFeedback {
                ts: 0,
                xmin: u64::MAX,
                catalog_xmin: u64::MAX,
            },
        };
        Timeline {
            timelineid,
            mutex: Mutex::new(shared_state),
            cond: Condvar::new(),
        }
    }

    // Notify caught-up WAL senders about new WAL data received
    fn notify_wal_senders(&self, commit_lsn: Lsn) {
        let mut shared_state = self.mutex.lock().unwrap();
        if shared_state.commit_lsn < commit_lsn {
            shared_state.commit_lsn = commit_lsn;
            self.cond.notify_all();
        }
    }

    fn _stop_wal_senders(&self) {
        self.notify_wal_senders(END_REPLICATION_MARKER);
    }

    fn get_info(&self) -> SafeKeeperInfo {
        return self.mutex.lock().unwrap().info;
    }

    fn set_info(&self, info: &SafeKeeperInfo) {
        self.mutex.lock().unwrap().info = *info;
    }

    // Accumulate hot standby feedbacks from replicas
    fn add_hs_feedback(&self, feedback: HotStandbyFeedback) {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.hs_feedback.xmin = min(shared_state.hs_feedback.xmin, feedback.xmin);
        shared_state.hs_feedback.catalog_xmin =
            min(shared_state.hs_feedback.catalog_xmin, feedback.catalog_xmin);
        shared_state.hs_feedback.ts = max(shared_state.hs_feedback.ts, feedback.ts);
    }

    fn get_hs_feedback(&self) -> HotStandbyFeedback {
        let shared_state = self.mutex.lock().unwrap();
        shared_state.hs_feedback
    }

    // Load and lock control file (prevent running more than one instance of safekeeper)
    fn load_control_file(&self, conf: &WalAcceptorConf) -> Result<()> {
        let mut shared_state = self.mutex.lock().unwrap();

        if shared_state.control_file.is_some() {
            info!(
                "control file for timeline {} is already open",
                self.timelineid
            );
            return Ok(());
        }

        let control_file_path = conf
            .data_dir
            .join(self.timelineid.to_string())
            .join(CONTROL_FILE_NAME);
        info!("loading control file {}", control_file_path.display());
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&control_file_path)
        {
            Ok(file) => {
                // Lock file to prevent two or more active wal_acceptors
                match file.try_lock_exclusive() {
                    Ok(()) => {}
                    Err(e) => {
                        bail!(
                            "Control file {:?} is locked by some other process: {}",
                            &control_file_path,
                            e
                        );
                    }
                }
                shared_state.control_file = Some(file);

                const SIZE: usize = mem::size_of::<SafeKeeperInfo>();
                let mut buf = [0u8; SIZE];
                if shared_state
                    .control_file
                    .as_mut()
                    .unwrap()
                    .read_exact(&mut buf)
                    .is_ok()
                {
                    let mut input = BytesMut::new();
                    input.extend_from_slice(&buf);
                    let my_info = SafeKeeperInfo::unpack(&mut input);

                    if my_info.magic != SK_MAGIC {
                        bail!("Invalid control file magic: {}", my_info.magic);
                    }
                    if my_info.format_version != SK_FORMAT_VERSION {
                        bail!(
                            "Incompatible format version: {} vs. {}",
                            my_info.format_version,
                            SK_FORMAT_VERSION
                        );
                    }
                    shared_state.info = my_info;
                }
            }
            Err(e) => {
                panic!(
                    "Failed to open control file {:?}: {}",
                    &control_file_path, e
                );
            }
        }
        Ok(())
    }

    fn save_control_file(&self, sync: bool) -> Result<()> {
        let mut buf = BytesMut::new();
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.info.pack(&mut buf);

        let file = shared_state.control_file.as_mut().unwrap();
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&buf[..])?;
        if sync {
            file.sync_all()?;
        }
        Ok(())
    }
}

impl Connection {
    pub fn new(socket: TcpStream, conf: WalAcceptorConf) -> Result<Connection> {
        let peer_addr = socket.peer_addr()?;
        let conn = Connection {
            timeline: None,
            stream_in: BufReader::new(socket.try_clone()?),
            stream_out: socket,
            peer_addr,
            outbuf: BytesMut::with_capacity(10 * 1024),
            conf,
        };
        Ok(conn)
    }

    fn run(mut self) -> Result<()> {
        // Peek at the first 4 bytes of the incoming data, to determine which protocol
        // is being spoken.
        // `fill_buf` does not consume any of the bytes we peek at; they are left
        // in the BufReader's internal buffer for the next reader.
        let peek_buf = self.stream_in.fill_buf()?;
        if peek_buf.len() < 4 {
            // Empty peek_buf means the socket was closed.
            // Less than 4 bytes doesn't seem likely unless the sender is malicious.
            // read_u32 would panic on any of these, so just return an error.
            bail!("fill_buf EOF or underrun");
        }
        let startup_pkg_len = BigEndian::read_u32(peek_buf);
        if startup_pkg_len == 0 {
            // Consume the 4 bytes we peeked at. This protocol begins after them.
            self.stream_in.read_u32::<BigEndian>()?;
            self.receive_wal()?; // internal protocol between wal_proposer and wal_acceptor
        } else {
            send_wal::SendWal::new(self).run()?; // libpq replication protocol between wal_acceptor and replicas/pagers
        }
        Ok(())
    }

    fn read_req<T: NewSerializer>(&mut self) -> Result<T> {
        // NewSerializer is always little-endian.
        Ok(T::des_from(&mut self.stream_in)?)
    }

    fn request_callback(&self) -> std::result::Result<(), postgres::error::Error> {
        if let Some(addr) = self.conf.pageserver_addr {
            let ps_connstr = format!(
                "host={} port={} dbname={} user={}",
                addr.ip(),
                addr.port(),
                "no_db",
                "no_user",
            );
            let callme = format!(
                "callmemaybe {} host={} port={} options='-c ztimelineid={}'",
                self.timeline.get().timelineid,
                self.conf.listen_addr.ip(),
                self.conf.listen_addr.port(),
                self.timeline.get().timelineid
            );
            info!(
                "requesting page server to connect to us: start {} {}",
                ps_connstr, callme
            );
            let mut client = Client::connect(&ps_connstr, NoTls)?;
            client.simple_query(&callme)?;
        }
        Ok(())
    }

    /// Receive WAL from wal_proposer
    fn receive_wal(&mut self) -> Result<()> {
        // Receive information about server
        let server_info = self.read_req::<ServerInfo>()?;
        info!(
            "Start handshake with wal_proposer {} sysid {} timeline {}",
            self.peer_addr, server_info.system_id, server_info.timeline_id,
        );
        // FIXME: also check that the system identifier matches
        self.timeline.set(server_info.timeline_id)?;
        self.timeline.get().load_control_file(&self.conf)?;

        let mut my_info = self.timeline.get().get_info();

        /* Check protocol compatibility */
        if server_info.protocol_version != SK_PROTOCOL_VERSION {
            bail!(
                "Incompatible protocol version {}, expected {}",
                server_info.protocol_version,
                SK_PROTOCOL_VERSION
            );
        }
        /* Postgres upgrade is not treated as fatal error */
        if server_info.pg_version != my_info.server.pg_version
            && my_info.server.pg_version != UNKNOWN_SERVER_VERSION
        {
            info!(
                "Incompatible server version {}, expected {}",
                server_info.pg_version, my_info.server.pg_version
            );
        }

        /* Update information about server, but preserve locally stored node_id */
        let node_id = my_info.server.node_id;
        my_info.server = server_info;
        my_info.server.node_id = node_id;

        /* Calculate WAL end based on local data */
        let (flush_lsn, timeline) = self.timeline.find_end_of_wal(&self.conf.data_dir, true);
        my_info.flush_lsn = flush_lsn;
        my_info.server.timeline = timeline;

        /* Report my identifier to proxy */
        self.start_sending();
        my_info.pack(&mut self.outbuf);
        self.send()?;

        /* Wait for vote request */
        let prop = self.read_req::<RequestVote>()?;
        /* This is Paxos check which should ensure that only one master can perform commits */
        if prop.node_id < my_info.server.node_id {
            /* Send my node-id to inform proxy that it's candidate was rejected */
            self.start_sending();
            my_info.server.node_id.pack(&mut self.outbuf);
            self.send()?;
            bail!(
                "Reject connection attempt with term {} because my term is {}",
                prop.node_id.term,
                my_info.server.node_id.term,
            );
        }
        my_info.server.node_id = prop.node_id;
        self.timeline.get().set_info(&my_info);
        /* Need to persist our vote first */
        self.timeline.get().save_control_file(true)?;

        let mut flushed_restart_lsn = Lsn(0);
        let wal_seg_size = server_info.wal_seg_size as usize;

        /* Acknowledge the proposed candidate by returning it to the proxy */
        self.start_sending();
        prop.node_id.pack(&mut self.outbuf);
        self.send()?;

        // Need to establish replication channel with page server.
        // Add far as replication in postgres is initiated by receiver, we should use callme mechanism
        if let Err(e) = self.request_callback() {
            // Do not treate it as fatal error and continue work
            // FIXME: we should retry after a while...
            error!("Failed to send callme request to pageserver: {}", e);
        }

        info!(
            "Start streaming from timeline {} address {:?}",
            server_info.timeline_id, self.peer_addr,
        );

        // Main loop
        loop {
            let mut sync_control_file = false;

            /* Receive message header */
            let req = self.read_req::<SafeKeeperRequest>()?;
            if req.sender_id != my_info.server.node_id {
                bail!("Sender NodeId is changed");
            }
            if req.begin_lsn == END_OF_STREAM {
                info!("Server stops streaming");
                break;
            }
            let start_pos = req.begin_lsn;
            let end_pos = req.end_lsn;
            let rec_size = end_pos.checked_sub(start_pos).unwrap().0 as usize;
            assert!(rec_size <= MAX_SEND_SIZE);

            debug!(
                "received for {} bytes between {} and {}",
                rec_size, start_pos, end_pos,
            );

            /* Receive message body */
            let mut inbuf = vec![0u8; rec_size];
            self.stream_in.read_exact(&mut inbuf)?;

            /* Save message in file */
            self.write_wal_file(start_pos, timeline, wal_seg_size, &inbuf)?;

            my_info.restart_lsn = req.restart_lsn;
            my_info.commit_lsn = req.commit_lsn;

            /*
             * Epoch switch happen when written WAL record cross the boundary.
             * The boundary is maximum of last WAL position at this node (FlushLSN) and global
             * maximum (vcl) determined by safekeeper_proxy during handshake.
             * Switching epoch means that node completes recovery and start writing in the WAL new data.
             */
            if my_info.epoch < prop.epoch && end_pos > max(my_info.flush_lsn, prop.vcl) {
                info!("Switch to new epoch {}", prop.epoch);
                my_info.epoch = prop.epoch; /* bump epoch */
                sync_control_file = true;
            }
            if end_pos > my_info.flush_lsn {
                my_info.flush_lsn = end_pos;
            }
            /*
             * Update restart LSN in control file.
             * To avoid negative impact on performance of extra fsync, do it only
             * when restart_lsn delta exceeds WAL segment size.
             */
            sync_control_file |= flushed_restart_lsn + (wal_seg_size as u64) < my_info.restart_lsn;
            self.timeline.get().save_control_file(sync_control_file)?;

            if sync_control_file {
                flushed_restart_lsn = my_info.restart_lsn;
            }

            /* Report flush position */
            //info!("Confirm LSN: {:X}/{:>08X}", (end_pos>>32) as u32, end_pos as u32);
            let resp = SafeKeeperResponse {
                epoch: my_info.epoch,
                flush_lsn: end_pos,
                hs_feedback: self.timeline.get().get_hs_feedback(),
            };
            self.start_sending();
            resp.pack(&mut self.outbuf);
            self.send()?;

            /*
             * Ping wal sender that new data is available.
             * FlushLSN (end_pos) can be smaller than commitLSN in case we are at catching-up safekeeper.
             */
            self.timeline
                .get()
                .notify_wal_senders(min(req.commit_lsn, end_pos));
        }
        Ok(())
    }

    ///
    /// Reset output buffer to start accumulating data of new message
    ///
    fn start_sending(&mut self) {
        self.outbuf.clear();
    }

    ///
    /// Send buffered messages
    ///
    fn send(&mut self) -> Result<()> {
        Ok(self.stream_out.write_all(&self.outbuf)?)
    }

    fn write_wal_file(
        &self,
        startpos: Lsn,
        timeline: TimeLineID,
        wal_seg_size: usize,
        buf: &[u8],
    ) -> Result<()> {
        let mut bytes_left: usize = buf.len();
        let mut bytes_written: usize = 0;
        let mut partial;
        let mut start_pos = startpos;
        const ZERO_BLOCK: &[u8] = &[0u8; XLOG_BLCKSZ];

        /* Extract WAL location for this block */
        let mut xlogoff = start_pos.segment_offset(wal_seg_size as u64) as usize;

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
            let segno = start_pos.segment_number(wal_seg_size as u64);
            let wal_file_name = XLogFileName(timeline, segno, wal_seg_size);
            let wal_file_path = self
                .conf
                .data_dir
                .join(self.timeline.get().timelineid.to_string())
                .join(wal_file_name.clone());
            let wal_file_partial_path = self
                .conf
                .data_dir
                .join(self.timeline.get().timelineid.to_string())
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
                                file.write_all(&ZERO_BLOCK)?;
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

                // Flush file is not prohibited
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
            if start_pos.segment_offset(wal_seg_size as u64) == 0 {
                xlogoff = 0;
                if partial {
                    fs::rename(&wal_file_partial_path, &wal_file_path)?;
                }
            }
        }
        Ok(())
    }
}

mod send_wal {
    use super::{
        Connection, HotStandbyFeedback, Timeline, TimelineTools, END_REPLICATION_MARKER,
        LIBPQ_HDR_SIZE, LIBPQ_MSG_SIZE_OFFS, MAX_SEND_SIZE, XLOG_HDR_SIZE,
    };
    use crate::pq_protocol::{
        BeMessage, FeMessage, FeStartupMessage, RowDescriptor, StartupRequestCode,
    };
    use crate::WalAcceptorConf;
    use anyhow::{anyhow, bail, Result};
    use bytes::{BufMut, Bytes, BytesMut};
    use log::*;
    use postgres_ffi::xlog_utils::{get_current_timestamp, XLogFileName};
    use regex::Regex;
    use std::cmp::min;
    use std::fs::File;
    use std::io::{BufReader, Read, Seek, SeekFrom, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::{str, thread};
    use zenith_utils::lsn::Lsn;

    pub struct SendWal {
        timeline: Option<Arc<Timeline>>,
        /// Postgres connection, buffered input
        stream_in: BufReader<TcpStream>,
        /// Postgres connection, output   FIXME: To buffer, or not to buffer? flush() is a pain.
        stream_out: TcpStream,
        /// The cached result of socket.peer_addr()
        peer_addr: SocketAddr,
        /// wal acceptor configuration
        conf: WalAcceptorConf,
        /// assigned application name
        appname: Option<String>,
    }

    impl SendWal {
        /// Create a new `SendWal`, consuming the `Connection`.
        pub fn new(conn: Connection) -> Self {
            Self {
                timeline: conn.timeline,
                stream_in: conn.stream_in,
                stream_out: conn.stream_out,
                peer_addr: conn.peer_addr,
                conf: conn.conf,
                appname: None,
            }
        }

        ///
        /// Send WAL to replica or WAL receiver using standard libpq replication protocol
        ///
        pub fn run(mut self) -> Result<()> {
            let peer_addr = self.peer_addr.clone();
            info!("WAL sender to {:?} is started", peer_addr);

            // Handle the startup message first.

            let m = FeStartupMessage::read_from(&mut self.stream_in)?;
            trace!("got startup message {:?}", m);
            match m.kind {
                StartupRequestCode::NegotiateGss | StartupRequestCode::NegotiateSsl => {
                    let mut buf = BytesMut::new();
                    BeMessage::write(&mut buf, &BeMessage::Negotiate);
                    info!("SSL requested");
                    self.stream_out.write_all(&buf)?;
                }
                StartupRequestCode::Normal => {
                    let mut buf = BytesMut::new();
                    BeMessage::write(&mut buf, &BeMessage::AuthenticationOk);
                    BeMessage::write(&mut buf, &BeMessage::ReadyForQuery);
                    self.stream_out.write_all(&buf)?;
                    self.timeline.set(m.timelineid)?;
                    self.appname = m.appname;
                }
                StartupRequestCode::Cancel => return Ok(()),
            }

            loop {
                let msg = FeMessage::read_from(&mut self.stream_in)?;
                match msg {
                    FeMessage::Query(q) => {
                        trace!("got query {:?}", q.body);

                        if q.body.starts_with(b"IDENTIFY_SYSTEM") {
                            self.handle_identify_system()?;
                        } else if q.body.starts_with(b"START_REPLICATION") {
                            // Create a new replication object, consuming `self`.
                            let mut replication = ReplicationHandler::new(self);
                            replication.run(&q.body)?;
                            break;
                        } else {
                            bail!("Unexpected command {:?}", q.body);
                        }
                    }
                    FeMessage::Terminate => {
                        break;
                    }
                    _ => {
                        bail!("unexpected message");
                    }
                }
            }
            info!("WAL sender to {:?} is finished", peer_addr);
            Ok(())
        }

        ///
        /// Handle IDENTIFY_SYSTEM replication command
        ///
        fn handle_identify_system(&mut self) -> Result<()> {
            let (start_pos, timeline) = self.timeline.find_end_of_wal(&self.conf.data_dir, false);
            let lsn = start_pos.to_string();
            let tli = timeline.to_string();
            let sysid = self.timeline.get().get_info().server.system_id.to_string();
            let lsn_bytes = lsn.as_bytes();
            let tli_bytes = tli.as_bytes();
            let sysid_bytes = sysid.as_bytes();

            let mut outbuf = BytesMut::new();
            BeMessage::write(
                &mut outbuf,
                &BeMessage::RowDescription(&[
                    RowDescriptor {
                        name: b"systemid\0",
                        typoid: 25,
                        typlen: -1,
                    },
                    RowDescriptor {
                        name: b"timeline\0",
                        typoid: 23,
                        typlen: 4,
                    },
                    RowDescriptor {
                        name: b"xlogpos\0",
                        typoid: 25,
                        typlen: -1,
                    },
                    RowDescriptor {
                        name: b"dbname\0",
                        typoid: 25,
                        typlen: -1,
                    },
                ]),
            );
            BeMessage::write(
                &mut outbuf,
                &BeMessage::DataRow(&[Some(sysid_bytes), Some(tli_bytes), Some(lsn_bytes), None]),
            );
            BeMessage::write(
                &mut outbuf,
                &BeMessage::CommandComplete(b"IDENTIFY_SYSTEM\0"),
            );
            BeMessage::write(&mut outbuf, &BeMessage::ReadyForQuery);
            self.stream_out.write_all(&outbuf)?;
            Ok(())
        }
    }

    pub struct ReplicationHandler {
        timeline: Option<Arc<Timeline>>,
        /// Postgres connection, buffered input
        stream_in: Option<BufReader<TcpStream>>,
        /// Postgres connection, output   FIXME: To buffer, or not to buffer? flush() is a pain.
        stream_out: Mutex<TcpStream>,
        /// wal acceptor configuration
        conf: WalAcceptorConf,
        /// assigned application name
        appname: Option<String>,
    }

    impl ReplicationHandler {
        /// Create a new `SendWal`, consuming the `Connection`.
        pub fn new(conn: SendWal) -> Self {
            Self {
                timeline: conn.timeline,
                stream_in: Some(conn.stream_in),
                stream_out: Mutex::new(conn.stream_out),
                conf: conn.conf,
                appname: None,
            }
        }

        /// Handle incoming messages from the network.
        ///
        /// This is spawned into the background by `handle_start_replication`.
        ///
        fn background_thread(mut stream_in: impl Read, timeline: Arc<Timeline>) -> Result<()> {
            // Wait for replica's feedback.
            // We only handle `CopyData` messages. Anything else is ignored.

            loop {
                match FeMessage::read_from(&mut stream_in)? {
                    FeMessage::CopyData(m) => {
                        timeline.add_hs_feedback(HotStandbyFeedback::parse(&m.body))
                    }
                    msg => {
                        info!("unexpected message {:?}", msg);
                    }
                }
            }
        }

        /// Helper function that parses a pair of LSNs.
        fn parse_start_stop(cmd: &[u8]) -> Result<(Lsn, Lsn)> {
            let re = Regex::new(r"([[:xdigit:]]+/[[:xdigit:]]+)").unwrap();
            let caps = re.captures_iter(str::from_utf8(&cmd[..])?);
            let mut lsns = caps.map(|cap| cap[1].parse::<Lsn>());
            let start_pos = lsns
                .next()
                .ok_or_else(|| anyhow!("failed to find start LSN"))??;
            let stop_pos = lsns.next().transpose()?.unwrap_or(Lsn(0));
            Ok((start_pos, stop_pos))
        }

        /// Helper function for opening a wal file.
        fn open_wal_file(wal_file_path: &Path) -> Result<File> {
            // First try to open the .partial file.
            let mut partial_path = wal_file_path.to_owned();
            partial_path.set_extension("partial");
            if let Ok(opened_file) = File::open(&partial_path) {
                return Ok(opened_file);
            }

            // If that failed, try it without the .partial extension.
            match File::open(&wal_file_path) {
                Ok(opened_file) => return Ok(opened_file),
                Err(e) => {
                    error!("Failed to open log file {:?}: {}", &wal_file_path, e);
                    return Err(e.into());
                }
            }
        }

        ///
        /// Handle START_REPLICATION replication command
        ///
        fn run(&mut self, cmd: &Bytes) -> Result<()> {
            // spawn the background thread which receives HotStandbyFeedback messages.
            let bg_timeline = Arc::clone(self.timeline.get());
            let bg_stream_in = self.stream_in.take().unwrap();

            thread::spawn(move || {
                if let Err(err) = Self::background_thread(bg_stream_in, bg_timeline) {
                    error!("socket error: {}", err);
                }
            });

            let (mut start_pos, mut stop_pos) = Self::parse_start_stop(&cmd)?;

            let wal_seg_size = self.timeline.get().get_info().server.wal_seg_size as usize;
            if wal_seg_size == 0 {
                bail!("Can not start replication before connecting to wal_proposer");
            }
            let (wal_end, timeline) = self.timeline.find_end_of_wal(&self.conf.data_dir, false);
            if start_pos == Lsn(0) {
                start_pos = wal_end;
            }
            if stop_pos == Lsn(0) && self.appname == Some("wal_proposer_recovery".to_string()) {
                stop_pos = wal_end;
            }
            info!("Start replication from {} till {}", start_pos, stop_pos);

            let mut outbuf = BytesMut::new();
            BeMessage::write(&mut outbuf, &BeMessage::Copy);
            self.send(&outbuf)?;
            outbuf.clear();

            let mut end_pos: Lsn;
            let mut commit_lsn: Lsn;
            let mut wal_file: Option<File> = None;

            loop {
                /* Wait until we have some data to stream */
                if stop_pos != Lsn(0) {
                    /* recovery mode: stream up to the specified LSN (VCL) */
                    if start_pos >= stop_pos {
                        /* recovery finished */
                        break;
                    }
                    end_pos = stop_pos;
                } else {
                    /* normal mode */
                    let timeline = self.timeline.get();
                    let mut shared_state = timeline.mutex.lock().unwrap();
                    loop {
                        commit_lsn = shared_state.commit_lsn;
                        if start_pos < commit_lsn {
                            end_pos = commit_lsn;
                            break;
                        }
                        shared_state = timeline.cond.wait(shared_state).unwrap();
                    }
                }
                if end_pos == END_REPLICATION_MARKER {
                    break;
                }

                // Take the `File` from `wal_file`, or open a new file.
                let mut file = match wal_file.take() {
                    Some(file) => file,
                    None => {
                        // Open a new file.
                        let segno = start_pos.segment_number(wal_seg_size as u64);
                        let wal_file_name = XLogFileName(timeline, segno, wal_seg_size);
                        let timeline_id = self.timeline.get().timelineid.to_string();
                        let wal_file_path =
                            self.conf.data_dir.join(timeline_id).join(wal_file_name);
                        Self::open_wal_file(&wal_file_path)?
                    }
                };

                let xlogoff = start_pos.segment_offset(wal_seg_size as u64) as usize;

                // How much to read and send in message? We cannot cross the WAL file
                // boundary, and we don't want send more than MAX_SEND_SIZE.
                let send_size = end_pos.checked_sub(start_pos).unwrap().0 as usize;
                let send_size = min(send_size, wal_seg_size - xlogoff);
                let send_size = min(send_size, MAX_SEND_SIZE);

                let msg_size = LIBPQ_HDR_SIZE + XLOG_HDR_SIZE + send_size;

                // Read some data from the file.
                let mut file_buf = vec![0u8; send_size];
                file.seek(SeekFrom::Start(xlogoff as u64))?;
                file.read_exact(&mut file_buf)?;

                // Write some data to the network socket.
                // FIXME: turn these into structs.
                // 'd' is CopyData;
                // 'w' is "WAL records"
                // https://www.postgresql.org/docs/9.1/protocol-message-formats.html
                // src/backend/replication/walreceiver.c
                outbuf.clear();
                outbuf.put_u8(b'd');
                outbuf.put_u32((msg_size - LIBPQ_MSG_SIZE_OFFS) as u32);
                outbuf.put_u8(b'w');
                outbuf.put_u64(start_pos.0);
                outbuf.put_u64(end_pos.0);
                outbuf.put_u64(get_current_timestamp());

                assert!(outbuf.len() + file_buf.len() == msg_size);
                // FIXME: combine these two into a single send,
                // so that no other traffic can be sent in between them.
                self.send(&outbuf)?;
                self.send(&file_buf)?;
                start_pos += send_size as u64;

                debug!("Sent WAL to page server up to {}", end_pos);

                // Decide whether to reuse this file. If we don't set wal_file here
                // a new file will be opened next time.
                if start_pos.segment_offset(wal_seg_size as u64) != 0 {
                    wal_file = Some(file);
                }
            }
            Ok(())
        }

        /// Unlock the mutex and send bytes on the network.
        fn send(&self, buf: &[u8]) -> Result<()> {
            let mut writer = self.stream_out.lock().unwrap();
            writer.write_all(buf.as_ref())?;
            Ok(())
        }
    }
}
