//!
//!   WAL service listens for client connections and
//!   receive WAL from wal_proposer and send it to WAL receivers
//!
use anyhow::{anyhow, bail, Result};
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use fs2::FileExt;
use lazy_static::lazy_static;
use log::*;
use postgres::{Client, NoTls};
use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::mem;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::str;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use zenith_utils::bin_ser::LeSer;
use zenith_utils::lsn::Lsn;

use crate::pq_protocol::*;
use crate::WalAcceptorConf;
use pageserver::ZTimelineId;
use postgres_ffi::xlog_utils::{
    find_end_of_wal, get_current_timestamp, TimeLineID, TimestampTz, XLogFileName, XLOG_BLCKSZ,
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

/// Read some bytes from a type that implements [`Read`] into a [`BytesMut`]
///
/// Will return the number of bytes read, just like `Read::read()` would.
///
fn read_into(r: &mut impl Read, buf: &mut BytesMut) -> io::Result<usize> {
    // This is a workaround, because BytesMut and std::io don't play
    // well together.
    //
    // I think this code needs to go away, and I'm confident that
    // that's possible, if we are willing to refactor this code to
    // use std::io::BufReader instead of doing buffer management
    // ourselves.
    //
    // SAFETY: we already have exclusive access to self.inbuf, so
    // there are no concurrency problems; the only risk would be
    // accidentally exposing uninitialized parts of the buffer.
    //
    // We write into the buffer just past the known-initialized part,
    // then manually increment its length by the exact number of
    // bytes we read. So no uninitialized memory should be exposed.

    let start = buf.len();
    let end = buf.capacity();

    let num_bytes = unsafe {
        let fill_here = buf.get_unchecked_mut(start..end);
        let num_bytes_read = r.read(fill_here)?;
        buf.set_len(start + num_bytes_read);
        num_bytes_read
    };
    Ok(num_bytes)
}

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

/// Private data
#[derive(Debug)]
struct Connection {
    timeline: Option<Arc<Timeline>>,
    /// Postgres connection, buffered input
    stream_in: BufReader<TcpStream>,
    /// Postgres connection, output   FIXME: To buffer, or not to buffer? flush() is a pain.
    stream_out: TcpStream,
    /// The cached result of socket.peer_addr()
    peer_addr: SocketAddr,
    /// input buffer
    inbuf: BytesMut,
    /// output buffer
    outbuf: BytesMut,
    /// startup packet proceeded
    init_done: bool,
    /// assigned application name
    appname: Option<String>,
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
        let buf_r = buf.reader();
        Self::des_from(buf_r).unwrap()
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

pub fn thread_main(conf: WalAcceptorConf) {
    info!("Starting wal acceptor on {}", conf.listen_addr);
    main_loop(&conf).unwrap();
}

/// This is run by main_loop, inside a background thread.
///
/// This is only a separate function to make a convenient place to collect
/// all errors for logging. Our caller can log errors in a single place.
fn handle_socket(socket: TcpStream, conf: WalAcceptorConf) -> Result<()> {
    socket.set_nodelay(true)?;
    let mut conn = Connection::new(socket, conf)?;
    conn.run()?;
    Ok(())
}

/// Accept incoming TCP connections and spawn them into a background thread.
fn main_loop(conf: &WalAcceptorConf) -> Result<()> {
    let listener = TcpListener::bind(conf.listen_addr)?;
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
            inbuf: BytesMut::with_capacity(10 * 1024),
            outbuf: BytesMut::with_capacity(10 * 1024),
            init_done: false,
            appname: None,
            conf,
        };
        Ok(conn)
    }

    fn timeline(&self) -> Arc<Timeline> {
        self.timeline.as_ref().unwrap().clone()
    }

    fn run(&mut self) -> Result<()> {
        self.inbuf.resize(4, 0u8);
        self.stream_in.read_exact(&mut self.inbuf[0..4])?;
        let startup_pkg_len = BigEndian::read_u32(&self.inbuf[0..4]);
        if startup_pkg_len == 0 {
            self.receive_wal()?; // internal protocol between wal_proposer and wal_acceptor
        } else {
            self.send_wal()?; // libpq replication protocol between wal_acceptor and replicas/pagers
        }
        Ok(())
    }

    fn read_req<T: NewSerializer>(&mut self) -> Result<T> {
        let size = mem::size_of::<T>();
        self.inbuf.resize(size, 0u8);
        self.stream_in.read_exact(&mut self.inbuf[0..size])?;
        Ok(T::unpack(&mut self.inbuf))
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
                self.timeline().timelineid,
                self.conf.listen_addr.ip(),
                self.conf.listen_addr.port(),
                self.timeline().timelineid
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

    fn set_timeline(&mut self, timelineid: ZTimelineId) -> Result<()> {
        let mut timelines = TIMELINES.lock().unwrap();
        if !timelines.contains_key(&timelineid) {
            info!("creating timeline dir {}", timelineid);
            fs::create_dir_all(timelineid.to_string())?;
            timelines.insert(timelineid, Arc::new(Timeline::new(timelineid)));
        }
        self.timeline = Some(timelines.get(&timelineid).unwrap().clone());
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
        self.set_timeline(server_info.timeline_id)?;
        self.timeline().load_control_file(&self.conf)?;

        let mut my_info = self.timeline().get_info();

        /* Check protocol compatibility */
        if server_info.protocol_version != SK_PROTOCOL_VERSION {
            bail!(
                "Incompatible protocol version {} vs. {}",
                server_info.protocol_version,
                SK_PROTOCOL_VERSION
            );
        }
        /* Postgres upgrade is not treated as fatal error */
        if server_info.pg_version != my_info.server.pg_version
            && my_info.server.pg_version != UNKNOWN_SERVER_VERSION
        {
            info!(
                "Server version doesn't match {} vs. {}",
                server_info.pg_version, my_info.server.pg_version
            );
        }
        /* Update information about server, but preserve locally stored node_id */
        let node_id = my_info.server.node_id;
        my_info.server = server_info;
        my_info.server.node_id = node_id;

        /* Calculate WAL end based on local data */
        let (flush_lsn, timeline) = self.find_end_of_wal(true);
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
        self.timeline().set_info(&my_info);
        /* Need to persist our vote first */
        self.timeline().save_control_file(true)?;

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
            self.inbuf.resize(rec_size, 0u8);
            self.stream_in.read_exact(&mut self.inbuf[0..rec_size])?;

            /* Save message in file */
            self.write_wal_file(start_pos, timeline, wal_seg_size, &self.inbuf[0..rec_size])?;

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
            self.timeline().save_control_file(sync_control_file)?;

            if sync_control_file {
                flushed_restart_lsn = my_info.restart_lsn;
            }

            /* Report flush position */
            //info!("Confirm LSN: {:X}/{:>08X}", (end_pos>>32) as u32, end_pos as u32);
            let resp = SafeKeeperResponse {
                epoch: my_info.epoch,
                flush_lsn: end_pos,
                hs_feedback: self.timeline().get_hs_feedback(),
            };
            self.start_sending();
            resp.pack(&mut self.outbuf);
            self.send()?;

            /*
             * Ping wal sender that new data is available.
             * FlushLSN (end_pos) can be smaller than commitLSN in case we are at catching-up safekeeper.
             */
            self.timeline()
                .notify_wal_senders(min(req.commit_lsn, end_pos));
        }
        Ok(())
    }

    ///
    /// Read full message or return None if connection is closed
    ///
    fn read_message(&mut self) -> Result<Option<FeMessage>> {
        loop {
            if let Some(message) = self.parse_message()? {
                return Ok(Some(message));
            }

            if read_into(&mut self.stream_in, &mut self.inbuf)? == 0 {
                if self.inbuf.is_empty() {
                    return Ok(None);
                } else {
                    bail!("connection reset by peer");
                }
            }
        }
    }

    ///
    /// Parse libpq message
    ///
    fn parse_message(&mut self) -> Result<Option<FeMessage>> {
        let msg = if !self.init_done {
            FeStartupMessage::parse(&mut self.inbuf)?
        } else {
            FeMessage::parse(&mut self.inbuf)?
        };
        Ok(msg)
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

    ///
    /// Send WAL to replica or WAL receiver using standard libpq replication protocol
    ///
    fn send_wal(&mut self) -> Result<()> {
        info!("WAL sender to {:?} is started", self.peer_addr);
        loop {
            self.start_sending();
            match self.read_message()? {
                Some(FeMessage::StartupMessage(m)) => {
                    trace!("got message {:?}", m);

                    match m.kind {
                        StartupRequestCode::NegotiateGss | StartupRequestCode::NegotiateSsl => {
                            BeMessage::write(&mut self.outbuf, &BeMessage::Negotiate);
                            info!("SSL requested");
                            self.send()?;
                        }
                        StartupRequestCode::Normal => {
                            BeMessage::write(&mut self.outbuf, &BeMessage::AuthenticationOk);
                            BeMessage::write(&mut self.outbuf, &BeMessage::ReadyForQuery);
                            self.send()?;
                            self.init_done = true;
                            self.set_timeline(m.timelineid)?;
                            self.appname = m.appname;
                        }
                        StartupRequestCode::Cancel => return Ok(()),
                    }
                }
                Some(FeMessage::Query(m)) => {
                    if !self.process_query(&m)? {
                        break;
                    }
                }
                Some(FeMessage::Terminate) => {
                    break;
                }
                None => {
                    info!("connection closed");
                    break;
                }
                _ => {
                    bail!("unexpected message");
                }
            }
        }
        info!("WAL sender to {:?} is finished", self.peer_addr);
        Ok(())
    }

    ///
    /// Handle IDENTIFY_SYSTEM replication command
    ///
    fn handle_identify_system(&mut self) -> Result<bool> {
        let (start_pos, timeline) = self.find_end_of_wal(false);
        let lsn = start_pos.to_string();
        let tli = timeline.to_string();
        let sysid = self.timeline().get_info().server.system_id.to_string();
        let lsn_bytes = lsn.as_bytes();
        let tli_bytes = tli.as_bytes();
        let sysid_bytes = sysid.as_bytes();

        BeMessage::write(
            &mut self.outbuf,
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
            &mut self.outbuf,
            &BeMessage::DataRow(&[Some(sysid_bytes), Some(tli_bytes), Some(lsn_bytes), None]),
        );
        BeMessage::write(
            &mut self.outbuf,
            &BeMessage::CommandComplete(b"IDENTIFY_SYSTEM\0"),
        );
        BeMessage::write(&mut self.outbuf, &BeMessage::ReadyForQuery);
        self.send()?;
        Ok(true)
    }

    ///
    /// Handle START_REPLICATION replication command
    ///
    fn handle_start_replication(&mut self, cmd: &Bytes) -> Result<bool> {
        // helper function to encapsulate the regex -> Lsn magic
        fn get_start_stop(cmd: &[u8]) -> Result<(Lsn, Lsn)> {
            let re = Regex::new(r"([[:xdigit:]]+/[[:xdigit:]]+)").unwrap();
            let caps = re.captures_iter(str::from_utf8(&cmd[..])?);
            let mut lsns = caps.map(|cap| cap[1].parse::<Lsn>());
            let start_pos = lsns
                .next()
                .ok_or_else(|| anyhow!("failed to find start LSN"))??;
            let stop_pos = lsns.next().transpose()?.unwrap_or(Lsn(0));
            Ok((start_pos, stop_pos))
        }

        let (mut start_pos, mut stop_pos) = get_start_stop(&cmd)?;

        let wal_seg_size = self.timeline().get_info().server.wal_seg_size as usize;
        if wal_seg_size == 0 {
            bail!("Can not start replication before connecting to wal_proposer");
        }
        let (wal_end, timeline) = self.find_end_of_wal(false);
        if start_pos == Lsn(0) {
            start_pos = wal_end;
        }
        if stop_pos == Lsn(0) && self.appname == Some("wal_proposer_recovery".to_string()) {
            stop_pos = wal_end;
        }
        info!("Start replication from {} till {}", start_pos, stop_pos);
        BeMessage::write(&mut self.outbuf, &BeMessage::Copy);
        self.send()?;

        let mut end_pos: Lsn;
        let mut commit_lsn: Lsn;
        let mut wal_file: Option<File> = None;
        self.outbuf
            .resize(LIBPQ_HDR_SIZE + XLOG_HDR_SIZE + MAX_SEND_SIZE, 0u8);
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
                let timeline = self.timeline();
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
            // Try to fetch replica's feedback

            // Temporarily set this stream into nonblocking mode.
            // FIXME: This seems like a dirty hack.
            // Should this task be done on a background thread?
            // FIXME: set_nonblocking plus BufReader seems questionable.
            self.stream_in.get_ref().set_nonblocking(true).unwrap();
            let read_result = self.stream_in.read(&mut self.inbuf);
            self.stream_in.get_ref().set_nonblocking(false).unwrap();

            match read_result {
                Ok(0) => break,
                Ok(_) => {
                    if let Some(FeMessage::CopyData(m)) = self.parse_message()? {
                        self.timeline()
                            .add_hs_feedback(HotStandbyFeedback::parse(&m.body))
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => {
                    return Err(e.into());
                }
            }

            /* Open file if not opened yet */
            let curr_file = wal_file.take();
            let mut file: File;
            if let Some(opened_file) = curr_file {
                file = opened_file;
            } else {
                let segno = start_pos.segment_number(wal_seg_size as u64);
                let wal_file_name = XLogFileName(timeline, segno, wal_seg_size);
                let wal_file_path = self
                    .conf
                    .data_dir
                    .join(self.timeline().timelineid.to_string())
                    .join(wal_file_name.clone() + ".partial");
                if let Ok(opened_file) = File::open(&wal_file_path) {
                    file = opened_file;
                } else {
                    let wal_file_path = self
                        .conf
                        .data_dir
                        .join(self.timeline().timelineid.to_string())
                        .join(wal_file_name);
                    match File::open(&wal_file_path) {
                        Ok(opened_file) => file = opened_file,
                        Err(e) => {
                            error!("Failed to open log file {:?}: {}", &wal_file_path, e);
                            return Err(e.into());
                        }
                    }
                }
            }
            let xlogoff = start_pos.segment_offset(wal_seg_size as u64) as usize;

            // How much to read and send in message? We cannot cross the WAL file
            // boundary, and we don't want send more than MAX_SEND_SIZE.
            let send_size = end_pos.checked_sub(start_pos).unwrap().0 as usize;
            let send_size = min(send_size, wal_seg_size - xlogoff);
            let send_size = min(send_size, MAX_SEND_SIZE);

            let msg_size = LIBPQ_HDR_SIZE + XLOG_HDR_SIZE + send_size;
            let data_start = LIBPQ_HDR_SIZE + XLOG_HDR_SIZE;
            let data_end = data_start + send_size;

            file.seek(SeekFrom::Start(xlogoff as u64))?;
            file.read_exact(&mut self.outbuf[data_start..data_end])?;
            self.outbuf[0] = b'd';
            BigEndian::write_u32(
                &mut self.outbuf[1..5],
                (msg_size - LIBPQ_MSG_SIZE_OFFS) as u32,
            );
            self.outbuf[5] = b'w';
            BigEndian::write_u64(&mut self.outbuf[6..14], start_pos.0);
            BigEndian::write_u64(&mut self.outbuf[14..22], end_pos.0);
            BigEndian::write_u64(&mut self.outbuf[22..30], get_current_timestamp());

            self.stream_out.write_all(&self.outbuf[0..msg_size])?;
            start_pos += send_size as u64;

            debug!("Sent WAL to page server up to {}", end_pos);

            if start_pos.segment_offset(wal_seg_size as u64) != 0 {
                wal_file = Some(file);
            }
        }
        Ok(false)
    }

    fn process_query(&mut self, q: &FeQueryMessage) -> Result<bool> {
        trace!("got query {:?}", q.body);

        if q.body.starts_with(b"IDENTIFY_SYSTEM") {
            self.handle_identify_system()
        } else if q.body.starts_with(b"START_REPLICATION") {
            self.handle_start_replication(&q.body)
        } else {
            bail!("Unexpected command {:?}", q.body);
        }
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
                .join(self.timeline().timelineid.to_string())
                .join(wal_file_name.clone());
            let wal_file_partial_path = self
                .conf
                .data_dir
                .join(self.timeline().timelineid.to_string())
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

    /// Find last WAL record. If "precise" is false then just locate last partial segment
    fn find_end_of_wal(&self, precise: bool) -> (Lsn, TimeLineID) {
        let (lsn, timeline) = find_end_of_wal(
            &self.conf.data_dir,
            self.timeline().get_info().server.wal_seg_size as usize,
            precise,
        );
        (Lsn(lsn), timeline)
    }
}
