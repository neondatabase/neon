//! This implements the Safekeeper protocol.
//!
//! FIXME: better description needed here

use anyhow::{bail, Result};
use log::*;
use postgres::{Client, NoTls};
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::net::{SocketAddr, TcpStream};
use std::str;
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use zenith_utils::bin_ser::LeSer;
use zenith_utils::lsn::Lsn;

use crate::pq_protocol::*;
use crate::replication::HotStandbyFeedback;
use crate::timeline::{Timeline, TimelineTools};
use crate::WalAcceptorConf;
use pageserver::ZTimelineId;
use postgres_ffi::xlog_utils::{TimeLineID, XLogFileName, MAX_SEND_SIZE, XLOG_BLCKSZ};

pub const SK_MAGIC: u32 = 0xcafeceefu32;
pub const SK_FORMAT_VERSION: u32 = 1;
const SK_PROTOCOL_VERSION: u32 = 1;
const UNKNOWN_SERVER_VERSION: u32 = 0;
const END_OF_STREAM: Lsn = Lsn(0);
pub const CONTROL_FILE_NAME: &str = "safekeeper.control";

/// Unique node identifier used by Paxos
#[derive(Debug, Clone, Copy, Ord, PartialOrd, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeId {
    term: u64,
    uuid: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerInfo {
    /// proxy-safekeeper protocol version
    pub protocol_version: u32,
    /// Postgres server version
    pub pg_version: u32,
    pub node_id: NodeId,
    pub system_id: SystemId,
    /// Zenith timelineid
    pub timeline_id: ZTimelineId,
    pub wal_end: Lsn,
    pub timeline: TimeLineID,
    pub wal_seg_size: u32,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SafeKeeperInfo {
    /// magic for verifying content the control file
    pub magic: u32,
    /// safekeeper format version
    pub format_version: u32,
    /// safekeeper's epoch
    pub epoch: u64,
    /// information about server
    pub server: ServerInfo,
    /// part of WAL acknowledged by quorum
    pub commit_lsn: Lsn,
    /// locally flushed part of WAL
    pub flush_lsn: Lsn,
    /// minimal LSN which may be needed for recovery of some safekeeper: min(commit_lsn) for all safekeepers
    pub restart_lsn: Lsn,
}

impl SafeKeeperInfo {
    pub fn new() -> SafeKeeperInfo {
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

#[derive(Debug)]
pub struct ReceiveWalConn {
    pub timeline: Option<Arc<Timeline>>,
    /// Postgres connection, buffered input
    pub stream_in: BufReader<TcpStream>,
    /// Postgres connection, output
    pub stream_out: TcpStream,
    /// The cached result of socket.peer_addr()
    pub peer_addr: SocketAddr,
    /// wal acceptor configuration
    pub conf: WalAcceptorConf,
}

///
/// Periodically request pageserver to call back.
/// If pageserver already has replication channel, it will just ignore this request
///
fn request_callback(conf: WalAcceptorConf, timelineid: ZTimelineId) {
    let addr = conf.pageserver_addr.unwrap();
    let ps_connstr = format!(
        "host={} port={} dbname={} user={}",
        addr.ip(),
        addr.port(),
        "no_db",
        "no_user",
    );
    let callme = format!(
        "callmemaybe {} host={} port={} options='-c ztimelineid={}'",
        timelineid,
        conf.listen_addr.ip(),
        conf.listen_addr.port(),
        timelineid
    );
    loop {
        info!(
            "requesting page server to connect to us: start {} {}",
            ps_connstr, callme
        );
        match Client::connect(&ps_connstr, NoTls) {
            Ok(mut client) => {
                if let Err(e) = client.simple_query(&callme) {
                    error!("Failed to send callme request to pageserver: {}", e);
                }
            }
            Err(e) => error!("Failed to connect to pageserver {}: {}", &ps_connstr, e),
        }

        if let Some(period) = conf.recall_period {
            sleep(period);
        } else {
            break;
        }
    }
}

impl ReceiveWalConn {
    pub fn new(socket: TcpStream, conf: WalAcceptorConf) -> Result<ReceiveWalConn> {
        let peer_addr = socket.peer_addr()?;
        let conn = ReceiveWalConn {
            timeline: None,
            stream_in: BufReader::new(socket.try_clone()?),
            stream_out: socket,
            peer_addr,
            conf,
        };
        Ok(conn)
    }

    fn read_req<T: LeSer>(&mut self) -> Result<T> {
        // As the trait bound implies, this always encodes little-endian.
        Ok(T::des_from(&mut self.stream_in)?)
    }

    /// Receive WAL from wal_proposer
    pub fn run(&mut self) -> Result<()> {
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
        my_info.server = server_info.clone();
        my_info.server.node_id = node_id;

        /* Calculate WAL end based on local data */
        let (flush_lsn, timeline) = self.timeline.find_end_of_wal(&self.conf.data_dir, true);
        my_info.flush_lsn = flush_lsn;
        my_info.server.timeline = timeline;

        /* Report my identifier to proxy */
        my_info.ser_into(&mut self.stream_out)?;

        /* Wait for vote request */
        let prop = self.read_req::<RequestVote>()?;
        /* This is Paxos check which should ensure that only one master can perform commits */
        if prop.node_id < my_info.server.node_id {
            /* Send my node-id to inform proxy that it's candidate was rejected */
            my_info.server.node_id.ser_into(&mut self.stream_out)?;
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
        prop.node_id.ser_into(&mut self.stream_out)?;

        if self.conf.pageserver_addr.is_some() {
            // Need to establish replication channel with page server.
            // Add far as replication in postgres is initiated by receiver, we should use callme mechanism
            let conf = self.conf.clone();
            let timelineid = self.timeline.get().timelineid;
            thread::spawn(move || {
                request_callback(conf, timelineid);
            });
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
            resp.ser_into(&mut self.stream_out)?;

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
            if start_pos.segment_offset(wal_seg_size) == 0 {
                xlogoff = 0;
                if partial {
                    fs::rename(&wal_file_partial_path, &wal_file_path)?;
                }
            }
        }
        Ok(())
    }
}
