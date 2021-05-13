//! This module implements the replication protocol, starting with the
//! "START REPLICATION" message.

use crate::pq_protocol::{BeMessage, FeMessage};
use crate::send_wal::SendWal;
use crate::wal_service::{
    HotStandbyFeedback, Timeline, TimelineTools, END_REPLICATION_MARKER, MAX_SEND_SIZE,
};
use crate::WalAcceptorConf;
use anyhow::{anyhow, bail, Result};
use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, Bytes, BytesMut};
use log::*;
use postgres_ffi::xlog_utils::{get_current_timestamp, XLogFileName};
use regex::Regex;
use std::cmp::min;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::{str, thread};
use zenith_utils::lsn::Lsn;

const XLOG_HDR_SIZE: usize = 1 + 8 * 3; /* 'w' + startPos + walEnd + timestamp */
const LIBPQ_HDR_SIZE: usize = 5; /* 1 byte with message type + 4 bytes length */
const LIBPQ_MSG_SIZE_OFFS: usize = 1;

// FIXME: we don't use consistent endian on this data structure.
// In wal_service it's little-endian, but here it's big-endian.
// FIXME: This function should go away and be replaced by
// derived serde::Deserialize
impl HotStandbyFeedback {
    fn parse(body: &Bytes) -> HotStandbyFeedback {
        HotStandbyFeedback {
            ts: BigEndian::read_u64(&body[0..8]),
            xmin: BigEndian::read_u64(&body[8..16]),
            catalog_xmin: BigEndian::read_u64(&body[16..24]),
        }
    }
}

pub struct ReplicationHandler {
    timeline: Option<Arc<Timeline>>,
    /// Postgres connection, buffered input
    ///
    /// This is an `Option` because we will spawn a background thread that will
    /// `take` it from us.
    stream_in: Option<BufReader<TcpStream>>,
    /// Postgres connection, output
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
    pub fn run(&mut self, cmd: &Bytes) -> Result<()> {
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
                end_pos = timeline.wait_for_lsn(start_pos);
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
                    let wal_file_path = self.conf.data_dir.join(timeline_id).join(wal_file_name);
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
