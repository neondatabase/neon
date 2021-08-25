//! This module implements the streaming side of replication protocol, starting
//! with the "START_REPLICATION" message.

use crate::send_wal::SendWalHandler;
use crate::timeline::{Timeline, TimelineTools};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use log::*;
use postgres_ffi::xlog_utils::{get_current_timestamp, TimestampTz, XLogFileName, MAX_SEND_SIZE};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use std::{str, thread};
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeMessage, XLogDataBody};

pub const END_REPLICATION_MARKER: Lsn = Lsn::MAX;

type FullTransactionId = u64;

/// Hot standby feedback received from replica
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HotStandbyFeedback {
    pub ts: TimestampTz,
    pub xmin: FullTransactionId,
    pub catalog_xmin: FullTransactionId,
}

/// A network connection that's speaking the replication protocol.
pub struct ReplicationConn {
    /// This is an `Option` because we will spawn a background thread that will
    /// `take` it from us.
    stream_in: Option<BufReader<TcpStream>>,
}

// TODO: move this to crate::timeline when there's more users
// TODO: design a proper Timeline mock api
trait HsFeedbackSubscriber {
    fn add_hs_feedback(&self, _feedback: HotStandbyFeedback) {}
}

impl HsFeedbackSubscriber for Arc<Timeline> {
    #[inline(always)]
    fn add_hs_feedback(&self, feedback: HotStandbyFeedback) {
        Timeline::add_hs_feedback(self, feedback);
    }
}

impl ReplicationConn {
    /// Create a new `ReplicationConn`
    pub fn new(pgb: &mut PostgresBackend) -> Self {
        Self {
            stream_in: pgb.take_stream_in(),
        }
    }

    /// Handle incoming messages from the network.
    /// This is spawned into the background by `handle_start_replication`.
    fn background_thread(
        mut stream_in: impl Read,
        subscriber: impl HsFeedbackSubscriber,
    ) -> Result<()> {
        // Wait for replica's feedback.
        while let Some(msg) = FeMessage::read(&mut stream_in)? {
            match msg {
                FeMessage::CopyData(m) => {
                    let feedback = HotStandbyFeedback::des(&m)?;
                    subscriber.add_hs_feedback(feedback);
                }
                FeMessage::Sync => {}
                FeMessage::CopyFailed => return Err(anyhow!("Copy failed")),
                _ => {
                    // We only handle `CopyData`, 'Sync', 'CopyFailed' messages. Anything else is ignored.
                    info!("unexpected message {:?}", msg);
                }
            }
        }

        Ok(())
    }

    /// Helper function that parses a pair of LSNs.
    fn parse_start_stop(cmd: &[u8]) -> Result<(Lsn, Lsn)> {
        let re = Regex::new(r"([[:xdigit:]]+/[[:xdigit:]]+)").unwrap();
        let caps = re.captures_iter(str::from_utf8(cmd)?);
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
            Ok(opened_file) => Ok(opened_file),
            Err(e) => {
                error!("Failed to open log file {:?}: {}", &wal_file_path, e);
                Err(e.into())
            }
        }
    }

    ///
    /// Handle START_REPLICATION replication command
    ///
    pub fn run(
        &mut self,
        swh: &mut SendWalHandler,
        pgb: &mut PostgresBackend,
        cmd: &Bytes,
    ) -> Result<()> {
        // spawn the background thread which receives HotStandbyFeedback messages.
        let bg_timeline = Arc::clone(swh.timeline.get());
        let bg_stream_in = self.stream_in.take().unwrap();

        thread::spawn(move || {
            if let Err(err) = Self::background_thread(bg_stream_in, bg_timeline) {
                error!("socket error: {}", err);
            }
        });

        let (mut start_pos, mut stop_pos) = Self::parse_start_stop(&cmd)?;

        let mut wal_seg_size: usize;
        loop {
            wal_seg_size = swh.timeline.get().get_info().server.wal_seg_size as usize;
            if wal_seg_size == 0 {
                error!("Can not start replication before connecting to wal_proposer");
                sleep(Duration::from_secs(1));
            } else {
                break;
            }
        }
        let (wal_end, timeline) = swh.timeline.find_end_of_wal(&swh.conf.data_dir, true);
        if start_pos == Lsn(0) {
            start_pos = wal_end;
        }
        if stop_pos == Lsn(0) && swh.appname == Some("wal_proposer_recovery".to_string()) {
            stop_pos = wal_end;
        }
        info!("Start replication from {} till {}", start_pos, stop_pos);

        // switch to copy
        pgb.write_message(&BeMessage::CopyBothResponse)?;

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
                let timeline = swh.timeline.get();
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
                    let segno = start_pos.segment_number(wal_seg_size);
                    let wal_file_name = XLogFileName(timeline, segno, wal_seg_size);
                    let timeline_id = swh.timeline.get().timelineid.to_string();
                    let wal_file_path = swh.conf.data_dir.join(timeline_id).join(wal_file_name);
                    Self::open_wal_file(&wal_file_path)?
                }
            };

            let xlogoff = start_pos.segment_offset(wal_seg_size) as usize;

            // How much to read and send in message? We cannot cross the WAL file
            // boundary, and we don't want send more than MAX_SEND_SIZE.
            let send_size = end_pos.checked_sub(start_pos).unwrap().0 as usize;
            let send_size = min(send_size, wal_seg_size - xlogoff);
            let send_size = min(send_size, MAX_SEND_SIZE);

            // Read some data from the file.
            let mut file_buf = vec![0u8; send_size];
            file.seek(SeekFrom::Start(xlogoff as u64))?;
            file.read_exact(&mut file_buf)?;

            // Write some data to the network socket.
            pgb.write_message(&BeMessage::XLogData(XLogDataBody {
                wal_start: start_pos.0,
                wal_end: end_pos.0,
                timestamp: get_current_timestamp(),
                data: &file_buf,
            }))?;

            info!("Sent WAL to page server {}..{}, end_pos={}", start_pos, start_pos + send_size as u64, end_pos);

            start_pos += send_size as u64;

            // Decide whether to reuse this file. If we don't set wal_file here
            // a new file will be opened next time.
            if start_pos.segment_offset(wal_seg_size) != 0 {
                wal_file = Some(file);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A no-op impl for tests
    impl HsFeedbackSubscriber for () {}

    #[test]
    fn test_replication_conn_background_thread_eof() {
        // Test that background_thread recognizes EOF
        let stream: &[u8] = &[];
        ReplicationConn::background_thread(stream, ()).unwrap();
    }
}
