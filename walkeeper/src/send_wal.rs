//! This module implements the streaming side of replication protocol, starting
//! with the "START_REPLICATION" message.

use crate::handler::SafekeeperPostgresHandler;
use crate::timeline::{ReplicaState, Timeline, TimelineTools};
use anyhow::{anyhow, bail, Context, Result};

use postgres_ffi::xlog_utils::{
    get_current_timestamp, TimestampTz, XLogFileName, MAX_SEND_SIZE, PG_TLI,
};

use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::net::Shutdown;
use std::path::Path;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use std::{str, thread};
use tracing::*;
use zenith_utils::bin_ser::BeSer;
use zenith_utils::lsn::Lsn;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeMessage, WalSndKeepAlive, XLogDataBody};
use zenith_utils::sock_split::ReadStream;

use crate::callmemaybe::CallmeEvent;
use tokio::sync::mpsc::UnboundedSender;
use zenith_utils::zid::{ZTenantId, ZTimelineId};

// See: https://www.postgresql.org/docs/13/protocol-replication.html
const HOT_STANDBY_FEEDBACK_TAG_BYTE: u8 = b'h';
const STANDBY_STATUS_UPDATE_TAG_BYTE: u8 = b'r';

type FullTransactionId = u64;

/// Hot standby feedback received from replica
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HotStandbyFeedback {
    pub ts: TimestampTz,
    pub xmin: FullTransactionId,
    pub catalog_xmin: FullTransactionId,
}

impl HotStandbyFeedback {
    pub fn empty() -> HotStandbyFeedback {
        HotStandbyFeedback {
            ts: 0,
            xmin: 0,
            catalog_xmin: 0,
        }
    }
}

/// Standby status update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StandbyReply {
    pub write_lsn: Lsn, // last lsn received by pageserver
    pub flush_lsn: Lsn, // pageserver's disk consistent lSN
    pub apply_lsn: Lsn, // pageserver's remote consistent lSN
    pub reply_ts: TimestampTz,
    pub reply_requested: bool,
}

/// A network connection that's speaking the replication protocol.
pub struct ReplicationConn {
    /// This is an `Option` because we will spawn a background thread that will
    /// `take` it from us.
    stream_in: Option<ReadStream>,
}

/// Scope guard to unregister replication connection from timeline
struct ReplicationConnGuard {
    replica: usize, // replica internal ID assigned by timeline
    timeline: Arc<Timeline>,
}

impl Drop for ReplicationConnGuard {
    fn drop(&mut self) {
        self.timeline.update_replica_state(self.replica, None);
    }
}

// XXX: Naming is a bit messy here.
// This ReplicationStreamGuard lives as long as ReplicationConn
// and current ReplicationConnGuard is tied to the background thread
// that receives feedback.
struct ReplicationStreamGuard {
    tx: UnboundedSender<CallmeEvent>,
    tenant_id: ZTenantId,
    timelineid: ZTimelineId,
}

impl Drop for ReplicationStreamGuard {
    fn drop(&mut self) {
        // the connection with pageserver is lost,
        // resume callback subscription
        debug!("Connection to pageserver is gone. Resume callmemeybe subsciption if necessary. tenantid {} timelineid {}",
         self.tenant_id, self.timelineid);

        self.tx
            .send(CallmeEvent::Resume(self.tenant_id, self.timelineid))
            .unwrap_or_else(|e| {
                error!("failed to send Resume request to callmemaybe thread {}", e);
            });
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
        mut stream_in: ReadStream,
        timeline: Arc<Timeline>,
        replica_id: usize,
    ) -> Result<()> {
        let mut state = ReplicaState::new();
        let _guard = ReplicationConnGuard {
            replica: replica_id,
            timeline: timeline.clone(),
        };
        // Wait for replica's feedback.
        while let Some(msg) = FeMessage::read(&mut stream_in)? {
            match &msg {
                FeMessage::CopyData(m) => {
                    // There's two possible data messages that the client is supposed to send here:
                    // `HotStandbyFeedback` and `StandbyStatusUpdate`.

                    match m.first().cloned() {
                        Some(HOT_STANDBY_FEEDBACK_TAG_BYTE) => {
                            // Note: deserializing is on m[1..] because we skip the tag byte.
                            state.hs_feedback = HotStandbyFeedback::des(&m[1..])
                                .context("failed to deserialize HotStandbyFeedback")?;
                            timeline.update_replica_state(replica_id, Some(state));
                        }
                        Some(STANDBY_STATUS_UPDATE_TAG_BYTE) => {
                            let reply = StandbyReply::des(&m[1..])
                                .context("failed to deserialize StandbyReply")?;
                            state.last_received_lsn = reply.write_lsn;
                            state.disk_consistent_lsn = reply.flush_lsn;
                            state.remote_consistent_lsn = reply.apply_lsn;
                            timeline.update_replica_state(replica_id, Some(state));
                        }
                        _ => warn!("unexpected message {:?}", msg),
                    }
                }
                FeMessage::Sync => {}
                FeMessage::CopyFail => {
                    // Shutdown the connection, because rust-postgres client cannot be dropped
                    // when connection is alive.
                    let _ = stream_in.shutdown(Shutdown::Both);
                    return Err(anyhow!("Copy failed"));
                }
                _ => {
                    // We only handle `CopyData`, 'Sync', 'CopyFail' messages. Anything else is ignored.
                    info!("unexpected message {:?}", msg);
                }
            }
        }

        Ok(())
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
        File::open(&wal_file_path)
            .with_context(|| format!("Failed to open WAL file {:?}", wal_file_path))
            .map_err(|e| {
                error!("{}", e);
                e
            })
    }

    ///
    /// Handle START_REPLICATION replication command
    ///
    pub fn run(
        &mut self,
        spg: &mut SafekeeperPostgresHandler,
        pgb: &mut PostgresBackend,
        mut start_pos: Lsn,
    ) -> Result<()> {
        let _enter = info_span!("WAL sender", timeline = %spg.ztimelineid.unwrap()).entered();

        // spawn the background thread which receives HotStandbyFeedback messages.
        let bg_timeline = Arc::clone(spg.timeline.get());
        let bg_stream_in = self.stream_in.take().unwrap();

        let state = ReplicaState::new();
        // This replica_id is used below to check if it's time to stop replication.
        let replica_id = bg_timeline.add_replica(state);

        // TODO: here we got two threads, one for writing WAL and one for receiving
        // feedback. If one of them fails, we should shutdown the other one too.
        let _ = thread::Builder::new()
            .name("HotStandbyFeedback thread".into())
            .spawn(move || {
                if let Err(err) = Self::background_thread(bg_stream_in, bg_timeline, replica_id) {
                    error!("Replication background thread failed: {}", err);
                }
            })
            .unwrap();

        let mut wal_seg_size: usize;
        loop {
            wal_seg_size = spg.timeline.get().get_info().server.wal_seg_size as usize;
            if wal_seg_size == 0 {
                error!("Cannot start replication before connecting to wal_proposer");
                sleep(Duration::from_secs(1));
            } else {
                break;
            }
        }
        let wal_end = spg.timeline.get().get_end_of_wal();
        // Walproposer gets special handling: safekeeper must give proposer all
        // local WAL till the end, whether committed or not (walproposer will
        // hang otherwise). That's because walproposer runs the consensus and
        // synchronizes safekeepers on the most advanced one.
        //
        // There is a small risk of this WAL getting concurrently garbaged if
        // another compute rises which collects majority and starts fixing log
        // on this safekeeper itself. That's ok as (old) proposer will never be
        // able to commit such WAL.
        let stop_pos: Option<Lsn> = if spg.appname == Some("wal_proposer_recovery".to_string()) {
            Some(wal_end)
        } else {
            None
        };
        info!("Start replication from {:?} till {:?}", start_pos, stop_pos);

        // Don't spam pageserver with callmemaybe queries
        // when replication connection with pageserver is already established.
        let _guard = {
            if spg.appname == Some("wal_proposer_recovery".to_string()) {
                None
            } else {
                let timelineid = spg.timeline.get().timelineid;
                let tenant_id = spg.ztenantid.unwrap();
                let tx_clone = spg.tx.clone();
                spg.tx
                    .send(CallmeEvent::Pause(tenant_id, timelineid))
                    .unwrap_or_else(|e| {
                        error!("failed to send Pause request to callmemaybe thread {}", e);
                    });

                // create a guard to subscribe callback again, when this connection will exit
                Some(ReplicationStreamGuard {
                    tx: tx_clone,
                    tenant_id,
                    timelineid,
                })
            }
        };

        // switch to copy
        pgb.write_message(&BeMessage::CopyBothResponse)?;

        let mut end_pos = Lsn(0);
        let mut wal_file: Option<File> = None;

        loop {
            if let Some(stop_pos) = stop_pos {
                if start_pos >= stop_pos {
                    break; /* recovery finished */
                }
                end_pos = stop_pos;
            } else {
                /* Wait until we have some data to stream */
                let lsn = spg.timeline.get().wait_for_lsn(start_pos);

                if let Some(lsn) = lsn {
                    end_pos = lsn;
                } else {
                    // TODO: also check once in a while whether we are walsender
                    // to right pageserver.
                    if spg.timeline.get().check_deactivate(
                        replica_id,
                        spg.ztenantid.unwrap(),
                        &spg.tx,
                    )? {
                        // Shut down, timeline is suspended.
                        // TODO create proper error type for this
                        bail!("end streaming to {:?}", spg.appname);
                    }

                    // timeout expired: request pageserver status
                    pgb.write_message(&BeMessage::KeepAlive(WalSndKeepAlive {
                        sent_ptr: end_pos.0,
                        timestamp: get_current_timestamp(),
                        request_reply: true,
                    }))
                    .context("Failed to send KeepAlive message")?;
                    continue;
                }
            }

            // Take the `File` from `wal_file`, or open a new file.
            let mut file = match wal_file.take() {
                Some(file) => file,
                None => {
                    // Open a new file.
                    let segno = start_pos.segment_number(wal_seg_size);
                    let wal_file_name = XLogFileName(PG_TLI, segno, wal_seg_size);
                    let timeline_id = spg.timeline.get().timelineid;
                    let wal_file_path = spg.conf.timeline_dir(&timeline_id).join(wal_file_name);
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
            file.seek(SeekFrom::Start(xlogoff as u64))
                .and_then(|_| file.read_exact(&mut file_buf))
                .context("Failed to read data from WAL file")?;

            // Write some data to the network socket.
            pgb.write_message(&BeMessage::XLogData(XLogDataBody {
                wal_start: start_pos.0,
                wal_end: end_pos.0,
                timestamp: get_current_timestamp(),
                data: &file_buf,
            }))
            .context("Failed to send XLogData")?;

            start_pos += send_size as u64;

            info!("sent WAL up to {}", start_pos);

            // Decide whether to reuse this file. If we don't set wal_file here
            // a new file will be opened next time.
            if start_pos.segment_offset(wal_seg_size) != 0 {
                wal_file = Some(file);
            }
        }
        Ok(())
    }
}
