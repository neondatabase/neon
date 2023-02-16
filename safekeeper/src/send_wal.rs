//! This module implements the streaming side of replication protocol, starting
//! with the "START_REPLICATION" message.

use crate::handler::SafekeeperPostgresHandler;
use crate::timeline::{ReplicaState, Timeline};
use crate::wal_storage::WalReader;
use crate::GlobalTimelines;
use anyhow::Context as AnyhowContext;
use bytes::Bytes;
use postgres_backend::PostgresBackend;
use postgres_backend::{CopyStreamHandlerEnd, PostgresBackendReader, QueryError};
use postgres_ffi::get_current_timestamp;
use postgres_ffi::{TimestampTz, MAX_SEND_SIZE};
use pq_proto::{BeMessage, ReplicationFeedback, WalSndKeepAlive, XLogDataBody};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};

use std::cmp::min;
use std::str;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch::Receiver;
use tokio::time::timeout;
use tracing::*;
use utils::{bin_ser::BeSer, lsn::Lsn};

// See: https://www.postgresql.org/docs/13/protocol-replication.html
const HOT_STANDBY_FEEDBACK_TAG_BYTE: u8 = b'h';
const STANDBY_STATUS_UPDATE_TAG_BYTE: u8 = b'r';
// neon extension of replication protocol
const NEON_STATUS_UPDATE_TAG_BYTE: u8 = b'z';

type FullTransactionId = u64;

/// Hot standby feedback received from replica
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Deserialize)]
pub struct StandbyReply {
    pub write_lsn: Lsn, // last lsn received by pageserver
    pub flush_lsn: Lsn, // pageserver's disk consistent lSN
    pub apply_lsn: Lsn, // pageserver's remote consistent lSN
    pub reply_ts: TimestampTz,
    pub reply_requested: bool,
}

/// Scope guard to unregister replication connection from timeline
struct ReplicationConnGuard {
    replica: usize, // replica internal ID assigned by timeline
    timeline: Arc<Timeline>,
}

impl Drop for ReplicationConnGuard {
    fn drop(&mut self) {
        self.timeline.remove_replica(self.replica);
    }
}

impl SafekeeperPostgresHandler {
    /// Wrapper around handle_start_replication_guts handling result. Error is
    /// handled here while we're still in walsender ttid span; with API
    /// extension, this can probably be moved into postgres_backend.
    pub async fn handle_start_replication<IO: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        start_pos: Lsn,
    ) -> Result<(), QueryError> {
        if let Err(end) = self.handle_start_replication_guts(pgb, start_pos).await {
            // Log the result and probably send it to the client, closing the stream.
            pgb.handle_copy_stream_end(end).await;
        }
        Ok(())
    }

    pub async fn handle_start_replication_guts<IO: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        start_pos: Lsn,
    ) -> Result<(), CopyStreamHandlerEnd> {
        let appname = self.appname.clone();
        let tli =
            GlobalTimelines::get(self.ttid).map_err(|e| CopyStreamHandlerEnd::Other(e.into()))?;

        let state = ReplicaState::new();
        // This replica_id is used below to check if it's time to stop replication.
        let replica_id = tli.add_replica(state);

        // Use a guard object to remove our entry from the timeline, when the background
        // thread and us have both finished using it.
        let _guard = Arc::new(ReplicationConnGuard {
            replica: replica_id,
            timeline: tli.clone(),
        });

        // Walproposer gets special handling: safekeeper must give proposer all
        // local WAL till the end, whether committed or not (walproposer will
        // hang otherwise). That's because walproposer runs the consensus and
        // synchronizes safekeepers on the most advanced one.
        //
        // There is a small risk of this WAL getting concurrently garbaged if
        // another compute rises which collects majority and starts fixing log
        // on this safekeeper itself. That's ok as (old) proposer will never be
        // able to commit such WAL.
        let stop_pos: Option<Lsn> = if self.is_walproposer_recovery() {
            let wal_end = tli.get_flush_lsn();
            Some(wal_end)
        } else {
            None
        };
        let end_pos = stop_pos.unwrap_or(Lsn::INVALID);

        info!(
            "starting streaming from {:?} till {:?}",
            start_pos, stop_pos
        );

        // switch to copy
        pgb.write_message(&BeMessage::CopyBothResponse).await?;

        let (_, persisted_state) = tli.get_state();
        let wal_reader = WalReader::new(
            self.conf.workdir.clone(),
            self.conf.timeline_dir(&tli.ttid),
            &persisted_state,
            start_pos,
            self.conf.wal_backup_enabled,
        )?;

        // Split to concurrently receive and send data; replies are generally
        // not synchronized with sends, so this avoids deadlocks.
        let reader = pgb.split().context("START_REPLICATION split")?;

        let mut sender = WalSender {
            pgb,
            tli: tli.clone(),
            appname,
            start_pos,
            end_pos,
            stop_pos,
            commit_lsn_watch_rx: tli.get_commit_lsn_watch_rx(),
            replica_id,
            wal_reader,
            send_buf: [0; MAX_SEND_SIZE],
        };
        let mut reply_reader = ReplyReader {
            reader,
            tli,
            replica_id,
            feedback: ReplicaState::new(),
        };

        let res = tokio::select! {
            // todo: add read|write .context to these errors
            r = sender.run() => r,
            r = reply_reader.run() => r,
        };
        // Join pg backend back.
        pgb.unsplit(reply_reader.reader)?;

        res
    }
}

/// A half driving sending WAL.
struct WalSender<'a, IO> {
    pgb: &'a mut PostgresBackend<IO>,
    tli: Arc<Timeline>,
    appname: Option<String>,
    // Position since which we are sending next chunk.
    start_pos: Lsn,
    // WAL up to this position is known to be locally available.
    end_pos: Lsn,
    // If present, terminate after reaching this position; used by walproposer
    // in recovery.
    stop_pos: Option<Lsn>,
    commit_lsn_watch_rx: Receiver<Lsn>,
    replica_id: usize,
    wal_reader: WalReader,
    // buffer for readling WAL into to send it
    send_buf: [u8; MAX_SEND_SIZE],
}

impl<IO: AsyncRead + AsyncWrite + Unpin> WalSender<'_, IO> {
    /// Send WAL until
    /// - an error occurs
    /// - if we are streaming to walproposer, we've streamed until stop_pos
    ///   (recovery finished)
    /// - receiver is caughtup and there is no computes
    ///
    /// Err(CopyStreamHandlerEnd) is always returned; Result is used only for ?
    /// convenience.
    async fn run(&mut self) -> Result<(), CopyStreamHandlerEnd> {
        loop {
            // If we are streaming to walproposer, check it is time to stop.
            if let Some(stop_pos) = self.stop_pos {
                if self.start_pos >= stop_pos {
                    // recovery finished
                    return Err(CopyStreamHandlerEnd::ServerInitiated(format!(
                        "ending streaming to walproposer at {}, recovery finished",
                        self.start_pos
                    )));
                }
            } else {
                // Wait for the next portion if it is not there yet, or just
                // update our end of WAL available for sending value, we
                // communicate it to the receiver.
                self.wait_wal().await?;
            }

            // try to send as much as available, capped by MAX_SEND_SIZE
            let mut send_size = self
                .end_pos
                .checked_sub(self.start_pos)
                .context("reading wal without waiting for it first")?
                .0 as usize;
            send_size = min(send_size, self.send_buf.len());
            let send_buf = &mut self.send_buf[..send_size];
            // read wal into buffer
            send_size = self.wal_reader.read(send_buf).await?;
            let send_buf = &send_buf[..send_size];

            // and send it
            self.pgb
                .write_message(&BeMessage::XLogData(XLogDataBody {
                    wal_start: self.start_pos.0,
                    wal_end: self.end_pos.0,
                    timestamp: get_current_timestamp(),
                    data: send_buf,
                }))
                .await?;

            trace!(
                "sent {} bytes of WAL {}-{}",
                send_size,
                self.start_pos,
                self.start_pos + send_size as u64
            );
            self.start_pos += send_size as u64;
        }
    }

    /// wait until we have WAL to stream, sending keepalives and checking for
    /// exit in the meanwhile
    async fn wait_wal(&mut self) -> Result<(), CopyStreamHandlerEnd> {
        loop {
            if let Some(lsn) = wait_for_lsn(&mut self.commit_lsn_watch_rx, self.start_pos).await? {
                self.end_pos = lsn;
                return Ok(());
            }
            // Timed out waiting for WAL, check for termination and send KA
            if self.tli.should_walsender_stop(self.replica_id) {
                // Terminate if there is nothing more to send.
                // TODO close the stream properly
                return Err(CopyStreamHandlerEnd::ServerInitiated(format!(
                    "ending streaming to {:?} at {}, receiver is caughtup and there is no computes",
                    self.appname, self.start_pos,
                )));
            }
            self.pgb
                .write_message(&BeMessage::KeepAlive(WalSndKeepAlive {
                    sent_ptr: self.end_pos.0,
                    timestamp: get_current_timestamp(),
                    request_reply: true,
                }))
                .await?;
        }
    }
}

/// A half driving receiving replies.
struct ReplyReader<IO> {
    reader: PostgresBackendReader<IO>,
    tli: Arc<Timeline>,
    replica_id: usize,
    feedback: ReplicaState,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> ReplyReader<IO> {
    async fn run(&mut self) -> Result<(), CopyStreamHandlerEnd> {
        loop {
            let msg = self.reader.read_copy_message().await?;
            self.handle_feedback(&msg)?
        }
    }

    fn handle_feedback(&mut self, msg: &Bytes) -> anyhow::Result<()> {
        match msg.first().cloned() {
            Some(HOT_STANDBY_FEEDBACK_TAG_BYTE) => {
                // Note: deserializing is on m[1..] because we skip the tag byte.
                self.feedback.hs_feedback = HotStandbyFeedback::des(&msg[1..])
                    .context("failed to deserialize HotStandbyFeedback")?;
                self.tli
                    .update_replica_state(self.replica_id, self.feedback);
            }
            Some(STANDBY_STATUS_UPDATE_TAG_BYTE) => {
                let _reply =
                    StandbyReply::des(&msg[1..]).context("failed to deserialize StandbyReply")?;
                // This must be a regular postgres replica,
                // because pageserver doesn't send this type of messages to safekeeper.
                // Currently we just ignore this, tracking progress for them is not supported.
            }
            Some(NEON_STATUS_UPDATE_TAG_BYTE) => {
                // pageserver sends this.
                // Note: deserializing is on m[9..] because we skip the tag byte and len bytes.
                let buf = Bytes::copy_from_slice(&msg[9..]);
                let reply = ReplicationFeedback::parse(buf);

                trace!("ReplicationFeedback is {:?}", reply);
                // Only pageserver sends ReplicationFeedback, so set the flag.
                // This replica is the source of information to resend to compute.
                self.feedback.pageserver_feedback = Some(reply);

                self.tli
                    .update_replica_state(self.replica_id, self.feedback);
            }
            _ => warn!("unexpected message {:?}", msg),
        }
        Ok(())
    }
}

const POLL_STATE_TIMEOUT: Duration = Duration::from_secs(1);

/// Wait until we have commit_lsn > lsn or timeout expires. Returns
/// - Ok(Some(commit_lsn)) if needed lsn is successfully observed;
/// - Ok(None) if timeout expired;
/// - Err in case of error (if watch channel is in trouble, shouldn't happen).
async fn wait_for_lsn(rx: &mut Receiver<Lsn>, lsn: Lsn) -> anyhow::Result<Option<Lsn>> {
    let commit_lsn: Lsn = *rx.borrow();
    if commit_lsn > lsn {
        return Ok(Some(commit_lsn));
    }

    let res = timeout(POLL_STATE_TIMEOUT, async move {
        let mut commit_lsn;
        loop {
            rx.changed().await?;
            commit_lsn = *rx.borrow();
            if commit_lsn > lsn {
                break;
            }
        }

        Ok(commit_lsn)
    })
    .await;

    match res {
        // success
        Ok(Ok(commit_lsn)) => Ok(Some(commit_lsn)),
        // error inside closure
        Ok(Err(err)) => Err(err),
        // timeout
        Err(_) => Ok(None),
    }
}
