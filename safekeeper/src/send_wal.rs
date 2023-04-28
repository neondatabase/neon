//! This module implements the streaming side of replication protocol, starting
//! with the "START_REPLICATION" message, and registry of walsenders.

use crate::handler::SafekeeperPostgresHandler;
use crate::timeline::Timeline;
use crate::wal_service::ConnectionId;
use crate::wal_storage::WalReader;
use crate::GlobalTimelines;
use anyhow::Context as AnyhowContext;
use bytes::Bytes;
use parking_lot::Mutex;
use postgres_backend::PostgresBackend;
use postgres_backend::{CopyStreamHandlerEnd, PostgresBackendReader, QueryError};
use postgres_ffi::get_current_timestamp;
use postgres_ffi::{TimestampTz, MAX_SEND_SIZE};
use pq_proto::{BeMessage, WalSndKeepAlive, XLogDataBody};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use tokio::io::{AsyncRead, AsyncWrite};
use utils::id::TenantTimelineId;
use utils::lsn::AtomicLsn;
use utils::pageserver_feedback::PageserverFeedback;

use std::cmp::{max, min};
use std::net::SocketAddr;
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

const INVALID_FULL_TRANSACTION_ID: FullTransactionId = 0;

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
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StandbyReply {
    pub write_lsn: Lsn, // The location of the last WAL byte + 1 received and written to disk in the standby.
    pub flush_lsn: Lsn, // The location of the last WAL byte + 1 flushed to disk in the standby.
    pub apply_lsn: Lsn, // The location of the last WAL byte + 1 applied in the standby.
    pub reply_ts: TimestampTz, // The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
    pub reply_requested: bool,
}

impl StandbyReply {
    fn empty() -> Self {
        StandbyReply {
            write_lsn: Lsn::INVALID,
            flush_lsn: Lsn::INVALID,
            apply_lsn: Lsn::INVALID,
            reply_ts: 0,
            reply_requested: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StandbyFeedback {
    reply: StandbyReply,
    hs_feedback: HotStandbyFeedback,
}

/// WalSenders registry. Timeline holds it (wrapped in Arc).
pub struct WalSenders {
    /// Lsn maximized over all walsenders *and* peer data, so might be higher
    /// than what we receive from replicas.
    remote_consistent_lsn: AtomicLsn,
    mutex: Mutex<WalSendersShared>,
}

impl WalSenders {
    pub fn new(remote_consistent_lsn: Lsn) -> Arc<WalSenders> {
        Arc::new(WalSenders {
            remote_consistent_lsn: AtomicLsn::from(remote_consistent_lsn),
            mutex: Mutex::new(WalSendersShared::new()),
        })
    }

    /// Register new walsender. Returned guard provides access to the slot and
    /// automatically deregisters in Drop.
    fn register(
        self: &Arc<WalSenders>,
        ttid: TenantTimelineId,
        addr: SocketAddr,
        conn_id: ConnectionId,
        appname: Option<String>,
    ) -> WalSenderGuard {
        let slots = &mut self.mutex.lock().slots;
        let walsender_state = WalSenderState {
            ttid,
            addr,
            conn_id,
            appname,
            feedback: ReplicationFeedback::Pageserver(PageserverFeedback::empty()),
        };
        // find empty slot or create new one
        let pos = if let Some(pos) = slots.iter().position(|s| s.is_none()) {
            slots[pos] = Some(walsender_state);
            pos
        } else {
            let pos = slots.len();
            slots.push(Some(walsender_state));
            pos
        };
        WalSenderGuard {
            id: pos,
            walsenders: self.clone(),
        }
    }

    /// Get state of all walsenders.
    pub fn get_all(self: &Arc<WalSenders>) -> Vec<WalSenderState> {
        self.mutex.lock().slots.iter().flatten().cloned().collect()
    }

    /// Get aggregated pageserver feedback.
    pub fn get_ps_feedback(self: &Arc<WalSenders>) -> PageserverFeedback {
        self.mutex.lock().agg_ps_feedback
    }

    /// Get aggregated pageserver and hot standby feedback (we send them to compute).
    pub fn get_feedbacks(self: &Arc<WalSenders>) -> (PageserverFeedback, HotStandbyFeedback) {
        let shared = self.mutex.lock();
        (shared.agg_ps_feedback, shared.agg_hs_feedback)
    }

    /// Record new pageserver feedback, update aggregated values.
    fn record_ps_feedback(self: &Arc<WalSenders>, id: WalSenderId, feedback: &PageserverFeedback) {
        let mut shared = self.mutex.lock();
        shared.get_slot_mut(id).feedback = ReplicationFeedback::Pageserver(*feedback);
        shared.update_ps_feedback();
        self.update_remote_consistent_lsn(shared.agg_ps_feedback.remote_consistent_lsn);
    }

    /// Record standby reply.
    fn record_standby_reply(self: &Arc<WalSenders>, id: WalSenderId, reply: &StandbyReply) {
        let mut shared = self.mutex.lock();
        let slot = shared.get_slot_mut(id);
        match &mut slot.feedback {
            ReplicationFeedback::Standby(sf) => sf.reply = *reply,
            ReplicationFeedback::Pageserver(_) => {
                slot.feedback = ReplicationFeedback::Standby(StandbyFeedback {
                    reply: *reply,
                    hs_feedback: HotStandbyFeedback::empty(),
                })
            }
        }
    }

    /// Record hot standby feedback, update aggregated value.
    fn record_hs_feedback(self: &Arc<WalSenders>, id: WalSenderId, feedback: &HotStandbyFeedback) {
        let mut shared = self.mutex.lock();
        let slot = shared.get_slot_mut(id);
        match &mut slot.feedback {
            ReplicationFeedback::Standby(sf) => sf.hs_feedback = *feedback,
            ReplicationFeedback::Pageserver(_) => {
                slot.feedback = ReplicationFeedback::Standby(StandbyFeedback {
                    reply: StandbyReply::empty(),
                    hs_feedback: *feedback,
                })
            }
        }
        shared.update_hs_feedback();
    }

    /// Get remote_consistent_lsn reported by the pageserver. Returns None if
    /// client is not pageserver.
    fn get_ws_remote_consistent_lsn(self: &Arc<WalSenders>, id: WalSenderId) -> Option<Lsn> {
        let shared = self.mutex.lock();
        let slot = shared.get_slot(id);
        match slot.feedback {
            ReplicationFeedback::Pageserver(feedback) => Some(feedback.remote_consistent_lsn),
            _ => None,
        }
    }

    /// Get remote_consistent_lsn maximized across all walsenders and peers.
    pub fn get_remote_consistent_lsn(self: &Arc<WalSenders>) -> Lsn {
        self.remote_consistent_lsn.load()
    }

    /// Update maximized remote_consistent_lsn, return new (potentially) value.
    pub fn update_remote_consistent_lsn(self: &Arc<WalSenders>, candidate: Lsn) -> Lsn {
        self.remote_consistent_lsn
            .fetch_max(candidate)
            .max(candidate)
    }

    /// Unregister walsender.
    fn unregister(self: &Arc<WalSenders>, id: WalSenderId) {
        let mut shared = self.mutex.lock();
        shared.slots[id] = None;
        shared.update_hs_feedback();
    }
}

struct WalSendersShared {
    // aggregated over all walsenders value
    agg_hs_feedback: HotStandbyFeedback,
    // aggregated over all walsenders value
    agg_ps_feedback: PageserverFeedback,
    slots: Vec<Option<WalSenderState>>,
}

impl WalSendersShared {
    fn new() -> Self {
        WalSendersShared {
            agg_hs_feedback: HotStandbyFeedback::empty(),
            agg_ps_feedback: PageserverFeedback::empty(),
            slots: Vec::new(),
        }
    }

    /// Get content of provided id slot, it must exist.
    fn get_slot(&self, id: WalSenderId) -> &WalSenderState {
        self.slots[id].as_ref().expect("walsender doesn't exist")
    }

    /// Get mut content of provided id slot, it must exist.
    fn get_slot_mut(&mut self, id: WalSenderId) -> &mut WalSenderState {
        self.slots[id].as_mut().expect("walsender doesn't exist")
    }

    /// Update aggregated hot standy feedback. We just take min of valid xmins
    /// and ts.
    fn update_hs_feedback(&mut self) {
        let mut agg = HotStandbyFeedback::empty();
        for ws_state in self.slots.iter().flatten() {
            if let ReplicationFeedback::Standby(standby_feedback) = ws_state.feedback {
                let hs_feedback = standby_feedback.hs_feedback;
                // doing Option math like op1.iter().chain(op2.iter()).min()
                // would be nicer, but we serialize/deserialize this struct
                // directly, so leave as is for now
                if hs_feedback.xmin != INVALID_FULL_TRANSACTION_ID {
                    if agg.xmin != INVALID_FULL_TRANSACTION_ID {
                        agg.xmin = min(agg.xmin, hs_feedback.xmin);
                    } else {
                        agg.xmin = hs_feedback.xmin;
                    }
                    agg.ts = min(agg.ts, hs_feedback.ts);
                }
                if hs_feedback.catalog_xmin != INVALID_FULL_TRANSACTION_ID {
                    if agg.catalog_xmin != INVALID_FULL_TRANSACTION_ID {
                        agg.catalog_xmin = min(agg.catalog_xmin, hs_feedback.catalog_xmin);
                    } else {
                        agg.catalog_xmin = hs_feedback.catalog_xmin;
                    }
                    agg.ts = min(agg.ts, hs_feedback.ts);
                }
            }
        }
        self.agg_hs_feedback = agg;
    }

    /// Update aggregated pageserver feedback. LSNs (last_received,
    /// disk_consistent, remote_consistent) and reply timestamp are just
    /// maximized; timeline_size if taken from feedback with highest
    /// last_received lsn. This is generally reasonable, but we might want to
    /// implement other policies once multiple pageservers start to be actively
    /// used.
    fn update_ps_feedback(&mut self) {
        let init = PageserverFeedback::empty();
        let acc =
            self.slots
                .iter()
                .flatten()
                .fold(init, |mut acc, ws_state| match ws_state.feedback {
                    ReplicationFeedback::Pageserver(feedback) => {
                        if feedback.last_received_lsn > acc.last_received_lsn {
                            acc.current_timeline_size = feedback.current_timeline_size;
                        }
                        acc.last_received_lsn =
                            max(feedback.last_received_lsn, acc.last_received_lsn);
                        acc.disk_consistent_lsn =
                            max(feedback.disk_consistent_lsn, acc.disk_consistent_lsn);
                        acc.remote_consistent_lsn =
                            max(feedback.remote_consistent_lsn, acc.remote_consistent_lsn);
                        acc.replytime = max(feedback.replytime, acc.replytime);
                        acc
                    }
                    ReplicationFeedback::Standby(_) => acc,
                });
        self.agg_ps_feedback = acc;
    }
}

// Serialized is used only for pretty printing in json.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSenderState {
    #[serde_as(as = "DisplayFromStr")]
    ttid: TenantTimelineId,
    addr: SocketAddr,
    conn_id: ConnectionId,
    // postgres application_name
    appname: Option<String>,
    feedback: ReplicationFeedback,
}

// Receiver is either pageserver or regular standby, which have different
// feedbacks.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum ReplicationFeedback {
    Pageserver(PageserverFeedback),
    Standby(StandbyFeedback),
}

// id of the occupied slot in WalSenders to access it (and save in the
// WalSenderGuard). We could give Arc directly to the slot, but there is not
// much sense in that as values aggregation which is performed on each feedback
// receival iterates over all walsenders.
pub type WalSenderId = usize;

/// Scope guard to access slot in WalSenders registry and unregister from it in
/// Drop.
pub struct WalSenderGuard {
    id: WalSenderId,
    walsenders: Arc<WalSenders>,
}

impl Drop for WalSenderGuard {
    fn drop(&mut self) {
        self.walsenders.unregister(self.id);
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

        // Use a guard object to remove our entry from the timeline when we are done.
        let ws_guard = Arc::new(tli.get_walsenders().register(
            self.ttid,
            *pgb.get_peer_addr(),
            self.conn_id,
            self.appname.clone(),
        ));

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
            let wal_end = tli.get_flush_lsn().await;
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

        let (_, persisted_state) = tli.get_state().await;
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
            ws_guard: ws_guard.clone(),
            wal_reader,
            send_buf: [0; MAX_SEND_SIZE],
        };
        let mut reply_reader = ReplyReader { reader, ws_guard };

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
    ws_guard: Arc<WalSenderGuard>,
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
            if let Some(remote_consistent_lsn) = self
                .ws_guard
                .walsenders
                .get_ws_remote_consistent_lsn(self.ws_guard.id)
            {
                if self.tli.should_walsender_stop(remote_consistent_lsn).await {
                    // Terminate if there is nothing more to send.
                    return Err(CopyStreamHandlerEnd::ServerInitiated(format!(
                        "ending streaming to {:?} at {}, receiver is caughtup and there is no computes",
                        self.appname, self.start_pos,
                    )));
                }
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
    ws_guard: Arc<WalSenderGuard>,
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
                let hs_feedback = HotStandbyFeedback::des(&msg[1..])
                    .context("failed to deserialize HotStandbyFeedback")?;
                self.ws_guard
                    .walsenders
                    .record_hs_feedback(self.ws_guard.id, &hs_feedback);
            }
            Some(STANDBY_STATUS_UPDATE_TAG_BYTE) => {
                let reply =
                    StandbyReply::des(&msg[1..]).context("failed to deserialize StandbyReply")?;
                self.ws_guard
                    .walsenders
                    .record_standby_reply(self.ws_guard.id, &reply);
            }
            Some(NEON_STATUS_UPDATE_TAG_BYTE) => {
                // pageserver sends this.
                // Note: deserializing is on m[9..] because we skip the tag byte and len bytes.
                let buf = Bytes::copy_from_slice(&msg[9..]);
                let ps_feedback = PageserverFeedback::parse(buf);

                trace!("PageserverFeedback is {:?}", ps_feedback);
                self.ws_guard
                    .walsenders
                    .record_ps_feedback(self.ws_guard.id, &ps_feedback);
                // in principle new remote_consistent_lsn could allow to
                // deactivate the timeline, but we check that regularly through
                // broker updated, not need to do it here
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

#[cfg(test)]
mod tests {
    use postgres_protocol::PG_EPOCH;
    use utils::id::{TenantId, TimelineId};

    use super::*;

    fn mock_ttid() -> TenantTimelineId {
        TenantTimelineId {
            tenant_id: TenantId::from_slice(&[0x00; 16]).unwrap(),
            timeline_id: TimelineId::from_slice(&[0x00; 16]).unwrap(),
        }
    }

    fn mock_addr() -> SocketAddr {
        "127.0.0.1:8080".parse().unwrap()
    }

    // add to wss specified feedback setting other fields to dummy values
    fn push_feedback(wss: &mut WalSendersShared, feedback: ReplicationFeedback) {
        let walsender_state = WalSenderState {
            ttid: mock_ttid(),
            addr: mock_addr(),
            conn_id: 1,
            appname: None,
            feedback,
        };
        wss.slots.push(Some(walsender_state))
    }

    // form standby feedback with given hot standby feedback ts/xmin and the
    // rest set to dummy values.
    fn hs_feedback(ts: TimestampTz, xmin: FullTransactionId) -> ReplicationFeedback {
        ReplicationFeedback::Standby(StandbyFeedback {
            reply: StandbyReply::empty(),
            hs_feedback: HotStandbyFeedback {
                ts,
                xmin,
                catalog_xmin: 0,
            },
        })
    }

    // test that hs aggregation works as expected
    #[test]
    fn test_hs_feedback_no_valid() {
        let mut wss = WalSendersShared::new();
        push_feedback(&mut wss, hs_feedback(1, INVALID_FULL_TRANSACTION_ID));
        wss.update_hs_feedback();
        assert_eq!(wss.agg_hs_feedback.xmin, INVALID_FULL_TRANSACTION_ID);
    }

    #[test]
    fn test_hs_feedback() {
        let mut wss = WalSendersShared::new();
        push_feedback(&mut wss, hs_feedback(1, INVALID_FULL_TRANSACTION_ID));
        push_feedback(&mut wss, hs_feedback(1, 42));
        push_feedback(&mut wss, hs_feedback(1, 64));
        wss.update_hs_feedback();
        assert_eq!(wss.agg_hs_feedback.xmin, 42);
    }

    // form pageserver feedback with given last_record_lsn / tli size and the
    // rest set to dummy values.
    fn ps_feedback(current_timeline_size: u64, last_received_lsn: Lsn) -> ReplicationFeedback {
        ReplicationFeedback::Pageserver(PageserverFeedback {
            current_timeline_size,
            last_received_lsn,
            disk_consistent_lsn: Lsn::INVALID,
            remote_consistent_lsn: Lsn::INVALID,
            replytime: *PG_EPOCH,
        })
    }

    // test that ps aggregation works as expected
    #[test]
    fn test_ps_feedback() {
        let mut wss = WalSendersShared::new();
        push_feedback(&mut wss, ps_feedback(8, Lsn(42)));
        push_feedback(&mut wss, ps_feedback(4, Lsn(84)));
        wss.update_ps_feedback();
        assert_eq!(wss.agg_ps_feedback.current_timeline_size, 4);
        assert_eq!(wss.agg_ps_feedback.last_received_lsn, Lsn(84));
    }
}
