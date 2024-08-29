//! Safekeeper communication endpoint to WAL proposer (compute node).
//! Gets messages from the network, passes them down to consensus module and
//! sends replies back.

use crate::handler::SafekeeperPostgresHandler;
use crate::safekeeper::AcceptorProposerMessage;
use crate::safekeeper::ProposerAcceptorMessage;
use crate::safekeeper::ServerInfo;
use crate::timeline::FullAccessTimeline;
use crate::wal_service::ConnectionId;
use crate::GlobalTimelines;
use anyhow::{anyhow, Context};
use bytes::BytesMut;
use parking_lot::MappedMutexGuard;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use postgres_backend::CopyStreamHandlerEnd;
use postgres_backend::PostgresBackend;
use postgres_backend::PostgresBackendReader;
use postgres_backend::QueryError;
use pq_proto::BeMessage;
use serde::Deserialize;
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio::time::Instant;
use tracing::*;
use utils::id::TenantTimelineId;
use utils::lsn::Lsn;
use utils::pageserver_feedback::PageserverFeedback;

const DEFAULT_FEEDBACK_CAPACITY: usize = 8;

/// Registry of WalReceivers (compute connections). Timeline holds it (wrapped
/// in Arc).
pub struct WalReceivers {
    mutex: Mutex<WalReceiversShared>,
    pageserver_feedback_tx: tokio::sync::broadcast::Sender<PageserverFeedback>,

    num_computes_tx: tokio::sync::watch::Sender<usize>,
    num_computes_rx: tokio::sync::watch::Receiver<usize>,
}

/// Id under which walreceiver is registered in shmem.
type WalReceiverId = usize;

impl WalReceivers {
    pub fn new() -> Arc<WalReceivers> {
        let (pageserver_feedback_tx, _) =
            tokio::sync::broadcast::channel(DEFAULT_FEEDBACK_CAPACITY);

        let (num_computes_tx, num_computes_rx) = tokio::sync::watch::channel(0usize);

        Arc::new(WalReceivers {
            mutex: Mutex::new(WalReceiversShared { slots: Vec::new() }),
            pageserver_feedback_tx,
            num_computes_tx,
            num_computes_rx,
        })
    }

    /// Register new walreceiver. Returned guard provides access to the slot and
    /// automatically deregisters in Drop.
    pub fn register(self: &Arc<WalReceivers>, conn_id: Option<ConnectionId>) -> WalReceiverGuard {
        let mut shared = self.mutex.lock();
        let slots = &mut shared.slots;
        let walreceiver = WalReceiverState {
            conn_id,
            status: WalReceiverStatus::Voting,
        };
        // find empty slot or create new one
        let pos = if let Some(pos) = slots.iter().position(|s| s.is_none()) {
            slots[pos] = Some(walreceiver);
            pos
        } else {
            let pos = slots.len();
            slots.push(Some(walreceiver));
            pos
        };

        self.update_num(&shared);

        WalReceiverGuard {
            id: pos,
            walreceivers: self.clone(),
        }
    }

    /// Get reference to locked slot contents. Slot must exist (registered
    /// earlier).
    fn get_slot<'a>(
        self: &'a Arc<WalReceivers>,
        id: WalReceiverId,
    ) -> MappedMutexGuard<'a, WalReceiverState> {
        MutexGuard::map(self.mutex.lock(), |locked| {
            locked.slots[id]
                .as_mut()
                .expect("walreceiver doesn't exist")
        })
    }

    /// Get number of walreceivers (compute connections).
    pub fn get_num(self: &Arc<WalReceivers>) -> usize {
        self.mutex.lock().get_num()
    }

    /// Get channel for number of walreceivers.
    pub fn get_num_rx(self: &Arc<WalReceivers>) -> tokio::sync::watch::Receiver<usize> {
        self.num_computes_rx.clone()
    }

    /// Should get called after every update of slots.
    fn update_num(self: &Arc<WalReceivers>, shared: &MutexGuard<WalReceiversShared>) {
        let num = shared.get_num();
        self.num_computes_tx.send_replace(num);
    }

    /// Get state of all walreceivers.
    pub fn get_all(self: &Arc<WalReceivers>) -> Vec<WalReceiverState> {
        self.mutex.lock().slots.iter().flatten().cloned().collect()
    }

    /// Get number of streaming walreceivers (normally 0 or 1) from compute.
    pub fn get_num_streaming(self: &Arc<WalReceivers>) -> usize {
        self.mutex
            .lock()
            .slots
            .iter()
            .flatten()
            // conn_id.is_none skips recovery which also registers here
            .filter(|s| s.conn_id.is_some() && matches!(s.status, WalReceiverStatus::Streaming))
            .count()
    }

    /// Unregister walreceiver.
    fn unregister(self: &Arc<WalReceivers>, id: WalReceiverId) {
        let mut shared = self.mutex.lock();
        shared.slots[id] = None;
        self.update_num(&shared);
    }

    /// Broadcast pageserver feedback to connected walproposers.
    pub fn broadcast_pageserver_feedback(&self, feedback: PageserverFeedback) {
        // Err means there is no subscribers, it is fine.
        let _ = self.pageserver_feedback_tx.send(feedback);
    }
}

/// Only a few connections are expected (normally one), so store in Vec.
struct WalReceiversShared {
    slots: Vec<Option<WalReceiverState>>,
}

impl WalReceiversShared {
    /// Get number of walreceivers (compute connections).
    fn get_num(&self) -> usize {
        self.slots.iter().flatten().count()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalReceiverState {
    /// None means it is recovery initiated by us (this safekeeper).
    pub conn_id: Option<ConnectionId>,
    pub status: WalReceiverStatus,
}

/// Walreceiver status. Currently only whether it passed voting stage and
/// started receiving the stream, but it is easy to add more if needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalReceiverStatus {
    Voting,
    Streaming,
}

/// Scope guard to access slot in WalReceivers registry and unregister from
/// it in Drop.
pub struct WalReceiverGuard {
    id: WalReceiverId,
    walreceivers: Arc<WalReceivers>,
}

impl WalReceiverGuard {
    /// Get reference to locked shared state contents.
    fn get(&self) -> MappedMutexGuard<WalReceiverState> {
        self.walreceivers.get_slot(self.id)
    }
}

impl Drop for WalReceiverGuard {
    fn drop(&mut self) {
        self.walreceivers.unregister(self.id);
    }
}

pub const MSG_QUEUE_SIZE: usize = 256;
pub const REPLY_QUEUE_SIZE: usize = 16;

impl SafekeeperPostgresHandler {
    /// Wrapper around handle_start_wal_push_guts handling result. Error is
    /// handled here while we're still in walreceiver ttid span; with API
    /// extension, this can probably be moved into postgres_backend.
    pub async fn handle_start_wal_push<IO: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
    ) -> Result<(), QueryError> {
        let mut tli: Option<FullAccessTimeline> = None;
        if let Err(end) = self.handle_start_wal_push_guts(pgb, &mut tli).await {
            // Log the result and probably send it to the client, closing the stream.
            let handle_end_fut = pgb.handle_copy_stream_end(end);
            // If we managed to create the timeline, augment logging with current LSNs etc.
            if let Some(tli) = tli {
                let info = tli.get_safekeeper_info(&self.conf).await;
                handle_end_fut
                    .instrument(info_span!("", term=%info.term, last_log_term=%info.last_log_term, flush_lsn=%Lsn(info.flush_lsn), commit_lsn=%Lsn(info.commit_lsn)))
                    .await;
            } else {
                handle_end_fut.await;
            }
        }
        Ok(())
    }

    pub async fn handle_start_wal_push_guts<IO: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
        tli: &mut Option<FullAccessTimeline>,
    ) -> Result<(), CopyStreamHandlerEnd> {
        // Notify the libpq client that it's allowed to send `CopyData` messages
        pgb.write_message(&BeMessage::CopyBothResponse).await?;

        // Experiments [1] confirm that doing network IO in one (this) thread and
        // processing with disc IO in another significantly improves
        // performance; we spawn off WalAcceptor thread for message processing
        // to this end.
        //
        // [1] https://github.com/neondatabase/neon/pull/1318
        let (msg_tx, msg_rx) = channel(MSG_QUEUE_SIZE);
        let (reply_tx, reply_rx) = channel(REPLY_QUEUE_SIZE);
        let mut acceptor_handle: Option<JoinHandle<anyhow::Result<()>>> = None;

        // Concurrently receive and send data; replies are not synchronized with
        // sends, so this avoids deadlocks.
        let mut pgb_reader = pgb.split().context("START_WAL_PUSH split")?;
        let peer_addr = *pgb.get_peer_addr();
        let mut network_reader = NetworkReader {
            ttid: self.ttid,
            conn_id: self.conn_id,
            pgb_reader: &mut pgb_reader,
            peer_addr,
            acceptor_handle: &mut acceptor_handle,
        };

        // Read first message and create timeline if needed.
        let res = network_reader.read_first_message().await;

        let network_res = if let Ok((timeline, next_msg)) = res {
            let pageserver_feedback_rx: tokio::sync::broadcast::Receiver<PageserverFeedback> =
                timeline
                    .get_walreceivers()
                    .pageserver_feedback_tx
                    .subscribe();
            *tli = Some(timeline.clone());

            tokio::select! {
                // todo: add read|write .context to these errors
                r = network_reader.run(msg_tx, msg_rx, reply_tx, timeline.clone(), next_msg) => r,
                r = network_write(pgb, reply_rx, pageserver_feedback_rx) => r,
            }
        } else {
            res.map(|_| ())
        };

        // Join pg backend back.
        pgb.unsplit(pgb_reader)?;

        // Join the spawned WalAcceptor. At this point chans to/from it passed
        // to network routines are dropped, so it will exit as soon as it
        // touches them.
        match acceptor_handle {
            None => {
                // failed even before spawning; read_network should have error
                Err(network_res.expect_err("no error with WalAcceptor not spawn"))
            }
            Some(handle) => {
                let wal_acceptor_res = handle.await;

                // If there was any network error, return it.
                network_res?;

                // Otherwise, WalAcceptor thread must have errored.
                match wal_acceptor_res {
                    Ok(Ok(_)) => Ok(()), // can't happen currently; would be if we add graceful termination
                    Ok(Err(e)) => Err(CopyStreamHandlerEnd::Other(e.context("WAL acceptor"))),
                    Err(_) => Err(CopyStreamHandlerEnd::Other(anyhow!(
                        "WalAcceptor task panicked",
                    ))),
                }
            }
        }
    }
}

struct NetworkReader<'a, IO> {
    ttid: TenantTimelineId,
    conn_id: ConnectionId,
    pgb_reader: &'a mut PostgresBackendReader<IO>,
    peer_addr: SocketAddr,
    // WalAcceptor is spawned when we learn server info from walproposer and
    // create timeline; handle is put here.
    acceptor_handle: &'a mut Option<JoinHandle<anyhow::Result<()>>>,
}

impl<'a, IO: AsyncRead + AsyncWrite + Unpin> NetworkReader<'a, IO> {
    async fn read_first_message(
        &mut self,
    ) -> Result<(FullAccessTimeline, ProposerAcceptorMessage), CopyStreamHandlerEnd> {
        // Receive information about server to create timeline, if not yet.
        let next_msg = read_message(self.pgb_reader).await?;
        let tli = match next_msg {
            ProposerAcceptorMessage::Greeting(ref greeting) => {
                info!(
                    "start handshake with walproposer {} sysid {} timeline {}",
                    self.peer_addr, greeting.system_id, greeting.tli,
                );
                let server_info = ServerInfo {
                    pg_version: greeting.pg_version,
                    system_id: greeting.system_id,
                    wal_seg_size: greeting.wal_seg_size,
                };
                let tli =
                    GlobalTimelines::create(self.ttid, server_info, Lsn::INVALID, Lsn::INVALID)
                        .await?;
                tli.full_access_guard().await?
            }
            _ => {
                return Err(CopyStreamHandlerEnd::Other(anyhow::anyhow!(
                    "unexpected message {next_msg:?} instead of greeting"
                )))
            }
        };
        Ok((tli, next_msg))
    }

    async fn run(
        self,
        msg_tx: Sender<ProposerAcceptorMessage>,
        msg_rx: Receiver<ProposerAcceptorMessage>,
        reply_tx: Sender<AcceptorProposerMessage>,
        tli: FullAccessTimeline,
        next_msg: ProposerAcceptorMessage,
    ) -> Result<(), CopyStreamHandlerEnd> {
        *self.acceptor_handle = Some(WalAcceptor::spawn(
            tli,
            msg_rx,
            reply_tx,
            Some(self.conn_id),
        ));

        // Forward all messages to WalAcceptor
        read_network_loop(self.pgb_reader, msg_tx, next_msg).await
    }
}

/// Read next message from walproposer.
/// TODO: Return Ok(None) on graceful termination.
async fn read_message<IO: AsyncRead + AsyncWrite + Unpin>(
    pgb_reader: &mut PostgresBackendReader<IO>,
) -> Result<ProposerAcceptorMessage, CopyStreamHandlerEnd> {
    let copy_data = pgb_reader.read_copy_message().await?;
    let msg = ProposerAcceptorMessage::parse(copy_data)?;
    Ok(msg)
}

async fn read_network_loop<IO: AsyncRead + AsyncWrite + Unpin>(
    pgb_reader: &mut PostgresBackendReader<IO>,
    msg_tx: Sender<ProposerAcceptorMessage>,
    mut next_msg: ProposerAcceptorMessage,
) -> Result<(), CopyStreamHandlerEnd> {
    loop {
        if msg_tx.send(next_msg).await.is_err() {
            return Ok(()); // chan closed, WalAcceptor terminated
        }
        next_msg = read_message(pgb_reader).await?;
    }
}

/// Read replies from WalAcceptor and pass them back to socket. Returns Ok(())
/// if reply_rx closed; it must mean WalAcceptor terminated, joining it should
/// tell the error.
async fn network_write<IO: AsyncRead + AsyncWrite + Unpin>(
    pgb_writer: &mut PostgresBackend<IO>,
    mut reply_rx: Receiver<AcceptorProposerMessage>,
    mut pageserver_feedback_rx: tokio::sync::broadcast::Receiver<PageserverFeedback>,
) -> Result<(), CopyStreamHandlerEnd> {
    let mut buf = BytesMut::with_capacity(128);

    // storing append_response to inject PageserverFeedback into it
    let mut last_append_response = None;

    loop {
        // trying to read either AcceptorProposerMessage or PageserverFeedback
        let msg = tokio::select! {
            reply = reply_rx.recv() => {
                if let Some(msg) = reply {
                    if let AcceptorProposerMessage::AppendResponse(append_response) = &msg {
                        last_append_response = Some(append_response.clone());
                    }
                    Some(msg)
                } else {
                    return Ok(()); // chan closed, WalAcceptor terminated
                }
            }

            feedback = pageserver_feedback_rx.recv() =>
                match (feedback, &last_append_response) {
                    (Ok(feedback), Some(append_response)) => {
                        // clone AppendResponse and inject PageserverFeedback into it
                        let mut append_response = append_response.clone();
                        append_response.pageserver_feedback = Some(feedback);
                        Some(AcceptorProposerMessage::AppendResponse(append_response))
                    }
                    _ => None,
                }
        };

        let Some(msg) = msg else {
            continue;
        };

        buf.clear();
        msg.serialize(&mut buf)?;
        pgb_writer.write_message(&BeMessage::CopyData(&buf)).await?;
    }
}

// Send keepalive messages to walproposer, to make sure it receives updates
// even when it writes a steady stream of messages.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(1);

/// Encapsulates a task which takes messages from msg_rx, processes and pushes
/// replies to reply_tx; reading from socket and writing to disk in parallel is
/// beneficial for performance, this struct provides writing to disk part.
pub struct WalAcceptor {
    tli: FullAccessTimeline,
    msg_rx: Receiver<ProposerAcceptorMessage>,
    reply_tx: Sender<AcceptorProposerMessage>,
    conn_id: Option<ConnectionId>,
}

impl WalAcceptor {
    /// Spawn task with WalAcceptor running, return handle to it. Task returns
    /// Ok(()) if either of channels has closed, and Err if any error during
    /// message processing is encountered.
    ///
    /// conn_id None means WalAcceptor is used by recovery initiated at this safekeeper.
    pub fn spawn(
        tli: FullAccessTimeline,
        msg_rx: Receiver<ProposerAcceptorMessage>,
        reply_tx: Sender<AcceptorProposerMessage>,
        conn_id: Option<ConnectionId>,
    ) -> JoinHandle<anyhow::Result<()>> {
        task::spawn(async move {
            let mut wa = WalAcceptor {
                tli,
                msg_rx,
                reply_tx,
                conn_id,
            };

            let span_ttid = wa.tli.ttid; // satisfy borrow checker
            wa.run()
                .instrument(
                    info_span!("WAL acceptor", cid = %conn_id.unwrap_or(0), ttid = %span_ttid),
                )
                .await
        })
    }

    /// The main loop. Returns Ok(()) if either msg_rx or reply_tx got closed;
    /// it must mean that network thread terminated.
    async fn run(&mut self) -> anyhow::Result<()> {
        let walreceiver_guard = self.tli.get_walreceivers().register(self.conn_id);

        // After this timestamp we will stop processing AppendRequests and send a response
        // to the walproposer. walproposer sends at least one AppendRequest per second,
        // we will send keepalives by replying to these requests once per second.
        let mut next_keepalive = Instant::now();

        loop {
            let opt_msg = self.msg_rx.recv().await;
            if opt_msg.is_none() {
                return Ok(()); // chan closed, streaming terminated
            }
            let mut next_msg = opt_msg.unwrap();

            // Update walreceiver state in shmem for reporting.
            if let ProposerAcceptorMessage::Elected(_) = &next_msg {
                walreceiver_guard.get().status = WalReceiverStatus::Streaming;
            }

            let reply_msg = if matches!(next_msg, ProposerAcceptorMessage::AppendRequest(_)) {
                // loop through AppendRequest's while it's readily available to
                // write as many WAL as possible without fsyncing
                //
                // Note: this will need to be rewritten if we want to read non-AppendRequest messages here.
                // Otherwise, we might end up in a situation where we read a message, but don't
                // process it.
                while let ProposerAcceptorMessage::AppendRequest(append_request) = next_msg {
                    let noflush_msg = ProposerAcceptorMessage::NoFlushAppendRequest(append_request);

                    if let Some(reply) = self.tli.process_msg(&noflush_msg).await? {
                        if self.reply_tx.send(reply).await.is_err() {
                            return Ok(()); // chan closed, streaming terminated
                        }
                    }

                    // get out of this loop if keepalive time is reached
                    if Instant::now() >= next_keepalive {
                        break;
                    }

                    match self.msg_rx.try_recv() {
                        Ok(msg) => next_msg = msg,
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => return Ok(()), // chan closed, streaming terminated
                    }
                }

                // flush all written WAL to the disk
                self.tli
                    .process_msg(&ProposerAcceptorMessage::FlushWAL)
                    .await?
            } else {
                // process message other than AppendRequest
                self.tli.process_msg(&next_msg).await?
            };

            if let Some(reply) = reply_msg {
                if self.reply_tx.send(reply).await.is_err() {
                    return Ok(()); // chan closed, streaming terminated
                }
                // reset keepalive time
                next_keepalive = Instant::now() + KEEPALIVE_INTERVAL;
            }
        }
    }
}
