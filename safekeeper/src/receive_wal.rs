//! Safekeeper communication endpoint to WAL proposer (compute node).
//! Gets messages from the network, passes them down to consensus module and
//! sends replies back.

use crate::handler::SafekeeperPostgresHandler;
use crate::safekeeper::AcceptorProposerMessage;
use crate::safekeeper::ProposerAcceptorMessage;
use crate::safekeeper::ServerInfo;
use crate::timeline::Timeline;
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

/// Registry of WalReceivers (compute connections). Timeline holds it (wrapped
/// in Arc).
pub struct WalReceivers {
    mutex: Mutex<WalReceiversShared>,
}

/// Id under which walreceiver is registered in shmem.
type WalReceiverId = usize;

impl WalReceivers {
    pub fn new() -> Arc<WalReceivers> {
        Arc::new(WalReceivers {
            mutex: Mutex::new(WalReceiversShared { slots: Vec::new() }),
        })
    }

    /// Register new walreceiver. Returned guard provides access to the slot and
    /// automatically deregisters in Drop.
    pub fn register(self: &Arc<WalReceivers>) -> WalReceiverGuard {
        let slots = &mut self.mutex.lock().slots;
        let walreceiver = WalReceiverState::Voting;
        // find empty slot or create new one
        let pos = if let Some(pos) = slots.iter().position(|s| s.is_none()) {
            slots[pos] = Some(walreceiver);
            pos
        } else {
            let pos = slots.len();
            slots.push(Some(walreceiver));
            pos
        };
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
        self.mutex.lock().slots.iter().flatten().count()
    }

    /// Get state of all walreceivers.
    pub fn get_all(self: &Arc<WalReceivers>) -> Vec<WalReceiverState> {
        self.mutex.lock().slots.iter().flatten().cloned().collect()
    }

    /// Unregister walsender.
    fn unregister(self: &Arc<WalReceivers>, id: WalReceiverId) {
        let mut shared = self.mutex.lock();
        shared.slots[id] = None;
    }
}

/// Only a few connections are expected (normally one), so store in Vec.
struct WalReceiversShared {
    slots: Vec<Option<WalReceiverState>>,
}

/// Walreceiver status. Currently only whether it passed voting stage and
/// started receiving the stream, but it is easy to add more if needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalReceiverState {
    Voting,
    Streaming,
}

/// Scope guard to access slot in WalSenders registry and unregister from it in
/// Drop.
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

const MSG_QUEUE_SIZE: usize = 256;
const REPLY_QUEUE_SIZE: usize = 16;

impl SafekeeperPostgresHandler {
    /// Wrapper around handle_start_wal_push_guts handling result. Error is
    /// handled here while we're still in walreceiver ttid span; with API
    /// extension, this can probably be moved into postgres_backend.
    pub async fn handle_start_wal_push<IO: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
    ) -> Result<(), QueryError> {
        if let Err(end) = self.handle_start_wal_push_guts(pgb).await {
            // Log the result and probably send it to the client, closing the stream.
            pgb.handle_copy_stream_end(end).await;
        }
        Ok(())
    }

    pub async fn handle_start_wal_push_guts<IO: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        pgb: &mut PostgresBackend<IO>,
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
        let network_reader = NetworkReader {
            ttid: self.ttid,
            conn_id: self.conn_id,
            pgb_reader: &mut pgb_reader,
            peer_addr,
            acceptor_handle: &mut acceptor_handle,
        };
        let res = tokio::select! {
            // todo: add read|write .context to these errors
            r = network_reader.run(msg_tx, msg_rx, reply_tx) => r,
            r = network_write(pgb, reply_rx) => r,
        };

        // Join pg backend back.
        pgb.unsplit(pgb_reader)?;

        // Join the spawned WalAcceptor. At this point chans to/from it passed
        // to network routines are dropped, so it will exit as soon as it
        // touches them.
        match acceptor_handle {
            None => {
                // failed even before spawning; read_network should have error
                Err(res.expect_err("no error with WalAcceptor not spawn"))
            }
            Some(handle) => {
                let wal_acceptor_res = handle.await;

                // If there was any network error, return it.
                res?;

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
    async fn run(
        self,
        msg_tx: Sender<ProposerAcceptorMessage>,
        msg_rx: Receiver<ProposerAcceptorMessage>,
        reply_tx: Sender<AcceptorProposerMessage>,
    ) -> Result<(), CopyStreamHandlerEnd> {
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
                GlobalTimelines::create(self.ttid, server_info, Lsn::INVALID, Lsn::INVALID).await?
            }
            _ => {
                return Err(CopyStreamHandlerEnd::Other(anyhow::anyhow!(
                    "unexpected message {next_msg:?} instead of greeting"
                )))
            }
        };

        *self.acceptor_handle = Some(WalAcceptor::spawn(
            tli.clone(),
            msg_rx,
            reply_tx,
            self.conn_id,
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
) -> Result<(), CopyStreamHandlerEnd> {
    let mut buf = BytesMut::with_capacity(128);

    loop {
        match reply_rx.recv().await {
            Some(msg) => {
                buf.clear();
                msg.serialize(&mut buf)?;
                pgb_writer.write_message(&BeMessage::CopyData(&buf)).await?;
            }
            None => return Ok(()), // chan closed, WalAcceptor terminated
        }
    }
}

// Send keepalive messages to walproposer, to make sure it receives updates
// even when it writes a steady stream of messages.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(1);

/// Takes messages from msg_rx, processes and pushes replies to reply_tx.
struct WalAcceptor {
    tli: Arc<Timeline>,
    msg_rx: Receiver<ProposerAcceptorMessage>,
    reply_tx: Sender<AcceptorProposerMessage>,
}

impl WalAcceptor {
    /// Spawn thread with WalAcceptor running, return handle to it.
    fn spawn(
        tli: Arc<Timeline>,
        msg_rx: Receiver<ProposerAcceptorMessage>,
        reply_tx: Sender<AcceptorProposerMessage>,
        conn_id: ConnectionId,
    ) -> JoinHandle<anyhow::Result<()>> {
        task::spawn(async move {
            let mut wa = WalAcceptor {
                tli,
                msg_rx,
                reply_tx,
            };

            let span_ttid = wa.tli.ttid; // satisfy borrow checker
            wa.run()
                .instrument(info_span!("WAL acceptor", cid = %conn_id, ttid = %span_ttid))
                .await
        })
    }

    /// The main loop. Returns Ok(()) if either msg_rx or reply_tx got closed;
    /// it must mean that network thread terminated.
    async fn run(&mut self) -> anyhow::Result<()> {
        // Register the connection and defer unregister.
        // Order of the next two lines is important: we want first to remove our entry and then
        // update status which depends on registered connections.
        let _compute_conn_guard = ComputeConnectionGuard {
            timeline: Arc::clone(&self.tli),
        };
        let walreceiver_guard = self.tli.get_walreceivers().register();
        self.tli.update_status_notify().await?;

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
                *walreceiver_guard.get() = WalReceiverState::Streaming;
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

/// Calls update_status_notify in drop to update timeline status.
struct ComputeConnectionGuard {
    timeline: Arc<Timeline>,
}

impl Drop for ComputeConnectionGuard {
    fn drop(&mut self) {
        let tli = self.timeline.clone();
        tokio::spawn(async move {
            if let Err(e) = tli.update_status_notify().await {
                error!("failed to update timeline status: {}", e);
            }
        });
    }
}
