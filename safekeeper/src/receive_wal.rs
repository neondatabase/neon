//! Safekeeper communication endpoint to WAL proposer (compute node).
//! Gets messages from the network, passes them down to consensus module and
//! sends replies back.

use anyhow::anyhow;
use anyhow::Context;

use bytes::BytesMut;
use tracing::*;
use utils::lsn::Lsn;
use utils::postgres_backend::QueryError;

use crate::safekeeper::ServerInfo;
use crate::timeline::Timeline;
use crate::GlobalTimelines;

use std::net::SocketAddr;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;

use std::sync::Arc;
use std::thread;

use crate::safekeeper::AcceptorProposerMessage;
use crate::safekeeper::ProposerAcceptorMessage;

use crate::handler::SafekeeperPostgresHandler;
use pq_proto::{BeMessage, FeMessage};
use utils::{postgres_backend::PostgresBackend, sock_split::ReadStream};

pub struct ReceiveWalConn<'pg> {
    /// Postgres connection
    pg_backend: &'pg mut PostgresBackend,
    /// The cached result of `pg_backend.socket().peer_addr()` (roughly)
    peer_addr: SocketAddr,
}

impl<'pg> ReceiveWalConn<'pg> {
    pub fn new(pg: &'pg mut PostgresBackend) -> ReceiveWalConn<'pg> {
        let peer_addr = *pg.get_peer_addr();
        ReceiveWalConn {
            pg_backend: pg,
            peer_addr,
        }
    }

    // Send message to the postgres
    fn write_msg(&mut self, msg: &AcceptorProposerMessage) -> anyhow::Result<()> {
        let mut buf = BytesMut::with_capacity(128);
        msg.serialize(&mut buf)?;
        self.pg_backend.write_message(&BeMessage::CopyData(&buf))?;
        Ok(())
    }

    /// Receive WAL from wal_proposer
    pub fn run(&mut self, spg: &mut SafekeeperPostgresHandler) -> Result<(), QueryError> {
        let _enter = info_span!("WAL acceptor", ttid = %spg.ttid).entered();

        // Notify the libpq client that it's allowed to send `CopyData` messages
        self.pg_backend
            .write_message(&BeMessage::CopyBothResponse)?;

        let r = self
            .pg_backend
            .take_stream_in()
            .ok_or_else(|| anyhow!("failed to take read stream from pgbackend"))?;
        let mut poll_reader = ProposerPollStream::new(r)?;

        // Receive information about server
        let next_msg = poll_reader.recv_msg()?;
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
                GlobalTimelines::create(spg.ttid, server_info, Lsn::INVALID, Lsn::INVALID)?
            }
            _ => {
                return Err(QueryError::Other(anyhow::anyhow!(
                    "unexpected message {next_msg:?} instead of greeting"
                )))
            }
        };

        let mut next_msg = Some(next_msg);

        let mut first_time_through = true;
        let mut _guard: Option<ComputeConnectionGuard> = None;
        loop {
            if matches!(next_msg, Some(ProposerAcceptorMessage::AppendRequest(_))) {
                // poll AppendRequest's without blocking and write WAL to disk without flushing,
                // while it's readily available
                while let Some(ProposerAcceptorMessage::AppendRequest(append_request)) = next_msg {
                    let msg = ProposerAcceptorMessage::NoFlushAppendRequest(append_request);

                    let reply = tli.process_msg(&msg)?;
                    if let Some(reply) = reply {
                        self.write_msg(&reply)?;
                    }

                    next_msg = poll_reader.poll_msg();
                }

                // flush all written WAL to the disk
                let reply = tli.process_msg(&ProposerAcceptorMessage::FlushWAL)?;
                if let Some(reply) = reply {
                    self.write_msg(&reply)?;
                }
            } else if let Some(msg) = next_msg.take() {
                // process other message
                let reply = tli.process_msg(&msg)?;
                if let Some(reply) = reply {
                    self.write_msg(&reply)?;
                }
            }
            if first_time_through {
                // Register the connection and defer unregister. Do that only
                // after processing first message, as it sets wal_seg_size,
                // wanted by many.
                tli.on_compute_connect()?;
                _guard = Some(ComputeConnectionGuard {
                    timeline: Arc::clone(&tli),
                });
                first_time_through = false;
            }

            // blocking wait for the next message
            if next_msg.is_none() {
                next_msg = Some(poll_reader.recv_msg()?);
            }
        }
    }
}

struct ProposerPollStream {
    msg_rx: Receiver<ProposerAcceptorMessage>,
    read_thread: Option<thread::JoinHandle<Result<(), QueryError>>>,
}

impl ProposerPollStream {
    fn new(mut r: ReadStream) -> anyhow::Result<Self> {
        let (msg_tx, msg_rx) = channel();

        let read_thread = thread::Builder::new()
            .name("Read WAL thread".into())
            .spawn(move || -> Result<(), QueryError> {
                loop {
                    let copy_data = match FeMessage::read(&mut r)? {
                        Some(FeMessage::CopyData(bytes)) => Ok(bytes),
                        Some(msg) => Err(QueryError::Other(anyhow::anyhow!(
                            "expected `CopyData` message, found {msg:?}"
                        ))),
                        None => Err(QueryError::from(std::io::Error::new(
                            std::io::ErrorKind::ConnectionAborted,
                            "walproposer closed the connection",
                        ))),
                    }?;

                    let msg = ProposerAcceptorMessage::parse(copy_data)?;
                    msg_tx
                        .send(msg)
                        .context("Failed to send the proposer message")?;
                }
                // msg_tx will be dropped here, this will also close msg_rx
            })?;

        Ok(Self {
            msg_rx,
            read_thread: Some(read_thread),
        })
    }

    fn recv_msg(&mut self) -> Result<ProposerAcceptorMessage, QueryError> {
        self.msg_rx.recv().map_err(|_| {
            // return error from the read thread
            let res = match self.read_thread.take() {
                Some(thread) => thread.join(),
                None => return QueryError::Other(anyhow::anyhow!("read thread is gone")),
            };

            match res {
                Ok(Ok(())) => {
                    QueryError::Other(anyhow::anyhow!("unexpected result from read thread"))
                }
                Err(err) => QueryError::Other(anyhow::anyhow!("read thread panicked: {err:?}")),
                Ok(Err(err)) => err,
            }
        })
    }

    fn poll_msg(&mut self) -> Option<ProposerAcceptorMessage> {
        let res = self.msg_rx.try_recv();

        match res {
            Err(_) => None,
            Ok(msg) => Some(msg),
        }
    }
}

struct ComputeConnectionGuard {
    timeline: Arc<Timeline>,
}

impl Drop for ComputeConnectionGuard {
    fn drop(&mut self) {
        if let Err(e) = self.timeline.on_compute_disconnect() {
            error!("failed to unregister compute connection: {}", e);
        }
    }
}
