//! Safekeeper communication endpoint to WAL proposer (compute node).
//! Gets messages from the network, passes them down to consensus module and
//! sends replies back.

use anyhow::{anyhow, bail, Result};

use bytes::BytesMut;
use tokio::sync::mpsc::UnboundedSender;
use tracing::*;

use crate::timeline::Timeline;

use std::net::SocketAddr;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;

use std::sync::Arc;
use std::thread;

use crate::safekeeper::AcceptorProposerMessage;
use crate::safekeeper::ProposerAcceptorMessage;

use crate::handler::SafekeeperPostgresHandler;
use crate::timeline::TimelineTools;
use utils::{
    postgres_backend::PostgresBackend,
    pq_proto::{BeMessage, FeMessage},
    sock_split::ReadStream,
};

use crate::callmemaybe::CallmeEvent;

pub struct ReceiveWalConn<'pg> {
    /// Postgres connection
    pg_backend: &'pg mut PostgresBackend,
    /// The cached result of `pg_backend.socket().peer_addr()` (roughly)
    peer_addr: SocketAddr,
    /// Pageserver connection string forwarded from compute
    /// NOTE that it is allowed to operate without a pageserver.
    /// So if compute has no pageserver configured do not use it.
    pageserver_connstr: Option<String>,
}

impl<'pg> ReceiveWalConn<'pg> {
    pub fn new(
        pg: &'pg mut PostgresBackend,
        pageserver_connstr: Option<String>,
    ) -> ReceiveWalConn<'pg> {
        let peer_addr = *pg.get_peer_addr();
        ReceiveWalConn {
            pg_backend: pg,
            peer_addr,
            pageserver_connstr,
        }
    }

    // Send message to the postgres
    fn write_msg(&mut self, msg: &AcceptorProposerMessage) -> Result<()> {
        let mut buf = BytesMut::with_capacity(128);
        msg.serialize(&mut buf)?;
        self.pg_backend.write_message(&BeMessage::CopyData(&buf))?;
        Ok(())
    }

    /// Receive WAL from wal_proposer
    pub fn run(&mut self, spg: &mut SafekeeperPostgresHandler) -> Result<()> {
        let _enter = info_span!("WAL acceptor", timeline = %spg.ztimelineid.unwrap()).entered();

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
        match next_msg {
            ProposerAcceptorMessage::Greeting(ref greeting) => {
                info!(
                    "start handshake with wal proposer {} sysid {} timeline {}",
                    self.peer_addr, greeting.system_id, greeting.tli,
                );
            }
            _ => bail!("unexpected message {:?} instead of greeting", next_msg),
        }

        // Register the connection and defer unregister.
        spg.timeline
            .get()
            .on_compute_connect(self.pageserver_connstr.as_ref(), &spg.tx)?;
        let _guard = ComputeConnectionGuard {
            timeline: Arc::clone(spg.timeline.get()),
            callmemaybe_tx: spg.tx.clone(),
        };

        let mut next_msg = Some(next_msg);

        loop {
            if matches!(next_msg, Some(ProposerAcceptorMessage::AppendRequest(_))) {
                // poll AppendRequest's without blocking and write WAL to disk without flushing,
                // while it's readily available
                while let Some(ProposerAcceptorMessage::AppendRequest(append_request)) = next_msg {
                    let msg = ProposerAcceptorMessage::NoFlushAppendRequest(append_request);

                    let reply = spg.process_safekeeper_msg(&msg)?;
                    if let Some(reply) = reply {
                        self.write_msg(&reply)?;
                    }

                    next_msg = poll_reader.poll_msg();
                }

                // flush all written WAL to the disk
                let reply = spg.process_safekeeper_msg(&ProposerAcceptorMessage::FlushWAL)?;
                if let Some(reply) = reply {
                    self.write_msg(&reply)?;
                }
            } else if let Some(msg) = next_msg.take() {
                // process other message
                let reply = spg.process_safekeeper_msg(&msg)?;
                if let Some(reply) = reply {
                    self.write_msg(&reply)?;
                }
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
    read_thread: Option<thread::JoinHandle<Result<()>>>,
}

impl ProposerPollStream {
    fn new(mut r: ReadStream) -> Result<Self> {
        let (msg_tx, msg_rx) = channel();

        let read_thread = thread::Builder::new()
            .name("Read WAL thread".into())
            .spawn(move || -> Result<()> {
                loop {
                    let copy_data = match FeMessage::read(&mut r)? {
                        Some(FeMessage::CopyData(bytes)) => bytes,
                        Some(msg) => bail!("expected `CopyData` message, found {:?}", msg),
                        None => bail!("connection closed unexpectedly"),
                    };

                    let msg = ProposerAcceptorMessage::parse(copy_data)?;
                    msg_tx.send(msg)?;
                }
                // msg_tx will be dropped here, this will also close msg_rx
            })?;

        Ok(Self {
            msg_rx,
            read_thread: Some(read_thread),
        })
    }

    fn recv_msg(&mut self) -> Result<ProposerAcceptorMessage> {
        self.msg_rx.recv().map_err(|_| {
            // return error from the read thread
            let res = match self.read_thread.take() {
                Some(thread) => thread.join(),
                None => return anyhow!("read thread is gone"),
            };

            match res {
                Ok(Ok(())) => anyhow!("unexpected result from read thread"),
                Err(err) => anyhow!("read thread panicked: {:?}", err),
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
    callmemaybe_tx: UnboundedSender<CallmeEvent>,
}

impl Drop for ComputeConnectionGuard {
    fn drop(&mut self) {
        self.timeline
            .on_compute_disconnect(&self.callmemaybe_tx)
            .unwrap();
    }
}
