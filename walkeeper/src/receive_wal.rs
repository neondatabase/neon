//! Safekeeper communication endpoint to WAL proposer (compute node).
//! Gets messages from the network, passes them down to consensus module and
//! sends replies back.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use bytes::BytesMut;
use tokio::sync::mpsc::UnboundedSender;
use tracing::*;

use crate::timeline::Timeline;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::thread;
use std::io::{self, Write};

use crate::safekeeper::AcceptorProposerMessage;
use crate::safekeeper::ProposerAcceptorMessage;

use crate::handler::SafekeeperPostgresHandler;
use crate::timeline::TimelineTools;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeMessage};

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

    // Read and extract the bytes of a `CopyData` message from the postgres instance
    fn read_msg_bytes(&mut self) -> Result<Bytes> {
        match self.pg_backend.read_message()? {
            Some(FeMessage::CopyData(bytes)) => Ok(bytes),
            Some(msg) => bail!("expected `CopyData` message, found {:?}", msg),
            None => bail!("connection closed unexpectedly"),
        }
    }

    // Read and parse message sent from the postgres instance
    fn read_msg(&mut self) -> Result<ProposerAcceptorMessage> {
        let data = self.read_msg_bytes()?;
        ProposerAcceptorMessage::parse(data)
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

        // Receive information about server
        let mut msg = self
            .read_msg()
            .context("failed to receive proposer greeting")?;
        match msg {
            ProposerAcceptorMessage::Greeting(ref greeting) => {
                info!(
                    "start handshake with wal proposer {} sysid {} timeline {}",
                    self.peer_addr, greeting.system_id, greeting.tli,
                );
            }
            _ => bail!("unexpected message {:?} instead of greeting", msg),
        }

        // Register the connection and defer unregister.
        spg.timeline
            .get()
            .on_compute_connect(self.pageserver_connstr.as_ref(), &spg.tx)?;
        let _guard = ComputeConnectionGuard {
            timeline: Arc::clone(spg.timeline.get()),
            callmemaybe_tx: spg.tx.clone(),
        };

        let (mut r, mut w) = self.pg_backend.take_streams_io().unwrap();

        let (reply_tx, reply_rx) = channel::<Option<AcceptorProposerMessage>>();

        let write_thread = thread::Builder::new()
            .name("Reply feedback thread".into())
            .spawn(move|| -> Result<()> {
                let mut buf_out = BytesMut::with_capacity(10 * 1024);

                // thread code
                while let Ok(msg) = reply_rx.recv() {
                    if let Some(reply) = msg {
                        let mut buf = BytesMut::with_capacity(128);
                        reply.serialize(&mut buf)?;
                        let copy_data = BeMessage::CopyData(&buf);
                        BeMessage::write(&mut buf_out, &copy_data)?;
                        w.write_all(&buf_out)?;
                        buf_out.clear();
                    }
                }
                Ok(())
            }).unwrap();

        let (request_tx, request_rx) = channel();

        let read_thread = thread::Builder::new()
            .name("Read WAL thread".into())
            .spawn(move|| -> Result<()> {
                request_tx.send(msg)?;
                
                loop {
                    let copy_data = match FeMessage::read(&mut r)? {
                        Some(FeMessage::CopyData(bytes)) => bytes,
                        Some(msg) => bail!("expected `CopyData` message, found {:?}", msg),
                        None => bail!("connection closed unexpectedly"),
                    };
            
                    msg = ProposerAcceptorMessage::parse(copy_data)?;
                    request_tx.send(msg)?;
                }
            }).unwrap();

        let mut pending_flush = false;
        let mut pending_reply = None;
        loop {
            let msg;
            let res = request_rx.try_recv();
            if res.is_ok() && matches!(res, Ok(ProposerAcceptorMessage::AppendRequest(_))) {
                msg = res.unwrap();
            } else {
                if pending_flush {
                    reply_tx.send(pending_reply)?;
                    pending_flush = false;
                    pending_reply = None;
                }
                if res.is_ok() {
                    msg = res.unwrap();
                } else {
                    msg = request_rx.recv()?;
                }
            }

            let reply = spg
                .timeline
                .get()
                .process_msg(&msg)
                .context("failed to process ProposerAcceptorMessage")?;

            if let Some(AcceptorProposerMessage::AppendResponse(_)) = reply {
                pending_reply = reply;
                pending_flush = true;
            } else {
                reply_tx.send(reply)?;
            }
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
