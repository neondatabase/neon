//! Safekeeper communication endpoint to wal proposer (compute node).
//! Gets messages from the network, passes them down to consensus module and
//! sends replies back.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use bytes::BytesMut;
<<<<<<< HEAD
use postgres::{Client, Config, NoTls};
use tracing::*;
=======
use log::*;
>>>>>>> callmemaybe refactoring

use std::net::SocketAddr;

use crate::safekeeper::AcceptorProposerMessage;
use crate::safekeeper::ProposerAcceptorMessage;

use crate::send_wal::SendWalHandler;
use crate::timeline::TimelineTools;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeMessage};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use crate::callmemaybe::CallmeEvent;
use tokio::sync::mpsc::Sender;

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
    pub fn run(&mut self, swh: &mut SendWalHandler) -> Result<()> {
        let _enter = info_span!("WAL acceptor", timeline = %swh.timelineid.unwrap()).entered();

        // Notify the libpq client that it's allowed to send `CopyData` messages
        self.pg_backend
            .write_message(&BeMessage::CopyBothResponse)?;

        // Receive information about server
        let mut msg = self
            .read_msg()
            .context("failed to receive proposer greeting")?;
        let tenant_id: ZTenantId;
        match msg {
            ProposerAcceptorMessage::Greeting(ref greeting) => {
                info!(
                    "start handshake with wal proposer {} sysid {} timeline {}",
                    self.peer_addr, greeting.system_id, greeting.tli,
                );
                tenant_id = greeting.tenant_id;
            }
            _ => bail!("unexpected message {:?} instead of greeting", msg),
        }

        // if requested, ask pageserver to fetch wal from us
        // as long as this wal_stream is alive, callmemaybe thread
        // will send requests to pageserver
        let _guard = match self.pageserver_connstr {
            Some(ref pageserver_connstr) => {
                // Need to establish replication channel with page server.
                // Add far as replication in postgres is initiated by receiver, we should use callme mechanism
                let timelineid = swh.timeline.get().timelineid;
                let tx_clone = swh.tx.clone();
                let pageserver_connstr = pageserver_connstr.to_owned();
                swh.tx
                    .blocking_send(CallmeEvent::Subscribe(
                        tenant_id,
                        timelineid,
                        pageserver_connstr,
                    ))
                    .unwrap();

                // create a guard to unsubscribe callback, when this wal_stream will exit
                Some(SendWalHandlerGuard {
                    tx: tx_clone,
                    tenant_id,
                    timelineid,
                })
            }
            None => None,
        };

        loop {
            let reply = swh
                .timeline
                .get()
                .process_msg(&msg)
                .with_context(|| "failed to process ProposerAcceptorMessage")?;
            if let Some(reply) = reply {
                self.write_msg(&reply)?;
            }
            msg = self.read_msg()?;
        }
    }
}

struct SendWalHandlerGuard {
    tx: Sender<CallmeEvent>,
    tenant_id: ZTenantId,
    timelineid: ZTimelineId,
}

impl Drop for SendWalHandlerGuard {
    fn drop(&mut self) {
        self.tx
            .blocking_send(CallmeEvent::Unsubscribe(self.tenant_id, self.timelineid))
            .unwrap();
    }
}
