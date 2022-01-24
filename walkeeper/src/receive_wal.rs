//! Safekeeper communication endpoint to WAL proposer (compute node).
//! Gets messages from the network, passes them down to consensus module and
//! sends replies back.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use bytes::BytesMut;
use tracing::*;

use crate::timeline::Timeline;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::safekeeper::AcceptorProposerMessage;
use crate::safekeeper::ProposerAcceptorMessage;

use crate::handler::SafekeeperPostgresHandler;
use crate::timeline::TimelineTools;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeMessage};
use zenith_utils::zid::ZTenantId;

use crate::callmemaybe::CallmeEvent;
use crate::callmemaybe::SubscriptionStateKey;

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

        // Incoming WAL stream resumed, so reset information about the timeline pause.
        spg.timeline.get().continue_streaming();

        // if requested, ask pageserver to fetch wal from us
        // as long as this wal_stream is alive, callmemaybe thread
        // will send requests to pageserver
        let _guard = match self.pageserver_connstr {
            Some(ref pageserver_connstr) => {
                // Need to establish replication channel with page server.
                // Add far as replication in postgres is initiated by receiver
                // we should use callmemaybe mechanism.
                let timeline_id = spg.timeline.get().timeline_id;
                let subscription_key = SubscriptionStateKey::new(
                    tenant_id,
                    timeline_id,
                    pageserver_connstr.to_owned(),
                );
                spg.tx
                    .send(CallmeEvent::Subscribe(subscription_key))
                    .unwrap_or_else(|e| {
                        error!(
                            "failed to send Subscribe request to callmemaybe thread {}",
                            e
                        );
                    });

                // create a guard to unsubscribe callback, when this wal_stream will exit
                Some(SendWalHandlerGuard {
                    timeline: Arc::clone(spg.timeline.get()),
                })
            }
            None => None,
        };

        loop {
            let reply = spg
                .timeline
                .get()
                .process_msg(&msg)
                .context("failed to process ProposerAcceptorMessage")?;
            if let Some(reply) = reply {
                self.write_msg(&reply)?;
            }
            msg = self.read_msg()?;
        }
    }
}

struct SendWalHandlerGuard {
    timeline: Arc<Timeline>,
}

impl Drop for SendWalHandlerGuard {
    fn drop(&mut self) {
        self.timeline.stop_streaming();
    }
}
