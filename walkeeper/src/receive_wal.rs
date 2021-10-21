//! Safekeeper communication endpoint to wal proposer (compute node).
//! Gets messages from the network, passes them down to consensus module and
//! sends replies back.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use log::*;
use postgres::{Client, Config, NoTls};

use std::net::SocketAddr;
use std::thread;
use std::thread::sleep;

use crate::safekeeper::AcceptorProposerMessage;
use crate::safekeeper::ProposerAcceptorMessage;

use crate::send_wal::SendWalHandler;
use crate::timeline::TimelineTools;
use crate::SafeKeeperConf;
use zenith_utils::connstring::connection_host_port;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeMessage};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

pub struct ReceiveWalConn<'pg> {
    /// Postgres connection
    pg_backend: &'pg mut PostgresBackend,
    /// The cached result of `pg_backend.socket().peer_addr()` (roughly)
    peer_addr: SocketAddr,
}

///
/// Periodically request pageserver to call back.
/// If pageserver already has replication channel, it will just ignore this request
///
fn request_callback(conf: SafeKeeperConf, timelineid: ZTimelineId, tenantid: ZTenantId) {
    let ps_addr = conf.pageserver_addr.unwrap();
    let ps_connstr = format!(
        "postgresql://no_user:{}@{}/no_db",
        &conf.pageserver_auth_token.unwrap_or_default(),
        ps_addr
    );

    // use Config parsing because SockAddr parsing doesnt allow to use host names instead of ip addresses
    let me_connstr = format!("postgresql://no_user@{}/no_db", conf.listen_pg_addr);
    let me_conf: Config = me_connstr.parse().unwrap();
    let (host, port) = connection_host_port(&me_conf);
    let callme = format!(
        "callmemaybe {} {} host={} port={} options='-c ztimelineid={} ztenantid={}'",
        tenantid, timelineid, host, port, timelineid, tenantid,
    );

    loop {
        info!(
            "requesting page server to connect to us: start {} {}",
            ps_connstr, callme
        );
        match Client::connect(&ps_connstr, NoTls) {
            Ok(mut client) => {
                if let Err(e) = client.simple_query(&callme) {
                    error!("Failed to send callme request to pageserver: {}", e);
                }
            }
            Err(e) => error!("Failed to connect to pageserver {}: {}", &ps_connstr, e),
        }

        if let Some(period) = conf.recall_period {
            sleep(period);
        } else {
            break;
        }
    }
}

impl<'pg> ReceiveWalConn<'pg> {
    pub fn new(pg: &'pg mut PostgresBackend) -> Result<ReceiveWalConn<'pg>> {
        let peer_addr = *pg.get_peer_addr();
        Ok(ReceiveWalConn {
            pg_backend: pg,
            peer_addr,
        })
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
        let mut buf = Vec::new();
        msg.serialize(&mut buf)?;
        self.pg_backend.write_message(&BeMessage::CopyData(&buf))?;
        Ok(())
    }

    /// Receive WAL from wal_proposer
    pub fn run(&mut self, swh: &mut SendWalHandler) -> Result<()> {
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
        // xxx: this place seems not really fitting
        if swh.conf.pageserver_addr.is_some() {
            // Need to establish replication channel with page server.
            // Add far as replication in postgres is initiated by receiver, we should use callme mechanism
            let conf = swh.conf.clone();
            let timelineid = swh.timeline.get().timelineid;
            thread::spawn(move || {
                request_callback(conf, timelineid, tenant_id);
            });
        }

        loop {
            let reply = swh.timeline.get().process_msg(&msg)?;
            self.write_msg(&reply)?;
            msg = self.read_msg()?;
        }
    }
}
