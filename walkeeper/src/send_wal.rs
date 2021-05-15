//! This implements the libpq replication protocol between wal_acceptor
//! and replicas/pagers
//!

use crate::pq_protocol::{
    BeMessage, FeMessage, FeStartupMessage, RowDescriptor, StartupRequestCode,
};
use crate::replication::ReplicationConn;
use crate::timeline::{Timeline, TimelineTools};
use crate::WalAcceptorConf;
use anyhow::{bail, Result};
use bytes::BytesMut;
use log::*;
use std::io::{BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

/// A network connection that's speaking the libpq replication protocol.
pub struct SendWalConn {
    pub timeline: Option<Arc<Timeline>>,
    /// Postgres connection, buffered input
    pub stream_in: BufReader<TcpStream>,
    /// Postgres connection, output
    pub stream_out: TcpStream,
    /// The cached result of socket.peer_addr()
    pub peer_addr: SocketAddr,
    /// wal acceptor configuration
    pub conf: WalAcceptorConf,
    /// assigned application name
    appname: Option<String>,
}

impl SendWalConn {
    /// Create a new `SendWal`, consuming the `Connection`.
    pub fn new(socket: TcpStream, conf: WalAcceptorConf) -> Result<Self> {
        let peer_addr = socket.peer_addr()?;
        let conn = SendWalConn {
            timeline: None,
            stream_in: BufReader::new(socket.try_clone()?),
            stream_out: socket,
            peer_addr,
            conf,
            appname: None,
        };
        Ok(conn)
    }

    ///
    /// Send WAL to replica or WAL receiver using standard libpq replication protocol
    ///
    pub fn run(mut self) -> Result<()> {
        let peer_addr = self.peer_addr.clone();
        info!("WAL sender to {:?} is started", peer_addr);

        // Handle the startup message first.

        let m = FeStartupMessage::read_from(&mut self.stream_in)?;
        trace!("got startup message {:?}", m);
        match m.kind {
            StartupRequestCode::NegotiateGss | StartupRequestCode::NegotiateSsl => {
                let mut buf = BytesMut::new();
                BeMessage::write(&mut buf, &BeMessage::Negotiate);
                info!("SSL requested");
                self.stream_out.write_all(&buf)?;
            }
            StartupRequestCode::Normal => {
                let mut buf = BytesMut::new();
                BeMessage::write(&mut buf, &BeMessage::AuthenticationOk);
                BeMessage::write(&mut buf, &BeMessage::ReadyForQuery);
                self.stream_out.write_all(&buf)?;
                self.timeline.set(m.timelineid)?;
                self.appname = m.appname;
            }
            StartupRequestCode::Cancel => return Ok(()),
        }

        loop {
            let msg = FeMessage::read_from(&mut self.stream_in)?;
            match msg {
                FeMessage::Query(q) => {
                    trace!("got query {:?}", q.body);

                    if q.body.starts_with(b"IDENTIFY_SYSTEM") {
                        self.handle_identify_system()?;
                    } else if q.body.starts_with(b"START_REPLICATION") {
                        // Create a new replication object, consuming `self`.
                        ReplicationConn::new(self).run(&q.body)?;
                        break;
                    } else {
                        bail!("Unexpected command {:?}", q.body);
                    }
                }
                FeMessage::Terminate => {
                    break;
                }
                _ => {
                    bail!("unexpected message");
                }
            }
        }
        info!("WAL sender to {:?} is finished", peer_addr);
        Ok(())
    }

    ///
    /// Handle IDENTIFY_SYSTEM replication command
    ///
    fn handle_identify_system(&mut self) -> Result<()> {
        let (start_pos, timeline) = self.timeline.find_end_of_wal(&self.conf.data_dir, false);
        let lsn = start_pos.to_string();
        let tli = timeline.to_string();
        let sysid = self.timeline.get().get_info().server.system_id.to_string();
        let lsn_bytes = lsn.as_bytes();
        let tli_bytes = tli.as_bytes();
        let sysid_bytes = sysid.as_bytes();

        let mut outbuf = BytesMut::new();
        BeMessage::write(
            &mut outbuf,
            &BeMessage::RowDescription(&[
                RowDescriptor {
                    name: b"systemid\0",
                    typoid: 25,
                    typlen: -1,
                },
                RowDescriptor {
                    name: b"timeline\0",
                    typoid: 23,
                    typlen: 4,
                },
                RowDescriptor {
                    name: b"xlogpos\0",
                    typoid: 25,
                    typlen: -1,
                },
                RowDescriptor {
                    name: b"dbname\0",
                    typoid: 25,
                    typlen: -1,
                },
            ]),
        );
        BeMessage::write(
            &mut outbuf,
            &BeMessage::DataRow(&[Some(sysid_bytes), Some(tli_bytes), Some(lsn_bytes), None]),
        );
        BeMessage::write(
            &mut outbuf,
            &BeMessage::CommandComplete(b"IDENTIFY_SYSTEM\0"),
        );
        BeMessage::write(&mut outbuf, &BeMessage::ReadyForQuery);
        self.stream_out.write_all(&outbuf)?;
        Ok(())
    }
}
