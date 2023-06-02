//! Safekeeper communication endpoint to WAL proposer (compute node).
//! Gets messages from the network, passes them down to consensus module and
//! sends replies back.

use std::collections::HashMap;

use bytes::BytesMut;
use log::info;
use safekeeper::{simlib::{node_os::NodeOs, network::TCP, world::NodeEvent, proto::AnyMessage}, safekeeper::{ProposerAcceptorMessage, ServerInfo, SafeKeeperState, UNKNOWN_SERVER_VERSION, SafeKeeper}, timeline::{TimelineError}, SafeKeeperConf};
use utils::{id::TenantTimelineId, lsn::Lsn};
use anyhow::{Result, bail};

use crate::simtest::storage::{InMemoryState, DummyWalStore};

struct ConnState {
    tcp: TCP,
    conf: SafeKeeperConf,

    greeting: bool,
    ttid: TenantTimelineId,
    tli: Option<SharedState>,
    flush_pending: bool,
}

struct SharedState {
    sk: SafeKeeper<InMemoryState, DummyWalStore>,
}

pub fn run_server(os: NodeOs) -> Result<()> {
    println!("started server {}", os.id());

    let mut conns: HashMap<i64, ConnState> = HashMap::new();

    let epoll = os.epoll();
    loop {
        // waiting for the next message
        let mut next_event = Some(epoll.recv());

        loop {
            let event = match next_event {
                Some(event) => event,
                None => break,
            };

            match event {
                NodeEvent::Accept(tcp) => {
                    conns.insert(tcp.id(), ConnState {
                        tcp,
                        conf: SafeKeeperConf::dummy(),
                        greeting: false,
                        ttid: TenantTimelineId::empty(),
                        tli: None,
                        flush_pending: false,
                    });
                }
                NodeEvent::Message((msg, tcp)) => {
                    let conn = conns.get_mut(&tcp.id());
                    if let Some(conn) = conn {
                        let res = conn.process_any(msg);
                        if res.is_err() {
                            println!("conn {:?} error: {:?}", tcp, res);
                            conns.remove(&tcp.id());
                        }
                    } else {
                        println!("conn {:?} was closed, dropping msg {:?}", tcp, msg);
                    }
                }
                NodeEvent::Closed(_) => {}
            }

            // TODO: make simulator support multiple events per tick
            next_event = epoll.try_recv();
        }

        conns.retain(|_, conn| {
            let res = conn.flush();
            if res.is_err() {
                println!("conn {:?} error: {:?}", conn.tcp, res);
            }
            res.is_ok()
        });
    }
}

impl ConnState {
    fn process_any(&mut self, any: AnyMessage) -> Result<()> {
        if let AnyMessage::Bytes(copy_data) = any {
            let msg = ProposerAcceptorMessage::parse(copy_data)?;
            return self.process(msg);
        } else {
            bail!("unexpected message, expected AnyMessage::Bytes");
        }
    }

    fn create_timeline(&mut self, ttid: TenantTimelineId, server_info: ServerInfo) -> Result<()> {
        info!("creating new timeline {}", ttid);
        self.ttid = ttid;

        let commit_lsn = Lsn::INVALID;
        let local_start_lsn = Lsn::INVALID;

        // TODO: load state from in-memory storage
        let state = SafeKeeperState::new(&ttid, server_info, vec![], commit_lsn, local_start_lsn);
        
        if state.server.wal_seg_size == 0 {
            bail!(TimelineError::UninitializedWalSegSize(ttid));
        }

        if state.server.pg_version == UNKNOWN_SERVER_VERSION {
            bail!(TimelineError::UninitialinzedPgVersion(ttid));
        }

        if state.commit_lsn < state.local_start_lsn {
            bail!(
                "commit_lsn {} is higher than local_start_lsn {}",
                state.commit_lsn,
                state.local_start_lsn
            );
        }

        // TODO: implement "persistent" storage for tests
        let control_store = InMemoryState::new(state.clone());

        // TODO: implement "persistent" storage for tests
        let wal_store = DummyWalStore::new();
        
        let sk = SafeKeeper::new(control_store, wal_store, self.conf.my_id)?;

        self.tli = Some(SharedState {
            sk,
        });

        Ok(())
    }

    fn process(&mut self, msg: ProposerAcceptorMessage) -> Result<()> {
        if !self.greeting {
            self.greeting = true;
            
            match msg {
                ProposerAcceptorMessage::Greeting(ref greeting) => {
                    info!(
                        "start handshake with walproposer {:?} sysid {} timeline {}",
                        self.tcp, greeting.system_id, greeting.tli,
                    );
                    let server_info = ServerInfo {
                        pg_version: greeting.pg_version,
                        system_id: greeting.system_id,
                        wal_seg_size: greeting.wal_seg_size,
                    };
                    let ttid = TenantTimelineId::new(greeting.tenant_id, greeting.timeline_id);
                    self.create_timeline(ttid, server_info)?
                }
                _ => {
                    bail!(
                        "unexpected message {msg:?} instead of greeting"
                    );
                }
            }

            return Ok(());
        }

        match msg {
            ProposerAcceptorMessage::AppendRequest(append_request) => {
                self.flush_pending = true;
                self.process_sk_msg(&ProposerAcceptorMessage::NoFlushAppendRequest(append_request))?;
            }
            other => {
                self.process_sk_msg(&other)?;
            }
        }

        Ok(())
    }

    /// Process FlushWAL if needed.
    // TODO: add extra flushes, to verify that extra flushes don't break anything
    fn flush(&mut self) -> Result<()> {
        if !self.flush_pending {
            return Ok(());
        }
        self.flush_pending = false;
        self.process_sk_msg(&ProposerAcceptorMessage::FlushWAL)
    }

    /// Make safekeeper process a message and send a reply to the TCP
    fn process_sk_msg(&mut self, msg: &ProposerAcceptorMessage) -> Result<()> {
        let shared_state = self.tli.as_mut().unwrap();
        let reply = shared_state.sk.process_msg(msg)?;
        if let Some(reply) = reply {
            let mut buf = BytesMut::with_capacity(128);
            reply.serialize(&mut buf)?;
            self.tcp.send(AnyMessage::Bytes(buf.into()));
        }
        Ok(())
    }
}

impl Drop for ConnState {
    fn drop(&mut self) {
        println!("dropping conn: {:?}", self.tcp);
        self.tcp.close();
        // TODO: clean up non-fsynced WAL
    }
}
