//! Server-side synchronous Postgres connection, as limited as we need.
//! To use, create PostgresBackend and run() it, passing the Handler
//! implementation determining how to process the queries. Currently its API
//! is rather narrow, but we can extend it once required.

use crate::pq_proto::{BeMessage, FeMessage, FeStartupMessage, StartupRequestCode};
use anyhow::bail;
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use log::*;
use rand::Rng;
use std::io;
use std::io::{BufReader, Write};
use std::net::TcpStream;

pub trait Handler {
    /// Handle single query.
    /// postgres_backend will issue ReadyForQuery after calling this (this
    /// might be not what we want after CopyData streaming, but currently we don't
    /// care).
    fn process_query(&mut self, pgb: &mut PostgresBackend, query_string: Bytes) -> Result<()>;

    /// Called on startup packet receival, allows to process params.
    ///
    /// If Ok(false) is returned postgres_backend will skip auth -- that is needed for new users
    /// creation is the proxy code. That is quite hacky and ad-hoc solution, may be we could allow
    /// to override whole init logic in implementations.
    fn startup(&mut self, _pgb: &mut PostgresBackend, _sm: &FeStartupMessage) -> Result<()> {
        Ok(())
    }

    /// Check auth
    fn check_auth_md5(&mut self, _pgb: &mut PostgresBackend, _md5_response: &[u8]) -> Result<()> {
        bail!("Auth failed")
    }
}

#[derive(PartialEq)]
pub enum ProtoState {
    Initialization,
    Authentication,
    Established,
}

#[derive(PartialEq)]
pub enum AuthType {
    Trust,
    MD5,
}

pub struct PostgresBackend {
    // replication.rs wants to handle reading on its own in separate thread, so
    // wrap in Option to be able to take and transfer the BufReader. Ugly, but I
    // have no better ideas.
    stream_in: Option<BufReader<TcpStream>>,
    stream_out: TcpStream,
    // Output buffer. c.f. BeMessage::write why we are using BytesMut here.
    buf_out: BytesMut,

    pub state: ProtoState,

    md5_salt: [u8; 4],
    auth_type: AuthType,
}

pub fn query_from_cstring(query_string: Bytes) -> Vec<u8> {
    let mut query_string = query_string.to_vec();
    if let Some(ch) = query_string.last() {
        if *ch == 0 {
            query_string.pop();
        }
    }
    query_string
}

impl PostgresBackend {
    pub fn new(socket: TcpStream, auth_type: AuthType) -> Result<Self, std::io::Error> {
        let mut pb = PostgresBackend {
            stream_in: None,
            stream_out: socket,
            buf_out: BytesMut::with_capacity(10 * 1024),
            state: ProtoState::Initialization,
            md5_salt: [0u8; 4],
            auth_type,
        };
        // if socket cloning fails, report the error and bail out
        pb.stream_in = match pb.stream_out.try_clone() {
            Ok(read_sock) => Some(BufReader::new(read_sock)),
            Err(error) => {
                let errmsg = format!("{}", error);
                let _res = pb.write_message_noflush(&BeMessage::ErrorResponse(errmsg));
                return Err(error);
            }
        };
        Ok(pb)
    }

    pub fn into_stream(self) -> TcpStream {
        self.stream_out
    }

    /// Get direct reference (into the Option) to the read stream.
    fn get_stream_in(&mut self) -> Result<&mut BufReader<TcpStream>> {
        match self.stream_in {
            Some(ref mut stream_in) => Ok(stream_in),
            None => bail!("stream_in was taken"),
        }
    }

    pub fn take_stream_in(&mut self) -> Option<BufReader<TcpStream>> {
        self.stream_in.take()
    }

    /// Read full message or return None if connection is closed.
    pub fn read_message(&mut self) -> Result<Option<FeMessage>> {
        match self.state {
            ProtoState::Initialization => FeStartupMessage::read(self.get_stream_in()?),
            ProtoState::Authentication | ProtoState::Established => {
                FeMessage::read(self.get_stream_in()?)
            }
        }
    }

    /// Write message into internal output buffer.
    pub fn write_message_noflush(&mut self, message: &BeMessage) -> io::Result<&mut Self> {
        BeMessage::write(&mut self.buf_out, message)?;
        Ok(self)
    }

    /// Flush output buffer into the socket.
    pub fn flush(&mut self) -> io::Result<&mut Self> {
        self.stream_out.write_all(&self.buf_out)?;
        self.buf_out.clear();
        Ok(self)
    }

    /// Write message into internal buffer and flush it.
    pub fn write_message(&mut self, message: &BeMessage) -> io::Result<&mut Self> {
        self.write_message_noflush(message)?;
        self.flush()
    }

    pub fn run(&mut self, handler: &mut impl Handler) -> Result<()> {
        let peer_addr = self.stream_out.peer_addr()?;
        info!("postgres backend to {:?} started", peer_addr);
        let mut unnamed_query_string = Bytes::new();

        loop {
            let msg = self.read_message()?;
            trace!("got message {:?}", msg);

            // Allow only startup and password messages during auth. Otherwise client would be able to bypass auth
            // TODO: change that to proper top-level match of protocol state with separate message handling for each state
            if self.state == ProtoState::Authentication || self.state == ProtoState::Initialization
            {
                match msg {
                    Some(FeMessage::PasswordMessage(ref _m)) => {}
                    Some(FeMessage::StartupMessage(ref _m)) => {}
                    Some(_) => {
                        bail!("protocol violation");
                    }
                    None => {}
                };
            }

            match msg {
                Some(FeMessage::StartupMessage(m)) => {
                    trace!("got startup message {:?}", m);

                    match m.kind {
                        StartupRequestCode::NegotiateGss | StartupRequestCode::NegotiateSsl => {
                            info!("SSL requested");
                            self.write_message(&BeMessage::Negotiate)?;
                        }
                        StartupRequestCode::Normal => {
                            // NB: startup() may change self.auth_type -- we are using that in proxy code
                            // to bypass auth for new users.
                            handler.startup(self, &m)?;

                            match self.auth_type {
                                AuthType::Trust => {
                                    self.write_message_noflush(&BeMessage::AuthenticationOk)?;
                                    // psycopg2 will not connect if client_encoding is not
                                    // specified by the server
                                    self.write_message_noflush(&BeMessage::ParameterStatus)?;
                                    self.write_message(&BeMessage::ReadyForQuery)?;
                                    self.state = ProtoState::Established;
                                }
                                AuthType::MD5 => {
                                    rand::thread_rng().fill(&mut self.md5_salt);
                                    let md5_salt = self.md5_salt;
                                    self.write_message(&BeMessage::AuthenticationMD5Password(
                                        &md5_salt,
                                    ))?;
                                    self.state = ProtoState::Authentication;
                                }
                            }
                        }
                        StartupRequestCode::Cancel => break,
                    }
                }

                Some(FeMessage::PasswordMessage(m)) => {
                    trace!("got password message '{:?}'", m);

                    assert!(self.state == ProtoState::Authentication);

                    let (_, md5_response) = m
                        .split_last()
                        .ok_or_else(|| anyhow::Error::msg("protocol violation"))?;

                    if let Err(e) = handler.check_auth_md5(self, md5_response) {
                        self.write_message(&BeMessage::ErrorResponse(format!("{}", e)))?;
                        bail!("auth failed: {}", e);
                    } else {
                        self.write_message_noflush(&BeMessage::AuthenticationOk)?;
                        // psycopg2 will not connect if client_encoding is not
                        // specified by the server
                        self.write_message_noflush(&BeMessage::ParameterStatus)?;
                        self.write_message(&BeMessage::ReadyForQuery)?;
                        self.state = ProtoState::Established;
                    }
                }

                Some(FeMessage::Query(m)) => {
                    trace!("got query {:?}", m.body);
                    // xxx distinguish fatal and recoverable errors?
                    if let Err(e) = handler.process_query(self, m.body) {
                        let errmsg = format!("{}", e);
                        self.write_message_noflush(&BeMessage::ErrorResponse(errmsg))?;
                    }
                    self.write_message(&BeMessage::ReadyForQuery)?;
                }
                Some(FeMessage::Parse(m)) => {
                    unnamed_query_string = m.query_string;
                    self.write_message(&BeMessage::ParseComplete)?;
                }
                Some(FeMessage::Describe(_)) => {
                    self.write_message_noflush(&BeMessage::ParameterDescription)?
                        .write_message(&BeMessage::NoData)?;
                }
                Some(FeMessage::Bind(_)) => {
                    self.write_message(&BeMessage::BindComplete)?;
                }
                Some(FeMessage::Close(_)) => {
                    self.write_message(&BeMessage::CloseComplete)?;
                }
                Some(FeMessage::Execute(_)) => {
                    handler.process_query(self, unnamed_query_string.clone())?;
                }
                Some(FeMessage::Sync) => {
                    self.write_message(&BeMessage::ReadyForQuery)?;
                }
                Some(FeMessage::Terminate) => {
                    break;
                }
                None => {
                    info!("connection closed");
                    break;
                }
                x => {
                    bail!("unexpected message type : {:?}", x);
                }
            }
        }
        info!("postgres backend to {:?} exited", peer_addr);
        Ok(())
    }
}
