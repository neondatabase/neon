//! Server-side synchronous Postgres connection, as limited as we need.
//! To use, create PostgresBackend and run() it, passing the Handler
//! implementation determining how to process the queries. Currently its API
//! is rather narrow, but we can extend it once required.

use crate::pq_proto::{BeMessage, FeMessage, FeStartupMessage, StartupRequestCode};
use anyhow::bail;
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use log::*;
use std::io;
use std::io::{BufReader, Write};
use std::net::{Shutdown, TcpStream};

pub trait Handler {
    /// Handle single query.
    /// postgres_backend will issue ReadyForQuery after calling this (this
    /// might be not what we want after CopyData streaming, but currently we don't
    /// care).
    fn process_query(&mut self, pgb: &mut PostgresBackend, query_string: Bytes) -> Result<()>;
    /// Called on startup packet receival, allows to process params.
    fn startup(&mut self, _pgb: &mut PostgresBackend, _sm: &FeStartupMessage) -> Result<()> {
        Ok(())
    }
}

pub struct PostgresBackend {
    // replication.rs wants to handle reading on its own in separate thread, so
    // wrap in Option to be able to take and transfer the BufReader. Ugly, but I
    // have no better ideas.
    stream_in: Option<BufReader<TcpStream>>,
    stream_out: TcpStream,
    // Output buffer. c.f. BeMessage::write why we are using BytesMut here.
    buf_out: BytesMut,
    init_done: bool,
}

// In replication.rs a separate thread is reading keepalives from the
// socket. When main one finishes, tell it to get down by shutdowning the
// socket.
impl Drop for PostgresBackend {
    fn drop(&mut self) {
        let _res = self.stream_out.shutdown(Shutdown::Both);
    }
}

impl PostgresBackend {
    pub fn new(socket: TcpStream) -> Result<Self, std::io::Error> {
        let mut pb = PostgresBackend {
            stream_in: None,
            stream_out: socket,
            buf_out: BytesMut::with_capacity(10 * 1024),
            init_done: false,
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
        if !self.init_done {
            FeStartupMessage::read(self.get_stream_in()?)
        } else {
            FeMessage::read(self.get_stream_in()?)
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
            match msg {
                Some(FeMessage::StartupMessage(m)) => {
                    trace!("got startup message {:?}", m);

                    handler.startup(self, &m)?;

                    match m.kind {
                        StartupRequestCode::NegotiateGss | StartupRequestCode::NegotiateSsl => {
                            info!("SSL requested");
                            self.write_message(&BeMessage::Negotiate)?;
                        }
                        StartupRequestCode::Normal => {
                            self.write_message_noflush(&BeMessage::AuthenticationOk)?;
                            // psycopg2 will not connect if client_encoding is not
                            // specified by the server
                            self.write_message_noflush(&BeMessage::ParameterStatus)?;
                            self.write_message(&BeMessage::ReadyForQuery)?;
                            self.init_done = true;
                        }
                        StartupRequestCode::Cancel => break,
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
