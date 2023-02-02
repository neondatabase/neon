//! Server-side asynchronous Postgres connection, as limited as we need.
//! To use, create PostgresBackend and run() it, passing the Handler
//! implementation determining how to process the queries. Currently its API
//! is rather narrow, but we can extend it once required.
use anyhow::Context;
use bytes::{Buf, Bytes, BytesMut};
use futures::stream::StreamExt;
use futures::{pin_mut, Sink, SinkExt};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::{fmt, io};
use std::{future::Future, str::FromStr};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;
use tracing::{debug, error, info, trace};

use pq_proto::codec::{ConnectionError, PostgresCodec};
use pq_proto::{BeMessage, FeMessage, FeStartupPacket, SQLSTATE_INTERNAL_ERROR};

/// An error, occurred during query processing:
/// either during the connection ([`ConnectionError`]) or before/after it.
#[derive(thiserror::Error, Debug)]
pub enum QueryError {
    /// The connection was lost while processing the query.
    #[error(transparent)]
    Disconnected(#[from] ConnectionError),
    /// Some other error
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<io::Error> for QueryError {
    fn from(e: io::Error) -> Self {
        Self::Disconnected(ConnectionError::Io(e))
    }
}

impl QueryError {
    pub fn pg_error_code(&self) -> &'static [u8; 5] {
        match self {
            Self::Disconnected(_) => b"08006",         // connection failure
            Self::Other(_) => SQLSTATE_INTERNAL_ERROR, // internal error
        }
    }
}

pub fn is_expected_io_error(e: &io::Error) -> bool {
    use io::ErrorKind::*;
    matches!(
        e.kind(),
        ConnectionRefused | ConnectionAborted | ConnectionReset
    )
}

#[async_trait::async_trait]
pub trait Handler {
    /// Handle single query.
    /// postgres_backend will issue ReadyForQuery after calling this (this
    /// might be not what we want after CopyData streaming, but currently we don't
    /// care).
    async fn process_query(
        &mut self,
        pgb: &mut PostgresBackend,
        query_string: &str,
    ) -> Result<(), QueryError>;

    /// Called on startup packet receival, allows to process params.
    ///
    /// If Ok(false) is returned postgres_backend will skip auth -- that is needed for new users
    /// creation is the proxy code. That is quite hacky and ad-hoc solution, may be we could allow
    /// to override whole init logic in implementations.
    fn startup(
        &mut self,
        _pgb: &mut PostgresBackend,
        _sm: &FeStartupPacket,
    ) -> Result<(), QueryError> {
        Ok(())
    }

    /// Check auth jwt
    fn check_auth_jwt(
        &mut self,
        _pgb: &mut PostgresBackend,
        _jwt_response: &[u8],
    ) -> Result<(), QueryError> {
        Err(QueryError::Other(anyhow::anyhow!("JWT auth failed")))
    }
}

/// PostgresBackend protocol state.
/// XXX: The order of the constructors matters.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum ProtoState {
    Initialization,
    // Encryption handshake is done; waiting for encrypted Startup message.
    Encrypted,
    Authentication,
    Established,
    Closed,
}

#[derive(Clone, Copy)]
pub enum ProcessMsgResult {
    Continue,
    Break,
}

/// Either plain TCP stream or encrypted one, implementing AsyncRead + AsyncWrite.
pub enum MaybeTlsStream {
    Unencrypted(tokio::net::TcpStream),
    Tls(Box<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>),
    Broken, // temporary value for switch to TLS
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
            _ => unreachable!(),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
            _ => unreachable!(),
        }
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
            _ => unreachable!(),
        }
    }
}
impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum AuthType {
    Trust,
    // This mimics postgres's AuthenticationCleartextPassword but instead of password expects JWT
    NeonJWT,
}

impl FromStr for AuthType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Trust" => Ok(Self::Trust),
            "NeonJWT" => Ok(Self::NeonJWT),
            _ => anyhow::bail!("invalid value \"{s}\" for auth type"),
        }
    }
}

impl fmt::Display for AuthType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            AuthType::Trust => "Trust",
            AuthType::NeonJWT => "NeonJWT",
        })
    }
}

pub struct PostgresBackend {
    // Provides serialization/deserialization to the underlying transport backed
    // with buffers; implements Sink consuming messages and Stream reading them.
    //
    // Sink::start_send only queues message to the interal buffer.
    // SinkExt::flush flushes buffer to the stream.
    //
    // StreamExt::read reads next message. In case of EOF without partial
    // message it returns None.
    stream: Framed<MaybeTlsStream, PostgresCodec>,

    pub state: ProtoState,

    auth_type: AuthType,

    peer_addr: SocketAddr,
    pub tls_config: Option<Arc<rustls::ServerConfig>>,
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

// Cast a byte slice to a string slice, dropping null terminator if there's one.
fn cstr_to_str(bytes: &[u8]) -> anyhow::Result<&str> {
    let without_null = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    std::str::from_utf8(without_null).map_err(|e| e.into())
}

impl PostgresBackend {
    pub fn new(
        socket: tokio::net::TcpStream,
        auth_type: AuthType,
        tls_config: Option<Arc<rustls::ServerConfig>>,
    ) -> io::Result<Self> {
        let peer_addr = socket.peer_addr()?;
        let stream = MaybeTlsStream::Unencrypted(socket);

        Ok(Self {
            stream: Framed::new(stream, PostgresCodec::new()),
            state: ProtoState::Initialization,
            auth_type,
            tls_config,
            peer_addr,
        })
    }

    pub fn get_peer_addr(&self) -> &SocketAddr {
        &self.peer_addr
    }

    /// Read full message or return None if connection is closed.
    pub async fn read_message(&mut self) -> Result<Option<FeMessage>, ConnectionError> {
        if let ProtoState::Closed = self.state {
            Ok(None)
        } else {
            let msg = self.stream.next().await;
            // Option<Result<...>>, so swap.
            msg.map_or(Ok(None), |res| res.map(Some))
        }
    }

    /// Polling version of read_message, saves the caller need to pin.
    pub fn poll_read_message(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<Option<FeMessage>, ConnectionError>> {
        let read_fut = self.read_message();
        pin_mut!(read_fut);
        read_fut.poll(cx)
    }

    /// Flush output buffer into the socket.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.stream.flush().await.map_err(|e| match e {
            ConnectionError::Io(e) => e,
            // the only error we can get from flushing is IO
            _ => unreachable!(),
        })
    }

    /// Polling version of `flush()`, saves the caller need to pin.
    pub fn poll_flush(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let flush_fut = self.flush();
        pin_mut!(flush_fut);
        flush_fut.poll(cx)
    }

    /// Write message into internal output buffer. Technically error type can be
    /// only ProtocolError here (if, unlikely, serialization fails), but callers
    /// typically wrap it anyway.
    pub fn write_message(&mut self, message: &BeMessage<'_>) -> Result<&mut Self, ConnectionError> {
        Pin::new(&mut self.stream).start_send(message)?;
        Ok(self)
    }

    /// Write message into internal output buffer and flush it to the stream.
    pub async fn write_message_flush(
        &mut self,
        message: &BeMessage<'_>,
    ) -> Result<&mut Self, ConnectionError> {
        self.write_message(message)?;
        self.flush().await?;
        Ok(self)
    }

    /// Returns an AsyncWrite implementation that wraps all the data written
    /// to it in CopyData messages, and writes them to the connection
    ///
    /// The caller is responsible for sending CopyOutResponse and CopyDone messages.
    pub fn copyout_writer(&mut self) -> CopyDataWriter {
        CopyDataWriter { pgb: self }
    }

    // Wrapper for run_message_loop() that shuts down socket when we are done
    pub async fn run<F, S>(
        mut self,
        handler: &mut impl Handler,
        shutdown_watcher: F,
    ) -> Result<(), QueryError>
    where
        F: Fn() -> S,
        S: Future,
    {
        let ret = self.run_message_loop(handler, shutdown_watcher).await;
        let _ = self.stream.get_mut().shutdown();
        ret
    }

    async fn run_message_loop<F, S>(
        &mut self,
        handler: &mut impl Handler,
        shutdown_watcher: F,
    ) -> Result<(), QueryError>
    where
        F: Fn() -> S,
        S: Future,
    {
        trace!("postgres backend to {:?} started", self.peer_addr);

        tokio::select!(
            biased;

            _ = shutdown_watcher() => {
                // We were requested to shut down.
                tracing::info!("shutdown request received during handshake");
                return Ok(())
            },

            result = async {
                while self.state < ProtoState::Established {
                    if let Some(msg) = self.read_message().await? {
                        trace!("got message {msg:?} during handshake");

                        match self.process_handshake_message(handler, msg).await? {
                            ProcessMsgResult::Continue => {
                                self.flush().await?;
                                continue;
                            }
                            ProcessMsgResult::Break => {
                                trace!("postgres backend to {:?} exited during handshake", self.peer_addr);
                                return Ok(());
                            }
                        }
                    } else {
                        trace!("postgres backend to {:?} exited during handshake", self.peer_addr);
                        return Ok(());
                    }
                }
                Ok::<(), QueryError>(())
            } => {
                // Handshake complete.
                result?;
            }
        );

        // Authentication completed
        let mut query_string = Bytes::new();
        while let Some(msg) = tokio::select!(
            biased;
            _ = shutdown_watcher() => {
                // We were requested to shut down.
                tracing::info!("shutdown request received in run_message_loop");
                Ok(None)
            },
            msg = self.read_message() => { msg },
        )? {
            trace!("got message {:?}", msg);

            let result = self.process_message(handler, msg, &mut query_string).await;
            self.flush().await?;
            match result? {
                ProcessMsgResult::Continue => {
                    self.flush().await?;
                    continue;
                }
                ProcessMsgResult::Break => break,
            }
        }

        trace!("postgres backend to {:?} exited", self.peer_addr);
        Ok(())
    }

    async fn start_tls(&mut self) -> anyhow::Result<()> {
        if let MaybeTlsStream::Unencrypted(plain_stream) =
            // temporary replace stream with fake broken to prepare TLS one
            std::mem::replace(self.stream.get_mut(), MaybeTlsStream::Broken)
        {
            let acceptor = TlsAcceptor::from(self.tls_config.clone().unwrap());
            match acceptor.accept(plain_stream).await {
                Ok(tls_stream) => {
                    // push back ready TLS stream
                    *self.stream.get_mut() = MaybeTlsStream::Tls(Box::new(tls_stream));
                    return Ok(());
                }
                Err(e) => {
                    self.state = ProtoState::Closed;
                    return Err(e.into());
                }
            }
        };
        anyhow::bail!("TLS already started");
    }

    async fn process_handshake_message(
        &mut self,
        handler: &mut impl Handler,
        msg: FeMessage,
    ) -> Result<ProcessMsgResult, QueryError> {
        assert!(self.state < ProtoState::Established);
        let have_tls = self.tls_config.is_some();
        match msg {
            FeMessage::StartupPacket(m) => {
                match m {
                    FeStartupPacket::SslRequest => {
                        debug!("SSL requested");

                        self.write_message(&BeMessage::EncryptionResponse(have_tls))?;

                        if have_tls {
                            self.start_tls().await?;
                            self.state = ProtoState::Encrypted;
                        }
                    }
                    FeStartupPacket::GssEncRequest => {
                        debug!("GSS requested");
                        self.write_message(&BeMessage::EncryptionResponse(false))?;
                    }
                    FeStartupPacket::StartupMessage { .. } => {
                        if have_tls && !matches!(self.state, ProtoState::Encrypted) {
                            self.write_message(&BeMessage::ErrorResponse(
                                "must connect with TLS",
                                None,
                            ))?;
                            return Err(QueryError::Other(anyhow::anyhow!(
                                "client did not connect with TLS"
                            )));
                        }

                        // NB: startup() may change self.auth_type -- we are using that in proxy code
                        // to bypass auth for new users.
                        handler.startup(self, &m)?;

                        match self.auth_type {
                            AuthType::Trust => {
                                self.write_message(&BeMessage::AuthenticationOk)?
                                    .write_message(&BeMessage::CLIENT_ENCODING)?
                                    .write_message(&BeMessage::INTEGER_DATETIMES)?
                                    // The async python driver requires a valid server_version
                                    .write_message(&BeMessage::server_version("14.1"))?
                                    .write_message(&BeMessage::ReadyForQuery)?;
                                self.state = ProtoState::Established;
                            }
                            AuthType::NeonJWT => {
                                self.write_message(&BeMessage::AuthenticationCleartextPassword)?;
                                self.state = ProtoState::Authentication;
                            }
                        }
                    }
                    FeStartupPacket::CancelRequest { .. } => {
                        self.state = ProtoState::Closed;
                        return Ok(ProcessMsgResult::Break);
                    }
                }
            }

            FeMessage::PasswordMessage(m) => {
                trace!("got password message '{:?}'", m);

                assert!(self.state == ProtoState::Authentication);

                match self.auth_type {
                    AuthType::Trust => unreachable!(),
                    AuthType::NeonJWT => {
                        let (_, jwt_response) = m.split_last().context("protocol violation")?;

                        if let Err(e) = handler.check_auth_jwt(self, jwt_response) {
                            self.write_message(&BeMessage::ErrorResponse(
                                &e.to_string(),
                                Some(e.pg_error_code()),
                            ))?;
                            return Err(e);
                        }
                    }
                }
                self.write_message(&BeMessage::AuthenticationOk)?
                    .write_message(&BeMessage::CLIENT_ENCODING)?
                    .write_message(&BeMessage::INTEGER_DATETIMES)?
                    .write_message(&BeMessage::ReadyForQuery)?;
                self.state = ProtoState::Established;
            }

            _ => {
                self.state = ProtoState::Closed;
                return Ok(ProcessMsgResult::Break);
            }
        }
        Ok(ProcessMsgResult::Continue)
    }

    async fn process_message(
        &mut self,
        handler: &mut impl Handler,
        msg: FeMessage,
        unnamed_query_string: &mut Bytes,
    ) -> Result<ProcessMsgResult, QueryError> {
        // Allow only startup and password messages during auth. Otherwise client would be able to bypass auth
        // TODO: change that to proper top-level match of protocol state with separate message handling for each state
        assert!(self.state == ProtoState::Established);

        match msg {
            FeMessage::StartupPacket(_) | FeMessage::PasswordMessage(_) => {
                return Err(QueryError::Other(anyhow::anyhow!("protocol violation")));
            }

            FeMessage::Query(body) => {
                // remove null terminator
                let query_string = cstr_to_str(&body)?;

                trace!("got query {query_string:?}");
                if let Err(e) = handler.process_query(self, query_string).await {
                    log_query_error(query_string, &e);
                    let short_error = short_error(&e);
                    self.write_message(&BeMessage::ErrorResponse(
                        &short_error,
                        Some(e.pg_error_code()),
                    ))?;
                }
                self.write_message(&BeMessage::ReadyForQuery)?;
            }

            FeMessage::Parse(m) => {
                *unnamed_query_string = m.query_string;
                self.write_message(&BeMessage::ParseComplete)?;
            }

            FeMessage::Describe(_) => {
                self.write_message(&BeMessage::ParameterDescription)?
                    .write_message(&BeMessage::NoData)?;
            }

            FeMessage::Bind(_) => {
                self.write_message(&BeMessage::BindComplete)?;
            }

            FeMessage::Close(_) => {
                self.write_message(&BeMessage::CloseComplete)?;
            }

            FeMessage::Execute(_) => {
                let query_string = cstr_to_str(unnamed_query_string)?;
                trace!("got execute {query_string:?}");
                if let Err(e) = handler.process_query(self, query_string).await {
                    log_query_error(query_string, &e);
                    self.write_message(&BeMessage::ErrorResponse(
                        &e.to_string(),
                        Some(e.pg_error_code()),
                    ))?;
                }
                // NOTE there is no ReadyForQuery message. This handler is used
                // for basebackup and it uses CopyOut which doesn't require
                // ReadyForQuery message and backend just switches back to
                // processing mode after sending CopyDone or ErrorResponse.
            }

            FeMessage::Sync => {
                self.write_message(&BeMessage::ReadyForQuery)?;
            }

            FeMessage::Terminate => {
                return Ok(ProcessMsgResult::Break);
            }

            // We prefer explicit pattern matching to wildcards, because
            // this helps us spot the places where new variants are missing
            FeMessage::CopyData(_) | FeMessage::CopyDone | FeMessage::CopyFail => {
                return Err(QueryError::Other(anyhow::anyhow!(
                    "unexpected message type: {:?}",
                    msg
                )));
            }
        }

        Ok(ProcessMsgResult::Continue)
    }
}

///
/// A futures::AsyncWrite implementation that wraps all data written to it in CopyData
/// messages.
///

pub struct CopyDataWriter<'a> {
    pgb: &'a mut PostgresBackend,
}

impl<'a> AsyncWrite for CopyDataWriter<'a> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = self.get_mut();

        // It's not strictly required to flush between each message, but makes it easier
        // to view in wireshark, and usually the messages that the callers write are
        // decently-sized anyway.
        match this.pgb.poll_flush(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        }

        // CopyData
        // XXX: if the input is large, we should split it into multiple messages.
        // Not sure what the threshold should be, but the ultimate hard limit is that
        // the length cannot exceed u32.
        this.pgb
            .write_message(&BeMessage::CopyData(buf))
            // write_message only writes to buffer, so can fail iff message is
            // invaid, but CopyData can't be invalid.
            .expect("failed to serialize CopyData");

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let this = self.get_mut();
        this.pgb.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let this = self.get_mut();
        this.pgb.poll_flush(cx)
    }
}

pub fn short_error(e: &QueryError) -> String {
    match e {
        QueryError::Disconnected(connection_error) => connection_error.to_string(),
        QueryError::Other(e) => format!("{e:#}"),
    }
}

pub(super) fn log_query_error(query: &str, e: &QueryError) {
    match e {
        QueryError::Disconnected(ConnectionError::Io(io_error)) => {
            if is_expected_io_error(io_error) {
                info!("query handler for '{query}' failed with expected io error: {io_error}");
            } else {
                error!("query handler for '{query}' failed with io error: {io_error}");
            }
        }
        QueryError::Disconnected(other_connection_error) => {
            error!("query handler for '{query}' failed with connection error: {other_connection_error:?}")
        }
        QueryError::Other(e) => {
            error!("query handler for '{query}' failed: {e:?}");
        }
    }
}
