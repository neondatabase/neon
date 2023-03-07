//! Server-side asynchronous Postgres connection, as limited as we need.
//! To use, create PostgresBackend and run() it, passing the Handler
//! implementation determining how to process the queries. Currently its API
//! is rather narrow, but we can extend it once required.

use anyhow::Context;
use bytes::{Buf, Bytes, BytesMut};
use pq_proto::{BeMessage, ConnectionError, FeMessage, FeStartupPacket, SQLSTATE_INTERNAL_ERROR};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::{future::Future, task::ready};
use tracing::{debug, error, info, trace};
use utils::postgres_backend::AuthType;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio_rustls::TlsAcceptor;

pub fn is_expected_io_error(e: &io::Error) -> bool {
    use io::ErrorKind::*;
    matches!(
        e.kind(),
        ConnectionRefused | ConnectionAborted | ConnectionReset
    )
}

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
        Self::Disconnected(ConnectionError::Socket(e))
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

/// Always-writeable sock_split stream.
/// May not be readable. See [`PostgresBackend::take_stream_in`]
pub enum Stream {
    Unencrypted(BufReader<tokio::net::TcpStream>),
    Tls(Box<tokio_rustls::server::TlsStream<BufReader<tokio::net::TcpStream>>>),
    Broken,
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Broken => unreachable!(),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
            Self::Broken => unreachable!(),
        }
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Broken => unreachable!(),
        }
    }
}
impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Unencrypted(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Broken => unreachable!(),
        }
    }
}

pub struct PostgresBackend {
    stream: Stream,

    // Output buffer. c.f. BeMessage::write why we are using BytesMut here.
    // The data between 0 and "current position" as tracked by the bytes::Buf
    // implementation of BytesMut, have already been written.
    buf_out: BytesMut,

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

        Ok(Self {
            stream: Stream::Unencrypted(BufReader::new(socket)),
            buf_out: BytesMut::with_capacity(10 * 1024),
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
    pub async fn read_message(&mut self) -> Result<Option<FeMessage>, QueryError> {
        use ProtoState::*;
        match self.state {
            Initialization | Encrypted => FeStartupPacket::read_fut(&mut self.stream).await,
            Authentication | Established => FeMessage::read_fut(&mut self.stream).await,
            Closed => Ok(None),
        }
        .map_err(QueryError::from)
    }

    /// Flush output buffer into the socket.
    pub async fn flush(&mut self) -> io::Result<()> {
        while self.buf_out.has_remaining() {
            let bytes_written = self.stream.write(self.buf_out.chunk()).await?;
            self.buf_out.advance(bytes_written);
        }
        self.buf_out.clear();
        Ok(())
    }

    /// Write message into internal output buffer.
    pub fn write_message_noflush(&mut self, message: &BeMessage<'_>) -> io::Result<&mut Self> {
        BeMessage::write(&mut self.buf_out, message)?;
        Ok(self)
    }

    /// Returns an AsyncWrite implementation that wraps all the data written
    /// to it in CopyData messages, and writes them to the connection
    ///
    /// The caller is responsible for sending CopyOutResponse and CopyDone messages.
    pub fn copyout_writer(&mut self) -> CopyDataWriter {
        CopyDataWriter { pgb: self }
    }

    /// A polling function that tries to write all the data from 'buf_out' to the
    /// underlying stream.
    fn poll_write_buf(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        while self.buf_out.has_remaining() {
            match ready!(Pin::new(&mut self.stream).poll_write(cx, self.buf_out.chunk())) {
                Ok(bytes_written) => self.buf_out.advance(bytes_written),
                Err(err) => return Poll::Ready(Err(err)),
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_flush(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.stream).poll_flush(cx)
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
        let _ = self.stream.shutdown();
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
        if let Stream::Unencrypted(plain_stream) =
            std::mem::replace(&mut self.stream, Stream::Broken)
        {
            let acceptor = TlsAcceptor::from(self.tls_config.clone().unwrap());
            let tls_stream = acceptor.accept(plain_stream).await?;

            self.stream = Stream::Tls(Box::new(tls_stream));
            return Ok(());
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
                trace!("got startup message {m:?}");

                match m {
                    FeStartupPacket::SslRequest => {
                        debug!("SSL requested");

                        self.write_message_noflush(&BeMessage::EncryptionResponse(have_tls))?;
                        if have_tls {
                            self.start_tls().await?;
                            self.state = ProtoState::Encrypted;
                        }
                    }
                    FeStartupPacket::GssEncRequest => {
                        debug!("GSS requested");
                        self.write_message_noflush(&BeMessage::EncryptionResponse(false))?;
                    }
                    FeStartupPacket::StartupMessage { .. } => {
                        if have_tls && !matches!(self.state, ProtoState::Encrypted) {
                            self.write_message_noflush(&BeMessage::ErrorResponse(
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
                                self.write_message_noflush(&BeMessage::AuthenticationOk)?
                                    .write_message_noflush(&BeMessage::CLIENT_ENCODING)?
                                    // The async python driver requires a valid server_version
                                    .write_message_noflush(&BeMessage::server_version("14.1"))?
                                    .write_message_noflush(&BeMessage::ReadyForQuery)?;
                                self.state = ProtoState::Established;
                            }
                            AuthType::NeonJWT => {
                                self.write_message_noflush(
                                    &BeMessage::AuthenticationCleartextPassword,
                                )?;
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
                            self.write_message_noflush(&BeMessage::ErrorResponse(
                                &e.to_string(),
                                Some(e.pg_error_code()),
                            ))?;
                            return Err(e);
                        }
                    }
                }
                self.write_message_noflush(&BeMessage::AuthenticationOk)?
                    .write_message_noflush(&BeMessage::CLIENT_ENCODING)?
                    .write_message_noflush(&BeMessage::ReadyForQuery)?;
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
                    self.write_message_noflush(&BeMessage::ErrorResponse(
                        &short_error,
                        Some(e.pg_error_code()),
                    ))?;
                }
                self.write_message_noflush(&BeMessage::ReadyForQuery)?;
            }

            FeMessage::Parse(m) => {
                *unnamed_query_string = m.query_string;
                self.write_message_noflush(&BeMessage::ParseComplete)?;
            }

            FeMessage::Describe(_) => {
                self.write_message_noflush(&BeMessage::ParameterDescription)?
                    .write_message_noflush(&BeMessage::NoData)?;
            }

            FeMessage::Bind(_) => {
                self.write_message_noflush(&BeMessage::BindComplete)?;
            }

            FeMessage::Close(_) => {
                self.write_message_noflush(&BeMessage::CloseComplete)?;
            }

            FeMessage::Execute(_) => {
                let query_string = cstr_to_str(unnamed_query_string)?;
                trace!("got execute {query_string:?}");
                if let Err(e) = handler.process_query(self, query_string).await {
                    log_query_error(query_string, &e);
                    self.write_message_noflush(&BeMessage::ErrorResponse(
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
                self.write_message_noflush(&BeMessage::ReadyForQuery)?;
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
        match ready!(this.pgb.poll_write_buf(cx)) {
            Ok(()) => {}
            Err(err) => return Poll::Ready(Err(err)),
        }

        // CopyData
        // XXX: if the input is large, we should split it into multiple messages.
        // Not sure what the threshold should be, but the ultimate hard limit is that
        // the length cannot exceed u32.
        this.pgb.write_message_noflush(&BeMessage::CopyData(buf))?;

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let this = self.get_mut();
        match ready!(this.pgb.poll_write_buf(cx)) {
            Ok(()) => {}
            Err(err) => return Poll::Ready(Err(err)),
        }
        this.pgb.poll_flush(cx)
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let this = self.get_mut();
        match ready!(this.pgb.poll_write_buf(cx)) {
            Ok(()) => {}
            Err(err) => return Poll::Ready(Err(err)),
        }
        this.pgb.poll_flush(cx)
    }
}

pub fn short_error(e: &QueryError) -> String {
    match e {
        QueryError::Disconnected(connection_error) => connection_error.to_string(),
        QueryError::Other(e) => format!("{e:#}"),
    }
}

pub fn log_query_error(query: &str, e: &QueryError) {
    match e {
        QueryError::Disconnected(ConnectionError::Socket(io_error)) => {
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
