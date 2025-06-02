use futures::{FutureExt, TryFutureExt};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, info, warn};

use crate::auth::endpoint_sni;
use crate::config::TlsConfig;
use crate::context::RequestContext;
use crate::error::ReportableError;
use crate::metrics::Metrics;
use crate::pqproto::{
    BeMessage, CancelKeyData, FeStartupPacket, ProtocolVersion, StartupMessageParams,
};
use crate::proxy::TlsRequired;
use crate::stream::{PqStream, Stream, StreamUpgradeError};
use crate::tls::PG_ALPN_PROTOCOL;

#[derive(Error, Debug)]
pub(crate) enum HandshakeError {
    #[error("data is sent before server replied with EncryptionResponse")]
    EarlyData,

    #[error("protocol violation")]
    ProtocolViolation,

    #[error("{0}")]
    StreamUpgradeError(#[from] StreamUpgradeError),

    #[error("{0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    ReportedError(#[from] crate::stream::ReportedError),
}

impl ReportableError for HandshakeError {
    fn get_error_kind(&self) -> crate::error::ErrorKind {
        match self {
            HandshakeError::EarlyData => crate::error::ErrorKind::User,
            HandshakeError::ProtocolViolation => crate::error::ErrorKind::User,
            HandshakeError::StreamUpgradeError(upgrade) => match upgrade {
                StreamUpgradeError::AlreadyTls => crate::error::ErrorKind::Service,
                StreamUpgradeError::Io(_) => crate::error::ErrorKind::ClientDisconnect,
            },
            HandshakeError::Io(_) => crate::error::ErrorKind::ClientDisconnect,
            HandshakeError::ReportedError(e) => e.get_error_kind(),
        }
    }
}

pub(crate) enum HandshakeData<S> {
    Startup(PqStream<Stream<S>>, StartupMessageParams),
    Cancel(CancelKeyData),
}

/// Establish a (most probably, secure) connection with the client.
/// For better testing experience, `stream` can be any object satisfying the traits.
/// It's easier to work with owned `stream` here as we need to upgrade it to TLS;
/// we also take an extra care of propagating only the select handshake errors to client.
#[tracing::instrument(skip_all)]
pub(crate) async fn handshake<S: AsyncRead + AsyncWrite + Unpin + Send>(
    ctx: &RequestContext,
    stream: S,
    mut tls: Option<&TlsConfig>,
    record_handshake_error: bool,
) -> Result<HandshakeData<S>, HandshakeError> {
    // Client may try upgrading to each protocol only once
    let (mut tried_ssl, mut tried_gss) = (false, false);

    const PG_PROTOCOL_EARLIEST: ProtocolVersion = ProtocolVersion::new(3, 0);
    const PG_PROTOCOL_LATEST: ProtocolVersion = ProtocolVersion::new(3, 0);

    let (mut stream, mut msg) = PqStream::parse_startup(Stream::from_raw(stream)).await?;
    loop {
        match msg {
            FeStartupPacket::SslRequest { direct } => match stream.get_ref() {
                Stream::Raw { .. } if !tried_ssl => {
                    tried_ssl = true;

                    if let Some(tls) = tls.take() {
                        // Upgrade raw stream into a secure TLS-backed stream.
                        // NOTE: We've consumed `tls`; this fact will be used later.

                        let mut read_buf;
                        let raw = if let Some(direct) = &direct {
                            read_buf = &direct[..];
                            stream.accept_direct_tls()
                        } else {
                            read_buf = &[];
                            stream.accept_tls().await?
                        };

                        let Stream::Raw { raw } = raw else {
                            return Err(HandshakeError::StreamUpgradeError(
                                StreamUpgradeError::AlreadyTls,
                            ));
                        };

                        let mut res = Ok(());
                        let accept = tokio_rustls::TlsAcceptor::from(tls.pg_config.clone())
                            .accept_with(raw, |session| {
                                // push the early data to the tls session
                                while !read_buf.is_empty() {
                                    match session.read_tls(&mut read_buf) {
                                        Ok(_) => {}
                                        Err(e) => {
                                            res = Err(e);
                                            break;
                                        }
                                    }
                                }
                            })
                            .map_ok(Box::new)
                            .boxed();

                        res?;

                        if !read_buf.is_empty() {
                            return Err(HandshakeError::EarlyData);
                        }

                        let tls_stream = accept.await.inspect_err(|_| {
                            if record_handshake_error {
                                Metrics::get().proxy.tls_handshake_failures.inc();
                            }
                        })?;

                        let conn_info = tls_stream.get_ref().1;

                        // try parse endpoint
                        let ep = conn_info
                            .server_name()
                            .and_then(|sni| endpoint_sni(sni, &tls.common_names));
                        if let Some(ep) = ep {
                            ctx.set_endpoint_id(ep);
                        }

                        // check the ALPN, if exists, as required.
                        match conn_info.alpn_protocol() {
                            None | Some(PG_ALPN_PROTOCOL) => {}
                            Some(other) => {
                                let alpn = String::from_utf8_lossy(other);
                                warn!(%alpn, "unexpected ALPN");
                                return Err(HandshakeError::ProtocolViolation);
                            }
                        }

                        let (_, tls_server_end_point) =
                            tls.cert_resolver.resolve(conn_info.server_name());

                        let tls = Stream::Tls {
                            tls: tls_stream,
                            tls_server_end_point,
                        };
                        (stream, msg) = PqStream::parse_startup(tls).await?;
                    } else {
                        if direct.is_some() {
                            // client sent us a ClientHello already, we can't do anything with it.
                            return Err(HandshakeError::ProtocolViolation);
                        }
                        msg = stream.reject_encryption().await?;
                    }
                }
                _ => return Err(HandshakeError::ProtocolViolation),
            },
            FeStartupPacket::GssEncRequest => match stream.get_ref() {
                Stream::Raw { .. } if !tried_gss => {
                    tried_gss = true;

                    // Currently, we don't support GSSAPI
                    msg = stream.reject_encryption().await?;
                }
                _ => return Err(HandshakeError::ProtocolViolation),
            },
            FeStartupPacket::StartupMessage { params, version }
                if PG_PROTOCOL_EARLIEST <= version && version <= PG_PROTOCOL_LATEST =>
            {
                // Check that the config has been consumed during upgrade
                // OR we didn't provide it at all (for dev purposes).
                if tls.is_some() {
                    Err(stream.throw_error(TlsRequired, None).await)?;
                }

                // This log highlights the start of the connection.
                // This contains useful information for debugging, not logged elsewhere, like role name and endpoint id.
                info!(
                    ?version,
                    ?params,
                    session_type = "normal",
                    "successful handshake"
                );
                break Ok(HandshakeData::Startup(stream, params));
            }
            // downgrade protocol version
            FeStartupPacket::StartupMessage { params, version }
                if version.major() == 3 && version > PG_PROTOCOL_LATEST =>
            {
                debug!(?version, "unsupported minor version");

                // no protocol extensions are supported.
                // <https://github.com/postgres/postgres/blob/ca481d3c9ab7bf69ff0c8d71ad3951d407f6a33c/src/backend/tcop/backend_startup.c#L744-L753>
                let mut unsupported = vec![];
                let mut supported = StartupMessageParams::default();

                for (k, v) in params.iter() {
                    if k.starts_with("_pq_.") {
                        unsupported.push(k);
                    } else {
                        supported.insert(k, v);
                    }
                }

                stream.write_message(BeMessage::NegotiateProtocolVersion {
                    version: PG_PROTOCOL_LATEST,
                    options: &unsupported,
                });
                stream.flush().await?;

                info!(
                    ?version,
                    ?params,
                    session_type = "normal",
                    "successful handshake; unsupported minor version requested"
                );
                break Ok(HandshakeData::Startup(stream, supported));
            }
            FeStartupPacket::StartupMessage { version, params } => {
                warn!(
                    ?version,
                    ?params,
                    session_type = "normal",
                    "unsuccessful handshake; unsupported version"
                );
                return Err(HandshakeError::ProtocolViolation);
            }
            FeStartupPacket::CancelRequest(cancel_key_data) => {
                info!(session_type = "cancellation", "successful handshake");
                break Ok(HandshakeData::Cancel(cancel_key_data));
            }
        }
    }
}
