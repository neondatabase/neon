use crate::{
    auth::{self, backend::NeedsAuthentication},
    cancellation::CancelMap,
    config::{ProxyConfig, TlsConfig},
    context::RequestMonitoring,
    error::ReportableError,
    proxy::{ERR_INSECURE_CONNECTION, ERR_PROTO_VIOLATION},
    rate_limiter::EndpointRateLimiter,
    state_machine::{DynStage, Finished, ResultExt, Stage, StageError},
    stream::{PqStream, Stream, StreamUpgradeError},
};
use anyhow::{anyhow, Context};
use pq_proto::{BeMessage as Be, FeStartupPacket, StartupMessageParams};
use std::{io, sync::Arc};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{error, info};

use super::ClientMode;

pub struct NeedsHandshake<S> {
    pub stream: S,
    pub config: &'static ProxyConfig,
    pub cancel_map: Arc<CancelMap>,
    pub mode: ClientMode,
    pub endpoint_rate_limiter: Arc<EndpointRateLimiter>,

    // monitoring
    pub ctx: RequestMonitoring,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + 'static> Stage for NeedsHandshake<S> {
    fn span(&self) -> tracing::Span {
        tracing::info_span!("handshake")
    }
    async fn run(self) -> Result<DynStage, StageError> {
        let Self {
            stream,
            config,
            cancel_map,
            mode,
            endpoint_rate_limiter,
            mut ctx,
        } = self;

        let tls = config.tls_config.as_ref();

        let pause_timer = ctx.latency_timer.pause();
        let handshake = handshake(stream, mode.handshake_tls(tls), &cancel_map).await;
        drop(pause_timer);

        let (stream, params) = match handshake {
            Err(err) => {
                // TODO: proper handling
                error!("could not complete handshake: {err:#}");
                return Err(StageError::Done);
            }
            // cancellation
            Ok(None) => return Ok(Box::new(Finished)),
            Ok(Some(s)) => s,
        };

        let hostname = mode.hostname(stream.get_ref());

        let common_names = tls.map(|tls| &tls.common_names);
        let (creds, stream) = config
            .auth_backend
            .as_ref()
            .map(|_| {
                auth::ComputeUserInfoMaybeEndpoint::parse(&mut ctx, &params, hostname, common_names)
            })
            .transpose()
            .send_error_to_user(&mut ctx, stream)?;

        ctx.set_endpoint_id(creds.get_endpoint());

        Ok(Box::new(NeedsAuthentication {
            stream,
            creds,
            params,
            endpoint_rate_limiter,
            mode,
            config,

            ctx,
            cancel_session: cancel_map.get_session(),
        }))
    }
}

#[derive(Error, Debug)]
pub enum HandshakeError {
    #[error("client disconnected: {0}")]
    ClientIO(#[from] io::Error),
    #[error("protocol violation: {0}")]
    ProtocolError(#[from] anyhow::Error),
    #[error("could not initiate tls connection: {0}")]
    TLSError(#[from] StreamUpgradeError),
    #[error("could not cancel connection: {0}")]
    Cancel(anyhow::Error),
}

impl ReportableError for HandshakeError {
    fn get_error_type(&self) -> crate::error::ErrorKind {
        match self {
            HandshakeError::ClientIO(_) => crate::error::ErrorKind::Disconnect,
            HandshakeError::ProtocolError(_) => crate::error::ErrorKind::User,
            HandshakeError::TLSError(_) => crate::error::ErrorKind::User,
            HandshakeError::Cancel(_) => crate::error::ErrorKind::Compute,
        }
    }
}

type SuccessfulHandshake<S> = (PqStream<Stream<S>>, StartupMessageParams);

/// Establish a (most probably, secure) connection with the client.
/// For better testing experience, `stream` can be any object satisfying the traits.
/// It's easier to work with owned `stream` here as we need to upgrade it to TLS;
/// we also take an extra care of propagating only the select handshake errors to client.
pub async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    mut tls: Option<&TlsConfig>,
    cancel_map: &CancelMap,
) -> Result<Option<SuccessfulHandshake<S>>, HandshakeError> {
    // Client may try upgrading to each protocol only once
    let (mut tried_ssl, mut tried_gss) = (false, false);

    let mut stream = PqStream::new(Stream::from_raw(stream));
    loop {
        let msg = stream.read_startup_packet().await?;
        info!("received {msg:?}");

        use FeStartupPacket::*;
        match msg {
            SslRequest => match stream.get_ref() {
                Stream::Raw { .. } if !tried_ssl => {
                    tried_ssl = true;

                    // We can't perform TLS handshake without a config
                    let enc = tls.is_some();
                    stream.write_message(&Be::EncryptionResponse(enc)).await?;
                    if let Some(tls) = tls.take() {
                        // Upgrade raw stream into a secure TLS-backed stream.
                        // NOTE: We've consumed `tls`; this fact will be used later.

                        let (raw, read_buf) = stream.into_inner();
                        // TODO: Normally, client doesn't send any data before
                        // server says TLS handshake is ok and read_buf is empy.
                        // However, you could imagine pipelining of postgres
                        // SSLRequest + TLS ClientHello in one hunk similar to
                        // pipelining in our node js driver. We should probably
                        // support that by chaining read_buf with the stream.
                        if !read_buf.is_empty() {
                            return Err(HandshakeError::ProtocolError(anyhow!(
                                "data is sent before server replied with EncryptionResponse"
                            )));
                        }
                        let tls_stream = raw.upgrade(tls.to_server_config()).await?;

                        let (_, tls_server_end_point) = tls
                            .cert_resolver
                            .resolve(tls_stream.get_ref().1.server_name())
                            .context("missing certificate")?;

                        stream = PqStream::new(Stream::Tls {
                            tls: Box::new(tls_stream),
                            tls_server_end_point,
                        });
                    }
                }
                _ => return Err(HandshakeError::ProtocolError(anyhow!(ERR_PROTO_VIOLATION))),
            },
            GssEncRequest => match stream.get_ref() {
                Stream::Raw { .. } if !tried_gss => {
                    tried_gss = true;

                    // Currently, we don't support GSSAPI
                    stream.write_message(&Be::EncryptionResponse(false)).await?;
                }
                _ => return Err(HandshakeError::ProtocolError(anyhow!(ERR_PROTO_VIOLATION))),
            },
            StartupMessage { params, .. } => {
                // Check that the config has been consumed during upgrade
                // OR we didn't provide it at all (for dev purposes).
                if tls.is_some() {
                    stream.throw_error_str(ERR_INSECURE_CONNECTION).await?;
                }

                info!(session_type = "normal", "successful handshake");
                break Ok(Some((stream, params)));
            }
            CancelRequest(cancel_key_data) => {
                cancel_map
                    .cancel_session(cancel_key_data)
                    .await
                    .map_err(HandshakeError::Cancel)?;

                info!(session_type = "cancellation", "successful handshake");
                break Ok(None);
            }
        }
    }
}
