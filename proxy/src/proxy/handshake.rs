use anyhow::{bail, Context};
use pq_proto::{BeMessage as Be, FeStartupPacket, StartupMessageParams};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::info;

use crate::{
    cancellation::CancelMap,
    config::TlsConfig,
    proxy::{ERR_INSECURE_CONNECTION, ERR_PROTO_VIOLATION},
    stream::{PqStream, Stream},
};

/// Establish a (most probably, secure) connection with the client.
/// For better testing experience, `stream` can be any object satisfying the traits.
/// It's easier to work with owned `stream` here as we need to upgrade it to TLS;
/// we also take an extra care of propagating only the select handshake errors to client.
#[tracing::instrument(skip_all)]
pub async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    mut tls: Option<&TlsConfig>,
    cancel_map: &CancelMap,
) -> anyhow::Result<Option<(PqStream<Stream<S>>, StartupMessageParams)>> {
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
                            bail!("data is sent before server replied with EncryptionResponse");
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
                _ => bail!(ERR_PROTO_VIOLATION),
            },
            GssEncRequest => match stream.get_ref() {
                Stream::Raw { .. } if !tried_gss => {
                    tried_gss = true;

                    // Currently, we don't support GSSAPI
                    stream.write_message(&Be::EncryptionResponse(false)).await?;
                }
                _ => bail!(ERR_PROTO_VIOLATION),
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
                cancel_map.cancel_session(cancel_key_data).await?;

                info!(session_type = "cancellation", "successful handshake");
                break Ok(None);
            }
        }
    }
}
