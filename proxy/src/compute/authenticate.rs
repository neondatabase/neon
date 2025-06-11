use bytes::BufMut;
use postgres_client::tls::{ChannelBinding, TlsStream};
use postgres_protocol::authentication::sasl;
use postgres_protocol::authentication::sasl::{SCRAM_SHA_256, SCRAM_SHA_256_PLUS};
use tokio::io::{AsyncRead, AsyncWrite};

use super::{Auth, MaybeRustlsStream};
use crate::compute::RustlsStream;
use crate::pqproto::{
    AUTH_OK, AUTH_SASL, AUTH_SASL_CONT, AUTH_SASL_FINAL, FE_PASSWORD_MESSAGE, StartupMessageParams,
};
use crate::stream::{PostgresError, PqBeStream};

pub async fn authenticate<S>(
    stream: MaybeRustlsStream<S>,
    auth: Option<&Auth>,
    params: &StartupMessageParams,
) -> Result<PqBeStream<MaybeRustlsStream<S>>, PostgresError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    RustlsStream<S>: TlsStream + Unpin,
{
    let mut stream = PqBeStream::new(stream, params);
    stream.flush().await?;

    let channel_binding = stream.get_ref().channel_binding();

    // TODO: rather than checking for SASL, maybe we can just assume it.
    // With SCRAM_SHA_256 if we're not using TLS,
    // and SCRAM_SHA_256_PLUS if we are using TLS.

    let (channel_binding, mechanism) = match stream.read_auth_message().await? {
        (AUTH_OK, _) => return Ok(stream),
        (AUTH_SASL, mechanisms) => {
            let mut has_scram = false;
            let mut has_scram_plus = false;
            for mechanism in mechanisms.split(|&b| b == b'\0') {
                match mechanism {
                    b"SCRAM-SHA-256" => has_scram = true,
                    b"SCRAM-SHA-256-PLUS" => has_scram_plus = true,
                    _ => {}
                }
            }

            match (channel_binding, has_scram, has_scram_plus) {
                (cb, true, false) => {
                    if cb.tls_server_end_point.is_some() {
                        // I don't think this can happen in our setup, but I would like to monitor it.
                        tracing::warn!(
                            "TLS is enabled, but compute doesn't support SCRAM-SHA-256-PLUS."
                        );
                    }
                    (sasl::ChannelBinding::unrequested(), SCRAM_SHA_256)
                }
                (
                    ChannelBinding {
                        tls_server_end_point: None,
                    },
                    true,
                    _,
                ) => (sasl::ChannelBinding::unsupported(), SCRAM_SHA_256),
                (
                    ChannelBinding {
                        tls_server_end_point: Some(h),
                    },
                    _,
                    true,
                ) => (
                    sasl::ChannelBinding::tls_server_end_point(h),
                    SCRAM_SHA_256_PLUS,
                ),
                (_, false, _) => {
                    tracing::error!(
                        "compute responded with unsupported auth mechanisms: {}",
                        String::from_utf8_lossy(mechanisms)
                    );
                    return Err(PostgresError::InvalidAuthMessage);
                }
            }
        }
        (tag, msg) => {
            tracing::error!(
                "compute responded with unexpected auth message with tag[{tag}]: {}",
                String::from_utf8_lossy(msg)
            );
            return Err(PostgresError::InvalidAuthMessage);
        }
    };

    let mut scram = match auth {
        // We only touch passwords when it comes to console-redirect.
        Some(Auth::Password(pw)) => sasl::ScramSha256::new(pw, channel_binding),
        Some(Auth::Scram(keys)) => sasl::ScramSha256::new_with_keys(**keys, channel_binding),
        None => {
            // local_proxy does not set credentials, since it relies on trust and expects an OK message above
            tracing::error!("compute requested SASL auth, but there are no credentials available",);
            return Err(PostgresError::InvalidAuthMessage);
        }
    };

    stream.write_raw(0, FE_PASSWORD_MESSAGE.0, |buf| {
        buf.put_slice(mechanism.as_bytes());
        buf.put_u8(b'\0');

        let data = scram.message();
        buf.put_u32(data.len() as u32);
        buf.put_slice(data);
    });
    stream.flush().await?;

    loop {
        // wait for SASLContinue or SASLFinal.
        match stream.read_auth_message().await? {
            (AUTH_SASL_CONT, data) => scram.update(data).await?,
            (AUTH_SASL_FINAL, data) => {
                scram.finish(data)?;
                break;
            }
            (tag, msg) => {
                tracing::error!(
                    "compute responded with unexpected auth message with tag[{tag}]: {}",
                    String::from_utf8_lossy(msg)
                );
                return Err(PostgresError::InvalidAuthMessage);
            }
        }

        stream.write_raw(0, FE_PASSWORD_MESSAGE.0, |buf| {
            buf.put_slice(scram.message());
        });
        stream.flush().await?;
    }

    match stream.read_auth_message().await? {
        (AUTH_OK, _) => {}
        (tag, msg) => {
            tracing::error!(
                "compute responded with unexpected auth message with tag[{tag}]: {}",
                String::from_utf8_lossy(msg)
            );
            return Err(PostgresError::InvalidAuthMessage);
        }
    }

    Ok(stream)
}
