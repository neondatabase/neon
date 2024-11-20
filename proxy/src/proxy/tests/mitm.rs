//! Man-in-the-middle tests
//!
//! Channel binding should prevent a proxy server
//! *that has access to create valid certificates*
//! from controlling the TLS connection.

use std::fmt::Debug;

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncReadExt, DuplexStream};
use tokio_postgres::tls::TlsConnect;
use tokio_util::codec::{Decoder, Encoder};

use super::*;

enum Intercept {
    None,
    Methods,
    SASLResponse,
}

async fn proxy_mitm(
    intercept: Intercept,
) -> (DuplexStream, DuplexStream, ClientConfig<'static>, TlsConfig) {
    let (end_server1, client1) = tokio::io::duplex(1024);
    let (server2, end_client2) = tokio::io::duplex(1024);

    let (client_config1, server_config1) =
        generate_tls_config("generic-project-name.localhost", "localhost").unwrap();
    let (client_config2, server_config2) =
        generate_tls_config("generic-project-name.localhost", "localhost").unwrap();

    tokio::spawn(async move {
        // begin handshake with end_server
        let end_server = connect_tls(server2, client_config2.make_tls_connect().unwrap()).await;
        let (end_client, startup) = match handshake(
            &RequestContext::test(),
            client1,
            Some(&server_config1),
            false,
        )
        .await
        .unwrap()
        {
            HandshakeData::Startup(stream, params) => (stream, params),
            HandshakeData::Cancel(_) => panic!("cancellation not supported"),
        };

        let mut end_server = tokio_util::codec::Framed::new(end_server, PgFrame);
        let (end_client, buf) = end_client.framed.into_inner();
        assert!(buf.is_empty());
        let mut end_client = tokio_util::codec::Framed::new(end_client, PgFrame);

        // give the end_server the startup parameters
        let mut buf = BytesMut::new();
        frontend::startup_message(startup.iter(), &mut buf).unwrap();
        end_server.send(buf.freeze()).await.unwrap();

        // proxy messages between end_client and end_server
        loop {
            tokio::select! {
                message = end_server.next() => {
                    match message {
                        Some(Ok(message)) => {
                            // intercept SASL and return only SCRAM-SHA-256 ;)
                            if matches!(intercept, Intercept::Methods) && message.starts_with(b"R") && message[5..].starts_with(&[0,0,0,10]) {
                                end_client.send(Bytes::from_static(b"R\0\0\0\x17\0\0\0\x0aSCRAM-SHA-256\0\0")).await.unwrap();
                                continue;
                            }
                            end_client.send(message).await.unwrap();
                        }
                        _ => break,
                    }
                }
                message = end_client.next() => {
                    match message {
                        Some(Ok(message)) => {
                            // intercept SASL response and return SCRAM-SHA-256 with no channel binding ;)
                            if matches!(intercept, Intercept::SASLResponse) && message.starts_with(b"p") && message[5..].starts_with(b"SCRAM-SHA-256-PLUS\0") {
                                let sasl_message = &message[1+4+19+4..];
                                let mut new_message = b"n,,".to_vec();
                                new_message.extend_from_slice(sasl_message.strip_prefix(b"p=tls-server-end-point,,").unwrap());

                                let mut buf = BytesMut::new();
                                frontend::sasl_initial_response("SCRAM-SHA-256", &new_message, &mut buf).unwrap();

                                end_server.send(buf.freeze()).await.unwrap();
                                continue;
                            }
                            end_server.send(message).await.unwrap();
                        }
                        _ => break,
                    }
                }
                else => { break }
            }
        }
    });

    (end_server1, end_client2, client_config1, server_config2)
}

/// taken from tokio-postgres
pub(crate) async fn connect_tls<S, T>(mut stream: S, tls: T) -> T::Stream
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsConnect<S>,
    T::Error: Debug,
{
    let mut buf = BytesMut::new();
    frontend::ssl_request(&mut buf);
    stream.write_all(&buf).await.unwrap();

    let mut buf = [0];
    stream.read_exact(&mut buf).await.unwrap();

    assert!(buf[0] == b'S', "ssl not supported by server");

    tls.connect(stream).await.unwrap()
}

struct PgFrame;
impl Decoder for PgFrame {
    type Item = Bytes;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 5 {
            src.reserve(5 - src.len());
            return Ok(None);
        }
        let len = u32::from_be_bytes(src[1..5].try_into().unwrap()) as usize + 1;
        if src.len() < len {
            src.reserve(len - src.len());
            return Ok(None);
        }
        Ok(Some(src.split_to(len).freeze()))
    }
}
impl Encoder<Bytes> for PgFrame {
    type Error = std::io::Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(&item);
        Ok(())
    }
}

/// If the client doesn't support channel bindings, it can be exploited.
#[tokio::test]
async fn scram_auth_disable_channel_binding() -> anyhow::Result<()> {
    let (server, client, client_config, server_config) = proxy_mitm(Intercept::None).await;
    let proxy = tokio::spawn(dummy_proxy(
        client,
        Some(server_config),
        Scram::new("password").await?,
    ));

    let _client_err = tokio_postgres::Config::new()
        .channel_binding(tokio_postgres::config::ChannelBinding::Disable)
        .user("user")
        .dbname("db")
        .password("password")
        .ssl_mode(SslMode::Require)
        .connect_raw(server, client_config.make_tls_connect()?)
        .await?;

    proxy.await?
}

/// If the client chooses SCRAM-PLUS, it will fail
#[tokio::test]
async fn scram_auth_prefer_channel_binding() -> anyhow::Result<()> {
    connect_failure(
        Intercept::None,
        tokio_postgres::config::ChannelBinding::Prefer,
    )
    .await
}

/// If the MITM pretends like SCRAM-PLUS isn't available, but the client supports it, it will fail
#[tokio::test]
async fn scram_auth_prefer_channel_binding_intercept() -> anyhow::Result<()> {
    connect_failure(
        Intercept::Methods,
        tokio_postgres::config::ChannelBinding::Prefer,
    )
    .await
}

/// If the MITM pretends like the client doesn't support channel bindings, it will fail
#[tokio::test]
async fn scram_auth_prefer_channel_binding_intercept_response() -> anyhow::Result<()> {
    connect_failure(
        Intercept::SASLResponse,
        tokio_postgres::config::ChannelBinding::Prefer,
    )
    .await
}

/// If the client chooses SCRAM-PLUS, it will fail
#[tokio::test]
async fn scram_auth_require_channel_binding() -> anyhow::Result<()> {
    connect_failure(
        Intercept::None,
        tokio_postgres::config::ChannelBinding::Require,
    )
    .await
}

/// If the client requires SCRAM-PLUS, and it is spoofed to remove SCRAM-PLUS, it will fail
#[tokio::test]
async fn scram_auth_require_channel_binding_intercept() -> anyhow::Result<()> {
    connect_failure(
        Intercept::Methods,
        tokio_postgres::config::ChannelBinding::Require,
    )
    .await
}

/// If the client requires SCRAM-PLUS, and it is spoofed to remove SCRAM-PLUS, it will fail
#[tokio::test]
async fn scram_auth_require_channel_binding_intercept_response() -> anyhow::Result<()> {
    connect_failure(
        Intercept::SASLResponse,
        tokio_postgres::config::ChannelBinding::Require,
    )
    .await
}

async fn connect_failure(
    intercept: Intercept,
    channel_binding: tokio_postgres::config::ChannelBinding,
) -> anyhow::Result<()> {
    let (server, client, client_config, server_config) = proxy_mitm(intercept).await;
    let proxy = tokio::spawn(dummy_proxy(
        client,
        Some(server_config),
        Scram::new("password").await?,
    ));

    let _client_err = tokio_postgres::Config::new()
        .channel_binding(channel_binding)
        .user("user")
        .dbname("db")
        .password("password")
        .ssl_mode(SslMode::Require)
        .connect_raw(server, client_config.make_tls_connect()?)
        .await
        .err()
        .context("client shouldn't be able to connect")?;

    let _server_err = proxy
        .await?
        .err()
        .context("server shouldn't accept client")?;

    Ok(())
}
