use crate::auth;
use crate::cancellation::{self, CancelClosure, CancelMap};
use crate::compute::DatabaseInfo;
use crate::config::{ProxyConfig, TlsConfig};
use crate::stream::{MetricsStream, PqStream, Stream};
use anyhow::{bail, Context};
use lazy_static::lazy_static;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use zenith_metrics::{new_common_metric_name, register_int_counter, IntCounter};
use zenith_utils::pq_proto::{BeMessage as Be, *};

lazy_static! {
    static ref NUM_CONNECTIONS_ACCEPTED_COUNTER: IntCounter = register_int_counter!(
        new_common_metric_name("num_connections_accepted"),
        "Number of TCP client connections accepted."
    )
    .unwrap();
    static ref NUM_CONNECTIONS_CLOSED_COUNTER: IntCounter = register_int_counter!(
        new_common_metric_name("num_connections_closed"),
        "Number of TCP client connections closed."
    )
    .unwrap();
    static ref NUM_BYTES_PROXIED_COUNTER: IntCounter = register_int_counter!(
        new_common_metric_name("num_bytes_proxied"),
        "Number of bytes sent/received between any client and backend."
    )
    .unwrap();
}

async fn log_error<R, F>(future: F) -> F::Output
where
    F: std::future::Future<Output = anyhow::Result<R>>,
{
    future.await.map_err(|err| {
        println!("error: {}", err);
        err
    })
}

pub async fn thread_main(
    config: &'static ProxyConfig,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        println!("proxy has shut down");
    }

    let cancel_map = Arc::new(CancelMap::default());
    loop {
        let (socket, peer_addr) = listener.accept().await?;
        println!("accepted connection from {}", peer_addr);

        let cancel_map = Arc::clone(&cancel_map);
        tokio::spawn(log_error(async move {
            socket
                .set_nodelay(true)
                .context("failed to set socket option")?;

            handle_client(config, &cancel_map, socket).await
        }));
    }
}

async fn handle_client(
    config: &ProxyConfig,
    cancel_map: &CancelMap,
    stream: impl AsyncRead + AsyncWrite + Unpin,
) -> anyhow::Result<()> {
    // The `closed` counter will increase when this future is destroyed.
    NUM_CONNECTIONS_ACCEPTED_COUNTER.inc();
    scopeguard::defer! {
        NUM_CONNECTIONS_CLOSED_COUNTER.inc();
    }

    let tls = config.tls_config.clone();
    if let Some((client, creds)) = handshake(stream, tls, cancel_map).await? {
        cancel_map
            .with_session(|session| async {
                connect_client_to_db(config, session, client, creds).await
            })
            .await?;
    }

    Ok(())
}

/// Handle a connection from one client.
/// For better testing experience, `stream` can be
/// any object satisfying the traits.
async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    mut tls: Option<TlsConfig>,
    cancel_map: &CancelMap,
) -> anyhow::Result<Option<(PqStream<Stream<S>>, auth::ClientCredentials)>> {
    // Client may try upgrading to each protocol only once
    let (mut tried_ssl, mut tried_gss) = (false, false);

    let mut stream = PqStream::new(Stream::from_raw(stream));
    loop {
        let msg = stream.read_startup_packet().await?;
        println!("got message: {:?}", msg);

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
                        stream = PqStream::new(stream.into_inner().upgrade(tls).await?);
                    }
                }
                _ => bail!("protocol violation"),
            },
            GssEncRequest => match stream.get_ref() {
                Stream::Raw { .. } if !tried_gss => {
                    tried_gss = true;

                    // Currently, we don't support GSSAPI
                    stream.write_message(&Be::EncryptionResponse(false)).await?;
                }
                _ => bail!("protocol violation"),
            },
            StartupMessage { params, .. } => {
                // Check that the config has been consumed during upgrade
                // OR we didn't provide it at all (for dev purposes).
                if tls.is_some() {
                    let msg = "connection is insecure (try using `sslmode=require`)";
                    stream.write_message(&Be::ErrorResponse(msg)).await?;
                    bail!(msg);
                }

                break Ok(Some((stream, params.try_into()?)));
            }
            CancelRequest(cancel_key_data) => {
                cancel_map.cancel_session(cancel_key_data).await?;

                break Ok(None);
            }
        }
    }
}

async fn connect_client_to_db(
    config: &ProxyConfig,
    session: cancellation::Session<'_>,
    mut client: PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    creds: auth::ClientCredentials,
) -> anyhow::Result<()> {
    let db_info = creds.authenticate(config, &mut client).await?;
    let (db, version, cancel_closure) = connect_to_db(db_info).await?;
    let cancel_key_data = session.enable_cancellation(cancel_closure);

    client
        .write_message_noflush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::ServerVersion(&version),
        ))?
        .write_message_noflush(&Be::BackendKeyData(cancel_key_data))?
        .write_message(&BeMessage::ReadyForQuery)
        .await?;

    // This function will be called for writes to either direction.
    fn inc_proxied(cnt: usize) {
        // Consider inventing something more sophisticated
        // if this ever becomes a bottleneck (cacheline bouncing).
        NUM_BYTES_PROXIED_COUNTER.inc_by(cnt as u64);
    }

    let mut db = MetricsStream::new(db, inc_proxied);
    let mut client = MetricsStream::new(client.into_inner(), inc_proxied);
    let _ = tokio::io::copy_bidirectional(&mut client, &mut db).await?;

    Ok(())
}

/// Connect to a corresponding compute node.
async fn connect_to_db(
    db_info: DatabaseInfo,
) -> anyhow::Result<(TcpStream, String, CancelClosure)> {
    // TODO: establish a secure connection to the DB
    let socket_addr = db_info.socket_addr()?;
    let mut socket = TcpStream::connect(socket_addr).await?;

    let (client, conn) = tokio_postgres::Config::from(db_info)
        .connect_raw(&mut socket, NoTls)
        .await?;

    let version = conn
        .parameter("server_version")
        .context("failed to fetch postgres server version")?
        .into();

    let cancel_closure = CancelClosure::new(socket_addr, client.cancel_token());

    Ok((socket, version, cancel_closure))
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::DuplexStream;
    use tokio_postgres::config::SslMode;
    use tokio_postgres::tls::MakeTlsConnect;
    use tokio_postgres_rustls::MakeRustlsConnect;

    async fn dummy_proxy(
        client: impl AsyncRead + AsyncWrite + Unpin,
        tls: Option<TlsConfig>,
    ) -> anyhow::Result<()> {
        let cancel_map = CancelMap::default();

        // TODO: add some infra + tests for credentials
        let (mut stream, _creds) = handshake(client, tls, &cancel_map)
            .await?
            .context("no stream")?;

        stream
            .write_message_noflush(&Be::AuthenticationOk)?
            .write_message_noflush(&BeParameterStatusMessage::encoding())?
            .write_message(&BeMessage::ReadyForQuery)
            .await?;

        Ok(())
    }

    fn generate_certs(
        hostname: &str,
    ) -> anyhow::Result<(rustls::Certificate, rustls::Certificate, rustls::PrivateKey)> {
        let ca = rcgen::Certificate::from_params({
            let mut params = rcgen::CertificateParams::default();
            params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            params
        })?;

        let cert = rcgen::generate_simple_self_signed(vec![hostname.into()])?;
        Ok((
            rustls::Certificate(ca.serialize_der()?),
            rustls::Certificate(cert.serialize_der_with_signer(&ca)?),
            rustls::PrivateKey(cert.serialize_private_key_der()),
        ))
    }

    #[tokio::test]
    async fn handshake_tls_is_enforced_by_proxy() -> anyhow::Result<()> {
        let (client, server) = tokio::io::duplex(1024);

        let server_config = {
            let (_ca, cert, key) = generate_certs("localhost")?;

            let mut config = rustls::ServerConfig::new(rustls::NoClientAuth::new());
            config.set_single_cert(vec![cert], key)?;
            config
        };

        let proxy = tokio::spawn(dummy_proxy(client, Some(server_config.into())));

        tokio_postgres::Config::new()
            .user("john_doe")
            .dbname("earth")
            .ssl_mode(SslMode::Disable)
            .connect_raw(server, NoTls)
            .await
            .err() // -> Option<E>
            .context("client shouldn't be able to connect")?;

        proxy
            .await?
            .err() // -> Option<E>
            .context("server shouldn't accept client")?;

        Ok(())
    }

    #[tokio::test]
    async fn handshake_tls() -> anyhow::Result<()> {
        let (client, server) = tokio::io::duplex(1024);

        let (ca, cert, key) = generate_certs("localhost")?;

        let server_config = {
            let mut config = rustls::ServerConfig::new(rustls::NoClientAuth::new());
            config.set_single_cert(vec![cert], key)?;
            config
        };

        let proxy = tokio::spawn(dummy_proxy(client, Some(server_config.into())));

        let client_config = {
            let mut config = rustls::ClientConfig::new();
            config.root_store.add(&ca)?;
            config
        };

        let mut mk = MakeRustlsConnect::new(client_config);
        let tls = MakeTlsConnect::<DuplexStream>::make_tls_connect(&mut mk, "localhost")?;

        let (_client, _conn) = tokio_postgres::Config::new()
            .user("john_doe")
            .dbname("earth")
            .ssl_mode(SslMode::Require)
            .connect_raw(server, tls)
            .await?;

        proxy.await?
    }

    #[tokio::test]
    async fn handshake_raw() -> anyhow::Result<()> {
        let (client, server) = tokio::io::duplex(1024);

        let proxy = tokio::spawn(dummy_proxy(client, None));

        let (_client, _conn) = tokio_postgres::Config::new()
            .user("john_doe")
            .dbname("earth")
            .ssl_mode(SslMode::Prefer)
            .connect_raw(server, NoTls)
            .await?;

        proxy.await?
    }
}
