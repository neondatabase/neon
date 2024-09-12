use std::{sync::Arc, time::Duration};

use futures::TryStreamExt;
use proxy::{PglbCodec, PglbControlMessage, PglbMessage};
use quinn::{
    crypto::rustls::QuicClientConfig, rustls::client::danger, Endpoint, RecvStream, SendStream,
    VarInt,
};
use tokio::{
    io::{join, AsyncWriteExt},
    select,
    signal::unix::{signal, SignalKind},
    time::interval,
};
use tokio_util::{codec::Framed, task::TaskTracker};

#[tokio::main]
async fn main() {
    let server = "127.0.0.1:5634".parse().unwrap();
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();

    let crypto = quinn::rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();

    let crypto = QuicClientConfig::try_from(crypto).unwrap();

    let config = quinn::ClientConfig::new(Arc::new(crypto));
    endpoint.set_default_client_config(config);

    let mut int = signal(SignalKind::interrupt()).unwrap();
    let mut term = signal(SignalKind::terminate()).unwrap();

    let conn = endpoint.connect(server, "pglb").unwrap().await.unwrap();
    let mut interval = interval(Duration::from_secs(2));

    let tasks = TaskTracker::new();

    loop {
        select! {
            _ = int.recv() => break,
            _ = term.recv() => break,
            _ = interval.tick() => {
                let mut stream = conn.open_uni().await.unwrap();
                stream.flush().await.unwrap();
                stream.finish().unwrap();
            }
            stream = conn.accept_bi() => {
                let (send, recv) = stream.unwrap();
                tasks.spawn(handle_stream(send, recv));
            }
        }
    }

    // graceful shutdown
    {
        let mut stream = conn.open_uni().await.unwrap();
        stream.write_all(b"shutdown").await.unwrap();
        stream.flush().await.unwrap();
        stream.finish().unwrap();
    }

    tasks.close();
    tasks.wait().await;
    conn.close(VarInt::from_u32(1), b"graceful shutdown");
}

#[derive(Copy, Clone, Debug)]
struct NoVerify;

impl danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<danger::ServerCertVerified, quinn::rustls::Error> {
        Ok(danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &quinn::rustls::DigitallySignedStruct,
    ) -> Result<danger::HandshakeSignatureValid, quinn::rustls::Error> {
        Ok(danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &quinn::rustls::DigitallySignedStruct,
    ) -> Result<danger::HandshakeSignatureValid, quinn::rustls::Error> {
        Ok(danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<quinn::rustls::SignatureScheme> {
        vec![quinn::rustls::SignatureScheme::ECDSA_NISTP256_SHA256]
    }
}

async fn handle_stream(send: SendStream, recv: RecvStream) {
    let mut stream = Framed::new(join(recv, send), PglbCodec);

    let first_msg = stream.try_next().await.unwrap();
    let Some(PglbMessage::Control(PglbControlMessage::ConnectionInitiated(_first_msg))) = first_msg
    else {
        panic!("invalid first msg")
    };
}
