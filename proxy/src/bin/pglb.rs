use std::net::SocketAddr;

use anyhow::Context;
use quinn::Endpoint;

#[tokio::main]
async fn main() {
    let endpoint: Endpoint = endpoint_config("0.0.0.0:5634".parse().unwrap())
        .await
        .unwrap();

    let quinn_handle = tokio::spawn(quinn_server(endpoint.clone()));

    // tcp listener goes here

    quinn_handle.await.unwrap();
}

async fn endpoint_config(addr: SocketAddr) -> anyhow::Result<Endpoint> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    let mut params = rcgen::CertificateParams::new(vec!["pglb".to_string()]);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "pglb");
    let key = rcgen::KeyPair::generate(&rcgen::PKCS_ECDSA_P256_SHA256).context("keygen")?;
    params.key_pair = Some(key);

    let cert = rcgen::Certificate::from_params(params).context("cert")?;
    let cert_der = cert.serialize_der().context("serialize")?;
    let key_der = cert.serialize_private_key_der();
    let cert = CertificateDer::from(cert_der);
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der));

    let config = quinn::ServerConfig::with_single_cert(vec![cert], key).context("server config")?;
    Endpoint::server(config, addr).context("endpoint")
}

async fn quinn_server(_ep: Endpoint) {
    std::future::pending().await
}
