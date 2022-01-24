use anyhow::{anyhow, ensure, Context};
use rustls::{internal::pemfile, NoClientAuth, ProtocolVersion, ServerConfig};
use std::net::SocketAddr;
use std::sync::Arc;

pub type TlsConfig = Arc<ServerConfig>;

pub struct ProxyConfig {
    /// main entrypoint for users to connect to
    pub proxy_address: SocketAddr,

    /// internally used for status and prometheus metrics
    pub http_address: SocketAddr,

    /// management endpoint. Upon user account creation control plane
    /// will notify us here, so that we can 'unfreeze' user session.
    /// TODO It uses postgres protocol over TCP but should be migrated to http.
    pub mgmt_address: SocketAddr,

    /// send unauthenticated users to this URI
    pub redirect_uri: String,

    /// control plane address where we would check auth.
    pub auth_endpoint: String,

    pub tls_config: Option<TlsConfig>,
}

pub fn configure_ssl(key_path: &str, cert_path: &str) -> anyhow::Result<TlsConfig> {
    let key = {
        let key_bytes = std::fs::read(key_path).context("SSL key file")?;
        let mut keys = pemfile::pkcs8_private_keys(&mut &key_bytes[..])
            .map_err(|_| anyhow!("couldn't read TLS keys"))?;
        ensure!(keys.len() == 1, "keys.len() = {} (should be 1)", keys.len());
        keys.pop().unwrap()
    };

    let cert_chain = {
        let cert_chain_bytes = std::fs::read(cert_path).context("SSL cert file")?;
        pemfile::certs(&mut &cert_chain_bytes[..])
            .map_err(|_| anyhow!("couldn't read TLS certificates"))?
    };

    let mut config = ServerConfig::new(NoClientAuth::new());
    config.set_single_cert(cert_chain, key)?;
    config.versions = vec![ProtocolVersion::TLSv1_3];

    Ok(config.into())
}
