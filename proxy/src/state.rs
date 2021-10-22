use crate::cplane_api::DatabaseInfo;
use anyhow::{anyhow, ensure, Context};
use rustls::{internal::pemfile, NoClientAuth, ProtocolVersion, ServerConfig};
use std::net::SocketAddr;
use std::sync::Arc;

pub type SslConfig = Arc<ServerConfig>;

pub struct ProxyConfig {
    /// main entrypoint for users to connect to
    pub proxy_address: SocketAddr,

    /// http management endpoint. Upon user account creation control plane
    /// will notify us here, so that we can 'unfreeze' user session.
    pub mgmt_address: SocketAddr,

    /// send unauthenticated users to this URI
    pub redirect_uri: String,

    /// control plane address where we would check auth.
    pub auth_endpoint: String,

    pub ssl_config: Option<SslConfig>,
}

pub type ProxyWaiters = crate::waiters::Waiters<Result<DatabaseInfo, String>>;

pub struct ProxyState {
    pub conf: ProxyConfig,
    pub waiters: ProxyWaiters,
}

impl ProxyState {
    pub fn new(conf: ProxyConfig) -> Self {
        Self {
            conf,
            waiters: ProxyWaiters::default(),
        }
    }
}

pub fn configure_ssl(key_path: &str, cert_path: &str) -> anyhow::Result<SslConfig> {
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
