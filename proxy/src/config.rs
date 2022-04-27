use anyhow::{bail, ensure, Context};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

pub type TlsConfig = Arc<rustls::ServerConfig>;

#[non_exhaustive]
pub enum ClientAuthMethod {
    Password,
    Link,

    /// Use password auth only if username ends with "@zenith"
    Mixed,
}

pub enum RouterConfig {
    Static { host: String, port: u16 },
    Dynamic(ClientAuthMethod),
}

impl FromStr for ClientAuthMethod {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        use ClientAuthMethod::*;
        match s {
            "password" => Ok(Password),
            "link" => Ok(Link),
            "mixed" => Ok(Mixed),
            _ => bail!("Invalid option for router: `{}`", s),
        }
    }
}

pub struct ProxyConfig {
    /// main entrypoint for users to connect to
    pub proxy_address: SocketAddr,

    /// method of assigning compute nodes
    pub router_config: RouterConfig,

    /// internally used for status and prometheus metrics
    pub http_address: SocketAddr,

    /// management endpoint. Upon user account creation control plane
    /// will notify us here, so that we can 'unfreeze' user session.
    /// TODO It uses postgres protocol over TCP but should be migrated to http.
    pub mgmt_address: SocketAddr,

    /// send unauthenticated users to this URI
    pub redirect_uri: String,

    /// control plane address where we would check auth.
    pub auth_endpoint: reqwest::Url,

    pub tls_config: Option<TlsConfig>,
}

pub fn configure_ssl(key_path: &str, cert_path: &str) -> anyhow::Result<TlsConfig> {
    let key = {
        let key_bytes = std::fs::read(key_path).context("SSL key file")?;
        let mut keys = rustls_pemfile::pkcs8_private_keys(&mut &key_bytes[..])
            .context("couldn't read TLS keys")?;

        ensure!(keys.len() == 1, "keys.len() = {} (should be 1)", keys.len());
        keys.pop().map(rustls::PrivateKey).unwrap()
    };

    let cert_chain = {
        let cert_chain_bytes = std::fs::read(cert_path).context("SSL cert file")?;
        rustls_pemfile::certs(&mut &cert_chain_bytes[..])
            .context("couldn't read TLS certificate chain")?
            .into_iter()
            .map(rustls::Certificate)
            .collect()
    };

    let config = rustls::ServerConfig::builder()
        .with_safe_default_cipher_suites()
        .with_safe_default_kx_groups()
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)?;

    Ok(config.into())
}
