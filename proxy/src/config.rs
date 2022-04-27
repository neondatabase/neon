use crate::cloud;
use anyhow::{bail, ensure, Context};
use std::sync::Arc;

pub struct ProxyConfig {
    /// Unauthenticated users will be redirected to this URL.
    pub redirect_uri: reqwest::Url,

    /// Cloud API endpoint for user authentication.
    pub cloud_endpoint: CloudApi,

    /// TLS configuration for the proxy.
    pub tls_config: Option<TlsConfig>,
}

/// Cloud API configuration.
pub enum CloudApi {
    /// We'll drop this one when [`CloudApi::V2`] is stable.
    V1(crate::cloud::Legacy),
    /// The new version of the cloud API.
    V2(crate::cloud::BoxedApi),
}

impl CloudApi {
    /// Configure Cloud API provider.
    pub fn new(version: &str, url: reqwest::Url) -> anyhow::Result<Self> {
        Ok(match version {
            "v1" => Self::V1(cloud::Legacy::new(url)),
            "v2" => Self::V2(cloud::new(url)?),
            _ => bail!("unknown cloud API version: {}", version),
        })
    }
}

pub type TlsConfig = Arc<rustls::ServerConfig>;

/// Configure TLS for the main endpoint.
pub fn configure_tls(key_path: &str, cert_path: &str) -> anyhow::Result<TlsConfig> {
    let key = {
        let key_bytes = std::fs::read(key_path).context("TLS key file")?;
        let mut keys = rustls_pemfile::pkcs8_private_keys(&mut &key_bytes[..])
            .context("couldn't read TLS keys")?;

        ensure!(keys.len() == 1, "keys.len() = {} (should be 1)", keys.len());
        keys.pop().map(rustls::PrivateKey).unwrap()
    };

    let cert_chain = {
        let cert_chain_bytes = std::fs::read(cert_path).context("TLS cert file")?;
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
