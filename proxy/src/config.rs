use crate::auth;
use anyhow::{ensure, Context};
use std::sync::Arc;

pub struct ProxyConfig {
    pub tls_config: Option<TlsConfig>,
    pub auth_backend: auth::BackendType<'static, ()>,
    pub metric_collection: Option<MetricCollectionConfig>,
    pub api_caches: auth::caches::ApiCaches,
}

pub struct MetricCollectionConfig {
    pub endpoint: reqwest::Url,
    pub interval: std::time::Duration,
}

pub struct TlsConfig {
    pub config: Arc<rustls::ServerConfig>,
    pub common_name: Option<String>,
}

impl TlsConfig {
    pub fn to_server_config(&self) -> Arc<rustls::ServerConfig> {
        self.config.clone()
    }
}

/// Configure TLS for the main endpoint.
pub fn configure_tls(key_path: &str, cert_path: &str) -> anyhow::Result<TlsConfig> {
    let key = {
        let key_bytes = std::fs::read(key_path).context("TLS key file")?;
        let mut keys = rustls_pemfile::pkcs8_private_keys(&mut &key_bytes[..])
            .context(format!("Failed to read TLS keys at '{key_path}'"))?;

        ensure!(keys.len() == 1, "keys.len() = {} (should be 1)", keys.len());
        keys.pop().map(rustls::PrivateKey).unwrap()
    };

    let cert_chain_bytes = std::fs::read(cert_path)
        .context(format!("Failed to read TLS cert file at '{cert_path}.'"))?;
    let cert_chain = {
        rustls_pemfile::certs(&mut &cert_chain_bytes[..])
            .context(format!(
                "Failed to read TLS certificate chain from bytes from file at '{cert_path}'."
            ))?
            .into_iter()
            .map(rustls::Certificate)
            .collect()
    };

    let config = rustls::ServerConfig::builder()
        .with_safe_default_cipher_suites()
        .with_safe_default_kx_groups()
        // allow TLS 1.2 to be compatible with older client libraries
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)?
        .into();

    // determine common name from tls-cert (-c server.crt param).
    // used in asserting project name formatting invariant.
    let common_name = {
        let pem = x509_parser::pem::parse_x509_pem(&cert_chain_bytes)
            .context(format!(
                "Failed to parse PEM object from bytes from file at '{cert_path}'."
            ))?
            .1;
        let common_name = pem.parse_x509()?.subject().to_string();
        common_name.strip_prefix("CN=*.").map(|s| s.to_string())
    };

    Ok(TlsConfig {
        config,
        common_name,
    })
}
