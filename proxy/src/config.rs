use anyhow::{ensure, Context};
use std::{str::FromStr, sync::Arc};

#[non_exhaustive]
pub enum AuthBackendType {
    LegacyConsole,
    Console,
    Postgres,
    Link,
}

impl FromStr for AuthBackendType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        println!("ClientAuthMethod::from_str: '{}'", s);
        use AuthBackendType::*;
        match s {
            "legacy" => Ok(LegacyConsole),
            "console" => Ok(Console),
            "postgres" => Ok(Postgres),
            "link" => Ok(Link),
            _ => Err(anyhow::anyhow!("Invlid option for auth method")),
        }
    }
}

pub struct ProxyConfig {
    /// TLS configuration for the proxy.
    pub tls_config: Option<TlsConfig>,

    pub auth_backend: AuthBackendType,

    pub auth_endpoint: reqwest::Url,

    pub auth_link_uri: reqwest::Url,
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
