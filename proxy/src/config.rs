use crate::auth;
use anyhow::{bail, ensure, Context};
use std::{str::FromStr, sync::Arc, time::Duration};

pub struct ProxyConfig {
    pub tls_config: Option<TlsConfig>,
    pub auth_backend: auth::BackendType<'static, ()>,
    pub metric_collection: Option<MetricCollectionConfig>,
    pub api_caches: auth::caches::ApiCaches,
}

pub struct MetricCollectionConfig {
    pub endpoint: reqwest::Url,
    pub interval: Duration,
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

/// Helper for cmdline cache options parsing.
pub struct CacheOptions {
    /// Max number of entries.
    pub size: usize,
    /// Entry's time-to-live.
    pub ttl: Duration,
}

impl CacheOptions {
    /// Default options for [`crate::auth::caches::NodeInfoCache`].
    pub const DEFAULT_OPTIONS_NODE_INFO: &str = "size=4000,ttl=5m";

    /// Parse cache options passed via cmdline.
    /// Example: [`Self::DEFAULT_OPTIONS_NODE_INFO`].
    fn parse(options: &str) -> anyhow::Result<Self> {
        let mut size = None;
        let mut ttl = None;

        for option in options.split(',') {
            let (key, value) = option
                .split_once('=')
                .with_context(|| format!("bad key-value pair: {option}"))?;

            match key {
                "size" => size = Some(value.parse()?),
                "ttl" => ttl = Some(humantime::parse_duration(value)?),
                unknown => bail!("unknown key: {unknown}"),
            }
        }

        // TTL doesn't matter if cache is always empty.
        if let Some(0) = size {
            ttl.get_or_insert(Duration::default());
        }

        Ok(Self {
            size: size.context("missing `size`")?,
            ttl: ttl.context("missing `ttl`")?,
        })
    }
}

impl FromStr for CacheOptions {
    type Err = anyhow::Error;

    fn from_str(options: &str) -> Result<Self, Self::Err> {
        let error = || format!("failed to parse cache options '{options}'");
        Self::parse(options).with_context(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cache_options() -> anyhow::Result<()> {
        let CacheOptions { size, ttl } = "size=4096,ttl=5min".parse()?;
        assert_eq!(size, 4096);
        assert_eq!(ttl, Duration::from_secs(5 * 60));

        let CacheOptions { size, ttl } = "ttl=4m,size=2".parse()?;
        assert_eq!(size, 2);
        assert_eq!(ttl, Duration::from_secs(4 * 60));

        let CacheOptions { size, ttl } = "size=0,ttl=1s".parse()?;
        assert_eq!(size, 0);
        assert_eq!(ttl, Duration::from_secs(1));

        let CacheOptions { size, ttl } = "size=0".parse()?;
        assert_eq!(size, 0);
        assert_eq!(ttl, Duration::default());

        Ok(())
    }
}
