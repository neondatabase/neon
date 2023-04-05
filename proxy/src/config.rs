use crate::auth;
use anyhow::{bail, ensure, Context, Ok};
use rustls::sign;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

pub struct ProxyConfig {
    pub tls_config: Option<TlsConfig>,
    pub auth_backend: auth::BackendType<'static, ()>,
    pub metric_collection: Option<MetricCollectionConfig>,
}

#[derive(Debug)]
pub struct MetricCollectionConfig {
    pub endpoint: reqwest::Url,
    pub interval: Duration,
}

pub struct TlsConfig {
    pub config: Arc<rustls::ServerConfig>,
    pub common_names: Option<HashSet<String>>,
}

impl TlsConfig {
    pub fn to_server_config(&self) -> Arc<rustls::ServerConfig> {
        self.config.clone()
    }
}

/// Configure TLS for the main endpoint.
pub fn configure_tls(
    key_path: &str,
    cert_path: &str,
    certs_dir: Option<&String>,
) -> anyhow::Result<TlsConfig> {
    let mut cert_resolver = CertResolver::new();

    // add default certificate
    cert_resolver.add_cert(key_path, cert_path)?;

    // add extra certificates
    if let Some(certs_dir) = certs_dir {
        for entry in std::fs::read_dir(certs_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                // file names aligned with default cert-manager names
                let key_path = path.join("tls.key");
                let cert_path = path.join("tls.crt");
                if key_path.exists() && cert_path.exists() {
                    cert_resolver
                        .add_cert(&key_path.to_string_lossy(), &cert_path.to_string_lossy())?;
                }
            }
        }
    }

    let common_names = cert_resolver.get_common_names();

    let config = rustls::ServerConfig::builder()
        .with_safe_default_cipher_suites()
        .with_safe_default_kx_groups()
        // allow TLS 1.2 to be compatible with older client libraries
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(cert_resolver))
        .into();

    Ok(TlsConfig {
        config,
        common_names: Some(common_names),
    })
}

struct CertResolver {
    certs: HashMap<String, Arc<rustls::sign::CertifiedKey>>,
}

impl CertResolver {
    fn new() -> Self {
        Self {
            certs: HashMap::new(),
        }
    }

    fn add_cert(&mut self, key_path: &str, cert_path: &str) -> anyhow::Result<()> {
        let priv_key = {
            let key_bytes = std::fs::read(key_path).context("TLS key file")?;
            let mut keys = rustls_pemfile::pkcs8_private_keys(&mut &key_bytes[..])
                .context(format!("Failed to read TLS keys at '{key_path}'"))?;

            ensure!(keys.len() == 1, "keys.len() = {} (should be 1)", keys.len());
            keys.pop().map(rustls::PrivateKey).unwrap()
        };

        let key = sign::any_supported_type(&priv_key).context("invalid private key")?;

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

        let common_name = {
            let pem = x509_parser::pem::parse_x509_pem(&cert_chain_bytes)
                .context(format!(
                    "Failed to parse PEM object from bytes from file at '{cert_path}'."
                ))?
                .1;
            let common_name = pem.parse_x509()?.subject().to_string();
            common_name.strip_prefix("CN=*.").map(|s| s.to_string())
        }
        .context(format!(
            "Failed to parse common name from certificate at '{cert_path}'."
        ))?;

        self.certs.insert(
            common_name,
            Arc::new(rustls::sign::CertifiedKey::new(cert_chain, key)),
        );

        Ok(())
    }

    fn get_common_names(&self) -> HashSet<String> {
        self.certs.keys().map(|s| s.to_string()).collect()
    }
}

impl rustls::server::ResolvesServerCert for CertResolver {
    fn resolve(
        &self,
        _client_hello: rustls::server::ClientHello,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        // loop here and cut off more and more subdomains until we find
        // a match to get a proper wildcard support. OTOH, we now do not
        // use nested domains, so keep this simple for now.
        //
        // With the current coding foo.com will match *.foo.com and that
        // repeats behavior of the old code.
        if let Some(mut sni_name) = _client_hello.server_name() {
            loop {
                if let Some(cert) = self.certs.get(sni_name) {
                    return Some(cert.clone());
                }
                if let Some((_, rest)) = sni_name.split_once('.') {
                    sni_name = rest;
                } else {
                    return None;
                }
            }
        } else {
            None
        }
    }
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
