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
    pub allow_self_signed_compute: bool,
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
    cert_resolver.add_cert(key_path, cert_path, true)?;

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
                    cert_resolver.add_cert(
                        &key_path.to_string_lossy(),
                        &cert_path.to_string_lossy(),
                        false,
                    )?;
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
    default: Option<Arc<rustls::sign::CertifiedKey>>,
}

impl CertResolver {
    fn new() -> Self {
        Self {
            certs: HashMap::new(),
            default: None,
        }
    }

    fn add_cert(
        &mut self,
        key_path: &str,
        cert_path: &str,
        is_default: bool,
    ) -> anyhow::Result<()> {
        let priv_key = {
            let key_bytes = std::fs::read(key_path)
                .context(format!("Failed to read TLS keys at '{key_path}'"))?;
            let mut keys = rustls_pemfile::pkcs8_private_keys(&mut &key_bytes[..])
                .context(format!("Failed to parse TLS keys at '{key_path}'"))?;

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

            // We only use non-wildcard certificates in link proxy so it seems okay to treat them the same as
            // wildcard ones as we don't use SNI there. That treatment only affects certificate selection, so
            // verify-full will still check wildcard match. Old coding here just ignored non-wildcard common names
            // and passed None instead, which blows up number of cases downstream code should handle. Proper coding
            // here should better avoid Option for common_names, and do wildcard-based certificate selection instead
            // of cutting off '*.' parts.
            if common_name.starts_with("CN=*.") {
                common_name.strip_prefix("CN=*.").map(|s| s.to_string())
            } else {
                common_name.strip_prefix("CN=").map(|s| s.to_string())
            }
        }
        .context(format!(
            "Failed to parse common name from certificate at '{cert_path}'."
        ))?;

        let cert = Arc::new(rustls::sign::CertifiedKey::new(cert_chain, key));

        if is_default {
            self.default = Some(cert.clone());
        }

        self.certs.insert(common_name, cert);

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
            // No SNI, use the default certificate, otherwise we can't get to
            // options parameter which can be used to set endpoint name too.
            // That means that non-SNI flow will not work for CNAME domains in
            // verify-full mode.
            //
            // If that will be a problem we can:
            //
            // a) Instead of multi-cert approach use single cert with extra
            //    domains listed in Subject Alternative Name (SAN).
            // b) Deploy separate proxy instances for extra domains.
            self.default.as_ref().cloned()
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
    /// Default options for [`crate::console::provider::NodeInfoCache`].
    pub const DEFAULT_OPTIONS_NODE_INFO: &str = "size=4000,ttl=4m";

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
