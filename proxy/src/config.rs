use crate::url::ApiUrl;
use anyhow::{bail, ensure, Context};
use std::{str::FromStr, sync::Arc};

#[derive(Debug)]
pub enum AuthBackendType {
    /// Legacy Cloud API (V1).
    LegacyConsole,
    /// Authentication via a web browser.
    Link,
    /// Current Cloud API (V2).
    Console,
    /// Local mock of Cloud API (V2).
    Postgres,
}

impl FromStr for AuthBackendType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        use AuthBackendType::*;
        Ok(match s {
            "legacy" => LegacyConsole,
            "console" => Console,
            "postgres" => Postgres,
            "link" => Link,
            _ => bail!("Invalid option `{s}` for auth method"),
        })
    }
}

pub struct ProxyConfig {
    pub tls_config: Option<TlsConfig>,
    pub auth_backend: AuthBackendType,
    pub auth_endpoint: ApiUrl,
    pub auth_link_uri: ApiUrl,
}

#[derive(Clone)]
pub struct TlsConfig {
    pub tls_config: Arc<rustls::ServerConfig>,
    pub common_name: Option<&'static str>,
}

impl TlsConfig {
    pub fn get_tls_config(self) -> Arc<rustls::ServerConfig> {
        self.tls_config
    }
}

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
        // allow TLS 1.2 to be compatible with older client libraries
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])?
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)?;

    // determine common name from tls-cert (-c server.crt param).
    // used in asserting project name formatting invariant.
    let common_name = {
        let file = std::fs::File::open(cert_path)?;
        let almost_common_name = x509_parser::pem::Pem::read(std::io::BufReader::new(file))?
            .0
            .parse_x509()?
            .tbs_certificate
            .subject
            .to_string();
        let expected_prefix = "CN=*.";
        let option_common_name = almost_common_name.strip_prefix(expected_prefix);
        let common_name = match option_common_name {
            Some(common_name) => {
                let leaked_str: &'static str =
                    Box::<str>::leak(common_name.to_string().into_boxed_str());
                Some(leaked_str)
            }
            None => None,
        };
        common_name
    };

    Ok(TlsConfig {
        tls_config: config.into(),
        common_name,
    })
}
