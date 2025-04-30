use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, bail};
use itertools::Itertools;
use rustls::crypto::ring::{self, sign};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::sign::CertifiedKey;
use x509_cert::der::{Reader, SliceReader};

use super::{PG_ALPN_PROTOCOL, TlsServerEndPoint};

pub struct TlsConfig {
    // unfortunate split since we cannot change the ALPN on demand.
    // <https://github.com/rustls/rustls/issues/2260>
    pub http_config: Arc<rustls::ServerConfig>,
    pub pg_config: Arc<rustls::ServerConfig>,
    pub common_names: HashSet<String>,
    pub cert_resolver: Arc<CertResolver>,
}

/// Configure TLS for the main endpoint.
pub fn configure_tls(
    key_path: &str,
    cert_path: &str,
    certs_dir: Option<&String>,
    allow_tls_keylogfile: bool,
) -> anyhow::Result<TlsConfig> {
    // add default certificate
    let mut cert_resolver = CertResolver::parse_new(key_path, cert_path)?;

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
                        .add_cert_path(&key_path.to_string_lossy(), &cert_path.to_string_lossy())?;
                }
            }
        }
    }

    let common_names = cert_resolver.get_common_names();

    let cert_resolver = Arc::new(cert_resolver);

    // allow TLS 1.2 to be compatible with older client libraries
    let mut config =
        rustls::ServerConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .context("ring should support TLS1.2 and TLS1.3")?
            .with_no_client_auth()
            .with_cert_resolver(cert_resolver.clone());

    config.alpn_protocols = vec![PG_ALPN_PROTOCOL.to_vec()];

    if allow_tls_keylogfile {
        // KeyLogFile will check for the SSLKEYLOGFILE environment variable.
        config.key_log = Arc::new(rustls::KeyLogFile::new());
    }

    let mut http_config = config.clone();
    let mut pg_config = config;

    http_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    pg_config.alpn_protocols = vec![b"postgresql".to_vec()];

    Ok(TlsConfig {
        http_config: Arc::new(http_config),
        pg_config: Arc::new(pg_config),
        common_names,
        cert_resolver,
    })
}

#[derive(Debug)]
pub struct CertResolver {
    certs: HashMap<String, (Arc<rustls::sign::CertifiedKey>, TlsServerEndPoint)>,
    default: (Arc<rustls::sign::CertifiedKey>, TlsServerEndPoint),
}

impl CertResolver {
    fn parse_new(key_path: &str, cert_path: &str) -> anyhow::Result<Self> {
        let (priv_key, cert_chain) = parse_key_cert(key_path, cert_path)?;
        Self::new(priv_key, cert_chain)
    }

    pub fn new(
        priv_key: PrivateKeyDer<'static>,
        cert_chain: Vec<CertificateDer<'static>>,
    ) -> anyhow::Result<Self> {
        let (common_name, cert, tls_server_end_point) = process_key_cert(priv_key, cert_chain)?;

        let mut certs = HashMap::new();
        let default = (cert.clone(), tls_server_end_point);
        certs.insert(common_name, (cert, tls_server_end_point));
        Ok(Self { certs, default })
    }

    fn add_cert_path(&mut self, key_path: &str, cert_path: &str) -> anyhow::Result<()> {
        let (priv_key, cert_chain) = parse_key_cert(key_path, cert_path)?;
        self.add_cert(priv_key, cert_chain)
    }

    fn add_cert(
        &mut self,
        priv_key: PrivateKeyDer<'static>,
        cert_chain: Vec<CertificateDer<'static>>,
    ) -> anyhow::Result<()> {
        let (common_name, cert, tls_server_end_point) = process_key_cert(priv_key, cert_chain)?;
        self.certs.insert(common_name, (cert, tls_server_end_point));
        Ok(())
    }

    pub fn get_common_names(&self) -> HashSet<String> {
        self.certs.keys().cloned().collect()
    }
}

fn parse_key_cert(
    key_path: &str,
    cert_path: &str,
) -> anyhow::Result<(PrivateKeyDer<'static>, Vec<CertificateDer<'static>>)> {
    let priv_key = {
        let key_bytes = std::fs::read(key_path)
            .with_context(|| format!("Failed to read TLS keys at '{key_path}'"))?;
        rustls_pemfile::private_key(&mut &key_bytes[..])
            .with_context(|| format!("Failed to parse TLS keys at '{key_path}'"))?
            .with_context(|| format!("Failed to parse TLS keys at '{key_path}'"))?
    };

    let cert_chain_bytes = std::fs::read(cert_path)
        .context(format!("Failed to read TLS cert file at '{cert_path}.'"))?;

    let cert_chain = {
        rustls_pemfile::certs(&mut &cert_chain_bytes[..])
            .try_collect()
            .with_context(|| {
                format!(
                    "Failed to read TLS certificate chain from bytes from file at '{cert_path}'."
                )
            })?
    };

    Ok((priv_key, cert_chain))
}

fn process_key_cert(
    priv_key: PrivateKeyDer<'static>,
    cert_chain: Vec<CertificateDer<'static>>,
) -> anyhow::Result<(String, Arc<CertifiedKey>, TlsServerEndPoint)> {
    let key = sign::any_supported_type(&priv_key).context("invalid private key")?;

    let first_cert = &cert_chain[0];
    let tls_server_end_point = TlsServerEndPoint::new(first_cert)?;

    let certificate = SliceReader::new(first_cert)
        .context("Failed to parse cerficiate")?
        .decode::<x509_cert::Certificate>()
        .context("Failed to parse cerficiate")?;

    let common_name = certificate.tbs_certificate.subject.to_string();

    // We need to get the canonical name for this certificate so we can match them against any domain names
    // seen within the proxy codebase.
    //
    // In scram-proxy we use wildcard certificates only, with the database endpoint as the wildcard subdomain, taken from SNI.
    // We need to remove the wildcard prefix for the purposes of certificate selection.
    //
    // auth-broker does not use SNI and instead uses the Neon-Connection-String header.
    // Auth broker has the subdomain `apiauth` we need to remove for the purposes of validating the Neon-Connection-String.
    //
    // Console Redirect proxy does not use any wildcard domains and does not need any certificate selection or conn string
    // validation, so let's we can continue with any common-name
    let common_name = if let Some(s) = common_name.strip_prefix("CN=*.") {
        s.to_string()
    } else if let Some(s) = common_name.strip_prefix("CN=apiauth.") {
        s.to_string()
    } else if let Some(s) = common_name.strip_prefix("CN=") {
        s.to_string()
    } else {
        bail!("Failed to parse common name from certificate")
    };

    let cert = Arc::new(rustls::sign::CertifiedKey::new(cert_chain, key));

    Ok((common_name, cert, tls_server_end_point))
}

impl rustls::server::ResolvesServerCert for CertResolver {
    fn resolve(
        &self,
        client_hello: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        Some(self.resolve(client_hello.server_name()).0)
    }
}

impl CertResolver {
    pub fn resolve(
        &self,
        server_name: Option<&str>,
    ) -> (Arc<rustls::sign::CertifiedKey>, TlsServerEndPoint) {
        // loop here and cut off more and more subdomains until we find
        // a match to get a proper wildcard support. OTOH, we now do not
        // use nested domains, so keep this simple for now.
        //
        // With the current coding foo.com will match *.foo.com and that
        // repeats behavior of the old code.
        if let Some(mut sni_name) = server_name {
            loop {
                if let Some(cert) = self.certs.get(sni_name) {
                    return cert.clone();
                }
                if let Some((_, rest)) = sni_name.split_once('.') {
                    sni_name = rest;
                } else {
                    // The customer has some custom DNS mapping - just return
                    // a default certificate.
                    //
                    // This will error if the customer uses anything stronger
                    // than sslmode=require. That's a choice they can make.
                    return self.default.clone();
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
            self.default.clone()
        }
    }
}
