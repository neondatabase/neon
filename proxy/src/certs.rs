use rustls::{
    server::{ClientHello, ResolvesServerCert},
    sign::CertifiedKey,
};
use std::{io, sync::Arc};

pub mod config {
    use super::*;
    use serde::Deserialize;
    use std::path::Path;

    /// Collection of TLS-related configurations of virtual proxy servers.
    #[derive(Debug, Default, Clone, Deserialize)]
    #[serde(transparent)]
    pub struct TlsServers(Vec<TlsServer>);

    impl TlsServers {
        /// Load [`Self`] config from a file.
        pub fn from_config_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
            let config = serde_dhall::from_file(path).parse()?;
            Ok(config)
        }
    }

    /// This lets us merge multiple configs into one (semigroup).
    impl FromIterator<Self> for TlsServers {
        fn from_iter<T: IntoIterator<Item = Self>>(iter: T) -> Self {
            Self(iter.into_iter().flat_map(|xs| xs.0).collect())
        }
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(transparent)]
    pub struct TlsCert(
        /// The wrapped rustls certificate.
        #[serde(deserialize_with = "deserialize_certs")]
        pub Vec<rustls::Certificate>,
    );

    fn deserialize_certs<'de, D>(des: D) -> Result<Vec<rustls::Certificate>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let text = String::deserialize(des)?;
        parse_certs(&mut text.as_bytes()).map_err(serde::de::Error::custom)
    }

    #[derive(Debug, Clone, Deserialize)]
    #[serde(transparent)]
    pub struct TlsKey(
        /// The wrapped rustls private key.
        #[serde(deserialize_with = "deserialize_key")]
        pub rustls::PrivateKey,
    );

    fn deserialize_key<'de, D>(des: D) -> Result<rustls::PrivateKey, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let text = String::deserialize(des)?;
        parse_key(&mut text.as_bytes()).map_err(serde::de::Error::custom)
    }

    /// TODO: explain.
    #[derive(Debug, Clone, Deserialize)]
    pub struct TlsServer {
        pub certificate: TlsCert,
        pub private_key: TlsKey,
    }
}

fn parse_certs(buf: &mut impl io::BufRead) -> io::Result<Vec<rustls::Certificate>> {
    let chain = rustls_pemfile::certs(buf)?
        .into_iter()
        .map(rustls::Certificate)
        .collect();

    Ok(chain)
}

fn parse_key(buf: &mut impl io::BufRead) -> io::Result<rustls::PrivateKey> {
    let mut keys = rustls_pemfile::pkcs8_private_keys(buf)?;

    // We expect to see only 1 key.
    if keys.len() != 1 {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "there should be exactly one TLS key in buffer",
        ));
    }

    Ok(rustls::PrivateKey(keys.pop().unwrap()))
}

pub struct CertResolver {
    resolver: rustls::server::ResolvesServerCertUsingSni,
}

impl CertResolver {
    pub fn new() -> Self {
        todo!("CertResolver ctor")
    }
}

impl ResolvesServerCert for CertResolver {
    fn resolve(&self, message: ClientHello) -> Option<Arc<CertifiedKey>> {
        self.resolver.resolve(message)
    }
}
