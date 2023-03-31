use rustls::{
    server::{ClientHello, ResolvesServerCert},
    sign::CertifiedKey,
};
use std::sync::Arc;

pub mod config {
    use serde::Deserialize;
    use std::path::Path;

    /// TODO: explain.
    #[derive(Debug, Default, Clone, Deserialize)]
    pub struct TlsServers(Vec<TlsServer>);

    impl TlsServers {
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

    /// TODO: explain.
    #[derive(Debug, Clone, Deserialize)]
    pub struct TlsServer {
        server_name: Box<str>,
        certificate: Box<str>,
        private_key: Box<str>,
    }
}

pub struct CertResolver {
    resolver: rustls::server::ResolvesServerCertUsingSni,
}

impl CertResolver {
    pub fn new() -> Self {
        todo!()
    }
}

impl ResolvesServerCert for CertResolver {
    fn resolve(&self, message: ClientHello) -> Option<Arc<CertifiedKey>> {
        self.resolver.resolve(message)
    }
}
