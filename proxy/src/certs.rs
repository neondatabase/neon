use rustls::{
    server::{ClientHello, ResolvesServerCert},
    sign::CertifiedKey,
};
use std::{collections::BTreeMap, io, ops::Bound, sync::Arc};

/// App-level configuration structs for TLS certificates.
pub mod config {
    use super::*;
    use serde::{de, Deserialize};
    use std::path::Path;

    /// Collection of TLS-related configurations of virtual proxy servers.
    #[derive(Debug, Default, Clone, Deserialize)]
    #[serde(transparent)]
    pub struct TlsServers(pub Vec<TlsServer>);

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

    /// Helps deserialize certificate chain from a string.
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
        parse_certs(&mut text.as_bytes()).map_err(de::Error::custom)
    }

    /// Helps deserialize private key from a string.
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
        parse_key(&mut text.as_bytes()).map_err(de::Error::custom)
    }

    /// Represents TLS config of a single virtual proxy server.
    #[derive(Debug, Clone, Deserialize)]
    pub struct TlsServer {
        /// Proxy server's certificate chain.
        pub certificate: TlsCert,
        /// Proxy server's private key.
        pub private_key: TlsKey,
    }

    impl TlsServer {
        pub fn into_certified_key(
            self,
        ) -> Result<rustls::sign::CertifiedKey, rustls::sign::SignError> {
            Ok(rustls::sign::CertifiedKey::new(
                self.certificate.0,
                rustls::sign::any_supported_type(&self.private_key.0)?,
            ))
        }
    }
}

/// Parse TLS certificate chain from a byte buffer.
fn parse_certs(buf: &mut impl io::BufRead) -> io::Result<Vec<rustls::Certificate>> {
    let chain = rustls_pemfile::certs(buf)?
        .into_iter()
        .map(rustls::Certificate)
        .collect();

    Ok(chain)
}

/// Parse exactly one TLS private key from a byte buffer.
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

/// Extract domain names from a certificate: first CN, then SANs.
/// Further reading: <https://www.rfc-editor.org/rfc/rfc4985>.
fn certificate_names(cert: &rustls::Certificate) -> anyhow::Result<Vec<String>> {
    use x509_parser::{extensions::GeneralName, x509::AttributeTypeAndValue};

    let get_dns_name = |gn: &GeneralName| match gn {
        GeneralName::DNSName(name) => Some(name.to_string()),
        _other => None,
    };

    let get_common_name = |attr: &AttributeTypeAndValue| {
        // There really shouldn't be anything but string here.
        attr.attr_value().as_string().expect("bad CN attribute")
    };

    let (rest, cert) = x509_parser::parse_x509_certificate(cert.0.as_ref())?;
    anyhow::ensure!(rest.is_empty(), "excessive bytes in DER certificate");

    // Extract CN, Common Name.
    let mut names: Vec<String> = cert
        .subject()
        .iter_common_name()
        .map(get_common_name)
        .collect();

    // Now append SANs, Subject Alternative Names, if any.
    if let Some(extension) = cert.subject_alternative_name()? {
        let alt_names = &extension.value.general_names;
        names.extend(alt_names.iter().filter_map(get_dns_name));
    }

    Ok(names)
}

#[derive(Debug)]
struct GlobMapBuilder<V> {
    builder: globset::GlobSetBuilder,
    values: BTreeMap<usize, V>,
}

impl<V> GlobMapBuilder<V> {
    fn new() -> Self {
        Self {
            builder: globset::GlobSetBuilder::new(),
            values: Default::default(),
        }
    }

    fn add(&mut self, globs: impl IntoIterator<Item = globset::Glob>, value: V) -> &mut Self {
        let mut cnt = 0;
        for glob in globs {
            self.builder.add(glob);
            cnt += 1;
        }

        if cnt > 0 {
            let offset = self.values.last_key_value().map(|(k, _)| *k).unwrap_or(0);
            self.values.insert(offset + cnt, value);
        }

        self
    }

    fn build(self) -> Result<GlobMap<V>, globset::Error> {
        Ok(GlobMap {
            set: self.builder.build()?,
            values: self.values,
        })
    }
}

/// Maps a set of matching globs to an arbitrary value.
/// See the tests below in case this description doesn't help.
#[derive(Debug)]
struct GlobMap<V> {
    /// An ordered set of all loaded globs.
    set: globset::GlobSet,
    /// Store single value per range of globs.
    values: BTreeMap<usize, V>,
}

impl<V> GlobMap<V> {
    fn query(&self, text: &str) -> Vec<&V> {
        let indices = self.set.matches(text);
        let mut res = Vec::with_capacity(indices.len());

        for i in indices {
            let mut range = self.values.range((Bound::Excluded(i), Bound::Unbounded));
            let (_, value) = range.next().expect("invariant: entry must exist");
            res.push(value);
        }

        res
    }
}

struct CertResolverEntry {
    raw: Arc<rustls::sign::CertifiedKey>,
    names: Vec<String>,
}

pub struct CertResolver {
    storage: GlobMap<CertResolverEntry>,
}

impl CertResolver {
    pub fn new(config: config::TlsServers) -> anyhow::Result<Self> {
        let mut builder = GlobMapBuilder::new();
        for server in config.0 {
            let Some(cert) = server.certificate.0.first() else {
                tracing::warn!("found empty certificate, skipping");
                continue;
            };

            let names = certificate_names(cert)?;
            let globs = names
                .iter()
                .map(|s| globset::Glob::new(s))
                .collect::<Result<Vec<_>, _>>()?;

            let entry = CertResolverEntry {
                raw: Arc::new(server.into_certified_key()?),
                names,
            };

            builder.add(globs, entry);
        }

        Ok(Self {
            storage: builder.build()?,
        })
    }
}

impl ResolvesServerCert for CertResolver {
    fn resolve(&self, message: ClientHello) -> Option<Arc<CertifiedKey>> {
        let name = message.server_name()?;
        let certs = self.storage.query(name);
        let first = certs.first()?;

        // TODO: warn if there's more than one match!
        Some(first.raw.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use globset::Glob;

    #[test]
    fn check_glob_map_basic() -> anyhow::Result<()> {
        let mut builder = GlobMapBuilder::new();
        builder
            .add([Glob::new("*.localhost")?], 0)
            .add([Glob::new("bar.localhost")?], 1)
            .add([Glob::new("*.foo.localhost")?], 2);

        let map = builder.build()?;

        assert!(map.query("random").is_empty());
        assert!(map.query("localhost").is_empty());
        assert_eq!(map.query("foo.localhost"), [&0]);
        assert_eq!(map.query("bar.localhost"), [&0, &1]);
        assert_eq!(map.query("project.foo.localhost"), [&0, &2]);

        Ok(())
    }

    #[test]
    fn check_glob_map() -> anyhow::Result<()> {
        let mut builder = GlobMapBuilder::new();
        builder
            .add(
                [
                    Glob::new("*.neon.tech")?,
                    Glob::new("*.neon.internal.tech")?,
                ],
                "neon",
            )
            .add([Glob::new("*.localhost")?], "mock");

        let map = builder.build()?;

        assert!(map.query("random").is_empty());
        assert!(map.query("localhost").is_empty());
        assert_eq!(map.query("ep-1.neon.tech"), [&"neon"]);
        assert_eq!(map.query("ep-1.neon.internal.tech"), [&"neon"]);
        assert_eq!(map.query("ep-1.foo.localhost"), [&"mock"]);

        Ok(())
    }
}
