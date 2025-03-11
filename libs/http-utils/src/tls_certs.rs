use camino::Utf8Path;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub fn load_cert_chain(filename: &Utf8Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(filename)?;
    let mut reader = std::io::BufReader::new(file);

    Ok(rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?)
}

pub fn load_private_key(filename: &Utf8Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(filename)?;
    let mut reader = std::io::BufReader::new(file);

    let key = rustls_pemfile::private_key(&mut reader)?;

    key.ok_or(anyhow::anyhow!(
        "no private key found in {}",
        filename.as_str(),
    ))
}

// pub fn create_tls_acceptor(key_filename: &Utf8Path)
