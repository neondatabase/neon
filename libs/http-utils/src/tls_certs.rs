use std::{sync::Arc, time::Duration};

use anyhow::Context;
use arc_swap::ArcSwap;
use camino::Utf8Path;
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    server::{ClientHello, ResolvesServerCert},
    sign::CertifiedKey,
};

pub async fn load_cert_chain(filename: &Utf8Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let cert_data = tokio::fs::read(filename)
        .await
        .context(format!("failed reading certificate file {filename:?}"))?;
    let mut reader = std::io::Cursor::new(&cert_data);

    let cert_chain = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .context(format!("failed parsing certificate from file {filename:?}"))?;

    Ok(cert_chain)
}

pub async fn load_private_key(filename: &Utf8Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    let key_data = tokio::fs::read(filename)
        .await
        .context(format!("failed reading private key file {filename:?}"))?;
    let mut reader = std::io::Cursor::new(&key_data);

    let key = rustls_pemfile::private_key(&mut reader)
        .context(format!("failed parsing private key from file {filename:?}"))?;

    key.ok_or(anyhow::anyhow!(
        "no private key found in {}",
        filename.as_str(),
    ))
}

pub async fn load_certified_key(
    key_filename: &Utf8Path,
    cert_filename: &Utf8Path,
) -> anyhow::Result<CertifiedKey> {
    let cert_chain = load_cert_chain(cert_filename).await?;
    let key = load_private_key(key_filename).await?;

    let key = rustls::crypto::ring::default_provider()
        .key_provider
        .load_private_key(key)?;

    let certified_key = CertifiedKey::new(cert_chain, key);
    certified_key.keys_match()?;
    Ok(certified_key)
}

/// Implementation of [`rustls::ResolvesServerCert`] which reloads certificates from
/// the disk periodically.
#[derive(Debug)]
pub struct ReloadingCertificateResolver {
    certified_key: ArcSwap<CertifiedKey>,
}

impl ReloadingCertificateResolver {
    /// Creates a new Resolver by loading certificate and private key from FS and
    /// creating tokio::task to reload them with provided reload_period.
    pub async fn new(
        key_filename: &Utf8Path,
        cert_filename: &Utf8Path,
        reload_period: Duration,
    ) -> anyhow::Result<Arc<Self>> {
        let this = Arc::new(Self {
            certified_key: ArcSwap::from_pointee(
                load_certified_key(key_filename, cert_filename).await?,
            ),
        });

        tokio::spawn({
            let weak_this = Arc::downgrade(&this);
            let key_filename = key_filename.to_owned();
            let cert_filename = cert_filename.to_owned();
            async move {
                let start = tokio::time::Instant::now() + reload_period;
                let mut interval = tokio::time::interval_at(start, reload_period);
                let mut last_reload_failed = false;
                loop {
                    interval.tick().await;
                    let this = match weak_this.upgrade() {
                        Some(this) => this,
                        None => break, // Resolver has been destroyed, exit.
                    };
                    match load_certified_key(&key_filename, &cert_filename).await {
                        Ok(new_certified_key) => {
                            if new_certified_key.cert == this.certified_key.load().cert {
                                tracing::debug!("Certificate has not changed since last reloading");
                            } else {
                                tracing::info!("Certificate has been reloaded");
                                this.certified_key.store(Arc::new(new_certified_key));
                            }
                            last_reload_failed = false;
                        }
                        Err(err) => {
                            // Note: Reloading certs may fail if it conflicts with the script updating
                            // the files at the same time. Warn only if the error is persistent.
                            if last_reload_failed {
                                tracing::warn!("Error reloading certificate: {err:#}");
                            } else {
                                tracing::info!("Error reloading certificate: {err:#}");
                            }
                            last_reload_failed = true;
                        }
                    }
                }
            }
        });

        Ok(this)
    }
}

impl ResolvesServerCert for ReloadingCertificateResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.certified_key.load_full())
    }
}
