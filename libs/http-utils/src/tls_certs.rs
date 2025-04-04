use std::{sync::Arc, time::Duration};

use anyhow::Context;
use arc_swap::ArcSwap;
use camino::Utf8Path;
use metrics::{IntCounterVec, UIntGaugeVec, register_int_counter_vec, register_uint_gauge_vec};
use once_cell::sync::Lazy;
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, UnixTime},
    server::{ClientHello, ResolvesServerCert},
    sign::CertifiedKey,
};
use x509_cert::der::Reader;

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

/// rustls's CertifiedKey with extra parsed fields used for metrics.
struct ParsedCertifiedKey {
    certified_key: CertifiedKey,
    expiration_time: UnixTime,
}

/// Parse expiration time from an X509 certificate.
fn parse_expiration_time(cert: &CertificateDer<'_>) -> anyhow::Result<UnixTime> {
    let parsed_cert = x509_cert::der::SliceReader::new(cert)
        .context("Failed to parse cerficiate")?
        .decode::<x509_cert::Certificate>()
        .context("Failed to parse cerficiate")?;

    Ok(UnixTime::since_unix_epoch(
        parsed_cert
            .tbs_certificate
            .validity
            .not_after
            .to_unix_duration(),
    ))
}

async fn load_and_parse_certified_key(
    key_filename: &Utf8Path,
    cert_filename: &Utf8Path,
) -> anyhow::Result<ParsedCertifiedKey> {
    let certified_key = load_certified_key(key_filename, cert_filename).await?;
    let expiration_time = parse_expiration_time(certified_key.end_entity_cert()?)?;
    Ok(ParsedCertifiedKey {
        certified_key,
        expiration_time,
    })
}

static CERT_EXPIRATION_TIME: Lazy<UIntGaugeVec> = Lazy::new(|| {
    register_uint_gauge_vec!(
        "tls_certs_expiration_time_seconds",
        "Expiration time of the loaded certificate since unix epoch in seconds",
        &["resolver_name"]
    )
    .expect("failed to define a metric")
});

static CERT_RELOAD_STARTED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "tls_certs_reload_started_total",
        "Number of certificate reload loop iterations started",
        &["resolver_name"]
    )
    .expect("failed to define a metric")
});

static CERT_RELOAD_UPDATED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "tls_certs_reload_updated_total",
        "Number of times the certificate was updated to the new one",
        &["resolver_name"]
    )
    .expect("failed to define a metric")
});

static CERT_RELOAD_FAILED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "tls_certs_reload_failed_total",
        "Number of times the certificate reload failed",
        &["resolver_name"]
    )
    .expect("failed to define a metric")
});

/// Implementation of [`rustls::server::ResolvesServerCert`] which reloads certificates from
/// the disk periodically.
#[derive(Debug)]
pub struct ReloadingCertificateResolver {
    certified_key: ArcSwap<CertifiedKey>,
}

impl ReloadingCertificateResolver {
    /// Creates a new Resolver by loading certificate and private key from FS and
    /// creating tokio::task to reload them with provided reload_period.
    /// resolver_name is used as metric's label.
    pub async fn new(
        resolver_name: &str,
        key_filename: &Utf8Path,
        cert_filename: &Utf8Path,
        reload_period: Duration,
    ) -> anyhow::Result<Arc<Self>> {
        // Create metrics for current resolver.
        let cert_expiration_time = CERT_EXPIRATION_TIME.with_label_values(&[resolver_name]);
        let cert_reload_started_counter =
            CERT_RELOAD_STARTED_COUNTER.with_label_values(&[resolver_name]);
        let cert_reload_updated_counter =
            CERT_RELOAD_UPDATED_COUNTER.with_label_values(&[resolver_name]);
        let cert_reload_failed_counter =
            CERT_RELOAD_FAILED_COUNTER.with_label_values(&[resolver_name]);

        let parsed_key = load_and_parse_certified_key(key_filename, cert_filename).await?;

        let this = Arc::new(Self {
            certified_key: ArcSwap::from_pointee(parsed_key.certified_key),
        });
        cert_expiration_time.set(parsed_key.expiration_time.as_secs());

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
                    cert_reload_started_counter.inc();

                    match load_and_parse_certified_key(&key_filename, &cert_filename).await {
                        Ok(parsed_key) => {
                            if parsed_key.certified_key.cert == this.certified_key.load().cert {
                                tracing::debug!("Certificate has not changed since last reloading");
                            } else {
                                tracing::info!("Certificate has been reloaded");
                                this.certified_key.store(Arc::new(parsed_key.certified_key));
                                cert_expiration_time.set(parsed_key.expiration_time.as_secs());
                                cert_reload_updated_counter.inc();
                            }
                            last_reload_failed = false;
                        }
                        Err(err) => {
                            cert_reload_failed_counter.inc();
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
