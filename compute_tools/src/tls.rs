use std::{io::Write, os::unix::fs::OpenOptionsExt, path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use compute_api::responses::TlsConfig;
use ring::digest;
use spki::ObjectIdentifier;
use spki::der::{Decode, PemReader};
use x509_cert::Certificate;

#[derive(Clone, Copy)]
pub struct CertDigest(digest::Digest);

pub async fn watch_cert_for_changes(cert_path: String) -> tokio::sync::watch::Receiver<CertDigest> {
    let mut digest = compute_digest(&cert_path).await;
    let (tx, rx) = tokio::sync::watch::channel(digest);
    tokio::spawn(async move {
        while !tx.is_closed() {
            let new_digest = compute_digest(&cert_path).await;
            if digest.0.as_ref() != new_digest.0.as_ref() {
                digest = new_digest;
                _ = tx.send(digest);
            }

            tokio::time::sleep(Duration::from_secs(60)).await
        }
    });
    rx
}

async fn compute_digest(cert_path: &str) -> CertDigest {
    loop {
        match try_compute_digest(cert_path).await {
            Ok(d) => break d,
            Err(e) => {
                tracing::error!("could not read cert file {e:?}");
                tokio::time::sleep(Duration::from_secs(1)).await
            }
        }
    }
}

async fn try_compute_digest(cert_path: &str) -> Result<CertDigest> {
    let data = tokio::fs::read(cert_path).await?;
    // sha256 is extremely collision resistent. can safely assume the digest to be unique
    Ok(CertDigest(digest::digest(&digest::SHA256, &data)))
}

pub const SERVER_CRT: &str = "server.crt";
pub const SERVER_KEY: &str = "server.key";

pub fn update_key_path_blocking(pg_data: &Path, tls_config: &TlsConfig) {
    loop {
        match try_update_key_path_blocking(pg_data, tls_config) {
            Ok(()) => break,
            Err(e) => {
                tracing::error!("could not create key file {e:?}");
                std::thread::sleep(Duration::from_secs(1))
            }
        }
    }
}

// Postgres requires the keypath be "secure". This means
// 1. Owned by the postgres user.
// 2. Have permission 600.
fn try_update_key_path_blocking(pg_data: &Path, tls_config: &TlsConfig) -> Result<()> {
    let key = std::fs::read_to_string(&tls_config.key_path)?;
    let crt = std::fs::read_to_string(&tls_config.cert_path)?;

    // to mitigate a race condition during renewal.
    verify_key_cert(&key, &crt)?;

    let mut key_file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(pg_data.join(SERVER_KEY))?;

    let mut crt_file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(pg_data.join(SERVER_CRT))?;

    key_file.write_all(key.as_bytes())?;
    crt_file.write_all(crt.as_bytes())?;

    Ok(())
}

fn verify_key_cert(key: &str, cert: &str) -> Result<()> {
    const ECDSA_WITH_SHA256: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.10045.4.3.2");

    let cert = Certificate::decode(&mut PemReader::new(cert.as_bytes()).context("pem reader")?)
        .context("decode cert")?;

    match cert.signature_algorithm.oid {
        ECDSA_WITH_SHA256 => {
            let key = p256::SecretKey::from_sec1_pem(key).context("parse key")?;

            let a = key.public_key().to_sec1_bytes();
            let b = cert
                .tbs_certificate
                .subject_public_key_info
                .subject_public_key
                .raw_bytes();

            if *a != *b {
                bail!("private key file does not match certificate")
            }
        }
        _ => bail!("unknown TLS key type"),
    }

    Ok(())
}
