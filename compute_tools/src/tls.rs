use std::{io::Write, os::unix::fs::OpenOptionsExt, path::Path, time::Duration};

use anyhow::Result;
use compute_api::responses::TlsConfig;
use ring::digest;

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
        .open(pg_data.join(SERVER_KEY))?;

    // Race condition risk:
    // 1. key is read
    // 2. key and crt are updated
    // 3. crt is read
    // key does not match crt.
    // We can either:
    // 1. Accept this as an unlikely risk (chance to occur every 23 hours).
    // 2. Parse the certificate and verify that the key matches (seems awkward).
    let key = std::fs::read(&tls_config.key_path)?;
    let crt = std::fs::read(&tls_config.cert_path)?;

    key_file.write_all(&key)?;
    crt_file.write_all(&crt)?;

    Ok(())
}
