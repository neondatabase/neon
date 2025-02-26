use std::{path::Path, time::Duration};

use anyhow::Result;
use ring::digest;
use tokio::io::AsyncWriteExt;

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

            tokio::time::sleep(Duration::from_secs(60 * 15)).await
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

pub async fn update_key_path(pg_data: &Path, key_path: &str) {
    loop {
        match try_update_key_path(pg_data, key_path).await {
            Ok(()) => break,
            Err(e) => {
                tracing::error!("could not create key file {e:?}");
                tokio::time::sleep(Duration::from_secs(1)).await
            }
        }
    }
}

pub const SERVER_KEY: &str = "server.key";

// Postgres requires the keypath be "secure". This means
// 1. Owned by the postgres user.
// 2. Have permission 600.
async fn try_update_key_path(pg_data: &Path, key_path: &str) -> Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(pg_data.join(SERVER_KEY))
        .await?;

    let data = tokio::fs::read(key_path).await?;
    file.write_all(&data).await?;

    Ok(())
}
