use std::time::Duration;

use anyhow::Result;
use compute_api::responses::TlsConfig;
use ring::digest;

pub struct CertDigest(digest::Digest);
pub async fn watch_cert_for_changes(config: TlsConfig) -> tokio::sync::watch::Receiver<CertDigest> {
    let init = compute_digest(&config.cert_path).await;
    let (tx, rx) = tokio::sync::watch::channel(init);
    tokio::spawn(async move {
        while !tx.is_closed() {
            let digest = compute_digest(&config.cert_path).await;
            tx.send_if_modified(|d| {
                if d.0.as_ref() != digest.0.as_ref() {
                    *d = digest;
                    return true;
                }
                false
            });
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
                tokio::time::sleep(Duration::from_secs(30)).await
            }
        }
    }
}

async fn try_compute_digest(cert_path: &str) -> Result<CertDigest> {
    let data = tokio::fs::read(cert_path).await?;
    // sha256 is extremely collision resistent. can safely assume the digest to be unique
    Ok(CertDigest(digest::digest(&digest::SHA256, &data)))
}
