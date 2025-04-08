//! `object_storage` is a service which provides API for uploading and downloading
//! files. It is used by compute and control plane for accessing LFC prewarm data.
//! This service is deployed either as a separate component or as part of compute image
//! for large computes.
mod app;
use anyhow::Context;
use tracing::info;
use utils::logging;

//see set()
const fn max_upload_file_limit() -> usize {
    100 * 1024 * 1024
}

#[derive(serde::Deserialize)]
#[serde(tag = "type")]
struct Config {
    listen: std::net::SocketAddr,
    pemfile: camino::Utf8PathBuf,
    #[serde(flatten)]
    storage_config: remote_storage::RemoteStorageConfig,
    #[serde(default = "max_upload_file_limit")]
    max_upload_file_limit: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init(
        logging::LogFormat::Plain,
        logging::TracingErrorLayerEnablement::EnableWithRustLogFilter,
        logging::Output::Stdout,
    )?;

    let config: String = std::env::args().skip(1).take(1).collect();
    if config.is_empty() {
        anyhow::bail!("Usage: s3proxy config.json")
    }
    info!("Reading config from {config}");
    let config = std::fs::read_to_string(config.clone())?;
    let config: Config = serde_json::from_str(&config).context("parsing config")?;
    info!("Reading pemfile from {}", config.pemfile.clone());
    let pemfile = std::fs::read(config.pemfile.clone())?;
    info!("Loading public key from {}", config.pemfile.clone());
    let auth = s3proxy::JwtAuth::new(&pemfile)?;

    let listener = tokio::net::TcpListener::bind(config.listen).await.unwrap();
    info!("listening on {}", listener.local_addr().unwrap());

    let storage = remote_storage::GenericRemoteStorage::from_config(&config.storage_config).await?;
    let cancel = tokio_util::sync::CancellationToken::new();
    app::check_storage_permissions(&storage, cancel.clone()).await?;

    let proxy = std::sync::Arc::new(s3proxy::Proxy {
        auth,
        storage,
        cancel: cancel.clone(),
        max_upload_file_limit: config.max_upload_file_limit,
    });

    tokio::spawn(utils::signals::signal_handler(cancel.clone()));
    axum::serve(listener, app::app(proxy))
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await?;
    Ok(())
}
