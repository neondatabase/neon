//! `endpoint_storage` is a service which provides API for uploading and downloading
//! files. It is used by compute and control plane for accessing LFC prewarm data.
//! This service is deployed either as a separate component or as part of compute image
//! for large computes.
mod app;
use anyhow::{Context, bail};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tracing::info;
use utils::logging;

//see set()
const fn max_upload_file_limit() -> usize {
    100 * 1024 * 1024
}

const fn listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 51243)
}

#[derive(serde::Deserialize)]
#[serde(tag = "type")]
struct Config {
    #[serde(default = "listen")]
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

    // Allow either passing filename or inline config (for k8s helm chart)
    let args: Vec<String> = std::env::args().skip(1).collect();
    let config: Config = if args.len() == 1 && args[0].ends_with(".json") {
        info!("Reading config from {}", args[0]);
        let config = std::fs::read_to_string(args[0].clone())?;
        serde_json::from_str(&config).context("parsing config")?
    } else if !args.is_empty() && args[0].starts_with("--config=") {
        info!("Reading inline config");
        let config = args.join(" ");
        let config = config.strip_prefix("--config=").unwrap();
        serde_json::from_str(config).context("parsing config")?
    } else {
        bail!("Usage: endpoint_storage config.json or endpoint_storage --config=JSON");
    };

    info!("Reading pemfile from {}", config.pemfile.clone());
    let pemfile = std::fs::read(config.pemfile.clone())?;
    info!("Loading public key from {}", config.pemfile.clone());
    let auth = endpoint_storage::JwtAuth::new(&pemfile)?;

    let listener = tokio::net::TcpListener::bind(config.listen).await.unwrap();
    info!("listening on {}", listener.local_addr().unwrap());

    let storage = remote_storage::GenericRemoteStorage::from_config(&config.storage_config).await?;
    let cancel = tokio_util::sync::CancellationToken::new();
    app::check_storage_permissions(&storage, cancel.clone()).await?;

    let proxy = std::sync::Arc::new(endpoint_storage::Storage {
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
