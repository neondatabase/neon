//! `endpoint_storage` is a service which provides API for uploading and downloading
//! files. It is used by compute and control plane for accessing LFC prewarm data.
//! This service is deployed either as a separate component or as part of compute image
//! for large computes.
mod app;
use anyhow::Context;
use clap::Parser;
use postgres_backend::AuthType;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tracing::info;
use utils::auth::JwtAuth;
use utils::logging;

//see set()
const fn max_upload_file_limit() -> usize {
    100 * 1024 * 1024
}

const fn listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 51243)
}

const fn default_auth_type() -> AuthType {
    AuthType::NeonJWT
}

#[derive(Parser)]
struct Args {
    #[arg(exclusive = true)]
    config_file: Option<String>,
    #[arg(long, default_value = "false", requires = "config")]
    /// to allow testing k8s helm chart where we don't have s3 credentials
    no_s3_check_on_startup: bool,
    #[arg(long, value_name = "FILE")]
    /// inline config mode for k8s helm chart
    config: Option<String>,
}

#[derive(serde::Deserialize)]
struct Config {
    #[serde(default = "listen")]
    listen: std::net::SocketAddr,
    pemfile: camino::Utf8PathBuf,
    #[serde(flatten)]
    storage_kind: remote_storage::TypedRemoteStorageKind,
    #[serde(default = "max_upload_file_limit")]
    max_upload_file_limit: usize,
    #[serde(default = "default_auth_type")]
    auth_type: AuthType,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init(
        logging::LogFormat::Plain,
        logging::TracingErrorLayerEnablement::EnableWithRustLogFilter,
        logging::Output::Stdout,
    )?;

    let args = Args::parse();
    let config: Config = if let Some(config_path) = args.config_file {
        info!("Reading config from {config_path}");
        let config = std::fs::read_to_string(config_path)?;
        serde_json::from_str(&config).context("parsing config")?
    } else if let Some(config) = args.config {
        info!("Reading inline config");
        serde_json::from_str(&config).context("parsing config")?
    } else {
        anyhow::bail!("Supply either config file path or --config=inline-config");
    };

    if config.auth_type == AuthType::Trust {
        anyhow::bail!("Trust based auth is not supported");
    }

    let auth = match config.auth_type {
        AuthType::NeonJWT => JwtAuth::from_key_path(&config.pemfile)?,
        AuthType::HadronJWT => JwtAuth::from_cert_path(&config.pemfile)?,
        AuthType::Trust => unreachable!(),
    };

    let listener = tokio::net::TcpListener::bind(config.listen).await.unwrap();
    info!("listening on {}", listener.local_addr().unwrap());

    let storage =
        remote_storage::GenericRemoteStorage::from_storage_kind(config.storage_kind).await?;
    let cancel = tokio_util::sync::CancellationToken::new();
    if !args.no_s3_check_on_startup {
        app::check_storage_permissions(&storage, cancel.clone()).await?;
    }

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
