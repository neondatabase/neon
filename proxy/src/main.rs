//! Postgres protocol proxy/router.
//!
//! This service listens psql port and can check auth via external service
//! (control plane API in our case) and can create new databases and accounts
//! in somewhat transparent manner (again via communication with control plane API).

mod auth;
mod cancellation;
mod compute;
mod config;
mod error;
mod http;
mod mgmt;
mod parse;
mod proxy;
mod sasl;
mod scram;
mod stream;
mod url;
mod waiters;

use anyhow::{bail, Context};
use clap::{self, Arg};
use config::ProxyConfig;
use futures::FutureExt;
use metrics::set_build_info_metric;
use std::{borrow::Cow, future::Future, net::SocketAddr};
use tokio::{net::TcpListener, task::JoinError};
use tracing::info;
use utils::project_git_version;

project_git_version!(GIT_VERSION);

/// Flattens `Result<Result<T>>` into `Result<T>`.
async fn flatten_err(
    f: impl Future<Output = Result<anyhow::Result<()>, JoinError>>,
) -> anyhow::Result<()> {
    f.map(|r| r.context("join error").and_then(|x| x)).await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_ansi(atty::is(atty::Stream::Stdout))
        .with_target(false)
        .init();

    let arg_matches = cli().get_matches();

    let tls_config = match (
        arg_matches.get_one::<String>("tls-key"),
        arg_matches.get_one::<String>("tls-cert"),
    ) {
        (Some(key_path), Some(cert_path)) => Some(config::configure_tls(key_path, cert_path)?),
        (None, None) => None,
        _ => bail!("either both or neither tls-key and tls-cert must be specified"),
    };

    let proxy_address: SocketAddr = arg_matches.get_one::<String>("proxy").unwrap().parse()?;
    let mgmt_address: SocketAddr = arg_matches.get_one::<String>("mgmt").unwrap().parse()?;
    let http_address: SocketAddr = arg_matches.get_one::<String>("http").unwrap().parse()?;

    let auth_backend = match arg_matches
        .get_one::<String>("auth-backend")
        .unwrap()
        .as_str()
    {
        "console" => {
            let url = arg_matches
                .get_one::<String>("auth-endpoint")
                .unwrap()
                .parse()?;
            let endpoint = http::Endpoint::new(url, reqwest::Client::new());
            auth::BackendType::Console(Cow::Owned(endpoint), ())
        }
        "postgres" => {
            let url = arg_matches
                .get_one::<String>("auth-endpoint")
                .unwrap()
                .parse()?;
            auth::BackendType::Postgres(Cow::Owned(url), ())
        }
        "link" => {
            let url = arg_matches.get_one::<String>("uri").unwrap().parse()?;
            auth::BackendType::Link(Cow::Owned(url))
        }
        other => bail!("unsupported auth backend: {other}"),
    };

    let config: &ProxyConfig = Box::leak(Box::new(ProxyConfig {
        tls_config,
        auth_backend,
    }));

    info!("Version: {GIT_VERSION}");
    info!("Authentication backend: {}", config.auth_backend);

    // Check that we can bind to address before further initialization
    info!("Starting http on {http_address}");
    let http_listener = TcpListener::bind(http_address).await?.into_std()?;

    info!("Starting mgmt on {mgmt_address}");
    let mgmt_listener = TcpListener::bind(mgmt_address).await?.into_std()?;

    info!("Starting proxy on {proxy_address}");
    let proxy_listener = TcpListener::bind(proxy_address).await?;

    let tasks = [
        tokio::spawn(http::server::task_main(http_listener)),
        tokio::spawn(proxy::task_main(config, proxy_listener)),
        tokio::task::spawn_blocking(move || mgmt::thread_main(mgmt_listener)),
    ]
    .map(flatten_err);

    set_build_info_metric(GIT_VERSION);
    // This will block until all tasks have completed.
    // Furthermore, the first one to fail will cancel the rest.
    let _: Vec<()> = futures::future::try_join_all(tasks).await?;

    Ok(())
}

fn cli() -> clap::Command {
    clap::Command::new("Neon proxy/router")
        .disable_help_flag(true)
        .version(GIT_VERSION)
        .arg(
            Arg::new("proxy")
                .short('p')
                .long("proxy")
                .help("listen for incoming client connections on ip:port")
                .default_value("127.0.0.1:4432"),
        )
        .arg(
            Arg::new("auth-backend")
                .long("auth-backend")
                .value_parser(["console", "postgres", "link"])
                .default_value("link"),
        )
        .arg(
            Arg::new("mgmt")
                .short('m')
                .long("mgmt")
                .help("listen for management callback connection on ip:port")
                .default_value("127.0.0.1:7000"),
        )
        .arg(
            Arg::new("http")
                .long("http")
                .help("listen for incoming http connections (metrics, etc) on ip:port")
                .default_value("127.0.0.1:7001"),
        )
        .arg(
            Arg::new("uri")
                .short('u')
                .long("uri")
                .help("redirect unauthenticated users to the given uri in case of link auth")
                .default_value("http://localhost:3000/psql_session/"),
        )
        .arg(
            Arg::new("auth-endpoint")
                .short('a')
                .long("auth-endpoint")
                .help("cloud API endpoint for authenticating users")
                .default_value("http://localhost:3000/authenticate_proxy_request/"),
        )
        .arg(
            Arg::new("tls-key")
                .short('k')
                .long("tls-key")
                .alias("ssl-key") // backwards compatibility
                .help("path to TLS key for client postgres connections"),
        )
        .arg(
            Arg::new("tls-cert")
                .short('c')
                .long("tls-cert")
                .alias("ssl-cert") // backwards compatibility
                .help("path to TLS cert for client postgres connections"),
        )
}

#[test]
fn verify_cli() {
    cli().debug_assert();
}
