//! Postgres protocol proxy/router.
//!
//! This service listens psql port and can check auth via external service
//! (control plane API in our case) and can create new databases and accounts
//! in somewhat transparent manner (again via communication with control plane API).

mod auth;
mod cache;
mod cancellation;
mod compute;
mod config;
mod console;
mod error;
mod http;
mod logging;
mod metrics;
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
use std::{borrow::Cow, future::Future, net::SocketAddr};
use tokio::{net::TcpListener, task::JoinError};
use tracing::{info, info_span, Instrument};
use utils::{project_git_version, sentry_init::init_sentry};

project_git_version!(GIT_VERSION);

/// Flattens `Result<Result<T>>` into `Result<T>`.
async fn flatten_err(
    f: impl Future<Output = Result<anyhow::Result<()>, JoinError>>,
) -> anyhow::Result<()> {
    f.map(|r| r.context("join error").and_then(|x| x)).await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logging_guard = logging::init().await?;
    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);

    info!("Version: {GIT_VERSION}");
    ::metrics::set_build_info_metric(GIT_VERSION);

    let args = cli().get_matches();
    let config = build_config(&args)?;

    info!("Authentication backend: {}", config.auth_backend);

    // Check that we can bind to address before further initialization
    let http_address: SocketAddr = args.get_one::<String>("http").unwrap().parse()?;
    info!("Starting http on {http_address}");
    let http_listener = TcpListener::bind(http_address).await?.into_std()?;

    let mgmt_address: SocketAddr = args.get_one::<String>("mgmt").unwrap().parse()?;
    info!("Starting mgmt on {mgmt_address}");
    let mgmt_listener = TcpListener::bind(mgmt_address).await?.into_std()?;

    let proxy_address: SocketAddr = args.get_one::<String>("proxy").unwrap().parse()?;
    info!("Starting proxy on {proxy_address}");
    let proxy_listener = TcpListener::bind(proxy_address).await?;

    let mut tasks = vec![
        tokio::spawn(http::server::task_main(http_listener)),
        tokio::spawn(proxy::task_main(config, proxy_listener)),
        tokio::task::spawn_blocking(move || console::mgmt::thread_main(mgmt_listener)),
    ];

    if let Some(wss_address) = args.get_one::<String>("wss") {
        let wss_address: SocketAddr = wss_address.parse()?;
        info!("Starting wss on {wss_address}");
        let wss_listener = TcpListener::bind(wss_address).await?;

        tasks.push(tokio::spawn(http::websocket::task_main(
            wss_listener,
            config,
        )));
    }

    // TODO: refactor.
    if let Some(metric_collection) = &config.metric_collection {
        let hostname = hostname::get()?
            .into_string()
            .map_err(|e| anyhow::anyhow!("failed to get hostname {e:?}"))?;

        tasks.push(tokio::spawn(
            metrics::collect_metrics(
                &metric_collection.endpoint,
                metric_collection.interval,
                hostname,
            )
            .instrument(info_span!("collect_metrics")),
        ));
    }

    // This will block until all tasks have completed.
    // Furthermore, the first one to fail will cancel the rest.
    let tasks = tasks.into_iter().map(flatten_err);
    let _: Vec<()> = futures::future::try_join_all(tasks).await?;

    Ok(())
}

/// ProxyConfig is created at proxy startup, and lives forever.
fn build_config(args: &clap::ArgMatches) -> anyhow::Result<&'static ProxyConfig> {
    let tls_config = match (
        args.get_one::<String>("tls-key"),
        args.get_one::<String>("tls-cert"),
    ) {
        (Some(key_path), Some(cert_path)) => Some(config::configure_tls(key_path, cert_path)?),
        (None, None) => None,
        _ => bail!("either both or neither tls-key and tls-cert must be specified"),
    };

    let metric_collection = match (
        args.get_one::<String>("metric-collection-endpoint"),
        args.get_one::<String>("metric-collection-interval"),
    ) {
        (Some(endpoint), Some(interval)) => Some(config::MetricCollectionConfig {
            endpoint: endpoint.parse()?,
            interval: humantime::parse_duration(interval)?,
        }),
        (None, None) => None,
        _ => bail!(
            "either both or neither metric-collection-endpoint \
             and metric-collection-interval must be specified"
        ),
    };

    let auth_backend = match args.get_one::<String>("auth-backend").unwrap().as_str() {
        "console" => {
            let config::CacheOptions { size, ttl } = args
                .get_one::<String>("wake-compute-cache")
                .unwrap()
                .parse()?;

            info!("Using NodeInfoCache (wake_compute) with size={size} ttl={ttl:?}");
            let caches = Box::leak(Box::new(console::caches::ApiCaches {
                node_info: console::caches::NodeInfoCache::new("node_info_cache", size, ttl),
            }));

            let url = args.get_one::<String>("auth-endpoint").unwrap().parse()?;
            let endpoint = http::Endpoint::new(url, http::new_client());

            let api = console::provider::neon::Api::new(endpoint, caches);
            auth::BackendType::Console(Cow::Owned(api), ())
        }
        "postgres" => {
            let url = args.get_one::<String>("auth-endpoint").unwrap().parse()?;
            let api = console::provider::mock::Api::new(url);
            auth::BackendType::Postgres(Cow::Owned(api), ())
        }
        "link" => {
            let url = args.get_one::<String>("uri").unwrap().parse()?;
            auth::BackendType::Link(Cow::Owned(url))
        }
        other => bail!("unsupported auth backend: {other}"),
    };

    let config = Box::leak(Box::new(ProxyConfig {
        tls_config,
        auth_backend,
        metric_collection,
    }));

    Ok(config)
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
            Arg::new("wss")
                .long("wss")
                .help("listen for incoming wss connections on ip:port"),
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
        .arg(
            Arg::new("metric-collection-endpoint")
                .long("metric-collection-endpoint")
                .help("http endpoint to receive periodic metric updates"),
        )
        .arg(
            Arg::new("metric-collection-interval")
                .long("metric-collection-interval")
                .help("how often metrics should be sent to a collection endpoint"),
        )
        .arg(
            Arg::new("wake-compute-cache")
                .long("wake-compute-cache")
                .help("cache for `wake_compute` api method (use `size=0` to disable)")
                .default_value(config::CacheOptions::DEFAULT_OPTIONS_NODE_INFO),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_cli() {
        cli().debug_assert();
    }
}
