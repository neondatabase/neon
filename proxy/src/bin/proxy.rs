use anyhow::{bail, Context};
use clap::{self, Arg};
use futures::future::try_join_all;
use proxy::{
    auth,
    config::{self, MetricCollectionConfig, ProxyConfig, TlsConfig},
    console, http, metrics,
};
use std::{borrow::Cow, net::SocketAddr, sync::atomic::Ordering};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use utils::{project_git_version, sentry_init::init_sentry};

project_git_version!(GIT_VERSION);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logging_guard = proxy::logging::init().await?;
    let _panic_hook_guard = utils::logging::replace_panic_hook_with_tracing_panic_hook();
    let _sentry_guard = init_sentry(Some(GIT_VERSION.into()), &[]);

    info!("Version: {GIT_VERSION}");
    ::metrics::set_build_info_metric(GIT_VERSION);

    let args = cli().get_matches();
    let config: &ProxyConfig = Box::leak(Box::new(build_config(&args)?));
    info!("Authentication backend: {}", config.auth_backend);

    // Check that we can bind to address before further initialization
    let http_address: SocketAddr = args.get_one::<String>("http").unwrap().parse()?;
    info!("Starting http on {http_address}");
    let http_listener = TcpListener::bind(http_address).await?.into_std()?;

    let mgmt_address: SocketAddr = args.get_one::<String>("mgmt").unwrap().parse()?;
    info!("Starting mgmt on {mgmt_address}");
    let mgmt_listener = TcpListener::bind(mgmt_address).await?;

    let proxy_address: SocketAddr = args.get_one::<String>("proxy").unwrap().parse()?;
    info!("Starting proxy on {proxy_address}");
    let proxy_listener = TcpListener::bind(proxy_address).await?;
    let cancellation_token = CancellationToken::new();

    let mut client_tasks = vec![tokio::spawn(proxy::proxy::task_main(
        config,
        proxy_listener,
        cancellation_token.clone(),
    ))];

    if let Some(wss_address) = args.get_one::<String>("wss") {
        let wss_address: SocketAddr = wss_address.parse()?;
        info!("Starting wss on {wss_address}");
        let wss_listener = TcpListener::bind(wss_address).await?;

        client_tasks.push(tokio::spawn(http::websocket::task_main(
            config,
            wss_listener,
            cancellation_token.clone(),
        )));
    }

    let mut tasks = vec![
        tokio::spawn(proxy::handle_signals(cancellation_token)),
        tokio::spawn(http::server::task_main(http_listener)),
        tokio::spawn(console::mgmt::task_main(mgmt_listener)),
    ];

    if let Some(metrics_config) = &config.metric_collection {
        tasks.push(tokio::spawn(metrics::task_main(metrics_config)));
    }

    if let auth::BackendType::Console(api, _) = &config.auth_backend {
        if let Some(url) = args.get_one::<String>("redis-notifications") {
            info!("Starting redis notifications listener ({url})");
            tasks.push(tokio::spawn(console::notifications::task_main(
                url.to_owned(),
                api.caches,
            )));
        }
    }

    let tasks = try_join_all(tasks.into_iter().map(proxy::flatten_err));
    let client_tasks = try_join_all(client_tasks.into_iter().map(proxy::flatten_err));
    tokio::select! {
        // We are only expecting an error from these forever tasks
        res = tasks => { res?; },
        res = client_tasks => { res?; },
    }

    Ok(())
}

fn build_tls_config(args: &clap::ArgMatches) -> anyhow::Result<Option<TlsConfig>> {
    let config = match (
        args.get_one::<String>("tls-key"),
        args.get_one::<String>("tls-cert"),
    ) {
        (Some(key_path), Some(cert_path)) => Some(config::configure_tls(
            key_path,
            cert_path,
            args.get_one::<String>("certs-dir"),
        )?),
        (None, None) => None,
        _ => bail!("either both or neither tls-key and tls-cert must be specified"),
    };

    let allow_self_signed_compute: bool = args
        .get_one::<String>("allow-self-signed-compute")
        .unwrap()
        .parse()?;

    if allow_self_signed_compute {
        warn!("allowing self-signed compute certificates");
        proxy::compute::ALLOW_SELF_SIGNED_COMPUTE.store(true, Ordering::Relaxed);
    }

    Ok(config)
}

fn build_metrics_config(args: &clap::ArgMatches) -> anyhow::Result<Option<MetricCollectionConfig>> {
    let endpoint = args.get_one::<String>("metric-collection-endpoint");
    let interval = args.get_one::<String>("metric-collection-interval");

    let config = match (endpoint, interval) {
        (Some(endpoint), Some(interval)) => Some(config::MetricCollectionConfig {
            endpoint: endpoint.parse().context("bad metrics endpoint")?,
            interval: humantime::parse_duration(interval)?,
        }),
        (None, None) => None,
        _ => bail!(
            "either both or neither metric-collection-endpoint \
             and metric-collection-interval must be specified"
        ),
    };

    Ok(config)
}

fn make_caches(args: &clap::ArgMatches) -> anyhow::Result<console::caches::ApiCaches> {
    let config::CacheOptions { size, ttl } = args
        .get_one::<String>("get-auth-info-cache")
        .unwrap()
        .parse()?;

    info!("Using AuthInfoCache (get_auth_info) with size={size} ttl={ttl:?}");
    let auth_info = console::caches::AuthInfoCache::new("auth_info_cache", size, ttl);

    let config::CacheOptions { size, ttl } = args
        .get_one::<String>("wake-compute-cache")
        .unwrap()
        .parse()?;

    info!("Using NodeInfoCache (wake_compute) with size={size} ttl={ttl:?}");
    let node_info = console::caches::NodeInfoCache::new("node_info_cache", size, ttl);

    let caches = console::caches::ApiCaches {
        auth_info,
        node_info,
    };

    Ok(caches)
}

fn build_auth_config(args: &clap::ArgMatches) -> anyhow::Result<auth::BackendType<'static, ()>> {
    let config = match args.get_one::<String>("auth-backend").unwrap().as_str() {
        "console" => {
            let url = args.get_one::<String>("auth-endpoint").unwrap().parse()?;
            let endpoint = http::Endpoint::new(url, http::new_client());
            let caches = Box::leak(Box::new(make_caches(args)?));
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

    Ok(config)
}

/// ProxyConfig is created at proxy startup, and lives forever.
fn build_config(args: &clap::ArgMatches) -> anyhow::Result<ProxyConfig> {
    let config = ProxyConfig {
        tls_config: build_tls_config(args)?,
        auth_backend: build_auth_config(args)?,
        metric_collection: build_metrics_config(args)?,
    };

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
        // tls-key and tls-cert are for backwards compatibility, we can put all certs in one dir
        .arg(
            Arg::new("certs-dir")
                .long("certs-dir")
                .help("path to directory with TLS certificates for client postgres connections"),
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
            Arg::new("redis-notifications")
                .long("redis-notifications")
                .help("for receiving notifications from console (e.g. redis://127.0.0.1:6379)"),
        )
        .arg(
            Arg::new("get-auth-info-cache")
                .long("get-auth-info-cache")
                .help("cache for `get_auth_info` api method (use `size=0` to disable)")
                .default_value(config::CacheOptions::DEFAULT_AUTH_INFO),
        )
        .arg(
            Arg::new("wake-compute-cache")
                .long("wake-compute-cache")
                .help("cache for `wake_compute` api method (use `size=0` to disable)")
                .default_value(config::CacheOptions::DEFAULT_NODE_INFO),
        )
        .arg(
            Arg::new("allow-self-signed-compute")
                .long("allow-self-signed-compute")
                .help("Allow self-signed certificates for compute nodes (for testing)")
                .default_value("false"),
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
