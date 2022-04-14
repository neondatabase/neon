//! Postgres protocol proxy/router.
//!
//! This service listens psql port and can check auth via external service
//! (control plane API in our case) and can create new databases and accounts
//! in somewhat transparent manner (again via communication with control plane API).

mod auth;
mod cancellation;
mod compute;
mod config;
mod cplane_api;
mod error;
mod http;
mod mgmt;
mod proxy;
mod stream;
mod waiters;

// Currently SCRAM is only used in tests
#[cfg(test)]
mod parse;
#[cfg(test)]
mod sasl;
#[cfg(test)]
mod scram;

use anyhow::{bail, Context};
use clap::{Arg, Command};
use config::ProxyConfig;
use futures::FutureExt;
use std::future::Future;
use tokio::{net::TcpListener, task::JoinError};
use zenith_utils::GIT_VERSION;

use crate::config::{ClientAuthMethod, RouterConfig};

/// Flattens `Result<Result<T>>` into `Result<T>`.
async fn flatten_err(
    f: impl Future<Output = Result<anyhow::Result<()>, JoinError>>,
) -> anyhow::Result<()> {
    f.map(|r| r.context("join error").and_then(|x| x)).await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    zenith_metrics::set_common_metrics_prefix("zenith_proxy");
    let arg_matches = Command::new("Zenith proxy/router")
        .version(GIT_VERSION)
        .arg(
            Arg::new("proxy")
                .short('p')
                .long("proxy")
                .takes_value(true)
                .help("listen for incoming client connections on ip:port")
                .default_value("127.0.0.1:4432"),
        )
        .arg(
            Arg::new("auth-method")
                .long("auth-method")
                .takes_value(true)
                .help("Possible values: password | link | mixed")
                .default_value("mixed"),
        )
        .arg(
            Arg::new("static-router")
                .short('s')
                .long("static-router")
                .takes_value(true)
                .help("Route all clients to host:port"),
        )
        .arg(
            Arg::new("mgmt")
                .short('m')
                .long("mgmt")
                .takes_value(true)
                .help("listen for management callback connection on ip:port")
                .default_value("127.0.0.1:7000"),
        )
        .arg(
            Arg::new("http")
                .short('h')
                .long("http")
                .takes_value(true)
                .help("listen for incoming http connections (metrics, etc) on ip:port")
                .default_value("127.0.0.1:7001"),
        )
        .arg(
            Arg::new("uri")
                .short('u')
                .long("uri")
                .takes_value(true)
                .help("redirect unauthenticated users to given uri")
                .default_value("http://localhost:3000/psql_session/"),
        )
        .arg(
            Arg::new("auth-endpoint")
                .short('a')
                .long("auth-endpoint")
                .takes_value(true)
                .help("API endpoint for authenticating users")
                .default_value("http://localhost:3000/authenticate_proxy_request/"),
        )
        .arg(
            Arg::new("ssl-key")
                .short('k')
                .long("ssl-key")
                .takes_value(true)
                .help("path to SSL key for client postgres connections"),
        )
        .arg(
            Arg::new("ssl-cert")
                .short('c')
                .long("ssl-cert")
                .takes_value(true)
                .help("path to SSL cert for client postgres connections"),
        )
        .get_matches();

    let tls_config = match (
        arg_matches.value_of("ssl-key"),
        arg_matches.value_of("ssl-cert"),
    ) {
        (Some(key_path), Some(cert_path)) => Some(config::configure_ssl(key_path, cert_path)?),
        (None, None) => None,
        _ => bail!("either both or neither ssl-key and ssl-cert must be specified"),
    };

    let auth_method = arg_matches.value_of("auth-method").unwrap().parse()?;
    let router_config = match arg_matches.value_of("static-router") {
        None => RouterConfig::Dynamic(auth_method),
        Some(addr) => {
            if let ClientAuthMethod::Password = auth_method {
                let (host, port) = addr.split_once(':').unwrap();
                RouterConfig::Static {
                    host: host.to_string(),
                    port: port.parse().unwrap(),
                }
            } else {
                bail!("static-router requires --auth-method password")
            }
        }
    };

    let config: &ProxyConfig = Box::leak(Box::new(ProxyConfig {
        router_config,
        proxy_address: arg_matches.value_of("proxy").unwrap().parse()?,
        mgmt_address: arg_matches.value_of("mgmt").unwrap().parse()?,
        http_address: arg_matches.value_of("http").unwrap().parse()?,
        redirect_uri: arg_matches.value_of("uri").unwrap().parse()?,
        auth_endpoint: arg_matches.value_of("auth-endpoint").unwrap().parse()?,
        tls_config,
    }));

    println!("Version: {}", GIT_VERSION);

    // Check that we can bind to address before further initialization
    println!("Starting http on {}", config.http_address);
    let http_listener = TcpListener::bind(config.http_address).await?.into_std()?;

    println!("Starting mgmt on {}", config.mgmt_address);
    let mgmt_listener = TcpListener::bind(config.mgmt_address).await?.into_std()?;

    println!("Starting proxy on {}", config.proxy_address);
    let proxy_listener = TcpListener::bind(config.proxy_address).await?;

    let http = tokio::spawn(http::thread_main(http_listener));
    let proxy = tokio::spawn(proxy::thread_main(config, proxy_listener));
    let mgmt = tokio::task::spawn_blocking(move || mgmt::thread_main(mgmt_listener));

    let tasks = [flatten_err(http), flatten_err(proxy), flatten_err(mgmt)];
    let _: Vec<()> = futures::future::try_join_all(tasks).await?;

    Ok(())
}
