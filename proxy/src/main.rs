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
use clap::{App, Arg};
use config::ProxyConfig;
use futures::FutureExt;
use std::{future::Future, net::SocketAddr};
use tokio::{net::TcpListener, task::JoinError};
use utils::project_git_version;
use substring::Substring;

project_git_version!(GIT_VERSION);

/// Flattens `Result<Result<T>>` into `Result<T>`.
async fn flatten_err(
    f: impl Future<Output = Result<anyhow::Result<()>, JoinError>>,
) -> anyhow::Result<()> {
    f.map(|r| r.context("join error").and_then(|x| x)).await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arg_matches = App::new("Neon proxy/router")
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
            Arg::new("auth-backend")
                .long("auth-backend")
                .takes_value(true)
                .help("Possible values: legacy | console | postgres | link")
                .default_value("legacy"),
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
                .help("redirect unauthenticated users to the given uri in case of link auth")
                .default_value("http://localhost:3000/psql_session/"),
        )
        .arg(
            Arg::new("auth-endpoint")
                .short('a')
                .long("auth-endpoint")
                .takes_value(true)
                .help("cloud API endpoint for authenticating users")
                .default_value("http://localhost:3000/authenticate_proxy_request/"),
        )
        .arg(
            Arg::new("tls-key")
                .short('k')
                .long("tls-key")
                .alias("ssl-key") // backwards compatibility
                .takes_value(true)
                .help("path to TLS key for client postgres connections"),
        )
        .arg(
            Arg::new("tls-cert")
                .short('c')
                .long("tls-cert")
                .alias("ssl-cert") // backwards compatibility
                .takes_value(true)
                .help("path to TLS cert for client postgres connections"),
        )
        .get_matches();

    let tls_config = match (
        arg_matches.value_of("tls-key"),
        arg_matches.value_of("tls-cert"),
    ) {
        (Some(key_path), Some(cert_path)) => Some(config::configure_tls(key_path, cert_path)?),
        (None, None) => None,
        _ => bail!("either both or neither tls-key and tls-cert must be specified"),
    };

    let proxy_address: SocketAddr = arg_matches.value_of("proxy").unwrap().parse()?;
    let mgmt_address: SocketAddr = arg_matches.value_of("mgmt").unwrap().parse()?;
    let http_address: SocketAddr = arg_matches.value_of("http").unwrap().parse()?;

    // determine common name from tls-cert (-c server.crt param).
    // used in asserting project name formatting invariant.
    let common_name = match arg_matches.value_of("tls-cert") {
        Some(cert_path) => {
            let file = std::fs::File::open(cert_path).unwrap();
            let almost_common_name = x509_parser::pem::Pem::read(std::io::BufReader::new(file))
                .unwrap().0
                .parse_x509().unwrap()
                .tbs_certificate.subject.to_string();
            let prefix = "CN=*";
            // making sure formatting is as expected: almost_common_name = "'CN=*'[<common name>]"
            assert_eq!(almost_common_name.substring(0, prefix.len()), prefix);
            Some(almost_common_name.substring(prefix.len(), almost_common_name.len()).to_string())
        }
        None => None,
    };

    let config: &ProxyConfig = Box::leak(Box::new(ProxyConfig {
        tls_config,
        auth_backend: arg_matches.value_of("auth-backend").unwrap().parse()?,
        auth_endpoint: arg_matches.value_of("auth-endpoint").unwrap().parse()?,
        auth_link_uri: arg_matches.value_of("uri").unwrap().parse()?,
        common_name,
    }));

    println!("Version: {GIT_VERSION}");
    println!("Authentication backend: {:?}", config.auth_backend);

    // Check that we can bind to address before further initialization
    println!("Starting http on {}", http_address);
    let http_listener = TcpListener::bind(http_address).await?.into_std()?;

    println!("Starting mgmt on {}", mgmt_address);
    let mgmt_listener = TcpListener::bind(mgmt_address).await?.into_std()?;

    println!("Starting proxy on {}", proxy_address);
    let proxy_listener = TcpListener::bind(proxy_address).await?;

    let tasks = [
        tokio::spawn(http::thread_main(http_listener)),
        tokio::spawn(proxy::thread_main(config, proxy_listener)),
        tokio::task::spawn_blocking(move || mgmt::thread_main(mgmt_listener)),
    ]
    .map(flatten_err);

    // This will block until all tasks have completed.
    // Furthermore, the first one to fail will cancel the rest.
    let _: Vec<()> = futures::future::try_join_all(tasks).await?;

    Ok(())
}
