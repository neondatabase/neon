use std::net::SocketAddr;

///
/// Postgres protocol proxy/router.
///
/// This service listens psql port and can check auth via external service
/// (control plane API in our case) and can create new databases and accounts
/// in somewhat transparent manner (again via communication with control plane API).
///
use anyhow::bail;
use clap::{App, Arg};
use state::{ProxyConfig, ProxyState};
use zenith_utils::{tcp_listener, GIT_VERSION};

use crate::router::{DefaultRouter, LinkRouter, Router, StaticRouter};

mod cplane_mock;
mod router;
mod auth;
mod cancellation;
mod cplane_api;
mod http;
mod mgmt;
mod proxy;
mod state;
mod stream;
mod waiters;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    zenith_metrics::set_common_metrics_prefix("zenith_proxy");
    let arg_matches = App::new("Zenith proxy/router")
        .version(GIT_VERSION)
        .arg(
            Arg::with_name("proxy")
                .short("p")
                .long("proxy")
                .takes_value(true)
                .help("listen for incoming client connections on ip:port")
                .default_value("127.0.0.1:4432"),
        )
        .arg(
            Arg::with_name("mgmt")
                .short("m")
                .long("mgmt")
                .takes_value(true)
                .help("listen for management callback connection on ip:port")
                .default_value("127.0.0.1:7000"),
        )
        .arg(
            Arg::with_name("http")
                .short("h")
                .long("http")
                .takes_value(true)
                .help("listen for incoming http connections (metrics, etc) on ip:port")
                .default_value("127.0.0.1:7001"),
        )
        .arg(
            Arg::with_name("uri")
                .short("u")
                .long("uri")
                .takes_value(true)
                .help("redirect unauthenticated users to given uri")
                .default_value("http://localhost:3000/psql_session/"),
        )
        .arg(
            Arg::with_name("auth-endpoint")
                .short("a")
                .long("auth-endpoint")
                .takes_value(true)
                .help("API endpoint for authenticating users")
                .default_value("http://localhost:3000/authenticate_proxy_request/"),
        )
        .arg(
            Arg::with_name("ssl-key")
                .short("k")
                .long("ssl-key")
                .takes_value(true)
                .help("path to SSL key for client postgres connections"),
        )
        .arg(
            Arg::with_name("ssl-cert")
                .short("c")
                .long("ssl-cert")
                .takes_value(true)
                .help("path to SSL cert for client postgres connections"),
        )
        .get_matches();

    let ssl_config = match (
        arg_matches.value_of("ssl-key"),
        arg_matches.value_of("ssl-cert"),
    ) {
        (Some(key_path), Some(cert_path)) => {
            Some(crate::state::configure_ssl(key_path, cert_path)?)
        }
        (None, None) => None,
        _ => bail!("either both or neither ssl-key and ssl-cert must be specified"),
    };

    // TODO read this from args
    let listen_address = "127.0.0.1:4432".parse().unwrap();
    let router = Router::Static(StaticRouter {
        listen_address,
        postgres_host: "127.0.0.1".into(),
        postgres_port: 5432,
    });

    let mock_cplane_address: SocketAddr = "127.0.0.1:9999".parse().unwrap();

    // TODO read this from args
    let auth_endpoint: String = "http://127.0.0.1:9999/auth".into();
    let router = Router::Default(DefaultRouter {
        listen_address,
        auth_endpoint,
    });

    // TODO read this from args
    let redirect_uri: String = "http://127.0.0.1:9999/link/".into();
    let router = Router::Link(LinkRouter {
        listen_address,
        redirect_uri,
    });

    let config = ProxyConfig {
        router,
        // proxy_address: arg_matches.value_of("proxy").unwrap().parse()?,
        mgmt_address: arg_matches.value_of("mgmt").unwrap().parse()?,
        http_address: arg_matches.value_of("http").unwrap().parse()?,
        // redirect_uri: arg_matches.value_of("uri").unwrap().parse()?,
        // auth_endpoint: arg_matches.value_of("auth-endpoint").unwrap().parse()?,
        ssl_config,
    };
    let state: &ProxyState = Box::leak(Box::new(ProxyState::new(config)));

    println!("Version: {}", GIT_VERSION);

    println!("Starting mock cplane on {}", mock_cplane_address);
    let cplane_mock_listener = tcp_listener::bind(mock_cplane_address)?;

    // Check that we can bind to address before further initialization
    println!("Starting http on {}", state.conf.http_address);
    let http_listener = tcp_listener::bind(state.conf.http_address)?;

    println!("Starting proxy on {}", listen_address);
    let proxy_listener = tokio::net::TcpListener::bind(listen_address).await?;

    println!("Starting mgmt on {}", state.conf.mgmt_address);
    let mgmt_listener = tcp_listener::bind(state.conf.mgmt_address)?;

    let cplane_mock = tokio::spawn(cplane_mock::thread_main(cplane_mock_listener));
    let http = tokio::spawn(http::thread_main(http_listener));
    let proxy = tokio::spawn(proxy::thread_main(state, proxy_listener));
    let mgmt = tokio::task::spawn_blocking(move || mgmt::thread_main(state, mgmt_listener));

    let _ = futures::future::try_join_all([cplane_mock, http, proxy, mgmt])
        .await?
        .into_iter()
        .collect::<Result<Vec<()>, _>>()?;

    Ok(())
}
