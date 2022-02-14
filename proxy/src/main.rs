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

use crate::router::{DefaultRouter, LinkRouter, MixedRouter, Router, StaticRouter};

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
            Arg::with_name("router")
                .short("r")
                .long("router")
                .takes_value(true)
                .help("Possible values: default | link | mixed | static")
                .default_value("mixed"),
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
            Arg::with_name("static-destination")
                .short("s")
                .long("static-destination")
                .takes_value(true)
                .help("Route all pg connections to this host:port")
                .default_value("localhost:5432"),
        )
        .arg(
            Arg::with_name("mock-cplane")
                .long("mock-cplane")
                .takes_value(true)
                .help("Provide auth endpoint and redirect uri at this address"),
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

    let router = match arg_matches.value_of("router").unwrap() {
        "default" => Router::Default(DefaultRouter {
            auth_endpoint: arg_matches.value_of("auth-endpoint").unwrap().to_string(),
        }),
        "link" => Router::Link(LinkRouter {
            redirect_uri: arg_matches.value_of("uri").unwrap().to_string(),
        }),
        "mixed" => Router::Mixed(MixedRouter {
            default: DefaultRouter {
                auth_endpoint: arg_matches.value_of("auth-endpoint").unwrap().to_string(),
            },
            link: LinkRouter {
                redirect_uri: arg_matches.value_of("uri").unwrap().to_string(),
            },
        }),
        "static" => {
            let (host, port) = arg_matches.value_of("static-destination")
                .unwrap().split_once(":").unwrap();
            Router::Static(StaticRouter {
                postgres_host: host.to_string(),
                postgres_port: port.parse().unwrap(),
            })
        }
        _ => bail!("invalid option for router")
    };

    let listen_address = arg_matches.value_of("proxy").unwrap().parse()?;
    let config = ProxyConfig {
        router,
        listen_address,
        mgmt_address: arg_matches.value_of("mgmt").unwrap().parse()?,
        http_address: arg_matches.value_of("http").unwrap().parse()?,
        ssl_config,
    };
    let state: &ProxyState = Box::leak(Box::new(ProxyState::new(config)));

    println!("Version: {}", GIT_VERSION);


    // Check that we can bind to address before further initialization
    let cplane_mock_listener = arg_matches.value_of("mock-cplane").map(|mock_addr| {
        println!("Starting mock cplane on {}", mock_addr);
        tcp_listener::bind(mock_addr).unwrap()
    });

    println!("Starting http on {}", state.conf.http_address);
    let http_listener = tcp_listener::bind(state.conf.http_address)?;

    println!("Starting proxy on {}", listen_address);
    let proxy_listener = tokio::net::TcpListener::bind(listen_address).await?;

    println!("Starting mgmt on {}", state.conf.mgmt_address);
    let mgmt_listener = tcp_listener::bind(state.conf.mgmt_address)?;

    let mut servers = vec![];
    if let Some(cplane_mock_listener) = cplane_mock_listener {
        servers.push(tokio::spawn(cplane_mock::thread_main(cplane_mock_listener)));
    }

    servers.push(tokio::spawn(http::thread_main(http_listener)));
    servers.push(tokio::spawn(proxy::thread_main(state, proxy_listener)));
    servers.push(tokio::task::spawn_blocking(move || mgmt::thread_main(state, mgmt_listener)));

    let _ = futures::future::try_join_all(servers)
        .await?
        .into_iter()
        .collect::<Result<Vec<()>, _>>()?;

    Ok(())
}
