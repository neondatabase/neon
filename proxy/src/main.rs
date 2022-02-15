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
use std::thread;
use zenith_utils::http::endpoint;
use zenith_utils::{tcp_listener, GIT_VERSION};

mod cplane_api;
mod http;
mod mgmt;
mod proxy;
mod state;
mod waiters;

fn main() -> anyhow::Result<()> {
    zenith_metrics::set_common_metrics_prefix("zenith_proxy");
    let arg_matches = App::new("Zenith proxy/router")
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
        .arg(
            Arg::new("keepalive")
                .long("keepalive")
                .takes_value(true)
                .help("Enable TCP keepalive"),
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
    let config = ProxyConfig {
        proxy_address: arg_matches.value_of("proxy").unwrap().parse()?,
        mgmt_address: arg_matches.value_of("mgmt").unwrap().parse()?,
        http_address: arg_matches.value_of("http").unwrap().parse()?,
        redirect_uri: arg_matches.value_of("uri").unwrap().parse()?,
        auth_endpoint: arg_matches.value_of("auth-endpoint").unwrap().parse()?,
        tcp_keepalive: arg_matches.value_of("keepalive").map(|enabled|std::str::FromStr::from_str(enabled)).transpose()?,
        ssl_config,
    };
    let state: &ProxyState = Box::leak(Box::new(ProxyState::new(config)));

    println!("Version: {}", GIT_VERSION);

    // Check that we can bind to address before further initialization
    println!("Starting http on {}", state.conf.http_address);
    let http_listener = tcp_listener::bind(state.conf.http_address)?;

    println!("Starting proxy on {}", state.conf.proxy_address);
    let pageserver_listener = tcp_listener::bind(state.conf.proxy_address)?;

    println!("Starting mgmt on {}", state.conf.mgmt_address);
    let mgmt_listener = tcp_listener::bind(state.conf.mgmt_address)?;

    let threads = [
        thread::Builder::new()
            .name("Http thread".into())
            .spawn(move || {
                let router = http::make_router();
                endpoint::serve_thread_main(
                    router,
                    http_listener,
                    std::future::pending(), // never shut down
                )
            })?,
        // Spawn a thread to listen for connections. It will spawn further threads
        // for each connection.
        thread::Builder::new()
            .name("Listener thread".into())
            .spawn(move || proxy::thread_main(state, pageserver_listener))?,
        thread::Builder::new()
            .name("Mgmt thread".into())
            .spawn(move || mgmt::thread_main(state, mgmt_listener))?,
    ];

    for t in threads {
        t.join().unwrap()?;
    }

    Ok(())
}
