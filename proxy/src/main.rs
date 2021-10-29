///
/// Postgres protocol proxy/router.
///
/// This service listens psql port and can check auth via external service
/// (control plane API in our case) and can create new databases and accounts
/// in somewhat transparent manner (again via communication with control plane API).
///
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{mpsc, Arc, Mutex},
    thread,
};

use anyhow::{anyhow, bail, ensure, Context};
use clap::{App, Arg, ArgMatches};

use cplane_api::DatabaseInfo;
use rustls::{internal::pemfile, NoClientAuth, ProtocolVersion, ServerConfig};
use zenith_utils::{tcp_listener, GIT_VERSION};

mod cplane_api;
mod mgmt;
mod proxy;

pub struct ProxyConf {
    /// main entrypoint for users to connect to
    pub proxy_address: SocketAddr,

    /// http management endpoint. Upon user account creation control plane
    /// will notify us here, so that we can 'unfreeze' user session.
    pub mgmt_address: SocketAddr,

    /// send unauthenticated users to this URI
    pub redirect_uri: String,

    /// control plane address where we would check auth.
    pub auth_endpoint: String,

    pub ssl_config: Option<Arc<ServerConfig>>,
}

pub struct ProxyState {
    pub conf: ProxyConf,
    pub waiters: Mutex<HashMap<String, mpsc::Sender<anyhow::Result<DatabaseInfo>>>>,
}

fn configure_ssl(arg_matches: &ArgMatches) -> anyhow::Result<Option<Arc<ServerConfig>>> {
    let (key_path, cert_path) = match (
        arg_matches.value_of("ssl-key"),
        arg_matches.value_of("ssl-cert"),
    ) {
        (Some(key_path), Some(cert_path)) => (key_path, cert_path),
        (None, None) => return Ok(None),
        _ => bail!("either both or neither ssl-key and ssl-cert must be specified"),
    };

    let key = {
        let key_bytes = std::fs::read(key_path).context("SSL key file")?;
        let mut keys = pemfile::pkcs8_private_keys(&mut &key_bytes[..])
            .map_err(|_| anyhow!("couldn't read TLS keys"))?;
        ensure!(keys.len() == 1, "keys.len() = {} (should be 1)", keys.len());
        keys.pop().unwrap()
    };

    let cert_chain = {
        let cert_chain_bytes = std::fs::read(cert_path).context("SSL cert file")?;
        pemfile::certs(&mut &cert_chain_bytes[..])
            .map_err(|_| anyhow!("couldn't read TLS certificates"))?
    };

    let mut config = ServerConfig::new(NoClientAuth::new());
    config.set_single_cert(cert_chain, key)?;
    config.versions = vec![ProtocolVersion::TLSv1_3];

    Ok(Some(Arc::new(config)))
}

fn main() -> anyhow::Result<()> {
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
                .help("redirect unauthenticated users to given uri")
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

    let conf = ProxyConf {
        proxy_address: arg_matches.value_of("proxy").unwrap().parse()?,
        mgmt_address: arg_matches.value_of("mgmt").unwrap().parse()?,
        redirect_uri: arg_matches.value_of("uri").unwrap().parse()?,
        auth_endpoint: arg_matches.value_of("auth-endpoint").unwrap().parse()?,
        ssl_config: configure_ssl(&arg_matches)?,
    };
    let state = ProxyState {
        conf,
        waiters: Mutex::new(HashMap::new()),
    };
    let state: &'static ProxyState = Box::leak(Box::new(state));

    println!("Version: {}", GIT_VERSION);

    // Check that we can bind to address before further initialization
    println!("Starting proxy on {}", state.conf.proxy_address);
    let pageserver_listener = tcp_listener::bind(state.conf.proxy_address)?;

    println!("Starting mgmt on {}", state.conf.mgmt_address);
    let mgmt_listener = tcp_listener::bind(state.conf.mgmt_address)?;

    let threads = [
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
