///
/// Postgres protocol proxy/router.
///
/// This service listens psql port and can check auth via external service
/// (control plane API in our case) and can create new databases and accounts
/// in somewhat transparent manner (again via communication with control plane API).
///
use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener},
    sync::{mpsc, Mutex},
    thread,
};

use clap::{App, Arg};

use cplane_api::DatabaseInfo;

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
    pub cplane_address: SocketAddr,
}

pub struct ProxyState {
    pub conf: ProxyConf,
    pub waiters: Mutex<HashMap<String, mpsc::Sender<anyhow::Result<DatabaseInfo>>>>,
}

fn main() -> anyhow::Result<()> {
    let arg_matches = App::new("Zenith proxy/router")
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
        .get_matches();

    let conf = ProxyConf {
        proxy_address: arg_matches.value_of("proxy").unwrap().parse()?,
        mgmt_address: arg_matches.value_of("mgmt").unwrap().parse()?,
        redirect_uri: arg_matches.value_of("uri").unwrap().parse()?,
        cplane_address: "127.0.0.1:3000".parse()?,
    };
    let state = ProxyState {
        conf,
        waiters: Mutex::new(HashMap::new()),
    };
    let state: &'static ProxyState = Box::leak(Box::new(state));

    // Check that we can bind to address before further initialization
    println!("Starting proxy on {}", state.conf.proxy_address);
    let pageserver_listener = TcpListener::bind(state.conf.proxy_address)?;

    println!("Starting mgmt on {}", state.conf.mgmt_address);
    let mgmt_listener = TcpListener::bind(state.conf.mgmt_address)?;

    let mut threads = Vec::new();

    // Spawn a thread to listen for connections. It will spawn further threads
    // for each connection.
    threads.push(
        thread::Builder::new()
            .name("Proxy thread".into())
            .spawn(move || proxy::thread_main(&state, pageserver_listener))?,
    );

    threads.push(
        thread::Builder::new()
            .name("Mgmt thread".into())
            .spawn(move || mgmt::thread_main(&state, mgmt_listener))?,
    );

    for t in threads {
        let _ = t.join().unwrap();
    }

    Ok(())
}
