///
/// Postgres protocol proxy/router.
///
/// This service listens psql port and can check auth via external service
/// (control plane API in our case) and can create new databases and accounts
/// in somewhat transparent manner (again via communication with control plane API).
///
use std::{
    net::{SocketAddr, TcpListener},
    thread,
};

mod cplane_api;
mod proxy;

pub struct ProxyConf {
    /// main entrypoint for users to connect to
    pub proxy_address: SocketAddr,

    /// http management endpoint. Upon user account creation control plane
    /// will notify us here, so that we can 'unfreeze' user session.
    pub mgmt_address: SocketAddr,

    /// control plane address where we check auth and create clusters.
    pub cplane_address: SocketAddr,
}

fn main() -> anyhow::Result<()> {
    let conf = ProxyConf {
        proxy_address: "0.0.0.0:4000".parse()?,
        mgmt_address: "0.0.0.0:8080".parse()?,
        cplane_address: "127.0.0.1:3000".parse()?,
    };
    let conf: &'static ProxyConf = Box::leak(Box::new(conf));

    // Check that we can bind to address before further initialization
    println!("Starting proxy on {}", conf.proxy_address);
    let pageserver_listener = TcpListener::bind(conf.proxy_address)?;

    // Spawn a thread to listen for connections. It will spawn further threads
    // for each connection.
    let proxy_listener_thread = thread::Builder::new()
        .name("Proxy thread".into())
        .spawn(move || proxy::thread_main(&conf, pageserver_listener))?;

    proxy_listener_thread.join().unwrap()
}
