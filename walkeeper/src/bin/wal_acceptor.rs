//
// Main entry point for the wal_acceptor executable
//
use anyhow::Result;
use clap::{App, Arg};
use const_format::formatcp;
use daemonize::Daemonize;
use log::*;
use std::env;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::thread;
use zenith_utils::http::endpoint;
use zenith_utils::logging;

use walkeeper::defaults::{DEFAULT_HTTP_LISTEN_ADDR, DEFAULT_PG_LISTEN_ADDR};
use walkeeper::s3_offload;
use walkeeper::wal_service;
use walkeeper::WalAcceptorConf;

fn main() -> Result<()> {
    let arg_matches = App::new("Zenith wal_acceptor")
        .about("Store WAL stream to local file system and push it to WAL receivers")
        .arg(
            Arg::with_name("datadir")
                .short("D")
                .long("dir")
                .takes_value(true)
                .help("Path to the WAL acceptor data directory"),
        )
        .arg(
            Arg::with_name("listen-pg")
                .short("l")
                .long("listen-pg")
                .alias("listen") // for compatibility
                .takes_value(true)
                .help(formatcp!("listen for incoming WAL data connections on ip:port (default: {DEFAULT_PG_LISTEN_ADDR})")),
        )
        .arg(
            Arg::with_name("listen-http")
                .long("listen-http")
                .takes_value(true)
                .help(formatcp!("http endpoint address for metrics on ip:port (default: {DEFAULT_HTTP_LISTEN_ADDR})")),
        )
        .arg(
            Arg::with_name("pageserver")
                .short("p")
                .long("pageserver")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("ttl")
                .long("ttl")
                .takes_value(true)
                .help("interval for keeping WAL as walkeeper node, after which them will be uploaded to S3 and removed locally"),
        )
        .arg(
            Arg::with_name("recall")
                .long("recall")
                .takes_value(true)
                .help("Period for requestion pageserver to call for replication"),
        )
        .arg(
            Arg::with_name("daemonize")
                .short("d")
                .long("daemonize")
                .takes_value(false)
                .help("Run in the background"),
        )
        .arg(
            Arg::with_name("no-sync")
                .short("n")
                .long("no-sync")
                .takes_value(false)
                .help("Do not wait for changes to be written safely to disk"),
        )
        .get_matches();

    let mut conf = WalAcceptorConf {
        data_dir: PathBuf::from("./"),
        daemonize: false,
        no_sync: false,
        pageserver_addr: None,
        listen_pg_addr: DEFAULT_PG_LISTEN_ADDR.to_string(),
        listen_http_addr: DEFAULT_HTTP_LISTEN_ADDR.to_string(),
        ttl: None,
        recall_period: None,
        pageserver_auth_token: env::var("PAGESERVER_AUTH_TOKEN").ok(),
    };

    if let Some(dir) = arg_matches.value_of("datadir") {
        conf.data_dir = PathBuf::from(dir);

        // change into the data directory.
        std::env::set_current_dir(&conf.data_dir)?;
    }

    if arg_matches.is_present("no-sync") {
        conf.no_sync = true;
    }

    if arg_matches.is_present("daemonize") {
        conf.daemonize = true;
    }

    if let Some(addr) = arg_matches.value_of("listen-pg") {
        conf.listen_pg_addr = addr.to_owned();
    }

    if let Some(addr) = arg_matches.value_of("listen-http") {
        conf.listen_http_addr = addr.to_owned();
    }

    if let Some(addr) = arg_matches.value_of("pageserver") {
        conf.pageserver_addr = Some(addr.to_owned());
    }

    if let Some(ttl) = arg_matches.value_of("ttl") {
        conf.ttl = Some(humantime::parse_duration(ttl)?);
    }

    if let Some(recall) = arg_matches.value_of("recall") {
        conf.recall_period = Some(humantime::parse_duration(recall)?);
    }

    start_wal_acceptor(conf)
}

fn start_wal_acceptor(conf: WalAcceptorConf) -> Result<()> {
    let log_filename = conf.data_dir.join("wal_acceptor.log");
    let (_scope_guard, log_file) = logging::init(log_filename, conf.daemonize)?;

    let http_listener = TcpListener::bind(conf.listen_http_addr.clone()).map_err(|e| {
        error!("failed to bind to address {}: {}", conf.listen_http_addr, e);
        e
    })?;

    if conf.daemonize {
        info!("daemonizing...");

        // There should'n be any logging to stdin/stdout. Redirect it to the main log so
        // that we will see any accidental manual fprintf's or backtraces.
        let stdout = log_file.try_clone().unwrap();
        let stderr = log_file;

        let daemonize = Daemonize::new()
            .pid_file("wal_acceptor.pid")
            .working_directory(Path::new("."))
            .stdout(stdout)
            .stderr(stderr);

        match daemonize.start() {
            Ok(_) => info!("Success, daemonized"),
            Err(e) => error!("Error, {}", e),
        }
    }

    let mut threads = Vec::new();

    let http_endpoint_thread = thread::Builder::new()
        .name("http_endpoint_thread".into())
        .spawn(move || {
            // TODO(yeputons): maybe add auth
            let router = endpoint::make_router();
            endpoint::serve_thread_main(router, http_listener).unwrap();
        })
        .unwrap();
    threads.push(http_endpoint_thread);

    if conf.ttl.is_some() {
        let s3_conf = conf.clone();
        let s3_offload_thread = thread::Builder::new()
            .name("S3 offload thread".into())
            .spawn(|| {
                // thread code
                s3_offload::thread_main(s3_conf);
            })
            .unwrap();
        threads.push(s3_offload_thread);
    }

    let wal_acceptor_thread = thread::Builder::new()
        .name("WAL acceptor thread".into())
        .spawn(|| {
            // thread code
            let thread_result = wal_service::thread_main(conf);
            if let Err(e) = thread_result {
                info!("wal_service thread terminated: {}", e);
            }
        })
        .unwrap();
    threads.push(wal_acceptor_thread);

    for t in threads {
        t.join().unwrap()
    }
    Ok(())
}
