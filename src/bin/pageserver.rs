//
// Main entry point for the Page Server executable
//

use log::*;
use std::io::Error;
use std::{net::IpAddr, thread};

use clap::{App, Arg};

use pageserver::page_service;
use pageserver::restore_s3;
use pageserver::walreceiver;
use pageserver::walredo;
use pageserver::PageServerConf;

fn main() -> Result<(), Error> {
    let arg_matches = App::new("Zenith page server")
        .about("Materializes WAL stream to pages and serves them to the postgres")
        .arg(Arg::with_name("datadir")
                 .short("D")
                 .long("dir")
                 .takes_value(true)
                 .help("Path to the page server data directory"))
        .arg(Arg::with_name("wal_producer")
                 .short("w")
                 .long("wal-producer")
                 .takes_value(true)
                 .help("connect to the WAL sender (postgres or wal_acceptor) on ip:port (default: 127.0.0.1:65432)"))
        .arg(Arg::with_name("daemonize")
                 .short("d")
                 .long("daemonize")
                 .takes_value(false)
                 .help("Run in the background"))
        .arg(Arg::with_name("skip_recovery")
                 .long("skip-recovery")
                 .takes_value(false)
                 .help("Skip S3 recovery procedy and start empty"))
        .get_matches();

    let mut conf = PageServerConf {
        data_dir: String::from("."),
        daemonize: false,
        wal_producer_ip: "127.0.0.1".parse::<IpAddr>().unwrap(),
        wal_producer_port: 65432,
        skip_recovery: false,
    };

    if let Some(dir) = arg_matches.value_of("datadir") {
        conf.data_dir = String::from(dir);
    }

    if arg_matches.is_present("daemonize") {
        conf.daemonize = true;
    }

    if arg_matches.is_present("skip_recovery") {
        conf.skip_recovery = true;
    }

    if let Some(addr) = arg_matches.value_of("wal_producer") {
        let parts: Vec<&str> = addr.split(':').collect();
        conf.wal_producer_ip = parts[0].parse().unwrap();
        conf.wal_producer_port = parts[1].parse().unwrap();
    }

    start_pageserver(conf)
}

fn start_pageserver(conf: PageServerConf) -> Result<(), Error> {
    let mut threads = Vec::new();

    // Initialize logger
    stderrlog::new()
        .verbosity(3)
        .module("pageserver")
        .init()
        .unwrap();
    info!("starting...");

    // Initialize the WAL applicator
    let walredo_thread = thread::Builder::new()
        .name("WAL redo thread".into())
        .spawn(|| {
            walredo::wal_applicator_main();
        })
        .unwrap();
    threads.push(walredo_thread);

    // Before opening up for connections, restore the latest base backup from S3.
    // (We don't persist anything to local disk at the moment, so we need to do
    // this at every startup)
    if !conf.skip_recovery {
        restore_s3::restore_main();
    }

    // Launch the WAL receiver thread. It will try to connect to the WAL safekeeper,
    // and stream the WAL. If the connection is lost, it will reconnect on its own.
    // We just fire and forget it here.
    let walreceiver_thread = thread::Builder::new()
        .name("WAL receiver thread".into())
        .spawn(|| {
            // thread code
            walreceiver::thread_main(conf);
        })
        .unwrap();
    threads.push(walreceiver_thread);

    // GetPage@LSN requests are served by another thread. (It uses async I/O,
    // but the code in page_service sets up it own thread pool for that)
    let page_server_thread = thread::Builder::new()
        .name("Page Service thread".into())
        .spawn(|| {
            // thread code
            page_service::thread_main();
        })
        .unwrap();
    threads.push(page_server_thread);

    // never returns.
    for t in threads {
        t.join().unwrap()
    }
    Ok(())
}
