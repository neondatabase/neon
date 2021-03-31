//
// Main entry point for the wal_acceptor executable
//
use log::*;
use std::io;
use std::path::PathBuf;
use std::thread;

use clap::{App, Arg};

use slog;
use slog_stdlog;
use slog_scope;
use slog::Drain;

use pageserver::wal_service;
use pageserver::WalAcceptorConf;

fn main() -> Result<(), io::Error> {
    let arg_matches = App::new("Zenith wal_acceptor")
        .about("Store WAL stream to local file system and push it to WAL receivers")
        .arg(Arg::with_name("datadir")
             .short("D")
             .long("dir")
             .takes_value(true)
             .help("Path to the page server data directory"))
		.arg(Arg::with_name("listen")
             .short("l")
             .long("listen")
             .takes_value(true)
             .help("listen for incoming page requests on ip:port (default: 127.0.0.1:5430)"))
        .arg(Arg::with_name("no-sync")
             .short("n")
             .long("no-sync")
             .takes_value(false)
             .help("Do not wait for changes to be written safely to disk"))
        .get_matches();

	let mut conf = WalAcceptorConf {
        data_dir: PathBuf::from("./"),
        no_sync: false,
        listen_addr: "127.0.0.1:5454".parse().unwrap()
    };

    if let Some(dir) = arg_matches.value_of("datadir") {
        conf.data_dir = PathBuf::from(dir);
    }

    if arg_matches.is_present("no-sync") {
        conf.no_sync = true;
    }

    if let Some(addr) = arg_matches.value_of("listen") {
        conf.listen_addr = addr.parse().unwrap();
    }

	start_wal_acceptor(conf)
}

fn start_wal_acceptor(conf: WalAcceptorConf) -> Result<(), io::Error> {
    // Initialize logger
    let _scope_guard = init_noninteractive_logging();
    let _log_guard = slog_stdlog::init().unwrap();
    // Note: this `info!(...)` macro comes from `log` crate
    info!("standard logging redirected to slog");

	let mut threads = Vec::new();
	let wal_acceptor_thread = thread::Builder::new()
        .name("WAL acceptor thread".into())
        .spawn(|| {
            // thread code
            wal_service::thread_main(conf);
        })
        .unwrap();
    threads.push(wal_acceptor_thread);

    for t in threads {
        t.join().unwrap()
    }
    Ok(())
}

fn init_noninteractive_logging() -> slog_scope::GlobalLoggerGuard {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).chan_size(1000).build().fuse();
    let logger = slog::Logger::root(drain, slog::o!());
    return slog_scope::set_global_logger(logger);
}
