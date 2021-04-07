//
// Main entry point for the wal_acceptor executable
//
use daemonize::Daemonize;
use log::*;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::thread;
use std::{fs::File, fs::OpenOptions};

use clap::{App, Arg};

use slog;
use slog::Drain;
use slog_scope;
use slog_stdlog;

use walkeeper::wal_service;
use walkeeper::WalAcceptorConf;

fn main() -> Result<(), io::Error> {
    let arg_matches = App::new("Zenith wal_acceptor")
        .about("Store WAL stream to local file system and push it to WAL receivers")
        .arg(
            Arg::with_name("datadir")
                .short("D")
                .long("dir")
                .takes_value(true)
                .help("Path to the page server data directory"),
        )
        .arg(
            Arg::with_name("listen")
                .short("l")
                .long("listen")
                .takes_value(true)
                .help("listen for incoming page requests on ip:port (default: 127.0.0.1:5454)"),
        )
        .arg(
            Arg::with_name("pageserver")
                .short("p")
                .long("pageserver")
                .takes_value(true)
                .help("address ip:port of pageserver with which wal_acceptor should establish connection"),
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
        listen_addr: "127.0.0.1:5454".parse().unwrap(),
    };

    if let Some(dir) = arg_matches.value_of("datadir") {
        conf.data_dir = PathBuf::from(dir);
    }

    if arg_matches.is_present("no-sync") {
        conf.no_sync = true;
    }

    if arg_matches.is_present("daemonize") {
        conf.daemonize = true;
    }

    if let Some(addr) = arg_matches.value_of("listen") {
        conf.listen_addr = addr.parse().unwrap();
    }

    if let Some(addr) = arg_matches.value_of("pageserver") {
        conf.pageserver_addr = Some(addr.parse().unwrap());
    }

    start_wal_acceptor(conf)
}

fn start_wal_acceptor(conf: WalAcceptorConf) -> Result<(), io::Error> {
    // Initialize logger
    let _scope_guard = init_logging(&conf);
    let _log_guard = slog_stdlog::init().unwrap();
    // Note: this `info!(...)` macro comes from `log` crate
    info!("standard logging redirected to slog");

    if conf.daemonize {
        info!("daemonizing...");

        // There should'n be any logging to stdin/stdout. Redirect it to the main log so
        // that we will see any accidental manual fpritf's or backtraces.
        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(conf.data_dir.join("wal_acceptor.log"))
            .unwrap();
        let stderr = OpenOptions::new()
            .create(true)
            .append(true)
            .open(conf.data_dir.join("wal_acceptor.log"))
            .unwrap();

        let daemonize = Daemonize::new()
            .pid_file(conf.data_dir.join("wal_acceptor.pid"))
            .working_directory(Path::new("."))
            .stdout(stdout)
            .stderr(stderr);

        match daemonize.start() {
            Ok(_) => info!("Success, daemonized"),
            Err(e) => error!("Error, {}", e),
        }
    }

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

fn init_logging(conf: &WalAcceptorConf) -> slog_scope::GlobalLoggerGuard {
    if conf.daemonize {
        let log = conf.data_dir.join("wal_acceptor.log");
        let log_file = File::create(log).unwrap_or_else(|_| panic!("Could not create log file"));
        let decorator = slog_term::PlainSyncDecorator::new(log_file);
        let drain = slog_term::CompactFormat::new(decorator).build();
        let drain = std::sync::Mutex::new(drain).fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        slog_scope::set_global_logger(logger)
    } else {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).chan_size(1000).build().fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        return slog_scope::set_global_logger(logger);
    }
}
