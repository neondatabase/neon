//
// Main entry point for the wal_acceptor executable
//
use daemonize::Daemonize;
use log::*;
use parse_duration::parse;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use std::{fs::File, fs::OpenOptions};

use anyhow::Result;
use clap::{App, Arg};

use slog::Drain;

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
            Arg::with_name("systemid")
                .long("systemid")
                .takes_value(true)
                .required(true)
                .help("PostgreSQL system id, from pg_control"),
        )
        .arg(
            Arg::with_name("listen")
                .short("l")
                .long("listen")
                .takes_value(true)
                .help("listen for incoming connections on ip:port (default: 127.0.0.1:5454)"),
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

    let systemid_str = arg_matches.value_of("systemid").unwrap();
    let systemid: u64 = systemid_str.parse()?;

    let mut conf = WalAcceptorConf {
        data_dir: PathBuf::from("./"),
        systemid,
        daemonize: false,
        no_sync: false,
        pageserver_addr: None,
        listen_addr: "127.0.0.1:5454".parse()?,
        ttl: None,
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

    if let Some(addr) = arg_matches.value_of("listen") {
        conf.listen_addr = addr.parse().unwrap();
    }

    if let Some(addr) = arg_matches.value_of("pageserver") {
        conf.pageserver_addr = Some(addr.parse().unwrap());
    }

    if let Some(ttl) = arg_matches.value_of("ttl") {
        conf.ttl = Some::<Duration>(parse(ttl)?);
    }

    start_wal_acceptor(conf)
}

fn start_wal_acceptor(conf: WalAcceptorConf) -> Result<()> {
    // Initialize logger
    let _scope_guard = init_logging(&conf)?;
    let _log_guard = slog_stdlog::init().unwrap();
    // Note: this `info!(...)` macro comes from `log` crate
    info!("standard logging redirected to slog");

    if conf.daemonize {
        info!("daemonizing...");

        // There should'n be any logging to stdin/stdout. Redirect it to the main log so
        // that we will see any accidental manual fprintf's or backtraces.
        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open("wal_acceptor.log")
            .unwrap();
        let stderr = OpenOptions::new()
            .create(true)
            .append(true)
            .open("wal_acceptor.log")
            .unwrap();

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
            wal_service::thread_main(conf);
        })
        .unwrap();
    threads.push(wal_acceptor_thread);

    for t in threads {
        t.join().unwrap()
    }
    Ok(())
}

fn init_logging(conf: &WalAcceptorConf) -> Result<slog_scope::GlobalLoggerGuard, io::Error> {
    if conf.daemonize {
        let log = conf.data_dir.join("wal_acceptor.log");
        let log_file = File::create(&log).map_err(|err| {
            // We failed to initialize logging, so we can't log this message with error!
            eprintln!("Could not create log file {:?}: {}", log, err);
            err
        })?;
        let decorator = slog_term::PlainSyncDecorator::new(log_file);
        let drain = slog_term::CompactFormat::new(decorator).build();
        let drain = std::sync::Mutex::new(drain).fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        Ok(slog_scope::set_global_logger(logger))
    } else {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).chan_size(1000).build().fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        Ok(slog_scope::set_global_logger(logger))
    }
}
