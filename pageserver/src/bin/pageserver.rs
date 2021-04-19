//
// Main entry point for the Page Server executable
//

use log::*;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::exit;
use std::thread;
use std::fs::OpenOptions;

use anyhow::{Context, Result};
use clap::{App, Arg};
use daemonize::Daemonize;

use slog::Drain;

use pageserver::page_service;
use pageserver::restore_datadir;
use pageserver::restore_s3;
use pageserver::tui;
use pageserver::walreceiver;
use pageserver::PageServerConf;

fn main() -> Result<()> {
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
                 .help("connect to the WAL sender (postgres or wal_acceptor) on connstr (default: 'host=127.0.0.1 port=65432 user=zenith')"))
        .arg(Arg::with_name("listen")
                 .short("l")
                 .long("listen")
                 .takes_value(true)
                 .help("listen for incoming page requests on ip:port (default: 127.0.0.1:5430)"))
        .arg(Arg::with_name("interactive")
                 .short("i")
                 .long("interactive")
                 .takes_value(false)
                 .help("Interactive mode"))
        .arg(Arg::with_name("daemonize")
                 .short("d")
                 .long("daemonize")
                 .takes_value(false)
                 .help("Run in the background"))
        .arg(Arg::with_name("restore_from")
                 .long("restore-from")
                 .takes_value(true)
                 .help("Upload data from s3 or datadir"))
        .get_matches();

    let mut conf = PageServerConf {
        data_dir: PathBuf::from("./"),
        daemonize: false,
        interactive: false,
        wal_producer_connstr: None,
        listen_addr: "127.0.0.1:5430".parse().unwrap(),
        restore_from: String::new(),
    };

    if let Some(dir) = arg_matches.value_of("datadir") {
        conf.data_dir = PathBuf::from(dir);
    }

    if arg_matches.is_present("daemonize") {
        conf.daemonize = true;
    }

    if arg_matches.is_present("interactive") {
        conf.interactive = true;
    }

    if conf.daemonize && conf.interactive {
        eprintln!("--daemonize is not allowed with --interactive: choose one");
        exit(1);
    }

    if let Some(restore_from) = arg_matches.value_of("restore_from") {
        conf.restore_from = String::from(restore_from);
    }

    if let Some(addr) = arg_matches.value_of("wal_producer") {
        conf.wal_producer_connstr = Some(String::from(addr));
    }

    if let Some(addr) = arg_matches.value_of("listen") {
        conf.listen_addr = addr.parse()?;
    }

    start_pageserver(&conf)
}

fn start_pageserver(conf: &PageServerConf) -> Result<()> {
    // Initialize logger
    let _scope_guard = init_logging(&conf)?;
    let _log_guard = slog_stdlog::init()?;

    // Note: this `info!(...)` macro comes from `log` crate
    info!("standard logging redirected to slog");

    let tui_thread: Option<thread::JoinHandle<()>>;
    if conf.interactive {
        // Initialize the UI
        tui_thread = Some(
            thread::Builder::new()
                .name("UI thread".into())
                .spawn(|| {
                    let _ = tui::ui_main();
                })
                .unwrap(),
        );
        //threads.push(tui_thread);
    } else {
        tui_thread = None;
    }

    if conf.daemonize {
        info!("daemonizing...");

        // There shouldn't be any logging to stdin/stdout. Redirect it to the main log so
        // that we will see any accidental manual fprintf's or backtraces.
        let log_filename = conf.data_dir.join("pageserver.log");
        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_filename)
            .with_context(|| format!("failed to open {:?}", log_filename))?;
        let stderr = stdout.try_clone()?;

        let daemonize = Daemonize::new()
            .pid_file(conf.data_dir.join("pageserver.pid"))
            .working_directory(conf.data_dir.clone())
            .stdout(stdout)
            .stderr(stderr);

        match daemonize.start() {
            Ok(_) => info!("Success, daemonized"),
            Err(e) => error!("Error, {}", e),
        }
    }

    let mut threads = Vec::new();

    info!("starting... {}", conf.restore_from);

    // Before opening up for connections, restore the latest base backup from S3.
    // (We don't persist anything to local disk at the moment, so we need to do
    // this at every startup)
    if conf.restore_from.eq("s3") {
        info!("restore-from s3...");
        restore_s3::restore_main(&conf);
    } else if conf.restore_from.eq("local") {
        info!("restore-from local...");
        restore_datadir::restore_main(&conf);
    }

    // Create directory for wal-redo datadirs
    match fs::create_dir(conf.data_dir.join("wal-redo")) {
        Ok(_) => {}
        Err(e) => match e.kind() {
            io::ErrorKind::AlreadyExists => {}
            _ => {
                anyhow::bail!("Failed to create wal-redo data directory: {}", e);
            }
        },
    }

    // Launch the WAL receiver thread if pageserver was started with --wal-producer
    // option. It will try to connect to the WAL safekeeper, and stream the WAL. If
    // the connection is lost, it will reconnect on its own. We just fire and forget
    // it here.
    //
    // All other wal receivers are started on demand by "callmemaybe" command
    // sent to pageserver.
    if let Some(wal_producer) = &conf.wal_producer_connstr {
        let conf_copy = conf.clone();
        let wal_producer = wal_producer.clone();
        let walreceiver_thread = thread::Builder::new()
            .name("static WAL receiver thread".into())
            .spawn(move || {
                walreceiver::thread_main(&conf_copy, &wal_producer);
            })
            .unwrap();
        threads.push(walreceiver_thread);
    }

    // GetPage@LSN requests are served by another thread. (It uses async I/O,
    // but the code in page_service sets up it own thread pool for that)
    let conf_copy = conf.clone();
    let page_server_thread = thread::Builder::new()
        .name("Page Service thread".into())
        .spawn(move || {
            // thread code
            page_service::thread_main(&conf_copy);
        })
        .unwrap();
    threads.push(page_server_thread);

    if tui_thread.is_some() {
        // The TUI thread exits when the user asks to Quit.
        tui_thread.unwrap().join().unwrap();
    } else {
        // In non-interactive mode, wait forever.
        for t in threads {
            t.join().unwrap()
        }
    }
    Ok(())
}

fn init_logging(conf: &PageServerConf) -> Result<slog_scope::GlobalLoggerGuard, io::Error> {
    if conf.interactive {
        Ok(tui::init_logging())
    } else if conf.daemonize {
        let log = conf.data_dir.join("pageserver.log");
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
			.unwrap_or_else(|_| {
				eprintln!("Could not create log file {:?}: {}", log, err);
				err
        })?;

        let decorator = slog_term::PlainSyncDecorator::new(log_file);
        let drain = slog_term::CompactFormat::new(decorator).build();
        let drain = slog::Filter::new(drain, |record: &slog::Record| {
            if record.level().is_at_least(slog::Level::Info) {
                return true;
            }
            return false;
        });
        let drain = std::sync::Mutex::new(drain).fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        Ok(slog_scope::set_global_logger(logger))
    } else {
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).chan_size(1000).build().fuse();
        let drain = slog::Filter::new(drain, |record: &slog::Record| {
            if record.level().is_at_least(slog::Level::Info) {
                return true;
            }
            if record.level().is_at_least(slog::Level::Debug)
                && record.module().starts_with("pageserver")
            {
                return true;
            }
            return false;
        })
        .fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        Ok(slog_scope::set_global_logger(logger))
    }
}
