//
// Main entry point for the Page Server executable
//

use log::*;
use parse_duration::parse;
use std::fs::{File, OpenOptions};
use std::io;
use std::process::exit;
use std::thread;
use std::time::Duration;
use std::{env, path::PathBuf};

use anyhow::{Context, Result};
use clap::{App, Arg};
use daemonize::Daemonize;

use slog::{Drain, FnValue};

use pageserver::{branches, page_cache, page_service, tui, PageServerConf};

const DEFAULT_GC_HORIZON: u64 = 64 * 1024 * 1024;
const DEFAULT_GC_PERIOD_SEC: u64 = 10;
//const DEFAULT_GC_HORIZON: u64 = 1024 * 1024 * 1024;
//const DEFAULT_GC_PERIOD_SEC: u64 = 600;

fn main() -> Result<()> {
    let arg_matches = App::new("Zenith page server")
        .about("Materializes WAL stream to pages and serves them to the postgres")
        .arg(
            Arg::with_name("listen")
                .short("l")
                .long("listen")
                .takes_value(true)
                .help("listen for incoming page requests on ip:port (default: 127.0.0.1:5430)"),
        )
        .arg(
            Arg::with_name("interactive")
                .short("i")
                .long("interactive")
                .takes_value(false)
                .help("Interactive mode"),
        )
        .arg(
            Arg::with_name("daemonize")
                .short("d")
                .long("daemonize")
                .takes_value(false)
                .help("Run in the background"),
        )
        .arg(
            Arg::with_name("init")
                .long("init")
                .takes_value(false)
                .help("Initialize pageserver repo"),
        )
        .arg(
            Arg::with_name("gc_horizon")
                .long("gc_horizon")
                .takes_value(true)
                .help("Distance from current LSN to perform all wal records cleanup"),
        )
        .arg(
            Arg::with_name("gc_period")
                .long("gc_period")
                .takes_value(true)
                .help("Interval between garbage collector iterations"),
        )
        .arg(
            Arg::with_name("workdir")
                .short("D")
                .long("workdir")
                .takes_value(true)
                .help("Working directory for the pageserver"),
        )
        .get_matches();

    let workdir = if let Some(workdir_arg) = arg_matches.value_of("workdir") {
        PathBuf::from(workdir_arg)
    } else if let Some(workdir_arg) = std::env::var_os("ZENITH_REPO_DIR") {
        PathBuf::from(workdir_arg.to_str().unwrap())
    } else {
        PathBuf::from(".zenith")
    };

    let pg_distrib_dir: PathBuf = {
        if let Some(postgres_bin) = env::var_os("POSTGRES_DISTRIB_DIR") {
            postgres_bin.into()
        } else {
            let cwd = env::current_dir()?;
            cwd.join("tmp_install")
        }
    };

    if !pg_distrib_dir.join("bin/postgres").exists() {
        anyhow::bail!("Can't find postgres binary at {:?}", pg_distrib_dir);
    }

    let mut conf = PageServerConf {
        daemonize: false,
        interactive: false,
        gc_horizon: DEFAULT_GC_HORIZON,
        gc_period: Duration::from_secs(DEFAULT_GC_PERIOD_SEC),
        listen_addr: "127.0.0.1:64000".parse().unwrap(),
        // we will change the current working directory to the repository below,
        // so always set 'workdir' to '.'
        workdir: PathBuf::from("."),
        pg_distrib_dir,
    };

    // Create repo and exit if init was requested
    if arg_matches.is_present("init") {
        branches::init_repo(&conf, &workdir)?;
        return Ok(());
    }

    // Set CWD to workdir for non-daemon modes
    env::set_current_dir(&workdir)?;

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

    if let Some(addr) = arg_matches.value_of("listen") {
        conf.listen_addr = addr.parse()?;
    }

    if let Some(horizon) = arg_matches.value_of("gc_horizon") {
        conf.gc_horizon = horizon.parse()?;
    }

    if let Some(period) = arg_matches.value_of("gc_period") {
        conf.gc_period = parse(period)?;
    }

    start_pageserver(&conf)
}

fn start_pageserver(conf: &PageServerConf) -> Result<()> {
    let log_filename = "pageserver.log";
    // Don't open the same file for output multiple times;
    // the different fds could overwrite each other's output.
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_filename)
        .with_context(|| format!("failed to open {:?}", &log_filename))?;

    // Initialize logger
    let logger_file = log_file.try_clone().unwrap();
    let _scope_guard = init_logging(&conf, logger_file)?;
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

        // There should'n be any logging to stdin/stdout. Redirect it to the main log so
        // that we will see any accidental manual fprintf's or backtraces.
        let stdout = log_file.try_clone().unwrap();
        let stderr = log_file;

        let daemonize = Daemonize::new()
            .pid_file("pageserver.pid")
            .working_directory(".")
            .stdout(stdout)
            .stderr(stderr);

        match daemonize.start() {
            Ok(_) => info!("Success, daemonized"),
            Err(e) => error!("Error, {}", e),
        }
    }

    let mut threads = Vec::new();

    // TODO: Check that it looks like a valid repository before going further

    page_cache::init(conf);

    // Spawn a thread to listen for connections. It will spawn further threads
    // for each connection.
    let conf_copy = conf.clone();
    let page_server_thread = thread::Builder::new()
        .name("Page Service thread".into())
        .spawn(move || {
            // thread code
            page_service::thread_main(&conf_copy);
        })
        .unwrap();
    threads.push(page_server_thread);

    if let Some(tui_thread) = tui_thread {
        // The TUI thread exits when the user asks to Quit.
        tui_thread.join().unwrap();
    } else {
        // In non-interactive mode, wait forever.
        for t in threads {
            t.join().unwrap()
        }
    }
    Ok(())
}

fn init_logging(
    conf: &PageServerConf,
    log_file: File,
) -> Result<slog_scope::GlobalLoggerGuard, io::Error> {
    if conf.interactive {
        Ok(tui::init_logging())
    } else if conf.daemonize {
        let decorator = slog_term::PlainSyncDecorator::new(log_file);
        let drain = slog_term::FullFormat::new(decorator).build();
        let drain = slog::Filter::new(drain, |record: &slog::Record| {
            if record.level().is_at_least(slog::Level::Info) {
                return true;
            }
            false
        });
        let drain = std::sync::Mutex::new(drain).fuse();
        let logger = slog::Logger::root(
            drain,
            slog::o!(
                "location" =>
                FnValue(move |record| {
                    format!("{}, {}:{}",
                            record.module(),
                            record.file(),
                            record.line()
                            )
                    }
                )
            ),
        );
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
            false
        })
        .fuse();
        let logger = slog::Logger::root(drain, slog::o!());
        Ok(slog_scope::set_global_logger(logger))
    }
}
