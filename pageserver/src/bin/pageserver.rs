//
// Main entry point for the Page Server executable
//

use log::*;
use serde::{Deserialize, Serialize};
use std::{
    env,
    net::{TcpListener},
    path::{Path, PathBuf},
    process::exit,
    thread,
    time::Duration,
};

use anyhow::{Result};
use clap::{App, Arg, ArgMatches};
use daemonize::Daemonize;

use pageserver::{branches, page_cache, page_service, tui, logger, PageServerConf};

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:64000";

const DEFAULT_GC_HORIZON: u64 = 64 * 1024 * 1024;
const DEFAULT_GC_PERIOD: Duration = Duration::from_secs(100);

const DEFAULT_SUPERUSER: &str = "zenith_admin";

/// String arguments that can be declared via CLI or config file
#[derive(Serialize, Deserialize)]
struct CfgFileParams {
    listen_addr: Option<String>,
    gc_horizon: Option<String>,
    gc_period: Option<String>,
    pg_distrib_dir: Option<String>,
}

impl CfgFileParams {
    /// Extract string arguments from CLI
    fn from_args(arg_matches: &ArgMatches) -> Self {
        let get_arg = |arg_name: &str| -> Option<String> {
            arg_matches.value_of(arg_name).map(str::to_owned)
        };

        Self {
            listen_addr: get_arg("listen"),
            gc_horizon: get_arg("gc_horizon"),
            gc_period: get_arg("gc_period"),
            pg_distrib_dir: get_arg("postgres-distrib"),
        }
    }

    /// Fill missing values in `self` with `other`
    fn or(self, other: CfgFileParams) -> Self {
        // TODO cleaner way to do this
        Self {
            listen_addr: self.listen_addr.or(other.listen_addr),
            gc_horizon: self.gc_horizon.or(other.gc_horizon),
            gc_period: self.gc_period.or(other.gc_period),
            pg_distrib_dir: self.pg_distrib_dir.or(other.pg_distrib_dir),
        }
    }

    /// Create a PageServerConf from these string parameters
    fn try_into_config(&self) -> Result<PageServerConf> {
        let listen_addr = match self.listen_addr.as_ref() {
            Some(addr) => addr.clone(),
            None => DEFAULT_LISTEN_ADDR.to_owned(),
        };

        let gc_horizon: u64 = match self.gc_horizon.as_ref() {
            Some(horizon_str) => horizon_str.parse()?,
            None => DEFAULT_GC_HORIZON,
        };
        let gc_period = match self.gc_period.as_ref() {
            Some(period_str) => humantime::parse_duration(period_str)?,
            None => DEFAULT_GC_PERIOD,
        };

        let pg_distrib_dir = match self.pg_distrib_dir.as_ref() {
            Some(pg_distrib_dir_str) => PathBuf::from(pg_distrib_dir_str),
            None => env::current_dir()?.join("tmp_install"),
        };

        if !pg_distrib_dir.join("bin/postgres").exists() {
            anyhow::bail!("Can't find postgres binary at {:?}", pg_distrib_dir);
        }

        Ok(PageServerConf {
            daemonize: false,
            interactive: false,

            listen_addr,
            gc_horizon,
            gc_period,

            superuser: String::from(DEFAULT_SUPERUSER),

            workdir: PathBuf::from("."),

            pg_distrib_dir,
        })
    }
}

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
        .arg(
            Arg::with_name("postgres-distrib")
                .long("postgres-distrib")
                .takes_value(true)
                .help("Postgres distribution directory"),
        )
        .get_matches();

    let workdir = Path::new(arg_matches.value_of("workdir").unwrap_or(".zenith"));
    let cfg_file_path = workdir.canonicalize()?.join("pageserver.toml");

    let args_params = CfgFileParams::from_args(&arg_matches);

    let init = arg_matches.is_present("init");
    let params = if init {
        // We're initializing the repo, so there's no config file yet
        args_params
    } else {
        // Supplement the CLI arguments with the config file
        let cfg_file_contents = std::fs::read_to_string(&cfg_file_path)?;
        let file_params: CfgFileParams = toml::from_str(&cfg_file_contents)?;
        args_params.or(file_params)
    };

    // Ensure the config is valid, even if just init-ing
    let mut conf = params.try_into_config()?;

    conf.daemonize = arg_matches.is_present("daemonize");
    conf.interactive = arg_matches.is_present("interactive");

    if init && (conf.daemonize || conf.interactive) {
        eprintln!("--daemonize and --interactive may not be used with --init");
        exit(1);
    }

    if conf.daemonize && conf.interactive {
        eprintln!("--daemonize is not allowed with --interactive: choose one");
        exit(1);
    }

    // The configuration is all set up now. Turn it into a 'static
    // that can be freely stored in structs and passed across threads
    // as a ref.
    let conf: &'static PageServerConf = Box::leak(Box::new(conf));

    // Create repo and exit if init was requested
    if init {
        branches::init_repo(conf, &workdir)?;

        // write the config file
        let cfg_file_contents = toml::to_string_pretty(&params)?;
        std::fs::write(&cfg_file_path, cfg_file_contents)?;

        return Ok(());
    }

    // Set CWD to workdir for non-daemon modes
    env::set_current_dir(&workdir)?;

    start_pageserver(conf)
}

fn start_pageserver(conf: &'static PageServerConf) -> Result<()> {

    // Initialize logger
    let (_scope_guard, log_file) = logger::init_logging(&conf, "pageserver.log")?;
    let _log_guard = slog_stdlog::init()?;

    // Note: this `info!(...)` macro comes from `log` crate
    info!("standard logging redirected to slog");

    let tui_thread = if conf.interactive {
        // Initialize the UI
        Some(
            thread::Builder::new()
                .name("UI thread".into())
                .spawn(|| {
                    let _ = tui::ui_main();
                })
                .unwrap(),
        )
    } else {
        None
    };

    // TODO: Check that it looks like a valid repository before going further

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

    // Check that we can bind to address before starting threads to simplify shutdown
    // sequence if port is occupied.
    info!("Starting pageserver on {}", conf.listen_addr);
    let pageserver_listener = TcpListener::bind(conf.listen_addr.clone())?;

    // Initialize page cache, this will spawn walredo_thread
    page_cache::init(conf);

    // Spawn a thread to listen for connections. It will spawn further threads
    // for each connection.
    let page_service_thread = thread::Builder::new()
        .name("Page Service thread".into())
        .spawn(move || page_service::thread_main(conf, pageserver_listener))?;

    if let Some(tui_thread) = tui_thread {
        // The TUI thread exits when the user asks to Quit.
        tui_thread.join().unwrap();
    } else {
        page_service_thread
            .join()
            .expect("Page service thread has panicked")?
    }

    Ok(())
}
