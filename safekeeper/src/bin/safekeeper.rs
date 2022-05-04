//
// Main entry point for the safekeeper executable
//
use anyhow::{bail, Context, Result};
use clap::{App, Arg};
use const_format::formatcp;
use daemonize::Daemonize;
use fs2::FileExt;
use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::thread;
use tokio::sync::mpsc;
use tracing::*;
use url::{ParseError, Url};

use safekeeper::control_file::{self};
use safekeeper::defaults::{DEFAULT_HTTP_LISTEN_ADDR, DEFAULT_PG_LISTEN_ADDR};
use safekeeper::remove_wal;
use safekeeper::wal_service;
use safekeeper::SafeKeeperConf;
use safekeeper::{broker, callmemaybe};
use safekeeper::{http, s3_offload};
use utils::{
    http::endpoint, logging, shutdown::exit_now, signals, tcp_listener, zid::ZNodeId, GIT_VERSION,
};

const LOCK_FILE_NAME: &str = "safekeeper.lock";
const ID_FILE_NAME: &str = "safekeeper.id";

fn main() -> Result<()> {
    metrics::set_common_metrics_prefix("safekeeper");
    let arg_matches = App::new("Zenith safekeeper")
        .about("Store WAL stream to local file system and push it to WAL receivers")
        .version(GIT_VERSION)
        .arg(
            Arg::new("datadir")
                .short('D')
                .long("dir")
                .takes_value(true)
                .help("Path to the safekeeper data directory"),
        )
        .arg(
            Arg::new("init")
                .long("init")
                .takes_value(false)
                .help("Initialize safekeeper with ID"),
        )
        .arg(
            Arg::new("listen-pg")
                .short('l')
                .long("listen-pg")
                .alias("listen") // for compatibility
                .takes_value(true)
                .help(formatcp!("listen for incoming WAL data connections on ip:port (default: {DEFAULT_PG_LISTEN_ADDR})")),
        )
        .arg(
            Arg::new("listen-http")
                .long("listen-http")
                .takes_value(true)
                .help(formatcp!("http endpoint address for metrics on ip:port (default: {DEFAULT_HTTP_LISTEN_ADDR})")),
        )
        // FIXME this argument is no longer needed since pageserver address is forwarded from compute.
        // However because this argument is in use by console's e2e tests lets keep it for now and remove separately.
        // So currently it is a noop.
        .arg(
            Arg::new("pageserver")
                .short('p')
                .long("pageserver")
                .takes_value(true),
        )
        .arg(
            Arg::new("ttl")
                .long("ttl")
                .takes_value(true)
                .help("interval for keeping WAL at safekeeper node, after which them will be uploaded to S3 and removed locally"),
        )
        .arg(
            Arg::new("recall")
                .long("recall")
                .takes_value(true)
                .help("Period for requestion pageserver to call for replication"),
        )
        .arg(
            Arg::new("daemonize")
                .short('d')
                .long("daemonize")
                .takes_value(false)
                .help("Run in the background"),
        )
        .arg(
            Arg::new("no-sync")
                .short('n')
                .long("no-sync")
                .takes_value(false)
                .help("Do not wait for changes to be written safely to disk"),
        )
        .arg(
            Arg::new("dump-control-file")
                .long("dump-control-file")
                .takes_value(true)
                .help("Dump control file at path specifed by this argument and exit"),
        )
        .arg(
            Arg::new("id").long("id").takes_value(true).help("safekeeper node id: integer")
        ).arg(
            Arg::new("broker-endpoints")
            .long("broker-endpoints")
            .takes_value(true)
            .help("a comma separated broker (etcd) endpoints for storage nodes coordination, e.g. 'http://127.0.0.1:2379'"),
        )
        .get_matches();

    if let Some(addr) = arg_matches.value_of("dump-control-file") {
        let state = control_file::FileStorage::load_control_file(Path::new(addr))?;
        let json = serde_json::to_string(&state)?;
        print!("{}", json);
        return Ok(());
    }

    let mut conf: SafeKeeperConf = Default::default();

    if let Some(dir) = arg_matches.value_of("datadir") {
        // change into the data directory.
        std::env::set_current_dir(PathBuf::from(dir))?;
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

    if let Some(ttl) = arg_matches.value_of("ttl") {
        conf.ttl = Some(humantime::parse_duration(ttl)?);
    }

    if let Some(recall) = arg_matches.value_of("recall") {
        conf.recall_period = humantime::parse_duration(recall)?;
    }

    let mut given_id = None;
    if let Some(given_id_str) = arg_matches.value_of("id") {
        given_id = Some(ZNodeId(
            given_id_str
                .parse()
                .context("failed to parse safekeeper id")?,
        ));
    }

    if let Some(addr) = arg_matches.value_of("broker-endpoints") {
        let collected_ep: Result<Vec<Url>, ParseError> = addr.split(',').map(Url::parse).collect();
        conf.broker_endpoints = Some(collected_ep?);
    }

    start_safekeeper(conf, given_id, arg_matches.is_present("init"))
}

fn start_safekeeper(mut conf: SafeKeeperConf, given_id: Option<ZNodeId>, init: bool) -> Result<()> {
    let log_file = logging::init("safekeeper.log", conf.daemonize)?;

    info!("version: {}", GIT_VERSION);

    // Prevent running multiple safekeepers on the same directory
    let lock_file_path = conf.workdir.join(LOCK_FILE_NAME);
    let lock_file = File::create(&lock_file_path).context("failed to open lockfile")?;
    lock_file.try_lock_exclusive().with_context(|| {
        format!(
            "control file {} is locked by some other process",
            lock_file_path.display()
        )
    })?;

    // Set or read our ID.
    set_id(&mut conf, given_id)?;
    if init {
        return Ok(());
    }

    let http_listener = tcp_listener::bind(conf.listen_http_addr.clone()).map_err(|e| {
        error!("failed to bind to address {}: {}", conf.listen_http_addr, e);
        e
    })?;

    info!("Starting safekeeper on {}", conf.listen_pg_addr);
    let pg_listener = tcp_listener::bind(conf.listen_pg_addr.clone()).map_err(|e| {
        error!("failed to bind to address {}: {}", conf.listen_pg_addr, e);
        e
    })?;

    // XXX: Don't spawn any threads before daemonizing!
    if conf.daemonize {
        info!("daemonizing...");

        // There should'n be any logging to stdin/stdout. Redirect it to the main log so
        // that we will see any accidental manual fprintf's or backtraces.
        let stdout = log_file.try_clone().unwrap();
        let stderr = log_file;

        let daemonize = Daemonize::new()
            .pid_file("safekeeper.pid")
            .working_directory(Path::new("."))
            .stdout(stdout)
            .stderr(stderr);

        // XXX: The parent process should exit abruptly right after
        // it has spawned a child to prevent coverage machinery from
        // dumping stats into a `profraw` file now owned by the child.
        // Otherwise, the coverage data will be damaged.
        match daemonize.exit_action(|| exit_now(0)).start() {
            Ok(_) => info!("Success, daemonized"),
            Err(e) => error!("Error, {}", e),
        }
    }

    let signals = signals::install_shutdown_handlers()?;
    let mut threads = vec![];

    let conf_ = conf.clone();
    threads.push(
        thread::Builder::new()
            .name("http_endpoint_thread".into())
            .spawn(|| {
                // TODO authentication
                let router = http::make_router(conf_);
                endpoint::serve_thread_main(
                    router,
                    http_listener,
                    std::future::pending(), // never shut down
                )
                .unwrap();
            })?,
    );

    if conf.ttl.is_some() {
        let conf_ = conf.clone();
        threads.push(
            thread::Builder::new()
                .name("S3 offload thread".into())
                .spawn(|| {
                    s3_offload::thread_main(conf_);
                })?,
        );
    }

    let (tx, rx) = mpsc::unbounded_channel();
    let conf_cloned = conf.clone();
    let safekeeper_thread = thread::Builder::new()
        .name("Safekeeper thread".into())
        .spawn(|| {
            // thread code
            let thread_result = wal_service::thread_main(conf_cloned, pg_listener, tx);
            if let Err(e) = thread_result {
                info!("safekeeper thread terminated: {}", e);
            }
        })
        .unwrap();

    threads.push(safekeeper_thread);

    let conf_cloned = conf.clone();
    let callmemaybe_thread = thread::Builder::new()
        .name("callmemaybe thread".into())
        .spawn(|| {
            // thread code
            let thread_result = callmemaybe::thread_main(conf_cloned, rx);
            if let Err(e) = thread_result {
                error!("callmemaybe thread terminated: {}", e);
            }
        })
        .unwrap();
    threads.push(callmemaybe_thread);

    if conf.broker_endpoints.is_some() {
        let conf_ = conf.clone();
        threads.push(
            thread::Builder::new()
                .name("broker thread".into())
                .spawn(|| {
                    broker::thread_main(conf_);
                })?,
        );
    }

    let conf_ = conf.clone();
    threads.push(
        thread::Builder::new()
            .name("WAL removal thread".into())
            .spawn(|| {
                remove_wal::thread_main(conf_);
            })?,
    );

    // TODO: put more thoughts into handling of failed threads
    // We probably should restart them.

    // NOTE: we still have to handle signals like SIGQUIT to prevent coredumps
    signals.handle(|signal| {
        // TODO: implement graceful shutdown with joining threads etc
        info!(
            "Got {}. Terminating in immediate shutdown mode",
            signal.name()
        );
        std::process::exit(111);
    })
}

/// Determine safekeeper id and set it in config.
fn set_id(conf: &mut SafeKeeperConf, given_id: Option<ZNodeId>) -> Result<()> {
    let id_file_path = conf.workdir.join(ID_FILE_NAME);

    let my_id: ZNodeId;
    // If ID exists, read it in; otherwise set one passed
    match fs::read(&id_file_path) {
        Ok(id_serialized) => {
            my_id = ZNodeId(
                std::str::from_utf8(&id_serialized)
                    .context("failed to parse safekeeper id")?
                    .parse()
                    .context("failed to parse safekeeper id")?,
            );
            if let Some(given_id) = given_id {
                if given_id != my_id {
                    bail!(
                        "safekeeper already initialized with id {}, can't set {}",
                        my_id,
                        given_id
                    );
                }
            }
            info!("safekeeper ID {}", my_id);
        }
        Err(error) => match error.kind() {
            ErrorKind::NotFound => {
                my_id = if let Some(given_id) = given_id {
                    given_id
                } else {
                    bail!("safekeeper id is not specified");
                };
                let mut f = File::create(&id_file_path)?;
                f.write_all(my_id.to_string().as_bytes())?;
                f.sync_all()?;
                info!("initialized safekeeper ID {}", my_id);
            }
            _ => {
                return Err(error.into());
            }
        },
    }
    conf.my_id = my_id;
    Ok(())
}
