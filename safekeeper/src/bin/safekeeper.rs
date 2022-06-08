//
// Main entry point for the safekeeper executable
//
use anyhow::{bail, Context, Result};
use clap::{App, Arg};
use const_format::formatcp;
use daemonize::Daemonize;
use fs2::FileExt;
use remote_storage::RemoteStorageConfig;
use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::thread;
use tokio::sync::mpsc;
use toml_edit::Document;
use tracing::*;
use url::{ParseError, Url};

use safekeeper::broker;
use safekeeper::control_file;
use safekeeper::defaults::{
    DEFAULT_HTTP_LISTEN_ADDR, DEFAULT_PG_LISTEN_ADDR, DEFAULT_WAL_BACKUP_RUNTIME_THREADS,
};
use safekeeper::http;
use safekeeper::remove_wal;
use safekeeper::timeline::GlobalTimelines;
use safekeeper::wal_backup;
use safekeeper::wal_service;
use safekeeper::SafeKeeperConf;
use utils::{
    http::endpoint, logging, project_git_version, shutdown::exit_now, signals, tcp_listener,
    zid::NodeId,
};

const LOCK_FILE_NAME: &str = "safekeeper.lock";
const ID_FILE_NAME: &str = "safekeeper.id";
project_git_version!(GIT_VERSION);

fn main() -> anyhow::Result<()> {
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
                .help("Dump control file at path specified by this argument and exit"),
        )
        .arg(
            Arg::new("id").long("id").takes_value(true).help("safekeeper node id: integer")
        ).arg(
            Arg::new("broker-endpoints")
            .long("broker-endpoints")
            .takes_value(true)
            .help("a comma separated broker (etcd) endpoints for storage nodes coordination, e.g. 'http://127.0.0.1:2379'"),
        )
        .arg(
            Arg::new("broker-etcd-prefix")
            .long("broker-etcd-prefix")
            .takes_value(true)
            .help("a prefix to always use when polling/pusing data in etcd from this safekeeper"),
        )
        .arg(
            Arg::new("wal-backup-threads").long("backup-threads").takes_value(true).help(formatcp!("number of threads for wal backup (default {DEFAULT_WAL_BACKUP_RUNTIME_THREADS}")),
        ).arg(
            Arg::new("remote-storage")
                .long("remote-storage")
                .takes_value(true)
                .help("Remote storage configuration for WAL backup (offloading to s3) as TOML inline table, e.g. {\"max_concurrent_syncs\" = 17, \"max_sync_errors\": 13, \"bucket_name\": \"<BUCKETNAME>\", \"bucket_region\":\"<REGION>\", \"concurrency_limit\": 119}.\nSafekeeper offloads WAL to [prefix_in_bucket/]<tenant_id>/<timeline_id>/<segment_file>, mirroring structure on the file system.")
        )
        .arg(
            Arg::new("enable-wal-backup")
                .long("enable-wal-backup")
                .takes_value(true)
                .default_value("true")
                .default_missing_value("true")
                .help("Enable/disable WAL backup to s3. When disabled, safekeeper removes WAL ignoring WAL backup horizon."),
        )
        .get_matches();

    if let Some(addr) = arg_matches.value_of("dump-control-file") {
        let state = control_file::FileStorage::load_control_file(Path::new(addr))?;
        let json = serde_json::to_string(&state)?;
        print!("{}", json);
        return Ok(());
    }

    let mut conf = SafeKeeperConf::default();

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

    if let Some(recall) = arg_matches.value_of("recall") {
        conf.recall_period = humantime::parse_duration(recall)?;
    }

    let mut given_id = None;
    if let Some(given_id_str) = arg_matches.value_of("id") {
        given_id = Some(NodeId(
            given_id_str
                .parse()
                .context("failed to parse safekeeper id")?,
        ));
    }

    if let Some(addr) = arg_matches.value_of("broker-endpoints") {
        let collected_ep: Result<Vec<Url>, ParseError> = addr.split(',').map(Url::parse).collect();
        conf.broker_endpoints = collected_ep.context("Failed to parse broker endpoint urls")?;
    }
    if let Some(prefix) = arg_matches.value_of("broker-etcd-prefix") {
        conf.broker_etcd_prefix = prefix.to_string();
    }

    if let Some(backup_threads) = arg_matches.value_of("wal-backup-threads") {
        conf.backup_runtime_threads = backup_threads
            .parse()
            .with_context(|| format!("Failed to parse backup threads {}", backup_threads))?;
    }
    if let Some(storage_conf) = arg_matches.value_of("remote-storage") {
        // funny toml doesn't consider plain inline table as valid document, so wrap in a key to parse
        let storage_conf_toml = format!("remote_storage = {}", storage_conf);
        let parsed_toml = storage_conf_toml.parse::<Document>()?; // parse
        let (_, storage_conf_parsed_toml) = parsed_toml.iter().next().unwrap(); // and strip key off again
        conf.remote_storage = Some(RemoteStorageConfig::from_toml(storage_conf_parsed_toml)?);
    }
    // Seems like there is no better way to accept bool values explicitly in clap.
    conf.wal_backup_enabled = arg_matches
        .value_of("enable-wal-backup")
        .unwrap()
        .parse()
        .context("failed to parse bool enable-s3-offload bool")?;

    start_safekeeper(conf, given_id, arg_matches.is_present("init"))
}

fn start_safekeeper(mut conf: SafeKeeperConf, given_id: Option<NodeId>, init: bool) -> Result<()> {
    let log_file = logging::init("safekeeper.log", conf.daemonize)?;

    info!("version: {GIT_VERSION}");

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
            Err(err) => bail!("Error: {err}. could not daemonize. bailing."),
        }
    }

    // Register metrics collector for active timelines. It's important to do this
    // after daemonizing, otherwise process collector will be upset.
    let registry = metrics::default_registry();
    let timeline_collector = safekeeper::metrics::TimelineCollector::new();
    registry.register(Box::new(timeline_collector))?;

    let signals = signals::install_shutdown_handlers()?;
    let mut threads = vec![];
    let (wal_backup_launcher_tx, wal_backup_launcher_rx) = mpsc::channel(100);
    GlobalTimelines::init(wal_backup_launcher_tx);

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

    let conf_cloned = conf.clone();
    let safekeeper_thread = thread::Builder::new()
        .name("Safekeeper thread".into())
        .spawn(|| {
            if let Err(e) = wal_service::thread_main(conf_cloned, pg_listener) {
                info!("safekeeper thread terminated: {e}");
            }
        })
        .unwrap();

    threads.push(safekeeper_thread);

    if !conf.broker_endpoints.is_empty() {
        let conf_ = conf.clone();
        threads.push(
            thread::Builder::new()
                .name("broker thread".into())
                .spawn(|| {
                    broker::thread_main(conf_);
                })?,
        );
    } else {
        warn!("No broker endpoints providing, starting without node sync")
    }

    let conf_ = conf.clone();
    threads.push(
        thread::Builder::new()
            .name("WAL removal thread".into())
            .spawn(|| {
                remove_wal::thread_main(conf_);
            })?,
    );

    let conf_ = conf.clone();
    threads.push(
        thread::Builder::new()
            .name("wal backup launcher thread".into())
            .spawn(move || {
                wal_backup::wal_backup_launcher_thread_main(conf_, wal_backup_launcher_rx);
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
fn set_id(conf: &mut SafeKeeperConf, given_id: Option<NodeId>) -> Result<()> {
    let id_file_path = conf.workdir.join(ID_FILE_NAME);

    let my_id: NodeId;
    // If ID exists, read it in; otherwise set one passed
    match fs::read(&id_file_path) {
        Ok(id_serialized) => {
            my_id = NodeId(
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
