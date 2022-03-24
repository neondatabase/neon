//! Main entry point for the Page Server executable.

use std::{env, path::Path, str::FromStr};
use tracing::*;
use zenith_utils::{
    auth::JwtAuth,
    logging,
    postgres_backend::AuthType,
    tcp_listener,
    zid::{ZTenantId, ZTimelineId},
    GIT_VERSION,
};

use anyhow::{bail, Context, Result};

use clap::{App, Arg};
use daemonize::Daemonize;

use pageserver::{
    config::{defaults::*, PageServerConf},
    http, page_cache, page_service,
    remote_storage::{self, SyncStartupData},
    repository::{Repository, TimelineSyncStatusUpdate},
    tenant_mgr, thread_mgr,
    thread_mgr::ThreadKind,
    timelines, virtual_file, LOG_FILE_NAME,
};
use zenith_utils::http::endpoint;
use zenith_utils::postgres_backend;
use zenith_utils::shutdown::exit_now;
use zenith_utils::signals::{self, Signal};

fn main() -> Result<()> {
    zenith_metrics::set_common_metrics_prefix("pageserver");
    let arg_matches = App::new("Zenith page server")
        .about("Materializes WAL stream to pages and serves them to the postgres")
        .version(GIT_VERSION)
        .arg(
            Arg::new("daemonize")
                .short('d')
                .long("daemonize")
                .takes_value(false)
                .help("Run in the background"),
        )
        .arg(
            Arg::new("init")
                .long("init")
                .takes_value(false)
                .help("Initialize pageserver service: creates an initial config, tenant and timeline, if specified"),
        )
        .arg(
            Arg::new("workdir")
                .short('D')
                .long("workdir")
                .takes_value(true)
                .help("Working directory for the pageserver"),
        )
        .arg(
            Arg::new("create-tenant")
                .long("create-tenant")
                .takes_value(true)
                .help("Create tenant during init")
                .requires("init"),
        )
        .arg(
            Arg::new("initial-timeline-id")
                .long("initial-timeline-id")
                .takes_value(true)
                .help("Use a specific timeline id during init and tenant creation")
                .requires("create-tenant"),
        )
        // See `settings.md` for more details on the extra configuration patameters pageserver can process
        .arg(
            Arg::new("config-override")
                .short('c')
                .takes_value(true)
                .number_of_values(1)
                .multiple_occurrences(true)
                .help("Additional configuration overrides of the ones from the toml config file (or new ones to add there).
                Any option has to be a valid toml document, example: `-c=\"foo='hey'\"` `-c=\"foo={value=1}\"`"),
        )
        .get_matches();

    let workdir = Path::new(arg_matches.value_of("workdir").unwrap_or(".zenith"));
    let workdir = workdir
        .canonicalize()
        .with_context(|| format!("Error opening workdir '{}'", workdir.display()))?;
    let cfg_file_path = workdir.join("pageserver.toml");

    let init = arg_matches.is_present("init");
    let create_tenant = arg_matches
        .value_of("create-tenant")
        .map(ZTenantId::from_str)
        .transpose()
        .context("Failed to parse tenant id from the arguments")?;
    let initial_timeline_id = arg_matches
        .value_of("initial-timeline-id")
        .map(ZTimelineId::from_str)
        .transpose()
        .context("Failed to parse timeline id from the arguments")?;

    // Set CWD to workdir for non-daemon modes
    env::set_current_dir(&workdir).with_context(|| {
        format!(
            "Failed to set application's current dir to '{}'",
            workdir.display()
        )
    })?;

    let daemonize = arg_matches.is_present("daemonize");
    if init && daemonize {
        bail!("--daemonize cannot be used with --init")
    }

    let mut toml = if init {
        // We're initializing the repo, so there's no config file yet
        DEFAULT_CONFIG_FILE
            .parse::<toml_edit::Document>()
            .expect("could not parse built-in config file")
    } else {
        // Supplement the CLI arguments with the config file
        let cfg_file_contents = std::fs::read_to_string(&cfg_file_path)
            .with_context(|| format!("No pageserver config at '{}'", cfg_file_path.display()))?;
        cfg_file_contents
            .parse::<toml_edit::Document>()
            .with_context(|| {
                format!(
                    "Failed to read '{}' as pageserver config",
                    cfg_file_path.display()
                )
            })?
    };

    // Process any extra options given with -c
    if let Some(values) = arg_matches.values_of("config-override") {
        for option_line in values {
            let doc = toml_edit::Document::from_str(option_line).with_context(|| {
                format!(
                    "Option '{}' could not be parsed as a toml document",
                    option_line
                )
            })?;

            for (key, item) in doc.iter() {
                if key == "id" {
                    anyhow::ensure!(
                        init,
                        "node id can only be set during pageserver init and cannot be overridden"
                    );
                }
                toml.insert(key, item.clone());
            }
        }
    }
    trace!("Resulting toml: {}", toml);
    let conf = PageServerConf::parse_and_validate(&toml, &workdir)
        .context("Failed to parse pageserver configuration")?;

    // The configuration is all set up now. Turn it into a 'static
    // that can be freely stored in structs and passed across threads
    // as a ref.
    let conf: &'static PageServerConf = Box::leak(Box::new(conf));

    // Basic initialization of things that don't change after startup
    virtual_file::init(conf.max_file_descriptors);
    page_cache::init(conf.page_cache_size);

    // Create repo and exit if init was requested
    if init {
        timelines::init_pageserver(conf, create_tenant, initial_timeline_id)
            .context("Failed to init pageserver")?;
        // write the config file
        std::fs::write(&cfg_file_path, toml.to_string()).with_context(|| {
            format!(
                "Failed to initialize pageserver config at '{}'",
                cfg_file_path.display()
            )
        })?;
        Ok(())
    } else {
        start_pageserver(conf, daemonize).context("Failed to start pageserver")
    }
}

fn start_pageserver(conf: &'static PageServerConf, daemonize: bool) -> Result<()> {
    // Initialize logger
    let log_file = logging::init(LOG_FILE_NAME, daemonize)?;

    info!("version: {}", GIT_VERSION);

    // TODO: Check that it looks like a valid repository before going further

    // bind sockets before daemonizing so we report errors early and do not return until we are listening
    info!(
        "Starting pageserver http handler on {}",
        conf.listen_http_addr
    );
    let http_listener = tcp_listener::bind(conf.listen_http_addr.clone())?;

    info!(
        "Starting pageserver pg protocol handler on {}",
        conf.listen_pg_addr
    );
    let pageserver_listener = tcp_listener::bind(conf.listen_pg_addr.clone())?;

    // NB: Don't spawn any threads before daemonizing!
    if daemonize {
        info!("daemonizing...");

        // There shouldn't be any logging to stdin/stdout. Redirect it to the main log so
        // that we will see any accidental manual fprintf's or backtraces.
        let stdout = log_file.try_clone().unwrap();
        let stderr = log_file;

        let daemonize = Daemonize::new()
            .pid_file("pageserver.pid")
            .working_directory(".")
            .stdout(stdout)
            .stderr(stderr);

        // XXX: The parent process should exit abruptly right after
        // it has spawned a child to prevent coverage machinery from
        // dumping stats into a `profraw` file now owned by the child.
        // Otherwise, the coverage data will be damaged.
        match daemonize.exit_action(|| exit_now(0)).start() {
            Ok(_) => info!("Success, daemonized"),
            Err(err) => error!(%err, "could not daemonize"),
        }
    }

    let signals = signals::install_shutdown_handlers()?;

    // Initialize repositories with locally available timelines.
    // Timelines that are only partially available locally (remote storage has more data than this pageserver)
    // are scheduled for download and added to the repository once download is completed.
    let SyncStartupData {
        remote_index,
        local_timeline_init_statuses,
    } = remote_storage::start_local_timeline_sync(conf)
        .context("Failed to set up local files sync with external storage")?;

    for (tenant_id, local_timeline_init_statuses) in local_timeline_init_statuses {
        // initialize local tenant
        let repo = tenant_mgr::load_local_repo(conf, tenant_id, &remote_index);
        for (timeline_id, init_status) in local_timeline_init_statuses {
            match init_status {
                remote_storage::LocalTimelineInitStatus::LocallyComplete => {
                    debug!("timeline {} for tenant {} is locally complete, registering it in repository", tenant_id, timeline_id);
                    // Lets fail here loudly to be on the safe side.
                    // XXX: It may be a better api to actually distinguish between repository startup
                    //   and processing of newly downloaded timelines.
                    repo.apply_timeline_remote_sync_status_update(
                        timeline_id,
                        TimelineSyncStatusUpdate::Downloaded,
                    )
                    .with_context(|| {
                        format!(
                            "Failed to bootstrap timeline {} for tenant {}",
                            timeline_id, tenant_id
                        )
                    })?
                }
                remote_storage::LocalTimelineInitStatus::NeedsSync => {
                    debug!(
                        "timeline {} for tenant {} needs sync, \
                         so skipped for adding into repository until sync is finished",
                        tenant_id, timeline_id
                    );
                }
            }
        }
    }

    // initialize authentication for incoming connections
    let auth = match &conf.auth_type {
        AuthType::Trust | AuthType::MD5 => None,
        AuthType::ZenithJWT => {
            // unwrap is ok because check is performed when creating config, so path is set and file exists
            let key_path = conf.auth_validation_public_key_path.as_ref().unwrap();
            Some(JwtAuth::from_key_path(key_path)?.into())
        }
    };
    info!("Using auth: {:#?}", conf.auth_type);

    // Spawn a new thread for the http endpoint
    // bind before launching separate thread so the error reported before startup exits
    let auth_cloned = auth.clone();
    thread_mgr::spawn(
        ThreadKind::HttpEndpointListener,
        None,
        None,
        "http_endpoint_thread",
        move || {
            let router = http::make_router(conf, auth_cloned, remote_index);
            endpoint::serve_thread_main(router, http_listener, thread_mgr::shutdown_watcher())
        },
    )?;

    // Spawn a thread to listen for libpq connections. It will spawn further threads
    // for each connection.
    thread_mgr::spawn(
        ThreadKind::LibpqEndpointListener,
        None,
        None,
        "libpq endpoint thread",
        move || page_service::thread_main(conf, auth, pageserver_listener, conf.auth_type),
    )?;

    signals.handle(|signal| match signal {
        Signal::Quit => {
            info!(
                "Got {}. Terminating in immediate shutdown mode",
                signal.name()
            );
            std::process::exit(111);
        }

        Signal::Interrupt | Signal::Terminate => {
            info!(
                "Got {}. Terminating gracefully in fast shutdown mode",
                signal.name()
            );
            shutdown_pageserver();
            unreachable!()
        }
    })
}

fn shutdown_pageserver() {
    // Shut down the libpq endpoint thread. This prevents new connections from
    // being accepted.
    thread_mgr::shutdown_threads(Some(ThreadKind::LibpqEndpointListener), None, None);

    // Shut down any page service threads.
    postgres_backend::set_pgbackend_shutdown_requested();
    thread_mgr::shutdown_threads(Some(ThreadKind::PageRequestHandler), None, None);

    // Shut down all the tenants. This flushes everything to disk and kills
    // the checkpoint and GC threads.
    tenant_mgr::shutdown_all_tenants();

    // Stop syncing with remote storage.
    //
    // FIXME: Does this wait for the sync thread to finish syncing what's queued up?
    // Should it?
    thread_mgr::shutdown_threads(Some(ThreadKind::StorageSync), None, None);

    // Shut down the HTTP endpoint last, so that you can still check the server's
    // status while it's shutting down.
    thread_mgr::shutdown_threads(Some(ThreadKind::HttpEndpointListener), None, None);

    // There should be nothing left, but let's be sure
    thread_mgr::shutdown_threads(None, None, None);

    info!("Shut down successfully completed");
    std::process::exit(0);
}
