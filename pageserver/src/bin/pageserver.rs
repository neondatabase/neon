//! Main entry point for the Page Server executable.

use std::{env, path::Path, str::FromStr};
use tracing::*;

use anyhow::{bail, Context, Result};

use clap::{App, Arg};
use daemonize::Daemonize;

use fail::FailScenario;
use pageserver::{
    config::{defaults::*, PageServerConf},
    http, page_cache, page_service, profiling, tenant_mgr, thread_mgr,
    thread_mgr::ThreadKind,
    timelines, virtual_file, LOG_FILE_NAME,
};
use utils::{
    auth::JwtAuth,
    http::endpoint,
    logging,
    postgres_backend::AuthType,
    project_git_version,
    shutdown::exit_now,
    signals::{self, Signal},
    tcp_listener,
    zid::{ZTenantId, ZTimelineId},
};

project_git_version!(GIT_VERSION);

fn version() -> String {
    format!(
        "{GIT_VERSION} profiling:{} failpoints:{}",
        cfg!(feature = "profiling"),
        fail::has_failpoints()
    )
}

fn main() -> anyhow::Result<()> {
    let arg_matches = App::new("Zenith page server")
        .about("Materializes WAL stream to pages and serves them to the postgres")
        .version(&*version())
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
        .arg(
            Arg::new("enabled-features")
                .long("enabled-features")
                .takes_value(false)
                .help("Show enabled compile time features"),
        )
        .get_matches();

    if arg_matches.is_present("enabled-features") {
        let features: &[&str] = &[
            #[cfg(feature = "failpoints")]
            "failpoints",
            #[cfg(feature = "profiling")]
            "profiling",
        ];
        println!("{{\"features\": {features:?} }}");
        return Ok(());
    }

    let workdir = Path::new(arg_matches.value_of("workdir").unwrap_or(".neon"));
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
            .context("could not parse built-in config file")?
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

    // Initialize up failpoints support
    let scenario = FailScenario::setup();

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
    } else {
        start_pageserver(conf, daemonize).context("Failed to start pageserver")?;
    }

    scenario.teardown();
    Ok(())
}

fn start_pageserver(conf: &'static PageServerConf, daemonize: bool) -> Result<()> {
    // Initialize logger
    let log_file = logging::init(LOG_FILE_NAME, daemonize)?;

    info!("version: {GIT_VERSION}");

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
        let stdout = log_file
            .try_clone()
            .with_context(|| format!("Failed to clone log file '{:?}'", log_file))?;
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
            Err(err) => bail!("{err}. could not daemonize. bailing."),
        }
    }

    let signals = signals::install_shutdown_handlers()?;

    // start profiler (if enabled)
    let profiler_guard = profiling::init_profiler(conf);

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

    let remote_index = tenant_mgr::init_tenant_mgr(conf)?;

    // Spawn a new thread for the http endpoint
    // bind before launching separate thread so the error reported before startup exits
    let auth_cloned = auth.clone();
    thread_mgr::spawn(
        ThreadKind::HttpEndpointListener,
        None,
        None,
        "http_endpoint_thread",
        true,
        move || {
            let router = http::make_router(conf, auth_cloned, remote_index)?;
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
        true,
        move || page_service::thread_main(conf, auth, pageserver_listener, conf.auth_type),
    )?;

    signals.handle(|signal| match signal {
        Signal::Quit => {
            info!(
                "Got {}. Terminating in immediate shutdown mode",
                signal.name()
            );
            profiling::exit_profiler(conf, &profiler_guard);
            std::process::exit(111);
        }

        Signal::Interrupt | Signal::Terminate => {
            info!(
                "Got {}. Terminating gracefully in fast shutdown mode",
                signal.name()
            );
            profiling::exit_profiler(conf, &profiler_guard);
            pageserver::shutdown_pageserver(0);
            unreachable!()
        }
    })
}
