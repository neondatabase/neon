//! Main entry point for the Page Server executable.

use std::{env, path::Path, str::FromStr, thread};
use tracing::*;
use zenith_utils::{auth::JwtAuth, logging, postgres_backend::AuthType, tcp_listener, GIT_VERSION};

use anyhow::{bail, Context, Result};

use clap::{App, Arg};
use daemonize::Daemonize;

use pageserver::{
    branches,
    config::{defaults::*, PageServerConf},
    http, page_cache, page_service, remote_storage, tenant_mgr, virtual_file, LOG_FILE_NAME,
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
            Arg::with_name("workdir")
                .short("D")
                .long("workdir")
                .takes_value(true)
                .help("Working directory for the pageserver"),
        )
        .arg(
            Arg::with_name("create-tenant")
                .long("create-tenant")
                .takes_value(true)
                .help("Create tenant during init")
                .requires("init"),
        )
        // See `settings.md` for more details on the extra configuration patameters pageserver can process
        .arg(
            Arg::with_name("config-option")
                .short("c")
                .takes_value(true)
                .number_of_values(1)
                .multiple(true)
                .help("Additional configuration options or overrides of the ones from the toml config file.
                Any option has to be a valid toml document, example: `-c \"foo='hey'\"` `-c \"foo={value=1}\"`"),
        )
        .get_matches();

    let workdir = Path::new(arg_matches.value_of("workdir").unwrap_or(".zenith"));
    let workdir = workdir
        .canonicalize()
        .with_context(|| format!("Error opening workdir '{}'", workdir.display()))?;
    let cfg_file_path = workdir.join("pageserver.toml");

    let init = arg_matches.is_present("init");
    let create_tenant = arg_matches.value_of("create-tenant");

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
    if let Some(values) = arg_matches.values_of("config-option") {
        for option_line in values {
            let doc = toml_edit::Document::from_str(option_line).with_context(|| {
                format!(
                    "Option '{}' could not be parsed as a toml document",
                    option_line
                )
            })?;
            for (key, item) in doc.iter() {
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

    page_cache::init(conf);

    // Create repo and exit if init was requested
    if init {
        branches::init_pageserver(conf, create_tenant).context("Failed to init pageserver")?;
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

    // XXX: Don't spawn any threads before daemonizing!
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
    let mut threads = Vec::new();

    let sync_startup = remote_storage::start_local_timeline_sync(conf)
        .context("Failed to set up local files sync with external storage")?;

    if let Some(handle) = sync_startup.sync_loop_handle {
        threads.push(handle);
    }

    // Initialize tenant manager.
    tenant_mgr::set_timeline_states(conf, sync_startup.initial_timeline_states);

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
    let cloned = auth.clone();
    threads.push(
        thread::Builder::new()
            .name("http_endpoint_thread".into())
            .spawn(move || {
                let router = http::make_router(conf, cloned);
                endpoint::serve_thread_main(router, http_listener)
            })?,
    );

    // Spawn a thread to listen for connections. It will spawn further threads
    // for each connection.
    threads.push(
        thread::Builder::new()
            .name("Page Service thread".into())
            .spawn(move || {
                page_service::thread_main(conf, auth, pageserver_listener, conf.auth_type)
            })?,
    );

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

            postgres_backend::set_pgbackend_shutdown_requested();
            tenant_mgr::shutdown_all_tenants()?;
            endpoint::shutdown();

            for handle in std::mem::take(&mut threads) {
                handle
                    .join()
                    .expect("thread panicked")
                    .expect("thread exited with an error");
            }

            info!("Shut down successfully completed");
            std::process::exit(0);
        }
    })
}
