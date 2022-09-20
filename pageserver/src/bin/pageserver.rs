//! Main entry point for the Page Server executable.

use remote_storage::GenericRemoteStorage;
use std::{env, ops::ControlFlow, path::Path, str::FromStr};
use tracing::*;

use anyhow::{anyhow, bail, Context, Result};

use clap::{App, Arg};
use daemonize::Daemonize;

use fail::FailScenario;
use pageserver::{
    config::{defaults::*, PageServerConf},
    http, page_cache, page_service, profiling, task_mgr,
    task_mgr::TaskKind,
    task_mgr::{
        BACKGROUND_RUNTIME, COMPUTE_REQUEST_RUNTIME, MGMT_REQUEST_RUNTIME, WALRECEIVER_RUNTIME,
    },
    tenant_mgr, virtual_file, LOG_FILE_NAME,
};
use utils::{
    auth::JwtAuth,
    logging,
    postgres_backend::AuthType,
    project_git_version,
    shutdown::exit_now,
    signals::{self, Signal},
    tcp_listener,
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
    let arg_matches = App::new("Neon page server")
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
                .help("Initialize pageserver with all given config overrides"),
        )
        .arg(
            Arg::new("workdir")
                .short('D')
                .long("workdir")
                .takes_value(true)
                .help("Working directory for the pageserver"),
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
        .arg(Arg::new("update-config").long("update-config").takes_value(false).help(
            "Update the config file when started",
        ))
        .arg(
            Arg::new("enabled-features")
                .long("enabled-features")
                .takes_value(false)
                .help("Show enabled compile time features"),
        )
        .get_matches();

    if arg_matches.is_present("enabled-features") {
        let features: &[&str] = &[
            #[cfg(feature = "testing")]
            "testing",
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

    // Set CWD to workdir for non-daemon modes
    env::set_current_dir(&workdir).with_context(|| {
        format!(
            "Failed to set application's current dir to '{}'",
            workdir.display()
        )
    })?;

    let daemonize = arg_matches.is_present("daemonize");

    let conf = match initialize_config(&cfg_file_path, arg_matches, &workdir)? {
        ControlFlow::Continue(conf) => conf,
        ControlFlow::Break(()) => {
            info!("Pageserver config init successful");
            return Ok(());
        }
    };

    let tenants_path = conf.tenants_path();
    if !tenants_path.exists() {
        utils::crashsafe_dir::create_dir_all(conf.tenants_path()).with_context(|| {
            format!(
                "Failed to create tenants root dir at '{}'",
                tenants_path.display()
            )
        })?;
    }

    // Initialize up failpoints support
    let scenario = FailScenario::setup();

    // Basic initialization of things that don't change after startup
    virtual_file::init(conf.max_file_descriptors);
    page_cache::init(conf.page_cache_size);

    start_pageserver(conf, daemonize).context("Failed to start pageserver")?;

    scenario.teardown();
    Ok(())
}

fn initialize_config(
    cfg_file_path: &Path,
    arg_matches: clap::ArgMatches,
    workdir: &Path,
) -> anyhow::Result<ControlFlow<(), &'static PageServerConf>> {
    let init = arg_matches.is_present("init");
    let update_config = init || arg_matches.is_present("update-config");

    let (mut toml, config_file_exists) = if cfg_file_path.is_file() {
        if init {
            anyhow::bail!(
                "Config file '{}' already exists, cannot init it, use --update-config to update it",
                cfg_file_path.display()
            );
        }
        // Supplement the CLI arguments with the config file
        let cfg_file_contents = std::fs::read_to_string(&cfg_file_path).with_context(|| {
            format!(
                "Failed to read pageserver config at '{}'",
                cfg_file_path.display()
            )
        })?;
        (
            cfg_file_contents
                .parse::<toml_edit::Document>()
                .with_context(|| {
                    format!(
                        "Failed to parse '{}' as pageserver config",
                        cfg_file_path.display()
                    )
                })?,
            true,
        )
    } else if cfg_file_path.exists() {
        anyhow::bail!(
            "Config file '{}' exists but is not a regular file",
            cfg_file_path.display()
        );
    } else {
        // We're initializing the tenant, so there's no config file yet
        (
            DEFAULT_CONFIG_FILE
                .parse::<toml_edit::Document>()
                .context("could not parse built-in config file")?,
            false,
        )
    };

    if let Some(values) = arg_matches.values_of("config-override") {
        for option_line in values {
            let doc = toml_edit::Document::from_str(option_line).with_context(|| {
                format!(
                    "Option '{}' could not be parsed as a toml document",
                    option_line
                )
            })?;

            for (key, item) in doc.iter() {
                if config_file_exists && update_config && key == "id" && toml.contains_key(key) {
                    anyhow::bail!("Pageserver config file exists at '{}' and has node id already, it cannot be overridden", cfg_file_path.display());
                }
                toml.insert(key, item.clone());
            }
        }
    }

    debug!("Resulting toml: {toml}");
    let conf = PageServerConf::parse_and_validate(&toml, workdir)
        .context("Failed to parse pageserver configuration")?;

    if update_config {
        info!("Writing pageserver config to '{}'", cfg_file_path.display());

        std::fs::write(&cfg_file_path, toml.to_string()).with_context(|| {
            format!(
                "Failed to write pageserver config to '{}'",
                cfg_file_path.display()
            )
        })?;
        info!(
            "Config successfully written to '{}'",
            cfg_file_path.display()
        )
    }

    Ok(if init {
        ControlFlow::Break(())
    } else {
        ControlFlow::Continue(Box::leak(Box::new(conf)))
    })
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

    WALRECEIVER_RUNTIME.block_on(pageserver::walreceiver::init_etcd_client(conf))?;

    // initialize authentication for incoming connections
    let auth = match &conf.auth_type {
        AuthType::Trust | AuthType::MD5 => None,
        AuthType::NeonJWT => {
            // unwrap is ok because check is performed when creating config, so path is set and file exists
            let key_path = conf.auth_validation_public_key_path.as_ref().unwrap();
            Some(JwtAuth::from_key_path(key_path)?.into())
        }
    };
    info!("Using auth: {:#?}", conf.auth_type);

    let remote_storage = conf
        .remote_storage_config
        .as_ref()
        .map(|storage_config| {
            GenericRemoteStorage::from_config(conf.workdir.clone(), storage_config)
        })
        .transpose()
        .context("Failed to init generic remote storage")?;
    let remote_index = {
        let _rt_guard = BACKGROUND_RUNTIME.enter();
        tenant_mgr::init_tenant_mgr(conf, remote_storage.clone())?
    };

    // Spawn all HTTP related tasks in the MGMT_REQUEST_RUNTIME.
    // bind before launching separate thread so the error reported before startup exits

    // Create a Service from the router above to handle incoming requests.
    {
        let _rt_guard = MGMT_REQUEST_RUNTIME.enter();

        let router = http::make_router(conf, auth.clone(), remote_index, remote_storage)?;
        let service =
            utils::http::RouterService::new(router.build().map_err(|err| anyhow!(err))?).unwrap();
        let server = hyper::Server::from_tcp(http_listener)?
            .serve(service)
            .with_graceful_shutdown(task_mgr::shutdown_watcher());

        task_mgr::spawn(
            MGMT_REQUEST_RUNTIME.handle(),
            TaskKind::HttpEndpointListener,
            None,
            None,
            "http endpoint listener",
            true,
            async {
                server.await?;
                Ok(())
            },
        );
    }

    // Spawn a task to listen for libpq connections. It will spawn further tasks
    // for each connection.
    task_mgr::spawn(
        COMPUTE_REQUEST_RUNTIME.handle(),
        TaskKind::LibpqEndpointListener,
        None,
        None,
        "libpq endpoint listener",
        true,
        async move {
            page_service::libpq_listener_main(conf, auth, pageserver_listener, conf.auth_type).await
        },
    );

    // All started up! Now just sit and wait for shutdown signal.
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
            BACKGROUND_RUNTIME.block_on(pageserver::shutdown_pageserver(0));
            unreachable!()
        }
    })
}
