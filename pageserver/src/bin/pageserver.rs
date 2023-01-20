//! Main entry point for the Page Server executable.

use std::env::{var, VarError};
use std::sync::Arc;
use std::{env, ops::ControlFlow, path::Path, str::FromStr};

use anyhow::{anyhow, Context};
use clap::{Arg, ArgAction, Command};
use fail::FailScenario;
use remote_storage::GenericRemoteStorage;
use tracing::*;

use metrics::set_build_info_metric;
use pageserver::{
    config::{defaults::*, PageServerConf},
    context::{DownloadBehavior, RequestContext},
    http, page_cache, page_service, task_mgr,
    task_mgr::TaskKind,
    task_mgr::{
        BACKGROUND_RUNTIME, COMPUTE_REQUEST_RUNTIME, MGMT_REQUEST_RUNTIME, WALRECEIVER_RUNTIME,
    },
    tenant::mgr,
    virtual_file,
};
use utils::{
    auth::JwtAuth,
    logging,
    postgres_backend::AuthType,
    project_git_version,
    sentry_init::{init_sentry, release_name},
    signals::{self, Signal},
    tcp_listener,
};

project_git_version!(GIT_VERSION);

const PID_FILE_NAME: &str = "pageserver.pid";

const FEATURES: &[&str] = &[
    #[cfg(feature = "testing")]
    "testing",
    #[cfg(feature = "fail/failpoints")]
    "fail/failpoints",
];

fn version() -> String {
    format!(
        "{GIT_VERSION} failpoints: {}, features: {:?}",
        fail::has_failpoints(),
        FEATURES,
    )
}

fn main() -> anyhow::Result<()> {
    let arg_matches = cli().get_matches();

    if arg_matches.get_flag("enabled-features") {
        println!("{{\"features\": {FEATURES:?} }}");
        return Ok(());
    }

    let workdir = arg_matches
        .get_one::<String>("workdir")
        .map(Path::new)
        .unwrap_or_else(|| Path::new(".neon"));
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

    let conf = match initialize_config(&cfg_file_path, arg_matches, &workdir)? {
        ControlFlow::Continue(conf) => conf,
        ControlFlow::Break(()) => {
            info!("Pageserver config init successful");
            return Ok(());
        }
    };

    // initialize sentry if SENTRY_DSN is provided
    let _sentry_guard = init_sentry(release_name!(), &[("node_id", &conf.id.to_string())]);

    let tenants_path = conf.tenants_path();
    if !tenants_path.exists() {
        utils::crashsafe::create_dir_all(conf.tenants_path()).with_context(|| {
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

    start_pageserver(conf).context("Failed to start pageserver")?;

    scenario.teardown();
    Ok(())
}

fn initialize_config(
    cfg_file_path: &Path,
    arg_matches: clap::ArgMatches,
    workdir: &Path,
) -> anyhow::Result<ControlFlow<(), &'static PageServerConf>> {
    let init = arg_matches.get_flag("init");
    let update_config = init || arg_matches.get_flag("update-config");

    let (mut toml, config_file_exists) = if cfg_file_path.is_file() {
        if init {
            anyhow::bail!(
                "Config file '{}' already exists, cannot init it, use --update-config to update it",
                cfg_file_path.display()
            );
        }
        // Supplement the CLI arguments with the config file
        let cfg_file_contents = std::fs::read_to_string(cfg_file_path).with_context(|| {
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

    if let Some(values) = arg_matches.get_many::<String>("config-override") {
        for option_line in values {
            let doc = toml_edit::Document::from_str(option_line).with_context(|| {
                format!("Option '{option_line}' could not be parsed as a toml document")
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

        std::fs::write(cfg_file_path, toml.to_string()).with_context(|| {
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

fn start_pageserver(conf: &'static PageServerConf) -> anyhow::Result<()> {
    // Initialize logging
    logging::init(conf.log_format)?;

    // Print version to the log, and expose it as a prometheus metric too.
    info!("version: {}", version());
    set_build_info_metric(GIT_VERSION);

    // If any failpoints were set from FAILPOINTS environment variable,
    // print them to the log for debugging purposes
    let failpoints = fail::list();
    if !failpoints.is_empty() {
        info!(
            "started with failpoints: {}",
            failpoints
                .iter()
                .map(|(name, actions)| format!("{name}={actions}"))
                .collect::<Vec<String>>()
                .join(";")
        )
    }

    // Create and lock PID file. This ensures that there cannot be more than one
    // pageserver process running at the same time.
    let lock_file_path = conf.workdir.join(PID_FILE_NAME);
    let lock_file =
        utils::pid_file::claim_for_current_process(&lock_file_path).context("claim pid file")?;
    info!("Claimed pid file at {lock_file_path:?}");

    // Ensure that the lock file is held even if the main thread of the process panics.
    // We need to release the lock file only when the process exits.
    std::mem::forget(lock_file);

    // Bind the HTTP and libpq ports early, so that if they are in use by some other
    // process, we error out early.
    let http_addr = &conf.listen_http_addr;
    info!("Starting pageserver http handler on {http_addr}");
    let http_listener = tcp_listener::bind(http_addr)?;

    let pg_addr = &conf.listen_pg_addr;
    info!("Starting pageserver pg protocol handler on {pg_addr}");
    let pageserver_listener = tcp_listener::bind(pg_addr)?;

    // Install signal handlers
    let signals = signals::install_shutdown_handlers()?;

    // Launch broker client
    WALRECEIVER_RUNTIME.block_on(pageserver::walreceiver::init_broker_client(conf))?;

    // Initialize authentication for incoming connections
    let auth = match &conf.auth_type {
        AuthType::Trust => None,
        AuthType::NeonJWT => {
            // unwrap is ok because check is performed when creating config, so path is set and file exists
            let key_path = conf.auth_validation_public_key_path.as_ref().unwrap();
            Some(JwtAuth::from_key_path(key_path)?.into())
        }
    };
    info!("Using auth: {:#?}", conf.auth_type);

    // TODO: remove ZENITH_AUTH_TOKEN once it's not used anywhere in development/staging/prod configuration.
    match (var("ZENITH_AUTH_TOKEN"), var("NEON_AUTH_TOKEN")) {
        (old, Ok(v)) => {
            info!("Loaded JWT token for authentication with Safekeeper");
            if let Ok(v_old) = old {
                warn!(
                    "JWT token for Safekeeper is specified twice, ZENITH_AUTH_TOKEN is deprecated"
                );
                if v_old != v {
                    warn!("JWT token for Safekeeper has two different values, choosing NEON_AUTH_TOKEN");
                }
            }
            pageserver::config::SAFEKEEPER_AUTH_TOKEN
                .set(Arc::new(v))
                .map_err(|_| anyhow!("Could not initialize SAFEKEEPER_AUTH_TOKEN"))?;
        }
        (Ok(v), _) => {
            info!("Loaded JWT token for authentication with Safekeeper");
            warn!("Please update pageserver configuration: the JWT token should be NEON_AUTH_TOKEN, not ZENITH_AUTH_TOKEN");
            pageserver::config::SAFEKEEPER_AUTH_TOKEN
                .set(Arc::new(v))
                .map_err(|_| anyhow!("Could not initialize SAFEKEEPER_AUTH_TOKEN"))?;
        }
        (_, Err(VarError::NotPresent)) => {
            info!("No JWT token for authentication with Safekeeper detected");
        }
        (_, Err(e)) => {
            return Err(e).with_context(|| {
                "Failed to either load to detect non-present NEON_AUTH_TOKEN environment variable"
            })
        }
    };

    // Set up remote storage client
    let remote_storage = create_remote_storage_client(conf)?;

    // Scan the local 'tenants/' directory and start loading the tenants
    BACKGROUND_RUNTIME.block_on(mgr::init_tenant_mgr(conf, remote_storage.clone()))?;

    // Start up the service to handle HTTP mgmt API request. We created the
    // listener earlier already.
    {
        let _rt_guard = MGMT_REQUEST_RUNTIME.enter();

        let router = http::make_router(conf, auth.clone(), remote_storage)?
            .build()
            .map_err(|err| anyhow!(err))?;
        let service = utils::http::RouterService::new(router).unwrap();
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

        if let Some(metric_collection_endpoint) = &conf.metric_collection_endpoint {
            let metrics_ctx = RequestContext::todo_child(
                TaskKind::MetricsCollection,
                // This task itself shouldn't download anything.
                // The actual size calculation does need downloads, and
                // creates a child context with the right DownloadBehavior.
                DownloadBehavior::Error,
            );
            task_mgr::spawn(
                MGMT_REQUEST_RUNTIME.handle(),
                TaskKind::MetricsCollection,
                None,
                None,
                "consumption metrics collection",
                true,
                async move {
                    pageserver::consumption_metrics::collect_metrics(
                        metric_collection_endpoint,
                        conf.metric_collection_interval,
                        conf.synthetic_size_calculation_interval,
                        conf.id,
                        metrics_ctx,
                    )
                    .instrument(info_span!("metrics_collection"))
                    .await?;
                    Ok(())
                },
            );
        }
    }

    // Spawn a task to listen for libpq connections. It will spawn further tasks
    // for each connection. We created the listener earlier already.
    {
        let libpq_ctx = RequestContext::todo_child(
            TaskKind::LibpqEndpointListener,
            // listener task shouldn't need to download anything. (We will
            // create a separate sub-contexts for each connection, with their
            // own download behavior. This context is used only to listen and
            // accept connections.)
            DownloadBehavior::Error,
        );
        task_mgr::spawn(
            COMPUTE_REQUEST_RUNTIME.handle(),
            TaskKind::LibpqEndpointListener,
            None,
            None,
            "libpq endpoint listener",
            true,
            async move {
                page_service::libpq_listener_main(
                    conf,
                    auth,
                    pageserver_listener,
                    conf.auth_type,
                    libpq_ctx,
                )
                .await
            },
        );
    }

    // All started up! Now just sit and wait for shutdown signal.
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
            BACKGROUND_RUNTIME.block_on(pageserver::shutdown_pageserver(0));
            unreachable!()
        }
    })
}

fn create_remote_storage_client(
    conf: &'static PageServerConf,
) -> anyhow::Result<Option<GenericRemoteStorage>> {
    let config = if let Some(config) = &conf.remote_storage_config {
        config
    } else {
        // No remote storage configured.
        return Ok(None);
    };

    // Create the client
    let mut remote_storage = GenericRemoteStorage::from_config(config)?;

    // If `test_remote_failures` is non-zero, wrap the client with a
    // wrapper that simulates failures.
    if conf.test_remote_failures > 0 {
        if !cfg!(feature = "testing") {
            anyhow::bail!("test_remote_failures option is not available because pageserver was compiled without the 'testing' feature");
        }
        info!(
            "Simulating remote failures for first {} attempts of each op",
            conf.test_remote_failures
        );
        remote_storage =
            GenericRemoteStorage::unreliable_wrapper(remote_storage, conf.test_remote_failures);
    }

    Ok(Some(remote_storage))
}

fn cli() -> Command {
    Command::new("Neon page server")
        .about("Materializes WAL stream to pages and serves them to the postgres")
        .version(version())
        .arg(
            Arg::new("init")
                .long("init")
                .action(ArgAction::SetTrue)
                .help("Initialize pageserver with all given config overrides"),
        )
        .arg(
            Arg::new("workdir")
                .short('D')
                .long("workdir")
                .help("Working directory for the pageserver"),
        )
        // See `settings.md` for more details on the extra configuration patameters pageserver can process
        .arg(
            Arg::new("config-override")
                .short('c')
                .num_args(1)
                .action(ArgAction::Append)
                .help("Additional configuration overrides of the ones from the toml config file (or new ones to add there). \
                Any option has to be a valid toml document, example: `-c=\"foo='hey'\"` `-c=\"foo={value=1}\"`"),
        )
        .arg(
            Arg::new("update-config")
                .long("update-config")
                .action(ArgAction::SetTrue)
                .help("Update the config file when started"),
        )
        .arg(
            Arg::new("enabled-features")
                .long("enabled-features")
                .action(ArgAction::SetTrue)
                .help("Show enabled compile time features"),
        )
}

#[test]
fn verify_cli() {
    cli().debug_assert();
}
