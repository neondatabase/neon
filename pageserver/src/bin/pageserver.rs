//! Main entry point for the Page Server executable.

use std::env::{var, VarError};
use std::sync::Arc;
use std::{env, ops::ControlFlow, path::Path, str::FromStr};

use anyhow::{anyhow, Context};
use clap::{Arg, ArgAction, Command};
use fail::FailScenario;
use metrics::launch_timestamp::{set_launch_timestamp_metric, LaunchTimestamp};
use pageserver::disk_usage_eviction_task::{self, launch_disk_usage_global_eviction_task};
use pageserver::task_mgr::WALRECEIVER_RUNTIME;
use remote_storage::GenericRemoteStorage;
use tracing::*;

use metrics::set_build_info_metric;
use pageserver::{
    config::{defaults::*, PageServerConf},
    context::{DownloadBehavior, RequestContext},
    http, page_cache, page_service, task_mgr,
    task_mgr::TaskKind,
    task_mgr::{BACKGROUND_RUNTIME, COMPUTE_REQUEST_RUNTIME, MGMT_REQUEST_RUNTIME},
    tenant::mgr,
    virtual_file,
};
use postgres_backend::AuthType;
use utils::logging::TracingErrorLayerEnablement;
use utils::signals::ShutdownSignals;
use utils::{
    auth::JwtAuth, logging, project_git_version, sentry_init::init_sentry, signals::Signal,
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
    let launch_ts = Box::leak(Box::new(LaunchTimestamp::generate()));

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

    // Initialize logging.
    //
    // It must be initialized before the custom panic hook is installed below.
    //
    // Regarding tracing_error enablement: at this time, we only use the
    // tracing_error crate to debug_assert that log spans contain tenant and timeline ids.
    // See `debug_assert_current_span_has_tenant_and_timeline_id` in the timeline module
    let tracing_error_layer_enablement = if cfg!(debug_assertions) {
        TracingErrorLayerEnablement::EnableWithRustLogFilter
    } else {
        TracingErrorLayerEnablement::Disabled
    };
    logging::init(conf.log_format, tracing_error_layer_enablement)?;

    // mind the order required here: 1. logging, 2. panic_hook, 3. sentry.
    // disarming this hook on pageserver, because we never tear down tracing.
    logging::replace_panic_hook_with_tracing_panic_hook().forget();

    // initialize sentry if SENTRY_DSN is provided
    let _sentry_guard = init_sentry(
        Some(GIT_VERSION.into()),
        &[("node_id", &conf.id.to_string())],
    );

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

    start_pageserver(launch_ts, conf).context("Failed to start pageserver")?;

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

fn start_pageserver(
    launch_ts: &'static LaunchTimestamp,
    conf: &'static PageServerConf,
) -> anyhow::Result<()> {
    // Print version and launch timestamp to the log,
    // and expose them as prometheus metrics.
    // A changed version string indicates changed software.
    // A changed launch timestamp indicates a pageserver restart.
    info!(
        "version: {} launch_timestamp: {}",
        version(),
        launch_ts.to_string()
    );
    set_build_info_metric(GIT_VERSION);
    set_launch_timestamp_metric(launch_ts);
    pageserver::preinitialize_metrics();

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

    // Launch broker client
    // The storage_broker::connect call needs to happen inside a tokio runtime thread.
    let broker_client = WALRECEIVER_RUNTIME
        .block_on(async {
            // Note: we do not attempt connecting here (but validate endpoints sanity).
            storage_broker::connect(conf.broker_endpoint.clone(), conf.broker_keepalive_interval)
        })
        .with_context(|| {
            format!(
                "create broker client for uri={:?} keepalive_interval={:?}",
                &conf.broker_endpoint, conf.broker_keepalive_interval,
            )
        })?;

    // Initialize authentication for incoming connections
    let http_auth;
    let pg_auth;
    if conf.http_auth_type == AuthType::NeonJWT || conf.pg_auth_type == AuthType::NeonJWT {
        // unwrap is ok because check is performed when creating config, so path is set and file exists
        let key_path = conf.auth_validation_public_key_path.as_ref().unwrap();
        info!(
            "Loading public key for verifying JWT tokens from {:#?}",
            key_path
        );
        let auth: Arc<JwtAuth> = Arc::new(JwtAuth::from_key_path(key_path)?);

        http_auth = match &conf.http_auth_type {
            AuthType::Trust => None,
            AuthType::NeonJWT => Some(auth.clone()),
        };
        pg_auth = match &conf.pg_auth_type {
            AuthType::Trust => None,
            AuthType::NeonJWT => Some(auth),
        };
    } else {
        http_auth = None;
        pg_auth = None;
    }
    info!("Using auth for http API: {:#?}", conf.http_auth_type);
    info!("Using auth for pg connections: {:#?}", conf.pg_auth_type);

    match var("NEON_AUTH_TOKEN") {
        Ok(v) => {
            info!("Loaded JWT token for authentication with Safekeeper");
            pageserver::config::SAFEKEEPER_AUTH_TOKEN
                .set(Arc::new(v))
                .map_err(|_| anyhow!("Could not initialize SAFEKEEPER_AUTH_TOKEN"))?;
        }
        Err(VarError::NotPresent) => {
            info!("No JWT token for authentication with Safekeeper detected");
        }
        Err(e) => {
            return Err(e).with_context(|| {
                "Failed to either load to detect non-present NEON_AUTH_TOKEN environment variable"
            })
        }
    };

    // Set up remote storage client
    let remote_storage = create_remote_storage_client(conf)?;

    // Startup staging or optimizing:
    //
    // We want to minimize downtime for `page_service` connections, and trying not to overload
    // BACKGROUND_RUNTIME by doing initial compactions and initial logical sizes at the same time.
    //
    // init_done_rx will notify when all initial load operations have completed.
    //
    // background_jobs_can_start (same name used to hold off background jobs from starting at
    // consumer side) will be dropped once we can start the background jobs. Currently it is behind
    // completing all initial logical size calculations (init_logical_size_done_rx) and a timeout
    // (background_task_maximum_delay).
    let (init_done_tx, init_done_rx) = utils::completion::channel();

    let (init_logical_size_done_tx, init_logical_size_done_rx) = utils::completion::channel();

    let (background_jobs_can_start, background_jobs_barrier) = utils::completion::channel();

    let order = pageserver::InitializationOrder {
        initial_tenant_load: Some(init_done_tx),
        initial_logical_size_can_start: init_done_rx.clone(),
        initial_logical_size_attempt: init_logical_size_done_tx,
        background_jobs_can_start: background_jobs_barrier.clone(),
    };

    // Scan the local 'tenants/' directory and start loading the tenants
    let init_started_at = std::time::Instant::now();
    let shutdown_pageserver = tokio_util::sync::CancellationToken::new();

    BACKGROUND_RUNTIME.block_on(mgr::init_tenant_mgr(
        conf,
        broker_client.clone(),
        remote_storage.clone(),
        order,
    ))?;

    BACKGROUND_RUNTIME.spawn({
        let init_done_rx = init_done_rx;
        let shutdown_pageserver = shutdown_pageserver.clone();
        let drive_init = async move {
            // NOTE: unlike many futures in pageserver, this one is cancellation-safe
            let guard = scopeguard::guard_on_success((), |_| tracing::info!("Cancelled before initial load completed"));

            init_done_rx.wait().await;
            // initial logical sizes can now start, as they were waiting on init_done_rx.

            scopeguard::ScopeGuard::into_inner(guard);

            let init_done = std::time::Instant::now();
            let elapsed = init_done - init_started_at;

            tracing::info!(
                elapsed_millis = elapsed.as_millis(),
                "Initial load completed"
            );

            let mut init_sizes_done = std::pin::pin!(init_logical_size_done_rx.wait());

            let timeout = conf.background_task_maximum_delay;

            let guard = scopeguard::guard_on_success((), |_| tracing::info!("Cancelled before initial logical sizes completed"));

            let init_sizes_done = match tokio::time::timeout(timeout, &mut init_sizes_done).await {
                Ok(_) => {
                    let now = std::time::Instant::now();
                    tracing::info!(
                        from_init_done_millis = (now - init_done).as_millis(),
                        from_init_millis = (now - init_started_at).as_millis(),
                        "Initial logical sizes completed"
                    );
                    None
                }
                Err(_) => {
                    tracing::info!(
                        timeout_millis = timeout.as_millis(),
                        "Initial logical size timeout elapsed; starting background jobs"
                    );
                    Some(init_sizes_done)
                }
            };

            scopeguard::ScopeGuard::into_inner(guard);

            // allow background jobs to start
            drop(background_jobs_can_start);

            if let Some(init_sizes_done) = init_sizes_done {
                // ending up here is not a bug; at the latest logical sizes will be queried by
                // consumption metrics.
                let guard = scopeguard::guard_on_success((), |_| tracing::info!("Cancelled before initial logical sizes completed"));
                init_sizes_done.await;

                scopeguard::ScopeGuard::into_inner(guard);

                let now = std::time::Instant::now();
                tracing::info!(
                    from_init_done_millis = (now - init_done).as_millis(),
                    from_init_millis = (now - init_started_at).as_millis(),
                    "Initial logical sizes completed after timeout (background jobs already started)"
                );

            }
        };

        async move {
            let mut drive_init = std::pin::pin!(drive_init);
            // just race these tasks
            tokio::select! {
                _ = shutdown_pageserver.cancelled() => {},
                _ = &mut drive_init => {},
            }
        }
    });

    // shared state between the disk-usage backed eviction background task and the http endpoint
    // that allows triggering disk-usage based eviction manually. note that the http endpoint
    // is still accessible even if background task is not configured as long as remote storage has
    // been configured.
    let disk_usage_eviction_state: Arc<disk_usage_eviction_task::State> = Arc::default();

    if let Some(remote_storage) = &remote_storage {
        launch_disk_usage_global_eviction_task(
            conf,
            remote_storage.clone(),
            disk_usage_eviction_state.clone(),
            background_jobs_barrier.clone(),
        )?;
    }

    // Start up the service to handle HTTP mgmt API request. We created the
    // listener earlier already.
    {
        let _rt_guard = MGMT_REQUEST_RUNTIME.enter();

        let router = http::make_router(
            conf,
            launch_ts,
            http_auth,
            broker_client.clone(),
            remote_storage,
            disk_usage_eviction_state,
        )?
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
    }

    if let Some(metric_collection_endpoint) = &conf.metric_collection_endpoint {
        let background_jobs_barrier = background_jobs_barrier;
        let metrics_ctx = RequestContext::todo_child(
            TaskKind::MetricsCollection,
            // This task itself shouldn't download anything.
            // The actual size calculation does need downloads, and
            // creates a child context with the right DownloadBehavior.
            DownloadBehavior::Error,
        );
        task_mgr::spawn(
            crate::BACKGROUND_RUNTIME.handle(),
            TaskKind::MetricsCollection,
            None,
            None,
            "consumption metrics collection",
            true,
            async move {
                // first wait until background jobs are cleared to launch.
                //
                // this is because we only process active tenants and timelines, and the
                // Timeline::get_current_logical_size will spawn the logical size calculation,
                // which will not be rate-limited.
                let cancel = task_mgr::shutdown_token();

                tokio::select! {
                    _ = cancel.cancelled() => { return Ok(()); },
                    _ = background_jobs_barrier.wait() => {}
                };

                pageserver::consumption_metrics::collect_metrics(
                    metric_collection_endpoint,
                    conf.metric_collection_interval,
                    conf.cached_metric_collection_interval,
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
                    broker_client,
                    pg_auth,
                    pageserver_listener,
                    conf.pg_auth_type,
                    libpq_ctx,
                )
                .await
            },
        );
    }

    let mut shutdown_pageserver = Some(shutdown_pageserver.drop_guard());

    // All started up! Now just sit and wait for shutdown signal.
    ShutdownSignals::handle(|signal| match signal {
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

            // This cancels the `shutdown_pageserver` cancellation tree.
            // Right now that tree doesn't reach very far, and `task_mgr` is used instead.
            // The plan is to change that over time.
            shutdown_pageserver.take();
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
