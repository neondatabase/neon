#![recursion_limit = "300"]

//! Main entry point for the Page Server executable.

use std::env;
use std::env::{VarError, var};
use std::io::Read;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use camino::Utf8Path;
use clap::{Arg, ArgAction, Command};
use http_utils::tls_certs::ReloadingCertificateResolver;
use metrics::launch_timestamp::{LaunchTimestamp, set_launch_timestamp_metric};
use metrics::set_build_info_metric;
use nix::sys::socket::{setsockopt, sockopt};
use pageserver::basebackup_cache::BasebackupCache;
use pageserver::config::{PageServerConf, PageserverIdentity, ignored_fields};
use pageserver::controller_upcall_client::StorageControllerUpcallClient;
use pageserver::deletion_queue::DeletionQueue;
use pageserver::disk_usage_eviction_task::{self, launch_disk_usage_global_eviction_task};
use pageserver::feature_resolver::FeatureResolver;
use pageserver::metrics::{STARTUP_DURATION, STARTUP_IS_LOADING};
use pageserver::page_service::GrpcPageServiceHandler;
use pageserver::task_mgr::{
    BACKGROUND_RUNTIME, COMPUTE_REQUEST_RUNTIME, MGMT_REQUEST_RUNTIME, WALRECEIVER_RUNTIME,
};
use pageserver::tenant::{TenantSharedResources, mgr, secondary};
use pageserver::{
    CancellableTask, ConsumptionMetricsTasks, HttpEndpointListener, HttpsEndpointListener,
    MetricsCollectionTask, http, page_cache, page_service, task_mgr, virtual_file,
};
use postgres_backend::AuthType;
use remote_storage::GenericRemoteStorage;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::*;
use tracing_utils::OtelGuard;
use utils::auth::{JwtAuth, SwappableJwtAuth};
use utils::crashsafe::syncfs;
use utils::logging::TracingErrorLayerEnablement;
use utils::metrics_collector::{METRICS_COLLECTION_INTERVAL, METRICS_COLLECTOR};
use utils::sentry_init::init_sentry;
use utils::{failpoint_support, logging, project_build_tag, project_git_version, tcp_listener};

project_git_version!(GIT_VERSION);
project_build_tag!(BUILD_TAG);

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Configure jemalloc to profile heap allocations by sampling stack traces every 2 MB (1 << 21).
/// This adds roughly 3% overhead for allocations on average, which is acceptable considering
/// performance-sensitive code will avoid allocations as far as possible anyway.
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:21\0";

const PID_FILE_NAME: &str = "pageserver.pid";

const FEATURES: &[&str] = &[
    #[cfg(feature = "testing")]
    "testing",
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

    // Initialize up failpoints support
    let scenario = failpoint_support::init();

    let workdir = arg_matches
        .get_one::<String>("workdir")
        .map(Utf8Path::new)
        .unwrap_or_else(|| Utf8Path::new(".neon"));
    let workdir = workdir
        .canonicalize_utf8()
        .with_context(|| format!("Error opening workdir '{workdir}'"))?;

    let cfg_file_path = workdir.join("pageserver.toml");
    let identity_file_path = workdir.join("identity.toml");

    // Set CWD to workdir for non-daemon modes
    env::set_current_dir(&workdir)
        .with_context(|| format!("Failed to set application's current dir to '{workdir}'"))?;

    let (conf, ignored) = initialize_config(&identity_file_path, &cfg_file_path, &workdir)?;

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

    logging::init(
        conf.log_format,
        tracing_error_layer_enablement,
        logging::Output::Stdout,
    )?;

    let otel_enablement = match &conf.tracing {
        Some(cfg) => tracing_utils::OtelEnablement::Enabled {
            service_name: "pageserver".to_string(),
            export_config: (&cfg.export_config).into(),
            runtime: *COMPUTE_REQUEST_RUNTIME,
        },
        None => tracing_utils::OtelEnablement::Disabled,
    };

    let otel_guard = tracing_utils::init_performance_tracing(otel_enablement);

    if otel_guard.is_some() {
        info!(?conf.tracing, "starting with OTEL tracing enabled");
    }

    // mind the order required here: 1. logging, 2. panic_hook, 3. sentry.
    // disarming this hook on pageserver, because we never tear down tracing.
    logging::replace_panic_hook_with_tracing_panic_hook().forget();

    // initialize sentry if SENTRY_DSN is provided
    let _sentry_guard = init_sentry(
        Some(GIT_VERSION.into()),
        &[("node_id", &conf.id.to_string())],
    );

    // Warn about ignored config items; see pageserver_api::config::ConfigToml
    // doc comment for rationale why we prefer this over serde(deny_unknown_fields).
    {
        let ignored_fields::Paths { paths } = &ignored;
        for path in paths {
            warn!(?path, "ignoring unknown configuration item");
        }
    }

    // Log configuration items for feature-flag-like config
    // (maybe we should automate this with a visitor?).
    info!(?conf.virtual_file_io_engine, "starting with virtual_file IO engine");
    info!(?conf.virtual_file_io_mode, "starting with virtual_file IO mode");
    info!(?conf.validate_wal_contiguity, "starting with WAL contiguity validation");
    info!(?conf.page_service_pipelining, "starting with page service pipelining config");
    info!(?conf.get_vectored_concurrent_io, "starting with get_vectored IO concurrency config");

    // The tenants directory contains all the pageserver local disk state.
    // Create if not exists and make sure all the contents are durable before proceeding.
    // Ensuring durability eliminates a whole bug class where we come up after an unclean shutdown.
    // After unclea shutdown, we don't know if all the filesystem content we can read via syscalls is actually durable or not.
    // Examples for that: OOM kill, systemd killing us during shutdown, self abort due to unrecoverable IO error.
    let tenants_path = conf.tenants_path();
    {
        let open = || {
            nix::dir::Dir::open(
                tenants_path.as_std_path(),
                nix::fcntl::OFlag::O_DIRECTORY | nix::fcntl::OFlag::O_RDONLY,
                nix::sys::stat::Mode::empty(),
            )
        };
        let dirfd = match open() {
            Ok(dirfd) => dirfd,
            Err(e) => match e {
                nix::errno::Errno::ENOENT => {
                    utils::crashsafe::create_dir_all(&tenants_path).with_context(|| {
                        format!("Failed to create tenants root dir at '{tenants_path}'")
                    })?;
                    open().context("open tenants dir after creating it")?
                }
                e => anyhow::bail!(e),
            },
        };

        if conf.no_sync {
            info!("Skipping syncfs on startup");
        } else {
            let started = Instant::now();
            syncfs(dirfd)?;
            let elapsed = started.elapsed();
            info!(
                elapsed_ms = elapsed.as_millis(),
                "made tenant directory contents durable"
            );
        }
    }

    // Basic initialization of things that don't change after startup
    tracing::info!("Initializing virtual_file...");
    virtual_file::init(
        conf.max_file_descriptors,
        conf.virtual_file_io_engine,
        conf.virtual_file_io_mode,
        if conf.no_sync {
            virtual_file::SyncMode::UnsafeNoSync
        } else {
            virtual_file::SyncMode::Sync
        },
    );
    tracing::info!("Initializing page_cache...");
    page_cache::init(conf.page_cache_size);

    start_pageserver(launch_ts, conf, ignored, otel_guard).context("Failed to start pageserver")?;

    scenario.teardown();
    Ok(())
}

fn initialize_config(
    identity_file_path: &Utf8Path,
    cfg_file_path: &Utf8Path,
    workdir: &Utf8Path,
) -> anyhow::Result<(&'static PageServerConf, ignored_fields::Paths)> {
    // The deployment orchestrator writes out an indentity file containing the node id
    // for all pageservers. This file is the source of truth for the node id. In order
    // to allow for rolling back pageserver releases, the node id is also included in
    // the pageserver config that the deployment orchestrator writes to disk for the pageserver.
    // A rolled back version of the pageserver will get the node id from the pageserver.toml
    // config file.
    let identity = match std::fs::File::open(identity_file_path) {
        Ok(mut f) => {
            let md = f.metadata().context("stat config file")?;
            if !md.is_file() {
                anyhow::bail!(
                    "Pageserver found identity file but it is a dir entry: {identity_file_path}. Aborting start up ..."
                );
            }

            let mut s = String::new();
            f.read_to_string(&mut s).context("read identity file")?;
            toml_edit::de::from_str::<PageserverIdentity>(&s)?
        }
        Err(e) => {
            anyhow::bail!(
                "Pageserver could not read identity file: {identity_file_path}: {e}. Aborting start up ..."
            );
        }
    };

    let config_file_contents =
        std::fs::read_to_string(cfg_file_path).context("read config file from filesystem")?;

    // Deserialize the config file contents into a ConfigToml.
    let config_toml: pageserver_api::config::ConfigToml = {
        let deserializer = toml_edit::de::Deserializer::from_str(&config_file_contents)
            .context("build toml deserializer")?;
        let mut path_to_error_track = serde_path_to_error::Track::new();
        let deserializer =
            serde_path_to_error::Deserializer::new(deserializer, &mut path_to_error_track);
        serde::Deserialize::deserialize(deserializer).context("deserialize config toml")?
    };

    // Find unknown fields by re-serializing the parsed ConfigToml and comparing it to the on-disk file.
    // Any fields that are only in the on-disk version are unknown.
    // (The assumption here is that the ConfigToml doesn't to skip_serializing_if.)
    // (Make sure to read the ConfigToml doc comment on why we only want to warn about, but not fail startup, on unknown fields).
    let ignored = {
        let ondisk_toml = config_file_contents
            .parse::<toml_edit::DocumentMut>()
            .context("parse original config as toml document")?;
        let parsed_toml = toml_edit::ser::to_document(&config_toml)
            .context("re-serialize config to toml document")?;
        pageserver::config::ignored_fields::find(ondisk_toml, parsed_toml)
    };

    // Construct the runtime god object (it's called PageServerConf but actually is just global shared state).
    let conf = PageServerConf::parse_and_validate(identity.id, config_toml, workdir)
        .context("runtime-validation of config toml")?;
    let conf = Box::leak(Box::new(conf));

    Ok((conf, ignored))
}

struct WaitForPhaseResult<F: std::future::Future + Unpin> {
    timeout_remaining: Duration,
    skipped: Option<F>,
}

/// During startup, we apply a timeout to our waits for readiness, to avoid
/// stalling the whole service if one Tenant experiences some problem.  Each
/// phase may consume some of the timeout: this function returns the updated
/// timeout for use in the next call.
async fn wait_for_phase<F>(phase: &str, mut fut: F, timeout: Duration) -> WaitForPhaseResult<F>
where
    F: std::future::Future + Unpin,
{
    let initial_t = Instant::now();
    let skipped = match tokio::time::timeout(timeout, &mut fut).await {
        Ok(_) => None,
        Err(_) => {
            tracing::info!(
                timeout_millis = timeout.as_millis(),
                %phase,
                "Startup phase timed out, proceeding anyway"
            );
            Some(fut)
        }
    };

    WaitForPhaseResult {
        timeout_remaining: timeout
            .checked_sub(Instant::now().duration_since(initial_t))
            .unwrap_or(Duration::ZERO),
        skipped,
    }
}

fn startup_checkpoint(started_at: Instant, phase: &str, human_phase: &str) {
    let elapsed = started_at.elapsed();
    let secs = elapsed.as_secs_f64();
    STARTUP_DURATION.with_label_values(&[phase]).set(secs);

    info!(
        elapsed_ms = elapsed.as_millis(),
        "{human_phase} ({secs:.3}s since start)"
    )
}

fn start_pageserver(
    launch_ts: &'static LaunchTimestamp,
    conf: &'static PageServerConf,
    ignored: ignored_fields::Paths,
    otel_guard: Option<OtelGuard>,
) -> anyhow::Result<()> {
    // Monotonic time for later calculating startup duration
    let started_startup_at = Instant::now();

    // Print version and launch timestamp to the log,
    // and expose them as prometheus metrics.
    // A changed version string indicates changed software.
    // A changed launch timestamp indicates a pageserver restart.
    info!(
        "version: {} launch_timestamp: {} build_tag: {}",
        version(),
        launch_ts.to_string(),
        BUILD_TAG,
    );
    set_build_info_metric(GIT_VERSION, BUILD_TAG);
    set_launch_timestamp_metric(launch_ts);
    #[cfg(target_os = "linux")]
    metrics::register_internal(Box::new(metrics::more_process_metrics::Collector::new())).unwrap();
    metrics::register_internal(Box::new(
        pageserver::metrics::tokio_epoll_uring::Collector::new(),
    ))
    .unwrap();
    pageserver::preinitialize_metrics(conf, ignored);

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
    info!("Claiming pid file at {lock_file_path:?}...");
    let lock_file =
        utils::pid_file::claim_for_current_process(&lock_file_path).context("claim pid file")?;
    info!("Claimed pid file at {lock_file_path:?}");

    // Ensure that the lock file is held even if the main thread of the process panics.
    // We need to release the lock file only when the process exits.
    std::mem::forget(lock_file);

    // Bind the HTTP, libpq, and gRPC ports early, to error out if they are
    // already in use.
    info!(
        "Starting pageserver http handler on {} with auth {:#?}",
        conf.listen_http_addr, conf.http_auth_type
    );
    let http_listener = tcp_listener::bind(&conf.listen_http_addr)?;

    let https_listener = match conf.listen_https_addr.as_ref() {
        Some(https_addr) => {
            info!(
                "Starting pageserver https handler on {https_addr} with auth {:#?}",
                conf.http_auth_type
            );
            Some(tcp_listener::bind(https_addr)?)
        }
        None => None,
    };

    info!(
        "Starting pageserver pg protocol handler on {} with auth {:#?}",
        conf.listen_pg_addr, conf.pg_auth_type,
    );
    let pageserver_listener = tcp_listener::bind(&conf.listen_pg_addr)?;

    // Enable SO_KEEPALIVE on the socket, to detect dead connections faster.
    // These are configured via net.ipv4.tcp_keepalive_* sysctls.
    //
    // TODO: also set this on the walreceiver socket, but tokio-postgres doesn't
    // support enabling keepalives while using the default OS sysctls.
    setsockopt(&pageserver_listener, sockopt::KeepAlive, &true)?;

    let mut grpc_listener = None;
    if let Some(grpc_addr) = &conf.listen_grpc_addr {
        info!(
            "Starting pageserver gRPC handler on {grpc_addr} with auth {:#?}",
            conf.grpc_auth_type
        );
        grpc_listener = Some(tcp_listener::bind(grpc_addr).map_err(|e| anyhow!("{e}"))?);
    }

    // Launch broker client
    // The storage_broker::connect call needs to happen inside a tokio runtime thread.
    let broker_client = WALRECEIVER_RUNTIME
        .block_on(async {
            let tls_config = storage_broker::ClientTlsConfig::new().ca_certificates(
                conf.ssl_ca_certs
                    .iter()
                    .map(pem::encode)
                    .map(storage_broker::Certificate::from_pem),
            );
            // Note: we do not attempt connecting here (but validate endpoints sanity).
            storage_broker::connect(
                conf.broker_endpoint.clone(),
                conf.broker_keepalive_interval,
                tls_config,
            )
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
    let grpc_auth;
    if [conf.http_auth_type, conf.pg_auth_type, conf.grpc_auth_type].contains(&AuthType::NeonJWT) {
        // unwrap is ok because check is performed when creating config, so path is set and exists
        let key_path = conf.auth_validation_public_key_path.as_ref().unwrap();
        info!("Loading public key(s) for verifying JWT tokens from {key_path:?}");

        let jwt_auth = JwtAuth::from_key_path(key_path)?;
        let auth: Arc<SwappableJwtAuth> = Arc::new(SwappableJwtAuth::new(jwt_auth));

        http_auth = match conf.http_auth_type {
            AuthType::Trust => None,
            AuthType::NeonJWT => Some(auth.clone()),
        };
        pg_auth = match conf.pg_auth_type {
            AuthType::Trust => None,
            AuthType::NeonJWT => Some(auth.clone()),
        };
        grpc_auth = match conf.grpc_auth_type {
            AuthType::Trust => None,
            AuthType::NeonJWT => Some(auth),
        };
    } else {
        http_auth = None;
        pg_auth = None;
        grpc_auth = None;
    }

    let tls_server_config = if conf.listen_https_addr.is_some() || conf.enable_tls_page_service_api
    {
        let resolver = BACKGROUND_RUNTIME.block_on(ReloadingCertificateResolver::new(
            "main",
            &conf.ssl_key_file,
            &conf.ssl_cert_file,
            conf.ssl_cert_reload_period,
        ))?;

        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(resolver);

        Some(Arc::new(server_config))
    } else {
        None
    };

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
        Err(e) => return Err(e).with_context(
            || "Failed to either load to detect non-present NEON_AUTH_TOKEN environment variable",
        ),
    };

    // Top-level cancellation token for the process
    let shutdown_pageserver = tokio_util::sync::CancellationToken::new();

    // Set up remote storage client
    let remote_storage = BACKGROUND_RUNTIME.block_on(create_remote_storage_client(conf))?;

    let feature_resolver = create_feature_resolver(
        conf,
        shutdown_pageserver.clone(),
        BACKGROUND_RUNTIME.handle(),
    )?;

    // Set up deletion queue
    let (deletion_queue, deletion_workers) = DeletionQueue::new(
        remote_storage.clone(),
        StorageControllerUpcallClient::new(conf, &shutdown_pageserver),
        conf,
    );
    deletion_workers.spawn_with(BACKGROUND_RUNTIME.handle());

    // Up to this point no significant I/O has been done: this should have been fast.  Record
    // duration prior to starting I/O intensive phase of startup.
    startup_checkpoint(started_startup_at, "initial", "Starting loading tenants");
    STARTUP_IS_LOADING.set(1);

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
    let (init_remote_done_tx, init_remote_done_rx) = utils::completion::channel();
    let (init_done_tx, init_done_rx) = utils::completion::channel();

    let (background_jobs_can_start, background_jobs_barrier) = utils::completion::channel();

    let order = pageserver::InitializationOrder {
        initial_tenant_load_remote: Some(init_done_tx),
        initial_tenant_load: Some(init_remote_done_tx),
        background_jobs_can_start: background_jobs_barrier.clone(),
    };

    info!(config=?conf.l0_flush, "using l0_flush config");
    let l0_flush_global_state =
        pageserver::l0_flush::L0FlushGlobalState::new(conf.l0_flush.clone());

    // Scan the local 'tenants/' directory and start loading the tenants
    let (basebackup_cache, basebackup_prepare_receiver) = BasebackupCache::new(
        conf.basebackup_cache_dir(),
        conf.basebackup_cache_config.clone(),
    );
    let deletion_queue_client = deletion_queue.new_client();
    let background_purges = mgr::BackgroundPurges::default();

    let tenant_manager = mgr::init(
        conf,
        background_purges.clone(),
        TenantSharedResources {
            broker_client: broker_client.clone(),
            remote_storage: remote_storage.clone(),
            deletion_queue_client,
            l0_flush_global_state,
            basebackup_cache: Arc::clone(&basebackup_cache),
            feature_resolver: feature_resolver.clone(),
        },
        shutdown_pageserver.clone(),
    );
    let tenant_manager = Arc::new(tenant_manager);
    BACKGROUND_RUNTIME.block_on(mgr::init_tenant_mgr(tenant_manager.clone(), order))?;

    basebackup_cache.spawn_background_task(
        BACKGROUND_RUNTIME.handle(),
        basebackup_prepare_receiver,
        Arc::clone(&tenant_manager),
        shutdown_pageserver.child_token(),
    );

    BACKGROUND_RUNTIME.spawn({
        let shutdown_pageserver = shutdown_pageserver.clone();
        let drive_init = async move {
            // NOTE: unlike many futures in pageserver, this one is cancellation-safe
            let guard = scopeguard::guard_on_success((), |_| {
                tracing::info!("Cancelled before initial load completed")
            });

            let timeout = conf.background_task_maximum_delay;

            let init_remote_done = std::pin::pin!(async {
                init_remote_done_rx.wait().await;
                startup_checkpoint(
                    started_startup_at,
                    "initial_tenant_load_remote",
                    "Remote part of initial load completed",
                );
            });

            let WaitForPhaseResult {
                timeout_remaining: timeout,
                skipped: init_remote_skipped,
            } = wait_for_phase("initial_tenant_load_remote", init_remote_done, timeout).await;

            let init_load_done = std::pin::pin!(async {
                init_done_rx.wait().await;
                startup_checkpoint(
                    started_startup_at,
                    "initial_tenant_load",
                    "Initial load completed",
                );
                STARTUP_IS_LOADING.set(0);
            });

            let WaitForPhaseResult {
                timeout_remaining: _timeout,
                skipped: init_load_skipped,
            } = wait_for_phase("initial_tenant_load", init_load_done, timeout).await;

            // initial logical sizes can now start, as they were waiting on init_done_rx.

            scopeguard::ScopeGuard::into_inner(guard);

            // allow background jobs to start: we either completed prior stages, or they reached timeout
            // and were skipped.  It is important that we do not let them block background jobs indefinitely,
            // because things like consumption metrics for billing are blocked by this barrier.
            drop(background_jobs_can_start);
            startup_checkpoint(
                started_startup_at,
                "background_jobs_can_start",
                "Starting background jobs",
            );

            // We are done. If we skipped any phases due to timeout, run them to completion here so that
            // they will eventually update their startup_checkpoint, and so that we do not declare the
            // 'complete' stage until all the other stages are really done.
            let guard = scopeguard::guard_on_success((), |_| {
                tracing::info!("Cancelled before waiting for skipped phases done")
            });
            if let Some(f) = init_remote_skipped {
                f.await;
            }
            if let Some(f) = init_load_skipped {
                f.await;
            }
            scopeguard::ScopeGuard::into_inner(guard);

            startup_checkpoint(started_startup_at, "complete", "Startup complete");
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

    let (secondary_controller, secondary_controller_tasks) = secondary::spawn_tasks(
        tenant_manager.clone(),
        remote_storage.clone(),
        background_jobs_barrier.clone(),
        shutdown_pageserver.clone(),
    );

    // shared state between the disk-usage backed eviction background task and the http endpoint
    // that allows triggering disk-usage based eviction manually. note that the http endpoint
    // is still accessible even if background task is not configured as long as remote storage has
    // been configured.
    let disk_usage_eviction_state: Arc<disk_usage_eviction_task::State> = Arc::default();

    let disk_usage_eviction_task = launch_disk_usage_global_eviction_task(
        conf,
        remote_storage.clone(),
        disk_usage_eviction_state.clone(),
        tenant_manager.clone(),
        background_jobs_barrier.clone(),
    );

    // Start up the service to handle HTTP mgmt API request. We created the
    // listener earlier already.
    let (http_endpoint_listener, https_endpoint_listener) = {
        let _rt_guard = MGMT_REQUEST_RUNTIME.enter(); // for hyper

        let router_state = Arc::new(
            http::routes::State::new(
                conf,
                tenant_manager.clone(),
                http_auth.clone(),
                remote_storage.clone(),
                broker_client.clone(),
                disk_usage_eviction_state,
                deletion_queue.new_client(),
                secondary_controller,
                feature_resolver,
            )
            .context("Failed to initialize router state")?,
        );

        let router = http::make_router(router_state, launch_ts, http_auth.clone())?
            .build()
            .map_err(|err| anyhow!(err))?;

        let service =
            Arc::new(http_utils::RequestServiceBuilder::new(router).map_err(|err| anyhow!(err))?);

        let http_task = {
            let server =
                http_utils::server::Server::new(Arc::clone(&service), http_listener, None)?;
            let cancel = CancellationToken::new();

            let task = MGMT_REQUEST_RUNTIME.spawn(task_mgr::exit_on_panic_or_error(
                "http endpoint listener",
                server.serve(cancel.clone()),
            ));
            HttpEndpointListener(CancellableTask { task, cancel })
        };

        let https_task = match https_listener {
            Some(https_listener) => {
                let tls_server_config = tls_server_config
                    .clone()
                    .expect("tls_server_config is set earlier if https is enabled");

                let tls_acceptor = tokio_rustls::TlsAcceptor::from(tls_server_config);

                let server =
                    http_utils::server::Server::new(service, https_listener, Some(tls_acceptor))?;
                let cancel = CancellationToken::new();

                let task = MGMT_REQUEST_RUNTIME.spawn(task_mgr::exit_on_panic_or_error(
                    "https endpoint listener",
                    server.serve(cancel.clone()),
                ));
                Some(HttpsEndpointListener(CancellableTask { task, cancel }))
            }
            None => None,
        };

        (http_task, https_task)
    };

    /* BEGIN_HADRON */
    let metrics_collection_task = {
        let cancel = shutdown_pageserver.child_token();
        let task = crate::BACKGROUND_RUNTIME.spawn({
            let cancel = cancel.clone();
            let background_jobs_barrier = background_jobs_barrier.clone();
            async move {
                if conf.force_metric_collection_on_scrape {
                    return;
                }

                // first wait until background jobs are cleared to launch.
                tokio::select! {
                    _ = cancel.cancelled() => { return; },
                    _ = background_jobs_barrier.wait() => {}
                };
                let mut interval = tokio::time::interval(METRICS_COLLECTION_INTERVAL);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            tracing::info!("cancelled metrics collection task, exiting...");
                             break;
                        },
                        _ = interval.tick() => {}
                    }
                    tokio::task::spawn_blocking(|| {
                        METRICS_COLLECTOR.run_once(true);
                    });
                }
            }
        });
        MetricsCollectionTask(CancellableTask { task, cancel })
    };
    /* END_HADRON */

    let consumption_metrics_tasks = {
        let cancel = shutdown_pageserver.child_token();
        let task = crate::BACKGROUND_RUNTIME.spawn({
            let tenant_manager = tenant_manager.clone();
            let cancel = cancel.clone();
            async move {
                // first wait until background jobs are cleared to launch.
                //
                // this is because we only process active tenants and timelines, and the
                // Timeline::get_current_logical_size will spawn the logical size calculation,
                // which will not be rate-limited.
                tokio::select! {
                    _ = cancel.cancelled() => { return; },
                    _ = background_jobs_barrier.wait() => {}
                };

                pageserver::consumption_metrics::run(conf, tenant_manager, cancel).await;
            }
        });
        ConsumptionMetricsTasks(CancellableTask { task, cancel })
    };

    // Spawn a task to listen for libpq connections. It will spawn further tasks
    // for each connection. We created the listener earlier already.
    let perf_trace_dispatch = otel_guard.as_ref().map(|g| g.dispatch.clone());
    let page_service = page_service::spawn(
        conf,
        tenant_manager.clone(),
        pg_auth,
        perf_trace_dispatch,
        {
            let _entered = COMPUTE_REQUEST_RUNTIME.enter(); // TcpListener::from_std requires it
            pageserver_listener
                .set_nonblocking(true)
                .context("set listener to nonblocking")?;
            tokio::net::TcpListener::from_std(pageserver_listener)
                .context("create tokio listener")?
        },
        if conf.enable_tls_page_service_api {
            tls_server_config
        } else {
            None
        },
    );

    // Spawn a Pageserver gRPC server task. It will spawn separate tasks for
    // each stream/request.
    //
    // TODO: this uses a separate Tokio runtime for the page service. If we want
    // other gRPC services, they will need their own port and runtime. Is this
    // necessary?
    let mut page_service_grpc = None;
    if let Some(grpc_listener) = grpc_listener {
        page_service_grpc = Some(GrpcPageServiceHandler::spawn(
            tenant_manager.clone(),
            grpc_auth,
            otel_guard.as_ref().map(|g| g.dispatch.clone()),
            conf.get_vectored_concurrent_io,
            grpc_listener,
        )?);
    }

    // All started up! Now just sit and wait for shutdown signal.
    BACKGROUND_RUNTIME.block_on(async move {
        let signal_token = CancellationToken::new();
        let signal_cancel = signal_token.child_token();

        tokio::spawn(utils::signals::signal_handler(signal_token));

        // Wait for cancellation signal and shut down the pageserver.
        //
        // This cancels the `shutdown_pageserver` cancellation tree. Right now that tree doesn't
        // reach very far, and `task_mgr` is used instead. The plan is to change that over time.
        signal_cancel.cancelled().await;

        shutdown_pageserver.cancel();
        pageserver::shutdown_pageserver(
            http_endpoint_listener,
            https_endpoint_listener,
            page_service,
            page_service_grpc,
            metrics_collection_task,
            consumption_metrics_tasks,
            disk_usage_eviction_task,
            &tenant_manager,
            background_purges,
            deletion_queue.clone(),
            secondary_controller_tasks,
            0,
        )
        .await;
        unreachable!();
    })
}

fn create_feature_resolver(
    conf: &'static PageServerConf,
    shutdown_pageserver: CancellationToken,
    handle: &tokio::runtime::Handle,
) -> anyhow::Result<FeatureResolver> {
    FeatureResolver::spawn(conf, shutdown_pageserver, handle)
}

async fn create_remote_storage_client(
    conf: &'static PageServerConf,
) -> anyhow::Result<GenericRemoteStorage> {
    let config = if let Some(config) = &conf.remote_storage_config {
        config
    } else {
        anyhow::bail!("no remote storage configured, this is a deprecated configuration");
    };

    // Create the client
    let mut remote_storage = GenericRemoteStorage::from_config(config).await?;

    // If `test_remote_failures` is non-zero, wrap the client with a
    // wrapper that simulates failures.
    if conf.test_remote_failures > 0 {
        if !cfg!(feature = "testing") {
            anyhow::bail!(
                "test_remote_failures option is not available because pageserver was compiled without the 'testing' feature"
            );
        }
        info!(
            "Simulating remote failures for first {} attempts of each op",
            conf.test_remote_failures
        );
        remote_storage = GenericRemoteStorage::unreliable_wrapper(
            remote_storage,
            conf.test_remote_failures,
            conf.test_remote_failures_probability,
        );
    }

    Ok(remote_storage)
}

fn cli() -> Command {
    Command::new("Neon page server")
        .about("Materializes WAL stream to pages and serves them to the postgres")
        .version(version())
        .arg(
            Arg::new("workdir")
                .short('D')
                .long("workdir")
                .help("Working directory for the pageserver"),
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
