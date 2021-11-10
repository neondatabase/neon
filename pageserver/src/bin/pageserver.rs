//
// Main entry point for the Page Server executable
//

use serde::{Deserialize, Serialize};
use std::{
    env,
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    str::FromStr,
    thread,
};
use tracing::*;
use zenith_utils::{auth::JwtAuth, logging, postgres_backend::AuthType, tcp_listener, GIT_VERSION};

use anyhow::{bail, ensure, Context, Result};
use signal_hook::consts::signal::*;
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
use signal_hook::iterator::exfiltrator::WithOrigin;
use signal_hook::iterator::SignalsInfo;
use std::process::exit;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use clap::{App, Arg, ArgMatches};
use daemonize::Daemonize;

use pageserver::{
    branches, defaults::*, http, page_cache, page_service, remote_storage, tenant_mgr,
    virtual_file, PageServerConf, RemoteStorageConfig, RemoteStorageKind, S3Config, LOG_FILE_NAME,
};
use zenith_utils::http::endpoint;
use zenith_utils::postgres_backend;

use const_format::formatcp;

/// String arguments that can be declared via CLI or config file
#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
struct CfgFileParams {
    listen_pg_addr: Option<String>,
    listen_http_addr: Option<String>,
    checkpoint_distance: Option<String>,
    checkpoint_period: Option<String>,
    gc_horizon: Option<String>,
    gc_period: Option<String>,
    open_mem_limit: Option<String>,
    page_cache_size: Option<String>,
    max_file_descriptors: Option<String>,
    pg_distrib_dir: Option<String>,
    auth_validation_public_key_path: Option<String>,
    auth_type: Option<String>,
    remote_storage_max_concurrent_sync: Option<String>,
    remote_storage_max_sync_errors: Option<String>,
    /////////////////////////////////
    //// Don't put `Option<String>` and other "simple" values below.
    ////
    /// `Option<RemoteStorage>` is a <a href='https://toml.io/en/v1.0.0#table'>table</a> in TOML.
    /// Values in TOML cannot be defined after tables (other tables can),
    /// and [`toml`] crate serializes all fields in the order of their appearance.
    ////////////////////////////////
    remote_storage: Option<RemoteStorage>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone)]
// Without this attribute, enums with values won't be serialized by the `toml` library (but can be deserialized nonetheless!).
// See https://github.com/alexcrichton/toml-rs/blob/6c162e6562c3e432bf04c82a3d1d789d80761a86/examples/enum_external.rs for the examples
#[serde(untagged)]
enum RemoteStorage {
    Local {
        local_path: String,
    },
    AwsS3 {
        bucket_name: String,
        bucket_region: String,
        #[serde(skip_serializing)]
        access_key_id: Option<String>,
        #[serde(skip_serializing)]
        secret_access_key: Option<String>,
    },
}

impl CfgFileParams {
    /// Extract string arguments from CLI
    fn from_args(arg_matches: &ArgMatches) -> Self {
        let get_arg = |arg_name: &str| -> Option<String> {
            arg_matches.value_of(arg_name).map(str::to_owned)
        };

        let remote_storage = if let Some(local_path) = get_arg("remote-storage-local-path") {
            Some(RemoteStorage::Local { local_path })
        } else if let Some((bucket_name, bucket_region)) =
            get_arg("remote-storage-s3-bucket").zip(get_arg("remote-storage-region"))
        {
            Some(RemoteStorage::AwsS3 {
                bucket_name,
                bucket_region,
                access_key_id: get_arg("remote-storage-access-key"),
                secret_access_key: get_arg("remote-storage-secret-access-key"),
            })
        } else {
            None
        };

        Self {
            listen_pg_addr: get_arg("listen_pg_addr"),
            listen_http_addr: get_arg("listen_http_addr"),
            checkpoint_distance: get_arg("checkpoint_distance"),
            checkpoint_period: get_arg("checkpoint_period"),
            gc_horizon: get_arg("gc_horizon"),
            gc_period: get_arg("gc_period"),
            open_mem_limit: get_arg("open_mem_limit"),
            page_cache_size: get_arg("page_cache_size"),
            max_file_descriptors: get_arg("max_file_descriptors"),
            pg_distrib_dir: get_arg("postgres-distrib"),
            auth_validation_public_key_path: get_arg("auth-validation-public-key-path"),
            auth_type: get_arg("auth-type"),
            remote_storage,
            remote_storage_max_concurrent_sync: get_arg("remote-storage-max-concurrent-sync"),
            remote_storage_max_sync_errors: get_arg("remote-storage-max-sync-errors"),
        }
    }

    /// Fill missing values in `self` with `other`
    fn or(self, other: CfgFileParams) -> Self {
        // TODO cleaner way to do this
        Self {
            listen_pg_addr: self.listen_pg_addr.or(other.listen_pg_addr),
            listen_http_addr: self.listen_http_addr.or(other.listen_http_addr),
            checkpoint_distance: self.checkpoint_distance.or(other.checkpoint_distance),
            checkpoint_period: self.checkpoint_period.or(other.checkpoint_period),
            gc_horizon: self.gc_horizon.or(other.gc_horizon),
            gc_period: self.gc_period.or(other.gc_period),
            open_mem_limit: self.open_mem_limit.or(other.open_mem_limit),
            page_cache_size: self.page_cache_size.or(other.page_cache_size),
            max_file_descriptors: self.max_file_descriptors.or(other.max_file_descriptors),
            pg_distrib_dir: self.pg_distrib_dir.or(other.pg_distrib_dir),
            auth_validation_public_key_path: self
                .auth_validation_public_key_path
                .or(other.auth_validation_public_key_path),
            auth_type: self.auth_type.or(other.auth_type),
            remote_storage: self.remote_storage.or(other.remote_storage),
            remote_storage_max_concurrent_sync: self
                .remote_storage_max_concurrent_sync
                .or(other.remote_storage_max_concurrent_sync),
            remote_storage_max_sync_errors: self
                .remote_storage_max_sync_errors
                .or(other.remote_storage_max_sync_errors),
        }
    }

    /// Create a PageServerConf from these string parameters
    fn try_into_config(&self) -> Result<PageServerConf> {
        let workdir = PathBuf::from(".");

        let listen_pg_addr = match self.listen_pg_addr.as_ref() {
            Some(addr) => addr.clone(),
            None => DEFAULT_PG_LISTEN_ADDR.to_owned(),
        };

        let listen_http_addr = match self.listen_http_addr.as_ref() {
            Some(addr) => addr.clone(),
            None => DEFAULT_HTTP_LISTEN_ADDR.to_owned(),
        };

        let checkpoint_distance: u64 = match self.checkpoint_distance.as_ref() {
            Some(checkpoint_distance_str) => checkpoint_distance_str.parse()?,
            None => DEFAULT_CHECKPOINT_DISTANCE,
        };
        let checkpoint_period = match self.checkpoint_period.as_ref() {
            Some(checkpoint_period_str) => humantime::parse_duration(checkpoint_period_str)?,
            None => DEFAULT_CHECKPOINT_PERIOD,
        };

        let gc_horizon: u64 = match self.gc_horizon.as_ref() {
            Some(horizon_str) => horizon_str.parse()?,
            None => DEFAULT_GC_HORIZON,
        };
        let gc_period = match self.gc_period.as_ref() {
            Some(period_str) => humantime::parse_duration(period_str)?,
            None => DEFAULT_GC_PERIOD,
        };

        let open_mem_limit: usize = match self.open_mem_limit.as_ref() {
            Some(open_mem_limit_str) => open_mem_limit_str.parse()?,
            None => DEFAULT_OPEN_MEM_LIMIT,
        };

        let page_cache_size: usize = match self.page_cache_size.as_ref() {
            Some(page_cache_size_str) => page_cache_size_str.parse()?,
            None => DEFAULT_PAGE_CACHE_SIZE,
        };

        let max_file_descriptors: usize = match self.max_file_descriptors.as_ref() {
            Some(max_file_descriptors_str) => max_file_descriptors_str.parse()?,
            None => DEFAULT_MAX_FILE_DESCRIPTORS,
        };

        let pg_distrib_dir = match self.pg_distrib_dir.as_ref() {
            Some(pg_distrib_dir_str) => PathBuf::from(pg_distrib_dir_str),
            None => env::current_dir()?.join("tmp_install"),
        };

        let auth_validation_public_key_path = self
            .auth_validation_public_key_path
            .as_ref()
            .map(PathBuf::from);

        let auth_type = self
            .auth_type
            .as_ref()
            .map_or(Ok(AuthType::Trust), |auth_type| {
                AuthType::from_str(auth_type)
            })?;

        if !pg_distrib_dir.join("bin/postgres").exists() {
            bail!("Can't find postgres binary at {:?}", pg_distrib_dir);
        }

        if auth_type == AuthType::ZenithJWT {
            ensure!(
                auth_validation_public_key_path.is_some(),
                "Missing auth_validation_public_key_path when auth_type is ZenithJWT"
            );
            let path_ref = auth_validation_public_key_path.as_ref().unwrap();
            ensure!(
                path_ref.exists(),
                format!("Can't find auth_validation_public_key at {:?}", path_ref)
            );
        }

        let max_concurrent_sync = match self.remote_storage_max_concurrent_sync.as_deref() {
            Some(number_str) => number_str.parse()?,
            None => NonZeroUsize::new(DEFAULT_REMOTE_STORAGE_MAX_CONCURRENT_SYNC).unwrap(),
        };
        let max_sync_errors = match self.remote_storage_max_sync_errors.as_deref() {
            Some(number_str) => number_str.parse()?,
            None => NonZeroU32::new(DEFAULT_REMOTE_STORAGE_MAX_SYNC_ERRORS).unwrap(),
        };
        let remote_storage_config = self.remote_storage.as_ref().map(|storage_params| {
            let storage = match storage_params.clone() {
                RemoteStorage::Local { local_path } => {
                    RemoteStorageKind::LocalFs(PathBuf::from(local_path))
                }
                RemoteStorage::AwsS3 {
                    bucket_name,
                    bucket_region,
                    access_key_id,
                    secret_access_key,
                } => RemoteStorageKind::AwsS3(S3Config {
                    bucket_name,
                    bucket_region,
                    access_key_id,
                    secret_access_key,
                }),
            };
            RemoteStorageConfig {
                max_concurrent_sync,
                max_sync_errors,
                storage,
            }
        });

        Ok(PageServerConf {
            daemonize: false,

            listen_pg_addr,
            listen_http_addr,
            checkpoint_distance,
            checkpoint_period,
            gc_horizon,
            gc_period,
            open_mem_limit,
            page_cache_size,
            max_file_descriptors,

            superuser: String::from(DEFAULT_SUPERUSER),

            workdir,

            pg_distrib_dir,

            auth_validation_public_key_path,
            auth_type,
            remote_storage_config,
        })
    }
}

fn main() -> Result<()> {
    zenith_metrics::set_common_metrics_prefix("pageserver");
    let arg_matches = App::new("Zenith page server")
        .about("Materializes WAL stream to pages and serves them to the postgres")
        .version(GIT_VERSION)
        .arg(
            Arg::with_name("listen_pg_addr")
                .short("l")
                .long("listen_pg_addr")
                .aliases(&["listen", "listen-pg"]) // keep some compatibility
                .takes_value(true)
                .help(formatcp!("listen for incoming page requests on ip:port (default: {DEFAULT_PG_LISTEN_ADDR})")),
        )
        .arg(
            Arg::with_name("listen_http_addr")
                .long("listen_http_addr")
                .aliases(&["http_endpoint", "listen-http"]) // keep some compatibility
                .takes_value(true)
                .help(formatcp!("http endpoint address for metrics and management API calls on ip:port (default: {DEFAULT_HTTP_LISTEN_ADDR})")),
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
            Arg::with_name("checkpoint_distance")
                .long("checkpoint_distance")
                .takes_value(true)
                .help("Distance from current LSN to perform checkpoint of in-memory layers"),
        )
        .arg(
            Arg::with_name("checkpoint_period")
                .long("checkpoint_period")
                .takes_value(true)
                .help("Interval between checkpoint iterations"),
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
            Arg::with_name("open_mem_limit")
                .long("open_mem_limit")
                .takes_value(true)
                .help("Amount of memory reserved for buffering incoming WAL"),
        )
        .arg(

            Arg::with_name("page_cache_size")
                .long("page_cache_size")
                .takes_value(true)
                .help("Number of pages in the page cache"),
        )
        .arg(
            Arg::with_name("max_file_descriptors")
                .long("max_file_descriptors")
                .takes_value(true)
                .help("Max number of file descriptors to keep open for files"),
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
        .arg(
            Arg::with_name("create-tenant")
                .long("create-tenant")
                .takes_value(true)
                .help("Create tenant during init")
                .requires("init"),
        )
        .arg(
            Arg::with_name("auth-validation-public-key-path")
                .long("auth-validation-public-key-path")
                .takes_value(true)
                .help("Path to public key used to validate jwt signature"),
        )
        .arg(
            Arg::with_name("auth-type")
                .long("auth-type")
                .takes_value(true)
                .help("Authentication scheme type. One of: Trust, MD5, ZenithJWT"),
        )
        .arg(
            Arg::with_name("remote-storage-local-path")
                .long("remote-storage-local-path")
                .takes_value(true)
                .help("Path to the local directory, to be used as an external remote storage")
                .conflicts_with_all(&[
                    "remote-storage-s3-bucket",
                    "remote-storage-region",
                    "remote-storage-access-key",
                    "remote-storage-secret-access-key",
                ]),
        )
        .arg(
            Arg::with_name("remote-storage-s3-bucket")
                .long("remote-storage-s3-bucket")
                .takes_value(true)
                .help("Name of the AWS S3 bucket to use an external remote storage")
                .requires("remote-storage-region"),
        )
        .arg(
            Arg::with_name("remote-storage-region")
                .long("remote-storage-region")
                .takes_value(true)
                .help("Region of the AWS S3 bucket"),
        )
        .arg(
            Arg::with_name("remote-storage-access-key")
                .long("remote-storage-access-key")
                .takes_value(true)
                .help("Credentials to access the AWS S3 bucket"),
        )
        .arg(
            Arg::with_name("remote-storage-secret-access-key")
                .long("remote-storage-secret-access-key")
                .takes_value(true)
                .help("Credentials to access the AWS S3 bucket"),
        )
        .arg(
            Arg::with_name("remote-storage-max-concurrent-sync")
                .long("remote-storage-max-concurrent-sync")
                .takes_value(true)
                .help("Maximum allowed concurrent synchronisations with storage"),
        )
        .get_matches();

    let workdir = Path::new(arg_matches.value_of("workdir").unwrap_or(".zenith"));
    let cfg_file_path = workdir
        .canonicalize()
        .with_context(|| format!("Error opening workdir '{}'", workdir.display()))?
        .join("pageserver.toml");

    let args_params = CfgFileParams::from_args(&arg_matches);

    let init = arg_matches.is_present("init");
    let create_tenant = arg_matches.value_of("create-tenant");

    let params = if init {
        // We're initializing the repo, so there's no config file yet
        args_params
    } else {
        // Supplement the CLI arguments with the config file
        let cfg_file_contents = std::fs::read_to_string(&cfg_file_path)
            .with_context(|| format!("No pageserver config at '{}'", cfg_file_path.display()))?;
        let file_params: CfgFileParams = toml::from_str(&cfg_file_contents).with_context(|| {
            format!(
                "Failed to read '{}' as pageserver config",
                cfg_file_path.display()
            )
        })?;
        args_params.or(file_params)
    };

    // Set CWD to workdir for non-daemon modes
    env::set_current_dir(&workdir).with_context(|| {
        format!(
            "Failed to set application's current dir to '{}'",
            workdir.display()
        )
    })?;

    // Ensure the config is valid, even if just init-ing
    let mut conf = params.try_into_config().with_context(|| {
        format!(
            "Pageserver config at '{}' is not valid",
            cfg_file_path.display()
        )
    })?;

    conf.daemonize = arg_matches.is_present("daemonize");

    if init && conf.daemonize {
        bail!("--daemonize cannot be used with --init")
    }

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
        let cfg_file_contents = toml::to_string_pretty(&params)
            .context("Failed to create pageserver config contents for initialisation")?;
        // TODO support enable-auth flag
        std::fs::write(&cfg_file_path, cfg_file_contents).with_context(|| {
            format!(
                "Failed to initialize pageserver config at '{}'",
                cfg_file_path.display()
            )
        })?;
        Ok(())
    } else {
        start_pageserver(conf).context("Failed to start pageserver")
    }
}

fn start_pageserver(conf: &'static PageServerConf) -> Result<()> {
    // Initialize logger
    let log_file = logging::init(LOG_FILE_NAME, conf.daemonize)?;

    info!("version: {}", GIT_VERSION);

    let term_now = Arc::new(AtomicBool::new(false));
    for sig in TERM_SIGNALS {
        // When terminated by a second term signal, exit with exit code 1.
        // This will do nothing the first time (because term_now is false).
        flag::register_conditional_shutdown(*sig, 1, Arc::clone(&term_now))?;
        // But this will "arm" the above for the second time, by setting it to true.
        // The order of registering these is important, if you put this one first, it will
        // first arm and then terminate ‒ all in the first round.
        flag::register(*sig, Arc::clone(&term_now))?;
    }

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

    if conf.daemonize {
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

        match daemonize.start() {
            Ok(_) => info!("Success, daemonized"),
            Err(err) => error!(%err, "could not daemonize"),
        }
    }

    // keep join handles for spawned threads
    // don't spawn threads before daemonizing
    let mut join_handles = Vec::new();

    if let Some(handle) = remote_storage::run_storage_sync_thread(conf)? {
        join_handles.push(handle);
    }
    // Initialize tenant manager.
    tenant_mgr::init(conf);

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
    let http_endpoint_thread = thread::Builder::new()
        .name("http_endpoint_thread".into())
        .spawn(move || {
            let router = http::make_router(conf, cloned);
            endpoint::serve_thread_main(router, http_listener)
        })?;

    join_handles.push(http_endpoint_thread);

    // Spawn a thread to listen for connections. It will spawn further threads
    // for each connection.
    let page_service_thread = thread::Builder::new()
        .name("Page Service thread".into())
        .spawn(move || {
            page_service::thread_main(conf, auth, pageserver_listener, conf.auth_type)
        })?;

    for info in SignalsInfo::<WithOrigin>::new(TERM_SIGNALS)?.into_iter() {
        match info.signal {
            SIGQUIT => {
                info!("Got SIGQUIT. Terminate pageserver in immediate shutdown mode");
                exit(111);
            }
            SIGINT | SIGTERM => {
                info!("Got SIGINT/SIGTERM. Terminate gracefully in fast shutdown mode");
                // Terminate postgres backends
                postgres_backend::set_pgbackend_shutdown_requested();
                // Stop all tenants and flush their data
                tenant_mgr::shutdown_all_tenants()?;
                // Wait for pageservice thread to complete the job
                page_service_thread
                    .join()
                    .expect("thread panicked")
                    .expect("thread exited with an error");

                // Shut down http router
                endpoint::shutdown();

                // Wait for all threads
                for handle in join_handles.into_iter() {
                    handle
                        .join()
                        .expect("thread panicked")
                        .expect("thread exited with an error");
                }
                info!("Pageserver shut down successfully completed");
                exit(0);
            }
            unknown_signal => {
                debug!("Unknown signal {}", unknown_signal);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_server_conf_toml_serde() {
        let params = CfgFileParams {
            listen_pg_addr: Some("listen_pg_addr_VALUE".to_string()),
            listen_http_addr: Some("listen_http_addr_VALUE".to_string()),
            checkpoint_distance: Some("checkpoint_distance_VALUE".to_string()),
            checkpoint_period: Some("checkpoint_period_VALUE".to_string()),
            gc_horizon: Some("gc_horizon_VALUE".to_string()),
            gc_period: Some("gc_period_VALUE".to_string()),
            open_mem_limit: Some("open_mem_limit_VALUE".to_string()),
            page_cache_size: Some("page_cache_size_VALUE".to_string()),
            max_file_descriptors: Some("max_file_descriptors_VALUE".to_string()),
            pg_distrib_dir: Some("pg_distrib_dir_VALUE".to_string()),
            auth_validation_public_key_path: Some(
                "auth_validation_public_key_path_VALUE".to_string(),
            ),
            auth_type: Some("auth_type_VALUE".to_string()),
            remote_storage: Some(RemoteStorage::Local {
                local_path: "remote_storage_local_VALUE".to_string(),
            }),
            remote_storage_max_concurrent_sync: Some(
                "remote_storage_max_concurrent_sync_VALUE".to_string(),
            ),
            remote_storage_max_sync_errors: Some(
                "remote_storage_max_sync_errors_VALUE".to_string(),
            ),
        };

        let toml_string = toml::to_string(&params).expect("Failed to serialize correct config");
        let toml_pretty_string =
            toml::to_string_pretty(&params).expect("Failed to serialize correct config");
        assert_eq!(
            r#"listen_pg_addr = 'listen_pg_addr_VALUE'
listen_http_addr = 'listen_http_addr_VALUE'
checkpoint_distance = 'checkpoint_distance_VALUE'
checkpoint_period = 'checkpoint_period_VALUE'
gc_horizon = 'gc_horizon_VALUE'
gc_period = 'gc_period_VALUE'
open_mem_limit = 'open_mem_limit_VALUE'
page_cache_size = 'page_cache_size_VALUE'
max_file_descriptors = 'max_file_descriptors_VALUE'
pg_distrib_dir = 'pg_distrib_dir_VALUE'
auth_validation_public_key_path = 'auth_validation_public_key_path_VALUE'
auth_type = 'auth_type_VALUE'
remote_storage_max_concurrent_sync = 'remote_storage_max_concurrent_sync_VALUE'
remote_storage_max_sync_errors = 'remote_storage_max_sync_errors_VALUE'

[remote_storage]
local_path = 'remote_storage_local_VALUE'
"#,
            toml_pretty_string
        );

        let params_from_serialized: CfgFileParams = toml::from_str(&toml_string)
            .expect("Failed to deserialize the serialization result of the config");
        let params_from_serialized_pretty: CfgFileParams = toml::from_str(&toml_pretty_string)
            .expect("Failed to deserialize the prettified serialization result of the config");
        assert!(
            params_from_serialized == params,
            "Expected the same config in the end of config -> serialize -> deserialize chain"
        );
        assert!(
            params_from_serialized_pretty == params,
            "Expected the same config in the end of config -> serialize pretty -> deserialize chain"
        );
    }

    #[test]
    fn credentials_omitted_during_serialization() {
        let params = CfgFileParams {
            listen_pg_addr: Some("listen_pg_addr_VALUE".to_string()),
            listen_http_addr: Some("listen_http_addr_VALUE".to_string()),
            checkpoint_distance: Some("checkpoint_distance_VALUE".to_string()),
            checkpoint_period: Some("checkpoint_period_VALUE".to_string()),
            gc_horizon: Some("gc_horizon_VALUE".to_string()),
            gc_period: Some("gc_period_VALUE".to_string()),
            open_mem_limit: Some("open_mem_limit_VALUE".to_string()),
            page_cache_size: Some("page_cache_size_VALUE".to_string()),
            max_file_descriptors: Some("max_file_descriptors_VALUE".to_string()),
            pg_distrib_dir: Some("pg_distrib_dir_VALUE".to_string()),
            auth_validation_public_key_path: Some(
                "auth_validation_public_key_path_VALUE".to_string(),
            ),
            auth_type: Some("auth_type_VALUE".to_string()),
            remote_storage: Some(RemoteStorage::AwsS3 {
                bucket_name: "bucket_name_VALUE".to_string(),
                bucket_region: "bucket_region_VALUE".to_string(),
                access_key_id: Some("access_key_id_VALUE".to_string()),
                secret_access_key: Some("secret_access_key_VALUE".to_string()),
            }),
            remote_storage_max_concurrent_sync: Some(
                "remote_storage_max_concurrent_sync_VALUE".to_string(),
            ),
            remote_storage_max_sync_errors: Some(
                "remote_storage_max_sync_errors_VALUE".to_string(),
            ),
        };

        let toml_string = toml::to_string(&params).expect("Failed to serialize correct config");
        let toml_pretty_string =
            toml::to_string_pretty(&params).expect("Failed to serialize correct config");
        assert_eq!(
            r#"listen_pg_addr = 'listen_pg_addr_VALUE'
listen_http_addr = 'listen_http_addr_VALUE'
checkpoint_distance = 'checkpoint_distance_VALUE'
checkpoint_period = 'checkpoint_period_VALUE'
gc_horizon = 'gc_horizon_VALUE'
gc_period = 'gc_period_VALUE'
open_mem_limit = 'open_mem_limit_VALUE'
page_cache_size = 'page_cache_size_VALUE'
max_file_descriptors = 'max_file_descriptors_VALUE'
pg_distrib_dir = 'pg_distrib_dir_VALUE'
auth_validation_public_key_path = 'auth_validation_public_key_path_VALUE'
auth_type = 'auth_type_VALUE'
remote_storage_max_concurrent_sync = 'remote_storage_max_concurrent_sync_VALUE'
remote_storage_max_sync_errors = 'remote_storage_max_sync_errors_VALUE'

[remote_storage]
bucket_name = 'bucket_name_VALUE'
bucket_region = 'bucket_region_VALUE'
"#,
            toml_pretty_string
        );

        let params_from_serialized: CfgFileParams = toml::from_str(&toml_string)
            .expect("Failed to deserialize the serialization result of the config");
        let params_from_serialized_pretty: CfgFileParams = toml::from_str(&toml_pretty_string)
            .expect("Failed to deserialize the prettified serialization result of the config");

        let mut expected_params = params;
        expected_params.remote_storage = Some(RemoteStorage::AwsS3 {
            bucket_name: "bucket_name_VALUE".to_string(),
            bucket_region: "bucket_region_VALUE".to_string(),
            access_key_id: None,
            secret_access_key: None,
        });
        assert!(
            params_from_serialized == expected_params,
            "Expected the config without credentials in the end of a 'config -> serialize -> deserialize' chain"
        );
        assert!(
            params_from_serialized_pretty == expected_params,
            "Expected the config without credentials in the end of a 'config -> serialize pretty -> deserialize' chain"
        );
    }
}
