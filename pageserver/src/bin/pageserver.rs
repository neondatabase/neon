//
// Main entry point for the Page Server executable
//

use serde::{Deserialize, Serialize};
use std::{env, path::Path, thread};
use tracing::*;
use zenith_utils::{auth::JwtAuth, logging, postgres_backend::AuthType, tcp_listener, GIT_VERSION};

use anyhow::{bail, Context, Result};
use signal_hook::consts::signal::*;
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
use signal_hook::iterator::exfiltrator::WithOrigin;
use signal_hook::iterator::SignalsInfo;
use std::process::exit;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use clap::{App, Arg};
use daemonize::Daemonize;
use toml_edit::Document;

use pageserver::{
    branches, config, config::PageServerConf, http, page_service, remote_storage, tenant_mgr,
    virtual_file, LOG_FILE_NAME,
};
use zenith_utils::http::endpoint;
use zenith_utils::postgres_backend;

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
        .arg(
            Arg::with_name("config-option")
                .short("c")
                .takes_value(true)
                .number_of_values(1)
                .multiple(true)
                .help("Additional configuration options overriding config file"),
        )
        .get_matches();

    let workdir = Path::new(arg_matches.value_of("workdir").unwrap_or(".zenith"));
    let cfg_file_path = workdir
        .canonicalize()
        .with_context(|| format!("Error opening workdir '{}'", workdir.display()))?
        .join("pageserver.toml");

    let init = arg_matches.is_present("init");
    let create_tenant = arg_matches.value_of("create-tenant");

    let mut toml = if init {
        // We're initializing the repo, so there's no config file yet

        config::defaults::DEFAULT_CONFIG_FILE
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
            let doc = Document::from_str(option_line)?;

            for (key, item) in doc.iter() {
                toml.insert(key, item.clone());
            }
        }
    }
    let conf = PageServerConf::parse_config(&toml)?;

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

    // The configuration is all set up now. Turn it into a 'static
    // that can be freely stored in structs and passed across threads
    // as a ref.
    let conf: &'static PageServerConf = Box::leak(Box::new(conf));

    // Basic initialization of things that don't change after startup
    virtual_file::init(conf.max_file_descriptors);

    // Create repo and exit if init was requested
    if init {
        branches::init_pageserver(conf, create_tenant).context("Failed to init pageserver")?;
        // write the config file
        let cfg_file_contents = toml.to_string();
        std::fs::write(&cfg_file_path, cfg_file_contents).with_context(|| {
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
