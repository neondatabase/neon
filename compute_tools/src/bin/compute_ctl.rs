//!
//! Postgres wrapper (`compute_ctl`) is intended to be run as a Docker entrypoint or as a `systemd`
//! `ExecStart` option. It will handle all the `Neon` specifics during compute node
//! initialization:
//! - `compute_ctl` accepts cluster (compute node) specification as a JSON file.
//! - Every start is a fresh start, so the data directory is removed and
//!   initialized again on each run.
//! - Next it will put configuration files into the `PGDATA` directory.
//! - Sync safekeepers and get commit LSN.
//! - Get `basebackup` from pageserver using the returned on the previous step LSN.
//! - Try to start `postgres` and wait until it is ready to accept connections.
//! - Check and alter/drop/create roles and databases.
//! - Hang waiting on the `postmaster` process to exit.
//!
//! Also `compute_ctl` spawns two separate service threads:
//! - `compute-monitor` checks the last Postgres activity timestamp and saves it
//!   into the shared `ComputeNode`;
//! - `http-endpoint` runs a Hyper HTTP API server, which serves readiness and the
//!   last activity requests.
//!
//! Usage example:
//! ```sh
//! compute_ctl -D /var/db/postgres/compute \
//!             -C 'postgresql://zenith_admin@localhost/postgres' \
//!             -S /var/db/postgres/specs/current.json \
//!             -b /usr/local/bin/postgres
//! ```
//!
use std::fs::File;
use std::panic;
use std::path::Path;
use std::process::exit;
use std::sync::{Arc, RwLock};
use std::{thread, time::Duration};

use anyhow::Result;
use chrono::Utc;
use clap::Arg;
use log::{error, info};

use compute_tools::compute::{ComputeMetrics, ComputeNode, ComputeState, ComputeStatus};
use compute_tools::http::api::launch_http_server;
use compute_tools::logger::*;
use compute_tools::monitor::launch_monitor;
use compute_tools::params::*;
use compute_tools::pg_helpers::*;
use compute_tools::spec::*;

fn main() -> Result<()> {
    // TODO: re-use `utils::logging` later
    init_logger(DEFAULT_LOG_LEVEL)?;

    // Env variable is set by `cargo`
    let version: Option<&str> = option_env!("CARGO_PKG_VERSION");
    let matches = clap::App::new("compute_ctl")
        .version(version.unwrap_or("unknown"))
        .arg(
            Arg::new("connstr")
                .short('C')
                .long("connstr")
                .value_name("DATABASE_URL")
                .required(true),
        )
        .arg(
            Arg::new("pgdata")
                .short('D')
                .long("pgdata")
                .value_name("DATADIR")
                .required(true),
        )
        .arg(
            Arg::new("pgbin")
                .short('b')
                .long("pgbin")
                .value_name("POSTGRES_PATH"),
        )
        .arg(
            Arg::new("spec")
                .short('s')
                .long("spec")
                .value_name("SPEC_JSON"),
        )
        .arg(
            Arg::new("spec-path")
                .short('S')
                .long("spec-path")
                .value_name("SPEC_PATH"),
        )
        .get_matches();

    let pgdata = matches.value_of("pgdata").expect("PGDATA path is required");
    let connstr = matches
        .value_of("connstr")
        .expect("Postgres connection string is required");
    let spec = matches.value_of("spec");
    let spec_path = matches.value_of("spec-path");

    // Try to use just 'postgres' if no path is provided
    let pgbin = matches.value_of("pgbin").unwrap_or("postgres");

    let spec: ComputeSpec = match spec {
        // First, try to get cluster spec from the cli argument
        Some(json) => serde_json::from_str(json)?,
        None => {
            // Second, try to read it from the file if path is provided
            if let Some(sp) = spec_path {
                let path = Path::new(sp);
                let file = File::open(path)?;
                serde_json::from_reader(file)?
            } else {
                panic!("cluster spec should be provided via --spec or --spec-path argument");
            }
        }
    };

    let pageserver_connstr = spec
        .cluster
        .settings
        .find("neon.pageserver_connstring")
        .expect("pageserver connstr should be provided");
    let tenant = spec
        .cluster
        .settings
        .find("neon.tenantid")
        .expect("tenant id should be provided");
    let timeline = spec
        .cluster
        .settings
        .find("neon.timelineid")
        .expect("tenant id should be provided");

    let compute_state = ComputeNode {
        start_time: Utc::now(),
        connstr: connstr.to_string(),
        pgdata: pgdata.to_string(),
        pgbin: pgbin.to_string(),
        spec,
        tenant,
        timeline,
        pageserver_connstr,
        metrics: ComputeMetrics::new(),
        state: RwLock::new(ComputeState::new()),
    };
    let compute = Arc::new(compute_state);

    // Launch service threads first, so we were able to serve availability
    // requests, while configuration is still in progress.
    let _http_handle = launch_http_server(&compute).expect("cannot launch http endpoint thread");
    let _monitor_handle = launch_monitor(&compute).expect("cannot launch compute monitor thread");

    // Run compute (Postgres) and hang waiting on it.
    match compute.prepare_and_run() {
        Ok(ec) => {
            let code = ec.code().unwrap_or(1);
            info!("Postgres exited with code {}, shutting down", code);
            exit(code)
        }
        Err(error) => {
            error!("could not start the compute node: {}", error);

            let mut state = compute.state.write().unwrap();
            state.error = Some(format!("{:?}", error));
            state.status = ComputeStatus::Failed;
            drop(state);

            // Keep serving HTTP requests, so the cloud control plane was able to
            // get the actual error.
            info!("giving control plane 30s to collect the error before shutdown");
            thread::sleep(Duration::from_secs(30));
            info!("shutting down");
            Err(error)
        }
    }
}
