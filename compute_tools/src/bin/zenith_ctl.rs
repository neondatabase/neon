//!
//! Postgres wrapper (`zenith_ctl`) is intended to be run as a Docker entrypoint or as a `systemd`
//! `ExecStart` option. It will handle all the `zenith` specifics during compute node
//! initialization:
//! - `zenith_ctl` accepts cluster (compute node) specification as a JSON file.
//! - Every start is a fresh start, so the data directory is removed and
//!   initialized again on each run.
//! - Next it will put configuration files into the `PGDATA` directory.
//! - Sync safekeepers and get commit LSN.
//! - Get `basebackup` from pageserver using the returned on the previous step LSN.
//! - Try to start `postgres` and wait until it is ready to accept connections.
//! - Check and alter/drop/create roles and databases.
//! - Hang waiting on the `postmaster` process to exit.
//!
//! Also `zenith_ctl` spawns two separate service threads:
//! - `compute-monitor` checks the last Postgres activity timestamp and saves it
//!   into the shared `ComputeState`;
//! - `http-endpoint` runs a Hyper HTTP API server, which serves readiness and the
//!   last activity requests.
//!
//! Usage example:
//! ```sh
//! zenith_ctl -D /var/db/postgres/compute \
//!            -C 'postgresql://zenith_admin@localhost/postgres' \
//!            -S /var/db/postgres/specs/current.json \
//!            -b /usr/local/bin/postgres
//! ```
//!
use std::fs::File;
use std::path::Path;
use std::process::{exit, Command, ExitStatus};
use std::sync::{Arc, RwLock};
use std::{env, panic};

use anyhow::Result;
use chrono::Utc;
use libc::{prctl, PR_SET_PDEATHSIG, SIGINT};
use log::info;
use postgres::{Client, NoTls};

use compute_tools::config;
use compute_tools::http_api::launch_http_server;
use compute_tools::logger::*;
use compute_tools::monitor::launch_monitor;
use compute_tools::params::*;
use compute_tools::pg_helpers::*;
use compute_tools::spec::*;
use compute_tools::zenith::*;

/// Do all the preparations like PGDATA directory creation, configuration,
/// safekeepers sync, basebackup, etc.
fn prepare_pgdata(state: &Arc<RwLock<ComputeState>>) -> Result<()> {
    let state = state.read().unwrap();
    let spec = &state.spec;
    let pgdata_path = Path::new(&state.pgdata);
    let pageserver_connstr = spec
        .cluster
        .settings
        .find("zenith.page_server_connstring")
        .expect("pageserver connstr should be provided");
    let tenant = spec
        .cluster
        .settings
        .find("zenith.zenith_tenant")
        .expect("tenant id should be provided");
    let timeline = spec
        .cluster
        .settings
        .find("zenith.zenith_timeline")
        .expect("tenant id should be provided");

    info!(
        "applying spec for cluster #{}, operation #{}",
        spec.cluster.cluster_id,
        spec.operation_uuid.as_ref().unwrap()
    );

    // Remove/create an empty pgdata directory and put configuration there.
    create_pgdata(&state.pgdata)?;
    config::write_postgres_conf(&pgdata_path.join("postgresql.conf"), spec)?;

    info!("starting safekeepers syncing");
    let lsn = sync_safekeepers(&state.pgdata, &state.pgbin)?;
    info!("safekeepers synced at LSN {}", lsn);

    get_basebackup(&state.pgdata, &pageserver_connstr, &tenant, &timeline, &lsn)?;
    // Update pg_hba.conf received with basebackup.
    update_pg_hba(pgdata_path)?;

    Ok(())
}

/// Start Postgres as a child process and manage DBs/roles.
/// After that this will hang waiting on the postmaster process to exit.
fn run_compute(state: &Arc<RwLock<ComputeState>>) -> Result<ExitStatus> {
    let read_state = state.read().unwrap();
    let pgdata_path = Path::new(&read_state.pgdata);

    // Run postgres as a child process.
    let mut pg = Command::new(&read_state.pgbin)
        .args(&["-D", &read_state.pgdata])
        .spawn()
        .expect("cannot start postgres process");

    // Try default Postgres port if it is not provided
    let port = read_state
        .spec
        .cluster
        .settings
        .find("port")
        .unwrap_or_else(|| "5432".to_string());
    wait_for_postgres(&port, pgdata_path)?;

    let mut client = Client::connect(&read_state.connstr, NoTls)?;

    handle_roles(&read_state.spec, &mut client)?;
    handle_databases(&read_state.spec, &mut client)?;

    // 'Close' connection
    drop(client);

    info!(
        "finished configuration of cluster #{}",
        read_state.spec.cluster.cluster_id
    );

    // Release the read lock.
    drop(read_state);

    // Get the write lock, update state and release the lock, so HTTP API
    // was able to serve requests, while we are blocked waiting on
    // Postgres.
    let mut state = state.write().unwrap();
    state.ready = true;
    drop(state);

    // Wait for child postgres process basically forever. In this state Ctrl+C
    // will be propagated to postgres and it will be shut down as well.
    let ecode = pg.wait().expect("failed to wait on postgres");

    Ok(ecode)
}

fn main() -> Result<()> {
    // During configuration we are starting Postgres as a child process. If we
    // fail we do not want to leave it running. PR_SET_PDEATHSIG sets the signal
    // that will be sent to the child process when the parent dies. NB: this is
    // cleared for the child of a fork(). SIGINT means fast shutdown for Postgres.
    // This does not matter much for Docker, where `zenith_ctl` is an entrypoint,
    // so the whole container will exit if it exits. But could be useful when
    // `zenith_ctl` is used in e.g. systemd.
    unsafe {
        prctl(PR_SET_PDEATHSIG, SIGINT);
    }

    // TODO: re-use `zenith_utils::logging` later
    init_logger(DEFAULT_LOG_LEVEL)?;

    let matches = clap::App::new("zenith_ctl")
        .version("0.1.0")
        .arg(
            clap::Arg::with_name("connstr")
                .short("C")
                .long("connstr")
                .value_name("DATABASE_URL")
                .required(true),
        )
        .arg(
            clap::Arg::with_name("pgdata")
                .short("D")
                .long("pgdata")
                .value_name("DATADIR")
                .required(true),
        )
        .arg(
            clap::Arg::with_name("pgbin")
                .short("b")
                .long("pgbin")
                .value_name("POSTGRES_PATH"),
        )
        .arg(
            clap::Arg::with_name("spec")
                .short("s")
                .long("spec")
                .value_name("SPEC_JSON"),
        )
        .arg(
            clap::Arg::with_name("spec-path")
                .short("S")
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

    let spec: ClusterSpec = match spec {
        // First, try to get cluster spec from the cli argument
        Some(json) => serde_json::from_str(json)?,
        None => {
            // Second, try to read it from the file if path is provided
            if let Some(sp) = spec_path {
                let path = Path::new(sp);
                let file = File::open(path)?;
                serde_json::from_reader(file)?
            } else {
                // Finally, try to fetch it from the env
                // XXX: not tested well and kept as a backup option for k8s, Docker, etc.
                // TODO: remove later
                match env::var("CLUSTER_SPEC") {
                    Ok(json) => serde_json::from_str(&json)?,
                    Err(_) => panic!("cluster spec should be provided via --spec, --spec-path or env variable CLUSTER_SPEC")
                }
            }
        }
    };

    let compute_state = ComputeState {
        connstr: connstr.to_string(),
        pgdata: pgdata.to_string(),
        pgbin: pgbin.to_string(),
        spec,
        ready: false,
        last_active: Utc::now(),
    };
    let compute_state = Arc::new(RwLock::new(compute_state));

    // Launch service threads first, so we were able to serve availability
    // requests, while configuration is still in progress.
    let mut _threads = vec![
        launch_http_server(&compute_state).expect("cannot launch compute monitor thread"),
        launch_monitor(&compute_state).expect("cannot launch http endpoint thread"),
    ];

    prepare_pgdata(&compute_state)?;

    // Run compute (Postgres) and hang waiting on it. Panic if any error happens,
    // it will help us to trigger unwind and kill postmaster as well.
    match run_compute(&compute_state) {
        Ok(ec) => exit(ec.success() as i32),
        Err(error) => panic!("cannot start compute node, error: {}", error),
    }
}
