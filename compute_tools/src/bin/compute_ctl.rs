//!
//! Postgres wrapper (`compute_ctl`) is intended to be run as a Docker entrypoint or as a `systemd`
//! `ExecStart` option. It will handle all the `Neon` specifics during compute node
//! initialization:
//! - `compute_ctl` accepts cluster (compute node) specification as a JSON file.
//! - Every start is a fresh start, so the data directory is removed and
//!   initialized again on each run.
//! - If remote_extension_config is provided, it will be used to fetch extensions list
//!   and download `shared_preload_libraries` from the remote storage.
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
//! If `AUTOSCALING` environment variable is set, `compute_ctl` will start the
//! `vm-monitor` located in [`neon/libs/vm_monitor`]. For VM compute nodes,
//! `vm-monitor` communicates with the VM autoscaling system. It coordinates
//! downscaling and requests immediate upscaling under resource pressure.
//!
//! Usage example:
//! ```sh
//! compute_ctl -D /var/db/postgres/compute \
//!             -C 'postgresql://cloud_admin@localhost/postgres' \
//!             -c /var/db/postgres/configs/config.json \
//!             -b /usr/local/bin/postgres \
//!             -r http://pg-ext-s3-gateway \
//! ```
use std::ffi::OsString;
use std::fs::File;
use std::process::exit;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use compute_api::responses::ComputeConfig;
use compute_tools::compute::{
    BUILD_TAG, ComputeNode, ComputeNodeParams, forward_termination_signal,
};
use compute_tools::extension_server::get_pg_version_string;
use compute_tools::logger::*;
use compute_tools::params::*;
use compute_tools::spec::*;
use rlimit::{Resource, setrlimit};
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook::iterator::Signals;
use tracing::{error, info};
use url::Url;
use utils::failpoint_support;

// Compatibility hack: if the control plane specified any remote-ext-config
// use the default value for extension storage proxy gateway.
// Remove this once the control plane is updated to pass the gateway URL
fn parse_remote_ext_base_url(arg: &str) -> Result<String> {
    const FALLBACK_PG_EXT_GATEWAY_BASE_URL: &str =
        "http://pg-ext-s3-gateway.pg-ext-s3-gateway.svc.cluster.local";

    Ok(if arg.starts_with("http") {
        arg
    } else {
        FALLBACK_PG_EXT_GATEWAY_BASE_URL
    }
    .to_owned())
}

#[derive(Parser)]
#[command(rename_all = "kebab-case")]
struct Cli {
    #[arg(short = 'b', long, default_value = "postgres", env = "POSTGRES_PATH")]
    pub pgbin: String,

    /// The base URL for the remote extension storage proxy gateway.
    /// Should be in the form of `http(s)://<gateway-hostname>[:<port>]`.
    #[arg(short = 'r', long, value_parser = parse_remote_ext_base_url, alias = "remote-ext-config")]
    pub remote_ext_base_url: Option<String>,

    /// The port to bind the external listening HTTP server to. Clients running
    /// outside the compute will talk to the compute through this port. Keep
    /// the previous name for this argument around for a smoother release
    /// with the control plane.
    #[arg(long, default_value_t = 3080)]
    pub external_http_port: u16,

    /// The port to bind the internal listening HTTP server to. Clients include
    /// the neon extension (for installing remote extensions) and local_proxy.
    #[arg(long, default_value_t = 3081)]
    pub internal_http_port: u16,

    #[arg(short = 'D', long, value_name = "DATADIR")]
    pub pgdata: String,

    #[arg(short = 'C', long, value_name = "DATABASE_URL")]
    pub connstr: String,

    #[cfg(target_os = "linux")]
    #[arg(long, default_value = "neon-postgres")]
    pub cgroup: String,

    #[cfg(target_os = "linux")]
    #[arg(
        long,
        default_value = "host=localhost port=5432 dbname=postgres user=cloud_admin sslmode=disable application_name=vm-monitor"
    )]
    pub filecache_connstr: String,

    #[cfg(target_os = "linux")]
    #[arg(long, default_value = "0.0.0.0:10301")]
    pub vm_monitor_addr: String,

    #[arg(long, action = clap::ArgAction::SetTrue)]
    pub resize_swap_on_bind: bool,

    #[arg(long)]
    pub set_disk_quota_for_fs: Option<String>,

    #[arg(short = 'c', long)]
    pub config: Option<OsString>,

    #[arg(short = 'i', long, group = "compute-id")]
    pub compute_id: String,

    #[arg(
        short = 'p',
        long,
        conflicts_with = "config",
        value_name = "CONTROL_PLANE_API_BASE_URL",
        requires = "compute-id"
    )]
    pub control_plane_uri: Option<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let scenario = failpoint_support::init();

    // For historical reasons, the main thread that processes the config and launches postgres
    // is synchronous, but we always have this tokio runtime available and we "enter" it so
    // that you can use tokio::spawn() and tokio::runtime::Handle::current().block_on(...)
    // from all parts of compute_ctl.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let _rt_guard = runtime.enter();

    runtime.block_on(init())?;

    // enable core dumping for all child processes
    setrlimit(Resource::CORE, rlimit::INFINITY, rlimit::INFINITY)?;

    let connstr = Url::parse(&cli.connstr).context("cannot parse connstr as a URL")?;

    let config = get_config(&cli)?;

    let compute_node = ComputeNode::new(
        ComputeNodeParams {
            compute_id: cli.compute_id,
            connstr,
            pgdata: cli.pgdata.clone(),
            pgbin: cli.pgbin.clone(),
            pgversion: get_pg_version_string(&cli.pgbin),
            external_http_port: cli.external_http_port,
            internal_http_port: cli.internal_http_port,
            remote_ext_base_url: cli.remote_ext_base_url.clone(),
            resize_swap_on_bind: cli.resize_swap_on_bind,
            set_disk_quota_for_fs: cli.set_disk_quota_for_fs,
            #[cfg(target_os = "linux")]
            filecache_connstr: cli.filecache_connstr,
            #[cfg(target_os = "linux")]
            cgroup: cli.cgroup,
            #[cfg(target_os = "linux")]
            vm_monitor_addr: cli.vm_monitor_addr,
        },
        config,
    )?;

    let exit_code = compute_node.run()?;

    scenario.teardown();

    deinit_and_exit(exit_code);
}

async fn init() -> Result<()> {
    init_tracing_and_logging(DEFAULT_LOG_LEVEL).await?;

    let mut signals = Signals::new([SIGINT, SIGTERM, SIGQUIT])?;
    thread::spawn(move || {
        for sig in signals.forever() {
            handle_exit_signal(sig);
        }
    });

    info!("compute build_tag: {}", &BUILD_TAG.to_string());

    Ok(())
}

fn get_config(cli: &Cli) -> Result<ComputeConfig> {
    // First, read the config from the path if provided
    if let Some(ref config) = cli.config {
        let file = File::open(config)?;
        return Ok(serde_json::from_reader(&file)?);
    }

    // If the config wasn't provided in the CLI arguments, then retrieve it from
    // the control plane
    match get_config_from_control_plane(cli.control_plane_uri.as_ref().unwrap(), &cli.compute_id) {
        Ok(config) => Ok(config),
        Err(e) => {
            error!(
                "cannot get response from control plane: {}\n\
                neither spec nor confirmation that compute is in the Empty state was received",
                e
            );
            Err(e)
        }
    }
}

fn deinit_and_exit(exit_code: Option<i32>) -> ! {
    // Shutdown trace pipeline gracefully, so that it has a chance to send any
    // pending traces before we exit. Shutting down OTEL tracing provider may
    // hang for quite some time, see, for example:
    // - https://github.com/open-telemetry/opentelemetry-rust/issues/868
    // - and our problems with staging https://github.com/neondatabase/cloud/issues/3707#issuecomment-1493983636
    //
    // Yet, we want computes to shut down fast enough, as we may need a new one
    // for the same timeline ASAP. So wait no longer than 2s for the shutdown to
    // complete, then just error out and exit the main thread.
    info!("shutting down tracing");
    let (sender, receiver) = mpsc::channel();
    let _ = thread::spawn(move || {
        tracing_utils::shutdown_tracing();
        sender.send(()).ok()
    });
    let shutdown_res = receiver.recv_timeout(Duration::from_millis(2000));
    if shutdown_res.is_err() {
        error!("timed out while shutting down tracing, exiting anyway");
    }

    info!("shutting down");
    exit(exit_code.unwrap_or(1))
}

/// When compute_ctl is killed, send also termination signal to sync-safekeepers
/// to prevent leakage. TODO: it is better to convert compute_ctl to async and
/// wait for termination which would be easy then.
fn handle_exit_signal(sig: i32) {
    info!("received {sig} termination signal");
    forward_termination_signal();
    exit(1);
}

#[cfg(test)]
mod test {
    use clap::CommandFactory;

    use super::Cli;

    #[test]
    fn verify_cli() {
        Cli::command().debug_assert()
    }

    #[test]
    fn parse_pg_ext_gateway_base_url() {
        let arg = "http://pg-ext-s3-gateway2";
        let result = super::parse_remote_ext_base_url(arg).unwrap();
        assert_eq!(result, arg);

        let arg = "pg-ext-s3-gateway";
        let result = super::parse_remote_ext_base_url(arg).unwrap();
        assert_eq!(
            result,
            "http://pg-ext-s3-gateway.pg-ext-s3-gateway.svc.cluster.local"
        );
    }
}
