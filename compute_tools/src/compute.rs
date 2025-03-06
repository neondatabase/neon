use std::collections::HashMap;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};
use std::{env, fs};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use compute_api::privilege::Privilege;
use compute_api::responses::{ComputeCtlConfig, ComputeMetrics, ComputeStatus};
use compute_api::spec::{
    ComputeAudit, ComputeFeature, ComputeMode, ComputeSpec, ExtVersion, PgIdent,
};
use futures::StreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use postgres;
use postgres::NoTls;
use postgres::error::SqlState;
use remote_storage::{DownloadError, RemotePath};
use tokio::spawn;
use tracing::{Instrument, debug, error, info, instrument, warn};
use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;
use utils::measured_stream::MeasuredReader;

use crate::configurator::launch_configurator;
use crate::disk_quota::set_disk_quota;
use crate::installed_extensions::get_installed_extensions;
use crate::logger::startup_context_from_env;
use crate::lsn_lease::launch_lsn_lease_bg_task_for_static;
use crate::monitor::launch_monitor;
use crate::pg_helpers::*;
use crate::rsyslog::configure_audit_rsyslog;
use crate::spec::*;
use crate::swap::resize_swap;
use crate::sync_sk::{check_if_synced, ping_safekeeper};
use crate::{config, extension_server, local_proxy};

pub static SYNC_SAFEKEEPERS_PID: AtomicU32 = AtomicU32::new(0);
pub static PG_PID: AtomicU32 = AtomicU32::new(0);

/// Static configuration params that don't change after startup. These mostly
/// come from the CLI args, or are derived from them.
pub struct ComputeNodeParams {
    /// The ID of the compute
    pub compute_id: String,
    // Url type maintains proper escaping
    pub connstr: url::Url,

    pub resize_swap_on_bind: bool,
    pub set_disk_quota_for_fs: Option<String>,

    // VM monitor parameters
    #[cfg(target_os = "linux")]
    pub filecache_connstr: String,
    #[cfg(target_os = "linux")]
    pub cgroup: String,
    #[cfg(target_os = "linux")]
    pub vm_monitor_addr: String,

    pub pgdata: String,
    pub pgbin: String,
    pub pgversion: String,
    pub build_tag: String,

    /// The port that the compute's external HTTP server listens on
    pub external_http_port: u16,
    /// The port that the compute's internal HTTP server listens on
    pub internal_http_port: u16,

    /// the address of extension storage proxy gateway
    pub ext_remote_storage: Option<String>,

    /// We should only allow live re- / configuration of the compute node if
    /// it uses 'pull model', i.e. it can go to control-plane and fetch
    /// the latest configuration. Otherwise, there could be a case:
    /// - we start compute with some spec provided as argument
    /// - we push new spec and it does reconfiguration
    /// - but then something happens and compute pod / VM is destroyed,
    ///   so k8s controller starts it again with the **old** spec
    ///
    /// and the same for empty computes:
    /// - we started compute without any spec
    /// - we push spec and it does configuration
    /// - but then it is restarted without any spec again
    pub live_config_allowed: bool,
}

/// Compute node info shared across several `compute_ctl` threads.
pub struct ComputeNode {
    pub params: ComputeNodeParams,

    // We connect to Postgres from many different places, so build configs once
    // and reuse them where needed. These are derived from 'params.connstr'
    pub conn_conf: postgres::config::Config,
    pub tokio_conn_conf: tokio_postgres::config::Config,

    /// Volatile part of the `ComputeNode`, which should be used under `Mutex`.
    /// To allow HTTP API server to serving status requests, while configuration
    /// is in progress, lock should be held only for short periods of time to do
    /// read/write, not the whole configuration process.
    pub state: Mutex<ComputeState>,
    /// `Condvar` to allow notifying waiters about state changes.
    pub state_changed: Condvar,

    // key: ext_archive_name, value: started download time, download_completed?
    pub ext_download_progress: RwLock<HashMap<String, (DateTime<Utc>, bool)>>,
}

// store some metrics about download size that might impact startup time
#[derive(Clone, Debug)]
pub struct RemoteExtensionMetrics {
    num_ext_downloaded: u64,
    largest_ext_size: u64,
    total_ext_download_size: u64,
}

#[derive(Clone, Debug)]
pub struct ComputeState {
    pub start_time: DateTime<Utc>,
    pub status: ComputeStatus,
    /// Timestamp of the last Postgres activity. It could be `None` if
    /// compute wasn't used since start.
    pub last_active: Option<DateTime<Utc>>,
    pub error: Option<String>,

    /// Compute spec. This can be received from the CLI or - more likely -
    /// passed by the control plane with a /configure HTTP request.
    pub pspec: Option<ParsedSpec>,

    pub compute_ctl_config: ComputeCtlConfig,

    /// If the spec is passed by a /configure request, 'startup_span' is the
    /// /configure request's tracing span. The main thread enters it when it
    /// processes the compute startup, so that the compute startup is considered
    /// to be part of the /configure request for tracing purposes.
    ///
    /// If the request handling thread/task called startup_compute() directly,
    /// it would automatically be a child of the request handling span, and we
    /// wouldn't need this. But because we use the main thread to perform the
    /// startup, and the /configure task just waits for it to finish, we need to
    /// set up the span relationship ourselves.
    pub startup_span: Option<tracing::span::Span>,

    pub metrics: ComputeMetrics,
}

impl ComputeState {
    pub fn new() -> Self {
        Self {
            start_time: Utc::now(),
            status: ComputeStatus::Empty,
            last_active: None,
            error: None,
            pspec: None,
            compute_ctl_config: ComputeCtlConfig::default(),
            startup_span: None,
            metrics: ComputeMetrics::default(),
        }
    }

    pub fn set_status(&mut self, status: ComputeStatus, state_changed: &Condvar) {
        let prev = self.status;
        info!("Changing compute status from {} to {}", prev, status);
        self.status = status;
        state_changed.notify_all();
    }

    pub fn set_failed_status(&mut self, err: anyhow::Error, state_changed: &Condvar) {
        self.error = Some(format!("{err:?}"));
        self.set_status(ComputeStatus::Failed, state_changed);
    }
}

impl Default for ComputeState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct ParsedSpec {
    pub spec: ComputeSpec,
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub pageserver_connstr: String,
    pub safekeeper_connstrings: Vec<String>,
    pub storage_auth_token: Option<String>,
}

impl TryFrom<ComputeSpec> for ParsedSpec {
    type Error = String;
    fn try_from(spec: ComputeSpec) -> Result<Self, String> {
        // Extract the options from the spec file that are needed to connect to
        // the storage system.
        //
        // For backwards-compatibility, the top-level fields in the spec file
        // may be empty. In that case, we need to dig them from the GUCs in the
        // cluster.settings field.
        let pageserver_connstr = spec
            .pageserver_connstring
            .clone()
            .or_else(|| spec.cluster.settings.find("neon.pageserver_connstring"))
            .ok_or("pageserver connstr should be provided")?;
        let safekeeper_connstrings = if spec.safekeeper_connstrings.is_empty() {
            if matches!(spec.mode, ComputeMode::Primary) {
                spec.cluster
                    .settings
                    .find("neon.safekeepers")
                    .ok_or("safekeeper connstrings should be provided")?
                    .split(',')
                    .map(|str| str.to_string())
                    .collect()
            } else {
                vec![]
            }
        } else {
            spec.safekeeper_connstrings.clone()
        };
        let storage_auth_token = spec.storage_auth_token.clone();
        let tenant_id: TenantId = if let Some(tenant_id) = spec.tenant_id {
            tenant_id
        } else {
            spec.cluster
                .settings
                .find("neon.tenant_id")
                .ok_or("tenant id should be provided")
                .map(|s| TenantId::from_str(&s))?
                .or(Err("invalid tenant id"))?
        };
        let timeline_id: TimelineId = if let Some(timeline_id) = spec.timeline_id {
            timeline_id
        } else {
            spec.cluster
                .settings
                .find("neon.timeline_id")
                .ok_or("timeline id should be provided")
                .map(|s| TimelineId::from_str(&s))?
                .or(Err("invalid timeline id"))?
        };

        Ok(ParsedSpec {
            spec,
            pageserver_connstr,
            safekeeper_connstrings,
            storage_auth_token,
            tenant_id,
            timeline_id,
        })
    }
}

/// If we are a VM, returns a [`Command`] that will run in the `neon-postgres`
/// cgroup. Otherwise returns the default `Command::new(cmd)`
///
/// This function should be used to start postgres, as it will start it in the
/// neon-postgres cgroup if we are a VM. This allows autoscaling to control
/// postgres' resource usage. The cgroup will exist in VMs because vm-builder
/// creates it during the sysinit phase of its inittab.
fn maybe_cgexec(cmd: &str) -> Command {
    // The cplane sets this env var for autoscaling computes.
    // use `var_os` so we don't have to worry about the variable being valid
    // unicode. Should never be an concern . . . but just in case
    if env::var_os("AUTOSCALING").is_some() {
        let mut command = Command::new("cgexec");
        command.args(["-g", "memory:neon-postgres"]);
        command.arg(cmd);
        command
    } else {
        Command::new(cmd)
    }
}

struct PostgresHandle {
    postgres: std::process::Child,
    log_collector: tokio::task::JoinHandle<Result<()>>,
}

impl PostgresHandle {
    /// Return PID of the postgres (postmaster) process
    fn pid(&self) -> Pid {
        Pid::from_raw(self.postgres.id() as i32)
    }
}

struct StartVmMonitorResult {
    #[cfg(target_os = "linux")]
    token: tokio_util::sync::CancellationToken,
    #[cfg(target_os = "linux")]
    vm_monitor: Option<tokio::task::JoinHandle<Result<()>>>,
}

pub(crate) fn construct_superuser_query(spec: &ComputeSpec) -> String {
    let roles = spec
        .cluster
        .roles
        .iter()
        .map(|r| escape_literal(&r.name))
        .collect::<Vec<_>>();

    let dbs = spec
        .cluster
        .databases
        .iter()
        .map(|db| escape_literal(&db.name))
        .collect::<Vec<_>>();

    let roles_decl = if roles.is_empty() {
        String::from("roles text[] := NULL;")
    } else {
        format!(
            r#"
               roles text[] := ARRAY(SELECT rolname
                                     FROM pg_catalog.pg_roles
                                     WHERE rolname IN ({}));"#,
            roles.join(", ")
        )
    };

    let database_decl = if dbs.is_empty() {
        String::from("dbs text[] := NULL;")
    } else {
        format!(
            r#"
               dbs text[] := ARRAY(SELECT datname
                                   FROM pg_catalog.pg_database
                                   WHERE datname IN ({}));"#,
            dbs.join(", ")
        )
    };

    // ALL PRIVILEGES grants CREATE, CONNECT, and TEMPORARY on all databases
    // (see https://www.postgresql.org/docs/current/ddl-priv.html)
    let query = format!(
        r#"
            DO $$
                DECLARE
                    r text;
                    {}
                    {}
                BEGIN
                    IF NOT EXISTS (
                        SELECT FROM pg_catalog.pg_roles WHERE rolname = 'neon_superuser')
                    THEN
                        CREATE ROLE neon_superuser CREATEDB CREATEROLE NOLOGIN REPLICATION BYPASSRLS IN ROLE pg_read_all_data, pg_write_all_data;
                        IF array_length(roles, 1) IS NOT NULL THEN
                            EXECUTE format('GRANT neon_superuser TO %s',
                                           array_to_string(ARRAY(SELECT quote_ident(x) FROM unnest(roles) as x), ', '));
                            FOREACH r IN ARRAY roles LOOP
                                EXECUTE format('ALTER ROLE %s CREATEROLE CREATEDB', quote_ident(r));
                            END LOOP;
                        END IF;
                        IF array_length(dbs, 1) IS NOT NULL THEN
                            EXECUTE format('GRANT ALL PRIVILEGES ON DATABASE %s TO neon_superuser',
                                           array_to_string(ARRAY(SELECT quote_ident(x) FROM unnest(dbs) as x), ', '));
                        END IF;
                    END IF;
                END
            $$;"#,
        roles_decl, database_decl,
    );

    query
}

impl ComputeNode {
    pub fn new(
        params: ComputeNodeParams,
        cli_spec: Option<ComputeSpec>,
        compute_ctl_config: ComputeCtlConfig,
    ) -> Result<Self> {
        let connstr = params.connstr.as_str();
        let conn_conf = postgres::config::Config::from_str(connstr)
            .context("cannot build postgres config from connstr")?;
        let tokio_conn_conf = tokio_postgres::config::Config::from_str(connstr)
            .context("cannot build tokio postgres config from connstr")?;

        let mut new_state = ComputeState::new();
        if let Some(cli_spec) = cli_spec {
            let pspec = ParsedSpec::try_from(cli_spec).map_err(|msg| anyhow::anyhow!(msg))?;
            new_state.pspec = Some(pspec);
        }
        new_state.compute_ctl_config = compute_ctl_config;

        Ok(ComputeNode {
            params,
            conn_conf,
            tokio_conn_conf,
            state: Mutex::new(new_state),
            state_changed: Condvar::new(),
            ext_download_progress: RwLock::new(HashMap::new()),
        })
    }

    /// Top-level control flow of compute_ctl. Returns a process exit code we should
    /// exit with.
    pub fn run(self) -> Result<Option<i32>> {
        let this = Arc::new(self);

        let cli_spec = this.state.lock().unwrap().pspec.clone();

        // If this is a pooled VM, prewarm before starting HTTP server and becoming
        // available for binding. Prewarming helps Postgres start quicker later,
        // because QEMU will already have its memory allocated from the host, and
        // the necessary binaries will already be cached.
        if cli_spec.is_none() {
            this.prewarm_postgres()?;
        }

        // Launch the external HTTP server first, so that we can serve control plane
        // requests while configuration is still in progress.
        crate::http::server::Server::External {
            port: this.params.external_http_port,
            jwks: this.state.lock().unwrap().compute_ctl_config.jwks.clone(),
            compute_id: this.params.compute_id.clone(),
        }
        .launch(&this);

        // The internal HTTP server could be launched later, but there isn't much
        // sense in waiting.
        crate::http::server::Server::Internal {
            port: this.params.internal_http_port,
        }
        .launch(&this);

        // If we got a spec from the CLI already, use that. Otherwise wait for the
        // control plane to pass it to us with a /configure HTTP request
        let pspec = if let Some(cli_spec) = cli_spec {
            cli_spec
        } else {
            this.wait_spec()?
        };

        launch_lsn_lease_bg_task_for_static(&this);

        // We have a spec, start the compute
        let mut delay_exit = false;
        let mut vm_monitor = None;
        let mut pg_process: Option<PostgresHandle> = None;

        match this.start_compute(&mut pg_process) {
            Ok(()) => {
                // Success! Launch remaining services (just vm-monitor currently)
                vm_monitor =
                    Some(this.start_vm_monitor(pspec.spec.disable_lfc_resizing.unwrap_or(false)));
            }
            Err(err) => {
                // Something went wrong with the startup. Log it and expose the error to
                // HTTP status requests.
                error!("could not start the compute node: {:#}", err);
                this.set_failed_status(err);
                delay_exit = true;

                // If the error happened after starting PostgreSQL, kill it
                if let Some(ref pg_process) = pg_process {
                    kill(pg_process.pid(), Signal::SIGQUIT).ok();
                }
            }
        }

        // If startup was successful, or it failed in the late stages,
        // PostgreSQL is now running. Wait until it exits.
        let exit_code = if let Some(pg_handle) = pg_process {
            let exit_status = this.wait_postgres(pg_handle);
            info!("Postgres exited with code {}, shutting down", exit_status);
            exit_status.code()
        } else {
            None
        };

        // Terminate the vm_monitor so it releases the file watcher on
        // /sys/fs/cgroup/neon-postgres.
        // Note: the vm-monitor only runs on linux because it requires cgroups.
        if let Some(vm_monitor) = vm_monitor {
            cfg_if::cfg_if! {
                if #[cfg(target_os = "linux")] {
                    // Kills all threads spawned by the monitor
                    vm_monitor.token.cancel();
                    if let Some(handle) = vm_monitor.vm_monitor {
                        // Kills the actual task running the monitor
                        handle.abort();
                    }
                } else {
                    _ = vm_monitor; // appease unused lint on macOS
                }
            }
        }

        // Reap the postgres process
        delay_exit |= this.cleanup_after_postgres_exit()?;

        // If launch failed, keep serving HTTP requests for a while, so the cloud
        // control plane can get the actual error.
        if delay_exit {
            info!("giving control plane 30s to collect the error before shutdown");
            std::thread::sleep(Duration::from_secs(30));
        }
        Ok(exit_code)
    }

    pub fn wait_spec(&self) -> Result<ParsedSpec> {
        info!("no compute spec provided, waiting");
        let mut state = self.state.lock().unwrap();
        while state.status != ComputeStatus::ConfigurationPending {
            state = self.state_changed.wait(state).unwrap();
        }

        info!("got spec, continue configuration");
        let spec = state.pspec.as_ref().unwrap().clone();

        // Record for how long we slept waiting for the spec.
        let now = Utc::now();
        state.metrics.wait_for_spec_ms = now
            .signed_duration_since(state.start_time)
            .to_std()
            .unwrap()
            .as_millis() as u64;

        // Reset start time, so that the total startup time that is calculated later will
        // not include the time that we waited for the spec.
        state.start_time = now;

        Ok(spec)
    }

    /// Start compute.
    ///
    /// Prerequisites:
    /// - the compute spec has been placed in self.state.pspec
    ///
    /// On success:
    /// - status is set to ComputeStatus::Running
    /// - self.running_postgres is set
    ///
    /// On error:
    /// - status is left in ComputeStatus::Init. The caller is responsible for setting it to Failed
    /// - if Postgres was started before the fatal error happened, self.running_postgres is
    ///   set. The caller is responsible for killing it.
    ///
    /// Note that this is in the critical path of a compute cold start. Keep this fast.
    /// Try to do things concurrently, to hide the latencies.
    fn start_compute(self: &Arc<Self>, pg_handle: &mut Option<PostgresHandle>) -> Result<()> {
        let compute_state: ComputeState;

        let start_compute_span;
        let _this_entered;
        {
            let mut state_guard = self.state.lock().unwrap();

            // Create a tracing span for the startup operation.
            //
            // We could otherwise just annotate the function with #[instrument], but if
            // we're being configured from a /configure HTTP request, we want the
            // startup to be considered part of the /configure request.
            //
            // Similarly, if a trace ID was passed in env variables, attach it to the span.
            start_compute_span = {
                // Temporarily enter the parent span, so that the new span becomes its child.
                if let Some(p) = state_guard.startup_span.take() {
                    let _parent_entered = p.entered();
                    tracing::info_span!("start_compute")
                } else if let Some(otel_context) = startup_context_from_env() {
                    use tracing_opentelemetry::OpenTelemetrySpanExt;
                    let span = tracing::info_span!("start_compute");
                    span.set_parent(otel_context);
                    span
                } else {
                    tracing::info_span!("start_compute")
                }
            };
            _this_entered = start_compute_span.enter();

            state_guard.set_status(ComputeStatus::Init, &self.state_changed);
            compute_state = state_guard.clone()
        }

        let pspec = compute_state.pspec.as_ref().expect("spec must be set");
        info!(
            "starting compute for project {}, operation {}, tenant {}, timeline {}, features {:?}, spec.remote_extensions {:?}",
            pspec.spec.cluster.cluster_id.as_deref().unwrap_or("None"),
            pspec.spec.operation_uuid.as_deref().unwrap_or("None"),
            pspec.tenant_id,
            pspec.timeline_id,
            pspec.spec.features,
            pspec.spec.remote_extensions,
        );

        ////// PRE-STARTUP PHASE: things that need to be finished before we start the Postgres process

        // Collect all the tasks that must finish here
        let mut pre_tasks = tokio::task::JoinSet::new();

        // If there are any remote extensions in shared_preload_libraries, start downloading them
        if pspec.spec.remote_extensions.is_some() {
            let (this, spec) = (self.clone(), pspec.spec.clone());
            pre_tasks.spawn(async move {
                this.download_preload_extensions(&spec)
                    .in_current_span()
                    .await
            });
        }

        // Prepare pgdata directory. This downloads the basebackup, among other things.
        {
            let (this, cs) = (self.clone(), compute_state.clone());
            pre_tasks.spawn_blocking_child(move || this.prepare_pgdata(&cs));
        }

        // Resize swap to the desired size if the compute spec says so
        if let (Some(size_bytes), true) =
            (pspec.spec.swap_size_bytes, self.params.resize_swap_on_bind)
        {
            pre_tasks.spawn_blocking_child(move || {
                // To avoid 'swapoff' hitting postgres startup, we need to run resize-swap to completion
                // *before* starting postgres.
                //
                // In theory, we could do this asynchronously if SkipSwapon was enabled for VMs, but this
                // carries a risk of introducing hard-to-debug issues - e.g. if postgres sometimes gets
                // OOM-killed during startup because swap wasn't available yet.
                resize_swap(size_bytes).context("failed to resize swap")?;
                let size_mib = size_bytes as f32 / (1 << 20) as f32; // just for more coherent display.
                info!(%size_bytes, %size_mib, "resized swap");

                Ok::<(), anyhow::Error>(())
            });
        }

        // Set disk quota if the compute spec says so
        if let (Some(disk_quota_bytes), Some(disk_quota_fs_mountpoint)) = (
            pspec.spec.disk_quota_bytes,
            self.params.set_disk_quota_for_fs.as_ref(),
        ) {
            let disk_quota_fs_mountpoint = disk_quota_fs_mountpoint.clone();
            pre_tasks.spawn_blocking_child(move || {
                set_disk_quota(disk_quota_bytes, &disk_quota_fs_mountpoint)
                    .context("failed to set disk quota")?;
                let size_mib = disk_quota_bytes as f32 / (1 << 20) as f32; // just for more coherent display.
                info!(%disk_quota_bytes, %size_mib, "set disk quota");

                Ok::<(), anyhow::Error>(())
            });
        }

        // tune pgbouncer
        if let Some(pgbouncer_settings) = &pspec.spec.pgbouncer_settings {
            info!("tuning pgbouncer");

            // Spawn a background task to do the tuning,
            // so that we don't block the main thread that starts Postgres.
            let pgbouncer_settings = pgbouncer_settings.clone();
            let _handle = tokio::spawn(async move {
                let res = tune_pgbouncer(pgbouncer_settings).await;
                if let Err(err) = res {
                    error!("error while tuning pgbouncer: {err:?}");
                    // Continue with the startup anyway
                }
            });
        }

        // configure local_proxy
        if let Some(local_proxy) = &pspec.spec.local_proxy_config {
            info!("configuring local_proxy");

            // Spawn a background task to do the configuration,
            // so that we don't block the main thread that starts Postgres.
            let local_proxy = local_proxy.clone();
            let _handle = tokio::spawn(async move {
                if let Err(err) = local_proxy::configure(&local_proxy) {
                    error!("error while configuring local_proxy: {err:?}");
                    // Continue with the startup anyway
                }
            });
        }

        // Configure and start rsyslog if necessary
        if let ComputeAudit::Hipaa = pspec.spec.audit_log_level {
            let remote_endpoint = std::env::var("AUDIT_LOGGING_ENDPOINT").unwrap_or("".to_string());
            if remote_endpoint.is_empty() {
                anyhow::bail!("AUDIT_LOGGING_ENDPOINT is empty");
            }

            let log_directory_path = Path::new(&self.params.pgdata).join("log");
            // TODO: make this more robust
            // now rsyslog starts once and there is no monitoring or restart if it fails
            configure_audit_rsyslog(
                log_directory_path.to_str().unwrap(),
                "hipaa",
                &remote_endpoint,
            )?;
        }

        // Launch remaining service threads
        let _monitor_handle = launch_monitor(self);
        let _configurator_handle = launch_configurator(self);

        // Wait for all the pre-tasks to finish before starting postgres
        let rt = tokio::runtime::Handle::current();
        while let Some(res) = rt.block_on(pre_tasks.join_next()) {
            res??;
        }

        ////// START POSTGRES
        let start_time = Utc::now();
        let pg_process = self.start_postgres(pspec.storage_auth_token.clone())?;
        let postmaster_pid = pg_process.pid();
        *pg_handle = Some(pg_process);

        // If this is a primary endpoint, perform some post-startup configuration before
        // opening it up for the world.
        let config_time = Utc::now();
        if pspec.spec.mode == ComputeMode::Primary {
            self.configure_as_primary(&compute_state)?;

            let conf = self.get_conn_conf(None);
            tokio::task::spawn_blocking(|| {
                let res = get_installed_extensions(conf);
                match res {
                    Ok(extensions) => {
                        info!(
                            "[NEON_EXT_STAT] {}",
                            serde_json::to_string(&extensions)
                                .expect("failed to serialize extensions list")
                        );
                    }
                    Err(err) => error!("could not get installed extensions: {err:?}"),
                }
            });
        }

        // All done!
        let startup_end_time = Utc::now();
        let metrics = {
            let mut state = self.state.lock().unwrap();
            state.metrics.start_postgres_ms = config_time
                .signed_duration_since(start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64;
            state.metrics.config_ms = startup_end_time
                .signed_duration_since(config_time)
                .to_std()
                .unwrap()
                .as_millis() as u64;
            state.metrics.total_startup_ms = startup_end_time
                .signed_duration_since(compute_state.start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64;
            state.metrics.clone()
        };
        self.set_status(ComputeStatus::Running);

        // Log metrics so that we can search for slow operations in logs
        info!(?metrics, postmaster_pid = %postmaster_pid, "compute start finished");

        Ok(())
    }

    #[instrument(skip_all)]
    async fn download_preload_extensions(&self, spec: &ComputeSpec) -> Result<()> {
        let remote_extensions = if let Some(remote_extensions) = &spec.remote_extensions {
            remote_extensions
        } else {
            return Ok(());
        };

        // First, create control files for all available extensions
        extension_server::create_control_files(remote_extensions, &self.params.pgbin);

        let library_load_start_time = Utc::now();
        let remote_ext_metrics = self.prepare_preload_libraries(spec).await?;

        let library_load_time = Utc::now()
            .signed_duration_since(library_load_start_time)
            .to_std()
            .unwrap()
            .as_millis() as u64;
        let mut state = self.state.lock().unwrap();
        state.metrics.load_ext_ms = library_load_time;
        state.metrics.num_ext_downloaded = remote_ext_metrics.num_ext_downloaded;
        state.metrics.largest_ext_size = remote_ext_metrics.largest_ext_size;
        state.metrics.total_ext_download_size = remote_ext_metrics.total_ext_download_size;
        info!(
            "Loading shared_preload_libraries took {:?}ms",
            library_load_time
        );
        info!("{:?}", remote_ext_metrics);

        Ok(())
    }

    /// Start the vm-monitor if directed to. The vm-monitor only runs on linux
    /// because it requires cgroups.
    fn start_vm_monitor(&self, disable_lfc_resizing: bool) -> StartVmMonitorResult {
        cfg_if::cfg_if! {
            if #[cfg(target_os = "linux")] {
                use std::env;
                use tokio_util::sync::CancellationToken;

                // This token is used internally by the monitor to clean up all threads
                let token = CancellationToken::new();

                // don't pass postgres connection string to vm-monitor if we don't want it to resize LFC
                let pgconnstr = if disable_lfc_resizing {
                    None
                } else {
                    Some(self.params.filecache_connstr.clone())
                };

                let vm_monitor = if env::var_os("AUTOSCALING").is_some() {
                    let vm_monitor = tokio::spawn(vm_monitor::start(
                        Box::leak(Box::new(vm_monitor::Args {
                            cgroup: Some(self.params.cgroup.clone()),
                            pgconnstr,
                            addr: self.params.vm_monitor_addr.clone(),
                        })),
                        token.clone(),
                    ));
                    Some(vm_monitor)
                } else {
                    None
                };
                StartVmMonitorResult { token, vm_monitor }
            } else {
                _ = disable_lfc_resizing; // appease unused lint on macOS
                StartVmMonitorResult { }
            }
        }
    }

    fn cleanup_after_postgres_exit(&self) -> Result<bool> {
        // Maybe sync safekeepers again, to speed up next startup
        let compute_state = self.state.lock().unwrap().clone();
        let pspec = compute_state.pspec.as_ref().expect("spec must be set");
        if matches!(pspec.spec.mode, compute_api::spec::ComputeMode::Primary) {
            info!("syncing safekeepers on shutdown");
            let storage_auth_token = pspec.storage_auth_token.clone();
            let lsn = self.sync_safekeepers(storage_auth_token)?;
            info!("synced safekeepers at lsn {lsn}");
        }

        let mut delay_exit = false;
        let mut state = self.state.lock().unwrap();
        if state.status == ComputeStatus::TerminationPending {
            state.status = ComputeStatus::Terminated;
            self.state_changed.notify_all();
            // we were asked to terminate gracefully, don't exit to avoid restart
            delay_exit = true
        }
        drop(state);

        if let Err(err) = self.check_for_core_dumps() {
            error!("error while checking for core dumps: {err:?}");
        }

        Ok(delay_exit)
    }

    /// Check that compute node has corresponding feature enabled.
    pub fn has_feature(&self, feature: ComputeFeature) -> bool {
        let state = self.state.lock().unwrap();

        if let Some(s) = state.pspec.as_ref() {
            s.spec.features.contains(&feature)
        } else {
            false
        }
    }

    pub fn set_status(&self, status: ComputeStatus) {
        let mut state = self.state.lock().unwrap();
        state.set_status(status, &self.state_changed);
    }

    pub fn set_failed_status(&self, err: anyhow::Error) {
        let mut state = self.state.lock().unwrap();
        state.set_failed_status(err, &self.state_changed);
    }

    pub fn get_status(&self) -> ComputeStatus {
        self.state.lock().unwrap().status
    }

    pub fn get_timeline_id(&self) -> Option<TimelineId> {
        self.state
            .lock()
            .unwrap()
            .pspec
            .as_ref()
            .map(|s| s.timeline_id)
    }

    // Remove `pgdata` directory and create it again with right permissions.
    fn create_pgdata(&self) -> Result<()> {
        // Ignore removal error, likely it is a 'No such file or directory (os error 2)'.
        // If it is something different then create_dir() will error out anyway.
        let pgdata = &self.params.pgdata;
        let _ok = fs::remove_dir_all(pgdata);
        fs::create_dir(pgdata)?;
        fs::set_permissions(pgdata, fs::Permissions::from_mode(0o700))?;

        Ok(())
    }

    // Get basebackup from the libpq connection to pageserver using `connstr` and
    // unarchive it to `pgdata` directory overriding all its previous content.
    #[instrument(skip_all, fields(%lsn))]
    fn try_get_basebackup(&self, compute_state: &ComputeState, lsn: Lsn) -> Result<()> {
        let spec = compute_state.pspec.as_ref().expect("spec must be set");
        let start_time = Instant::now();

        let shard0_connstr = spec.pageserver_connstr.split(',').next().unwrap();
        let mut config = postgres::Config::from_str(shard0_connstr)?;

        // Use the storage auth token from the config file, if given.
        // Note: this overrides any password set in the connection string.
        if let Some(storage_auth_token) = &spec.storage_auth_token {
            info!("Got storage auth token from spec file");
            config.password(storage_auth_token);
        } else {
            info!("Storage auth token not set");
        }

        // Connect to pageserver
        let mut client = config.connect(NoTls)?;
        let pageserver_connect_micros = start_time.elapsed().as_micros() as u64;

        let basebackup_cmd = match lsn {
            Lsn(0) => {
                if spec.spec.mode != ComputeMode::Primary {
                    format!(
                        "basebackup {} {} --gzip --replica",
                        spec.tenant_id, spec.timeline_id
                    )
                } else {
                    format!("basebackup {} {} --gzip", spec.tenant_id, spec.timeline_id)
                }
            }
            _ => {
                if spec.spec.mode != ComputeMode::Primary {
                    format!(
                        "basebackup {} {} {} --gzip --replica",
                        spec.tenant_id, spec.timeline_id, lsn
                    )
                } else {
                    format!(
                        "basebackup {} {} {} --gzip",
                        spec.tenant_id, spec.timeline_id, lsn
                    )
                }
            }
        };

        let copyreader = client.copy_out(basebackup_cmd.as_str())?;
        let mut measured_reader = MeasuredReader::new(copyreader);
        let mut bufreader = std::io::BufReader::new(&mut measured_reader);

        // Read the archive directly from the `CopyOutReader`
        //
        // Set `ignore_zeros` so that unpack() reads all the Copy data and
        // doesn't stop at the end-of-archive marker. Otherwise, if the server
        // sends an Error after finishing the tarball, we will not notice it.
        let mut ar = tar::Archive::new(flate2::read::GzDecoder::new(&mut bufreader));
        ar.set_ignore_zeros(true);
        ar.unpack(&self.params.pgdata)?;

        // Report metrics
        let mut state = self.state.lock().unwrap();
        state.metrics.pageserver_connect_micros = pageserver_connect_micros;
        state.metrics.basebackup_bytes = measured_reader.get_byte_count() as u64;
        state.metrics.basebackup_ms = start_time.elapsed().as_millis() as u64;
        Ok(())
    }

    // Gets the basebackup in a retry loop
    #[instrument(skip_all, fields(%lsn))]
    pub fn get_basebackup(&self, compute_state: &ComputeState, lsn: Lsn) -> Result<()> {
        let mut retry_period_ms = 500.0;
        let mut attempts = 0;
        const DEFAULT_ATTEMPTS: u16 = 10;
        #[cfg(feature = "testing")]
        let max_attempts = if let Ok(v) = env::var("NEON_COMPUTE_TESTING_BASEBACKUP_RETRIES") {
            u16::from_str(&v).unwrap()
        } else {
            DEFAULT_ATTEMPTS
        };
        #[cfg(not(feature = "testing"))]
        let max_attempts = DEFAULT_ATTEMPTS;
        loop {
            let result = self.try_get_basebackup(compute_state, lsn);
            match result {
                Ok(_) => {
                    return result;
                }
                Err(ref e) if attempts < max_attempts => {
                    warn!(
                        "Failed to get basebackup: {} (attempt {}/{})",
                        e, attempts, max_attempts
                    );
                    std::thread::sleep(std::time::Duration::from_millis(retry_period_ms as u64));
                    retry_period_ms *= 1.5;
                }
                Err(_) => {
                    return result;
                }
            }
            attempts += 1;
        }
    }

    pub async fn check_safekeepers_synced_async(
        &self,
        compute_state: &ComputeState,
    ) -> Result<Option<Lsn>> {
        // Construct a connection config for each safekeeper
        let pspec: ParsedSpec = compute_state
            .pspec
            .as_ref()
            .expect("spec must be set")
            .clone();
        let sk_connstrs: Vec<String> = pspec.safekeeper_connstrings.clone();
        let sk_configs = sk_connstrs.into_iter().map(|connstr| {
            // Format connstr
            let id = connstr.clone();
            let connstr = format!("postgresql://no_user@{}", connstr);
            let options = format!(
                "-c timeline_id={} tenant_id={}",
                pspec.timeline_id, pspec.tenant_id
            );

            // Construct client
            let mut config = tokio_postgres::Config::from_str(&connstr).unwrap();
            config.options(&options);
            if let Some(storage_auth_token) = pspec.storage_auth_token.clone() {
                config.password(storage_auth_token);
            }

            (id, config)
        });

        // Create task set to query all safekeepers
        let mut tasks = FuturesUnordered::new();
        let quorum = sk_configs.len() / 2 + 1;
        for (id, config) in sk_configs {
            let timeout = tokio::time::Duration::from_millis(100);
            let task = tokio::time::timeout(timeout, ping_safekeeper(id, config));
            tasks.push(tokio::spawn(task));
        }

        // Get a quorum of responses or errors
        let mut responses = Vec::new();
        let mut join_errors = Vec::new();
        let mut task_errors = Vec::new();
        let mut timeout_errors = Vec::new();
        while let Some(response) = tasks.next().await {
            match response {
                Ok(Ok(Ok(r))) => responses.push(r),
                Ok(Ok(Err(e))) => task_errors.push(e),
                Ok(Err(e)) => timeout_errors.push(e),
                Err(e) => join_errors.push(e),
            };
            if responses.len() >= quorum {
                break;
            }
            if join_errors.len() + task_errors.len() + timeout_errors.len() >= quorum {
                break;
            }
        }

        // In case of error, log and fail the check, but don't crash.
        // We're playing it safe because these errors could be transient
        // and we don't yet retry. Also being careful here allows us to
        // be backwards compatible with safekeepers that don't have the
        // TIMELINE_STATUS API yet.
        if responses.len() < quorum {
            error!(
                "failed sync safekeepers check {:?} {:?} {:?}",
                join_errors, task_errors, timeout_errors
            );
            return Ok(None);
        }

        Ok(check_if_synced(responses))
    }

    // Fast path for sync_safekeepers. If they're already synced we get the lsn
    // in one roundtrip. If not, we should do a full sync_safekeepers.
    #[instrument(skip_all)]
    pub fn check_safekeepers_synced(&self, compute_state: &ComputeState) -> Result<Option<Lsn>> {
        let start_time = Utc::now();

        let rt = tokio::runtime::Handle::current();
        let result = rt.block_on(self.check_safekeepers_synced_async(compute_state));

        // Record runtime
        self.state.lock().unwrap().metrics.sync_sk_check_ms = Utc::now()
            .signed_duration_since(start_time)
            .to_std()
            .unwrap()
            .as_millis() as u64;
        result
    }

    // Run `postgres` in a special mode with `--sync-safekeepers` argument
    // and return the reported LSN back to the caller.
    #[instrument(skip_all)]
    pub fn sync_safekeepers(&self, storage_auth_token: Option<String>) -> Result<Lsn> {
        let start_time = Utc::now();

        let mut sync_handle = maybe_cgexec(&self.params.pgbin)
            .args(["--sync-safekeepers"])
            .env("PGDATA", &self.params.pgdata) // we cannot use -D in this mode
            .envs(if let Some(storage_auth_token) = &storage_auth_token {
                vec![("NEON_AUTH_TOKEN", storage_auth_token)]
            } else {
                vec![]
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("postgres --sync-safekeepers failed to start");
        SYNC_SAFEKEEPERS_PID.store(sync_handle.id(), Ordering::SeqCst);

        // `postgres --sync-safekeepers` will print all log output to stderr and
        // final LSN to stdout. So we leave stdout to collect LSN, while stderr logs
        // will be collected in a child thread.
        let stderr = sync_handle
            .stderr
            .take()
            .expect("stderr should be captured");
        let logs_handle = handle_postgres_logs(stderr);

        let sync_output = sync_handle
            .wait_with_output()
            .expect("postgres --sync-safekeepers failed");
        SYNC_SAFEKEEPERS_PID.store(0, Ordering::SeqCst);

        // Process has exited, so we can join the logs thread.
        let _ = tokio::runtime::Handle::current()
            .block_on(logs_handle)
            .map_err(|e| tracing::error!("log task panicked: {:?}", e));

        if !sync_output.status.success() {
            anyhow::bail!(
                "postgres --sync-safekeepers exited with non-zero status: {}. stdout: {}",
                sync_output.status,
                String::from_utf8(sync_output.stdout)
                    .expect("postgres --sync-safekeepers exited, and stdout is not utf-8"),
            );
        }

        self.state.lock().unwrap().metrics.sync_safekeepers_ms = Utc::now()
            .signed_duration_since(start_time)
            .to_std()
            .unwrap()
            .as_millis() as u64;

        let lsn = Lsn::from_str(String::from_utf8(sync_output.stdout)?.trim())?;

        Ok(lsn)
    }

    /// Do all the preparations like PGDATA directory creation, configuration,
    /// safekeepers sync, basebackup, etc.
    #[instrument(skip_all)]
    pub fn prepare_pgdata(&self, compute_state: &ComputeState) -> Result<()> {
        let pspec = compute_state.pspec.as_ref().expect("spec must be set");
        let spec = &pspec.spec;
        let pgdata_path = Path::new(&self.params.pgdata);

        // Remove/create an empty pgdata directory and put configuration there.
        self.create_pgdata()?;
        config::write_postgres_conf(
            &pgdata_path.join("postgresql.conf"),
            &pspec.spec,
            self.params.internal_http_port,
        )?;

        // Syncing safekeepers is only safe with primary nodes: if a primary
        // is already connected it will be kicked out, so a secondary (standby)
        // cannot sync safekeepers.
        let lsn = match spec.mode {
            ComputeMode::Primary => {
                info!("checking if safekeepers are synced");
                let lsn = if let Ok(Some(lsn)) = self.check_safekeepers_synced(compute_state) {
                    lsn
                } else {
                    info!("starting safekeepers syncing");
                    self.sync_safekeepers(pspec.storage_auth_token.clone())
                        .with_context(|| "failed to sync safekeepers")?
                };
                info!("safekeepers synced at LSN {}", lsn);
                lsn
            }
            ComputeMode::Static(lsn) => {
                info!("Starting read-only node at static LSN {}", lsn);
                lsn
            }
            ComputeMode::Replica => {
                info!("Initializing standby from latest Pageserver LSN");
                Lsn(0)
            }
        };

        info!(
            "getting basebackup@{} from pageserver {}",
            lsn, &pspec.pageserver_connstr
        );
        self.get_basebackup(compute_state, lsn).with_context(|| {
            format!(
                "failed to get basebackup@{} from pageserver {}",
                lsn, &pspec.pageserver_connstr
            )
        })?;

        // Update pg_hba.conf received with basebackup.
        update_pg_hba(pgdata_path)?;

        // Place pg_dynshmem under /dev/shm. This allows us to use
        // 'dynamic_shared_memory_type = mmap' so that the files are placed in
        // /dev/shm, similar to how 'dynamic_shared_memory_type = posix' works.
        //
        // Why on earth don't we just stick to the 'posix' default, you might
        // ask.  It turns out that making large allocations with 'posix' doesn't
        // work very well with autoscaling. The behavior we want is that:
        //
        // 1. You can make large DSM allocations, larger than the current RAM
        //    size of the VM, without errors
        //
        // 2. If the allocated memory is really used, the VM is scaled up
        //    automatically to accommodate that
        //
        // We try to make that possible by having swap in the VM. But with the
        // default 'posix' DSM implementation, we fail step 1, even when there's
        // plenty of swap available. PostgreSQL uses posix_fallocate() to create
        // the shmem segment, which is really just a file in /dev/shm in Linux,
        // but posix_fallocate() on tmpfs returns ENOMEM if the size is larger
        // than available RAM.
        //
        // Using 'dynamic_shared_memory_type = mmap' works around that, because
        // the Postgres 'mmap' DSM implementation doesn't use
        // posix_fallocate(). Instead, it uses repeated calls to write(2) to
        // fill the file with zeros. It's weird that that differs between
        // 'posix' and 'mmap', but we take advantage of it. When the file is
        // filled slowly with write(2), the kernel allows it to grow larger, as
        // long as there's swap available.
        //
        // In short, using 'dynamic_shared_memory_type = mmap' allows us one DSM
        // segment to be larger than currently available RAM. But because we
        // don't want to store it on a real file, which the kernel would try to
        // flush to disk, so symlink pg_dynshm to /dev/shm.
        //
        // We don't set 'dynamic_shared_memory_type = mmap' here, we let the
        // control plane control that option. If 'mmap' is not used, this
        // symlink doesn't affect anything.
        //
        // See https://github.com/neondatabase/autoscaling/issues/800
        std::fs::remove_dir(pgdata_path.join("pg_dynshmem"))?;
        symlink("/dev/shm/", pgdata_path.join("pg_dynshmem"))?;

        match spec.mode {
            ComputeMode::Primary => {}
            ComputeMode::Replica | ComputeMode::Static(..) => {
                add_standby_signal(pgdata_path)?;
            }
        }

        Ok(())
    }

    /// Start and stop a postgres process to warm up the VM for startup.
    pub fn prewarm_postgres(&self) -> Result<()> {
        info!("prewarming");

        // Create pgdata
        let pgdata = &format!("{}.warmup", self.params.pgdata);
        create_pgdata(pgdata)?;

        // Run initdb to completion
        info!("running initdb");
        let initdb_bin = Path::new(&self.params.pgbin)
            .parent()
            .unwrap()
            .join("initdb");
        Command::new(initdb_bin)
            .args(["--pgdata", pgdata])
            .output()
            .expect("cannot start initdb process");

        // Write conf
        use std::io::Write;
        let conf_path = Path::new(pgdata).join("postgresql.conf");
        let mut file = std::fs::File::create(conf_path)?;
        writeln!(file, "shared_buffers=65536")?;
        writeln!(file, "port=51055")?; // Nobody should be connecting
        writeln!(file, "shared_preload_libraries = 'neon'")?;

        // Start postgres
        info!("starting postgres");
        let mut pg = maybe_cgexec(&self.params.pgbin)
            .args(["-D", pgdata])
            .spawn()
            .expect("cannot start postgres process");

        // Stop it when it's ready
        info!("waiting for postgres");
        wait_for_postgres(&mut pg, Path::new(pgdata))?;
        // SIGQUIT orders postgres to exit immediately. We don't want to SIGKILL
        // it to avoid orphaned processes prowling around while datadir is
        // wiped.
        let pm_pid = Pid::from_raw(pg.id() as i32);
        kill(pm_pid, Signal::SIGQUIT)?;
        info!("sent SIGQUIT signal");
        pg.wait()?;
        info!("done prewarming");

        // clean up
        let _ok = fs::remove_dir_all(pgdata);
        Ok(())
    }

    /// Start Postgres as a child process and wait for it to start accepting
    /// connections.
    ///
    /// Returns a handle to the child process and a handle to the logs thread.
    #[instrument(skip_all)]
    pub fn start_postgres(&self, storage_auth_token: Option<String>) -> Result<PostgresHandle> {
        let pgdata_path = Path::new(&self.params.pgdata);

        // Run postgres as a child process.
        let mut pg = maybe_cgexec(&self.params.pgbin)
            .args(["-D", &self.params.pgdata])
            .envs(if let Some(storage_auth_token) = &storage_auth_token {
                vec![("NEON_AUTH_TOKEN", storage_auth_token)]
            } else {
                vec![]
            })
            .stderr(Stdio::piped())
            .spawn()
            .expect("cannot start postgres process");
        PG_PID.store(pg.id(), Ordering::SeqCst);

        // Start a task to collect logs from stderr.
        let stderr = pg.stderr.take().expect("stderr should be captured");
        let logs_handle = handle_postgres_logs(stderr);

        wait_for_postgres(&mut pg, pgdata_path)?;

        Ok(PostgresHandle {
            postgres: pg,
            log_collector: logs_handle,
        })
    }

    /// Wait for the child Postgres process forever. In this state Ctrl+C will
    /// propagate to Postgres and it will be shut down as well.
    fn wait_postgres(&self, mut pg_handle: PostgresHandle) -> std::process::ExitStatus {
        info!(postmaster_pid = %pg_handle.postgres.id(), "Waiting for Postgres to exit");

        let ecode = pg_handle
            .postgres
            .wait()
            .expect("failed to start waiting on Postgres process");
        PG_PID.store(0, Ordering::SeqCst);

        // Process has exited. Wait for the log collecting task to finish.
        let _ = tokio::runtime::Handle::current()
            .block_on(pg_handle.log_collector)
            .map_err(|e| tracing::error!("log task panicked: {:?}", e));

        ecode
    }

    /// Do post configuration of the already started Postgres. This function spawns a background task to
    /// configure the database after applying the compute spec. Currently, it upgrades the neon extension
    /// version. In the future, it may upgrade all 3rd-party extensions.
    #[instrument(skip_all)]
    pub fn post_apply_config(&self) -> Result<()> {
        let conf = self.get_tokio_conn_conf(Some("compute_ctl:post_apply_config"));
        tokio::spawn(async move {
            let res = async {
                let (mut client, connection) = conf.connect(NoTls).await?;
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("connection error: {}", e);
                    }
                });

                handle_neon_extension_upgrade(&mut client)
                    .await
                    .context("handle_neon_extension_upgrade")?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Err(err) = res {
                error!("error while post_apply_config: {err:#}");
            }
        });
        Ok(())
    }

    pub fn get_conn_conf(&self, application_name: Option<&str>) -> postgres::Config {
        let mut conf = self.conn_conf.clone();
        if let Some(application_name) = application_name {
            conf.application_name(application_name);
        }
        conf
    }

    pub fn get_tokio_conn_conf(&self, application_name: Option<&str>) -> tokio_postgres::Config {
        let mut conf = self.tokio_conn_conf.clone();
        if let Some(application_name) = application_name {
            conf.application_name(application_name);
        }
        conf
    }

    pub async fn get_maintenance_client(
        conf: &tokio_postgres::Config,
    ) -> Result<tokio_postgres::Client> {
        let mut conf = conf.clone();
        conf.application_name("compute_ctl:apply_config");

        let (client, conn) = match conf.connect(NoTls).await {
            // If connection fails, it may be the old node with `zenith_admin` superuser.
            //
            // In this case we need to connect with old `zenith_admin` name
            // and create new user. We cannot simply rename connected user,
            // but we can create a new one and grant it all privileges.
            Err(e) => match e.code() {
                Some(&SqlState::INVALID_PASSWORD)
                | Some(&SqlState::INVALID_AUTHORIZATION_SPECIFICATION) => {
                    // Connect with zenith_admin if cloud_admin could not authenticate
                    info!(
                        "cannot connect to postgres: {}, retrying with `zenith_admin` username",
                        e
                    );
                    let mut zenith_admin_conf = postgres::config::Config::from(conf.clone());
                    zenith_admin_conf.application_name("compute_ctl:apply_config");
                    zenith_admin_conf.user("zenith_admin");

                    let mut client =
                        zenith_admin_conf.connect(NoTls)
                            .context("broken cloud_admin credential: tried connecting with cloud_admin but could not authenticate, and zenith_admin does not work either")?;

                    // Disable forwarding so that users don't get a cloud_admin role
                    let mut func = || {
                        client.simple_query("SET neon.forward_ddl = false")?;
                        client.simple_query("CREATE USER cloud_admin WITH SUPERUSER")?;
                        client.simple_query("GRANT zenith_admin TO cloud_admin")?;
                        Ok::<_, anyhow::Error>(())
                    };
                    func().context("apply_config setup cloud_admin")?;

                    drop(client);

                    // Reconnect with connstring with expected name
                    conf.connect(NoTls).await?
                }
                _ => return Err(e.into()),
            },
            Ok((client, conn)) => (client, conn),
        };

        spawn(async move {
            if let Err(e) = conn.await {
                error!("maintenance client connection error: {}", e);
            }
        });

        // Disable DDL forwarding because control plane already knows about the roles/databases
        // we're about to modify.
        client
            .simple_query("SET neon.forward_ddl = false")
            .await
            .context("apply_config SET neon.forward_ddl = false")?;

        Ok(client)
    }

    /// Do initial configuration of the already started Postgres.
    #[instrument(skip_all)]
    pub fn apply_config(&self, compute_state: &ComputeState) -> Result<()> {
        let conf = self.get_tokio_conn_conf(Some("compute_ctl:apply_config"));

        let conf = Arc::new(conf);
        let spec = Arc::new(
            compute_state
                .pspec
                .as_ref()
                .expect("spec must be set")
                .spec
                .clone(),
        );

        let max_concurrent_connections = self.max_service_connections(compute_state, &spec);

        // Merge-apply spec & changes to PostgreSQL state.
        self.apply_spec_sql(spec.clone(), conf.clone(), max_concurrent_connections)?;

        if let Some(local_proxy) = &spec.clone().local_proxy_config {
            info!("configuring local_proxy");
            local_proxy::configure(local_proxy).context("apply_config local_proxy")?;
        }

        // Run migrations separately to not hold up cold starts
        tokio::spawn(async move {
            let mut conf = conf.as_ref().clone();
            conf.application_name("compute_ctl:migrations");

            match conf.connect(NoTls).await {
                Ok((mut client, connection)) => {
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("connection error: {}", e);
                        }
                    });
                    if let Err(e) = handle_migrations(&mut client).await {
                        error!("Failed to run migrations: {}", e);
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to connect to the compute for running migrations: {}",
                        e
                    );
                }
            };
        });

        Ok::<(), anyhow::Error>(())
    }

    // Wrapped this around `pg_ctl reload`, but right now we don't use
    // `pg_ctl` for start / stop.
    #[instrument(skip_all)]
    fn pg_reload_conf(&self) -> Result<()> {
        let pgctl_bin = Path::new(&self.params.pgbin)
            .parent()
            .unwrap()
            .join("pg_ctl");
        Command::new(pgctl_bin)
            .args(["reload", "-D", &self.params.pgdata])
            .output()
            .expect("cannot run pg_ctl process");
        Ok(())
    }

    /// Similar to `apply_config()`, but does a bit different sequence of operations,
    /// as it's used to reconfigure a previously started and configured Postgres node.
    #[instrument(skip_all)]
    pub fn reconfigure(&self) -> Result<()> {
        let spec = self.state.lock().unwrap().pspec.clone().unwrap().spec;

        if let Some(ref pgbouncer_settings) = spec.pgbouncer_settings {
            info!("tuning pgbouncer");

            // Spawn a background task to do the tuning,
            // so that we don't block the main thread that starts Postgres.
            let pgbouncer_settings = pgbouncer_settings.clone();
            tokio::spawn(async move {
                let res = tune_pgbouncer(pgbouncer_settings).await;
                if let Err(err) = res {
                    error!("error while tuning pgbouncer: {err:?}");
                }
            });
        }

        if let Some(ref local_proxy) = spec.local_proxy_config {
            info!("configuring local_proxy");

            // Spawn a background task to do the configuration,
            // so that we don't block the main thread that starts Postgres.
            let local_proxy = local_proxy.clone();
            tokio::spawn(async move {
                if let Err(err) = local_proxy::configure(&local_proxy) {
                    error!("error while configuring local_proxy: {err:?}");
                }
            });
        }

        // Write new config
        let pgdata_path = Path::new(&self.params.pgdata);
        let postgresql_conf_path = pgdata_path.join("postgresql.conf");
        config::write_postgres_conf(&postgresql_conf_path, &spec, self.params.internal_http_port)?;

        if !spec.skip_pg_catalog_updates {
            let max_concurrent_connections = spec.reconfigure_concurrency;
            // Temporarily reset max_cluster_size in config
            // to avoid the possibility of hitting the limit, while we are reconfiguring:
            // creating new extensions, roles, etc.
            config::with_compute_ctl_tmp_override(pgdata_path, "neon.max_cluster_size=-1", || {
                self.pg_reload_conf()?;

                if spec.mode == ComputeMode::Primary {
                    let mut conf =
                        tokio_postgres::Config::from_str(self.params.connstr.as_str()).unwrap();
                    conf.application_name("apply_config");
                    let conf = Arc::new(conf);

                    let spec = Arc::new(spec.clone());

                    self.apply_spec_sql(spec, conf, max_concurrent_connections)?;
                }

                Ok(())
            })?;
        }

        self.pg_reload_conf()?;

        let unknown_op = "unknown".to_string();
        let op_id = spec.operation_uuid.as_ref().unwrap_or(&unknown_op);
        info!(
            "finished reconfiguration of compute node for operation {}",
            op_id
        );

        Ok(())
    }

    #[instrument(skip_all)]
    pub fn configure_as_primary(&self, compute_state: &ComputeState) -> Result<()> {
        let pspec = compute_state.pspec.as_ref().expect("spec must be set");

        assert!(pspec.spec.mode == ComputeMode::Primary);
        if !pspec.spec.skip_pg_catalog_updates {
            let pgdata_path = Path::new(&self.params.pgdata);
            // temporarily reset max_cluster_size in config
            // to avoid the possibility of hitting the limit, while we are applying config:
            // creating new extensions, roles, etc...
            config::with_compute_ctl_tmp_override(pgdata_path, "neon.max_cluster_size=-1", || {
                self.pg_reload_conf()?;

                self.apply_config(compute_state)?;

                Ok(())
            })?;

            let postgresql_conf_path = pgdata_path.join("postgresql.conf");
            if config::line_in_file(
                &postgresql_conf_path,
                "neon.disable_logical_replication_subscribers=false",
            )? {
                info!(
                    "updated postgresql.conf to set neon.disable_logical_replication_subscribers=false"
                );
            }
            self.pg_reload_conf()?;
        }
        self.post_apply_config()?;

        Ok(())
    }

    /// Update the `last_active` in the shared state, but ensure that it's a more recent one.
    pub fn update_last_active(&self, last_active: Option<DateTime<Utc>>) {
        let mut state = self.state.lock().unwrap();
        // NB: `Some(<DateTime>)` is always greater than `None`.
        if last_active > state.last_active {
            state.last_active = last_active;
            debug!("set the last compute activity time to: {:?}", last_active);
        }
    }

    // Look for core dumps and collect backtraces.
    //
    // EKS worker nodes have following core dump settings:
    //   /proc/sys/kernel/core_pattern -> core
    //   /proc/sys/kernel/core_uses_pid -> 1
    //   ulimit -c -> unlimited
    // which results in core dumps being written to postgres data directory as core.<pid>.
    //
    // Use that as a default location and pattern, except macos where core dumps are written
    // to /cores/ directory by default.
    //
    // With default Linux settings, the core dump file is called just "core", so check for
    // that too.
    pub fn check_for_core_dumps(&self) -> Result<()> {
        let core_dump_dir = match std::env::consts::OS {
            "macos" => Path::new("/cores/"),
            _ => Path::new(&self.params.pgdata),
        };

        // Collect core dump paths if any
        info!("checking for core dumps in {}", core_dump_dir.display());
        let files = fs::read_dir(core_dump_dir)?;
        let cores = files.filter_map(|entry| {
            let entry = entry.ok()?;

            let is_core_dump = match entry.file_name().to_str()? {
                n if n.starts_with("core.") => true,
                "core" => true,
                _ => false,
            };
            if is_core_dump {
                Some(entry.path())
            } else {
                None
            }
        });

        // Print backtrace for each core dump
        for core_path in cores {
            warn!(
                "core dump found: {}, collecting backtrace",
                core_path.display()
            );

            // Try first with gdb
            let backtrace = Command::new("gdb")
                .args(["--batch", "-q", "-ex", "bt", &self.params.pgbin])
                .arg(&core_path)
                .output();

            // Try lldb if no gdb is found -- that is handy for local testing on macOS
            let backtrace = match backtrace {
                Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {
                    warn!("cannot find gdb, trying lldb");
                    Command::new("lldb")
                        .arg("-c")
                        .arg(&core_path)
                        .args(["--batch", "-o", "bt all", "-o", "quit"])
                        .output()
                }
                _ => backtrace,
            }?;

            warn!(
                "core dump backtrace: {}",
                String::from_utf8_lossy(&backtrace.stdout)
            );
            warn!(
                "debugger stderr: {}",
                String::from_utf8_lossy(&backtrace.stderr)
            );
        }

        Ok(())
    }

    /// Select `pg_stat_statements` data and return it as a stringified JSON
    pub async fn collect_insights(&self) -> String {
        let mut result_rows: Vec<String> = Vec::new();
        let conf = self.get_tokio_conn_conf(Some("compute_ctl:collect_insights"));
        let connect_result = conf.connect(NoTls).await;
        let (client, connection) = connect_result.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        let result = client
            .simple_query(
                "SELECT
    row_to_json(pg_stat_statements)
FROM
    pg_stat_statements
WHERE
    userid != 'cloud_admin'::regrole::oid
ORDER BY
    (mean_exec_time + mean_plan_time) DESC
LIMIT 100",
            )
            .await;

        if let Ok(raw_rows) = result {
            for message in raw_rows.iter() {
                if let postgres::SimpleQueryMessage::Row(row) = message {
                    if let Some(json) = row.get(0) {
                        result_rows.push(json.to_string());
                    }
                }
            }

            format!("{{\"pg_stat_statements\": [{}]}}", result_rows.join(","))
        } else {
            "{{\"pg_stat_statements\": []}}".to_string()
        }
    }

    // download an archive, unzip and place files in correct locations
    pub async fn download_extension(
        &self,
        real_ext_name: String,
        ext_path: RemotePath,
    ) -> Result<u64, DownloadError> {
        let ext_remote_storage =
            self.params
                .ext_remote_storage
                .as_ref()
                .ok_or(DownloadError::BadInput(anyhow::anyhow!(
                    "Remote extensions storage is not configured",
                )))?;

        let ext_archive_name = ext_path.object_name().expect("bad path");

        let mut first_try = false;
        if !self
            .ext_download_progress
            .read()
            .expect("lock err")
            .contains_key(ext_archive_name)
        {
            self.ext_download_progress
                .write()
                .expect("lock err")
                .insert(ext_archive_name.to_string(), (Utc::now(), false));
            first_try = true;
        }
        let (download_start, download_completed) =
            self.ext_download_progress.read().expect("lock err")[ext_archive_name];
        let start_time_delta = Utc::now()
            .signed_duration_since(download_start)
            .to_std()
            .unwrap()
            .as_millis() as u64;

        // how long to wait for extension download if it was started by another process
        const HANG_TIMEOUT: u64 = 3000; // milliseconds

        if download_completed {
            info!("extension already downloaded, skipping re-download");
            return Ok(0);
        } else if start_time_delta < HANG_TIMEOUT && !first_try {
            info!(
                "download {ext_archive_name} already started by another process, hanging untill completion or timeout"
            );
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(500));
            loop {
                info!("waiting for download");
                interval.tick().await;
                let (_, download_completed_now) =
                    self.ext_download_progress.read().expect("lock")[ext_archive_name];
                if download_completed_now {
                    info!("download finished by whoever else downloaded it");
                    return Ok(0);
                }
            }
            // NOTE: the above loop will get terminated
            // based on the timeout of the download function
        }

        // if extension hasn't been downloaded before or the previous
        // attempt to download was at least HANG_TIMEOUT ms ago
        // then we try to download it here
        info!("downloading new extension {ext_archive_name}");

        let download_size = extension_server::download_extension(
            &real_ext_name,
            &ext_path,
            ext_remote_storage,
            &self.params.pgbin,
        )
        .await
        .map_err(DownloadError::Other);

        if download_size.is_ok() {
            self.ext_download_progress
                .write()
                .expect("bad lock")
                .insert(ext_archive_name.to_string(), (download_start, true));
        }

        download_size
    }

    pub async fn set_role_grants(
        &self,
        db_name: &PgIdent,
        schema_name: &PgIdent,
        privileges: &[Privilege],
        role_name: &PgIdent,
    ) -> Result<()> {
        use tokio_postgres::NoTls;

        let mut conf = self.get_tokio_conn_conf(Some("compute_ctl:set_role_grants"));
        conf.dbname(db_name);

        let (db_client, conn) = conf
            .connect(NoTls)
            .await
            .context("Failed to connect to the database")?;
        tokio::spawn(conn);

        // TODO: support other types of grants apart from schemas?
        let query = format!(
            "GRANT {} ON SCHEMA {} TO {}",
            privileges
                .iter()
                // should not be quoted as it's part of the command.
                // is already sanitized so it's ok
                .map(|p| p.as_str())
                .collect::<Vec<&'static str>>()
                .join(", "),
            // quote the schema and role name as identifiers to sanitize them.
            schema_name.pg_quote(),
            role_name.pg_quote(),
        );
        db_client
            .simple_query(&query)
            .await
            .with_context(|| format!("Failed to execute query: {}", query))?;

        Ok(())
    }

    pub async fn install_extension(
        &self,
        ext_name: &PgIdent,
        db_name: &PgIdent,
        ext_version: ExtVersion,
    ) -> Result<ExtVersion> {
        use tokio_postgres::NoTls;

        let mut conf = self.get_tokio_conn_conf(Some("compute_ctl:install_extension"));
        conf.dbname(db_name);

        let (db_client, conn) = conf
            .connect(NoTls)
            .await
            .context("Failed to connect to the database")?;
        tokio::spawn(conn);

        let version_query = "SELECT extversion FROM pg_extension WHERE extname = $1";
        let version: Option<ExtVersion> = db_client
            .query_opt(version_query, &[&ext_name])
            .await
            .with_context(|| format!("Failed to execute query: {}", version_query))?
            .map(|row| row.get(0));

        // sanitize the inputs as postgres idents.
        let ext_name: String = ext_name.pg_quote();
        let quoted_version: String = ext_version.pg_quote();

        if let Some(installed_version) = version {
            if installed_version == ext_version {
                return Ok(installed_version);
            }
            let query = format!("ALTER EXTENSION {ext_name} UPDATE TO {quoted_version}");
            db_client
                .simple_query(&query)
                .await
                .with_context(|| format!("Failed to execute query: {}", query))?;
        } else {
            let query =
                format!("CREATE EXTENSION IF NOT EXISTS {ext_name} WITH VERSION {quoted_version}");
            db_client
                .simple_query(&query)
                .await
                .with_context(|| format!("Failed to execute query: {}", query))?;
        }

        Ok(ext_version)
    }

    pub async fn prepare_preload_libraries(
        &self,
        spec: &ComputeSpec,
    ) -> Result<RemoteExtensionMetrics> {
        if self.params.ext_remote_storage.is_none() {
            return Ok(RemoteExtensionMetrics {
                num_ext_downloaded: 0,
                largest_ext_size: 0,
                total_ext_download_size: 0,
            });
        }
        let remote_extensions = spec
            .remote_extensions
            .as_ref()
            .ok_or(anyhow::anyhow!("Remote extensions are not configured"))?;

        info!("parse shared_preload_libraries from spec.cluster.settings");
        let mut libs_vec = Vec::new();
        if let Some(libs) = spec.cluster.settings.find("shared_preload_libraries") {
            libs_vec = libs
                .split(&[',', '\'', ' '])
                .filter(|s| *s != "neon" && !s.is_empty())
                .map(str::to_string)
                .collect();
        }
        info!("parse shared_preload_libraries from provided postgresql.conf");

        // that is used in neon_local and python tests
        if let Some(conf) = &spec.cluster.postgresql_conf {
            let conf_lines = conf.split('\n').collect::<Vec<&str>>();
            let mut shared_preload_libraries_line = "";
            for line in conf_lines {
                if line.starts_with("shared_preload_libraries") {
                    shared_preload_libraries_line = line;
                }
            }
            let mut preload_libs_vec = Vec::new();
            if let Some(libs) = shared_preload_libraries_line.split("='").nth(1) {
                preload_libs_vec = libs
                    .split(&[',', '\'', ' '])
                    .filter(|s| *s != "neon" && !s.is_empty())
                    .map(str::to_string)
                    .collect();
            }
            libs_vec.extend(preload_libs_vec);
        }

        // Don't try to download libraries that are not in the index.
        // Assume that they are already present locally.
        libs_vec.retain(|lib| remote_extensions.library_index.contains_key(lib));

        info!("Downloading to shared preload libraries: {:?}", &libs_vec);

        let mut download_tasks = Vec::new();
        for library in &libs_vec {
            let (ext_name, ext_path) = remote_extensions.get_ext(
                library,
                true,
                &self.params.build_tag,
                &self.params.pgversion,
            )?;
            download_tasks.push(self.download_extension(ext_name, ext_path));
        }
        let results = join_all(download_tasks).await;

        let mut remote_ext_metrics = RemoteExtensionMetrics {
            num_ext_downloaded: 0,
            largest_ext_size: 0,
            total_ext_download_size: 0,
        };
        for result in results {
            let download_size = match result {
                Ok(res) => {
                    remote_ext_metrics.num_ext_downloaded += 1;
                    res
                }
                Err(err) => {
                    // if we failed to download an extension, we don't want to fail the whole
                    // process, but we do want to log the error
                    error!("Failed to download extension: {}", err);
                    0
                }
            };

            remote_ext_metrics.largest_ext_size =
                std::cmp::max(remote_ext_metrics.largest_ext_size, download_size);
            remote_ext_metrics.total_ext_download_size += download_size;
        }
        Ok(remote_ext_metrics)
    }

    /// Waits until current thread receives a state changed notification and
    /// the pageserver connection strings has changed.
    ///
    /// The operation will time out after a specified duration.
    pub fn wait_timeout_while_pageserver_connstr_unchanged(&self, duration: Duration) {
        let state = self.state.lock().unwrap();
        let old_pageserver_connstr = state
            .pspec
            .as_ref()
            .expect("spec must be set")
            .pageserver_connstr
            .clone();
        let mut unchanged = true;
        let _ = self
            .state_changed
            .wait_timeout_while(state, duration, |s| {
                let pageserver_connstr = &s
                    .pspec
                    .as_ref()
                    .expect("spec must be set")
                    .pageserver_connstr;
                unchanged = pageserver_connstr == &old_pageserver_connstr;
                unchanged
            })
            .unwrap();
        if !unchanged {
            info!("Pageserver config changed");
        }
    }
}

pub fn forward_termination_signal() {
    let ss_pid = SYNC_SAFEKEEPERS_PID.load(Ordering::SeqCst);
    if ss_pid != 0 {
        let ss_pid = nix::unistd::Pid::from_raw(ss_pid as i32);
        kill(ss_pid, Signal::SIGTERM).ok();
    }
    let pg_pid = PG_PID.load(Ordering::SeqCst);
    if pg_pid != 0 {
        let pg_pid = nix::unistd::Pid::from_raw(pg_pid as i32);
        // Use 'fast' shutdown (SIGINT) because it also creates a shutdown checkpoint, which is important for
        // ROs to get a list of running xacts faster instead of going through the CLOG.
        // See https://www.postgresql.org/docs/current/server-shutdown.html for the list of modes and signals.
        kill(pg_pid, Signal::SIGINT).ok();
    }
}

// helper trait to call JoinSet::spawn_blocking(f), but propagates the current
// tracing span to the thread.
trait JoinSetExt<T> {
    fn spawn_blocking_child<F>(&mut self, f: F) -> tokio::task::AbortHandle
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send;
}

impl<T: 'static> JoinSetExt<T> for tokio::task::JoinSet<T> {
    fn spawn_blocking_child<F>(&mut self, f: F) -> tokio::task::AbortHandle
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send,
    {
        let sp = tracing::Span::current();
        self.spawn_blocking(move || {
            let _e = sp.enter();
            f()
        })
    }
}
