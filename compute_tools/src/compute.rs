//
// XXX: This starts to be scarry similar to the `PostgresNode` from `control_plane`,
// but there are several things that makes `PostgresNode` usage inconvenient in the
// cloud:
// - it inherits from `LocalEnv`, which contains **all-all** the information about
//   a complete service running
// - it uses `PageServerNode` with information about http endpoint, which we do not
//   need in the cloud again
// - many tiny pieces like, for example, we do not use `pg_ctl` in the cloud
//
// Thus, to use `PostgresNode` in the cloud, we need to 'mock' a bunch of required
// attributes (not required for the cloud). Yet, it is still tempting to unify these
// `PostgresNode` and `ComputeNode` and use one in both places.
//
// TODO: stabilize `ComputeNode` and think about using it in the `control_plane`.
//
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::sync::{Condvar, Mutex};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use postgres::{Client, NoTls};
use tokio_postgres;
use tracing::{info, instrument, warn};
use utils::id::{TenantId, TimelineId};
use utils::lsn::Lsn;

use compute_api::responses::{ComputeMetrics, ComputeStatus};
use compute_api::spec::ComputeSpec;

use crate::checker::create_writability_check_data;
use crate::config;
use crate::pg_helpers::*;
use crate::spec::*;

/// Compute node info shared across several `compute_ctl` threads.
pub struct ComputeNode {
    pub start_time: DateTime<Utc>,
    // Url type maintains proper escaping
    pub connstr: url::Url,
    pub pgdata: String,
    pub pgbin: String,
    /// We should only allow live re- / configuration of the compute node if
    /// it uses 'pull model', i.e. it can go to control-plane and fetch
    /// the latest configuration. Otherwise, there could be a case:
    /// - we start compute with some spec provided as argument
    /// - we push new spec and it does reconfiguration
    /// - but then something happens and compute pod / VM is destroyed,
    ///   so k8s controller starts it again with the **old** spec
    /// and the same for empty computes:
    /// - we started compute without any spec
    /// - we push spec and it does configuration
    /// - but then it is restarted without any spec again
    pub live_config_allowed: bool,
    /// Volatile part of the `ComputeNode`, which should be used under `Mutex`.
    /// To allow HTTP API server to serving status requests, while configuration
    /// is in progress, lock should be held only for short periods of time to do
    /// read/write, not the whole configuration process.
    pub state: Mutex<ComputeState>,
    /// `Condvar` to allow notifying waiters about state changes.
    pub state_changed: Condvar,
}

#[derive(Clone, Debug)]
pub struct ComputeState {
    pub status: ComputeStatus,
    /// Timestamp of the last Postgres activity
    pub last_active: DateTime<Utc>,
    pub error: Option<String>,
    pub pspec: Option<ParsedSpec>,
    pub metrics: ComputeMetrics,
}

impl ComputeState {
    pub fn new() -> Self {
        Self {
            status: ComputeStatus::Empty,
            last_active: Utc::now(),
            error: None,
            pspec: None,
            metrics: ComputeMetrics::default(),
        }
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
    pub storage_auth_token: Option<String>,
}

impl TryFrom<ComputeSpec> for ParsedSpec {
    type Error = String;
    fn try_from(spec: ComputeSpec) -> Result<Self, String> {
        let pageserver_connstr = spec
            .cluster
            .settings
            .find("neon.pageserver_connstring")
            .ok_or("pageserver connstr should be provided")?;
        let storage_auth_token = spec.storage_auth_token.clone();
        let tenant_id: TenantId = spec
            .cluster
            .settings
            .find("neon.tenant_id")
            .ok_or("tenant id should be provided")
            .map(|s| TenantId::from_str(&s))?
            .or(Err("invalid tenant id"))?;
        let timeline_id: TimelineId = spec
            .cluster
            .settings
            .find("neon.timeline_id")
            .ok_or("timeline id should be provided")
            .map(|s| TimelineId::from_str(&s))?
            .or(Err("invalid timeline id"))?;

        Ok(ParsedSpec {
            spec,
            pageserver_connstr,
            storage_auth_token,
            tenant_id,
            timeline_id,
        })
    }
}

impl ComputeNode {
    pub fn set_status(&self, status: ComputeStatus) {
        let mut state = self.state.lock().unwrap();
        state.status = status;
        self.state_changed.notify_all();
    }

    pub fn get_status(&self) -> ComputeStatus {
        self.state.lock().unwrap().status
    }

    // Remove `pgdata` directory and create it again with right permissions.
    fn create_pgdata(&self) -> Result<()> {
        // Ignore removal error, likely it is a 'No such file or directory (os error 2)'.
        // If it is something different then create_dir() will error out anyway.
        let _ok = fs::remove_dir_all(&self.pgdata);
        fs::create_dir(&self.pgdata)?;
        fs::set_permissions(&self.pgdata, fs::Permissions::from_mode(0o700))?;

        Ok(())
    }

    // Get basebackup from the libpq connection to pageserver using `connstr` and
    // unarchive it to `pgdata` directory overriding all its previous content.
    #[instrument(skip(self, compute_state))]
    fn get_basebackup(&self, compute_state: &ComputeState, lsn: Lsn) -> Result<()> {
        let spec = compute_state.pspec.as_ref().expect("spec must be set");
        let start_time = Utc::now();

        let mut config = postgres::Config::from_str(&spec.pageserver_connstr)?;

        // Use the storage auth token from the config file, if given.
        // Note: this overrides any password set in the connection string.
        if let Some(storage_auth_token) = &spec.storage_auth_token {
            info!("Got storage auth token from spec file");
            config.password(storage_auth_token);
        } else {
            info!("Storage auth token not set");
        }

        let mut client = config.connect(NoTls)?;
        let basebackup_cmd = match lsn {
            Lsn(0) => format!("basebackup {} {}", spec.tenant_id, spec.timeline_id), // First start of the compute
            _ => format!("basebackup {} {} {}", spec.tenant_id, spec.timeline_id, lsn),
        };
        let copyreader = client.copy_out(basebackup_cmd.as_str())?;

        // Read the archive directly from the `CopyOutReader`
        //
        // Set `ignore_zeros` so that unpack() reads all the Copy data and
        // doesn't stop at the end-of-archive marker. Otherwise, if the server
        // sends an Error after finishing the tarball, we will not notice it.
        let mut ar = tar::Archive::new(copyreader);
        ar.set_ignore_zeros(true);
        ar.unpack(&self.pgdata)?;

        self.state.lock().unwrap().metrics.basebackup_ms = Utc::now()
            .signed_duration_since(start_time)
            .to_std()
            .unwrap()
            .as_millis() as u64;
        Ok(())
    }

    // Run `postgres` in a special mode with `--sync-safekeepers` argument
    // and return the reported LSN back to the caller.
    #[instrument(skip(self, storage_auth_token))]
    fn sync_safekeepers(&self, storage_auth_token: Option<String>) -> Result<Lsn> {
        let start_time = Utc::now();

        let sync_handle = Command::new(&self.pgbin)
            .args(["--sync-safekeepers"])
            .env("PGDATA", &self.pgdata) // we cannot use -D in this mode
            .envs(if let Some(storage_auth_token) = &storage_auth_token {
                vec![("NEON_AUTH_TOKEN", storage_auth_token)]
            } else {
                vec![]
            })
            .stdout(Stdio::piped())
            .spawn()
            .expect("postgres --sync-safekeepers failed to start");

        // `postgres --sync-safekeepers` will print all log output to stderr and
        // final LSN to stdout. So we pipe only stdout, while stderr will be automatically
        // redirected to the caller output.
        let sync_output = sync_handle
            .wait_with_output()
            .expect("postgres --sync-safekeepers failed");

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
    #[instrument(skip(self, compute_state))]
    pub fn prepare_pgdata(&self, compute_state: &ComputeState) -> Result<()> {
        #[derive(Clone)]
        enum Replication {
            Primary,
            Static { lsn: String },
            HotStandby,
        }

        let pspec = compute_state.pspec.as_ref().expect("spec must be set");
        let pgdata_path = Path::new(&self.pgdata);

        let hot_replica = if let Some(option) = spec.cluster.settings.find_ref("hot_standby") {
            if let Some(value) = &option.value {
                matches!(value.as_str(), "on" | "yes" | "true")
            } else {
                false
            }
        } else {
            false
        };

        let replication = if hot_replica {
            Replication::HotStandby
        } else if let Some(lsn) = spec.cluster.settings.find("recovery_target_lsn") {
                Replication::Static { lsn }
        } else {
            Replication::Primary
        };

        // Remove/create an empty pgdata directory and put configuration there.
        self.create_pgdata()?;
        config::write_postgres_conf(&pgdata_path.join("postgresql.conf"), &pspec.spec)?;

        // Syncing safekeepers is only safe with primary nodes: if a primary
        // is already connected it will be kicked out, so a secondary (standby)
        // cannot sync safekeepers.
        let lsn = match &replication {
            Replication::Primary => {
                info!("starting safekeepers syncing");
                let lsn = self
                    .sync_safekeepers(pspec.storage_auth_token.clone())
                    .with_context(|| "failed to sync safekeepers")?;
                info!("safekeepers synced at LSN {}", lsn);
                lsn
            }
            Replication::Static { lsn } => {
                info!("Starting read-only node at static LSN {}", lsn);
                lsn.clone()
            }
            Replication::HotStandby => {
                info!("Initializing standby from latest Pageserver LSN");
                "0/0".to_string()
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

        match &replication {
            Replication::Primary | Replication::Static { .. } => {}
            Replication::HotStandby => {
                add_standby_signal(pgdata_path)?;
            }
        }

        Ok(())
    }

    /// Start Postgres as a child process and manage DBs/roles.
    /// After that this will hang waiting on the postmaster process to exit.
    #[instrument(skip(self))]
    pub fn start_postgres(
        &self,
        storage_auth_token: Option<String>,
    ) -> Result<std::process::Child> {
        let pgdata_path = Path::new(&self.pgdata);

        // Run postgres as a child process.
        let mut pg = Command::new(&self.pgbin)
            .args(["-D", &self.pgdata])
            .envs(if let Some(storage_auth_token) = &storage_auth_token {
                vec![("NEON_AUTH_TOKEN", storage_auth_token)]
            } else {
                vec![]
            })
            .spawn()
            .expect("cannot start postgres process");

        wait_for_postgres(&mut pg, pgdata_path)?;

        Ok(pg)
    }

    /// Do initial configuration of the already started Postgres.
    #[instrument(skip(self, compute_state))]
    pub fn apply_config(&self, compute_state: &ComputeState) -> Result<()> {
        // If connection fails,
        // it may be the old node with `zenith_admin` superuser.
        //
        // In this case we need to connect with old `zenith_admin` name
        // and create new user. We cannot simply rename connected user,
        // but we can create a new one and grant it all privileges.
        let mut client = match Client::connect(self.connstr.as_str(), NoTls) {
            Err(e) => {
                info!(
                    "cannot connect to postgres: {}, retrying with `zenith_admin` username",
                    e
                );
                let mut zenith_admin_connstr = self.connstr.clone();

                zenith_admin_connstr
                    .set_username("zenith_admin")
                    .map_err(|_| anyhow::anyhow!("invalid connstr"))?;

                let mut client = Client::connect(zenith_admin_connstr.as_str(), NoTls)?;
                client.simple_query("CREATE USER cloud_admin WITH SUPERUSER")?;
                client.simple_query("GRANT zenith_admin TO cloud_admin")?;
                drop(client);

                // reconnect with connsting with expected name
                Client::connect(self.connstr.as_str(), NoTls)?
            }
            Ok(client) => client,
        };

        // Proceed with post-startup configuration. Note, that order of operations is important.
        let spec = &compute_state.pspec.as_ref().expect("spec must be set").spec;
        handle_roles(spec, &mut client)?;
        handle_databases(spec, &mut client)?;
        handle_role_deletions(spec, self.connstr.as_str(), &mut client)?;
        handle_grants(spec, self.connstr.as_str(), &mut client)?;
        create_writability_check_data(&mut client)?;
        handle_extensions(spec, &mut client)?;

        // 'Close' connection
        drop(client);

        info!(
            "finished configuration of compute for project {}",
            spec.cluster.cluster_id
        );

        Ok(())
    }

    #[instrument(skip(self))]
    pub fn start_compute(&self) -> Result<std::process::Child> {
        let compute_state = self.state.lock().unwrap().clone();
        let spec = compute_state.pspec.as_ref().expect("spec must be set");
        info!(
            "starting compute for project {}, operation {}, tenant {}, timeline {}",
            spec.spec.cluster.cluster_id,
            spec.spec.operation_uuid.as_deref().unwrap_or("None"),
            spec.tenant_id,
            spec.timeline_id,
        );

        self.prepare_pgdata(&compute_state)?;

        let start_time = Utc::now();

        let pg = self.start_postgres(spec.storage_auth_token.clone())?;

        self.apply_config(&compute_state)?;

        let startup_end_time = Utc::now();
        {
            let mut state = self.state.lock().unwrap();
            state.metrics.config_ms = startup_end_time
                .signed_duration_since(start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64;
            state.metrics.total_startup_ms = startup_end_time
                .signed_duration_since(self.start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64;
        }
        self.set_status(ComputeStatus::Running);

        Ok(pg)
    }

    // Look for core dumps and collect backtraces.
    //
    // EKS worker nodes have following core dump settings:
    //   /proc/sys/kernel/core_pattern -> core
    //   /proc/sys/kernel/core_uses_pid -> 1
    //   ulimint -c -> unlimited
    // which results in core dumps being written to postgres data directory as core.<pid>.
    //
    // Use that as a default location and pattern, except macos where core dumps are written
    // to /cores/ directory by default.
    pub fn check_for_core_dumps(&self) -> Result<()> {
        let core_dump_dir = match std::env::consts::OS {
            "macos" => Path::new("/cores/"),
            _ => Path::new(&self.pgdata),
        };

        // Collect core dump paths if any
        info!("checking for core dumps in {}", core_dump_dir.display());
        let files = fs::read_dir(core_dump_dir)?;
        let cores = files.filter_map(|entry| {
            let entry = entry.ok()?;
            let _ = entry.file_name().to_str()?.strip_prefix("core.")?;
            Some(entry.path())
        });

        // Print backtrace for each core dump
        for core_path in cores {
            warn!(
                "core dump found: {}, collecting backtrace",
                core_path.display()
            );

            // Try first with gdb
            let backtrace = Command::new("gdb")
                .args(["--batch", "-q", "-ex", "bt", &self.pgbin])
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
        let connect_result = tokio_postgres::connect(self.connstr.as_str(), NoTls).await;
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
}
