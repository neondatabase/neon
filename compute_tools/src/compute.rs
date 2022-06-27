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
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use log::info;
use postgres::{Client, NoTls};
use serde::{Serialize, Serializer};

use crate::checker::create_writablity_check_data;
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
    pub spec: ComputeSpec,
    pub tenant: String,
    pub timeline: String,
    pub pageserver_connstr: String,
    pub metrics: ComputeMetrics,
    /// Volatile part of the `ComputeNode` so should be used under `RwLock`
    /// to allow HTTP API server to serve status requests, while configuration
    /// is in progress.
    pub state: RwLock<ComputeState>,
}

fn rfc3339_serialize<S>(x: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    x.to_rfc3339().serialize(s)
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ComputeState {
    pub status: ComputeStatus,
    /// Timestamp of the last Postgres activity
    #[serde(serialize_with = "rfc3339_serialize")]
    pub last_active: DateTime<Utc>,
    pub error: Option<String>,
}

impl ComputeState {
    pub fn new() -> Self {
        Self {
            status: ComputeStatus::Init,
            last_active: Utc::now(),
            error: None,
        }
    }
}

impl Default for ComputeState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ComputeStatus {
    Init,
    Running,
    Failed,
}

#[derive(Serialize)]
pub struct ComputeMetrics {
    pub sync_safekeepers_ms: AtomicU64,
    pub basebackup_ms: AtomicU64,
    pub config_ms: AtomicU64,
    pub total_startup_ms: AtomicU64,
}

impl ComputeMetrics {
    pub fn new() -> Self {
        Self {
            sync_safekeepers_ms: AtomicU64::new(0),
            basebackup_ms: AtomicU64::new(0),
            config_ms: AtomicU64::new(0),
            total_startup_ms: AtomicU64::new(0),
        }
    }
}

impl Default for ComputeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ComputeNode {
    pub fn set_status(&self, status: ComputeStatus) {
        self.state.write().unwrap().status = status;
    }

    pub fn get_status(&self) -> ComputeStatus {
        self.state.read().unwrap().status
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
    fn get_basebackup(&self, lsn: &str) -> Result<()> {
        let start_time = Utc::now();

        let mut client = Client::connect(&self.pageserver_connstr, NoTls)?;
        let basebackup_cmd = match lsn {
            "0/0" => format!("basebackup {} {}", &self.tenant, &self.timeline), // First start of the compute
            _ => format!("basebackup {} {} {}", &self.tenant, &self.timeline, lsn),
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

        self.metrics.basebackup_ms.store(
            Utc::now()
                .signed_duration_since(start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64,
            Ordering::Relaxed,
        );

        Ok(())
    }

    // Run `postgres` in a special mode with `--sync-safekeepers` argument
    // and return the reported LSN back to the caller.
    fn sync_safekeepers(&self) -> Result<String> {
        let start_time = Utc::now();

        let sync_handle = Command::new(&self.pgbin)
            .args(&["--sync-safekeepers"])
            .env("PGDATA", &self.pgdata) // we cannot use -D in this mode
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
                "postgres --sync-safekeepers exited with non-zero status: {}",
                sync_output.status,
            );
        }

        self.metrics.sync_safekeepers_ms.store(
            Utc::now()
                .signed_duration_since(start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64,
            Ordering::Relaxed,
        );

        let lsn = String::from(String::from_utf8(sync_output.stdout)?.trim());

        Ok(lsn)
    }

    /// Do all the preparations like PGDATA directory creation, configuration,
    /// safekeepers sync, basebackup, etc.
    pub fn prepare_pgdata(&self) -> Result<()> {
        let spec = &self.spec;
        let pgdata_path = Path::new(&self.pgdata);

        // Remove/create an empty pgdata directory and put configuration there.
        self.create_pgdata()?;
        config::write_postgres_conf(&pgdata_path.join("postgresql.conf"), spec)?;

        info!("starting safekeepers syncing");
        let lsn = self
            .sync_safekeepers()
            .with_context(|| "failed to sync safekeepers")?;
        info!("safekeepers synced at LSN {}", lsn);

        info!(
            "getting basebackup@{} from pageserver {}",
            lsn, &self.pageserver_connstr
        );
        self.get_basebackup(&lsn).with_context(|| {
            format!(
                "failed to get basebackup@{} from pageserver {}",
                lsn, &self.pageserver_connstr
            )
        })?;

        // Update pg_hba.conf received with basebackup.
        update_pg_hba(pgdata_path)?;

        Ok(())
    }

    /// Start Postgres as a child process and manage DBs/roles.
    /// After that this will hang waiting on the postmaster process to exit.
    pub fn run(&self) -> Result<ExitStatus> {
        let start_time = Utc::now();

        let pgdata_path = Path::new(&self.pgdata);

        // Run postgres as a child process.
        let mut pg = Command::new(&self.pgbin)
            .args(&["-D", &self.pgdata])
            .spawn()
            .expect("cannot start postgres process");

        // Try default Postgres port if it is not provided
        let port = self
            .spec
            .cluster
            .settings
            .find("port")
            .unwrap_or_else(|| "5432".to_string());
        wait_for_postgres(&mut pg, &port, pgdata_path)?;

        // If connection fails,
        // it may be the old node with `zenith_admin` superuser.
        //
        // In this case we need to connect with old `zenith_admin`name
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

                let mut client = Client::connect(self.connstr.as_str(), NoTls)?;
                client.simple_query("CREATE USER cloud_admin WITH SUPERUSER")?;
                client.simple_query("GRANT zenith_admin TO cloud_admin")?;
                drop(client);

                // reconnect with connsting with expected name
                Client::connect(self.connstr.as_str(), NoTls)?
            }
            Ok(client) => client,
        };

        handle_roles(&self.spec, &mut client)?;
        handle_databases(&self.spec, &mut client)?;
        handle_role_deletions(self, &mut client)?;
        handle_grants(&self.spec, &mut client)?;
        create_writablity_check_data(&mut client)?;

        // 'Close' connection
        drop(client);
        let startup_end_time = Utc::now();

        self.metrics.config_ms.store(
            startup_end_time
                .signed_duration_since(start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64,
            Ordering::Relaxed,
        );
        self.metrics.total_startup_ms.store(
            startup_end_time
                .signed_duration_since(self.start_time)
                .to_std()
                .unwrap()
                .as_millis() as u64,
            Ordering::Relaxed,
        );

        self.set_status(ComputeStatus::Running);

        info!(
            "finished configuration of compute for project {}",
            self.spec.cluster.cluster_id
        );

        // Wait for child Postgres process basically forever. In this state Ctrl+C
        // will propagate to Postgres and it will be shut down as well.
        let ecode = pg
            .wait()
            .expect("failed to start waiting on Postgres process");

        Ok(ecode)
    }

    pub fn prepare_and_run(&self) -> Result<ExitStatus> {
        info!(
            "starting compute for project {}, operation {}, tenant {}, timeline {}",
            self.spec.cluster.cluster_id,
            self.spec.operation_uuid.as_ref().unwrap(),
            self.tenant,
            self.timeline,
        );

        self.prepare_pgdata()?;
        self.run()
    }
}
