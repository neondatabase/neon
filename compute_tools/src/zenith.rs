use std::process::{Command, Stdio};

use anyhow::Result;
use chrono::{DateTime, Utc};
use postgres::{Client, NoTls};
use serde::Deserialize;

use crate::pg_helpers::*;

/// Compute node state shared across several `zenith_ctl` threads.
/// Should be used under `RwLock` to allow HTTP API server to serve
/// status requests, while configuration is in progress.
pub struct ComputeState {
    pub connstr: String,
    pub pgdata: String,
    pub pgbin: String,
    pub spec: ClusterSpec,
    /// Compute setup process has finished
    pub ready: bool,
    /// Timestamp of the last Postgres activity
    pub last_active: DateTime<Utc>,
}

/// Cluster spec or configuration represented as an optional number of
/// delta operations + final cluster state description.
#[derive(Clone, Deserialize)]
pub struct ClusterSpec {
    pub format_version: f32,
    pub timestamp: String,
    pub operation_uuid: Option<String>,
    /// Expected cluster state at the end of transition process.
    pub cluster: Cluster,
    pub delta_operations: Option<Vec<DeltaOp>>,
}

/// Cluster state seen from the perspective of the external tools
/// like Rails web console.
#[derive(Clone, Deserialize)]
pub struct Cluster {
    pub cluster_id: String,
    pub name: String,
    pub state: Option<String>,
    pub roles: Vec<Role>,
    pub databases: Vec<Database>,
    pub settings: GenericOptions,
}

/// Single cluster state changing operation that could not be represented as
/// a static `Cluster` structure. For example:
/// - DROP DATABASE
/// - DROP ROLE
/// - ALTER ROLE name RENAME TO new_name
/// - ALTER DATABASE name RENAME TO new_name
#[derive(Clone, Deserialize)]
pub struct DeltaOp {
    pub action: String,
    pub name: PgIdent,
    pub new_name: Option<PgIdent>,
}

/// Get basebackup from the libpq connection to pageserver using `connstr` and
/// unarchive it to `pgdata` directory overriding all its previous content.
pub fn get_basebackup(
    pgdata: &str,
    connstr: &str,
    tenant: &str,
    timeline: &str,
    lsn: &str,
) -> Result<()> {
    let mut client = Client::connect(connstr, NoTls)?;
    let basebackup_cmd = match lsn {
        "0/0" => format!("basebackup {} {}", tenant, timeline), // First start of the compute
        _ => format!("basebackup {} {} {}", tenant, timeline, lsn),
    };
    let copyreader = client.copy_out(basebackup_cmd.as_str())?;
    let mut ar = tar::Archive::new(copyreader);

    ar.unpack(&pgdata)?;

    Ok(())
}

/// Run `postgres` in a special mode with `--sync-safekeepers` argument
/// and return the reported LSN back to the caller.
pub fn sync_safekeepers(pgdata: &str, pgbin: &str) -> Result<String> {
    let sync_handle = Command::new(&pgbin)
        .args(&["--sync-safekeepers"])
        .env("PGDATA", &pgdata) // we cannot use -D in this mode
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("postgres --sync-safekeepers failed to start");

    let sync_output = sync_handle
        .wait_with_output()
        .expect("postgres --sync-safekeepers failed");
    if !sync_output.status.success() {
        anyhow::bail!(
            "postgres --sync-safekeepers failed: '{}'",
            String::from_utf8_lossy(&sync_output.stderr)
        );
    }

    let lsn = String::from(String::from_utf8(sync_output.stdout)?.trim());

    Ok(lsn)
}
