use std::fs;
use std::io::BufRead;
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
use compute_api::spec::{ComputeMode, ComputeSpec};
use utils::measured_stream::MeasuredReader;

use crate::config;
use crate::pg_helpers::*;
use crate::spec::*;

/// Compute node info shared across several `compute_ctl` threads.
pub struct ComputeNode {
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
    pub start_time: DateTime<Utc>,
    pub status: ComputeStatus,
    /// Timestamp of the last Postgres activity. It could be `None` if
    /// compute wasn't used since start.
    pub last_active: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub pspec: Option<ParsedSpec>,
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
            storage_auth_token,
            tenant_id,
            timeline_id,
        })
    }
}

/// Create special neon_superuser role, that's a slightly nerfed version of a real superuser
/// that we give to customers
fn create_neon_superuser(spec: &ComputeSpec, client: &mut Client) -> Result<()> {
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
                        CREATE ROLE neon_superuser CREATEDB CREATEROLE NOLOGIN IN ROLE pg_read_all_data, pg_write_all_data;
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
    info!("Neon superuser created:\n{}", &query);
    client
        .simple_query(&query)
        .map_err(|e| anyhow::anyhow!(e).context(query))?;
    Ok(())
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
    #[instrument(skip_all, fields(%lsn))]
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
            // HACK We don't use compression on first start (Lsn(0)) because there's no API for it
            Lsn(0) => format!("basebackup {} {}", spec.tenant_id, spec.timeline_id),
            _ => format!(
                "basebackup {} {} {} --gzip",
                spec.tenant_id, spec.timeline_id, lsn
            ),
        };

        let copyreader = client.copy_out(basebackup_cmd.as_str())?;
        let mut measured_reader = MeasuredReader::new(copyreader);

        // Check the magic number to see if it's a gzip or not. Even though
        // we might explicitly ask for gzip, an old pageserver with no implementation
        // of gzip compression might send us uncompressed data. After some time
        // passes we can assume all pageservers know how to compress and we can
        // delete this check.
        //
        // If the data is not gzip, it will be tar. It will not be mistakenly
        // recognized as gzip because tar starts with an ascii encoding of a filename,
        // and 0x1f and 0x8b are unlikely first characters for any filename. Moreover,
        // we send the "global" directory first from the pageserver, so it definitely
        // won't be recognized as gzip.
        let mut bufreader = std::io::BufReader::new(&mut measured_reader);
        let gzip = {
            let peek = bufreader.fill_buf().unwrap();
            peek[0] == 0x1f && peek[1] == 0x8b
        };

        // Read the archive directly from the `CopyOutReader`
        //
        // Set `ignore_zeros` so that unpack() reads all the Copy data and
        // doesn't stop at the end-of-archive marker. Otherwise, if the server
        // sends an Error after finishing the tarball, we will not notice it.
        if gzip {
            let mut ar = tar::Archive::new(flate2::read::GzDecoder::new(&mut bufreader));
            ar.set_ignore_zeros(true);
            ar.unpack(&self.pgdata)?;
        } else {
            let mut ar = tar::Archive::new(&mut bufreader);
            ar.set_ignore_zeros(true);
            ar.unpack(&self.pgdata)?;
        };

        // Report metrics
        self.state.lock().unwrap().metrics.basebackup_bytes =
            measured_reader.get_byte_count() as u64;
        self.state.lock().unwrap().metrics.basebackup_ms = Utc::now()
            .signed_duration_since(start_time)
            .to_std()
            .unwrap()
            .as_millis() as u64;
        Ok(())
    }

    // Run `postgres` in a special mode with `--sync-safekeepers` argument
    // and return the reported LSN back to the caller.
    #[instrument(skip_all)]
    pub fn sync_safekeepers(&self, storage_auth_token: Option<String>) -> Result<Lsn> {
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
    #[instrument(skip_all)]
    pub fn prepare_pgdata(&self, compute_state: &ComputeState) -> Result<()> {
        let pspec = compute_state.pspec.as_ref().expect("spec must be set");
        let spec = &pspec.spec;
        let pgdata_path = Path::new(&self.pgdata);

        // Remove/create an empty pgdata directory and put configuration there.
        self.create_pgdata()?;
        config::write_postgres_conf(&pgdata_path.join("postgresql.conf"), &pspec.spec)?;

        // Syncing safekeepers is only safe with primary nodes: if a primary
        // is already connected it will be kicked out, so a secondary (standby)
        // cannot sync safekeepers.
        let lsn = match spec.mode {
            ComputeMode::Primary => {
                info!("starting safekeepers syncing");
                let lsn = self
                    .sync_safekeepers(pspec.storage_auth_token.clone())
                    .with_context(|| "failed to sync safekeepers")?;
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

        match spec.mode {
            ComputeMode::Primary => {}
            ComputeMode::Replica | ComputeMode::Static(..) => {
                add_standby_signal(pgdata_path)?;
            }
        }

        Ok(())
    }

    /// Start Postgres as a child process and manage DBs/roles.
    /// After that this will hang waiting on the postmaster process to exit.
    #[instrument(skip_all)]
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
    #[instrument(skip_all)]
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
                // Disable forwarding so that users don't get a cloud_admin role
                client.simple_query("SET neon.forward_ddl = false")?;
                client.simple_query("CREATE USER cloud_admin WITH SUPERUSER")?;
                client.simple_query("GRANT zenith_admin TO cloud_admin")?;
                drop(client);

                // reconnect with connsting with expected name
                Client::connect(self.connstr.as_str(), NoTls)?
            }
            Ok(client) => client,
        };

        // Disable DDL forwarding because control plane already knows about these roles/databases.
        client.simple_query("SET neon.forward_ddl = false")?;

        // Proceed with post-startup configuration. Note, that order of operations is important.
        let spec = &compute_state.pspec.as_ref().expect("spec must be set").spec;
        create_neon_superuser(spec, &mut client)?;
        handle_roles(spec, &mut client)?;
        handle_databases(spec, &mut client)?;
        handle_role_deletions(spec, self.connstr.as_str(), &mut client)?;
        handle_grants(spec, self.connstr.as_str())?;
        handle_extensions(spec, &mut client)?;

        // 'Close' connection
        drop(client);

        Ok(())
    }

    // We could've wrapped this around `pg_ctl reload`, but right now we don't use
    // `pg_ctl` for start / stop, so this just seems much easier to do as we already
    // have opened connection to Postgres and superuser access.
    #[instrument(skip_all)]
    fn pg_reload_conf(&self, client: &mut Client) -> Result<()> {
        client.simple_query("SELECT pg_reload_conf()")?;
        Ok(())
    }

    /// Similar to `apply_config()`, but does a bit different sequence of operations,
    /// as it's used to reconfigure a previously started and configured Postgres node.
    #[instrument(skip_all)]
    pub fn reconfigure(&self) -> Result<()> {
        let spec = self.state.lock().unwrap().pspec.clone().unwrap().spec;

        // Write new config
        let pgdata_path = Path::new(&self.pgdata);
        config::write_postgres_conf(&pgdata_path.join("postgresql.conf"), &spec)?;

        let mut client = Client::connect(self.connstr.as_str(), NoTls)?;
        self.pg_reload_conf(&mut client)?;

        // Proceed with post-startup configuration. Note, that order of operations is important.
        // Disable DDL forwarding because control plane already knows about these roles/databases.
        if spec.mode == ComputeMode::Primary {
            client.simple_query("SET neon.forward_ddl = false")?;
            handle_roles(&spec, &mut client)?;
            handle_databases(&spec, &mut client)?;
            handle_role_deletions(&spec, self.connstr.as_str(), &mut client)?;
            handle_grants(&spec, self.connstr.as_str())?;
            handle_extensions(&spec, &mut client)?;
        }

        // 'Close' connection
        drop(client);

        let unknown_op = "unknown".to_string();
        let op_id = spec.operation_uuid.as_ref().unwrap_or(&unknown_op);
        info!(
            "finished reconfiguration of compute node for operation {}",
            op_id
        );

        Ok(())
    }

    #[instrument(skip_all)]
    pub fn start_compute(&self) -> Result<std::process::Child> {
        let compute_state = self.state.lock().unwrap().clone();
        let pspec = compute_state.pspec.as_ref().expect("spec must be set");
        info!(
            "starting compute for project {}, operation {}, tenant {}, timeline {}",
            pspec.spec.cluster.cluster_id.as_deref().unwrap_or("None"),
            pspec.spec.operation_uuid.as_deref().unwrap_or("None"),
            pspec.tenant_id,
            pspec.timeline_id,
        );

        self.prepare_pgdata(&compute_state)?;

        let start_time = Utc::now();
        let pg = self.start_postgres(pspec.storage_auth_token.clone())?;

        let config_time = Utc::now();
        if pspec.spec.mode == ComputeMode::Primary && !pspec.spec.skip_pg_catalog_updates {
            self.apply_config(&compute_state)?;
        }

        let startup_end_time = Utc::now();
        {
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
        }
        self.set_status(ComputeStatus::Running);

        info!(
            "finished configuration of compute for project {}",
            pspec.spec.cluster.cluster_id.as_deref().unwrap_or("None")
        );

        // Log metrics so that we can search for slow operations in logs
        let metrics = {
            let state = self.state.lock().unwrap();
            state.metrics.clone()
        };
        info!(?metrics, "compute start finished");

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
