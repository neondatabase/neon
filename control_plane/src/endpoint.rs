use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use crate::local_env::{LocalEnv, DEFAULT_PG_VERSION};
use crate::pageserver::PageServerNode;
use crate::postgresql_conf::PostgresConf;

//
// ComputeControlPlane
//
pub struct ComputeControlPlane {
    base_port: u16,

    // endpoint ID is the key
    pub endpoints: BTreeMap<String, Arc<Endpoint>>,

    env: LocalEnv,
    pageserver: Arc<PageServerNode>,
}

impl ComputeControlPlane {
    // Load current endpoints from the endpoints/ subdirectories
    pub fn load(env: LocalEnv) -> Result<ComputeControlPlane> {
        let pageserver = Arc::new(PageServerNode::from_env(&env));

        let mut endpoints = BTreeMap::default();
        for endpoint_dir in fs::read_dir(env.endpoints_path())
            .with_context(|| format!("failed to list {}", env.endpoints_path().display()))?
        {
            let ep = Endpoint::from_dir_entry(endpoint_dir?, &env, &pageserver)?;
            endpoints.insert(ep.name.clone(), Arc::new(ep));
        }

        Ok(ComputeControlPlane {
            base_port: 55431,
            endpoints,
            env,
            pageserver,
        })
    }

    fn get_port(&mut self) -> u16 {
        1 + self
            .endpoints
            .values()
            .map(|ep| ep.address.port())
            .max()
            .unwrap_or(self.base_port)
    }

    pub fn new_endpoint(
        &mut self,
        tenant_id: TenantId,
        name: &str,
        timeline_id: TimelineId,
        port: Option<u16>,
        pg_version: u32,
        replication: Replication,
    ) -> Result<Arc<Endpoint>> {
        let port = port.unwrap_or_else(|| self.get_port());

        let ep = Arc::new(Endpoint {
            name: name.to_owned(),
            address: SocketAddr::new("127.0.0.1".parse().unwrap(), port),
            env: self.env.clone(),
            pageserver: Arc::clone(&self.pageserver),
            timeline_id,
            replication,
            tenant_id,
            pg_version,
        });

        ep.create_pgdata()?;
        ep.setup_pg_conf()?;

        self.endpoints.insert(ep.name.clone(), Arc::clone(&ep));

        Ok(ep)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Replication {
    // Regular read-write node
    Primary,
    // if recovery_target_lsn is provided
    Static(Lsn),
    // Hot standby running on a timleine
    Replica,
}

#[derive(Debug)]
pub struct Endpoint {
    /// used as the directory name
    name: String,
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    // Some(lsn) if this is a read-only endpoint anchored at 'lsn'. None for the primary.
    pub replication: Replication,

    // port and address of the Postgres server
    pub address: SocketAddr,
    pg_version: u32,

    // These are not part of the endpoint as such, but the environment
    // the endpoint runs in.
    pub env: LocalEnv,
    pageserver: Arc<PageServerNode>,
}

impl Endpoint {
    fn from_dir_entry(
        entry: std::fs::DirEntry,
        env: &LocalEnv,
        pageserver: &Arc<PageServerNode>,
    ) -> Result<Endpoint> {
        if !entry.file_type()?.is_dir() {
            anyhow::bail!(
                "Endpoint::from_dir_entry failed: '{}' is not a directory",
                entry.path().display()
            );
        }

        // parse data directory name
        let fname = entry.file_name();
        let name = fname.to_str().unwrap().to_string();

        // Read config file into memory
        let cfg_path = entry.path().join("pgdata").join("postgresql.conf");
        let cfg_path_str = cfg_path.to_string_lossy();
        let mut conf_file = File::open(&cfg_path)
            .with_context(|| format!("failed to open config file in {}", cfg_path_str))?;
        let conf = PostgresConf::read(&mut conf_file)
            .with_context(|| format!("failed to read config file in {}", cfg_path_str))?;

        // Read a few options from the config file
        let context = format!("in config file {}", cfg_path_str);
        let port: u16 = conf.parse_field("port", &context)?;
        let timeline_id: TimelineId = conf.parse_field("neon.timeline_id", &context)?;
        let tenant_id: TenantId = conf.parse_field("neon.tenant_id", &context)?;

        // Read postgres version from PG_VERSION file to determine which postgres version binary to use.
        // If it doesn't exist, assume broken data directory and use default pg version.
        let pg_version_path = entry.path().join("PG_VERSION");

        let pg_version_str =
            fs::read_to_string(pg_version_path).unwrap_or_else(|_| DEFAULT_PG_VERSION.to_string());
        let pg_version = u32::from_str(&pg_version_str)?;

        // parse recovery_target_lsn and primary_conninfo into Recovery Target, if any
        let replication = if let Some(lsn_str) = conf.get("recovery_target_lsn") {
            Replication::Static(Lsn::from_str(lsn_str)?)
        } else if let Some(slot_name) = conf.get("primary_slot_name") {
            let slot_name = slot_name.to_string();
            let prefix = format!("repl_{}_{}_", timeline_id, name);
            assert!(slot_name.starts_with(&prefix));
            Replication::Replica
        } else {
            Replication::Primary
        };

        // ok now
        Ok(Endpoint {
            address: SocketAddr::new("127.0.0.1".parse().unwrap(), port),
            name,
            env: env.clone(),
            pageserver: Arc::clone(pageserver),
            timeline_id,
            replication,
            tenant_id,
            pg_version,
        })
    }

    fn sync_safekeepers(&self, auth_token: &Option<String>, pg_version: u32) -> Result<Lsn> {
        let pg_path = self.env.pg_bin_dir(pg_version)?.join("postgres");
        let mut cmd = Command::new(pg_path);

        cmd.arg("--sync-safekeepers")
            .env_clear()
            .env(
                "LD_LIBRARY_PATH",
                self.env.pg_lib_dir(pg_version)?.to_str().unwrap(),
            )
            .env(
                "DYLD_LIBRARY_PATH",
                self.env.pg_lib_dir(pg_version)?.to_str().unwrap(),
            )
            .env("PGDATA", self.pgdata().to_str().unwrap())
            .stdout(Stdio::piped())
            // Comment this to avoid capturing stderr (useful if command hangs)
            .stderr(Stdio::piped());

        if let Some(token) = auth_token {
            cmd.env("NEON_AUTH_TOKEN", token);
        }

        let sync_handle = cmd
            .spawn()
            .expect("postgres --sync-safekeepers failed to start");

        let sync_output = sync_handle
            .wait_with_output()
            .expect("postgres --sync-safekeepers failed");
        if !sync_output.status.success() {
            anyhow::bail!(
                "sync-safekeepers failed: '{}'",
                String::from_utf8_lossy(&sync_output.stderr)
            );
        }

        let lsn = Lsn::from_str(std::str::from_utf8(&sync_output.stdout)?.trim())?;
        println!("Safekeepers synced on {}", lsn);
        Ok(lsn)
    }

    /// Get basebackup from the pageserver as a tar archive and extract it
    /// to the `self.pgdata()` directory.
    fn do_basebackup(&self, lsn: Option<Lsn>) -> Result<()> {
        println!(
            "Extracting base backup to create postgres instance: path={} port={}",
            self.pgdata().display(),
            self.address.port()
        );

        let sql = if let Some(lsn) = lsn {
            format!("basebackup {} {} {}", self.tenant_id, self.timeline_id, lsn)
        } else {
            format!("basebackup {} {}", self.tenant_id, self.timeline_id)
        };

        let mut client = self
            .pageserver
            .page_server_psql_client()
            .context("connecting to page server failed")?;

        let copyreader = client
            .copy_out(sql.as_str())
            .context("page server 'basebackup' command failed")?;

        // Read the archive directly from the `CopyOutReader`
        //
        // Set `ignore_zeros` so that unpack() reads all the Copy data and
        // doesn't stop at the end-of-archive marker. Otherwise, if the server
        // sends an Error after finishing the tarball, we will not notice it.
        let mut ar = tar::Archive::new(copyreader);
        ar.set_ignore_zeros(true);
        ar.unpack(&self.pgdata())
            .context("extracting base backup failed")?;

        Ok(())
    }

    fn create_pgdata(&self) -> Result<()> {
        fs::create_dir_all(self.pgdata()).with_context(|| {
            format!(
                "could not create data directory {}",
                self.pgdata().display()
            )
        })?;
        fs::set_permissions(self.pgdata().as_path(), fs::Permissions::from_mode(0o700))
            .with_context(|| {
                format!(
                    "could not set permissions in data directory {}",
                    self.pgdata().display()
                )
            })
    }

    // Write postgresql.conf with default configuration
    // and PG_VERSION file to the data directory of a new endpoint.
    fn setup_pg_conf(&self) -> Result<()> {
        let mut conf = PostgresConf::new();
        conf.append("max_wal_senders", "10");
        conf.append("wal_log_hints", "off");
        conf.append("max_replication_slots", "10");
        conf.append("hot_standby", "on");
        conf.append("shared_buffers", "1MB");
        conf.append("fsync", "off");
        conf.append("max_connections", "100");
        conf.append("wal_level", "replica");
        // wal_sender_timeout is the maximum time to wait for WAL replication.
        // It also defines how often the walreciever will send a feedback message to the wal sender.
        conf.append("wal_sender_timeout", "5s");
        conf.append("listen_addresses", &self.address.ip().to_string());
        conf.append("port", &self.address.port().to_string());
        conf.append("wal_keep_size", "0");
        // walproposer panics when basebackup is invalid, it is pointless to restart in this case.
        conf.append("restart_after_crash", "off");

        // Configure the Neon Postgres extension to fetch pages from pageserver
        let pageserver_connstr = {
            let config = &self.pageserver.pg_connection_config;
            let (host, port) = (config.host(), config.port());

            // NOTE: avoid spaces in connection string, because it is less error prone if we forward it somewhere.
            format!("postgresql://no_user@{host}:{port}")
        };
        conf.append("shared_preload_libraries", "neon");
        conf.append_line("");
        conf.append("neon.pageserver_connstring", &pageserver_connstr);
        conf.append("neon.tenant_id", &self.tenant_id.to_string());
        conf.append("neon.timeline_id", &self.timeline_id.to_string());

        conf.append_line("");
        // Replication-related configurations, such as WAL sending
        match &self.replication {
            Replication::Primary => {
                // Configure backpressure
                // - Replication write lag depends on how fast the walreceiver can process incoming WAL.
                //   This lag determines latency of get_page_at_lsn. Speed of applying WAL is about 10MB/sec,
                //   so to avoid expiration of 1 minute timeout, this lag should not be larger than 600MB.
                //   Actually latency should be much smaller (better if < 1sec). But we assume that recently
                //   updates pages are not requested from pageserver.
                // - Replication flush lag depends on speed of persisting data by checkpointer (creation of
                //   delta/image layers) and advancing disk_consistent_lsn. Safekeepers are able to
                //   remove/archive WAL only beyond disk_consistent_lsn. Too large a lag can cause long
                //   recovery time (in case of pageserver crash) and disk space overflow at safekeepers.
                // - Replication apply lag depends on speed of uploading changes to S3 by uploader thread.
                //   To be able to restore database in case of pageserver node crash, safekeeper should not
                //   remove WAL beyond this point. Too large lag can cause space exhaustion in safekeepers
                //   (if they are not able to upload WAL to S3).
                conf.append("max_replication_write_lag", "15MB");
                conf.append("max_replication_flush_lag", "10GB");

                if !self.env.safekeepers.is_empty() {
                    // Configure Postgres to connect to the safekeepers
                    conf.append("synchronous_standby_names", "walproposer");

                    let safekeepers = self
                        .env
                        .safekeepers
                        .iter()
                        .map(|sk| format!("localhost:{}", sk.pg_port))
                        .collect::<Vec<String>>()
                        .join(",");
                    conf.append("neon.safekeepers", &safekeepers);
                } else {
                    // We only use setup without safekeepers for tests,
                    // and don't care about data durability on pageserver,
                    // so set more relaxed synchronous_commit.
                    conf.append("synchronous_commit", "remote_write");

                    // Configure the node to stream WAL directly to the pageserver
                    // This isn't really a supported configuration, but can be useful for
                    // testing.
                    conf.append("synchronous_standby_names", "pageserver");
                }
            }
            Replication::Static(lsn) => {
                conf.append("recovery_target_lsn", &lsn.to_string());
            }
            Replication::Replica => {
                assert!(!self.env.safekeepers.is_empty());

                // TODO: use future host field from safekeeper spec
                // Pass the list of safekeepers to the replica so that it can connect to any of them,
                // whichever is alailiable.
                let sk_ports = self
                    .env
                    .safekeepers
                    .iter()
                    .map(|x| x.pg_port.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let sk_hosts = vec!["localhost"; self.env.safekeepers.len()].join(",");

                let connstr = format!(
                    "host={} port={} options='-c timeline_id={} tenant_id={}' application_name=replica replication=true",
                    sk_hosts,
                    sk_ports,
                    &self.timeline_id.to_string(),
                    &self.tenant_id.to_string(),
                );

                let slot_name = format!("repl_{}_", self.timeline_id);
                conf.append("primary_conninfo", connstr.as_str());
                conf.append("primary_slot_name", slot_name.as_str());
                conf.append("hot_standby", "on");
            }
        }

        let mut file = File::create(self.pgdata().join("postgresql.conf"))?;
        file.write_all(conf.to_string().as_bytes())?;

        let mut file = File::create(self.pgdata().join("PG_VERSION"))?;
        file.write_all(self.pg_version.to_string().as_bytes())?;

        Ok(())
    }

    fn load_basebackup(&self, auth_token: &Option<String>) -> Result<()> {
        let backup_lsn = match &self.replication {
            Replication::Primary => {
                if !self.env.safekeepers.is_empty() {
                    // LSN 0 means that it is bootstrap and we need to download just
                    // latest data from the pageserver. That is a bit clumsy but whole bootstrap
                    // procedure evolves quite actively right now, so let's think about it again
                    // when things would be more stable (TODO).
                    let lsn = self.sync_safekeepers(auth_token, self.pg_version)?;
                    if lsn == Lsn(0) {
                        None
                    } else {
                        Some(lsn)
                    }
                } else {
                    None
                }
            }
            Replication::Static(lsn) => Some(*lsn),
            Replication::Replica => {
                None // Take the latest snapshot available to start with
            }
        };

        self.do_basebackup(backup_lsn)?;

        Ok(())
    }

    pub fn endpoint_path(&self) -> PathBuf {
        self.env.endpoints_path().join(&self.name)
    }

    pub fn pgdata(&self) -> PathBuf {
        self.endpoint_path().join("pgdata")
    }

    pub fn status(&self) -> &str {
        let timeout = Duration::from_millis(300);
        let has_pidfile = self.pgdata().join("postmaster.pid").exists();
        let can_connect = TcpStream::connect_timeout(&self.address, timeout).is_ok();

        match (has_pidfile, can_connect) {
            (true, true) => "running",
            (false, false) => "stopped",
            (true, false) => "crashed",
            (false, true) => "running, no pidfile",
        }
    }

    fn pg_ctl(&self, args: &[&str], auth_token: &Option<String>) -> Result<()> {
        let pg_ctl_path = self.env.pg_bin_dir(self.pg_version)?.join("pg_ctl");
        let mut cmd = Command::new(&pg_ctl_path);
        cmd.args(
            [
                &[
                    "-D",
                    self.pgdata().to_str().unwrap(),
                    "-l",
                    self.pgdata().join("pg.log").to_str().unwrap(),
                    "-w", //wait till pg_ctl actually does what was asked
                ],
                args,
            ]
            .concat(),
        )
        .env_clear()
        .env(
            "LD_LIBRARY_PATH",
            self.env.pg_lib_dir(self.pg_version)?.to_str().unwrap(),
        )
        .env(
            "DYLD_LIBRARY_PATH",
            self.env.pg_lib_dir(self.pg_version)?.to_str().unwrap(),
        );

        // Pass authentication token used for the connections to pageserver and safekeepers
        if let Some(token) = auth_token {
            cmd.env("NEON_AUTH_TOKEN", token);
        }

        let pg_ctl = cmd
            .output()
            .context(format!("{} failed", pg_ctl_path.display()))?;
        if !pg_ctl.status.success() {
            anyhow::bail!(
                "pg_ctl failed, exit code: {}, stdout: {}, stderr: {}",
                pg_ctl.status,
                String::from_utf8_lossy(&pg_ctl.stdout),
                String::from_utf8_lossy(&pg_ctl.stderr),
            );
        }
        Ok(())
    }

    pub fn start(&self, auth_token: &Option<String>) -> Result<()> {
        if self.status() == "running" {
            anyhow::bail!("The endpoint is already running");
        }

        // 1. We always start Postgres from scratch, so
        // if old dir exists, preserve 'postgresql.conf' and drop the directory
        let postgresql_conf_path = self.pgdata().join("postgresql.conf");
        let postgresql_conf = fs::read(&postgresql_conf_path).with_context(|| {
            format!(
                "failed to read config file in {}",
                postgresql_conf_path.to_str().unwrap()
            )
        })?;
        fs::remove_dir_all(self.pgdata())?;
        self.create_pgdata()?;

        // 2. Bring back config files
        fs::write(&postgresql_conf_path, postgresql_conf)?;

        // 3. Load basebackup
        self.load_basebackup(auth_token)?;

        if self.replication != Replication::Primary {
            File::create(self.pgdata().join("standby.signal"))?;
        }

        // 4. Finally start postgres
        println!("Starting postgres at '{}'", self.connstr());
        self.pg_ctl(&["start"], auth_token)
    }

    pub fn stop(&self, destroy: bool) -> Result<()> {
        // If we are going to destroy data directory,
        // use immediate shutdown mode, otherwise,
        // shutdown gracefully to leave the data directory sane.
        //
        // Postgres is always started from scratch, so stop
        // without destroy only used for testing and debugging.
        //
        if destroy {
            self.pg_ctl(&["-m", "immediate", "stop"], &None)?;
            println!(
                "Destroying postgres data directory '{}'",
                self.pgdata().to_str().unwrap()
            );
            fs::remove_dir_all(self.endpoint_path())?;
        } else {
            self.pg_ctl(&["stop"], &None)?;
        }
        Ok(())
    }

    pub fn connstr(&self) -> String {
        format!(
            "host={} port={} user={} dbname={}",
            self.address.ip(),
            self.address.port(),
            "cloud_admin",
            "postgres"
        )
    }
}
