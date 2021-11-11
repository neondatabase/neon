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
use zenith_utils::connstring::connection_host_port;
use zenith_utils::lsn::Lsn;
use zenith_utils::postgres_backend::AuthType;
use zenith_utils::zid::ZTenantId;
use zenith_utils::zid::ZTimelineId;

use crate::local_env::LocalEnv;
use crate::postgresql_conf::PostgresConf;
use crate::storage::PageServerNode;

//
// ComputeControlPlane
//
pub struct ComputeControlPlane {
    base_port: u16,
    pageserver: Arc<PageServerNode>,
    pub nodes: BTreeMap<(ZTenantId, String), Arc<PostgresNode>>,
    env: LocalEnv,
}

impl ComputeControlPlane {
    // Load current nodes with ports from data directories on disk
    // Directory structure has the following layout:
    // pgdatadirs
    // |- tenants
    // |  |- <tenant_id>
    // |  |   |- <branch name>
    pub fn load(env: LocalEnv) -> Result<ComputeControlPlane> {
        let pageserver = Arc::new(PageServerNode::from_env(&env));

        let mut nodes = BTreeMap::default();
        let pgdatadirspath = &env.pg_data_dirs_path();

        for tenant_dir in fs::read_dir(&pgdatadirspath)
            .with_context(|| format!("failed to list {}", pgdatadirspath.display()))?
        {
            let tenant_dir = tenant_dir?;
            for timeline_dir in fs::read_dir(tenant_dir.path())
                .with_context(|| format!("failed to list {}", tenant_dir.path().display()))?
            {
                let node = PostgresNode::from_dir_entry(timeline_dir?, &env, &pageserver)?;
                nodes.insert((node.tenantid, node.name.clone()), Arc::new(node));
            }
        }

        Ok(ComputeControlPlane {
            base_port: 55431,
            pageserver,
            nodes,
            env,
        })
    }

    fn get_port(&mut self) -> u16 {
        1 + self
            .nodes
            .iter()
            .map(|(_name, node)| node.address.port())
            .max()
            .unwrap_or(self.base_port)
    }

    // FIXME: see also parse_point_in_time in branches.rs.
    fn parse_timeline_spec(
        &self,
        tenantid: ZTenantId,
        s: &str,
        replica_of: Option<&str>,
    ) -> Result<(ZTimelineId, Replication)> {
        let mut strings = s.split('@');
        let name = strings.next().unwrap();

        let target;

        if let Some(replspec) = strings.next() {
            target = Replication::Static(
                Lsn::from_str(replspec)
                    .with_context(|| "invalid LSN in point-in-time specification")?,
            );
        } else if let Some(name) = replica_of {
            return Ok((
                ZTimelineId::from_str(s)?,
                Replication::Replica(name.to_string()),
            ));
        } else {
            target = Replication::Primary
        }

        // Resolve the timeline ID, given the human-readable branch name
        let timeline_id = self
            .pageserver
            .branch_get_by_name(&tenantid, name)?
            .timeline_id;

        Ok((timeline_id, target))
    }

    pub fn new_node(
        &mut self,
        tenantid: ZTenantId,
        name: &str,
        timeline_name: &str,
        port: Option<u16>,
        replica_of: Option<&str>,
    ) -> Result<Arc<PostgresNode>> {
        // Resolve the human-readable timeline spec into timeline ID and LSN
        let (timelineid, replication) =
            self.parse_timeline_spec(tenantid, timeline_name, replica_of)?;

        let port = port.unwrap_or_else(|| self.get_port());
        let node = Arc::new(PostgresNode {
            name: name.to_owned(),
            address: SocketAddr::new("127.0.0.1".parse().unwrap(), port),
            env: self.env.clone(),
            pageserver: Arc::clone(&self.pageserver),
            is_test: false,
            timelineid,
            replication,
            tenantid,
            uses_wal_proposer: !self.env.safekeepers.is_empty(),
        });

        node.create_pgdata()?;
        node.setup_pg_conf(self.env.pageserver.auth_type)?;

        self.nodes
            .insert((tenantid, node.name.clone()), Arc::clone(&node));

        Ok(node)
    }
}

///////////////////////////////////////////////////////////////////////////////
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Replication {
    Primary,
    Static(Lsn),
    Replica(String),
}

#[derive(Debug)]
pub struct PostgresNode {
    pub address: SocketAddr,
    name: String,
    pub env: LocalEnv,
    pageserver: Arc<PageServerNode>,
    is_test: bool,
    pub timelineid: ZTimelineId,
    pub replication: Replication,
    pub tenantid: ZTenantId,
    uses_wal_proposer: bool,
}

impl PostgresNode {
    fn from_dir_entry(
        entry: std::fs::DirEntry,
        env: &LocalEnv,
        pageserver: &Arc<PageServerNode>,
    ) -> Result<PostgresNode> {
        if !entry.file_type()?.is_dir() {
            anyhow::bail!(
                "PostgresNode::from_dir_entry failed: '{}' is not a directory",
                entry.path().display()
            );
        }

        // parse data directory name
        let fname = entry.file_name();
        let name = fname.to_str().unwrap().to_string();

        // Read config file into memory
        let cfg_path = entry.path().join("postgresql.conf");
        let cfg_path_str = cfg_path.to_string_lossy();
        let mut conf_file = File::open(&cfg_path)
            .with_context(|| format!("failed to open config file in {}", cfg_path_str))?;
        let conf = PostgresConf::read(&mut conf_file)
            .with_context(|| format!("failed to read config file in {}", cfg_path_str))?;

        // Read a few options from the config file
        let context = format!("in config file {}", cfg_path_str);
        let port: u16 = conf.parse_field("port", &context)?;
        let timelineid: ZTimelineId = conf.parse_field("zenith.zenith_timeline", &context)?;
        let tenantid: ZTenantId = conf.parse_field("zenith.zenith_tenant", &context)?;
        let uses_wal_proposer = conf.get("wal_acceptors").is_some();

        // parse recovery_target_lsn and primary_conninfo into Recovery Target, if any
        let replication;

        if conf.get("recovery_target_lsn").is_some() {
            let lsn = conf
                .parse_field_optional("recovery_target_lsn", &context)?
                .with_context(|| "Unreadable recovery target LSN")?;
            replication = Replication::Static(lsn);
        } else if let Some(slot_name) = conf.get("primary_slot_name") {
            let slot_name = slot_name.to_string();
            println!("{}", conf.to_string());
            let prefix = format!("repl_{}_{}_", timelineid, name);
            assert!(slot_name.starts_with(&prefix));
            replication = Replication::Replica(slot_name[prefix.len()..].to_owned());
        } else {
            replication = Replication::Primary;
        }

        // ok now
        Ok(PostgresNode {
            address: SocketAddr::new("127.0.0.1".parse().unwrap(), port),
            name,
            env: env.clone(),
            pageserver: Arc::clone(pageserver),
            is_test: false,
            timelineid,
            replication,
            tenantid,
            uses_wal_proposer,
        })
    }

    fn sync_safekeepers(&self) -> Result<Lsn> {
        let pg_path = self.env.pg_bin_dir().join("postgres");
        let sync_handle = Command::new(pg_path)
            .arg("--sync-safekeepers")
            .env_clear()
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .env("PGDATA", self.pgdata().to_str().unwrap())
            .stdout(Stdio::piped())
            // Comment this to avoid capturing stderr (useful if command hangs)
            .stderr(Stdio::piped())
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
            format!("basebackup {} {} {}", self.tenantid, self.timelineid, lsn)
        } else {
            format!("basebackup {} {}", self.tenantid, self.timelineid)
        };

        let mut client = self
            .pageserver
            .page_server_psql_client()
            .with_context(|| "connecting to page server failed")?;

        let copyreader = client
            .copy_out(sql.as_str())
            .with_context(|| "page server 'basebackup' command failed")?;

        // Read the archive directly from the `CopyOutReader`
        tar::Archive::new(copyreader)
            .unpack(&self.pgdata())
            .with_context(|| "extracting base backup failed")?;

        Ok(())
    }

    fn create_pgdata(&self) -> Result<()> {
        fs::create_dir_all(&self.pgdata()).with_context(|| {
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

    // Connect to a page server, get base backup, and untar it to initialize a
    // new data directory
    fn setup_pg_conf(&self, auth_type: AuthType) -> Result<()> {
        let mut conf = PostgresConf::new();
        conf.append("max_wal_senders", "10");
        // wal_log_hints is mandatory when running against pageserver (see gh issue#192)
        // TODO: is it possible to check wal_log_hints at pageserver side via XLOG_PARAMETER_CHANGE?
        conf.append("wal_log_hints", "on");
        conf.append("max_replication_slots", "10");
        conf.append("hot_standby", "on");
        conf.append("shared_buffers", "1MB");
        conf.append("fsync", "off");
        conf.append("max_connections", "100");
        conf.append("wal_sender_timeout", "0");
        conf.append("wal_level", "replica");
        conf.append("listen_addresses", &self.address.ip().to_string());
        conf.append("port", &self.address.port().to_string());

        // Configure the node to fetch pages from pageserver
        let pageserver_connstr = {
            let (host, port) = connection_host_port(&self.pageserver.pg_connection_config);

            // Set up authentication
            //
            // $ZENITH_AUTH_TOKEN will be replaced with value from environment
            // variable during compute pg startup. It is done this way because
            // otherwise user will be able to retrieve the value using SHOW
            // command or pg_settings
            let password = if let AuthType::ZenithJWT = auth_type {
                "$ZENITH_AUTH_TOKEN"
            } else {
                ""
            };

            format!("host={} port={} password={}", host, port, password)
        };
        conf.append("shared_preload_libraries", "zenith");
        conf.append_line("");
        conf.append("zenith.page_server_connstring", &pageserver_connstr);
        conf.append("zenith.zenith_tenant", &self.tenantid.to_string());
        conf.append("zenith.zenith_timeline", &self.timelineid.to_string());

        conf.append_line("");

        // Replication-related configurations, such as WAL sending
        match &self.replication {
            Replication::Primary => {
                if !self.env.safekeepers.is_empty() {
                    // Configure the node to connect to the safekeepers
                    conf.append("synchronous_standby_names", "walproposer");

                    let wal_acceptors = self
                        .env
                        .safekeepers
                        .iter()
                        .map(|sk| format!("localhost:{}", sk.pg_port))
                        .collect::<Vec<String>>()
                        .join(",");
                    conf.append("wal_acceptors", &wal_acceptors);
                } else {
                    // On the primary, never clean up old WAL.
                    // slot or something proper, to prevent the compute node
                    // from removing WAL that hasn't been streamed to the page server yet.
                    // Safekeeper is configured with a replication slot, so there this
                    // wal_keep_size is actively harmful in preventing local WAL cleanup.
                    // (gh issue #349)
                    conf.append("wal_keep_size", "10TB");

                    // Configure the node to stream WAL directly to the pageserver
                    // This isn't really a supported configuration, but can be useful for
                    // testing.
                    conf.append("synchronous_standby_names", "pageserver");
                    conf.append("zenith.callmemaybe_connstring", &self.connstr());
                }
            }
            Replication::Static(lsn) => {
                conf.append("recovery_target_lsn", &lsn.to_string());
            }
            Replication::Replica(of) => {
                assert!(!self.env.safekeepers.is_empty());

                // TODO: use future host field from safekeeper spec
                let connstr = format!(
                    "host=localhost port={} options='-c ztimelineid={} ztenantid={}' application_name=replica replication=true",
                    self.env.safekeepers[0].pg_port,
                    &self.timelineid.to_string(),
                    &self.tenantid.to_string(),
                );
                let slot_name =
                    format!("repl_{}_{}_{}", self.timelineid.to_string(), self.name, of);
                conf.append("primary_conninfo", connstr.as_str());
                conf.append("primary_slot_name", slot_name.as_str());
                conf.append("hot_standby", "on");
            }
        }

        let mut file = File::create(self.pgdata().join("postgresql.conf"))?;
        file.write_all(conf.to_string().as_bytes())?;

        Ok(())
    }

    fn load_basebackup(&self) -> Result<()> {
        let backup_lsn = match &self.replication {
            Replication::Primary => {
                if self.uses_wal_proposer {
                    let lsn = self.sync_safekeepers()?;
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
            Replication::Replica(..) => {
                None // Take the latest snapshot available to start with
            }
        };

        self.do_basebackup(backup_lsn)?;

        Ok(())
    }

    pub fn pgdata(&self) -> PathBuf {
        self.env.pg_data_dir(&self.tenantid, &self.name)
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
        let pg_ctl_path = self.env.pg_bin_dir().join("pg_ctl");
        let mut cmd = Command::new(pg_ctl_path);
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
        .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
        .env("DYLD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap());

        if let Some(token) = auth_token {
            cmd.env("ZENITH_AUTH_TOKEN", token);
        }
        let pg_ctl = cmd.status().with_context(|| "pg_ctl failed")?;

        if !pg_ctl.success() {
            anyhow::bail!("pg_ctl failed");
        }
        Ok(())
    }

    pub fn start(&self, auth_token: &Option<String>) -> Result<()> {
        // Bail if the node already running.
        if self.status() == "running" {
            anyhow::bail!("The node is already running");
        }

        // 1. We always start compute node from scratch, so
        // if old dir exists, preserve 'postgresql.conf' and drop the directory
        let postgresql_conf_path = self.pgdata().join("postgresql.conf");
        let postgresql_conf = fs::read(&postgresql_conf_path).with_context(|| {
            format!(
                "failed to read config file in {}",
                postgresql_conf_path.to_str().unwrap()
            )
        })?;
        fs::remove_dir_all(&self.pgdata())?;
        self.create_pgdata()?;

        // 2. Bring back config files
        fs::write(&postgresql_conf_path, postgresql_conf)?;

        // 3. Load basebackup
        self.load_basebackup()?;

        if self.replication != Replication::Primary {
            File::create(self.pgdata().join("standby.signal"))?;
        }

        // 4. Finally start the compute node postgres
        println!("Starting postgres node at '{}'", self.connstr());
        self.pg_ctl(&["start"], auth_token)
    }

    pub fn restart(&self, auth_token: &Option<String>) -> Result<()> {
        self.pg_ctl(&["restart"], auth_token)
    }

    pub fn stop(&self, destroy: bool) -> Result<()> {
        // If we are going to destroy data directory,
        // use immediate shutdown mode, otherwise,
        // shutdown gracefully to leave the data directory sane.
        //
        // Compute node always starts from scratch, so stop
        // without destroy only used for testing and debugging.
        //
        if destroy {
            self.pg_ctl(&["-m", "immediate", "stop"], &None)?;
            println!(
                "Destroying postgres data directory '{}'",
                self.pgdata().to_str().unwrap()
            );
            fs::remove_dir_all(&self.pgdata())?;
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
            "zenith_admin",
            "postgres"
        )
    }

    // XXX: cache that in control plane
    pub fn whoami(&self) -> String {
        let output = Command::new("whoami")
            .output()
            .expect("failed to execute whoami");

        assert!(output.status.success(), "whoami failed");

        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }
}

impl Drop for PostgresNode {
    // destructor to clean up state after test is done
    // XXX: we may detect failed test by setting some flag in catch_unwind()
    // and checking it here. But let just clean datadirs on start.
    fn drop(&mut self) {
        if self.is_test {
            let _ = self.stop(true);
        }
    }
}
