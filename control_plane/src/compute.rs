use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::net::TcpStream;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{Context, Result};
use lazy_static::lazy_static;
use regex::Regex;

use postgres::{Client, NoTls};

use crate::local_env::LocalEnv;
use crate::storage::{PageServerNode, WalProposerNode};
use pageserver::{zenith_repo_dir, ZTimelineId};

//
// ComputeControlPlane
//
pub struct ComputeControlPlane {
    base_port: u16,
    pageserver: Arc<PageServerNode>,
    pub nodes: BTreeMap<String, Arc<PostgresNode>>,
    env: LocalEnv,
}

impl ComputeControlPlane {
    // Load current nodes with ports from data directories on disk
    pub fn load(env: LocalEnv) -> Result<ComputeControlPlane> {
        // TODO: since pageserver do not have config file yet we believe here that
        // it is running on default port. Change that when pageserver will have config.
        let pageserver = Arc::new(PageServerNode::from_env(&env));

        let pgdatadirspath = env.repo_path.join("pgdatadirs");
        let nodes: Result<BTreeMap<_, _>> = fs::read_dir(&pgdatadirspath)
            .with_context(|| format!("failed to list {}", pgdatadirspath.display()))?
            .into_iter()
            .map(|f| {
                PostgresNode::from_dir_entry(f?, &env, &pageserver)
                    .map(|node| (node.name.clone(), Arc::new(node)))
            })
            .collect();
        let nodes = nodes?;

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

    pub fn local(local_env: &LocalEnv, pageserver: &Arc<PageServerNode>) -> ComputeControlPlane {
        ComputeControlPlane {
            base_port: 65431,
            pageserver: Arc::clone(pageserver),
            nodes: BTreeMap::new(),
            env: local_env.clone(),
        }
    }

    /// Connect to a page server, get base backup, and untar it to initialize a
    /// new data directory
    pub fn new_from_page_server(
        &mut self,
        is_test: bool,
        timelineid: ZTimelineId,
    ) -> Result<Arc<PostgresNode>> {
        let node_id = self.nodes.len() as u32 + 1;

        let node = Arc::new(PostgresNode {
            name: format!("pg{}", node_id),
            address: SocketAddr::new("127.0.0.1".parse().unwrap(), self.get_port()),
            env: self.env.clone(),
            pageserver: Arc::clone(&self.pageserver),
            is_test,
            timelineid,
        });

        node.init_from_page_server()?;
        self.nodes.insert(node.name.clone(), Arc::clone(&node));

        Ok(node)
    }

    pub fn new_test_node(&mut self, timelineid: ZTimelineId) -> Arc<PostgresNode> {
        let node = self.new_from_page_server(true, timelineid);
        assert!(node.is_ok());
        let node = node.unwrap();

        // Configure the node to stream WAL directly to the pageserver
        node.append_conf(
            "postgresql.conf",
            format!(
                "callmemaybe_connstring = '{}'\n", // FIXME escaping
                node.connstr()
            )
            .as_str(),
        );

        node
    }

    pub fn new_test_master_node(&mut self, timelineid: ZTimelineId) -> Arc<PostgresNode> {
        let node = self.new_from_page_server(true, timelineid).unwrap();

        node.append_conf(
            "postgresql.conf",
            "synchronous_standby_names = 'safekeeper_proxy'\n",
        );

        node
    }

    pub fn new_node(&mut self, timelineid: ZTimelineId) -> Result<Arc<PostgresNode>> {
        let node = self.new_from_page_server(false, timelineid).unwrap();

        // Configure the node to stream WAL directly to the pageserver
        node.append_conf(
            "postgresql.conf",
            format!(
                "callmemaybe_connstring = '{}'\n", // FIXME escaping
                node.connstr()
            )
            .as_str(),
        );

        Ok(node)
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct PostgresNode {
    pub address: SocketAddr,
    name: String,
    pub env: LocalEnv,
    pageserver: Arc<PageServerNode>,
    is_test: bool,
    timelineid: ZTimelineId,
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

        lazy_static! {
            static ref CONF_PORT_RE: Regex = Regex::new(r"(?m)^\s*port\s*=\s*(\d+)\s*$").unwrap();
        }

        // parse data directory name
        let fname = entry.file_name();
        let name = fname.to_str().unwrap().to_string();

        // find out tcp port in config file
        let cfg_path = entry.path().join("postgresql.conf");
        let config = fs::read_to_string(cfg_path.clone()).with_context(|| {
            format!(
                "failed to read config file in {}",
                cfg_path.to_str().unwrap()
            )
        })?;

        let err_msg = format!(
            "failed to find port definition in config file {}",
            cfg_path.to_str().unwrap()
        );
        let port: u16 = CONF_PORT_RE
            .captures(config.as_str())
            .ok_or_else(|| anyhow::Error::msg(err_msg.clone() + " 1"))?
            .iter()
            .last()
            .ok_or_else(|| anyhow::Error::msg(err_msg.clone() + " 2"))?
            .ok_or_else(|| anyhow::Error::msg(err_msg.clone() + " 3"))?
            .as_str()
            .parse()
            .with_context(|| err_msg)?;

        // FIXME: What timeline is this server on? Would have to parse the postgresql.conf
        // file for that, too. It's currently not needed for anything, but it would be
        // nice to list the timeline in "zenith pg list"
        let timelineid_buf = [0u8; 16];
        let timelineid = ZTimelineId::from(timelineid_buf);

        // ok now
        Ok(PostgresNode {
            address: SocketAddr::new("127.0.0.1".parse().unwrap(), port),
            name,
            env: env.clone(),
            pageserver: Arc::clone(pageserver),
            is_test: false,
            timelineid,
        })
    }

    // Connect to a page server, get base backup, and untar it to initialize a
    // new data directory
    pub fn init_from_page_server(&self) -> Result<()> {
        let pgdata = self.pgdata();

        println!(
            "Extracting base backup to create postgres instance: path={} port={}",
            pgdata.display(),
            self.address.port()
        );

        // initialize data directory
        if self.is_test {
            fs::remove_dir_all(&pgdata).ok();
        }

        let sql = format!("basebackup {}", self.timelineid);
        let mut client = self
            .pageserver
            .page_server_psql_client()
            .with_context(|| "connecting to page server failed")?;

        fs::create_dir_all(&pgdata)
            .with_context(|| format!("could not create data directory {}", pgdata.display()))?;
        fs::set_permissions(pgdata.as_path(), fs::Permissions::from_mode(0o700)).with_context(
            || {
                format!(
                    "could not set permissions in data directory {}",
                    pgdata.display()
                )
            },
        )?;

        // FIXME: The compute node should be able to stream the WAL it needs from the WAL safekeepers or archive.
        // But that's not implemented yet. For now, 'pg_wal' is included in the base backup tarball that
        // we receive from the Page Server, so we don't need to create the empty 'pg_wal' directory here.
        //fs::create_dir_all(pgdata.join("pg_wal"))?;

        let mut copyreader = client
            .copy_out(sql.as_str())
            .with_context(|| "page server 'basebackup' command failed")?;

        // FIXME: Currently, we slurp the whole tarball into memory, and then extract it,
        // but we really should do this:
        //let mut ar = tar::Archive::new(copyreader);
        let mut buf = vec![];
        copyreader
            .read_to_end(&mut buf)
            .with_context(|| "reading base backup from page server failed")?;
        let mut ar = tar::Archive::new(buf.as_slice());
        ar.unpack(&pgdata)
            .with_context(|| "extracting page backup failed")?;

        // listen for selected port
        self.append_conf(
            "postgresql.conf",
            &format!(
                "max_wal_senders = 10\n\
                 max_replication_slots = 10\n\
                 hot_standby = on\n\
                 shared_buffers = 1MB\n\
				 fsync = off\n\
                 max_connections = 100\n\
				 wal_sender_timeout = 0\n\
                 wal_level = replica\n\
                 listen_addresses = '{address}'\n\
                 port = {port}\n",
                address = self.address.ip(),
                port = self.address.port()
            ),
        );

        // Never clean up old WAL. TODO: We should use a replication
        // slot or something proper, to prevent the compute node
        // from removing WAL that hasn't been streamed to the safekeepr or
        // page server yet. But this will do for now.
        self.append_conf("postgresql.conf", "wal_keep_size='10TB'\n");

        // Connect it to the page server.

        // Configure that node to take pages from pageserver
        self.append_conf(
            "postgresql.conf",
            &format!(
                "page_server_connstring = 'host={} port={}'\n\
                      zenith_timeline='{}'\n",
                self.pageserver.address().ip(),
                self.pageserver.address().port(),
                self.timelineid
            ),
        );

        Ok(())
    }

    fn pgdata(&self) -> PathBuf {
        self.env.repo_path.join("pgdatadirs").join(&self.name)
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

    pub fn append_conf(&self, config: &str, opts: &str) {
        OpenOptions::new()
            .append(true)
            .open(self.pgdata().join(config).to_str().unwrap())
            .unwrap()
            .write_all(opts.as_bytes())
            .unwrap();
    }

    fn pg_ctl(&self, args: &[&str]) -> Result<()> {
        let pg_ctl_path = self.env.pg_bin_dir().join("pg_ctl");

        let pg_ctl = Command::new(pg_ctl_path)
            .args(
                [
                    &[
                        "-D",
                        self.pgdata().to_str().unwrap(),
                        "-l",
                        self.pgdata().join("log").to_str().unwrap(),
                    ],
                    args,
                ]
                .concat(),
            )
            .env_clear()
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .status()
            .with_context(|| "pg_ctl failed")?;
        if !pg_ctl.success() {
            anyhow::bail!("pg_ctl failed");
        }
        Ok(())
    }

    pub fn start(&self) -> Result<()> {
        println!("Starting postgres node at '{}'", self.connstr());
        self.pg_ctl(&["start"])
    }

    pub fn restart(&self) -> Result<()> {
        self.pg_ctl(&["restart"])
    }

    pub fn stop(&self) -> Result<()> {
        self.pg_ctl(&["-m", "immediate", "stop"])
    }

    pub fn connstr(&self) -> String {
        format!(
            "host={} port={} user={}",
            self.address.ip(),
            self.address.port(),
            self.whoami()
        )
    }

    // XXX: cache that in control plane
    pub fn whoami(&self) -> String {
        let output = Command::new("whoami")
            .output()
            .expect("failed to execute whoami");

        if !output.status.success() {
            panic!("whoami failed");
        }

        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    fn dump_log_files(&self) {
        let dir = zenith_repo_dir();
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let mut file = File::open(&path).unwrap();
            let mut buffer = String::new();
            file.read_to_string(&mut buffer).unwrap();
            println!("File {:?}:\n{}", &path, buffer);
        }
    }

    pub fn safe_psql(&self, db: &str, sql: &str) -> Vec<tokio_postgres::Row> {
        let connstring = format!(
            "host={} port={} dbname={} user={}",
            self.address.ip(),
            self.address.port(),
            db,
            self.whoami()
        );
        let mut client = Client::connect(connstring.as_str(), NoTls).unwrap();

        println!("Running {}", sql);
        let result = client.query(sql, &[]);
        if result.is_err() {
            self.dump_log_files();
        }
        result.unwrap()
    }

    pub fn open_psql(&self, db: &str) -> Client {
        let connstring = format!(
            "host={} port={} dbname={} user={}",
            self.address.ip(),
            self.address.port(),
            db,
            self.whoami()
        );
        Client::connect(connstring.as_str(), NoTls).unwrap()
    }

    pub fn start_proxy(&self, wal_acceptors: &str) -> WalProposerNode {
        let proxy_path = self.env.pg_bin_dir().join("safekeeper_proxy");
        match Command::new(proxy_path.as_path())
            .args(&["--ztimelineid", &self.timelineid.to_string()])
            .args(&["-s", wal_acceptors])
            .args(&["-h", &self.address.ip().to_string()])
            .args(&["-p", &self.address.port().to_string()])
            .arg("-v")
            .stderr(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.pgdata().join("safekeeper_proxy.log"))
                    .unwrap(),
            )
            .spawn()
        {
            Ok(child) => WalProposerNode { pid: child.id() },
            Err(e) => panic!("Failed to launch {:?}: {}", proxy_path, e),
        }
    }

    pub fn pg_regress(&self) {
        self.safe_psql("postgres", "CREATE DATABASE regression");
        let data_dir = zenith_repo_dir();
        let regress_run_path = data_dir.join("regress");
        fs::create_dir_all(&regress_run_path).unwrap();
        fs::create_dir_all(regress_run_path.join("testtablespace")).unwrap();
        std::env::set_current_dir(regress_run_path).unwrap();

        let regress_build_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../tmp_install/build/src/test/regress");
        let regress_src_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../vendor/postgres/src/test/regress");

        let _regress_check = Command::new(regress_build_path.join("pg_regress"))
            .args(&[
                "--bindir=''",
                "--use-existing",
                format!("--bindir={}", self.env.pg_bin_dir().to_str().unwrap()).as_str(),
                format!("--dlpath={}", regress_build_path.to_str().unwrap()).as_str(),
                format!(
                    "--schedule={}",
                    regress_src_path.join("parallel_schedule").to_str().unwrap()
                )
                .as_str(),
                format!("--inputdir={}", regress_src_path.to_str().unwrap()).as_str(),
            ])
            .env_clear()
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .env("PGPORT", self.address.port().to_string())
            .env("PGUSER", self.whoami())
            .env("PGHOST", self.address.ip().to_string())
            .status()
            .expect("pg_regress failed");
    }

    pub fn pg_bench(&self, clients: u32, seconds: u32) {
        let port = self.address.port().to_string();
        let clients = clients.to_string();
        let seconds = seconds.to_string();
        let _pg_bench_init = Command::new(self.env.pg_bin_dir().join("pgbench"))
            .args(&["-i", "-p", port.as_str(), "postgres"])
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .status()
            .expect("pgbench -i");
        let _pg_bench_run = Command::new(self.env.pg_bin_dir().join("pgbench"))
            .args(&[
                "-p",
                port.as_str(),
                "-T",
                seconds.as_str(),
                "-P",
                "1",
                "-c",
                clients.as_str(),
                "-M",
                "prepared",
                "postgres",
            ])
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .env("DYLD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .status()
            .expect("pgbench run");
    }
}

impl Drop for PostgresNode {
    // destructor to clean up state after test is done
    // XXX: we may detect failed test by setting some flag in catch_unwind()
    // and checking it here. But let just clean datadirs on start.
    fn drop(&mut self) {
        if self.is_test {
            let _ = self.stop();
        }
    }
}
