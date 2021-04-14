use std::error;
use std::fs::File;
use std::fs::{self, OpenOptions};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;
use std::{collections::BTreeMap, path::PathBuf};
use std::{io::Write, net::SocketAddr};

use lazy_static::lazy_static;
use postgres::{Client, NoTls};
use regex::Regex;

use crate::local_env::{self, LocalEnv};
use crate::storage::{PageServerNode, WalProposerNode};

type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

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

        let nodes: Result<BTreeMap<_, _>> = fs::read_dir(env.compute_dir())
            .map_err(|e| {
                format!(
                    "failed to list {}: {}",
                    env.compute_dir().to_str().unwrap(),
                    e
                )
            })?
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

    pub fn local(pageserver: &Arc<PageServerNode>) -> ComputeControlPlane {
        let env = local_env::test_env();
        ComputeControlPlane {
            base_port: 65431,
            pageserver: Arc::clone(pageserver),
            nodes: BTreeMap::new(),
            env: env.clone(),
        }
    }

    fn new_vanilla_node(&mut self, is_test: bool) -> Result<Arc<PostgresNode>> {
        // allocate new node entry with generated port
        let node_id = self.nodes.len() as u32 + 1;
        let node = Arc::new(PostgresNode {
            name: format!("pg{}", node_id),
            address: SocketAddr::new("127.0.0.1".parse().unwrap(), self.get_port()),
            env: self.env.clone(),
            pageserver: Arc::clone(&self.pageserver),
            is_test,
        });
        node.init_vanilla()?;
        self.nodes.insert(node.name.clone(), Arc::clone(&node));

        Ok(node)
    }

    pub fn new_test_node(&mut self) -> Arc<PostgresNode> {
        let addr = self.pageserver.address().clone();
        let node = self.new_vanilla_node(true).unwrap();

        // Configure that node to take pages from pageserver
        node.append_conf(
            "postgresql.conf",
            format!(
                "page_server_connstring = 'host={} port={}'\n",
                addr.ip(),
                addr.port()
            )
            .as_str(),
        );

        node
    }

    pub fn new_test_master_node(&mut self) -> Arc<PostgresNode> {
        let node = self.new_vanilla_node(true).unwrap();

        node.append_conf(
            "postgresql.conf",
            "synchronous_standby_names = 'safekeeper_proxy'\n",
        );

        node
    }

    pub fn new_node(&mut self) -> Result<Arc<PostgresNode>> {
        let addr = self.pageserver.address().clone();
        let node = self.new_vanilla_node(false)?;

        // Configure that node to take pages from pageserver
        node.append_conf(
            "postgresql.conf",
            format!(
                "page_server_connstring = 'host={} port={}'\n",
                addr.ip(),
                addr.port()
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
}

impl PostgresNode {
    fn from_dir_entry(
        entry: std::fs::DirEntry,
        env: &LocalEnv,
        pageserver: &Arc<PageServerNode>,
    ) -> Result<PostgresNode> {
        if !entry.file_type()?.is_dir() {
            let err_msg = format!(
                "PostgresNode::from_dir_entry failed: '{}' is not a directory",
                entry.path().to_str().unwrap()
            );
            return Err(err_msg.into());
        }

        lazy_static! {
            static ref CONF_PORT_RE: Regex = Regex::new(r"(?m)^\s*port\s*=\s*(\d+)\s*$").unwrap();
        }

        // parse data directory name
        let fname = entry.file_name();
        let name = fname.to_str().unwrap().to_string();

        // find out tcp port in config file
        let cfg_path = entry.path().join("postgresql.conf");
        let config = fs::read_to_string(cfg_path.clone()).map_err(|e| {
            format!(
                "failed to read config file in {}: {}",
                cfg_path.to_str().unwrap(),
                e
            )
        })?;

        let err_msg = format!(
            "failed to find port definition in config file {}",
            cfg_path.to_str().unwrap()
        );
        let port: u16 = CONF_PORT_RE
            .captures(config.as_str())
            .ok_or(err_msg.clone() + " 1")?
            .iter()
            .last()
            .ok_or(err_msg.clone() + " 3")?
            .ok_or(err_msg.clone() + " 3")?
            .as_str()
            .parse()
            .map_err(|e| format!("{}: {}", err_msg, e))?;

        // ok now
        Ok(PostgresNode {
            address: SocketAddr::new("127.0.0.1".parse().unwrap(), port),
            name,
            env: env.clone(),
            pageserver: Arc::clone(pageserver),
            is_test: false,
        })
    }

    fn init_vanilla(&self) -> Result<()> {
        println!(
            "Creating new postgres: path={} port={}",
            self.pgdata().to_str().unwrap(),
            self.address.port()
        );

        // initialize data directory

        if self.is_test {
            fs::remove_dir_all(self.pgdata().to_str().unwrap()).ok();
        }

        fs::create_dir_all(self.pgdata().to_str().unwrap())?;

        let initdb_path = self.env.pg_bin_dir().join("initdb");
        let initdb = Command::new(initdb_path)
            .args(&["-D", self.pgdata().to_str().unwrap()])
            .arg("-N")
            .arg("-A trust")
            .arg("--no-instructions")
            .env_clear()
            .env("LD_LIBRARY_PATH", self.env.pg_lib_dir().to_str().unwrap())
            .stdout(Stdio::null())
            .status()?;

        if !initdb.success() {
            return Err("initdb failed".into());
        }

        // listen for selected port
        self.append_conf(
            "postgresql.conf",
            format!(
                "max_wal_senders = 10\n\
            max_replication_slots = 10\n\
            hot_standby = on\n\
            shared_buffers = 1MB\n\
            max_connections = 100\n\
            wal_level = replica\n\
            listen_addresses = '{address}'\n\
            port = {port}\n",
                address = self.address.ip(),
                port = self.address.port()
            )
            .as_str(),
        );

        println!("Database initialized");
        Ok(())
    }

    fn pgdata(&self) -> PathBuf {
        self.env.compute_dir().join(self.name.clone())
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
            .status()?;

        if !pg_ctl.success() {
            Err("pg_ctl failed".into())
        } else {
            Ok(())
        }
    }

    pub fn start(&self) -> Result<()> {
        let _res = self
            .pageserver
            .page_server_psql(format!("callmemaybe {}", self.connstr()).as_str());
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
        client.query(sql, &[]).unwrap()
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

    /* Create stub controlfile and respective xlog to start computenode */
    pub fn setup_controlfile(&self) {
        let filepath = format!("{}/global/pg_control", self.pgdata().to_str().unwrap());

        {
            File::create(filepath).unwrap();
        }

        let pg_resetwal_path = self.env.pg_bin_dir().join("pg_resetwal");

        let pg_resetwal = Command::new(pg_resetwal_path)
            .args(&["-D", self.pgdata().to_str().unwrap()])
            .arg("-f")
            // TODO probably we will have to modify pg_resetwal
            // .arg("--compute-node")
            .status()
            .expect("failed to execute pg_resetwal");

        if !pg_resetwal.success() {
            panic!("pg_resetwal failed");
        }
    }

    pub fn start_proxy(&self, wal_acceptors: String) -> WalProposerNode {
        let proxy_path = self.env.pg_bin_dir().join("safekeeper_proxy");
        match Command::new(proxy_path.as_path())
            .args(&["-s", &wal_acceptors])
            .args(&["-h", &self.address.ip().to_string()])
            .args(&["-p", &self.address.port().to_string()])
            .arg("-v")
            .stderr(File::create(self.env.data_dir.join("safepkeeper_proxy.log")).unwrap())
            .spawn()
        {
            Ok(child) => WalProposerNode { pid: child.id() },
            Err(e) => panic!("Failed to launch {:?}: {}", proxy_path, e),
        }
    }

    // TODO
    pub fn pg_bench() {}
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
